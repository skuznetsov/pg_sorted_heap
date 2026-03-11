#!/usr/bin/env bash
set -Eeuo pipefail

if [ "$#" -gt 2 ]; then
  echo "usage: $0 [tmp_root_abs_dir] [format=text|jsonl]" >&2
  exit 2
fi

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
FORMAT="${2:-text}"
if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root_abs_dir must be absolute: $TMP_ROOT" >&2
  exit 2
fi
if [ ! -d "$TMP_ROOT" ]; then
  echo "tmp_root_abs_dir not found: $TMP_ROOT" >&2
  exit 2
fi
if [ "$FORMAT" != "text" ] && [ "$FORMAT" != "jsonl" ]; then
  echo "unsupported format: $FORMAT (supported: text|jsonl)" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEMA_VERSION=1
total_elapsed=0
test_count=0
event_seq=0
RUNNER_PID="$$"
RUN_ID="$(date -u +"%Y%m%dT%H%M%SZ")-$RUNNER_PID"
RUN_LABEL="${LIGHTWEIGHT_SELFTEST_RUN_LABEL:-$RUN_ID}"
AUTO_TMP_CLEAN_MODE="${LIGHTWEIGHT_SELFTEST_AUTO_TMP_CLEAN:-off}"
AUTO_TMP_CLEAN_MIN_AGE_S="${LIGHTWEIGHT_SELFTEST_AUTO_TMP_CLEAN_MIN_AGE_S:-0}"
AUTO_TMP_CLEAN_ENABLED=0
ACTIVE_SCRIPT=""
ACTIVE_STARTED=0
IN_RUN_ONE=0

if ! [[ "$RUN_LABEL" =~ ^[A-Za-z0-9._:-]+$ ]]; then
  echo "LIGHTWEIGHT_SELFTEST_RUN_LABEL contains unsupported characters: $RUN_LABEL" >&2
  exit 2
fi

case "$AUTO_TMP_CLEAN_MODE" in
  off|0|false|no)
    AUTO_TMP_CLEAN_ENABLED=0
    ;;
  on|1|true|yes)
    AUTO_TMP_CLEAN_ENABLED=1
    ;;
  *)
    echo "LIGHTWEIGHT_SELFTEST_AUTO_TMP_CLEAN must be one of: off|on|0|1|false|true|no|yes (got: $AUTO_TMP_CLEAN_MODE)" >&2
    exit 2
    ;;
esac

if [ "$AUTO_TMP_CLEAN_ENABLED" -eq 1 ] && ! [[ "$AUTO_TMP_CLEAN_MIN_AGE_S" =~ ^[0-9]+$ ]]; then
  echo "LIGHTWEIGHT_SELFTEST_AUTO_TMP_CLEAN_MIN_AGE_S must be a non-negative integer when auto-clean is enabled: $AUTO_TMP_CLEAN_MIN_AGE_S" >&2
  exit 2
fi

TMP_CLEAN_SCRIPT="$SCRIPT_DIR/tmp_clean_pg_sorted_heap.sh"
if [ "$AUTO_TMP_CLEAN_ENABLED" -eq 1 ] && [ ! -x "$TMP_CLEAN_SCRIPT" ]; then
  echo "tmp-clean script not executable: $TMP_CLEAN_SCRIPT" >&2
  exit 2
fi

event_ts() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

emit_begin() {
  local script_name="$1"
  local ts
  local seq
  ts="$(event_ts)"
  event_seq=$((event_seq + 1))
  seq="$event_seq"
  if [ "$FORMAT" = "jsonl" ]; then
    echo "{\"schema_version\":$SCHEMA_VERSION,\"run_id\":\"$RUN_ID\",\"run_label\":\"$RUN_LABEL\",\"runner_pid\":$RUNNER_PID,\"event_seq\":$seq,\"event\":\"begin\",\"event_ts\":\"$ts\",\"script\":\"$script_name\"}"
  else
    echo "lightweight_selftest begin script=$script_name"
  fi
}

emit_ok() {
  local script_name="$1"
  local elapsed="$2"
  local ts
  local seq
  ts="$(event_ts)"
  event_seq=$((event_seq + 1))
  seq="$event_seq"
  if [ "$FORMAT" = "jsonl" ]; then
    echo "{\"schema_version\":$SCHEMA_VERSION,\"run_id\":\"$RUN_ID\",\"run_label\":\"$RUN_LABEL\",\"runner_pid\":$RUNNER_PID,\"event_seq\":$seq,\"event\":\"ok\",\"event_ts\":\"$ts\",\"script\":\"$script_name\",\"elapsed_s\":$elapsed}"
  else
    echo "lightweight_selftest ok script=$script_name elapsed_s=$elapsed"
  fi
}

emit_done() {
  local ts
  local seq
  ts="$(event_ts)"
  event_seq=$((event_seq + 1))
  seq="$event_seq"
  if [ "$FORMAT" = "jsonl" ]; then
    echo "{\"schema_version\":$SCHEMA_VERSION,\"run_id\":\"$RUN_ID\",\"run_label\":\"$RUN_LABEL\",\"runner_pid\":$RUNNER_PID,\"event_seq\":$seq,\"event\":\"final\",\"event_ts\":\"$ts\",\"status\":\"ok\",\"tests\":$test_count,\"total_elapsed_s\":$total_elapsed}"
  else
    echo "lightweight_selftest status=ok tests=$test_count total_elapsed_s=$total_elapsed"
  fi
}

emit_fail() {
  local script_name="$1"
  local elapsed="$2"
  local exit_code="$3"
  local ts
  local seq
  ts="$(event_ts)"
  event_seq=$((event_seq + 1))
  seq="$event_seq"
  if [ "$FORMAT" = "jsonl" ]; then
    echo "{\"schema_version\":$SCHEMA_VERSION,\"run_id\":\"$RUN_ID\",\"run_label\":\"$RUN_LABEL\",\"runner_pid\":$RUNNER_PID,\"event_seq\":$seq,\"event\":\"fail\",\"event_ts\":\"$ts\",\"script\":\"$script_name\",\"elapsed_s\":$elapsed,\"exit_code\":$exit_code}"
  else
    echo "lightweight_selftest fail script=$script_name elapsed_s=$elapsed exit_code=$exit_code" >&2
  fi
}

handle_err() {
  local exit_code="$?"
  local finished
  local elapsed
  trap - ERR
  if [ "$IN_RUN_ONE" -eq 1 ] && [ -n "$ACTIVE_SCRIPT" ]; then
    finished="$(date +%s)"
    elapsed=$((finished - ACTIVE_STARTED))
    emit_fail "$ACTIVE_SCRIPT" "$elapsed" "$exit_code"
  fi
  exit "$exit_code"
}

trap handle_err ERR

run_one() {
  local script_name="$1"
  local script_path="$SCRIPT_DIR/$script_name"
  local started
  local finished
  local elapsed

  ACTIVE_SCRIPT="$script_name"
  emit_begin "$script_name"
  started="$(date +%s)"
  ACTIVE_STARTED="$started"

  if [ ! -x "$script_path" ]; then
    emit_fail "$script_name" 0 2
    IN_RUN_ONE=0
    ACTIVE_SCRIPT=""
    ACTIVE_STARTED=0
    echo "selftest script not executable: $script_path" >&2
    exit 2
  fi

  IN_RUN_ONE=1
  "$script_path" "$TMP_ROOT"
  IN_RUN_ONE=0
  finished="$(date +%s)"
  elapsed=$((finished - started))
  total_elapsed=$((total_elapsed + elapsed))
  test_count=$((test_count + 1))
  emit_ok "$script_name" "$elapsed"
  ACTIVE_SCRIPT=""
  ACTIVE_STARTED=0
}

run_one "selftest_compare_perf_probe_logsets.sh"
run_one "selftest_compare_unnest_ab_logsets.sh"
run_one "selftest_lint_comparator_policy.sh"
run_one "selftest_policy_lint_strict_target.sh"
run_one "selftest_run_unnest_ab_probe_gate.sh"
run_one "selftest_run_unnest_ab_probe_mixed_shapes_warmup_validation.sh"
run_one "selftest_run_unnest_ab_perf_sentinel.sh"
run_one "selftest_run_unnest_ab_startup_sensitivity_probe.sh"
run_one "selftest_run_unnest_ab_startup_sensitivity_guard.sh"
run_one "selftest_run_unnest_ab_startup_sensitivity_sentinel.sh"
run_one "selftest_run_pg_core_regression_smoke.sh"
run_one "selftest_make_unnest_ab_startup_sensitivity_guard.sh"
run_one "selftest_make_unnest_ab_perf_sentinel_cold_observe.sh"
run_one "selftest_make_help_unnest_ab_startup_sensitivity.sh"
run_one "selftest_make_help_unnest_ab_startup_sensitivity_sentinel.sh"
run_one "selftest_make_help_unnest_ab_startup_script_overrides.sh"
run_one "selftest_make_help_pg_core_regression_smoke.sh"
run_one "selftest_make_help_graph_builder.sh"
run_one "selftest_find_vector_python_portability.sh"
run_one "selftest_make_defaults_unnest_ab_startup_sensitivity.sh"
run_one "selftest_docs_make_sentinel_cold_observe_contract.sh"
run_one "selftest_policy_safety_workflow_contract.sh"
run_one "selftest_run_unnest_ab_tuning_matrix.sh"
run_one "selftest_unnest_gate_make_defaults.sh"
run_one "selftest_unnest_ab_gate_make_arg_contract.sh"
run_one "selftest_summarize_unnest_ab_boundary_history.sh"
run_one "selftest_check_unnest_ab_boundary_history_gate.sh"
run_one "selftest_derive_unnest_ab_boundary_history_gate_thresholds.sh"
run_one "selftest_compare_unnest_ab_boundary_history_gate_policy_delta.sh"
run_one "selftest_accumulate_unnest_ab_boundary_history_summaries.sh"
run_one "selftest_run_unnest_ab_boundary_history_policy_review_window.sh"
run_one "selftest_benchmark_unnest_ab_boundary_history_date_map_modes.sh"
run_one "selftest_benchmark_unnest_ab_boundary_history_policy_review_manifest_modes.sh"
run_one "selftest_check_unnest_ab_boundary_history_policy_review_manifest_freshness.sh"
run_one "selftest_make_unnest_ab_boundary_history_policy_review_window_trusted_workflow.sh"
run_one "selftest_make_help_policy_review_trusted_workflow.sh"
run_one "selftest_make_help_policy_safety_selftest.sh"
run_one "selftest_policy_review_manifest_prechecked_matrix.sh"
run_one "selftest_check_unnest_ab_kv_output_fields.sh"
run_one "selftest_check_planner_probe_cost_ratio.sh"
run_one "selftest_check_planner_probe_default_path.sh"
run_one "selftest_compare_planner_probe_logsets.sh"
run_one "selftest_summarize_planner_probe_log.sh"
run_one "selftest_run_planner_probe_with_summary.sh"
run_one "selftest_run_planner_probe_gate.sh"
run_one "selftest_runtime_tmpdir_validation.sh"
run_one "selftest_runtime_output_target_validation.sh"
run_one "selftest_runtime_port_validation.sh"
run_one "selftest_extract_pg_sorted_heap_define.sh"
run_one "selftest_docs_graph_builder_contract.sh"
run_one "selftest_docs_policy_safety_quickstart_contract.sh"
run_one "selftest_docs_pg_core_regression_smoke_contract.sh"
run_one "selftest_policy_safety_target_composition.sh"
run_one "selftest_policy_safety_workflow_target_sync.sh"
run_one "selftest_runtime_profile_export.sh"
run_one "selftest_workflow_path_filter_coverage.sh"
run_one "selftest_workflow_files_are_guarded.sh"
run_one "selftest_lightweight_workflow_script_coverage.sh"
run_one "selftest_selftest_script_baseline.sh"
run_one "selftest_run_lightweight_selftests_run_label_validation.sh"
run_one "selftest_run_lightweight_selftests_auto_tmp_clean_validation.sh"
run_one "selftest_run_lightweight_selftests_run_label_propagation.sh"
run_one "selftest_run_lightweight_selftests_success_stream.sh"
run_one "selftest_run_lightweight_selftests_failure_event.sh"
run_one "selftest_run_lightweight_selftests_preflight_failure_event.sh"
run_one "selftest_tmp_clean_pg_sorted_heap.sh"

emit_done

if [ "$AUTO_TMP_CLEAN_ENABLED" -eq 1 ]; then
  "$TMP_CLEAN_SCRIPT" "$TMP_ROOT" "$AUTO_TMP_CLEAN_MIN_AGE_S"
fi
