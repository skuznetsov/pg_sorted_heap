#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -gt 1 ]; then
  echo "usage: $0 [tmp_root_abs_dir]" >&2
  exit 2
fi

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root_abs_dir must be absolute: $TMP_ROOT" >&2
  exit 2
fi
if [ ! -d "$TMP_ROOT" ]; then
  echo "tmp_root_abs_dir not found: $TMP_ROOT" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RUNNER_SCRIPT="$ROOT_DIR/scripts/run_lightweight_selftests.sh"
WORKFLOW="$ROOT_DIR/.github/workflows/perf-compare-selftest.yml"

if [ ! -f "$RUNNER_SCRIPT" ]; then
  echo "runner script not found: $RUNNER_SCRIPT" >&2
  exit 2
fi
if [ ! -f "$WORKFLOW" ]; then
  echo "selftest_lightweight_workflow_script_coverage status=skipped reason=workflow_files_absent"
  exit 0
fi

count_path_hits() {
  local path="$1"
  local hits_unquoted
  local hits_quoted
  hits_unquoted="$(grep -F --count "      - $path" "$WORKFLOW" || true)"
  hits_quoted="$(grep -F --count "      - '$path'" "$WORKFLOW" || true)"
  echo $((hits_unquoted + hits_quoted))
}

count_chmod_hits() {
  local path="$1"
  local wildcard_hits
  local explicit_hits
  wildcard_hits="$(grep -F --count "chmod +x scripts/*.sh" "$WORKFLOW" || true)"
  explicit_hits="$(grep -F --count "$path" "$WORKFLOW" || true)"
  echo $((wildcard_hits + explicit_hits))
}

line_of_first() {
  local pattern="$1"
  local line
  line="$(grep -Fn -- "$pattern" "$WORKFLOW" | head -n 1 | cut -d: -f1 || true)"
  if [ -z "$line" ]; then
    echo 0
  else
    echo "$line"
  fi
}

script_count=0
while IFS= read -r script_name; do
  [ -n "$script_name" ] || continue
  script_count=$((script_count + 1))
  script_path="scripts/$script_name"

  path_hits="$(count_path_hits "$script_path")"
  if [ "$path_hits" -lt 2 ]; then
    echo "expected path filter '$script_path' in both pull_request and push blocks (hits=$path_hits)" >&2
    exit 1
  fi

  chmod_hits="$(count_chmod_hits "$script_path")"
  if [ "$chmod_hits" -lt 1 ]; then
    echo "expected chmod coverage for '$script_path' in workflow" >&2
    exit 1
  fi
done < <(sed -n 's/^run_one "\(.*\)"/\1/p' "$RUNNER_SCRIPT")

if [ "$script_count" -lt 1 ]; then
  echo "no run_one entries found in $RUNNER_SCRIPT" >&2
  exit 2
fi

runner_hits="$(count_path_hits "scripts/run_lightweight_selftests.sh")"
if [ "$runner_hits" -lt 2 ]; then
  echo "expected path filter 'scripts/run_lightweight_selftests.sh' in both pull_request and push blocks (hits=$runner_hits)" >&2
  exit 1
fi

strict_policy_step_hits="$(grep -F --count "run: make policy-lint-strict" "$WORKFLOW" || true)"
if [ "$strict_policy_step_hits" -ne 1 ]; then
  echo "expected exactly one strict policy lint step via 'make policy-lint-strict' (hits=$strict_policy_step_hits)" >&2
  exit 1
fi

lightweight_run_hits="$(grep -F --count "./scripts/run_lightweight_selftests.sh" "$WORKFLOW" || true)"
if [ "$lightweight_run_hits" -ne 1 ]; then
  echo "expected exactly one workflow lightweight runner invocation (hits=$lightweight_run_hits)" >&2
  exit 1
fi
lightweight_jsonl_mode_hits="$(grep -F --count "./scripts/run_lightweight_selftests.sh /tmp jsonl" "$WORKFLOW" || true)"
if [ "$lightweight_jsonl_mode_hits" -ne 1 ]; then
  echo "expected exactly one workflow lightweight run step in jsonl mode (/tmp jsonl) (hits=$lightweight_jsonl_mode_hits)" >&2
  exit 1
fi
lightweight_auto_clean_env_hits="$(grep -F --count "LIGHTWEIGHT_SELFTEST_AUTO_TMP_CLEAN: \"on\"" "$WORKFLOW" || true)"
if [ "$lightweight_auto_clean_env_hits" -ne 1 ]; then
  echo "expected exactly one workflow env contract 'LIGHTWEIGHT_SELFTEST_AUTO_TMP_CLEAN: \"on\"' (hits=$lightweight_auto_clean_env_hits)" >&2
  exit 1
fi
lightweight_auto_clean_age_env_hits="$(grep -F --count "LIGHTWEIGHT_SELFTEST_AUTO_TMP_CLEAN_MIN_AGE_S: \"0\"" "$WORKFLOW" || true)"
if [ "$lightweight_auto_clean_age_env_hits" -ne 1 ]; then
  echo "expected exactly one workflow env contract 'LIGHTWEIGHT_SELFTEST_AUTO_TMP_CLEAN_MIN_AGE_S: \"0\"' (hits=$lightweight_auto_clean_age_env_hits)" >&2
  exit 1
fi
lightweight_tune_probe_root_env_hits="$(grep -F --count "UNNEST_TUNE_PROBE_OUT_ROOT: /tmp/pg_sorted_heap_unnest_tune_probe_\${{ github.run_id }}_\${{ github.run_attempt }}" "$WORKFLOW" || true)"
if [ "$lightweight_tune_probe_root_env_hits" -ne 1 ]; then
  echo "expected exactly one workflow env contract for UNNEST_TUNE_PROBE_OUT_ROOT with run-scoped path (hits=$lightweight_tune_probe_root_env_hits)" >&2
  exit 1
fi
lightweight_tune_probe_root_prepare_hits="$(grep -F --count "run: mkdir -p /tmp/pg_sorted_heap_unnest_tune_probe_\${{ github.run_id }}_\${{ github.run_attempt }}" "$WORKFLOW" || true)"
if [ "$lightweight_tune_probe_root_prepare_hits" -ne 1 ]; then
  echo "expected exactly one workflow preparation step creating run-scoped tuning probe root (hits=$lightweight_tune_probe_root_prepare_hits)" >&2
  exit 1
fi
lightweight_tee_hits="$(grep -F --count "tee /tmp/pg_sorted_heap_lightweight_selftests.jsonl" "$WORKFLOW" || true)"
if [ "$lightweight_tee_hits" -ne 1 ]; then
  echo "expected exactly one tee sink to /tmp/pg_sorted_heap_lightweight_selftests.jsonl (hits=$lightweight_tee_hits)" >&2
  exit 1
fi

strict_step_line="$(line_of_first "run: make policy-lint-strict")"
lightweight_run_line="$(line_of_first "run: ./scripts/run_lightweight_selftests.sh")"
if [ "$strict_step_line" -le 0 ] || [ "$lightweight_run_line" -le 0 ]; then
  echo "unable to resolve strict-lint/lightweight step ordering lines in workflow" >&2
  exit 1
fi
if [ "$strict_step_line" -ge "$lightweight_run_line" ]; then
  echo "expected strict policy lint step before lightweight selftests (strict_line=$strict_step_line lightweight_line=$lightweight_run_line)" >&2
  exit 1
fi

upload_step_hits="$(grep -F --count -- "- name: Upload lightweight selftest log" "$WORKFLOW" || true)"
if [ "$upload_step_hits" -ne 1 ]; then
  echo "expected exactly one workflow upload step for lightweight selftest log (hits=$upload_step_hits)" >&2
  exit 1
fi
require_literal_upload() {
  local literal="$1"
  if ! grep -Fq "$literal" "$WORKFLOW"; then
    echo "expected literal '$literal' in workflow upload step" >&2
    exit 1
  fi
}

require_literal_upload_exactly_once() {
  local literal="$1"
  local hits
  hits="$(grep -F --count "$literal" "$WORKFLOW" || true)"
  if [ "$hits" -ne 1 ]; then
    echo "expected literal '$literal' exactly once in workflow upload contract (hits=$hits)" >&2
    exit 1
  fi
}

require_literal_upload "if: always()"
require_literal_upload "uses: actions/upload-artifact@v4"
require_literal_upload "path: /tmp/pg_sorted_heap_lightweight_selftests.jsonl"
require_literal_upload "name: clustered-pg-lightweight-selftest-\${{ github.run_id }}-\${{ github.run_attempt }}"
require_literal_upload "if-no-files-found: error"

require_literal_upload_exactly_once "if: always()"
require_literal_upload_exactly_once "uses: actions/upload-artifact@v4"
require_literal_upload_exactly_once "path: /tmp/pg_sorted_heap_lightweight_selftests.jsonl"
require_literal_upload_exactly_once "name: clustered-pg-lightweight-selftest-\${{ github.run_id }}-\${{ github.run_attempt }}"
require_literal_upload_exactly_once "if-no-files-found: error"

upload_step_line="$(line_of_first "- name: Upload lightweight selftest log")"
if [ "$upload_step_line" -le 0 ]; then
  echo "unable to resolve upload step line in workflow" >&2
  exit 1
fi
if [ "$lightweight_run_line" -ge "$upload_step_line" ]; then
  echo "expected upload step after lightweight selftest run (run_line=$lightweight_run_line upload_line=$upload_step_line)" >&2
  exit 1
fi

legacy_prefixed_run_hits="$(grep -F --count "POLICY_LINT_WARNINGS_MAX=0 ./scripts/run_lightweight_selftests.sh" "$WORKFLOW" || true)"
if [ "$legacy_prefixed_run_hits" -gt 0 ]; then
  echo "unexpected env-prefixed lightweight run found; strict policy lint must be isolated to dedicated step" >&2
  exit 1
fi

behavior_prefixed_run_hits="$(grep -E --count "PROFILE_BEHAVIOR_[A-Z_]+=.*[[:space:]]\\./scripts/run_lightweight_selftests.sh" "$WORKFLOW" || true)"
if [ "$behavior_prefixed_run_hits" -gt 0 ]; then
  echo "unexpected PROFILE_BEHAVIOR_* env-prefixed lightweight run found; behavior-guard thresholds must stay in script defaults or explicit selftests" >&2
  exit 1
fi

for static_path in "scripts/lint_comparator_policy.sh" "scripts/comparator_policy_contract.json"; do
  static_hits="$(count_path_hits "$static_path")"
  if [ "$static_hits" -lt 2 ]; then
    echo "expected path filter '$static_path' in both pull_request and push blocks (hits=$static_hits)" >&2
    exit 1
  fi
done

echo "selftest_lightweight_workflow_script_coverage status=ok scripts=$script_count"
