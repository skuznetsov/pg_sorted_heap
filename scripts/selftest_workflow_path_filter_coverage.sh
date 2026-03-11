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
LIGHTWORKFLOW="$ROOT_DIR/.github/workflows/perf-compare-selftest.yml"
SANITY_WORKFLOW="$ROOT_DIR/.github/workflows/workflow-path-filter-sanity.yml"

if [ ! -f "$LIGHTWORKFLOW" ]; then
  echo "workflow not found: $LIGHTWORKFLOW" >&2
  exit 2
fi
if [ ! -f "$SANITY_WORKFLOW" ]; then
  echo "workflow not found: $SANITY_WORKFLOW" >&2
  exit 2
fi

require_path_filter_twice() {
  local workflow="$1"
  local path="$2"
  local hits_unquoted
  local hits_quoted
  local hits
  hits_unquoted="$(grep -F --count "      - $path" "$workflow" || true)"
  hits_quoted="$(grep -F --count "      - '$path'" "$workflow" || true)"
  hits=$((hits_unquoted + hits_quoted))
  if [ "$hits" -lt 2 ]; then
    echo "expected path filter '$path' in both pull_request and push blocks for $workflow (hits=$hits)" >&2
    exit 1
  fi
}

require_literal() {
  local workflow="$1"
  local literal="$2"
  if ! grep -Fq "$literal" "$workflow"; then
    echo "expected literal '$literal' in $workflow" >&2
    exit 1
  fi
}

assert_lightweight_runner_selftests_are_guarded() {
  local workflow="$1"
  local runner_script="$2"
  local tmpdir
  tmpdir="$(mktemp -d "$TMP_ROOT/workflow_runner_guard.XXXXXX")"

  sed -n 's/^run_one "\(.*\)"/scripts\/\1/p' "$runner_script" | sort -u >"$tmpdir/runner_scripts.sorted"
  if [ ! -s "$tmpdir/runner_scripts.sorted" ]; then
    rm -rf "$tmpdir"
    echo "no run_one selftest entries found in $runner_script" >&2
    exit 1
  fi

  while IFS= read -r script_path; do
    [ -n "$script_path" ] || continue
    require_path_filter_twice "$workflow" "$script_path"
  done <"$tmpdir/runner_scripts.sorted"

  rm -rf "$tmpdir"
}

extract_paths_for_event() {
  local workflow="$1"
  local event="$2"
  awk -v event="$event" '
    function unquote(s) {
      if (s ~ /^'\''.*'\''$/) {
        sub(/^'\''/, "", s)
        sub(/'\''$/, "", s)
      }
      return s
    }
    $0 == "  " event ":" {
      in_event = 1
      in_paths = 0
      next
    }
    in_event && $0 ~ /^  [A-Za-z_]+:/ && $0 != "    paths:" {
      if ($0 !~ /^    /) {
        in_event = 0
        in_paths = 0
      }
      next
    }
    in_event && $0 == "    paths:" {
      in_paths = 1
      next
    }
    in_event && in_paths && $0 ~ /^      - / {
      path = $0
      sub(/^      - /, "", path)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", path)
      path = unquote(path)
      print path
      next
    }
    in_event && in_paths && $0 !~ /^      - / && $0 !~ /^[[:space:]]*$/ {
      in_paths = 0
      next
    }
  ' "$workflow"
}

assert_event_path_parity() {
  local workflow="$1"
  local tmpdir
  local dup_count
  tmpdir="$(mktemp -d "$TMP_ROOT/workflow_path_parity.XXXXXX")"
  extract_paths_for_event "$workflow" "pull_request" >"$tmpdir/pull_request.raw"
  extract_paths_for_event "$workflow" "push" >"$tmpdir/push.raw"

  if [ ! -s "$tmpdir/pull_request.raw" ] || [ ! -s "$tmpdir/push.raw" ]; then
    rm -rf "$tmpdir"
    echo "expected non-empty pull_request and push path filters in $workflow" >&2
    exit 1
  fi

  sort "$tmpdir/pull_request.raw" >"$tmpdir/pull_request.sorted"
  sort "$tmpdir/push.raw" >"$tmpdir/push.sorted"

  dup_count="$(uniq -d "$tmpdir/pull_request.sorted" | wc -l | tr -d ' ')"
  if [ "${dup_count:-0}" -gt 0 ]; then
    rm -rf "$tmpdir"
    echo "unexpected duplicate path filter entries in pull_request block for $workflow" >&2
    exit 1
  fi
  dup_count="$(uniq -d "$tmpdir/push.sorted" | wc -l | tr -d ' ')"
  if [ "${dup_count:-0}" -gt 0 ]; then
    rm -rf "$tmpdir"
    echo "unexpected duplicate path filter entries in push block for $workflow" >&2
    exit 1
  fi

  if ! cmp -s "$tmpdir/pull_request.sorted" "$tmpdir/push.sorted"; then
    rm -rf "$tmpdir"
    echo "path filter mismatch between pull_request and push blocks for $workflow" >&2
    exit 1
  fi

  rm -rf "$tmpdir"
}

assert_event_path_parity "$LIGHTWORKFLOW"
assert_event_path_parity "$SANITY_WORKFLOW"
assert_lightweight_runner_selftests_are_guarded "$LIGHTWORKFLOW" "$ROOT_DIR/scripts/run_lightweight_selftests.sh"

require_path_filter_twice "$LIGHTWORKFLOW" ".github/workflows/workflow-path-filter-sanity.yml"
require_path_filter_twice "$LIGHTWORKFLOW" "scripts/selftest_workflow_path_filter_coverage.sh"
require_path_filter_twice "$LIGHTWORKFLOW" "scripts/selftest_workflow_files_are_guarded.sh"
require_path_filter_twice "$LIGHTWORKFLOW" "scripts/selftest_lightweight_workflow_script_coverage.sh"
require_path_filter_twice "$LIGHTWORKFLOW" "scripts/selftest_selftest_script_baseline.sh"
require_path_filter_twice "$LIGHTWORKFLOW" "scripts/run_lightweight_selftests.sh"
require_literal "$LIGHTWORKFLOW" "concurrency:"
require_literal "$LIGHTWORKFLOW" "cancel-in-progress: true"
require_literal "$LIGHTWORKFLOW" "chmod +x scripts/*.sh"

require_path_filter_twice "$SANITY_WORKFLOW" ".github/workflows/perf-compare-selftest.yml"
require_path_filter_twice "$SANITY_WORKFLOW" ".github/workflows/workflow-path-filter-sanity.yml"
require_path_filter_twice "$SANITY_WORKFLOW" "scripts/selftest_workflow_path_filter_coverage.sh"
require_path_filter_twice "$SANITY_WORKFLOW" "scripts/selftest_workflow_files_are_guarded.sh"
require_path_filter_twice "$SANITY_WORKFLOW" "scripts/selftest_lightweight_workflow_script_coverage.sh"
require_path_filter_twice "$SANITY_WORKFLOW" "scripts/selftest_selftest_script_baseline.sh"
require_path_filter_twice "$SANITY_WORKFLOW" "Makefile"
require_literal "$SANITY_WORKFLOW" "concurrency:"
require_literal "$SANITY_WORKFLOW" "cancel-in-progress: true"
require_literal "$SANITY_WORKFLOW" "make workflow-path-filter-coverage-selftest workflow-files-guard-selftest lightweight-workflow-script-coverage-selftest selftest-script-baseline"

echo "selftest_workflow_path_filter_coverage status=ok"
