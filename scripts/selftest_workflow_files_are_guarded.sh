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
WORKFLOW_DIR="$ROOT_DIR/.github/workflows"
LIGHTWORKFLOW="$WORKFLOW_DIR/perf-compare-selftest.yml"
SANITY_WORKFLOW="$WORKFLOW_DIR/workflow-path-filter-sanity.yml"

if [ ! -d "$WORKFLOW_DIR" ]; then
  echo "selftest_workflow_files_are_guarded status=skipped reason=workflow_files_absent"
  exit 0
fi
if [ ! -f "$LIGHTWORKFLOW" ]; then
  echo "selftest_workflow_files_are_guarded status=skipped reason=workflow_files_absent"
  exit 0
fi
if [ ! -f "$SANITY_WORKFLOW" ]; then
  echo "selftest_workflow_files_are_guarded status=skipped reason=workflow_files_absent"
  exit 0
fi

count_path_hits() {
  local workflow="$1"
  local path="$2"
  local hits_unquoted
  local hits_quoted
  hits_unquoted="$(grep -F --count "      - $path" "$workflow" || true)"
  hits_quoted="$(grep -F --count "      - '$path'" "$workflow" || true)"
  echo $((hits_unquoted + hits_quoted))
}

workflow_count=0
while IFS= read -r workflow_file; do
  [ -e "$workflow_file" ] || continue
  workflow_count=$((workflow_count + 1))
  rel_path=".github/workflows/$(basename "$workflow_file")"
  hits_light="$(count_path_hits "$LIGHTWORKFLOW" "$rel_path")"
  hits_sanity="$(count_path_hits "$SANITY_WORKFLOW" "$rel_path")"
  total_hits=$((hits_light + hits_sanity))
  if [ "$total_hits" -lt 1 ]; then
    echo "workflow path is not guarded by path filters: $rel_path" >&2
    exit 1
  fi
done < <(find "$WORKFLOW_DIR" -maxdepth 1 -type f \( -name '*.yml' -o -name '*.yaml' \) | sort)

if [ "$workflow_count" -lt 1 ]; then
  echo "no workflow files found in $WORKFLOW_DIR" >&2
  exit 2
fi

echo "selftest_workflow_files_are_guarded status=ok workflows=$workflow_count"
