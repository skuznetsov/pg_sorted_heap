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
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKFLOW="$REPO_ROOT/.github/workflows/policy-safety-selftest.yml"

if [ ! -f "$WORKFLOW" ]; then
  echo "selftest_policy_safety_workflow_contract status=skipped reason=workflow_files_absent"
  exit 0
fi

require_line() {
  local pattern="$1"
  local message="$2"
  if ! rg -n "$pattern" "$WORKFLOW" >/dev/null; then
    echo "$message" >&2
    exit 1
  fi
}

require_fixed_count_eq() {
  local line="$1"
  local expected="$2"
  local message="$3"
  local count
  count="$(grep -Fxc "$line" "$WORKFLOW" || true)"
  if [ "$count" -ne "$expected" ]; then
    echo "$message (expected=$expected found=$count)" >&2
    exit 1
  fi
}

require_line '^name:\s+policy-safety-selftest$' "missing workflow name policy-safety-selftest"
require_line '^\s+pull_request:\s*$' "missing pull_request trigger"
require_line '^\s+push:\s*$' "missing push trigger"
require_fixed_count_eq '      - Makefile' 2 "missing Makefile path coverage across pull_request/push"
require_fixed_count_eq '      - README.md' 2 "missing README.md path coverage across pull_request/push"
require_fixed_count_eq '      - OPERATIONS.md' 2 "missing OPERATIONS.md path coverage across pull_request/push"
require_fixed_count_eq '      - .github/workflows/policy-safety-selftest.yml' 2 "missing workflow self-trigger coverage across pull_request/push"
require_fixed_count_eq '      - scripts/selftest_docs_make_sentinel_cold_observe_contract.sh' 2 "missing cold-observe docs contract path coverage across pull_request/push"
require_fixed_count_eq '      - scripts/selftest_docs_policy_safety_quickstart_contract.sh' 2 "missing policy-safety quickstart docs contract path coverage across pull_request/push"
require_fixed_count_eq '      - scripts/selftest_make_help_policy_safety_selftest.sh' 2 "missing policy-safety help contract path coverage across pull_request/push"
require_fixed_count_eq '      - scripts/selftest_policy_safety_target_composition.sh' 2 "missing policy-safety target composition path coverage across pull_request/push"
require_fixed_count_eq '      - scripts/selftest_policy_safety_workflow_target_sync.sh' 2 "missing policy-safety workflow-target sync path coverage across pull_request/push"
require_fixed_count_eq '      - scripts/selftest_run_unnest_ab_probe_gate.sh' 2 "missing gate selftest path coverage across pull_request/push"
require_line '^\s+runs-on:\s+ubuntu-latest$' "missing ubuntu-latest runner declaration"
require_line 'make -s --no-print-directory policy-safety-selftest' "missing policy-safety-selftest run command"
require_line 'UNNEST_AB_SELFTEST_TMP_ROOT=/tmp' "missing UNNEST_AB_SELFTEST_TMP_ROOT=/tmp in run command"
require_line 'UNNEST_GATE_SELFTEST_TMP_ROOT=/tmp' "missing UNNEST_GATE_SELFTEST_TMP_ROOT=/tmp in run command"

echo "selftest_policy_safety_workflow_contract status=ok"
