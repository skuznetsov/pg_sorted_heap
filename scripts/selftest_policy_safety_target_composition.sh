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
MAKEFILE="$ROOT_DIR/Makefile"

if [ ! -f "$MAKEFILE" ]; then
  echo "missing Makefile: $MAKEFILE" >&2
  exit 1
fi

TARGET_BLOCK="$(
  awk '
    /^policy-safety-selftest:[[:space:]]*$/ {in_block=1; print; next}
    in_block && /^[A-Za-z0-9_.-]+:[[:space:]]*$/ {exit}
    in_block {print}
  ' "$MAKEFILE"
)"

if [ -z "$TARGET_BLOCK" ]; then
  echo "failed to extract policy-safety-selftest target block" >&2
  exit 1
fi

require_fragment() {
  local fragment="$1"
  if ! printf '%s\n' "$TARGET_BLOCK" | grep -Fq "$fragment"; then
    echo "missing expected fragment in policy-safety-selftest target: $fragment" >&2
    printf '%s\n' "$TARGET_BLOCK" >&2
    exit 1
  fi
}

require_fragment '$(MAKE) unnest-ab-profile-boundary-history-policy-review-trust-selftest'
require_fragment 'bash ./scripts/selftest_run_unnest_ab_probe_gate.sh'
require_fragment 'bash ./scripts/selftest_docs_make_sentinel_cold_observe_contract.sh'
require_fragment 'bash ./scripts/selftest_docs_policy_safety_quickstart_contract.sh'
require_fragment 'bash ./scripts/selftest_policy_safety_workflow_contract.sh'
require_fragment 'bash ./scripts/selftest_policy_safety_target_composition.sh'
require_fragment 'bash ./scripts/selftest_release_archive_contract.sh'

echo "selftest_policy_safety_target_composition status=ok"
