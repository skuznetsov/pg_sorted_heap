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
WORKFLOW="$ROOT_DIR/.github/workflows/policy-safety-selftest.yml"

if [ ! -f "$MAKEFILE" ]; then
  echo "missing Makefile: $MAKEFILE" >&2
  exit 1
fi
if [ ! -f "$WORKFLOW" ]; then
  echo "selftest_policy_safety_workflow_target_sync status=skipped reason=workflow_files_absent"
  exit 0
fi

extract_target_block() {
  local target_name="$1"
  awk -v t="$target_name" '
    $0 ~ ("^" t ":[[:space:]]*$") {in_block=1; print; next}
    in_block && /^[A-Za-z0-9_.-]+:[[:space:]]*$/ {exit}
    in_block {print}
  ' "$MAKEFILE"
}

extract_scripts_from_block() {
  awk '
    {
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^\.\/scripts\/[^[:space:]]+$/) {
          sub(/^\.\//, "", $i)
          print $i
        }
      }
    }
  '
}

POLICY_BLOCK="$(extract_target_block "policy-safety-selftest")"
TRUST_BLOCK="$(extract_target_block "unnest-ab-profile-boundary-history-policy-review-trust-selftest")"

if [ -z "$POLICY_BLOCK" ]; then
  echo "failed to extract policy-safety-selftest block" >&2
  exit 1
fi
if [ -z "$TRUST_BLOCK" ]; then
  echo "failed to extract unnest-ab-profile-boundary-history-policy-review-trust-selftest block" >&2
  exit 1
fi

REQUIRED_SCRIPT_PATHS="$(
  {
    printf '%s\n' "$POLICY_BLOCK" | extract_scripts_from_block
    printf '%s\n' "$TRUST_BLOCK" | extract_scripts_from_block
  } | awk 'NF' | sort -u
)"

if [ -z "$REQUIRED_SCRIPT_PATHS" ]; then
  echo "no required script paths extracted from policy-safety targets" >&2
  exit 1
fi

while IFS= read -r script_path; do
  [ -n "$script_path" ] || continue
  count="$(
    awk -v line="      - $script_path" '
      $0 == line { c++ }
      END { print c + 0 }
    ' "$WORKFLOW"
  )"
  if [ "$count" -lt 2 ]; then
    echo "workflow paths missing synced script entry for both pull_request and push: $script_path (found=$count)" >&2
    exit 1
  fi
done <<EOF_REQUIRED
$REQUIRED_SCRIPT_PATHS
EOF_REQUIRED

echo "selftest_policy_safety_workflow_target_sync status=ok"
