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
HELPER="$ROOT_DIR/scripts/find_vector_python.sh"

if rg -n '/Users/sergey/' "$HELPER" >/dev/null; then
  echo "helper should not hardcode repo-specific home paths" >&2
  exit 1
fi

BASELINE_PY="$("$HELPER")"
if [ ! -x "$BASELINE_PY" ]; then
  echo "helper did not resolve an executable python" >&2
  exit 1
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_find_vector_python.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

mkdir -p "$WORKDIR/home/miniconda3/bin"
ln -s "$BASELINE_PY" "$WORKDIR/home/miniconda3/bin/python3"

FOUND_PY="$(HOME="$WORKDIR/home" PATH="/bin:/usr/bin" "$HELPER")"
EXPECTED_PY="$WORKDIR/home/miniconda3/bin/python3"

if [ "$FOUND_PY" != "$EXPECTED_PY" ]; then
  echo "expected helper to honor HOME-based miniconda3 fallback" >&2
  echo "expected: $EXPECTED_PY" >&2
  echo "actual:   $FOUND_PY" >&2
  exit 1
fi

echo "selftest_find_vector_python_portability status=ok"
