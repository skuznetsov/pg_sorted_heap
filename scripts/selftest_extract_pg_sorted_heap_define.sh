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
HELPER="$SCRIPT_DIR/extract_pg_sorted_heap_define.sh"
SOURCE_C="$SCRIPT_DIR/../src/pg_sorted_heap.c"

if [ ! -x "$HELPER" ]; then
  echo "required script is not executable: $HELPER" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_extract_define_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

expect_fail_contains() {
  local expected="$1"
  shift
  local out="$WORKDIR/expect_fail.out"
  if "$@" >"$out" 2>&1; then
    echo "expected failure but command succeeded: $*" >&2
    cat "$out" >&2
    exit 1
  fi
  if ! grep -Fq "$expected" "$out"; then
    echo "expected failure output to contain: $expected" >&2
    cat "$out" >&2
    exit 1
  fi
}

value="$(bash "$HELPER" "$SOURCE_C" CLUSTERED_PG_ZONE_MAP_MAX_KEYS)"
if ! [[ "$value" =~ ^[0-9]+$ ]] || [ "$value" -le 0 ]; then
  echo "expected positive numeric value, got: $value" >&2
  exit 1
fi

expect_fail_contains "failed to parse CLUSTERED_PG_NOT_A_REAL_DEFINE" \
  bash "$HELPER" "$SOURCE_C" CLUSTERED_PG_NOT_A_REAL_DEFINE

expect_fail_contains "invalid define_name: BAD-NAME" \
  bash "$HELPER" "$SOURCE_C" "BAD-NAME"

echo "selftest_extract_pg_sorted_heap_define status=ok zone_map_max_keys=$value"
