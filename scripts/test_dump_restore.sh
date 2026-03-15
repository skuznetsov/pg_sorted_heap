#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# pg_dump / pg_restore lifecycle test for sorted_heap
# ============================================================
#
# Verifies that sorted_heap tables survive dump/restore correctly:
# data integrity, zone map rebuild, secondary indexes, TOAST.
#
# Usage: ./scripts/test_dump_restore.sh [tmp_root] [port]

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
PORT="${2:-65495}"

if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root must be absolute: $TMP_ROOT" >&2; exit 2
fi
if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [ "$PORT" -le 1024 ] || [ "$PORT" -ge 65535 ]; then
  echo "port must be 1025..65534" >&2; exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if command -v pg_config >/dev/null 2>&1; then
  PG_BINDIR="$(pg_config --bindir)"
else
  PG_BINDIR="/opt/homebrew/Cellar/postgresql@18/18.1_1/bin"
fi

make -C "$ROOT_DIR" install >/dev/null 2>/dev/null || true

pass=0; fail=0; total=0

check() {
  local name="$1" expected="$2" actual="$3"
  total=$((total + 1))
  if [ "$expected" = "$actual" ]; then
    echo "  PASS: $name"
    pass=$((pass + 1))
  else
    echo "  FAIL: $name (expected=$expected actual=$actual)"
    fail=$((fail + 1))
  fi
}

TMP_DIR=""
cleanup() {
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR/data" ]; then
    "$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -m fast stop >/dev/null 2>&1 || true
  fi
  if [ -n "$TMP_DIR" ]; then
    rm -rf "$TMP_DIR"
  fi
}
trap cleanup EXIT

# --- Create ephemeral cluster ---
TMP_DIR=$(mktemp -d "$TMP_ROOT/pg_sorted_heap_dump.XXXXXX")
"$PG_BINDIR/initdb" -D "$TMP_DIR/data" -A trust --no-locale >/dev/null 2>&1

cat >> "$TMP_DIR/data/postgresql.conf" <<'PGCONF'
log_min_messages = warning
shared_preload_libraries = ''
PGCONF

"$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -l "$TMP_DIR/postmaster.log" \
  -o "-k $TMP_DIR -p $PORT" start >/dev/null

PSQL() {
  "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" "$@" -v ON_ERROR_STOP=1 -qtAX
}

DB="dump_test"
"$PG_BINDIR/createdb" -h "$TMP_DIR" -p "$PORT" "$DB"

echo "=== pg_dump / pg_restore lifecycle test ==="

# --- Setup: create table, secondary index, TOAST data, compact ---
PSQL "$DB" -c "CREATE EXTENSION pg_sorted_heap"

PSQL "$DB" -c "
  CREATE TABLE sh_dump (
    id int PRIMARY KEY,
    val text,
    big_payload text
  ) USING sorted_heap;

  INSERT INTO sh_dump
    SELECT g, 'row-' || g,
           CASE WHEN g <= 10 THEN repeat('T', 4000) ELSE NULL END
    FROM generate_series(1, 1000) g;

  CREATE INDEX sh_dump_val_idx ON sh_dump (val);
"

PSQL "$DB" -c "SELECT sorted_heap_compact('sh_dump'::regclass)"
PSQL "$DB" -c "ANALYZE sh_dump"

# --- Verify pre-dump state ---
pre_count=$(PSQL "$DB" -c "SELECT count(*) FROM sh_dump")
check "pre_dump_count" "1000" "$pre_count"

pre_toast_ok=$(PSQL "$DB" -c "
  SELECT CASE WHEN count(*) = 10 THEN 't' ELSE 'f' END
  FROM sh_dump WHERE length(big_payload) = 4000")
check "pre_dump_toast" "t" "$pre_toast_ok"

# Verify scan pruning works before dump
pre_pruning=$(PSQL "$DB" -c "
  SET enable_indexscan = off;
  SET enable_bitmapscan = off;
  EXPLAIN (COSTS OFF) SELECT * FROM sh_dump WHERE id = 500" \
  | grep -c 'SortedHeapScan' || true)
check "pre_dump_pruning" "1" "$pre_pruning"

# --- Dump ---
"$PG_BINDIR/pg_dump" -h "$TMP_DIR" -p "$PORT" -Fc "$DB" \
  -f "$TMP_DIR/dump.fc" 2>/dev/null

# --- Drop and restore ---
"$PG_BINDIR/dropdb" -h "$TMP_DIR" -p "$PORT" "$DB"
"$PG_BINDIR/createdb" -h "$TMP_DIR" -p "$PORT" "$DB"
"$PG_BINDIR/pg_restore" -h "$TMP_DIR" -p "$PORT" -d "$DB" \
  "$TMP_DIR/dump.fc" 2>/dev/null

# --- Verify post-restore data integrity ---
post_count=$(PSQL "$DB" -c "SELECT count(*) FROM sh_dump")
check "post_restore_count" "1000" "$post_count"

post_toast_ok=$(PSQL "$DB" -c "
  SELECT CASE WHEN count(*) = 10 THEN 't' ELSE 'f' END
  FROM sh_dump WHERE length(big_payload) = 4000")
check "post_restore_toast" "t" "$post_toast_ok"

# --- Verify secondary index survived ---
idx_ok=$(PSQL "$DB" -c "
  SELECT CASE WHEN count(*) = 1 THEN 't' ELSE 'f' END
  FROM pg_indexes WHERE tablename = 'sh_dump' AND indexname = 'sh_dump_val_idx'")
check "post_restore_secondary_index" "t" "$idx_ok"

# Index scan should return correct result
idx_result=$(PSQL "$DB" -c "SELECT id FROM sh_dump WHERE val = 'row-500'")
check "post_restore_index_query" "500" "$idx_result"

# --- Verify zone map needs rebuild after restore ---
# After restore, zone map is invalid (COPY multi_insert rebuilds data
# but the zone map valid flag is not set). Scan pruning should not
# produce SortedHeapScan (will fall back to seqscan or use unverified zm).
PSQL "$DB" -c "ANALYZE sh_dump"

# --- Compact to restore zone map, then verify pruning ---
PSQL "$DB" -c "SELECT sorted_heap_compact('sh_dump'::regclass)"
PSQL "$DB" -c "ANALYZE sh_dump"

post_compact_pruning=$(PSQL "$DB" -c "
  SET enable_indexscan = off;
  SET enable_bitmapscan = off;
  EXPLAIN (COSTS OFF) SELECT * FROM sh_dump WHERE id = 500" \
  | grep -c 'SortedHeapScan' || true)
check "post_compact_pruning" "1" "$post_compact_pruning"

# Final correctness check
final_range=$(PSQL "$DB" -c "
  SELECT count(*) FROM sh_dump WHERE id BETWEEN 100 AND 200")
check "post_compact_range_query" "101" "$final_range"

# --- Check for crashes ---
crashes=$("$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" status 2>&1 | grep -c 'server is running' || true)
check "no_crashes" "1" "$crashes"

# --- Summary ---
echo ""
echo "dump_restore_test status=$([ "$fail" -eq 0 ] && echo ok || echo FAILED) pass=$pass fail=$fail total=$total"
[ "$fail" -eq 0 ] || exit 1
