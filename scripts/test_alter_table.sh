#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# ALTER TABLE on sorted_heap tables
# ============================================================
#
# Tests ADD COLUMN, DROP COLUMN, RENAME, ALTER TYPE, DROP/ADD PK
# on a sorted_heap table with data and zone map, verifying data
# integrity and scan pruning after each DDL.
#
# Usage: ./scripts/test_alter_table.sh [tmp_root] [port]

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
PORT="${2:-65493}"

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

TMP_DIR=""
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

cleanup() {
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR/data" ]; then
    "$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -m immediate stop >/dev/null 2>&1 || true
  fi
  if [ -n "$TMP_DIR" ]; then
    rm -rf "$TMP_DIR"
  fi
}
trap cleanup EXIT

# --- Create ephemeral cluster ---
TMP_DIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_alter.XXXXXX")"
make -C "$ROOT_DIR" install >/dev/null 2>/dev/null || true
"$PG_BINDIR/initdb" -D "$TMP_DIR/data" -A trust --no-locale >/dev/null 2>&1
"$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -l "$TMP_DIR/postmaster.log" \
  -o "-k $TMP_DIR -p $PORT" start >/dev/null

PSQL() {
  "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -v ON_ERROR_STOP=1 -qtAX "$@"
}

PSQL -c "CREATE EXTENSION pg_sorted_heap"

# --- Setup: table with data + zone map ---
PSQL <<'SQL'
CREATE TABLE alter_test(
    id int PRIMARY KEY,
    category int,
    val text
) USING sorted_heap;

INSERT INTO alter_test
  SELECT g, g % 100, 'row-' || g
  FROM generate_series(1, 10000) g;

SELECT sorted_heap_compact('alter_test'::regclass);
SQL

echo "Setup: 10000 rows, compacted, zone map valid"

# Helper: check zone map valid (flags contain 'valid')
zm_valid() {
  PSQL -c "
    SELECT CASE WHEN sorted_heap_zonemap_stats('alter_test'::regclass) LIKE '%flags=valid%'
                THEN '1' ELSE '0' END
  "
}

# Helper: check scan pruning works (EXPLAIN shows Zone Map)
has_pruning() {
  local col="$1" lo="$2" hi="$3"
  PSQL -c "EXPLAIN SELECT * FROM alter_test WHERE $col BETWEEN $lo AND $hi" \
    | grep -c "Zone Map" || true
}

# ============================================================
# Test 1: ADD COLUMN (no default)
# ============================================================
echo ""
echo "--- Test 1: ADD COLUMN (no default) ---"

PSQL -c "ALTER TABLE alter_test ADD COLUMN extra int"

count=$(PSQL -c "SELECT count(*) FROM alter_test")
check "add_col_count" "10000" "$count"

nulls=$(PSQL -c "SELECT count(*) FROM alter_test WHERE extra IS NULL")
check "add_col_nulls" "10000" "$nulls"

check "add_col_zm_valid" "1" "$(zm_valid)"
check "add_col_pruning" "1" "$(has_pruning id 100 200)"

# ============================================================
# Test 2: ADD COLUMN (with default)
# ============================================================
echo ""
echo "--- Test 2: ADD COLUMN (with default) ---"

PSQL -c "ALTER TABLE alter_test ADD COLUMN flag boolean DEFAULT true"

count=$(PSQL -c "SELECT count(*) FROM alter_test")
check "add_col_default_count" "10000" "$count"

flags=$(PSQL -c "SELECT count(*) FROM alter_test WHERE flag = true")
check "add_col_default_all_true" "10000" "$flags"

# INSERT after ADD COLUMN
PSQL -c "INSERT INTO alter_test VALUES (10001, 50, 'new', 42, false)"
count=$(PSQL -c "SELECT count(*) FROM alter_test")
check "add_col_insert_count" "10001" "$count"

check "add_col_default_zm_valid" "1" "$(zm_valid)"

# ============================================================
# Test 3: DROP COLUMN (non-PK)
# ============================================================
echo ""
echo "--- Test 3: DROP COLUMN (non-PK) ---"

PSQL -c "ALTER TABLE alter_test DROP COLUMN extra"

count=$(PSQL -c "SELECT count(*) FROM alter_test")
check "drop_col_count" "10001" "$count"

check "drop_col_zm_valid" "1" "$(zm_valid)"
check "drop_col_pruning" "1" "$(has_pruning id 100 200)"

# ============================================================
# Test 4: RENAME COLUMN (non-PK)
# ============================================================
echo ""
echo "--- Test 4: RENAME COLUMN (non-PK) ---"

PSQL -c "ALTER TABLE alter_test RENAME COLUMN val TO description"

count=$(PSQL -c "SELECT count(*) FROM alter_test")
check "rename_col_count" "10001" "$count"

accessible=$(PSQL -c "SELECT count(*) FROM alter_test WHERE description IS NOT NULL")
check "rename_col_accessible" "10001" "$accessible"

check "rename_col_zm_valid" "1" "$(zm_valid)"

# ============================================================
# Test 5: RENAME PK COLUMN
# ============================================================
echo ""
echo "--- Test 5: RENAME PK COLUMN ---"

PSQL -c "ALTER TABLE alter_test RENAME COLUMN id TO pk_id"

count=$(PSQL -c "SELECT count(*) FROM alter_test")
check "rename_pk_count" "10001" "$count"

check "rename_pk_zm_valid" "1" "$(zm_valid)"
check "rename_pk_pruning" "1" "$(has_pruning pk_id 100 200)"

# ============================================================
# Test 6: ALTER TYPE (non-PK column)
# ============================================================
echo ""
echo "--- Test 6: ALTER TYPE (non-PK, triggers table rewrite) ---"

PSQL -c "ALTER TABLE alter_test ALTER COLUMN category TYPE bigint"

count=$(PSQL -c "SELECT count(*) FROM alter_test")
check "alter_type_count" "10001" "$count"

# Table rewrite creates new filelocator → new meta page (zone map empty).
# Zone map flags will NOT be 2 after rewrite since the new meta page is blank.
# Need a compact to rebuild zone map.
PSQL -c "SELECT sorted_heap_compact('alter_test'::regclass)" >/dev/null

check "alter_type_zm_after_compact" "1" "$(zm_valid)"
check "alter_type_pruning" "1" "$(has_pruning pk_id 100 200)"

# ============================================================
# Test 7: INSERT after all DDL
# ============================================================
echo ""
echo "--- Test 7: INSERT after all DDL ---"

PSQL -c "
INSERT INTO alter_test
  SELECT g, g % 100, 'post-ddl-' || g, true
  FROM generate_series(10002, 10100) g;
"

count=$(PSQL -c "SELECT count(*) FROM alter_test")
check "post_ddl_insert_count" "10100" "$count"

# ============================================================
# Test 8: compact after all DDL
# ============================================================
echo ""
echo "--- Test 8: compact after all DDL ---"

PSQL -c "SELECT sorted_heap_compact('alter_test'::regclass)" >/dev/null

count=$(PSQL -c "SELECT count(*) FROM alter_test")
check "compact_after_ddl_count" "10100" "$count"

check "compact_after_ddl_zm_valid" "1" "$(zm_valid)"

# ============================================================
# Test 9: online compact after DDL
# ============================================================
echo ""
echo "--- Test 9: online compact after DDL ---"

PSQL -c "INSERT INTO alter_test
  SELECT g, g % 100, 'more-' || g, true
  FROM generate_series(10101, 10200) g;
"

PSQL -c "CALL sorted_heap_compact_online('alter_test'::regclass)" 2>/dev/null

count=$(PSQL -c "SELECT count(*) FROM alter_test")
check "online_compact_after_ddl_count" "10200" "$count"

dups=$(PSQL -c "SELECT count(*) FROM (SELECT pk_id FROM alter_test GROUP BY pk_id HAVING count(*) > 1) sub")
check "online_compact_after_ddl_no_dups" "0" "$dups"

# ============================================================
# Test 10: ADD secondary index after DDL
# ============================================================
echo ""
echo "--- Test 10: ADD secondary index ---"

PSQL -c "CREATE INDEX alter_test_cat_idx ON alter_test(category)"

idx_count=$(PSQL -c "SET enable_seqscan = off; SELECT count(*) FROM alter_test WHERE category >= 0")
seq_count=$(PSQL -c "SET enable_indexscan = off; SET enable_bitmapscan = off; SELECT count(*) FROM alter_test")
check "secondary_idx_consistent" "$seq_count" "$idx_count"

# ============================================================
# Test 11: DROP PRIMARY KEY
# ============================================================
echo ""
echo "--- Test 11: DROP PRIMARY KEY ---"

PSQL -c "ALTER TABLE alter_test DROP CONSTRAINT alter_test_pkey"

count=$(PSQL -c "SELECT count(*) FROM alter_test")
check "drop_pk_count" "10200" "$count"

# DML still works without PK
PSQL -c "INSERT INTO alter_test VALUES (99999, 1, 'no-pk', true)"
count=$(PSQL -c "SELECT count(*) FROM alter_test")
check "drop_pk_insert_works" "10201" "$count"

# Zone map pruning should NOT work (no PK → no SortedHeapScan)
pruning=$(has_pruning pk_id 100 200)
check "drop_pk_no_pruning" "0" "$pruning"

# ============================================================
# Test 12: Re-ADD PRIMARY KEY
# ============================================================
echo ""
echo "--- Test 12: Re-ADD PRIMARY KEY ---"

# Remove the duplicate-safe row first (99999 exists once, no dups)
PSQL -c "ALTER TABLE alter_test ADD PRIMARY KEY (pk_id)"

count=$(PSQL -c "SELECT count(*) FROM alter_test")
check "readd_pk_count" "10201" "$count"

PSQL -c "SELECT sorted_heap_compact('alter_test'::regclass)" >/dev/null

check "readd_pk_zm_valid" "1" "$(zm_valid)"
check "readd_pk_pruning" "1" "$(has_pruning pk_id 100 200)"

# ============================================================
# Test 13: Concurrent DDL + active scan
# ============================================================
echo ""
echo "--- Test 13: Concurrent DDL + active scan ---"

# Background: SELECT count(*) in a loop
bg_ok=0
(
  for i in $(seq 1 10); do
    "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres \
      -v ON_ERROR_STOP=1 -qtAX \
      -c "SELECT count(*) FROM alter_test" >/dev/null
  done
  echo "bg_done"
) > "$TMP_DIR/bg_result.txt" 2>&1 &
BG_PID=$!

# Small delay so at least some SELECTs overlap with the DDL
sleep 0.1

# Foreground: ALTER TABLE ADD COLUMN during active scans
PSQL -c "ALTER TABLE alter_test ADD COLUMN concurrent_col int DEFAULT 0"

# Wait for background to finish
if wait $BG_PID; then
  bg_result=$(cat "$TMP_DIR/bg_result.txt")
  if [ "$bg_result" = "bg_done" ]; then
    bg_ok=1
  fi
fi
check "concurrent_ddl_bg_finished" "1" "$bg_ok"

# Verify the new column exists and is accessible
concurrent_col_exists=$(PSQL -c "
  SELECT CASE WHEN count(*) = 1 THEN '1' ELSE '0' END
  FROM information_schema.columns
  WHERE table_name = 'alter_test' AND column_name = 'concurrent_col'")
check "concurrent_ddl_column_exists" "1" "$concurrent_col_exists"

# Verify data still intact
count=$(PSQL -c "SELECT count(*) FROM alter_test")
check "concurrent_ddl_count" "10201" "$count"

# --- Safety: no crashes ---
crashes=$(grep -c 'SIGSEGV\|SIGBUS\|SIGABRT\|server process.*was terminated' "$TMP_DIR/postmaster.log" 2>/dev/null || true)
check "no_crashes" "0" "$crashes"

# ============================================================
# Summary
# ============================================================
echo ""
if [ "$fail" -eq 0 ]; then
  echo "alter_table_test status=ok pass=$pass fail=$fail total=$total"
else
  echo "alter_table_test status=FAIL pass=$pass fail=$fail total=$total"
  exit 1
fi
