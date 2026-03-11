#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Concurrent DML during online compact / online merge
# ============================================================
#
# Spins up an ephemeral PG cluster, creates a sorted_heap table,
# runs online compact and online merge while background workers
# perform INSERT / UPDATE / DELETE / SELECT concurrently.
#
# Usage: ./scripts/test_concurrent_online_ops.sh [tmp_root] [port]

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
PORT="${2:-65490}"
INITIAL_ROWS=50000
INSERT_RANGE_START=100001
DML_DURATION=15          # seconds per test phase
WORKER_SLEEP=0.01        # 10ms between DML ops

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
WORKER_PIDS=()
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
  for pid in "${WORKER_PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  WORKER_PIDS=()
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR/data" ]; then
    "$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -m immediate stop >/dev/null 2>&1 || true
  fi
  if [ -n "$TMP_DIR" ]; then
    rm -rf "$TMP_DIR"
  fi
}
trap cleanup EXIT

# --- Create ephemeral cluster ---
TMP_DIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_concurrent.XXXXXX")"
make -C "$ROOT_DIR" install >/dev/null
"$PG_BINDIR/initdb" -D "$TMP_DIR/data" -A trust --no-locale >/dev/null 2>&1
"$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -l "$TMP_DIR/postmaster.log" \
  -o "-k $TMP_DIR -p $PORT" start >/dev/null

PSQL() {
  "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -v ON_ERROR_STOP=1 -qtAX "$@"
}

PSQL -c "CREATE EXTENSION pg_sorted_heap"

# --- Schema + initial data ---
PSQL <<SQL
CREATE TABLE conc_test(
    id int PRIMARY KEY,
    category int,
    val text
) USING sorted_heap;

CREATE INDEX conc_test_category_idx ON conc_test(category);

INSERT INTO conc_test
  SELECT g, g % 100, repeat('x', 80)
  FROM generate_series(1, $INITIAL_ROWS) g;

SELECT sorted_heap_compact('conc_test'::regclass);
SQL

echo "Setup: ${INITIAL_ROWS} rows, compacted, secondary index created"

# --- Worker functions ---
insert_worker() {
  local id=$INSERT_RANGE_START
  local end_time=$(($(date +%s) + DML_DURATION))
  while [ "$(date +%s)" -lt "$end_time" ]; do
    "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -qtAX \
      -c "INSERT INTO conc_test VALUES ($id, $id % 100, 'ins-$id')" \
      2>/dev/null || true
    id=$((id + 1))
    sleep $WORKER_SLEEP
  done
  echo "$((id - INSERT_RANGE_START))" > "$TMP_DIR/inserts_done"
}

update_worker() {
  local end_time=$(($(date +%s) + DML_DURATION))
  local count=0
  while [ "$(date +%s)" -lt "$end_time" ]; do
    local target=$((RANDOM % INITIAL_ROWS + 1))
    "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -qtAX \
      -c "UPDATE conc_test SET val = 'upd-$count' WHERE id = $target" \
      2>/dev/null || true
    count=$((count + 1))
    sleep $WORKER_SLEEP
  done
  echo "$count" > "$TMP_DIR/updates_done"
}

delete_worker() {
  local end_time=$(($(date +%s) + DML_DURATION))
  local count=0
  while [ "$(date +%s)" -lt "$end_time" ]; do
    local target=$((40000 + RANDOM % 10000 + 1))
    "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -qtAX \
      -c "DELETE FROM conc_test WHERE id = $target" \
      2>/dev/null || true
    count=$((count + 1))
    sleep $WORKER_SLEEP
  done
  echo "$count" > "$TMP_DIR/deletes_done"
}

select_worker() {
  local end_time=$(($(date +%s) + DML_DURATION))
  local count=0
  while [ "$(date +%s)" -lt "$end_time" ]; do
    local lo=$((RANDOM % 40000 + 1))
    local hi=$((lo + 100))
    "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -qtAX \
      -c "SELECT count(*) FROM conc_test WHERE id BETWEEN $lo AND $hi" \
      >/dev/null 2>&1 || true
    count=$((count + 1))
    sleep $WORKER_SLEEP
  done
  echo "$count" > "$TMP_DIR/selects_done"
}

# --- Verification ---
verify_table() {
  local phase="$1"

  # V1: positive row count
  local count
  count=$(PSQL -c "SELECT count(*) FROM conc_test")
  check "${phase}_count_positive" "t" "$([ "$count" -gt 0 ] && echo t || echo f)"

  # V2: no duplicate PKs
  local dups
  dups=$(PSQL -c \
    "SELECT count(*) FROM (SELECT id FROM conc_test GROUP BY id HAVING count(*) > 1) sub")
  check "${phase}_no_duplicate_pks" "0" "$dups"

  # V3: secondary index consistent with seqscan
  local idx_count seq_count
  idx_count=$(PSQL -c \
    "SET enable_seqscan = off; SELECT count(*) FROM conc_test WHERE category >= 0")
  seq_count=$(PSQL -c \
    "SET enable_indexscan = off; SET enable_bitmapscan = off; SELECT count(*) FROM conc_test")
  check "${phase}_index_consistent" "$seq_count" "$idx_count"

  # V4: zone map — just verify the function works (concurrent DML
  # invalidates the zone map flag, which is correct behaviour)
  local zm
  zm=$(PSQL -c "SELECT sorted_heap_zonemap_stats('conc_test'::regclass)")
  echo "  ${phase}: zonemap_stats returned ok (flags may be 0 due to concurrent DML)"

  # Report worker stats if available
  local ins upd del sel
  ins=$(cat "$TMP_DIR/inserts_done" 2>/dev/null || echo "?")
  upd=$(cat "$TMP_DIR/updates_done" 2>/dev/null || echo "?")
  del=$(cat "$TMP_DIR/deletes_done" 2>/dev/null || echo "?")
  sel=$(cat "$TMP_DIR/selects_done" 2>/dev/null || echo "?")
  echo "  ${phase}: rows=$count dups=$dups idx=$idx_count seq=$seq_count"
  echo "  ${phase}: workers ins=$ins upd=$upd del=$del sel=$sel"
  rm -f "$TMP_DIR"/{inserts,updates,deletes,selects}_done
}

# ============================================================
# Test A: Online compact with concurrent DML
# ============================================================
echo ""
echo "=== Test A: Online compact with concurrent DML ==="

WORKER_PIDS=()
insert_worker &
WORKER_PIDS+=($!)
update_worker &
WORKER_PIDS+=($!)
delete_worker &
WORKER_PIDS+=($!)
select_worker &
WORKER_PIDS+=($!)

PSQL -c "CALL sorted_heap_compact_online('conc_test'::regclass)" 2>/dev/null

for pid in "${WORKER_PIDS[@]}"; do
  wait "$pid" 2>/dev/null || true
done
WORKER_PIDS=()

verify_table "online_compact"

# ============================================================
# Test B: Online merge with concurrent DML
# ============================================================
echo ""
echo "=== Test B: Online merge with concurrent DML ==="

# Add overlapping rows for merge tail (negative IDs sort before existing)
PSQL -c "
INSERT INTO conc_test
  SELECT g, abs(g) % 100, 'merge-tail-' || g
  FROM generate_series(-5000, -1) g
  ON CONFLICT (id) DO NOTHING;
"

WORKER_PIDS=()
insert_worker &
WORKER_PIDS+=($!)
update_worker &
WORKER_PIDS+=($!)
delete_worker &
WORKER_PIDS+=($!)
select_worker &
WORKER_PIDS+=($!)

PSQL -c "CALL sorted_heap_merge_online('conc_test'::regclass)" 2>/dev/null

for pid in "${WORKER_PIDS[@]}"; do
  wait "$pid" 2>/dev/null || true
done
WORKER_PIDS=()

verify_table "online_merge"

# ============================================================
# Test C: UPDATE PK during online compact — no duplicates
# ============================================================
echo ""
echo "=== Test C: UPDATE PK during online compact ==="

# Create a fresh table for this test
PSQL <<SQL
CREATE TABLE pkchange_test(
    id int PRIMARY KEY,
    val text
) USING sorted_heap;

INSERT INTO pkchange_test
  SELECT g, repeat('x', 200)
  FROM generate_series(1, 50000) g;

SELECT sorted_heap_compact('pkchange_test'::regclass);
SQL

# Worker that changes PKs: shifts rows from 40001-50000 up by 100000
pkchange_worker() {
  local count=0
  local end_time=$(($(date +%s) + DML_DURATION))
  while [ "$(date +%s)" -lt "$end_time" ]; do
    local target=$((40001 + RANDOM % 10000))
    local new_id=$((target + 100000))
    "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -qtAX \
      -c "UPDATE pkchange_test SET id = $new_id WHERE id = $target" \
      2>/dev/null || true
    count=$((count + 1))
    sleep $WORKER_SLEEP
  done
  echo "$count" > "$TMP_DIR/pkchanges_done"
}

WORKER_PIDS=()
pkchange_worker &
WORKER_PIDS+=($!)
select_worker &
WORKER_PIDS+=($!)

PSQL -c "CALL sorted_heap_compact_online('pkchange_test'::regclass)" 2>/dev/null

for pid in "${WORKER_PIDS[@]}"; do
  wait "$pid" 2>/dev/null || true
done
WORKER_PIDS=()

# Verify: exactly 50000 rows (PK change = move, not duplicate)
pkc_count=$(PSQL -c "SELECT count(*) FROM pkchange_test")
check "pkchange_count" "50000" "$pkc_count"

# No duplicate PKs
pkc_dups=$(PSQL -c \
  "SELECT count(*) FROM (SELECT id FROM pkchange_test GROUP BY id HAVING count(*) > 1) sub")
check "pkchange_no_duplicate_pks" "0" "$pkc_dups"

# No old PKs remain that were moved (old range 40001-50000 should have gaps,
# new range 140001-150000 should have corresponding rows)
pkc_moved=$(cat "$TMP_DIR/pkchanges_done" 2>/dev/null || echo "?")
echo "  pkchange: total_rows=$pkc_count dups=$pkc_dups pk_changes_attempted=$pkc_moved"

# Verify rows with new PKs exist
pkc_new_range=$(PSQL -c "SELECT count(*) FROM pkchange_test WHERE id > 100000")
echo "  pkchange: rows_in_new_range=$pkc_new_range"

PSQL -c "DROP TABLE pkchange_test" 2>/dev/null || true

# ============================================================
# Summary
# ============================================================
echo ""
if [ "$fail" -eq 0 ]; then
  echo "concurrent_online_test status=ok pass=$pass fail=$fail total=$total"
else
  echo "concurrent_online_test status=FAIL pass=$pass fail=$fail total=$total"
  exit 1
fi
