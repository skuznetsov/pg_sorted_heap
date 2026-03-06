#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# TOAST data integrity + Concurrent online compact guard
# ============================================================
#
# Spins up an ephemeral PG cluster and runs:
#   Area 1: TOAST data (>2KB values) through all sorted_heap ops
#   Area 2: Concurrent online compact/merge on the same table
#
# Usage: ./scripts/test_toast_and_concurrent_compact.sh [tmp_root] [port]

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
PORT="${2:-65492}"

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
BG_PIDS=()
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
  for pid in "${BG_PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  BG_PIDS=()
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR/data" ]; then
    "$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -m immediate stop >/dev/null 2>&1 || true
  fi
  if [ -n "$TMP_DIR" ]; then
    rm -rf "$TMP_DIR"
  fi
}
trap cleanup EXIT

# --- Create ephemeral cluster ---
TMP_DIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_toast.XXXXXX")"
make -C "$ROOT_DIR" install >/dev/null 2>&1 || true
"$PG_BINDIR/initdb" -D "$TMP_DIR/data" -A trust --no-locale >/dev/null 2>&1
"$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -l "$TMP_DIR/postmaster.log" \
  -o "-k $TMP_DIR -p $PORT" start >/dev/null

PSQL() {
  "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -v ON_ERROR_STOP=1 -qtAX "$@"
}

PSQL -c "CREATE EXTENSION pg_sorted_heap"

# ============================================================
# Area 1: TOAST Data Integrity
# ============================================================
echo ""
echo "=== Area 1: TOAST data integrity ==="

PSQL <<'SQL'
CREATE TABLE toast_test(
    id int PRIMARY KEY,
    category int,
    payload text
) USING sorted_heap;
SQL

# --- Step 1: Initial load with TOASTed values ---
echo ""
echo "--- Step 1: INSERT 10K rows with 4KB payload ---"

PSQL -c "
INSERT INTO toast_test
  SELECT g, g % 100, repeat('x', 4000)
  FROM generate_series(1, 10000) g;
"

count=$(PSQL -c "SELECT count(*) FROM toast_test")
check "toast_initial_count" "10000" "$count"

intact=$(PSQL -c "SELECT count(*) FROM toast_test WHERE length(payload) = 4000")
check "toast_initial_payload_intact" "10000" "$intact"

has_toast=$(PSQL -c "
SELECT CASE WHEN reltoastrelid <> 0 THEN 't' ELSE 'f' END
FROM pg_class WHERE relname = 'toast_test'
")
check "toast_table_exists" "t" "$has_toast"

# --- Step 2: compact ---
echo ""
echo "--- Step 2: compact ---"

PSQL -c "SELECT sorted_heap_compact('toast_test'::regclass)" >/dev/null

count=$(PSQL -c "SELECT count(*) FROM toast_test")
check "toast_compact_count" "10000" "$count"

intact=$(PSQL -c "SELECT count(*) FROM toast_test WHERE length(payload) = 4000")
check "toast_compact_payload_intact" "10000" "$intact"

zm_valid=$(PSQL -c "
SELECT CASE WHEN sorted_heap_zonemap_stats('toast_test'::regclass) LIKE '%flags=valid%'
            THEN '1' ELSE '0' END
")
check "toast_compact_zm_valid" "1" "$zm_valid"

# --- Step 3: INSERT more + online compact ---
echo ""
echo "--- Step 3: INSERT 5K + online compact ---"

PSQL -c "
INSERT INTO toast_test
  SELECT g, g % 100, repeat('y', 4000)
  FROM generate_series(10001, 15000) g;
"

PSQL -c "CALL sorted_heap_compact_online('toast_test'::regclass)" 2>/dev/null

count=$(PSQL -c "SELECT count(*) FROM toast_test")
check "toast_online_compact_count" "15000" "$count"

intact=$(PSQL -c "SELECT count(*) FROM toast_test WHERE length(payload) = 4000")
check "toast_online_compact_payload_intact" "15000" "$intact"

dups=$(PSQL -c "SELECT count(*) FROM (SELECT id FROM toast_test GROUP BY id HAVING count(*) > 1) sub")
check "toast_online_compact_no_dups" "0" "$dups"

# --- Step 4: INSERT overlapping + merge ---
echo ""
echo "--- Step 4: INSERT 5K overlapping + merge ---"

PSQL -c "
INSERT INTO toast_test
  SELECT g, abs(g) % 100, repeat('z', 4000)
  FROM generate_series(-5000, -1) g;
"

PSQL -c "SELECT sorted_heap_merge('toast_test'::regclass)" >/dev/null

count=$(PSQL -c "SELECT count(*) FROM toast_test")
check "toast_merge_count" "20000" "$count"

intact=$(PSQL -c "SELECT count(*) FROM toast_test WHERE length(payload) = 4000")
check "toast_merge_payload_intact" "20000" "$intact"

zm_valid=$(PSQL -c "
SELECT CASE WHEN sorted_heap_zonemap_stats('toast_test'::regclass) LIKE '%flags=valid%'
            THEN '1' ELSE '0' END
")
check "toast_merge_zm_valid" "1" "$zm_valid"

# --- Step 5: INSERT more overlapping + online merge ---
echo ""
echo "--- Step 5: INSERT 5K overlapping + online merge ---"

PSQL -c "
INSERT INTO toast_test
  SELECT g, abs(g) % 100, repeat('w', 4000)
  FROM generate_series(-10000, -5001) g;
"

PSQL -c "CALL sorted_heap_merge_online('toast_test'::regclass)" 2>/dev/null

count=$(PSQL -c "SELECT count(*) FROM toast_test")
check "toast_online_merge_count" "25000" "$count"

intact=$(PSQL -c "SELECT count(*) FROM toast_test WHERE length(payload) = 4000")
check "toast_online_merge_payload_intact" "25000" "$intact"

dups=$(PSQL -c "SELECT count(*) FROM (SELECT id FROM toast_test GROUP BY id HAVING count(*) > 1) sub")
check "toast_online_merge_no_dups" "0" "$dups"

# ============================================================
# Area 2: Concurrent Online Operation Guard
# ============================================================
echo ""
echo "=== Area 2: Concurrent online operation guard ==="

PSQL <<'SQL'
CREATE TABLE conc_guard_test(
    id int PRIMARY KEY,
    val text
) USING sorted_heap;

-- Wide rows so online compact Phase 2 copy takes several seconds
INSERT INTO conc_guard_test
  SELECT g, repeat('x', 2000)
  FROM generate_series(1, 200000) g;

SELECT sorted_heap_compact('conc_guard_test'::regclass);
SQL

pre_count=$(PSQL -c "SELECT count(*) FROM conc_guard_test")
echo "Setup: $pre_count rows compacted (wide rows for slow Phase 2)"

# --- Test 2A: Two concurrent compact_online ---
echo ""
echo "--- Test 2A: concurrent compact_online x2 ---"

"$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -c \
  "CALL sorted_heap_compact_online('conc_guard_test'::regclass)" \
  >/dev/null 2>&1 &
BG_PIDS+=($!)

sleep 1

err_output=$("$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -c \
  "CALL sorted_heap_compact_online('conc_guard_test'::regclass)" 2>&1 || true)

has_error=$(echo "$err_output" | grep -c "already exists" || true)
check "conc_compact_session_b_rejected" "t" "$([ "$has_error" -gt 0 ] && echo t || echo f)"

for pid in "${BG_PIDS[@]}"; do
  wait "$pid" 2>/dev/null || true
done
BG_PIDS=()

count=$(PSQL -c "SELECT count(*) FROM conc_guard_test")
check "conc_compact_count_preserved" "$pre_count" "$count"

dups=$(PSQL -c "SELECT count(*) FROM (SELECT id FROM conc_guard_test GROUP BY id HAVING count(*) > 1) sub")
check "conc_compact_no_dups" "0" "$dups"

# Re-run should succeed (log table cleaned up)
PSQL -c "CALL sorted_heap_compact_online('conc_guard_test'::regclass)" 2>/dev/null
rerun_ok=$?
check "conc_compact_rerun_succeeds" "0" "$rerun_ok"

# --- Test 2B: Two concurrent merge_online ---
echo ""
echo "--- Test 2B: concurrent merge_online x2 ---"

PSQL -c "
INSERT INTO conc_guard_test
  SELECT g, repeat('y', 2000)
  FROM generate_series(-100000, -1) g
  ON CONFLICT (id) DO NOTHING;
"
pre_count=$(PSQL -c "SELECT count(*) FROM conc_guard_test")

"$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -c \
  "CALL sorted_heap_merge_online('conc_guard_test'::regclass)" \
  >/dev/null 2>&1 &
BG_PIDS+=($!)

sleep 1

err_output=$("$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -c \
  "CALL sorted_heap_merge_online('conc_guard_test'::regclass)" 2>&1 || true)

has_error=$(echo "$err_output" | grep -c "already exists" || true)
check "conc_merge_session_b_rejected" "t" "$([ "$has_error" -gt 0 ] && echo t || echo f)"

for pid in "${BG_PIDS[@]}"; do
  wait "$pid" 2>/dev/null || true
done
BG_PIDS=()

count=$(PSQL -c "SELECT count(*) FROM conc_guard_test")
check "conc_merge_count_preserved" "$pre_count" "$count"

dups=$(PSQL -c "SELECT count(*) FROM (SELECT id FROM conc_guard_test GROUP BY id HAVING count(*) > 1) sub")
check "conc_merge_no_dups" "0" "$dups"

# --- Test 2C: Cross-operation (compact_online vs merge_online) ---
echo ""
echo "--- Test 2C: compact_online vs merge_online ---"

PSQL -c "
INSERT INTO conc_guard_test
  SELECT g, repeat('z', 2000)
  FROM generate_series(-200000, -100001) g
  ON CONFLICT (id) DO NOTHING;
"
pre_count=$(PSQL -c "SELECT count(*) FROM conc_guard_test")

"$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -c \
  "CALL sorted_heap_compact_online('conc_guard_test'::regclass)" \
  >/dev/null 2>&1 &
BG_PIDS+=($!)

sleep 1

err_output=$("$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -c \
  "CALL sorted_heap_merge_online('conc_guard_test'::regclass)" 2>&1 || true)

has_error=$(echo "$err_output" | grep -c "already exists" || true)
check "conc_cross_session_b_rejected" "t" "$([ "$has_error" -gt 0 ] && echo t || echo f)"

for pid in "${BG_PIDS[@]}"; do
  wait "$pid" 2>/dev/null || true
done
BG_PIDS=()

count=$(PSQL -c "SELECT count(*) FROM conc_guard_test")
check "conc_cross_count_preserved" "$pre_count" "$count"

dups=$(PSQL -c "SELECT count(*) FROM (SELECT id FROM conc_guard_test GROUP BY id HAVING count(*) > 1) sub")
check "conc_cross_no_dups" "0" "$dups"

# --- Safety: no crashes ---
crashes=$(grep -c 'SIGSEGV\|SIGBUS\|SIGABRT\|server process.*was terminated' "$TMP_DIR/postmaster.log" 2>/dev/null || true)
check "no_crashes" "0" "$crashes"

# ============================================================
# Summary
# ============================================================
echo ""
if [ "$fail" -eq 0 ]; then
  echo "toast_and_concurrent_compact status=ok pass=$pass fail=$fail total=$total"
else
  echo "toast_and_concurrent_compact status=FAIL pass=$pass fail=$fail total=$total"
  exit 1
fi
