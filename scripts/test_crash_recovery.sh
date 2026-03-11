#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Crash recovery tests for sorted_heap zone map and online ops
# ============================================================
#
# Spins up ephemeral PG clusters per scenario, simulates crashes
# with pg_ctl stop -m immediate, restarts, and verifies integrity.
#
# Usage: ./scripts/test_crash_recovery.sh [tmp_root] [base_port]

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
BASE_PORT="${2:-65491}"

if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root must be absolute: $TMP_ROOT" >&2; exit 2
fi
if ! [[ "$BASE_PORT" =~ ^[0-9]+$ ]] || [ "$BASE_PORT" -le 1024 ] || [ "$BASE_PORT" -ge 65530 ]; then
  echo "base_port must be 1025..65529" >&2; exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if command -v pg_config >/dev/null 2>&1; then
  PG_BINDIR="$(pg_config --bindir)"
else
  PG_BINDIR="/opt/homebrew/Cellar/postgresql@18/18.1_1/bin"
fi

make -C "$ROOT_DIR" install >/dev/null

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

# --- Cluster lifecycle helpers ---
create_cluster() {
  local name="$1"
  local dir
  dir=$(mktemp -d "$TMP_ROOT/pg_sorted_heap_crash_${name}.XXXXXX")

  "$PG_BINDIR/initdb" -D "$dir/data" -A trust --no-locale >/dev/null 2>&1
  cat >> "$dir/data/postgresql.conf" <<'PGCONF'
log_min_messages = warning
checkpoint_timeout = 30s
max_wal_size = 64MB
PGCONF

  echo "$dir"
}

start_cluster() {
  local dir="$1" port="$2"
  "$PG_BINDIR/pg_ctl" -D "$dir/data" -l "$dir/postmaster.log" \
    -o "-k $dir -p $port" start >/dev/null
  # Wait for ready
  local i
  for i in $(seq 1 30); do
    if "$PG_BINDIR/psql" -h "$dir" -p "$port" postgres -c "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  echo "ERROR: cluster at $dir failed to start" >&2
  cat "$dir/postmaster.log" >&2
  return 1
}

crash_cluster() {
  local dir="$1"
  "$PG_BINDIR/pg_ctl" -D "$dir/data" -m immediate stop >/dev/null 2>&1 || true
}

destroy_cluster() {
  local dir="$1"
  "$PG_BINDIR/pg_ctl" -D "$dir/data" -m fast stop >/dev/null 2>&1 || true
  rm -rf "$dir"
}

PSQL_CMD() {
  local dir="$1" port="$2"
  shift 2
  "$PG_BINDIR/psql" -h "$dir" -p "$port" postgres -v ON_ERROR_STOP=1 -qtAX "$@"
}

# ============================================================
# Scenario 1: Crash during COPY (large batch insert)
# ============================================================
scenario_crash_during_copy() {
  echo "=== Scenario 1: Crash during COPY ==="
  local port=$((BASE_PORT))
  local dir
  dir=$(create_cluster "copy")

  start_cluster "$dir" "$port"
  PSQL_CMD "$dir" "$port" -c "CREATE EXTENSION pg_sorted_heap"
  PSQL_CMD "$dir" "$port" -c "
    CREATE TABLE crash_copy(id int PRIMARY KEY, val text) USING sorted_heap;
    INSERT INTO crash_copy SELECT g, repeat('x', 200) FROM generate_series(1, 5000) g;
  "

  local pre_count
  pre_count=$(PSQL_CMD "$dir" "$port" -c "SELECT count(*) FROM crash_copy")

  # Start a large INSERT in background
  "$PG_BINDIR/psql" -h "$dir" -p "$port" postgres -c "
    INSERT INTO crash_copy SELECT g, repeat('y', 200)
    FROM generate_series(5001, 200000) g;
  " >/dev/null 2>&1 &
  local bg_pid=$!

  sleep 0.5
  crash_cluster "$dir"
  wait "$bg_pid" 2>/dev/null || true

  start_cluster "$dir" "$port"

  local post_count accessible="t"
  post_count=$(PSQL_CMD "$dir" "$port" -c "SELECT count(*) FROM crash_copy" 2>/dev/null) || {
    accessible="f"; post_count=0
  }

  check "copy_crash_accessible" "t" "$accessible"

  local ge_pre
  ge_pre=$([ "$post_count" -ge "$pre_count" ] && echo t || echo f)
  check "copy_crash_count_ge_pre" "t" "$ge_pre"

  local dups
  dups=$(PSQL_CMD "$dir" "$port" -c \
    "SELECT count(*) FROM (SELECT id FROM crash_copy GROUP BY id HAVING count(*) > 1) sub")
  check "copy_crash_no_dups" "0" "$dups"

  echo "  pre=$pre_count post=$post_count"
  destroy_cluster "$dir"
}

# ============================================================
# Scenario 2: Crash after compact (zone map persistence)
# ============================================================
scenario_crash_after_compact() {
  echo "=== Scenario 2: Crash after compact ==="
  local port=$((BASE_PORT + 1))
  local dir
  dir=$(create_cluster "compact")

  start_cluster "$dir" "$port"
  PSQL_CMD "$dir" "$port" -c "CREATE EXTENSION pg_sorted_heap"
  PSQL_CMD "$dir" "$port" -c "
    CREATE TABLE crash_compact(id int PRIMARY KEY, val text) USING sorted_heap;
    INSERT INTO crash_compact SELECT g, repeat('x', 80) FROM generate_series(1, 10000) g;
    SELECT sorted_heap_compact('crash_compact'::regclass);
  "

  # Verify zone map valid before crash
  local zm_before valid_before
  zm_before=$(PSQL_CMD "$dir" "$port" -c \
    "SELECT sorted_heap_zonemap_stats('crash_compact'::regclass)")
  valid_before=$(echo "$zm_before" | grep -c 'flags=valid' || true)
  check "compact_zm_valid_before_crash" "1" "$valid_before"

  crash_cluster "$dir"
  start_cluster "$dir" "$port"

  # Verify zone map still valid after recovery
  local zm_after valid_after
  zm_after=$(PSQL_CMD "$dir" "$port" -c \
    "SELECT sorted_heap_zonemap_stats('crash_compact'::regclass)")
  valid_after=$(echo "$zm_after" | grep -c 'flags=valid' || true)
  check "compact_zm_valid_after_crash" "1" "$valid_after"

  local count
  count=$(PSQL_CMD "$dir" "$port" -c "SELECT count(*) FROM crash_compact")
  check "compact_count_after_crash" "10000" "$count"

  # Verify scan pruning works
  local plan has_zm
  plan=$(PSQL_CMD "$dir" "$port" -c "
    SET enable_indexscan = off;
    SET enable_bitmapscan = off;
    SET max_parallel_workers_per_gather = 0;
    EXPLAIN SELECT * FROM crash_compact WHERE id BETWEEN 100 AND 200;
  ")
  has_zm=$(echo "$plan" | grep -c 'Zone Map' || true)
  check "compact_pruning_after_crash" "1" "$has_zm"

  echo "  count=$count zm_valid=$valid_after"
  destroy_cluster "$dir"
}

# ============================================================
# Scenario 3: Crash during zone map rebuild
# ============================================================
scenario_crash_during_zm_rebuild() {
  echo "=== Scenario 3: Crash during zone map rebuild ==="
  local port=$((BASE_PORT + 2))
  local dir
  dir=$(create_cluster "rebuild")

  start_cluster "$dir" "$port"
  PSQL_CMD "$dir" "$port" -c "CREATE EXTENSION pg_sorted_heap"
  PSQL_CMD "$dir" "$port" -c "
    CREATE TABLE crash_zm(id int PRIMARY KEY, val text) USING sorted_heap;
    INSERT INTO crash_zm SELECT g, repeat('x', 200) FROM generate_series(1, 50000) g;
  "

  # Start rebuild in background
  "$PG_BINDIR/psql" -h "$dir" -p "$port" postgres -c \
    "SELECT sorted_heap_rebuild_zonemap('crash_zm'::regclass)" >/dev/null 2>&1 &
  local bg_pid=$!

  sleep 0.3
  crash_cluster "$dir"
  wait "$bg_pid" 2>/dev/null || true

  start_cluster "$dir" "$port"

  local count accessible="t"
  count=$(PSQL_CMD "$dir" "$port" -c "SELECT count(*) FROM crash_zm" 2>/dev/null) || {
    accessible="f"; count=0
  }

  check "zm_rebuild_crash_accessible" "t" "$accessible"
  check "zm_rebuild_crash_count" "50000" "$count"

  # Re-rebuild should succeed after crash recovery
  local rebuild_ok="t"
  PSQL_CMD "$dir" "$port" -c \
    "SELECT sorted_heap_rebuild_zonemap('crash_zm'::regclass)" 2>/dev/null || rebuild_ok="f"
  check "zm_rebuild_post_recovery_succeeds" "t" "$rebuild_ok"

  echo "  count=$count rebuild_ok=$rebuild_ok"
  destroy_cluster "$dir"
}

# ============================================================
# Scenario 4: Crash during online compact Phase 2
# ============================================================
scenario_crash_during_online_compact() {
  echo "=== Scenario 4: Crash during online compact ==="
  local port=$((BASE_PORT + 3))
  local dir
  dir=$(create_cluster "online")

  start_cluster "$dir" "$port"
  PSQL_CMD "$dir" "$port" -c "CREATE EXTENSION pg_sorted_heap"
  PSQL_CMD "$dir" "$port" -c "
    CREATE TABLE crash_online(id int PRIMARY KEY, val text) USING sorted_heap;
    INSERT INTO crash_online SELECT g, repeat('x', 200)
      FROM generate_series(1, 50000) g;
    SELECT sorted_heap_compact('crash_online'::regclass);
  "

  local pre_count
  pre_count=$(PSQL_CMD "$dir" "$port" -c "SELECT count(*) FROM crash_online")

  # Start online compact in background
  "$PG_BINDIR/psql" -h "$dir" -p "$port" postgres -c \
    "CALL sorted_heap_compact_online('crash_online'::regclass)" >/dev/null 2>&1 &
  local bg_pid=$!

  sleep 1.0
  crash_cluster "$dir"
  wait "$bg_pid" 2>/dev/null || true

  start_cluster "$dir" "$port"

  # Original table should be intact
  local post_count accessible="t"
  post_count=$(PSQL_CMD "$dir" "$port" -c \
    "SELECT count(*) FROM crash_online" 2>/dev/null) || {
    accessible="f"; post_count=0
  }

  check "online_compact_crash_accessible" "t" "$accessible"
  check "online_compact_crash_count_preserved" "$pre_count" "$post_count"

  # Zone map should still be valid
  local zm_after valid_after
  zm_after=$(PSQL_CMD "$dir" "$port" -c \
    "SELECT sorted_heap_zonemap_stats('crash_online'::regclass)")
  valid_after=$(echo "$zm_after" | grep -c 'flags=valid' || true)
  check "online_compact_crash_zm_preserved" "1" "$valid_after"

  # No orphaned log tables (UNLOGGED → lost on crash)
  local log_tables
  log_tables=$(PSQL_CMD "$dir" "$port" -c \
    "SELECT count(*) FROM pg_tables WHERE tablename LIKE '_sh_compact_log_%'")
  check "online_compact_crash_no_log_table" "0" "$log_tables"

  # Can do a clean compact after recovery
  local compact_ok="t"
  PSQL_CMD "$dir" "$port" -c \
    "SELECT sorted_heap_compact('crash_online'::regclass)" 2>/dev/null || compact_ok="f"
  local final_count
  final_count=$(PSQL_CMD "$dir" "$port" -c "SELECT count(*) FROM crash_online")
  check "online_compact_post_recovery_compact" "$pre_count" "$final_count"

  echo "  pre=$pre_count post=$post_count final=$final_count"
  destroy_cluster "$dir"
}

# ============================================================
# Run all scenarios
# ============================================================
scenario_crash_during_copy
echo ""
scenario_crash_after_compact
echo ""
scenario_crash_during_zm_rebuild
echo ""
scenario_crash_during_online_compact

# ============================================================
# Summary
# ============================================================
echo ""
if [ "$fail" -eq 0 ]; then
  echo "crash_recovery_test status=ok pass=$pass fail=$fail total=$total"
else
  echo "crash_recovery_test status=FAIL pass=$pass fail=$fail total=$total"
  exit 1
fi
