#!/usr/bin/env bash
set -euo pipefail

# Verify cluster-wide relation-aware SortedHeapScan counters when the extension
# is loaded through shared_preload_libraries.
#
# Usage: ./scripts/test_shared_relation_scan_stats.sh [tmp_root] [port]

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
PORT="${2:-65486}"

if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root must be absolute: $TMP_ROOT" >&2
  exit 2
fi
if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [ "$PORT" -le 1024 ] || [ "$PORT" -ge 65535 ]; then
  echo "port must be 1025..65534" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if command -v pg_config >/dev/null 2>&1; then
  PG_BINDIR="$(pg_config --bindir)"
else
  PG_BINDIR="/opt/homebrew/Cellar/postgresql@18/18.3/bin"
fi

make -C "$ROOT_DIR" install >/dev/null

pass=0
fail=0
total=0

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

TMP_DIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_shared_stats.XXXXXX")"
"$PG_BINDIR/initdb" -D "$TMP_DIR/data" -A trust --no-locale >/dev/null 2>&1

cat >> "$TMP_DIR/data/postgresql.conf" <<'PGCONF'
log_min_messages = warning
shared_preload_libraries = 'pg_sorted_heap'
PGCONF

"$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -l "$TMP_DIR/postmaster.log" \
  -o "-k $TMP_DIR -p $PORT" start >/dev/null

PSQL() {
  "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" "$@" -v ON_ERROR_STOP=1 -qtAX
}

DB="shared_stats_test"
"$PG_BINDIR/createdb" -h "$TMP_DIR" -p "$PORT" "$DB"

echo "=== shared relation scan stats test ==="

PSQL "$DB" -c "CREATE EXTENSION pg_sorted_heap"
PSQL "$DB" -c "
  CREATE TABLE sh_shared_stats_a(id int PRIMARY KEY, val text) USING sorted_heap;
  CREATE TABLE sh_shared_stats_b(id int PRIMARY KEY, val text) USING sorted_heap;
  INSERT INTO sh_shared_stats_a SELECT g, repeat('a', 80) FROM generate_series(1, 1000) g;
  INSERT INTO sh_shared_stats_b SELECT g, repeat('b', 80) FROM generate_series(1, 1000) g;
  SELECT sorted_heap_compact('sh_shared_stats_a'::regclass);
  SELECT sorted_heap_compact('sh_shared_stats_b'::regclass);
  ANALYZE sh_shared_stats_a;
  ANALYZE sh_shared_stats_b;
  SELECT sorted_heap_reset_stats();
"

# Use separate psql invocations so relation-aware counters must cross backends.
PSQL "$DB" -c "
  SET enable_indexscan = off;
  SET enable_bitmapscan = off;
  SELECT count(*) FROM sh_shared_stats_a WHERE id BETWEEN 10 AND 20;
" >/dev/null

PSQL "$DB" -c "
  SET enable_indexscan = off;
  SET enable_bitmapscan = off;
  SELECT count(*) FROM sh_shared_stats_b WHERE id BETWEEN 30 AND 40;
" >/dev/null

stats_source=$(PSQL "$DB" -c "SELECT source FROM sorted_heap_scan_stats()")
check "aggregate_source_shared" "shared" "$stats_source"

rel_source_rows=$(PSQL "$DB" -c "
  SELECT count(*)
  FROM sorted_heap_scan_stats_by_relation()
  WHERE relid IN ('sh_shared_stats_a'::regclass, 'sh_shared_stats_b'::regclass)
    AND source = 'shared'
    AND total_scans >= 1
    AND blocks_scanned >= 1
")
check "relation_rows_shared_cross_backend" "2" "$rel_source_rows"

PSQL "$DB" -c "SELECT sorted_heap_reset_stats()" >/dev/null
post_reset_rows=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_scan_stats_by_relation()")
check "relation_stats_reset_shared" "0" "$post_reset_rows"

echo "Summary: pass=$pass fail=$fail total=$total"
if [ "$fail" -ne 0 ]; then
  exit 1
fi
