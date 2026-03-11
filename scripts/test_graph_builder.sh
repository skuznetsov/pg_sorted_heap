#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# build_graph.py lifecycle test
# ============================================================
#
# Verifies that the graph builder can:
#   1. bootstrap from a main table
#   2. rebuild from an existing graph sidecar
#   3. handle duplicate id values across partitions via src_tid/ctid join
#
# Usage: ./scripts/test_graph_builder.sh [tmp_root] [port]

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
PORT="${2:-65489}"

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
  PG_BINDIR="/opt/homebrew/Cellar/postgresql@18/18.1_1/bin"
fi

PYTHON_BIN="${PYTHON_BIN:-$("$ROOT_DIR/scripts/find_vector_python.sh")}"

make -C "$ROOT_DIR" install >/dev/null 2>&1 || true

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

TMP_DIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_graph_builder.XXXXXX")"
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

DB="graph_builder_test"
"$PG_BINDIR/createdb" -h "$TMP_DIR" -p "$PORT" "$DB"

echo "=== graph builder lifecycle test ==="

PSQL "$DB" -c "CREATE EXTENSION pg_sorted_heap"

PSQL "$DB" -c "
  CREATE TABLE gb_main (
    partition_id int2 NOT NULL,
    id           text NOT NULL,
    embedding    svec(8),
    PRIMARY KEY (partition_id, id)
  ) USING sorted_heap;

  INSERT INTO gb_main (partition_id, id, embedding) VALUES
    (0, 'dup',   '[1,0,0,0,0,0,0,0]'),
    (0, 'u01',   '[0.9,0.1,0,0,0,0,0,0]'),
    (0, 'u02',   '[0.8,0.2,0,0,0,0,0,0]'),
    (1, 'dup',   '[0,1,0,0,0,0,0,0]'),
    (1, 'u11',   '[0,0.9,0.1,0,0,0,0,0]'),
    (1, 'u12',   '[0,0.8,0.2,0,0,0,0,0]'),
    (2, 'u20',   '[0,0,1,0,0,0,0,0]'),
    (2, 'u21',   '[0,0,0.9,0.1,0,0,0,0]');
"

DSN="host=$TMP_DIR port=$PORT dbname=$DB"

"$PYTHON_BIN" "$ROOT_DIR/scripts/build_graph.py" \
  --dsn "$DSN" \
  --table gb_main \
  --graph-table gb_graph \
  --entry-table gb_entries \
  --bootstrap \
  --sketch-dim 8 \
  --M 4 \
  --M-max 8 \
  --n-adjacent 1 \
  --no-prune \
  --seed 42 >/dev/null

boot_rows="$(PSQL "$DB" -c "SELECT count(*) FROM gb_graph")"
check "bootstrap_graph_rows" "8" "$boot_rows"

boot_entries="$(PSQL "$DB" -c "SELECT count(*) FROM gb_entries")"
check "bootstrap_entry_rows" "3" "$boot_entries"

boot_unique_tids="$(PSQL "$DB" -c "SELECT count(DISTINCT src_tid) FROM gb_graph")"
check "bootstrap_unique_src_tid" "8" "$boot_unique_tids"

dup_rows="$(PSQL "$DB" -c "SELECT count(*) FROM gb_graph WHERE src_id = 'dup'")"
check "bootstrap_duplicate_id_rows" "2" "$dup_rows"

"$PYTHON_BIN" "$ROOT_DIR/scripts/build_graph.py" \
  --dsn "$DSN" \
  --table gb_main \
  --graph-table gb_graph \
  --entry-table gb_entries \
  --M 4 \
  --M-max 8 \
  --n-adjacent 1 \
  --no-prune \
  --seed 42 >/dev/null

rebuild_rows="$(PSQL "$DB" -c "SELECT count(*) FROM gb_graph")"
check "rebuild_graph_rows" "8" "$rebuild_rows"

rebuild_unique_tids="$(PSQL "$DB" -c "SELECT count(DISTINCT src_tid) FROM gb_graph")"
check "rebuild_unique_src_tid" "8" "$rebuild_unique_tids"

rebuild_dups="$(PSQL "$DB" -c "
  SELECT count(*)
  FROM gb_graph g
  JOIN gb_main m ON m.ctid = g.src_tid
  WHERE g.src_id = 'dup'
    AND ((m.partition_id = 0 AND m.embedding::text = '[1,0,0,0,0,0,0,0]')
      OR (m.partition_id = 1 AND m.embedding::text = '[0,1,0,0,0,0,0,0]'))
")"
check "rebuild_duplicate_id_ctid_join" "2" "$rebuild_dups"

cover_idx="$(PSQL "$DB" -c "
  SELECT CASE WHEN count(*) = 1 THEN 't' ELSE 'f' END
  FROM pg_indexes
  WHERE tablename = 'gb_graph'
    AND indexname = 'gb_graph_cover'
")"
check "covering_index_created" "t" "$cover_idx"

echo ""
echo "graph_builder_test status=$([ "$fail" -eq 0 ] && echo ok || echo FAILED) pass=$pass fail=$fail total=$total"
[ "$fail" -eq 0 ] || exit 1
