#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Crash recovery tests for GraphRAG fact-shaped tables
# ============================================================
#
# Verifies that registered GraphRAG alias schemas survive immediate-crash
# recovery in the important release paths:
# - committed registered table + sorted_hnsw index durability
# - crash during insert into a registered/indexed graph table
# - crash during compact on a registered graph table
#
# Usage: ./scripts/test_graph_rag_crash_recovery.sh [tmp_root] [base_port]

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
BASE_PORT="${2:-65498}"

if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root must be absolute: $TMP_ROOT" >&2
  exit 2
fi
if ! [[ "$BASE_PORT" =~ ^[0-9]+$ ]] || [ "$BASE_PORT" -le 1024 ] || [ "$BASE_PORT" -ge 65530 ]; then
  echo "base_port must be 1025..65529" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if command -v pg_config >/dev/null 2>&1; then
  PG_BINDIR="$(pg_config --bindir)"
else
  PG_BINDIR="/opt/homebrew/Cellar/postgresql@18/18.1_1/bin"
fi

make -C "$ROOT_DIR" install >/dev/null 2>/dev/null || true

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

create_cluster() {
  local name="$1"
  local dir
  dir=$(mktemp -d "$TMP_ROOT/pg_sorted_heap_graphrag_crash_${name}.XXXXXX")
  "$PG_BINDIR/initdb" -D "$dir/data" -A trust --no-locale >/dev/null 2>&1
  cat >> "$dir/data/postgresql.conf" <<'PGCONF'
log_min_messages = warning
checkpoint_timeout = 30s
max_wal_size = 64MB
shared_preload_libraries = ''
PGCONF
  echo "$dir"
}

port_is_free() {
  local port="$1"
  python3 - "$port" <<'PY'
import socket
import sys

port = int(sys.argv[1])
for family, host in ((socket.AF_INET, "127.0.0.1"), (socket.AF_INET6, "::1")):
    try:
        s = socket.socket(family, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.close()
    except OSError:
        sys.exit(1)
sys.exit(0)
PY
}

pick_free_port() {
  local candidate="$1"
  while ! port_is_free "$candidate"; do
    candidate=$((candidate + 1))
    if [ "$candidate" -ge 65530 ]; then
      echo "ERROR: no free port found starting from $1" >&2
      exit 1
    fi
  done
  echo "$candidate"
}

start_cluster() {
  local dir="$1" port="$2"
  "$PG_BINDIR/pg_ctl" -D "$dir/data" -l "$dir/postmaster.log" \
    -o "-k $dir -p $port" start >/dev/null
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

graph_config_sql() {
  local rel="$1"
  cat <<SQL
SELECT format(
  '%s|%s|%s|%s|%s|%s',
  entity_column,
  relation_column,
  target_column,
  embedding_column,
  payload_column,
  is_registered
)
FROM sorted_heap_graph_config('${rel}'::regclass);
SQL
}

graph_signature_sql() {
  local rel="$1"
  cat <<SQL
SELECT COALESCE(
  string_agg(
    format('%s:%s:%s:%s', entity_id, relation_id, target_id, payload),
    '|' ORDER BY distance, entity_id, relation_id, target_id
  ),
  ''
)
FROM sorted_heap_graph_rag(
  '${rel}'::regclass,
  '[0,0,1,0]'::svec,
  relation_path := ARRAY[1,2],
  ann_k := 2,
  top_k := 2,
  score_mode := 'path',
  limit_rows := 0
);
SQL
}

sorted_hnsw_index_exists_sql() {
  local idx="$1"
  cat <<SQL
SELECT CASE WHEN count(*) = 1 THEN 't' ELSE 'f' END
FROM pg_class c
JOIN pg_am am ON am.oid = c.relam
WHERE c.relname = '${idx}'
  AND am.amname = 'sorted_hnsw';
SQL
}

sorted_hnsw_index_valid_sql() {
  local idx="$1"
  cat <<SQL
SELECT CASE WHEN i.indisvalid THEN 't' ELSE 'f' END
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
WHERE c.relname = '${idx}';
SQL
}

sorted_hnsw_plan_usable() {
  local dir="$1" port="$2" table_name="$3"
  local plan
  plan=$(PSQL_CMD "$dir" "$port" -c "
    SET enable_seqscan = off;
    EXPLAIN (COSTS OFF)
    SELECT src_id
    FROM ${table_name}
    ORDER BY vec <=> '[1,0,0,0]'::svec
    LIMIT 2;
  ")
  echo "$plan" | grep -c 'Index Scan using' || true
}

setup_alias_graph() {
  local dir="$1" port="$2" table_name="$3" rows="$4" with_index="$5"
  PSQL_CMD "$dir" "$port" -c "CREATE EXTENSION pg_sorted_heap VERSION '0.13.0'"
  PSQL_CMD "$dir" "$port" <<SQL
CREATE TABLE ${table_name} (
  src_id    int4 NOT NULL,
  edge_type int2 NOT NULL,
  dst_id    int4 NOT NULL,
  vec       svec(4) NOT NULL,
  body      text NOT NULL,
  PRIMARY KEY (src_id, edge_type, dst_id)
) USING sorted_heap;

INSERT INTO ${table_name} VALUES
  (1, 1, 2, '[1,0,0,0]'::svec, 'a'),
  (1, 2, 3, '[0.9,0.1,0,0]'::svec, 'b'),
  (2, 1, 4, '[0,1,0,0]'::svec, 'c'),
  (3, 1, 1, '[0,0,1,0]'::svec, 'd'),
  (3, 2, 5, '[0,0,0.9,0.1]'::svec, 'e'),
  (4, 1, 6, '[0,0,0,1]'::svec, 'f');
SQL

  if [ "$rows" -gt 6 ]; then
    PSQL_CMD "$dir" "$port" <<SQL
INSERT INTO ${table_name}
SELECT
  g,
  CASE WHEN (g % 2) = 0 THEN 1::int2 ELSE 2::int2 END,
  g + 100000,
  '[0,0,0,1]'::svec,
  repeat('z', 96)
FROM generate_series(1000, $((rows + 993))) g;
SQL
  fi

  PSQL_CMD "$dir" "$port" -c "SELECT sorted_heap_compact('${table_name}'::regclass)" >/dev/null
  PSQL_CMD "$dir" "$port" -c "
    SELECT sorted_heap_graph_register(
      '${table_name}'::regclass,
      entity_column := 'src_id',
      relation_column := 'edge_type',
      target_column := 'dst_id',
      embedding_column := 'vec',
      payload_column := 'body'
    )"

  if [ "$with_index" = "yes" ]; then
    PSQL_CMD "$dir" "$port" -c "
      CREATE INDEX ${table_name}_vec_idx
      ON ${table_name} USING sorted_hnsw (vec svec_cosine_ops)"
  fi

  PSQL_CMD "$dir" "$port" -c "ANALYZE ${table_name}"
}

scenario_crash_after_committed_graph_setup() {
  echo "=== Scenario 1: Crash after committed GraphRAG setup ==="
  local port
  port=$(pick_free_port "$BASE_PORT")
  local dir
  dir=$(create_cluster "setup")
  start_cluster "$dir" "$port"

  setup_alias_graph "$dir" "$port" "crash_graph_setup" 6 yes

  local config_before signature_before index_exists_before index_valid_before plan_ok_before
  config_before=$(PSQL_CMD "$dir" "$port" -c "$(graph_config_sql crash_graph_setup)")
  signature_before=$(PSQL_CMD "$dir" "$port" -c "$(graph_signature_sql crash_graph_setup)")
  index_exists_before=$(PSQL_CMD "$dir" "$port" -c "$(sorted_hnsw_index_exists_sql crash_graph_setup_vec_idx)")
  index_valid_before=$(PSQL_CMD "$dir" "$port" -c "$(sorted_hnsw_index_valid_sql crash_graph_setup_vec_idx)")
  plan_ok_before=$(sorted_hnsw_plan_usable "$dir" "$port" "crash_graph_setup")

  check "setup_config_before_crash" "src_id|edge_type|dst_id|vec|body|t" "$config_before"
  check "setup_index_exists_before_crash" "t" "$index_exists_before"
  check "setup_index_valid_before_crash" "t" "$index_valid_before"
  check "setup_knn_plan_before_crash" "1" "$plan_ok_before"

  crash_cluster "$dir"
  start_cluster "$dir" "$port"

  local config_after signature_after index_exists_after index_valid_after plan_ok_after count_after registry_rows
  config_after=$(PSQL_CMD "$dir" "$port" -c "$(graph_config_sql crash_graph_setup)")
  signature_after=$(PSQL_CMD "$dir" "$port" -c "$(graph_signature_sql crash_graph_setup)")
  index_exists_after=$(PSQL_CMD "$dir" "$port" -c "$(sorted_hnsw_index_exists_sql crash_graph_setup_vec_idx)")
  index_valid_after=$(PSQL_CMD "$dir" "$port" -c "$(sorted_hnsw_index_valid_sql crash_graph_setup_vec_idx)")
  plan_ok_after=$(sorted_hnsw_plan_usable "$dir" "$port" "crash_graph_setup")
  count_after=$(PSQL_CMD "$dir" "$port" -c "SELECT count(*) FROM crash_graph_setup")
  registry_rows=$(PSQL_CMD "$dir" "$port" -c "SELECT count(*) FROM sorted_heap_graph_registry")

  check "setup_count_after_crash" "6" "$count_after"
  check "setup_registry_after_crash" "1" "$registry_rows"
  check "setup_config_after_crash" "$config_before" "$config_after"
  check "setup_signature_after_crash" "$signature_before" "$signature_after"
  check "setup_index_exists_after_crash" "t" "$index_exists_after"
  check "setup_index_valid_after_crash" "t" "$index_valid_after"
  check "setup_knn_plan_after_crash" "1" "$plan_ok_after"

  destroy_cluster "$dir"
}

scenario_crash_during_graph_insert() {
  echo "=== Scenario 2: Crash during insert into registered graph ==="
  local port
  port=$(pick_free_port "$((BASE_PORT + 1))")
  local dir
  dir=$(create_cluster "insert")
  start_cluster "$dir" "$port"

  setup_alias_graph "$dir" "$port" "crash_graph_insert" 5000 yes

  local pre_count signature_before config_before
  pre_count=$(PSQL_CMD "$dir" "$port" -c "SELECT count(*) FROM crash_graph_insert")
  signature_before=$(PSQL_CMD "$dir" "$port" -c "$(graph_signature_sql crash_graph_insert)")
  config_before=$(PSQL_CMD "$dir" "$port" -c "$(graph_config_sql crash_graph_insert)")

  "$PG_BINDIR/psql" -h "$dir" -p "$port" postgres -v ON_ERROR_STOP=1 -c "
    INSERT INTO crash_graph_insert
    SELECT
      g,
      CASE WHEN (g % 2) = 0 THEN 1::int2 ELSE 2::int2 END,
      g + 200000,
      '[0,0,0,1]'::svec,
      repeat('y', 128)
    FROM generate_series(200000, 320000) g;
  " >/dev/null 2>&1 &
  local bg_pid=$!

  sleep 0.4
  crash_cluster "$dir"
  wait "$bg_pid" 2>/dev/null || true
  start_cluster "$dir" "$port"

  local post_count accessible="t" no_dups signature_after config_after index_valid_after
  post_count=$(PSQL_CMD "$dir" "$port" -c "SELECT count(*) FROM crash_graph_insert" 2>/dev/null) || {
    accessible="f"
    post_count=0
  }
  no_dups=$(PSQL_CMD "$dir" "$port" -c "
    SELECT count(*) FROM (
      SELECT src_id, edge_type, dst_id
      FROM crash_graph_insert
      GROUP BY src_id, edge_type, dst_id
      HAVING count(*) > 1
    ) dup")
  signature_after=$(PSQL_CMD "$dir" "$port" -c "$(graph_signature_sql crash_graph_insert)")
  config_after=$(PSQL_CMD "$dir" "$port" -c "$(graph_config_sql crash_graph_insert)")
  index_valid_after=$(PSQL_CMD "$dir" "$port" -c "$(sorted_hnsw_index_valid_sql crash_graph_insert_vec_idx)")

  check "insert_accessible_after_crash" "t" "$accessible"
  check "insert_count_ge_pre" "t" "$([ "$post_count" -ge "$pre_count" ] && echo t || echo f)"
  check "insert_no_duplicate_pk" "0" "$no_dups"
  check "insert_config_after_crash" "$config_before" "$config_after"
  check "insert_signature_after_crash" "$signature_before" "$signature_after"
  check "insert_index_valid_after_crash" "t" "$index_valid_after"

  destroy_cluster "$dir"
}

scenario_crash_during_graph_compact() {
  echo "=== Scenario 3: Crash during compact on registered graph ==="
  local port
  port=$(pick_free_port "$((BASE_PORT + 2))")
  local dir
  dir=$(create_cluster "compact")
  start_cluster "$dir" "$port"

  setup_alias_graph "$dir" "$port" "crash_graph_compact" 50000 no

  local pre_count config_before signature_before
  pre_count=$(PSQL_CMD "$dir" "$port" -c "SELECT count(*) FROM crash_graph_compact")
  config_before=$(PSQL_CMD "$dir" "$port" -c "$(graph_config_sql crash_graph_compact)")
  signature_before=$(PSQL_CMD "$dir" "$port" -c "$(graph_signature_sql crash_graph_compact)")

  "$PG_BINDIR/psql" -h "$dir" -p "$port" postgres -v ON_ERROR_STOP=1 -c \
    "SELECT sorted_heap_compact('crash_graph_compact'::regclass)" >/dev/null 2>&1 &
  local bg_pid=$!

  sleep 0.2
  crash_cluster "$dir"
  wait "$bg_pid" 2>/dev/null || true
  start_cluster "$dir" "$port"

  local post_count accessible="t" config_after signature_after compact_ok="t"
  post_count=$(PSQL_CMD "$dir" "$port" -c "SELECT count(*) FROM crash_graph_compact" 2>/dev/null) || {
    accessible="f"
    post_count=0
  }
  config_after=$(PSQL_CMD "$dir" "$port" -c "$(graph_config_sql crash_graph_compact)")
  signature_after=$(PSQL_CMD "$dir" "$port" -c "$(graph_signature_sql crash_graph_compact)")
  PSQL_CMD "$dir" "$port" -c "SELECT sorted_heap_compact('crash_graph_compact'::regclass)" >/dev/null 2>&1 || compact_ok="f"

  check "compact_accessible_after_crash" "t" "$accessible"
  check "compact_count_after_crash" "$pre_count" "$post_count"
  check "compact_config_after_crash" "$config_before" "$config_after"
  check "compact_signature_after_crash" "$signature_before" "$signature_after"
  check "compact_post_recovery_succeeds" "t" "$compact_ok"

  destroy_cluster "$dir"
}

scenario_crash_after_committed_graph_setup
echo ""
scenario_crash_during_graph_insert
echo ""
scenario_crash_during_graph_compact

echo ""
if [ "$fail" -eq 0 ]; then
  echo "graph_rag_crash_recovery_test status=ok pass=$pass fail=$fail total=$total"
else
  echo "graph_rag_crash_recovery_test status=FAIL pass=$pass fail=$fail total=$total"
  exit 1
fi
