#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Concurrent DML + online compact/merge on GraphRAG tables
# ============================================================
#
# Verifies that a registered alias-schema fact graph remains usable while:
# - INSERT / UPDATE / DELETE mutate non-seed rows
# - GraphRAG queries execute concurrently
# - sorted_hnsw KNN queries execute concurrently
# - sorted_heap_compact_online / sorted_heap_merge_online run in the foreground
#
# Usage: ./scripts/test_graph_rag_concurrent.sh [tmp_root] [port]

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
PORT="${2:-65499}"
FILLER_ROWS=50000
DML_DURATION=12
WORKER_SLEEP=0.01
INSERT_START=200000
GRAPH_ANN_K=16
GRAPH_TOP_K=2
GRAPH_EF_SEARCH=128

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

make -C "$ROOT_DIR" install >/dev/null 2>/dev/null || true

TMP_DIR=""
WORKER_PIDS=()
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

TMP_DIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_graphrag_concurrent.XXXXXX")"
"$PG_BINDIR/initdb" -D "$TMP_DIR/data" -A trust --no-locale >/dev/null 2>&1
cat >> "$TMP_DIR/data/postgresql.conf" <<'PGCONF'
log_min_messages = warning
shared_preload_libraries = ''
PGCONF
"$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -l "$TMP_DIR/postmaster.log" \
  -o "-k $TMP_DIR -p $PORT" start >/dev/null

PSQL() {
  "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -v ON_ERROR_STOP=1 -qtAX "$@"
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
  '[1,0,0,0]'::svec,
  relation_path := ARRAY[2],
  ann_k := ${GRAPH_ANN_K},
  top_k := ${GRAPH_TOP_K},
  score_mode := 'endpoint',
  limit_rows := 0
);
SQL
}

helper_signature_sql() {
  local rel="$1"
  cat <<SQL
SELECT COALESCE(
  string_agg(
    format('%s:%s:%s:%s', entity_id, relation_id, target_id, payload),
    '|' ORDER BY distance, entity_id, relation_id, target_id
  ),
  ''
)
FROM sorted_heap_expand_rerank(
  '${rel}'::regclass,
  ARRAY[1],
  '[1,0,0,0]'::svec,
  ${GRAPH_TOP_K},
  2,
  0
);
SQL
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
  local table_name="$1"
  local plan
  plan=$(PSQL -c "
    SET enable_seqscan = off;
    EXPLAIN (COSTS OFF)
    SELECT src_id
    FROM ${table_name}
    ORDER BY vec <=> '[1,0,0,0]'::svec
    LIMIT 2;
  ")
  echo "$plan" | grep -c 'Index Scan using' || true
}

setup_graph() {
  local table_name="$1"
  PSQL -c "CREATE EXTENSION pg_sorted_heap VERSION '0.13.0'"
  PSQL <<SQL
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

INSERT INTO ${table_name}
SELECT
  g,
  CASE WHEN (g % 2) = 0 THEN 1::int2 ELSE 2::int2 END,
  g + 100000,
  '[0,0,0,1]'::svec,
  repeat('z', 256)
FROM generate_series(1000, $((FILLER_ROWS + 999))) g;

SELECT sorted_heap_compact('${table_name}'::regclass);
CREATE INDEX ${table_name}_vec_idx
ON ${table_name} USING sorted_hnsw (vec svec_cosine_ops)
WITH (m = 16, ef_construction = 200);
ANALYZE ${table_name};

SELECT sorted_heap_graph_register(
  '${table_name}'::regclass,
  entity_column := 'src_id',
  relation_column := 'edge_type',
  target_column := 'dst_id',
  embedding_column := 'vec',
  payload_column := 'body'
);
SQL
}

insert_worker() {
  local table_name="$1"
  local id=$INSERT_START
  local ok=0
  local err=0
  local end_time=$(($(date +%s) + DML_DURATION))
  while [ "$(date +%s)" -lt "$end_time" ]; do
    if "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -qtAX \
      -c "INSERT INTO ${table_name} VALUES ($id, CASE WHEN ($id % 2)=0 THEN 1 ELSE 2 END, $((id + 300000)), '[0,0,0,1]'::svec, repeat('i', 256)) ON CONFLICT DO NOTHING" \
      >/dev/null 2>&1; then
      ok=$((ok + 1))
    else
      err=$((err + 1))
    fi
    id=$((id + 1))
    sleep "$WORKER_SLEEP"
  done
  printf '%s|%s\n' "$ok" "$err" > "$TMP_DIR/insert_worker.stats"
}

update_worker() {
  local table_name="$1"
  local ok=0
  local err=0
  local n=0
  local end_time=$(($(date +%s) + DML_DURATION))
  while [ "$(date +%s)" -lt "$end_time" ]; do
    local target=$((1000 + RANDOM % FILLER_ROWS))
    if "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -qtAX \
      -c "UPDATE ${table_name} SET body = 'upd-${n}' WHERE src_id = $target" \
      >/dev/null 2>&1; then
      ok=$((ok + 1))
    else
      err=$((err + 1))
    fi
    n=$((n + 1))
    sleep "$WORKER_SLEEP"
  done
  printf '%s|%s\n' "$ok" "$err" > "$TMP_DIR/update_worker.stats"
}

delete_worker() {
  local table_name="$1"
  local ok=0
  local err=0
  local end_time=$(($(date +%s) + DML_DURATION))
  while [ "$(date +%s)" -lt "$end_time" ]; do
    local target=$((40000 + RANDOM % 5000))
    if "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -qtAX \
      -c "DELETE FROM ${table_name} WHERE src_id = $target" \
      >/dev/null 2>&1; then
      ok=$((ok + 1))
    else
      err=$((err + 1))
    fi
    sleep "$WORKER_SLEEP"
  done
  printf '%s|%s\n' "$ok" "$err" > "$TMP_DIR/delete_worker.stats"
}

graph_worker() {
  local table_name="$1"
  local ok=0
  local err=0
  local end_time=$(($(date +%s) + DML_DURATION))
  while [ "$(date +%s)" -lt "$end_time" ]; do
    if "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -qtAX \
      -c "
        SELECT count(*)
        FROM sorted_heap_graph_rag(
          '${table_name}'::regclass,
          '[1,0,0,0]'::svec,
          relation_path := ARRAY[2],
          ann_k := ${GRAPH_ANN_K},
          top_k := ${GRAPH_TOP_K},
          score_mode := 'endpoint',
          limit_rows := 0
        )" \
      >/dev/null 2>&1; then
      ok=$((ok + 1))
    else
      err=$((err + 1))
    fi
    sleep "$WORKER_SLEEP"
  done
  printf '%s|%s\n' "$ok" "$err" > "$TMP_DIR/graph_worker.stats"
}

knn_worker() {
  local table_name="$1"
  local ok=0
  local err=0
  local end_time=$(($(date +%s) + DML_DURATION))
  while [ "$(date +%s)" -lt "$end_time" ]; do
    if "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -qtAX \
      -c "
        SELECT count(*)
        FROM (
          SELECT src_id
          FROM ${table_name}
          ORDER BY vec <=> '[1,0,0,0]'::svec
          LIMIT 2
        ) q" \
      >/dev/null 2>&1; then
      ok=$((ok + 1))
    else
      err=$((err + 1))
    fi
    sleep "$WORKER_SLEEP"
  done
  printf '%s|%s\n' "$ok" "$err" > "$TMP_DIR/knn_worker.stats"
}

worker_metric() {
  local name="$1" field="$2"
  local stats_file="$TMP_DIR/${name}.stats"
  if [ ! -f "$stats_file" ]; then
    echo "0"
    return
  fi
  IFS='|' read -r ok err < "$stats_file"
  if [ "$field" = "ok" ]; then
    echo "$ok"
  else
    echo "$err"
  fi
}

run_workers() {
  local table_name="$1"
  rm -f "$TMP_DIR"/*.stats
  WORKER_PIDS=()
  insert_worker "$table_name" &
  WORKER_PIDS+=($!)
  update_worker "$table_name" &
  WORKER_PIDS+=($!)
  delete_worker "$table_name" &
  WORKER_PIDS+=($!)
  graph_worker "$table_name" &
  WORKER_PIDS+=($!)
  knn_worker "$table_name" &
  WORKER_PIDS+=($!)
}

wait_workers() {
  for pid in "${WORKER_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
  WORKER_PIDS=()
}

verify_graph_table() {
  local phase="$1"
  local table_name="$2"
  local baseline_helper="$3"
  local baseline_cfg="$4"

  local count dups cfg registry_rows index_exists index_valid plan_ok
  local unified_sig helper_sig graph_api graph_seed graph_expanded graph_reranked graph_returned
  local insert_ok insert_err update_ok update_err delete_ok delete_err graph_ok graph_err knn_ok knn_err

  count=$(PSQL -c "SELECT count(*) FROM ${table_name}")
  dups=$(PSQL -c "
    SELECT count(*) FROM (
      SELECT src_id, edge_type, dst_id
      FROM ${table_name}
      GROUP BY src_id, edge_type, dst_id
      HAVING count(*) > 1
    ) dup")
  cfg=$(PSQL -c "$(graph_config_sql "$table_name")")
  registry_rows=$(PSQL -c "SELECT count(*) FROM sorted_heap_graph_registry")
  unified_sig=$(PSQL -c "$(graph_signature_sql "$table_name")")
  helper_sig=$(PSQL -c "$(helper_signature_sql "$table_name")")
  index_exists=$(PSQL -c "$(sorted_hnsw_index_exists_sql ${table_name}_vec_idx)")
  index_valid=$(PSQL -c "$(sorted_hnsw_index_valid_sql ${table_name}_vec_idx)")
  plan_ok=$(sorted_hnsw_plan_usable "$table_name")

  IFS='|' read -r graph_api graph_seed graph_expanded graph_reranked graph_returned < <(
    PSQL <<SQL | tail -n 1
SELECT sorted_heap_graph_rag_reset_stats();
SELECT count(*)
FROM sorted_heap_graph_rag(
  '${table_name}'::regclass,
  '[1,0,0,0]'::svec,
  relation_path := ARRAY[2],
  ann_k := ${GRAPH_ANN_K},
  top_k := ${GRAPH_TOP_K},
  score_mode := 'endpoint',
  limit_rows := 0
);
SELECT format(
  '%s|%s|%s|%s|%s',
  api,
  seed_count,
  expanded_rows,
  reranked_rows,
  returned_rows
)
FROM sorted_heap_graph_rag_stats();
SQL
  )

  insert_ok=$(worker_metric insert_worker ok)
  insert_err=$(worker_metric insert_worker err)
  update_ok=$(worker_metric update_worker ok)
  update_err=$(worker_metric update_worker err)
  delete_ok=$(worker_metric delete_worker ok)
  delete_err=$(worker_metric delete_worker err)
  graph_ok=$(worker_metric graph_worker ok)
  graph_err=$(worker_metric graph_worker err)
  knn_ok=$(worker_metric knn_worker ok)
  knn_err=$(worker_metric knn_worker err)

  check "${phase}_count_positive" "t" "$([ "$count" -gt 0 ] && echo t || echo f)"
  check "${phase}_no_duplicate_pk" "0" "$dups"
  check "${phase}_config_preserved" "$baseline_cfg" "$cfg"
  check "${phase}_registry_rows" "1" "$registry_rows"
  check "${phase}_unified_signature_nonempty" "t" "$([ -n "$unified_sig" ] && echo t || echo f)"
  check "${phase}_helper_signature" "$baseline_helper" "$helper_sig"
  check "${phase}_index_exists" "t" "$index_exists"
  check "${phase}_index_valid" "t" "$index_valid"
  check "${phase}_knn_plan_usable" "1" "$plan_ok"
  check "${phase}_graph_worker_ok" "t" "$([ "$graph_ok" -gt 0 ] && echo t || echo f)"
  check "${phase}_graph_worker_no_errors" "0" "$graph_err"
  check "${phase}_knn_worker_ok" "t" "$([ "$knn_ok" -gt 0 ] && echo t || echo f)"
  check "${phase}_knn_worker_no_errors" "0" "$knn_err"
  check "${phase}_stats_api" "sorted_heap_expand_rerank" "$graph_api"
  check "${phase}_stats_seed_positive" "t" "$([ "$graph_seed" -gt 0 ] && echo t || echo f)"
  check "${phase}_stats_expanded_positive" "t" "$([ "$graph_expanded" -gt 0 ] && echo t || echo f)"
  check "${phase}_stats_reranked_positive" "t" "$([ "$graph_reranked" -gt 0 ] && echo t || echo f)"
  check "${phase}_stats_returned_positive" "t" "$([ "$graph_returned" -gt 0 ] && echo t || echo f)"

  echo "  ${phase}: rows=$count ins=${insert_ok}/${insert_err} upd=${update_ok}/${update_err} del=${delete_ok}/${delete_err} graph=${graph_ok}/${graph_err} knn=${knn_ok}/${knn_err}"
}

echo "=== GraphRAG concurrent online operations test ==="

TABLE_NAME="graph_conc"
setup_graph "$TABLE_NAME"
PSQL -c "ALTER DATABASE postgres SET sorted_hnsw.ef_search = ${GRAPH_EF_SEARCH}" >/dev/null

baseline_cfg=$(PSQL -c "$(graph_config_sql "$TABLE_NAME")")
baseline_unified_sig=$(PSQL -c "$(graph_signature_sql "$TABLE_NAME")")
baseline_helper_sig=$(PSQL -c "$(helper_signature_sql "$TABLE_NAME")")

check "setup_config_registered" "src_id|edge_type|dst_id|vec|body|t" "$baseline_cfg"
check "setup_unified_signature_nonempty" "t" "$([ -n "$baseline_unified_sig" ] && echo t || echo f)"
check "setup_helper_signature_nonempty" "t" "$([ -n "$baseline_helper_sig" ] && echo t || echo f)"
check "setup_knn_plan_before_ops" "1" "$(sorted_hnsw_plan_usable "$TABLE_NAME")"

echo ""
echo "=== Test A: Online compact with concurrent DML + GraphRAG/KNN ==="
run_workers "$TABLE_NAME"
PSQL -c "CALL sorted_heap_compact_online('${TABLE_NAME}'::regclass)" >/dev/null
wait_workers
verify_graph_table "online_compact" "$TABLE_NAME" "$baseline_helper_sig" "$baseline_cfg"

echo ""
echo "=== Test B: Online merge with concurrent DML + GraphRAG/KNN ==="
PSQL -c "
  INSERT INTO ${TABLE_NAME}
  SELECT
    g,
    CASE WHEN (abs(g) % 2) = 0 THEN 1::int2 ELSE 2::int2 END,
    abs(g) + 500000,
    '[0,0,0,1]'::svec,
    repeat('m', 256)
  FROM generate_series(-5000, -1) g
  ON CONFLICT DO NOTHING
" >/dev/null
run_workers "$TABLE_NAME"
PSQL -c "CALL sorted_heap_merge_online('${TABLE_NAME}'::regclass)" >/dev/null
wait_workers
verify_graph_table "online_merge" "$TABLE_NAME" "$baseline_helper_sig" "$baseline_cfg"

echo ""
if [ "$fail" -eq 0 ]; then
  echo "graph_rag_concurrent_test status=ok pass=$pass fail=$fail total=$total"
else
  echo "graph_rag_concurrent_test status=FAIL pass=$pass fail=$fail total=$total"
  exit 1
fi
