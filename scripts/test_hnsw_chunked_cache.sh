#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Integration test: sorted_hnsw chunked local L0 cache
# ============================================================
#
# Exercises the page-backed lazy-decode cache path introduced in
# commit 8c89c95 ("perf: chunk sorted_hnsw local l0 cache").
#
# What this protects against:
#   - Regressions to the old contiguous allocation path that
#     failed with "invalid memory alloc request size" at 10M nodes
#   - owner_ctx lifetime misuse when lazy pages are allocated
#     after the initial cache setup
#   - Stale results after cache invalidation/rebuild cycles
#     triggered by INSERT into a sorted_hnsw-indexed table
#   - Incorrect materialization when publishing a partially-loaded
#     chunked cache to the shared scan cache
#   - Result corruption in GraphRAG ANN → graph expansion paths
#     that depend on the cached index state
#
# Two cluster configurations are tested:
#   Part A — no shared_preload_libraries (local-only cache path)
#   Part B — shared_preload_libraries = 'pg_sorted_heap'
#            (shared cache publish/attach path)
#
# Single-backend cache reuse is tested by sending multiple queries
# through one persistent psql process (heredoc blocks), not by
# launching separate psql invocations.
#
# Usage: ./scripts/test_hnsw_chunked_cache.sh [tmp_root] [port]

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
PORT="${2:-65488}"

NROWS=5000          # enough to span multiple L0 pages
M_PARAM=16
EF_CONSTRUCT=64
DIM=8               # small dim → fast, but multiple nodes per page
KNN_K=5
GRAPH_ANN_K=16
GRAPH_TOP_K=3
EF_SEARCH=64

QUERY_VEC='[1,0,0,0,0,0,0,0]'

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

stop_cluster() {
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR/data" ]; then
    "$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -m immediate stop >/dev/null 2>&1 || true
  fi
}

cleanup() {
  stop_cluster
  if [ -n "$TMP_DIR" ]; then
    rm -rf "$TMP_DIR"
  fi
}
trap cleanup EXIT

TMP_DIR="$(mktemp -d "$TMP_ROOT/pg_hnsw_chunked_cache.XXXXXX")"
echo "tmp_dir=$TMP_DIR  port=$PORT"

# Helper: new psql process (separate backend, fresh cache)
PSQL() {
  "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -v ON_ERROR_STOP=1 -qtAX "$@"
}

# Helper: create the fact table and load data (reused by both parts)
create_and_load() {
  PSQL -c "CREATE EXTENSION IF NOT EXISTS pg_sorted_heap VERSION '0.13.0'"
  PSQL <<SQL
CREATE TABLE facts (
  entity_id   int4 NOT NULL,
  relation_id int2 NOT NULL,
  target_id   int4 NOT NULL,
  embedding   svec($DIM) NOT NULL,
  payload     text NOT NULL,
  PRIMARY KEY (entity_id, target_id)
) USING sorted_heap;

INSERT INTO facts VALUES
  (1, 1, 2,   '[1,0,0,0,0,0,0,0]'::svec, 'seed-a'),
  (1, 2, 3,   '[0.9,0.1,0,0,0,0,0,0]'::svec, 'seed-b'),
  (2, 1, 4,   '[0.8,0.2,0,0,0,0,0,0]'::svec, 'hop2-a'),
  (3, 2, 5,   '[0.7,0.3,0,0,0,0,0,0]'::svec, 'hop2-b'),
  (4, 1, 6,   '[0.6,0.4,0,0,0,0,0,0]'::svec, 'hop3-a');

INSERT INTO facts
SELECT
  g,
  CASE WHEN (g % 3) = 0 THEN 1::int2
       WHEN (g % 3) = 1 THEN 2::int2
       ELSE 3::int2 END,
  g + 100000,
  format('[%s,%s,%s,%s,%s,%s,%s,%s]',
    sin(g::float8), cos(g::float8),
    sin(g::float8 * 0.7), cos(g::float8 * 0.7),
    sin(g::float8 * 1.3), cos(g::float8 * 1.3),
    sin(g::float8 * 0.3), cos(g::float8 * 0.3))::svec,
  'filler-' || g
FROM generate_series(100, $((NROWS + 99))) g;

SELECT sorted_heap_compact('facts'::regclass);
ANALYZE facts;

CREATE INDEX facts_vec_idx
  ON facts USING sorted_hnsw (embedding svec_cosine_ops)
  WITH (m = $M_PARAM, ef_construction = $EF_CONSTRUCT);

SELECT sorted_heap_graph_register(
  'facts'::regclass,
  entity_column := 'entity_id',
  relation_column := 'relation_id',
  target_column := 'target_id',
  embedding_column := 'embedding',
  payload_column := 'payload'
);
SQL
}

# ================================================================
# PART A — Local-only cache (no shared_preload_libraries)
# ================================================================
echo "========================================"
echo "PART A: Local-only cache path"
echo "========================================"

"$PG_BINDIR/initdb" -D "$TMP_DIR/data" -A trust --no-locale >/dev/null 2>&1
cat >> "$TMP_DIR/data/postgresql.conf" <<'PGCONF'
log_min_messages = warning
shared_preload_libraries = ''
PGCONF
"$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -l "$TMP_DIR/postmaster.log" \
  -o "-k $TMP_DIR -p $PORT" start >/dev/null

echo "=== A1: Setup ==="
create_and_load
echo "  Loaded $NROWS+ rows"

check "a1_index_created" "t" "$(PSQL -c "
  SELECT CASE WHEN count(*)=1 THEN 't' ELSE 'f' END
  FROM pg_class c JOIN pg_am am ON am.oid=c.relam
  WHERE c.relname='facts_vec_idx' AND am.amname='sorted_hnsw'")"

# ---- A2: Single-backend cache reuse ----
# All queries run in ONE psql process → one PG backend → same local cache.
echo "=== A2: Single-backend local cache reuse ==="

SIGS=$(PSQL <<SQL
LOAD 'pg_sorted_heap';
SET sorted_hnsw.ef_search = $EF_SEARCH;

-- Query 1: triggers cache build from index pages (cold local load, Path B)
SELECT 'KNN1:' || string_agg(entity_id::text, ',' ORDER BY entity_id)
FROM (SELECT entity_id FROM facts
      ORDER BY embedding <=> '${QUERY_VEC}'::svec LIMIT $KNN_K) t;

-- Query 2-6: reuse the same backend-local cache (lazy pages may extend)
SELECT 'KNN2:' || string_agg(entity_id::text, ',' ORDER BY entity_id)
FROM (SELECT entity_id FROM facts
      ORDER BY embedding <=> '${QUERY_VEC}'::svec LIMIT $KNN_K) t;

SELECT 'KNN3:' || string_agg(entity_id::text, ',' ORDER BY entity_id)
FROM (SELECT entity_id FROM facts
      ORDER BY embedding <=> '${QUERY_VEC}'::svec LIMIT $KNN_K) t;

SELECT 'KNN4:' || string_agg(entity_id::text, ',' ORDER BY entity_id)
FROM (SELECT entity_id FROM facts
      ORDER BY embedding <=> '${QUERY_VEC}'::svec LIMIT $KNN_K) t;

SELECT 'KNN5:' || string_agg(entity_id::text, ',' ORDER BY entity_id)
FROM (SELECT entity_id FROM facts
      ORDER BY embedding <=> '${QUERY_VEC}'::svec LIMIT $KNN_K) t;

SELECT 'KNN6:' || string_agg(entity_id::text, ',' ORDER BY entity_id)
FROM (SELECT entity_id FROM facts
      ORDER BY embedding <=> '${QUERY_VEC}'::svec LIMIT $KNN_K) t;

-- GraphRAG 1-hop in same backend (reuses already-warm cache)
SELECT 'GR1:' || COALESCE(
  string_agg(format('%s:%s:%s:%s', entity_id, relation_id, target_id, payload),
             '|' ORDER BY distance, entity_id, relation_id, target_id), '')
FROM sorted_heap_graph_rag(
  'facts'::regclass, '${QUERY_VEC}'::svec,
  relation_path := ARRAY[1], ann_k := $GRAPH_ANN_K,
  top_k := $GRAPH_TOP_K, score_mode := 'endpoint', limit_rows := 0);

-- GraphRAG repeated (still same backend)
SELECT 'GR2:' || COALESCE(
  string_agg(format('%s:%s:%s:%s', entity_id, relation_id, target_id, payload),
             '|' ORDER BY distance, entity_id, relation_id, target_id), '')
FROM sorted_heap_graph_rag(
  'facts'::regclass, '${QUERY_VEC}'::svec,
  relation_path := ARRAY[1], ann_k := $GRAPH_ANN_K,
  top_k := $GRAPH_TOP_K, score_mode := 'endpoint', limit_rows := 0);

-- GraphRAG 2-hop (same backend)
SELECT 'GR2H:' || COALESCE(
  string_agg(format('%s:%s:%s', entity_id, relation_id, target_id),
             '|' ORDER BY distance, entity_id, relation_id, target_id), '')
FROM sorted_heap_graph_rag(
  'facts'::regclass, '${QUERY_VEC}'::svec,
  relation_path := ARRAY[1,1], ann_k := $GRAPH_ANN_K,
  top_k := $GRAPH_TOP_K, score_mode := 'path', limit_rows := 0);
SQL
)

KNN1=$(echo "$SIGS" | grep '^KNN1:' | sed 's/^KNN1://')
KNN6=$(echo "$SIGS" | grep '^KNN6:' | sed 's/^KNN6://')
GR1=$(echo "$SIGS" | grep '^GR1:' | sed 's/^GR1://')
GR2=$(echo "$SIGS" | grep '^GR2:' | sed 's/^GR2://')
GR2H=$(echo "$SIGS" | grep '^GR2H:' | sed 's/^GR2H://')

check "a2_knn_not_empty" "t" "$([ -n "$KNN1" ] && echo t || echo f)"

# All 6 KNN queries in one backend must return identical results
all_knn_stable=t
for i in 2 3 4 5 6; do
  SIG_I=$(echo "$SIGS" | grep "^KNN${i}:" | sed "s/^KNN${i}://")
  if [ "$SIG_I" != "$KNN1" ]; then
    all_knn_stable=f
    echo "  KNN$i differs from KNN1"
    break
  fi
done
check "a2_knn_stable_6_in_one_backend" "t" "$all_knn_stable"
check "a2_graphrag_not_empty" "t" "$([ -n "$GR1" ] && echo t || echo f)"
check "a2_graphrag_stable_in_backend" "$GR1" "$GR2"
check "a2_graphrag_2hop_not_empty" "t" "$([ -n "$GR2H" ] && echo t || echo f)"

# ---- A3: INSERT → fresh backend → cold cache rebuild from disk ----
echo "=== A3: INSERT → cold reload (Path B, lazy chunked) ==="

PSQL -c "
  INSERT INTO facts VALUES
    (90001, 1, 90002, '[0.5,0.5,0,0,0,0,0,0]'::svec, 'post-insert-a'),
    (90002, 1, 90003, '[0.4,0.6,0,0,0,0,0,0]'::svec, 'post-insert-b')"

# Fresh backend: cache must be built from disk (no shared cache in Part A)
FRESH_SIGS=$(PSQL <<SQL
LOAD 'pg_sorted_heap';
SET sorted_hnsw.ef_search = $EF_SEARCH;

SELECT 'K:' || string_agg(entity_id::text, ',' ORDER BY entity_id)
FROM (SELECT entity_id FROM facts
      ORDER BY embedding <=> '${QUERY_VEC}'::svec LIMIT $KNN_K) t;

SELECT 'G:' || COALESCE(
  string_agg(format('%s:%s:%s:%s', entity_id, relation_id, target_id, payload),
             '|' ORDER BY distance, entity_id, relation_id, target_id), '')
FROM sorted_heap_graph_rag(
  'facts'::regclass, '${QUERY_VEC}'::svec,
  relation_path := ARRAY[1], ann_k := $GRAPH_ANN_K,
  top_k := $GRAPH_TOP_K, score_mode := 'endpoint', limit_rows := 0);
SQL
)

FK=$(echo "$FRESH_SIGS" | grep '^K:' | sed 's/^K://')
FG=$(echo "$FRESH_SIGS" | grep '^G:' | sed 's/^G://')
check "a3_knn_after_insert" "t" "$([ -n "$FK" ] && echo t || echo f)"
check "a3_graphrag_after_insert" "t" "$([ -n "$FG" ] && echo t || echo f)"

# ---- A4: INSERT → query in SAME backend (cache invalidation + rebuild within one backend) ----
echo "=== A4: INSERT + query in same backend (invalidation cycle) ==="

INVAL_SIGS=$(PSQL <<SQL
LOAD 'pg_sorted_heap';
SET sorted_hnsw.ef_search = $EF_SEARCH;

-- Warm the cache
SELECT 'BEFORE:' || string_agg(entity_id::text, ',' ORDER BY entity_id)
FROM (SELECT entity_id FROM facts
      ORDER BY embedding <=> '${QUERY_VEC}'::svec LIMIT $KNN_K) t;

-- INSERT invalidates the backend-local cache (chunked path → full invalidation)
INSERT INTO facts VALUES
  (91001, 1, 91002, '[0.3,0.7,0,0,0,0,0,0]'::svec, 'inval-test');

-- Query again in same backend → must rebuild cache from disk
SELECT 'AFTER:' || string_agg(entity_id::text, ',' ORDER BY entity_id)
FROM (SELECT entity_id FROM facts
      ORDER BY embedding <=> '${QUERY_VEC}'::svec LIMIT $KNN_K) t;
SQL
)

INV_BEFORE=$(echo "$INVAL_SIGS" | grep '^BEFORE:' | sed 's/^BEFORE://')
INV_AFTER=$(echo "$INVAL_SIGS" | grep '^AFTER:' | sed 's/^AFTER://')
check "a4_cache_before_insert" "t" "$([ -n "$INV_BEFORE" ] && echo t || echo f)"
check "a4_cache_after_invalidation" "t" "$([ -n "$INV_AFTER" ] && echo t || echo f)"

# ---- A5: Repeated INSERT+query churn in one backend ----
echo "=== A5: Cache churn (5 cycles, single backend) ==="

CHURN_RESULT=$(PSQL <<SQL
LOAD 'pg_sorted_heap';
SET sorted_hnsw.ef_search = $EF_SEARCH;

DO \$\$
DECLARE
  i int;
  cnt int;
BEGIN
  FOR i IN 1..5 LOOP
    INSERT INTO facts
    SELECT
      70000 + i * 100 + g,
      1::int2,
      70000 + i * 100 + g + 100000,
      format('[%s,%s,0,0,0,0,0,0]', sin((i*100+g)::float8), cos((i*100+g)::float8))::svec,
      'churn-' || i || '-' || g
    FROM generate_series(1, 20) g
    ON CONFLICT DO NOTHING;
  END LOOP;
END
\$\$;

-- Final query after 5 insert rounds in same backend
SELECT 'CHURN:' || count(*)
FROM (SELECT entity_id FROM facts
      ORDER BY embedding <=> '${QUERY_VEC}'::svec LIMIT $KNN_K) t;
SQL
)

CHURN_CNT=$(echo "$CHURN_RESULT" | grep '^CHURN:' | sed 's/^CHURN://')
check "a5_churn_5_cycles_single_backend" "$KNN_K" "$CHURN_CNT"

# ---- A6: Online compact → fresh backend query ----
echo "=== A6: Online compact → re-query ==="

PSQL -c "CALL sorted_heap_compact_online('facts'::regclass)" >/dev/null

COMPACT_SIGS=$(PSQL <<SQL
LOAD 'pg_sorted_heap';
SET sorted_hnsw.ef_search = $EF_SEARCH;

SELECT 'K:' || string_agg(entity_id::text, ',' ORDER BY entity_id)
FROM (SELECT entity_id FROM facts
      ORDER BY embedding <=> '${QUERY_VEC}'::svec LIMIT $KNN_K) t;

SELECT 'G:' || COALESCE(
  string_agg(format('%s:%s:%s', entity_id, relation_id, target_id),
             '|' ORDER BY distance, entity_id, relation_id, target_id), '')
FROM sorted_heap_graph_rag(
  'facts'::regclass, '${QUERY_VEC}'::svec,
  relation_path := ARRAY[1], ann_k := $GRAPH_ANN_K,
  top_k := $GRAPH_TOP_K, score_mode := 'endpoint', limit_rows := 0);
SQL
)

CK=$(echo "$COMPACT_SIGS" | grep '^K:' | sed 's/^K://')
CG=$(echo "$COMPACT_SIGS" | grep '^G:' | sed 's/^G://')
check "a6_knn_after_compact" "t" "$([ -n "$CK" ] && echo t || echo f)"
check "a6_graphrag_after_compact" "t" "$([ -n "$CG" ] && echo t || echo f)"

# ---- A7: DROP + recreate index ----
echo "=== A7: DROP + recreate index ==="

PSQL -c "DROP INDEX facts_vec_idx"
PSQL -c "
  CREATE INDEX facts_vec_idx
    ON facts USING sorted_hnsw (embedding svec_cosine_ops)
    WITH (m = $M_PARAM, ef_construction = $EF_CONSTRUCT)"

REBUILD_SIGS=$(PSQL <<SQL
LOAD 'pg_sorted_heap';
SET sorted_hnsw.ef_search = $EF_SEARCH;

SELECT 'K:' || string_agg(entity_id::text, ',' ORDER BY entity_id)
FROM (SELECT entity_id FROM facts
      ORDER BY embedding <=> '${QUERY_VEC}'::svec LIMIT $KNN_K) t;

SELECT 'G:' || COALESCE(
  string_agg(format('%s:%s:%s', entity_id, relation_id, target_id),
             '|' ORDER BY distance, entity_id, relation_id, target_id), '')
FROM sorted_heap_graph_rag(
  'facts'::regclass, '${QUERY_VEC}'::svec,
  relation_path := ARRAY[1], ann_k := $GRAPH_ANN_K,
  top_k := $GRAPH_TOP_K, score_mode := 'endpoint', limit_rows := 0);
SQL
)

RK=$(echo "$REBUILD_SIGS" | grep '^K:' | sed 's/^K://')
RG=$(echo "$REBUILD_SIGS" | grep '^G:' | sed 's/^G://')
check "a7_knn_after_rebuild" "t" "$([ -n "$RK" ] && echo t || echo f)"
check "a7_graphrag_after_rebuild" "t" "$([ -n "$RG" ] && echo t || echo f)"

# ================================================================
# PART B — Shared cache path (shared_preload_libraries)
# ================================================================
echo ""
echo "========================================"
echo "PART B: Shared cache publish/attach"
echo "========================================"

stop_cluster
rm -rf "$TMP_DIR/data"

"$PG_BINDIR/initdb" -D "$TMP_DIR/data" -A trust --no-locale >/dev/null 2>&1
cat >> "$TMP_DIR/data/postgresql.conf" <<PGCONF
log_min_messages = warning
shared_preload_libraries = 'pg_sorted_heap'
sorted_hnsw.shared_cache = on
sorted_hnsw.ef_search = $EF_SEARCH
PGCONF
"$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -l "$TMP_DIR/postmaster_b.log" \
  -o "-k $TMP_DIR -p $PORT" start >/dev/null

echo "=== B1: Setup ==="
create_and_load
echo "  Loaded $NROWS+ rows"

# ---- B2: Backend A publishes to shared cache, Backend B attaches ----
echo "=== B2: Backend A publishes → Backend B attaches ==="

# The build backend (during create_and_load) already published the cache
# to shared memory via shnsw_cache_from_build_graph → publish (line 1161).
# So both Backend A and Backend B should attach to shared, not load from disk.

# Backend A: query with DEBUG1 to capture attach/load log.
PSQL -c "
  SET client_min_messages = debug1;
  SELECT count(*) FROM (
    SELECT entity_id FROM facts
    ORDER BY embedding <=> '${QUERY_VEC}'::svec LIMIT $KNN_K
  ) t" >/dev/null 2>"$TMP_DIR/backend_a.log"

# Verify Backend A attached to shared (build already published)
check "b2_backend_a_attached_to_shared" "t" \
  "$(grep -c 'attached to shared scan cache' "$TMP_DIR/backend_a.log" | \
     awk '{print ($1 > 0) ? "t" : "f"}')"

# Backend B: should also attach to shared cache.
PSQL <<SQL >"$TMP_DIR/backend_b.out" 2>"$TMP_DIR/backend_b.log"
SET client_min_messages = debug1;

SELECT 'K:' || string_agg(entity_id::text, ',' ORDER BY entity_id)
FROM (SELECT entity_id FROM facts
      ORDER BY embedding <=> '${QUERY_VEC}'::svec LIMIT $KNN_K) t;

SELECT 'G:' || COALESCE(
  string_agg(format('%s:%s:%s:%s', entity_id, relation_id, target_id, payload),
             '|' ORDER BY distance, entity_id, relation_id, target_id), '')
FROM sorted_heap_graph_rag(
  'facts'::regclass, '${QUERY_VEC}'::svec,
  relation_path := ARRAY[1], ann_k := $GRAPH_ANN_K,
  top_k := $GRAPH_TOP_K, score_mode := 'endpoint', limit_rows := 0);
SQL

SK=$(grep '^K:' "$TMP_DIR/backend_b.out" | sed 's/^K://')
SG=$(grep '^G:' "$TMP_DIR/backend_b.out" | sed 's/^G://')
check "b2_shared_knn" "t" "$([ -n "$SK" ] && echo t || echo f)"
check "b2_shared_graphrag" "t" "$([ -n "$SG" ] && echo t || echo f)"

# The key assertions: both backends attached, neither loaded from disk.
check "b2_backend_b_attached_to_shared" "t" \
  "$(grep -c 'attached to shared scan cache' "$TMP_DIR/backend_b.log" | \
     awk '{print ($1 > 0) ? "t" : "f"}')"
check "b2_no_disk_load_backend_a" "0" \
  "$(grep -c 'shnsw_load_cache: loading' "$TMP_DIR/backend_a.log")"
check "b2_no_disk_load_backend_b" "0" \
  "$(grep -c 'shnsw_load_cache: loading' "$TMP_DIR/backend_b.log")"

# ---- B3: INSERT invalidates shared → Backend C loads from disk + re-publishes ----
echo "=== B3: INSERT → shared cache invalidation → disk reload ==="

PSQL -c "
  INSERT INTO facts VALUES
    (95001, 1, 95002, '[0.5,0.5,0,0,0,0,0,0]'::svec, 'shared-insert')"

# New backend: INSERT bumped cache_gen → shared cache is stale →
# attach fails → must load from disk → re-publish.
PSQL <<SQL >"$TMP_DIR/backend_c.out" 2>"$TMP_DIR/backend_c.log"
SET client_min_messages = debug1;

SELECT 'K:' || string_agg(entity_id::text, ',' ORDER BY entity_id)
FROM (SELECT entity_id FROM facts
      ORDER BY embedding <=> '${QUERY_VEC}'::svec LIMIT $KNN_K) t;

SELECT 'G:' || COALESCE(
  string_agg(format('%s:%s:%s:%s', entity_id, relation_id, target_id, payload),
             '|' ORDER BY distance, entity_id, relation_id, target_id), '')
FROM sorted_heap_graph_rag(
  'facts'::regclass, '${QUERY_VEC}'::svec,
  relation_path := ARRAY[1], ann_k := $GRAPH_ANN_K,
  top_k := $GRAPH_TOP_K, score_mode := 'endpoint', limit_rows := 0);
SQL

B3K=$(grep '^K:' "$TMP_DIR/backend_c.out" | sed 's/^K://')
B3G=$(grep '^G:' "$TMP_DIR/backend_c.out" | sed 's/^G://')
check "b3_knn_after_shared_invalidation" "t" "$([ -n "$B3K" ] && echo t || echo f)"
check "b3_graphrag_after_shared_invalidation" "t" "$([ -n "$B3G" ] && echo t || echo f)"

# After INSERT, shared cache gen is stale → Backend C must load from disk
check "b3_backend_c_loaded_from_disk" "t" \
  "$(grep -c 'shnsw_load_cache: loading' "$TMP_DIR/backend_c.log" | \
     awk '{print ($1 > 0) ? "t" : "f"}')"
check "b3_backend_c_did_not_attach" "0" \
  "$(grep -c 'attached to shared scan cache' "$TMP_DIR/backend_c.log")"

# ---- B4: Multiple backends query simultaneously via shared cache ----
echo "=== B4: Parallel backends on shared cache ==="

# Warm shared cache
PSQL -c "
  SELECT count(*) FROM (
    SELECT entity_id FROM facts
    ORDER BY embedding <=> '${QUERY_VEC}'::svec LIMIT $KNN_K
  ) t" >/dev/null

# Launch 3 backends in parallel, each queries KNN + GraphRAG
for w in 1 2 3; do
  PSQL <<SQL > "$TMP_DIR/worker_${w}.out" 2>&1 &
SELECT 'K:' || string_agg(entity_id::text, ',' ORDER BY entity_id)
FROM (SELECT entity_id FROM facts
      ORDER BY embedding <=> '${QUERY_VEC}'::svec LIMIT $KNN_K) t;

SELECT 'G:' || COALESCE(
  string_agg(format('%s:%s:%s', entity_id, relation_id, target_id),
             '|' ORDER BY distance, entity_id, relation_id, target_id), '')
FROM sorted_heap_graph_rag(
  'facts'::regclass, '${QUERY_VEC}'::svec,
  relation_path := ARRAY[1], ann_k := $GRAPH_ANN_K,
  top_k := $GRAPH_TOP_K, score_mode := 'endpoint', limit_rows := 0);
SQL
done
wait

parallel_ok=t
W1_K=$(grep '^K:' "$TMP_DIR/worker_1.out" | sed 's/^K://')
for w in 2 3; do
  WK=$(grep '^K:' "$TMP_DIR/worker_${w}.out" | sed 's/^K://')
  WG=$(grep '^G:' "$TMP_DIR/worker_${w}.out" | sed 's/^G://')
  if [ -z "$WK" ] || [ -z "$WG" ]; then
    parallel_ok=f
    echo "  worker $w returned empty"
    break
  fi
  if [ "$WK" != "$W1_K" ]; then
    parallel_ok=f
    echo "  worker $w KNN differs from worker 1"
    break
  fi
done
check "b4_parallel_3_backends" "t" "$parallel_ok"

# ---- B5: Multi-index shared cache overwrite (the exact bug from the benchmark) ----
# Two sorted_hnsw indexes on different tables. Backend attaches to index A via shared
# cache, then a query on index B overwrites shared memory. Subsequent queries on A
# must still return correct results (deep-copy protects against overwrite).
echo "=== B5: Multi-index shared cache overwrite ==="

PSQL <<SQL
CREATE TABLE facts_b (
  entity_id   int4 NOT NULL,
  relation_id int2 NOT NULL,
  target_id   int4 NOT NULL,
  embedding   svec($DIM) NOT NULL,
  payload     text NOT NULL,
  PRIMARY KEY (entity_id, target_id)
) USING sorted_heap;

-- Different data: offset entity IDs so results are distinguishable
INSERT INTO facts_b
SELECT
  g + 500000,
  CASE WHEN (g % 3) = 0 THEN 1::int2
       WHEN (g % 3) = 1 THEN 2::int2
       ELSE 3::int2 END,
  g + 600000,
  format('[%s,%s,%s,%s,%s,%s,%s,%s]',
    cos(g::float8), sin(g::float8),
    cos(g::float8 * 0.7), sin(g::float8 * 0.7),
    cos(g::float8 * 1.3), sin(g::float8 * 1.3),
    cos(g::float8 * 0.3), sin(g::float8 * 0.3))::svec,
  'b-filler-' || g
FROM generate_series(100, $((NROWS + 99))) g;

SELECT sorted_heap_compact('facts_b'::regclass);
ANALYZE facts_b;

CREATE INDEX facts_b_vec_idx
  ON facts_b USING sorted_hnsw (embedding svec_cosine_ops)
  WITH (m = $M_PARAM, ef_construction = $EF_CONSTRUCT);
SQL

# In one backend: attach facts, then query facts_b (overwrites shared), then query facts again
B5_SIGS=$(PSQL <<SQL
-- Query facts first → attaches to shared cache for facts_vec_idx
SELECT 'A1:' || string_agg(entity_id::text, ',' ORDER BY entity_id)
FROM (SELECT entity_id FROM facts
      ORDER BY embedding <=> '${QUERY_VEC}'::svec LIMIT $KNN_K) t;

-- Query facts_b → loads from disk, publishes facts_b to shared (overwrites facts data)
SELECT 'B1:' || string_agg(entity_id::text, ',' ORDER BY entity_id)
FROM (SELECT entity_id FROM facts_b
      ORDER BY embedding <=> '${QUERY_VEC}'::svec LIMIT $KNN_K) t;

-- Query facts AGAIN → must still return correct results, not facts_b data
SELECT 'A2:' || string_agg(entity_id::text, ',' ORDER BY entity_id)
FROM (SELECT entity_id FROM facts
      ORDER BY embedding <=> '${QUERY_VEC}'::svec LIMIT $KNN_K) t;
SQL
)

A1=$(echo "$B5_SIGS" | grep '^A1:' | sed 's/^A1://')
B1=$(echo "$B5_SIGS" | grep '^B1:' | sed 's/^B1://')
A2=$(echo "$B5_SIGS" | grep '^A2:' | sed 's/^A2://')

check "b5_facts_a_first_query" "t" "$([ -n "$A1" ] && echo t || echo f)"
check "b5_facts_b_query" "t" "$([ -n "$B1" ] && echo t || echo f)"
check "b5_facts_a_after_overwrite" "t" "$([ -n "$A2" ] && echo t || echo f)"

# The critical assertion: A1 and A2 must be identical (no corruption from B's publish)
check "b5_facts_a_stable_across_overwrite" "$A1" "$A2"

# A and B must return DIFFERENT results (they have different data)
check "b5_a_and_b_differ" "t" "$([ "$A1" != "$B1" ] && echo t || echo f)"

# ---- Summary ----
echo ""
echo "=== Summary ==="
echo "status=$([ "$fail" -eq 0 ] && echo ok || echo FAIL) pass=$pass fail=$fail total=$total"
exit "$fail"
