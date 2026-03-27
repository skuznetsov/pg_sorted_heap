#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Reproducer: shared_cache=on vs off divergence in GraphRAG multihop
# ============================================================
#
# Builds a deterministic 2-hop fact graph (person→parent→city),
# then compares KNN seeds, graph expansion, and full GraphRAG
# results between shared_cache=on and shared_cache=off.
#
# Reports divergence at each stage to localize the root cause.
#
# Usage: ./scripts/repro_shared_cache_multihop.sh [tmp_root] [port]

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
PORT="${2:-65487}"

NROWS="${REPRO_NROWS:-2000}"
DIM="${REPRO_DIM:-32}"
M_PARAM="${REPRO_M:-16}"
EF_CONSTRUCT="${REPRO_EF_CONSTRUCT:-64}"
EF_SEARCH="${REPRO_EF_SEARCH:-64}"
ANN_K="${REPRO_ANN_K:-32}"
TOP_K="${REPRO_TOP_K:-10}"
NQUERIES="${REPRO_NQUERIES:-20}"
SEED="${REPRO_SEED:-42}"

if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root must be absolute: $TMP_ROOT" >&2
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
divergences=0

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

TMP_DIR="$(mktemp -d "$TMP_ROOT/pg_shared_cache_repro.XXXXXX")"
echo "tmp_dir=$TMP_DIR  port=$PORT"

# Start cluster WITH shared_preload_libraries (so shared cache structures exist)
"$PG_BINDIR/initdb" -D "$TMP_DIR/data" -A trust --no-locale >/dev/null 2>&1
cat >> "$TMP_DIR/data/postgresql.conf" <<PGCONF
log_min_messages = warning
shared_preload_libraries = 'pg_sorted_heap'
sorted_hnsw.ef_search = $EF_SEARCH
PGCONF
"$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -l "$TMP_DIR/postmaster.log" \
  -o "-k $TMP_DIR -p $PORT" start >/dev/null

PSQL() {
  "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -v ON_ERROR_STOP=1 -qtAX "$@"
}

# ---- Setup: deterministic 2-hop fact graph ----
echo "=== Setup: 2-hop fact graph ($NROWS pairs, dim=$DIM) ==="

PSQL -c "CREATE EXTENSION pg_sorted_heap VERSION '0.13.0'"

# Generate deterministic data: person→parent (rel 1), parent→city (rel 2)
# Vectors are sin/cos hashes of entity IDs for reproducibility
PSQL <<SQL
CREATE TABLE facts (
  entity_id   int4 NOT NULL,
  relation_id int2 NOT NULL,
  target_id   int4 NOT NULL,
  embedding   svec($DIM) NOT NULL,
  payload     text NOT NULL,
  PRIMARY KEY (entity_id, target_id)
) USING sorted_heap;

-- person_i → parent_i (relation 1)
INSERT INTO facts
SELECT
  g AS entity_id,
  1::int2 AS relation_id,
  g + $NROWS AS target_id,
  (SELECT format('[%s]', string_agg(
    (sin(g::float8 * (s + 1) * 0.37) / sqrt($DIM::float8))::text, ','))
   FROM generate_series(1, $DIM) s)::svec AS embedding,
  'person_' || g || ' parent_' || (g + $NROWS) AS payload
FROM generate_series(1, $NROWS) g;

-- parent_i → city_(i%100) (relation 2)
INSERT INTO facts
SELECT
  g + $NROWS AS entity_id,
  2::int2 AS relation_id,
  (g % 100) + 2 * $NROWS AS target_id,
  (SELECT format('[%s]', string_agg(
    (cos(g::float8 * (s + 1) * 0.53) / sqrt($DIM::float8))::text, ','))
   FROM generate_series(1, $DIM) s)::svec AS embedding,
  'parent_' || (g + $NROWS) || ' city_' || ((g % 100) + 2 * $NROWS) AS payload
FROM generate_series(1, $NROWS) g;

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

echo "  Loaded $((NROWS * 2)) facts, index built"

# Generate deterministic query vectors (same sin/cos hash as person entities)
# Query for person_i: "where does person_i's parent live?"
# Expected answer: city_(i%100 + 2*NROWS)
PSQL <<SQL
CREATE TABLE queries AS
SELECT
  g AS person_id,
  g + $NROWS AS parent_id,
  (g % 100) + 2 * $NROWS AS expected_city_id,
  (SELECT format('[%s]', string_agg(
    (sin(g::float8 * (s + 1) * 0.37) / sqrt($DIM::float8))::text, ','))
   FROM generate_series(1, $DIM) s) AS query_vec
FROM (
  SELECT g FROM generate_series(1, $NROWS) g
  ORDER BY hashint4(g + $SEED)
  LIMIT $NQUERIES
) sub;
SQL

echo "  Generated $NQUERIES queries"

# ============================================================
# Stage 1: Direct KNN comparison (shared_cache off vs on)
# ============================================================
echo ""
echo "=== Stage 1: Direct KNN seeds ==="

# shared_cache=off: new backend, disable shared cache
PSQL <<SQL > "$TMP_DIR/knn_off.txt"
SET sorted_hnsw.shared_cache = off;

SELECT person_id, string_agg(entity_id::text, ',' ORDER BY entity_id) AS seeds
FROM queries q,
LATERAL (
  SELECT entity_id FROM facts
  ORDER BY embedding <=> q.query_vec::svec
  LIMIT $ANN_K
) ann
GROUP BY person_id
ORDER BY person_id;
SQL

# shared_cache=on: new backend, enable shared cache
PSQL <<SQL > "$TMP_DIR/knn_on.txt"
SET sorted_hnsw.shared_cache = on;

SELECT person_id, string_agg(entity_id::text, ',' ORDER BY entity_id) AS seeds
FROM queries q,
LATERAL (
  SELECT entity_id FROM facts
  ORDER BY embedding <=> q.query_vec::svec
  LIMIT $ANN_K
) ann
GROUP BY person_id
ORDER BY person_id;
SQL

KNN_DIFF=$(diff "$TMP_DIR/knn_off.txt" "$TMP_DIR/knn_on.txt" | head -20 || true)
if [ -z "$KNN_DIFF" ]; then
  echo "  KNN seeds: IDENTICAL between shared_cache=off and on"
  check "stage1_knn_seeds_match" "t" "t"
else
  echo "  KNN seeds: DIVERGED"
  echo "$KNN_DIFF"
  divergences=$((divergences + 1))
  check "stage1_knn_seeds_match" "t" "f"
fi

# ============================================================
# Stage 2: GraphRAG 1-hop comparison
# ============================================================
echo ""
echo "=== Stage 2: GraphRAG 1-hop ==="

PSQL <<SQL > "$TMP_DIR/gr1_off.txt"
SET sorted_hnsw.shared_cache = off;

SELECT person_id,
  COALESCE(string_agg(
    format('%s:%s:%s', entity_id, relation_id, target_id),
    '|' ORDER BY distance, entity_id, relation_id, target_id), '') AS results
FROM queries q,
LATERAL sorted_heap_graph_rag(
  'facts'::regclass, q.query_vec::svec,
  relation_path := ARRAY[1], ann_k := $ANN_K,
  top_k := $TOP_K, score_mode := 'endpoint', limit_rows := 0
) gr
GROUP BY person_id
ORDER BY person_id;
SQL

PSQL <<SQL > "$TMP_DIR/gr1_on.txt"
SET sorted_hnsw.shared_cache = on;

SELECT person_id,
  COALESCE(string_agg(
    format('%s:%s:%s', entity_id, relation_id, target_id),
    '|' ORDER BY distance, entity_id, relation_id, target_id), '') AS results
FROM queries q,
LATERAL sorted_heap_graph_rag(
  'facts'::regclass, q.query_vec::svec,
  relation_path := ARRAY[1], ann_k := $ANN_K,
  top_k := $TOP_K, score_mode := 'endpoint', limit_rows := 0
) gr
GROUP BY person_id
ORDER BY person_id;
SQL

GR1_DIFF=$(diff "$TMP_DIR/gr1_off.txt" "$TMP_DIR/gr1_on.txt" | head -20 || true)
if [ -z "$GR1_DIFF" ]; then
  echo "  GraphRAG 1-hop: IDENTICAL"
  check "stage2_graphrag_1hop_match" "t" "t"
else
  echo "  GraphRAG 1-hop: DIVERGED"
  echo "$GR1_DIFF"
  divergences=$((divergences + 1))
  check "stage2_graphrag_1hop_match" "t" "f"
fi

# ============================================================
# Stage 3: GraphRAG 2-hop (the multihop path)
# ============================================================
echo ""
echo "=== Stage 3: GraphRAG 2-hop multihop ==="

PSQL <<SQL > "$TMP_DIR/gr2_off.txt"
SET sorted_hnsw.shared_cache = off;

SELECT person_id,
  COALESCE(string_agg(
    format('%s:%s:%s', entity_id, relation_id, target_id),
    '|' ORDER BY distance, entity_id, relation_id, target_id), '') AS results
FROM queries q,
LATERAL sorted_heap_graph_rag(
  'facts'::regclass, q.query_vec::svec,
  relation_path := ARRAY[1,2], ann_k := $ANN_K,
  top_k := $TOP_K, score_mode := 'path', limit_rows := 0
) gr
GROUP BY person_id
ORDER BY person_id;
SQL

PSQL <<SQL > "$TMP_DIR/gr2_on.txt"
SET sorted_hnsw.shared_cache = on;

SELECT person_id,
  COALESCE(string_agg(
    format('%s:%s:%s', entity_id, relation_id, target_id),
    '|' ORDER BY distance, entity_id, relation_id, target_id), '') AS results
FROM queries q,
LATERAL sorted_heap_graph_rag(
  'facts'::regclass, q.query_vec::svec,
  relation_path := ARRAY[1,2], ann_k := $ANN_K,
  top_k := $TOP_K, score_mode := 'path', limit_rows := 0
) gr
GROUP BY person_id
ORDER BY person_id;
SQL

GR2_DIFF=$(diff "$TMP_DIR/gr2_off.txt" "$TMP_DIR/gr2_on.txt" | head -20 || true)
if [ -z "$GR2_DIFF" ]; then
  echo "  GraphRAG 2-hop: IDENTICAL"
  check "stage3_graphrag_2hop_match" "t" "t"
else
  echo "  GraphRAG 2-hop: DIVERGED"
  echo "$GR2_DIFF"
  divergences=$((divergences + 1))
  check "stage3_graphrag_2hop_match" "t" "f"
fi

# ============================================================
# Stage 4: Quality comparison (hit@1, hit@k)
# ============================================================
echo ""
echo "=== Stage 4: Quality metrics ==="

for mode in off on; do
  PSQL <<SQL > "$TMP_DIR/quality_${mode}.txt"
SET sorted_hnsw.shared_cache = ${mode};

WITH results AS (
  SELECT q.person_id, q.expected_city_id,
    array_agg(gr.target_id ORDER BY gr.distance, gr.entity_id) AS targets
  FROM queries q,
  LATERAL sorted_heap_graph_rag(
    'facts'::regclass, q.query_vec::svec,
    relation_path := ARRAY[1,2], ann_k := $ANN_K,
    top_k := $TOP_K, score_mode := 'path', limit_rows := 0
  ) gr
  GROUP BY q.person_id, q.expected_city_id
)
SELECT
  count(*) AS total,
  count(*) FILTER (WHERE targets[1] = expected_city_id) AS hit1,
  count(*) FILTER (WHERE expected_city_id = ANY(targets)) AS hitk,
  round(100.0 * count(*) FILTER (WHERE targets[1] = expected_city_id) / count(*), 1) AS hit1_pct,
  round(100.0 * count(*) FILTER (WHERE expected_city_id = ANY(targets)) / count(*), 1) AS hitk_pct
FROM results;
SQL
done

echo "  shared_cache=off: $(cat "$TMP_DIR/quality_off.txt")"
echo "  shared_cache=on:  $(cat "$TMP_DIR/quality_on.txt")"

QDIFF=$(diff "$TMP_DIR/quality_off.txt" "$TMP_DIR/quality_on.txt" || true)
if [ -z "$QDIFF" ]; then
  echo "  Quality: IDENTICAL"
  check "stage4_quality_match" "t" "t"
else
  echo "  Quality: DIVERGED"
  divergences=$((divergences + 1))
  check "stage4_quality_match" "t" "f"
fi

# ============================================================
# Stage 5: Row count comparison (the "collapsed rows" signal)
# ============================================================
echo ""
echo "=== Stage 5: Row counts per query ==="

for mode in off on; do
  PSQL <<SQL > "$TMP_DIR/rowcnt_${mode}.txt"
SET sorted_hnsw.shared_cache = ${mode};

SELECT q.person_id, count(*) AS nrows
FROM queries q,
LATERAL sorted_heap_graph_rag(
  'facts'::regclass, q.query_vec::svec,
  relation_path := ARRAY[1,2], ann_k := $ANN_K,
  top_k := $TOP_K, score_mode := 'path', limit_rows := 0
) gr
GROUP BY q.person_id
ORDER BY q.person_id;
SQL
done

RDIFF=$(diff "$TMP_DIR/rowcnt_off.txt" "$TMP_DIR/rowcnt_on.txt" | head -20 || true)
if [ -z "$RDIFF" ]; then
  echo "  Row counts: IDENTICAL"
  check "stage5_rowcounts_match" "t" "t"
else
  echo "  Row counts: DIVERGED (collapsed rows?)"
  echo "$RDIFF"
  divergences=$((divergences + 1))
  check "stage5_rowcounts_match" "t" "f"
fi

# ============================================================
# Stage 6: Verify cache paths via DEBUG1 logs
# ============================================================
echo ""
echo "=== Stage 6: Cache path verification ==="

# shared_cache=off backend should NOT attach
PSQL -c "
  SET client_min_messages = debug1;
  SET sorted_hnsw.shared_cache = off;
  SELECT count(*) FROM (
    SELECT entity_id FROM facts
    ORDER BY embedding <=> (SELECT query_vec::svec FROM queries LIMIT 1)
    LIMIT $ANN_K
  ) t" >/dev/null 2>"$TMP_DIR/log_off.txt"

check "stage6_off_no_attach" "0" \
  "$(grep -c 'attached to shared scan cache' "$TMP_DIR/log_off.txt")"

# shared_cache=on backend SHOULD attach (build already published)
PSQL -c "
  SET client_min_messages = debug1;
  SET sorted_hnsw.shared_cache = on;
  SELECT count(*) FROM (
    SELECT entity_id FROM facts
    ORDER BY embedding <=> (SELECT query_vec::svec FROM queries LIMIT 1)
    LIMIT $ANN_K
  ) t" >/dev/null 2>"$TMP_DIR/log_on.txt"

check "stage6_on_attached" "t" \
  "$(grep -c 'attached to shared scan cache' "$TMP_DIR/log_on.txt" | \
     awk '{print ($1 > 0) ? "t" : "f"}')"

# ---- Summary ----
echo ""
echo "=== Summary ==="
echo "status=$([ "$fail" -eq 0 ] && echo ok || echo FAIL) pass=$pass fail=$fail total=$total divergences=$divergences"

if [ "$divergences" -eq 0 ]; then
  echo ""
  echo "NO DIVERGENCE FOUND at this scale ($NROWS pairs, $DIM dim, $NQUERIES queries)."
  echo "The shared_cache=on path produces identical results to shared_cache=off."
  echo "If the original bug required larger scale or different parameters, increase NROWS/DIM."
fi

exit "$fail"
