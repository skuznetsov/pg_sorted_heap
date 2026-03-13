#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Live k8s Gutenberg ANN benchmark
# ============================================================
#
# Reproducible benchmark for the live Gutenberg corpus on a Kubernetes
# PostgreSQL pod. It compares:
#   1. pgvector HNSW on halfvec
#   2. sorted_heap IVF-PQ baseline
#   3. sorted_heap IVF-PQ + sketch sidecar
#   4. sorted_heap graph scan (svec_graph_scan) — skipped if GRAPH_SKIP=1
#      or if the graph table does not exist
#   5. sorted_heap hierarchical HNSW with session-warm L0+upper cache
#      (svec_hnsw_scan) — skipped if HNSW_CACHED_SKIP=1 or tables absent
#      Reports two operating points: ef_latency (default 64) and ef_quality (96)
#
# Prerequisite for graph: build_graph.py must have been run against the pod:
#   "$(./scripts/find_vector_python.sh)" scripts/build_graph.py \
#     --dsn 'host=localhost port=PORT dbname=DATABASE' \
#     --table gutenberg_gptoss_sh \
#     --graph-table gutenberg_gptoss_sh_graph \
#     --entry-table gutenberg_gptoss_sh_graph_entries \
#     --bootstrap --sketch-dim 384 --M 32 --M-max 64 --n-adjacent 4 --no-prune --seed 42
#
# Methodology:
#   - fixed query set from a query table
#   - fresh brute-force ground truth for those exact queries
#   - latency from server-side EXPLAIN Execution Time
#   - recall@K from overlap with the fresh ground truth
#   - normalized storage report that accounts for duplicate HNSW indexes
#
# Usage:
#   ./scripts/bench_gutenberg_k8s_ann.sh [namespace] [pod] [database]

NAMESPACE="${1:-default}"
POD="${2:-pgvector-dev-1}"
DATABASE="${3:-cogniformerus}"
DBUSER="${DBUSER:-postgres}"

EXT_SCHEMA="${EXT_SCHEMA:-public}"
HNSW_TABLE="${HNSW_TABLE:-public.gutenberg_gptoss}"
SH_TABLE="${SH_TABLE:-public.gutenberg_gptoss_sh}"
SKETCH_TABLE="${SKETCH_TABLE:-public.gutenberg_gptoss_sh_sketch}"
QUERY_TABLE="${QUERY_TABLE:-public.bench_gptoss_queries}"
QUERY_ID_COL="${QUERY_ID_COL:-qid}"
QUERY_VEC_COL="${QUERY_VEC_COL:-qvec}"

QUERY_LIMIT="${QUERY_LIMIT:-10}"
K="${K:-10}"
WARMUP_RUNS="${WARMUP_RUNS:-1}"
HNSW_EF_SEARCH="${HNSW_EF_SEARCH:-100}"
NPROBE="${NPROBE:-10}"
RERANK_TOPK="${RERANK_TOPK:-2000}"
SKETCH_TOPK="${SKETCH_TOPK:-128}"
CB_ID="${CB_ID:-7}"
IVF_CB_ID="${IVF_CB_ID:-1}"
PQ_COLUMN="${PQ_COLUMN:-pq_code}"

GRAPH_TABLE="${GRAPH_TABLE:-public.gutenberg_gptoss_sh_graph}"
GRAPH_ENTRY_TABLE="${GRAPH_ENTRY_TABLE:-public.gutenberg_gptoss_sh_graph_entries}"
GRAPH_EF_SEARCH="${GRAPH_EF_SEARCH:-256}"
GRAPH_RERANK_TOPK="${GRAPH_RERANK_TOPK:-0}"
GRAPH_SKIP="${GRAPH_SKIP:-0}"

HNSW_CACHED_SKIP="${HNSW_CACHED_SKIP:-0}"
HNSW_PREFIX="${HNSW_PREFIX:-public.gutenberg_gptoss_sh_hnsw}"
HNSW_CACHED_EF_LATENCY="${HNSW_CACHED_EF_LATENCY:-64}"
HNSW_CACHED_EF_QUALITY="${HNSW_CACHED_EF_QUALITY:-96}"

# Ground truth mode (applies to ALL methods including cached-HNSW section)
#   halfvec  — brute-force seqscan on the pgvector table (fast, 16-bit distances)
#   svec     — exhaustive IVF svec_ann_scan with full nprobe (slower, float32 exact)
GT_MODE="${GT_MODE:-halfvec}"
GT_SVEC_NPROBE="${GT_SVEC_NPROBE:-256}"         # svec mode: scan all IVF partitions
GT_SVEC_RERANK_TOPK="${GT_SVEC_RERANK_TOPK:-5000}"  # svec mode: rerank window (>> K)

if ! [[ "$QUERY_LIMIT" =~ ^[0-9]+$ ]] || [ "$QUERY_LIMIT" -lt 1 ]; then
  echo "QUERY_LIMIT must be >= 1" >&2
  exit 2
fi
if ! [[ "$K" =~ ^[0-9]+$ ]] || [ "$K" -lt 1 ]; then
  echo "K must be >= 1" >&2
  exit 2
fi
if ! [[ "$WARMUP_RUNS" =~ ^[0-9]+$ ]]; then
  echo "WARMUP_RUNS must be >= 0" >&2
  exit 2
fi

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl not found" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/bench_gutenberg_k8s_ann.XXXXXX")"
RESULTS_TSV="$TMP_DIR/results.tsv"
QUERY_TSV="$TMP_DIR/queries.tsv"
trap 'rm -rf "$TMP_DIR"' EXIT

POD_PSQL() {
  kubectl exec -i -n "$NAMESPACE" "$POD" -- \
    psql -U "$DBUSER" -d "$DATABASE" -v ON_ERROR_STOP=1 -Atq "$@"
}

extract_exec_ms() {
  sed -n 's/^Execution Time: \([0-9.][0-9.]*\) ms$/\1/p' | tail -1
}

compute_recall() {
  local truth_csv="$1"
  local got_csv="$2"
  local count=0
  local id
  declare -A truth=()

  IFS=',' read -r -a truth_ids <<< "$truth_csv"
  for id in "${truth_ids[@]}"; do
    [ -n "$id" ] && truth["$id"]=1
  done

  IFS=',' read -r -a got_ids <<< "$got_csv"
  for id in "${got_ids[@]}"; do
    if [ -n "${truth[$id]:-}" ]; then
      count=$((count + 1))
    fi
  done
  echo "$count"
}

sql_hnsw_ids() {
  local qvec="$1"
  cat <<SQL
BEGIN;
SET LOCAL hnsw.ef_search = ${HNSW_EF_SEARCH};
PREPARE q(halfvec) AS
SELECT string_agg(id, ',' ORDER BY ord)
FROM (
  SELECT id, row_number() OVER () AS ord
  FROM (
    SELECT id
    FROM ${HNSW_TABLE}
    ORDER BY embedding <=> \$1
    LIMIT ${K}
  ) s
) x;
EXECUTE q('${qvec}'::halfvec);
DEALLOCATE q;
COMMIT;
SQL
}

sql_hnsw_explain() {
  local qvec="$1"
  cat <<SQL
BEGIN;
SET LOCAL hnsw.ef_search = ${HNSW_EF_SEARCH};
PREPARE q(halfvec) AS
SELECT id
FROM ${HNSW_TABLE}
ORDER BY embedding <=> \$1
LIMIT ${K};
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF) EXECUTE q('${qvec}'::halfvec);
DEALLOCATE q;
COMMIT;
SQL
}

sql_ground_truth_ids() {
  local qvec="$1"
  if [ "$GT_MODE" = "svec" ]; then
    # Near-exact svec GT: exhaustive IVF scan (nprobe=all partitions) + float32 rerank.
    # GT_SVEC_RERANK_TOPK must be >> K to ensure true top-K are not dropped by PQ scoring.
    cat <<SQL
PREPARE q(${EXT_SCHEMA}.svec) AS
SELECT string_agg(id, ',' ORDER BY ord)
FROM (
  SELECT id, row_number() OVER () AS ord
  FROM ${EXT_SCHEMA}.svec_ann_scan(
    '${SH_TABLE}'::regclass,
    \$1,
    ${GT_SVEC_NPROBE}, ${K}, ${GT_SVEC_RERANK_TOPK},
    ${CB_ID}, ${IVF_CB_ID},
    '${PQ_COLUMN}'
  )
) x;
EXECUTE q('${qvec}'::${EXT_SCHEMA}.svec);
DEALLOCATE q;
SQL
  else
    cat <<SQL
BEGIN;
SET LOCAL enable_indexscan = off;
SET LOCAL enable_bitmapscan = off;
SET LOCAL enable_indexonlyscan = off;
PREPARE q(halfvec) AS
SELECT string_agg(id, ',' ORDER BY ord)
FROM (
  SELECT id, row_number() OVER () AS ord
  FROM (
    SELECT id
    FROM ${HNSW_TABLE}
    ORDER BY embedding <=> \$1
    LIMIT ${K}
  ) s
) x;
EXECUTE q('${qvec}'::halfvec);
DEALLOCATE q;
COMMIT;
SQL
  fi
}

sql_ivfpq_ids() {
  local qvec="$1"
  cat <<SQL
PREPARE q(${EXT_SCHEMA}.svec) AS
SELECT string_agg(id, ',' ORDER BY ord)
FROM (
  SELECT id, row_number() OVER () AS ord
  FROM ${EXT_SCHEMA}.svec_ann_scan(
    '${SH_TABLE}'::regclass,
    \$1,
    ${NPROBE}, ${K}, ${RERANK_TOPK},
    ${CB_ID}, ${IVF_CB_ID},
    '${PQ_COLUMN}'
  )
) x;
EXECUTE q('${qvec}'::${EXT_SCHEMA}.svec);
DEALLOCATE q;
SQL
}

sql_ivfpq_explain() {
  local qvec="$1"
  cat <<SQL
PREPARE q(${EXT_SCHEMA}.svec) AS
SELECT id
FROM ${EXT_SCHEMA}.svec_ann_scan(
  '${SH_TABLE}'::regclass,
  \$1,
  ${NPROBE}, ${K}, ${RERANK_TOPK},
  ${CB_ID}, ${IVF_CB_ID},
  '${PQ_COLUMN}'
);
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF) EXECUTE q('${qvec}'::${EXT_SCHEMA}.svec);
DEALLOCATE q;
SQL
}

sql_ivfpq_sketch_ids() {
  local qvec="$1"
  cat <<SQL
PREPARE q(${EXT_SCHEMA}.svec) AS
SELECT string_agg(id, ',' ORDER BY ord)
FROM (
  SELECT id, row_number() OVER () AS ord
  FROM ${EXT_SCHEMA}.svec_ann_scan(
    '${SH_TABLE}'::regclass,
    \$1,
    ${NPROBE}, ${K}, ${RERANK_TOPK},
    ${CB_ID}, ${IVF_CB_ID},
    '${PQ_COLUMN}',
    '${SKETCH_TABLE}',
    ${SKETCH_TOPK}
  )
) x;
EXECUTE q('${qvec}'::${EXT_SCHEMA}.svec);
DEALLOCATE q;
SQL
}

sql_ivfpq_sketch_explain() {
  local qvec="$1"
  cat <<SQL
PREPARE q(${EXT_SCHEMA}.svec) AS
SELECT id
FROM ${EXT_SCHEMA}.svec_ann_scan(
  '${SH_TABLE}'::regclass,
  \$1,
  ${NPROBE}, ${K}, ${RERANK_TOPK},
  ${CB_ID}, ${IVF_CB_ID},
  '${PQ_COLUMN}',
  '${SKETCH_TABLE}',
  ${SKETCH_TOPK}
);
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF) EXECUTE q('${qvec}'::${EXT_SCHEMA}.svec);
DEALLOCATE q;
SQL
}

sql_graph_ids() {
  local qvec="$1"
  cat <<SQL
PREPARE q(${EXT_SCHEMA}.svec) AS
SELECT string_agg(id, ',' ORDER BY ord)
FROM (
  SELECT id, row_number() OVER () AS ord
  FROM ${EXT_SCHEMA}.svec_graph_scan(
    '${SH_TABLE}'::regclass,
    \$1,
    '${GRAPH_TABLE}',
    ${GRAPH_EF_SEARCH}, ${K}, ${GRAPH_RERANK_TOPK},
    '${GRAPH_ENTRY_TABLE}'
  )
) x;
EXECUTE q('${qvec}'::${EXT_SCHEMA}.svec);
DEALLOCATE q;
SQL
}

sql_graph_explain() {
  local qvec="$1"
  cat <<SQL
PREPARE q(${EXT_SCHEMA}.svec) AS
SELECT id
FROM ${EXT_SCHEMA}.svec_graph_scan(
  '${SH_TABLE}'::regclass,
  \$1,
  '${GRAPH_TABLE}',
  ${GRAPH_EF_SEARCH}, ${K}, ${GRAPH_RERANK_TOPK},
  '${GRAPH_ENTRY_TABLE}'
);
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF) EXECUTE q('${qvec}'::${EXT_SCHEMA}.svec);
DEALLOCATE q;
SQL
}

summarize_method() {
  local method="$1"
  awk -F'\t' -v m="$method" '
    $1 == m {
      cnt++
      sum_ms += $3
      sum_rec += $4
      if (cnt == 1 || $3 < min_ms) min_ms = $3
      if (cnt == 1 || $3 > max_ms) max_ms = $3
    }
    END {
      if (cnt == 0) exit 1
      printf "%s\t%.2f\t%.2f\t%.2f\t%.2f\n", m, sum_ms/cnt, min_ms, max_ms, sum_rec/cnt
    }
  ' "$RESULTS_TSV"
}

warm_method() {
  local sql="$1"
  local i

  for ((i = 0; i < WARMUP_RUNS; i++)); do
    printf '%s\n' "$sql" | POD_PSQL -f - >/dev/null
  done
}

echo "============================================================"
echo "Live Gutenberg ANN benchmark"
echo "============================================================"
echo "Context: $(kubectl config current-context)"
echo "Pod:     ${NAMESPACE}/${POD}"
echo "DB:      ${DATABASE}"
echo "Queries: ${QUERY_LIMIT} fixed queries from ${QUERY_TABLE}"
echo "K:       ${K}"
echo "Warmups: ${WARMUP_RUNS} per method/query before timed run"
echo "HNSW ef: ${HNSW_EF_SEARCH}"
echo "IVF-PQ:  nprobe=${NPROBE}, rerank_topk=${RERANK_TOPK}, cb_id=${CB_ID}, ivf_cb_id=${IVF_CB_ID}"
echo "Sketch:  table=${SKETCH_TABLE}, sketch_topk=${SKETCH_TOPK}"
echo "Graph:   table=${GRAPH_TABLE}, entry=${GRAPH_ENTRY_TABLE}, ef=${GRAPH_EF_SEARCH}, rerank=${GRAPH_RERANK_TOPK}, skip=${GRAPH_SKIP}"
echo "HNSWcache: prefix=${HNSW_PREFIX}, ef_latency=${HNSW_CACHED_EF_LATENCY}, ef_quality=${HNSW_CACHED_EF_QUALITY}, skip=${HNSW_CACHED_SKIP}"
if [ "$GT_MODE" = "svec" ]; then
  echo "GT:      mode=svec (exhaustive IVF nprobe=${GT_SVEC_NPROBE}, rerank_topk=${GT_SVEC_RERANK_TOPK})"
else
  echo "GT:      mode=halfvec (brute-force seqscan on ${HNSW_TABLE})"
fi
echo ""

POD_PSQL -c "SELECT extname, extversion FROM pg_extension WHERE extname IN ('pg_sorted_heap','vector') ORDER BY 1;"
echo ""

# Check whether graph table exists; auto-skip if absent
if [ "$GRAPH_SKIP" != "1" ]; then
  graph_exists="$(POD_PSQL -c "
    SELECT count(*) FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname || '.' || c.relname = '${GRAPH_TABLE}'
      OR c.relname = '${GRAPH_TABLE}';
  " | tr -d ' \r\n')"
  if [ "$graph_exists" = "0" ]; then
    echo "Graph table '${GRAPH_TABLE}' not found — skipping graph scan." >&2
    echo "(Run build_graph.py against the pod first; see script header for the command.)" >&2
    GRAPH_SKIP=1
  fi
fi

if [ "$HNSW_CACHED_SKIP" != "1" ]; then
  hnsw_cached_exists="$(POD_PSQL -c "SELECT to_regclass('${HNSW_PREFIX}_meta') IS NOT NULL;" | tr -d ' \r\n')"
  if [ "$hnsw_cached_exists" != "t" ]; then
    echo "Cached HNSW prefix '${HNSW_PREFIX}' not found — skipping." >&2
    echo "(Run svec_hnsw_build against the pod first to populate the sidecar tables.)" >&2
    HNSW_CACHED_SKIP=1
  fi
fi

POD_PSQL -F $'\t' -c "
SELECT ${QUERY_ID_COL}, ${QUERY_VEC_COL}::text
FROM ${QUERY_TABLE}
ORDER BY ${QUERY_ID_COL}
LIMIT ${QUERY_LIMIT};
" > "$QUERY_TSV"

if [ ! -s "$QUERY_TSV" ]; then
  echo "No queries returned from ${QUERY_TABLE}" >&2
  exit 1
fi

echo -e "method\tqid\tlatency_ms\trecall10" > "$RESULTS_TSV"

while IFS=$'\t' read -r qid qvec; do
  echo "[query] ${qid}" >&2

  truth_ids="$(sql_ground_truth_ids "$qvec" | POD_PSQL -f - | tr -d '\r')"
  if [ -z "$truth_ids" ]; then
    echo "Ground truth query returned no rows for ${qid}" >&2
    exit 1
  fi

  warm_method "$(sql_hnsw_ids "$qvec")"
  hnsw_ms="$(sql_hnsw_explain "$qvec" | POD_PSQL -f - | extract_exec_ms)"
  hnsw_ids="$(sql_hnsw_ids "$qvec" | POD_PSQL -f - | tr -d '\r')"
  hnsw_recall="$(compute_recall "$truth_ids" "$hnsw_ids")"
  echo -e "hnsw\t${qid}\t${hnsw_ms}\t${hnsw_recall}" >> "$RESULTS_TSV"

  warm_method "$(sql_ivfpq_ids "$qvec")"
  ivfpq_ms="$(sql_ivfpq_explain "$qvec" | POD_PSQL -f - | extract_exec_ms)"
  ivfpq_ids="$(sql_ivfpq_ids "$qvec" | POD_PSQL -f - | tr -d '\r')"
  ivfpq_recall="$(compute_recall "$truth_ids" "$ivfpq_ids")"
  echo -e "ivfpq\t${qid}\t${ivfpq_ms}\t${ivfpq_recall}" >> "$RESULTS_TSV"

  warm_method "$(sql_ivfpq_sketch_ids "$qvec")"
  ivfpq_sketch_ms="$(sql_ivfpq_sketch_explain "$qvec" | POD_PSQL -f - | extract_exec_ms)"
  ivfpq_sketch_ids="$(sql_ivfpq_sketch_ids "$qvec" | POD_PSQL -f - | tr -d '\r')"
  ivfpq_sketch_recall="$(compute_recall "$truth_ids" "$ivfpq_sketch_ids")"
  echo -e "ivfpq_sketch\t${qid}\t${ivfpq_sketch_ms}\t${ivfpq_sketch_recall}" >> "$RESULTS_TSV"

  if [ "$GRAPH_SKIP" != "1" ]; then
    warm_method "$(sql_graph_ids "$qvec")"
    graph_ms="$(sql_graph_explain "$qvec" | POD_PSQL -f - | extract_exec_ms)"
    graph_ids="$(sql_graph_ids "$qvec" | POD_PSQL -f - | tr -d '\r')"
    graph_recall="$(compute_recall "$truth_ids" "$graph_ids")"
    echo -e "graph\t${qid}\t${graph_ms}\t${graph_recall}" >> "$RESULTS_TSV"
  fi
done < "$QUERY_TSV"

echo ""
echo "Per-query results"
echo "------------------------------------------------------------"
column -t -s $'\t' "$RESULTS_TSV"

echo ""
echo "Summary"
echo "------------------------------------------------------------"
{
  echo -e "method\tavg_ms\tmin_ms\tmax_ms\tavg_recall10"
  summarize_method "hnsw"
  summarize_method "ivfpq"
  summarize_method "ivfpq_sketch"
  [ "$GRAPH_SKIP" != "1" ] && summarize_method "graph"
} | column -t -s $'\t'

if [ "$HNSW_CACHED_SKIP" != "1" ]; then
  echo ""
  echo "Cached HNSW (hierarchical, session-warm L0+upper cache)"
  echo "------------------------------------------------------------"
  if [ "$GT_MODE" = "svec" ]; then
    echo "GT:    near-exact svec (exhaustive IVF nprobe=${GT_SVEC_NPROBE}, rerank_topk=${GT_SVEC_RERANK_TOPK})"
    # Build the GT subquery block for svec mode (PL/pgSQL statements, no index hints needed)
    hnsw_gt_sql="        -- Near-exact svec GT: exhaustive IVF scan + float32 rerank.
        SELECT array_agg(id ORDER BY ord) INTO truth_ids
        FROM (
            SELECT id, row_number() OVER () AS ord
            FROM ${EXT_SCHEMA}.svec_ann_scan(
                '${SH_TABLE}'::regclass, qrec.qvec,
                ${GT_SVEC_NPROBE}, ${K}, ${GT_SVEC_RERANK_TOPK},
                ${CB_ID}, ${IVF_CB_ID}, '${PQ_COLUMN}'
            )
        ) s;"
  else
    echo "GT:    halfvec brute-force seqscan on ${HNSW_TABLE} (same as all other methods)"
    echo "       recall% is cross-method comparable but NOT full-svec exact recall."
    echo "       Use GT_MODE=svec for float32 exact GT (slower, requires IVF codebook)."
    # Build the GT subquery block for halfvec mode
    hnsw_gt_sql="        -- Halfvec brute-force GT (same GT as all other harness methods).
        PERFORM set_config('enable_indexscan',     'off', true);
        PERFORM set_config('enable_bitmapscan',    'off', true);
        PERFORM set_config('enable_indexonlyscan', 'off', true);

        SELECT array_agg(id ORDER BY rn) INTO truth_ids
        FROM (
            SELECT id,
                   row_number() OVER (ORDER BY embedding <=> qrec.qvec::text::halfvec) AS rn
            FROM ${HNSW_TABLE}
            LIMIT ${K}
        ) s;

        PERFORM set_config('enable_indexscan',     'on', true);
        PERFORM set_config('enable_bitmapscan',    'on', true);
        PERFORM set_config('enable_indexonlyscan', 'on', true);"
  fi
  # All statements run in one psql session so the backend-local L0/upper caches
  # built during warmup persist for the entire benchmark loop.
  kubectl exec -i -n "$NAMESPACE" "$POD" -- \
    psql -U "$DBUSER" -d "$DATABASE" -v ON_ERROR_STOP=1 <<SQL

SET sorted_heap.hnsw_cache_l0 = on;
CREATE TEMP TABLE _hnsw_bench (method text, latency_ms float8, recall int);

-- Warmup: prime L0 and upper-level caches before timing
SELECT id
FROM ${EXT_SCHEMA}.svec_hnsw_scan(
    '${SH_TABLE}'::regclass,
    (SELECT ${QUERY_VEC_COL} FROM ${QUERY_TABLE} ORDER BY ${QUERY_ID_COL} LIMIT 1),
    '${HNSW_PREFIX}', ${HNSW_CACHED_EF_LATENCY}, ${K}, 0)
LIMIT 1;

DO \$\$
DECLARE
    qrec       RECORD;
    t0         timestamptz;
    elapsed_ms float8;
    result_ids text[];
    truth_ids  text[];
    hits       int;
BEGIN
    FOR qrec IN
        SELECT ${QUERY_ID_COL} AS qid, ${QUERY_VEC_COL} AS qvec
        FROM ${QUERY_TABLE}
        ORDER BY ${QUERY_ID_COL}
        LIMIT ${QUERY_LIMIT}
    LOOP
${hnsw_gt_sql}

        -- Latency-first operating point (ef=${HNSW_CACHED_EF_LATENCY})
        t0 := clock_timestamp();
        SELECT array_agg(id) INTO result_ids
        FROM ${EXT_SCHEMA}.svec_hnsw_scan(
            '${SH_TABLE}'::regclass, qrec.qvec,
            '${HNSW_PREFIX}', ${HNSW_CACHED_EF_LATENCY}, ${K}, 0);
        elapsed_ms := extract(epoch from (clock_timestamp() - t0)) * 1000.0;

        SELECT count(*) INTO hits
        FROM unnest(result_ids) r(id)
        WHERE r.id = ANY(truth_ids);

        INSERT INTO _hnsw_bench VALUES
            ('hnsw_cached_ef${HNSW_CACHED_EF_LATENCY}', elapsed_ms, hits);

        -- Quality-first operating point (ef=${HNSW_CACHED_EF_QUALITY})
        t0 := clock_timestamp();
        SELECT array_agg(id) INTO result_ids
        FROM ${EXT_SCHEMA}.svec_hnsw_scan(
            '${SH_TABLE}'::regclass, qrec.qvec,
            '${HNSW_PREFIX}', ${HNSW_CACHED_EF_QUALITY}, ${K}, 0);
        elapsed_ms := extract(epoch from (clock_timestamp() - t0)) * 1000.0;

        SELECT count(*) INTO hits
        FROM unnest(result_ids) r(id)
        WHERE r.id = ANY(truth_ids);

        INSERT INTO _hnsw_bench VALUES
            ('hnsw_cached_ef${HNSW_CACHED_EF_QUALITY}', elapsed_ms, hits);
    END LOOP;
END;
\$\$ LANGUAGE plpgsql;

SELECT
    method,
    round(percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_ms)::numeric, 2) AS p50_ms,
    round(percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)::numeric, 2) AS p95_ms,
    round(avg(latency_ms)::numeric, 2)                                           AS avg_ms,
    round(avg(recall)::numeric * 100.0 / ${K}, 1)                               AS "recall%",
    count(*)                                                                     AS n
FROM _hnsw_bench
GROUP BY method
ORDER BY method;
SQL
fi

echo ""
echo "Storage"
echo "------------------------------------------------------------"
POD_PSQL -F $'\t' -c "
WITH rels AS (
  SELECT '${HNSW_TABLE}'::regclass AS hnsw_tbl,
         '${SH_TABLE}'::regclass AS sh_tbl,
         '${SKETCH_TABLE}'::regclass AS sketch_tbl,
         to_regclass('${GRAPH_TABLE}') AS graph_tbl,
         to_regclass('${GRAPH_ENTRY_TABLE}') AS graph_entry_tbl
),
hnsw_indexes AS (
  SELECT i.indexrelid::regclass::text AS index_name,
         pg_relation_size(i.indexrelid) AS index_bytes
  FROM pg_index i
  JOIN pg_class c ON c.oid = i.indexrelid
  JOIN pg_am am ON am.oid = c.relam
  JOIN rels r ON i.indrelid = r.hnsw_tbl
  WHERE am.amname = 'hnsw'
),
pick_one_hnsw AS (
  SELECT index_name, index_bytes
  FROM hnsw_indexes
  ORDER BY index_name
  LIMIT 1
),
pk_index AS (
  SELECT pg_relation_size(i.indexrelid) AS pk_bytes
  FROM pg_index i
  JOIN rels r ON i.indrelid = r.hnsw_tbl
  WHERE i.indisprimary
),
base AS (
  SELECT
    pg_total_relation_size(hnsw_tbl) AS hnsw_total,
    pg_relation_size(hnsw_tbl) AS hnsw_heap,
    pg_indexes_size(hnsw_tbl) AS hnsw_indexes_total,
    pg_total_relation_size(hnsw_tbl)
      - pg_relation_size(hnsw_tbl)
      - pg_indexes_size(hnsw_tbl) AS hnsw_toast,
    pg_total_relation_size(sh_tbl) + pg_total_relation_size(sketch_tbl) AS sh_total,
    CASE WHEN graph_tbl IS NOT NULL
         THEN pg_total_relation_size(graph_tbl)
              + COALESCE(pg_total_relation_size(graph_entry_tbl), 0)
         ELSE NULL END AS graph_total
  FROM rels
)
SELECT 'pgvector_total_current', pg_size_pretty(hnsw_total) FROM base
UNION ALL
SELECT 'pgvector_heap', pg_size_pretty(hnsw_heap) FROM base
UNION ALL
SELECT 'pgvector_toast', pg_size_pretty(hnsw_toast) FROM base
UNION ALL
SELECT 'pgvector_indexes_current', pg_size_pretty(hnsw_indexes_total) FROM base
UNION ALL
SELECT 'pgvector_hnsw_index_count', count(*)::text FROM hnsw_indexes
UNION ALL
SELECT 'pgvector_one_hnsw_index', pg_size_pretty(index_bytes) FROM pick_one_hnsw
UNION ALL
SELECT 'pgvector_total_normalized_one_hnsw',
       pg_size_pretty((SELECT hnsw_heap + hnsw_toast FROM base)
                      + COALESCE((SELECT pk_bytes FROM pk_index), 0)
                      + COALESCE((SELECT index_bytes FROM pick_one_hnsw), 0))
FROM base
UNION ALL
SELECT 'sorted_heap_plus_sketch_total', pg_size_pretty(sh_total) FROM base
UNION ALL
SELECT 'graph_total', pg_size_pretty(graph_total) FROM base WHERE graph_total IS NOT NULL
ORDER BY 1;
"
