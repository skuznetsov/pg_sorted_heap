#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Partitioned sorted_hnsw route-first benchmark
# ============================================================
#
# Builds a synthetic partitioned sorted_heap table with one sorted_hnsw index per
# leaf, then compares:
#   1. selected-leaf route-first helper
#   2. parent filtered exact query
#   3. all-leaf helper fanout
#   4. parent Merge Append over leaf sorted_hnsw indexes
#
# Usage:
#   ./scripts/bench_partitioned_sorted_hnsw.sh [tmp_root] [port] [partitions] [rows_per_partition] [queries] [dim] [k] [local_k] [ef_search] [hnsw_m] [ef_construction]

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
PORT="${2:-65492}"
PARTITIONS="${3:-8}"
ROWS_PER_PARTITION="${4:-500}"
QUERIES="${5:-24}"
DIM="${6:-16}"
K="${7:-10}"
LOCAL_K="${8:-16}"
EF_SEARCH="${9:-32}"
HNSW_M="${10:-16}"
EF_CONSTRUCTION="${11:-64}"

if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root must be absolute: $TMP_ROOT" >&2
  exit 2
fi

for val_name in PORT PARTITIONS ROWS_PER_PARTITION QUERIES DIM K LOCAL_K EF_SEARCH HNSW_M EF_CONSTRUCTION; do
  val="${!val_name}"
  if ! [[ "$val" =~ ^[0-9]+$ ]] || [ "$val" -le 0 ]; then
    echo "$val_name must be a positive integer" >&2
    exit 2
  fi
done

if [ "$LOCAL_K" -lt "$K" ]; then
  echo "local_k must be >= k" >&2
  exit 2
fi
if [ "$LOCAL_K" -gt "$EF_SEARCH" ]; then
  echo "local_k must be <= ef_search" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PG_BINDIR="$(pg_config --bindir)"

TMP_DIR=""
cleanup() {
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR/data" ]; then
    "$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -m immediate stop >/dev/null 2>&1 || true
  fi
  [ -n "$TMP_DIR" ] && rm -rf "$TMP_DIR"
}
trap cleanup EXIT

PSQL() {
  "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -v ON_ERROR_STOP=1 -X -qtAX "$@"
}

echo "============================================================"
echo "partitioned sorted_hnsw route-first benchmark"
echo "============================================================"
echo "partitions:         $PARTITIONS"
echo "rows/partition:     $ROWS_PER_PARTITION"
echo "queries:            $QUERIES"
echo "dim:                $DIM"
echo "k:                  $K"
echo "local_k:            $LOCAL_K"
echo "ef_search:          $EF_SEARCH"
echo "hnsw_m:             $HNSW_M"
echo "ef_construction:    $EF_CONSTRUCTION"
echo "port:               $PORT"
echo

make -C "$ROOT_DIR" install >/dev/null

TMP_DIR="$(mktemp -d "$TMP_ROOT/pg_sorted_hnsw_part.XXXXXX")"
"$PG_BINDIR/initdb" -D "$TMP_DIR/data" -A trust --no-locale >/dev/null 2>&1

cat >> "$TMP_DIR/data/postgresql.conf" <<'PGCONF'
shared_buffers = 256MB
listen_addresses = ''
fsync = on
max_wal_size = 1GB
log_min_messages = warning
shared_preload_libraries = 'pg_sorted_heap'
PGCONF

"$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -l "$TMP_DIR/postmaster.log" \
  -o "-k $TMP_DIR -p $PORT" start >/dev/null

PSQL <<SQL
CREATE EXTENSION pg_sorted_heap;
SET client_min_messages = warning;
SET jit = off;
SET sorted_hnsw.ef_search = $EF_SEARCH;

CREATE TABLE bench_part(
  bucket int,
  id int,
  v svec($DIM),
  payload text,
  PRIMARY KEY(bucket, id)
) PARTITION BY RANGE(bucket);

DO \$ddl\$
DECLARE
  p int;
BEGIN
  FOR p IN 1..$PARTITIONS LOOP
    EXECUTE format(
      'CREATE TABLE bench_part_%s PARTITION OF bench_part FOR VALUES FROM (%s) TO (%s) USING sorted_heap',
      p, p, p + 1);
  END LOOP;
END
\$ddl\$;

WITH src AS (
  SELECT p AS bucket,
         i AS id,
         ('[' ||
          string_agg(
            ((((p * 1009) + (i * (((d * 17) % 97) + 1)) + (d * 13)) % 1000)::float / 1000.0)::text,
            ',' ORDER BY d
          ) ||
          ']') AS vec_text
  FROM generate_series(1, $PARTITIONS) AS p
  CROSS JOIN generate_series(1, $ROWS_PER_PARTITION) AS i
  CROSS JOIN LATERAL generate_series(1, $DIM) AS d
  GROUP BY p, i
)
INSERT INTO bench_part
SELECT bucket, id, vec_text::svec($DIM), repeat('x', 32)
FROM src;

SELECT 'compact_ok|' || count(*)
FROM sorted_heap_compact_partitions('bench_part'::regclass)
WHERE status = 'ok';

DO \$idx\$
DECLARE
  p int;
BEGIN
  FOR p IN 1..$PARTITIONS LOOP
    EXECUTE format(
      'CREATE INDEX bench_part_%s_v_idx ON bench_part_%s USING sorted_hnsw (v) WITH (m=$HNSW_M, ef_construction=$EF_CONSTRUCTION)',
      p, p);
  END LOOP;
END
\$idx\$;

ANALYZE bench_part;

CREATE TEMP TABLE bench_queries AS
WITH q AS (
  SELECT qid,
         ((qid - 1) % $PARTITIONS) + 1 AS bucket,
         ((qid * 97) % $ROWS_PER_PARTITION) + 1 AS sample_id
  FROM generate_series(1, $QUERIES) AS qid
)
SELECT qid,
       bucket,
       format('bench_part_%s', bucket)::regclass AS leaf_relid,
       (
         SELECT p.v::text
         FROM bench_part p
         WHERE p.bucket = q.bucket
           AND p.id = q.sample_id
       ) AS qtext
FROM q
ORDER BY qid;

CREATE TEMP TABLE bench_gt_selected AS
SELECT q.qid,
       ARRAY(
         SELECT bucket::text || ':' || id::text
         FROM bench_part
         WHERE bucket = q.bucket
         ORDER BY v <=> q.qtext::svec($DIM)
         LIMIT $K
       ) AS ids
FROM bench_queries q;

CREATE TEMP TABLE bench_gt_global AS
SELECT q.qid,
       ARRAY(
         SELECT bucket::text || ':' || id::text
         FROM bench_part
         ORDER BY v <=> q.qtext::svec($DIM)
         LIMIT $K
       ) AS ids
FROM bench_queries q;

CREATE TEMP TABLE bench_runs(
  method text,
  qid int,
  ms double precision,
  recall double precision
);

DO \$bench\$
DECLARE
  q record;
  t0 timestamptz;
  ids text[];
  gt text[];
  hit_count int;
BEGIN
  FOR q IN SELECT * FROM bench_queries ORDER BY qid LOOP
    SELECT g.ids INTO gt FROM bench_gt_selected g WHERE g.qid = q.qid;

    t0 := clock_timestamp();
    SELECT ARRAY(
      SELECT (row_data->>'bucket') || ':' || (row_data->>'id')
      FROM sorted_hnsw_partition_search(
        'bench_part'::regclass, 'v', q.qtext, $K, $LOCAL_K, ARRAY[q.leaf_relid])
    ) INTO ids;
    SELECT count(*) INTO hit_count FROM unnest(ids) AS got(id) WHERE got.id = ANY(gt);
    INSERT INTO bench_runs VALUES ('helper_selected', q.qid,
      EXTRACT(EPOCH FROM clock_timestamp() - t0) * 1000.0,
      hit_count::double precision / $K);

    t0 := clock_timestamp();
    EXECUTE format(
      'SELECT ARRAY(SELECT bucket::text || '':'' || id::text FROM bench_part WHERE bucket = \$1 ORDER BY v <=> \$2::svec(%s) LIMIT %s)',
      $DIM, $K)
    USING q.bucket, q.qtext
    INTO ids;
    SELECT count(*) INTO hit_count FROM unnest(ids) AS got(id) WHERE got.id = ANY(gt);
    INSERT INTO bench_runs VALUES ('parent_filtered_exact', q.qid,
      EXTRACT(EPOCH FROM clock_timestamp() - t0) * 1000.0,
      hit_count::double precision / $K);

    SELECT g.ids INTO gt FROM bench_gt_global g WHERE g.qid = q.qid;

    t0 := clock_timestamp();
    SELECT ARRAY(
      SELECT (row_data->>'bucket') || ':' || (row_data->>'id')
      FROM sorted_hnsw_partition_search(
        'bench_part'::regclass, 'v', q.qtext, $K, $LOCAL_K)
    ) INTO ids;
    SELECT count(*) INTO hit_count FROM unnest(ids) AS got(id) WHERE got.id = ANY(gt);
    INSERT INTO bench_runs VALUES ('helper_all_leaves', q.qid,
      EXTRACT(EPOCH FROM clock_timestamp() - t0) * 1000.0,
      hit_count::double precision / $K);

    t0 := clock_timestamp();
    EXECUTE format(
      'SELECT ARRAY(SELECT bucket::text || '':'' || id::text FROM bench_part ORDER BY v <=> \$1::svec(%s) LIMIT %s)',
      $DIM, $K)
    USING q.qtext
    INTO ids;
    SELECT count(*) INTO hit_count FROM unnest(ids) AS got(id) WHERE got.id = ANY(gt);
    INSERT INTO bench_runs VALUES ('parent_all_merge_append', q.qid,
      EXTRACT(EPOCH FROM clock_timestamp() - t0) * 1000.0,
      hit_count::double precision / $K);
  END LOOP;
END
\$bench\$;

SELECT method,
       round(avg(ms)::numeric, 3) AS avg_ms,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY ms)::numeric, 3) AS p50_ms,
       round(avg(recall)::numeric, 4) AS recall_at_k
FROM bench_runs
GROUP BY method
ORDER BY method;

SELECT 'parent_filtered_exact_plan';
EXPLAIN (COSTS OFF)
SELECT id
FROM bench_part
WHERE bucket = 1
ORDER BY v <=> (SELECT qtext::svec($DIM) FROM bench_queries WHERE qid = 1)
LIMIT $K;

SELECT 'parent_all_merge_append_plan';
EXPLAIN (COSTS OFF)
SELECT id
FROM bench_part
ORDER BY v <=> (SELECT qtext::svec($DIM) FROM bench_queries WHERE qid = 1)
LIMIT $K;
SQL
