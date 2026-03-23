#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# sorted_hnsw vs exact heap vs pgvector HNSW
# ============================================================
#
# Local reproducible benchmark on a temp PostgreSQL instance.
# It builds one synthetic corpus, one query set, then measures:
#   1. exact heap baseline on svec (ground truth)
#   2. sorted_hnsw ordered index scan on svec
#   3. pgvector HNSW on vector or halfvec
#
# Usage:
#   ./scripts/bench_sorted_hnsw_vs_pgvector.sh [tmp_root] [port] [rows] [queries] [dim] [k] [pgv_storage] [pgv_ef_search] [shnsw_ef_search]
#
# Output:
#   - method table with p50 / avg / recall@K
#   - index size summary

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
PORT="${2:-65481}"
ROWS="${3:-10000}"
QUERIES="${4:-20}"
DIM="${5:-384}"
K="${6:-10}"
PGV_STORAGE="${7:-vector}"
PGV_EF_SEARCH="${8:-64}"
SHNSW_EF_SEARCH="${9:-96}"

if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root must be absolute: $TMP_ROOT" >&2
  exit 2
fi
for val_name in PORT ROWS QUERIES DIM K PGV_EF_SEARCH SHNSW_EF_SEARCH; do
  val="${!val_name}"
  if ! [[ "$val" =~ ^[0-9]+$ ]] || [ "$val" -le 0 ]; then
    echo "$val_name must be a positive integer" >&2
    exit 2
  fi
done

case "$PGV_STORAGE" in
  vector)
    PGV_TYPE_SQL="vector($DIM)"
    PGV_CAST_SQL="vector($DIM)"
    PGV_OPCLASS="vector_cosine_ops"
    PGV_METHOD="pgvector_hnsw_vector"
    ;;
  halfvec)
    PGV_TYPE_SQL="halfvec($DIM)"
    PGV_CAST_SQL="halfvec($DIM)"
    PGV_OPCLASS="halfvec_cosine_ops"
    PGV_METHOD="pgvector_hnsw_halfvec"
    ;;
  *)
    echo "pgv_storage must be one of: vector, halfvec" >&2
    exit 2
    ;;
esac

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
  "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -v ON_ERROR_STOP=1 -qtAX "$@"
}

echo "============================================================"
echo "sorted_hnsw vs exact heap vs pgvector HNSW"
echo "============================================================"
echo "rows:    $ROWS"
echo "queries: $QUERIES"
echo "dim:     $DIM"
echo "k:       $K"
echo "pgv:     $PGV_STORAGE"
echo "pgv ef:  $PGV_EF_SEARCH"
echo "sh ef:   $SHNSW_EF_SEARCH"
echo "port:    $PORT"
echo

make -C "$ROOT_DIR" install >/dev/null

TMP_DIR="$(mktemp -d "$TMP_ROOT/pg_sorted_hnsw_vs_pgv.XXXXXX")"
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
CREATE EXTENSION vector;

CREATE TABLE bench_sh(
  id int PRIMARY KEY,
  v  svec($DIM)
);

CREATE TABLE bench_pgv(
  id int PRIMARY KEY,
  v  $PGV_TYPE_SQL
);

WITH src AS (
  SELECT g AS id,
         ('[' ||
          string_agg(
            ((((g * (((d * 17) % 97) + 1)) + (d * 13)) % 1000)::float / 1000.0)::text,
            ',' ORDER BY d
          ) ||
          ']') AS vec_text
  FROM generate_series(1, $ROWS) AS g
  CROSS JOIN LATERAL generate_series(1, $DIM) AS d
  GROUP BY g
)
INSERT INTO bench_sh
SELECT id, vec_text::svec($DIM)
FROM src;

WITH src AS (
  SELECT id, v::text AS vec_text
  FROM bench_sh
)
INSERT INTO bench_pgv
SELECT id, vec_text::$PGV_CAST_SQL
FROM src;

CREATE INDEX bench_sh_idx
ON bench_sh USING sorted_hnsw(v) WITH (m=16, ef_construction=64);

CREATE INDEX bench_pgv_idx
ON bench_pgv USING hnsw (v $PGV_OPCLASS) WITH (m=16, ef_construction=64);

ANALYZE bench_sh;
ANALYZE bench_pgv;

CREATE TEMP TABLE bench_queries AS
SELECT qid,
       vec_text::svec($DIM)    AS q_s,
       vec_text::$PGV_CAST_SQL AS q_h
FROM (
  SELECT qid,
         ('[' ||
          string_agg(
            (((((qid + $ROWS) * (((d * 19) % 89) + 3)) + (d * 29)) % 1000)::float / 1000.0)::text,
            ',' ORDER BY d
          ) ||
          ']') AS vec_text
  FROM generate_series(1, $QUERIES) AS qid
  CROSS JOIN LATERAL generate_series(1, $DIM) AS d
  GROUP BY qid
) q;

CREATE TEMP TABLE bench_gt AS
SELECT q.qid,
       ARRAY(
         SELECT id
         FROM bench_sh
         ORDER BY v <=> q.q_s
         LIMIT $K
       ) AS ids
FROM bench_queries q;

SET jit = off;

CREATE TEMP TABLE bench_exact(qid int, ms double precision, ids int[]);
CREATE TEMP TABLE bench_shnsw(qid int, ms double precision, ids int[]);
CREATE TEMP TABLE bench_pgv_run(qid int, ms double precision, ids int[]);

SET enable_seqscan = on;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
DO \$warm_exact\$
DECLARE
  q record;
BEGIN
  FOR q IN SELECT * FROM bench_queries ORDER BY qid LOOP
    PERFORM id
    FROM bench_sh
    ORDER BY v <=> q.q_s
    LIMIT $K;
  END LOOP;
END
\$warm_exact\$;

DO \$run_exact\$
DECLARE
  q record;
  t0 timestamptz;
  ids int[];
BEGIN
  FOR q IN SELECT * FROM bench_queries ORDER BY qid LOOP
    t0 := clock_timestamp();
    SELECT ARRAY(
      SELECT id
      FROM bench_sh
      ORDER BY v <=> q.q_s
      LIMIT $K
    ) INTO ids;
    INSERT INTO bench_exact(qid, ms, ids)
    VALUES (q.qid, EXTRACT(EPOCH FROM clock_timestamp() - t0) * 1000.0, ids);
  END LOOP;
END
\$run_exact\$;

SET enable_seqscan = off;
SET enable_indexscan = on;
SET enable_bitmapscan = on;
SET sorted_hnsw.shared_cache = on;
SET sorted_hnsw.ef_search = $SHNSW_EF_SEARCH;
DO \$warm_shnsw\$
DECLARE
  q record;
BEGIN
  FOR q IN SELECT * FROM bench_queries ORDER BY qid LOOP
    PERFORM id
    FROM bench_sh
    ORDER BY v <=> q.q_s
    LIMIT $K;
  END LOOP;
END
\$warm_shnsw\$;

DO \$run_shnsw\$
DECLARE
  q record;
  t0 timestamptz;
  ids int[];
BEGIN
  FOR q IN SELECT * FROM bench_queries ORDER BY qid LOOP
    t0 := clock_timestamp();
    SELECT ARRAY(
      SELECT id
      FROM bench_sh
      ORDER BY v <=> q.q_s
      LIMIT $K
    ) INTO ids;
    INSERT INTO bench_shnsw(qid, ms, ids)
    VALUES (q.qid, EXTRACT(EPOCH FROM clock_timestamp() - t0) * 1000.0, ids);
  END LOOP;
END
\$run_shnsw\$;

SET hnsw.ef_search = $PGV_EF_SEARCH;
SET enable_seqscan = off;
DO \$warm_pgv\$
DECLARE
  q record;
BEGIN
  FOR q IN SELECT * FROM bench_queries ORDER BY qid LOOP
    PERFORM id
    FROM bench_pgv
    ORDER BY v <=> q.q_h
    LIMIT $K;
  END LOOP;
END
\$warm_pgv\$;

DO \$run_pgv\$
DECLARE
  q record;
  t0 timestamptz;
  ids int[];
BEGIN
  FOR q IN SELECT * FROM bench_queries ORDER BY qid LOOP
    t0 := clock_timestamp();
    SELECT ARRAY(
      SELECT id
      FROM bench_pgv
      ORDER BY v <=> q.q_h
      LIMIT $K
    ) INTO ids;
    INSERT INTO bench_pgv_run(qid, ms, ids)
    VALUES (q.qid, EXTRACT(EPOCH FROM clock_timestamp() - t0) * 1000.0, ids);
  END LOOP;
END
\$run_pgv\$;

WITH metrics AS (
  SELECT
    'exact_heap'::text AS method,
    percentile_disc(0.5) WITHIN GROUP (ORDER BY ms) AS p50_ms,
    avg(ms) AS avg_ms,
    100.0::numeric AS recall_at_k
  FROM bench_exact
  UNION ALL
  SELECT
    'sorted_hnsw',
    percentile_disc(0.5) WITHIN GROUP (ORDER BY r.ms),
    avg(r.ms),
    avg((
      SELECT count(*)::numeric
      FROM unnest(r.ids) x
      WHERE x = ANY(g.ids)
    ) / $K::numeric) * 100.0
  FROM bench_shnsw r
  JOIN bench_gt g USING (qid)
  UNION ALL
  SELECT
    '$PGV_METHOD',
    percentile_disc(0.5) WITHIN GROUP (ORDER BY r.ms),
    avg(r.ms),
    avg((
      SELECT count(*)::numeric
      FROM unnest(r.ids) x
      WHERE x = ANY(g.ids)
    ) / $K::numeric) * 100.0
  FROM bench_pgv_run r
  JOIN bench_gt g USING (qid)
)
SELECT format(
  '%s|p50_ms=%s|avg_ms=%s|recall_at_%s=%s',
  method,
  to_char(p50_ms, 'FM999999990.000'),
  to_char(avg_ms, 'FM999999990.000'),
  $K,
  to_char(recall_at_k, 'FM999999990.0')
)
FROM metrics
ORDER BY
  CASE method
    WHEN 'exact_heap' THEN 1
    WHEN 'sorted_hnsw' THEN 2
    WHEN '$PGV_METHOD' THEN 3
    ELSE 99
  END;

SELECT format(
  'index_sizes|sorted_hnsw=%s|pgvector_hnsw=%s|bench_sh_total=%s|bench_pgv_total=%s',
  pg_size_pretty(pg_relation_size('bench_sh_idx'::regclass)),
  pg_size_pretty(pg_relation_size('bench_pgv_idx'::regclass)),
  pg_size_pretty(pg_total_relation_size('bench_sh'::regclass)),
  pg_size_pretty(pg_total_relation_size('bench_pgv'::regclass))
);
SQL
