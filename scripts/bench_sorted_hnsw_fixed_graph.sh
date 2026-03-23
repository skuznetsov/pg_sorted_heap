#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# sorted_hnsw ordered-scan benchmark on a fixed graph
# ============================================================
#
# Purpose:
#   Build a deterministic 4D bench table and HNSW index once, then measure
#   repeated ordered scans against that same on-disk graph. This avoids the
#   graph-shape noise you get when every timing run rebuilds HNSW from scratch.
#
# Usage:
#   ./scripts/bench_sorted_hnsw_fixed_graph.sh [tmp_root] [port] [rows] [runs] [dim] [mode]
#
# Output:
#   - per-run execution time in ms
#   - average execution time across runs

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
PORT="${2:-65441}"
ROWS="${3:-50000}"
RUNS="${4:-10}"
DIM="${5:-4}"
MODE="${6:-fresh}"

if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root must be absolute: $TMP_ROOT" >&2
  exit 2
fi
if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [ "$PORT" -le 1024 ] || [ "$PORT" -ge 65530 ]; then
  echo "port must be 1025..65529" >&2
  exit 2
fi
if ! [[ "$ROWS" =~ ^[0-9]+$ ]] || [ "$ROWS" -le 0 ]; then
  echo "rows must be a positive integer" >&2
  exit 2
fi
if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [ "$RUNS" -le 0 ]; then
  echo "runs must be a positive integer" >&2
  exit 2
fi
if ! [[ "$DIM" =~ ^[0-9]+$ ]] || [ "$DIM" -le 0 ]; then
  echo "dim must be a positive integer" >&2
  exit 2
fi
if [[ "$MODE" != "fresh" && "$MODE" != "reuse" ]]; then
  echo "mode must be 'fresh' or 'reuse'" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if command -v pg_config >/dev/null 2>&1; then
  PG_BINDIR="$(pg_config --bindir)"
else
  PG_BINDIR="/opt/homebrew/Cellar/postgresql@18/18.3/bin"
fi

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

make_query_vec() {
  awk -v dim="$DIM" 'BEGIN {
    printf "[";
    for (i = 1; i <= dim; i++) {
      if (i > 1) printf ",";
      printf "%.6f", (((i * 19) % 1000) / 1000.0);
    }
    printf "]";
  }'
}

echo "============================================================"
echo "sorted_hnsw fixed-graph search benchmark"
echo "============================================================"
echo "rows: $ROWS"
echo "runs: $RUNS"
echo "dim:  $DIM"
echo "mode: $MODE"
echo "port: $PORT"
echo

make -C "$ROOT_DIR" install >/dev/null

TMP_DIR="$(mktemp -d "$TMP_ROOT/pg_sorted_hnsw_fixed.XXXXXX")"
"$PG_BINDIR/initdb" -D "$TMP_DIR/data" -A trust --no-locale >/dev/null 2>&1

cat >> "$TMP_DIR/data/postgresql.conf" <<'PGCONF'
shared_buffers = 256MB
listen_addresses = ''
fsync = on
max_wal_size = 1GB
log_min_messages = warning
PGCONF

"$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -l "$TMP_DIR/postmaster.log" \
  -o "-k $TMP_DIR -p $PORT" start >/dev/null

PSQL <<SQL
CREATE EXTENSION pg_sorted_heap;
CREATE TABLE bench(id int PRIMARY KEY, v svec($DIM));
INSERT INTO bench
SELECT g,
       ('[' ||
        string_agg(
          ((((g * (((d * 17) % 97) + 1)) + (d * 13)) % 1000)::float / 1000.0)::text,
          ',' ORDER BY d
        ) ||
        ']')::svec($DIM)
FROM generate_series(1, $ROWS) AS g
CROSS JOIN LATERAL generate_series(1, $DIM) AS d
GROUP BY g;
CREATE INDEX bench_idx ON bench USING sorted_hnsw(v) WITH (m=16, ef_construction=64);
ANALYZE bench;
SQL

if [[ "$MODE" = "fresh" ]]; then
  echo "Built fixed graph once; measuring fresh-backend ordered scans on the same index."
else
  echo "Built fixed graph once; measuring warm same-backend ordered scans on the same index."
fi
echo

QUERY_VEC="$(make_query_vec)"

if [[ "$MODE" = "fresh" ]]; then
  sum=0
  for i in $(seq 1 "$RUNS"); do
    ms=$(PSQL <<SQL | awk '/Execution Time/ {print $(NF-1)}'
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS)
SELECT id
FROM bench
ORDER BY v <=> '$QUERY_VEC'::svec($DIM)
LIMIT 10;
SQL
)
    echo "run_$i=$ms"
    sum=$(awk -v a="$sum" -v b="$ms" 'BEGIN {printf "%.6f", a + b}')
  done

  awk -v total="$sum" -v runs="$RUNS" 'BEGIN {printf "avg_ms=%.4f\n", total / runs}'
else
  PSQL <<SQL
SET enable_seqscan = off;
DO \$warm\$
BEGIN
  PERFORM id
  FROM bench
  ORDER BY v <=> '$QUERY_VEC'::svec($DIM)
  LIMIT 1;
END
\$warm\$;

CREATE TEMP TABLE bench_times(run int, ms double precision);

DO \$do\$
DECLARE
  i int;
  t0 timestamptz;
BEGIN
  FOR i IN 1..$RUNS LOOP
    t0 := clock_timestamp();
    PERFORM id
    FROM bench
    ORDER BY v <=> '$QUERY_VEC'::svec($DIM)
    LIMIT 10;
    INSERT INTO bench_times(run, ms)
    VALUES (i, EXTRACT(EPOCH FROM clock_timestamp() - t0) * 1000.0);
  END LOOP;
END
\$do\$;

SELECT format('run_%s=%s', run, to_char(ms, 'FM999999990.000'))
FROM bench_times
ORDER BY run;

SELECT format('avg_ms=%s', to_char(avg(ms), 'FM999999990.0000'))
FROM bench_times;
SQL
fi
