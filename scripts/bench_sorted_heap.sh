#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# sorted_heap vs heap+btree vs heap seqscan — Comparative Benchmark
# ============================================================
#
# Measures INSERT rows/sec and SELECT performance at scale,
# comparing three configurations:
#   1. sorted_heap (zone map pruning, no btree index)
#   2. heap + btree PK (standard approach)
#   3. heap without index (seqscan baseline)
#
# Usage: ./scripts/bench_sorted_heap.sh [tmp_root] [port] [scales] [clients]
#   scales:  comma-separated list, e.g. "1000000,10000000,100000000"
#   clients: number of concurrent pgbench clients (default 1)

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
PORT="${2:-65494}"
SCALES_CSV="${3:-1000000,10000000}"
CLIENTS="${4:-1}"
PGBENCH_DURATION=10   # seconds per pgbench SELECT benchmark
EXPLAIN_ITERS=5       # iterations for EXPLAIN ANALYZE averaging

if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root must be absolute: $TMP_ROOT" >&2; exit 2
fi
if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [ "$PORT" -le 1024 ] || [ "$PORT" -ge 65535 ]; then
  echo "port must be 1025..65534" >&2; exit 2
fi

IFS=',' read -ra SCALES <<< "$SCALES_CSV"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if command -v pg_config >/dev/null 2>&1; then
  PG_BINDIR="$(pg_config --bindir)"
else
  PG_BINDIR="/opt/homebrew/Cellar/postgresql@18/18.1_1/bin"
fi

TMP_DIR=""

cleanup() {
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR/data" ]; then
    "$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -m immediate stop >/dev/null 2>&1 || true
  fi
  if [ -n "$TMP_DIR" ]; then
    rm -rf "$TMP_DIR"
  fi
}
trap cleanup EXIT

# --- Create ephemeral cluster with tuned settings ---
TMP_DIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_bench.XXXXXX")"
make -C "$ROOT_DIR" install >/dev/null
"$PG_BINDIR/initdb" -D "$TMP_DIR/data" -A trust --no-locale >/dev/null 2>&1

cat >> "$TMP_DIR/data/postgresql.conf" <<'PGCONF'
shared_buffers = 4GB
work_mem = 256MB
maintenance_work_mem = 2GB
effective_cache_size = 48GB
max_wal_size = 8GB
checkpoint_timeout = 30min
wal_level = minimal
max_wal_senders = 0
fsync = on
synchronous_commit = off
log_min_messages = warning
PGCONF

"$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -l "$TMP_DIR/postmaster.log" \
  -o "-k $TMP_DIR -p $PORT" start >/dev/null

PSQL() {
  "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -v ON_ERROR_STOP=1 -qtAX "$@"
}

PSQL -c "CREATE EXTENSION pg_sorted_heap"

# --- Helper: format number with commas ---
fmt() {
  printf "%'d" "$1" 2>/dev/null || echo "$1"
}

# --- Helper: extract ms from psql \timing output ---
extract_ms() {
  local output="$1"
  local ms
  ms=$(echo "$output" | grep -oE 'Time: [0-9]+(\.[0-9]+)? ms' | grep -oE '[0-9]+(\.[0-9]+)?' | head -1)
  if [ -n "$ms" ]; then
    echo "$ms"
    return
  fi
  local mmss
  mmss=$(echo "$output" | grep -oE 'Time: [0-9]+:[0-9]+\.[0-9]+' | grep -oE '[0-9]+:[0-9]+\.[0-9]+' | head -1)
  if [ -n "$mmss" ]; then
    local min sec
    min=$(echo "$mmss" | cut -d: -f1)
    sec=$(echo "$mmss" | cut -d: -f2)
    echo "$min $sec" | awk '{printf "%.3f", $1*60000 + $2*1000}'
    return
  fi
  echo "0"
}

# --- Helper: run pgbench and extract TPS ---
run_pgbench() {
  local script_file="$1" scale="$2" mode="${3:-simple}" clients="${4:-1}"
  local output
  output=$("$PG_BINDIR/pgbench" -h "$TMP_DIR" -p "$PORT" postgres \
    -n -T "$PGBENCH_DURATION" -f "$script_file" \
    -M "$mode" \
    -D scale="$scale" -c "$clients" -j "$clients" 2>&1)
  local tps
  tps=$(echo "$output" | grep -oE 'tps = [0-9]+(\.[0-9]+)?' | grep -oE '[0-9]+(\.[0-9]+)?' | head -1)
  if [ -z "$tps" ]; then tps="0"; fi
  echo "$tps"
}

# --- Helper: avg execution time from EXPLAIN ANALYZE (N iterations) ---
explain_avg_ms() {
  local query="$1" iters="$2"
  local total_ms=0
  for i in $(seq 1 "$iters"); do
    local ms
    ms=$(PSQL -c "EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF) $query" \
      | grep 'Execution Time' | grep -oE '[0-9]+\.[0-9]+' | head -1)
    total_ms=$(echo "$total_ms $ms" | awk '{printf "%.3f", $1 + $2}')
  done
  echo "$total_ms $iters" | awk '{printf "%.3f", $1 / $2}'
}

# --- Helper: extract blocks from single EXPLAIN ANALYZE ---
explain_blocks() {
  local query="$1"
  PSQL -c "EXPLAIN (ANALYZE, BUFFERS) $query" \
    | grep -oE 'shared hit=[0-9]+' | head -1 | grep -oE '[0-9]+' || echo "?"
}

# --- Create pgbench scripts ---
mkdir -p "$TMP_DIR/bench"

for tbl in sh_bench heap_bench; do
  cat > "$TMP_DIR/bench/point_${tbl}.sql" <<SQL
\\set r random(1, :scale)
SELECT * FROM ${tbl} WHERE id = :r;
SQL
  cat > "$TMP_DIR/bench/narrow_${tbl}.sql" <<SQL
\\set r random(1, :scale - 100)
SELECT count(*) FROM ${tbl} WHERE id BETWEEN :r AND :r + 100;
SQL
  cat > "$TMP_DIR/bench/medium_${tbl}.sql" <<SQL
\\set r random(1, :scale - 5000)
SELECT count(*) FROM ${tbl} WHERE id BETWEEN :r AND :r + 5000;
SQL
  cat > "$TMP_DIR/bench/wide_${tbl}.sql" <<SQL
\\set r random(1, :scale - 100000)
SELECT count(*) FROM ${tbl} WHERE id BETWEEN :r AND :r + 100000;
SQL
done

echo "============================================================"
echo "sorted_heap vs heap+btree vs heap seqscan"
echo "============================================================"
echo "Hardware: $(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo '?') CPUs, $(sysctl -n hw.memsize 2>/dev/null | awk '{printf "%.0f GB", $1/1024/1024/1024}' 2>/dev/null || echo '? GB') RAM"
echo "PG config: shared_buffers=4GB, work_mem=256MB, maintenance_work_mem=2GB"
echo "pgbench: ${PGBENCH_DURATION}s per test, ${CLIENTS} client(s)"
echo "EXPLAIN ANALYZE: best of ${EXPLAIN_ITERS} runs (execution time only)"
echo ""

for N in "${SCALES[@]}"; do
  echo "============================================================"
  echo "=== Scale: $(fmt "$N") rows ==="
  echo "============================================================"

  # --- Create tables ---
  PSQL -c "DROP TABLE IF EXISTS sh_bench CASCADE"
  PSQL -c "DROP TABLE IF EXISTS heap_bench CASCADE"
  PSQL -c "DROP TABLE IF EXISTS heap_noidx CASCADE"

  PSQL -c "CREATE TABLE sh_bench(id bigint PRIMARY KEY, category int, val text) USING sorted_heap"
  PSQL -c "CREATE TABLE heap_bench(id bigint PRIMARY KEY, category int, val text)"
  PSQL -c "CREATE TABLE heap_noidx(id bigint, category int, val text)"

  # ============================================================
  # INSERT benchmark
  # ============================================================
  echo ""
  echo "--- INSERT (bulk load) ---"

  sh_insert_output=$(PSQL -c "\\timing" -c "
    INSERT INTO sh_bench SELECT g, (g % 100)::int, 'row-' || g FROM generate_series(1, $N) g;
  " 2>&1)
  sh_insert_ms=$(extract_ms "$sh_insert_output")

  heap_insert_output=$(PSQL -c "\\timing" -c "
    INSERT INTO heap_bench SELECT g, (g % 100)::int, 'row-' || g FROM generate_series(1, $N) g;
  " 2>&1)
  heap_insert_ms=$(extract_ms "$heap_insert_output")

  noidx_insert_output=$(PSQL -c "\\timing" -c "
    INSERT INTO heap_noidx SELECT g, (g % 100)::int, 'row-' || g FROM generate_series(1, $N) g;
  " 2>&1)
  noidx_insert_ms=$(extract_ms "$noidx_insert_output")

  sh_insert_rps=$(echo "$N $sh_insert_ms" | awk '{if($2>0) printf "%.0f", $1/($2/1000); else print "?"}')
  heap_insert_rps=$(echo "$N $heap_insert_ms" | awk '{if($2>0) printf "%.0f", $1/($2/1000); else print "?"}')
  noidx_insert_rps=$(echo "$N $noidx_insert_ms" | awk '{if($2>0) printf "%.0f", $1/($2/1000); else print "?"}')
  sh_insert_sec=$(echo "$sh_insert_ms" | awk '{printf "%.1f", $1/1000}')
  heap_insert_sec=$(echo "$heap_insert_ms" | awk '{printf "%.1f", $1/1000}')
  noidx_insert_sec=$(echo "$noidx_insert_ms" | awk '{printf "%.1f", $1/1000}')

  echo "  sorted_heap:  $(fmt "$sh_insert_rps") rows/sec  (${sh_insert_sec}s)"
  echo "  heap+btree:   $(fmt "$heap_insert_rps") rows/sec  (${heap_insert_sec}s)"
  echo "  heap (no idx): $(fmt "$noidx_insert_rps") rows/sec  (${noidx_insert_sec}s)"

  # ============================================================
  # Compact
  # ============================================================
  echo ""
  echo "--- compact (sorted_heap only) ---"

  compact_output=$(PSQL -c "\\timing" -c "SELECT sorted_heap_compact('sh_bench'::regclass)" 2>&1)
  compact_ms=$(extract_ms "$compact_output")
  compact_sec=$(echo "$compact_ms" | awk '{printf "%.1f", $1/1000}')
  echo "  compact:      ${compact_sec}s"

  # ============================================================
  # Table sizes
  # ============================================================
  echo ""
  echo "--- Table size ---"

  sh_total=$(PSQL -c "SELECT pg_size_pretty(pg_total_relation_size('sh_bench'::regclass))")
  heap_table=$(PSQL -c "SELECT pg_size_pretty(pg_relation_size('heap_bench'::regclass))")
  heap_idx=$(PSQL -c "SELECT pg_size_pretty(pg_indexes_size('heap_bench'::regclass))")
  heap_total=$(PSQL -c "SELECT pg_size_pretty(pg_total_relation_size('heap_bench'::regclass))")
  noidx_total=$(PSQL -c "SELECT pg_size_pretty(pg_total_relation_size('heap_noidx'::regclass))")

  echo "  sorted_heap:   $sh_total"
  echo "  heap+btree:    $heap_total  (table $heap_table + index $heap_idx)"
  echo "  heap (no idx): $noidx_total"

  # ============================================================
  # CHECKPOINT + warm cache
  # ============================================================
  PSQL -c "CHECKPOINT"
  PSQL -c "SELECT count(*) FROM sh_bench" >/dev/null
  PSQL -c "SELECT count(*) FROM heap_bench" >/dev/null
  PSQL -c "SELECT count(*) FROM heap_noidx" >/dev/null

  # ============================================================
  # EXPLAIN ANALYZE: execution time + blocks
  # ============================================================
  echo ""
  echo "--- EXPLAIN ANALYZE: execution time (avg ${EXPLAIN_ITERS} runs) + buffers ---"

  mid=$((N / 2))

  # Point query
  sh_ms=$(explain_avg_ms "SELECT * FROM sh_bench WHERE id = $mid" "$EXPLAIN_ITERS")
  sh_blk=$(explain_blocks "SELECT * FROM sh_bench WHERE id = $mid")
  heap_ms=$(explain_avg_ms "SELECT * FROM heap_bench WHERE id = $mid" "$EXPLAIN_ITERS")
  heap_blk=$(explain_blocks "SELECT * FROM heap_bench WHERE id = $mid")
  noidx_ms=$(explain_avg_ms "SELECT * FROM heap_noidx WHERE id = $mid" "$EXPLAIN_ITERS")
  noidx_blk=$(explain_blocks "SELECT * FROM heap_noidx WHERE id = $mid")
  printf "  Point (1 row):   sh %7sms / %5s bufs | btree %7sms / %5s bufs | seq %9sms / %s bufs\n" \
    "$sh_ms" "$sh_blk" "$heap_ms" "$heap_blk" "$noidx_ms" "$noidx_blk"

  # Narrow range (100)
  lo=$mid; hi=$((mid + 100))
  sh_ms=$(explain_avg_ms "SELECT * FROM sh_bench WHERE id BETWEEN $lo AND $hi" "$EXPLAIN_ITERS")
  sh_blk=$(explain_blocks "SELECT * FROM sh_bench WHERE id BETWEEN $lo AND $hi")
  heap_ms=$(explain_avg_ms "SELECT * FROM heap_bench WHERE id BETWEEN $lo AND $hi" "$EXPLAIN_ITERS")
  heap_blk=$(explain_blocks "SELECT * FROM heap_bench WHERE id BETWEEN $lo AND $hi")
  noidx_ms=$(explain_avg_ms "SELECT * FROM heap_noidx WHERE id BETWEEN $lo AND $hi" "$EXPLAIN_ITERS")
  noidx_blk=$(explain_blocks "SELECT * FROM heap_noidx WHERE id BETWEEN $lo AND $hi")
  printf "  Narrow (100):    sh %7sms / %5s bufs | btree %7sms / %5s bufs | seq %9sms / %s bufs\n" \
    "$sh_ms" "$sh_blk" "$heap_ms" "$heap_blk" "$noidx_ms" "$noidx_blk"

  # Medium range (5K)
  hi=$((mid + 5000))
  sh_ms=$(explain_avg_ms "SELECT count(*) FROM sh_bench WHERE id BETWEEN $lo AND $hi" "$EXPLAIN_ITERS")
  sh_blk=$(explain_blocks "SELECT count(*) FROM sh_bench WHERE id BETWEEN $lo AND $hi")
  heap_ms=$(explain_avg_ms "SELECT count(*) FROM heap_bench WHERE id BETWEEN $lo AND $hi" "$EXPLAIN_ITERS")
  heap_blk=$(explain_blocks "SELECT count(*) FROM heap_bench WHERE id BETWEEN $lo AND $hi")
  noidx_ms=$(explain_avg_ms "SELECT count(*) FROM heap_noidx WHERE id BETWEEN $lo AND $hi" "$EXPLAIN_ITERS")
  noidx_blk=$(explain_blocks "SELECT count(*) FROM heap_noidx WHERE id BETWEEN $lo AND $hi")
  printf "  Medium (5K):     sh %7sms / %5s bufs | btree %7sms / %5s bufs | seq %9sms / %s bufs\n" \
    "$sh_ms" "$sh_blk" "$heap_ms" "$heap_blk" "$noidx_ms" "$noidx_blk"

  # Wide range (100K)
  if [ "$N" -ge 200000 ]; then
    hi=$((mid + 100000))
    sh_ms=$(explain_avg_ms "SELECT count(*) FROM sh_bench WHERE id BETWEEN $lo AND $hi" "$EXPLAIN_ITERS")
    sh_blk=$(explain_blocks "SELECT count(*) FROM sh_bench WHERE id BETWEEN $lo AND $hi")
    heap_ms=$(explain_avg_ms "SELECT count(*) FROM heap_bench WHERE id BETWEEN $lo AND $hi" "$EXPLAIN_ITERS")
    heap_blk=$(explain_blocks "SELECT count(*) FROM heap_bench WHERE id BETWEEN $lo AND $hi")
    noidx_ms=$(explain_avg_ms "SELECT count(*) FROM heap_noidx WHERE id BETWEEN $lo AND $hi" "$EXPLAIN_ITERS")
    noidx_blk=$(explain_blocks "SELECT count(*) FROM heap_noidx WHERE id BETWEEN $lo AND $hi")
    printf "  Wide (100K):     sh %7sms / %5s bufs | btree %7sms / %5s bufs | seq %9sms / %s bufs\n" \
      "$sh_ms" "$sh_blk" "$heap_ms" "$heap_blk" "$noidx_ms" "$noidx_blk"
  fi

  # ============================================================
  # pgbench throughput (sorted_heap vs heap+btree only)
  # ============================================================
  echo ""
  echo "--- pgbench throughput (${PGBENCH_DURATION}s, 1 client) ---"

  tps_sh=$(run_pgbench "$TMP_DIR/bench/point_sh_bench.sql" "$N")
  tps_heap=$(run_pgbench "$TMP_DIR/bench/point_heap_bench.sql" "$N")
  printf "  Point (1 row):   sorted_heap %s tps | heap+btree %s tps\n" \
    "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")" \
    "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"

  tps_sh=$(run_pgbench "$TMP_DIR/bench/narrow_sh_bench.sql" "$N")
  tps_heap=$(run_pgbench "$TMP_DIR/bench/narrow_heap_bench.sql" "$N")
  printf "  Narrow (100):    sorted_heap %s tps | heap+btree %s tps\n" \
    "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")" \
    "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"

  tps_sh=$(run_pgbench "$TMP_DIR/bench/medium_sh_bench.sql" "$N")
  tps_heap=$(run_pgbench "$TMP_DIR/bench/medium_heap_bench.sql" "$N")
  printf "  Medium (5K):     sorted_heap %s tps | heap+btree %s tps\n" \
    "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")" \
    "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"

  if [ "$N" -ge 200000 ]; then
    tps_sh=$(run_pgbench "$TMP_DIR/bench/wide_sh_bench.sql" "$N")
    tps_heap=$(run_pgbench "$TMP_DIR/bench/wide_heap_bench.sql" "$N")
    printf "  Wide (100K):     sorted_heap %s tps | heap+btree %s tps\n" \
      "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")" \
      "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"
  fi

  # ============================================================
  # pgbench throughput — prepared statements (plan once, execute many)
  # ============================================================
  echo ""
  echo "--- pgbench throughput — prepared (${PGBENCH_DURATION}s, 1 client) ---"

  tps_sh=$(run_pgbench "$TMP_DIR/bench/point_sh_bench.sql" "$N" prepared)
  tps_heap=$(run_pgbench "$TMP_DIR/bench/point_heap_bench.sql" "$N" prepared)
  printf "  Point (1 row):   sorted_heap %s tps | heap+btree %s tps\n" \
    "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")" \
    "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"

  tps_sh=$(run_pgbench "$TMP_DIR/bench/narrow_sh_bench.sql" "$N" prepared)
  tps_heap=$(run_pgbench "$TMP_DIR/bench/narrow_heap_bench.sql" "$N" prepared)
  printf "  Narrow (100):    sorted_heap %s tps | heap+btree %s tps\n" \
    "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")" \
    "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"

  tps_sh=$(run_pgbench "$TMP_DIR/bench/medium_sh_bench.sql" "$N" prepared)
  tps_heap=$(run_pgbench "$TMP_DIR/bench/medium_heap_bench.sql" "$N" prepared)
  printf "  Medium (5K):     sorted_heap %s tps | heap+btree %s tps\n" \
    "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")" \
    "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"

  if [ "$N" -ge 200000 ]; then
    tps_sh=$(run_pgbench "$TMP_DIR/bench/wide_sh_bench.sql" "$N" prepared)
    tps_heap=$(run_pgbench "$TMP_DIR/bench/wide_heap_bench.sql" "$N" prepared)
    printf "  Wide (100K):     sorted_heap %s tps | heap+btree %s tps\n" \
      "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")" \
      "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"
  fi

  # ============================================================
  # pgbench throughput — multi-client prepared (only when CLIENTS > 1)
  # ============================================================
  if [ "$CLIENTS" -gt 1 ]; then
    echo ""
    echo "--- pgbench throughput — prepared (${PGBENCH_DURATION}s, ${CLIENTS} clients) ---"

    tps_sh=$(run_pgbench "$TMP_DIR/bench/point_sh_bench.sql" "$N" prepared "$CLIENTS")
    tps_heap=$(run_pgbench "$TMP_DIR/bench/point_heap_bench.sql" "$N" prepared "$CLIENTS")
    printf "  Point (1 row):   sorted_heap %s tps | heap+btree %s tps\n" \
      "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")" \
      "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"

    tps_sh=$(run_pgbench "$TMP_DIR/bench/narrow_sh_bench.sql" "$N" prepared "$CLIENTS")
    tps_heap=$(run_pgbench "$TMP_DIR/bench/narrow_heap_bench.sql" "$N" prepared "$CLIENTS")
    printf "  Narrow (100):    sorted_heap %s tps | heap+btree %s tps\n" \
      "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")" \
      "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"

    tps_sh=$(run_pgbench "$TMP_DIR/bench/medium_sh_bench.sql" "$N" prepared "$CLIENTS")
    tps_heap=$(run_pgbench "$TMP_DIR/bench/medium_heap_bench.sql" "$N" prepared "$CLIENTS")
    printf "  Medium (5K):     sorted_heap %s tps | heap+btree %s tps\n" \
      "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")" \
      "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"

    if [ "$N" -ge 200000 ]; then
      tps_sh=$(run_pgbench "$TMP_DIR/bench/wide_sh_bench.sql" "$N" prepared "$CLIENTS")
      tps_heap=$(run_pgbench "$TMP_DIR/bench/wide_heap_bench.sql" "$N" prepared "$CLIENTS")
      printf "  Wide (100K):     sorted_heap %s tps | heap+btree %s tps\n" \
        "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")" \
        "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"
    fi
  fi

  echo ""
done

echo "============================================================"
echo "Benchmark complete."
echo "============================================================"
