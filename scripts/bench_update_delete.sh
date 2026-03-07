#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# UPDATE / DELETE benchmark: sorted_heap vs heap+btree
# ============================================================
#
# Tests:
#   1. Single-row UPDATE by PK (random)
#   2. Single-row DELETE by PK (random) + re-INSERT
#   3. Bulk UPDATE (1000 rows by PK range)
#   4. Mixed workload: 50% SELECT + 30% UPDATE + 20% INSERT
#
# Usage: ./scripts/bench_update_delete.sh [tmp_root] [port] [scale]

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
PORT="${2:-65494}"
N="${3:-1000000}"
PGBENCH_DURATION=10

if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root must be absolute: $TMP_ROOT" >&2; exit 2
fi

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
  [ -n "$TMP_DIR" ] && rm -rf "$TMP_DIR"
}
trap cleanup EXIT

# --- Create ephemeral cluster ---
TMP_DIR="$(mktemp -d "$TMP_ROOT/pg_sh_upd_bench.XXXXXX")"
make -C "$ROOT_DIR" install >/dev/null 2>&1 || true
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

fmt() { printf "%'d" "$1" 2>/dev/null || echo "$1"; }

run_pgbench() {
  local script_file="$1" mode="${2:-prepared}" label="$3"
  local output tps
  output=$("$PG_BINDIR/pgbench" -h "$TMP_DIR" -p "$PORT" postgres \
    -n -T "$PGBENCH_DURATION" -f "$script_file" \
    -M "$mode" -D scale="$N" 2>&1)
  tps=$(echo "$output" | grep -oE 'tps = [0-9]+(\.[0-9]+)?' | grep -oE '[0-9]+(\.[0-9]+)?' | head -1)
  [ -z "$tps" ] && tps="0"
  echo "$tps"
}

explain_avg_ms() {
  local query="$1" iters="${2:-5}"
  local total_ms=0
  for i in $(seq 1 "$iters"); do
    local ms
    ms=$(PSQL -c "EXPLAIN (ANALYZE, COSTS OFF, BUFFERS OFF) $query" \
      | grep 'Execution Time' | grep -oE '[0-9]+\.[0-9]+' | head -1)
    total_ms=$(echo "$total_ms $ms" | awk '{printf "%.3f", $1 + $2}')
  done
  echo "$total_ms $iters" | awk '{printf "%.3f", $1 / $2}'
}

PSQL -c "CREATE EXTENSION pg_sorted_heap"

echo "============================================================"
echo "UPDATE / DELETE benchmark: sorted_heap vs heap+btree"
echo "============================================================"
echo "Scale: $(fmt "$N") rows"
echo "pgbench: ${PGBENCH_DURATION}s per test, prepared mode"
echo ""

# --- Create and load tables ---
echo "--- Loading data ---"

PSQL -c "CREATE TABLE sh_bench(id bigint PRIMARY KEY, category int, val text) USING sorted_heap"
PSQL -c "CREATE TABLE heap_bench(id bigint PRIMARY KEY, category int, val text)"

PSQL -c "INSERT INTO sh_bench SELECT g, (g % 100)::int, 'row-' || g FROM generate_series(1, $N) g"
PSQL -c "INSERT INTO heap_bench SELECT g, (g % 100)::int, 'row-' || g FROM generate_series(1, $N) g"
PSQL -c "SELECT sorted_heap_compact('sh_bench'::regclass)" >/dev/null

echo "  sorted_heap: $(PSQL -c "SELECT pg_size_pretty(pg_total_relation_size('sh_bench'::regclass))")"
echo "  heap+btree:  $(PSQL -c "SELECT pg_size_pretty(pg_total_relation_size('heap_bench'::regclass))")"

# warm cache
PSQL -c "SELECT count(*) FROM sh_bench" >/dev/null
PSQL -c "SELECT count(*) FROM heap_bench" >/dev/null
PSQL -c "CHECKPOINT"

# --- Create pgbench scripts ---
mkdir -p "$TMP_DIR/bench"

# 1. Single-row UPDATE by PK (update non-PK column)
for tbl in sh_bench heap_bench; do
  cat > "$TMP_DIR/bench/update_${tbl}.sql" <<SQL
\\set r random(1, :scale)
UPDATE ${tbl} SET val = 'upd-' || :r WHERE id = :r;
SQL
done

# 2. Single-row DELETE by PK + re-INSERT (to keep row count stable)
for tbl in sh_bench heap_bench; do
  cat > "$TMP_DIR/bench/delete_insert_${tbl}.sql" <<SQL
\\set r random(1, :scale)
DELETE FROM ${tbl} WHERE id = :r;
INSERT INTO ${tbl} VALUES (:r, (:r % 100)::int, 'row-' || :r) ON CONFLICT (id) DO NOTHING;
SQL
done

# 3. Bulk UPDATE (1000 rows by PK range)
for tbl in sh_bench heap_bench; do
  cat > "$TMP_DIR/bench/bulk_update_${tbl}.sql" <<SQL
\\set r random(1, :scale - 1000)
UPDATE ${tbl} SET category = category + 1 WHERE id BETWEEN :r AND :r + 999;
SQL
done

# 4. Mixed workload: 50% SELECT + 30% UPDATE + 20% INSERT (upsert)
for tbl in sh_bench heap_bench; do
  cat > "$TMP_DIR/bench/mixed_${tbl}.sql" <<SQL
\\set r random(1, :scale)
\\set op random(1, 10)
SELECT CASE
  WHEN :op <= 5 THEN (SELECT val FROM ${tbl} WHERE id = :r)
  WHEN :op <= 8 THEN (SELECT val FROM (UPDATE ${tbl} SET val = 'mix-' || :r WHERE id = :r RETURNING val) t)
  ELSE (SELECT val FROM (INSERT INTO ${tbl} VALUES (:r, (:r % 100)::int, 'new-' || :r) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val RETURNING val) t)
END;
SQL
done

echo ""

# ============================================================
# Test 1: Single-row UPDATE
# ============================================================
echo "--- Single-row UPDATE by PK (prepared, ${PGBENCH_DURATION}s) ---"

tps_sh=$(run_pgbench "$TMP_DIR/bench/update_sh_bench.sql" prepared "sh")
tps_heap=$(run_pgbench "$TMP_DIR/bench/update_heap_bench.sql" prepared "heap")

printf "  sorted_heap:  %s tps\n" "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")"
printf "  heap+btree:   %s tps\n" "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"
ratio=$(echo "$tps_sh $tps_heap" | awk '{if($2>0) printf "%.1f%%", ($1/$2)*100; else print "?"}')
echo "  ratio (sh/heap): $ratio"

# EXPLAIN ANALYZE for single UPDATE
mid=$((N / 2))
sh_ms=$(explain_avg_ms "UPDATE sh_bench SET val = 'test' WHERE id = $mid" 5)
heap_ms=$(explain_avg_ms "UPDATE heap_bench SET val = 'test' WHERE id = $mid" 5)
echo "  EXPLAIN ANALYZE: sh ${sh_ms}ms | heap ${heap_ms}ms"

echo ""

# ============================================================
# Test 2: DELETE + re-INSERT
# ============================================================
echo "--- DELETE by PK + re-INSERT (prepared, ${PGBENCH_DURATION}s) ---"

tps_sh=$(run_pgbench "$TMP_DIR/bench/delete_insert_sh_bench.sql" prepared "sh")
tps_heap=$(run_pgbench "$TMP_DIR/bench/delete_insert_heap_bench.sql" prepared "heap")

printf "  sorted_heap:  %s tps\n" "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")"
printf "  heap+btree:   %s tps\n" "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"
ratio=$(echo "$tps_sh $tps_heap" | awk '{if($2>0) printf "%.1f%%", ($1/$2)*100; else print "?"}')
echo "  ratio (sh/heap): $ratio"

# EXPLAIN ANALYZE for single DELETE
sh_ms=$(explain_avg_ms "DELETE FROM sh_bench WHERE id = $((mid + 1))" 1)
heap_ms=$(explain_avg_ms "DELETE FROM heap_bench WHERE id = $((mid + 1))" 1)
echo "  EXPLAIN ANALYZE (1 DELETE): sh ${sh_ms}ms | heap ${heap_ms}ms"
# re-insert deleted rows
PSQL -c "INSERT INTO sh_bench VALUES ($((mid+1)), 0, 'x') ON CONFLICT DO NOTHING" 2>/dev/null || true
PSQL -c "INSERT INTO heap_bench VALUES ($((mid+1)), 0, 'x') ON CONFLICT DO NOTHING" 2>/dev/null || true

echo ""

# ============================================================
# Test 3: Bulk UPDATE (1000 rows)
# ============================================================
echo "--- Bulk UPDATE (1000 rows by PK range, prepared, ${PGBENCH_DURATION}s) ---"

tps_sh=$(run_pgbench "$TMP_DIR/bench/bulk_update_sh_bench.sql" prepared "sh")
tps_heap=$(run_pgbench "$TMP_DIR/bench/bulk_update_heap_bench.sql" prepared "heap")

printf "  sorted_heap:  %s tps\n" "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")"
printf "  heap+btree:   %s tps\n" "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"
ratio=$(echo "$tps_sh $tps_heap" | awk '{if($2>0) printf "%.1f%%", ($1/$2)*100; else print "?"}')
echo "  ratio (sh/heap): $ratio"

echo ""

# ============================================================
# Test 4: Mixed workload
# ============================================================
echo "--- Mixed workload: 50%% SELECT + 30%% UPDATE + 20%% UPSERT (prepared, ${PGBENCH_DURATION}s) ---"

tps_sh=$(run_pgbench "$TMP_DIR/bench/mixed_sh_bench.sql" prepared "sh")
tps_heap=$(run_pgbench "$TMP_DIR/bench/mixed_heap_bench.sql" prepared "heap")

printf "  sorted_heap:  %s tps\n" "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")"
printf "  heap+btree:   %s tps\n" "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"
ratio=$(echo "$tps_sh $tps_heap" | awk '{if($2>0) printf "%.1f%%", ($1/$2)*100; else print "?"}')
echo "  ratio (sh/heap): $ratio"

echo ""

# ============================================================
# Row counts verification
# ============================================================
echo "--- Verification ---"
sh_count=$(PSQL -c "SELECT count(*) FROM sh_bench")
heap_count=$(PSQL -c "SELECT count(*) FROM heap_bench")
echo "  sh_bench:   $sh_count rows"
echo "  heap_bench: $heap_count rows"

echo ""
echo "============================================================"
echo "Benchmark complete."
echo "============================================================"
