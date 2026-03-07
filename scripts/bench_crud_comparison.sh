#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# CRUD benchmark: sorted_heap vs heap+btree vs pgvector
# ============================================================
#
# Compares INSERT / SELECT / UPDATE / DELETE performance across
# three storage strategies for tables with vector embeddings:
#
#   1. sorted_heap  — sorted_heap AM + svec(DIM)
#   2. heap+btree   — standard heap + btree PK + svec(DIM) (no index on vectors)
#   3. pgvector     — standard heap + btree PK + vector(DIM)
#
# Tests:
#   1. Bulk INSERT (COPY-style via generate_series)
#   2. Single-row SELECT by PK (prepared, pgbench)
#   3. Single-row UPDATE non-vector col by PK (prepared, pgbench)
#   4. Single-row UPDATE vector col by PK (prepared, pgbench)
#   5. Single-row DELETE + re-INSERT (prepared, pgbench)
#   6. Bulk SELECT (1000-row PK range)
#   7. Mixed OLTP: 50% SELECT + 30% UPDATE + 20% UPSERT
#   8. Table sizes
#
# Usage: ./scripts/bench_crud_comparison.sh [tmp_root] [port] [scale] [dim]

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
PORT="${2:-65494}"
N="${3:-500000}"
DIM="${4:-128}"
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
TMP_DIR="$(mktemp -d "$TMP_ROOT/pg_crud_bench.XXXXXX")"
make -C "$ROOT_DIR" install >/dev/null 2>&1 || true
"$PG_BINDIR/initdb" -D "$TMP_DIR/data" -A trust --no-locale >/dev/null 2>&1

cat >> "$TMP_DIR/data/postgresql.conf" <<PGCONF
shared_preload_libraries = 'pg_sorted_heap'
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
  local script_file="$1" mode="${2:-prepared}" clients="${3:-1}"
  local output tps
  output=$("$PG_BINDIR/pgbench" -h "$TMP_DIR" -p "$PORT" postgres \
    -n -T "$PGBENCH_DURATION" -f "$script_file" \
    -M "$mode" -c "$clients" -D scale="$N" -D dim="$DIM" 2>&1)
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
    [ -z "$ms" ] && ms="0"
    total_ms=$(echo "$total_ms $ms" | awk '{printf "%.3f", $1 + $2}')
  done
  echo "$total_ms $iters" | awk '{printf "%.3f", $1 / $2}'
}

# --- Setup extensions ---
PSQL -c "CREATE EXTENSION pg_sorted_heap"
HAS_PGVECTOR=false
if PSQL -c "CREATE EXTENSION vector" 2>/dev/null; then
  HAS_PGVECTOR=true
fi

echo "============================================================"
echo "CRUD benchmark: sorted_heap vs heap+btree vs pgvector"
echo "============================================================"
echo "Scale: $(fmt "$N") rows   Dimensions: $DIM"
echo "pgbench: ${PGBENCH_DURATION}s per test, prepared mode"
echo "pgvector: $HAS_PGVECTOR"
echo ""

# ============================================================
# Create tables
# ============================================================

# Helper: generate a random vector string [0.1,0.2,...] for DIM dimensions
# We use a SQL function for this to keep it server-side
PSQL -c "
CREATE OR REPLACE FUNCTION random_vector_text(d int) RETURNS text AS \$\$
  SELECT '[' || string_agg((random()::float4)::text, ',') || ']'
  FROM generate_series(1, d);
\$\$ LANGUAGE sql VOLATILE;
"

echo "--- Creating tables ---"

PSQL -c "CREATE TABLE sh_bench(
  id bigint PRIMARY KEY,
  category int,
  val text,
  embedding svec($DIM)
) USING sorted_heap"

PSQL -c "CREATE TABLE heap_bench(
  id bigint PRIMARY KEY,
  category int,
  val text,
  embedding svec($DIM)
)"

if $HAS_PGVECTOR; then
  PSQL -c "CREATE TABLE pgv_bench(
    id bigint PRIMARY KEY,
    category int,
    val text,
    embedding vector($DIM)
  )"
fi

# ============================================================
# Test 1: Bulk INSERT
# ============================================================
echo ""
echo "--- Test 1: Bulk INSERT $(fmt "$N") rows ---"

t0=$(date +%s%N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1e9))')
PSQL -c "INSERT INTO sh_bench
  SELECT g, (g % 100)::int, 'row-' || g,
         random_vector_text($DIM)::svec
  FROM generate_series(1, $N) g"
t1=$(date +%s%N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1e9))')
sh_insert_ms=$(( (t1 - t0) / 1000000 ))
sh_rps=$(echo "$N $sh_insert_ms" | awk '{if($2>0) printf "%.0f", $1/($2/1000.0); else print "?"}')

t0=$(date +%s%N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1e9))')
PSQL -c "INSERT INTO heap_bench
  SELECT g, (g % 100)::int, 'row-' || g,
         random_vector_text($DIM)::svec
  FROM generate_series(1, $N) g"
t1=$(date +%s%N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1e9))')
heap_insert_ms=$(( (t1 - t0) / 1000000 ))
heap_rps=$(echo "$N $heap_insert_ms" | awk '{if($2>0) printf "%.0f", $1/($2/1000.0); else print "?"}')

if $HAS_PGVECTOR; then
  t0=$(date +%s%N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1e9))')
  PSQL -c "INSERT INTO pgv_bench
    SELECT g, (g % 100)::int, 'row-' || g,
           random_vector_text($DIM)::vector
    FROM generate_series(1, $N) g"
  t1=$(date +%s%N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1e9))')
  pgv_insert_ms=$(( (t1 - t0) / 1000000 ))
  pgv_rps=$(echo "$N $pgv_insert_ms" | awk '{if($2>0) printf "%.0f", $1/($2/1000.0); else print "?"}')
fi

# Compact sorted_heap
t0=$(date +%s%N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1e9))')
PSQL -c "SELECT sorted_heap_compact('sh_bench'::regclass)" >/dev/null
t1=$(date +%s%N 2>/dev/null || python3 -c 'import time; print(int(time.time()*1e9))')
compact_ms=$(( (t1 - t0) / 1000000 ))

printf "  sorted_heap:  %s rows/s  (%s ms + compact %s ms)\n" \
  "$(fmt "$sh_rps")" "$(fmt "$sh_insert_ms")" "$(fmt "$compact_ms")"
printf "  heap+btree:   %s rows/s  (%s ms)\n" \
  "$(fmt "$heap_rps")" "$(fmt "$heap_insert_ms")"
if $HAS_PGVECTOR; then
  printf "  pgvector:     %s rows/s  (%s ms)\n" \
    "$(fmt "$pgv_rps")" "$(fmt "$pgv_insert_ms")"
fi

# ============================================================
# Table sizes
# ============================================================
echo ""
echo "--- Table sizes ---"
echo "  sorted_heap:  $(PSQL -c "SELECT pg_size_pretty(pg_total_relation_size('sh_bench'::regclass))")"
echo "  heap+btree:   $(PSQL -c "SELECT pg_size_pretty(pg_total_relation_size('heap_bench'::regclass))")"
if $HAS_PGVECTOR; then
  echo "  pgvector:     $(PSQL -c "SELECT pg_size_pretty(pg_total_relation_size('pgv_bench'::regclass))")"
fi

# Warm cache
PSQL -c "SELECT count(*) FROM sh_bench" >/dev/null
PSQL -c "SELECT count(*) FROM heap_bench" >/dev/null
$HAS_PGVECTOR && PSQL -c "SELECT count(*) FROM pgv_bench" >/dev/null
PSQL -c "CHECKPOINT"

# ============================================================
# Create pgbench scripts
# ============================================================
mkdir -p "$TMP_DIR/bench"

# --- 2a. Single-row SELECT by PK (without embedding — fair index comparison) ---
for tbl in sh_bench heap_bench; do
  cat > "$TMP_DIR/bench/select_${tbl}.sql" <<SQL
\\set r random(1, :scale)
SELECT id, category, val FROM ${tbl} WHERE id = :r;
SQL
done
if $HAS_PGVECTOR; then
  cat > "$TMP_DIR/bench/select_pgv_bench.sql" <<SQL
\\set r random(1, :scale)
SELECT id, category, val FROM pgv_bench WHERE id = :r;
SQL
fi

# --- 2b. Single-row SELECT by PK (with embedding — measures detoast/serialization) ---
for tbl in sh_bench heap_bench; do
  cat > "$TMP_DIR/bench/select_emb_${tbl}.sql" <<SQL
\\set r random(1, :scale)
SELECT id, category, val, embedding FROM ${tbl} WHERE id = :r;
SQL
done
if $HAS_PGVECTOR; then
  cat > "$TMP_DIR/bench/select_emb_pgv_bench.sql" <<SQL
\\set r random(1, :scale)
SELECT id, category, val, embedding FROM pgv_bench WHERE id = :r;
SQL
fi

# --- 3. Single-row UPDATE non-vector col by PK ---
for tbl in sh_bench heap_bench; do
  cat > "$TMP_DIR/bench/update_${tbl}.sql" <<SQL
\\set r random(1, :scale)
UPDATE ${tbl} SET val = 'upd-' || :r WHERE id = :r;
SQL
done
if $HAS_PGVECTOR; then
  cat > "$TMP_DIR/bench/update_pgv_bench.sql" <<SQL
\\set r random(1, :scale)
UPDATE pgv_bench SET val = 'upd-' || :r WHERE id = :r;
SQL
fi

# --- 4. Single-row UPDATE vector col by PK ---
# Pre-generate a fixed vector for the update (avoid random_vector_text overhead)
FIXED_VEC=$(PSQL -c "SELECT random_vector_text($DIM)")
for tbl in sh_bench heap_bench; do
  typcast="svec"
  cat > "$TMP_DIR/bench/update_vec_${tbl}.sql" <<SQL
\\set r random(1, :scale)
UPDATE ${tbl} SET embedding = '${FIXED_VEC}'::${typcast} WHERE id = :r;
SQL
done
if $HAS_PGVECTOR; then
  cat > "$TMP_DIR/bench/update_vec_pgv_bench.sql" <<SQL
\\set r random(1, :scale)
UPDATE pgv_bench SET embedding = '${FIXED_VEC}'::vector WHERE id = :r;
SQL
fi

# --- 5. DELETE + re-INSERT ---
for tbl in sh_bench heap_bench; do
  cat > "$TMP_DIR/bench/delete_insert_${tbl}.sql" <<SQL
\\set r random(1, :scale)
DELETE FROM ${tbl} WHERE id = :r;
INSERT INTO ${tbl} VALUES (:r, (:r % 100)::int, 'row-' || :r, '${FIXED_VEC}'::svec) ON CONFLICT (id) DO NOTHING;
SQL
done
if $HAS_PGVECTOR; then
  cat > "$TMP_DIR/bench/delete_insert_pgv_bench.sql" <<SQL
\\set r random(1, :scale)
DELETE FROM pgv_bench WHERE id = :r;
INSERT INTO pgv_bench VALUES (:r, (:r % 100)::int, 'row-' || :r, '${FIXED_VEC}'::vector) ON CONFLICT (id) DO NOTHING;
SQL
fi

# --- 6. Bulk SELECT (1000 rows by PK range) ---
for tbl in sh_bench heap_bench; do
  cat > "$TMP_DIR/bench/bulk_select_${tbl}.sql" <<SQL
\\set r random(1, :scale - 1000)
SELECT id, category, val FROM ${tbl} WHERE id BETWEEN :r AND :r + 999;
SQL
done
if $HAS_PGVECTOR; then
  cat > "$TMP_DIR/bench/bulk_select_pgv_bench.sql" <<SQL
\\set r random(1, :scale - 1000)
SELECT id, category, val FROM pgv_bench WHERE id BETWEEN :r AND :r + 999;
SQL
fi

# --- 7. Mixed OLTP (separate scripts with weights: 5 SELECT, 3 UPDATE, 2 UPSERT) ---
for tbl in sh_bench heap_bench; do
  typcast="svec"
  cat > "$TMP_DIR/bench/mixed_sel_${tbl}.sql" <<SQL
\\set r random(1, :scale)
SELECT val FROM ${tbl} WHERE id = :r;
SQL
  cat > "$TMP_DIR/bench/mixed_upd_${tbl}.sql" <<SQL
\\set r random(1, :scale)
UPDATE ${tbl} SET val = 'mix-' || :r WHERE id = :r;
SQL
  cat > "$TMP_DIR/bench/mixed_ups_${tbl}.sql" <<SQL
\\set r random(1, :scale)
INSERT INTO ${tbl} VALUES (:r, (:r % 100)::int, 'new-' || :r, '${FIXED_VEC}'::${typcast}) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val;
SQL
done
if $HAS_PGVECTOR; then
  cat > "$TMP_DIR/bench/mixed_sel_pgv_bench.sql" <<SQL
\\set r random(1, :scale)
SELECT val FROM pgv_bench WHERE id = :r;
SQL
  cat > "$TMP_DIR/bench/mixed_upd_pgv_bench.sql" <<SQL
\\set r random(1, :scale)
UPDATE pgv_bench SET val = 'mix-' || :r WHERE id = :r;
SQL
  cat > "$TMP_DIR/bench/mixed_ups_pgv_bench.sql" <<SQL
\\set r random(1, :scale)
INSERT INTO pgv_bench VALUES (:r, (:r % 100)::int, 'new-' || :r, '${FIXED_VEC}'::vector) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val;
SQL
fi

# ============================================================
# Test 2a: Single-row SELECT by PK (without embedding)
# ============================================================
echo ""
echo "--- Test 2a: Single-row SELECT by PK — no embedding (prepared, ${PGBENCH_DURATION}s) ---"

tps_sh=$(run_pgbench "$TMP_DIR/bench/select_sh_bench.sql")
tps_heap=$(run_pgbench "$TMP_DIR/bench/select_heap_bench.sql")
tps_pgv="n/a"
$HAS_PGVECTOR && tps_pgv=$(run_pgbench "$TMP_DIR/bench/select_pgv_bench.sql")

printf "  sorted_heap:  %s tps\n" "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")"
printf "  heap+btree:   %s tps\n" "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"
if $HAS_PGVECTOR; then
  printf "  pgvector:     %s tps\n" "$(fmt "$(echo "$tps_pgv" | awk '{printf "%.0f",$1}')")"
fi

mid=$((N / 2))
sh_ms=$(explain_avg_ms "SELECT id, category, val FROM sh_bench WHERE id = $mid")
heap_ms=$(explain_avg_ms "SELECT id, category, val FROM heap_bench WHERE id = $mid")
pgv_ms="n/a"
$HAS_PGVECTOR && pgv_ms=$(explain_avg_ms "SELECT id, category, val FROM pgv_bench WHERE id = $mid")
echo "  EXPLAIN: sh ${sh_ms}ms | heap ${heap_ms}ms | pgv ${pgv_ms}ms"

# ============================================================
# Test 2b: Single-row SELECT by PK (with embedding)
# ============================================================
echo ""
echo "--- Test 2b: Single-row SELECT by PK — with embedding (prepared, ${PGBENCH_DURATION}s) ---"

tps_sh=$(run_pgbench "$TMP_DIR/bench/select_emb_sh_bench.sql")
tps_heap=$(run_pgbench "$TMP_DIR/bench/select_emb_heap_bench.sql")
tps_pgv="n/a"
$HAS_PGVECTOR && tps_pgv=$(run_pgbench "$TMP_DIR/bench/select_emb_pgv_bench.sql")

printf "  sorted_heap:  %s tps\n" "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")"
printf "  heap+btree:   %s tps\n" "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"
if $HAS_PGVECTOR; then
  printf "  pgvector:     %s tps\n" "$(fmt "$(echo "$tps_pgv" | awk '{printf "%.0f",$1}')")"
fi

# ============================================================
# Test 3: Single-row UPDATE (non-vector col)
# ============================================================
echo ""
echo "--- Test 3: UPDATE non-vector col by PK (prepared, ${PGBENCH_DURATION}s) ---"

tps_sh=$(run_pgbench "$TMP_DIR/bench/update_sh_bench.sql")
tps_heap=$(run_pgbench "$TMP_DIR/bench/update_heap_bench.sql")
tps_pgv="n/a"
$HAS_PGVECTOR && tps_pgv=$(run_pgbench "$TMP_DIR/bench/update_pgv_bench.sql")

printf "  sorted_heap:  %s tps\n" "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")"
printf "  heap+btree:   %s tps\n" "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"
if $HAS_PGVECTOR; then
  printf "  pgvector:     %s tps\n" "$(fmt "$(echo "$tps_pgv" | awk '{printf "%.0f",$1}')")"
fi

# ============================================================
# Test 4: Single-row UPDATE (vector col)
# ============================================================
echo ""
echo "--- Test 4: UPDATE vector col by PK (prepared, ${PGBENCH_DURATION}s) ---"

tps_sh=$(run_pgbench "$TMP_DIR/bench/update_vec_sh_bench.sql")
tps_heap=$(run_pgbench "$TMP_DIR/bench/update_vec_heap_bench.sql")
tps_pgv="n/a"
$HAS_PGVECTOR && tps_pgv=$(run_pgbench "$TMP_DIR/bench/update_vec_pgv_bench.sql")

printf "  sorted_heap:  %s tps\n" "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")"
printf "  heap+btree:   %s tps\n" "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"
if $HAS_PGVECTOR; then
  printf "  pgvector:     %s tps\n" "$(fmt "$(echo "$tps_pgv" | awk '{printf "%.0f",$1}')")"
fi

# ============================================================
# Test 5: DELETE + re-INSERT
# ============================================================
echo ""
echo "--- Test 5: DELETE + re-INSERT by PK (prepared, ${PGBENCH_DURATION}s) ---"

tps_sh=$(run_pgbench "$TMP_DIR/bench/delete_insert_sh_bench.sql")
tps_heap=$(run_pgbench "$TMP_DIR/bench/delete_insert_heap_bench.sql")
tps_pgv="n/a"
$HAS_PGVECTOR && tps_pgv=$(run_pgbench "$TMP_DIR/bench/delete_insert_pgv_bench.sql")

printf "  sorted_heap:  %s tps\n" "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")"
printf "  heap+btree:   %s tps\n" "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"
if $HAS_PGVECTOR; then
  printf "  pgvector:     %s tps\n" "$(fmt "$(echo "$tps_pgv" | awk '{printf "%.0f",$1}')")"
fi

# ============================================================
# Test 6: Bulk SELECT (1000 rows)
# ============================================================
echo ""
echo "--- Test 6: Bulk SELECT 1000 rows by PK range (prepared, ${PGBENCH_DURATION}s) ---"

tps_sh=$(run_pgbench "$TMP_DIR/bench/bulk_select_sh_bench.sql")
tps_heap=$(run_pgbench "$TMP_DIR/bench/bulk_select_heap_bench.sql")
tps_pgv="n/a"
$HAS_PGVECTOR && tps_pgv=$(run_pgbench "$TMP_DIR/bench/bulk_select_pgv_bench.sql")

printf "  sorted_heap:  %s tps\n" "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")"
printf "  heap+btree:   %s tps\n" "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"
if $HAS_PGVECTOR; then
  printf "  pgvector:     %s tps\n" "$(fmt "$(echo "$tps_pgv" | awk '{printf "%.0f",$1}')")"
fi

sh_ms=$(explain_avg_ms "SELECT id, category, val FROM sh_bench WHERE id BETWEEN $mid AND $((mid+999))")
heap_ms=$(explain_avg_ms "SELECT id, category, val FROM heap_bench WHERE id BETWEEN $mid AND $((mid+999))")
pgv_ms="n/a"
$HAS_PGVECTOR && pgv_ms=$(explain_avg_ms "SELECT id, category, val FROM pgv_bench WHERE id BETWEEN $mid AND $((mid+999))")
echo "  EXPLAIN: sh ${sh_ms}ms | heap ${heap_ms}ms | pgv ${pgv_ms}ms"

# ============================================================
# Test 7: Mixed OLTP (weighted scripts: 5 SELECT + 3 UPDATE + 2 UPSERT)
# ============================================================
echo ""
echo "--- Test 7: Mixed OLTP 50%% SELECT + 30%% UPDATE + 20%% UPSERT (prepared, ${PGBENCH_DURATION}s) ---"

run_pgbench_mixed() {
  local prefix="$1"
  local output tps
  output=$("$PG_BINDIR/pgbench" -h "$TMP_DIR" -p "$PORT" postgres \
    -n -T "$PGBENCH_DURATION" -M prepared -D scale="$N" \
    -f "$TMP_DIR/bench/mixed_sel_${prefix}.sql"@5 \
    -f "$TMP_DIR/bench/mixed_upd_${prefix}.sql"@3 \
    -f "$TMP_DIR/bench/mixed_ups_${prefix}.sql"@2 \
    2>&1)
  tps=$(echo "$output" | grep -oE 'tps = [0-9]+(\.[0-9]+)?' | grep -oE '[0-9]+(\.[0-9]+)?' | head -1)
  [ -z "$tps" ] && tps="0"
  echo "$tps"
}

tps_sh=$(run_pgbench_mixed "sh_bench")
tps_heap=$(run_pgbench_mixed "heap_bench")
tps_pgv="n/a"
$HAS_PGVECTOR && tps_pgv=$(run_pgbench_mixed "pgv_bench")

printf "  sorted_heap:  %s tps\n" "$(fmt "$(echo "$tps_sh" | awk '{printf "%.0f",$1}')")"
printf "  heap+btree:   %s tps\n" "$(fmt "$(echo "$tps_heap" | awk '{printf "%.0f",$1}')")"
if $HAS_PGVECTOR; then
  printf "  pgvector:     %s tps\n" "$(fmt "$(echo "$tps_pgv" | awk '{printf "%.0f",$1}')")"
fi

# ============================================================
# Verification
# ============================================================
echo ""
echo "--- Verification ---"
sh_count=$(PSQL -c "SELECT count(*) FROM sh_bench")
heap_count=$(PSQL -c "SELECT count(*) FROM heap_bench")
echo "  sh_bench:   $sh_count rows"
echo "  heap_bench: $heap_count rows"
if $HAS_PGVECTOR; then
  pgv_count=$(PSQL -c "SELECT count(*) FROM pgv_bench")
  echo "  pgv_bench:  $pgv_count rows"
fi

echo ""
echo "============================================================"
echo "Benchmark complete."
echo "============================================================"
