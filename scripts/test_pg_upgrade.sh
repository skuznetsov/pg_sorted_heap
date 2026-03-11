#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# pg_upgrade 17→18 lifecycle test for pg_sorted_heap
# ============================================================
#
# Creates a PG 17 cluster with sorted_heap tables, upgrades to PG 18,
# verifies data integrity, zone map stats, and scan pruning.
#
# Usage: ./scripts/test_pg_upgrade.sh [tmp_root] [port_old] [port_new]

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
PORT_OLD="${2:-65496}"
PORT_NEW="${3:-65497}"

if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root must be absolute: $TMP_ROOT" >&2; exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# --- Locate PG 17 and PG 18 binaries ---
detect_pg_bindir() {
  local ver="$1"
  local envvar="PG${ver}_BINDIR"
  if [ -n "${!envvar:-}" ]; then
    echo "${!envvar}"
    return
  fi
  # macOS Homebrew
  for p in "/opt/homebrew/opt/postgresql@${ver}/bin" "/usr/local/opt/postgresql@${ver}/bin"; do
    if [ -x "$p/pg_config" ]; then
      echo "$p"
      return
    fi
  done
  # Linux PGDG
  if [ -x "/usr/lib/postgresql/${ver}/bin/pg_config" ]; then
    echo "/usr/lib/postgresql/${ver}/bin"
    return
  fi
  echo ""
}

PG17_BIN="$(detect_pg_bindir 17)"
PG18_BIN="$(detect_pg_bindir 18)"

if [ -z "$PG17_BIN" ]; then
  echo "ERROR: PostgreSQL 17 not found. Set PG17_BINDIR or install via Homebrew/PGDG." >&2
  exit 2
fi
if [ -z "$PG18_BIN" ]; then
  echo "ERROR: PostgreSQL 18 not found. Set PG18_BINDIR or install via Homebrew/PGDG." >&2
  exit 2
fi

echo "PG 17: $PG17_BIN"
echo "PG 18: $PG18_BIN"

TMP_DIR=""
pass=0; fail=0; total=0

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
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR/old/data" ]; then
    "$PG17_BIN/pg_ctl" -D "$TMP_DIR/old/data" -m immediate stop >/dev/null 2>&1 || true
  fi
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR/new/data" ]; then
    "$PG18_BIN/pg_ctl" -D "$TMP_DIR/new/data" -m immediate stop >/dev/null 2>&1 || true
  fi
  if [ -n "$TMP_DIR" ]; then
    rm -rf "$TMP_DIR"
  fi
}
trap cleanup EXIT

TMP_DIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_upgrade.XXXXXX")"
mkdir -p "$TMP_DIR/old" "$TMP_DIR/new"

# ============================================================
# Phase 1: Build + install for PG 17
# ============================================================
echo ""
echo "=== Phase 1: Build for PG 17 ==="
make -C "$ROOT_DIR" clean install PG_CONFIG="$PG17_BIN/pg_config" >/dev/null

# ============================================================
# Phase 2: Create PG 17 cluster with test data
# ============================================================
echo "=== Phase 2: Create PG 17 cluster ==="
"$PG17_BIN/initdb" -D "$TMP_DIR/old/data" -A trust --no-locale --data-checksums >/dev/null 2>&1
"$PG17_BIN/pg_ctl" -D "$TMP_DIR/old/data" -l "$TMP_DIR/old/postmaster.log" \
  -o "-k $TMP_DIR -p $PORT_OLD" start >/dev/null

PSQL_OLD() {
  "$PG17_BIN/psql" -h "$TMP_DIR" -p "$PORT_OLD" postgres -v ON_ERROR_STOP=1 -qtAX "$@"
}

PSQL_OLD -c "CREATE EXTENSION pg_sorted_heap"

PSQL_OLD <<'SQL'
CREATE TABLE upgrade_test(
    id int PRIMARY KEY,
    category int,
    payload text
) USING sorted_heap;

-- Insert 1000 rows with TOAST data
INSERT INTO upgrade_test
  SELECT g, g % 50, repeat('x', 4000)
  FROM generate_series(1, 1000) g;

-- Compact to build zone map
SELECT sorted_heap_compact('upgrade_test'::regclass);

-- Secondary btree index
CREATE INDEX idx_upgrade_cat ON upgrade_test(category);
SQL

echo ""
echo "--- Pre-upgrade verification ---"

count=$(PSQL_OLD -c "SELECT count(*) FROM upgrade_test")
check "pre_upgrade_count" "1000" "$count"

intact=$(PSQL_OLD -c "SELECT count(*) FROM upgrade_test WHERE length(payload) = 4000")
check "pre_upgrade_toast_intact" "1000" "$intact"

zm_valid=$(PSQL_OLD -c "
SELECT CASE WHEN sorted_heap_zonemap_stats('upgrade_test'::regclass) LIKE '%flags=valid%'
            THEN '1' ELSE '0' END
")
check "pre_upgrade_zm_valid" "1" "$zm_valid"

# Stop PG 17
"$PG17_BIN/pg_ctl" -D "$TMP_DIR/old/data" -m fast stop >/dev/null

# ============================================================
# Phase 3: Build + install for PG 18
# ============================================================
echo ""
echo "=== Phase 3: Build for PG 18 ==="
make -C "$ROOT_DIR" clean install PG_CONFIG="$PG18_BIN/pg_config" >/dev/null

# ============================================================
# Phase 4: pg_upgrade
# ============================================================
echo "=== Phase 4: pg_upgrade 17→18 ==="
"$PG18_BIN/initdb" -D "$TMP_DIR/new/data" -A trust --no-locale >/dev/null 2>&1

(
  cd "$TMP_DIR/new"
  "$PG18_BIN/pg_upgrade" \
    --old-datadir "$TMP_DIR/old/data" \
    --new-datadir "$TMP_DIR/new/data" \
    --old-bindir  "$PG17_BIN" \
    --new-bindir  "$PG18_BIN" \
    --old-port    "$PORT_OLD" \
    --new-port    "$PORT_NEW" \
    --socketdir   "$TMP_DIR"
) 2>&1 | tail -5

# ============================================================
# Phase 5: Post-upgrade verification
# ============================================================
echo ""
echo "=== Phase 5: Post-upgrade verification ==="
"$PG18_BIN/pg_ctl" -D "$TMP_DIR/new/data" -l "$TMP_DIR/new/postmaster.log" \
  -o "-k $TMP_DIR -p $PORT_NEW" start >/dev/null

PSQL_NEW() {
  "$PG18_BIN/psql" -h "$TMP_DIR" -p "$PORT_NEW" postgres -v ON_ERROR_STOP=1 -qtAX "$@"
}

count=$(PSQL_NEW -c "SELECT count(*) FROM upgrade_test")
check "post_upgrade_count" "1000" "$count"

intact=$(PSQL_NEW -c "SELECT count(*) FROM upgrade_test WHERE length(payload) = 4000")
check "post_upgrade_toast_intact" "1000" "$intact"

idx_exists=$(PSQL_NEW -c "SELECT count(*) FROM pg_indexes WHERE tablename = 'upgrade_test' AND indexname = 'idx_upgrade_cat'")
check "post_upgrade_secondary_index_exists" "1" "$idx_exists"

idx_result=$(PSQL_NEW -c "SET enable_seqscan = off; SELECT count(*) FROM upgrade_test WHERE category = 5")
check "post_upgrade_index_query" "20" "$idx_result"

# Zone map survives upgrade
zm_valid=$(PSQL_NEW -c "
SELECT CASE WHEN sorted_heap_zonemap_stats('upgrade_test'::regclass) LIKE '%flags=valid%'
            THEN '1' ELSE '0' END
")
check "post_upgrade_zm_valid" "1" "$zm_valid"

# Compact works on upgraded data
echo ""
echo "--- Post-upgrade compact ---"
PSQL_NEW -c "
INSERT INTO upgrade_test
  SELECT g, g % 50, repeat('y', 4000)
  FROM generate_series(1001, 2000) g;
"
PSQL_NEW -c "SELECT sorted_heap_compact('upgrade_test'::regclass)" >/dev/null

count=$(PSQL_NEW -c "SELECT count(*) FROM upgrade_test")
check "post_compact_count" "2000" "$count"

intact=$(PSQL_NEW -c "SELECT count(*) FROM upgrade_test WHERE length(payload) = 4000")
check "post_compact_toast_intact" "2000" "$intact"

zm_valid=$(PSQL_NEW -c "
SELECT CASE WHEN sorted_heap_zonemap_stats('upgrade_test'::regclass) LIKE '%flags=valid%'
            THEN '1' ELSE '0' END
")
check "post_compact_zm_valid" "1" "$zm_valid"

# Verify zone map pruning via EXPLAIN (SETs + EXPLAIN in single session)
has_pruning=$("$PG18_BIN/psql" -h "$TMP_DIR" -p "$PORT_NEW" postgres -qtAX \
  -c "SET enable_indexscan = off; SET enable_bitmapscan = off; EXPLAIN SELECT count(*) FROM upgrade_test WHERE id = 500" \
  2>/dev/null | grep -c 'pruned' || true)
check "post_upgrade_pruning_works" "t" "$([ "$has_pruning" -gt 0 ] && echo t || echo f)"

# No crashes
crashes=$(grep -c 'SIGSEGV\|SIGBUS\|SIGABRT\|server process.*was terminated' "$TMP_DIR/new/postmaster.log" 2>/dev/null || true)
check "no_crashes" "0" "$crashes"

# ============================================================
# Summary
# ============================================================
echo ""
if [ "$fail" -eq 0 ]; then
  echo "pg_upgrade_test status=ok pass=$pass fail=$fail total=$total"
else
  echo "pg_upgrade_test status=FAIL pass=$pass fail=$fail total=$total"
  exit 1
fi
