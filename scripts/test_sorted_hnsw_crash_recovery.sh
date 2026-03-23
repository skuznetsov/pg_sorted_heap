#!/usr/bin/env bash
set -euo pipefail

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
BASE_PORT="${2:-65501}"

if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root must be absolute: $TMP_ROOT" >&2
  exit 2
fi
if ! [[ "$BASE_PORT" =~ ^[0-9]+$ ]] || [ "$BASE_PORT" -le 1024 ] || [ "$BASE_PORT" -ge 65530 ]; then
  echo "base_port must be 1025..65529" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if command -v pg_config >/dev/null 2>&1; then
  PG_BINDIR="$(pg_config --bindir)"
else
  PG_BINDIR="/opt/homebrew/Cellar/postgresql@18/18.3/bin"
fi

make -C "$ROOT_DIR" install >/dev/null

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

create_cluster() {
  local name="$1"
  local dir

  dir=$(mktemp -d "$TMP_ROOT/pg_sorted_hnsw_crash_${name}.XXXXXX")
  "$PG_BINDIR/initdb" -D "$dir/data" -A trust --no-locale >/dev/null 2>&1
  cat >> "$dir/data/postgresql.conf" <<'PGCONF'
log_min_messages = warning
checkpoint_timeout = 30s
max_wal_size = 64MB
PGCONF
  echo "$dir"
}

start_cluster() {
  local dir="$1" port="$2"

  "$PG_BINDIR/pg_ctl" -D "$dir/data" -l "$dir/postmaster.log" \
    -o "-k $dir -p $port" start >/dev/null
  for _ in $(seq 1 40); do
    if "$PG_BINDIR/psql" -h "$dir" -p "$port" postgres -c "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  echo "ERROR: cluster at $dir failed to start" >&2
  cat "$dir/postmaster.log" >&2
  return 1
}

crash_cluster() {
  local dir="$1"
  "$PG_BINDIR/pg_ctl" -D "$dir/data" -m immediate stop >/dev/null 2>&1 || true
}

destroy_cluster() {
  local dir="$1"
  "$PG_BINDIR/pg_ctl" -D "$dir/data" -m fast stop >/dev/null 2>&1 || true
  rm -rf "$dir"
}

PSQL_CMD() {
  local dir="$1" port="$2"
  shift 2
  "$PG_BINDIR/psql" -h "$dir" -p "$port" postgres -v ON_ERROR_STOP=1 -qtAX "$@"
}

scenario_insert_after_build() {
  echo "=== Scenario 1: INSERT survives immediate-stop restart ==="
  local dir port count before after

  port=$BASE_PORT
  dir=$(create_cluster "insert")
  start_cluster "$dir" "$port"

  PSQL_CMD "$dir" "$port" <<'SQL'
CREATE EXTENSION pg_sorted_heap;
CREATE TABLE hins(id int PRIMARY KEY, v svec(4));
INSERT INTO hins
SELECT g, format('[%s,%s,%s,%s]', g, g + 1, g + 2, g + 3)::svec(4)
FROM generate_series(1, 100) g;
CREATE INDEX hins_idx ON hins USING sorted_hnsw(v);
INSERT INTO hins VALUES (1001, '[0.8,0.6,0.9,0.1]'::svec(4));
SQL

  before=$(PSQL_CMD "$dir" "$port" -c \
    "SELECT string_agg(id::text, ',' ORDER BY ord) FROM (SELECT id, row_number() OVER () AS ord FROM (SELECT id FROM hins ORDER BY v <=> '[0.8,0.6,0.9,0.1]'::svec(4) LIMIT 5) q) q2")

  crash_cluster "$dir"
  start_cluster "$dir" "$port"

  count=$(PSQL_CMD "$dir" "$port" -c "SELECT count(*) FROM hins")
  after=$(PSQL_CMD "$dir" "$port" -c \
    "SELECT string_agg(id::text, ',' ORDER BY ord) FROM (SELECT id, row_number() OVER () AS ord FROM (SELECT id FROM hins ORDER BY v <=> '[0.8,0.6,0.9,0.1]'::svec(4) LIMIT 5) q) q2")

  check "insert_count_after_restart" "101" "$count"
  check "insert_top5_stable_across_restart" "$before" "$after"

  destroy_cluster "$dir"
}

scenario_vacuum_tombstones() {
  echo "=== Scenario 2: VACUUM tombstones survive immediate-stop restart ==="
  local dir port count before after

  port=$((BASE_PORT + 1))
  dir=$(create_cluster "vacuum")
  start_cluster "$dir" "$port"

  PSQL_CMD "$dir" "$port" <<'SQL'
CREATE EXTENSION pg_sorted_heap;
CREATE TABLE hdel(id int PRIMARY KEY, v svec(4));
INSERT INTO hdel
SELECT g, format('[%s,%s,%s,%s]', g, g + 1, g + 2, g + 3)::svec(4)
FROM generate_series(1, 100) g;
CREATE INDEX hdel_idx ON hdel USING sorted_hnsw(v);
DELETE FROM hdel WHERE id <= 10;
VACUUM hdel;
SQL

  before=$(PSQL_CMD "$dir" "$port" -c \
    "SELECT string_agg(id::text, ',' ORDER BY ord) FROM (SELECT id, row_number() OVER () AS ord FROM (SELECT id FROM hdel ORDER BY v <=> '[15,16,17,18]'::svec(4) LIMIT 5) q) q2")

  crash_cluster "$dir"
  start_cluster "$dir" "$port"

  count=$(PSQL_CMD "$dir" "$port" -c "SELECT count(*) FROM hdel")
  after=$(PSQL_CMD "$dir" "$port" -c \
    "SELECT string_agg(id::text, ',' ORDER BY ord) FROM (SELECT id, row_number() OVER () AS ord FROM (SELECT id FROM hdel ORDER BY v <=> '[15,16,17,18]'::svec(4) LIMIT 5) q) q2")

  check "vacuum_count_after_restart" "90" "$count"
  check "vacuum_top5_stable_across_restart" "$before" "$after"

  destroy_cluster "$dir"
}

scenario_insert_after_build
scenario_vacuum_tombstones

echo
echo "sorted_hnsw crash recovery: $pass/$total passed, $fail failed"
if [ "$fail" -ne 0 ]; then
  exit 1
fi
