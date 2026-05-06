#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PORT="${1:-65491}"
TMP_ROOT="${TMPDIR:-/tmp}"
TMP_DIR=""

if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [ "$PORT" -le 1024 ] || [ "$PORT" -ge 65535 ]; then
  echo "port must be an integer in range 1025..65534" >&2
  exit 2
fi

if [[ "$TMP_ROOT" != /* ]] || [ ! -d "$TMP_ROOT" ]; then
  echo "TMPDIR must be an existing absolute directory: $TMP_ROOT" >&2
  exit 2
fi

if command -v pg_config >/dev/null 2>&1; then
  PG_BINDIR="$(pg_config --bindir)"
else
  PG_BINDIR="/opt/homebrew/bin"
fi

cleanup() {
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR/data" ]; then
    "$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -m immediate stop >/dev/null 2>&1 || true
  fi
  if [ -n "$TMP_DIR" ]; then
    rm -rf "$TMP_DIR"
  fi
}
trap cleanup EXIT

TMP_DIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_partition_lock.XXXXXX")"
holder_out="$TMP_DIR/holder.out"
holder_err="$TMP_DIR/holder.err"
blocked_out="$TMP_DIR/blocked.out"
blocked_err="$TMP_DIR/blocked.err"

psqlq() {
  "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -v ON_ERROR_STOP=1 -X -qAt "$@"
}

make -C "$ROOT_DIR" install >/dev/null
"$PG_BINDIR/initdb" -D "$TMP_DIR/data" -A trust --no-locale >/dev/null
"$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -l "$TMP_DIR/postmaster.log" -o "-k $TMP_DIR -p $PORT" start >/dev/null

psqlq <<'SQL'
CREATE EXTENSION pg_sorted_heap;
CREATE TABLE sh_lock_parent(tenant_id int, id int, val text,
                            PRIMARY KEY (tenant_id, id))
PARTITION BY RANGE (tenant_id);
CREATE TABLE sh_lock_1 PARTITION OF sh_lock_parent
  FOR VALUES FROM (1) TO (2) USING sorted_heap;
CREATE TABLE sh_lock_2 PARTITION OF sh_lock_parent
  FOR VALUES FROM (2) TO (3) USING sorted_heap;
INSERT INTO sh_lock_parent
SELECT tenant_id, id, repeat('x', 80)
FROM generate_series(1, 2) tenant_id,
     generate_series(1, 2000) id;
SQL

"$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -v ON_ERROR_STOP=1 -X -qAt <<'SQL' >"$holder_out" 2>"$holder_err" &
BEGIN;
LOCK TABLE sh_lock_1 IN ACCESS SHARE MODE;
SELECT pg_sleep(5);
COMMIT;
SQL
holder_pid=$!

for _ in $(seq 1 50); do
  if [ "$(psqlq -c "SELECT count(*) FROM pg_locks WHERE relation = 'sh_lock_1'::regclass AND mode = 'AccessShareLock' AND granted;")" = "1" ]; then
    break
  fi
  sleep 0.1
done

if [ "$(psqlq -c "SELECT count(*) FROM pg_locks WHERE relation = 'sh_lock_1'::regclass AND mode = 'AccessShareLock' AND granted;")" != "1" ]; then
  echo "partition_lock_smoke status=error stage=lock_not_observed" >&2
  exit 1
fi

blocked="0"
set +e
"$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" postgres -v ON_ERROR_STOP=1 -X -qAt <<'SQL' >"$blocked_out" 2>"$blocked_err"
SET lock_timeout = '500ms';
SET client_min_messages = warning;
SELECT count(*) FROM sorted_heap_compact_partitions('sh_lock_parent'::regclass);
SQL
blocked_rc=$?
set -e

if [ "$blocked_rc" -ne 0 ] && grep -qi "lock timeout" "$blocked_err"; then
  blocked="1"
fi
echo "partition_lock_smoke|blocked_by_leaf_lock=$blocked"

wait "$holder_pid"

post_release_ok="$(psqlq <<'SQL'
SET client_min_messages = warning;
SELECT count(*) FROM sorted_heap_compact_partitions('sh_lock_parent'::regclass)
WHERE status = 'ok';
SQL
)"
echo "partition_lock_smoke|post_release_compact_ok=$post_release_ok"

if [ "$blocked" != "1" ] || [ "$post_release_ok" != "2" ]; then
  echo "partition_lock_smoke_status=fail" >&2
  exit 1
fi

echo "partition_lock_smoke_status=ok"
