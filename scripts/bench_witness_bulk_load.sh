#!/usr/bin/env bash
set -euo pipefail

# Benchmark COPY ingestion order strategies for clustered_heap.
#
# This is a falsifier harness for the witness-bearing bulk-load idea:
# compare unsorted COPY, bounded-window reorder, and full sorted COPY using
# the same deterministic row set and a fresh table per strategy.
#
# Usage:
#   ./scripts/bench_witness_bulk_load.sh [tmp_root] [port] [rows] [tenants] [window_rows_csv] [payload_bytes]
#
# Example:
#   ./scripts/bench_witness_bulk_load.sh /tmp 65511 65536 4096 1024,8192,65536 200

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
PORT="${2:-65511}"
ROWS="${3:-65536}"
TENANTS="${4:-4096}"
WINDOWS_CSV="${5:-1024,8192,65536}"
PAYLOAD_BYTES="${6:-200}"

if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root must be absolute: $TMP_ROOT" >&2
  exit 2
fi
if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [ "$PORT" -le 1024 ] || [ "$PORT" -ge 65535 ]; then
  echo "port must be 1025..65534" >&2
  exit 2
fi
if ! [[ "$ROWS" =~ ^[0-9]+$ ]] || [ "$ROWS" -le 0 ]; then
  echo "rows must be a positive integer" >&2
  exit 2
fi
if ! [[ "$TENANTS" =~ ^[0-9]+$ ]] || [ "$TENANTS" -le 0 ]; then
  echo "tenants must be a positive integer" >&2
  exit 2
fi
if ! [[ "$PAYLOAD_BYTES" =~ ^[0-9]+$ ]] || [ "$PAYLOAD_BYTES" -le 0 ]; then
  echo "payload_bytes must be a positive integer" >&2
  exit 2
fi

IFS=',' read -ra WINDOWS <<< "$WINDOWS_CSV"
for window in "${WINDOWS[@]}"; do
  if ! [[ "$window" =~ ^[0-9]+$ ]] || [ "$window" -le 0 ]; then
    echo "window_rows_csv contains invalid value: $window" >&2
    exit 2
  fi
done

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

TMP_DIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_wba_bulk.XXXXXX")"

make -C "$ROOT_DIR" install >/dev/null
"$PG_BINDIR/initdb" -D "$TMP_DIR/data" -A trust --no-locale >/dev/null 2>&1

cat >> "$TMP_DIR/data/postgresql.conf" <<'PGCONF'
shared_buffers = 512MB
work_mem = 128MB
maintenance_work_mem = 512MB
max_wal_size = 4GB
checkpoint_timeout = 30min
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

export ROWS TENANTS PAYLOAD_BYTES WINDOWS_CSV TMP_DIR
python3 - <<'PY'
import csv
import os
import random
from pathlib import Path

rows = int(os.environ["ROWS"])
tenants = int(os.environ["TENANTS"])
payload_bytes = int(os.environ["PAYLOAD_BYTES"])
windows = [int(x) for x in os.environ["WINDOWS_CSV"].split(",") if x]
tmp_dir = Path(os.environ["TMP_DIR"])

rng = random.Random(42)
payload = "x" * payload_bytes
records = []
for ordinal in range(rows):
    tenant_id = (ordinal % tenants) + 1
    item_id = (ordinal // tenants) + 1
    records.append((tenant_id, item_id, payload))

random_order = list(records)
rng.shuffle(random_order)

def write_csv(path: Path, data):
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(data)

write_csv(tmp_dir / "unsorted.csv", random_order)
write_csv(tmp_dir / "full_sort.csv", sorted(random_order, key=lambda row: (row[0], row[1])))

for window in windows:
    reordered = []
    for start in range(0, len(random_order), window):
        chunk = random_order[start:start + window]
        reordered.extend(sorted(chunk, key=lambda row: (row[0], row[1])))
    write_csv(tmp_dir / f"window_{window}.csv", reordered)
PY

extract_ms() {
  local output="$1"
  local ms
  ms=$(printf '%s\n' "$output" | grep -oE 'Time: [0-9]+(\.[0-9]+)? ms' | grep -oE '[0-9]+(\.[0-9]+)?' | head -1)
  if [ -n "$ms" ]; then
    echo "$ms"
  else
    echo "0"
  fi
}

safe_name() {
  printf '%s' "$1" | tr -c 'A-Za-z0-9_' '_'
}

run_case() {
  local label="$1"
  local csv_path="$2"
  local table="wba_$(safe_name "$label")"

  PSQL -c "DROP TABLE IF EXISTS $table CASCADE"
  PSQL -c "CREATE TABLE $table(tenant_id int, item_id int, payload text) USING clustered_heap"
  PSQL -c "CREATE INDEX ${table}_idx ON $table USING clustered_pk_index (tenant_id, item_id)"

  local copy_output
  copy_output=$(PSQL -c "\\timing on" -c "COPY $table(tenant_id, item_id, payload) FROM '$csv_path' WITH (FORMAT csv)" 2>&1)
  local insert_ms
  insert_ms="$(extract_ms "$copy_output")"

  PSQL -c "ANALYZE $table" >/dev/null

  local stats
  stats=$(PSQL -c "
WITH per_tenant AS (
  SELECT tenant_id,
         count(*) AS rows,
         count(DISTINCT substring(ctid::text from '\\(([0-9]+),')::bigint) AS blocks
  FROM $table
  GROUP BY tenant_id
),
rel AS (
  SELECT pg_relation_size('$table'::regclass) AS heap_bytes,
         pg_total_relation_size('$table'::regclass) AS total_bytes,
         greatest((pg_relation_size('$table'::regclass) / 8192.0)::numeric, 1.0::numeric) AS heap_pages
)
SELECT
  count(*) AS tenant_count,
  round(avg(blocks)::numeric, 3) AS avg_blocks_per_tenant,
  max(blocks) AS max_blocks_per_tenant,
  round(((SELECT heap_bytes FROM rel)::numeric) / 1024.0 / 1024.0, 3) AS heap_mb,
  round(((SELECT total_bytes FROM rel)::numeric) / 1024.0 / 1024.0, 3) AS total_mb,
  round($ROWS::numeric / (SELECT heap_pages FROM rel), 3) AS rows_per_heap_page
FROM per_tenant;
")

  IFS='|' read -r tenant_count avg_blocks max_blocks heap_mb total_mb rows_per_page <<< "$stats"
  printf 'wba_bulk_case|label=%s|rows=%s|tenants=%s|payload_bytes=%s|insert_ms=%s|tenant_count=%s|avg_blocks_per_tenant=%s|max_blocks_per_tenant=%s|heap_mb=%s|total_mb=%s|rows_per_heap_page=%s\n' \
    "$label" "$ROWS" "$TENANTS" "$PAYLOAD_BYTES" "$insert_ms" "$tenant_count" "$avg_blocks" "$max_blocks" "$heap_mb" "$total_mb" "$rows_per_page"
}

echo "wba_bulk_bench|rows=$ROWS|tenants=$TENANTS|payload_bytes=$PAYLOAD_BYTES|windows=$WINDOWS_CSV|port=$PORT|tmp_dir=$TMP_DIR"
run_case "unsorted" "$TMP_DIR/unsorted.csv"
for window in "${WINDOWS[@]}"; do
  run_case "window_${window}" "$TMP_DIR/window_${window}.csv"
done
run_case "full_sort" "$TMP_DIR/full_sort.csv"
echo "wba_bulk_bench|status=ok"
