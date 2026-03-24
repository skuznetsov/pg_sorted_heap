#!/usr/bin/env bash
set -euo pipefail

# Reproducible Gutenberg fixed-graph benchmark runner for a user-provided AWS host.
#
# It syncs the current repo subset, ensures the Gutenberg dump exists remotely,
# then restores/builds the graph once and measures multiple ef_search points on
# that same fixed on-disk graph.

HOST="${1:-${AWS_HOST:-}}"
REMOTE_DIR="${2:-${AWS_REMOTE_DIR:-}}"
REMOTE_DUMP="${3:-${AWS_REMOTE_DUMP:-}}"
PORT="${4:-${AWS_PORT:-65479}}"
REMOTE_PYTHON="${REMOTE_PYTHON:-python3}"

LOCAL_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCAL_DUMP="${LOCAL_DUMP:-/tmp/cogniformerus_backup/cogniformerus_backup.dump}"
K="${K:-10}"
EFS="${EFS:-32,48,64,96}"
SH_EF_CONSTRUCTION="${SH_EF_CONSTRUCTION:-64}"
QUERY_COUNT="${QUERY_COUNT:-50}"
REPEATS="${REPEATS:-1}"
BACKEND_MODE="${BACKEND_MODE:-reuse}"
SHARED_CACHE="${SHARED_CACHE:-on}"
EXTRA_ARGS="${EXTRA_ARGS:-}"

if [[ ! -f "$LOCAL_DUMP" ]]; then
  echo "Local dump not found: $LOCAL_DUMP" >&2
  exit 2
fi

if [[ -z "$HOST" || -z "$REMOTE_DIR" || -z "$REMOTE_DUMP" ]]; then
  echo "Usage: AWS_HOST=<user@host> AWS_REMOTE_DIR=/path/to/repo AWS_REMOTE_DUMP=/path/to/dump $0 [host] [remote_dir] [remote_dump] [port]" >&2
  exit 2
fi

if ! command -v rsync >/dev/null 2>&1; then
  echo "rsync not found" >&2
  exit 2
fi

SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=10)

echo "== aws fixed-graph preflight =="
ssh "${SSH_OPTS[@]}" "$HOST" "sudo -n true >/dev/null && command -v python3 >/dev/null && command -v psql >/dev/null && command -v pg_restore >/dev/null"

echo "== sync repo subset =="
rsync -az --delete \
  --exclude '.git/' \
  --exclude '.claude/' \
  --exclude '.crystal_ball/' \
  --exclude '__pycache__/' \
  --exclude '*.o' \
  --exclude '*.so' \
  --exclude '*.dylib' \
  --exclude '*.bc' \
  --exclude '*.tmp' \
  "$LOCAL_ROOT/Makefile" \
  "$LOCAL_ROOT/pg_sorted_heap.control" \
  "$LOCAL_ROOT/src" \
  "$LOCAL_ROOT/sql" \
  "$LOCAL_ROOT/expected" \
  "$LOCAL_ROOT/scripts" \
  "$HOST:$REMOTE_DIR/"

echo "== ensure dump on remote =="
if ! ssh "${SSH_OPTS[@]}" "$HOST" "test -f '$REMOTE_DUMP'"; then
  rsync -az "$LOCAL_DUMP" "$HOST:$REMOTE_DUMP"
fi

echo "== run remote Gutenberg fixed-graph benchmark =="
REMOTE_CMD=$(
  cat <<EOF
set -euo pipefail
cd '$REMOTE_DIR'
'$REMOTE_PYTHON' scripts/bench_gutenberg_fixed_graph.py \
  --dump '$REMOTE_DUMP' \
  --port '$PORT' \
  --k '$K' \
  --efs '$EFS' \
  --sh-ef-construction '$SH_EF_CONSTRUCTION' \
  --query-count '$QUERY_COUNT' \
  --repeats '$REPEATS' \
  --backend-mode '$BACKEND_MODE' \
  --shared-cache '$SHARED_CACHE' \
  --install-cmd "sudo make -C '$REMOTE_DIR' install" \
  $EXTRA_ARGS
EOF
)
ssh "${SSH_OPTS[@]}" "$HOST" "$REMOTE_CMD"
