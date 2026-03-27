#!/usr/bin/env bash
set -euo pipefail

# Reproducible GraphRAG multidepth benchmark runner for a user-provided AWS host.
#
# It syncs the current repo subset, installs the extension remotely, and runs
# the synthetic multidepth benchmark on the target host.
#
# Usage:
#   AWS_HOST=<user@host> AWS_REMOTE_DIR=/path/to/repo \
#     ./scripts/bench_graph_rag_multidepth_aws.sh [host] [remote_dir] [port]

HOST="${1:-${AWS_HOST:-}}"
REMOTE_DIR="${2:-${AWS_REMOTE_DIR:-}}"
PORT="${3:-${AWS_PORT:-65493}}"
REMOTE_PYTHON="${REMOTE_PYTHON:-python3}"

LOCAL_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NUM_PAIRS="${NUM_PAIRS:-5000}"
MAX_DEPTH="${MAX_DEPTH:-5}"
QUERY_COUNT="${QUERY_COUNT:-32}"
RUNS="${RUNS:-3}"
DIM="${DIM:-384}"
ANN_K="${ANN_K:-64}"
TOP_K="${TOP_K:-10}"
SEED="${SEED:-42}"
EF_SEARCH="${EF_SEARCH:-128}"
EF_CONSTRUCTION="${EF_CONSTRUCTION:-200}"
M="${M:-24}"
SHARED_BUFFERS_MB="${SHARED_BUFFERS_MB:-64}"
MAX_WAL_SIZE_GB="${MAX_WAL_SIZE_GB:-4}"
MAINTENANCE_WORK_MEM_MB="${MAINTENANCE_WORK_MEM_MB:-0}"
TABLE_SCOPE="${TABLE_SCOPE:-all}"
BACKEND_MODE="${BACKEND_MODE:-fresh}"
TMP_ROOT="${TMP_ROOT:-/tmp}"
EXTRA_ARGS="${EXTRA_ARGS:-}"

if [[ -z "$HOST" || -z "$REMOTE_DIR" ]]; then
  echo "Usage: AWS_HOST=<user@host> AWS_REMOTE_DIR=/path/to/repo $0 [host] [remote_dir] [port]" >&2
  exit 2
fi

if ! command -v rsync >/dev/null 2>&1; then
  echo "rsync not found" >&2
  exit 2
fi

SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=10)

echo "== aws multidepth preflight =="
ssh "${SSH_OPTS[@]}" "$HOST" "sudo -n true >/dev/null && command -v python3 >/dev/null && command -v psql >/dev/null"
ssh "${SSH_OPTS[@]}" "$HOST" "mkdir -p '$TMP_ROOT'"

echo "== sync repo subset =="
rsync -az --delete --delete-excluded \
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

echo "== run remote multidepth benchmark =="
REMOTE_CMD=$(
  cat <<EOF
set -euo pipefail
cd '$REMOTE_DIR'
'$REMOTE_PYTHON' scripts/bench_graph_rag_multidepth.py \
  --port '$PORT' \
  --tmp-root '$TMP_ROOT' \
  --num-pairs '$NUM_PAIRS' \
  --max-depth '$MAX_DEPTH' \
  --query-count '$QUERY_COUNT' \
  --runs '$RUNS' \
  --dim '$DIM' \
  --ann-k '$ANN_K' \
  --top-k '$TOP_K' \
  --seed '$SEED' \
  --ef-search '$EF_SEARCH' \
  --ef-construction '$EF_CONSTRUCTION' \
  --m '$M' \
  --shared-buffers-mb '$SHARED_BUFFERS_MB' \
  --max-wal-size-gb '$MAX_WAL_SIZE_GB' \
  --maintenance-work-mem-mb '$MAINTENANCE_WORK_MEM_MB' \
  --table-scope '$TABLE_SCOPE' \
  --backend-mode '$BACKEND_MODE' \
  --install-cmd "sudo make -C '$REMOTE_DIR' install" \
  $EXTRA_ARGS
EOF
)
ssh "${SSH_OPTS[@]}" "$HOST" "$REMOTE_CMD"
