#!/usr/bin/env bash
set -euo pipefail

# Run the repeated-build GraphRAG multihop protocol on a user-provided AWS host.
#
# This syncs the current repo subset, installs the extension remotely, and runs
# repeat_graph_rag_multihop_builds.py on the target host.
#
# Usage:
#   AWS_HOST=<user@host> AWS_REMOTE_DIR=/path/to/repo \
#     ./scripts/repeat_graph_rag_multihop_builds_aws.sh [host] [remote_dir]

HOST="${1:-${AWS_HOST:-}}"
REMOTE_DIR="${2:-${AWS_REMOTE_DIR:-}}"
REMOTE_PYTHON="${REMOTE_PYTHON:-python3}"

LOCAL_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPEATS="${REPEATS:-3}"
PORT_BASE="${PORT_BASE:-65440}"
NUM_PAIRS="${NUM_PAIRS:-5000}"
QUERY_COUNT="${QUERY_COUNT:-64}"
RUNS="${RUNS:-3}"
DIM="${DIM:-384}"
ANN_K="${ANN_K:-64}"
TOP_K="${TOP_K:-10}"
EF_SEARCH="${EF_SEARCH:-128}"
EF_CONSTRUCTION="${EF_CONSTRUCTION:-200}"
M="${M:-24}"
PGV_EF_SEARCH="${PGV_EF_SEARCH:-64}"
ZVEC_EF="${ZVEC_EF:-64}"
ZVEC_MEMORY_LIMIT_MB="${ZVEC_MEMORY_LIMIT_MB:-6144}"
QDRANT_EF="${QDRANT_EF:-64}"
SHARED_BUFFERS_MB="${SHARED_BUFFERS_MB:-64}"
BACKEND_MODE="${BACKEND_MODE:-fresh}"
CASES="${CASES:-}"
EXTRA_ARGS="${EXTRA_ARGS:-}"

if [[ -z "$HOST" || -z "$REMOTE_DIR" ]]; then
  echo "Usage: AWS_HOST=<user@host> AWS_REMOTE_DIR=/path/to/repo $0 [host] [remote_dir]" >&2
  exit 2
fi

if ! command -v rsync >/dev/null 2>&1; then
  echo "rsync not found" >&2
  exit 2
fi

SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=10)

echo "== aws repeated-build preflight =="
ssh "${SSH_OPTS[@]}" "$HOST" "sudo -n true >/dev/null && command -v python3 >/dev/null && command -v psql >/dev/null && command -v docker >/dev/null"

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

CASE_ARG=""
if [[ -n "$CASES" ]]; then
  CASE_ARG="--cases '$CASES'"
fi

echo "== run remote repeated-build protocol =="
REMOTE_CMD=$(
  cat <<EOF
set -euo pipefail
cd '$REMOTE_DIR'
'$REMOTE_PYTHON' scripts/repeat_graph_rag_multihop_builds.py \
  --repeats '$REPEATS' \
  --port-base '$PORT_BASE' \
  --num-pairs '$NUM_PAIRS' \
  --query-count '$QUERY_COUNT' \
  --runs '$RUNS' \
  --dim '$DIM' \
  --ann-k '$ANN_K' \
  --top-k '$TOP_K' \
  --ef-search '$EF_SEARCH' \
  --ef-construction '$EF_CONSTRUCTION' \
  --m '$M' \
  --pgv-ef-search '$PGV_EF_SEARCH' \
  --zvec-ef '$ZVEC_EF' \
  --zvec-memory-limit-mb '$ZVEC_MEMORY_LIMIT_MB' \
  --qdrant-ef '$QDRANT_EF' \
  --shared-buffers-mb '$SHARED_BUFFERS_MB' \
  --backend-mode '$BACKEND_MODE' \
  --install-cmd "sudo make -C '$REMOTE_DIR' install" \
  $CASE_ARG \
  $EXTRA_ARGS
EOF
)
ssh "${SSH_OPTS[@]}" "$HOST" "$REMOTE_CMD"
