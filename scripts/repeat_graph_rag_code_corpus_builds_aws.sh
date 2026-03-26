#!/usr/bin/env bash
set -euo pipefail

# Reproducible repeated-build runner for the real code-corpus GraphRAG benchmark
# on a user-provided AWS host.
#
# It syncs the current repo subset plus the minimal cogniformerus source tree
# needed by bench_graph_rag_code_corpus.py, installs the extension remotely,
# and runs repeat_graph_rag_code_corpus_builds.py on the target host.

HOST="${1:-${AWS_HOST:-}}"
REMOTE_DIR="${2:-${AWS_REMOTE_DIR:-}}"
PORT_BASE="${3:-${AWS_PORT_BASE:-65300}}"
REMOTE_PYTHON="${REMOTE_PYTHON:-python3}"
LOCAL_CODE_ROOT="${LOCAL_CODE_ROOT:-/Users/sergey/Projects/Crystal/cogniformerus}"
REMOTE_CODE_ROOT="${REMOTE_CODE_ROOT:-$REMOTE_DIR/.graph_rag_code_corpus}"

LOCAL_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPEATS="${REPEATS:-3}"
RUNS="${RUNS:-3}"
DIM="${DIM:-384}"
ANN_K="${ANN_K:-16}"
TOP_K="${TOP_K:-4}"
EF_SEARCH="${EF_SEARCH:-64}"
EF_CONSTRUCTION="${EF_CONSTRUCTION:-200}"
M="${M:-24}"
SHARED_BUFFERS_MB="${SHARED_BUFFERS_MB:-64}"
BACKEND_MODE="${BACKEND_MODE:-fresh}"
QUESTION_FILTER="${QUESTION_FILTER:-}"
EXTRA_ARGS="${EXTRA_ARGS:-}"

if [[ -z "$HOST" || -z "$REMOTE_DIR" ]]; then
  echo "Usage: AWS_HOST=<user@host> AWS_REMOTE_DIR=/path/to/repo $0 [host] [remote_dir] [port_base]" >&2
  exit 2
fi

if [[ ! -d "$LOCAL_CODE_ROOT/src/cogniformerus" || ! -f "$LOCAL_CODE_ROOT/bin/butler_code_test.cr" ]]; then
  echo "local cogniformerus tree not found under $LOCAL_CODE_ROOT" >&2
  exit 2
fi

if ! command -v rsync >/dev/null 2>&1; then
  echo "rsync not found" >&2
  exit 2
fi

SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=10)

echo "== aws code-corpus preflight =="
ssh "${SSH_OPTS[@]}" "$HOST" "sudo -n true >/dev/null && command -v python3 >/dev/null && command -v psql >/dev/null"

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

echo "== sync code corpus fixture =="
ssh "${SSH_OPTS[@]}" "$HOST" "mkdir -p '$REMOTE_CODE_ROOT/src' '$REMOTE_CODE_ROOT/bin'"
rsync -az --delete \
  "$LOCAL_CODE_ROOT/src/cogniformerus" \
  "$HOST:$REMOTE_CODE_ROOT/src/"
rsync -az \
  "$LOCAL_CODE_ROOT/bin/butler_code_test.cr" \
  "$HOST:$REMOTE_CODE_ROOT/bin/"

QUESTION_FILTER_ARG=""
if [[ -n "$QUESTION_FILTER" ]]; then
  QUESTION_FILTER_ARG="--question-filter '$QUESTION_FILTER'"
fi

echo "== run remote repeated-build benchmark =="
REMOTE_CMD=$(
  cat <<EOF
set -euo pipefail
cd '$REMOTE_DIR'
'$REMOTE_PYTHON' scripts/repeat_graph_rag_code_corpus_builds.py \
  --port-base '$PORT_BASE' \
  --repeats '$REPEATS' \
  --runs '$RUNS' \
  --dim '$DIM' \
  --ann-k '$ANN_K' \
  --top-k '$TOP_K' \
  --ef-search '$EF_SEARCH' \
  --ef-construction '$EF_CONSTRUCTION' \
  --m '$M' \
  --shared-buffers-mb '$SHARED_BUFFERS_MB' \
  --backend-mode '$BACKEND_MODE' \
  --cogniformerus-root '$REMOTE_CODE_ROOT' \
  --install-cmd "sudo make -C '$REMOTE_DIR' install" \
  $QUESTION_FILTER_ARG \
  $EXTRA_ARGS
EOF
)
ssh "${SSH_OPTS[@]}" "$HOST" "$REMOTE_CMD"
