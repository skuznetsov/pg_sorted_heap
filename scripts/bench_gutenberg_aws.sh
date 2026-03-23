#!/usr/bin/env bash
set -euo pipefail

# Reproducible Gutenberg benchmark runner for the AWS Ubuntu host.
#
# It syncs the current repo subset needed for build+benchmark, reuses or copies
# the Gutenberg custom dump, then runs bench_gutenberg_local_dump.py remotely
# with a sudo-backed install command suitable for Debian/Ubuntu PostgreSQL.
#
# Usage:
#   ./scripts/bench_gutenberg_aws.sh [host] [remote_dir] [remote_dump] [port]
#
# Example:
#   ./scripts/bench_gutenberg_aws.sh \
#     ubuntu@dev.rigelstar.com \
#     /home/ubuntu/clustered_pg \
#     /home/ubuntu/cogniformerus_backup.dump \
#     65471

HOST="${1:-ubuntu@dev.rigelstar.com}"
REMOTE_DIR="${2:-/home/ubuntu/clustered_pg}"
REMOTE_DUMP="${3:-/home/ubuntu/cogniformerus_backup.dump}"
PORT="${4:-65471}"
REMOTE_PYTHON="${REMOTE_PYTHON:-python3}"

LOCAL_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCAL_DUMP="${LOCAL_DUMP:-/tmp/cogniformerus_backup/cogniformerus_backup.dump}"
K="${K:-10}"
PGV_EF="${PGV_EF:-64}"
SH_EF="${SH_EF:-96}"
ZVEC_EF="${ZVEC_EF:-64}"
ZVEC_MEMORY_LIMIT_MB="${ZVEC_MEMORY_LIMIT_MB:-6144}"
QDRANT_EF="${QDRANT_EF:-64}"
EXTRA_ARGS="${EXTRA_ARGS:-}"

if [[ ! -f "$LOCAL_DUMP" ]]; then
  echo "Local dump not found: $LOCAL_DUMP" >&2
  exit 2
fi

if ! command -v rsync >/dev/null 2>&1; then
  echo "rsync not found" >&2
  exit 2
fi

SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=10)

echo "== aws bench preflight =="
ssh "${SSH_OPTS[@]}" "$HOST" "sudo -n true >/dev/null && command -v python3 >/dev/null && command -v psql >/dev/null && command -v pg_restore >/dev/null && command -v docker >/dev/null"

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

echo "== run remote Gutenberg benchmark =="
REMOTE_CMD=$(
  cat <<EOF
set -euo pipefail
cd '$REMOTE_DIR'
'$REMOTE_PYTHON' scripts/bench_gutenberg_local_dump.py \
  --dump '$REMOTE_DUMP' \
  --port '$PORT' \
  --k '$K' \
  --pgv-ef '$PGV_EF' \
  --sh-ef '$SH_EF' \
  --zvec-ef '$ZVEC_EF' \
  --zvec-memory-limit-mb '$ZVEC_MEMORY_LIMIT_MB' \
  --qdrant-ef '$QDRANT_EF' \
  --install-cmd "sudo make -C '$REMOTE_DIR' install" \
  $EXTRA_ARGS
EOF
)
ssh "${SSH_OPTS[@]}" "$HOST" "$REMOTE_CMD"
