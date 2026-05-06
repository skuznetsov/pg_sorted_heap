#!/usr/bin/env bash
set -euo pipefail

# Offline smoke for the large-vector ANN comparison matrix.
# Generates a small deterministic .npz corpus, then runs the real-dataset
# harness without downloading ANN-Benchmarks data.
#
# Usage:
#   bash scripts/bench_ann_matrix_offline_smoke.sh [tmp_root] [rows] [queries] [dim] [k]

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
ROWS="${2:-320}"
QUERIES="${3:-3}"
DIM="${4:-8}"
K="${5:-3}"

if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root must be absolute: $TMP_ROOT" >&2
  exit 2
fi
for val_name in ROWS QUERIES DIM K; do
  val="${!val_name}"
  if ! [[ "$val" =~ ^[0-9]+$ ]] || [ "$val" -le 0 ]; then
    echo "$val_name must be a positive integer" >&2
    exit 2
  fi
done
if [ "$ROWS" -lt 256 ]; then
  echo "ROWS must be at least 256 for IVF-PQ training" >&2
  exit 2
fi
if [ "$ROWS" -lt "$K" ]; then
  echo "ROWS must be >= K" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-$("$SCRIPT_DIR/find_vector_python.sh")}"

TMP_DIR="$(mktemp -d "$TMP_ROOT/pg_ann_matrix_offline.XXXXXX")"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

VECTORS_NPZ="$TMP_DIR/vectors.npz"
"$PYTHON_BIN" - "$VECTORS_NPZ" "$ROWS" "$QUERIES" "$DIM" <<'PY'
import sys
import numpy as np

out, rows, queries, dim = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
rng = np.random.default_rng(17)
np.savez(
    out,
    base=rng.normal(size=(rows, dim)).astype(np.float32),
    queries=rng.normal(size=(queries, dim)).astype(np.float32),
)
PY

"$PYTHON_BIN" "$ROOT_DIR/scripts/bench_ann_real_dataset.py" \
  --vectors "$VECTORS_NPZ" \
  --skip-zvec --skip-qdrant \
  --k "$K" \
  --enable-ivfpq \
  --ivfpq-nlist 8 \
  --ivfpq-nprobe 2 \
  --ivfpq-m 0 \
  --ivfpq-rerank-topk 20 \
  --ivfpq-train-iter 2 \
  --ivfpq-max-train 256 \
  --enable-flashhadamard \
  --flashhadamard-group-size 4
