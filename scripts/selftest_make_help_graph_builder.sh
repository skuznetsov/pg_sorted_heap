#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -gt 1 ]; then
  echo "usage: $0 [tmp_root_abs_dir]" >&2
  exit 2
fi

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root_abs_dir must be absolute: $TMP_ROOT" >&2
  exit 2
fi
if [ ! -d "$TMP_ROOT" ]; then
  echo "tmp_root_abs_dir not found: $TMP_ROOT" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

OUT_HELP="$(
  make -s -C "$ROOT_DIR" --no-print-directory help
)"

for literal in \
  "make test-graph-builder" \
  "make build-graph-bench-nomic" \
  "make bench-nomic-ann"
do
  if ! printf '%s\n' "$OUT_HELP" | grep -Fq "$literal"; then
    echo "expected help output to include $literal" >&2
    printf '%s\n' "$OUT_HELP" >&2
    exit 1
  fi
done

echo "selftest_make_help_graph_builder status=ok"
