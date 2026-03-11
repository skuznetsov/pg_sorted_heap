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
README="$ROOT_DIR/README.md"
QUICKSTART="$ROOT_DIR/docs/quickstart.md"
OPERATIONS="$ROOT_DIR/OPERATIONS.md"
VECTOR_DOC="$ROOT_DIR/docs/vector-search.md"

for file in "$README" "$QUICKSTART" "$OPERATIONS" "$VECTOR_DOC"; do
  if [ ! -f "$file" ]; then
    echo "missing documentation file: $file" >&2
    exit 1
  fi
done

if ! rg -n 'make test-graph-builder' "$README" >/dev/null; then
  echo "expected README to mention make test-graph-builder" >&2
  exit 1
fi
if ! rg -n 'make test-graph-builder' "$QUICKSTART" >/dev/null; then
  echo "expected docs/quickstart.md to mention make test-graph-builder" >&2
  exit 1
fi
if ! rg -n 'make test-graph-builder' "$OPERATIONS" >/dev/null; then
  echo "expected OPERATIONS to mention make test-graph-builder" >&2
  exit 1
fi
if ! rg -n 'make build-graph-bench-nomic' "$OPERATIONS" >/dev/null; then
  echo "expected OPERATIONS to mention make build-graph-bench-nomic" >&2
  exit 1
fi
if ! rg -n 'make bench-nomic-ann' "$OPERATIONS" >/dev/null; then
  echo "expected OPERATIONS to mention make bench-nomic-ann" >&2
  exit 1
fi
if ! rg -n 'make build-graph-bench-nomic' "$VECTOR_DOC" >/dev/null; then
  echo "expected docs/vector-search.md to mention make build-graph-bench-nomic" >&2
  exit 1
fi
if ! rg -n 'make bench-nomic-ann' "$VECTOR_DOC" >/dev/null; then
  echo "expected docs/vector-search.md to mention make bench-nomic-ann" >&2
  exit 1
fi
if ! rg -n 'find_vector_python\.sh' "$VECTOR_DOC" >/dev/null; then
  echo "expected docs/vector-search.md to mention find_vector_python.sh" >&2
  exit 1
fi
if ! rg -n 'find_vector_python\.sh' "$ROOT_DIR/scripts/build_graph.py" >/dev/null; then
  echo "expected scripts/build_graph.py usage to mention find_vector_python.sh" >&2
  exit 1
fi
if rg -n 'python3 scripts/build_graph\.py' "$ROOT_DIR/scripts/build_graph.py" >/dev/null; then
  echo "unexpected hardcoded python3 usage in scripts/build_graph.py" >&2
  exit 1
fi
if ! rg -n 'find_vector_python\.sh' "$ROOT_DIR/scripts/bench_nomic_local_ann.py" >/dev/null; then
  echo "expected scripts/bench_nomic_local_ann.py usage to mention find_vector_python.sh" >&2
  exit 1
fi

echo "selftest_docs_graph_builder_contract status=ok"
