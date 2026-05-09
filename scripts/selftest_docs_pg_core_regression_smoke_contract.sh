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
OPERATIONS="$ROOT_DIR/OPERATIONS.md"
SCRIPTS_README="$ROOT_DIR/scripts/README.md"

if [ ! -f "$README" ]; then
  echo "missing README: $README" >&2
  exit 1
fi
if [ ! -f "$OPERATIONS" ]; then
  echo "missing OPERATIONS: $OPERATIONS" >&2
  exit 1
fi
if [ ! -f "$SCRIPTS_README" ]; then
  echo "missing scripts README: $SCRIPTS_README" >&2
  exit 1
fi

if ! rg -n 'make pg-core-regression-smoke' "$README" >/dev/null; then
  echo "expected README to include make pg-core-regression-smoke quickstart command" >&2
  exit 1
fi
if ! rg -n 'Command selection quick map' "$README" >/dev/null; then
  echo "expected README to reference Command selection quick map for operations routing" >&2
  exit 1
fi
if ! rg -n 'make pg-core-regression-smoke' "$OPERATIONS" >/dev/null; then
  echo "expected OPERATIONS to include make pg-core-regression-smoke command" >&2
  exit 1
fi
if ! rg -n 'pg_core_regression_smoke status=ok' "$OPERATIONS" >/dev/null; then
  echo "expected OPERATIONS to include pg_core_regression_smoke status marker" >&2
  exit 1
fi
if ! rg -n 'Command selection quick map' "$OPERATIONS" >/dev/null; then
  echo "expected OPERATIONS to include Command selection quick map section" >&2
  exit 1
fi
if ! rg -n 'make policy-safety-selftest' "$OPERATIONS" >/dev/null; then
  echo "expected OPERATIONS quick map to include make policy-safety-selftest command" >&2
  exit 1
fi
if ! rg -n 'make selftest-lightweight' "$OPERATIONS" >/dev/null; then
  echo "expected OPERATIONS quick map to include make selftest-lightweight command" >&2
  exit 1
fi
if ! rg -n 'policy-safety-selftest' "$SCRIPTS_README" >/dev/null; then
  echo "expected scripts README release map to mention policy-safety-selftest" >&2
  exit 1
fi
if ! rg -n "installcheck REGRESS='pg_sorted_heap sorted_hnsw graph_rag'" "$SCRIPTS_README" >/dev/null; then
  echo "expected scripts README release map to mention GraphRAG installcheck gate" >&2
  exit 1
fi
if ! rg -n 'Makefile' "$SCRIPTS_README" >/dev/null || ! rg -n 'source of truth' "$SCRIPTS_README" >/dev/null; then
  echo "expected scripts README to identify Makefile as exact release-composition source" >&2
  exit 1
fi

echo "selftest_docs_pg_core_regression_smoke_contract status=ok"
