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
ATTRS="$ROOT_DIR/.gitattributes"

if [ ! -f "$ATTRS" ]; then
  echo "missing .gitattributes: $ATTRS" >&2
  exit 1
fi
if ! git -C "$ROOT_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "selftest_release_archive_contract status=skipped reason=git_metadata_absent"
  exit 0
fi

attr_value() {
  git -C "$ROOT_DIR" check-attr export-ignore -- "$1" | awk -F': ' '{print $3}'
}

require_file() {
  local path="$1"
  if [ ! -f "$ROOT_DIR/$path" ]; then
    echo "expected tracked archive contract file to exist: $path" >&2
    exit 1
  fi
}

require_in_archive() {
  local path="$1"
  local value

  require_file "$path"
  value="$(attr_value "$path")"
  if [ "$value" != "unspecified" ]; then
    echo "expected source archive to include $path, got export-ignore=$value" >&2
    exit 1
  fi
}

require_export_ignored() {
  local path="$1"
  local value

  require_file "$path"
  value="$(attr_value "$path")"
  if [ "$value" != "set" ]; then
    echo "expected source archive to exclude $path, got export-ignore=$value" >&2
    exit 1
  fi
}

require_export_ignored_path() {
  local path="$1"
  local value

  value="$(attr_value "$path")"
  if [ "$value" != "set" ]; then
    echo "expected source archive to exclude $path, got export-ignore=$value" >&2
    exit 1
  fi
}

require_reference() {
  local pattern="$1"
  local path="$2"

  if ! rg -n "$pattern" "$ROOT_DIR/$path" >/dev/null; then
    echo "expected $path to reference pattern: $pattern" >&2
    exit 1
  fi
}

if rg -n 'CLAUDE_\*\.md[[:space:]]+export-ignore' "$ATTRS" >/dev/null; then
  echo "stale CLAUDE_*.md export-ignore rule should not be present" >&2
  exit 1
fi

require_in_archive "docs/turboquant-consumer-plan.md"
require_reference 'docs/turboquant-consumer-plan\.md' "docs/flashhadamard-note.md"

require_in_archive "scripts/build_hnsw_graph.py"
require_reference 'scripts/build_hnsw_graph\.py' "docs/vector-search.md"
require_reference 'scripts/build_hnsw_graph\.py' "Makefile"

require_export_ignored_path ".agents/example"
require_export_ignored_path ".claude/settings.local.json"
require_export_ignored_path ".crystal_ball/analysis_cache.db"
require_export_ignored_path ".github/workflows/ci.yml"
require_export_ignored_path ".ruff_cache/CACHEDIR.TAG"
require_export_ignored "TODO.md"
require_export_ignored "docs/announcement-0.13.0.md"
require_export_ignored "scripts/bench_hnsw_pg.py"
require_export_ignored "scripts/ingest_gutenberg_gptoss_sh.py"
require_export_ignored "scripts/sim_hierarchical_graph.py"

echo "selftest_release_archive_contract status=ok"
