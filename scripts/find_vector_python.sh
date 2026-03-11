#!/usr/bin/env bash
set -euo pipefail

for candidate in \
  "${PYTHON_BIN:-}" \
  "python3" \
  "${HOME:-}/miniconda3/bin/python3" \
  "${HOME:-}/mambaforge/bin/python3" \
  "/opt/homebrew/Caskroom/miniconda/base/bin/python3" \
  "/opt/homebrew/miniconda3/bin/python3"
do
  [ -n "$candidate" ] || continue

  if command -v "$candidate" >/dev/null 2>&1; then
    resolved="$(command -v "$candidate")"
  elif [ -x "$candidate" ]; then
    resolved="$candidate"
  else
    continue
  fi

  if "$resolved" -c 'import numpy, psycopg2' >/dev/null 2>&1; then
    printf '%s\n' "$resolved"
    exit 0
  fi
done

echo "could not find python with numpy + psycopg2" >&2
exit 2
