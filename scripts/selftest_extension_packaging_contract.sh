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

python3 - "$ROOT_DIR" <<'PY'
import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
meta_path = root / "META.json"
control_path = root / "pg_sorted_heap.control"
makefile_path = root / "Makefile"
sql_dir = root / "sql"

meta = json.loads(meta_path.read_text(encoding="utf-8"))
version = meta.get("version")
if not version:
    raise SystemExit("META.json missing version")

control = control_path.read_text(encoding="utf-8")
expected_control_line = f"default_version = '{version}'"
if expected_control_line not in control:
    raise SystemExit(
        f"pg_sorted_heap.control default_version mismatch: expected {expected_control_line}"
    )

provides = meta.get("provides", {})
if "pg_sorted_heap" not in provides:
    raise SystemExit("META.json missing provides.pg_sorted_heap")

expected_release_sql = f"sql/pg_sorted_heap--{version}.sql"
for provide_name, provide in provides.items():
    if provide.get("version") != version:
        raise SystemExit(
            f"META.json provides.{provide_name}.version mismatch: "
            f"{provide.get('version')} != {version}"
        )
    if provide_name == "pg_sorted_heap" and provide.get("file") != expected_release_sql:
        raise SystemExit(
            "META.json provides.pg_sorted_heap.file mismatch: "
            f"expected {expected_release_sql}, got {provide.get('file')}"
        )
    for key in ("file", "docfile"):
        path = provide.get(key)
        if path and not (root / path).is_file():
            raise SystemExit(f"META.json provides.{provide_name}.{key} missing: {path}")

release_sql = sql_dir / f"pg_sorted_heap--{version}.sql"
if not release_sql.is_file():
    raise SystemExit(f"release SQL file missing for META/control version: {release_sql}")

makefile = makefile_path.read_text(encoding="utf-8").splitlines()
data_parts = []
in_data = False
for raw in makefile:
    line = raw.rstrip()
    if not in_data:
        if line.startswith("DATA = "):
            in_data = True
            line = line[len("DATA = ") :]
        else:
            continue
    continued = line.endswith("\\")
    if continued:
        line = line[:-1]
    data_parts.extend(part for part in line.split() if part)
    if not continued:
        break

if not data_parts:
    raise SystemExit("Makefile DATA assignment not found or empty")

sql_files = sorted(
    str(path.relative_to(root)) for path in sql_dir.glob("pg_sorted_heap--*.sql")
)
data_files = sorted(data_parts)
missing = sorted(set(sql_files) - set(data_files))
extra = sorted(set(data_files) - set(sql_files))
if missing:
    raise SystemExit("Makefile DATA missing SQL files: " + ", ".join(missing))
if extra:
    raise SystemExit("Makefile DATA references non-versioned SQL files: " + ", ".join(extra))

versions = []
upgrades = set()
for rel in sql_files:
    name = pathlib.Path(rel).name
    base = re.fullmatch(r"pg_sorted_heap--([0-9.]+)\.sql", name)
    upgrade = re.fullmatch(r"pg_sorted_heap--([0-9.]+)--([0-9.]+)\.sql", name)
    if base:
        versions.append(base.group(1))
    elif upgrade:
        upgrades.add((upgrade.group(1), upgrade.group(2)))

def version_key(value):
    return tuple(int(part) for part in value.split("."))

ordered_versions = sorted(versions, key=version_key)
if version not in versions:
    raise SystemExit(f"META/control version has no base SQL file: {version}")

for old, new in zip(ordered_versions, ordered_versions[1:]):
    if (old, new) not in upgrades:
        raise SystemExit(f"missing adjacent upgrade SQL file: {old} -> {new}")

print(
    "extension_packaging_contract status=ok "
    f"version={version} sql_files={len(sql_files)} upgrade_edges={len(upgrades)}"
)
PY
