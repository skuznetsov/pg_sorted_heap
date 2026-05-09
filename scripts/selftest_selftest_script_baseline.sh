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
RUNNER_SCRIPT="$SCRIPT_DIR/run_lightweight_selftests.sh"
WORKFLOW_DIR="$SCRIPT_DIR/../.github/workflows"
WORKFLOW_GUARDS_AVAILABLE=1
EXCLUDED_FROM_LIGHTWEIGHT=(
  "selftest_unnest_ab_probe_make_arg_contract.sh"
)

if [ ! -f "$RUNNER_SCRIPT" ]; then
  echo "runner script not found: $RUNNER_SCRIPT" >&2
  exit 2
fi
if [ ! -d "$WORKFLOW_DIR" ]; then
  WORKFLOW_GUARDS_AVAILABLE=0
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_selftest_baseline.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

count=0
while IFS= read -r file; do
  [ -n "$file" ] || continue
  count=$((count + 1))

  if [ ! -x "$file" ]; then
    echo "selftest script not executable: $file" >&2
    exit 1
  fi

  shebang="$(head -n 1 "$file" || true)"
  if [ "$shebang" != "#!/usr/bin/env bash" ]; then
    echo "selftest script has unexpected shebang: $file ($shebang)" >&2
    exit 1
  fi

  if ! grep -Fq "set -euo pipefail" "$file"; then
    echo "selftest script missing 'set -euo pipefail': $file" >&2
    exit 1
  fi

  basename "$file" >>"$WORKDIR/selftest_files.raw"
done < <(find "$SCRIPT_DIR" -maxdepth 1 -type f -name 'selftest_*.sh' | sort)

if [ "$count" -lt 1 ]; then
  echo "no selftest scripts found under $SCRIPT_DIR" >&2
  exit 2
fi

sort "$WORKDIR/selftest_files.raw" >"$WORKDIR/selftest_files.sorted"
if [ -n "$(uniq -d "$WORKDIR/selftest_files.sorted")" ]; then
  echo "duplicate selftest filenames discovered under $SCRIPT_DIR" >&2
  exit 1
fi

sed -n 's/^run_one "\(.*\)"/\1/p' "$RUNNER_SCRIPT" >"$WORKDIR/runner_entries.raw"
if [ ! -s "$WORKDIR/runner_entries.raw" ]; then
  echo "no run_one entries found in $RUNNER_SCRIPT" >&2
  exit 1
fi

sort "$WORKDIR/runner_entries.raw" >"$WORKDIR/runner_entries.sorted"
dup_runner="$(uniq -d "$WORKDIR/runner_entries.sorted" || true)"
if [ -n "$dup_runner" ]; then
  echo "duplicate run_one entries found in $RUNNER_SCRIPT: $dup_runner" >&2
  exit 1
fi

printf '%s\n' "${EXCLUDED_FROM_LIGHTWEIGHT[@]}" | sed '/^$/d' | sort -u >"$WORKDIR/excluded_from_lightweight.sorted"
if [ -s "$WORKDIR/excluded_from_lightweight.sorted" ]; then
  comm -23 "$WORKDIR/excluded_from_lightweight.sorted" "$WORKDIR/selftest_files.sorted" >"$WORKDIR/unknown_exclusions"
  if [ -s "$WORKDIR/unknown_exclusions" ]; then
    echo "excluded selftest entries not found in scripts directory:" >&2
    cat "$WORKDIR/unknown_exclusions" >&2
    exit 1
  fi
  comm -12 "$WORKDIR/excluded_from_lightweight.sorted" "$WORKDIR/runner_entries.sorted" >"$WORKDIR/excluded_but_listed"
  if [ -s "$WORKDIR/excluded_but_listed" ]; then
    echo "excluded selftests unexpectedly present in lightweight runner list:" >&2
    cat "$WORKDIR/excluded_but_listed" >&2
    exit 1
  fi
  while IFS= read -r excluded_name; do
    [ -n "$excluded_name" ] || continue
    if [ "$WORKFLOW_GUARDS_AVAILABLE" -ne 1 ]; then
      continue
    fi
    guarded=0
    while IFS= read -r workflow_file; do
      [ -f "$workflow_file" ] || continue
      hits_unquoted="$(grep -F --count "      - scripts/$excluded_name" "$workflow_file" || true)"
      hits_quoted="$(grep -F --count "      - 'scripts/$excluded_name'" "$workflow_file" || true)"
      hits=$((hits_unquoted + hits_quoted))
      if [ "$hits" -ge 2 ]; then
        guarded=1
        break
      fi
    done < <(find "$WORKFLOW_DIR" -maxdepth 1 -type f \( -name '*.yml' -o -name '*.yaml' \) | sort)
    if [ "$guarded" -ne 1 ]; then
      echo "excluded selftest is not guarded by path filters in any workflow: $excluded_name" >&2
      exit 1
    fi
  done <"$WORKDIR/excluded_from_lightweight.sorted"
  comm -23 "$WORKDIR/selftest_files.sorted" "$WORKDIR/excluded_from_lightweight.sorted" >"$WORKDIR/expected_in_runner.sorted"
else
  cp "$WORKDIR/selftest_files.sorted" "$WORKDIR/expected_in_runner.sorted"
fi

comm -23 "$WORKDIR/expected_in_runner.sorted" "$WORKDIR/runner_entries.sorted" >"$WORKDIR/missing_in_runner"
if [ -s "$WORKDIR/missing_in_runner" ]; then
  echo "selftest scripts missing in lightweight runner list:" >&2
  cat "$WORKDIR/missing_in_runner" >&2
  exit 1
fi

comm -13 "$WORKDIR/expected_in_runner.sorted" "$WORKDIR/runner_entries.sorted" >"$WORKDIR/unknown_in_runner"
if [ -s "$WORKDIR/unknown_in_runner" ]; then
  echo "runner references unknown selftest scripts:" >&2
  cat "$WORKDIR/unknown_in_runner" >&2
  exit 1
fi

if [ "$WORKFLOW_GUARDS_AVAILABLE" -eq 1 ]; then
  echo "selftest_selftest_script_baseline status=ok scripts=$count workflow_guards=checked"
else
  echo "selftest_selftest_script_baseline status=ok scripts=$count workflow_guards=skipped reason=workflow_files_absent"
fi
