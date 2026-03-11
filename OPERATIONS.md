# pg_sorted_heap operations runbook

This file captures the day-to-day diagnostics workflow for extension stability and performance checks.

## Entry point

```bash
make help
```

Use this as the canonical list of project-specific operational targets and default parameters.

Policy contract lint (fast static preflight for comparator mode/polarity safety):

```bash
make policy-lint
```

Contract source:
- `scripts/comparator_policy_contract.json`
- current `schema_version`: `1`
- requirement: `python3` available in execution environment.
- optional env: `POLICY_LINT_DUPLICATE_TARGET_MODE=error|warn` (default `error`).
- optional env: `POLICY_LINT_WARNINGS_MAX=<non-negative-int>` (unset/empty disables warning-threshold gate).

Policy/doc/workflow safety aggregate preflight:

```bash
make policy-safety-selftest \
  UNNEST_AB_SELFTEST_TMP_ROOT=/private/tmp \
  UNNEST_GATE_SELFTEST_TMP_ROOT=/private/tmp
```

Related CI lane: `.github/workflows/policy-safety-selftest.yml`.

## Command selection quick map

- Use `make pg-core-regression-smoke` when you need a fast capability check for upstream-style entrypoints (`installcheck`, TAP/`prove`, `pg_isolation_regress`) without spinning up heavy probes.
- Use `make policy-safety-selftest UNNEST_AB_SELFTEST_TMP_ROOT=/private/tmp UNNEST_GATE_SELFTEST_TMP_ROOT=/private/tmp` before release-oriented edits that touch policy/docs/workflow safety boundaries.
- Use `make selftest-lightweight LIGHTWEIGHT_SELFTEST_TMP_ROOT=/private/tmp` for canonical cross-surface regression validation across runtime, startup, perf, docs, and workflow contracts.

## Quick checks

Single-backend stability smoke:

```bash
make fastpath-stress STRESS_CYCLES=10 STRESS_PORT=65462
```

Larger soak:

```bash
make fastpath-stress STRESS_CYCLES=50 STRESS_PORT=65462
```

Mixed-cardinality perf probe:

```bash
make fastpath-perf-probe \
  PERF_ROWS=1024 PERF_ITERS=2000 \
  PERF_CHURN_ROWS=5000 PERF_CHURN_ITERS=2000 \
  PERF_PORT=65463
```

Median-of-N perf probe (reduces single-run jitter):

```bash
make fastpath-perf-probe-median \
  PERF_MEDIAN_RUNS=5 \
  PERF_ROWS=1024 PERF_ITERS=2000 \
  PERF_CHURN_ROWS=5000 PERF_CHURN_ITERS=2000 \
  PERF_PORT=65480 PERF_OUT_DIR=/private/tmp
```

Production perf sentinel (gating, steady-state default):

```bash
make unnest-ab-perf-sentinel \
  UNNEST_SENTINEL_WARMUP_SELECTS=1 \
  UNNEST_SENTINEL_MIN_INSERT_RATIO=0.90 \
  UNNEST_SENTINEL_MIN_JOIN_UNNEST_RATIO=1.10 \
  UNNEST_SENTINEL_MIN_ANY_ARRAY_RATIO=1.00
```

Cold-start observability lane (non-gating telemetry only):

```bash
make unnest-ab-perf-sentinel-cold-observe
```

In observability mode the sentinel emits `status=observe` and exits zero unless the probe itself fails.

Cold-vs-warm startup sensitivity probe (non-gating, emits warm-over-cold multipliers):

```bash
make unnest-ab-startup-sensitivity \
  UNNEST_STARTUP_RUNS=1 \
  UNNEST_STARTUP_BATCH_SIZE=400 \
  UNNEST_STARTUP_BATCHES=20 \
  UNNEST_STARTUP_SELECT_ITERS=60 \
  UNNEST_STARTUP_PROBE_SIZE=64
```

Output marker: `unnest_ab_startup_sensitivity status=ok|...|*_warm_over_cold=<ratio>`.

Startup sensitivity guard (fails when cold-start penalty exceeds configured envelope):

```bash
make unnest-ab-startup-sensitivity-guard \
  UNNEST_STARTUP_GUARD_MAX_INSERT_WARM_OVER_COLD=2.00 \
  UNNEST_STARTUP_GUARD_MAX_JOIN_UNNEST_WARM_OVER_COLD=2.00 \
  UNNEST_STARTUP_GUARD_MAX_ANY_ARRAY_WARM_OVER_COLD=1.80
```

Guard output marker:
- pass: `unnest_ab_startup_sensitivity_guard status=ok|...`
- fail: `unnest_ab_startup_sensitivity_guard status=regression|...`

PostgreSQL core-alignment smoke check (non-invasive, capability visibility):

```bash
make pg-core-regression-smoke
```

Output marker: `pg_core_regression_smoke status=ok|installcheck_target=...|tap_prove=...|isolation_regress=...`.
Use it to verify whether local environment is ready for upstream-style extension checks (`make installcheck`, TAP/`prove`, `pg_isolation_regress`).

Compare two perf probe logs against slowdown threshold:

```bash
make fastpath-perf-compare \
  PERF_REF=/private/tmp/perf_ref.log \
  PERF_NEW=/private/tmp/perf_new.log \
  PERF_MAX_SLOWDOWN=1.25
```

Compare two perf probe log sets (median-based):

```bash
make fastpath-perf-compare-median \
  PERF_REF_DIR=/private/tmp/perf_ref_set \
  PERF_NEW_DIR=/private/tmp/perf_new_set \
  PERF_MAX_SLOWDOWN=1.25 \
  PERF_SET_STAT_MODE=median \
  PERF_SET_MIN_SAMPLES=1
```

Default `PERF_SET_MIN_SAMPLES` is mode-aware:
- `median` defaults to `1`.
- `p95` and `trimmed-mean` default to `3` (and reject lower values).
- comparator header includes `metric_polarity=lower_is_better`.

Comparator self-test (no PostgreSQL startup; validates parser/gating invariants):

```bash
make fastpath-perf-compare-selftest PERF_SELFTEST_TMP_ROOT=/private/tmp
```

Runtime perf-probe self-test (starts temporary PostgreSQL; validates churn eviction path and leak/panic guards):

```bash
make fastpath-perf-probe-selftest \
  PERF_RUNTIME_SELFTEST_TMP_ROOT=/private/tmp \
  PERF_RUNTIME_SELFTEST_PORT=65473 \
  PERF_RUNTIME_SELFTEST_ROWS=128 \
  PERF_RUNTIME_SELFTEST_ITERS=50 \
  PERF_RUNTIME_SELFTEST_CHURN_ROWS=5000 \
  PERF_RUNTIME_SELFTEST_CHURN_ITERS=50
```

When overriding runtime selftest parameters, keep `PERF_RUNTIME_SELFTEST_CHURN_ROWS` high enough to exercise eviction-path checks (`local_hint_evictions > 0`).

Optional high-churn profile target (useful for deeper nightly/local stress):

```bash
make fastpath-perf-probe-selftest-high-churn \
  PERF_RUNTIME_SELFTEST_TMP_ROOT=/private/tmp \
  PERF_RUNTIME_SELFTEST_HIGH_PORT=65474 \
  PERF_RUNTIME_SELFTEST_HIGH_ROWS=256 \
  PERF_RUNTIME_SELFTEST_HIGH_ITERS=100 \
  PERF_RUNTIME_SELFTEST_HIGH_CHURN_ROWS=12000 \
  PERF_RUNTIME_SELFTEST_HIGH_CHURN_ITERS=100
```

Print current runtime profile presets (machine-readable):

```bash
make fastpath-perf-probe-selftest-profiles
```

JSON variant:

```bash
make fastpath-perf-probe-selftest-profiles RUNTIME_PROFILE_FORMAT=json
```

JSON output includes `schema_version` for forward-compatible parser contracts (`RUNTIME_PROFILE_SCHEMA_VERSION`, positive integer).
When evolving JSON fields, keep additive changes on the same schema version and bump schema version for removals/renames/type changes.

Planner ratio-check self-test (no PostgreSQL startup; validates pass/fail parser semantics):

```bash
make planner-cost-check-selftest PLANNER_SELFTEST_TMP_ROOT=/private/tmp
```

Planner cost probe (quantifies plan/cost deltas for `fastpath=off/on` across row counts):

```bash
make planner-cost-probe \
  PLANNER_PROBE_ROWS=1000,10000,50000 \
  PLANNER_PROBE_PORT=65496 \
  PLANNER_PROBE_OUT=auto:/private/tmp
```

Validate planner probe forced-index ratio against a minimum threshold:

```bash
make planner-cost-check \
  PLANNER_PROBE_LOG=/private/tmp/pg_sorted_heap_planner_probe_YYYYMMDD_HHMMSS_PID.log \
  PLANNER_MIN_OFF_OVER_ON=100.0
```

Validate that default point-query path uses an index for larger row sets:

```bash
make planner-cost-default-path-check \
  PLANNER_PROBE_LOG=/private/tmp/pg_sorted_heap_planner_probe_YYYYMMDD_HHMMSS_PID.log \
  PLANNER_MIN_DEFAULT_INDEX_ROWS=10000
```

Default-path checker self-test (no PostgreSQL startup):

```bash
make planner-cost-default-path-check-selftest PLANNER_SELFTEST_TMP_ROOT=/private/tmp
```

Generate machine-readable planner probe summary (for CI artifacts/trending):

```bash
make planner-cost-summary \
  PLANNER_PROBE_LOG=/private/tmp/pg_sorted_heap_planner_probe_YYYYMMDD_HHMMSS_PID.log \
  PLANNER_SUMMARY_FORMAT=json \
  PLANNER_SUMMARY_OUT=/private/tmp/pg_sorted_heap_planner_probe_summary.json
```

One-command probe + summary artifact pipeline:

```bash
make planner-cost-probe-summary \
  PLANNER_PROBE_ROWS=1000,10000,50000 \
  PLANNER_PROBE_PORT=65496 \
  PLANNER_PROBE_OUT=auto:/private/tmp \
  PLANNER_SUMMARY_FORMAT=json \
  PLANNER_SUMMARY_OUT=auto:/private/tmp
```

One-command strict gate (probe + summary + ratio check + default-path check):

```bash
make planner-cost-gate \
  PLANNER_PROBE_ROWS=1000,10000,50000 \
  PLANNER_PROBE_PORT=65496 \
  PLANNER_PROBE_OUT=auto:/private/tmp \
  PLANNER_SUMMARY_FORMAT=json \
  PLANNER_SUMMARY_OUT=auto:/private/tmp \
  PLANNER_MIN_OFF_OVER_ON=100.0 \
  PLANNER_MIN_DEFAULT_INDEX_ROWS=10000
```

Planner gate self-test (mocked chain; no PostgreSQL startup):

```bash
make planner-cost-gate-selftest PLANNER_SELFTEST_TMP_ROOT=/private/tmp
```

Compare two planner probe log sets (noise-resistant aggregate gate):

```bash
make planner-cost-compare-median \
  PLANNER_REF_DIR=/private/tmp/planner_ref_set \
  PLANNER_NEW_DIR=/private/tmp/planner_new_set \
  PLANNER_MIN_FRACTION=0.90 \
  PLANNER_SET_STAT_MODE=median \
  PLANNER_SET_MIN_SAMPLES=1
```

`PLANNER_SET_STAT_MODE` supports `median`, `p05`, `p95`, and `trimmed-mean`.
Default `PLANNER_SET_MIN_SAMPLES` is mode-aware (`1` for `median`, `3` for non-median modes).
For `off_over_on` ratios (`higher is better`), prefer `median`, `p05`, or `trimmed-mean`; `p95` is optimistic.
Planner set comparator header includes `metric_polarity=higher_is_better`.

Planner set-comparator self-test:

```bash
make planner-cost-compare-selftest PLANNER_SELFTEST_TMP_ROOT=/private/tmp
```

Planner summary self-test:

```bash
make planner-cost-summary-selftest PLANNER_SELFTEST_TMP_ROOT=/private/tmp
```

Probe output lines use normalized key/value format:
- `planner_probe|rows=<n>|fastpath=<off|on>|query=<point_default|range_default|point_forced_index>|plan=<node>|startup_cost=<x>|total_cost=<y>|plan_rows=<n>|plan_width=<n>`
- `planner_probe_compare|rows=<n>|forced_point_off_total=<x>|forced_point_on_total=<y>|off_over_on=<ratio>`
- `planner_probe_summary|forced_index_hits=<n>|forced_index_cases=<n>`
- optional warning marker when no index path is seen: `planner_probe_warning|no_index_path_detected=1`
- final success marker: `planner_probe_status=ok`.

Interpretation hint: very small tables can still prefer `Seq Scan` in default mode even with `fastpath=on`; use larger cardinalities to evaluate planner crossover behavior.

## Perf output capture modes

Write probe output to explicit file:

```bash
make fastpath-perf-probe PERF_OUT=/private/tmp/my_probe.log
```

Auto-generate filename in `/private/tmp`:

```bash
make fastpath-perf-probe PERF_OUT=auto
```

Auto-generate filename in custom absolute directory:

```bash
make fastpath-perf-probe PERF_OUT=auto:/private/tmp
```

`auto:<dir>` requires an absolute directory path; relative paths are rejected by design.

## UNNEST A/B probe knobs

`make unnest-ab-probe` accepts additional runtime knobs:

- `UNNEST_AB_LOCAL_HINT_MAX_KEYS=<n>` maps to `pg_sorted_heap.pkidx_local_hint_max_keys` (session-local hint cache key capacity).
- `UNNEST_AB_ASSUME_UNIQUE_KEYS=<on|off>` maps to `pg_sorted_heap.pkidx_assume_unique_keys`.
- `UNNEST_AB_WARMUP_SELECTS=<n>` performs unmeasured select warmup iterations before timed `JOIN UNNEST` / `ANY(...)` loops (`1` by default; set `0` for cold-start latency measurement).
- `UNNEST_AB_ALLOW_UNSAFE_UNIQUE=<on|off>` is a fail-closed override required when `UNNEST_AB_ASSUME_UNIQUE_KEYS=on`.
- Safe baseline for probe-heavy workloads: start with `UNNEST_AB_LOCAL_HINT_MAX_KEYS=16384` before enabling any unsafe assumptions.

Production drift sentinel (compact check):

```bash
make unnest-ab-perf-sentinel \
  UNNEST_SENTINEL_MIN_INSERT_RATIO=0.90 \
  UNNEST_SENTINEL_MIN_JOIN_UNNEST_RATIO=1.10 \
  UNNEST_SENTINEL_MIN_ANY_ARRAY_RATIO=1.00
```

Notes:
- Sentinel always runs `assume_unique_keys=off`.
- Default `UNNEST_SENTINEL_WARMUP_SELECTS=1` evaluates warm steady-state throughput.
- Set `UNNEST_SENTINEL_WARMUP_SELECTS=0` when you explicitly want cold-start latency sensitivity.

Safety note:
- `pg_sorted_heap.pkidx_assume_unique_keys` is superuser-only and defaults to `off`.
- Turn it on only for workloads with guaranteed unique probe keys; otherwise correctness can degrade on duplicate-key data.
- Even with unique keys, high-scale throughput can degrade; keep this mode `off` for production baselines unless workload-specific evidence proves benefit.
- Direct `run_unnest_ab_probe*` execution with `assume_unique_keys=on` requires explicit override: `UNNEST_AB_ALLOW_UNSAFE_UNIQUE=on`.

## UNNEST A/B logset comparator

For noise-resistant performance gating across multiple probe runs:

```bash
make unnest-ab-compare-median \
  UNNEST_AB_REF_DIR=/abs/ref_logs \
  UNNEST_AB_NEW_DIR=/abs/new_logs \
  UNNEST_AB_MIN_FRACTION=0.90 \
  UNNEST_AB_SET_STAT_MODE=median
```

Notes:
- log files must match `pg_sorted_heap_unnest_ab_*.log` and contain `ratio_kv|insert=...|join_unnest=...|any_array=...`.
- comparator supports `UNNEST_AB_SET_STAT_MODE=median|p05|p95|trimmed-mean`.
- default `UNNEST_AB_SET_MIN_SAMPLES` is `1` for `median` and `3` for `p05`/`p95`/`trimmed-mean`.
- comparator header includes `metric_polarity=higher_is_better` for policy automation.

## UNNEST A/B one-command gate

To auto-produce reference and candidate logsets and run comparator in one command:

```bash
make unnest-ab-gate \
  UNNEST_GATE_REF_RUNS=3 \
  UNNEST_GATE_NEW_RUNS=3 \
  UNNEST_GATE_BATCH_SIZE=400 \
  UNNEST_GATE_BATCHES=20 \
  UNNEST_GATE_SELECT_ITERS=120 \
  UNNEST_GATE_PROBE_SIZE=64 \
  UNNEST_GATE_BASE_PORT=65488 \
  UNNEST_GATE_OUT_ROOT=/private/tmp \
  UNNEST_GATE_KEYCACHE_TRIGGER=1 \
  UNNEST_GATE_KEYCACHE_MIN_DISTINCT=2 \
  UNNEST_GATE_KEYCACHE_MAX_TIDS=262144 \
  UNNEST_GATE_SEGMENT_PREFETCH_SPAN=128 \
  UNNEST_GATE_LOCAL_HINT_MAX_KEYS=16384 \
  UNNEST_GATE_ASSUME_UNIQUE_KEYS=off \
  UNNEST_GATE_MIN_FRACTION=0.93 \
  UNNEST_GATE_STAT_MODE=median
```

For throughput-ratio gating:
- `UNNEST_GATE_STAT_MODE=median`, `p05`, or `trimmed-mean` is recommended.
- `p95` is intentionally blocked by default in gate mode because it is optimistic for "higher-is-better" metrics.
- override only when explicitly needed: `UNNEST_GATE_ALLOW_OPTIMISTIC_TAIL=on`.
- `UNNEST_GATE_ASSUME_UNIQUE_KEYS=on` is fail-closed by default in gate mode; explicit override is required:
  - `UNNEST_GATE_ALLOW_UNSAFE_UNIQUE=on`

Output includes:
- comparator verdict (`unnest_ab_set_compare status=ok|regression`)
- produced directories (`unnest_ab_probe_gate_output|reference_dir=...|candidate_dir=...`)
- reference retention marker (`reference_retained=0|1`)

Reuse existing reference set (skip ref generation):

```bash
make unnest-ab-gate \
  UNNEST_GATE_REF_RUNS=0 \
  UNNEST_GATE_NEW_RUNS=3 \
  UNNEST_GATE_EXISTING_REF_DIR=/abs/ref_logs \
  UNNEST_GATE_OUT_ROOT=/private/tmp \
  UNNEST_GATE_MIN_FRACTION=0.93 \
  UNNEST_GATE_STAT_MODE=median
```

When `UNNEST_GATE_EXISTING_REF_DIR` is set:
- `UNNEST_GATE_REF_RUNS` may be `0`.

To reduce `/private/tmp` growth when reference set is generated on the fly:

```bash
make unnest-ab-gate \
  UNNEST_GATE_REF_RUNS=3 \
  UNNEST_GATE_NEW_RUNS=3 \
  UNNEST_GATE_OUT_ROOT=/private/tmp \
  UNNEST_GATE_KEEP_NEW_DIR=on
```

When `UNNEST_GATE_KEEP_NEW_DIR=on` and reference source is generated:
- gate removes generated reference directory after comparison (`reference_retained=0` in output).
- gate output includes `reference_source=existing`.
- calibrated safe default for gate mode is `UNNEST_GATE_MIN_FRACTION=0.93` (median, 5-run set on current benchmark profile).

## Interpreting observability in probe output

Expected direction in churn scenario (relative to baseline):

- `observability` should include `modes={segment_fastpath=on,...}` in runtime probe lanes.
- `segment_lookups` should be greater than `0` (confirms fastpath lookup path is exercised).
- `local_hint_touches` should be greater than `0` (confirms local-hint write path is exercised).
- `local_hint_evictions` can become non-zero after key-cap saturation.
- `local_hint_map_resets` should typically remain `0` during normal churn probes.
- `local_hint_merges` is diagnostic (merge-shape dependent) and should not be used as a strict pass/fail invariant.
- `defensive_state_recovers` should stay `0` in normal runs; non-zero indicates defensive self-recovery from inconsistent in-memory merge state.
- `elapsed_ms` is typically higher than baseline due to higher cardinality and eviction churn.
- `segment_lookup_errors` should remain `0`.
- `fastpath-perf-compare` should report `status=ok` unless either scenario slowdown exceeds `PERF_MAX_SLOWDOWN`.
- `fastpath-perf-probe-median` prints per-run values and median/mean summaries for baseline and churn scenarios.
- `fastpath-perf-compare-median` compares aggregated stats across log sets and reports `status=ok|regression`.
- `PERF_SET_STAT_MODE` supports `median`, `p95`, and `trimmed-mean`.
- `PERF_SET_MIN_SAMPLES` can override required set size; for non-median modes keep it at `>=3` for stable statistics.
- comparator input values for `baseline_fastpath`/`churn_fastpath` must be strictly positive; `0` or negative values fail fast.

## Cleanup policy

Operational scripts use ephemeral clusters under `/private/tmp` and cleanup on exit.
If a run is interrupted, remove stale dirs:

```bash
make tmp-clean TMP_CLEAN_ROOT=/private/tmp
```

Optional: clean only artifacts older than N seconds (protects very fresh run dirs):

```bash
make tmp-clean TMP_CLEAN_ROOT=/private/tmp TMP_CLEAN_MIN_AGE_S=3600
```

Cleaner now skips live PostgreSQL temp instances (directories with active `postmaster.pid`) and emits:
- `skip_live <path>` for active postmaster dirs.
- `skip_recent age_s=<n> min_age_s=<n> <path>` when age-threshold blocks deletion.
- summary line:
  `tmp_clean root=<dir> removed=<n> skipped_live=<n> skipped_recent=<n> min_age_s=<n> removed_kb=<n>`.

Cleanup self-test:

```bash
make tmp-clean-selftest TMP_SELFTEST_ROOT=/private/tmp
```

Run full lightweight selftest bundle (all script-level checks, no PostgreSQL startup):

```bash
make selftest-lightweight LIGHTWEIGHT_SELFTEST_TMP_ROOT=/private/tmp
```

The runner emits per-script `elapsed_s=<n>` and final `total_elapsed_s=<n>` telemetry.
It also includes script-level guards for runtime `TMPDIR`, `auto:<dir>` output-target, `PORT`, shared define-parser behavior, runtime-selftest profile validation, and runtime-profile export format (`selftest_runtime_tmpdir_validation.sh`, `selftest_runtime_output_target_validation.sh`, `selftest_runtime_port_validation.sh`, `selftest_extract_pg_sorted_heap_define.sh`, `selftest_runtime_perf_selftest_validation.sh`, `selftest_runtime_profile_export.sh`).
For machine-readable output use:

```bash
make selftest-lightweight \
  LIGHTWEIGHT_SELFTEST_TMP_ROOT=/private/tmp \
  LIGHTWEIGHT_SELFTEST_FORMAT=jsonl
```

Optional: set `LIGHTWEIGHT_SELFTEST_RUN_LABEL=<id>` for a human-readable per-run label.
JSONL events include `schema_version=1`, stable per-run `run_id`, optional/custom `run_label`, `runner_pid`, monotonic `event_seq`, and `event_ts` (UTC RFC3339, second precision).

CI note: workflow `perf-compare-selftest` stores the JSONL stream as an artifact (`clustered-pg-lightweight-selftest-<run_id>-<run_attempt>`) for post-run diagnostics.

## Vector tooling quick commands

Graph-sidecar tooling now has a dedicated lifecycle smoke:

```bash
make test-graph-builder TMP_SELFTEST_ROOT=/private/tmp TEST_GRAPH_PORT=65489
```

For the local `bench_nomic` synthetic ANN setup:

```bash
make build-graph-bench-nomic \
  VECTOR_BENCH_DSN='host=/tmp port=65432 dbname=bench_nomic'

make bench-nomic-ann \
  VECTOR_BENCH_DSN='host=/tmp port=65432 dbname=bench_nomic'
```

Both targets use `./scripts/find_vector_python.sh` to locate a Python with
`numpy` and `psycopg2`.
