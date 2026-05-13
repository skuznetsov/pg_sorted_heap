# Witness-Bearing Bulk Load

Status: experimental design and benchmark harness.

## Motivation

`clustered_heap` directed placement can improve locality for repeated keys, but
small `multi_insert` batches are the wrong abstraction for global bulk-load
layout. A local batch can reduce one debt metric while increasing another:
key locality, page density, write amplification, and future compaction cost are
coupled.

The next storage regime should treat bulk ingestion as construction of certified
append runs, not as repeated attempts to place every row into an old page.

## LTP/WBA framing

This document uses the LTP/WBA pattern as an engineering discipline:

- local trigger: a bounded bulk window whose physical disorder is measurable;
- transport: reorder rows inside that bounded window by clustering key;
- potential: a lexicographic debt vector that every accepted move must reduce
  or keep neutral on higher-priority components;
- witness: metadata proving what was built, so future merge/pruning code does
  not infer sortedness from page-level min/max alone.

Suggested potential:

1. correctness violations; always zero, hard constraint;
2. write amplification;
3. page underfill;
4. key dispersion, measured as blocks per logical key;
5. run fragmentation;
6. future compaction debt;
7. lookup/planner overhead.

## Current falsifier harness

`scripts/bench_witness_bulk_load.sh` compares deterministic COPY order
strategies on identical rows:

- `unsorted`: shuffled input copied as-is;
- `window_N`: shuffled input split into `N`-row windows, each window sorted by
  `(tenant_id, item_id)` before COPY;
- `full_sort`: whole input sorted by `(tenant_id, item_id)` before COPY.

The harness creates a fresh temporary PostgreSQL cluster, installs the
extension, loads each strategy into a fresh `clustered_heap` table with a
two-column `clustered_pk_index`, and reports:

- `insert_ms`;
- `avg_blocks_per_tenant`;
- `max_blocks_per_tenant`;
- heap and total relation size;
- rows per heap page.

The first implementation slice is `sorted_heap_bulk_load_ordered(target,
source_sql, order_by, analyze_after := false, key_columns := NULL)`. It is a
trusted-operator SQL helper that performs a full source ordering before insert
and records an observational append-run witness.

The witness registry records:

- relation OID and relfilenode at load time;
- inclusive heap block range from `INSERT ... RETURNING ctid`;
- row count;
- rows per touched heap block;
- first/last source rows under the requested ordering, stored as JSONB;
- optional normalized `bigint[]` first/last keys for one or two integer key
  columns when `key_columns` is supplied;
- ordering mode, currently `full-order`;
- `order_by` text and `md5(source_sql)`;
- creation transaction and timestamp.

`sorted_heap_append_run_status(target default NULL)` compares the recorded
relfilenode to the current relation filenode and exposes `is_current`.
`sorted_heap_append_run_plan(target)` summarizes current and stale witnesses but
returns `can_merge = false` until crash-safe merge consumption is implemented.
`sorted_heap_append_run_invalidate(target default NULL)` and
`sorted_heap_append_run_cleanup(target default NULL)` provide explicit
fail-closed maintenance controls. Cleanup removes invalid witnesses,
relfilenode-stale witnesses, and orphaned witnesses left after the recorded
relation is dropped.

Run:

```sh
make bench-witness-bulk-load
```

or directly:

```sh
./scripts/bench_witness_bulk_load.sh /private/tmp 65511 65536 4096 1024,8192,65536 200
```

## Initial local result

On 65,536 rows, 4,096 tenants, 200-byte payload, PostgreSQL 18 local temp
cluster:

| Strategy | insert_ms | avg blocks / tenant | max blocks / tenant | heap MB |
| --- | ---: | ---: | ---: | ---: |
| unsorted | 69.185 | 15.939 | 16 | 15.625 |
| window_1024 | 66.053 | 14.288 | 16 | 15.625 |
| window_8192 | 69.319 | 7.320 | 11 | 15.625 |
| window_65536 | 65.540 | 1.454 | 2 | 15.625 |
| full_sort | 68.733 | 1.454 | 2 | 15.625 |

This supports the hypothesis that bulk reorder before COPY can collapse key
dispersion without worsening heap footprint on this workload.

The same shape held on a 262,144-row run with 16,384 tenants and 200-byte
payload:

| Strategy | insert_ms | avg blocks / tenant | max blocks / tenant | heap MB |
| --- | ---: | ---: | ---: | ---: |
| unsorted | 293.276 | 15.987 | 16 | 62.125 |
| window_4096 | 267.917 | 14.315 | 16 | 62.125 |
| window_32768 | 273.884 | 7.323 | 11 | 62.125 |
| window_262144 | 274.899 | 1.455 | 2 | 62.125 |
| full_sort | 287.366 | 1.455 | 2 | 62.125 |

Two denser 65,536-row checks show that the absolute locality gain grows as
rows per tenant grows:

| Tenants | Strategy | insert_ms | avg blocks / tenant | max blocks / tenant | heap MB |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1,024 | unsorted | 72.678 | 62.978 | 64 | 15.625 |
| 1,024 | window_4096 | 65.503 | 17.218 | 22 | 15.625 |
| 1,024 | window_8192 | 66.620 | 9.710 | 13 | 15.625 |
| 1,024 | full_sort | 68.005 | 2.908 | 3 | 15.625 |
| 256 | unsorted | 72.001 | 240.641 | 249 | 15.625 |
| 256 | window_4096 | 65.957 | 23.309 | 28 | 15.625 |
| 256 | window_8192 | 66.620 | 15.535 | 18 | 15.625 |
| 256 | full_sort | 65.584 | 8.727 | 9 | 15.625 |

Interpretation:

- `multi_insert`-local directed placement is not enough to repair shuffled bulk
  input at this scale.
- Bounded reorder has a smooth debt curve: larger windows reduce dispersion
  without changing heap size in these runs.
- Full-key ordering reaches the physical lower bound shape: roughly
  `ceil(rows_per_key / rows_per_heap_page)`, plus page-boundary effects.
- The strongest next feature is an explicit bulk ingestion mode, not another
  threshold tweak in the TableAM callback.

## Not yet claimed

- The append-run registry is observational only; merge/compaction do not consume
  it yet.
- `sorted_heap_append_run_plan(...).can_merge` is intentionally false in this
  release.
- `first_row_json` and `last_row_json` are source-row bounds. The normalized
  `first_key` and `last_key` fields currently support only one or two integer
  source columns named by `key_columns`.
- No planner-visible lookup speedup is claimed from this harness; it measures
  physical locality debt in `clustered_heap`.
- No arbitrary UPDATE/DELETE churn safety is implied.
- Do not use page-level zone-map min/max entries as sorted-run certificates.

## Next implementation slice

The next safe slice is to promote the observational registry into a proof-bearing
merge input:

- add first/last key;
- broaden normalized key representation beyond one/two integer columns;
- add window-sorted ordering mode;
- add explicit invalidation for rewrite, truncate, restore, and partition
  attach;
- add crash/recovery tests proving stale witnesses fail closed.

Only after that metadata exists should merge/compaction code consume append
runs as proof-bearing inputs.
