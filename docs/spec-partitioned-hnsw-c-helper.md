---
layout: default
title: Partitioned HNSW C Helper Gate
nav_order: 22
---

# Spec: Partitioned HNSW C Helper Gate

Status: implemented
Risk tier: CAUTION
Primary goal: keep `sorted_hnsw_partition_search(...)` on the same SQL
contract while executing the per-leaf route-first search through the
`sorted_hnsw` Index AM instead of PL/pgSQL dynamic fanout.

## Problem

`sorted_hnsw_partition_search(...)` implements the safe route-first filtered
ANN contract for declarative partitions:

- validate selected leaves against the requested parent;
- require a valid leaf-local `sorted_hnsw` index on the vector column;
- run local ANN on selected leaves with `local_k`;
- union candidate pools;
- globally rerank by exact distance and return final `top_k`.
- optionally replace an underfilled ANN pool with an exact rerank over the
  same selected leaves when `exact_fallback := true`.

The original implementation was PL/pgSQL that built one dynamic `UNION ALL`
query across selected leaves. That was simple and safe, but route-first latency
on small partitions exposed orchestration overhead. The current implementation
is a C SRF bound to the same public SQL function.

## Current Evidence

Historical local PostgreSQL 18 baseline, `8 x 50K` rows, self-query top-10:

| Method | Average latency | Recall@10 |
|---|---:|---:|
| `direct_leaf_index` | `2.942 ms` | `100.0%` |
| `helper_selected` | `5.359 ms` | `100.0%` |
| `parent_filtered_exact` | `8.849 ms` | `100.0%` |
| `helper_all_leaves` | `23-25 ms` | workload-dependent |

Current C/Index-AM benchmark on the same default harness shape,
`8 x 50K` rows, 4 queries, `dim=16`, `k=10`, `local_k=32`,
`ef_search=64`:

| Method | avg ms | p50 ms | p95 ms | Recall@10 |
|---|---:|---:|---:|---:|
| `direct_leaf_index` | `3.647` | `3.045` | `5.169` | `1.0000` |
| `helper_selected` | `3.627` | `3.279` | `4.673` | `1.0000` |
| `helper_selected_exact_fallback` | `3.443` | `3.344` | `4.065` | `1.0000` |
| `parent_filtered_exact` | `9.094` | `9.100` | `9.365` | `1.0000` |
| `helper_all_leaves` | `25.417` | `24.856` | `27.060` | `1.0000` |
| `helper_all_leaves_exact_fallback` | `24.529` | `24.423` | `24.872` | `1.0000` |
| `parent_all_merge_append` | `24.232` | `24.167` | `24.728` | `1.0000` |

Interpretation:

- the C helper preserves recall on the benchmarked self-query workload;
- selected-leaf route-first helper latency is now effectively tied with a
  direct leaf index scan on this benchmark shape;
- the helper still beats parent filtered exact search for selected-leaf
  route-first workloads;
- all-leaf fanout remains comparable to PostgreSQL's parent `Merge Append`
  over all leaf indexes, so the main win is route-first filtered dispatch.

## Non-Goals

- Do not change the SQL signature or result shape before a separate API spec.
- Do not relax fail-closed leaf/index validation.
- Do not introduce transparent arbitrary `WHERE` filtered ANN planner support.
- Do not concatenate local top-k lists without global exact rerank.
- Do not promote transparent arbitrary `WHERE` filtered ANN planner support
  from this helper alone.

## Candidate C Contract

The C helper should preserve the SQL-level behavior:

- same function name or a private C implementation under the same SQL wrapper;
- same arguments: `parent`, `vector_column`, `query`, `top_k`, `local_k`,
  `leaf_relids`, `fail_on_unsupported`, `exact_fallback`;
- same returned columns: `leaf_relid`, `leaf_name`, `distance`, `row_data`;
- same validation failures for wrong-parent leaves, unsupported leaves,
  missing vector columns, unsupported vector types, missing leaf-local HNSW
  indexes, `local_k < top_k`, and `local_k > sorted_hnsw.ef_search`;
- same opt-in fallback behavior: default ANN-only semantics, and
  `exact_fallback := true` executes an exact selected-leaf rerank only when the
  ANN candidate pool underfills `top_k`;
- deterministic global ordering by exact distance, with a stable tie-breaker if
  required by regression output.

Implementation:

- the public SQL function is bound directly to the C symbol
  `sorted_hnsw_partition_search`;
- leaf discovery and validation happen in C against PostgreSQL catalogs;
- each selected leaf is opened with `AccessShareLock`;
- the local search uses PostgreSQL's Index AM APIs
  (`index_beginscan`, `index_rescan`, `index_getnext_slot`) against the
  leaf-local `sorted_hnsw` index;
- global result ordering is by distance with stable leaf/TID tie-breakers;
- `row_data` is built from the heap slot as `jsonb`, preserving SQL-visible
  result shape;
- `exact_fallback := true` still runs only when the ANN candidate pool
  underfills `top_k`.

## Promotion Gate

The C helper is promoted because the same benchmark harness shows:

- selected-leaf average latency within `25%` of `direct_leaf_index`
  (`3.627 ms` vs `3.647 ms`);
- `100%` self-query Recall@10 on the benchmarked shape;
- materially lower selected-leaf latency than the historical PL/pgSQL helper
  baseline (`5.359 ms` -> `3.627 ms`, about `32%` faster).

Required benchmark output:

- `helper_selected_sql` or historical PL/pgSQL baseline;
- candidate C helper row;
- `direct_leaf_index`;
- `parent_filtered_exact`;
- recall@10 against the same ground truth;
- p50/p95/average latency, not average only.

## Acceptance Tests

### C1. Behavioral parity

Run the existing `sorted_hnsw` regression cases for:

- selected leaves;
- all leaves;
- wrong-parent selected leaf rejection;
- selected leaf without HNSW index rejection;
- `local_k > sorted_hnsw.ef_search` rejection;
- default no-fallback underfill behavior and `exact_fallback := true`;
- `svec` and `hsvec` partitioned leaves.

Expected: identical rows or intentionally updated stable ordering with the
same semantic result set.

### C2. Global exact merge invariant

Use a partitioned table where all true top-k results come from one dense leaf.

Expected: the helper returns all global winners from that leaf. A local top-k
concatenation implementation must fail this test.

### C3. Benchmark gate

The benchmark must include both default and fallback-enabled helper rows if the
candidate changes fallback orchestration. Report the fallback row separately
because exact fallback is an operator-selected correctness/coverage trade-off,
not the default latency path.

The current SQL benchmark reports:

- `helper_selected` and `helper_selected_exact_fallback`;
- `helper_all_leaves` and `helper_all_leaves_exact_fallback`;
- `direct_leaf_index`;
- `parent_filtered_exact`;
- `parent_all_merge_append`;
- average, p50, p95, and recall@K for each method.

Run:

```bash
make bench-partitioned-sorted-hnsw
```

Expected: no recall regression and no loss of the selected-leaf latency tie
with direct leaf index scan without a documented reason.

## Decision

`sorted_hnsw_partition_search(...)` now uses the C/Index-AM implementation.
This closes the route-first helper overhead gap without changing the SQL
contract. Transparent parent-dispatched ANN/GraphRAG planner paths remain a
separate design problem.
