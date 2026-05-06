---
layout: default
title: Partitioned HNSW C Helper Gate
nav_order: 22
---

# Spec: Partitioned HNSW C Helper Gate

Status: proposed
Risk tier: CAUTION
Primary goal: decide when `sorted_hnsw_partition_search(...)` should move from
PL/pgSQL orchestration to a C implementation without changing the SQL contract.

## Problem

`sorted_hnsw_partition_search(...)` implements the safe route-first filtered
ANN contract for declarative partitions:

- validate selected leaves against the requested parent;
- require a valid leaf-local `sorted_hnsw` index on the vector column;
- run local ANN on selected leaves with `local_k`;
- union candidate pools;
- globally rerank by exact distance and return final `top_k`.

The current implementation is PL/pgSQL that builds one dynamic `UNION ALL`
query across selected leaves. This is simple and safe, but route-first latency
on small partitions can expose orchestration overhead.

## Current Evidence

Local PostgreSQL 18 benchmark, `8 x 50K` rows, self-query top-10:

| Method | Average latency | Recall@10 |
|---|---:|---:|
| `direct_leaf_index` | `2.942 ms` | `100.0%` |
| `helper_selected` | `5.359 ms` | `100.0%` |
| `parent_filtered_exact` | `8.849 ms` | `100.0%` |
| `helper_all_leaves` | `23-25 ms` | workload-dependent |

Interpretation:

- the helper already beats parent filtered exact search for the selected-leaf
  route-first workload;
- the gap to direct leaf index scan is about `2.4 ms` on the measured small
  partition shape;
- this justifies a C helper only if small-leaf routed latency is a product
  target, not as a correctness requirement.

## Non-Goals

- Do not change the SQL signature or result shape before a separate API spec.
- Do not relax fail-closed leaf/index validation.
- Do not introduce transparent arbitrary `WHERE` filtered ANN planner support.
- Do not concatenate local top-k lists without global exact rerank.
- Do not replace the existing PL/pgSQL helper until the C helper matches its
  behavior on error paths and result ordering.

## Candidate C Contract

The C helper should preserve the SQL-level behavior:

- same function name or a private C implementation under the same SQL wrapper;
- same arguments: `parent`, `vector_column`, `query`, `top_k`, `local_k`,
  `leaf_relids`, `fail_on_unsupported`;
- same returned columns: `leaf_relid`, `leaf_name`, `distance`, `row_data`;
- same validation failures for wrong-parent leaves, unsupported leaves,
  missing vector columns, unsupported vector types, missing leaf-local HNSW
  indexes, `local_k < top_k`, and `local_k > sorted_hnsw.ef_search`;
- deterministic global ordering by exact distance, with a stable tie-breaker if
  required by regression output.

Implementation options:

1. Keep the public PL/pgSQL wrapper and call a private C SRF after catalog
   validation.
2. Move validation and execution fully to C, using SPI only for the local leaf
   ordered scans.
3. Later, bypass SPI for local leaf scans only if a safe internal Index AM scan
   path is specified and benchmarked.

Option 1 or 2 is the first viable implementation. Option 3 is a separate
executor/internal-scan project.

## Promotion Gate

Promote the C helper only if the same benchmark harness shows one of:

- selected-leaf average latency within `25%` of `direct_leaf_index` while
  preserving `100%` self-query recall on the existing local benchmark;
- at least `30%` lower average latency than the PL/pgSQL helper on small-leaf
  route-first workloads;
- materially lower p95 latency on a real routed workload where helper overhead
  is visible in the profile.

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
- `svec` and `hsvec` partitioned leaves.

Expected: identical rows or intentionally updated stable ordering with the
same semantic result set.

### C2. Global exact merge invariant

Use a partitioned table where all true top-k results come from one dense leaf.

Expected: the helper returns all global winners from that leaf. A local top-k
concatenation implementation must fail this test.

### C3. Benchmark gate

Run:

```bash
make bench-partitioned-sorted-hnsw
```

Expected: the candidate C helper reaches one promotion condition above without
recall regression.

## Decision

The C helper is a performance-gated follow-up, not a release blocker for the
current route-first partitioned HNSW contract. The PL/pgSQL helper remains the
safe default until a C implementation clears behavioral parity and benchmark
promotion gates.
