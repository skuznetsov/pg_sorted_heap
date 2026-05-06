---
layout: default
title: Release 0.13.0
nav_order: 10
---

# Release 0.13.0

This page is the repo-owned release note for `v0.13.0`.

## Summary

`0.13.0` promotes the narrow fact-shaped GraphRAG contract to the stable
release surface.

Stable in this release:

- `sorted_heap` table AM
- `sorted_hnsw` Index AM for `svec` and `hsvec`
- Partition-aware `sorted_hnsw` helpers:
  - `sorted_hnsw_partition_search(...)`
  - `sorted_hnsw_partition_search_status(...)`
- Fact-shaped GraphRAG (single-table):
  - `sorted_heap_graph_rag(...)`
  - `sorted_heap_graph_register(...)`
  - `sorted_heap_graph_config(...)`
  - `sorted_heap_graph_unregister(...)`
  - `sorted_heap_graph_rag_stats()`
  - `sorted_heap_graph_rag_reset_stats()`
  - `sorted_heap_graph_route_last_stats()`
- Routed GraphRAG (multi-shard, the recommended app-facing flow):
  - `sorted_heap_graph_route(...)` — unified routed query dispatcher
  - `sorted_heap_graph_route_plan(...)` — routing introspection/explain
  - Setup helpers for the canonical flow:
    - `sorted_heap_graph_exact_register(...)` / `_unregister(...)`
    - `sorted_heap_graph_segment_register(...)` / `_unregister(...)`
    - `sorted_heap_graph_route_profile_register(...)` / `_unregister(...)`
    - `sorted_heap_graph_route_default_register(...)` / `_unregister(...)`
    - `sorted_heap_graph_route_policy_register(...)` / `_unregister(...)`
    - `sorted_heap_graph_segment_meta_register(...)` / `_unregister(...)`

Still beta:

- Lower-level routed GraphRAG building blocks:
  - `_routed(...)`, `_routed_exact(...)`, `_routed_policy(...)`,
    `_routed_profile(...)`, `_routed_default(...)` and their exact-key variants
  - `sorted_heap_graph_rag_segmented(...)` (direct shard-array merge)
  - Deep catalog/introspection helpers (`_segment_catalog`, `_exact_catalog`,
    `_route_profile_catalog`, `_route_catalog`)
  - Config/resolve helpers (`_segment_config`, `_exact_config`,
    `_route_profile_config`, `_route_default_config`, etc.)
- Lower-level GraphRAG expand/rerank helpers and scan wrappers
- Code-corpus snippet/symbol/lexical retrieval contracts
- FlashHadamard experimental retrieval lane:
  - SQL surface from `sql/flashhadamard_experimental.sql`
  - file-backed mmap store + exhaustive parallel engine scan
  - optional `FH_INT16=1` Apple/NEON path
  - documented tech debt around `pthread` inside the backend

Legacy/manual:

- `svec_ann_scan(...)`
- `svec_ann_search(...)`
- `svec_hnsw_scan(...)`

## Highlights

### Stable fact-shaped GraphRAG API

The primary entry point is now:

```sql
SELECT *
FROM sorted_heap_graph_rag(
    'facts'::regclass,
    '[0.1,0.2,0.3,...]'::svec,
    relation_path := ARRAY[1, 2],
    ann_k := 64,
    top_k := 10,
    score_mode := 'path'
);
```

This stable contract is intentionally narrow:

- fact rows clustered by `(entity_id, relation_id, target_id)`, or an
  equivalent registered alias mapping
- one-hop or two-hop expansion
- `score_mode := 'endpoint' | 'path'`
- exact rerank on the expanded candidate set

### Schema registration for non-canonical fact tables

GraphRAG now supports alias schemas through:

- `sorted_heap_graph_register(...)`
- `sorted_heap_graph_config(...)`
- `sorted_heap_graph_unregister(...)`

This keeps the user-facing GraphRAG syntax stable even when fact tables use
different column names.

### Lifecycle hardening

The fact-shaped GraphRAG path now has dedicated coverage for:

- `0.12.0 -> 0.13.0` extension upgrade
- dump/restore persistence of graph registration metadata
- dump/restore persistence of segmented/routed GraphRAG registries, including
  shared shard metadata, shared `segment_labels`, and effective default
  `segment_labels`
- dump/restore persistence of the unified router flow:
  `sorted_heap_graph_route(...)` and `sorted_heap_graph_route_plan(...)`
  produce identical results before and after restore
- crash recovery for registered/indexed graph tables
- concurrent DML with `sorted_heap_compact_online(...)`
- concurrent DML with `sorted_heap_merge_online(...)`
- `sorted_hnsw` chunked cache integration test covering local-only,
  shared-cache publish/attach, and multi-index overwrite scenarios

### Unified routed GraphRAG dispatcher

For multi-shard workloads (tenant isolation, knowledge-base routing), the
new unified dispatcher replaces the previous zoo of per-routing-mode
wrappers with one app-facing entry point:

```sql
SELECT *
FROM sorted_heap_graph_route(
    'tenants',
    '[0.1,0.2,0.3,...]'::svec,
    relation_path := ARRAY[1, 2],
    route_key := 'acme',
    ann_k := 64,
    top_k := 10,
    score_mode := 'path'
);

SELECT * FROM sorted_heap_graph_route_plan(
    'tenants', route_key := 'acme');
```

Resolution order: exact/range key → explicit profile → explicit policy →
call-site overrides → default profile → base dispatcher. See `docs/api.md`
"Routed GraphRAG: operator recipe" for the full setup/inspect/query flow.

### Shared cache correctness fix

Fixed a multi-index shared-cache corruption bug where
`shnsw_shared_scan_cache_attach()` held bare pointers into shared memory.
A second index's publish could silently corrupt the first index's cache.
The attach path now deep-copies all bulk data. Regression-guarded by the
multi-index overwrite phase in `scripts/test_hnsw_chunked_cache.sh`.

### Backend-local stage observability

GraphRAG now exposes backend-local last-call stats through:

- `sorted_heap_graph_rag_stats()`
- `sorted_heap_graph_rag_reset_stats()`
- `sorted_heap_graph_route_last_stats()` for segmented/routed per-shard rows

The stats cover:

- seed count
- expanded row count
- reranked row count
- returned row count
- ANN / expand / rerank / total timing

For segmented/routed calls, `sorted_heap_graph_route_last_stats()` identifies
the concrete `source_rel` for each shard and the public aggregate stats row is
the sum of those shard rows in the same backend.

## Benchmark snapshots

### AWS Gutenberg (`~104K x 2880D`, top-10)

- `sorted_hnsw (hsvec)`: `1.404 ms`, `100.0% Recall@10`
- pgvector `halfvec`: `2.031 ms`, `99.8% Recall@10`

### AWS fact-shaped multihop GraphRAG (`5K` chains, `384D`)

- `sorted_heap_expand_twohop_path_rerank()`: median `0.962 ms`,
  stable `98.4% / 98.4%`
- `sorted_heap_graph_rag_twohop_path_scan()`: median `1.025 ms`,
  stable `98.4% / 98.4%`
- `pgvector` parity row: median `1.434 ms`, `84.4-89.1`
- `zvec`: median `1.711 ms`, `100.0 / 100.0`
- `Qdrant`: median `3.355 ms`, `100.0 / 100.0`

These numbers describe the narrow stable fact-shaped contract, not the broader
code-corpus reference workflows.

### FlashHadamard experimental engine snapshot (`~103K x 2880D`)

- canonical engine point: exhaustive parallel scan, `5-8 ms` p50 on the
  local Gutenberg workload
- benchmark-reference external helper path: `8.7 ms`
- Apple/NEON `FH_INT16=1` remains experimental, with a current
  approximately `9%` end-to-end win on the validated local query set
- Intel AVX2 int16 LUT port is refuted for the current design; the stronger
  x86 kernel branch is the separate AVX2 gather float experiment and is not
  part of the `0.13` integrated release surface

FlashHadamard is included in `0.13` as an experimental research lane, not as
part of the stable GraphRAG contract or the default ANN path.

## Upgrade

```sql
ALTER EXTENSION pg_sorted_heap UPDATE TO '0.13.0';
```

## Recommended verification after upgrade

```bash
make test-release
```

If you only need the narrow fact-shaped GraphRAG release bundle:

```bash
make test-graphrag-release
```

`make test-release` runs the full extension-wide `0.13` release-candidate
bundle, including the narrower GraphRAG bundle.

`make test-graphrag-release` runs the full narrow GraphRAG release bundle:

- SQL regression (`pg_sorted_heap`, `sorted_hnsw`, `graph_rag`)
- lifecycle
- crash recovery
- concurrent online-operation coverage

## Release-candidate verification

Fresh `0.13.0` release-candidate checks ran on `2026-04-07`:

```bash
make test-release
```

Observed GraphRAG-related signals from that full release bundle:

- `make installcheck ...` -> `All 3 tests passed`
- `make test-graphrag-lifecycle` -> `status=ok pass=60 fail=0 total=60`
- `make test-graphrag-crash` -> `status=ok pass=22 fail=0 total=22`
- `make test-graphrag-concurrent` -> `status=ok pass=40 fail=0 total=40`
- `make test-hnsw-chunked-cache` -> `status=ok pass=31 fail=0 total=31`

This bundle covers the narrow stable GraphRAG surface directly:

- SQL regression coverage for unified syntax, schema registration,
  unified router, and stats
- upgrade + dump/restore lifecycle for registered fact graphs, the
  segmented/routed GraphRAG control plane, unified router flow, shared/default
  `segment_labels` persistence
- crash recovery for registered/indexed graph tables
- concurrent DML with online compact / online merge on registered fact graphs
- `sorted_hnsw` chunked cache integration including multi-index shared-cache
  overwrite regression

## Extension-wide release-candidate checks

Fresh extension-wide release checks ran on `2026-04-07`:

```bash
make test-release
```

Observed signals:

- `make test-release` -> wrapper target verified end-to-end with the
  constituent pass signals below
- `make pg-core-regression-smoke` ->
  `status=ok|installcheck_target=present|tap_prove=present|isolation_regress=missing`
- `make policy-safety-selftest` -> all constituent selftests reported `status=ok`
- `make test-dump-restore` -> `status=ok pass=13 fail=0 total=13`
- `make test-toast` -> `status=ok pass=26 fail=0 total=26`
- `make test-alter-table` -> `status=ok pass=36 fail=0 total=36`
- `make test-crash-recovery` -> `status=ok pass=15 fail=0 total=15`
- `make test-concurrent` -> `status=ok pass=8 fail=0 total=8`
- `make test-pg-upgrade` -> `status=ok pass=13 fail=0 total=13`

These checks exercise the already-stable core extension surface around:

- table AM crash recovery
- online compact / merge under concurrent DML
- TOAST integrity across rewrite paths
- ALTER TABLE compatibility
- dump / restore lifecycle
- `pg_upgrade` compatibility from PostgreSQL 17 to 18

## Release positioning

The clean `0.13` split is:

- **stable**: sorted storage, planner-integrated vector search, the narrow
  fact-shaped GraphRAG API, and the unified routed GraphRAG app flow
  (`sorted_heap_graph_route` + `sorted_heap_graph_route_plan` + setup helpers)
- **beta**: lower-level routed building blocks, expand/rerank helpers, scan
  wrappers, deep catalog/introspection functions, and the FlashHadamard
  experimental retrieval lane
- **reference logic**: code-corpus retrieval contracts from the benchmark
  harnesses

The stable routed flow is safe to release because:

1. It dispatches to the existing beta building blocks without introducing new
   scoring or retrieval semantics
2. It has explicit validation for ambiguous/conflicting parameters
3. Zero-shard resolution returns empty results (no crash, no silent error)
4. The canonical setup → inspect → query flow is lifecycle-covered through
   extension upgrade and dump/restore (60 lifecycle checks)
5. The underlying shared-cache correctness bug is fixed and regression-guarded
   (31 chunked-cache checks including multi-index overwrite)
