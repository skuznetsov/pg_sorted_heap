---
layout: default
title: Release 0.13.0
nav_order: 10
---

# Release 0.13.0 draft

This page is the repo-owned draft for the `v0.13.0` GitHub release.

## Summary

`0.13.0` promotes the narrow fact-shaped GraphRAG contract to the stable
release surface.

Stable in this release:

- `sorted_heap` table AM
- `sorted_hnsw` Index AM for `svec` and `hsvec`
- fact-shaped GraphRAG:
  - `sorted_heap_graph_rag(...)`
  - `sorted_heap_graph_register(...)`
  - `sorted_heap_graph_config(...)`
  - `sorted_heap_graph_unregister(...)`
  - `sorted_heap_graph_rag_stats()`
  - `sorted_heap_graph_rag_reset_stats()`

Still beta:

- lower-level GraphRAG helper/wrapper building blocks
- code-corpus snippet/symbol/lexical retrieval contracts used by the benchmark
  harnesses

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
- crash recovery for registered/indexed graph tables
- concurrent DML with `sorted_heap_compact_online(...)`
- concurrent DML with `sorted_heap_merge_online(...)`

### Backend-local stage observability

GraphRAG now exposes backend-local last-call stats through:

- `sorted_heap_graph_rag_stats()`
- `sorted_heap_graph_rag_reset_stats()`

The stats cover:

- seed count
- expanded row count
- reranked row count
- returned row count
- ANN / expand / rerank / total timing

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

## Upgrade

```sql
ALTER EXTENSION pg_sorted_heap UPDATE TO '0.13.0';
```

## Recommended verification after upgrade

```bash
make test-graphrag-release
```

This wrapper target runs the full narrow GraphRAG release bundle:

- SQL regression (`pg_sorted_heap`, `sorted_hnsw`, `graph_rag`)
- lifecycle
- crash recovery
- concurrent online-operation coverage

## Release-candidate verification

Fresh `0.13.0` release-candidate checks run on `2026-03-26`:

```bash
make test-graphrag-release
```

Observed signals:

- `make installcheck ...` -> `All 3 tests passed`
- `make test-graphrag-lifecycle` -> `status=ok pass=20 fail=0 total=20`
- `make test-graphrag-crash` -> `status=ok pass=22 fail=0 total=22`
- `make test-graphrag-concurrent` -> `status=ok pass=40 fail=0 total=40`
- `make test-graphrag-release` -> wrapper target verified end-to-end with the
  same pass signals above

This bundle covers the narrow stable GraphRAG surface directly:

- SQL regression coverage for unified syntax, schema registration, and stats
- upgrade + dump/restore lifecycle for registered fact graphs
- crash recovery for registered/indexed graph tables
- concurrent DML with online compact / online merge on registered fact graphs

## Extension-wide release-candidate checks

Fresh extension-wide release checks run on `2026-03-26`:

```bash
make pg-core-regression-smoke
make policy-safety-selftest
make test-dump-restore
make test-toast
make test-alter-table
make test-crash-recovery
make test-concurrent
make test-pg-upgrade
```

Observed signals:

- `make pg-core-regression-smoke` ->
  `status=ok|installcheck_target=present|tap_prove=present|isolation_regress=missing`
- `make policy-safety-selftest` -> all constituent selftests reported `status=ok`
- `make test-dump-restore` -> `status=ok pass=10 fail=0 total=10`
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

- **stable**: sorted storage, planner-integrated vector search, and the narrow
  fact-shaped GraphRAG API
- **beta**: lower-level GraphRAG helper composition
- **reference logic**: code-corpus retrieval contracts from the benchmark
  harnesses

That keeps the stable promise aligned with what is repeatedly verified in the
repo today.
