---
layout: default
title: GraphRAG Segmentation Plan
nav_order: 10
---

# GraphRAG segmentation plan

This document scopes the next large-scale GraphRAG branch after the `0.13`
fact-shaped stable release.

The immediate problem is not correctness of the narrow GraphRAG contract.
The immediate problem is scale on constrained-memory hosts.

At `10M x 64D` on the current AWS ARM64 box (`4 vCPU`, `8 GiB RAM`,
`4 GiB swap`), the monolithic `sorted_hnsw` build is still the practical
frontier even after the retained build improvements:

- streamed load survives
- `sorted_hnsw.build_sq8 = on` materially reduces build-vector memory
- the build now stays alive deep into `CREATE INDEX`
- but the operating model is still one large ANN graph on one small host

That is the wrong long-term shape for hundreds of millions or billions of
facts.

## Current verified constraint

Current GraphRAG helpers and wrappers operate on a concrete `sorted_heap`
relation. They do **not** currently dispatch across a partitioned-table
parent.

So the first scalable segmentation step is:

1. split facts into multiple concrete `sorted_heap` shards
2. build one `sorted_hnsw` index per shard
3. route each query to:
   - one shard when pruning is available, or
   - a bounded shard subset when pruning is partial
4. merge shard-local top-k rows globally and keep the final exact/path-aware
   rerank contract unchanged

## First benchmark result

The first segmented benchmark lives in
`scripts/bench_graph_rag_multidepth_segmented.py`.

It is a harness-side benchmark, not a released SQL API. It measures the first
two routing extremes:

- `route=all`
  - query every shard
  - merge all shard-local top-k rows
- `route=exact`
  - synthetic lower bound
  - route to the known owning shard only

Local `1M x 64D` lower-hop point (`8` shards, `ann_k=256`, `top_k=32`,
`ef_search=128`, `m=16`, `ef_construction=64`, `build_sq8=on`):

- monolith unified GraphRAG:
  - depth 1: `50.104 ms`, `100.0% / 100.0%`
  - depth 5: `121.524 ms`, `81.2% / 100.0%`
- segmented, `route=all`:
  - depth 1: `87.677 ms`, `100.0% / 100.0%`
  - depth 5: `142.472 ms`, `81.2% / 100.0%`
- segmented, `route=exact`:
  - depth 1: `10.574 ms`, `100.0% / 100.0%`
  - depth 5: `16.822 ms`, `100.0% / 100.0%`

This is the key lesson:

- segmentation alone is **not** a free latency win
- all-shard fanout preserves quality but pays a clear fanout tax
- the real gain comes only when the query path can prune most shards

So the right future contract is not just "partitioned indexes".
It is **segmentation + pruning/routing**.

## Recommended segmentation model

The recommended shape is:

- shard facts by a stable routing key
- keep each shard as a concrete `sorted_heap` table
- keep one `sorted_hnsw` index per shard
- query a bounded shard subset
- merge and rerank globally

Good routing keys depend on workload:

- `tenant_id`
  - strongest default for multi-tenant knowledge bases
- `knowledge_base_id`
  - if the system stores separate corpora per KB
- `relation family`
  - if relation sets are naturally disjoint
- time window / sealed segment
  - for append-heavy pipelines with freshness constraints

Avoid relying on entity-range sharding alone as the product story.
It is useful for synthetic benchmarking, but real deployments need routing keys
that are available from query context or cheap metadata.

## Rollout phases

### Phase 1: harness and operational benchmarks

Goal:
- prove that segmented builds fit constrained hosts better than monoliths
- quantify the difference between:
  - all-shard fanout
  - bounded fanout
  - exact routing

Deliverables:
- current local segmented harness
- AWS segmented runner:
  - `scripts/bench_graph_rag_multidepth_segmented_aws.sh`
- retained-temp build/query measurements on the same host that currently
  struggles with the monolithic `10M x 64D` point

### Phase 2: SQL-level segmented reference path

Goal:
- move beyond harness-only fanout

Reference design:
- first step now exists as a beta wrapper:
  - `sorted_heap_graph_rag_segmented(regclass[], ...)`
  - executes `sorted_heap_graph_rag(...)` per shard
  - merges candidate rows in SQL
- next step is still missing:
  - shard routing/pruning metadata or a router contract that picks a bounded
    shard subset before this wrapper runs

This is still intentionally a reference path, not the final router.

### Phase 3: productized router

Goal:
- make shard pruning cheap and stable

Possible router inputs:
- exact tenant / KB key from the application
- relation-path-level narrowing
- segment metadata tables
- a cheap centroid/sketch layer that picks a bounded shard subset before ANN

The router should not change the GraphRAG scoring contract.
Its job is only to narrow which shards need ANN seed retrieval.

### Phase 4: append-friendly large-scale operating model

For very large fact corpora, the likely long-term model is:

- sealed read-optimized segments
- one or more mutable hot segments
- background merge/compaction into larger sealed segments
- bounded query fanout across:
  - current hot segments
  - a pruned subset of sealed segments

This is a better fit for:

- hundreds of millions / billions of facts
- constrained-memory hosts
- fast insert + fast query requirements

## Current recommendation

The first comparison is now complete:

1. the low-memory monolithic AWS `10M x 64D` run completed
2. the same point completed through the streamed segmented AWS harness
3. the result was decisive:
   - `route=all` looked like the monolith
   - `route=exact` was much faster at the same quality

So the current recommendation is narrower and stronger:

1. keep monolithic low-memory work only as a survival path
2. treat `segmentation + routing` as the primary scale direction
3. spend the next engineering dollar on:
   - productizing routing/pruning
   - reducing harness-side shard fanout/merge into a real API/runtime path
   - preserving append-friendly segmented operation

The current evidence now points clearly toward segmented routing as the more
durable large-scale GraphRAG model.
