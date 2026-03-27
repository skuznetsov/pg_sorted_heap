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
- next step now also exists in narrow form:
  - `sorted_heap_graph_segment_register(...)`
  - `sorted_heap_graph_segment_resolve(...)`
  - `sorted_heap_graph_rag_routed(...)`
  - this is a metadata-driven `int8` range router layered on top of the
    segmented wrapper
- and one more practical routing surface now exists:
  - `sorted_heap_graph_exact_register(...)`
  - `sorted_heap_graph_exact_resolve(...)`
  - `sorted_heap_graph_rag_routed_exact(...)`
  - this is the exact-key router for tenant / KB style shard selection
- and the first richer metadata filter now exists on top of both routed paths:
  - optional `segment_group` labels at registration time
  - optional `segment_groups text[]` filters at resolve/query time
  - the filter array order is also a bounded-fanout preference order
  - this is the first beta surface for hot/sealed or relation-family pruning
- and the first registry-backed reuse layer now exists on top of that:
  - `sorted_heap_graph_route_policy_register(...)`
  - `sorted_heap_graph_route_policy_groups(...)`
  - `sorted_heap_graph_rag_routed_policy(...)`
  - `sorted_heap_graph_rag_routed_exact_policy(...)`
  - this keeps hot/sealed preference out of ad hoc query literals
- and the first second routing dimension now exists too:
  - optional `relation_family text` on both range-routed and exact-key shard
    registry rows
  - optional `relation_family := ...` filtering in config/resolve functions
    and in both raw/policy-backed routed GraphRAG wrappers
  - this is still narrow beta metadata, not a finished general router
- and the first reusable route-profile layer now exists on top of that:
  - `sorted_heap_graph_route_profile_register(...)`
  - `sorted_heap_graph_route_profile_resolve(...)`
  - `sorted_heap_graph_rag_routed_profile(...)`
  - `sorted_heap_graph_rag_routed_exact_profile(...)`
  - this now bundles either:
    - `policy_name + relation_family + fanout_limit`, or
    - inline `segment_groups + relation_family + fanout_limit`
  - so the operator no longer needs a separate policy row just to save one
    shard-group ordering
- and the next operator shortcut now exists on top of profiles:
  - `sorted_heap_graph_route_default_register(...)`
  - `sorted_heap_graph_route_default_resolve(...)`
  - `sorted_heap_graph_rag_routed_default(...)`
  - `sorted_heap_graph_rag_routed_exact_default(...)`
  - this lets one route bind a default profile once instead of passing
    `profile_name` in every query
- and the next registry cleanup now exists under the routed path:
  - `sorted_heap_graph_segment_meta_register(...)`
  - `sorted_heap_graph_segment_meta_config(...)`
  - `sorted_heap_graph_segment_meta_unregister(...)`
  - range-routed and exact-key routed rows can now leave
    `segment_group` / `relation_family` as `NULL` and inherit them from
    shard-local metadata instead
  - when both are present, row-local routed metadata still overrides the
    shared shard metadata
- and the next operator-facing introspection layer now exists on top of that:
  - `sorted_heap_graph_segment_catalog(...)`
  - `sorted_heap_graph_exact_catalog(...)`
  - these expose route-local metadata, shared shard metadata, effective
    resolved metadata, and per-column source markers (`route|shared|unset`)
  - this does not change routing behavior; it makes the current registry model
    easier to inspect and debug
- what is still missing:
  - more than one optional text metadata dimension per registry row
  - a product-quality shard router contract for tenant / KB / relation-family
    pruning without hand-managed registration tables

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

The newest bounded step makes that path slightly less hand-wired: routed and
exact-key routed GraphRAG can now combine:

- route range or exact route key
- stored shard-group policy order
- one optional `relation_family` filter

And the newest ergonomic layer removes one more repeated query burden:

- a named route profile can now store either a policy-backed or inline-group
  family/fanout combination
- profile-backed wrappers reuse the existing routed paths instead of adding a
  new scoring contract

And the newest operator shortcut removes one more argument from the query side:

- a route can now bind one default profile once
- default-backed wrappers resolve that profile implicitly at query time

And the newest narrow cleanup removes one more source of repeated registry
state:

- shard-local metadata can now be registered once per concrete shard relation
- both range-routed and exact-key routed rows can inherit that metadata when
  their own `segment_group` / `relation_family` values are `NULL`
- this reduces duplicated registry data, but it still does not replace the
  current hand-managed routing model

And the newest operator-facing layer makes that model more inspectable:

- range-routed and exact-key routed catalogs now show both raw and effective
  metadata
- each effective metadata column also reports whether it came from the route
  row, the shared shard metadata row, or remained unset
- this is deliberately introspection-only; it does not widen the routing
  contract

That is still beta, but it is the first real multi-dimensional routing surface
inside the extension. The next honest step is no longer “can we add metadata?”
but “which metadata should become first-class beyond shard group + one family
label, and how much routing can move from ad hoc registries into a cleaner
operator model?”
