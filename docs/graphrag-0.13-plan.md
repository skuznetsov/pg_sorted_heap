---
layout: default
title: GraphRAG 0.13 Plan
nav_order: 9
---

# GraphRAG 0.13 plan

This document narrows the release target for GraphRAG.

The goal is not to ship a general-purpose graph database API in `0.13`.
The goal is to ship a stable fact-shaped GraphRAG contract on top of the
already-verified `sorted_heap` + `sorted_hnsw` path.

## Stable target

The stable candidate surface for `0.13` is:

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

Contract:

- fact rows clustered by `(entity_id, relation_id, target_id)`, or by an
  equivalent registered alias mapping
- ANN seed retrieval on `entity_id`
- `relation_path` length `1` or `2`
- `score_mode = 'endpoint' | 'path'`
- exact rerank on the expanded candidate set

Semantics:

- `relation_path := ARRAY[1]`
  - one-hop expansion
  - exact rerank on the endpoint fact
- `relation_path := ARRAY[1, 2], score_mode := 'endpoint'`
  - two-hop expansion
  - exact rerank on the second-hop endpoint only
- `relation_path := ARRAY[1, 2], score_mode := 'path'`
  - two-hop expansion
  - path-aware rerank using hop-1 and hop-2 evidence together

## What stays beta

These remain beta even after the new syntax lands:

- lower-level helper zoo:
  - `sorted_heap_expand_ids(...)`
  - `sorted_heap_expand_rerank(...)`
  - `sorted_heap_expand_twohop_rerank(...)`
  - `sorted_heap_expand_twohop_path_rerank(...)`
  - `sorted_heap_graph_rag_scan(...)`
  - `sorted_heap_graph_rag_twohop_scan(...)`
  - `sorted_heap_graph_rag_twohop_path_scan(...)`
- code-corpus contracts that currently live in benchmark/harness logic:
  - prompt-focused snippet selection
  - prompt-symbol rescue
  - compact lexical rescue
- external-corpus rescue paths that are quality-correct but still much slower
  than the primary in-repo frontier

## Why this syntax

The existing beta surface works, but it is a function zoo.

`sorted_heap_graph_rag(...)` is the first stable-facing layer because it:

- collapses the public fact-graph contract to one entry point
- keeps the fast path on top of already-verified helper/wrapper internals
- fixes the semantic mismatch of the older one-hop wrapper for fact graphs by
  seeding one-hop expansion from ANN-selected `entity_id` values instead of
  `target_id`
- gives PostgreSQL users a query shape that is closer to the current
  `sorted_hnsw` experience: one primary entry point, with a few meaningful
  knobs

## Release gates for 0.13

`0.13` should not promote GraphRAG to stable until all of these are true:

1. Surface freeze
   - `sorted_heap_graph_rag(...)` is the documented primary entry point for
     fact-shaped GraphRAG
   - older wrappers remain available but are documented as lower-level
     building blocks

2. Lifecycle hardening
   - dump/restore coverage
   - crash recovery coverage
   - extension upgrade coverage
   - concurrent DML/compact interaction checks on GraphRAG-shaped tables

3. Observability
   - seed count
   - expanded row count
   - reranked row count
   - returned row count
   - ideally per-stage timing for ANN, expansion, rerank

4. Larger real-corpus verification
   - current in-repo real code corpus is still small
   - larger gates should use corpora from:
     - `~/Projects/Crystal`
     - `~/Projects/C`
     - `~/SrcArchives`

5. Non-canonical schema story
   - non-canonical fact schemas are now supported via:
     - `sorted_heap_graph_register(...)`
     - `sorted_heap_graph_config(...)`
     - `sorted_heap_graph_unregister(...)`
   - remaining work is hardening and documentation, not naming flexibility

## Implementation phases

### Phase 1: unified syntax

Done in this branch:

- introduce `sorted_heap_graph_rag(...)`
- keep the current lower-level wrappers as implementation building blocks
- document the stable candidate contract separately from the experimental
  code-corpus contracts

### Phase 2: hardening

Implemented in this branch:

- dump/restore coverage for registered GraphRAG alias mappings
- extension upgrade coverage for `0.12.0 -> 0.13.0`
- persistence coverage for `sorted_heap_graph_registry` across pg_dump/restore
- crash recovery coverage for registered GraphRAG alias schemas

Still needed:

- larger real-corpus repeated-build gates
- observability across seed / expand / rerank stages
- concurrent DML/compact interaction checks on GraphRAG-shaped tables

### Phase 3: schema registration

Implemented in this branch:

- register graph metadata for non-canonical column names
- keep `sorted_heap_graph_rag(...)` syntax stable while relaxing the schema
  naming constraint
- regression coverage now includes an alias schema:
  `src_id / edge_type / dst_id / vec / body`

### Phase 4: code-corpus productization

Future work, not required for `0.13` fact-graph stable:

- move snippet/symbol/lexical rescue logic from benchmark harnesses into a
  coherent user-facing API, or
- keep it explicitly documented as reference logic instead of product surface
