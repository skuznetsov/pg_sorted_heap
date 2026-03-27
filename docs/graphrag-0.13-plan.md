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

Current status: all listed release gates below are now covered for the narrow
fact-shaped `0.13` surface. The remaining work is release bundling and
documentation clarity, not new release-critical hardening.

## Stable target

The stable surface for `0.13` is:

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
- `relation_path` is a non-empty per-hop relation sequence
- `score_mode = 'endpoint' | 'path'`
- `limit_rows = 0` means unlimited helper work; positive values cap
  expansion/rerank work and do not replace the final `top_k` contract
- exact rerank on the expanded candidate set

Semantics:

- `relation_path := ARRAY[1]`
  - one-hop expansion
  - exact rerank on the endpoint fact
  - `score_mode := 'path'` is intentionally equivalent to `endpoint`
- `relation_path := ARRAY[1, 2], score_mode := 'endpoint'`
  - two-hop expansion
  - exact rerank on the second-hop endpoint only
- `relation_path := ARRAY[1, 2], score_mode := 'path'`
  - two-hop expansion
  - path-aware rerank using hop-1 and hop-2 evidence together
- `relation_path := ARRAY[1, 2, 3, ...]`
  - explicit multi-hop expansion
  - each array element is the relation filter for that hop
  - `score_mode := 'endpoint'` ranks only the final hop
  - `score_mode := 'path'` accumulates evidence across the whole path

## What stays beta

These remain beta even after the new syntax lands:

- lower-level helper zoo:
  - `sorted_heap_expand_ids(...)`
  - `sorted_heap_expand_rerank(...)`
  - `sorted_heap_expand_twohop_rerank(...)`
  - `sorted_heap_expand_twohop_path_rerank(...)`
  - `sorted_heap_expand_multihop_rerank(...)`
  - `sorted_heap_expand_multihop_path_rerank(...)`
  - `sorted_heap_graph_rag_scan(...)`
  - `sorted_heap_graph_rag_twohop_scan(...)`
  - `sorted_heap_graph_rag_twohop_path_scan(...)`
  - `sorted_heap_graph_rag_multihop_scan(...)`
  - `sorted_heap_graph_rag_multihop_path_scan(...)`
- code-corpus contracts that currently live in benchmark/harness logic:
  - prompt-focused snippet selection
  - prompt-symbol rescue
  - compact lexical rescue
- external-corpus rescue paths that are quality-correct but still much slower
  than the primary in-repo frontier

## Why this syntax

The existing beta surface works, but it is a function zoo.

`sorted_heap_graph_rag(...)` is the stable-facing layer because it:

- collapses the public fact-graph contract to one entry point
- keeps the fast path on top of already-verified helper/wrapper internals
- fixes the semantic mismatch of the older one-hop wrapper for fact graphs by
  seeding one-hop expansion from ANN-selected `entity_id` values instead of
  `target_id`
- gives PostgreSQL users a query shape that is closer to the current
  `sorted_hnsw` experience: one primary entry point, with a few meaningful
  knobs

## Release gates for 0.13

All of the following are now covered for the narrow fact-shaped contract:

1. Surface freeze
   - `sorted_heap_graph_rag(...)` is the documented primary entry point for
     fact-shaped GraphRAG
   - older wrappers remain available but are documented as lower-level
     building blocks

2. Lifecycle hardening
   - dump/restore coverage, including shared/default `segment_labels`
     persistence in the segmented/routed control plane
   - crash recovery coverage
   - extension upgrade coverage
   - concurrent DML/compact interaction checks on GraphRAG-shaped tables

3. Observability
   - implemented via:
     - `sorted_heap_graph_rag_stats()`
     - `sorted_heap_graph_rag_reset_stats()`
   - current stats include:
     - seed count
     - expanded row count
     - reranked row count
     - returned row count
     - per-stage timing for ANN, expansion, rerank
   - current scope is backend-local last-call observability, which is enough
     for release tuning and debugging but not a full tracing system

4. Larger real-corpus verification
   - current progress:
     - the smaller in-repo `cogniformerus` slice is already repeated-build
       stable at `100.0% / 100.0%`
     - a larger in-repo transfer gate on the full
       `~/Projects/Crystal/cogniformerus` repository now also passes
       repeated-build verification once the final result budget is raised from
       `top_k=4` to `top_k=8`
     - a first mixed-language gate on `~/Projects/C/pycdc` now also runs under
       the same harness family via JSON fixtures + configurable source
       extensions + quoted include-edge extraction
     - on that corpus, the fast generic point is repeated-build stable but only
       partial (`90.0% / 60.0%`), while the code-aware helper-backed compact
       include rescue is repeated-build stable at `100.0% / 100.0%`
     - a first archive-side gate on `~/SrcArchives/apple/ninja/src` is now also
       repeated-build stable:
       - generic `prompt_summary_snippet_py` closes at `100.0% / 100.0%`
         with `top_k=12`
       - code-aware `prompt_summary_snippet_py` remains partial there
   - the scoped `0.13` larger real-corpus gate is now covered across:
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
- document the stable contract separately from the experimental
  code-corpus contracts

### Phase 2: hardening

Implemented in this branch:

- dump/restore coverage for registered GraphRAG alias mappings
- extension upgrade coverage for `0.12.0 -> 0.13.0`
- persistence coverage for `sorted_heap_graph_registry` across pg_dump/restore
- persistence coverage for the segmented/routed GraphRAG control plane across
  pg_dump/restore:
  - shared shard metadata
  - shared `segment_labels`
  - range routing
  - exact-key routing
  - route policies
  - route profiles
  - route defaults
  - effective default `segment_labels`
- crash recovery coverage for registered GraphRAG alias schemas
- concurrent DML / online compact / online merge coverage for registered
  GraphRAG alias schemas

Still needed:

- no additional release-critical hardening is currently required for the
  narrow `0.13` fact-graph stable target
- remaining work is release packaging and keeping the stable/beta/reference
  split explicit in the public docs

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
