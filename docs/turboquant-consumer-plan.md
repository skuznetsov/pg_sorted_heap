---
layout: default
title: TurboQuant + Consumer Plan
nav_order: 11
---

# TurboQuant + consumer-first `0.13` plan

This note is repo-owned memory for two related decisions:

1. how to try TurboQuant without destabilizing the current stable AM path
2. how to expand the unpublished `0.13` toward the first real Cogniformerus
   consumer instead of more routing/control-plane infrastructure

It exists because local `cfmem` is currently unavailable on this machine
(`libggml.0.dylib` missing), so these ideas need a durable in-repo anchor.

## TurboQuant: implementation notes

### Primary sources

- Google Research blog, 2026-03-24:
  `https://research.google/blog/turboquant-redefining-ai-efficiency-with-extreme-compression/`
- TurboQuant paper:
  `https://arxiv.org/abs/2504.19874`

### What matters for this repo

The attractive properties are:

- online/data-oblivious vector quantization
- near-zero indexing/training cost compared with codebook-heavy PQ-style paths
- claims for both vector search and KV-cache compression

The dangerous temptation is to jam it directly into the stable
`sorted_hnsw`/GraphRAG path before we know whether it helps the real consumer.

### Chosen stance

Treat TurboQuant as an **experimental retrieval compression mode**, not as a
new stable storage/index mode and not as a KV-cache project.

Do **not** start with:

- `sorted_hnsw` core AM replacement
- shared-cache redesign
- KV-cache kernel/runtime work

Do start with the narrowest experiment lane that can answer:

> does TurboQuant improve the storage/quality/latency tradeoff for the real
> Cogniformerus retrieval workload relative to current `hsvec`, `sq8`, and PQ
> baselines?

### Ordered integration options

#### Option A — first experiment: retrieval-side offline evaluator

Build a Python-side evaluator first, outside the stable AM path.

Suggested location:

- `poc/turboquant_eval.py` or
- `scripts/bench_turboquant_retrieval.py`

Inputs:

- ANN-Benchmarks/real vector dataset already used in repo harnesses
- Cogniformerus summary/code-graph embeddings exported from the real consumer

Comparators:

- float32 `svec`
- float16 `hsvec`
- current `sorted_hnsw` SQ8 path where applicable
- existing PQ baseline where applicable
- TurboQuant experimental encode/decode/search path

Outputs:

- compression ratio
- encode/build time
- query latency or proxy distance-eval cost
- hit@1 / hit@k / recall@k
- quality versus compression curve

This is the first implementation point because it is reversible and does not
broaden the release surface.

#### Option B — second experiment: Cogniformerus external-memory mode

If Option A looks promising, add a **consumer-only** experimental mode in
Cogniformerus for summary vectors or external memory vectors.

This still avoids touching the stable `sorted_hnsw` AM path.

#### Option C — last: engine integration

Only after A/B show a meaningful win on the real consumer should TurboQuant be
considered for:

- `sorted_hnsw` sketch/storage internals
- planner-visible index modes
- GraphRAG stable path

### DoD for the TurboQuant experiment

Minimum acceptance:

1. one synthetic/benchmark dataset already used in this repo
2. one real Cogniformerus-derived embedding set
3. exact comparison against current baselines
4. measured answer to one question:
   - better compression-quality tradeoff than `hsvec`, or
   - better recall-latency tradeoff than existing PQ path, or
   - not worth pursuing

### Anti-goals

Do not let the first TurboQuant branch become:

- a new release-surface promise
- a new router/control-plane branch
- a KV-cache engineering detour
- a speculative kernel rewrite without consumer evidence

## Enlarged `0.13`: consumer-first scope

### Core judgment

Because `0.13` is still unpublished and Cogniformerus is the first real
customer, the right expansion is **consumer-first**, not more routing
infrastructure.

### What the current consumer already uses

Cogniformerus already has all of these:

- fact-shaped code-graph storage on `sorted_heap`
- ANN seed on `sorted_hnsw`
- multi-hop retrieval through `sorted_heap_graph_rag(...)`
- MCP tools that expose graph search / callers / callees

So the next work should target real consumer correctness and update cost.

### The real consumer gaps already visible

#### 1. Fact-shape mismatch

Current Cogniformerus code-graph store and loader collapse uniqueness to
`(entity_id, target_id)` instead of `(entity_id, relation_id, target_id)`.

That can lose relation distinctions in the real code graph.

#### 2. Directionality mismatch

`find_callers` and `find_callees` are effectively symmetric in the current
consumer code. That is not semantically honest unless reverse edges exist.

#### 3. Fake `relation=all`

Current `relation == "all"` behavior in the MCP tool path degrades to a
single relation family instead of a true multi-relation search.

#### 4. Brutal update model

Current reindex flow is full truncate + full reload + compact + index rebuild.
That is the most obvious real-world pain point.

## Split plan: `clustered_pg` vs `cogniformerus`

### `clustered_pg`

#### Include in enlarged `0.13`

1. **GraphRAG witness/explain improvements**
   - narrow app-facing explanation for why a result/path was returned
   - build on `sorted_heap_graph_rag_stats()` and current routed explain
   - do not add a new low-level wrapper zoo

2. **Real-workload regression fixture**
   - one tiny code-graph-shaped fixture or harness
   - guard the actual Cogniformerus query patterns, not just synthetic chains

3. **Only if real consumer data proves it is needed**
   - multi-relation hop support beyond the current one-relation-per-hop shape

#### Explicitly defer

- segment synopses
- adaptive widening
- temporal queries
- hub capsules
- new routed control-plane layers

### `cogniformerus`

#### Include in enlarged `0.13`

1. **Fix fact shape**
   - relation-aware PK/upsert semantics
   - relation-aware dedupe in the loader

2. **Fix callers/callees**
   - either reverse-edge ingest or honest reverse-query handling

3. **Implement real `relation=all`**
   - first acceptable solution: consumer-side union/merge over relation families
   - only push this into `clustered_pg` if the real workload proves it is
     necessary

4. **Incremental reindex**
   - file-scoped delete/upsert instead of full truncate + rebuild
   - periodic compaction/index maintenance can stay separate

5. **Real query set**
   - fix 20-50 code-graph queries as the first real consumer benchmark gate

## Ordered execution

1. Cogniformerus correctness:
   - fact shape
   - dedupe
   - callers/callees
   - `relation=all`
2. Cogniformerus real query set
3. `clustered_pg` witness/explain only if the consumer actually needs it
4. Cogniformerus incremental ingest
5. TurboQuant experiment lane (Option A) in parallel, still outside stable AM

## Release framing

The enlarged unpublished `0.13` should be treated as:

> first real code-graph consumer release

not as:

> more routed/segmented infrastructure

