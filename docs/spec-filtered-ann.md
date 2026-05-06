---
layout: default
title: Filtered ANN Contracts
nav_order: 17
---

# Spec: Filtered ANN Contracts

Status: proposed
Risk tier: CAUTION
Primary goal: define safe filtered vector-search contracts without weakening
the current `sorted_hnsw` ordered-scan guarantee.

## Problem

The planner-integrated `sorted_hnsw` index path currently targets a narrow
shape:

```sql
SELECT ...
FROM items
ORDER BY embedding <=> $query
LIMIT k;
```

It deliberately avoids extra base-table quals and parameterized scans. A normal
executor filter applied after a bounded ANN scan can under-return results: the
index might return `ef_search` approximate nearest candidates, the executor
then filters most of them away, and the final result contains fewer than `k`
rows even though more qualifying rows exist in the table.

This is a correctness contract issue, not just a planner-cost issue.

## Non-Goals

- Do not make the existing `sorted_hnsw` ordered Index AM silently accept
  arbitrary `WHERE` clauses.
- Do not claim pgvector-compatible filtered ANN semantics until the final
  top-k contract and underfill behavior are explicitly tested.
- Do not implement global cross-partition HNSW in this spec.
- Do not conflate fact-shaped GraphRAG routing with generic row filtering.

## Current Stable Contract

`sorted_hnsw` is stable for pure ordered KNN over a base relation:

- `ORDER BY embedding <=> query LIMIT k`;
- no unbounded KNN;
- no `LIMIT > sorted_hnsw.ef_search`;
- no extra base-table quals that would make executor filtering under-return
  candidates;
- exact rerank happens inside the index scan before rows are returned.

Filtered retrieval should currently use one of:

- materialize/filter first, then query the filtered relation;
- GraphRAG helper/wrapper APIs for fact-shaped relation filters;
- routed GraphRAG for tenant/segment/knowledge-base shard routing.

## Proposed Filtered ANN Modes

### F1. Pre-filter then ANN

Use when the filter is selective and can be materialized cheaply.

Contract:

- build or materialize the filtered candidate set first;
- run ANN or exact search over that candidate set;
- final top-k is relative to the filtered set.

Example shape:

```sql
CREATE TEMP TABLE candidate_items AS
SELECT *
FROM items
WHERE tenant_id = 42
  AND status = 'active';

CREATE INDEX candidate_items_embedding_idx
ON candidate_items USING sorted_hnsw (embedding);

SELECT *
FROM candidate_items
ORDER BY embedding <=> $query
LIMIT 10;
```

This is operationally heavier, but the contract is simple: the ANN index sees
only eligible rows.

### F2. ANN overfetch then exact filtered rerank

Use when the filter is not too selective and overfetch can be bounded.

Contract:

- ANN produces a candidate pool larger than `top_k`;
- executor or helper applies the filter to the pool;
- exact distance is recomputed on surviving candidates;
- if fewer than `top_k` rows survive, the result must expose `underfilled=true`
  or fall back to a larger search/exact path.

Required metadata:

- `ann_k`: ANN candidate budget;
- `top_k`: final result budget;
- `matched_after_filter`: number of candidates surviving the filter;
- `underfilled`: whether final rows `< top_k`;
- optional fallback marker: `none | larger_ann | exact_filtered`.

This should be a helper/function contract before it is a transparent planner
path, because callers need to see when the filtered result is approximate or
underfilled.

### F3. Partition/routing-aware filtered ANN

Use when filters map to physical shards, partitions, tenants, segment groups,
or knowledge-base routes.

Contract:

- route first, using exact metadata (`tenant_id`, segment registry, partition
  bounds, or GraphRAG route profile);
- run local ANN on selected shards/leaves;
- exact-rerank a global candidate pool;
- final top-k is selected by global exact distance, not by concatenating local
  top-k lists.

Required global merge invariant:

```text
global_top_k = exact_top_k(union(local_candidate_pools))
```

Local top-k concatenation is insufficient because a dense shard can contribute
more than `k_per_shard` globally relevant rows.

Implemented first-pass helper:

```sql
SELECT *
FROM sorted_hnsw_partition_search(
    'items_parent'::regclass,
    'embedding',
    '[0.1,0.2,0.3,...]',
    top_k := 10,
    local_k := 32,
    leaf_relids := ARRAY['items_tenant_42'::regclass]);
```

This helper keeps the planner guard intact. It does not make arbitrary
`WHERE ... ORDER BY embedding <=> query LIMIT k` transparent, because executor
filters can still underfill bounded ANN scans. Instead, callers route first to
whole eligible leaves, run local `sorted_hnsw` scans, and receive a globally
reranked result over the union of local candidate pools.

Safety checks:

- `local_k >= top_k`;
- `local_k <= sorted_hnsw.ef_search`;
- unsupported leaves fail closed by default;
- selected sorted_heap leaves must have a valid leaf-local `sorted_hnsw` index
  on the vector column, otherwise the helper fails instead of exact-sorting;
- explicit selected leaves must belong to the requested parent;
- selected leaf routing is explicit via `leaf_relids`.

## Acceptance Tests

### F1. Pre-filter materialization correctness

Create a table with two tenants and embeddings where the nearest global row is
outside the target tenant.

Expected:

- filtered materialization returns only target-tenant rows;
- final top-k matches exact search over the target tenant.

### F2. Overfetch underfill detection

Create a table where the first `ann_k` ANN candidates mostly fail the filter.

Expected:

- helper does not silently return fewer than `top_k` as if complete;
- result metadata reports `underfilled=true`, or the helper escalates to a
  documented fallback.

### F3. Cross-shard global merge

Create two shards where one shard contains all true top-k rows.

Expected:

- route selects both shards;
- local candidate pools are unioned;
- exact global rerank returns all true top-k rows from the dense shard;
- test fails if implementation concatenates fixed local top-k slices.

Current status:

- Covered for partitioned `svec` and `hsvec` leaves in the `sorted_hnsw`
  regression. The helper routes selected leaves, unions local pools, globally
  orders by exact distance, rejects `local_k > sorted_hnsw.ef_search`, and
  rejects selected leaves without a valid `sorted_hnsw` index or without a
  parent-child relationship to the requested parent.
- `scripts/bench_partitioned_sorted_hnsw.sh` provides an operator benchmark for
  selected-leaf routing versus parent filtered exact and all-leaf fanout.

### F4. Planner safety guard

Run a base-table `WHERE` clause with `ORDER BY embedding <=> query LIMIT k`.

Expected:

- planner does not select the transparent `sorted_hnsw` path when the qual
  could under-return candidates;
- if a future planner path supports this shape, EXPLAIN must expose the
  filtered-ANN mode and underfill/fallback behavior.

## Implementation Direction

Preferred order:

1. Keep the current planner guard intact.
2. Add helper-level F2 with explicit result metadata.
3. Reuse routed GraphRAG metadata for F3 where filters are actually routing
   predicates.
4. Only after helper contracts are tested, consider a transparent planner path
   for a narrow class of safe filters.

## Quadrumvirate Notes

Cassandra:

- Likely failure mode: making filtered ANN look like a normal index scan while
  silently under-returning after executor filtering.
- Likely failure mode: overfetch constants that pass one demo but lack
  underfill detection.

Daedalus:

- Reframe from "support WHERE filters in HNSW" to "choose a filtered retrieval
  contract based on whether the filter is a materialization, overfetch, or
  routing problem".

Maieutic:

- Assumption: users want pgvector-like syntax. Counterpoint: they need
  correctness and observability more than syntax if the result can underfill.
- Assumption: per-shard top-k is enough. Falsifier: one shard contains all true
  top-k rows.

Adversary:

- Any helper must expose underfill/fallback state.
- Any cross-shard path must do global exact rerank.
- Any planner-integrated path must explain why executor filtering cannot
  invalidate the final top-k contract.
