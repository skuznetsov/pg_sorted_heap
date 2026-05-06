---
layout: default
title: Parent Runtime Observability
nav_order: 23
---

# Spec: Parent Runtime Observability

Status: proposed
Risk tier: CAUTION
Primary goal: define parent-level runtime observability without mislabeling
global/backend-local counters as per-leaf partition telemetry.

## Problem

Partitioned `sorted_heap` deployments now have parent-level storage and index
health views:

- `sorted_heap_partition_status(parent)`;
- `sorted_heap_partition_index_status(parent)`;
- `sorted_heap_partition_maintenance_plan(parent, operation)`.

Runtime counters are not yet partition-aware:

- `sorted_heap_scan_stats()` reports total scans, blocks scanned, and blocks
  pruned from shared memory when available, otherwise from backend-local
  counters. It does not include relation OIDs.
- `sorted_heap_graph_rag_stats()` reports backend-local last-call GraphRAG
  stage stats. It is useful for one call in one backend, but it is not a
  durable per-shard or per-parent history.

The product risk is observability inflation: a parent-level function that joins
these global counters to leaf metadata would look useful but would be
misleading.

## Non-Goals

- Do not expose global scan counters as if they were per leaf.
- Do not infer GraphRAG per-shard timings from aggregate last-call stats.
- Do not add persistent telemetry tables by default.
- Do not make observability require `shared_preload_libraries`.
- Do not change the stable meaning of existing stats functions.

## Current Stable Surfaces

### Storage and index state

Use these for parent-level state:

```sql
SELECT * FROM sorted_heap_partition_status('events_parent'::regclass);
SELECT * FROM sorted_heap_partition_index_status('events_parent'::regclass);
```

These are relation-scoped and safe to display per leaf.

### Runtime scan counters

Use this only as a process/global counter:

```sql
SELECT * FROM sorted_heap_scan_stats();
```

Current semantics:

- `source = 'shared'`: counters are shared across backends.
- `source = 'local'`: counters are local to the current backend.
- counters are not keyed by relation, parent, leaf, query, or user.

### Runtime GraphRAG counters

Use this only immediately after a GraphRAG call in the same backend:

```sql
SELECT * FROM sorted_heap_graph_rag_stats();
```

Current semantics:

- `calls` is backend-local;
- stage row counts and timings describe the last top-level call;
- the result does not identify all selected shards or leaves;
- routed wrappers may merge results from multiple concrete relations, but the
  stats row is still an aggregate for the call path.

## Proposed Future Surfaces

### O1. Relation-aware scan stats

Add relation-aware counters before adding parent rollups.

Candidate API:

```sql
SELECT *
FROM sorted_heap_scan_stats_by_relation();
```

Candidate columns:

```text
relid regclass
relname text
total_scans bigint
blocks_scanned bigint
blocks_pruned bigint
source text
```

Parent rollup can then be a safe SQL helper:

```sql
SELECT *
FROM sorted_heap_partition_scan_stats('events_parent'::regclass);
```

Required invariant:

```text
parent rows = relation-aware counters joined to actual leaves under parent
```

No relation key means no parent rollup.

### O2. GraphRAG route execution stats

Add a route-call execution trace only if routed GraphRAG users need per-shard
observability.

Candidate API:

```sql
SELECT *
FROM sorted_heap_graph_route_last_stats();
```

Candidate columns:

```text
call_id bigint
api text
source_rel regclass
seed_count bigint
expanded_rows bigint
reranked_rows bigint
returned_rows bigint
ann_ms double precision
expand_ms double precision
rerank_ms double precision
total_ms double precision
```

This should remain backend-local unless a separate persistent telemetry
contract is designed.

### O3. Explain-only diagnostics

For one-off operator diagnosis, prefer `EXPLAIN (ANALYZE, BUFFERS)` and
existing route-plan helpers before adding persistent counters:

```sql
SELECT *
FROM sorted_heap_graph_route_plan(...);
```

This keeps runtime instrumentation optional and avoids misleading global
metrics.

## Acceptance Tests

### R1. Scan stats relation attribution

Run scans against two sorted_heap leaves in one backend.

Expected:

- relation-aware stats attribute scans and block counters to the correct leaf;
- parent rollup includes only leaves under the requested parent;
- unrelated sorted_heap tables do not appear in that parent rollup.

### R2. Shared/local source semantics

Run with and without shared stats backing.

Expected:

- `source` reports whether counters are shared or backend-local;
- docs state the reset/window behavior clearly;
- tests do not assume cross-backend visibility when source is local.

### R3. GraphRAG route stats attribution

Run a routed GraphRAG call over multiple selected shards.

Expected:

- per-shard stats identify `source_rel`;
- aggregate totals match the public `sorted_heap_graph_rag_stats()` last-call
  row for the same backend;
- selected shards with zero returned rows can still be represented if they did
  work.

## Decision

For `0.13`, parent-level observability is storage/index-health complete enough
for operations. Runtime parent observability should not be implemented until
scan stats carry relation identity or GraphRAG route stats carry source-rel
identity. Until then, use existing runtime stats only with their documented
global/backend-local scope.
