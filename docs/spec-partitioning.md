---
layout: default
title: Declarative Partitioning
nav_order: 15
---

# Spec: Declarative Partitioning Support

Status: partially implemented
Risk tier: CAUTION
Primary goal: make `sorted_heap` operationally useful for very large logical
tables by supporting partition-scoped maintenance and observability.

## Problem

`sorted_heap` currently works on concrete physical relations. That is a good
fit for one table at a time, but huge logical tables are commonly managed as
PostgreSQL declarative partitioned tables.

Today, the extension does not expose a first-class contract for:

- compacting all sorted_heap leaves under a partitioned parent;
- merging all sorted_heap leaves under a partitioned parent;
- rebuilding zone maps across a partition tree;
- inspecting per-leaf sorted_heap state from the parent;
- explaining how `SortedHeapScan` should behave after PostgreSQL partition
  pruning.

Fresh probe on PostgreSQL 18 showed the current split:

- `CREATE TABLE leaf PARTITION OF parent ... USING sorted_heap` works.
- `sorted_heap_compact(leaf)` works.
- A direct query against the leaf can use `Custom Scan (SortedHeapScan)`.
- Initial probe: the same predicate through the partitioned parent did not use
  `SortedHeapScan`; it planned a leaf `Seq Scan` when index/bitmap scans were
  disabled, or a disabled PK `Index Scan` when `enable_seqscan = off`.
- First-pass fix: the planner hook now accepts PostgreSQL partition child
  member relations (`RELOPT_OTHER_MEMBER_REL`) in addition to direct base
  relations. Regression `SH23-3` verifies that a parent query reaches
  `SortedHeapScan` on the pruned sorted_heap leaf.

This makes the huge-table story incomplete: full-table rewrite compaction is
operationally expensive, while partition-scoped compaction is the natural
answer.

## Non-Goals

The first partitioning pass will not:

- implement a global in-place compactor;
- compact multiple leaves atomically as one logical operation;
- transparently dispatch arbitrary filtered `sorted_hnsw` KNN through normal
  `WHERE` clauses;
- automatically dispatch GraphRAG across a partitioned parent;
- introduce a global cross-partition HNSW index;
- silently support foreign partitions.

Those can be separate specs after the storage maintenance contract is stable.

## Definitions

- **Parent**: a PostgreSQL declarative partitioned table.
- **Leaf**: a concrete relation under the partition tree that stores tuples.
- **Supported leaf**: a leaf whose table access method is `sorted_heap` and
  whose relation has a primary key.
- **Unsupported leaf**: a leaf with another table AM, a foreign table, or a
  relation kind that does not have sorted_heap storage metadata. A sorted_heap
  leaf without a primary key is also unsupported for maintenance helpers,
  because concrete compact/merge/rebuild need PK metadata.

## Proposed User Contract

### Leaf tables

Concrete partition leaves may use `sorted_heap` as their table access method.
Each leaf owns its own:

- block-0 meta page;
- zone map;
- sorted-prefix state;
- compact/merge lifecycle;
- `sorted_hnsw` indexes.

PostgreSQL's normal partition pruning chooses which leaves participate in a
query. Once a sorted_heap leaf is selected as a base relation, the existing
`SortedHeapScan` planner hook should add a zone-map-pruned path for that leaf.

Current verified coverage:

- Direct leaf queries use `SortedHeapScan`.
- Covered parent query shape now uses `SortedHeapScan` after partition pruning.
- Pure parent-level `ORDER BY embedding <=> query LIMIT k` can use PostgreSQL's
  normal `Merge Append` over leaf `sorted_hnsw` ordered index scans.
- Explicit `sorted_hnsw_partition_search(...)` supports route-first vector
  search over all leaves or a selected leaf set, with global exact rerank over
  local candidate pools. It requires a valid leaf-local `sorted_hnsw` index on
  every selected sorted_heap leaf, verifies that explicit selected leaves
  belong to the requested parent, and fails closed if that contract is not met.
- Broader parent-query shapes still need expansion tests before we claim
  complete planner coverage.

### Parent maintenance

Parent-level maintenance should be explicit and fail-closed by default.

Implemented first-pass API:

```text
sorted_heap_compact_partitions(parent regclass, fail_on_unsupported boolean default true)
  -> setof partition maintenance rows
sorted_heap_merge_partitions(parent regclass, fail_on_unsupported boolean default true)
  -> setof partition maintenance rows
sorted_heap_rebuild_zonemap_partitions(parent regclass, fail_on_unsupported boolean default true)
  -> setof partition maintenance rows
sorted_heap_partition_maintenance_plan(parent regclass, operation text default 'compact')
  -> setof partition maintenance plan rows
```

Result columns:

```text
parent_relid oid
leaf_relid oid
leaf_name text
operation_name text
status text
message text
elapsed_ms double precision
```

Plan result columns:

```text
parent_relid oid
leaf_relid oid
leaf_name text
operation_name text
status text                  -- would_run | blocked
message text
lock_mode text
tablespace_oid oid
tablespace_name name
tablespace_location text
relation_size_bytes bigint
estimated_temp_bytes bigint
```

Default behavior:

- recurse through the full partition tree;
- operate only on supported leaves;
- preflight and error if any leaf is unsupported or lacks a primary key;
- return one row per processed leaf;
- never operate on foreign partitions.

Optional policy:

- `fail_on_unsupported=false` reports unsupported leaves as `skipped` and
  operates on supported sorted_heap leaves. This is explicit at the call site
  and covered by regression.

### Parent observability

Implemented first-pass API:

```text
sorted_heap_partition_status(parent regclass) -> setof sorted_heap_partition_status
```

Result columns:

```text
parent_relid oid
leaf_relid oid
leaf_name text
relkind "char"
am_name name
is_sorted_heap boolean
has_primary_key boolean
zone_map_valid boolean
zone_map_sorted boolean
sorted_prefix_pages integer
zone_map_entries integer
overflow_pages integer
relation_size_bytes bigint
```

This function should not mutate data. It should report unsupported leaves
instead of erroring, unless the input is not a partitioned table or concrete
sorted_heap table.

### Existing APIs

Existing concrete-relation functions keep their current semantics:

- `sorted_heap_compact(regclass)`
- `sorted_heap_merge(regclass)`
- `sorted_heap_rebuild_zonemap(regclass)`
- `sorted_heap_zonemap_stats(regclass)`

Resolved design choice:

- The concrete-relation functions remain concrete-only.
- Partitioned parents use explicit parent helpers that return per-leaf result
  rows. This avoids surprising users who expect a single-relation operation and
  lets callers observe partial success/failure per leaf.

## Acceptance Tests

### P1. Leaf-level scan pruning under a partitioned parent

Create a partitioned parent with two sorted_heap leaves. Compact both leaves.
Query the parent with a partition key predicate and a primary-key range
predicate.

Expected:

- PostgreSQL prunes irrelevant partitions;
- selected sorted_heap leaf uses `Custom Scan (SortedHeapScan)` even when the
  query is issued against the parent;
- result count is correct.

Current status:

- Covered by `SH23-3` regression for a range-partitioned parent with
  sorted_heap leaves, partition key equality, and primary-key range predicate.
- Also covered for a multi-leaf parent query with a partition-key range that
  reaches both leaves and produces at least two `SortedHeapScan` child plans.
- Generic prepared parent query is covered with `plan_cache_mode =
  force_generic_plan`; the plan uses runtime bounds and the execution returns
  the expected count.

### P2. Parent status reports all leaves

Create a partitioned parent with two sorted_heap leaves and compact one leaf.

Expected:

- `sorted_heap_partition_status(parent)` returns two rows;
- compacted leaf reports `zone_map_valid = true`;
- uncompacted leaf reports its actual state;
- relation sizes and leaf names are populated.

Current status:

- Covered by `SH23` regression for both a concrete sorted_heap table and a
  two-leaf sorted_heap parent.

### P3. Parent compact operates leaf-by-leaf

Create a partitioned parent with two sorted_heap leaves. Insert unsorted data
into both leaves.

Expected:

- `sorted_heap_compact_partitions(parent)` returns two success rows;
- both leaves report valid/sorted zone maps afterward;
- parent query still returns all rows correctly.

Current status:

- Covered by `SH23-2` regression.

### P4. Unsupported leaf fails closed

Create a partitioned parent with one sorted_heap leaf and one heap leaf. Also
cover sorted_heap leaves that lack a primary key, because concrete maintenance
requires PK metadata.

Expected:

- parent status reports both leaves and identifies the heap leaf as
  unsupported;
- parent compact helper errors before silently skipping unsupported storage;
- sorted_heap leaves without a primary key fail during preflight, before any
  leaf rewrite starts.

Current status:

- Covered by `SH23-5` and `SH23-6` regressions. The explicit
  `fail_on_unsupported=false` mode is also covered and returns a `skipped` row
  for the heap leaf or the no-PK sorted_heap leaf.

### P5. Empty partition parent

Create a partitioned parent without concrete leaf partitions.

Expected:

- status helper returns zero rows;
- maintenance helper returns zero rows;
- the storage-less parent is not treated as an unsupported concrete leaf.

Current status:

- Covered by `SH23-7` regression.

### P6. Nested partition tree

Create a parent with an intermediate partitioned child and concrete sorted_heap
leaves below it.

Expected:

- status and maintenance helpers recurse to concrete leaves;
- intermediate partitioned nodes are not treated as storage relations.

Current status:

- Covered by `SH23-8` regression. Status and compact helpers recurse through
  an intermediate partitioned node and operate only on concrete sorted_heap
  leaves.

### P7. Lock behavior is documented

Run parent compact helper while a concurrent transaction holds a lock on one
leaf.

Expected:

- behavior is deterministic and documented;
- no silent partial success is hidden from the result stream.

Current status:

- Lock and temporary disk-space behavior is documented in `docs/api.md` and
  `docs/limitations.md`.
- Manual smoke `make test-partition-lock` verifies that a held leaf lock blocks
  `sorted_heap_compact_partitions(...)` via `lock_timeout`, and that the same
  helper processes both leaves after the lock is released.

## Implementation Sketch

Discovery:

- use PostgreSQL partition APIs to collect leaf OIDs from a parent;
- preserve deterministic leaf order by OID or partition bound order;
- validate each leaf relation kind and table AM.

Maintenance:

- for compact/merge/rebuild, call the existing concrete-relation internal
  implementation per leaf where possible;
- keep one leaf as the unit of rewrite and locking;
- return one result row per leaf.

Observability:

- reuse the existing meta-page read path from `sorted_heap_zonemap_stats`;
- expose row columns instead of a formatted text blob.

Planner validation:

- add SQL regression using `EXPLAIN (COSTS OFF)` on a partitioned parent;
- assert `SortedHeapScan` appears for selected sorted_heap leaves;
- accept PostgreSQL partition-member child relations in the hook path
  (`RELOPT_OTHER_MEMBER_REL` via `IS_SIMPLE_REL`) so parent-dispatched leaf
  scans can receive the same zone-map-pruned path as direct leaf scans.

## Resolved Decisions

- Parent helpers are `FUNCTION`s returning rows, because callers need a
  machine-readable per-leaf result stream.
- `sorted_heap_partition_maintenance_plan(...)` is read-only and reports all
  blockers instead of stopping at the first unsupported leaf. It is the
  operator-facing dry-run path for lock/headroom review, including portable
  tablespace identity for external free-space monitoring.
- Helpers preflight unsupported leaves by default and fail before work starts.
  Explicit `fail_on_unsupported=false` returns `skipped` rows for unsupported
  leaves.
- Parent maintenance processes one leaf at a time instead of locking all leaves
  up front. This keeps temporary disk-space requirements leaf-scoped.
- Parent-query `SortedHeapScan` is handled in the generic planner hook by
  accepting simple partition-member relations.

## Remaining Open Questions

Resolved in first-pass hardening:

- Parent maintenance validation: the dry-run plan now collects all blockers;
  mutating helpers still fail fast to avoid partial work.
- Route-first HNSW safety: explicit `leaf_relids` are validated against the
  requested parent, and every selected sorted_heap leaf must have a valid
  leaf-local `sorted_hnsw` index.

Still open for the next phase:

1. Should `sorted_hnsw_partition_search(...)` move from PL/pgSQL to C to reduce
   route-first helper overhead on small partitions?
2. Should GraphRAG parent fanout return a global exact top-k merge contract or
   require explicit routed-shard APIs only?
3. Should partition maintenance plans include tablespace/free-space data, or is
   relation-size headroom enough for the stable SQL contract?
   Resolved for `0.13`: include portable tablespace identity in the SQL plan;
   keep actual free-byte checks in OS/platform monitoring because PostgreSQL
   does not expose a portable SQL-level free-space metric.
4. Should attach/detach/default-partition lifecycle get a dedicated regression
   block, beyond the current nested/mixed/empty partition coverage?
   Resolved by SH23-9.

## Definition of Done

The first implementation is complete when:

- partition leaf scan pruning is regression-tested;
- parent status function is implemented and regression-tested;
- parent compact/merge/rebuild helpers are implemented and regression-tested;
- unsupported leaf behavior is fail-closed and tested;
- docs describe free-space and locking behavior per leaf;
- existing non-partition tests still pass.

Current completion state:

- Done: parent status, compact helper, fail-closed unsupported leaf behavior,
  explicit skip mode, merge/rebuild wrappers, concrete-table status/maintenance
  compatibility, nested partition traversal, covered parent-query
  `SortedHeapScan` for single-leaf, multi-leaf range, and generic prepared
  runtime-bound shapes, lock/free-space documentation, optional lock-wait
  smoke, upgrade-path SQL.
- Open: parent-dispatched GraphRAG/ANN fanout remains a separate future spec.
