---
layout: default
title: Huge-Table Compaction
nav_order: 16
---

# Spec: Huge-Table Compaction Operating Model

Status: implemented first pass
Risk tier: CAUTION
Primary goal: make the disk-space and locking contract explicit for large
`sorted_heap` deployments.

## Problem

`sorted_heap` gets its read performance from physical locality plus zone maps.
Eventually, write-heavy tables need maintenance to restore sorted order and
fresh pruning metadata. The maintenance verbs are intentionally rewrite-based:

- `sorted_heap_compact(regclass)` rewrites the relation in globally sorted PK
  order and rebuilds the zone map.
- `sorted_heap_merge(regclass)` detects the already-sorted prefix, sorts the
  unsorted tail, and writes a new relation.
- online variants reduce blocking time with trigger-based change capture, but
  they still build a replacement relation before the final swap.

That means compaction is not an in-place defragmenter. Operators need temporary
disk headroom for the rewrite unit.

## Current Contracts

### Concrete table operations

| Operation | Rewrite unit | Main lock shape | Disk headroom |
|-----------|--------------|-----------------|---------------|
| `sorted_heap_compact` | whole relation | `AccessExclusiveLock` for operation | replacement relation + rebuilt indexes |
| `sorted_heap_merge` | whole relation | `AccessExclusiveLock` for operation | replacement relation + rebuilt indexes |
| `sorted_heap_compact_online` | whole relation | long copy under weaker lock, brief final `AccessExclusiveLock` | replacement relation + log table + rebuilt indexes |
| `sorted_heap_merge_online` | whole relation | same online shape as compact | replacement relation + log table + rebuilt indexes |

`sorted_heap_merge` may be faster than full compact when the sorted prefix is
large, but it still produces a new relation. It is a CPU/sort optimization, not
a disk-headroom optimization.

### Partitioned parent operations

Partition helpers make the leaf the rewrite unit:

```sql
SELECT *
FROM sorted_heap_compact_partitions('events_parent'::regclass);

SELECT *
FROM sorted_heap_merge_partitions('events_parent'::regclass);
```

Contract:

- recurse through concrete leaves;
- preflight unsupported leaves and sorted_heap leaves without primary keys;
- process supported leaves one at a time;
- return one result row per processed or skipped leaf;
- reduce temporary disk headroom from "whole logical parent" to "current leaf";
- do not provide one global all-leaves transaction.

If a later leaf fails, earlier leaves remain processed. This is intentional:
the helper is an operational wrapper over concrete relation maintenance, not a
distributed transaction protocol.

## Recommended Huge-Table Strategy

Use declarative partitioning to bound the maintenance unit.

Typical partition keys:

- time range for event/time-series tables;
- tenant / knowledge-base / shard id for multi-tenant retrieval;
- coarse lifecycle segment for hot/cold data.

Recommended flow:

1. Keep each leaf at a size where one rewrite fits operational headroom.
2. Bulk load or ingest into leaves.
3. Run `sorted_heap_compact_partitions(...)` or
   `sorted_heap_merge_partitions(...)` during maintenance windows.
4. Inspect state with `sorted_heap_partition_status(parent)`.
5. For live systems, prefer online concrete operations on the hot leaf when
   blocking time matters more than total runtime.

## Sizing Heuristic

For a concrete leaf, reserve enough free space for:

```text
replacement heap relation
+ rebuilt secondary indexes
+ transient online log table, for online variants
+ normal PostgreSQL WAL/checkpoint headroom
```

This spec does not give a universal percentage because row width, index count,
TOAST use, WAL settings, and filesystem behavior dominate. The actionable rule
is simpler: make partitions small enough that one leaf rewrite is safe.

## Non-Goals

- No promise of in-place compaction.
- No segment-level rewrite inside one relation yet.
- No global all-partition atomic maintenance.
- No automatic repartitioning or partition-size advisor.

## Future Segment-Level Compaction

Segment-level compaction could reduce disk headroom inside a single large
relation, but it needs a separate storage design:

- crash-safe mapping from old segment pages to new segment pages;
- index TID update or indirection semantics;
- zone-map and sorted-prefix updates that remain valid across partial rewrite;
- WAL/recovery story for interrupted segment swaps;
- planner/executor behavior while old and new segments coexist.

Until those are specified and tested, partition-scoped rewrite is the supported
large-table operating model.

## Acceptance Tests

Already covered:

- `SH23` verifies partition parent helpers, nested leaves, unsupported leaves,
  and no-PK leaves.
- `make test-partition-lock` verifies a held leaf lock blocks parent compact
  and that the helper succeeds after release.
- Existing dump/restore, TOAST, crash, and concurrent online tests cover the
  underlying concrete rewrite paths.

Future tests:

- benchmark partitioned maintenance over multiple differently sized leaves;
- verify failure reporting when one later leaf cannot acquire lock;
- add an optional dry-run/estimate helper if operator demand justifies it.

## Quadrumvirate Notes

Cassandra:

- Likely failure mode: users infer "compact" means in-place. The docs must say
  rewrite-and-swap plainly.
- Likely failure mode: online compact is mistaken for lower disk usage. It
  lowers blocking, not rewrite headroom.

Daedalus:

- Reframe from "how do we compact a huge table" to "how do we bound the rewrite
  unit". Partitioning is the current answer.

Maieutic:

- Assumption: one logical table must be one physical rewrite unit. Refuted by
  PostgreSQL declarative partitioning and leaf-scoped helpers.
- Assumption: segment-level compaction is a small extension of merge. It is
  not; it changes crash recovery and index/TID semantics.

Adversary:

- Any future in-place/segment claim must specify index updates and crash
  recovery before implementation.
- Any operator-facing helper must expose partial failures explicitly.
