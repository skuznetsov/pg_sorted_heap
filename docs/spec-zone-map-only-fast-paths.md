---
layout: default
title: Zone-Map-Only Fast Paths
nav_order: 19
---

# Spec: Zone-Map-Only Fast Paths

Status: proposed
Risk tier: CAUTION
Primary goal: define the honest boundary between current zone-map pruning and
future heap-fetch-avoiding optimizations.

## Problem

The shorthand "index-only scan equivalent using zone map" is misleading for the
current storage format.

PostgreSQL `Index Only Scan` can return indexed column values without fetching a
heap tuple when visibility checks allow it. The current `sorted_heap` zone map
does not store tuple values or tuple identifiers. It stores page-level min/max
for the first two primary-key columns:

```text
page -> (col1_min, col1_max, col2_min, col2_max)
```

The `SortedHeapScan` custom scan uses this metadata to compute block ranges and
then reads heap tuples with `table_scan_getnextslot(...)`. Executor quals remain
the final correctness filter.

Therefore, the current zone map can skip heap pages, but it cannot by itself
return result rows.

## Current Contract

For ordinary `SELECT` queries:

- zone maps are a page-pruning accelerator;
- heap tuple fetch remains required;
- MVCC visibility remains checked through the normal heap scan path;
- executor quals and projection remain authoritative;
- `EXPLAIN` should keep showing `Custom Scan (SortedHeapScan)`, not an
  index-only path.

This is the right correctness boundary for `0.13`.

## Non-Goals

- Do not present `SortedHeapScan` as a PostgreSQL `Index Only Scan`.
- Do not return rows directly from page-level min/max metadata.
- Do not bypass heap visibility checks.
- Do not make exact-count claims from zone maps unless the table state carries
  enough exact row-count metadata and MVCC semantics are specified.

## Valid Future Paths

### Path A: Zone-map-only metadata answers

This path is only for queries whose answer can be proven from page metadata.

Candidate examples:

- empty-result proof for predicates whose zone-map overlap set is empty;
- planner/executor stats and observability helpers;
- future approximate planning estimates;
- future exact page-range summaries if the storage format adds exact tuple
  counts with a clear MVCC validity contract.

This path does not return user rows. It can only answer metadata-level
questions or prove no heap page can match.

### Path B: Covering sidecar / value-bearing index

This is the closest real equivalent to index-only behavior.

Required pieces:

- a value-bearing sidecar or proper Index AM path that stores the needed
  projected columns or a covering subset;
- tuple identity and visibility semantics compatible with PostgreSQL MVCC;
- invalidation/rebuild rules across compact, merge, online rewrite, restore,
  and partition attach/detach;
- planner rules that engage only when the target list and quals are covered;
- regression tests comparing results against normal heap execution.

This is a larger feature than the current zone map. It should be designed as a
new contract, not as an extension of page min/max pruning.

## Acceptance Tests For Any Implementation

### Z1. Empty-result pruning remains heap-safe

Create a compacted sorted_heap table and query a PK range outside all zone-map
entries.

Expected:

- result is empty;
- no false negatives are possible because the zone map proves no page overlaps;
- if implemented as a metadata fast path, MVCC semantics are explicitly
  documented.

### Z2. Covered-row path proves coverage

For any future covering sidecar:

- query target list contains only covered columns;
- quals contain only supported covered predicates;
- result equals the heap-backed plan for visible rows;
- disabling the sidecar produces the same rows.

### Z3. Non-covered query falls back

For a query that requests a non-covered column:

- planner does not choose the covering path;
- normal heap-backed execution is used.

### Z4. Rewrite lifecycle invalidates or rebuilds correctly

After compact, merge, online compact/merge, pg_restore, and partition
attach/detach:

- stale sidecar state is not used;
- rebuild/refresh path is explicit;
- query results match the heap-backed plan.

## Adversary Notes

- Page min/max metadata is lossy for rows inside a page. It can eliminate pages,
  not identify matching tuples.
- UUID and text/varchar zone-map keys can be lossy. They are acceptable for
  pruning because executor quals verify rows, but not for row-returning identity.
- MVCC visibility is the hardest part of any index-only-like path. A sidecar
  that stores values still needs a visibility story; otherwise it can return
  dead or uncommitted rows.
- Partitioned tables multiply lifecycle rules: every leaf needs independent
  sidecar validity plus parent-level planning/fanout rules.

## Decision

For `0.13`, treat "index-only scan equivalent" as resolved into this spec:
zone-map-only row return is not a valid feature on the current storage format.
Future work should choose explicitly between metadata-only fast paths and a
covering value-bearing sidecar.
