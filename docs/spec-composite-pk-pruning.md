---
layout: default
title: Composite-PK Pruning
nav_order: 14
---

# Spec: Composite Primary-Key Pruning

Status: implemented first pass
Risk tier: CAUTION
Primary goal: make two-column zone maps produce tight pruning for common
tenant/fact layouts such as `(tenant_id, id)` and
`(entity_id, relation_id, target_id)`.

## Problem

`sorted_heap` stores per-page min/max for the first two primary-key columns.
The documentation says composite PKs can prune on both columns.

A fresh probe found a weaker behavior before this spec was implemented:

- table shape: `(tenant_id int, id int, primary key (tenant_id, id))`
- storage: compacted `sorted_heap` leaf with valid/sorted zone map
- query: `tenant_id = 1 AND id BETWEEN 100 AND 110`
- plan: `Custom Scan (SortedHeapScan)`
- observed pruning: `Zone Map: 308 of 310 blocks (pruned 2)`

The zone map entries contained second-column ranges such as
`c2:1..65`, `c2:66..130`, etc. The storage had the information needed for a
tight range, but the scan-range computation did not use it tightly in the
sorted-prefix path.

## Current Contract

The sorted-prefix binary-search path is primarily first-column based. It can
find tight ranges when column 1 alone is selective.

The first implementation adds a conservative refinement for this common shape:

- column 1 has an inclusive equality bound;
- column 2 has an equality or range bound;
- the sorted-prefix first-column binary search has already found the candidate
  prefix span.

Inside that candidate span, the scanner applies the existing per-page
`sorted_heap_zone_overlaps(...)` second-column check and emits only matching
contiguous block ranges. If column 2 is not tracked for a page, the overlap
check remains conservative.

This is not yet a full lexicographic binary search. It intentionally optimizes
the validated `(tenant_id = const AND id bounded)` case first while preserving
fallback behavior for broader shapes.

## Target Contract And Follow-Ups

For a compacted/sorted table with a two-column zone map:

- `col1 = const AND col2 = const` should map to a tight block range.
- `col1 = const AND col2 BETWEEN lo AND hi` should map to a tight block range.
- `col1 BETWEEN lo AND hi` without a `col2` bound may use the current broader
  first-column range behavior.
- `col1 IN (...) AND col2 ...` can remain conservative in the first pass unless
  an exact contract is specified.

Correctness rule:

- Zone maps may only eliminate blocks. Executor quals remain the final source
  of truth.
- If any bound shape is ambiguous or unsupported, fall back to the existing
  conservative range.

## Acceptance Tests

### C1. Equality + second-column range

Create compacted table:

```text
CREATE TABLE tenant_events (
    tenant_id int,
    id int,
    payload text,
    PRIMARY KEY (tenant_id, id)
) USING sorted_heap;
```

Insert enough rows for multiple pages per tenant, compact, and run:

```text
EXPLAIN (ANALYZE, COSTS OFF)
SELECT * FROM tenant_events
WHERE tenant_id = 1 AND id BETWEEN 100 AND 110;
```

Expected:

- `Custom Scan (SortedHeapScan)` is used.
- scanned blocks are close to the pages overlapping `id 100..110`, not the
  whole tenant range.
- result count is correct.

Status: covered by `SH21B` regression. The guard requires the query to scan no
more than 3 zone-map blocks and verifies the result count is 11.

### C2. Equality + second-column equality

Run:

```text
SELECT * FROM tenant_events WHERE tenant_id = 1 AND id = 123;
```

Expected:

- tight block range;
- correct result.

Status: covered by `SH21B` regression. The guard requires no more than 2
zone-map blocks for `tenant_id = 1 AND id = 123`.

### C3. First-column-only range remains correct

Run:

```text
SELECT * FROM tenant_events WHERE tenant_id BETWEEN 1 AND 2;
```

Expected:

- existing first-column pruning behavior remains correct.

Status: indirectly covered by existing single-column and multi-range
regressions; no dedicated C3 assertion yet.

### C4. Unsorted tail remains conservative

After compact, insert rows that create an unsorted tail.

Expected:

- sorted prefix uses tight composite pruning;
- unsorted tail remains conservatively scanned where needed;
- no false negatives.

Status: not yet directly covered for second-column predicates. Existing tail
regressions cover first-column behavior.

## Implementation Sketch

Data model:

- Keep the current `SortedHeapZoneMapEntry` layout.
- Treat page ranges as lexicographic intervals:
  - page lower key: `(zme_min, zme_min2)`
  - page upper key: `(zme_max, zme_max2)`

Planner/executor bounds:

- `SortedHeapScanBounds` already stores first-column and second-column bounds.
- Add helper predicates for lexicographic lower/upper comparisons when column 1
  equality and column 2 bounds are present.

Sorted-prefix search:

- Implemented first pass for `col1 = const AND col2` bounded:
  - binary-search the first-column prefix span;
  - refine that span by applying second-column overlap checks to zone-map
    entries;
  - emit contiguous matching ranges.
- Future optimization:
  - replace the linear refinement inside one column-1 span with true
    lexicographic binary search when the page metadata proves it is safe.

Adversary checks:

- Pages can contain mixed column-2 ranges when column 1 min/max spans multiple
  values. The lexicographic optimization must only engage when it can prove the
  first-column equality is safe.
- Sentinel second-column values mean "not tracked"; fallback, do not optimize.
- UUID/text lossy mapping remains conservative.

## Definition of Done

- C1-C2 regression tests are present in `sql/pg_sorted_heap.sql` as `SH21B`.
- Existing single-column pruning tests pass in `pg_sorted_heap`.
- Existing `sorted_hnsw` regression passes after the scan change.
- Remaining follow-up: add direct C4 coverage for sorted-prefix + unsorted-tail
  second-column predicates.
