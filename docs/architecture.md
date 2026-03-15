---
layout: default
title: Architecture
nav_order: 4
---

# Architecture

## Overview

pg_sorted_heap is a PostgreSQL **table access method** (AM) that physically
clusters data by primary key and uses per-page zone maps to skip irrelevant
blocks at query time.

```
COPY --> sort by PK --> heap insert --> update zone map
                                            |
compact/merge --> rewrite --> rebuild zone map --> set valid flag
                                                      |
SELECT WHERE pk op const --> planner hook --> extract bounds
    --> zone map lookup --> block range --> heap_setscanlimits --> skip I/O
```

---

## Source file layout

| File | Purpose |
|------|---------|
| `src/pg_sorted_heap.c` | Extension entry point, index AM handler, GUC registration, observability |
| `src/sorted_heap.c` | Table AM handler, zone map persistence (load/flush), compact, merge, vacuum rebuild, PK auto-detection, multi\_insert sorting |
| `src/sorted_heap_scan.c` | Custom scan provider: planner hook, bounds extraction, block range computation, parallel scan, IN/ANY pruning, LATERAL/NestLoop deferred params, mid-scan staleness detection |
| `src/sorted_heap_online.c` | Online (non-blocking) compact and merge: trigger-based change capture, PK-to-TID hash, multi-pass replay |
| `src/sorted_heap.h` | Shared header: data structures, version/magic constants, function declarations |

---

## Zone map internals

### What is a zone map?

A zone map stores the **min and max primary-key values** for every data page
(block) in the table. When a query has a WHERE clause on the PK, the scan
provider checks each page's min/max range and skips pages that cannot contain
matching rows.

### Storage layout

**Block 0** of every sorted_heap table is the **meta page** (v7 format). It
contains:

- A 32-byte header (magic, version, flags, PK metadata, persisted sorted
  prefix count)
- Up to **250 zone map entries** (32 bytes each, two columns)
- Up to **32 overflow page references**

Each zone map entry tracks two PK columns:

```
struct SortedHeapZoneMapEntry {   /* 32 bytes */
    int64  zme_min;    /* column 1 min */
    int64  zme_max;    /* column 1 max */
    int64  zme_min2;   /* column 2 min */
    int64  zme_max2;   /* column 2 max */
};
```

### Overflow pages (v6 format)

Tables larger than 250 data pages use **overflow pages** -- additional blocks
that store more zone map entries in a linked list:

- Each overflow page holds **254 entries** (8,144 bytes)
- Overflow pages link via `shmo_next_block` (no hard capacity limit)
- The meta page references the first 32 overflow pages directly; further
  pages are reached through the linked list

### Supported PK types

Values are converted to `int64` for zone map storage:

| Type | Conversion |
|------|-----------|
| int2, int4, int8 | Direct cast |
| timestamp, timestamptz | Already int64 (microseconds) |
| date | int32 to int64 |
| uuid | First 8 bytes as big-endian uint64 (lossy but order-preserving) |
| text, varchar | First 8 bytes zero-padded (requires `COLLATE "C"`) |

Non-C collation text/varchar columns gracefully degrade: the zone map is
simply not built, and queries fall back to sequential scan.

### Zone map flags

| Flag | Meaning |
|------|---------|
| `ZONEMAP_VALID` | Zone map is accurate for scan pruning (set after compact/rebuild) |
| `ZM_SORTED` | Entries are monotonically sorted (enables binary search) |

---

## Custom scan provider

### Planner hook

pg_sorted_heap installs a `set_rel_pathlist_hook` that fires for every base
relation. When it finds a sorted_heap table with a valid zone map and WHERE
restrictions on PK columns, it:

1. **Extracts bounds** from `=`, `<`, `<=`, `>`, `>=`, `BETWEEN` operators
   and `IN` / `= ANY(array)` expressions
2. **Computes the block range** using the zone map:
   - If entries are monotonically sorted: **binary search** (O(log N))
   - Otherwise: linear scan (O(N entries))
3. **Adds a CustomPath** with the computed cost (based on blocks to read)

### IN / ANY pruning

For `WHERE pk IN (...)` or `WHERE pk = ANY(ARRAY[...])`, the scan provider:

1. **Extracts all array elements** and converts them to `int64` zone map keys
2. **Computes a bounding box** (min/max of the values) to limit the scan range
3. **Sorts the values** and stores them for per-block filtering
4. At scan time, each block's zone map entry is checked against the sorted
   value list using **O(log K) binary search** — blocks where no target value
   falls within `[zme_min, zme_max]` are skipped

This works with both literal arrays (resolved at plan time) and parameterized
arrays in generic prepared statements (resolved at execution time).

### Runtime parameter resolution

For prepared statements (`$1`, `$2`), bounds cannot be resolved at plan time.
The scan provider defers resolution to the executor, where it:

- Evaluates parameter expressions via `ExecEvalExpr`
- Merges parameter bounds with any constant bounds
- Recomputes the block range on each rescan (supports NestLoop joins)
- For `= ANY($1)` arrays: deconstructs the array, computes bounding box
  and sorted value list at execution time

### LATERAL / NestLoop deferred resolution

When `SortedHeapScan` appears as the inner side of a NestLoop or LATERAL
join with `PARAM_EXEC` parameters (e.g., `WHERE id = ANY(outer.arr)`),
the parameter values are not yet available during `BeginCustomScan`.

The scan provider detects `PARAM_EXEC` nodes in the runtime expression
list and defers bounds resolution until the first `ReScanCustomScan` call
(when the outer node has populated the parameters). This prevents crashes
from evaluating uninitialized executor parameters.

### Mid-scan zone map staleness detection

If another backend modifies the zone map while a scan is in progress
(e.g., concurrent `compact` or `rebuild_zonemap`), the scan could apply
stale zone map entries and skip blocks that now contain matching rows.

To prevent this, each scan snapshots the cluster-wide **zone map
generation counter** (`zm_generation`, an atomic uint64 in shared memory)
at start. On each block transition, the scan compares the current
generation against its snapshot. If the generation has changed, the scan
disables zone map pruning and per-page PK prefiltering for the remainder
of the scan, falling back to executor qual evaluation. The generation
snapshot is refreshed on each rescan.

This adds one atomic read per block transition — negligible cost.

### EXPLAIN output

```
Custom Scan (SortedHeapScan) on events
  Filter: ((id >= 500) AND (id <= 600))
  Zone Map: 2 of 1946 blocks (pruned 1944)
  Buffers: shared hit=2
```

### Parallel scan

The custom scan supports parallel workers. Each worker independently applies
zone map pruning and processes its share of the block range.

---

## Multi-insert sorting pipeline

When data arrives through the COPY path (bulk insert), each batch is sorted
by PK before being written to the heap:

1. **PK auto-detection** -- scan `pg_index` for the primary key, cache column
   metadata (sort operators, collations, null-first flags)
2. **In-memory sort** -- `qsort_arg` with `SortSupport` comparators, O(N log N)
3. **Heap insert** -- sorted slots written to pages in order
4. **Zone map update** -- for each inserted tuple, update the page's min/max
   entry; flush to meta page via `GenericXLog` (crash-safe)

---

## Compaction

### Offline compact (`sorted_heap_compact`)

Uses PostgreSQL's CLUSTER infrastructure:

1. Scan PK index in order, write sorted tuples to a new table
2. Rebuild zone map from scratch on the new table
3. Set `ZONEMAP_VALID` flag
4. Atomic filenode swap

**Lock:** AccessExclusiveLock (blocks all reads and writes).

### Merge compact (`sorted_heap_merge`)

Incremental merge that avoids rewriting already-sorted data:

1. Detect the **sorted prefix** -- zone map entries where
   `entry[i+1].min >= entry[i].max`
2. If the table is already fully sorted, return immediately
3. Otherwise, merge the sorted prefix (sequential scan) with the unsorted
   tail (tuplesort) into a new table
4. Rebuild zone map, swap filenode

**Benefit:** 50--90% faster than full compact when the table is already
partially sorted.

**Lock:** AccessExclusiveLock.

### Online compact (`sorted_heap_compact_online`)

Non-blocking variant using trigger-based change capture:

| Phase | Lock | What happens |
|-------|------|--------------|
| 1 | AccessShareLock | Create log table + AFTER trigger on the original table |
| 2 | ShareUpdateExclusiveLock | Copy data via PK index scan; replay logged changes in a loop until convergence |
| 3 | AccessExclusiveLock (brief) | Final replay, rebuild zone map, atomic filenode swap, drop log table |

Concurrent reads and writes proceed normally during phases 1 and 2.

### Online merge (`sorted_heap_merge_online`)

Same three-phase approach as online compact, but merges the sorted prefix
with the unsorted tail instead of doing a full rewrite.

---

## VACUUM integration

When `sorted_heap.vacuum_rebuild_zonemap` is enabled (default), VACUUM
automatically rebuilds the zone map if it has been invalidated. This
re-enables scan pruning without a manual compact step.

---

## Lazy update mode

`sorted_heap.lazy_update = on` (PGC_USERSET) skips per-UPDATE zone map
maintenance. The first UPDATE on a covered page clears `SHM_FLAG_ZONEMAP_VALID`
on disk; subsequent UPDATEs do no zone map work. The planner falls back to
Index Scan when the validity flag is cleared.

INSERT always uses eager maintenance regardless of this setting. Compact
or merge restores the zone map.

This is a manual choice, not automatically activated. See the README
"UPDATE modes" section for a decision guide.
