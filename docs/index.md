---
layout: default
title: Home
nav_order: 1
---

# pg_sorted_heap

A PostgreSQL extension that stores data physically sorted by primary key, uses
per-page zone maps to skip irrelevant blocks at query time, and delivers
btree-competitive query performance without a separate index structure.

{: .fs-6 .fw-300 }

[Get started](quickstart){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View on GitHub](https://github.com/skuznetsov/pg_sorted_heap){: .btn .fs-5 .mb-4 .mb-md-0 }

---

## Key features

- **Physically sorted storage** -- `multi_insert` (COPY path) sorts each batch
  by PK before writing to heap. Compact and merge operations maintain global order.

- **Zone map scan pruning** -- Block 0 stores per-page min/max for the first two
  PK columns. The custom scan provider skips irrelevant blocks at I/O level,
  reading only the blocks that contain matching rows.

- **Online compaction** -- Non-blocking `compact_online` and `merge_online`
  procedures use trigger-based copy + replay for zero-downtime maintenance.

- **Prepared statement support** -- Runtime parameter resolution enables scan
  pruning for parameterized queries (`$1`, `$2`), not just literal constants.

- **IN / ANY pruning** -- `WHERE pk IN (...)` and `pk = ANY(array)` queries
  benefit from per-block zone map pruning with O(log K) binary search.
  Works with literal arrays, prepared statements, and LATERAL/NestLoop
  runtime parameters.

- **Stable vector search** -- Built-in `svec` (float32, up to 16K dim) and
  `hsvec` (float16, up to 32K dim) with planner-integrated `sorted_hnsw` KNN
  index scans. See [Vector Search](vector-search).

- **Stable fact-shaped GraphRAG API** -- Unified one-hop and two-hop retrieval
  for fact graphs via `sorted_heap_graph_rag(...)`, plus schema registration
  and last-call stats. See [SQL API](api) and [Benchmarks](benchmarks).

- **Beta GraphRAG building blocks** -- Lower-level expansion/rerank helpers
  and wrapper primitives remain available for explicit control and reference
  workflows.

- **Legacy/manual ANN paths** -- IVF-PQ and sidecar HNSW APIs remain available
  for manual storage/recall tuning, but they are no longer the default path.

- **Lazy update mode** -- `sorted_heap.lazy_update = on` skips per-UPDATE zone
  map maintenance, reaching heap-parity UPDATE throughput. Compact/merge
  restores zone map pruning. Recommended for write-heavy workloads.

- **PG 17 and PG 18** -- Builds and runs on both versions with zero warnings.
  pg_upgrade from 17 to 18 tested and verified.

## How it works

```
COPY --> sort by PK --> heap insert --> update zone map
                                            |
compact/merge --> rewrite --> rebuild zone map --> set valid flag
                                                      |
SELECT WHERE pk op const --> planner hook --> extract bounds
    --> zone map lookup --> block range --> heap_setscanlimits --> skip I/O
```

## At a glance

```sql
CREATE EXTENSION pg_sorted_heap;

CREATE TABLE events (
    id      int PRIMARY KEY,
    ts      timestamptz,
    payload text
) USING sorted_heap;

INSERT INTO events
SELECT i, now() - (i || ' seconds')::interval, repeat('x', 80)
FROM generate_series(1, 100000) i;

SELECT sorted_heap_compact('events'::regclass);

EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM events WHERE id BETWEEN 500 AND 600;
-- Custom Scan (SortedHeapScan)
-- Zone Map: 2 of 1946 blocks (pruned 1944)
```

At 100M rows, a point query reads **1 buffer** (vs 8 for btree, 519,906 for seq scan).

## Release surface

- **Stable:** `sorted_heap` table AM, compaction/merge surface, zone-map scan
  pruning, `sorted_hnsw` Index AM for `svec` and `hsvec`, and the narrow
  fact-shaped GraphRAG API.
- **Beta:** Lower-level GraphRAG helpers/wrappers, code-corpus reference
  contracts, and the FlashHadamard experimental retrieval lane.
- **Legacy/manual:** IVF-PQ and sidecar HNSW paths.

## Contract specs

- [Composite-PK Pruning](spec-composite-pk-pruning) -- first-pass two-column
  pruning contract and follow-ups.
- [Declarative Partitioning](spec-partitioning) -- parent helper and
  leaf-scoped maintenance contract.
- [Huge-Table Compaction](spec-huge-table-compaction) -- rewrite/free-space
  operating model.
- [Filtered ANN](spec-filtered-ann) -- proposed safe modes for filtered vector
  retrieval.
- [Online Lossy-PK Maintenance](spec-online-lossy-pk) -- future lossless
  replay-key contract for UUID/text online maintenance.
