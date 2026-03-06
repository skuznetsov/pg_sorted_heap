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

- **Vector search** -- Built-in `svec` vector type (float32, up to 16,000 dim)
  with cosine distance and IVF-PQ approximate nearest neighbor search. 30x
  smaller index than HNSW, no pgvector dependency, no 2,000-dim index limit.
  See [Vector Search](vector-search).

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
