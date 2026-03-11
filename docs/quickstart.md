---
layout: default
title: Quick Start
nav_order: 2
---

# Quick Start

## Requirements

- PostgreSQL 17 or 18
- Standard PGXS build toolchain (`pg_config` in PATH)

## Build and install

```bash
git clone https://github.com/skuznetsov/pg_sorted_heap.git
cd pg_sorted_heap
make && make install
```

To build for a specific PG version:

```bash
make PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config
make install PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config
```

## Create a sorted_heap table

```sql
CREATE EXTENSION pg_sorted_heap;

CREATE TABLE events (
    id      int PRIMARY KEY,
    ts      timestamptz,
    payload text
) USING sorted_heap;
```

## Load data

The COPY path (`multi_insert`) automatically sorts each batch by PK:

```sql
INSERT INTO events
SELECT i, now() - (i || ' seconds')::interval, repeat('x', 80)
FROM generate_series(1, 100000) i;
```

## Compact

Compaction rewrites all data in globally sorted PK order and builds the zone map:

```sql
SELECT sorted_heap_compact('events'::regclass);
```

For non-blocking compaction on a live system:

```sql
CALL sorted_heap_compact_online('events'::regclass);
```

## Verify scan pruning

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM events WHERE id BETWEEN 500 AND 600;
```

Output:

```
 Custom Scan (SortedHeapScan) on events
   Filter: ((id >= 500) AND (id <= 600))
   Zone Map: 2 of 1946 blocks (pruned 1944)
   Buffers: shared hit=2
```

The zone map pruned 1,944 of 1,946 blocks -- only 2 blocks were read.

## Run tests

```bash
make installcheck              # regression tests
make test-crash-recovery       # crash recovery (4 scenarios)
make test-concurrent           # concurrent DML + online ops
make test-toast                # TOAST integrity + concurrent guard
make test-alter-table          # ALTER TABLE DDL (36 checks)
make test-dump-restore         # pg_dump/restore lifecycle (10 checks)
make test-graph-builder        # graph sidecar bootstrap + rebuild smoke
make test-pg-upgrade           # pg_upgrade 17->18 (13 checks)
```
