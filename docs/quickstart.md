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
make test-partition-lock       # partition helper lock behavior
make test-dump-restore         # pg_dump/restore lifecycle (10 checks)
make test-graph-builder        # graph sidecar bootstrap + rebuild smoke
make test-pg-upgrade           # pg_upgrade 17->18 (13 checks)
```

## Stable GraphRAG quick start

The stable `0.13` GraphRAG surface is intentionally narrow: fact-shaped
retrieval over a `sorted_heap` table clustered by
`(entity_id, relation_id, target_id)`.

```sql
CREATE EXTENSION pg_sorted_heap;

CREATE TABLE facts (
    entity_id   int4,
    relation_id int2,
    target_id   int4,
    embedding   svec(384),
    payload     text,
    PRIMARY KEY (entity_id, relation_id, target_id)
) USING sorted_heap;

CREATE INDEX facts_embedding_idx
ON facts USING sorted_hnsw (embedding)
WITH (m = 24, ef_construction = 200);

SET sorted_hnsw.ef_search = 128;
```

One-hop retrieval:

```sql
SELECT *
FROM sorted_heap_graph_rag(
    'facts'::regclass,
    '[0.1,0.2,0.3,...]'::svec,
    relation_path := ARRAY[1],
    ann_k := 64,
    top_k := 10,
    score_mode := 'endpoint'
);
```

Two-hop path-aware retrieval:

```sql
SELECT *
FROM sorted_heap_graph_rag(
    'facts'::regclass,
    '[0.1,0.2,0.3,...]'::svec,
    relation_path := ARRAY[1, 2],
    ann_k := 64,
    top_k := 10,
    score_mode := 'path'
);
```

If your fact table uses different column names, register the mapping once:

```sql
SELECT sorted_heap_graph_register(
    'facts_alias'::regclass,
    entity_column := 'src_id',
    relation_column := 'edge_type',
    target_column := 'dst_id',
    embedding_column := 'vec',
    payload_column := 'body'
);
```

For stage-level tuning, reset and inspect the backend-local last-call stats:

```sql
SELECT sorted_heap_graph_rag_reset_stats();

SELECT *
FROM sorted_heap_graph_rag(
    'facts'::regclass,
    '[0.1,0.2,0.3,...]'::svec,
    relation_path := ARRAY[1, 2],
    ann_k := 64,
    top_k := 10,
    score_mode := 'path'
);

SELECT * FROM sorted_heap_graph_rag_stats();
```
