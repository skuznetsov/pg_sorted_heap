# pg_sorted_heap

A PostgreSQL extension that physically sorts data by primary key, prunes
irrelevant blocks via per-page zone maps, and includes built-in vector search
-- all without pgvector or external indexes.

## When to use pg_sorted_heap

### Time-series and event logs

Data arrives ordered by time. sorted_heap keeps it physically sorted and skips
irrelevant time ranges at the I/O level. At 100M rows a point query reads
**1 buffer** (vs 8 for btree, 520K for seq scan).

```sql
CREATE TABLE events (
    ts    timestamptz,
    src   text,
    data  jsonb,
    PRIMARY KEY (ts, src)
) USING sorted_heap;

-- Bulk load (COPY path sorts by PK automatically)
COPY events FROM '/path/to/events.csv';
SELECT sorted_heap_compact('events');

-- Range query: reads only blocks that contain the time range
SELECT * FROM events
WHERE ts BETWEEN '2026-01-01' AND '2026-01-02'
  AND src = 'sensor-42';
-- Custom Scan (SortedHeapScan): 3 of 12,500 blocks (pruned 12,497)
```

### IoT and sensor data

Composite PK `(device_id, ts)` clusters readings by device. Zone map tracks
both columns -- queries on either or both benefit from pruning.

```sql
CREATE TABLE readings (
    device_id  int,
    ts         timestamptz,
    value      float8,
    PRIMARY KEY (device_id, ts)
) USING sorted_heap;

-- Insert millions of readings via COPY, then compact
SELECT sorted_heap_compact('readings');

-- Point lookup by device + time: 1-2 blocks
SELECT * FROM readings WHERE device_id = 1042 AND ts = '2026-03-01 12:00:00';

-- Range by device: zone map prunes all other devices
SELECT avg(value) FROM readings
WHERE device_id = 1042
  AND ts BETWEEN '2026-03-01' AND '2026-03-07';
```

### RAG and vector search (IVF-PQ)

Built-in `svec` (float32, up to 16K dims) and `hsvec` (float16, up to 32K
dims) types with IVF-PQ approximate nearest neighbor search. The sorted
storage itself acts as the inverted file index -- no separate HNSW graph, no
pgvector dependency, 30x smaller index.

```sql
CREATE TABLE documents (
    id           text,
    partition_id int2 GENERATED ALWAYS AS (
                     svec_ivf_assign(embedding, 1)) STORED,
    embedding    svec(768),
    pq_code      bytea GENERATED ALWAYS AS (
                     svec_pq_encode_residual(embedding,
                         svec_ivf_assign(embedding, 1), 1, 1)) STORED,
    content      text,
    PRIMARY KEY (partition_id, id)
) USING sorted_heap;

-- Load documents + embeddings, then compact
SELECT sorted_heap_compact('documents');

-- Train IVF centroids + PQ codebook (one-time)
SELECT * FROM svec_ann_train(
    'SELECT embedding FROM documents',
    nlist := 64, m := 192);
-- Re-compact after training to re-cluster by partition_id
SELECT sorted_heap_compact('documents');

-- Search: 8ms, 100% recall for self-query (RAG) workloads
SELECT * FROM svec_ann_scan(
    'documents', query_vec,
    nprobe := 3, lim := 10,
    cb_id := 1, ivf_cb_id := 1);
```

### Sub-millisecond HNSW search

Hierarchical HNSW via compact sidecar tables. Navigation uses hsvec(384)
sketches (no TOAST reads). With session-local cache: **0.98ms p50, 96.8%
recall@10** -- 3.6x faster than pgvector HNSW.

```sql
-- Enable session-local cache (~100 MB, built on first query)
SET sorted_heap.hnsw_cache_l0 = on;

-- Balanced: 0.98ms, 96.8% recall@10
SELECT * FROM svec_hnsw_scan(
    'documents', query_vec, 'documents_hnsw',
    ef_search := 96, lim := 10, rerank_topk := 48);

-- Quality-first: 1.83ms, 98.4% recall@10
SELECT * FROM svec_hnsw_scan(
    'documents', query_vec, 'documents_hnsw',
    ef_search := 96, lim := 10, rerank_topk := 0);
```

### Large-table analytics with range predicates

Any workload where queries filter on the primary key benefits from zone map
pruning. sorted_heap eliminates the need for a btree index on the PK --
the zone map replaces it with ~30% less storage.

```sql
CREATE TABLE orders (
    order_id   bigint PRIMARY KEY,
    customer   int,
    total      numeric,
    created_at timestamptz
) USING sorted_heap;

-- After compact: point query reads 1 block, range query reads exact range
-- No btree index needed -- zone map handles PK predicates directly
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM orders WHERE order_id BETWEEN 50000 AND 50100;
-- Custom Scan (SortedHeapScan): 1 of 8,000 blocks (pruned 7,999)
```

## UPDATE modes: eager vs lazy

sorted_heap has two zone map maintenance modes for UPDATEs, controlled by the
`sorted_heap.lazy_update` GUC. The mode is **never activated automatically** --
you choose it explicitly based on your workload.

### Eager mode (default)

```sql
SET sorted_heap.lazy_update = off;  -- default, no action needed
```

Every UPDATE that changes a zone map entry flushes the meta page to disk (WAL
write). This keeps the zone map accurate for scan pruning at all times, but
adds overhead per UPDATE: ~46% of heap throughput for small-column updates,
~100% for large-vector updates (where TOAST write dominates).

**Use when:** read-heavy workloads that depend on zone map scan pruning
(range queries, analytics). Occasional UPDATEs are fine -- the overhead
matters only at high UPDATE throughput.

### Lazy mode

```sql
SET sorted_heap.lazy_update = on;  -- per session
-- or globally:
ALTER SYSTEM SET sorted_heap.lazy_update = on;
SELECT pg_reload_conf();
```

The first UPDATE on a covered page invalidates the zone map on disk (one WAL
write). All subsequent UPDATEs skip zone map maintenance entirely. The planner
falls back to Index Scan (btree) instead of Custom Scan (zone map pruning).
INSERT always uses eager maintenance regardless of this setting.

**Use when:** write-heavy workloads (high UPDATE throughput), or when point
lookups use btree Index Scan anyway (no range queries). Compact or merge
restores zone map pruning when needed:

```sql
-- Restore zone map pruning after a batch of updates
SELECT sorted_heap_compact('t');
-- or incrementally:
SELECT sorted_heap_merge('t');
```

### Decision guide

| Workload | Mode | Why |
|----------|------|-----|
| Append-only (INSERT + range SELECT) | Eager | Zone map pruning is the main benefit |
| Mixed OLTP (UPDATE + point SELECT) | Lazy | Point lookups use btree; UPDATE needs speed |
| Bulk ETL (INSERT, then query) | Eager | Compact after load, queries use zone map |
| Write-heavy + periodic analytics | Lazy | Lazy during writes, compact before analytics |

### Per-transaction control

```sql
-- Temporarily switch to lazy for a batch update
BEGIN;
SET LOCAL sorted_heap.lazy_update = on;
UPDATE large_table SET status = 'done' WHERE batch_id = 42;
COMMIT;
-- Outer session remains in eager mode
```

## How it works

`sorted_heap` keeps data physically ordered by primary key:

1. **Sorted bulk insert** -- `multi_insert` (COPY path) sorts each batch by PK
   before writing to heap. Produces physically sorted runs.

2. **Zone maps** -- Block 0 stores per-page `(col1_min, col1_max, col2_min,
   col2_max)` for the first two PK columns. Unlimited capacity via overflow
   page chain. Supported types: int2/4/8, timestamp, timestamptz, date, uuid,
   text/varchar (`COLLATE "C"`).

3. **Compaction** -- `sorted_heap_compact(regclass)` does a full CLUSTER
   rewrite; `sorted_heap_merge(regclass)` does incremental two-way merge of
   sorted prefix + unsorted tail. Both have online (non-blocking) variants.

4. **Scan pruning** -- A planner hook injects a `SortedHeapScan` custom path
   when WHERE has PK predicates. The executor uses `heap_setscanlimits()` to
   physically skip pruned blocks. Supports constants, prepared statements,
   `IN`/`ANY(array)`, and `LATERAL`/NestLoop runtime parameters.

```
COPY -> sort by PK -> heap insert -> update zone map
                                          |
compact/merge -> rewrite -> rebuild zone map -> set valid flag
                                                    |
SELECT WHERE pk op const -> planner hook -> extract bounds
    -> zone map lookup -> block range -> heap_setscanlimits -> skip I/O
```

## Performance

PostgreSQL 18, Apple M-series, `shared_buffers=4GB`. Warm cache, avg 5 runs.

### Query latency (EXPLAIN ANALYZE)

**100M rows** (7.8 GB sorted_heap, 7.8 GB heap+btree):

| Query | sorted_heap | heap+btree | heap seqscan |
|-------|------------|-----------|-------------|
| Point (1 row) | 0.045ms / **1 buf** | 0.506ms / 8 bufs | 1,190ms / 519K bufs |
| Narrow (100) | 0.166ms / 2 bufs | 0.144ms / 9 bufs | 1,325ms / 520K bufs |
| Medium (5K) | 0.479ms / 38 bufs | 0.812ms / 58 bufs | 1,326ms / 519K bufs |
| Wide (100K) | 7.9ms / 737 bufs | 10.1ms / 1,017 bufs | 1,405ms / 518K bufs |

### Throughput (pgbench, 10s, 1 client, prepared mode)

| Query | 1M (sh/btree) | 10M (sh/btree) | 100M (sh/btree) |
|-------|:---:|:---:|:---:|
| Point | 46.9K/59.4K | 46.5K/58.0K | 32.6K/43.6K |
| Wide (100K) | 295/289 | 293/286 | 168/157 |

At 100M rows with simple mode, sorted_heap wins all query types: point 4x
(18.7K vs 4.6K TPS).

### CRUD performance (500K rows, svec(128), prepared mode)

| Operation | eager / heap | lazy / heap | Notes |
|-----------|:-----------:|:-----------:|-------|
| SELECT PK | 85% | 85% | Index Scan via btree |
| SELECT range 1K | 97% | -- | Custom Scan pruning |
| Bulk INSERT | 100% | 100% | Always eager |
| DELETE + INSERT | 63% | 63% | INSERT always eager |
| UPDATE non-vec | 46% | **100%** | Lazy skips zone map flush |
| UPDATE vec col | 102% | **100%** | Parity both modes |
| Mixed OLTP | 83% | **97%** | Near-parity with lazy |

**Eager mode** (default): maintains zone maps on every UPDATE for scan pruning.
**Lazy mode** (`SET sorted_heap.lazy_update = on`): trades scan pruning for
UPDATE parity with heap. Compact/merge restores pruning. Recommended for
write-heavy workloads.

### Vector search comparison

103K x 2880-dim vectors, Apple M-series, warm cache.

| Method | Recall@10 | p50 latency | Index size |
|--------|:---------:|:-----------:|:----------:|
| **svec_hnsw_scan ef=96, rk=48** | **96.8%** | **0.70ms** | ~100 MB |
| zvec HNSW M=32, ef=100 | 100% | 1.04ms | 1,173 MB |
| pgvector HNSW ef=64 | 99.8% | 1.70ms | 806 MB |
| IVF-PQ nprobe=10, rr=200 | 99.0% | 10.7ms | 27 MB |
| Qdrant HNSW M=32, ef=100 | 100% | 23.2ms | 2,626 MB |

zvec: in-process embedded C++ (Alibaba Proxima). Qdrant: Rust server via
Docker. pgvector / pg_sorted_heap: PostgreSQL extensions via unix socket.
Qdrant server-side latency ~19ms (rest is Docker VM overhead).

## Quick start

### Requirements

- PostgreSQL 17 or 18
- Standard PGXS build toolchain (`pg_config` in PATH)

### Build and install

```bash
make && make install
```

### Create a table

```sql
CREATE EXTENSION pg_sorted_heap;

CREATE TABLE events (
    id      int PRIMARY KEY,
    ts      timestamptz,
    payload text
) USING sorted_heap;

-- Bulk load (COPY path sorts by PK automatically)
INSERT INTO events
SELECT i, now() - (i || ' seconds')::interval, repeat('x', 80)
FROM generate_series(1, 100000) i;

-- Compact to globally sort and build zone map
SELECT sorted_heap_compact('events');

-- Zone map pruning kicks in automatically
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM events WHERE id BETWEEN 500 AND 600;
-- Custom Scan (SortedHeapScan)
-- Zone Map: 2 of 1946 blocks (pruned 1944)
```

### Run tests

```bash
make installcheck              # regression tests
make test-crash-recovery       # crash recovery (4 scenarios)
make test-concurrent           # concurrent DML + online ops
make test-toast                # TOAST integrity
make test-alter-table          # ALTER TABLE DDL (36 checks)
make test-dump-restore         # pg_dump/restore lifecycle
make test-graph-builder        # graph sidecar bootstrap + rebuild smoke
make test-pg-upgrade           # pg_upgrade 17->18
make policy-safety-selftest    # policy + doc contract checks
make pg-core-regression-smoke  # PG core regression smoke test
make selftest-lightweight      # lightweight selftest suite
```

Command selection quick map: see [OPERATIONS.md](OPERATIONS.md) for the full
list of available make targets and their descriptions.

## SQL API

### Compaction

```sql
-- Offline compact: full CLUSTER rewrite (AccessExclusiveLock)
SELECT sorted_heap_compact('t');

-- Online compact: non-blocking (ShareUpdateExclusiveLock,
-- brief AccessExclusiveLock for final swap)
CALL sorted_heap_compact_online('t');

-- Offline merge: sorted prefix + unsorted tail (faster than full compact)
SELECT sorted_heap_merge('t');

-- Online merge
CALL sorted_heap_merge_online('t');
```

### Zone map inspection

```sql
SELECT sorted_heap_zonemap_stats('t');   -- flags, entry count, ranges
SELECT sorted_heap_rebuild_zonemap('t'); -- manual rebuild
```

### Scan statistics

```sql
SELECT * FROM sorted_heap_scan_stats();  -- total_scans, blocks_scanned, blocks_pruned
SELECT sorted_heap_reset_stats();        -- reset counters
```

### Vector types

```sql
-- svec: float32, up to 16,000 dimensions
CREATE TABLE items (id text PRIMARY KEY, embedding svec(768)) USING sorted_heap;

-- hsvec: float16, up to 32,000 dimensions (half storage)
CREATE TABLE items_h (id text PRIMARY KEY, embedding hsvec(768)) USING sorted_heap;

-- Cosine distance: <=> operator
SELECT a.embedding <=> b.embedding FROM items a, items b WHERE a.id='a' AND b.id='b';
```

### IVF-PQ search

```sql
-- PQ-only (fastest, ~8ms)
SELECT * FROM svec_ann_scan('tbl', query, nprobe:=3, lim:=10, cb_id:=1, ivf_cb_id:=1);

-- With exact reranking (higher recall, ~22ms)
SELECT * FROM svec_ann_scan('tbl', query, nprobe:=10, lim:=10, rerank_topk:=200,
                            cb_id:=1, ivf_cb_id:=1);
```

### HNSW search

```sql
SET sorted_heap.hnsw_cache_l0 = on;  -- session-local cache

-- Balanced: 0.98ms, 96.8% recall@10
SELECT * FROM svec_hnsw_scan('tbl', query, 'tbl_hnsw',
    ef_search:=96, lim:=10, rerank_topk:=48);
```

### Configuration

```sql
SET sorted_heap.enable_scan_pruning = on;       -- zone map pruning (default: on)
SET sorted_heap.vacuum_rebuild_zonemap = on;     -- VACUUM rebuilds zone map (default: on)
SET sorted_heap.lazy_update = on;                -- skip per-UPDATE zone map flush
SET sorted_heap.hnsw_cache_l0 = on;              -- session-local HNSW L0+upper cache
SET sorted_heap.hnsw_ef_patience = 0;            -- adaptive ef early termination (0=off)
SET sorted_heap.ann_timing = on;                 -- timing breakdown in DEBUG1
```

## Architecture

| File | Purpose |
|------|---------|
| `sorted_heap.h` | Meta page layout, zone map structs (v7), SortedHeapRelInfo |
| `sorted_heap.c` | Table AM: sorted multi_insert, zone map persistence, compact, merge, vacuum |
| `sorted_heap_scan.c` | Custom scan: planner hook, parallel scan, multi-col pruning, IN/ANY, runtime params |
| `sorted_heap_online.c` | Online compact + merge: trigger-based copy, replay, swap |
| `pg_sorted_heap.c` | Extension entry, legacy clustered index AM, GUC registration |
| `svec.h` / `svec.c` | svec type (float32): I/O, typmod, NEON cosine distance `<=>` |
| `hsvec.h` / `hsvec.c` | hsvec type (float16): I/O, cosine distance, NEON SIMD, casts |
| `pq.h` / `pq.c` | PQ, IVF, ANN scan, HNSW search, graph scan, beam search |

### Zone map details

- **v7 format**: 32-byte entries with col1 + col2 min/max per page, persisted
  sorted prefix count
- Meta page (block 0): 250 entries in special space
- Overflow pages: 254 entries/page, linked list (no capacity limit)
- Updated atomically via GenericXLog during multi_insert
- Autovacuum rebuilds zone map when validity flag is not set

### Custom scan provider

- Hooks into `set_rel_pathlist_hook`
- Extracts PK bounds from WHERE clauses (constants, params, IN/ANY)
- Binary search on monotonically sorted zone map, linear scan otherwise
- `heap_setscanlimits(start, nblocks)` for physical block skip
- Parallel-aware: `add_partial_path` + Gather for multi-worker scans
- EXPLAIN: `Zone Map: N of M blocks (pruned P)`

## Limitations

- Zone map tracks first two PK columns. Supported types: int2/4/8,
  timestamp(tz), date, uuid, text/varchar (`COLLATE "C"`).
- Online compact/merge not supported for UUID/text/varchar PKs.
- UPDATE does not re-sort; use compact/merge periodically.
- `heap_setscanlimits()` only supports contiguous block ranges.
- pg_dump/restore: compact needed after restore.
- pg_upgrade 17 to 18: tested and verified.

## Documentation

- [Quick Start](docs/quickstart.md)
- [Vector Search](docs/vector-search.md) -- IVF-PQ, HNSW, graph search
- [Architecture](docs/architecture.md) -- zone maps, custom scan, compaction
- [SQL API](docs/api.md) -- full function reference
- [Benchmarks](docs/benchmarks.md) -- latency, throughput, vector search
- [Limitations](docs/limitations.md)
- [Changelog](CHANGELOG.md)
- [Operations](OPERATIONS.md) -- make targets and diagnostics

## License

Released under the [PostgreSQL License](LICENSE).
