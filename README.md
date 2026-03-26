# pg_sorted_heap

A PostgreSQL extension that physically sorts data by primary key, prunes
irrelevant blocks via per-page zone maps, and includes built-in vector types
plus planner-integrated HNSW search.

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

### Planner-integrated vector search (`sorted_hnsw`)

Built-in `svec` (float32, up to 16K dims) and `hsvec` (float16, up to 32K
dims) can now use a real PostgreSQL Index AM. The default ANN path is:

```sql
CREATE TABLE documents (
    id        bigserial PRIMARY KEY,
    embedding svec(384),
    content   text
);

CREATE INDEX documents_embedding_idx
ON documents USING sorted_hnsw (embedding)
WITH (m = 16, ef_construction = 64);

SET sorted_hnsw.shared_cache = on;  -- requires shared_preload_libraries='pg_sorted_heap'
SET sorted_hnsw.ef_search = 96;

SELECT id, content
FROM documents
ORDER BY embedding <=> '[0.1,0.2,0.3,...]'::svec
LIMIT 10;
```

This is planner-integrated KNN search: no sidecar prefix argument, no manual
rerank knob, and exact rerank happens inside the index scan.

If you want the base table footprint closer to pgvector `halfvec`, use native
`hsvec` instead of upcasting to `svec`:

```sql
CREATE TABLE documents_compact (
    id        bigserial PRIMARY KEY,
    embedding hsvec(384),
    content   text
);

CREATE INDEX documents_compact_embedding_idx
ON documents_compact USING sorted_hnsw (embedding hsvec_cosine_ops)
WITH (m = 16, ef_construction = 64);
```

### GraphRAG and fact graphs

`pg_sorted_heap` now also has a narrow GraphRAG path for fact-shaped multihop
queries. The intended workload is:

- ANN seed retrieval on `entity_id`
- 1-hop or 2-hop expansion over facts clustered by `(entity_id, relation_id, target_id)`
- exact rerank of the expanded candidates

The current helpers are:

- `sorted_heap_expand_rerank(...)`
- `sorted_heap_expand_twohop_rerank(...)`
- `sorted_heap_expand_twohop_path_rerank(...)`
- `sorted_heap_graph_rag_twohop_scan(...)`
- `sorted_heap_graph_rag_twohop_path_scan(...)`

On the current AWS ARM64 rerun (`4 vCPU`, `8 GiB RAM`), `5K` chains / `10K`
rows / `384D`, the current portable point is:

- `m=24`
- `ef_construction=200`
- `ann_k=64`
- `sorted_hnsw.ef_search=128`

which gives, on the current path-aware two-hop contract:

- `sorted_heap_expand_twohop_path_rerank()` -> `0.955 ms`, `hit@1 98.4%`, `hit@k 98.4%`
- `sorted_heap_graph_rag_twohop_path_scan()` -> `1.018 ms`, `hit@1 98.4%`, `hit@k 98.4%`

At the same knobs, the older city-only contract stays around `0.95-1.01 ms`
but drops to `hit@1 75.0%` / `hit@k 96.9%`, so the main quality gain is now
coming from the scorer contract rather than from a slower seed frontier.

The local M-series tuning run found a slightly different frontier, but the new
path-aware helper transferred cleanly to AWS at both `5K` and `10K` chains.
The full tuning history, reasoning, external-engine caveats, and larger-scale
results live in
[docs/design-graphrag.md](/Users/sergey/Projects/C/clustered_pg/docs/design-graphrag.md).

### Legacy/manual ANN paths

The older explicit ANN paths are still available when you want manual control
over recall/latency tradeoffs or compressed storage:

- `svec_ann_scan(...)` / `svec_ann_search(...)` for IVF-PQ
- `svec_hnsw_scan(...)` for sidecar-table HNSW experiments and manual rerank

Those paths are still documented, but they are no longer the default vector
story for this repository.

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

Current repo-owned harnesses:

- `python3 scripts/bench_gutenberg_local_dump.py --dump /tmp/cogniformerus_backup/cogniformerus_backup.dump --port 65473`
- `REMOTE_PYTHON=/path/to/python SH_EF=32 EXTRA_ARGS='--sh-ef-construction 200' ./scripts/bench_gutenberg_aws.sh <aws-host> /path/to/repo /path/to/dump 65485`
- `scripts/bench_sorted_hnsw_vs_pgvector.sh /tmp 65485 10000 20 384 10 vector 64 96`
- `python3 scripts/bench_ann_real_dataset.py --dataset nytimes-256 --sample-size 10000 --queries 20 --k 10 --pgv-ef 64 --sh-ef 96 --zvec-ef 64 --qdrant-ef 64`
- `python3 scripts/bench_qdrant_synthetic.py --rows 10000 --queries 20 --dim 384 --k 10 --ef 64`
- `python3 scripts/bench_zvec_synthetic.py --rows 10000 --queries 20 --dim 384 --k 10 --ef 64`

Restored Gutenberg dump on an AWS ARM64 host (4 vCPU,
8 GiB RAM), top-10. Ground truth is recomputed by exact PostgreSQL heap
search on the restored `svec` table; in the current rerun the stored
`bench_hnsw_gt` table matched that exact GT on 100% of the 50 benchmark
queries, so the fresh heap GT and the historical GT table agree. This rerun
uses `sorted_hnsw` with `ef_construction=200` and `ef_search=32`, and the
measurement phase reconnects after build before timing ordered scans.

| Method | p50 latency | Recall@10 | Notes |
|--------|:-----------:|:---------:|-------|
| Exact heap (`svec`) | 458.762ms | 100.0% | brute-force GT on restored corpus |
| **`sorted_hnsw` (`svec`)** | **1.287ms** | **100.0%** | `ef_construction=200`, `ef_search=32`, index 404 MB, total 1902 MB |
| `sorted_hnsw` (`hsvec`) | 1.404ms | 100.0% | `ef_construction=200`, `ef_search=32`, index 404 MB, total 1032 MB |
| pgvector HNSW (`halfvec`) | 2.031ms | 99.8% | `ef_search=64`, index 804 MB, total 1615 MB |
| zvec HNSW | 50.499ms | 100.0% | in-process collection, `ef=64`, ~1.12 GiB on disk |
| Qdrant HNSW | 6.028ms | 99.2% | local Docker on same AWS host, `hnsw_ef=64`, 103,260 points |

The precision-matched AWS row is `sorted_hnsw (hsvec)` vs pgvector
`halfvec`: `1.404ms @ 100.0%` versus `2.031ms @ 99.8%`, with total footprint
`1032 MB` versus `1615 MB`. The raw fastest PostgreSQL row on this corpus is
still `sorted_hnsw (svec)` at `1.287ms`, but that compares float32 source
storage against float16 storage. The `sorted_hnsw` index stays 404 MB for both
`svec` and `hsvec` because the index stores SQ8 graph state; the space win
from `hsvec` appears in the base table and TOAST footprint (`1902 MB -> 1032
MB`), not in the index itself.

Synthetic 10K x 384D cosine corpus, top-10, warm query loop. PostgreSQL
methods were rerun across 3 fresh builds and the table below reports median
`p50` / median recall. Qdrant uses 3 warm measurement passes on one local
Docker collection.

| Method | p50 latency | Recall@10 | Notes |
|--------|:-----------:|:---------:|-------|
| Exact heap (`svec`) | 2.03ms | 100% | Brute-force ground truth |
| **sorted_hnsw** | **0.158ms** | **100%** | `shared_cache=on`, `ef_search=96`, index ~5.4 MB |
| pgvector HNSW (`vector`) | 0.446ms | 90% median (90-95 range) | `ef_search=64`, same `M=16`, `ef_construction=64`, index ~2.0 MB |
| zvec HNSW | 0.611ms | 100% | local in-process collection, `ef=64` |
| Qdrant HNSW | 1.94ms | 100% | local Docker, `hnsw_ef=64` |

Real-dataset sample (`nytimes-256-angular`, sampled 10K x 256D, top-10). The
table below reports medians across 3 full harness runs. Ground truth is exact
heap search inside PostgreSQL on the sampled corpus, not numpy-side cosine.

| Method | p50 latency | Recall@10 | Notes |
|--------|:-----------:|:---------:|-------|
| Exact heap (`svec`) | 1.557ms | 100% | ground truth |
| **sorted_hnsw** | **0.327ms** | **85.0% median** (83.5-85.5 range) | `shared_cache=on`, `ef_search=96`, index ~4.1 MB |
| pgvector HNSW (`vector`) | 0.751ms | 79.0% median (78.5-79.0 range) | `ef_search=64`, same `M=16`, `ef_construction=64`, index ~13 MB |
| zvec HNSW | 0.403ms | 99.5% | local in-process collection, `ef=64`, ~14.1 MB on disk |
| Qdrant HNSW | 1.704ms | 99.5% | local Docker, `hnsw_ef=64` |

The synthetic corpus is an easy case; the `nytimes-256` sample is a much
harsher recall test at the same fixed HNSW settings. Treat the synthetic table
as an upper-bound speed/quality shape, not the only baseline.

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

### IVF-PQ search (legacy/manual path)

```sql
-- PQ-only (fastest, ~8ms)
SELECT * FROM svec_ann_scan('tbl', query, nprobe:=3, lim:=10, cb_id:=1, ivf_cb_id:=1);

-- With exact reranking (higher recall, ~22ms)
SELECT * FROM svec_ann_scan('tbl', query, nprobe:=10, lim:=10, rerank_topk:=200,
                            cb_id:=1, ivf_cb_id:=1);
```

### HNSW index (`sorted_hnsw`)

```sql
CREATE INDEX items_embedding_idx
ON items USING sorted_hnsw (embedding)
WITH (m = 16, ef_construction = 64);

SET sorted_hnsw.shared_cache = on;
SET sorted_hnsw.ef_search = 96;

SELECT id
FROM items
ORDER BY embedding <=> query
LIMIT 10;
```

### HNSW sidecar search (`svec_hnsw_scan`, legacy/manual path)

```sql
SET sorted_heap.hnsw_cache_l0 = on;  -- session-local sidecar cache

-- Fastest top-1: 0.51ms (SQ8 cache, 1 TOAST read)
SELECT * FROM svec_hnsw_scan('tbl', query, 'tbl_hnsw',
    ef_search:=32, lim:=1, rerank_topk:=1);

-- Fast top-10: 1.19ms, 98.6% recall (rerank = lim)
SELECT * FROM svec_hnsw_scan('tbl', query, 'tbl_hnsw',
    ef_search:=96, lim:=10, rerank_topk:=10);

-- Balanced top-10: 1.52ms, 99.8% recall (rerank = 2x lim)
SELECT * FROM svec_hnsw_scan('tbl', query, 'tbl_hnsw',
    ef_search:=96, lim:=10, rerank_topk:=20);
```

### Configuration

```sql
SET sorted_heap.enable_scan_pruning = on;       -- zone map pruning (default: on)
SET sorted_heap.vacuum_rebuild_zonemap = on;     -- VACUUM rebuilds zone map (default: on)
SET sorted_heap.lazy_update = on;                -- skip per-UPDATE zone map flush
SET sorted_hnsw.shared_cache = on;               -- shared decoded cache for sorted_hnsw
SET sorted_hnsw.ef_search = 96;                  -- sorted_hnsw beam width
SET sorted_heap.hnsw_cache_l0 = on;              -- session-local HNSW L0+upper cache
SET sorted_heap.hnsw_cache_sq8 = on;             -- SQ8 quantize svec in cache (4x smaller)
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
| `pq.h` / `pq.c` | PQ, IVF, ANN scan, sidecar HNSW search, graph scan, beam search |

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
- [Vector Search](docs/vector-search.md) -- `sorted_hnsw`, IVF-PQ, sidecar HNSW
- [Architecture](docs/architecture.md) -- zone maps, custom scan, compaction
- [SQL API](docs/api.md) -- full function reference
- [Benchmarks](docs/benchmarks.md) -- latency, throughput, vector search
- [Limitations](docs/limitations.md)
- [Changelog](CHANGELOG.md)
- [Operations](OPERATIONS.md) -- make targets and diagnostics

## License

Released under the [PostgreSQL License](LICENSE).
