# pg_sorted_heap

PostgreSQL extension providing physically sorted storage via custom Table AMs.
The primary access method is **`sorted_heap`** — a heap-based AM that sorts
bulk inserts by primary key, maintains per-page zone maps with unlimited
capacity, and uses a custom scan provider to skip irrelevant blocks at query
time. Supports parallel scans, prepared statements, online compaction, and
incremental merge.

## How it works

`sorted_heap` keeps data physically ordered by primary key:

1. **Sorted bulk insert** — `multi_insert` (COPY path) sorts each batch by PK
   before delegating to the standard heap. Produces physically sorted runs.

2. **Zone maps** — Block 0 is a meta page storing per-page `(col1_min,
   col1_max, col2_min, col2_max)` for the first two PK columns. Unlimited
   capacity via overflow page chain (v6 format). Supported types: int2, int4,
   int8, timestamp, timestamptz, date, uuid, text/varchar (C collation).

3. **Compaction** — `sorted_heap_compact(regclass)` does a full CLUSTER rewrite;
   `sorted_heap_merge(regclass)` does incremental two-way merge of sorted
   prefix + unsorted tail. Both have online (non-blocking) variants.

4. **Scan pruning** — A `set_rel_pathlist_hook` injects a `SortedHeapScan`
   custom path when the WHERE clause has PK predicates. The executor calls
   `heap_setscanlimits()` to physically skip pruned blocks, then does per-block
   zone map checks for fine-grained filtering. Supports both literal constants
   and parameterized queries (prepared statements).

```
COPY → sort by PK → heap insert → update zone map
                                        ↓
compact/merge → rewrite → rebuild zone map → set valid flag
                                                  ↓
SELECT WHERE pk op const → planner hook → extract bounds
    → zone map lookup → block range → heap_setscanlimits → skip I/O
```

## Performance

PostgreSQL 18, Apple M-series (12 CPU, 64 GB RAM), zone map v6.
shared_buffers=4GB, work_mem=256MB, maintenance_work_mem=2GB.

### EXPLAIN ANALYZE (warm cache, avg 5 runs)

**1M rows** (71 MB sorted_heap, 71 MB heap+btree)

| Query | sorted_heap | heap+btree | heap seqscan |
|-------|------------|-----------|-------------|
| Point (1 row) | 0.035ms / 1 buf | 0.046ms / 7 bufs | 15.2ms / 6,370 bufs |
| Narrow (100) | 0.043ms / 2 bufs | 0.067ms / 8 bufs | 16.2ms / 6,370 bufs |
| Medium (5K) | 0.434ms / 33 bufs | 0.492ms / 52 bufs | 16.1ms / 6,370 bufs |
| Wide (100K) | 7.5ms / 638 bufs | 8.9ms / 917 bufs | 17.4ms / 6,370 bufs |

**10M rows** (714 MB sorted_heap, 712 MB heap+btree)

| Query | sorted_heap | heap+btree | heap seqscan |
|-------|------------|-----------|-------------|
| Point (1 row) | 0.034ms / 1 buf | 0.047ms / 7 bufs | 117.9ms / 63,695 bufs |
| Narrow (100) | 0.037ms / 1 buf | 0.062ms / 7 bufs | 130.9ms / 63,695 bufs |
| Medium (5K) | 0.435ms / 32 bufs | 0.549ms / 51 bufs | 131.0ms / 63,695 bufs |
| Wide (100K) | 7.6ms / 638 bufs | 8.8ms / 917 bufs | 131.4ms / 63,695 bufs |

**100M rows** (7.8 GB sorted_heap, 7.8 GB heap+btree)

| Query | sorted_heap | heap+btree | heap seqscan |
|-------|------------|-----------|-------------|
| Point (1 row) | 0.045ms / 1 buf | 0.506ms / 8 bufs | 1,190ms / 519,906 bufs |
| Narrow (100) | 0.166ms / 2 bufs | 0.144ms / 9 bufs | 1,325ms / 520,782 bufs |
| Medium (5K) | 0.479ms / 38 bufs | 0.812ms / 58 bufs | 1,326ms / 519,857 bufs |
| Wide (100K) | 7.9ms / 737 bufs | 10.1ms / 1,017 bufs | 1,405ms / 518,896 bufs |

sorted_heap reads fewer blocks than btree at all selectivities. Zone map
prunes to exact block range; btree traverses 3-4 index pages per lookup. At
100M rows: point query reads 1 buffer vs 8 for btree, 519,906 for seqscan.

### pgbench Throughput (10s, 1 client)

**Prepared mode** (`-M prepared`): query planned once, re-executed with parameters.

| Query | 1M sh / btree | 10M sh / btree | 100M sh / btree |
|-------|-------------:|--------------:|---------------:|
| Point (1 row) | 46.9K / 59.4K | 46.5K / 58.0K | 32.6K / 43.6K |
| Narrow (100) | 22.3K / 29.1K | 22.5K / 28.8K | 17.9K / 18.1K |
| Medium (5K) | 3.4K / 5.1K | 3.4K / 4.8K | 2.4K / 2.4K |
| Wide (100K) | 295 / 289 | 293 / 286 | 168 / 157 |

**Simple mode** (`-M simple`): each query parsed, planned, and executed.

| Query | 1M sh / btree | 10M sh / btree | 100M sh / btree |
|-------|-------------:|--------------:|---------------:|
| Point (1 row) | 28.4K / 38.0K | 29.1K / 41.4K | 18.7K / 4.6K |
| Narrow (100) | 19.6K / 24.4K | 21.8K / 27.6K | 7.1K / 5.5K |
| Medium (5K) | 3.1K / 3.7K | 3.4K / 4.8K | 2.1K / 1.6K |
| Wide (100K) | 198 / 290 | 200 / 286 | 163 / 144 |

In prepared mode, sorted_heap point queries reach 46.5K TPS at 10M rows (+60%
vs simple mode). At 100M with simple mode, sorted_heap wins all query types:
point 4x (18.7K vs 4.6K), narrow +29%, medium +28%, wide +13%. Wide (100K row)
queries show sorted_heap slightly ahead at all scales.

## Quick start

### Requirements

- PostgreSQL 17 or 18
- Standard PGXS build toolchain (`pg_config` in PATH)

### Build and install

```bash
make && make install
```

### Create a sorted_heap table

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
SELECT pg_sorted_heap.sorted_heap_compact('events'::regclass);

-- Zone map pruning kicks in automatically
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM events WHERE id BETWEEN 500 AND 600;
-- Custom Scan (SortedHeapScan)
-- Zone Map: 2 of 1946 blocks (pruned 1944)
```

### Run tests

```bash
make installcheck              # regression tests (17 suites)
make test-crash-recovery       # crash recovery (4 scenarios)
make test-concurrent           # concurrent DML + online ops
make test-toast                # TOAST integrity + concurrent guard
make test-alter-table          # ALTER TABLE DDL (36 checks)
make test-dump-restore         # pg_dump/restore lifecycle (10 checks)
make test-pg-upgrade           # pg_upgrade 17->18 (13 checks)
```

## Vector search

pg_sorted_heap includes a built-in vector type (`svec`) and IVF-PQ approximate
nearest neighbor search. The key insight: sorted_heap's physical clustering by
primary key prefix **is** the inverted file index — no separate index structure,
no pgvector dependency, no 800 MB HNSW graph.

### Why not pgvector?

| | pg_sorted_heap IVF-PQ | pgvector HNSW |
|---|---|---|
| Index size (103K × 2880-dim) | **27 MB** (PQ codes) | 806 MB (full vectors) |
| Recall@10 | **100%** (pool=200) | 97.7% |
| Latency | 60 ms | 14 ms |
| Separate index needed? | No — PK prefix is the IVF | Yes — HNSW graph |
| External dependency | None | pgvector extension |
| Scales to 1M vectors | ~260 MB | ~8 GB |

PQ trades some latency for 30x smaller storage and higher recall. At scale,
the storage difference dominates: 8 GB HNSW index vs 260 MB PQ codes at 1M
vectors.

### svec type

```sql
-- Declare a 768-dimensional vector column
CREATE TABLE items (
    id    text PRIMARY KEY,
    embedding svec(768)
) USING sorted_heap;

-- Insert with bracket notation
INSERT INTO items VALUES ('doc1', '[0.1, 0.2, 0.3, ...]');

-- Cosine distance operator
SELECT a.id, a.embedding <=> b.embedding AS distance
FROM items a, items b
WHERE a.id = 'doc1' AND b.id = 'doc2';
```

### ANN search workflow

```sql
-- 1. Create table with IVF partition + PQ code as generated columns
CREATE TABLE vectors (
    id           text,
    partition_id int2 GENERATED ALWAYS AS (
                     pg_sorted_heap.svec_ivf_assign(embedding)) STORED,
    embedding    pg_sorted_heap.svec(2880),
    pq_code      bytea GENERATED ALWAYS AS (
                     pg_sorted_heap.svec_pq_encode(embedding)) STORED,
    PRIMARY KEY (partition_id, id)
) USING sorted_heap;

-- 2. Load data (COPY path sorts by PK automatically)
INSERT INTO vectors (id, embedding)
SELECT id, embedding FROM source_table;
SELECT pg_sorted_heap.sorted_heap_compact('vectors');

-- 3. Train IVF centroids + PQ codebook
SELECT * FROM pg_sorted_heap.svec_ann_train(
    'SELECT embedding FROM vectors',
    nlist := 64,    -- IVF partitions
    m := 180        -- PQ subvectors (2880/180 = 16-dim each)
);

-- 4. Re-encode with trained codebook (updates generated columns)
-- After training, compact again to re-cluster by new partition_id
SELECT pg_sorted_heap.sorted_heap_compact('vectors');

-- 5. Search — PQ-only (fastest, ~40ms)
SELECT * FROM pg_sorted_heap.svec_ann_search(
    'vectors', query_vec, nprobe := 10, lim := 10);

-- 5b. Search with exact reranking (highest recall, ~60ms)
SELECT * FROM pg_sorted_heap.svec_ann_search(
    'vectors', query_vec, nprobe := 10, lim := 10, rerank_topk := 200);

-- 5c. C-level scan (maximum throughput, same API)
SELECT * FROM pg_sorted_heap.svec_ann_scan(
    'vectors', query_vec, nprobe := 10, lim := 10, rerank_topk := 200);
```

### How IVF-PQ works

```
query vector
    │
    ├─ IVF probe: find nearest nprobe centroids (nprobe × L2 distance)
    │     → partition_id IN (3, 17, 42, ...)
    │
    ├─ PQ ADC scan: for each candidate row, sum M precomputed distances
    │     → O(M) per row using 180-byte PQ code (no TOAST decompression)
    │
    ├─ Top-K: max-heap selects best candidates
    │
    └─ Optional rerank: exact cosine on top-200 → return top-10
```

Physical clustering by `(partition_id, id)` means IVF probe translates directly
to a contiguous block range scan — sorted_heap's zone map skips all other
partitions at the I/O level.

### Performance (103K vectors, 2880-dim)

| Method | Recall@10 | Avg latency | Index size |
|---|---|---|---|
| Exact brute-force | 10.0/10 | 996 ms | — |
| pgvector HNSW ef=100 | 9.7/10 | 14 ms | 806 MB |
| PQ two-phase pool=200 | 10.0/10 | 60 ms | 27 MB |

PQ timing breakdown (single query):

| Phase | Time |
|---|---|
| Distance table precompute | 15.6 ms |
| ADC scan (103K codes) | 25.5 ms |
| Exact rerank (200 candidates) | 22.8 ms |
| **Total** | **63.9 ms** |

## SQL API

### Compaction

```sql
-- Offline compact: full CLUSTER rewrite (AccessExclusiveLock)
SELECT pg_sorted_heap.sorted_heap_compact('t'::regclass);

-- Online compact: trigger-based, non-blocking (ShareUpdateExclusiveLock,
-- brief AccessExclusiveLock for final swap)
CALL pg_sorted_heap.sorted_heap_compact_online('t'::regclass);

-- Offline merge: two-way merge of sorted prefix + unsorted tail
SELECT pg_sorted_heap.sorted_heap_merge('t'::regclass);

-- Online merge: non-blocking variant
CALL pg_sorted_heap.sorted_heap_merge_online('t'::regclass);
```

### Zone map inspection

```sql
-- Human-readable zone map stats (flags, entry count, ranges)
SELECT pg_sorted_heap.sorted_heap_zonemap_stats('t'::regclass);

-- Manual zone map rebuild (without compaction)
SELECT pg_sorted_heap.sorted_heap_rebuild_zonemap('t'::regclass);
```

### Scan statistics

```sql
-- Structured stats: total_scans, blocks_scanned, blocks_pruned, source
SELECT * FROM pg_sorted_heap.sorted_heap_scan_stats();

-- Reset counters
SELECT pg_sorted_heap.sorted_heap_reset_stats();
```

### Configuration

```sql
-- Disable scan pruning (default: on)
SET sorted_heap.enable_scan_pruning = off;

-- Disable autovacuum zone map rebuild (default: on)
SET sorted_heap.vacuum_rebuild_zonemap = off;
```

### Observability

```sql
SELECT pg_sorted_heap.version();
SELECT pg_sorted_heap.observability();
```

### clustered_heap (Table AM) — legacy

The extension also provides `clustered_heap`, a directed-placement Table AM
that routes INSERTs to the block where the same key already lives. Requires
a companion `clustered_pk_index` for key discovery and a standard btree for
query serving.

```sql
CREATE TABLE t (...) USING clustered_heap;
CREATE INDEX ON t USING clustered_pk_index (key_col);
CREATE INDEX ON t USING btree (key_col);
```

## Architecture

### Source files

| File | Lines | Purpose |
|------|------:|---------|
| `sorted_heap.h` | 183 | Meta page layout, zone map structs (v6), SortedHeapRelInfo |
| `sorted_heap.c` | 2,452 | Table AM: sorted multi_insert, zone map persistence, compact, merge, vacuum |
| `sorted_heap_scan.c` | 1,547 | Custom scan provider: planner hook, parallel scan, multi-col pruning, runtime params |
| `sorted_heap_online.c` | 1,053 | Online compact + online merge: trigger, copy, replay, swap |
| `pg_sorted_heap.c` | 1,537 | Extension entry point, legacy clustered index AM, GUC registration |
| `svec.h` / `svec.c` | 35 + 280 | svec vector type: I/O, typmod, cosine distance `<=>` |
| `sorted_vector_hash.c` | 590 | Vector hash functions: SimHash, VQ, RVQ, RPVQ, CVQ |
| `pq.h` / `pq.c` | 55 + 2,568 | Product Quantization, IVF, ANN scan (training, encode, ADC, top-K) |

### Zone map details

- **v6 format**: 32-byte entries with col1 + col2 min/max per page
- Meta page (block 0): 250 entries in special space
- Overflow pages: 254 entries/page, linked via `shmo_next_block` chain
- No capacity limit — overflow chain extends as needed
- Updated atomically via GenericXLog during `multi_insert`
- Validity flag (`SHM_FLAG_ZONEMAP_VALID`): set by compact/rebuild, cleared
  on first single-row INSERT into uncovered page
- Autovacuum rebuilds zone map when flag is not set

### Custom scan provider

- Hooks into `set_rel_pathlist_hook`
- Extracts PK bounds from `baserestrictinfo` (both `Const` and `Param` nodes)
- Maps operator OIDs to btree strategies via `get_op_opfamily_strategy()`
- Computes contiguous block range from zone map overlap
- Uses `heap_setscanlimits(start, nblocks)` for physical I/O skip
- Per-block zone map check in `ExecCustomScan` for fine-grained pruning
- Parallel-aware: `add_partial_path` + Gather for multi-worker scans
- Prepared statements: runtime parameter resolution via `ExecEvalExprSwitchContext`
- EXPLAIN shows: `Zone Map: N of M blocks (pruned P)`

## Limitations

- Zone map tracks first two PK columns. Supported types: int2, int4, int8,
  timestamp, timestamptz, date, uuid, text/varchar (`COLLATE "C"` required
  for text). UUID/text use lossy first-8-byte mapping.
- Online compact/merge not supported for UUID/text/varchar PKs (lossy int64
  hash causes collisions in replay). Use offline variants.
- Single-row INSERT into a covered page updates zone map in-place. INSERT
  into an uncovered page invalidates scan pruning until next compact (or
  autovacuum rebuild).
- `sorted_heap_compact()` and `sorted_heap_merge()` acquire
  AccessExclusiveLock. Use `_online` variants for non-blocking operation.
- `heap_setscanlimits()` only supports contiguous block ranges. Non-contiguous
  pruning handled per-block in ExecCustomScan.
- UPDATE does not re-sort; use compact/merge periodically for write-heavy
  workloads.
- pg_dump/restore: data restored via COPY, zone map needs compact after
  restore to re-enable scan pruning.
- pg_upgrade 17 to 18: tested and verified (13 checks). Data files including
  zone map are copied as-is.

## License

Released under the [PostgreSQL License](LICENSE).
