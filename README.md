# pg_sorted_heap

PostgreSQL extension providing physically sorted storage and built-in vector
search — no pgvector dependency.

**Sorted storage:** The `sorted_heap` Table AM sorts bulk inserts by primary
key, maintains per-page zone maps, and uses a custom scan provider to skip
irrelevant blocks at query time. At 100M rows, a point query reads 1 buffer
(vs 8 for btree, 519K for seq scan).

**Vector search:** Two built-in vector types — `svec` (float32, up to 16K
dims) and `hsvec` (float16, up to 32K dims) — with cosine distance and IVF-PQ
approximate nearest neighbor search. The sorted storage itself acts as the IVF
index: 30x smaller than HNSW, no 2,000-dim pgvector limit, 97-99% recall with
reranking.

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
   zone map checks for fine-grained filtering. Supports literal constants,
   parameterized queries (prepared statements), `IN`/`ANY(array)` with per-block
   value filtering, and `LATERAL`/NestLoop runtime parameters.

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
make installcheck              # regression tests
make test-crash-recovery       # crash recovery (4 scenarios)
make test-concurrent           # concurrent DML + online ops
make test-toast                # TOAST integrity + concurrent guard
make test-alter-table          # ALTER TABLE DDL (36 checks)
make test-dump-restore         # pg_dump/restore lifecycle (10 checks)
make test-graph-builder        # graph sidecar bootstrap + rebuild smoke
make test-pg-upgrade           # pg_upgrade 17->18 (13 checks)
make policy-safety-selftest    # policy + doc contract checks
make pg-core-regression-smoke  # PG core regression smoke test
make selftest-lightweight      # lightweight selftest suite
```

Command selection quick map: see [OPERATIONS.md](OPERATIONS.md) for the full
list of available make targets and their descriptions.

## Vector search

pg_sorted_heap includes two built-in vector types and IVF-PQ approximate
nearest neighbor search. The key insight: sorted_heap's physical clustering by
primary key prefix **is** the inverted file index — no separate index structure,
no pgvector dependency, no 800 MB HNSW graph.

| Type | Precision | Bytes/dim | Max dimensions |
|------|-----------|-----------|----------------|
| `svec` | float32 | 4 | 16,000 |
| `hsvec` | float16 | 2 | 32,000 |

### Why not pgvector?

| | pg_sorted_heap IVF-PQ | pgvector HNSW |
|---|---|---|
| Index size (103K × 2880-dim) | **27 MB** (PQ codes) | 806 MB (full vectors) |
| R@1 (cross-query) | 79–99% (tunable) | 97% |
| Latency (avg, 103K) | **8 ms** PQ-only / 22 ms rerank | 14 ms |
| Max indexed dimensions | **16,000** (svec) / **32,000** (hsvec) | 2,000 (`vector`) / 4,000 (`halfvec`) |
| Separate index needed? | No — PK prefix is the IVF | Yes — HNSW graph |
| External dependency | None | pgvector extension |
| Scales to 1M vectors | ~260 MB | ~8 GB |

IVF-PQ trades recall for 30x smaller storage. At nprobe=10 with reranking,
R@1 reaches 97–99% at 22 ms. For self-query workloads (searching your own
corpus), R@1 is 100% at nprobe=3 / 8 ms. See [FAQ](#faq) for tuning.

**High-dimensional advantage:** pgvector's dense `vector` type is limited to
2,000 dimensions, while `halfvec` extends that to 4,000. svec stores full
float32 up to 16,000 dims; hsvec stores float16 up to 32,000 dims. IVF-PQ has
a much larger native dimensional envelope for either type.

### svec and hsvec types

```sql
-- svec: float32, up to 16,000 dimensions
CREATE TABLE items (
    id    text PRIMARY KEY,
    embedding svec(768)
) USING sorted_heap;

-- hsvec: float16, up to 32,000 dimensions (half the storage)
CREATE TABLE items_compact (
    id    text PRIMARY KEY,
    embedding hsvec(768)
) USING sorted_heap;

-- Insert with bracket notation (same for both types)
INSERT INTO items VALUES ('doc1', '[0.1, 0.2, 0.3, ...]');

-- Cosine distance operator <=> works on both types
SELECT a.id, a.embedding <=> b.embedding AS distance
FROM items a, items b
WHERE a.id = 'doc1' AND b.id = 'doc2';

-- hsvec casts to svec implicitly — all PQ/IVF functions accept both
SELECT svec_cosine_distance('[1,0,0]'::hsvec, '[0,1,0]'::hsvec);
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
--    (requires CREATE privilege on the extension schema; see docs/vector-search.md)
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

-- 5b. Search with exact reranking (higher recall)
SELECT * FROM pg_sorted_heap.svec_ann_search(
    'vectors', query_vec, nprobe := 10, lim := 10, rerank_topk := 200);

-- 5c. C-level scan (maximum throughput, same API)
SELECT * FROM pg_sorted_heap.svec_ann_scan(
    'vectors', query_vec, nprobe := 10, lim := 10, rerank_topk := 200);

-- 5d. Residual PQ (higher recall, set ivf_cb_id to IVF codebook)
SELECT * FROM pg_sorted_heap.svec_ann_scan(
    'vectors', query_vec, nprobe := 10, lim := 10,
    rerank_topk := 50, cb_id := 2, ivf_cb_id := 1);
```

### How IVF-PQ works

```
query vector
    │
    ├─ IVF probe: find nearest nprobe centroids (nprobe × L2 distance)
    │     → partition_id IN (3, 17, 42, ...)
    │
    ├─ PQ ADC scan: for each candidate row, sum M precomputed distances
    │     → O(M) per row using M-byte PQ code (no TOAST decompression)
    │
    ├─ Top-K: max-heap selects best candidates
    │
    └─ Optional rerank: exact cosine on top-200 → return top-10
```

Physical clustering by `(partition_id, id)` means IVF probe translates directly
to a small set of physical block ranges — sorted_heap's zone map skips all
other partitions at the I/O level.

### Performance

#### 103K vectors, 2880-dim (Gutenberg corpus, 1 Gi k8s pod)

Residual IVF-PQ via `svec_ann_scan`, M=720, 256 IVF partitions.
100 cross-queries (query ≠ result, self-match excluded):

| Config | R@1 | Recall@10 | Avg latency |
|---|---|---|---|
| nprobe=1, PQ-only | 54% | 48% | 5.5 ms |
| nprobe=3, PQ-only | 79% | 71% | 8 ms |
| nprobe=3, rerank=96 | 82% | 74% | 10 ms |
| nprobe=5, rerank=96 | 89% | 86% | 12 ms |
| nprobe=10, rerank=200 | 97% | 94% | 22 ms |

**Self-query** (searching your own corpus): R@1 = **100%** at nprobe=3 / 8 ms.
This is the common RAG use case — you embedded the documents, now you search them.

#### 10K vectors, 2880-dim (float32 precision test)

Same corpus, pure svec (float32), nlist=64, M=720 residual PQ:

| Config | R@1 | Recall@10 |
|---|---|---|
| nprobe=1, PQ-only | 56% | 56% |
| nprobe=3, PQ-only | 72% | 82% |
| nprobe=5, rerank=96 | 93% | 93% |
| nprobe=10, rerank=200 | **99%** | **99%** |

Tested float32 (svec) vs float16-degraded (svec → hsvec → svec roundtrip) on
the same 10K vectors: **no measurable recall difference** — float16 precision loss
(~1e-7) is 1000× smaller than typical distance gaps between neighbors (~1e-4).
This confirms hsvec is a safe storage choice for ANN workloads.

#### Comparison with pgvector HNSW

| Method | R@1 | Avg latency | Index size |
|---|---|---|---|
| Exact brute-force | 100% | 996 ms | — |
| pgvector HNSW ef=100 | 97% | 14 ms | 806 MB |
| IVF-PQ nprobe=10, rerank=200 | 97–99% | 22 ms | 27 MB |

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
| `sorted_heap.h` | 187 | Meta page layout, zone map structs (v6), SortedHeapRelInfo |
| `sorted_heap.c` | 2,536 | Table AM: sorted multi_insert, zone map persistence, compact, merge, vacuum |
| `sorted_heap_scan.c` | 2,016 | Custom scan provider: planner hook, parallel scan, multi-col pruning, IN/ANY, runtime params |
| `sorted_heap_online.c` | 1,171 | Online compact + online merge: trigger, copy, replay, swap |
| `pg_sorted_heap.c` | 1,537 | Extension entry point, legacy clustered index AM, GUC registration |
| `svec.h` / `svec.c` | 38 + 301 | svec vector type (float32): I/O, typmod, cosine distance `<=>` |
| `hsvec.h` / `hsvec.c` | 165 + 358 | hsvec vector type (float16): I/O, cosine distance, casts to/from svec |
| `sorted_vector_hash.c` | 882 | Vector hash functions: SimHash, VQ, RVQ, RPVQ, CVQ |
| `pq.h` / `pq.c` | 57 + 2,714 | Product Quantization, IVF, ANN scan (training, encode, ADC, top-K) |

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
- Computes one or more block ranges from zone map overlap
- Uses `heap_setscanlimits(start, nblocks)` for each internal scan range
- Per-block zone map check in `ExecCustomScan` for fine-grained pruning
- `IN`/`ANY(array)` pruning: per-block binary search against sorted value list
- LATERAL/NestLoop: deferred PARAM_EXEC resolution at first rescan/execution
- Mid-scan staleness detection: atomic zone map generation counter per block
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
  into an uncovered page keeps the zone map valid, clears the global sorted
  flag, and falls back to sorted-prefix + conservative-tail pruning until the
  next compact/merge.
- `sorted_heap_compact()` and `sorted_heap_merge()` acquire
  AccessExclusiveLock. Use `_online` variants for non-blocking operation.
- `heap_setscanlimits()` only supports contiguous block ranges. Serial
  SortedHeapScan works around this by iterating an internal range array;
  parallel scan still only supports contiguous/narrow ranges and falls back
  to serial for disjoint sparse predicates.
- UPDATE does not re-sort; use compact/merge periodically for write-heavy
  workloads.
- pg_dump/restore: data restored via COPY, zone map needs compact after
  restore to re-enable scan pruning.
- pg_upgrade 17 to 18: tested and verified (13 checks). Data files including
  zone map are copied as-is.

## FAQ

### What do R@1, Hit@10, and Recall@10 mean?

- **R@1** — Is the ground-truth closest vector also the ANN's #1 result?
- **Hit@10** — Is the ground-truth closest vector anywhere in the ANN's top 10?
- **Recall@10** — What fraction of the ground-truth top 10 appear in the ANN's top 10?

All benchmarks above use **cross-query** evaluation: the query vector is excluded
from results, so self-match doesn't inflate the numbers. **Self-query** means the
vector you're searching for is in the dataset (the typical RAG case) — here R@1 is
100% because the query is trivially its own closest neighbor.

### How do I choose nprobe and rerank_topk?

| Use case | nprobe | rerank | Latency | R@1 |
|---|---|---|---|---|
| Lowest latency | 1 | 0 | 5.5 ms | 54% |
| Self-query RAG | 3 | 0 | 8 ms | 100% (self) |
| Balanced cross-query | 5 | 96 | 12 ms | 89–93% |
| Highest quality | 10 | 200 | 22 ms | 97–99% |

Start with nprobe=3 for self-query workloads. For cross-query (query not in
corpus), increase nprobe and add reranking until recall meets your threshold.

### Do I need pgvector?

No. pg_sorted_heap includes two vector types — `svec` (float32, up to 16K
dims) and `hsvec` (float16, up to 32K dims) — the `<=>` cosine distance
operator, and full IVF-PQ infrastructure. The PQ index is 30x smaller than
HNSW. pgvector is only needed if you want HNSW or IVFFlat index types.

pgvector's dense `vector` type is limited to 2,000 dimensions, while
`halfvec` extends that to 4,000. svec+IVF-PQ works natively at full precision
up to 16K dims, and hsvec extends that to 32K dims at half the storage cost.

### How do I reproduce these benchmarks?

```sql
-- 1. Create table with IVF partition + PQ code
CREATE TABLE bench (
    id           text,
    partition_id int2 GENERATED ALWAYS AS (
                     pg_sorted_heap.svec_ivf_assign(embedding, 1)) STORED,
    embedding    pg_sorted_heap.svec(768),
    pq_code      bytea GENERATED ALWAYS AS (
                     pg_sorted_heap.svec_pq_encode_residual(
                         embedding,
                         pg_sorted_heap.svec_ivf_assign(embedding, 1),
                         2, 1)) STORED,
    PRIMARY KEY (partition_id, id)
) USING sorted_heap;

-- 2. Load vectors
INSERT INTO bench (id, embedding)
SELECT id, embedding FROM your_source_table;
SELECT pg_sorted_heap.sorted_heap_compact('bench');

-- 3. Train IVF + residual PQ (one-time, requires CREATE on extension schema)
SELECT * FROM pg_sorted_heap.svec_ann_train(
    'SELECT embedding FROM bench',
    nlist := 64, m := 192);
-- Compact again after training to re-cluster by new partition_id
SELECT pg_sorted_heap.sorted_heap_compact('bench');

-- 4. Measure recall (cross-query, excluding self-match)
WITH queries AS (
    SELECT id AS qid, embedding AS qvec
    FROM bench ORDER BY random() LIMIT 100
),
ground_truth AS (
    SELECT q.qid,
           array_agg(t.id ORDER BY t.embedding <=> q.qvec) AS gt
    FROM queries q
    CROSS JOIN LATERAL (
        SELECT id, embedding FROM bench
        WHERE id != q.qid
        ORDER BY embedding <=> q.qvec LIMIT 10
    ) t GROUP BY q.qid
),
ann_results AS (
    SELECT q.qid,
           (array_agg(a.id ORDER BY a.distance))[2:11] AS ann
    FROM queries q
    CROSS JOIN LATERAL pg_sorted_heap.svec_ann_scan(
        'bench', q.qvec, nprobe := 3, lim := 11,
        cb_id := 2, ivf_cb_id := 1) a
    GROUP BY q.qid
)
SELECT
    round((avg(CASE WHEN gt.gt[1] = ar.ann[1]
               THEN 1.0 ELSE 0.0 END) * 100)::numeric, 1) AS "R@1",
    round((avg((SELECT count(*)::numeric
               FROM unnest(ar.ann) x
               WHERE x = ANY(gt.gt)) / 10.0) * 100)::numeric, 1) AS "Recall@10"
FROM ground_truth gt
JOIN ann_results ar ON gt.qid = ar.qid;
```

## License

Released under the [PostgreSQL License](LICENSE).
