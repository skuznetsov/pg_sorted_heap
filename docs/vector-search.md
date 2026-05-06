---
layout: default
title: Vector Search
nav_order: 3
---

# Vector Search

pg_sorted_heap includes two built-in vector types, a planner-integrated
`sorted_hnsw` Index AM, and legacy/manual ANN paths (`svec_ann_scan`,
`svec_hnsw_scan`). The default vector story is now `CREATE INDEX ... USING
sorted_hnsw`; the older IVF-PQ and sidecar HNSW APIs remain available when you
want explicit control over storage or rerank behavior.

Release guidance:

- **Stable:** `sorted_hnsw` on `svec` and `hsvec`
- **Legacy/manual:** `svec_ann_scan`, `svec_ann_search`, `svec_hnsw_scan`

---

## Vector types

| Type | Precision | Bytes/dim | Max dimensions | Use case |
|------|-----------|-----------|----------------|----------|
| `svec` | float32 | 4 | 16,000 | Full precision, training codebooks |
| `hsvec` | float16 | 2 | 32,000 | Storage-optimized, large embeddings |

Both types support the `<=>` cosine distance operator (returns 1 − cosine
similarity, range [0, 2]). Distance is accumulated in `float8` (64-bit) for
precision.

`hsvec` casts to `svec` implicitly — all PQ/IVF functions accept both types
without code changes. `svec` casts to `hsvec` via explicit or assignment cast
(lossy, float32 → float16).

**Dimension envelope:** pgvector's dense `vector` type is limited to 2,000
dimensions, while `halfvec` extends that to 4,000. `svec` supports up to 16K
dims and `hsvec` up to 32K dims, so pg_sorted_heap still has a larger native
ANN/storage envelope for very high-dimensional embeddings.

```sql
-- svec: float32, up to 16,000 dimensions
CREATE TABLE items (
    id        text PRIMARY KEY,
    embedding pg_sorted_heap.svec(768)
);

-- hsvec: float16, up to 32,000 dimensions
CREATE TABLE items_compact (
    id        text PRIMARY KEY,
    embedding pg_sorted_heap.hsvec(768)
);

-- Insert with bracket notation (same for both types)
INSERT INTO items VALUES ('doc1', '[0.1, 0.2, 0.3, ...]');

-- Cosine distance operator works on both types
SELECT a.id, a.embedding <=> b.embedding AS distance
FROM items a, items b
WHERE a.id = 'doc1' AND b.id = 'doc2';

-- hsvec casts to svec implicitly for PQ/IVF functions
SELECT svec_cosine_distance('[1,0,0]'::hsvec, '[0,1,0]'::hsvec);
```

---

## Current default: `sorted_hnsw`

For new deployments, prefer the Index AM. It supports both `svec` and `hsvec`
source columns; use `hsvec` when you want the heap/TOAST footprint to stay
close to pgvector `halfvec`, with float32 used only as internal scratch during
build/search/rerank.

```sql
CREATE TABLE items (
    id        bigserial PRIMARY KEY,
    embedding pg_sorted_heap.svec(384),
    body      text
);

CREATE INDEX items_embedding_idx
ON items USING sorted_hnsw (embedding)
WITH (m = 16, ef_construction = 200);

SET sorted_hnsw.shared_cache = on;
SET sorted_hnsw.ef_search = 96;

SELECT id, body
FROM items
ORDER BY embedding <=> '[0.1,0.2,0.3,...]'::pg_sorted_heap.svec
LIMIT 10;
```

On constrained builders, the current low-memory build knob is:

```sql
SET sorted_hnsw.build_sq8 = on;
```

That makes `CREATE INDEX ... USING sorted_hnsw` build the graph from
SQ8-compressed build vectors instead of a full float32 build slab. The tradeoff
is one extra heap scan during build and a possible graph-quality loss on some
corpora, but on the current local `1M x 64D` multidepth GraphRAG point it
preserved quality and slightly improved build time.

Compact-storage variant:

```sql
CREATE TABLE items_compact (
    id        bigserial PRIMARY KEY,
    embedding pg_sorted_heap.hsvec(384),
    body      text
);

CREATE INDEX items_compact_embedding_idx
ON items_compact USING sorted_hnsw (embedding hsvec_cosine_ops)
WITH (m = 16, ef_construction = 200);
```

This path is planner-integrated and exact-reranks internally. There is no
sidecar prefix argument and no manual `rerank_topk` in the index-scan path.

Current ordered-scan contract:

- automatic `sorted_hnsw` planning is for base-relation
  `ORDER BY embedding <=> query LIMIT k`
- the planner does not use the current Phase 1 path when there is no `LIMIT`,
  when `LIMIT > sorted_hnsw.ef_search`, or when extra base-table quals would
  make the index under-return candidates
- `sorted_hnsw.shared_cache` is most useful when
  `shared_preload_libraries = 'pg_sorted_heap'`; otherwise scans fall back to
  backend-local cache builds

For filtered retrieval or expansion workflows, materialize/filter first or use
the GraphRAG helper API instead of expecting the ordered index scan to serve as
a general filtered ANN primitive. The future filtered-ANN contract is tracked
in [Filtered ANN Contracts](spec-filtered-ann).

For declarative partitioned tables, `sorted_hnsw_partition_search(...)` provides
an explicit route-first helper. Selected leaves run local `sorted_hnsw` scans,
their candidate pools are unioned, and the final result is globally reranked by
exact distance. Use this when tenant/time/segment routing maps to whole
partitions instead of executor filters inside one leaf.

Benchmark the route-first contract with:

```bash
make bench-partitioned-sorted-hnsw
```

On the local PostgreSQL 18 default run (`8 x 50K` rows, self-query top-10),
selected-leaf routing measured `5.359 ms` average at `100.0%` recall@10 versus
`8.849 ms` for the parent filtered exact query. The same run showed all-leaf
fanout around `23-25 ms`. The script now also reports `direct_leaf_index`:
`2.942 ms` on the same run, which quantifies the current PL/pgSQL wrapper
overhead and is the main signal for a future C fanout helper.

---

## Legacy/manual IVF-PQ quick start

### 1. Create table

The table uses `partition_id` (IVF cluster assignment) as the leading PK column.
This makes sorted_heap cluster rows by IVF partition physically — the zone map
then skips irrelevant partitions at the I/O level.

```sql
CREATE TABLE vectors (
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
```

The `pq_code` column stores M-byte Product Quantization codes. Both columns are
generated automatically — you only INSERT `id` and `embedding`.

### 2. Train codebooks

> **Permissions:** Training creates internal metadata tables in the extension
> schema. The calling role needs `CREATE` privilege on that schema (or be the
> extension owner / superuser). For non-superuser roles:
> `GRANT CREATE ON SCHEMA <ext_schema> TO <role>;`

```sql
-- Train IVF centroids (nlist partitions) + PQ codebook (M subvectors)
SELECT * FROM pg_sorted_heap.svec_ann_train(
    'SELECT embedding FROM vectors',
    nlist := 64,     -- number of IVF partitions
    m     := 192     -- PQ subvectors (768/192 = 4-dim each)
);
-- Returns: ivf_cb_id=1, pq_cb_id=1

-- For higher recall, train residual PQ (trains on vec - centroid residuals)
SELECT pg_sorted_heap.svec_pq_train_residual(
    'SELECT embedding FROM vectors',
    m := 192, ivf_cb_id := 1);
-- Returns: pq_cb_id=2
```

After training, compact the table so rows re-cluster by their new `partition_id`:

```sql
SELECT pg_sorted_heap.sorted_heap_compact('vectors');
```

### 3. Search

```sql
-- PQ-only (fastest): ~8 ms, R@1 79% cross-query / 100% self-query
SELECT * FROM pg_sorted_heap.svec_ann_scan(
    'vectors', query_vec,
    nprobe := 3, lim := 10,
    cb_id := 2, ivf_cb_id := 1);

-- With reranking (higher recall): ~22 ms, R@1 97%
SELECT * FROM pg_sorted_heap.svec_ann_scan(
    'vectors', query_vec,
    nprobe := 10, lim := 10, rerank_topk := 200,
    cb_id := 2, ivf_cb_id := 1);
```

---

## How IVF-PQ works

```
query vector
    │
    ├─ IVF probe: find nearest nprobe centroids
    │     → partition_id IN (3, 17, 42, ...)
    │
    ├─ PQ ADC: for each candidate row, sum M precomputed distances
    │     → O(M) per row using M-byte PQ code (no TOAST decompression)
    │
    ├─ Top-K: max-heap selects best candidates
    │
    └─ Optional rerank: exact cosine on top candidates → return top-K
```

Physical clustering by `(partition_id, id)` means the IVF probe translates
directly to a small set of physical block ranges — sorted_heap's zone map
skips all other partitions at the I/O level.

### Residual PQ

Standard PQ encodes vectors directly. **Residual PQ** encodes the residual
`(vector − IVF centroid)` instead. This removes inter-centroid variance so PQ
focuses on fine intra-cluster distinctions, improving recall at no storage cost.

The trade-off: residual PQ requires computing a separate distance table per
probed centroid (vs one global table for standard PQ), roughly doubling the
PQ-only latency. With reranking, the difference is negligible.

Use residual PQ by passing `ivf_cb_id` to `svec_ann_scan`:

```sql
-- cb_id=2 is the residual PQ codebook, ivf_cb_id=1 is the IVF codebook
SELECT * FROM pg_sorted_heap.svec_ann_scan(
    'vectors', query_vec,
    nprobe := 3, lim := 10,
    cb_id := 2, ivf_cb_id := 1);
```

---

## Tuning guide

### nprobe and rerank_topk

| Use case | nprobe | rerank | Latency | R@1 | Recall@10 |
|---|---|---|---|---|---|
| Lowest latency | 1 | 0 | 5.5 ms | 54% | 48% |
| Self-query RAG | 3 | 0 | 8 ms | 100%* | 71% |
| Balanced | 5 | 96 | 12 ms | 89–93% | 86–93% |
| High quality | 10 | 200 | 22 ms | 97–99% | 94–99% |

\* Self-query R@1 = 100% because the query vector is in the dataset.
Cross-query R@1 at nprobe=3 is 79%. The recall ranges reflect different
dataset sizes (10K–103K vectors).

**Guidelines:**
- Start with **nprobe=3, no rerank** for RAG workloads (searching your own corpus)
- Add **rerank=96** if you need cross-query accuracy (query not in corpus)
- Increase **nprobe** for higher recall at the cost of latency
- **nprobe × (rows/nlist)** = approximate number of PQ codes scanned per query

### nlist and M

| Parameter | Effect |
|---|---|
| nlist (IVF partitions) | More partitions = more precise routing but smaller clusters. 64–256 typical. |
| M (PQ subvectors) | More subvectors = higher PQ fidelity. dim/M = subvector dimension (4–16 typical). |

Rule of thumb: `nlist ≈ sqrt(N)` where N is dataset size. `M = dim/4` gives
4-dimensional subvectors — a good balance of fidelity and code size.

---

## Benchmarks

The tables below mix the current stable Index AM path with the older
legacy/manual ANN paths. Use the `sorted_hnsw` rows as the release default;
use the IVF-PQ and sidecar rows only when you explicitly want those manual
trade-offs.

For future very-large tables, IVF-PQ / SQ-family revival is tracked as a
benchmark-gated scale feature, not as a default replacement. See
[Large-Vector Sublinear Search](spec-large-vector-sublinear).

SIMD ADC optimization and pgvectorscale StreamingDiskANN comparison are tracked
separately in [SIMD ADC And DiskANN Comparison](spec-simd-adc-diskann).

### 103K vectors, 2880-dim (Gutenberg corpus)

Residual PQ (M=720, dsub=4), 256 IVF partitions.
1 Gi k8s pod, PostgreSQL 18. 100 cross-queries (self-match excluded):

| Config | R@1 | Recall@10 | Avg latency |
|---|---|---|---|
| nprobe=1, PQ-only | 54% | 48% | 5.5 ms |
| nprobe=3, PQ-only | 79% | 71% | 8 ms |
| nprobe=3, rerank=96 | 82% | 74% | 10 ms |
| nprobe=5, rerank=96 | 89% | 86% | 12 ms |
| nprobe=10, rerank=200 | 97% | 94% | 22 ms |

### 10K vectors, 2880-dim (float32 precision test)

Same corpus, pure svec (float32), nlist=64, M=720 residual PQ:

| Config | R@1 | Recall@10 |
|---|---|---|
| nprobe=1, PQ-only | 56% | 56% |
| nprobe=3, PQ-only | 72% | 82% |
| nprobe=5, rerank=96 | 93% | 93% |
| nprobe=10, rerank=200 | **99%** | **99%** |

### float32 vs float16 precision

Tested the same 10K Gutenberg vectors stored as float32 (svec) vs
float16-degraded (svec → hsvec → svec roundtrip). Both trained independently
with identical parameters. **No measurable recall difference** — float16
precision loss (~1e-7) is 1000× smaller than typical distance gaps between
neighbors (~1e-4). Precision is not the recall bottleneck; PQ quantization
and IVF routing are. This confirms hsvec is a safe storage choice for ANN.

### Comparison with other vector search engines

Current repo-owned harnesses:

- `python3 scripts/bench_gutenberg_local_dump.py --dump /tmp/cogniformerus_backup/cogniformerus_backup.dump --port 65473`
- `REMOTE_PYTHON=/path/to/python SH_EF=32 EXTRA_ARGS='--sh-ef-construction 200' ./scripts/bench_gutenberg_aws.sh <aws-host> /path/to/repo /path/to/dump 65485`
- `scripts/bench_sorted_hnsw_vs_pgvector.sh /tmp 65485 10000 20 384 10 vector 64 96`
- `make bench-partitioned-sorted-hnsw`
- `python3 scripts/bench_ann_real_dataset.py --dataset nytimes-256 --sample-size 10000 --queries 20 --k 10 --pgv-ef 64 --sh-ef 96 --zvec-ef 64 --qdrant-ef 64`
- `python3 scripts/bench_qdrant_synthetic.py --rows 10000 --queries 20 --dim 384 --k 10 --ef 64`
- `python3 scripts/bench_zvec_synthetic.py --rows 10000 --queries 20 --dim 384 --k 10 --ef 64`

AWS restored Gutenberg dump (`~104K x 2880D`, top-10, exact heap GT on the
restored `svec` table). Host: AWS ARM64, 4 vCPU,
8 GiB RAM. In the current rerun the stored `bench_hnsw_gt` table matched the
recomputed exact GT on 100% of the 50 benchmark queries after restore, so the
fresh exact heap GT and the historical GT table agree. This rerun uses
`sorted_hnsw` `ef_construction=200` and `ef_search=32`, and the benchmark
harness reconnects after build before timing ordered scans.

| Method | p50 latency | Recall@10 | Notes |
|--------|:-----------:|:---------:|-------|
| Exact heap (`svec`) | 458.762 ms | 100.0% | brute-force GT on restored corpus |
| **`sorted_hnsw` (`svec`)** | **1.287 ms** | **100.0%** | `ef_construction=200`, `ef_search=32`, index 404 MB, total 1902 MB |
| `sorted_hnsw` (`hsvec`) | 1.404 ms | 100.0% | `ef_construction=200`, `ef_search=32`, index 404 MB, total 1032 MB |
| pgvector HNSW (`halfvec`) | 2.031 ms | 99.8% | `ef_search=64`, index 804 MB, total 1615 MB |
| zvec HNSW | 50.499 ms | 100.0% | in-process collection, `ef=64`, ~1.12 GiB on disk |
| Qdrant HNSW | 6.028 ms | 99.2% | local Docker on same AWS host, `hnsw_ef=64`, 103,260 points |

The precision-matched PostgreSQL row on Gutenberg is `sorted_hnsw (hsvec)`
vs pgvector `halfvec`: `1.404 ms @ 100.0%` versus `2.031 ms @ 99.8%`, with
total footprint `1032 MB` versus `1615 MB`. The raw fastest PostgreSQL row on
this corpus is still `sorted_hnsw (svec)` at `1.287 ms`, but that uses
float32 source storage. The `sorted_hnsw` index stays 404 MB in both cases
because it stores SQ8 graph state; the size win from `hsvec` appears in the
source table and TOAST footprint (`1902 MB -> 1032 MB`), not in the index.

Synthetic 10K x 384D cosine corpus, top-10, warm query loop. PostgreSQL
methods were rerun across 3 fresh builds and the table below reports median
`p50` / median recall. Qdrant uses 3 warm measurement passes on one local
Docker collection.

| Method | p50 latency | Recall@10 | Notes |
|--------|:-----------:|:---------:|-------|
| Exact heap (`svec`) | 2.03 ms | 100% | Brute-force ground truth |
| **sorted_hnsw** | **0.158 ms** | **100%** | `shared_cache=on`, `ef_search=96`, index ~5.4 MB |
| pgvector HNSW (`vector`) | 0.446 ms | 90% median (90-95 range) | `ef_search=64`, same `M=16`, `ef_construction=64`, index ~2.0 MB |
| zvec HNSW | 0.611 ms | 100% | local in-process collection, `ef=64` |
| Qdrant HNSW | 1.94 ms | 100% | local Docker, `hnsw_ef=64` |

Real-dataset sample (`nytimes-256-angular`, sampled 10K x 256D, top-10). The
table below reports medians across 3 full harness runs. Ground truth is exact
heap search inside PostgreSQL on the sampled corpus.

| Method | p50 latency | Recall@10 | Notes |
|--------|:-----------:|:---------:|-------|
| Exact heap (`svec`) | 1.557 ms | 100% | ground truth |
| **sorted_hnsw** | **0.327 ms** | **85.0% median** (83.5-85.5 range) | `shared_cache=on`, `ef_search=96`, index ~4.1 MB |
| pgvector HNSW (`vector`) | 0.751 ms | 79.0% median (78.5-79.0 range) | `ef_search=64`, same `M=16`, `ef_construction=64`, index ~13 MB |
| zvec HNSW | 0.403 ms | 99.5% | local in-process collection, `ef=64`, ~14.1 MB on disk |
| Qdrant HNSW | 1.704 ms | 99.5% | local Docker, `hnsw_ef=64` |

This dataset is far harsher than the deterministic synthetic corpus. Use the
synthetic table for controlled regression tracking and the `nytimes-256`
sample when you want a better read on fixed-parameter recall.

See [Sidecar HNSW search](#sidecar-hnsw-search-legacy-svec_hnsw_scan) below
for the legacy/manual `svec_hnsw_scan` path and its cache-mode tradeoffs.

### Self-query vs cross-query

**Self-query**: the query vector exists in the dataset. This is the common RAG
case — you embedded documents, now you search them. R@1 is 100% because the
query is trivially its own closest neighbor.

**Cross-query**: the query vector is NOT in the dataset (e.g., a user's question
embedded at search time). R@1 depends on nprobe and PQ fidelity.

### Reproducible benchmark

```sql
-- Pick 100 random queries, compute ground truth and ANN recall
WITH queries AS (
    SELECT id AS qid, embedding AS qvec
    FROM your_table ORDER BY random() LIMIT 100
),
ground_truth AS (
    SELECT q.qid,
           array_agg(t.id ORDER BY t.embedding <=> q.qvec) AS gt
    FROM queries q
    CROSS JOIN LATERAL (
        SELECT id, embedding FROM your_table
        WHERE id != q.qid
        ORDER BY embedding <=> q.qvec LIMIT 10
    ) t GROUP BY q.qid
),
ann_results AS (
    SELECT q.qid,
           (array_agg(a.id ORDER BY a.distance))[2:11] AS ann
    FROM queries q
    CROSS JOIN LATERAL pg_sorted_heap.svec_ann_scan(
        'your_table', q.qvec, nprobe := 3, lim := 11,
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

Note: `lim := 11` and `[2:11]` skip the self-match (position 1) for cross-query
evaluation. For self-query benchmarks, use `lim := 10` without slicing.

For the local synthetic `bench_nomic` setup used during graph/IVF tuning, use
`scripts/bench_nomic_local_ann.py` to reproduce exact ground truth,
`svec_graph_scan`, and `svec_ann_scan` latency/recall curves from one
command. The reproducible Make targets are:

```bash
make build-graph-bench-nomic \
  VECTOR_BENCH_DSN='host=/tmp port=65432 dbname=bench_nomic'

make bench-nomic-ann \
  VECTOR_BENCH_DSN='host=/tmp port=65432 dbname=bench_nomic'
```

Local graph/ANN tooling expects the Python packages listed in
`scripts/requirements-vector-tools.txt`. CI installs that file directly; for
local runs, `scripts/find_vector_python.sh` resolves a Python that can import
the same dependency set.

To rebuild the graph sidecar used by `svec_graph_scan`, use
`scripts/build_graph.py`. The committed workflow for the local `bench_nomic`
setup is:

```bash
"$(./scripts/find_vector_python.sh)" scripts/build_graph.py \
  --dsn 'host=/tmp port=65432 dbname=bench_nomic' \
  --table bench_nomic_8k \
  --graph-table bench_nomic_graph \
  --entry-table bench_nomic_graph_entries \
  --bootstrap \
  --sketch-dim 384 \
  --M 32 \
  --M-max 64 \
  --n-adjacent 4 \
  --no-prune \
  --seed 42
```

Then benchmark the rebuilt graph against exact and IVF baselines:

```bash
"$(./scripts/find_vector_python.sh)" scripts/bench_nomic_local_ann.py \
  --dsn 'host=/tmp port=65432 dbname=bench_nomic' \
  --graph-table bench_nomic_graph \
  --entry-table bench_nomic_graph_entries \
  --query-limit 20 \
  --graph-efs 128,256,512,1024 \
  --ivf-nprobes 40 \
  --warmup 1
```

Builder notes:

- `--bootstrap` reads directly from the main table and derives `src_tid` from
  `ctid`.
- rebuild mode rejoins on `src_tid`/`ctid`, not `id`, so it stays correct for
  `(partition_id, id)` primary keys where `id` is not globally unique.
- `M`, `M-max`, and `n-adjacent` change graph topology; re-run the harness
  after each build rather than carrying over numbers from an older graph.

**pg_dump / pg_restore limitation:** HNSW sidecar tables store `src_tid`
(physical heap tuple pointer) which changes after `pg_restore` because
COPY rewrites all heap pages with new TIDs. After restore, the sidecar's
`src_tid` values point to wrong or nonexistent heap tuples, causing
recall to silently drop (observed: 88% → 99.8% after rebuild on the same
data). **Always rebuild the HNSW sidecar after `pg_dump`/`pg_restore`.**
This limitation does **not** affect `sorted_hnsw`, which uses PostgreSQL's
normal index infrastructure instead of sidecar `src_tid` joins.

---

## Sidecar HNSW search (legacy: `svec_hnsw_scan`)

`svec_hnsw_scan` performs hierarchical HNSW search using compact sidecar
tables. The latency/recall tables in this section describe that legacy/manual
path, not the current `sorted_hnsw` Index AM baseline. The L0 column type
controls the recall/memory tradeoff:

| L0 column | Cache mode | Cache size (103K) | Recall@10 (ef=96) | p50 |
|-----------|------------|:------------------:|:-----------------:|:---:|
| `hsvec(384)` | float16 sketch | ~75 MB | 97% | 0.7ms |
| `svec(D)` | SQ8 quantized (default) | ~D/4 × N | 98.4% | 1.3ms |
| `svec(D)` + `sq8=off` | float32 full | ~D×4 × N | 99.6% | 1.5ms |

The cache auto-detects the L0 column type (svec vs hsvec) at build time.
For svec columns, SQ8 scalar quantization (uint8 per dimension) is applied
automatically for 4x memory savings, controlled by the
`sorted_heap.hnsw_cache_sq8` GUC (default on).

### Table requirements

```
{prefix}_meta   — entry_nid int4, max_level int2
{prefix}_l0     — nid int4 PK, sketch hsvec(N)|svec(D), neighbors int4[],
                  src_id text, src_tid tid
{prefix}_l1..lN — nid int4 PK, sketch hsvec(N), neighbors int4[]
```

Upper levels always use hsvec sketches. Only L0 supports svec for hybrid mode.

### Building the graph

```bash
# Sketch-only L0 (fastest, smallest cache, ~97% recall)
python scripts/build_hnsw_graph.py \
  --dsn 'host=... dbname=...' \
  --source-table my_graph_table \
  --prefix my_hnsw \
  --M 16 --ef-construction 200

# Hybrid L0: full vectors in L0, sketches in upper levels (~99%+ recall)
python scripts/build_hnsw_graph.py \
  --dsn 'host=... dbname=...' \
  --source-table my_graph_table \
  --prefix my_hnsw \
  --M 16 --ef-construction 200 \
  --full-vectors --main-table my_sorted_heap_table

# Truncated L0: first 768 dims only (for MRL/Matryoshka embeddings)
python scripts/build_hnsw_graph.py \
  --dsn 'host=... dbname=...' \
  --source-table my_graph_table \
  --prefix my_hnsw \
  --M 16 --ef-construction 200 \
  --full-vectors --main-table my_sorted_heap_table --l0-dim 768
```

### Calling the function

```sql
SELECT * FROM svec_hnsw_scan(
    tbl          := 'my_table'::regclass,
    query        := '[0.1, 0.2, ...]'::svec,
    prefix       := 'my_table_hnsw',
    ef_search    := 96,    -- beam width for L0 traversal
    lim          := 10,    -- results to return
    rerank_topk  := 48,    -- candidates to exact-rerank (see below)
    rerank1_topk := 0      -- dense r1 pre-filter (0 = disabled)
);
```

Enable the session-local cache for best latency (built once per session):

```sql
SET sorted_heap.hnsw_cache_l0 = on;

-- SQ8 quantization (default on, 4x memory saving for svec L0):
SET sorted_heap.hnsw_cache_sq8 = on;   -- default
-- To disable SQ8 for maximum recall without rerank:
SET sorted_heap.hnsw_cache_sq8 = off;
```

### `rerank_topk` semantics

`rerank_topk` controls how many L0 candidates are passed to exact svec cosine
rerank. Exact rerank always runs when the L0 table has a `src_tid` column
(which `build_hnsw_graph.py` always adds).

| `rerank_topk` value | Candidates reranked | Effect |
|---|---|---|
| `0` (default) | all `ef_search` | No truncation. Highest recall, `ef_search` TOAST reads. |
| `0 < rk < ef_search` | `rk` | Truncates before rerank. Fewer TOAST reads, lower recall. |
| `rk >= ef_search` | all `ef_search` | No effect (same as 0). |

**`rerank_topk=0` does NOT skip exact rerank.** It means "rerank all
candidates". To return results by sketch distance only (skipping TOAST reads
entirely), the L0 table must omit the `src_tid` column — this is not the
default build.

### Recommended operating points (103K × 2880-dim, k8s 2 Gi pod)

**hsvec(384) sketch L0:**

| Goal | ef_search | rerank_topk | p50 latency | Recall@10 |
|---|---|---|---|---|
| Balanced | 96 | 48 | 1.02ms | 96.8% |

**svec(D) hybrid L0 (SQ8 cache, default):**

| Goal | ef_search | lim | rerank_topk | p50 latency | Recall@10 |
|---|---|---|---|---|---|
| Fastest top-1 | 32 | 1 | 1 | 0.51ms | — |
| Fast top-5 | 64 | 5 | 5 | 0.87ms | 98.8% |
| Fast top-10 | 96 | 10 | 10 | 1.25ms | 98.6% |
| Balanced top-10 | 96 | 10 | 20 | 1.35ms | 99.8% |
| Safe top-10 | 96 | 10 | 48 | 1.64ms | 99.8% |
| Rerank-all | 96 | 10 | 0 | 6.94ms | 99.8% |

**Tuning `rerank_topk` for lowest latency:** set `rerank_topk = max(lim, 20)`
for 99.8% recall with minimal TOAST reads. Each TOAST read fetches one full
svec(D) row (~11.5 KB for 2880-dim), so fewer reads = lower latency. The SQ8
cache navigates accurately enough that reranking just 20 candidates already
achieves 99.8% recall — no need for 48 or more.

SQ8 quantizes float32 → uint8 per dimension in the session-local cache (4x
memory savings). The streaming two-pass build avoids allocating a float32
intermediate buffer, so peak memory is just the SQ8 cache itself (283 MB for
103K × 2880-dim). This runs comfortably on 2 Gi pods. Set
`sorted_heap.hnsw_cache_sq8 = off` only when memory is abundant and you need
zero-rerank operation.

Measured with `shared_buffers=512MB` (2 Gi pod), warm cache, 50 queries.
Requires `sorted_heap.hnsw_cache_l0 = on`. Cold first-call latency is 2–3x
higher due to TOAST page faults and cache build.

### Dense r1 pre-filter (`rerank1_topk`)

An optional intermediate stage using a `{prefix}_r1 (nid int4 PK, rerank_vec
hsvec(768))` sidecar. Set `rerank1_topk > 0` to enable. The r1 stage scores
all `ef_search` candidates via hsvec(768) cosine, keeps the closest
`Max(rerank1_topk, lim)`, then passes those to exact svec rerank.

**On a warm TOAST pool, r1 provides marginal benefit.** At ef=64, r1=24 saves
~0.3 ms but costs ~0.12 recall (9.74→9.62). At ef≥96 the r1 btree overhead
exceeds the TOAST savings. r1 is most useful in cold-TOAST scenarios (first
query of a session, or very large datasets where TOAST pages don't fit in
shared_buffers).

If `{prefix}_r1` does not exist, the stage is silently skipped.

---

## API reference

### Training

| Function | Description |
|---|---|
| `svec_ann_train(query, nlist, m)` | Train IVF + PQ codebooks in one call |
| `svec_ivf_train(query, nlist)` | Train IVF centroids only |
| `svec_pq_train(query, m)` | Train raw PQ codebook |
| `svec_pq_train_residual(query, m, ivf_cb_id)` | Train residual PQ codebook |

### Encoding

| Function | Description |
|---|---|
| `svec_ivf_assign(vec, cb_id)` | Assign vector to nearest IVF centroid → int2 |
| `svec_pq_encode(vec, cb_id)` | Encode vector as PQ code → bytea |
| `svec_pq_encode_residual(vec, centroid_id, pq_cb_id, ivf_cb_id)` | Encode residual as PQ code → bytea |

### Search

| Function | Description |
|---|---|
| `svec_hnsw_scan(tbl, query, prefix, ef_search, lim, rerank_topk, rerank1_topk)` | Hierarchical HNSW via sidecar tables (sub-ms with cache) |
| `svec_graph_scan(tbl, query, graph_tbl, entries_tbl, ef_search, lim, rerank_topk)` | Flat NSW graph search |
| `svec_ann_scan(tbl, query, nprobe, lim, rerank_topk, cb_id, ivf_cb_id, pq_column)` | C-level IVF-PQ scan |
| `svec_ann_search(tbl, query, nprobe, lim, rerank_topk, cb_id)` | SQL-level IVF-PQ search |
| `svec_ivf_probe(vec, nprobe, cb_id)` | Return nearest nprobe centroid IDs |

### Low-level distance

| Function | Description |
|---|---|
| `svec_pq_distance_table(vec, cb_id)` | Precompute M×256 distance table → bytea |
| `svec_pq_distance_table_residual(vec, centroid_id, pq_cb_id, ivf_cb_id)` | Distance table for residual PQ |
| `svec_pq_adc_lookup(dist_table, pq_code)` | ADC distance from precomputed table |
| `svec_pq_adc(vec, pq_code, cb_id)` | ADC distance (builds table internally) |
| `svec_cosine_distance(a, b)` | Exact cosine distance (also available as `<=>`) |
