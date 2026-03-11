---
layout: default
title: Vector Search
nav_order: 3
---

# Vector Search

pg_sorted_heap includes two built-in vector types and IVF-PQ approximate
nearest neighbor search. The key insight: sorted_heap's physical clustering by
primary key prefix **is** the inverted file index — no separate index structure
needed, no pgvector dependency, no 800 MB HNSW graph.

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

## Quick start

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

### Comparison with pgvector HNSW

Same dataset (103K × 2880-dim), same k8s pod.

| Method | R@1 | Avg latency | Index size | Max dim |
|---|---|---|---|---|
| Exact brute-force (svec `<=>`) | 100% | 996 ms | — | 16,000 / 32,000 |
| pgvector HNSW ef=100 | 97% | 14 ms | 806 MB | 2,000 (`vector`) / 4,000 (`halfvec`) |
| IVF-PQ nprobe=10, rerank=200 | 97–99% | 22 ms | 27 MB | 16,000 / 32,000 |

IVF-PQ is 30x smaller on storage at comparable recall. HNSW is faster for
single queries but requires storing full vectors in the index. `svec`/`hsvec`
still support a much wider dimensional range and keep the ANN path inside the
table rather than an additional large graph index.

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
| `svec_ann_scan(tbl, query, nprobe, lim, rerank_topk, cb_id, ivf_cb_id, pq_column)` | C-level IVF-PQ scan (fastest) |
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
