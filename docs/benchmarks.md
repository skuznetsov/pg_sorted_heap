---
layout: default
title: Benchmarks
nav_order: 6
---

# Benchmarks

All benchmarks run on PostgreSQL 18, Apple M-series (12 CPU, 64 GB RAM)
with `shared_buffers=4GB`, `work_mem=256MB`, `maintenance_work_mem=2GB`.
Warm cache, average of 5 runs.

---

## EXPLAIN ANALYZE -- query latency and buffer reads

### 1M rows (71 MB)

| Query | sorted_heap | heap + btree | heap seqscan |
|-------|------------|-------------|-------------|
| Point (1 row) | 0.035 ms / 1 buf | 0.046 ms / 7 bufs | 15.2 ms / 6,370 bufs |
| Narrow (100 rows) | 0.043 ms / 2 bufs | 0.067 ms / 8 bufs | 16.2 ms / 6,370 bufs |
| Medium (5K rows) | 0.434 ms / 33 bufs | 0.492 ms / 52 bufs | 16.1 ms / 6,370 bufs |
| Wide (100K rows) | 7.5 ms / 638 bufs | 8.9 ms / 917 bufs | 17.4 ms / 6,370 bufs |

### 10M rows (714 MB)

| Query | sorted_heap | heap + btree | heap seqscan |
|-------|------------|-------------|-------------|
| Point (1 row) | 0.034 ms / 1 buf | 0.047 ms / 7 bufs | 117.9 ms / 63,695 bufs |
| Narrow (100 rows) | 0.037 ms / 1 buf | 0.062 ms / 7 bufs | 130.9 ms / 63,695 bufs |
| Medium (5K rows) | 0.435 ms / 32 bufs | 0.549 ms / 51 bufs | 131.0 ms / 63,695 bufs |
| Wide (100K rows) | 7.6 ms / 638 bufs | 8.8 ms / 917 bufs | 131.4 ms / 63,695 bufs |

### 100M rows (7.8 GB)

| Query | sorted_heap | heap + btree | heap seqscan |
|-------|------------|-------------|-------------|
| Point (1 row) | 0.045 ms / **1 buf** | 0.506 ms / 8 bufs | 1,190 ms / 519,906 bufs |
| Narrow (100 rows) | 0.166 ms / 2 bufs | 0.144 ms / 9 bufs | 1,325 ms / 520,782 bufs |
| Medium (5K rows) | 0.479 ms / 38 bufs | 0.812 ms / 58 bufs | 1,326 ms / 519,857 bufs |
| Wide (100K rows) | 7.9 ms / 737 bufs | 10.1 ms / 1,017 bufs | 1,405 ms / 518,896 bufs |

At 100M rows, a point query reads **1 buffer** (vs 8 for btree, 519,906 for
sequential scan).

---

## pgbench throughput (TPS)

### Prepared mode (`-M prepared`)

Query planned once, re-executed with parameters. 10 s, 1 client.

| Query | 1M (sh / btree) | 10M (sh / btree) | 100M (sh / btree) |
|-------|----------------:|------------------:|-------------------:|
| Point | 46.9K / 59.4K | 46.5K / 58.0K | 32.6K / 43.6K |
| Narrow | 22.3K / 29.1K | 22.5K / 28.8K | 17.9K / 18.1K |
| Medium | 3.4K / 5.1K | 3.4K / 4.8K | 2.4K / 2.4K |
| Wide | 295 / 289 | 293 / 286 | 168 / 157 |

### Simple mode (`-M simple`)

Each query parsed, planned, and executed separately. 10 s, 1 client.

| Query | 1M (sh / btree) | 10M (sh / btree) | 100M (sh / btree) |
|-------|----------------:|------------------:|-------------------:|
| Point | 28.4K / 38.0K | 29.1K / 41.4K | **18.7K / 4.6K** |
| Narrow | 19.6K / 24.4K | 21.8K / 27.6K | **7.1K / 5.5K** |
| Medium | 3.1K / 3.7K | 3.4K / 4.8K | **2.1K / 1.6K** |
| Wide | 198 / 290 | 200 / 286 | **163 / 144** |

At 100M rows in simple mode, sorted_heap wins all query types. Point queries
reach 18.7K TPS vs 4.6K for btree (4x).

---

## INSERT and compaction throughput

| Scale | sorted_heap | heap + btree | heap (no index) | compact time |
|------:|------------:|-----------:|--------------:|-----------:|
| 1M | 923K rows/s | 961K | 1.91M | 0.3 s |
| 10M | 908K rows/s | 901K | 1.65M | 3.1 s |
| 100M | 840K rows/s | 1.22M | 2.22M | 41.3 s |

### Storage

| Scale | sorted_heap | heap + btree | heap (no index) |
|------:|------------:|------------:|--------------:|
| 1M | 71 MB | 71 MB (50 + 21) | 50 MB |
| 10M | 714 MB | 712 MB (498 + 214) | 498 MB |
| 100M | 7.8 GB | 7.8 GB (5.7 + 2.1) | 5.7 GB |

sorted_heap stores data + zone map in the same space as a heap table.
The zone map replaces the btree index, so total storage is comparable to
heap-without-index -- roughly 30% less than heap + btree at scale.

---

## Vector search

### Current local synthetic benchmark (`sorted_hnsw` Index AM)

Repo-owned harnesses:

- `python3 scripts/bench_gutenberg_local_dump.py --dump /tmp/cogniformerus_backup/cogniformerus_backup.dump --port 65473`
- `REMOTE_PYTHON=/home/ubuntu/.venvs/zvec-bench/bin/python ./scripts/bench_gutenberg_aws.sh ubuntu@dev.rigelstar.com /home/ubuntu/clustered_pg /home/ubuntu/cogniformerus_backup.dump 65475`
- `scripts/bench_sorted_hnsw_vs_pgvector.sh /tmp 65485 10000 20 384 10 vector 64 96`
- `python3 scripts/bench_ann_real_dataset.py --dataset nytimes-256 --sample-size 10000 --queries 20 --k 10 --pgv-ef 64 --sh-ef 96 --zvec-ef 64 --qdrant-ef 64`
- `python3 scripts/bench_qdrant_synthetic.py --rows 10000 --queries 20 --dim 384 --k 10 --ef 64`
- `python3 scripts/bench_zvec_synthetic.py --rows 10000 --queries 20 --dim 384 --k 10 --ef 64`

### Current AWS restored-corpus benchmark (`~104K x 2880D`, Gutenberg dump)

AWS ARM64 host `ubuntu@dev.rigelstar.com` (4 CPU, 7.6 GiB RAM), top-10,
restored PostgreSQL custom dump. Ground truth is recomputed by exact heap
search on the restored `svec` table. In the current rerun the stored
`bench_hnsw_gt` table matched the exact heap GT on 100% of the 50 benchmark
queries, so the fresh exact heap GT and the historical GT table agree.

| Method | p50 latency | Recall@10 | Notes |
|--------|:-----------:|:---------:|-------|
| Exact heap (`svec`) | 467.628 ms | 100% | brute-force GT on restored corpus |
| `sorted_hnsw` (`svec`) | 2.353 ms | 99.8% | `ef_search=96`, index 404 MB, total 1902 MB |
| **`sorted_hnsw` (`hsvec`)** | **2.306 ms** | **99.8%** | `ef_search=96`, index 404 MB, total 1032 MB |
| pgvector HNSW (`halfvec`) | 1.985 ms | 97.8% | `ef_search=64`, index 804 MB, total 1615 MB |
| zvec HNSW | 51.130 ms | 100% | in-process collection, `ef=64`, ~1.12 GiB on disk |
| Qdrant HNSW | 6.689 ms | 99.6% | local Docker on same AWS host, `hnsw_ef=64`, 103,260 points |

The precision-matched PostgreSQL comparison on Gutenberg is now
`sorted_hnsw (hsvec)` vs pgvector `halfvec`. `sorted_hnsw` keeps the same
404 MB index in both cases because the AM stores SQ8 graph state; the storage
gain from `hsvec` appears in the base table and TOAST footprint instead.

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

### Current local real-dataset sample (`nytimes-256-angular`)

Repo-owned harness:

- `python3 scripts/bench_ann_real_dataset.py --dataset nytimes-256 --sample-size 10000 --queries 20 --k 10 --pgv-ef 64 --sh-ef 96 --zvec-ef 64 --qdrant-ef 64`

ANN-Benchmarks `nytimes-256-angular`, sampled to 10K base vectors and 20
queries, top-10. The table below reports medians across 3 full harness runs.
Ground truth comes from exact PostgreSQL heap search on the sampled `svec`
corpus.

| Method | p50 latency | Recall@10 | Notes |
|--------|:-----------:|:---------:|-------|
| Exact heap (`svec`) | 1.557 ms | 100% | brute-force ground truth |
| **sorted_hnsw** | **0.327 ms** | **85.0% median** (83.5-85.5 range) | `shared_cache=on`, `ef_search=96`, index ~4.1 MB |
| pgvector HNSW (`vector`) | 0.751 ms | 79.0% median (78.5-79.0 range) | `ef_search=64`, same `M=16`, `ef_construction=64`, index ~13 MB |
| zvec HNSW | 0.403 ms | 99.5% | local in-process collection, `ef=64`, ~14.1 MB on disk |
| Qdrant HNSW | 1.704 ms | 99.5% | local Docker, `hnsw_ef=64` |

This corpus is materially harder than the deterministic synthetic one. It is a
better signal for default-parameter recall, while the synthetic table remains
useful for controlled same-host engine comparisons and regression tracking.

### Legacy/manual IVF-PQ benchmark

The sections below are still useful for the explicit IVF-PQ API
(`svec_ann_scan`), but they are no longer the default ANN baseline for the
repository. Those measurements target the legacy/manual vector path, not the
planner-integrated `sorted_hnsw` Index AM.

All IVF-PQ benchmarks below use `svec_ann_scan` (C-level) with residual PQ.
1 Gi k8s pod, PostgreSQL 18.

### 103K vectors, 2880-dim (Gutenberg corpus)

Residual PQ (M=720, dsub=4), 256 IVF partitions.
100 cross-queries (self-match excluded):

| Config | R@1 | Recall@10 | Avg latency |
|---|---|---|---|
| nprobe=1, PQ-only | 54% | 48% | 5.5 ms |
| nprobe=3, PQ-only | 79% | 71% | 8 ms |
| nprobe=3, rerank=96 | 82% | 74% | 10 ms |
| nprobe=5, rerank=96 | 89% | 86% | 12 ms |
| nprobe=10, rerank=200 | 97% | 94% | 22 ms |

Self-query (vector in dataset): R@1 = 100% at nprobe=3 / 8 ms.

### 10K vectors, 2880-dim (float32 precision test)

Same corpus, pure svec (float32), nlist=64, M=720 residual PQ.
100 cross-queries:

| Config | R@1 | Recall@10 |
|---|---|---|
| nprobe=1, PQ-only | 56% | 56% |
| nprobe=3, PQ-only | 72% | 82% |
| nprobe=5, rerank=96 | 93% | 93% |
| nprobe=10, rerank=200 | 99% | 99% |

### float32 vs float16 precision impact

Tested the same 10K Gutenberg vectors in two configurations:
- **float32 (svec):** native 32-bit storage, independently trained codebooks
- **float16-degraded (hsvec):** svec → hsvec → svec roundtrip, independently trained

Result: **no measurable recall difference**. Float16 precision loss (~1e-7) is
1000× smaller than typical distance gaps between consecutive neighbors (~1e-4).
The recall bottleneck is PQ quantization and IVF routing, not input precision.
This confirms hsvec is a safe storage choice for ANN workloads.

### CRUD performance (500K rows, svec(128), prepared mode)

| Operation | eager / heap | lazy / heap | Notes |
|-----------|:-----------:|:-----------:|-------|
| SELECT PK | 85% | 85% | Index Scan via btree |
| SELECT range 1K | 97% | -- | Custom Scan pruning (eager only) |
| Bulk INSERT | 100% | 100% | Always eager |
| DELETE + INSERT | 63% | 63% | INSERT always eager |
| UPDATE non-vec | 46% | **100%** | Lazy skips zone map flush |
| UPDATE vec col | 102% | **100%** | Parity both modes |
| Mixed OLTP | 83% | **97%** | Near-parity with lazy |

Eager mode (default) maintains zone maps on every UPDATE for scan pruning.
Lazy mode (`sorted_heap.lazy_update = on`) trades scan pruning for UPDATE
parity with heap. Compact/merge restores pruning.

### Self-query vs cross-query

**Self-query**: query vector is in the dataset (typical RAG case — you
embedded documents, now you search them). The vector is always found as its
own closest neighbor, so R@1 = 100%.

**Cross-query**: query vector is NOT in the dataset (e.g., user question
embedded at search time). R@1 depends on nprobe and PQ fidelity.

When comparing benchmarks, verify whether self-match is included or excluded.
The tables above use cross-query (self-match excluded) for honest comparison.

---

## Methodology notes

- **EXPLAIN ANALYZE:** warm cache (pg_prewarm), average of 5 runs, actual
  execution time + buffer reads reported
- **pgbench:** 10 s runtime, 1 client, includes pgbench overhead (connection
  management, query dispatch); useful for relative throughput comparison
- **INSERT:** COPY path via `INSERT ... SELECT generate_series()`
- **Compact time:** wall-clock time for `sorted_heap_compact()` on warm data
- **Vector search:** 100 random queries from the dataset, self-match excluded
  by requesting `lim := 11` and taking positions 2–11. Ground truth via exact
  brute-force cosine (`<=>` operator). Latency measured via `clock_timestamp()`
  per-query in PL/pgSQL loop (20 queries, warm cache)
- **Current local `sorted_hnsw` comparison:** deterministic synthetic 10K x
  384D corpus via `scripts/bench_sorted_hnsw_vs_pgvector.sh`, 3 fresh builds
  for PostgreSQL methods, median p50 / median recall reported; Qdrant via
  `scripts/bench_qdrant_synthetic.py`, 3 warm measurement passes on one local
  Docker collection; zvec via `scripts/bench_zvec_synthetic.py`, 3 warm
  measurement passes on one local in-process collection
- **Current local real-dataset sample:** `scripts/bench_ann_real_dataset.py`
  on ANN-Benchmarks `nytimes-256-angular`, sampled to 10K base vectors and 20
  queries. Ground truth is exact PostgreSQL `svec` heap search on the sampled
  corpus. Numbers above are medians across 3 full harness runs.
