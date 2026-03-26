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
- `REMOTE_PYTHON=/path/to/python SH_EF=32 EXTRA_ARGS='--sh-ef-construction 200' ./scripts/bench_gutenberg_aws.sh <aws-host> /path/to/repo /path/to/dump 65485`
- `scripts/bench_sorted_hnsw_vs_pgvector.sh /tmp 65485 10000 20 384 10 vector 64 96`
- `python3 scripts/bench_ann_real_dataset.py --dataset nytimes-256 --sample-size 10000 --queries 20 --k 10 --pgv-ef 64 --sh-ef 96 --zvec-ef 64 --qdrant-ef 64`
- `python3 scripts/bench_qdrant_synthetic.py --rows 10000 --queries 20 --dim 384 --k 10 --ef 64`
- `python3 scripts/bench_zvec_synthetic.py --rows 10000 --queries 20 --dim 384 --k 10 --ef 64`

### Current AWS restored-corpus benchmark (`~104K x 2880D`, Gutenberg dump)

AWS ARM64 host (4 vCPU, 8 GiB RAM), top-10,
restored PostgreSQL custom dump. Ground truth is recomputed by exact heap
search on the restored `svec` table. In the current rerun the stored
`bench_hnsw_gt` table matched the exact heap GT on 100% of the 50 benchmark
queries, so the fresh exact heap GT and the historical GT table agree. This
rerun uses `sorted_hnsw` `ef_construction=200` and `ef_search=32`, and the
harness reconnects after build before timing ordered scans.

| Method | p50 latency | Recall@10 | Notes |
|--------|:-----------:|:---------:|-------|
| Exact heap (`svec`) | 458.762 ms | 100.0% | brute-force GT on restored corpus |
| **`sorted_hnsw` (`svec`)** | **1.287 ms** | **100.0%** | `ef_construction=200`, `ef_search=32`, index 404 MB, total 1902 MB |
| `sorted_hnsw` (`hsvec`) | 1.404 ms | 100.0% | `ef_construction=200`, `ef_search=32`, index 404 MB, total 1032 MB |
| pgvector HNSW (`halfvec`) | 2.031 ms | 99.8% | `ef_search=64`, index 804 MB, total 1615 MB |
| zvec HNSW | 50.499 ms | 100.0% | in-process collection, `ef=64`, ~1.12 GiB on disk |
| Qdrant HNSW | 6.028 ms | 99.2% | local Docker on same AWS host, `hnsw_ef=64`, 103,260 points |

The precision-matched PostgreSQL comparison on Gutenberg is now
`sorted_hnsw (hsvec)` vs pgvector `halfvec`: `1.404 ms @ 100.0%` versus
`2.031 ms @ 99.8%`, with total footprint `1032 MB` versus `1615 MB`. The raw
fastest PostgreSQL row on this corpus is still `sorted_hnsw (svec)` at
`1.287 ms`, but that uses float32 source storage. `sorted_hnsw` keeps the same
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

### Current local GraphRAG benchmark (`person -> parent -> city`, stable fact contract)

Repo-owned harness:

- `python3 scripts/bench_graph_rag_multihop.py --num-pairs 5000 --query-count 64 --runs 3 --dim 384 --ann-k 64 --top-k 10 --ef-search 128 --ef-construction 200 --m 24 --pgv-ef-search 64 --zvec-ef 64 --qdrant-ef 64 --shared-buffers-mb 64 --backend-mode fresh`

Deterministic fact graph, `5K` chains / `10K` rows total, `384D`, top-10.
This is the current balanced stable GraphRAG point for the narrow fact-shaped
fact-retrieval workload. The stable-facing SQL entry point is
`sorted_heap_graph_rag(...)`; the table below also shows the underlying helper
and wrapper paths because the harness measures the dispatched execution path
directly.

| Method | p50 latency | hit@1 | hit@k | Notes |
|--------|:-----------:|:-----:|:-----:|-------|
| Heap two-hop SQL | 0.762 ms | 75.0% | 96.9% | exact rerank over expanded heap set |
| sorted_heap_graph_rag_twohop_scan() | 0.720 ms | 75.0% | 96.9% | old city-only rerank contract |
| sorted_heap_expand_twohop_rerank() | 0.712 ms | 75.0% | 96.9% | same city-only seed point |
| sorted_heap SQL `pathsum` baseline | 0.847 ms | 98.4% | 98.4% | `hop1_distance + hop2_distance` in SQL |
| sorted_heap_graph_rag_twohop_path_scan() | 0.739 ms | 98.4% | 98.4% | fused path-aware wrapper |
| **sorted_heap_expand_twohop_path_rerank()** | **0.726 ms** | **98.4%** | **98.4%** | same knobs, fused path-aware helper |
| pgvector HNSW + heap expansion | 2.588 ms | 90.6% | 90.6% | path-aware rerank, `ef_search=64` |
| zvec HNSW + heap expansion | 2.507 ms | 100.0% | 100.0% | path-aware rerank, `ef=64` |
| Qdrant HNSW + heap expansion | 4.947 ms | 100.0% | 100.0% | path-aware rerank, `hnsw_ef=64` |

The path-aware helper changes the local conclusion materially: the dominant
quality issue on this fact-shaped workload was the old hop-2-only rerank
contract, not seed ANN quality. The fused path-aware helper now gives the best
local latency/quality point at the same `m=24`, `ef_construction=200`,
`ann_k=64`, `ef_search=128` operating point.

Under the same path-aware scorer contract, the current local conclusion gets
sharper: `sorted_heap` keeps the latency lead, while `zvec` and Qdrant reach
the strongest observed answer quality on this deterministic fact graph.

A repeated-build local protocol then quantified how much of this is just
single-run luck. Using three independent fresh builds of the same `5K`/`384D`
balanced point:

- `sorted_heap_expand_twohop_path_rerank()` median `0.798 ms`, range
  `0.771-0.819 ms`, `hit@1 = 98.4%`, `hit@k = 98.4%` on every build
- `sorted_heap_graph_rag_twohop_path_scan()` median `0.796 ms`, range
  `0.778-0.804 ms`, `hit@1 = 98.4%`, `hit@k = 98.4%` on every build
- `pgvector` path-aware parity row median `1.405 ms`, `hit@1/hit@k`
  `85.9-89.1%`
- `zvec` path-aware parity row median `1.076 ms`, `100.0% / 100.0%`
- `Qdrant` path-aware parity row median `2.799 ms`, `100.0% / 100.0%`

So on the local balanced point, the current `sorted_heap` path-aware rows are
not a fragile one-off. The latency band is tight, and the answer quality did
not drift across the three rebuilds.

### Current AWS GraphRAG benchmark (`person -> parent -> city`, stable fact contract)

Repo-owned harness:

- `REMOTE_PYTHON=/path/to/python QUERY_COUNT=64 RUNS=3 NUM_PAIRS=5000 DIM=384 ANN_K=64 TOP_K=10 EF_SEARCH=128 EF_CONSTRUCTION=200 M=24 PGV_EF_SEARCH=64 ZVEC_EF=64 QDRANT_EF=64 SHARED_BUFFERS_MB=64 BACKEND_MODE=fresh ./scripts/bench_graph_rag_multihop_aws.sh <aws-host> /path/to/repo 65492`

AWS ARM64 host (`4 vCPU`, `8 GiB RAM`), deterministic fact graph,
`5K` chains / `10K` rows total, `384D`, top-10, `64` queries, `3` runs.
This is the current portable stable multihop GraphRAG point for the narrow
fact-shaped contract.

| Method | p50 latency | hit@1 | hit@k | Notes |
|--------|:-----------:|:-----:|:-----:|-------|
| Heap two-hop SQL | 1.088 ms | 75.0% | 96.9% | exact rerank over expanded heap set |
| sorted_heap_expand_twohop_rerank() | 0.952 ms | 75.0% | 96.9% | older city-only helper |
| sorted_heap_graph_rag_twohop_scan() | 1.012 ms | 75.0% | 96.9% | older city-only wrapper |
| sorted_heap SQL `pathsum` baseline | 1.204 ms | 98.4% | 98.4% | same ANN seeds, `hop1_distance + hop2_distance` |
| **sorted_heap_expand_twohop_path_rerank()** | **0.955 ms** | **98.4%** | **98.4%** | fused path-aware helper |
| sorted_heap_graph_rag_twohop_path_scan() | 1.018 ms | 98.4% | 98.4% | fused path-aware wrapper |
| pgvector HNSW + heap expansion | 1.422 ms | 85.9% | 85.9% | path-aware rerank, `ef_search=64` |
| zvec HNSW + heap expansion | 1.720 ms | 100.0% | 100.0% | path-aware rerank, `ef=64` |
| Qdrant HNSW + heap expansion | 3.435 ms | 100.0% | 100.0% | path-aware rerank, `hnsw_ef=64` |

The new AWS result matches the local diagnostic cleanly: the dominant quality
loss on this workload was the old hop-2-only rerank contract, not the seed ANN
frontier. The path-aware helper preserves the quality gain on ARM64 with only
trivial latency cost versus the older helper.

On the apples-to-apples path-aware contract, the portable frontier is now:
- `sorted_heap` fastest
- `zvec` and Qdrant strongest on answer quality
- `pgvector` still behind on both latency and quality at this operating point

One intermediate AWS all-engines rerun temporarily dropped the `sorted_heap`
path-aware rows to `96.9%` / `96.9%`. An immediate `sorted_heap`-only control
and a second full all-engines rerun both returned the stable `98.4% / 98.4%`
point above, so the published table uses the confirmed rerun rather than the
single outlier.

An AWS repeated-build protocol then tightened that confidence band on the same
balanced `5K` point. Using three independent fresh builds:

- `sorted_heap_expand_twohop_path_rerank()` median `0.962 ms`, range
  `0.956-0.965 ms`, `hit@1/hit@k = 98.4/98.4` on all three builds
- `sorted_heap_graph_rag_twohop_path_scan()` median `1.025 ms`, range
  `1.018-1.043 ms`, `hit@1/hit@k = 98.4/98.4` on all three builds
- `pgvector` path-aware parity row median `1.434 ms`, `hit@1/hit@k`
  `84.4-89.1`
- `zvec` path-aware parity row median `1.711 ms`, `100.0/100.0`
- `Qdrant` path-aware parity row median `3.355 ms`, `100.0/100.0`

So on the current portable `5K` point, the earlier AWS outlier now looks like
an anomaly rather than a broad instability. The balanced `sorted_heap`
path-aware rows stayed fixed across all three rebuilds.

The larger `10K`-chain AWS rerun now tells a different story than the older
city-only benchmark. At the same portable point:

- heap two-hop SQL: `1.319 ms`, `hit@1 71.9%`, `hit@k 92.2%`
- city-only `sorted_heap_graph_rag_twohop_scan()`: `1.197 ms`, `hit@1 73.4%`, `hit@k 93.8%`
- SQL `pathsum` baseline: `1.436 ms`, `hit@1 96.9%`, `hit@k 98.4%`
- `sorted_heap_expand_twohop_path_rerank()`: `1.185 ms`, `hit@1 96.9%`, `hit@k 98.4%`
- `sorted_heap_graph_rag_twohop_path_scan()`: `1.212 ms`, `hit@1 96.9%`, `hit@k 98.4%`

So the old larger-scale caveat now narrows materially: the main `10K` loss was
also the city-only rerank contract, not a fundamental collapse of the seed
frontier at that scale.

An AWS repeated-build protocol then checked whether the remaining `10K`
difference was really a build-variance problem. Using three independent fresh
builds on the same `10K` path-aware point:

- `sorted_heap_expand_twohop_path_rerank()` median `1.177 ms`, range
  `1.148-1.191 ms`, `hit@1/hit@k = 95.3/96.9` on all three builds
- `sorted_heap_graph_rag_twohop_path_scan()` median `1.236 ms`, range
  `1.211-1.240 ms`, `hit@1/hit@k = 95.3/96.9` on all three builds
- `pgvector` path-aware parity row median `1.667 ms`, `hit@1/hit@k`
  `76.6-82.8`
- `zvec` path-aware parity row median `2.788 ms`, `98.4/100.0`
- `Qdrant` path-aware parity row median `3.818 ms`, `98.4/100.0`

So the larger `10K` AWS point is now repeated-build stable too. The remaining
issue is scale frontier, not build instability: the `10K` quality band is
lower than `5K`, but it stayed fixed across fresh builds.

An exact-seed diagnostic on the local `5K` and `10K` points did not improve
`hit@1` or `hit@k` versus the ANN-seeded `sorted_heap` helper. So on this
benchmark shape, the remaining gap is not explained by ANN approximation alone.
The stronger result is that seed coverage itself was identical for ANN and
exact seeds: `98.4%` at `5K` and `96.9%` at `10K`. So the remaining loss is
downstream of seeding. The new rerank-rank diagnostic narrows that further:
at `5K`, the correct city is still within the top 6 for 95% of reachable
queries, and at `10K` it is still within the top 3 for 95% of reachable
queries. The quality drop is therefore driven by a few severe outliers
(`max rank 17` at `5K`, `20` at `10K`), not by a broad collapse.

A path-aware SQL rerank baseline then changed the picture materially. Keeping
the same ANN seeds and the same two-hop expansion, but scoring candidates as
`hop1_distance + hop2_distance`, moved the local balanced points to:
- `5K`: `0.957 ms`, `hit@1 98.4%`, `hit@k 98.4%`
- `10K`: `1.179 ms`, `hit@1 95.3%`, `hit@k 96.9%`

That branch is now implemented in the extension and verified on both local and
AWS ARM64 runs. The fused path-aware helper measured:
- local `5K`: `0.726 ms`, `hit@1 98.4%`, `hit@k 98.4%`
- local `10K`: `0.823 ms`, `hit@1 95.3%`, `hit@k 96.9%`
- AWS `5K`: `0.955 ms`, `hit@1 98.4%`, `hit@k 98.4%`
- AWS `10K`: `1.185 ms`, `hit@1 96.9%`, `hit@k 98.4%`

So the current strongest portable GraphRAG result is no longer the SQL
baseline or the old city-only helper. It is the fused path-aware helper.

### Current real code-corpus GraphRAG reference benchmark (`cogniformerus` CrossFile)

Repo-owned harnesses:

- `python3 scripts/bench_graph_rag_code_corpus.py --runs 3 --backend-mode fresh --ann-k 16 --top-k 4`
- `python3 scripts/repeat_graph_rag_code_corpus_builds.py --repeats 3 --runs 3 --backend-mode fresh`
- `REMOTE_PYTHON=/path/to/python REPEATS=3 RUNS=3 BACKEND_MODE=fresh bash scripts/repeat_graph_rag_code_corpus_builds_aws.sh <aws-host> /path/to/repo 65320`

This benchmark uses the actual `cogniformerus` source tree (`40` files,
`840` rows after chunk + summary expansion) and the real CrossFile prompts from
`butler_code_test.cr`. The current real-corpus conclusion is **not** a single
universal winner. The frontier splits by embedding mode:

| Mode | Best case | Local repeated-build p50 | AWS repeated-build p50 | Keyword coverage | Full hits | Notes |
|------|-----------|:------------------------:|:----------------------:|:----------------:|:---------:|-------|
| generic | `prompt_summary_snippet_py` | `0.613 ms` | `0.955 ms` | `100.0%` | `100.0%` | symbol-aware variant is strictly slower with no quality gain |
| code-aware | `prompt_symbol_summary_snippet_py` | `0.963 ms` | `1.541 ms` | `100.0%` | `100.0%` | exact prompt-symbol rescue is required in summary seeding |

The most important diagnostic result was the old code-aware miss:

- `prompt_summary_snippet_py` on `facts_sh`
  - local repeated-build: `97.6%` keyword coverage, `83.3%` full hits
  - AWS repeated-build: `97.6%`, `83.3%`
- `prompt_symbol_summary_snippet_py` on `facts_sh`
  - local repeated-build: `100.0%`, `100.0%`
  - AWS repeated-build: `100.0%`, `100.0%`

So the code-aware quality win is now both repeated-build stable and
cross-environment stable. The change in winner is not a local-only artifact.

### Larger in-repo `cogniformerus` transfer gate

The smaller `40`-file code-corpus slice above is a useful stable benchmark, but
it is not the only in-repo transfer check anymore. The same repeated-build
protocol was rerun on the full `cogniformerus` repository (`183` Crystal
files), still using the real CrossFile prompts from `butler_code_test.cr`.

Control point at the old tiny-budget contract (`top_k=4`, `1` fresh build):

| Mode | Best case | Local p50 | Keyword coverage | Full hits | Avg returned rows | Notes |
|------|-----------|:---------:|:----------------:|:---------:|:-----------------:|-------|
| generic | `prompt_summary_snippet_py` | `0.770 ms` | `87.1%` | `66.7%` | `3.67` | larger corpus exposes a result-budget cliff |
| code-aware | `prompt_symbol_summary_snippet_py` | `1.824 ms` | `87.6%` | `66.7%` | `4.00` | same cliff under code-aware embeddings |

Bounded recovery point (`top_k=8`, `3` fresh builds):

| Mode | Best case | Local repeated-build p50 | Keyword coverage | Full hits | Avg returned rows | Notes |
|------|-----------|:------------------------:|:----------------:|:---------:|:-----------------:|-------|
| generic | `prompt_summary_snippet_py` | `0.819 ms` | `100.0%` | `100.0%` | `6.33` | larger in-repo Crystal transfer now verified |
| code-aware | `prompt_symbol_summary_snippet_py` | `1.814 ms` | `100.0%` | `100.0%` | `7.50` | same winner, but needs the larger final budget |

Interpretation:

- the current real code-corpus contracts do transfer beyond the tiny `40`-file
  slice
- the dominant larger-corpus issue on the in-repo Crystal side is result
  budget, not a new retrieval failure
- this larger-corpus Crystal gate is now covered and serves as a transfer
  check for the benchmark-side code-retrieval logic, not as the stable
  release contract for GraphRAG

### Mixed-language external code-corpus GraphRAG reference benchmark (`pycdc`)

The code-corpus harness now also supports:

- JSON question fixtures
- configurable source extensions
- quoted local `#include "..."` dependency edges for C/C++ corpora

The first mixed-language adversary corpus was `pycdc`, using a repo-owned
fixture in `scripts/fixtures/graph_rag_pycdc_questions.json`. This run used the
real `pycdc` source tree (`138` files, `1281` rows after chunk + summary
expansion, `72` local dependency edges) and `3` fresh builds at `top_k=8`.

| Mode | Best case | Local repeated-build p50 | Keyword coverage | Full hits | Avg returned rows | Notes |
|------|-----------|:------------------------:|:----------------:|:---------:|:-----------------:|-------|
| generic | `prompt_symbol_summary_snippet_py` | `0.850 ms` | `90.0%` | `60.0%` | `6.40` | fastest mixed-language point, but it does not close the corpus |
| code-aware | `prompt_compactseed_require_summary_snippet_fn` | `8.006 ms` | `100.0%` | `100.0%` | `5.80` | helper-backed compact lexical seed + one-hop include rescue closes the corpus |

Interpretation:

- the mixed-language gate is now covered for a real `~/Projects/C` corpus
- the result split is sharper than on the Crystal corpora:
  - the fast generic path plateaus below perfect quality
  - the code-aware include-rescue path closes the corpus, but at a much higher
    latency
- so `~/Projects/C` is now covered as part of the broader code-corpus
  reference matrix, even though the fastest generic point remains partial

### Archive-side code-corpus GraphRAG reference benchmark (`ninja/src`)

The same widened harness was then pointed at an archive-side corpus under
`~/SrcArchives`: `apple/ninja/src`. This run used a second repo-owned fixture in
`scripts/fixtures/graph_rag_ninja_questions.json` and the local include graph
inside `ninja/src` (`103` files, `1757` rows after chunk + summary expansion,
`282` dependency edges).

Initial smoke at `top_k=8`:

| Mode | Best fast case | Local p50 | Keyword coverage | Full hits | Notes |
|------|----------------|:---------:|:----------------:|:---------:|-------|
| generic | `prompt_summary_snippet_py` | `0.898 ms` | `95.0%` | `80.0%` | already close without rescue |
| code-aware | `prompt_summary_snippet_py` | `0.928 ms` | `85.0%` | `80.0%` | code-aware mode is weaker on this corpus |

Bounded budget probe (`top_k=12`, `3` fresh builds):

| Mode | Best case | Local repeated-build p50 | Keyword coverage | Full hits | Avg returned rows | Notes |
|------|-----------|:------------------------:|:----------------:|:---------:|:-----------------:|-------|
| generic | `prompt_summary_snippet_py` | `0.914 ms` | `100.0%` | `100.0%` | `7.80` | archive-side gate closes with a small result-budget bump |
| code-aware | `prompt_summary_snippet_py` | `0.871 ms` | `85.0%` | `80.0%` | `7.60` | still not the winner on this corpus |

Interpretation:

- the `~/SrcArchives` side is now covered by a real repeated-build gate
- unlike `pycdc`, this archive corpus does **not** need a dependency-rescue
  contract to close
- the winner is the simple generic summary-snippet path at a slightly larger
  final budget (`top_k=12`)
- the larger real-corpus verification matrix for the narrow `0.13`
  fact-graph release now spans:
  - `~/Projects/Crystal`
  - `~/Projects/C`
  - `~/SrcArchives`

### External folding stress corpus for GraphRAG reference logic (`folding/src`)

The same harness was then pointed at a second real code corpus outside this
repository: `folding/src` with prompts from `butler_folding_test.cr`. This is
not the primary publishable frontier for the repository, but it is a strong
adversary corpus because it falsifies overfit retrieval contracts quickly.

Current repeated-build result:

| Mode | Case | Local repeated-build p50 | AWS repeated-build p50 | Keyword coverage | Full hits | Notes |
|------|------|:------------------------:|:----------------------:|:----------------:|:---------:|-------|
| generic | `prompt_summary_snippet_py` | `1.048 ms` | `1.540 ms` | `90.5%` | `83.3%` | fast baseline drifts below perfect quality on this corpus |
| generic | `prompt_compactseed_require_summary_snippet_fn` | `5.940 ms` | `8.839 ms` | `100.0%` | `100.0%` | compact lexical seed table + helper-backed one-hop `REQUIRES_FILE` rescue |
| generic | `prompt_lexseed_require_summary_snippet_fn` | `28.266 ms` | `41.960 ms` | `100.0%` | `100.0%` | historical full-summary lexical rescue, now dominated |
| code-aware | `prompt_summary_snippet_py` | `1.080 ms` | `1.775 ms` | `79.8%` | `66.7%` | worse baseline than the primary `cogniformerus` corpus |
| code-aware | `prompt_compactseed_require_summary_snippet_fn` | `5.804 ms` | `8.392 ms` | `100.0%` | `100.0%` | compact lexical seed table + helper-backed one-hop `REQUIRES_FILE` rescue |
| code-aware | `prompt_lexseed_require_summary_snippet_fn` | `36.676 ms` | `60.457 ms` | `100.0%` | `100.0%` | historical full-summary lexical rescue, now dominated |

Interpretation:

- the external folding miss was a real seed-selection problem, not a snippet
  extraction bug
- the rescue is now verified on both local Apple Silicon and AWS ARM64
- the current documented external rescue is no longer the old full-summary
  lexical path; it is the compact lexical-seed table variant
- compact lexical seeding keeps `100.0% / 100.0%` while cutting the old rescue
  by about `4.8x` locally and `4.7-7.2x` on AWS, depending on mode
- an isolated local timing split shows the helper-backed rescue is still
  dominated by lexical-seed + `REQUIRES_FILE` fetch work (`~10.7-11.0 ms/query`)
  with snippet postprocess as a secondary cold-start cost (`~7.7-8.0 ms/query`)
- the old full-summary lexical rescue remains useful as a diagnostic, but it is
  no longer the external default frontier
- even the compact rescue is still slower than the primary in-repo winners, so
  it does not replace them as the default GraphRAG contract

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
