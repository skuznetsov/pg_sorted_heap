---
layout: default
title: Large-Vector Sublinear Search
nav_order: 20
---

# Spec: Large-Vector Sublinear Search

Status: proposed
Risk tier: CAUTION
Primary goal: revive IVF-PQ / SQ-family work only where it beats the current
defaults on latency, recall, and footprint for very large vector tables.

## Problem

The current vector story has three different regimes:

- `sorted_hnsw` is the stable planner-integrated default.
- FlashHadamard is a documented experimental packed exhaustive-search lane.
- IVF-PQ is a legacy/manual path that can scan compact PQ codes inside
  physically clustered IVF partitions.

At `103K x 2880D`, current docs already record that exhaustive packed
FlashHadamard and `sorted_hnsw` are stronger product defaults than IVF-style
pruning. Re-enabling IVF-PQ as a headline feature at that scale would reopen a
refuted branch.

The valid revival target is larger tables, where scanning every packed vector
per query becomes memory-bandwidth-bound and sublinear routing can pay for its
complexity.

## Current Evidence

Validated anchors in this repository:

- `svec_ann_scan(...)` already implements C-level IVF-PQ scan:
  IVF probe, SPI candidate fetch, ADC over PQ codes, top-k selection, and
  optional exact rerank.
- Residual PQ is implemented and documented. It improves intra-partition
  fidelity at the cost of one distance table per probed centroid.
- Physical clustering by `(partition_id, id)` maps IVF probes to sorted_heap
  block ranges, so zone-map pruning can skip non-probed partitions.
- On `103K x 2880D`, IVF/pruning is not the recommended default; current
  FlashHadamard notes explicitly park pruning at that scale.
- `sorted_hnsw.build_sq8` and the sidecar HNSW cache already use SQ8 as an
  internal memory-reduction substrate.
- No stable SQ4 row-store or planner-integrated SQ4 scan exists today.

## Revival Contract

Do not promote a sublinear lane unless it beats at least one current default in
a clearly named operating regime.

Required comparison axes:

- latency: p50, p95, and average query time;
- quality: hit@1 and recall@10 against exact heap ground truth;
- footprint: heap, index/sidecar, codebook, and auxiliary storage;
- build/training time;
- operational complexity: rebuild/restore behavior, generated columns,
  codebooks, compaction, and partition routing.

Required baselines:

- exact heap search;
- `sorted_hnsw` on `svec` and/or `hsvec`;
- pgvector HNSW when the dimension is inside pgvector's envelope;
- FlashHadamard packed exhaustive search when a compatible store exists;
- IVF-PQ residual with exact rerank.

## Candidate Lanes

### Lane A: IVF-PQ residual, explicit manual API

This is the lowest-risk revival because the primitives already exist.

Use when:

- table size is large enough that exhaustive packed scan is bandwidth-bound;
- training and generated columns are acceptable;
- storage footprint matters more than lowest single-query latency;
- the application can tune `nlist`, `nprobe`, `M`, and `rerank_topk`.

Do not hide this behind the generic planner until filtered/routed underfill and
global-merge semantics are proven.

### Lane B: SQ8 candidate scan + exact rerank

SQ8 is a good hardware-native rerank/filter substrate because it is simple,
SIMD-friendly, and already used by `sorted_hnsw` internals.

Potential shape:

- store per-row SQ8 codes in a sidecar or generated column;
- scan only routed partitions or segments;
- exact-rerank a bounded candidate pool from heap vectors;
- compare against `sorted_hnsw` and FlashHadamard at equal recall.

This may be a better first product lane than SQ4 because quality loss is lower
and implementation risk is smaller.

### Lane C: SQ4 / 4-bit packed scan

SQ4 should not be introduced as plain scalar 4-bit quantization by default.
Existing FlashHadamard evidence suggests the 4-bit lane needs rotation,
equalization, packed layout, and careful rerank to be competitive.

Valid SQ4 path:

- reuse or adapt the FlashHadamard packed scorer/store;
- define a PostgreSQL-safe storage lifecycle;
- prove quality across more than the current Gutenberg operating point;
- include exact rerank gates.

Invalid SQ4 path:

- add a raw 4-bit scalar generated column and assume it will be good enough.

## Scale Gates

### S1. 103K regression gate

Purpose: prevent regression against known evidence.

Expected:

- IVF-PQ may remain documented and callable;
- it does not become the default recommendation at this scale;
- any new SQ path must beat or clearly complement current `sorted_hnsw` /
  FlashHadamard rows before promotion.

### S2. 500K to 1M product gate

Purpose: find where exhaustive packed scan stops being the best latency /
quality / footprint tradeoff.

Benchmark requirements:

- at least one real high-dimensional corpus;
- at least one lower-dimensional ANN-Benchmarks corpus;
- fixed query set and exact heap ground truth;
- cold-build and warm-query metrics;
- footprint reported from PostgreSQL relation sizes and sidecar files.
- synthetic baseline runs should start from `make bench-large-vector-synthetic`;
  its defaults are intentionally small for smoke checks, while `500K` / `1M`
  runs are selected by overriding `BENCH_LARGE_VECTOR_ROWS`,
  `BENCH_LARGE_VECTOR_QUERIES`, and `BENCH_LARGE_VECTOR_DIM`.

Promotion condition:

- sublinear lane is faster than exhaustive packed scan at comparable recall, or
  materially smaller than `sorted_hnsw` at acceptable recall;
- benchmark is reproducible from a documented script.

### S3. Partitioned large-table gate

Purpose: connect sublinear vector search to the native partitioning work.

Expected:

- routing by tenant/time/segment maps to whole partitions or IVF partitions;
- selected leaves/shards are globally merged and exact-reranked;
- local top-k concatenation is not accepted as a correctness contract.

## Implementation Plan

1. Extend the real-dataset benchmark harness with an opt-in IVF-PQ residual
   row. Baseline flag: `scripts/bench_ann_real_dataset.py --enable-ivfpq`.
   The row reports training time, table build time, compact time, codebook
   footprint, p50/p95/average latency, recall@K, and rerank settings.
2. Add a larger synthetic scale harness that can generate `500K` and `1M`
   vectors without external downloads, with deterministic query and exact GT
   sampling. Baseline entry point: `make bench-large-vector-synthetic`, which
   reuses the existing exact/sorted_hnsw/pgvector/optional-DiskANN harness.
3. Compare three candidates on the same harness: `sorted_hnsw`, IVF-PQ
   residual, and FlashHadamard packed exhaustive.
4. Only after a winning region is identified, design a product API or planner
   integration. Until then, keep IVF-PQ as legacy/manual.

## Acceptance Tests

### L1. IVF-PQ harness row is reproducible

Run the real-dataset harness on a small sample with IVF-PQ enabled.

Smoke shape:

`python3 scripts/bench_ann_real_dataset.py --sample-size 1000 --queries 5 --skip-zvec --skip-qdrant --enable-ivfpq --ivfpq-nlist 32 --ivfpq-nprobe 4`

Expected:

- codebooks train;
- generated columns populate;
- compact completes;
- `svec_ann_scan(...)` returns `k` rows per query;
- exact ground truth comparison is reported.

### L2. Recall/latency matrix is apples-to-apples

For every reported method:

- same vectors;
- same query set;
- same `k`;
- same exact heap ground truth;
- warm-query timing excludes build/training unless explicitly labeled.

### L3. Large-scale winner must beat a baseline

For any promotion claim:

- state the exact scale and dimension;
- show the baseline it beats;
- show recall and footprint, not only latency.

## Adversary Notes

- IVF can look fast by scanning too few partitions and silently losing recall.
  Recall gates must be mandatory.
- PQ can look small while moving cost into codebooks, generated columns, and
  exact rerank heap fetches. Footprint must include all auxiliary storage.
- SQ8/SQ4 can look better in cache-only experiments than in PostgreSQL because
  SPI, TOAST, sidecar lifecycle, and MVCC integration dominate.
- FlashHadamard pruning failures at `103K` do not prove pruning is bad at `1M`;
  they prove the scale boundary matters.
- Partition fanout needs global exact rerank. Local top-k concat can under-rank
  cross-partition candidates.

## Decision

For `0.13`, keep `sorted_hnsw` as the stable default and IVF-PQ as
legacy/manual. Treat IVF-PQ/SQ revival as a benchmark-gated large-scale track,
not as an immediate planner feature.
