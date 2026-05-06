---
layout: default
title: SIMD ADC And DiskANN Comparison
nav_order: 21
---

# Spec: SIMD ADC And pgvectorscale DiskANN Comparison

Status: proposed
Risk tier: CAUTION
Primary goal: separate local SIMD ADC optimization from an apples-to-apples
comparison against pgvectorscale StreamingDiskANN.

## Problem

The TODO shorthand combines two different tracks:

- **SIMD ADC lookup:** make our compressed-vector scoring kernels faster.
- **pgvectorscale DiskANN comparison:** benchmark against a graph index with
  PostgreSQL integration, SBQ compression, rescoring, and filtered-search
  features.

Treating them as one task would blur algorithmic quality, storage layout,
execution model, and PostgreSQL integration overhead.

## Current Local Evidence

### PQ ADC

`svec_pq_adc_lookup(dist_table, code)` currently performs scalar lookup and
accumulation:

```text
for each subvector m:
    total += dist_table[m][code[m]]
```

This is simple and portable, but it is still scalar per candidate. The
C-level `svec_ann_scan(...)` path avoids per-row fmgr overhead and should be
the baseline before micro-optimizing the standalone SQL ADC function.

### FlashHadamard ADC

FlashHadamard already has a packed byte-table scorer and a separate CPU kernel
lab:

- Apple/NEON int16 LUT is integrated behind `FH_INT16=1` and showed a narrow
  end-to-end win on the validated local path.
- Intel/AVX2 int16 LUT was refuted in the existing notes.
- Intel/AVX2 float gather is promising in the standalone kernel lab but is not
  integrated into the engine.

The safe conclusion is hardware-specific: SIMD ADC optimization is worthwhile,
but each kernel needs a platform-specific parity and latency gate.

## Current pgvectorscale Baseline

As of the upstream README checked on 2026-05-06, pgvectorscale provides:

- a `diskann` access method named StreamingDiskANN;
- Statistical Binary Quantization storage layout by default;
- label-based filtering through a `smallint[]` label column in the index;
- arbitrary `WHERE` post-filtering;
- query-time knobs such as `diskann.query_search_list_size` and
  `diskann.query_rescore`;
- relaxed ordering by default, with materialized CTE reordering recommended
  when strict final distance order is required;
- no UNLOGGED-table index support.

This is not a direct replacement for our ADC scorer. It is a PostgreSQL graph
index baseline that should be compared at the product level.

Upstream source: <https://github.com/timescale/pgvectorscale>

## Track A: SIMD ADC Optimization

### A1. PQ ADC

Do not optimize `svec_pq_adc_lookup(...)` first if the measured path uses
`svec_ann_scan(...)`, because the standalone SQL function may not be the hot
path.

Required first measurement:

- `svec_ann_scan(...)` phase timing with `sorted_heap.ann_timing = on`;
- candidate count;
- `M`;
- ADC time vs SPI fetch vs rerank.

Candidate optimizations:

- unroll scalar PQ ADC for common `M`;
- accumulate in `float` then widen once if quality is unchanged;
- add NEON/AVX gather variants only behind compile-time/runtime dispatch;
- keep scalar fallback as reference.

Parity gate:

- exact same top-k order or bounded score delta against scalar reference;
- run with at least two `M` values and two dimensions.

### A2. FlashHadamard ADC

Continue the existing platform-specific path:

- keep NEON int16 behind `FH_INT16=1` until larger query/dataset validation;
- integrate AVX2 gather only if it beats current engine path, not only the
  standalone microbench;
- keep AVX2 int16 refuted unless fresh evidence contradicts it.

Parity gate:

- compare top-k overlap, hit@1, recall@10, and score delta against the scalar
  packed scorer;
- benchmark end-to-end, not only kernel throughput.

## Track B: pgvectorscale DiskANN Benchmark

### Required Methods

Benchmark on the same dataset and query set:

- exact heap ground truth;
- `sorted_hnsw` on `svec` or `hsvec`;
- pgvector HNSW when dimension allows;
- pgvectorscale `diskann`;
- optional FlashHadamard packed exhaustive when a compatible store exists;
- optional IVF-PQ residual when codebooks are trained.

### Required pgvectorscale Settings

Record:

- pgvectorscale version;
- PostgreSQL version;
- `storage_layout`;
- `num_neighbors`;
- `search_list_size`;
- `num_dimensions`;
- `num_bits_per_dimension`;
- `diskann.query_search_list_size`;
- `diskann.query_rescore`;
- whether strict result reordering was applied.

### Required Metrics

- p50, p95, and average latency;
- recall@10 and hit@1 against exact ground truth;
- index size, table size, and total footprint;
- build time;
- memory-related build settings such as `maintenance_work_mem`;
- filter mode: none, label-based, or arbitrary post-filter.

## Acceptance Tests

### D1. pgvectorscale harness is optional and fail-open

If `vectorscale` is not installed, the benchmark should skip the DiskANN row
and report why. It must not make local PostgreSQL tests depend on an external
extension.

### D2. Strict-order mode is explicit

Because pgvectorscale can use relaxed ordering, the harness must record whether
the final result set was reordered by exact distance before recall/latency
reporting.

### D3. SIMD kernels keep scalar fallback

Every SIMD ADC implementation must have:

- scalar reference path;
- platform guard;
- parity test;
- runtime or build-time disable switch.

### D4. Product comparison includes footprint

No benchmark row is accepted without storage footprint. DiskANN, HNSW,
FlashHadamard stores, PQ codebooks, and generated-code columns must all be
counted.

## Adversary Notes

- Kernel microbench wins can disappear inside PostgreSQL due to SPI, tuple
  fetch, TOAST, cache warmup, or rerank overhead.
- Relaxed ordering can make a DiskANN result look faster while returning a
  slightly unsorted top-k; strict-order mode must be visible.
- Filtered DiskANN label support and sorted_heap partition routing solve
  different filtering problems. Compare both only on equivalent predicates.
- SBQ/quantized graph storage and our FlashHadamard/PQ storage have different
  quality/footprint curves. A single recall number is insufficient.
- pgvectorscale is an external moving target; benchmark docs must record the
  exact version or commit.

## Decision

For `0.13`, keep SIMD ADC and pgvectorscale DiskANN comparison as benchmark
tracks. Do not claim superiority until a shared harness reports quality,
latency, footprint, build cost, and strict-order behavior on the same workload.
