# FlashHadamard: Structured Packed Retrieval Without Codebooks

## Thesis

FlashHadamard is a no-codebook vector quantization family for approximate
nearest neighbor search. It uses structured block-Hadamard rotation to
decorrelate vector coordinates, followed by 4-bit scalar quantization with
Gaussian Lloyd-Max levels, packed into nibble-transposed storage for fast
ADC scoring via a fused C helper with per-thread top-k selection.

FlashHadamard achieves 87–89% recall@10 at 4 bits/dim on a real 103K×2880D
retrieval workload (Gutenberg embeddings), with 8ms query latency and
effectively zero metadata overhead. It requires no codebook training,
making it fully online and data-oblivious.

FlashHadamard is **not** a general-purpose KV-cache compression method, not
a replacement for SQ8 on quality-sensitive workloads, and not yet compared
against the official Google TurboQuant implementation.

## Core Contribution

**Structured rotation.** A seed-derived random permutation + random sign
flip + block Fast Walsh-Hadamard Transform (FWHT) decorrelates coordinates
in O(d log d) with O(1) metadata (just the seed). This replaces the dense
random orthogonal matrix used in the original TurboQuant MSE path, which
requires O(d²) metadata storage.

**Group RMS equalization.** After rotation, coordinates are grouped (e.g.
groups of 16) and each group is scaled by its RMS to normalize variance
before quantization. This improves hit@1 without losing recall@10 when the
group size is tuned (group_size=16 is the current best on 2880D).

**Packed nibble ADC.** 4-bit codes are packed two per byte and stored in
transposed layout (byte-major, row-minor) for cache-friendly sequential
access during scoring. Query coefficients are folded with group scales at
search time, so the packed scorer is shared across all blockwise variants.

**Fused C helper.** A standalone C library (`turboquant_packed_adc.c`)
implements the hot scoring path: per-query byte-table build, 2-byte-fused
transposed scorer with 4× row unrolling, optional multi-threaded row
sharding, and fused per-thread top-k with merge. No external dependencies.

## Canonical Operating Points

**Setup:** Full Gutenberg, 103210 base vectors (50 held out as queries),
2880 dimensions, cosine metric, k=10, seed=42, 8-thread C helper,
Apple M2 Max.

### Single-stage (packed-only)

| Lane | hit@1 | recall@10 | p50 ms | bytes/vec | bits/dim |
|------|-------|-----------|--------|-----------|----------|
| FlashHadamard-16 (topk) | 80% | **89.4%** | 8.7 | 1440 | 4 |
| FlashHadamard-Base (topk) | **86%** | 87.8% | 7.9 | 1440 | 4 |

### Two-stage (packed shortlist → rerank)

| Config | hit@1 | recall@10 | p50 ms | avg ms | p95 ms | bytes/vec | bits/dim |
|--------|-------|-----------|--------|--------|--------|-----------|----------|
| **SQ8 rerank M=12** | **94%** | **92.8%** | **9.2** | **9.4** | **11.0** | **4320** | **12** |
| SQ8 rerank M=20 | 94% | 95.8% | 11.4 | 11.8 | 15.4 | 4320 | 12 |
| FP32 rerank M=20 | 100% | 99.6% | 11.4 | 11.7 | 14.2 | (resident fp32) | - |

### Reference baselines

| Method | hit@1 | recall@10 | bytes/vec |
|--------|-------|-----------|-----------|
| float32 exact | 100% | 100% | 11520 |
| SQ8 linear | ~99% | ~99% | 2880 |
| PQ k-means | ~45% | ~67% | 16 + codebooks |
| IVF32 nprobe=4 | 82% | 86.2% | 1444 + centroids |

### Recommended serving point

**FlashHadamard-16 + SQ8 rerank M=12**: best current exhaustive CPU
operating point. 94% hit@1, 92.8% recall@10, 9.2ms p50, 4320 bytes/vec
(12 bits/dim = 2.7× compression vs float32).

For applications where hit@1 matters most and storage is tight, packed-only
FlashHadamard-Base at 4 bits/dim provides 86% hit@1 at 7.9ms.

### Latency breakdown

| Stage | p50 ms | Share |
|-------|--------|-------|
| Hadamard transform | 0.15 | 2% |
| C packed scorer (8 threads) | 6.6 | 72% |
| Argpartition / top-k | 0.9 | 10% |
| SQ8 gather + rerank | 0.5 | 5% |
| Python overhead | 1.0 | 11% |
| **Total** | **~9.2** | |

The packed scorer is the dominant cost (~6.6ms), scanning 103K × 1440 bytes
= 149MB per query. This is the current dominant cost under the present
exhaustive CPU layout, not a fundamental limit. Shortlist size M has
minimal impact on latency (8.5-9.5ms for M=8-20) because the scorer cost
is fixed. Rerank overhead is negligible (<0.5ms for M=12).

### Robustness

Stable across seeds 42/123/7/999 on vetted 50-query subset. 200-query
runs confirm 50-query results within noise (see consumer-plan.md).
Earlier numbers in the evidence trail used different setups (non-TopK inner,
different holdout splits); the canonical numbers above supersede them.

### Naming

- **FlashHadamard** — the family
- **FlashHadamard-16** — group_size=16, best recall@10
- **FlashHadamard-Base** — no group scaling, best hit@1

## Negative Results

| Branch | Result | Reason | Dataset |
|--------|--------|--------|---------|
| IVF at 103K | Not recommended | Exhaustive packed topk (8ms) matches or beats all IVF nprobe settings at comparable recall | Gutenberg 103K×2880D |
| Packed block32_dither | Refuted | Per-row dither correction essential; dropping it hurts quality vs non-dithered | Gutenberg 103K×2880D |
| Residual QJL (turboquant_prod) | Refuted | Underperforms simpler MSE path on real code-graph workload at 2–4 bits | Code-graph 1124×768D |
| Tail-similarity memory routing | Refuted | Random block selection outperforms sketch-based routing; similarity selects redundant context, not complementary | 3 Gutenberg novels, Gemma 3 4B |
| Per-dimension whitening | Refuted | Underperforms plain block-Hadamard on recall@10 across 2–4 bits | Code-graph 1124×768D |
| Companding (power β=0.75) | Refuted | Worse than block-Hadamard at all tested bit rates | Code-graph 1124×768D |
| D4 lattice quantization | Refuted | Worse recall + 25× slower encode in Python | Code-graph 1124×768D |

## Interpretation

The current evidence supports FlashHadamard as a **retrieval quantization
family** — a way to compress and search high-dimensional vectors without
codebook training. It is not yet validated as a general-purpose vector
search engine replacement, a KV-cache compression method, or superior to
the official TurboQuant implementation.

The strongest practical story is: for embeddings in the 768–2880 dimension
range, at 4 bits/dim, FlashHadamard provides 87–89% recall@10 with 8ms
query latency and zero training cost. This is competitive with traditional
PQ (which requires codebook fitting) and much cheaper in metadata than
dense random rotation approaches.

## Two-Stage Rerank

The packed quantizer's errors concentrate at the top-k boundary. A cheap
second-stage rerank on a small shortlist fixes most of them.

**FP32 rerank (quality ceiling, not honest storage):**

| M (shortlist) | hit@1 | recall@10 | p50 ms |
|---------------|-------|-----------|--------|
| no rerank | 80% | 89.4% | 6.8 |
| M=20 | 100% | 99.6% | 11.3 |
| M=50 | 100% | 100% | 23.5 |

**SQ8 rerank (honest storage: 12 bits/dim total):**

| M | hit@1 | recall@10 | p50 ms | p95 ms | bytes/vec |
|---|-------|-----------|--------|--------|-----------|
| 8 | 100% | 78.2% | 10.7 | 19.4 | 4320 |
| 12 | 100% | 96.2% | 11.5 | 20.0 | 4320 |
| 16 | 100% | 97.0% | 12.1 | 21.8 | 4320 |
| 20 | 100% | 97.6% | 12.3 | 23.8 | 4320 |

Sweet spot: **M=12 with SQ8 rerank** — 94% hit@1, 92.8% recall@10,
8.9ms p50 (with fused TopK inner), 4320 bytes/vec (3× compression).

Shortlist size has minimal latency impact (8.5-9.5ms for M=8-20) because
the packed scorer cost is fixed at ~6.6ms regardless of M. Reducing M
saves rerank time (<0.5ms) but not scorer time.

**Latency breakdown (SQ8 rerank M=12 on full 103K):**

| Stage | p50 ms | Share |
|-------|--------|-------|
| Hadamard transform | 0.15 | 1% |
| C packed scorer (8 threads) | 6.6 | 89% |
| SQ8 gather + rerank + topk | 0.5 | 7% |
| Python overhead | 0.3 | 3% |
| **Total** | **~7.5** | |

The packed scorer dominates (6.6ms). It scans 103K × 1440 bytes = 149MB
per query. M2 Max theoretical bandwidth floor: ~0.75ms. The 6.6ms includes
per-byte LUT lookups, thread sync, and cache effects. Rerank overhead is
negligible (<0.5ms for M=12).

Fusing rerank into C would save ~0.3ms Python overhead. This is the
current floor for exhaustive CPU full-scan on this scorer layout, not
a fundamental limit. Paths to materially faster serving:
- Fewer candidates scanned (IVF at 500K+ scale, pre-filtering)
- Fewer bytes per query (lower dim, coarser first stage)
- Different execution substrate (GPU scorer via Metal)
- Smaller shortlist M (if quality holds at M=8)

Note: the 12 bits/dim serving footprint (4-bit packed + 8-bit SQ8 rerank
payload) is no longer a pure 4-bit lane. This is a two-tier storage
tradeoff: fast packed scan for shortlisting + higher-fidelity SQ8 for
boundary disambiguation.

## Engine Integration Status

PG extension prototype (`flashhadamard_build` + `flashhadamard_scan`):

| Scale | Build | Scan | Notes |
|-------|-------|------|-------|
| 5 × 8D | <1ms | 0.17ms | Correctness verified |
| 1000 × 2880D | 63ms | 1.6ms | Local PG |
| 5000 × 2880D | 290ms | 34ms | Chunked (2 segments) |
| 103K × 2880D (SPI+TOAST) | 17.2s | 185-700ms | Chunked sidecar, local PG |
| **103K × 2880D (mmap store)** | **10s** | **49ms** | **Raw file store, mmap, fused top-k** |

**Engine scan evolution (103K × 2880D, warm, pre-fetched query):**

| Path | Scan | vs benchmark | Change |
|------|------|--------------|--------|
| SPI + TOAST chunked sidecar | 185-700ms | 23-87× | initial |
| Raw file read() store | 62ms | 7.7× | -68% from raw I/O |
| **mmap + fused top-k** | **49ms** | **6.1×** | **-21% from mmap + fused** |
| Python C helper (8 threads) | ~8ms | 1× | benchmark |

**Final engine numbers (103K × 2880D, warm, pre-fetched query):**

| Path | p50 ms | Notes |
|------|--------|-------|
| SPI + TOAST sidecar | 185-700 | initial prototype |
| mmap single-thread | 41 | kernel parity with helper 1t (40ms) |
| **mmap + pthread (8t)** | **5-8** | **beats Python helper path (8.7ms)** |

Apples-to-apples (same corpus, same query, 8 threads):
- PG engine mmap: 6.9ms p50
- Python C helper: 8.7ms p50
- Engine wins by ~1.3× (no Python/ctypes overhead)

Caveats:
- "Beats helper" means the Python-wrapped benchmark path, not a raw C
  apples-to-apples outside the harness
- pthread inside PG backend is experimental: no PG cancel/error
  integration, no signal handling in worker threads
- Partial thread-launch failure handled correctly (inline tail scoring)

**Refuted optimizations:**
- Per-row early-exit pruning: 0% rows pruned after Hadamard (bounds too loose)
- NEON vtbl nibble lookup: doesn't apply to 256-entry float byte tables
- Segment pruning at 103K scale: overhead exceeds savings (see below)

## Segment Pruning (Research, Not Recommended at 103K)

Per-segment centroids in FH search space. Content-sorted store.
Parity gate: nprobe=n_segments = exhaustive (100% recall, by construction).

| nprobe | segments | recall@10 | Notes |
|--------|----------|-----------|-------|
| 4 | 15% | 93.5% | real pruning quality |
| 8-20 | 31-77% | 93.5% | flat — not probe-count dependent |
| 26 | 100% | 100% | parity gate PASS |

Verdict:
- 6.5% recall loss is real, from boundary vectors in low-scoring segments
- Flat frontier means **segment design is the bottleneck**, not probe count
- Thread-per-segment overhead eats latency savings at 103K
- Exhaustive parallel (5-8ms) already faster than pruned parallel

Not recommended at current scale. Becomes relevant at 500K+ where
fewer probed segments = materially less data to scan.

Next research direction (not current scope):
- Better segment construction (k-means in FH space, multi-centroid)
- Integer/NEON kernel redesign
- PG-native parallel model (replace experimental pthread)

## Status

**Recommended 103K serving path: exhaustive parallel (5-8ms).**

Single-thread scorer at kernel parity with C helper (40ms).
Parallel 8-thread scorer beats Python helper path (6.9ms vs 8.7ms).
Segment pruning is research-only — not recommended at this scale.

## Reproducibility

```sh
# Regression tests (5 tests: correctness, dim mismatch, missing store, score, medium build)
PGDATABASE=fh_test make test-flashhadamard

# Benchmark (requires gutenberg_local table with 103K × 2880D vectors)
PGDATABASE=fh_test make bench-flashhadamard

# Thread control (set before PG start)
export FH_THREADS=8   # default; 1-16 supported
```

Source:
- Engine: `src/flashhadamard.c`, `src/flashhadamard_store.c`
- Headers: `src/flashhadamard.h`, `src/flashhadamard_store.h`
- SQL: `sql/flashhadamard_experimental.sql`
- Tests: `scripts/test_flashhadamard.sql`
- Python harness: `scripts/bench_turboquant_retrieval.py`
- C helper: `scripts/turboquant_packed_adc.c`

## Open Work (Research Only)

1. **Better segment construction.** FH-space k-means, multi-centroid
   summaries. Gate: low nprobe must beat 93.5% recall.
2. **Integer/SIMD kernel.** Outside PG first, ARM NEON, then Intel AVX.
3. **Larger scale (500K+).** Where pruning and IVF become relevant.
4. **PG-native parallel model.** Replace experimental pthread.
5. **Official TurboQuant comparison.** No public implementation was
   available at time of writing.
4. **Diversity-based memory routing.** The naive similarity routing failed.
   Coverage/MMR-based selection might succeed but is untested. (Parked.)
5. **Engine integration.** The current evaluator + C helper is a research
   harness, not a production index path. Integration into pg_sorted_heap
   as a native quantization mode is the logical next step.

## Reproducibility

All results use the repo-owned evaluator and Gutenberg dataset:

```sh
# Exhaustive packed lanes
make bench-turboquant-gutenberg-vetted \
  TURBOQUANT_GUTENBERG_METHODS="turboquant_block16_packed4_topk,turboquant_blockhadamard_packed4_topk"

# IVF sweep with cache
make bench-turboquant-gutenberg-vetted \
  TURBOQUANT_GUTENBERG_METHODS="turboquant_ivf32_block32_packed4" \
  TURBOQUANT_ARGS="--ivf-cache-dir /tmp/ivf_cache --ivf-nprobe 8"

# Screening harness for packed parity
make bench-turboquant-gutenberg-screen
```

Source:
- Evaluator: `scripts/bench_turboquant_retrieval.py`
- C helper: `scripts/turboquant_packed_adc.c`
- Screen: `scripts/bench_turboquant_packed_screen.py`
- Evidence trail: `docs/turboquant-consumer-plan.md`
