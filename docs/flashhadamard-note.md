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

**HIGGS-compatible byte-grid extension.** The packed layout can also host an
experimental HIGGS-inspired p=2 quantizer: one byte indexes a joint 2D Gaussian
grid instead of two independent scalar 4-bit Lloyd-Max codes. This keeps the
same 1 byte / 2 dimensions storage shape, while changing only the encoder and
per-query byte-table construction.

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

### FlashHIGGS2 research snapshot

FlashHIGGS2 is a research lane inspired by HIGGS (`arXiv:2411.17525`). It
tests whether the existing FlashHadamard byte layout can be strengthened by
replacing two independent scalar nibbles with a single joint p=2 Gaussian grid
code. In local Gaussian sanity testing, the p=2 grid reduced MSE per dimension
by about 15.3% versus scalar p=1 Lloyd-Max.

Important caveat: this repo's first FlashHIGGS2 spike uses deterministic
MiniBatchKMeans over `N(0, I_2)` as a CLVQ approximation. The HIGGS paper uses
Gaussian MSE-optimal grids computed by CLVQ and notes that `p=2` is preferable
to `p=1` for their FLUTE-grid setting. Therefore this lane validates the
systems shape first (same byte layout, different byte table), not exact
paper-faithful HIGGS grid quality.

Full Gutenberg result (`103260 x 2880D`, 50 reverse-order queries, cosine,
seed 42, `TURBOQUANT_ADC_THREADS=8`):

| Lane | hit@1 | recall@10 | p50 ms | avg ms | bytes/vec | bits/dim |
|------|------:|----------:|-------:|-------:|----------:|---------:|
| FlashHadamard-16 packed | 100.0 | 88.6 | 6.676 | 6.746 | 1440 | 4 |
| FlashHIGGS2 packed | 100.0 | 91.0 | 7.284 | 7.328 | 1440 | 4 |
| FlashHadamard-16 + SQ8 M=12 | 100.0 | 93.8 | 10.385 | 10.701 | 4320 | 12 |
| FlashHIGGS2 + SQ8 M=12 | 100.0 | 95.8 | 10.354 | 10.388 | 4320 | 12 |
| FlashHadamard-16 + SQ8 M=20 | 100.0 | 98.0 | 10.102 | 10.419 | 4320 | 12 |
| FlashHIGGS2 + SQ8 M=20 | 100.0 | 98.0 | 10.624 | 10.734 | 4320 | 12 |

Follow-up 4-seed quality sweep (`103260 x 2880D`, 200 fixed queries, seeds
42/123/7/999, M sweep reusing one fitted inner quantizer per family):

| M | scalar recall@10 | HIGGS2 recall@10 | delta |
|---:|---:|---:|---:|
| 8 | 76.67 | 77.33 | +0.65 |
| 10 | 89.79 | 91.12 | +1.34 |
| 12 | 94.10 | 94.89 | +0.79 |
| 14 | 95.74 | 96.08 | +0.34 |
| 16 | 96.38 | 96.56 | +0.19 |
| 18 | 96.70 | 96.85 | +0.15 |
| 20 | 96.88 | 96.99 | +0.11 |

The HIGGS2 quality gain is consistent but modest and concentrated at smaller
shortlists. Latency from these full sweeps is intentionally not treated as a
product claim: repeated local runs moved by more than 2x under host
load/thermal/memory pressure. HIGGS2 now has a chunked encoder and a C-side
byte-table + fused top-k path, but latency needs a controlled raw-kernel or
single-run benchmark before making speed claims.

Controlled in-memory latency check after fit (`seed=42`, `k=20`, 200 queries,
1 warmup run, 3 measured runs, C helper, `TURBOQUANT_ADC_THREADS=8`):

| lane | fit ms | measured searches | p50 ms | p90 ms | p95 ms | avg ms |
|---|---:|---:|---:|---:|---:|---:|
| scalar FlashHadamard-16 | 27710.8 | 600 | 7.179 | 8.522 | 8.955 | 7.248 |
| FlashHIGGS2 | 27374.8 | 600 | 7.025 | 8.460 | 8.895 | 7.186 |

Interpretation: the C-side HIGGS2 byte-table path removes the obvious Python
table-build penalty. HIGGS2 reached latency parity with scalar in this
controlled run, but one HIGGS2 max-latency outlier was observed, so this is
still a parity signal rather than a production speed claim.

Interpretation: FlashHIGGS2 is a better candidate generator at smaller
shortlists, not a universally better reranker. The initial single-seed snapshot
showed a larger `M=12` gain (+2.0 recall@10), while the 4-seed 200-query sweep
shows a smaller but more robust gain (+0.79 at M=12, +1.34 at M=10). At `M=20`,
the scalar shortlist is already near saturation and HIGGS2 adds only +0.11
recall@10. Encode is currently slower because p=2 assignment uses nearest-grid
lookup; this is acceptable for a research lane but not yet a default engine
path. The current encoder chunks the grid assignment to avoid materializing all
dimension-pairs at once, and the search path builds HIGGS2 byte tables in C
before the same fused top-k scorer.

Decision: keep FlashHIGGS2 as an experimental benchmark/quantizer option. Do
not promote it to the PG engine until a larger query set and preferably a
raw-kernel latency benchmark prove that a smaller HIGGS2 shortlist can replace
a larger scalar shortlist at equal or lower end-to-end latency.
The evaluator exposes these sweep lanes as
`turboquant_block16_{higgs2_,}packed4_sq8rerank{M}` for
`M in {8,10,12,14,16,18,20,50,100}`.
For full Gutenberg sweeps, prefer `scripts/bench_flashhiggs2_sweep.py`;
it fits the scalar and HIGGS2 inner quantizers once each, then reuses the
same max-M shortlist for all SQ8 rerank M values.

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
| FH-space RP-sort segments | Refuted | 1D random projection of 2880D vectors loses cluster structure; recall@10 = 20.8% at nprobe=4 vs 93.5% content-sort baseline. | Gutenberg 103K×2880D |
| FH-space k-means segments | Refuted | 26-cluster Lloyd's k-means in FH equalized-rotated space; recall@10 = 54.0% at nprobe=4. 2.6× better than RP-sort but still far below 93.5% content-sort baseline. FH space doesn't cluster well enough for segment pruning at 2880D. | Gutenberg 103K×2880D |
| AVX2 int16 kernel (Intel) | Refuted | Same 256-entry int16 LUT approach as NEON, but 2× SLOWER on Intel Xeon Platinum 8223CL (110ms vs 57ms float). The NEON win is Apple Silicon-specific (vaddw_s16 microarchitecture advantage). On Intel, the compiler's float path + AVX auto-vectorization is already strong; int16 LUT lookups don't improve cache utilization enough to offset the widening overhead. | Synthetic 103K×2880D, c5.large |

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

## NEON Int16 Kernel (Experimental)

**Status:** Integrated behind `FH_INT16=1` env var. Promising, partially validated.

**Mechanism:** 256-entry int16 LUTs (quantized from float at scale=2048) + NEON
`vaddw_s16` widening accumulation in int32. Converts to float for top-k + SQ8 rerank.
Works in both single-thread and multi-thread paths.

**End-to-end SQ8-reranked quality (50 Gutenberg queries, vs 8t float baseline):**

| Config | hit@1 | top-10 overlap | max reranked score diff |
|--------|-------|----------------|------------------------|
| 1t float | 50/50 | 100.0% | - |
| 1t int16 | 50/50 | 100.0% | 0.00000000 |
| 8t int16 | 50/50 | 77.6% | 0.00000000 |

**Latency (10 queries, best of 3 rounds):**

| Config | Total | Per query | vs float |
|--------|-------|-----------|----------|
| 1t float | 404 ms | 40.4 ms | baseline |
| 1t int16 | 368 ms | 36.8 ms | -8.9% |
| 8t float | 77 ms | 7.7 ms | baseline |
| 8t int16 | 70 ms | 7.0 ms | -9.0% |

**Why it works:** Smaller LUT entries (2 bytes vs 4) → better L1 cache utilization.
Zero clipping at scale=2048 on real query coefficients. Standalone microbench shows
14% kernel-only improvement; end-to-end is 9% due to fixed overhead (SQ8 rerank,
table quantization, PG runtime).

**Reproduce:**
```sh
# Build store (once)
psql -d fh_test -c "SELECT flashhadamard_store_build('gutenberg_local'::regclass,
  'embedding', '/tmp/fh_gutenberg.store', 42, 4)"

# For each config: stop PG, export vars, start PG, run quality script
#   Configs: FH_THREADS=1, FH_THREADS=1 FH_INT16=1,
#            FH_THREADS=8, FH_THREADS=8 FH_INT16=1
psql -d fh_test -v config=8t_float -f scripts/bench_fh_int16_quality.sql
# ... restart with FH_INT16=1, repeat with config=8t_int16 ...

# Compare all configs (tables persist across sessions)
psql -d fh_test -f scripts/bench_fh_int16_compare.sql
```

**Caveats:**
- 8t int16 top-10 overlap is 77.6% — int16 shortlist differs at the boundary.
  Reranked scores for overlapping results are identical (zero diff).
- 1t int16 is quality-identical to float (100% overlap, zero diff).
- Standalone kernel microbench (`scripts/bench_fh_kernel.c`) shows larger speedup
  (14-19%) than end-to-end (9%) — the gap is fixed per-query overhead.
- Not yet default. 50 queries on one dataset is a start, not comprehensive validation.
- **Intel/AVX2 int16: refuted.** Same int16 LUT approach is 2× slower on Xeon.
  The NEON int16 win is Apple Silicon-specific.
- **Intel/AVX2 float gather: promising.** `_mm256_i32gather_ps` gives 28% kernel
  speedup on Xeon (54.4→39.1ms). Not yet integrated into engine. Zero quality
  risk (bit-identical float output). See `scripts/bench_fh_kernel.c` k4.

## Open Work (Research Only)

1. **Segment construction.** Both RP-sort and FH-space k-means refuted.
   Content-sort (93.5%) remains best. Pruning branch is **parked** at 103K scale.
   Infrastructure preserved: nprobe SQL parameter, offline k-means script.
   Revisit at 500K+ where exhaustive becomes expensive.
2. **SIMD kernel candidates (standalone microbench, not yet in engine).**
   - Apple NEON int16 LUT: -17% kernel (integrated as `FH_INT16=1`, ~9% e2e)
   - Intel AVX2 gather (`_mm256_i32gather_ps`): **-28% kernel** (not yet integrated)
   - Intel AVX2 int16 LUT: refuted (+92%, slower)
   - See `scripts/bench_fh_kernel.c` for all variants and repro commands.
3. **Larger scale (500K+).** Where pruning and IVF become relevant.
4. **PG-native parallel model.** Replace experimental pthread.
5. **Official TurboQuant comparison.** No public implementation was
   available at time of writing.
6. **Pruned ranges scorer anomaly.** `fh_packed_score_ranges_topk` has a
   parked correctness anomaly when `n_ranges == shortlist_m` and around
   `FH_MAX_THREADS`: on the Gutenberg 103K store, `shortlist_m=12,nprobe=12`
   dropped recall@10 to roughly 3% while adjacent `nprobe` values behaved
   normally. This does not affect the current exhaustive operating point or the
   parity gate where `nprobe >= n_segments` routes to the canonical exhaustive
   scorer, but it must be fixed before any future pruned-search promotion.
   Start at `src/flashhadamard.c` in `fh_packed_score_ranges_topk`; the same
   parked branch also lost inline-fallback results when `n_ranges` exceeded
   `FH_MAX_THREADS` because `*filled = 0` was reset before thread merge.
   Repro: build the experimental store, compare 50-query ground truth from
   `flashhadamard_store_scan(..., nprobe := 100)` against
   `nprobe := 12, shortlist_m := 12`.
7. **Diversity-based memory routing.** The naive similarity routing failed.
   Coverage/MMR-based selection might succeed but is untested. (Parked.)
8. **Engine integration.** The current evaluator + C helper is a research
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
