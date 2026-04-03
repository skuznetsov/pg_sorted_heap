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

## Validated Operating Points

Full Gutenberg benchmark (103210 vectors, 2880 dimensions, cosine, k=10,
4 bits/dim):

| Lane | hit@1 | recall@10 | p50 ms | metadata | status |
|------|-------|-----------|--------|----------|--------|
| float32 exact | 100% | 100% | 17.3 | - | baseline |
| **FlashHadamard-16** (block16_packed4_topk) | 80% | **89.4%** | **8.0** | 0.8 KB | best recall exhaustive |
| **FlashHadamard-Base** (blockhadamard_packed4_topk) | **86%** | 87.8% | 7.9 | 0.1 KB | best hit@1 exhaustive |
| IVF32 nprobe=4 | 82% | 86.2% | 7.8 | 371 KB | not recommended |
| SQ8 linear | ~99% | ~99% | - | 0 | quality ceiling (8× more bytes) |
| PQ k-means (16 sub) | ~45% | ~67% | - | codebooks | baseline |

Robustness: stable across seeds 42/123/7/999 on vetted 50-query subset.
200-query runs confirm 50-query results within noise (see consumer-plan.md).

Compression: 4 bits/dim = 1440 bytes/vec for 2880D (8× less than float32,
4× less than SQ8). Metadata is negligible (seed + 90 group scales for
FlashHadamard-16).

Latency breakdown (vetted Gutenberg, packed4 helper with 8 threads):
- Query transform (Hadamard rotation): ~0.15 ms
- Byte-table build in C: ~0.23 ms
- Packed scoring + top-k: ~6 ms (dominates)
- Total: ~8 ms

**Naming:**
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

Sweet spot: **M=12 with SQ8 rerank** — 100% hit@1, 96.2% recall@10,
11.5ms p50, 4320 bytes/vec (3× compression vs float32).

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

## Open Work

1. **Larger datasets.** IVF becomes relevant at 500K+ vectors. The centroid
   caching infrastructure is ready but not yet tested at that scale.
2. **Higher dimensions.** Synthetic scaling to 65536D shows the approach
   remains tractable, but real recall numbers at very high dim are missing.
3. **Official TurboQuant comparison.** No public implementation was
   available at time of writing.
4. **Diversity-based memory routing.** The naive similarity routing failed.
   Coverage/MMR-based selection might succeed but is untested.
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
