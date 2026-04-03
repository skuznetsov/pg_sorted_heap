---
layout: default
title: TurboQuant + Consumer Plan
nav_order: 11
---

# TurboQuant + consumer-first `0.13` plan

This note is repo-owned memory for two related decisions:

1. how to try TurboQuant without destabilizing the current stable AM path
2. how to expand the unpublished `0.13` toward the first real Cogniformerus
   consumer instead of more routing/control-plane infrastructure

It exists because local `cfmem` is currently unavailable on this machine
(`libggml.0.dylib` missing), so these ideas need a durable in-repo anchor.

## TurboQuant: implementation notes

### Primary sources

- Google Research blog, 2026-03-24:
  `https://research.google/blog/turboquant-redefining-ai-efficiency-with-extreme-compression/`
- TurboQuant paper:
  `https://arxiv.org/abs/2504.19874`

### What matters for this repo

The attractive properties are:

- online/data-oblivious vector quantization
- near-zero indexing/training cost compared with codebook-heavy PQ-style paths
- claims for both vector search and KV-cache compression

The dangerous temptation is to jam it directly into the stable
`sorted_hnsw`/GraphRAG path before we know whether it helps the real consumer.

### Chosen stance

Treat TurboQuant as an **experimental retrieval compression mode**, not as a
new stable storage/index mode and not as a KV-cache project.

Do **not** start with:

- `sorted_hnsw` core AM replacement
- shared-cache redesign
- KV-cache kernel/runtime work

Do start with the narrowest experiment lane that can answer:

> does TurboQuant improve the storage/quality/latency tradeoff for the real
> Cogniformerus retrieval workload relative to current `hsvec`, `sq8`, and PQ
> baselines?

### Ordered integration options

#### Option A — first experiment: retrieval-side offline evaluator

Build a Python-side evaluator first, outside the stable AM path.

Suggested location:

- `poc/turboquant_eval.py` or
- `scripts/bench_turboquant_retrieval.py`

Current repo status:

- implemented as `scripts/bench_turboquant_retrieval.py`
- repo-owned entry point: `make bench-turboquant`
- repo-owned SQL entry point: `make bench-turboquant-sql`
- repo-owned repeated-holdout SQL entry point:
  `make bench-turboquant-sql-holdout`
- the evaluator now also supports `--methods method_a,method_b,...`
  so Gutenberg and future workload-specific runs can select exact lanes
  without ad hoc imports or the full research bundle
- structured result capture is supported via `TURBOQUANT_ARGS='--json-out /path/out.json'`
  so later larger real-data runs can be compared without scraping text output
- current scope is intentionally narrow:
  - float32 exact reference
  - float16 baseline
  - SQ8 linear baseline
  - k-means PQ baseline
  - `turboquant_mse` experimental path
- `turboquant_prod` comparator now exists in the evaluator as a bounded
  second-stage QJL residual experiment; it is still evaluator-only and not an
  engine integration candidate
- `turboquant_blockhadamard` comparator now exists as a seed-derived
  sign+permutation+block-Hadamard rotation experiment intended to cut the
  dense rotation metadata cost of `turboquant_mse`
- `turboquant_blockhadamard_whitened` now exists as a diagonal-variance
  equalization experiment on top of the structured block-Hadamard transform
- `turboquant_blockhadamard_block32` now exists as a coarse blockwise-RMS
  equalization experiment on top of the structured block-Hadamard transform
- `turboquant_blockhadamard_packed4` now exists as a kernel-shape packed-ADC
  mirror of plain `blockhadamard`; it is explicit-only and intended to answer
  whether packed nibble lookup can preserve ranking before any low-level kernel
  work exists
- `turboquant_blockhadamard_packed4_topk` now exists as a helper-side
  top-k variant of the packed `blockhadamard` lane; it is explicit-only and
  intended to answer whether eliminating full score materialization plus
  Python-side `argpartition` buys a real end-to-end win on large workloads,
  with exactness tested separately instead of assumed
- a tiny repo-owned C helper now exists for packed ADC scoring:
  - source: `scripts/turboquant_packed_adc.c`
  - explicit build entry point: `make build-turboquant-packed-helper`
  - the evaluator also auto-builds/loads it via `ctypes` when available and
    falls back to Python otherwise
  - the current strongest helper path is byte-major/transposed for the plain
    `blockhadamard_packed4` lane
  - the current evaluator defaults to a coarse multi-threaded packed scorer for
    large searches (`threads=min(8, cpu_count)` unless
    `TURBOQUANT_ADC_THREADS` overrides it)
- `turboquant_block32_dimdither_packed4` now exists as a kernel-friendly
  dimension-only dither analogue for the `block32` family; it avoids
  per-value random dither state so it can be fused later if it earns its keep
- current `turboquant_mse` is only the first-stage MSE path:
  random orthogonal rotation + scalar quantization on rotated coordinates
- the residual `1-bit` QJL inner-product correction stage is **not** implemented
  in this first branch
- SQL-backed inputs are supported, so the evaluator can run on a real
  Cogniformerus-derived embedding set without touching the stable AM path
- verified on 2026-04-02 against one tiny local `halfvec` memory slice
  (`49` base / `10` query, `384D`):
  - `pq_kmeans`: `hit@1=100%`, `recall@5=100%`, `16 B/vec`
  - `turboquant_mse`: `hit@1=100%`, `recall@5=88%`, `196 B/vec`
  This is only a tiny real-data smoke signal, not a broad quality claim.
- because the live local consumer-derived slice is tiny, the harness now also
  supports repeated holdout folds from one shared SQL vector set so the real
  signal can be averaged over multiple random splits instead of one ad hoc cut
- verified on the same local `59`-row slice with `5` holdout folds
  (`49` base / `10` query each fold):
  - `pq_kmeans`: `hit@1=100%`, `recall@5=100%`
  - `turboquant_mse`: `hit@1=90%`, `recall@5=91.2%`
- verified on 2026-04-02 against a larger real Cogniformerus-derived
  code-graph summary set produced by the existing
  `bin/bench_code_graph_perf.cr --keep-table` flow on
  `src/cogniformerus`:
  - `1124` summary vectors total
  - `0` non-finite summary embeddings after the upstream
    `NativeMetalProvider` batch-recovery fix in Cogniformerus
  - repeated holdout (`5` folds, `200` queries/fold, `k=10`) on that
    clean set gave:
    - `pq_kmeans`: `hit@1=45.3%`, `recall@10=66.55%`, `16 B/vec`
    - `turboquant_mse`: `hit@1=86.5%`, `recall@10=91.33%`, `388 B/vec`
    - `sq8_linear`: `hit@1=98.6%`, `recall@10=99.28%`, `768 B/vec`
  Narrow conclusion:
  - the current MSE-only TurboQuant lane still clearly beats the simple PQ
    baseline on this larger real consumer-derived set
  - it still does not beat `sq8_linear` on quality
  - the previous non-finite-row caveat is no longer the blocker; the
    remaining caveat is algorithmic, not data-integrity-related
- verified on the same clean code-graph summary set with a bounded
  `turboquant_prod` bit sweep (`exact + mse + prod` only):
  - `2` bits:
    - `turboquant_mse`: `hit@1=74.1%`, `recall@10=81.03%`
    - `turboquant_prod`: `hit@1=62.5%`, `recall@10=71.04%`
  - `3` bits:
    - `turboquant_mse`: `hit@1=80.6%`, `recall@10=86.34%`
    - `turboquant_prod`: `hit@1=72.7%`, `recall@10=78.25%`
  - `4` bits:
    - `turboquant_mse`: `hit@1=86.5%`, `recall@10=91.33%`
    - `turboquant_prod`: `hit@1=79.8%`, `recall@10=86.13%`
  Narrow conclusion:
  - this dense-Gaussian residual-QJL variant underperforms the simpler
    first-stage MSE path on the real code-graph workload across `2-4` bits
  - it should stay as a negative-reference method in the evaluator, not as
    the next engine-integration hypothesis
- verified on the same clean code-graph summary set with a bounded
  `exact + turboquant_mse + turboquant_blockhadamard` run at `4` bits:
  - `turboquant_mse`: `hit@1=86.5%`, `recall@10=91.04%`,
    `388 B/vec`, `2304.1 KB` metadata, `702.3 ms` encode
  - `turboquant_blockhadamard`: `hit@1=86.1%`, `recall@10=91.13%`,
    `388 B/vec`, effectively `0 KB` metadata, `47.2 ms` encode
  Narrow conclusion:
  - on this real consumer-derived set, structured block-Hadamard rotation
    holds the same practical compression ratio as dense `turboquant_mse`
  - quality is nearly identical on the current holdout, with slightly lower
    `hit@1` but slightly higher `recall@10`
  - this is now the strongest next TurboQuant lane, because it removes the
    evaluator's biggest practical weakness without widening the engine surface
- verified on the same clean code-graph summary set with a bounded
  `exact + mse + blockhadamard + whitened + block32 + prod` bit sweep:
  - `2` bits:
    - `turboquant_blockhadamard`: `hit@1=71.9%`, `recall@10=80.00%`
    - `turboquant_blockhadamard_whitened`: `hit@1=72.6%`, `recall@10=79.45%`
    - `turboquant_blockhadamard_block32`: `hit@1=71.7%`, `recall@10=79.86%`
  - `3` bits:
    - `turboquant_blockhadamard`: `hit@1=81.8%`, `recall@10=86.14%`
    - `turboquant_blockhadamard_whitened`: `hit@1=78.7%`, `recall@10=84.55%`
    - `turboquant_blockhadamard_block32`: `hit@1=80.8%`, `recall@10=86.13%`
  - `4` bits, confirmatory rerun on the compact-metadata implementation:
    - `turboquant_blockhadamard`: `hit@1=86.2%`, `recall@10=91.06%`,
      `384 B/vec`, effectively `0 KB` metadata, `48.1 ms` encode
    - `turboquant_blockhadamard_whitened`: `hit@1=85.9%`, `recall@10=90.13%`,
      `384 B/vec`, `3.0 KB` metadata, `44.3 ms` encode
    - `turboquant_blockhadamard_block32`: `hit@1=86.3%`, `recall@10=91.57%`,
      `384 B/vec`, `0.1 KB` metadata, `55.1 ms` encode
  Narrow conclusion:
  - diagonal whitening is not the right next lane for this workload; it
    underperforms plain `blockhadamard` on real `recall@10` across `2-4` bits
  - coarse blockwise equalization is materially better behaved than
    diagonal whitening
  - `block32` is the current strongest experimental TurboQuant point at
    `4` bits on the real code-graph set: best `recall@10` among the
    evaluated TurboQuant lanes, while preserving tiny metadata and cheap
    encode cost relative to dense `mse`
  - `block32` does not dominate every lower-bit point, so the next likely
    improvement should stay in the no-codebook family rather than return to
    diagonal whitening or residual-QJL work
- verified on the same clean code-graph summary set with bounded
  no-codebook research lanes (`twopass`, `compand`, `dither`, `D4`, and a
  `twopass+dither` synthesis) at `2-4` bits:
  - `2` bits:
    - `turboquant_blockhadamard_twopass`: `hit@1=76.5%`, `recall@10=81.29%`
    - `turboquant_block32_dither`: `hit@1=48.4%`, `recall@10=60.70%`
    - `turboquant_block32_compand`: `hit@1=34.2%`, `recall@10=46.25%`
    - `turboquant_block32_d4`: `hit@1=29.7%`, `recall@10=42.06%`
  - `3` bits:
    - `turboquant_blockhadamard_twopass`: `hit@1=81.5%`, `recall@10=86.50%`
    - `turboquant_block32_dither`: `hit@1=75.9%`, `recall@10=82.55%`
    - `turboquant_block32_compand`: `hit@1=73.1%`, `recall@10=78.35%`
    - `turboquant_block32_d4`: `hit@1=75.9%`, `recall@10=81.53%`
  - `4` bits:
    - `turboquant_blockhadamard_twopass`: `hit@1=88.9%`, `recall@10=91.33%`,
      `0 KB` metadata, `61.3 ms` encode
    - `turboquant_block32_dither`: `hit@1=86.3%`, `recall@10=91.64%`,
      `0.1 KB` metadata, `23.5 ms` encode
    - `turboquant_twopass_block32_dither`: `hit@1=88.5%`, `recall@10=91.60%`,
      `0.1 KB` metadata, `39.9 ms` encode
    - `turboquant_block32_compand`: `hit@1=83.2%`, `recall@10=89.17%`
    - `turboquant_block32_d4`: `hit@1=86.2%`, `recall@10=90.51%`,
      but `1324-1379 ms` encode in the current Python implementation
  Narrow conclusion:
  - the strongest general no-codebook lane is now `twopass structured mixing`;
    it wins at `2` and `3` bits and gives the best `hit@1` at `4` bits
  - the strongest high-rate no-codebook lane is `block32_dither`; at `4` bits
    it gives the best observed `recall@10` while keeping tiny metadata and the
    cheapest encode among the competitive lanes
  - the `twopass+dither` synthesis is a good `4`-bit compromise, but it does
    not clearly dominate `twopass` on `hit@1` or `block32_dither` on
    `recall@10`
  - the current compander and D4 lanes are refuted on this real workload in
    their present form; they are not the next branch to invest in
- one subsequent fresh rebuild of `code_graph_turboquant_eval` failed again
  in the upstream Cogniformerus embedding path with a non-finite batch during
  `code_reindex_graph`; instead of blocking the algorithm loop on that flake,
  the next comparison pass used the valid `308`-row summary subset that had
  already been materialized before the failure
- verified on that partial live summary set (`308` rows, `5` folds,
  `50` queries/fold, `4` bits):
  - `turboquant_twopass_block32`: `hit@1=87.6%`, `recall@10=94.08%`
  - `turboquant_block16_dither_c2.5`: `hit@1=91.2%`, `recall@10=94.36%`
  - `turboquant_block64_dither_c3.0`: `hit@1=88.4%`, `recall@10=94.40%`
  - `turboquant_twopass_block32_dither_c3.0`: `hit@1=91.6%`,
    `recall@10=94.04%`
  Narrow conclusion:
  - there are tuned no-codebook settings that outperform the fixed
    `group_size=32`, `clip=3.0` choices on this live slice
  - `twopass_block32` is a real candidate, not just a theoretical combo
  - but this slice is too narrow to justify promoting these tuned settings to
    new defaults without a broader cross-check
- cross-checked the strongest partial-live candidates on ANN-Benchmarks
  `glove-100` and `nytimes-256` at `4` bits:
  - `turboquant_twopass_block32` did not dominate:
    - `glove-100`: `82.0% hit@1`, `86.2% recall@10`
    - `nytimes-256`: `89.0% hit@1`, `89.9% recall@10`
  - tuned dither settings also failed to generalize cleanly:
    - `block16_dither_c2.5` was competitive on the live slice, but on
      `nytimes-256` it fell to `87.6% recall@10`
    - `block64_dither_c3.0` reached `94.40% recall@10` on the live slice,
      but only `84.6%` on `glove-100` and `86.5%` on `nytimes-256`
  Narrow conclusion:
  - the tuned live-slice winners look workload-specific
  - the strongest robust general lane is still plain `twopass`
  - tuned dither and `twopass_block32` should remain research comparators, not
    new evaluator defaults
- synthetic scaling checks on the strongest structured lanes now extend to
  `65536D`:
  - `turboquant_blockhadamard_block32`: `fit_ms=390.5`, `search_ms=8.43`,
    `meta_kb=8.02`
  - `turboquant_blockhadamard_twopass`: `fit_ms=572.0`, `search_ms=8.74`,
    `meta_kb=0.03`
  - `turboquant_twopass_block32`: `fit_ms=595.9`, `search_ms=30.40`,
    `meta_kb=8.03`
  Narrow conclusion:
  - the surviving structured lanes keep metadata linear in dimension; at
    `65536D`, `block32`-style shared scales are still only about `8 KB`,
    implying about `32 KB` at `262144D`
  - `twopass_block32` currently carries a materially worse search-time
    constant factor at higher dimension, so it is not the next general lane to
    optimize or promote
- repo-owned Gutenberg entry points now exist:
  - `make bench-turboquant-gutenberg-vetted`
  - `make bench-turboquant-gutenberg-screen`
  - `make bench-turboquant-gutenberg-full`
  - both use the current evaluator directly and support `TURBOQUANT_METHODS`
    / `TURBOQUANT_GUTENBERG_METHODS`
  - if `TURBOQUANT_PG_DSN` is unset, they try the local cube fallback via
    `default/pgvector-superuser` and `127.0.0.1:30432/cogniformerus`
- verified on 2026-04-02 against the local Gutenberg cube in
  `cogniformerus.public.gutenberg_gptoss_sh`
  (`103260 x 2880D`, cosine, `k=10`)
  - vetted subset (`50` queries via `bench_hnsw_gt`) reproduced by the
    repo-owned target:
    - `turboquant_mse`: `96.0% hit@1`, `90.60% recall@10`,
      `30948.9 ms` encode, `22.575 ms` p50
    - `turboquant_blockhadamard`: `98.0% hit@1`, `91.20% recall@10`,
      `27813.1 ms` encode, `16.633 ms` p50
    - `turboquant_blockhadamard_twopass`: `98.0% hit@1`,
      `90.40% recall@10`, `44191.6 ms` encode, `17.550 ms` p50
    - `turboquant_block32_dither`: `100.0% hit@1`, `91.80% recall@10`,
      `23090.4 ms` encode, `27.515 ms` p50 in an isolate rerun
  - full stored query set (`200` queries) on the same cube via the narrow
    method-selected evaluator path:
    - `turboquant_mse`: `99.0% hit@1`, `89.55% recall@10`,
      `28851.6 ms` encode, `20.830 ms` p50
    - `turboquant_blockhadamard`: `99.5% hit@1`, `90.60% recall@10`,
      `30030.8 ms` encode, `15.382 ms` p50
    - `turboquant_blockhadamard_twopass`: `99.5% hit@1`,
      `89.00% recall@10`, `43870.0 ms` encode, `14.284 ms` p50
    - `turboquant_block32_dither`: `100.0% hit@1`, `90.95% recall@10`,
      `21276.0 ms` encode, `14.061 ms` p50 in the original direct evaluator run
  Narrow conclusion:
  - Gutenberg does **not** confirm `twopass` as the next default lane
  - plain `blockhadamard` already beats dense `mse` on this workload:
    better recall, better or comparable query latency, negligible metadata
  - `block32_dither` is the strongest current Gutenberg **quality** lane:
    best `hit@1`, best `recall@10`, and cheaper encode than the competing
    structured methods
  - `block32_dither` latency on the local cube shows more run-to-run variance
    than its recall/encode signal, so the current claim is stronger on quality
    than on p50 latency
- verified on 2026-04-02 against the same vetted Gutenberg subset with
  packed kernel-shape prototypes (`50` queries, `103260 x 2880D`, cosine,
  `k=10`):
  - original Python-only packed path:
    - `turboquant_blockhadamard_packed4`: `98.0% hit@1`, `91.20% recall@10`,
      `28474.8 ms` encode, `824.263 ms` p50
  - after the tiny C helper path (`packed_adc_backend=c-helper`):
    - first row-major C helper:
      - `turboquant_blockhadamard`: `98.0% hit@1`, `91.20% recall@10`,
        `27532.1 ms` encode, `17.208 ms` p50
      - `turboquant_blockhadamard_packed4`: `98.0% hit@1`, `91.20% recall@10`,
        `27655.5 ms` encode, `96.090 ms` p50
    - then byte-major/transposed C helper:
      - `turboquant_blockhadamard`: `98.0% hit@1`, `91.20% recall@10`,
        `28624.9 ms` encode, `14.287 ms` p50
      - `turboquant_blockhadamard_packed4`: `98.0% hit@1`, `91.20% recall@10`,
        `27996.0 ms` encode, `55.060 ms` p50
    - then byte-major/transposed + coarse multi-threaded helper
      (`packed_adc_backend=c-helper`, `threads=6`):
      - repeat A:
        - `turboquant_blockhadamard`: `98.0% hit@1`, `91.20% recall@10`,
          `28027.0 ms` encode, `13.936 ms` p50
        - `turboquant_blockhadamard_packed4`: `98.0% hit@1`, `91.20% recall@10`,
          `27522.8 ms` encode, `19.735 ms` p50
      - repeat B:
        - `turboquant_blockhadamard`: `98.0% hit@1`, `91.20% recall@10`,
          `27418.8 ms` encode, `14.018 ms` p50
        - `turboquant_blockhadamard_packed4`: `98.0% hit@1`, `91.20% recall@10`,
          `27479.9 ms` encode, `19.293 ms` p50
  - `turboquant_block32_dither`: `100.0% hit@1`, `91.80% recall@10`,
    `22886.3 ms` encode, `23.821 ms` p50
  - `turboquant_block32_dimdither_packed4`: `96.0% hit@1`, `90.00% recall@10`,
    `24406.1 ms` encode, `921.517 ms` p50
  Narrow conclusion:
  - `blockhadamard_packed4` exactly preserves the plain `blockhadamard`
    ranking on Gutenberg, so the packed nibble-ADC path is algorithmically
    faithful
  - moving from Python ADC to the first row-major C helper cut packed
    `blockhadamard` p50 by about `8.6x`
    (`824.263 ms -> 96.090 ms`) without changing quality
  - moving again to the byte-major/transposed helper cut it by another
    `1.7x` (`96.090 ms -> 55.060 ms`) with the same ranking
  - adding coarse multi-threaded row sharding cut it by another
    `2.8-2.9x` on repeated vetted Gutenberg runs
    (`55.060 ms -> 19.3-19.7 ms`) with the same ranking
  - that leaves packed `blockhadamard` only about `1.4x` slower than plain
    `blockhadamard` on this workload, which is the first point where an engine
    path looks genuinely plausible instead of merely interesting
  - explicit thread sweep on the same vetted Gutenberg target gave:
    - `threads=1`: `52.335 ms` p50
    - `threads=2`: `35.796 ms` p50
    - `threads=4`: `25.743 ms` p50
    - `threads=6`: `19.046 ms` p50
    - `threads=8`: `17.403 ms` p50
    - `threads=12`: `18.818 ms` p50 with worse `avg_ms`
  - narrow conclusion from that sweep:
    - `8` is the best current default on this Apple M-series local box
    - `12` does not help further on the real target, so the next step should
      return to inner-loop work, not add more threads
  - the dimension-only dither analogue does **not** survive Gutenberg:
    it loses both `hit@1` and `recall@10` relative to plain `block32_dither`
  - therefore the next kernelization candidate should stay centered on plain
    `blockhadamard` first, not on the dim-only dither surrogate
  - later local experiments on the same helper produced a useful negative
    pattern: more C-side micro-tuning of the packed scorer itself did not
    give robust wins
    - refuted branches:
      - tiled threaded worker
      - pointer-increment address arithmetic / local-offset rewrite
      - static pthread pool
  - the next real speedup instead came from the Python side: replacing the
    per-query `score_luts_to_byte_tables()` Python loop with vectorized nibble
    table materialization
    - direct `2880 x 16` microbench:
      - old builder: `2.233 ms` p50, `2.251 ms` avg
      - vectorized builder: `1.052 ms` p50, `1.054 ms` avg
      - outputs remained `allclose`
    - packed-only vetted Gutenberg repeats after that change:
      - repeat A:
        - `turboquant_blockhadamard_packed4`: `98.0% hit@1`,
          `91.20% recall@10`, `28321.1 ms` encode, `12.864 ms` p50
      - repeat B:
        - `turboquant_blockhadamard_packed4`: `98.0% hit@1`,
          `91.20% recall@10`, `28173.8 ms` encode, `13.058 ms` p50
    - one mixed-method vetted run on the same code also showed:
      - `turboquant_blockhadamard`: `98.0% hit@1`, `91.20% recall@10`,
        `33314.6 ms` encode, `20.542 ms` p50
      - `turboquant_blockhadamard_packed4`: `98.0% hit@1`,
        `91.20% recall@10`, `33386.9 ms` encode, `12.251 ms` p50
  Narrow conclusion:
  - the remaining packed-path tax was not just in the C helper; per-query LUT
    materialization was a first-class bottleneck
  - after vectorizing that stage, the packed `blockhadamard` lane is now
    consistently around `12.9-13.1 ms` p50 on packed-only vetted Gutenberg
    runs while preserving identical quality
  - this is the first repeatable point where the packed lane is materially
    faster than its earlier `17-20 ms` helper-era plateau, so future kernel
    work should treat LUT build + scoring as one fused path rather than chase
    more helper-thread micro-optimizations in isolation
  - the next narrowing branch tested two more ideas:
    - direct fused nibble scoring in C for `blockhadamard_packed4`
    - static pthread worker pool in the helper
  - both were refuted as next steps:
    - the fused nibble scorer preserved exact scores on adversarial random
      checks (including odd `dim=31`) but did not beat the vectorized
      Python-LUT + generic transposed scorer robustly on Gutenberg
    - the static pool did not produce a stable win over the existing
      create/join model
  - the surviving synthesis was narrower:
    - keep the dedicated `blockhadamard_packed4` helper entry points
    - build the per-query byte tables inside C once per query
    - then dispatch into the already-proven transposed packed scorer
  - adversary equivalence check for this C-built-table path:
    - random `dim=31`, `dim=32`, and `dim=2880` cases all matched the
      old generic LUT path with `allclose=True` and `max_abs=0.0`
  - vetted Gutenberg after this dedicated fused-build path:
    - packed-only repeat A:
      - `turboquant_blockhadamard_packed4`: `98.0% hit@1`,
        `91.20% recall@10`, `29275.6 ms` encode, `11.419 ms` p50
    - packed-only repeat B:
      - `turboquant_blockhadamard_packed4`: `98.0% hit@1`,
        `91.20% recall@10`, `28315.3 ms` encode, `11.216 ms` p50
    - mixed-method vetted run:
      - `turboquant_blockhadamard`: `98.0% hit@1`, `91.20% recall@10`,
        `42190.4 ms` encode, `14.500 ms` p50
      - `turboquant_blockhadamard_packed4`: `98.0% hit@1`,
        `91.20% recall@10`, `30015.4 ms` encode, `10.625 ms` p50
  Narrow conclusion:
  - dedicated in-C byte-table build plus the existing transposed scorer is
    the current strongest packed `blockhadamard` path
  - it now moves the packed lane from the earlier `12.9-13.1 ms` plateau
    down into the `10.6-11.4 ms` band on vetted Gutenberg, while preserving
    identical quality
  - this is the first repeatable point where the packed lane beats plain
    `blockhadamard` on the vetted Gutenberg target, so the next kernel work
    should start from this dedicated fused-build path, not from the refuted
    direct-nibble or thread-pool branches
  - a further narrow cleanup then removed the remaining batch-style temporary
    on the query transform side:
    - added `fwht_vec(...)` and `structured_block_hadamard_vec(...)`
    - switched single-query `blockhadamard` and `blockhadamard_packed4`
      search paths from
      `structured_block_hadamard(query[np.newaxis, ...])[0]`
      to the 1-D fast path
  - adversary equivalence check for the 1-D transform path:
    - random `dim=31`, `dim=32`, and `dim=2880` queries matched the old
      2-D batch path with `allclose=True` and `max_abs=0.0`
    - direct transform microbench on `2880D`:
      - old 2-D path: `0.132 ms` p50
      - new 1-D path: `0.119 ms` p50
  - vetted Gutenberg after the 1-D transform cleanup:
    - packed-only repeat A:
      - `turboquant_blockhadamard_packed4`: `98.0% hit@1`,
        `91.20% recall@10`, `27602.2 ms` encode, `11.113 ms` p50
    - packed-only repeat B:
      - `turboquant_blockhadamard_packed4`: `98.0% hit@1`,
        `91.20% recall@10`, `30029.0 ms` encode, `10.022 ms` p50
    - mixed-method vetted run:
      - `turboquant_blockhadamard`: `98.0% hit@1`, `91.20% recall@10`,
        `27754.3 ms` encode, `17.477 ms` p50
      - `turboquant_blockhadamard_packed4`: `98.0% hit@1`,
        `91.20% recall@10`, `28228.5 ms` encode, `9.583 ms` p50
  Narrow conclusion:
  - this is a smaller win than the fused C-built-table branch, but it is
    still a clean improvement with exact semantics
  - the packed `blockhadamard` lane now lives around the `10-11 ms` band on
    packed-only vetted Gutenberg runs and reached `9.583 ms` in the mixed
    comparison run
  - the remaining hot path is now concentrated even more clearly in the
    packed scorer itself, not in Python query-prep scaffolding
  - to avoid guessing on the next kernel step, the evaluator now also has a
    repo-owned `--profile-packed-stages` mode for
    `turboquant_blockhadamard_packed4`
    - it reports:
      - Python query transform time
      - C byte-table build time
      - C packed scoring time
  - vetted Gutenberg stage-profile repeats:
    - repeat A:
      - `turboquant_blockhadamard_packed4`: `11.717 ms` p50,
        `11.527 ms` avg
      - stage split:
        - `transform=0.155 ms/query`
        - `c_build=0.224 ms/query`
        - `c_score=9.788 ms/query`
    - repeat B:
      - `turboquant_blockhadamard_packed4`: `8.105 ms` p50,
        `8.589 ms` avg
      - stage split:
        - `transform=0.132 ms/query`
        - `c_build=0.227 ms/query`
        - `c_score=6.966 ms/query`
  - several cheap scalar scorer tweaks were then explicitly refuted on the
    same shape before the next win was accepted:
    - `4 -> 8` unroll in the generic transposed scorer preserved exactness,
      but regressed vetted Gutenberg to `11.892 ms` p50 and
      `c_score=10.614 ms/query`
    - fused first-byte initialization was slower on a direct scorer microbench
      (`103260 x 1440` bytes, `threads=8`):
      - baseline: `9.486 ms` p50, `9.284 ms` avg
      - fused-init: `9.926 ms` p50, `9.998 ms` avg
    - local pointer hoisting was also slower on the same microbench:
      - `10.232 ms` p50, `10.135 ms` avg
    - forcing the direct threaded `lo/hi nibble` scorer instead of the current
      `build 256-byte tables -> generic scorer` path was also worse on the
      same screening shape:
      - `10.174 ms` p50, `10.147 ms` avg
  - the next surviving branch fused `2` byte tables per inner pass in the
    generic transposed scorer, so each `out_scores` load/store is amortized
    across two gathers instead of one
    - direct scorer microbench on the same `103260 x 1440`-byte shape:
      - baseline: `9.486 ms` p50, `9.284 ms` avg
      - `2`-byte fusion: `5.430 ms` p50, `5.891 ms` avg
      - checksum changed only by float summation order:
        `-1481.380615 -> -1481.380493`
    - adversary check versus the Python transposed scorer:
      - `dim=31`: `max_abs=1.90734863e-06`, `top10_same=True`
      - `dim=32`: `max_abs=1.90734863e-06`, `top10_same=True`
      - `dim=2880`: `max_abs=0.000148773193`,
        `max_rel=0.000534369086`, `top10_same=True`
    - vetted Gutenberg stage-profile repeats after `2`-byte fusion:
      - repeat A:
        - `turboquant_blockhadamard_packed4`: `7.914 ms` p50,
          `8.082 ms` avg
        - stage split:
          - `transform=0.157 ms/query`
          - `c_build=0.233 ms/query`
          - `c_score=6.351 ms/query`
      - repeat B:
        - `turboquant_blockhadamard_packed4`: `7.796 ms` p50,
          `7.782 ms` avg
        - stage split:
          - `transform=0.151 ms/query`
          - `c_build=0.233 ms/query`
          - `c_score=6.031 ms/query`
  Narrow conclusion:
  - the stage ordering is stable even when absolute latency moves:
    `c_score` dominates, `c_build` is distant second, and query transform is
    small
  - the surviving packed-scorer win so far is **traffic reduction**, not more
    scalar cosmetics: amortizing score-slice RMW over two byte tables helped,
    while unroll, init-fusion, pointer-hoist, and direct `lo/hi` fallback did
    not
  - the next kernelization branch should still target the packed scoring
    loop first, but now it should build on the proven `2`-byte fusion path
    rather than the earlier single-byte generic loop
  - the next surviving branch after that moved helper-side top-k selection into the
    packed helper for `turboquant_blockhadamard_packed4_topk`, so the helper
    now builds byte tables once, scores each row chunk, keeps per-thread
    top-k candidates, and avoids materializing the full score vector before
    Python ranking
    - adversary check versus the current exact packed scorer:
      - `dim=31`: top-k ids identical
      - `dim=32`: top-k ids identical
      - `dim=2880`: top-k ids identical
    - vetted Gutenberg repeats with the same top-level quality metrics:
      - repeat A:
        - `turboquant_blockhadamard_packed4`: `98.0% hit@1`,
          `91.20% recall@10`, `9.726 ms` p50, `9.776 ms` avg
        - `turboquant_blockhadamard_packed4_topk`: `98.0% hit@1`,
          `91.20% recall@10`, `8.518 ms` p50, `8.630 ms` avg
      - repeat B:
        - `turboquant_blockhadamard_packed4`: `98.0% hit@1`,
          `91.20% recall@10`, `9.705 ms` p50, `10.192 ms` avg
        - `turboquant_blockhadamard_packed4_topk`: `98.0% hit@1`,
          `91.20% recall@10`, `7.059 ms` p50, `7.535 ms` avg
  Narrow conclusion:
  - the current strongest packed lane is no longer just exact packed scoring;
    it is packed scoring with helper-side top-k, but real-workload parity
    must be treated as an adversary check rather than assumed exact
  - on the real Gutenberg target this removes roughly `1-2.6 ms/query`
    from the end-to-end path while preserving the same `hit@1` and
    `recall@10` on the vetted Gutenberg benchmark
  - the remaining next kernelization question is now narrower:
    further reduce the helper-side scoring cost, not Python-side selection
  - a follow-up top-k-specific profiler now exists and narrows that further:
    on a profiled vetted Gutenberg comparison, `c_merge` for
    `turboquant_blockhadamard_packed4_topk` was effectively zero
    (`~0.001 ms/query`), while the helper-side `c_score` bucket still
    dominated; a non-profiled rerun on the same tree still kept the top-k
    lane ahead (`6.925 ms` p50 vs `8.294 ms` for plain `packed4`)
  - a repo-owned packed screening harness now exists:
    - `make bench-turboquant-gutenberg-screen`
    - it fits only the packed lanes on the real vetted Gutenberg shape and
      reports direct search latency plus exact-order and same-set mismatch
      counts against the plain packed lane
    - this harness is now the preferred adversary screen before accepting any
      packed-helper micro-optimization, because full evaluator runs are too
      expensive for every tiny helper branch
    - current screen on the real vetted Gutenberg set showed:
      - `packed4_topk`: `5.393 ms` p50 vs `9.747 ms` for plain `packed4`
      - `order_diff=2`
      - `set_diff=1`
    - follow-up screen (2026-04-03) with `tie_only` column confirmed:
      - `packed4`: `10.320 ms` p50
      - `packed4_topk`: `8.322 ms` p50, `order_diff=2`, `set_diff=1`,
        `tie_only=1`
    - since `tie_only == set_diff`, all observed set-membership differences
      on the current vetted Gutenberg run sit on the tie boundary
      (`|score_diff| ≤ 1e-6` for every XOR-different candidate)
    - this strongly supports (but does not universally prove) that the
      `packed4_topk` lane is scoring-exact relative to `packed4` on this
      workload, with mismatch arising only from tie-breaking policy
      differences between C heap-insert order and Python `argpartition`
    - before investing in a tie-aware fix, the repo needs a contract:
      is exact order required, or is same-set / same-metrics sufficient?
    - **chosen helper parity contract (2026-04-03): same-set equivalence**
      - the topk helper must produce the same RESULT SET as the non-topk
        scoring path for the same algorithm and seed
      - gate metric: `set_diff` — must be 0 or tie-only (`tie_only == set_diff`)
      - `order_diff` within top-k is informational, not a gate
      - `recall@k` is a consequence of set equivalence (same set = same recall)
      - `hit@1` is NOT a helper parity metric — it depends on ordering within
        the result set, which legitimately differs between topk heap-insert
        order and argpartition. hit@1 variation from reordering is expected.
      - a `set_diff` NOT on tie boundaries would indicate a scoring bug and
        block acceptance
      - consequence: the current `packed4_topk` and `block16_packed4_topk`
        lanes pass this contract (set_diff=0 on vetted Gutenberg)
    - **separate concern: product operating point selection**
      - which lane to recommend as default is a product decision based on
        end-to-end metrics (recall@k, hit@1, latency) against ground truth
      - this is distinct from helper parity — two lanes with different
        algorithms (e.g. block16 vs blockhadamard) are expected to differ
  Narrow conclusion:
  - the next exact helper branch should not spend time on final candidate
    merge or Python ranking
  - if the packed top-k lane is to improve further, the win has to come from
    the worker-side scan itself or its memory layout
  - the `set_diff=1` mismatch is strongly consistent with tie-only on
    this workload; a deterministic tie-break policy is a nice-to-have,
    not a correctness blocker

- verified on 2026-04-03 against vetted Gutenberg that the dithered-encode
  packed4 variant (`block32_dither_packed4`) does NOT recover dither quality
  when scoring via standard packed ADC without the per-row dither correction:
  - `block32_packed4` (no dither): `100.0% hit@1`, `89.40% recall@10`, `9.8 ms`
  - `block32_dither_packed4` (dithered codes, no correction): `98.0% hit@1`,
    `88.60% recall@10`, `10.1 ms`
  - `block32_dither` (full dithered, non-packed): `100.0% hit@1`,
    `91.80% recall@10`, `17.9 ms`
  Narrow conclusion:
  - dropping the per-row dither correction actively hurts quality because
    dithered code assignment disagrees with the non-corrected LUT decode
  - `block32_dither` is not packable in the current shared-LUT ADC form
  - future paths for packed dither: group-level dither with storable
    correction, or seed-derived on-the-fly correction (high compute cost);
    parked until a representation change is warranted

- **packed lane summary (vetted Gutenberg, 103260 x 2880D, 4 bits, k=10)**:

  | lane                        | hit@1  | recall@10 | p50 ms | role                         |
  |-----------------------------|--------|-----------|--------|------------------------------|
  | blockhadamard_packed4       |  98.0% |    91.20% |   7.6  | best packed recall@10 lane   |
  | blockhadamard_packed4_topk  |  98.0% |    91.20% |   8.3  | (same quality, fused top-k)  |
  | **block16_packed4**         | 100.0% |    91.00% |   7.1  | **best combined lane**       |
  | block32_packed4             | 100.0% |    89.40% |   7.6  | over-equalized, demoted      |
  | block32_dither (non-packed) | 100.0% |    91.80% |  17.2  | best overall quality (dense) |

  | block16_packed4_topk        | 100/98%|    91.00% |   5.7  | **fastest packed lane**      |
  | block32_packed4_topk        | 100.0% |    89.40% |   --   | (available, not primary)     |

  Recommended product operating points:
  - **fastest**: `block16_packed4_topk` — 91.0% recall@10, 5.7ms p50
  - **highest recall**: `blockhadamard_packed4_topk` — 91.2% recall@10, 6.2ms

- robustness pass (2026-04-03, vetted Gutenberg):
  - multi-seed (42/123/7/999) on 50 queries:

    | seed | bh_topk hit@1 | bh_topk r@10 | b16_topk hit@1 | b16_topk r@10 |
    |------|---------------|--------------|----------------|---------------|
    |   42 |         98.0% |       91.20% |          98.0% |        91.00% |
    |  123 |         98.0% |       90.80% |          98.0% |        90.40% |
    |    7 |         98.0% |       90.20% |          98.0% |        90.60% |
    |  999 |        100.0% |       89.60% |          98.0% |        90.00% |
    | mean |         98.5% |       90.45% |          98.0% |        90.50% |

  - full 200 queries at seed=42:
    - `blockhadamard_packed4_topk`: `99.5% hit@1`, `90.60% recall@10`, `6.2 ms`
    - `block16_packed4_topk`: `99.5% hit@1`, `90.75% recall@10`, `8.1 ms`
  - conclusion: both lanes are stable across seeds; the 50-query vetted set
    showed blockhadamard slightly ahead on recall@10, but on the full 200-query
    set block16 is marginally better (90.75% vs 90.60%); the difference is
    within noise for this sample size
  - latency is also within noise between the two lanes (6-8ms range)
  - both lanes remain valid operating points; neither clearly dominates

- block16_packed4_topk helper parity screen (2026-04-03, vetted Gutenberg):
  - parity-against block16_packed4 at seed=42: `order_diff=1`, `set_diff=0`
  - parity-against block16_packed4 at seed=123: `order_diff=1`, `set_diff=0`
  - passes helper parity contract: set_diff=0 at both seeds
  - hit@1 varies (100% at seed=123, 98% at seed=42) due to tie-break
    reordering of the true #1 within the same result set — this is expected
    under the helper parity contract and does not indicate scoring divergence

- block32 recall regression ablation (2026-04-03, vetted Gutenberg):
  - group_size sweep: 16/32/64/128 + plain blockhadamard (no scaling)
  - finding: group_size=32 over-equalizes on 2880D Gutenberg, losing 1.8%
    recall@10 vs plain blockhadamard while gaining 2% hit@1
  - group_size=16 is the sweet spot: preserves tail discrimination while
    still improving hit@1, yielding 100% hit@1 + 91.0% recall@10
  - shrinkage ablation (blend group_scale toward global RMS): shrinkage
    helps recall partially (90.4% at alpha=0.5-0.75) but does not match
    block16 (91.0%), and introduces an extra tuning parameter
  - block16_packed4 verified exact quality match vs non-packed block16

- IVF+TQ research lane added (2026-04-03) as
  `TurboQuantIVFBlock32PackedMethod` in the evaluator: k-means on
  equalized rotated space, packed block32 TQ scoring within probed clusters
  - glove-100 (50K base, 100D, cosine, k=10):
    - `block32_packed4` (exhaustive): `86% hit@1`, `85.6% recall@10`,
      `1.17 ms` p50
    - `ivf32_block32_packed4` (nprobe=8): `80% hit@1`, `78.8% recall@10`,
      `0.49 ms` p50 — 2.4x speedup, ~7% recall gap
    - `ivf64_block32_packed4` (nprobe=12): `78% hit@1`, `79.4% recall@10`,
      `0.48 ms` p50
  - vetted Gutenberg (103260 x 2880D, cosine, k=10, nprobe=4 old config):
    - `block32_packed4` (exhaustive): `100% hit@1`, `89.40% recall@10`,
      `15.9 ms` p50
    - `ivf32_block32_packed4`: `88% hit@1`, `81.40% recall@10`,
      `10.1 ms` p50 — 1.57x speedup, ~8% recall gap
    - encode cost: `507 s` (k-means fit on 103K x 2880D, one-time)
  - nprobe sweep on glove-100 (50K base, 100D, ivf32, k=10):

    | nprobe | hit@1 | recall@10 | p50 ms | speedup |
    |--------|-------|-----------|--------|---------|
    |      2 |  66%  |    64.8%  |  0.16  |   7.2x  |
    |      4 |  76%  |    71.8%  |  0.27  |   4.3x  |
    |      8 |  80%  |    78.8%  |  0.49  |   2.4x  |
    |     16 |  84%  |    83.8%  |  0.91  |   1.3x  |
    |     32 |  86%  |    85.6%  |  1.67  |   0.7x  |

    exhaustive `block32_packed4` baseline: `86% hit@1`, `85.6% recall@10`,
    `1.15 ms` p50

  - status: PROVISIONAL — nprobe sweep done on glove-100 (low-dim), but:
    - Gutenberg sweep blocked by k-means fit cost (507s per run)
    - nprobe=32 (all clusters) recovers exhaustive recall exactly,
      confirming correctness
    - sweet spot on glove-100: nprobe=8-16 (2.4-1.3x speedup, 79-84% recall)
    - higher-dim datasets should show tighter clusters and better
      recall at same nprobe, but this is untested
    - CLI now supports `--ivf-clusters` and `--ivf-nprobe` overrides
    - not a candidate for engine integration until Gutenberg sweep completes

### Publication threshold

The currently defensible publication thesis is narrow:

- a no-codebook, data-oblivious structured transform family
  (`blockhadamard`, `block32_dither`) can beat dense-rotation MSE-style
  TurboQuant proxies on both a real Cogniformerus code-graph set and a larger
  real Gutenberg retrieval workload while preserving tiny metadata

What is **not** yet defensible:

- claiming superiority over Google's TurboQuant implementation itself
- claiming `twopass` is the best general lane
- claiming the result generalizes beyond the current real workloads plus the
  small ANN-Benchmarks cross-checks

Before this becomes publishable instead of just interesting, the repo still
needs:

1. a tighter packed/kernelized path for the surviving lanes, not just Python
   eval plus a tiny C helper; the new packed `blockhadamard` result is good
   evidence that quality survives the format change and that even a small C
   loop buys a real speedup, but it is still far from plain `blockhadamard`
2. a stronger multi-dataset operating curve across `2-6` bits
3. at least one very-high-dimension run (`>= 65536D`, ideally `262144D`)
   with recall and throughput, not only synthetic metadata scaling
4. a clean comparison against the official TurboQuant implementation if one
   becomes available
5. a clear statement of what the new contribution is:
   - structured no-codebook transforms for very-high-dimensional retrieval, or
   - blockwise subtractive dither as the strongest real operating point
   under near-zero-metadata constraints

Inputs:

- ANN-Benchmarks/real vector dataset already used in repo harnesses
- Cogniformerus summary/code-graph embeddings exported from the real consumer

Comparators:

- float32 `svec`
- float16 `hsvec`
- current `sorted_hnsw` SQ8 path where applicable
- existing PQ baseline where applicable
- TurboQuant experimental encode/decode/search path

Outputs:

- compression ratio
- encode/build time
- query latency or proxy distance-eval cost
- hit@1 / hit@k / recall@k
- quality versus compression curve

This is the first implementation point because it is reversible and does not
broaden the release surface.

#### Option B — second experiment: Cogniformerus external-memory mode

If Option A looks promising, add a **consumer-only** experimental mode in
Cogniformerus for summary vectors or external memory vectors.

This still avoids touching the stable `sorted_hnsw` AM path.

#### Option C — last: engine integration

Only after A/B show a meaningful win on the real consumer should TurboQuant be
considered for:

- `sorted_hnsw` sketch/storage internals
- planner-visible index modes
- GraphRAG stable path

### DoD for the TurboQuant experiment

Minimum acceptance:

1. one synthetic/benchmark dataset already used in this repo
2. one real Cogniformerus-derived embedding set
3. exact comparison against current baselines
4. measured answer to one question:
   - better compression-quality tradeoff than `hsvec`, or
   - better recall-latency tradeoff than existing PQ path, or
   - not worth pursuing

### Anti-goals

Do not let the first TurboQuant branch become:

- a new release-surface promise
- a new router/control-plane branch
- a KV-cache engineering detour
- a speculative kernel rewrite without consumer evidence

## Enlarged `0.13`: consumer-first scope

### Core judgment

Because `0.13` is still unpublished and Cogniformerus is the first real
customer, the right expansion is **consumer-first**, not more routing
infrastructure.

### What the current consumer already uses

Cogniformerus already has all of these:

- fact-shaped code-graph storage on `sorted_heap`
- ANN seed on `sorted_hnsw`
- multi-hop retrieval through `sorted_heap_graph_rag(...)`
- MCP tools that expose graph search / callers / callees

So the next work should target real consumer correctness and update cost.

### The real consumer gaps already visible

#### 1. Fact-shape mismatch

Current Cogniformerus code-graph store and loader collapse uniqueness to
`(entity_id, target_id)` instead of `(entity_id, relation_id, target_id)`.

That can lose relation distinctions in the real code graph.

#### 2. Directionality mismatch

`find_callers` and `find_callees` are effectively symmetric in the current
consumer code. That is not semantically honest unless reverse edges exist.

#### 3. Fake `relation=all`

Current `relation == "all"` behavior in the MCP tool path degrades to a
single relation family instead of a true multi-relation search.

#### 4. Brutal update model

Current reindex flow is full truncate + full reload + compact + index rebuild.
That is the most obvious real-world pain point.

## Split plan: `clustered_pg` vs `cogniformerus`

### `clustered_pg`

#### Include in enlarged `0.13`

1. **GraphRAG witness/explain improvements**
   - narrow app-facing explanation for why a result/path was returned
   - build on `sorted_heap_graph_rag_stats()` and current routed explain
   - do not add a new low-level wrapper zoo

2. **Real-workload regression fixture**
   - one tiny code-graph-shaped fixture or harness
   - guard the actual Cogniformerus query patterns, not just synthetic chains

3. **Only if real consumer data proves it is needed**
   - multi-relation hop support beyond the current one-relation-per-hop shape

#### Explicitly defer

- segment synopses
- adaptive widening
- temporal queries
- hub capsules
- new routed control-plane layers

### `cogniformerus`

#### Include in enlarged `0.13`

1. **Fix fact shape**
   - relation-aware PK/upsert semantics
   - relation-aware dedupe in the loader

2. **Fix callers/callees**
   - either reverse-edge ingest or honest reverse-query handling

3. **Implement real `relation=all`**
   - first acceptable solution: consumer-side union/merge over relation families
   - only push this into `clustered_pg` if the real workload proves it is
     necessary

4. **Incremental reindex**
   - file-scoped delete/upsert instead of full truncate + rebuild
   - periodic compaction/index maintenance can stay separate

5. **Real query set**
   - fix 20-50 code-graph queries as the first real consumer benchmark gate

## Ordered execution

1. Cogniformerus correctness:
   - fact shape
   - dedupe
   - callers/callees
   - `relation=all`
2. Cogniformerus real query set
3. `clustered_pg` witness/explain only if the consumer actually needs it
4. Cogniformerus incremental ingest
5. TurboQuant experiment lane (Option A) in parallel, still outside stable AM

## Release framing

The enlarged unpublished `0.13` should be treated as:

> first real code-graph consumer release

not as:

> more routed/segmented infrastructure
