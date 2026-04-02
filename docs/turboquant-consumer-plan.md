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

1. a packed/kernelized path for the surviving lanes, not just Python eval
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
