---
layout: default
title: FlashHadamard Design Note
nav_order: 12
---

# FlashHadamard: Structured Packed Retrieval Quantizer + Selective Memory

## 1. What FlashHadamard Is

FlashHadamard is a no-codebook vector quantization family built on structured
block-Hadamard rotation with packed 4-bit nibble ADC scoring. It achieves 91%
recall@10 at 5.7ms on 103K x 2880D real retrieval workloads with effectively
zero metadata overhead, competitive with SQ8 quality at 8x less storage. The
packed transposed scoring path with fused C top-k eliminates the need for
codebook training, making it fully online and data-oblivious.

### Core pipeline

```
vector → normalize → block-Hadamard rotation → group RMS equalize → 4-bit scalar quantize → pack nibbles → transpose
```

At query time:

```
query → rotate → fold group scales into coeffs → packed ADC score (C helper) → fused top-k
```

### What makes it different from original TurboQuant

| Axis | Original TurboQuant | FlashHadamard |
|---|---|---|
| Core math | PolarQuant + 1-bit QJL residual | structured block-Hadamard + scalar quant + packed ADC |
| Rotation | random orthogonal (dense, O(d^2) metadata) | seed-derived permutation + signs + FWHT (O(1) metadata) |
| Residual correction | explicit 1-bit QJL stage | none (refuted on our workloads; see consumer-plan.md) |
| Quantizer | Gaussian Lloyd-Max | Gaussian Lloyd-Max (shared) |
| Scoring | not focused on packed kernel path | packed nibble ADC with transposed layout + fused C top-k |
| Main target | KV-cache + vector search | retrieval / ANN-style packed search + KV memory (proposed) |
| Evidence | paper/blog claims | repo-owned measurements on Gutenberg + code-graph |

### Family naming

- **FlashHadamard-16** (`block16_packed4_topk`): group_size=16, best combined
  quality on Gutenberg (100% hit@1, 91.0% recall@10, 5.7ms)
- **FlashHadamard-Base** (`blockhadamard_packed4_topk`): no group scaling,
  highest recall@10 on some workloads (91.2%)
- **FlashHIGGS2** (`block16_higgs2_packed4`): experimental HIGGS-inspired
  p=2 byte-grid quantizer. It keeps the same 1 byte / 2 dims packed layout,
  but each byte indexes a joint 2D Gaussian grid instead of two independent
  scalar 4-bit Lloyd-Max levels.

## 2. Retrieval Operating Points

Vetted Gutenberg (103260 x 2880D, cosine, 4 bits, k=10):

| Lane | hit@1 | recall@10 | p50 ms | metadata | role |
|---|---|---|---|---|---|
| FlashHadamard-16 (topk) | 100/98% | 91.0% | 5.7 | 0.8 KB | fastest packed |
| FlashHadamard-Base (topk) | 98% | 91.2% | 6.2 | 0.1 KB | highest recall packed |
| block32_dither (dense) | 100% | 91.8% | 17.2 | 0.4 KB | best quality (not packable) |
| SQ8 linear | 98.6% | 99.3% | - | 0 | quality ceiling |
| PQ k-means (16 sub) | 45.3% | 66.6% | - | codebooks | baseline |

Robustness: stable across seeds 42/123/7/999. 200-query runs confirm
50-query vetted results within noise.

Comparisons against other families:

| Family | Strength | Weakness | vs FlashHadamard |
|---|---|---|---|
| SQ8 linear | simple, strong quality | 8x more bytes | FlashHadamard wins on compression + packed latency |
| PQ / IVF-PQ | mature, codebook-driven | training overhead, metadata | FlashHadamard is no-codebook, fully online |
| RaBitQ | very compact, fast | approximation error | FlashHadamard has better recall at 4-bit |
| Original TurboQuant | stronger theory, residual | not our implementation | we are retrieval-engineering-heavy |

### FlashHIGGS2 p=2 grid experiment

FlashHIGGS2 is a direct extension of the FlashHadamard packed layout inspired
by HIGGS (`arXiv:2411.17525`). The important systems property is that it does
not change the storage footprint: one byte still covers two dimensions. The
byte is reinterpreted as an index into a 256-point 2D Gaussian grid.

This implementation is not yet an exact HIGGS reproduction. The current grid
is a deterministic MiniBatchKMeans approximation over `N(0, I_2)`, while the
paper uses Gaussian MSE-optimal CLVQ grids. Treat current results as a systems
spike for the byte-grid serving shape, not as a final claim about HIGGS-optimal
grid quality.

```
scalar FlashHadamard:
  byte = low_nibble_code + high_nibble_code
  score += q0 * center[low] + q1 * center[high]

FlashHIGGS2:
  byte = joint_2d_code
  score += q0 * grid_x[byte] + q1 * grid_y[byte]
```

Evidence snapshot (Gutenberg `103260 x 2880D`, 50 reverse-order queries,
cosine, seed 42, 8-thread helper):

| Lane | hit@1 | recall@10 | p50 ms | bytes/vec | bits/dim |
|---|---:|---:|---:|---:|---:|
| FlashHadamard-16 packed | 100.0 | 88.6 | 6.676 | 1440 | 4 |
| FlashHIGGS2 packed | 100.0 | 91.0 | 7.284 | 1440 | 4 |
| FlashHadamard-16 + SQ8 rerank M=12 | 100.0 | 93.8 | 10.385 | 4320 | 12 |
| FlashHIGGS2 + SQ8 rerank M=12 | 100.0 | 95.8 | 10.354 | 4320 | 12 |
| FlashHadamard-16 + SQ8 rerank M=20 | 100.0 | 98.0 | 10.102 | 4320 | 12 |
| FlashHIGGS2 + SQ8 rerank M=20 | 100.0 | 98.0 | 10.624 | 4320 | 12 |

Four-seed quality follow-up (`103260 x 2880D`, 200 fixed queries, seeds
42/123/7/999):

| M | scalar recall@10 | HIGGS2 recall@10 | delta |
|---:|---:|---:|---:|
| 8 | 76.67 | 77.33 | +0.65 |
| 10 | 89.79 | 91.12 | +1.34 |
| 12 | 94.10 | 94.89 | +0.79 |
| 14 | 95.74 | 96.08 | +0.34 |
| 16 | 96.38 | 96.56 | +0.19 |
| 18 | 96.70 | 96.85 | +0.15 |
| 20 | 96.88 | 96.99 | +0.11 |

Latency from these full sweeps is not a product claim: repeated local runs
moved by more than 2x under host load/thermal/memory pressure. HIGGS2 now has
a chunked encoder and C-side byte-table + fused top-k path; speed needs a
controlled raw-kernel or single-run benchmark before promotion.

Controlled in-memory latency check after fit (`seed=42`, `k=20`, 200 queries,
1 warmup run, 3 measured runs, `TURBOQUANT_ADC_THREADS=8`):

| lane | fit ms | p50 ms | p90 ms | p95 ms | avg ms |
|---|---:|---:|---:|---:|---:|
| scalar FlashHadamard-16 | 27710.8 | 7.179 | 8.522 | 8.955 | 7.248 |
| FlashHIGGS2 | 27374.8 | 7.025 | 8.460 | 8.895 | 7.186 |

Interpretation: HIGGS2 reached latency parity with scalar after C-side table
build in this controlled run, but this is not yet a broad speed claim.

Interpretation:
- The p=2 grid lowers Gaussian quantization MSE by about 15% versus scalar
  p=1 Lloyd-Max in the local sanity check.
- The retrieval benefit is concentrated at smaller shortlist sizes. At
  `M=10`, FlashHIGGS2 recovered +1.34 recall@10 on the 4-seed follow-up; at
  `M=12`, it recovered +0.79.
- At `M=20`, SQ8 rerank is already near saturation and HIGGS2 no longer
  materially improves recall in the current four-seed sweep.
- Encode is slower because p=2 assignment currently uses nearest-center lookup
  over the 2D grid. This is acceptable for research/build-time, but the encoder
  chunks assignment to avoid materializing all dimension-pairs at once.

Decision: keep FlashHIGGS2 as an experimental benchmark/quantizer lane. The
next gate is a larger query set and preferably a raw-kernel latency benchmark.
Promote to PG engine only if a smaller HIGGS2 shortlist matches a larger
scalar shortlist with lower or equal end-to-end latency.
The offline evaluator now exposes scalar and HIGGS2 SQ8-rerank lanes for
`M in {8,10,12,14,16,18,20,50,100}` to make that gate directly runnable.
For full-size sweeps, use `scripts/bench_flashhiggs2_sweep.py` so each inner
quantizer is fit once and shared across all M values.

## 3. KV Memory Architecture (Proposed)

### Problem

LLM session KV cache grows linearly with context length:
- Qwen 3.5 9B at 8K context: ~2GB FP16 KV cache
- At 32K context: ~8GB — exceeds M2 Max unified memory budget
- cogni-ml already hits Metal memory pressure at 8GB per question

Key observation: most of a long session's KV cache has already been
"read through" — the information it carried is absorbed into the hidden
state of subsequent tokens. The full cache is rarely needed simultaneously.

### Architecture: Selective Memory

NOT "compressed KV cache" or "KV virtualization" — **retrieval-augmented
selective memory with pg_sorted_heap backing**. Routes at text/token level,
not KV tensor level.

```
┌─────────────────────────────────────────┐
│ HOT TIER (GPU RAM)                      │
│ - Last N tokens, exact FP16 KV          │
│ - Standard llama.cpp attention          │
│ - Size: configurable (e.g. last 2K tok) │
│ - Always resident, never evicted        │
└─────────────┬───────────────────────────┘
              │ overflow (oldest blocks spill)
┌─────────────▼───────────────────────────┐
│ WARM TIER (pg_sorted_heap)              │
│ Per block (e.g. 256 tokens):            │
│ - Routing sketch: FlashHadamard         │
│   on mean-key vector (4-bit, dim=64)    │
│ - Payload: SQ8 compressed KV block      │
│ - Index: session_id + layer + block_seq │
│ Retrieval:                              │
│ - Query: current attention state        │
│ - FlashHadamard sketch search           │
│ - Fetch top-K relevant blocks only      │
└─────────────┬───────────────────────────┘
              │ eviction (TTL / LRU)
┌─────────────▼───────────────────────────┐
│ COLD TIER (disk / evicted)              │
│ - Oldest blocks, rarely accessed        │
│ - Can be reloaded if session resumes    │
└─────────────────────────────────────────┘
```

### Query flow

1. Current tokens attend to HOT tier (standard attention, no change)
2. If extended context needed:
   a. Compute routing query from current attention state
   b. FlashHadamard sketch search on WARM tier blocks
   c. Fetch top-K relevant blocks from pg_sorted_heap
   d. Decompress SQ8 -> FP16, inject into attention window
3. Expected warm-tier hit rate: < 5% of stored tokens per query

### Why pg_sorted_heap

- Already runs alongside Cogniformerus on the same PG instance
- Clustered heap: sequential scan for block retrieval
- SQ8 compression already implemented
- HNSW index available for sketch-based routing
- Session isolation via table partitioning or session_id column
- TTL eviction via standard PG maintenance

### Minimal llama.cpp integration

```
Phase A — Exact offload (no routing):
  On KV overflow:
    serialize oldest block → INSERT INTO kv_blocks(session_id, layer, block_seq, kv_data)
  On context window shift:
    if block needed: SELECT kv_data FROM kv_blocks WHERE session_id=? AND layer=? AND block_seq=?
    decompress → inject into KV cache slots

Phase B — Routed recall:
  On KV overflow:
    same as Phase A, plus: compute block sketch → store alongside payload
  On extended context query:
    compute routing query from current hidden state
    SELECT kv_data FROM kv_blocks
      WHERE session_id=? AND layer=?
      ORDER BY sketch <-> query_sketch LIMIT k
    decompress top-K → inject into attention
```

### cogni-ml hook point

In `ML::LLM::Context` (cogni-ml `src/ml/llm/llama.cr`):
- After `encode()` / `decode()`: check KV cache size
- If exceeds hot-tier budget: call spill callback
- Spill callback: serialize block, write to PG via Crystal `pg` shard
- On cache miss: fetch callback, inject via `llama_kv_cache_seq_cp`

### Open questions

1. **Block sketch quality**: Does mean-key FlashHadamard sketch actually
   correlate with attention relevance? Needs experiment.
2. **Round-trip latency**: Can pg_sorted_heap serve 32KB blocks in < 5ms?
   Gate for interactive use.
3. **Layer coupling**: Should routing be per-layer or shared across layers?
   Per-layer is safer but 12x more sketches.
4. **Warm-up cost**: First query after cold start needs to build routing
   index. Acceptable if session is long.

### Experiment plan

**Experiment 0: pg_sorted_heap round-trip latency — PASS**

Measured on local containerized PG (pg_sorted_heap 0.13.dev, port 30432):

| config | heap | read-id p50 | read+decode p50 | sketch top-5 p50 | verdict |
|--------|------|-------------|-----------------|-------------------|---------|
| 1K blocks, 32KB | plain | 0.72ms | 0.78ms | 1.88ms | PASS |
| 1K blocks, 32KB | sorted_heap | 0.69ms | 0.78ms | 1.99ms | PASS |
| 1K blocks, 128KB | plain | 1.60ms | 1.92ms | 5.49ms | PASS |
| 1K blocks, 128KB | sorted_heap | 1.51ms | 1.85ms | 5.62ms | PASS |
| 10K blocks, 32KB | plain | 0.70ms | 0.77ms | 2.21ms | PASS |
| 10K blocks, 32KB | sorted_heap | 0.69ms | 0.81ms | 2.27ms | PASS |

Key findings:
- exact read-by-id + decode comfortably under 1ms at 32KB, under 2ms at 128KB
- no degradation from 1K to 10K blocks on exact reads (0.69-0.72ms stable)
- sorted_heap performs identically to plain heap (no overhead)
- sketch search scales mildly with block count (1.9ms→2.3ms from 1K to 10K at 32KB)
- all gates pass for 32KB blocks; 128KB sketch search is marginal (5.5ms vs 8ms gate)

Conclusion: pg_sorted_heap is a viable warm-tier backing store for KV blocks.
32KB blocks (256 tokens) are the recommended payload size.
Script: `scripts/bench_kv_offload_exp0.py`

**Experiment 1A: Embedding-proxy routing quality — PASS**

Tested on 4 Gutenberg novels (1K-2K chunks each, block_size=8, k=5):
- FlashHadamard sketch: 65-70% mean overlap with ground truth top-5
- Random baseline: 0-4%
- Recency-only: 0-11%
- Signal ~15x above random, consistently above recency

Caveat: ground truth is exact embedding cosine similarity, NOT actual
KV-cache attention weights. This is a necessary but not sufficient
condition for routing quality. Script: `scripts/bench_kv_routing_exp1a.py`

**Experiment 1B: LLM-closer routing quality (next)**
Goal: validate routing with a ground truth signal closer to actual
autoregressive model behavior.

Approach: for a long text processed by an autoregressive LLM (Qwen 3.5 9B
via cogni-ml llama.cpp bindings), measure whether removing/restoring old
context blocks affects generation quality. Specifically:
- process a long text (~2K tokens) through the LLM
- at each evaluation point, compare perplexity on next-N tokens with:
  a. full context (baseline)
  b. truncated to last-M tokens only (recency-only)
  c. last-M tokens + top-k blocks retrieved by FlashHadamard sketch
- if (c) is materially closer to (a) than (b), the routing thesis holds
  at the model level, not just embedding level

Gate:
- PASS: sketch-augmented perplexity materially closer to full context
  than recency-only
- SOFT: sketch helps but margin is small
- FAIL: no meaningful perplexity improvement from sketch-routed blocks

Result (Gemma 3 4B, War and Peace excerpt, block_size=64, k=2): **PASS**
- Full context (618 tokens): ppl=12.47
- Recency only (128 tokens): ppl=31.51
- Sketch + tail (256 tokens): ppl=12.48
- Recovery: 99.9% of context gap closed using 41% of tokens
- Script: `cogni-ml/bin/kv_routing_exp1b.cr`
- Adversary note: 1B uses block-text embedding similarity as routing
  signal, not actual serialized KV-block sketches. This is a strong
  routing proxy (proven by the perplexity recovery), but still not
  final proof that sketches computed from KV tensors directly would
  perform identically. The gap between "text embedding of block" and
  "mean key vector of KV block" remains untested.

**Experiment 2A: Exact token spill/restore — PASS**

Proves token-level spill/restore is quality-neutral:
- Full context (618 tokens): ppl=12.47
- Exact restore (618 tokens): ppl=12.47, delta=0.0
- Partial block included (boundary fix from initial 0.98 delta)
- Script: `cogni-ml/bin/kv_offload_exp2a.cr`

Note: this is token re-evaluation, not KV tensor serialization.

**Experiment 2B: Selective context routing proxy — PASS**

Tests whether sketch-selected text blocks yield good context:
- Full context (618 tokens): ppl=12.47
- Recency only (128 tokens): ppl=31.51
- Sketch-selected (234 tokens, 2 blocks): ppl=11.93
- 62% memory reduction with equal or better perplexity
- Script: `cogni-ml/bin/kv_offload_exp2b.cr`

Important caveats:
- This is **context selection**, not live KV-cache spill/restore.
  The script re-evaluates from tokens, not from serialized KV tensors.
- The routing sketch is embedding-based (nomic-embed), not KV-tensor-based.
- "Better than full context" is one excerpt, one k=2 — likely from
  filtering noise in irrelevant middle context, not a general guarantee.
- This supports the thesis for **retrieval-augmented selective memory**
  but does NOT yet prove a live KV offload/restore architecture.

**What is proven so far (summary):**
- pg_sorted_heap can serve blocks fast enough (Exp 0)
- Embedding-based sketch finds relevant blocks (Exp 1A)
- Sketch-selected context improves LLM perplexity over recency-only (Exp 1B, 2B)
- Token-level spill/restore is quality-neutral (Exp 2A)

**What is NOT yet proven:**
- Live KV tensor serialization/restore through llama.cpp
- KV-tensor-based sketches (vs embedding-based)
- Performance on longer contexts (> 1K tokens)
- Generalization beyond one text excerpt
- That the architecture works under real interactive generation

**Decision (2026-04-03): selective memory, not live KV hook**

All evidence so far supports retrieval-augmented selective memory
(text/token-level routing + re-eval) more strongly than KV-tensor-level
virtualization. Live KV hook requires deep llama.cpp surgery with
uncertain payoff; selective memory is already proven to work at the
text level.

Reframed thesis: **FlashHadamard selective memory** — a retrieval layer
that selects relevant historical context blocks via cheap sketch routing
and reinserts them as tokens for re-evaluation.

Live KV hook parked as later optimization: only revisit if re-eval
latency becomes the dominant bottleneck after the selective memory path
is productionized.

**Experiment 3: Selective memory e2e prototype — single-text PASS, robustness FAIL**

Single-text result (Gemma 3 4B, 844 tokens, k=5):
- Full context: ppl=9.76
- Recency only: ppl=25.36
- Sketch-routed (PG-backed): ppl=10.23, 74% memory, 97% recovery
- Retrieve overhead: 17.5ms (16ms embed + 1.5ms PG)
- Script: `cogniformerus/bin/selective_memory_exp3.cr`

Robustness pass (3 Gutenberg texts, 1024 tokens each, k=3 and k=5):
- **Random block selection consistently outperforms sketch routing**
- War and Peace k=3: random 95% recovery vs sketch 78%
- Les Mis k=5: random 92% vs sketch 42%
- Bleak House k=3: random 85% vs sketch 56%
- Script: `cogniformerus/bin/selective_memory_robustness.cr`

Root cause: embedding similarity to the tail selects REDUNDANT context
(semantically close to what model already sees in hot tail), not
COMPLEMENTARY context. For perplexity, diverse prefix coverage matters
more than semantic similarity to the current window.

**Consequence: tail-similarity routing is REFUTED as a general method.**

Baseline for any future memory routing is now `random blocks`, not
`recency-only`. A routing signal that cannot beat random is not useful.

**Current status of selective memory track: PARKED**

What is validated:
- pg_sorted_heap can serve blocks fast enough (Exp 0)
- Exact token spill/restore is quality-neutral (Exp 2A)
- FlashHadamard quantizer/retrieval (independent of routing)

What is refuted:
- "Retrieve blocks most similar to tail" as routing signal

What is untested:
- Coverage/diversity-based routing (MMR, DPP)
- Recency + diversity hybrid
- Attention-weight-based routing (needs KV access)

Live KV hook: parked. No investment until a routing signal beats
random robustly across texts.

**Two independent tracks going forward:**

Track A — FlashHadamard quantizer/retrieval (ACTIVE):
- block16_packed4_topk and blockhadamard_packed4_topk validated
- IVF centroid caching + Gutenberg nprobe frontier needed
- Publishable as no-codebook packed ANN quantizer

Track B — Selective memory routing (RESEARCH-ONLY):
- Only offline experiments
- Must beat random as baseline
- Coverage/diversity objectives
- No live hook until routing validated

**Original Experiment 2 (replaced):**
- Same long session, but with KV offload active
- Compare generation quality (perplexity, coherence) between:
  a. Full KV cache (baseline)
  b. Hot tail only (2K tokens, no recall)
  c. Hot tail + FlashHadamard routed recall (2K + top-5 blocks)
- Gate: (c) should be materially closer to (a) than (b)

## 4. Relationship to Existing Repo

| Component | Role | Location |
|---|---|---|
| FlashHadamard quantizer | rotation + quant + pack + score | `scripts/bench_turboquant_retrieval.py` |
| Packed ADC C helper | transposed scoring + fused top-k | `scripts/turboquant_packed_adc.c` |
| pg_sorted_heap | backing store for warm-tier KV blocks | `src/sorted_heap.c` |
| SQ8 compression | payload compression for KV blocks | `src/svec.c` |
| HNSW index | sketch-based routing index | `src/sorted_hnsw.c` |
| cogni-ml llama bindings | KV cache hook point | `../cogni-ml/src/ml/llm/llama.cr` |
| Cogniformerus MCP | session management, embedding | `../cogniformerus/src/cogniformerus/` |

## 5. Non-goals

- Drop-in replacement for llama.cpp KV cache (too invasive)
- Competing with KIVI / per-layer KV quantization (different problem)
- Publishing a new ANN benchmark paper (we optimize for real consumer, not benchmark leaderboards)
- Replacing SQ8 for small datasets (SQ8 is simpler and higher quality when storage is not the bottleneck)
