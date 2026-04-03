---
layout: default
title: FlashHadamard Design Note
nav_order: 12
---

# FlashHadamard: Structured Packed Retrieval Quantizer + KV Memory Architecture

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

## 3. KV Memory Architecture (Proposed)

### Problem

LLM session KV cache grows linearly with context length:
- Qwen 3.5 9B at 8K context: ~2GB FP16 KV cache
- At 32K context: ~8GB — exceeds M2 Max unified memory budget
- cogni-ml already hits Metal memory pressure at 8GB per question

Key observation: most of a long session's KV cache has already been
"read through" — the information it carried is absorbed into the hidden
state of subsequent tokens. The full cache is rarely needed simultaneously.

### Architecture: Selective KV Memory

NOT "compressed KV cache" — **selective KV memory with pg_sorted_heap
backing**.

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

**Experiment 2: End-to-end quality (only after 1B PASS)**
- Same long session, but with actual KV offload active
- Compare generation quality between:
  a. Full KV cache (baseline)
  b. Hot tail only (no recall)
  c. Hot tail + FlashHadamard routed recall

**Experiment 1 (original, replaced by 1A+1B above):**
- Run Cogniformerus on a long session (8K+ tokens)
- Extract per-layer KV cache at each step
- Compute block-level mean-key sketches
- For each new token: rank old blocks by sketch similarity
- Compare against actual attention weight distribution
- Metric: does top-5 by sketch overlap with top-5 by attention weight?

**Experiment 2: End-to-end quality**
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
