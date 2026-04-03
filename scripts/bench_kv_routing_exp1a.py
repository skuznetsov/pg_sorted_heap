#!/usr/bin/env python3
"""
Experiment 1A: Offline routing-quality trace for FlashHadamard KV sketches.

Tests whether a cheap sketch (FlashHadamard on block embeddings) can identify
relevant historical context blocks. Uses embedding similarity as ground truth
proxy for attention relevance.

Approach:
1. Take a long text, split into fixed-size chunks ("blocks")
2. Embed each block via the embedding model (exact FP32)
3. For each query block (tail of document), find ground truth relevant
   blocks by exact cosine similarity
4. Compare three retrieval strategies:
   - Random baseline
   - Recency-only (most recent blocks)
   - FlashHadamard sketch search
5. Metric: overlap between retrieved set and ground truth top-k

This is NOT exact KV-cache attention. It tests the weaker but necessary
condition: can a cheap sketch find contextually relevant blocks?

Pass/fail:
  PASS:  sketch overlap consistently > random AND > recency-only
  SOFT:  sketch > random but ≈ recency-only
  FAIL:  sketch ≈ random
"""

from __future__ import annotations

import argparse
import importlib.util
import math
import os
import pathlib
import statistics
import sys
import time

import numpy as np

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
BENCH_PATH = REPO_ROOT / "scripts" / "bench_turboquant_retrieval.py"


def load_bench_module():
    spec = importlib.util.spec_from_file_location("bench_turboquant_retrieval", BENCH_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {BENCH_PATH}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Experiment 1A: offline KV block routing quality")
    ap.add_argument("--pg-dsn", default=os.environ.get(
        "KV_EXP1A_PG_DSN",
        "postgres://postgres:postgres@127.0.0.1:30432/cogniformerus"))
    ap.add_argument("--base-sql", required=True,
                    help="SQL returning (text_chunk, embedding::text) ordered by position")
    ap.add_argument("--block-size", type=int, default=8,
                    help="Number of consecutive text chunks per KV block")
    ap.add_argument("--query-blocks", type=int, default=10,
                    help="Number of tail blocks to use as queries")
    ap.add_argument("--k", type=int, default=5, help="Top-k blocks to retrieve")
    ap.add_argument("--turbo-bits", type=int, default=4)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--metric", choices=("cosine",), default="cosine")
    return ap.parse_args()


def cosine_sim(a: np.ndarray, b: np.ndarray) -> float:
    dot = float(np.dot(a, b))
    na = float(np.linalg.norm(a))
    nb = float(np.linalg.norm(b))
    if na < 1e-12 or nb < 1e-12:
        return 0.0
    return dot / (na * nb)


def overlap_at_k(retrieved: np.ndarray, ground_truth: np.ndarray, k: int) -> float:
    r_set = set(retrieved[:k].tolist())
    gt_set = set(ground_truth[:k].tolist())
    if not gt_set:
        return 0.0
    return len(r_set & gt_set) / len(gt_set)


def main() -> int:
    args = parse_args()
    mod = load_bench_module()

    # Load embeddings from PG
    import psycopg2
    conn = psycopg2.connect(args.pg_dsn)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute(args.base_sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if len(rows) < args.block_size * (args.query_blocks + args.k + 1):
        print(f"Not enough rows ({len(rows)}) for block_size={args.block_size}, "
              f"query_blocks={args.query_blocks}, k={args.k}", file=sys.stderr)
        return 1

    # Parse embeddings
    embeddings = np.array([mod.parse_vector_text(row[1]) for row in rows], dtype=np.float32)
    n_total = embeddings.shape[0]
    dim = embeddings.shape[1]
    print(f"Loaded {n_total} embeddings, dim={dim}", file=sys.stderr)

    # Group into blocks (mean-pool embeddings within each block)
    n_blocks = n_total // args.block_size
    block_embeddings = np.zeros((n_blocks, dim), dtype=np.float32)
    for b in range(n_blocks):
        start = b * args.block_size
        end = start + args.block_size
        block_emb = embeddings[start:end].mean(axis=0)
        norm = np.linalg.norm(block_emb)
        if norm > 1e-12:
            block_emb /= norm
        block_embeddings[b] = block_emb

    print(f"Created {n_blocks} blocks of {args.block_size} chunks each", file=sys.stderr)

    # Split: historical blocks vs query blocks
    n_history = n_blocks - args.query_blocks
    if n_history < args.k + 1:
        print(f"Not enough history blocks ({n_history}) for k={args.k}", file=sys.stderr)
        return 1

    history_embs = block_embeddings[:n_history]
    query_embs = block_embeddings[n_history:]

    # Build FlashHadamard index on history blocks
    history_normalized = mod.normalize_rows(history_embs)

    rng = np.random.default_rng(args.seed)
    perm = rng.permutation(dim).astype(np.int32)
    signs = rng.choice(np.array([-1.0, 1.0], dtype=np.float32), size=dim, replace=True)
    blocks = mod.power_of_two_blocks(dim)
    centers, bounds = mod.gaussian_lloyd_max(args.turbo_bits)

    rotated = mod.structured_block_hadamard(history_normalized, perm, signs, blocks) * math.sqrt(dim)
    codes = np.digitize(rotated, bounds[1:-1], right=False).astype(np.uint8)
    packed = mod.pack_nibbles(codes)
    packed_t = mod.transpose_packed_codes(packed)

    # Evaluate each query block
    random_overlaps = []
    recency_overlaps = []
    sketch_overlaps = []

    for qi in range(args.query_blocks):
        q_emb = query_embs[qi]

        # Ground truth: exact cosine similarity to all history blocks
        gt_scores = history_embs @ q_emb
        gt_topk = np.argsort(gt_scores)[::-1][:args.k]

        # Random baseline
        random_ids = rng.choice(n_history, size=args.k, replace=False)
        random_overlaps.append(overlap_at_k(random_ids, gt_topk, args.k))

        # Recency-only: most recent history blocks
        recency_ids = np.arange(n_history - args.k, n_history)[::-1]
        recency_overlaps.append(overlap_at_k(recency_ids, gt_topk, args.k))

        # FlashHadamard sketch search
        q_norm = q_emb / max(np.linalg.norm(q_emb), 1e-12)
        q_rot = mod.structured_block_hadamard_vec(q_norm, perm, signs, blocks)
        coeffs = (q_rot / math.sqrt(dim)).astype(np.float32, copy=False)
        sketch_scores = mod.packed_lookup_scores_blockhadamard_packed4_transposed(
            packed_t, coeffs, centers, None)
        sketch_topk = mod.topk_indices(sketch_scores, args.k)
        sketch_overlaps.append(overlap_at_k(sketch_topk, gt_topk, args.k))

    # Results
    print(f"\nExperiment 1A: KV block routing quality")
    print(f"  history blocks: {n_history}, query blocks: {args.query_blocks}, k={args.k}, dim={dim}")
    print(f"  block_size: {args.block_size} chunks")
    print(f"\n  {'Method':<25} {'mean overlap@k':>15} {'min':>8} {'max':>8}")
    print(f"  {'-'*58}")

    for name, overlaps in [
        ("random", random_overlaps),
        ("recency-only", recency_overlaps),
        ("FlashHadamard sketch", sketch_overlaps),
    ]:
        mean_o = statistics.fmean(overlaps)
        min_o = min(overlaps)
        max_o = max(overlaps)
        print(f"  {name:<25} {mean_o:>15.3f} {min_o:>8.3f} {max_o:>8.3f}")

    # Verdict
    sketch_mean = statistics.fmean(sketch_overlaps)
    random_mean = statistics.fmean(random_overlaps)
    recency_mean = statistics.fmean(recency_overlaps)

    print()
    if sketch_mean > random_mean * 1.5 and sketch_mean > recency_mean:
        print(f"  VERDICT: PASS (sketch {sketch_mean:.3f} > random {random_mean:.3f} AND > recency {recency_mean:.3f})")
    elif sketch_mean > random_mean * 1.2:
        print(f"  VERDICT: SOFT (sketch {sketch_mean:.3f} > random {random_mean:.3f} but ≈ recency {recency_mean:.3f})")
    else:
        print(f"  VERDICT: FAIL (sketch {sketch_mean:.3f} ≈ random {random_mean:.3f})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
