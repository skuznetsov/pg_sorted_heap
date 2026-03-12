#!/usr/bin/env python3
"""
Offline hierarchical NSW simulation on Gutenberg hsvec(384) sketches.

Builds a usearch HNSW index on the 103K hsvec(384) sketch vectors,
sweeps ef_search, and measures:
  - recall@10 vs brute-force exact top-10 (384-dim cosine)
  - median latency (ms per query)
  - visited_members (nodes examined per query)

Comparison baseline (from bench_gutenberg_k8s_ann.sh ef sweep):
  flat NSW ef=128 -> 98.0% recall, 6.0ms, ~11K buffer accesses
  flat NSW ef=256 -> 99.2% recall, 10.0ms, ~22K buffer accesses

Go criterion (per GPT R&D plan):
  At 98-99% recall, HNSW visited_members < 65% of flat NSW buffer accesses
  i.e., >=35% reduction -> hierarchy justified -> proceed to runtime redesign

Usage:
  python3 scripts/sim_hierarchical_graph.py \
    --dsn 'host=localhost port=15432 dbname=cogniformerus user=postgres password=postgres'
"""
from __future__ import annotations

import argparse
import time

import numpy as np
import psycopg2

from usearch.index import Index


SKETCH_DIM = 384
FLAT_NSW_BASELINE = {
    # ef -> (recall@10, avg_ms, approx_buffer_accesses)
    32:  (9.04,  2.49,  4_300),
    64:  (9.40,  3.57,  7_600),
    128: (9.80,  5.99, 11_000),
    192: (9.84,  7.99, 16_500),
    256: (9.92,  9.96, 22_226),
    384: (9.92, 14.08, 33_000),
}


def parse_vec(text: str) -> list[float]:
    return [float(x) for x in text[1:-1].split(",")]


def load_sketches(cur) -> tuple[np.ndarray, list[int]]:
    print("[1/4] Loading hsvec(384) sketches from graph table...", flush=True)
    cur.execute(
        "SELECT nid, sketch::text FROM gutenberg_gptoss_sh_graph ORDER BY nid"
    )
    rows = cur.fetchall()
    nids = [r[0] for r in rows]
    vecs = np.array([parse_vec(r[1]) for r in rows], dtype=np.float32)
    print(f"      {len(nids):,} rows, shape {vecs.shape}")
    return vecs, nids


def load_queries(cur, sketch_dim: int) -> tuple[np.ndarray, list[str]]:
    print("[2/4] Loading query vectors (prefix-truncated to 384 dims)...", flush=True)
    cur.execute("SELECT qid, qvec::text FROM bench_gptoss_queries")
    rows = cur.fetchall()
    qids = [r[0] for r in rows]
    qvecs = np.array(
        [parse_vec(r[1])[:sketch_dim] for r in rows], dtype=np.float32
    )
    print(f"      {len(qids)} queries, shape {qvecs.shape}")
    return qvecs, qids


def brute_force_top10(sketches: np.ndarray, queries: np.ndarray) -> np.ndarray:
    """Exact cosine top-10 for each query. Returns (n_queries, 10) index array."""
    print("[3/4] Computing brute-force exact top-10 (cosine, 384-dim)...", flush=True)
    s_norm = sketches / np.maximum(np.linalg.norm(sketches, axis=1, keepdims=True), 1e-10)
    q_norm = queries  / np.maximum(np.linalg.norm(queries,  axis=1, keepdims=True), 1e-10)

    batch = 50
    n_q = len(queries)
    top10 = np.empty((n_q, 10), dtype=np.int64)
    for i in range(0, n_q, batch):
        sims = q_norm[i:i+batch] @ s_norm.T
        top10[i:i+batch] = np.argsort(-sims, axis=1)[:, :10]

    print(f"      done, ground truth: {n_q} x 10 indices")
    return top10


def build_hnsw(sketches: np.ndarray, nids: list[int], M: int, ef_construction: int) -> Index:
    print(f"[4/4] Building usearch HNSW (M={M}, ef_construction={ef_construction})...",
          flush=True)
    t0 = time.time()
    idx = Index(
        ndim=SKETCH_DIM,
        metric="cos",
        connectivity=M,
        expansion_add=ef_construction,
    )
    idx.add(np.array(nids, dtype=np.int64), sketches)
    print(f"      built in {time.time()-t0:.1f}s, {idx.size:,} nodes")
    return idx


def sweep_ef(
    idx: Index,
    queries: np.ndarray,
    gt_indices: np.ndarray,
    ef_values: list[int],
    k: int = 10,
    n_warmup: int = 5,
) -> None:
    idx.expansion_search = ef_values[0]
    for q in queries[:n_warmup]:
        idx.search(q, k, exact=False)

    print()
    print(f"{'ef':>6}  {'avg_ms':>7}  {'p50_ms':>7}  {'p95_ms':>7}  "
          f"{'recall@10':>9}  {'visited_avg':>11}  {'% of flat':>9}")
    print(f"{'---':>6}  {'------':>7}  {'------':>7}  {'------':>7}  "
          f"{'---------':>9}  {'-----------':>11}  {'---------':>9}")

    results = []
    for ef in ef_values:
        idx.expansion_search = ef
        times_ms = []
        recalls = []
        visited_counts = []

        for i, q in enumerate(queries):
            t0 = time.perf_counter()
            m = idx.search(q, k, exact=False)
            elapsed_ms = (time.perf_counter() - t0) * 1000
            times_ms.append(elapsed_ms)

            returned = set(m.keys.tolist())
            gt_set   = set(gt_indices[i].tolist())
            recalls.append(len(returned & gt_set))
            visited_counts.append(int(m.visited_members))

        times_ms_s = sorted(times_ms)
        n       = len(times_ms_s)
        avg_ms  = sum(times_ms) / n
        p50_ms  = times_ms_s[n // 2]
        p95_ms  = times_ms_s[int(n * 0.95)]
        avg_rec = sum(recalls) / n
        avg_vis = sum(visited_counts) / n

        flat_acc = FLAT_NSW_BASELINE.get(ef, (None, None, None))[2]
        pct = f"{avg_vis / flat_acc * 100:.1f}%" if flat_acc else "N/A"

        print(
            f"{ef:>6}  {avg_ms:>7.2f}  {p50_ms:>7.2f}  {p95_ms:>7.2f}  "
            f"{avg_rec:>9.2f}  {avg_vis:>11.0f}  {pct:>9}"
        )
        results.append((ef, avg_ms, avg_rec, avg_vis, flat_acc))

    print()
    print("Baseline (flat NSW in PostgreSQL, warm cache):")
    print(f"{'ef':>6}  {'recall@10':>9}  {'avg_ms':>7}  {'buf_accesses':>12}")
    for ef, (rec, ms, acc) in sorted(FLAT_NSW_BASELINE.items()):
        print(f"{ef:>6}  {rec:>9.2f}  {ms:>7.2f}  {acc:>12,}")

    # Go/no-go verdict
    print()
    print("Go criterion: visited_avg at 98-99% recall < 65% of flat NSW buffer accesses")
    go_candidates = [(ef, avg_rec, avg_vis, flat_acc)
                     for ef, avg_ms, avg_rec, avg_vis, flat_acc in results
                     if avg_rec >= 9.8 and flat_acc is not None]
    if go_candidates:
        ef, rec, vis, flat = go_candidates[0]
        ratio = vis / flat
        verdict = "GO" if ratio < 0.65 else "NO GO"
        print(f"First ef >= 98% recall: ef={ef}, recall={rec:.2f}, "
              f"visited={vis:.0f}, flat_accesses={flat:,}, ratio={ratio:.2%}  -> {verdict}")
    else:
        print("No ef value reached 98% recall in sweep.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dsn",
        default="host=localhost port=15432 dbname=cogniformerus user=postgres password=postgres",
    )
    parser.add_argument("--M",               type=int, default=32)
    parser.add_argument("--ef-construction", type=int, default=200)
    parser.add_argument("--ef-values",       default="32,64,128,192,256,384")
    parser.add_argument("--k",               type=int, default=10)
    args = parser.parse_args()

    ef_values = [int(x) for x in args.ef_values.split(",")]

    conn = psycopg2.connect(args.dsn)
    cur  = conn.cursor()

    sketches, nids = load_sketches(cur)
    queries, _     = load_queries(cur, SKETCH_DIM)
    gt_indices     = brute_force_top10(sketches, queries)
    idx            = build_hnsw(sketches, nids, args.M, args.ef_construction)

    conn.close()

    print("\n=== usearch HNSW ef sweep — recall@10 vs 384-dim brute-force ===")
    sweep_ef(idx, queries, gt_indices, ef_values, k=args.k)


if __name__ == "__main__":
    main()
