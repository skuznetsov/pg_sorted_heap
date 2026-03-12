#!/usr/bin/env python3
"""
Python prototype validation of hierarchical HNSW search on PG tables.

Loads all HNSW tables into memory, runs top-down beam search, measures:
  - visit_count (exact proxy for C buffer accesses)
  - recall@10 vs brute-force 384-dim ground truth
  - predicted C latency = visit_count * 0.6us

Predicted C latency assumes:
  - Each node visit = 1 PostgreSQL buffer access (covering index, mostly cache-hit)
  - Buffer manager pin/unpin overhead: ~0.6us per access (measured on this cluster)

Compare against flat NSW (from ef sweep):
  ef=128 -> recall=9.80, visit~11000, actual C latency=6.0ms

Usage:
  python3 scripts/bench_hnsw_pg.py \
    --dsn 'host=localhost port=15432 dbname=cogniformerus user=postgres password=postgres'
"""
from __future__ import annotations

import argparse
import heapq
import time

import numpy as np
import psycopg2

SKETCH_DIM = 384
BUFFER_COST_US = 0.6   # us per buffer access (measured)
FLAT_NSW_BASELINE = {
    32:  (9.04,  2.49,  4_300),
    64:  (9.40,  3.57,  7_600),
    128: (9.80,  5.99, 11_000),
    192: (9.84,  7.99, 16_500),
    256: (9.92,  9.96, 22_226),
}


def parse_vec(text: str) -> np.ndarray:
    return np.array([float(x) for x in text[1:-1].split(",")], dtype=np.float32)


def cosine_dist(a: np.ndarray, b: np.ndarray) -> float:
    na = np.linalg.norm(a); nb = np.linalg.norm(b)
    if na < 1e-10 or nb < 1e-10:
        return 1.0
    return float(1.0 - np.dot(a, b) / (na * nb))


# ---------------------------------------------------------------------------
# Load tables into memory
# ---------------------------------------------------------------------------

def load_level(cur, table: str, has_src: bool) -> dict[int, dict]:
    """Load one level table into {nid: {sketch, neighbors[, src_tid]}}."""
    if has_src:
        cur.execute(f"SELECT nid, sketch::text, neighbors, src_tid::text FROM {table}")
    else:
        cur.execute(f"SELECT nid, sketch::text, neighbors FROM {table}")
    rows = cur.fetchall()
    result = {}
    for r in rows:
        nid    = r[0]
        sketch = parse_vec(r[1])
        nbrs   = list(r[2]) if r[2] else []
        entry  = {"sketch": sketch, "neighbors": nbrs}
        if has_src:
            entry["src_tid"] = r[3]
        result[nid] = entry
    return result


def load_all(dsn: str, prefix: str, max_level: int) \
        -> tuple[int, int, dict[int, dict], list[dict[int, dict]]]:
    """Returns (entry_nid, max_level, l0, upper_levels[1..max_level])."""
    conn = psycopg2.connect(dsn)
    cur  = conn.cursor()

    cur.execute(f"SELECT entry_nid, max_level FROM {prefix}_meta")
    entry_nid, max_lev = cur.fetchone()
    print(f"[load] entry_nid={entry_nid}  max_level={max_lev}")

    print(f"[load] L0 ({prefix}_l0)...", flush=True)
    t0 = time.time()
    l0 = load_level(cur, f"{prefix}_l0", has_src=True)
    print(f"       {len(l0):,} nodes ({time.time()-t0:.1f}s)")

    uppers = [None]   # index 0 unused
    for l in range(1, max_lev + 1):
        t0 = time.time()
        ul = load_level(cur, f"{prefix}_l{l}", has_src=False)
        print(f"[load] L{l}: {len(ul):,} nodes ({time.time()-t0:.1f}s)")
        uppers.append(ul)

    conn.close()
    return entry_nid, max_lev, l0, uppers


# ---------------------------------------------------------------------------
# HNSW search
# ---------------------------------------------------------------------------

def search_layer_greedy(
    q: np.ndarray,
    entry_nids: list[int],
    level_nodes: dict[int, dict],
    ef: int,
) -> tuple[list[int], int]:
    """
    Beam search at one level. Returns (top-ef nids by distance, visit_count).
    """
    visited  = set(entry_nids)
    # candidates: min-heap (dist, nid)
    # W: max-heap (neg dist, nid) — nearest so far
    candidates: list = []
    W: list = []
    visits = 0

    for ep in entry_nids:
        node = level_nodes.get(ep)
        if node is None:
            continue
        d = cosine_dist(q, node["sketch"])
        visits += 1
        heapq.heappush(candidates, (d, ep))
        heapq.heappush(W, (-d, ep))

    while candidates:
        d_c, c = heapq.heappop(candidates)
        f = -W[0][0]    # farthest in W
        if d_c > f:
            break

        node = level_nodes[c]
        for e in node["neighbors"]:
            if e in visited:
                continue
            visited.add(e)
            neighbor = level_nodes.get(e)
            if neighbor is None:
                continue
            d_e = cosine_dist(q, neighbor["sketch"])
            visits += 1
            f = -W[0][0]
            if d_e < f or len(W) < ef:
                heapq.heappush(candidates, (d_e, e))
                heapq.heappush(W, (-d_e, e))
                if len(W) > ef:
                    heapq.heappop(W)

    # W items: (-dist, nid) — sort by ascending dist (nearest first)
    result = sorted([(nid, -d) for d, nid in W], key=lambda x: x[1])
    return [nid for nid, _ in result], visits


def hnsw_search(
    q: np.ndarray,
    entry_nid: int,
    max_level: int,
    l0: dict[int, dict],
    uppers: list[dict[int, dict]],
    ef_search: int,
    k: int = 10,
) -> tuple[list[int], int]:
    """Top-down HNSW search. Returns (top-k nids, total_visits)."""
    total_visits = 0
    ep = [entry_nid]

    # Greedy descent through upper layers (ef=1)
    for lc in range(max_level, 0, -1):
        top, v = search_layer_greedy(q, ep, uppers[lc], ef=1)
        total_visits += v
        ep = top[:1]

    # Beam search at L0
    top_l0, v = search_layer_greedy(q, ep, l0, ef=ef_search)
    total_visits += v

    return top_l0[:k], total_visits


# ---------------------------------------------------------------------------
# Ground truth (brute force on 384-dim sketches)
# ---------------------------------------------------------------------------

def brute_force_gt(
    l0: dict[int, dict],
    queries: list[np.ndarray],
    k: int = 10,
) -> list[set[int]]:
    """Exact top-k by cosine for each query. Returns list of sets of nids."""
    print("[gt] Computing brute-force ground truth...", flush=True)
    t0 = time.time()
    nids_all    = np.array(list(l0.keys()), dtype=np.int64)
    sketches_m  = np.stack([l0[n]["sketch"] for n in nids_all])
    norms       = np.linalg.norm(sketches_m, axis=1, keepdims=True)
    sketches_n  = sketches_m / np.maximum(norms, 1e-10)

    gt = []
    for q in queries:
        qn = q / max(float(np.linalg.norm(q)), 1e-10)
        sims = sketches_n @ qn
        top  = np.argpartition(-sims, k)[:k]
        top  = top[np.argsort(-sims[top])]
        gt.append(set(int(nids_all[i]) for i in top))
    print(f"      done ({time.time()-t0:.1f}s)")
    return gt


# ---------------------------------------------------------------------------
# Main sweep
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dsn",
        default="host=localhost port=15432 dbname=cogniformerus user=postgres password=postgres",
    )
    parser.add_argument("--prefix",     default="public.gutenberg_gptoss_hnsw")
    parser.add_argument("--ef-values",  default="32,64,128,192,256,384")
    parser.add_argument("--k",          type=int, default=10)
    args = parser.parse_args()

    ef_values = [int(x) for x in args.ef_values.split(",")]

    # Single connection for everything
    conn = psycopg2.connect(args.dsn)
    cur  = conn.cursor()

    cur.execute(f"SELECT entry_nid, max_level FROM {args.prefix}_meta")
    entry_nid, max_level = cur.fetchone()
    print(f"[meta] entry_nid={entry_nid}  max_level={max_level}")

    # Load L0
    print(f"[load] L0 ({args.prefix}_l0)...", flush=True)
    t0 = time.time()
    l0 = load_level(cur, f"{args.prefix}_l0", has_src=True)
    print(f"       {len(l0):,} nodes ({time.time()-t0:.1f}s)")

    # Load upper levels
    uppers = [None]
    for l in range(1, max_level + 1):
        t0 = time.time()
        ul = load_level(cur, f"{args.prefix}_l{l}", has_src=False)
        print(f"[load] L{l}: {len(ul):,} nodes ({time.time()-t0:.1f}s)")
        uppers.append(ul)

    # Load queries
    cur.execute("SELECT qvec::text FROM bench_gptoss_queries")
    queries = [parse_vec(r[0])[:SKETCH_DIM] for r in cur.fetchall()]
    conn.close()
    print(f"[queries] {len(queries)} loaded")

    # Ground truth
    gt = brute_force_gt(l0, queries, args.k)

    # Sweep
    print()
    print(f"{'ef':>6}  {'recall@10':>9}  {'visits_avg':>10}  {'pred_ms':>8}  "
          f"{'vs_flat128':>10}")
    print(f"{'---':>6}  {'---------':>9}  {'----------':>10}  {'-------':>8}  "
          f"{'----------':>10}")

    for ef in ef_values:
        recalls, visits_all = [], []

        for i, q in enumerate(queries):
            top_nids, v = hnsw_search(
                q, entry_nid, max_level, l0, uppers, ef, args.k
            )
            recall = len(set(top_nids) & gt[i])
            recalls.append(recall)
            visits_all.append(v)

        avg_rec   = sum(recalls) / len(recalls)
        avg_vis   = sum(visits_all) / len(visits_all)
        pred_ms   = avg_vis * BUFFER_COST_US / 1000.0
        flat_vis  = FLAT_NSW_BASELINE.get(ef, (None, None, None))[2]
        vs_flat   = f"{avg_vis / flat_vis * 100:.1f}%" if flat_vis else "N/A"

        print(
            f"{ef:>6}  {avg_rec:>9.2f}  {avg_vis:>10.0f}  {pred_ms:>8.2f}ms  "
            f"{vs_flat:>10}"
        )

    print()
    print("Flat NSW baseline for comparison:")
    print(f"{'ef':>6}  {'recall@10':>9}  {'buf_acc':>10}  {'actual_ms':>9}")
    for ef, (rec, ms, acc) in sorted(FLAT_NSW_BASELINE.items()):
        print(f"{ef:>6}  {rec:>9.2f}  {acc:>10,}  {ms:>9.2f}ms")


if __name__ == "__main__":
    main()
