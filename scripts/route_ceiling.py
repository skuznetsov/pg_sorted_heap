#!/usr/bin/env python3
"""
Step 0: Partition routing ceiling analysis.

For each query, rank all partitions by cosine distance from query to their
entry centroid.  Then compute exact GT top-10 and check what fraction of GT
neighbors lie in the top-k routed partitions.

This answers: "Is hard partition filtering feasible, or will it kill recall?"

If top-2 partitions cover <80% of GT on average:
  -> hard filter kills recall; use soft/penalised approach
If top-2 partitions cover >=90% of GT:
  -> hard filter is safe; proceed to n_route_partitions prototype

Usage:
  "$(./scripts/find_vector_python.sh)" scripts/route_ceiling.py \\
    --dsn 'host=/tmp port=65432 dbname=bench_nomic'

Defaults match bench_nomic_local_ann.py conventions.
"""

import argparse
import statistics
import sys
import time

import psycopg2
import psycopg2.extras
import numpy as np


def parse_hsvec_text(text: str) -> np.ndarray:
    text = text.strip("[]")
    if not text:
        return np.array([], dtype=np.float64)
    return np.array([float(x) for x in text.split(",")], dtype=np.float64)


def cosine_dist(a: np.ndarray, b: np.ndarray) -> float:
    na = np.linalg.norm(a)
    nb = np.linalg.norm(b)
    if na == 0 or nb == 0:
        return float("nan")
    sim = np.dot(a, b) / (na * nb)
    return 1.0 - float(np.clip(sim, -1.0, 1.0))


def main() -> int:
    parser = argparse.ArgumentParser(description="Partition routing ceiling analysis")
    parser.add_argument("--dsn", default="host=/tmp port=65432 dbname=bench_nomic")
    parser.add_argument("--exact-table", default="bench_nomic_train_8k",
                        help="Table with full-precision embeddings for GT")
    parser.add_argument("--main-table", default="bench_nomic_8k",
                        help="Sorted-heap main table (has partition_id)")
    parser.add_argument("--graph-table", default="bench_nomic_graph",
                        help="Graph nodes table (nid, src_id, ...)")
    parser.add_argument("--entry-table", default="bench_nomic_graph_entries",
                        help="Entry points table (entry_nid, centroid)")
    parser.add_argument("--query-table", default="bench_nomic_query_200")
    parser.add_argument("--query-limit", type=int, default=100)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--route-ks", default="1,2,4,8",
                        help="Comma-separated list of top-k partitions to test")
    args = parser.parse_args()

    route_ks = [int(x.strip()) for x in args.route_ks.split(",") if x.strip()]

    conn = psycopg2.connect(args.dsn)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    print(f"=== Partition routing ceiling analysis ===")
    print(f"dsn={args.dsn}")
    print(f"exact_table={args.exact_table}  main_table={args.main_table}")
    print(f"graph_table={args.graph_table}  entry_table={args.entry_table}")
    print(f"queries={args.query_limit}  k={args.k}  route_ks={route_ks}")
    print()

    # ---- Load entry centroids with partition_id ----
    # graph_entries has (entry_nid, centroid) -- one per partition medoid
    # We recover partition_id by joining: entry_nid -> graph_nodes.src_id -> main_table.partition_id
    t0 = time.time()
    print("[1/4] Loading entry centroids...")
    cur.execute(f"""
        SELECT e.entry_nid,
               e.centroid::text,
               m.partition_id
        FROM {args.entry_table} e
        JOIN {args.graph_table} g ON g.nid = e.entry_nid
        JOIN {args.main_table}  m ON m.id  = g.src_id
        ORDER BY m.partition_id
    """)
    entry_rows = cur.fetchall()
    if not entry_rows:
        print("ERROR: no rows in entry table", file=sys.stderr)
        return 1

    # Build arrays: one entry per partition
    entry_nids       = np.array([r[0] for r in entry_rows], dtype=np.int32)
    entry_centroids  = np.array([parse_hsvec_text(r[1]) for r in entry_rows])  # (P, D)
    entry_parts      = np.array([r[2] for r in entry_rows], dtype=np.int16)
    n_parts          = len(entry_rows)
    sketch_dim       = entry_centroids.shape[1]

    print(f"  {n_parts} partitions, sketch_dim={sketch_dim}  ({time.time()-t0:.2f}s)")

    # Pre-normalise centroids for fast cosine via dot product
    centroid_norms = np.linalg.norm(entry_centroids, axis=1, keepdims=True)
    centroid_norms[centroid_norms == 0] = 1e-10
    centroids_normed = entry_centroids / centroid_norms  # (P, D)

    # Build partition_id -> partition_index map
    part_to_idx = {int(p): i for i, p in enumerate(entry_parts)}

    # ---- Load queries ----
    print("[2/4] Loading queries...")
    cur.execute(f"""
        SELECT qid, qvec::text
        FROM {args.query_table}
        ORDER BY qid
        LIMIT %s
    """, (args.query_limit,))
    query_rows = cur.fetchall()
    if not query_rows:
        print("ERROR: no queries found", file=sys.stderr)
        return 1
    print(f"  {len(query_rows)} queries loaded  ({time.time()-t0:.2f}s)")

    # ---- For each query: GT + ceiling measurement ----
    print("[3/4] Computing GT and ceiling per query...")
    # Per-route-k coverage: list of coverage fractions
    coverage: dict[int, list[float]] = {rk: [] for rk in route_ks}
    # Per-query: which partitions actually cover all GT
    min_parts_for_full: list[int] = []

    for qi, (qid, qvec_text) in enumerate(query_rows):
        # Parse query vector (truncate to sketch_dim for routing comparison)
        qvec_full = parse_hsvec_text(qvec_text)
        qvec = qvec_full[:sketch_dim]
        qnorm = np.linalg.norm(qvec)
        if qnorm == 0:
            continue
        qvec_n = qvec / qnorm

        # Rank partitions by cosine distance (fast via dot product with pre-normed centroids)
        sims = centroids_normed @ qvec_n          # (P,)
        dists = 1.0 - sims
        part_rank = np.argsort(dists)             # partition indices, closest first

        # Compute exact GT (id + partition_id from main table)
        # Use exact_table for distance ordering, main_table for partition_id lookup.
        # If exact_table == main_table, one query suffices.
        if args.exact_table == args.main_table:
            cur.execute(f"""
                SELECT id::text, partition_id
                FROM {args.main_table}
                ORDER BY embedding <=> %s::svec
                LIMIT %s
            """, (qvec_text, args.k))
            gt_rows = cur.fetchall()
            gt_ids   = [r[0] for r in gt_rows]
            gt_parts = [int(r[1]) for r in gt_rows]
        else:
            # Get GT ids from exact table
            cur.execute(f"""
                SELECT id::text
                FROM {args.exact_table}
                ORDER BY embedding <=> %s::halfvec
                LIMIT %s
            """, (qvec_text, args.k))
            gt_ids = [r[0] for r in cur.fetchall()]

            # Get partition_id for each GT id from main table
            if gt_ids:
                cur.execute(f"""
                    SELECT id::text, partition_id
                    FROM {args.main_table}
                    WHERE id = ANY(%s)
                """, (gt_ids,))
                id_to_part = {r[0]: int(r[1]) for r in cur.fetchall()}
                gt_parts = [id_to_part.get(gid, -1) for gid in gt_ids]
            else:
                gt_parts = []

        n_gt = len(gt_ids)
        if n_gt == 0:
            continue

        # Set of partition indices for GT neighbors
        gt_part_idxs = set()
        for p in gt_parts:
            if p >= 0 and p in part_to_idx:
                gt_part_idxs.add(part_to_idx[p])

        # For each route-k, check coverage
        for rk in route_ks:
            routed_set = set(part_rank[:rk].tolist())
            covered = sum(
                1 for p in gt_parts
                if p >= 0 and p in part_to_idx and part_to_idx[p] in routed_set
            )
            coverage[rk].append(covered / n_gt)

        # Minimum partitions needed to cover all GT
        if gt_part_idxs:
            # How many top-ranked partitions until all GT partitions included?
            needed = 0
            found = set()
            for pidx in part_rank:
                needed += 1
                if pidx in gt_part_idxs:
                    found.add(pidx)
                if found >= gt_part_idxs:
                    break
            min_parts_for_full.append(needed)

        if (qi + 1) % 10 == 0:
            print(f"  {qi+1}/{len(query_rows)} queries done  ({time.time()-t0:.2f}s)")

    # ---- Report ----
    print()
    print("[4/4] Results")
    print("=" * 60)
    print(f"{'route_k':<10} {'avg_coverage':>14} {'p50_coverage':>14} {'p10_coverage':>14}")
    print("-" * 60)
    for rk in route_ks:
        cov = coverage[rk]
        if not cov:
            continue
        avg = statistics.mean(cov)
        p50 = statistics.median(cov)
        p10 = sorted(cov)[max(0, int(len(cov) * 0.10) - 1)]
        print(f"{rk:<10} {avg*100:>13.1f}% {p50*100:>13.1f}% {p10*100:>13.1f}%")
    print("-" * 60)

    print()
    print("Min partitions needed to cover 100% of GT (per query):")
    if min_parts_for_full:
        p50_min = statistics.median(min_parts_for_full)
        p90_min = sorted(min_parts_for_full)[int(len(min_parts_for_full) * 0.90)]
        max_min = max(min_parts_for_full)
        print(f"  p50={p50_min:.0f}  p90={p90_min:.0f}  max={max_min}")
    print()

    # Decision signal
    if coverage.get(2):
        avg2 = statistics.mean(coverage[2])
        if avg2 >= 0.90:
            verdict = "GO: top-2 covers >=90% avg. Hard filter prototype is viable."
        elif avg2 >= 0.80:
            verdict = "MARGINAL: top-2 covers 80-90% avg. Consider soft routing."
        else:
            verdict = "STOP: top-2 covers <80% avg. Hard filter will kill recall. Use soft routing."
        print(f"VERDICT (top-2): {verdict}")

    print(f"\nTotal time: {time.time()-t0:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
