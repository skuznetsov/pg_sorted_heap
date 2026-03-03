#!/usr/bin/env python3
"""
PoC #6: WTA (Winner-Takes-All) hash for vector locality.

WTA hash — O(d), zero training:
1. Divide d dims into k groups of d/k each
2. For each group: index of max value
3. Concatenate indices → hash

Two evaluation modes:
A) Sort-based: convert WTA tuple to scalar (mixed-radix), sort, zone map scan
B) Cluster-based: group vectors by WTA hash, query probes matching/similar hashes
"""

import numpy as np
import time
from collections import defaultdict

RANDOM_SEED = 42
N_QUERIES = 200
BLOCK_SIZE = 256


def generate_clustered_data(n_vectors, dims, n_clusters=200, spread=0.15):
    rng = np.random.RandomState(RANDOM_SEED)
    centers = rng.randn(n_clusters, dims).astype(np.float32)
    centers /= np.linalg.norm(centers, axis=1, keepdims=True)
    labels = rng.randint(0, n_clusters, n_vectors)
    vectors = centers[labels] + rng.randn(n_vectors, dims).astype(np.float32) * spread
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms[norms == 0] = 1
    vectors /= norms
    q_labels = rng.randint(0, n_clusters, N_QUERIES)
    queries = centers[q_labels] + rng.randn(N_QUERIES, dims).astype(np.float32) * spread
    norms = np.linalg.norm(queries, axis=1, keepdims=True)
    norms[norms == 0] = 1
    queries /= norms
    return vectors, queries


def ground_truth_topk(vectors, queries, k):
    results = []
    for q in queries:
        dists = np.linalg.norm(vectors - q, axis=1)
        topk_idx = np.argpartition(dists, k)[:k]
        topk_idx = topk_idx[np.argsort(dists[topk_idx])]
        results.append(topk_idx)
    return results


# ============================================================
# WTA hash variants
# ============================================================

def wta_hash(vectors, k_groups):
    """
    WTA hash: divide dims into k groups, argmax per group.
    Returns array of tuples (as list of arrays).
    """
    n, d = vectors.shape
    group_size = d // k_groups
    # Truncate to exact multiple
    used_dims = group_size * k_groups
    reshaped = vectors[:, :used_dims].reshape(n, k_groups, group_size)
    # argmax per group
    winners = np.argmax(reshaped, axis=2)  # n x k_groups, values in [0, group_size)
    return winners, group_size


def wta_to_scalar(winners, group_size):
    """Convert WTA tuple to scalar via mixed-radix encoding (for sort-based)."""
    n, k = winners.shape
    # Use as many bits as fit in int64
    bits_per_group = int(np.ceil(np.log2(group_size)))
    total_bits = bits_per_group * k
    if total_bits > 63:
        # Truncate to first groups that fit
        max_groups = 63 // bits_per_group
        winners = winners[:, :max_groups]
        k = max_groups

    hashes = np.zeros(n, dtype=np.int64)
    for g in range(k):
        hashes |= winners[:, g].astype(np.int64) << (g * bits_per_group)
    return hashes


def wta_hamming_distance(w1, w2):
    """Number of groups where winners differ."""
    return np.sum(w1 != w2, axis=-1)


# ============================================================
# Evaluation: sort-based (zone map range)
# ============================================================

def eval_sorted(vectors, queries, gt, hashes_db, hashes_q,
                block_size, top_k, method_name):
    n = len(vectors)
    n_blocks = (n + block_size - 1) // block_size

    order = np.argsort(hashes_db)
    sorted_hashes = hashes_db[order]

    block_mins = []
    block_maxs = []
    block_orig = []
    for i in range(n_blocks):
        start = i * block_size
        end = min(start + block_size, n)
        block_mins.append(sorted_hashes[start])
        block_maxs.append(sorted_hashes[end - 1])
        block_orig.append(set(order[start:end]))

    block_mins = np.array(block_mins, dtype=np.float64)
    block_maxs = np.array(block_maxs, dtype=np.float64)

    blocks_list = []
    for qi in range(len(queries)):
        qh = float(hashes_q[qi])
        gt_set = set(gt[qi][:top_k])
        block_mids = (block_mins + block_maxs) / 2.0
        block_order = np.argsort(np.abs(block_mids - qh))

        found = set()
        scanned = 0
        for bi in block_order:
            scanned += 1
            found.update(block_orig[bi] & gt_set)
            if len(found) >= top_k:
                break
        blocks_list.append(scanned)

    avg = np.mean(blocks_list)
    p50 = np.median(blocks_list)
    p95 = np.percentile(blocks_list, 95)
    skip = (1 - avg / n_blocks) * 100

    print(f"  {method_name:<45s} |  blocks: avg={avg:5.1f} p50={p50:4.0f} p95={p95:4.0f} / {n_blocks}  |  skip={skip:5.1f}%")
    return skip


# ============================================================
# Evaluation: cluster-based (WTA hash as cluster_id)
# ============================================================

def eval_wta_cluster(vectors, queries, gt, winners_db, winners_q,
                     block_size, top_k, n_probes_list, method_name):
    """
    Sort by WTA hash (scalar), build blocks.
    Query: compute WTA hash, find blocks with matching or similar hashes.
    Use Hamming distance on WTA tuples to rank centroids.
    """
    n = len(vectors)
    n_blocks = (n + block_size - 1) // block_size

    # Convert WTA tuples to scalar for sorting
    group_size = winners_db.max() + 1
    scalar_db = wta_to_scalar(winners_db, group_size)

    # Sort by scalar hash
    order = np.argsort(scalar_db)
    sorted_scalars = scalar_db[order]
    sorted_winners = winners_db[order]

    # Build blocks: store WTA tuples per block for matching
    block_orig = []
    block_wta_set = []  # unique WTA hashes per block
    for i in range(n_blocks):
        start = i * block_size
        end = min(start + block_size, n)
        block_orig.append(set(order[start:end]))

    # Build hash → block index
    # Group unique WTA hashes and their blocks
    hash_to_blocks = defaultdict(set)
    for i in range(n_blocks):
        start = i * block_size
        end = min(start + block_size, n)
        unique_hashes = set(map(tuple, sorted_winners[start:end]))
        for h in unique_hashes:
            hash_to_blocks[h].add(i)

    # All unique WTA hashes
    unique_hashes = list(hash_to_blocks.keys())
    unique_hash_arr = np.array(unique_hashes)  # M x k_groups

    for n_probes in n_probes_list:
        blocks_list = []
        recall_list = []

        for qi in range(len(queries)):
            q_wta = winners_q[qi]  # k_groups
            gt_set = set(gt[qi][:top_k])

            # Rank unique hashes by Hamming distance to query WTA
            hamming = wta_hamming_distance(unique_hash_arr, q_wta)
            nearest_idx = np.argsort(hamming)[:n_probes]

            # Collect blocks
            blocks_to_scan = set()
            for idx in nearest_idx:
                h = unique_hashes[idx]
                blocks_to_scan.update(hash_to_blocks[h])

            found = set()
            for bi in blocks_to_scan:
                found.update(block_orig[bi] & gt_set)

            blocks_list.append(len(blocks_to_scan))
            recall_list.append(len(found) / top_k)

        avg_blocks = np.mean(blocks_list)
        p95_blocks = np.percentile(blocks_list, 95)
        skip = (1 - avg_blocks / n_blocks) * 100
        avg_recall = np.mean(recall_list) * 100
        p5_recall = np.percentile(recall_list, 5) * 100

        print(
            f"  {method_name} probe={n_probes:<4d}                    "
            f"|  blocks: avg={avg_blocks:5.1f} p95={p95_blocks:4.0f} / {n_blocks}  "
            f"|  skip={skip:5.1f}%  |  recall: avg={avg_recall:5.1f}% p5={p5_recall:5.1f}%"
        )


def eval_wta_adaptive(vectors, queries, gt, winners_db, winners_q,
                      block_size, top_k, target_recall, method_name):
    """Expand WTA probes until target recall reached."""
    n = len(vectors)
    n_blocks = (n + block_size - 1) // block_size

    group_size = winners_db.max() + 1
    scalar_db = wta_to_scalar(winners_db, group_size)
    order = np.argsort(scalar_db)
    sorted_winners = winners_db[order]

    block_orig = []
    hash_to_blocks = defaultdict(set)
    for i in range(n_blocks):
        start = i * block_size
        end = min(start + block_size, n)
        block_orig.append(set(order[start:end]))
        for h in set(map(tuple, sorted_winners[start:end])):
            hash_to_blocks[h].add(i)

    unique_hashes = list(hash_to_blocks.keys())
    unique_hash_arr = np.array(unique_hashes)

    probes_list = []
    blocks_list = []

    for qi in range(len(queries)):
        q_wta = winners_q[qi]
        gt_set = set(gt[qi][:top_k])

        hamming = wta_hamming_distance(unique_hash_arr, q_wta)
        nearest_idx = np.argsort(hamming)

        blocks_to_scan = set()
        found = set()
        probes_used = 0

        for idx in nearest_idx:
            probes_used += 1
            h = unique_hashes[idx]
            for bi in hash_to_blocks[h]:
                if bi not in blocks_to_scan:
                    blocks_to_scan.add(bi)
                    found.update(block_orig[bi] & gt_set)

            if len(found) / top_k >= target_recall:
                break

        probes_list.append(probes_used)
        blocks_list.append(len(blocks_to_scan))

    avg_probes = np.mean(probes_list)
    p95_probes = np.percentile(probes_list, 95)
    avg_blocks = np.mean(blocks_list)
    skip = (1 - avg_blocks / n_blocks) * 100
    n_unique = len(unique_hashes)

    print(
        f"  {method_name:<45s} |  probes: avg={avg_probes:5.0f} p95={p95_probes:5.0f} / {n_unique}  "
        f"|  skip={skip:5.1f}%  blocks={avg_blocks:5.0f}/{n_blocks}"
    )
    return skip


# ============================================================
# Main
# ============================================================

def main():
    for dims in [768, 1536]:
        n_vectors = 200000
        n_blocks = (n_vectors + BLOCK_SIZE - 1) // BLOCK_SIZE

        print(f"\n{'#'*80}")
        print(f"# {n_vectors} vectors, {dims} dims, block_size={BLOCK_SIZE}, blocks={n_blocks}")
        print(f"{'#'*80}")

        vectors, queries = generate_clustered_data(n_vectors, dims)

        print("Computing ground truth top-10 ...", end=" ", flush=True)
        t0 = time.time()
        gt = ground_truth_topk(vectors, queries, k=10)
        print(f"({time.time()-t0:.1f}s)")

        top_k = 10

        for k_groups in [4, 8, 16, 32, 64, 128]:
            if k_groups > dims:
                continue
            group_size = dims // k_groups

            print(f"\n{'='*80}")
            print(f"  WTA-{k_groups} groups × {group_size} dims/group")
            print(f"{'='*80}")

            t0 = time.time()
            winners_db, gs = wta_hash(vectors, k_groups)
            winners_q, _ = wta_hash(queries, k_groups)
            dt = time.time() - t0

            # Count unique hashes
            unique_db = len(set(map(tuple, winners_db)))
            unique_q = len(set(map(tuple, winners_q)))
            print(f"  Hash time: {dt:.2f}s | Unique hashes: {unique_db} (of {n_vectors})")

            # A) Sort-based evaluation
            print(f"\n  --- Sort-based (zone map range scan) ---")
            scalar_db = wta_to_scalar(winners_db, gs)
            scalar_q = wta_to_scalar(winners_q, gs)
            eval_sorted(vectors, queries, gt, scalar_db, scalar_q,
                       BLOCK_SIZE, top_k, f"WTA-{k_groups}g sort-based")

            # B) Cluster-based evaluation
            print(f"\n  --- Cluster-based (Hamming distance probing) ---")
            eval_wta_cluster(vectors, queries, gt, winners_db, winners_q,
                           BLOCK_SIZE, top_k, [1, 5, 10, 20, 50, 100],
                           f"WTA-{k_groups}g")

            # C) Adaptive probes for target recall
            print(f"\n  --- Adaptive probes ---")
            for target in [0.90, 0.95, 0.99]:
                eval_wta_adaptive(vectors, queries, gt, winners_db, winners_q,
                                BLOCK_SIZE, top_k, target,
                                f"WTA-{k_groups}g recall≥{target:.0%}")

    print(f"\n{'='*80}")
    print("ANALYSIS")
    print(f"{'='*80}")
    print("""
WTA hash: O(d), zero training, zero matrix multiply.
Just argmax per group of dimensions.

Compare with:
  - SimHash/CrossPolytope (sort-based): ~10% skip
  - Random Codebook VQ (cluster-based): ~67% skip @ 90% recall
  - Trained k-means (cluster-based): ~65% skip @ 90% recall

If WTA cluster-based matches Random Codebook → we have the simplest viable hash.
""")


if __name__ == "__main__":
    main()
