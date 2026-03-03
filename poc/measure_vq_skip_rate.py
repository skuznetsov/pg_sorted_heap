#!/usr/bin/env python3
"""
PoC #4: Vector Quantization hash → sort → zone map pruning.

Key insight (from user): cluster_id from k-means IS the locality-preserving hash.
- hash(v) = nearest_centroid_id
- Sort table by hash → similar vectors land on same pages
- Zone map on hash column → direct block lookup
- Collisions are DESIRABLE: they co-locate similar vectors

This combines pg_sorted_heap's 99.9998% scalar skip rate with IVF-quality search,
but with ZERO separate index — the storage layout IS the index.

Tests:
1. VQ hash with varying codebook sizes (N_centroids ≈ N_pages, 2×, 4×, etc.)
2. Single-probe: query nearest centroid → scan that block range
3. Multi-probe: query top-P nearest centroids → scan P block ranges
4. Recall@k measurement (not just skip rate)
5. Comparison: random layout vs VQ-sorted layout
"""

import numpy as np
import time
from sklearn.cluster import MiniBatchKMeans

RANDOM_SEED = 42
N_QUERIES = 200
BLOCK_SIZE = 256  # vectors per page


def generate_clustered_data(n_vectors, dims, n_clusters=200, spread=0.15):
    """Generate synthetic embeddings mimicking real embedding distributions."""
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
    """Brute-force exact top-k nearest neighbors."""
    results = []
    for q in queries:
        dists = np.linalg.norm(vectors - q, axis=1)
        topk_idx = np.argpartition(dists, k)[:k]
        topk_idx = topk_idx[np.argsort(dists[topk_idx])]
        results.append(topk_idx)
    return results


def train_vq_codebook(vectors, n_centroids):
    """Train VQ codebook (k-means). Returns (kmeans_model, labels)."""
    kmeans = MiniBatchKMeans(
        n_clusters=n_centroids,
        batch_size=min(10000, len(vectors)),
        random_state=RANDOM_SEED,
        n_init=3,
        max_iter=100,
    )
    labels = kmeans.fit_predict(vectors)
    return kmeans, labels


def eval_vq_skip_rate(vectors, queries, gt, kmeans, labels,
                      block_size, top_k, n_probes, method_name):
    """
    VQ hash → sort by cluster_id → zone map lookup.

    For each query:
    1. Find top-P nearest centroids (P = n_probes)
    2. Look up which blocks contain vectors with those cluster_ids (zone map)
    3. Scan only those blocks
    4. Measure: blocks scanned, skip rate, recall
    """
    n = len(vectors)
    n_blocks = (n + block_size - 1) // block_size

    # Sort vectors by cluster_id (this is what pg_sorted_heap does)
    order = np.argsort(labels)
    sorted_labels = labels[order]
    # Map from original index to sorted position
    orig_to_sorted = np.empty(n, dtype=np.int64)
    orig_to_sorted[order] = np.arange(n)

    # Build zone map: for each block, track min/max cluster_id
    # Also build cluster_id → block range mapping (more efficient lookup)
    cluster_to_blocks = {}  # cluster_id → set of block indices
    block_orig_indices = []  # block_idx → list of original vector indices

    for i in range(n_blocks):
        start = i * block_size
        end = min(start + block_size, n)
        block_labels = sorted_labels[start:end]
        block_orig = order[start:end]
        block_orig_indices.append(set(block_orig))

        for cl in np.unique(block_labels):
            if cl not in cluster_to_blocks:
                cluster_to_blocks[cl] = set()
            cluster_to_blocks[cl].add(i)

    # For each query: find nearest centroids, look up blocks, measure
    blocks_scanned_list = []
    recall_list = []

    centroids = kmeans.cluster_centers_

    for qi in range(len(queries)):
        q = queries[qi]
        gt_set = set(gt[qi][:top_k])

        # Find top-P nearest centroids
        centroid_dists = np.linalg.norm(centroids - q, axis=1)
        nearest_centroids = np.argsort(centroid_dists)[:n_probes]

        # Look up blocks for these centroids (zone map lookup)
        blocks_to_scan = set()
        for cl in nearest_centroids:
            if cl in cluster_to_blocks:
                blocks_to_scan.update(cluster_to_blocks[cl])

        # Count blocks scanned
        blocks_scanned_list.append(len(blocks_to_scan))

        # Check recall: how many of the true top-k are in scanned blocks?
        found = set()
        for bi in blocks_to_scan:
            found.update(block_orig_indices[bi] & gt_set)

        recall_list.append(len(found) / top_k)

    avg_blocks = np.mean(blocks_scanned_list)
    p50_blocks = np.median(blocks_scanned_list)
    p95_blocks = np.percentile(blocks_scanned_list, 95)
    skip_rate = (1 - avg_blocks / n_blocks) * 100
    scan_rate = avg_blocks / n_blocks * 100
    avg_recall = np.mean(recall_list) * 100
    p5_recall = np.percentile(recall_list, 5) * 100  # worst 5%

    print(
        f"  {method_name:<40s} |  "
        f"blocks: avg={avg_blocks:6.1f} p50={p50_blocks:5.0f} p95={p95_blocks:5.0f} / {n_blocks}  |  "
        f"skip={skip_rate:5.1f}%  scan={scan_rate:5.1f}%  |  "
        f"recall: avg={avg_recall:5.1f}% p5={p5_recall:5.1f}%"
    )
    return skip_rate, avg_recall


def eval_vq_adaptive_probes(vectors, queries, gt, kmeans, labels,
                            block_size, top_k, target_recall, method_name):
    """
    Adaptive probing: expand centroid search until we've scanned enough
    blocks to likely contain top-k results. Measures the minimum probes
    needed for target recall.
    """
    n = len(vectors)
    n_blocks = (n + block_size - 1) // block_size
    n_centroids = len(kmeans.cluster_centers_)

    # Sort and build index (same as above)
    order = np.argsort(labels)
    sorted_labels = labels[order]

    cluster_to_blocks = {}
    block_orig_indices = []

    for i in range(n_blocks):
        start = i * block_size
        end = min(start + block_size, n)
        block_labels = sorted_labels[start:end]
        block_orig = order[start:end]
        block_orig_indices.append(set(block_orig))

        for cl in np.unique(block_labels):
            if cl not in cluster_to_blocks:
                cluster_to_blocks[cl] = set()
            cluster_to_blocks[cl].add(i)

    centroids = kmeans.cluster_centers_

    # For each query: expand probes until target recall achieved
    probes_needed_list = []
    blocks_at_target_list = []
    final_recall_list = []

    for qi in range(len(queries)):
        q = queries[qi]
        gt_set = set(gt[qi][:top_k])

        centroid_dists = np.linalg.norm(centroids - q, axis=1)
        centroid_order = np.argsort(centroid_dists)

        blocks_to_scan = set()
        found = set()
        probes_used = 0

        for ci in centroid_order:
            probes_used += 1
            if ci in cluster_to_blocks:
                for bi in cluster_to_blocks[ci]:
                    if bi not in blocks_to_scan:
                        blocks_to_scan.add(bi)
                        found.update(block_orig_indices[bi] & gt_set)

            recall = len(found) / top_k
            if recall >= target_recall:
                break

        probes_needed_list.append(probes_used)
        blocks_at_target_list.append(len(blocks_to_scan))
        final_recall_list.append(len(found) / top_k)

    avg_probes = np.mean(probes_needed_list)
    p50_probes = np.median(probes_needed_list)
    p95_probes = np.percentile(probes_needed_list, 95)
    avg_blocks = np.mean(blocks_at_target_list)
    skip_rate = (1 - avg_blocks / n_blocks) * 100
    avg_recall = np.mean(final_recall_list) * 100

    print(
        f"  {method_name:<40s} |  "
        f"probes: avg={avg_probes:5.1f} p50={p50_probes:4.0f} p95={p95_probes:4.0f} / {n_centroids}  |  "
        f"skip={skip_rate:5.1f}%  blocks={avg_blocks:5.0f}/{n_blocks}  |  "
        f"recall={avg_recall:5.1f}%"
    )
    return skip_rate, avg_recall, avg_probes


def main():
    for dims in [768, 1536]:
        n_vectors = 200000
        print(f"\n{'#'*70}")
        print(f"# {n_vectors} vectors, {dims} dims, block_size={BLOCK_SIZE}")
        print(f"{'#'*70}")

        vectors, queries = generate_clustered_data(n_vectors, dims)

        n_blocks = (n_vectors + BLOCK_SIZE - 1) // BLOCK_SIZE
        print(f"Total blocks: {n_blocks}")

        print("Computing ground truth top-10 ...", end=" ", flush=True)
        t0 = time.time()
        gt = ground_truth_topk(vectors, queries, k=10)
        print(f"({time.time()-t0:.1f}s)")

        top_k = 10

        # Test various codebook sizes
        # Key insight: N_centroids ≈ N_blocks gives ~1 cluster per block
        # More centroids = finer granularity, fewer blocks per probe
        codebook_sizes = [
            n_blocks // 4,     # ~4 blocks per cluster
            n_blocks // 2,     # ~2 blocks per cluster
            n_blocks,          # ~1 block per cluster (sweet spot?)
            n_blocks * 2,      # ~0.5 blocks per cluster
            n_blocks * 4,      # ~0.25 blocks per cluster
            1000,              # typical IVF size
            2000,
            4000,
        ]
        # Deduplicate and sort
        codebook_sizes = sorted(set(cs for cs in codebook_sizes if 10 <= cs <= n_vectors // 10))

        for n_centroids in codebook_sizes:
            print(f"\n--- VQ codebook: {n_centroids} centroids (≈{n_vectors/n_centroids:.0f} vec/cluster, "
                  f"≈{n_centroids/n_blocks:.1f} clusters/block) ---")

            print(f"  Training k-means ...", end=" ", flush=True)
            t0 = time.time()
            kmeans, labels = train_vq_codebook(vectors, n_centroids)
            print(f"({time.time()-t0:.1f}s)")

            # Fixed probe counts
            print(f"\n  --- Fixed probes (top-{top_k}) ---")
            for n_probes in [1, 2, 3, 5, 10, 20]:
                if n_probes > n_centroids:
                    continue
                eval_vq_skip_rate(
                    vectors, queries, gt, kmeans, labels,
                    BLOCK_SIZE, top_k, n_probes,
                    f"VQ-{n_centroids}c probe={n_probes}"
                )

            # Adaptive probing for target recall
            print(f"\n  --- Adaptive probes for target recall (top-{top_k}) ---")
            for target_recall in [0.90, 0.95, 0.99, 1.00]:
                eval_vq_adaptive_probes(
                    vectors, queries, gt, kmeans, labels,
                    BLOCK_SIZE, top_k, target_recall,
                    f"VQ-{n_centroids}c recall≥{target_recall:.0%}"
                )

    print(f"\n{'='*70}")
    print("ANALYSIS")
    print(f"{'='*70}")
    print("""
VQ hash = cluster_id from k-means codebook.
Sort table by cluster_id → pg_sorted_heap zone map provides direct lookup.

Key metrics:
  - skip rate: fraction of blocks NOT read (higher = better)
  - recall: fraction of true top-k found in scanned blocks (higher = better)
  - probes: number of centroids checked (fewer = faster)

Sweet spot: high skip rate (>90%) + high recall (>95%) + few probes (<10).

If VQ achieves this:
  → pg_sorted_heap + VQ hash = IVF-quality search with ZERO separate index.
  → Storage layout IS the index. Zone map IS the inverted file.
  → Compaction = re-cluster + re-sort (offline, like VACUUM FULL).
  → INSERT = append to cluster's block range (or overflow page).
""")


if __name__ == "__main__":
    main()
