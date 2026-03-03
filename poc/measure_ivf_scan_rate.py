#!/usr/bin/env python3
"""
PoC #2: Alternative pruning strategies for vector table AM.

After PoC #1 showed centroid+radius bounding sphere pruning is 0% effective
in 768+ dims (curse of dimensionality), test approaches that ACTUALLY work:

1. IVF-in-storage: Cluster vectors, scan nearest clusters first, early stop.
   Question: What fraction of data do we scan to find true top-k?

2. Quantized pre-filter: SBQ (1-bit) per vector in page header.
   Question: How much faster is approximate scan + rerank vs full scan?

3. Centroid ranking (no bounding sphere): Just sort blocks by centroid
   proximity, scan in order with early termination heuristic.
   Question: How many blocks until we find true top-k?

4. Two-phase: quantized scan ALL vectors → identify candidate blocks →
   full-precision scan only candidates.
   Question: What's the recall at various candidate fractions?
"""

import numpy as np
import time
import sys
from sklearn.cluster import MiniBatchKMeans

RANDOM_SEED = 42
N_QUERIES = 200


def generate_clustered_data(n_vectors, dims, n_clusters, spread=0.15):
    """Generate realistic embeddings on low-dimensional manifold."""
    rng = np.random.RandomState(RANDOM_SEED)
    centers = rng.randn(n_clusters, dims).astype(np.float32)
    centers /= np.linalg.norm(centers, axis=1, keepdims=True)
    labels = rng.randint(0, n_clusters, n_vectors)
    vectors = centers[labels] + rng.randn(n_vectors, dims).astype(np.float32) * spread
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms[norms == 0] = 1
    vectors /= norms

    # Queries from same distribution
    q_labels = rng.randint(0, n_clusters, N_QUERIES)
    queries = centers[q_labels] + rng.randn(N_QUERIES, dims).astype(np.float32) * spread
    norms = np.linalg.norm(queries, axis=1, keepdims=True)
    norms[norms == 0] = 1
    queries /= norms

    return vectors, queries


def ground_truth_topk(vectors, queries, k):
    """Compute true top-k neighbors (brute force)."""
    # Use dot product for angular similarity (vectors are normalized)
    # Higher dot product = more similar, so we want LARGEST dot products
    # Equivalently, smallest L2 distance (for normalized vectors: L2^2 = 2 - 2*dot)
    results = []
    for q in queries:
        dists = np.linalg.norm(vectors - q, axis=1)
        topk_idx = np.argpartition(dists, k)[:k]
        topk_idx = topk_idx[np.argsort(dists[topk_idx])]
        results.append((topk_idx, dists[topk_idx]))
    return results


# ============================================================
# Strategy 1: IVF-in-storage
# ============================================================

def test_ivf_in_storage(vectors, queries, gt, n_clusters_list, top_k=10):
    """
    Cluster vectors into n_clusters groups (simulating page groups).
    For each query, rank clusters by centroid proximity, scan nearest
    clusters first. Measure: how many clusters needed for 95%/99% recall?
    """
    print(f"\n{'='*60}")
    print(f"STRATEGY 1: IVF-in-storage (scan nearest clusters first)")
    print(f"{'='*60}")

    n = len(vectors)
    dims = vectors.shape[1]

    for n_clusters in n_clusters_list:
        if n_clusters >= n:
            continue

        t0 = time.time()
        kmeans = MiniBatchKMeans(
            n_clusters=n_clusters,
            batch_size=min(10000, n),
            random_state=RANDOM_SEED,
            n_init=1,
        )
        labels = kmeans.fit_predict(vectors)
        centroids = kmeans.cluster_centers_
        build_time = time.time() - t0

        # Group vectors by cluster
        clusters = [[] for _ in range(n_clusters)]
        cluster_indices = [[] for _ in range(n_clusters)]
        for i, label in enumerate(labels):
            clusters[label].append(vectors[i])
            cluster_indices[label].append(i)

        # For each recall target, measure probes needed
        for recall_target in [0.90, 0.95, 0.99, 1.00]:
            probes_needed = []
            vectors_scanned_fracs = []

            for qi in range(len(queries)):
                q = queries[qi]
                gt_set = set(gt[qi][0][:top_k])

                # Rank clusters by centroid distance
                c_dists = np.linalg.norm(centroids - q, axis=1)
                c_order = np.argsort(c_dists)

                found = set()
                vectors_scanned = 0
                probes = 0

                for ci in c_order:
                    probes += 1
                    c_vecs = clusters[ci]
                    c_idx = cluster_indices[ci]
                    vectors_scanned += len(c_vecs)

                    for idx in c_idx:
                        if idx in gt_set:
                            found.add(idx)

                    recall = len(found) / top_k
                    if recall >= recall_target:
                        break

                probes_needed.append(probes)
                vectors_scanned_fracs.append(vectors_scanned / n * 100)

            avg_probes = np.mean(probes_needed)
            avg_scan = np.mean(vectors_scanned_fracs)
            p95_scan = np.percentile(vectors_scanned_fracs, 95)

            print(
                f"  clusters={n_clusters:>4d}  recall≥{recall_target:.0%}  |  "
                f"probes: {avg_probes:5.1f}/{n_clusters} ({avg_probes/n_clusters*100:4.1f}%)  |  "
                f"vectors scanned: avg={avg_scan:5.1f}% p95={p95_scan:5.1f}%"
            )


# ============================================================
# Strategy 2: Quantized pre-filter (SBQ-like)
# ============================================================

def test_quantized_prefilter(vectors, queries, gt, top_k=10):
    """
    Simulate SBQ: quantize each vector to 1-bit per dimension.
    Compute approximate distances using Hamming distance on binary codes.
    Select top candidates, verify with full precision.
    Measure recall at various candidate fractions.
    """
    print(f"\n{'='*60}")
    print(f"STRATEGY 2: Quantized pre-filter (1-bit SBQ)")
    print(f"{'='*60}")

    n = len(vectors)
    dims = vectors.shape[1]

    # Quantize: each dimension -> 1 bit (above/below mean)
    means = vectors.mean(axis=0)
    binary_db = (vectors > means).astype(np.uint8)  # n x dims binary matrix

    t0 = time.time()
    # Pack into uint64 for fast Hamming
    n_words = (dims + 63) // 64
    packed_db = np.zeros((n, n_words), dtype=np.uint64)
    for w in range(n_words):
        start = w * 64
        end = min(start + 64, dims)
        for b in range(end - start):
            packed_db[:, w] |= binary_db[:, start + b].astype(np.uint64) << b
    pack_time = time.time() - t0
    print(f"  Quantization: {dims}d → {n_words} uint64 words ({n_words*8} bytes/vector)")
    print(f"  Full precision: {dims*4} bytes/vector")
    print(f"  Compression: {dims*4/(n_words*8):.1f}x")
    print(f"  Pack time: {pack_time:.1f}s")

    for candidate_frac in [0.01, 0.02, 0.05, 0.10, 0.20, 0.50]:
        n_candidates = max(top_k, int(n * candidate_frac))
        recalls = []

        t0 = time.time()
        for qi in range(len(queries)):
            q = queries[qi]
            gt_set = set(gt[qi][0][:top_k])

            # Quantize query
            q_binary = (q > means).astype(np.uint8)
            q_packed = np.zeros(n_words, dtype=np.uint64)
            for w in range(n_words):
                start = w * 64
                end = min(start + 64, dims)
                for b in range(end - start):
                    q_packed[w] |= np.uint64(q_binary[start + b]) << b

            # Hamming distances (XOR + popcount)
            xor_result = packed_db ^ q_packed
            hamming = np.zeros(n, dtype=np.int32)
            for w in range(n_words):
                # popcount via numpy
                col = xor_result[:, w]
                # Bit counting
                for shift in [1, 2, 4, 8, 16, 32]:
                    mask = np.uint64((1 << shift) - 1)
                    # Apply mask pattern
                    if shift == 1:
                        hamming += (col & np.uint64(0x5555555555555555)).astype(np.int32) + \
                                   ((col >> np.uint64(1)) & np.uint64(0x5555555555555555)).astype(np.int32)
                    # Simplified: just use bin() counting
                # Simpler approach: convert to bytes and use lookup
                pass

            # Actually, simplest correct popcount with numpy:
            hamming_dists = np.zeros(n, dtype=np.int32)
            for w in range(n_words):
                x = xor_result[:, w].copy()
                # Kernighan's bit counting is too slow for arrays
                # Use the standard parallel bit count
                x = x - ((x >> np.uint64(1)) & np.uint64(0x5555555555555555))
                x = (x & np.uint64(0x3333333333333333)) + \
                    ((x >> np.uint64(2)) & np.uint64(0x3333333333333333))
                x = (x + (x >> np.uint64(4))) & np.uint64(0x0F0F0F0F0F0F0F0F)
                count = (x * np.uint64(0x0101010101010101)) >> np.uint64(56)
                hamming_dists += count.astype(np.int32)

            # Select top candidates by Hamming distance
            candidate_idx = np.argpartition(hamming_dists, n_candidates)[:n_candidates]

            # Rerank with full precision
            candidate_vecs = vectors[candidate_idx]
            true_dists = np.linalg.norm(candidate_vecs - q, axis=1)
            best_idx = candidate_idx[np.argsort(true_dists)[:top_k]]

            # Measure recall
            recall = len(set(best_idx) & gt_set) / top_k
            recalls.append(recall)

        elapsed = time.time() - t0
        avg_recall = np.mean(recalls)
        print(
            f"  candidates={candidate_frac:5.1%} ({n_candidates:>6d})  |  "
            f"recall@{top_k}={avg_recall:.3f}  |  "
            f"vectors reranked: {n_candidates/n*100:5.1f}%  |  "
            f"{elapsed/len(queries)*1000:.1f}ms/query"
        )


# ============================================================
# Strategy 3: Centroid-ordered scan with early termination
# ============================================================

def test_centroid_ordered_scan(vectors, queries, gt, block_size=256, top_k=10):
    """
    Organize vectors into blocks (clustered). Sort blocks by centroid distance
    to query. Scan in order. Measure: how many blocks until true top-k found?
    """
    print(f"\n{'='*60}")
    print(f"STRATEGY 3: Centroid-ordered scan (block_size={block_size})")
    print(f"{'='*60}")

    n = len(vectors)
    n_blocks = (n + block_size - 1) // block_size
    n_clusters = min(n_blocks, n // 2)

    # Cluster and organize
    kmeans = MiniBatchKMeans(
        n_clusters=n_clusters,
        batch_size=min(10000, n),
        random_state=RANDOM_SEED,
        n_init=1,
    )
    labels = kmeans.fit_predict(vectors)
    order = np.argsort(labels)
    sorted_vectors = vectors[order]
    sorted_orig_idx = np.arange(n)[order]

    # Build blocks
    blocks = []
    block_centroids = []
    block_orig_indices = []
    for i in range(n_blocks):
        start = i * block_size
        end = min(start + block_size, n)
        block_vecs = sorted_vectors[start:end]
        blocks.append(block_vecs)
        block_centroids.append(block_vecs.mean(axis=0))
        block_orig_indices.append(sorted_orig_idx[start:end])

    block_centroids = np.array(block_centroids)

    # For each recall target, how many blocks?
    for recall_target in [0.90, 0.95, 0.99, 1.00]:
        blocks_needed = []

        for qi in range(len(queries)):
            q = queries[qi]
            gt_set = set(gt[qi][0][:top_k])

            # Sort blocks by centroid distance
            c_dists = np.linalg.norm(block_centroids - q, axis=1)
            c_order = np.argsort(c_dists)

            found = set()
            scanned = 0

            for bi in c_order:
                scanned += 1
                for idx in block_orig_indices[bi]:
                    if idx in gt_set:
                        found.add(idx)
                if len(found) / top_k >= recall_target:
                    break

            blocks_needed.append(scanned)

        avg_blocks = np.mean(blocks_needed)
        p95_blocks = np.percentile(blocks_needed, 95)
        print(
            f"  recall≥{recall_target:.0%}  |  "
            f"blocks: avg={avg_blocks:5.1f}/{n_blocks} ({avg_blocks/n_blocks*100:4.1f}%)  |  "
            f"p95={p95_blocks:.0f} ({p95_blocks/n_blocks*100:4.1f}%)"
        )


# ============================================================
# Strategy 4: Two-phase (quantized scan + block rerank)
# ============================================================

def test_two_phase(vectors, queries, gt, block_size=256, top_k=10):
    """
    Phase 1: Scan ALL vectors using 1-bit quantized representation (fast).
    Phase 2: Identify blocks containing top candidates, read only those blocks
             at full precision for final reranking.
    """
    print(f"\n{'='*60}")
    print(f"STRATEGY 4: Two-phase quantized scan → block rerank")
    print(f"{'='*60}")

    n = len(vectors)
    dims = vectors.shape[1]
    n_blocks = (n + block_size - 1) // block_size

    # Cluster and organize
    n_clusters = min(n_blocks, n // 2)
    kmeans = MiniBatchKMeans(
        n_clusters=n_clusters,
        batch_size=min(10000, n),
        random_state=RANDOM_SEED,
        n_init=1,
    )
    labels = kmeans.fit_predict(vectors)
    order = np.argsort(labels)
    sorted_vectors = vectors[order]

    # Block assignment for each sorted vector
    block_ids = np.arange(n) // block_size

    # Quantize ALL sorted vectors
    means = sorted_vectors.mean(axis=0)
    binary_all = (sorted_vectors > means)  # bool array

    # For speed: compute approximate distances using float dot product
    # on binary vectors (equivalent to Hamming distance ranking)
    binary_float = binary_all.astype(np.float32)
    means_q = means  # query quantization threshold

    for rerank_frac in [0.01, 0.02, 0.05, 0.10]:
        recalls = []
        blocks_read = []

        for qi in range(len(queries)):
            q = queries[qi]
            gt_set = set(gt[qi][0][:top_k])

            # Phase 1: approximate distances via binary dot product
            q_binary = (q > means_q).astype(np.float32)
            # Hamming = dims - 2 * dot(q_binary, db_binary) + sum(q_binary) (monotonic with dot)
            approx_scores = binary_float @ q_binary
            # Higher score = more bits agree = more similar

            # Select top candidates
            n_candidates = max(top_k * 2, int(n * rerank_frac))
            candidate_idx = np.argpartition(-approx_scores, n_candidates)[:n_candidates]

            # Phase 2: identify unique blocks to read
            candidate_blocks = set(block_ids[candidate_idx])
            n_blocks_read = len(candidate_blocks)

            # Read full vectors from candidate blocks
            mask = np.isin(block_ids, list(candidate_blocks))
            block_vector_idx = np.where(mask)[0]
            block_vectors = sorted_vectors[block_vector_idx]

            # Map back to original indices for recall
            orig_idx_map = order[block_vector_idx]

            # Full precision rerank
            true_dists = np.linalg.norm(block_vectors - q, axis=1)
            best_local = np.argsort(true_dists)[:top_k]
            best_orig = set(orig_idx_map[best_local])

            recall = len(best_orig & gt_set) / top_k
            recalls.append(recall)
            blocks_read.append(n_blocks_read)

        avg_recall = np.mean(recalls)
        avg_blocks = np.mean(blocks_read)
        print(
            f"  rerank={rerank_frac:5.1%}  |  "
            f"recall@{top_k}={avg_recall:.3f}  |  "
            f"blocks read: {avg_blocks:.0f}/{n_blocks} ({avg_blocks/n_blocks*100:4.1f}%)  |  "
            f"I/O savings: {(1-avg_blocks/n_blocks)*100:.0f}%"
        )


# ============================================================
# Main
# ============================================================

def main():
    np.random.seed(RANDOM_SEED)

    for dims, n_clusters_data in [(768, 200), (1536, 200)]:
        n_vectors = 200000
        print(f"\n{'#'*60}")
        print(f"# DATASET: {n_vectors} vectors, {dims} dims, {n_clusters_data} clusters")
        print(f"{'#'*60}")

        vectors, queries = generate_clustered_data(
            n_vectors, dims, n_clusters_data
        )

        print("Computing ground truth top-k ...", end=" ", flush=True)
        t0 = time.time()
        gt = ground_truth_topk(vectors, queries, k=100)
        print(f"({time.time()-t0:.1f}s)")

        # Strategy 1: IVF-in-storage
        test_ivf_in_storage(
            vectors, queries, gt,
            n_clusters_list=[100, 200, 500, 1000],
            top_k=10
        )

        # Strategy 2: Quantized pre-filter
        test_quantized_prefilter(vectors, queries, gt, top_k=10)

        # Strategy 3: Centroid-ordered scan
        test_centroid_ordered_scan(vectors, queries, gt, block_size=256, top_k=10)

        # Strategy 4: Two-phase
        test_two_phase(vectors, queries, gt, block_size=256, top_k=10)

    # Summary
    print(f"\n{'='*60}")
    print("CONCLUSIONS")
    print(f"{'='*60}")
    print("""
PoC #1 showed centroid+radius bounding sphere = 0% skip rate in 768+ dims.

Viable alternatives for vector table AM:

1. IVF-in-storage: Cluster-ordered pages + centroid ranking.
   The storage layout IS the IVF index. No separate index structure.
   Scan nearest clusters first with early termination.

2. Quantized pre-filter: Binary (SBQ) codes in page headers.
   Approximate ALL-vector scan in ~1/32 the memory.
   Identify candidate blocks, full-precision rerank.

3. Two-phase: Combines quantized scan + block-level I/O savings.
   Only read full pages for blocks containing candidates.

Key insight: The pruning mechanism should be RANKING + EARLY STOP,
not GUARANTEED SKIP (which fails in high dims).
""")


if __name__ == "__main__":
    main()
