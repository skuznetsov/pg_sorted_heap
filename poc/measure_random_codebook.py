#!/usr/bin/env python3
"""
PoC #5: Random Codebook VQ — training-free locality-preserving hash.

Key idea: Generate C random unit vectors at CREATE TABLE time.
hash(v) = argmax(centroids · v) = index of nearest random centroid.

No training needed. INSERT = one matrix multiply. SIMD-friendly.
Collisions = co-location of similar vectors on same page.

Also tests:
- Cross-polytope LSH (fast Hadamard + argmax)
- SimHash with Gray code ordering (fix for PoC #3 flaw)
- Comparison: trained k-means vs random codebook vs pure LSH
"""

import numpy as np
import time

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
# Hash functions (all training-free)
# ============================================================

def random_codebook_hash(vectors, n_centroids, rng):
    """
    Random Codebook VQ: generate random unit vectors, hash = nearest centroid.
    O(C × d) per vector. No training.
    """
    dims = vectors.shape[1]
    centroids = rng.randn(n_centroids, dims).astype(np.float32)
    centroids /= np.linalg.norm(centroids, axis=1, keepdims=True)
    # hash = argmax(centroids · v) for normalized vectors = nearest centroid
    similarities = vectors @ centroids.T  # n × C
    labels = np.argmax(similarities, axis=1)
    return labels, centroids


def cross_polytope_hash(vectors, n_tables, rng):
    """
    Cross-polytope LSH: random rotation + argmax of absolute value.
    Each hash table uses a different random rotation.
    Hash value = (dimension_with_max_abs, sign) → 2d buckets per table.
    For multiple tables: concatenate hash values.
    """
    dims = vectors.shape[1]
    # Random rotation via QR decomposition of random matrix
    # (Hadamard would be faster but QR is simpler for PoC)
    random_matrix = rng.randn(dims, dims).astype(np.float32)
    Q, _ = np.linalg.qr(random_matrix)
    Q = Q.astype(np.float32)
    rotated = vectors @ Q  # n × d
    # Hash = dimension with max absolute value × sign
    abs_rotated = np.abs(rotated)
    max_dims = np.argmax(abs_rotated, axis=1)  # which dimension
    signs = np.sign(rotated[np.arange(len(vectors)), max_dims])  # positive or negative
    signs = (signs > 0).astype(np.int64)
    hashes = max_dims * 2 + signs  # 0..2d-1
    return hashes


def simhash_gray(vectors, n_bits, rng):
    """
    SimHash with Gray code ordering.
    Fix for PoC #3: Hamming-close hashes become integer-close.
    """
    dims = vectors.shape[1]
    hyperplanes = rng.randn(dims, n_bits).astype(np.float32)
    projections = vectors @ hyperplanes
    bits = (projections > 0).astype(np.int64)

    use_bits = min(n_bits, 63)
    # Standard binary hash
    binary_hashes = np.zeros(len(vectors), dtype=np.int64)
    for b in range(use_bits):
        binary_hashes |= bits[:, b] << b

    # Convert to Gray code: gray = n XOR (n >> 1)
    gray_hashes = binary_hashes ^ (binary_hashes >> 1)
    return gray_hashes


def random_proj_zorder(vectors, n_dims, rng):
    """
    Random projection to n_dims + Z-order curve.
    Like PCA+Z-order from PoC #3 but training-free.
    """
    dims = vectors.shape[1]
    # Random projection matrix (JL lemma guarantee)
    proj = rng.randn(dims, n_dims).astype(np.float32) / np.sqrt(n_dims)
    reduced = vectors @ proj  # n × n_dims

    # Quantize to 0..255
    mins = reduced.min(axis=0)
    maxs = reduced.max(axis=0)
    ranges = maxs - mins
    ranges[ranges == 0] = 1
    quantized = ((reduced - mins) / ranges * 255).astype(np.uint64).clip(0, 255)

    # Z-order interleaving
    max_total_bits = 63
    bits_per_dim = max(1, max_total_bits // n_dims)
    hashes = np.zeros(len(vectors), dtype=np.int64)
    bit_pos = 0
    for b in range(bits_per_dim):
        for d in range(n_dims):
            if bit_pos >= 63:
                break
            hashes |= ((quantized[:, d] >> b) & 1).astype(np.int64) << bit_pos
            bit_pos += 1

    return hashes, (proj, mins, ranges, n_dims, bits_per_dim)


def compute_query_hashes(queries, hash_type, params):
    """Compute hashes for queries using same parameters."""
    if hash_type == "random_codebook":
        centroids = params
        sims = queries @ centroids.T
        return np.argmax(sims, axis=1)
    elif hash_type == "random_proj_zorder":
        proj, mins, ranges, n_dims, bits_per_dim = params
        reduced = queries @ proj
        quantized = ((reduced - mins) / ranges * 255).astype(np.uint64).clip(0, 255)
        hashes = np.zeros(len(queries), dtype=np.int64)
        bit_pos = 0
        for b in range(bits_per_dim):
            for d in range(n_dims):
                if bit_pos >= 63:
                    break
                hashes |= ((quantized[:, d] >> b) & 1).astype(np.int64) << bit_pos
                bit_pos += 1
        return hashes
    return None


# ============================================================
# Evaluation: VQ-style (cluster_id lookup)
# ============================================================

def eval_cluster_hash(vectors, queries, gt, labels, centroids,
                      block_size, top_k, n_probes, method_name):
    """
    Evaluate hash where hash = cluster_id.
    Query finds nearest centroids, scans corresponding blocks.
    """
    n = len(vectors)
    n_blocks = (n + block_size - 1) // block_size

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

    blocks_scanned_list = []
    recall_list = []

    for qi in range(len(queries)):
        q = queries[qi]
        gt_set = set(gt[qi][:top_k])

        centroid_dists = np.linalg.norm(centroids - q, axis=1)
        nearest_centroids = np.argsort(centroid_dists)[:n_probes]

        blocks_to_scan = set()
        for cl in nearest_centroids:
            if cl in cluster_to_blocks:
                blocks_to_scan.update(cluster_to_blocks[cl])

        found = set()
        for bi in blocks_to_scan:
            found.update(block_orig_indices[bi] & gt_set)

        blocks_scanned_list.append(len(blocks_to_scan))
        recall_list.append(len(found) / top_k)

    avg_blocks = np.mean(blocks_scanned_list)
    p50_blocks = np.median(blocks_scanned_list)
    p95_blocks = np.percentile(blocks_scanned_list, 95)
    skip_rate = (1 - avg_blocks / n_blocks) * 100
    avg_recall = np.mean(recall_list) * 100
    p5_recall = np.percentile(recall_list, 5) * 100

    print(
        f"  {method_name:<45s} |  "
        f"blocks: avg={avg_blocks:5.1f} p50={p50_blocks:4.0f} p95={p95_blocks:4.0f} / {n_blocks}  |  "
        f"skip={skip_rate:5.1f}%  |  "
        f"recall: avg={avg_recall:5.1f}% p5={p5_recall:5.1f}%"
    )
    return skip_rate, avg_recall


# ============================================================
# Evaluation: sort-based (zone map range scan)
# ============================================================

def eval_sorted_hash(vectors, queries, gt, hashes_db, hashes_q,
                     block_size, top_k, method_name):
    """
    Sort by hash. Build zone maps (min/max hash per block).
    Query scans blocks nearest to query hash.
    Measures blocks needed to find all true top-k.
    """
    n = len(vectors)
    n_blocks = (n + block_size - 1) // block_size

    order = np.argsort(hashes_db)
    sorted_hashes = hashes_db[order]

    block_mins = []
    block_maxs = []
    block_orig_indices = []
    for i in range(n_blocks):
        start = i * block_size
        end = min(start + block_size, n)
        block_h = sorted_hashes[start:end]
        block_mins.append(block_h[0])
        block_maxs.append(block_h[-1])
        block_orig_indices.append(set(order[start:end]))

    block_mins = np.array(block_mins, dtype=np.float64)
    block_maxs = np.array(block_maxs, dtype=np.float64)

    blocks_scanned_list = []
    recall_list = []

    for qi in range(len(queries)):
        qh = float(hashes_q[qi])
        gt_set = set(gt[qi][:top_k])

        block_mids = (block_mins + block_maxs) / 2.0
        block_dist = np.abs(block_mids - qh)
        block_order = np.argsort(block_dist)

        found = set()
        scanned = 0
        for bi in block_order:
            scanned += 1
            found.update(block_orig_indices[bi] & gt_set)
            if len(found) >= top_k:
                break

        blocks_scanned_list.append(scanned)
        recall_list.append(len(found) / top_k)

    avg_blocks = np.mean(blocks_scanned_list)
    p50_blocks = np.median(blocks_scanned_list)
    p95_blocks = np.percentile(blocks_scanned_list, 95)
    skip_rate = (1 - avg_blocks / n_blocks) * 100
    avg_recall = np.mean(recall_list) * 100

    print(
        f"  {method_name:<45s} |  "
        f"blocks: avg={avg_blocks:5.1f} p50={p50_blocks:4.0f} p95={p95_blocks:4.0f} / {n_blocks}  |  "
        f"skip={skip_rate:5.1f}%  |  recall=100.0% (exact)"
    )
    return skip_rate


# ============================================================
# Evaluation: adaptive probes for target recall (cluster hash)
# ============================================================

def eval_adaptive(vectors, queries, gt, labels, centroids,
                  block_size, top_k, target_recall, method_name):
    n = len(vectors)
    n_blocks = (n + block_size - 1) // block_size
    n_centroids = len(centroids)

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

    probes_needed_list = []
    blocks_at_target_list = []

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

    avg_probes = np.mean(probes_needed_list)
    p95_probes = np.percentile(probes_needed_list, 95)
    avg_blocks = np.mean(blocks_at_target_list)
    skip_rate = (1 - avg_blocks / n_blocks) * 100

    print(
        f"  {method_name:<45s} |  "
        f"probes: avg={avg_probes:4.1f} p95={p95_probes:4.0f} / {n_centroids}  |  "
        f"skip={skip_rate:5.1f}%  blocks≈{avg_blocks:.0f}/{n_blocks}"
    )
    return skip_rate, avg_probes


# ============================================================
# Main
# ============================================================

def main():
    rng = np.random.RandomState(RANDOM_SEED)

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

        # ============================================================
        # 1. Trained k-means (baseline — best possible VQ)
        # ============================================================
        from sklearn.cluster import MiniBatchKMeans

        for n_centroids in [n_blocks, n_blocks * 2, 1000]:
            print(f"\n{'='*80}")
            print(f"  TRAINED K-MEANS: {n_centroids} centroids")
            print(f"{'='*80}")
            t0 = time.time()
            kmeans = MiniBatchKMeans(
                n_clusters=n_centroids, batch_size=min(10000, n_vectors),
                random_state=RANDOM_SEED, n_init=3, max_iter=100,
            )
            trained_labels = kmeans.fit_predict(vectors)
            print(f"  Training: {time.time()-t0:.1f}s")

            for n_probes in [1, 3, 5, 10, 20]:
                if n_probes > n_centroids:
                    continue
                eval_cluster_hash(
                    vectors, queries, gt, trained_labels,
                    kmeans.cluster_centers_,
                    BLOCK_SIZE, top_k, n_probes,
                    f"Trained-{n_centroids}c probe={n_probes}"
                )

            print()
            for target_recall in [0.90, 0.95, 0.99]:
                eval_adaptive(
                    vectors, queries, gt, trained_labels,
                    kmeans.cluster_centers_,
                    BLOCK_SIZE, top_k, target_recall,
                    f"Trained-{n_centroids}c recall≥{target_recall:.0%}"
                )

        # ============================================================
        # 2. Random Codebook VQ (training-free)
        # ============================================================
        for n_centroids in [n_blocks, n_blocks * 2, 1000]:
            print(f"\n{'='*80}")
            print(f"  RANDOM CODEBOOK: {n_centroids} centroids")
            print(f"{'='*80}")
            t0 = time.time()
            rand_labels, rand_centroids = random_codebook_hash(vectors, n_centroids, rng)
            print(f"  Hash computation: {time.time()-t0:.1f}s (no training)")

            for n_probes in [1, 3, 5, 10, 20]:
                if n_probes > n_centroids:
                    continue
                eval_cluster_hash(
                    vectors, queries, gt, rand_labels, rand_centroids,
                    BLOCK_SIZE, top_k, n_probes,
                    f"Random-{n_centroids}c probe={n_probes}"
                )

            print()
            for target_recall in [0.90, 0.95, 0.99]:
                eval_adaptive(
                    vectors, queries, gt, rand_labels, rand_centroids,
                    BLOCK_SIZE, top_k, target_recall,
                    f"Random-{n_centroids}c recall≥{target_recall:.0%}"
                )

        # ============================================================
        # 3. Cross-polytope LSH (training-free, sort-based)
        # ============================================================
        print(f"\n{'='*80}")
        print(f"  CROSS-POLYTOPE LSH (sort-based)")
        print(f"{'='*80}")
        t0 = time.time()
        cp_hashes = cross_polytope_hash(vectors, 1, rng)
        cp_hashes_q = cross_polytope_hash(queries, 1, rng)
        print(f"  Hash computation: {time.time()-t0:.1f}s")
        eval_sorted_hash(vectors, queries, gt, cp_hashes, cp_hashes_q,
                        BLOCK_SIZE, top_k, "CrossPolytope-1table")

        # ============================================================
        # 4. SimHash + Gray code (training-free, sort-based)
        # ============================================================
        print(f"\n{'='*80}")
        print(f"  SIMHASH + GRAY CODE (sort-based)")
        print(f"{'='*80}")
        for n_bits in [8, 16, 32]:
            t0 = time.time()
            sh_hashes = simhash_gray(vectors, n_bits, rng)
            sh_hashes_q = simhash_gray(queries, n_bits, rng)
            dt = time.time() - t0
            eval_sorted_hash(vectors, queries, gt, sh_hashes, sh_hashes_q,
                            BLOCK_SIZE, top_k, f"SimHash-{n_bits}bit+Gray ({dt:.1f}s)")

        # ============================================================
        # 5. Random Projection + Z-order (training-free, sort-based)
        # ============================================================
        print(f"\n{'='*80}")
        print(f"  RANDOM PROJECTION + Z-ORDER (sort-based)")
        print(f"{'='*80}")
        for n_dims in [4, 8, 16]:
            t0 = time.time()
            rp_hashes, rp_params = random_proj_zorder(vectors, n_dims, rng)
            rp_hashes_q = compute_query_hashes(queries, "random_proj_zorder", rp_params)
            dt = time.time() - t0
            eval_sorted_hash(vectors, queries, gt, rp_hashes, rp_hashes_q,
                            BLOCK_SIZE, top_k, f"RandProj-{n_dims}D+Zorder ({dt:.1f}s)")

    # ============================================================
    # Summary
    # ============================================================
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")
    print("""
COMPARISON OF TRAINING-FREE HASHES:

Hash Type           | How it works                    | Training? | INSERT cost
--------------------|--------------------------------|-----------|-------------
Random Codebook VQ  | argmax(centroids · v)          | NO        | O(C × d)
Cross-polytope LSH  | random rotation + argmax(|v|)  | NO        | O(d²) or O(d log d)
SimHash + Gray      | sign(hyperplanes · v) + gray   | NO        | O(d × bits)
RandProj + Z-order  | proj to kD + interleave bits   | NO        | O(d × k)
Trained k-means     | nearest learned centroid       | YES       | O(C × d)

KEY INSIGHT:
  Random Codebook VQ uses EXACT SAME lookup mechanism as trained k-means
  (query → nearest centroids → scan those blocks).
  The only difference is centroid quality.

  If Random Codebook VQ is within ~80% of trained k-means skip rate,
  we get IVF-quality search with:
    - ZERO training on INSERT
    - Centroid improvement during COMPACT (optional)
    - pg_sorted_heap zone map machinery does the rest
""")


if __name__ == "__main__":
    main()
