#!/usr/bin/env python3
"""
PoC #7: Three novel hash candidates + variable cluster tightness.

Candidates:
A) SimHash(log₂C bits) + cluster-based lookup (NOT sort-based!)
B) PQ-Hash: Product Quantization with random codebooks per subspace
C) Projection+Grid: k random projections + grid quantization

Control variable: cluster spread (0.01, 0.05, 0.10, 0.15, 0.30)
  - 0.01 = very tight clusters (realistic embeddings)
  - 0.15 = our previous PoCs (loose, adversarial)
  - 0.30 = nearly random (worst case)

Baseline: Random Codebook VQ (from PoC #5)
"""

import numpy as np
import time
from collections import defaultdict

RANDOM_SEED = 42
N_QUERIES = 200
BLOCK_SIZE = 256
TOP_K = 10


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
# Hash A: SimHash cluster-based
# ============================================================

def simhash_cluster(vectors, n_bits, rng):
    """SimHash: k random hyperplanes → k-bit hash → 2^k buckets."""
    dims = vectors.shape[1]
    hyperplanes = rng.randn(n_bits, dims).astype(np.float32)
    hyperplanes /= np.linalg.norm(hyperplanes, axis=1, keepdims=True)
    projections = vectors @ hyperplanes.T  # n × k
    bits = (projections > 0).astype(np.int32)
    # Convert to integer hash
    hashes = np.zeros(len(vectors), dtype=np.int64)
    for b in range(n_bits):
        hashes |= bits[:, b].astype(np.int64) << b
    return hashes, hyperplanes


def simhash_query(queries, hyperplanes):
    projections = queries @ hyperplanes.T
    bits = (projections > 0).astype(np.int32)
    n_bits = hyperplanes.shape[0]
    hashes = np.zeros(len(queries), dtype=np.int64)
    for b in range(n_bits):
        hashes |= bits[:, b].astype(np.int64) << b
    # Also compute projection magnitudes for probing order
    return hashes, np.abs(projections)


def hamming_neighbors(h, n_bits, max_dist):
    """Generate all hashes within Hamming distance max_dist of h."""
    neighbors = [h]
    if max_dist >= 1:
        for i in range(n_bits):
            neighbors.append(h ^ (1 << i))
    if max_dist >= 2:
        for i in range(n_bits):
            for j in range(i + 1, n_bits):
                neighbors.append(h ^ (1 << i) ^ (1 << j))
    if max_dist >= 3:
        for i in range(n_bits):
            for j in range(i + 1, n_bits):
                for k in range(j + 1, n_bits):
                    neighbors.append(h ^ (1 << i) ^ (1 << j) ^ (1 << k))
    return neighbors


# ============================================================
# Hash B: PQ-Hash (Product Quantization with random codebooks)
# ============================================================

def pq_hash(vectors, m_subspaces, k_centroids, rng):
    """
    Split dims into m subspaces, K random centroids per subspace.
    hash = tuple of nearest centroid indices → single integer.
    """
    n, d = vectors.shape
    sub_dim = d // m_subspaces
    used_dims = sub_dim * m_subspaces

    codebooks = []
    sub_labels = np.zeros((n, m_subspaces), dtype=np.int32)

    for s in range(m_subspaces):
        start = s * sub_dim
        end = start + sub_dim
        sub_vectors = vectors[:, start:end]

        # Random codebook for this subspace
        cb = rng.randn(k_centroids, sub_dim).astype(np.float32)
        cb /= np.linalg.norm(cb, axis=1, keepdims=True)
        codebooks.append(cb)

        # Assign each vector to nearest centroid in subspace
        # similarities = sub_vectors @ cb.T
        # For L2 on normalized sub-vectors, use dot product
        sims = sub_vectors @ cb.T  # n × K
        sub_labels[:, s] = np.argmax(sims, axis=1)

    # Combine into single hash via mixed-radix
    hashes = np.zeros(n, dtype=np.int64)
    multiplier = 1
    for s in range(m_subspaces):
        hashes += sub_labels[:, s].astype(np.int64) * multiplier
        multiplier *= k_centroids

    return hashes, codebooks, sub_labels


def pq_query(queries, codebooks, m_subspaces, k_centroids):
    """Compute PQ hash for queries and distance tables for probing."""
    n, d = queries.shape
    sub_dim = d // m_subspaces

    sub_labels = np.zeros((n, m_subspaces), dtype=np.int32)
    # Distance tables: for each query, for each subspace, distance to all centroids
    dist_tables = np.zeros((n, m_subspaces, k_centroids), dtype=np.float32)

    for s in range(m_subspaces):
        start = s * sub_dim
        end = start + sub_dim
        sub_queries = queries[:, start:end]
        cb = codebooks[s]
        sims = sub_queries @ cb.T
        sub_labels[:, s] = np.argmax(sims, axis=1)
        # Store negative similarity as distance (for ranking)
        dist_tables[:, s, :] = -sims

    hashes = np.zeros(n, dtype=np.int64)
    multiplier = 1
    for s in range(m_subspaces):
        hashes += sub_labels[:, s].astype(np.int64) * multiplier
        multiplier *= k_centroids

    return hashes, sub_labels, dist_tables


# ============================================================
# Hash C: Projection + Grid
# ============================================================

def proj_grid_hash(vectors, k_dims, n_bins, rng):
    """Project to k random dims, quantize to grid cells."""
    n, d = vectors.shape
    directions = rng.randn(k_dims, d).astype(np.float32)
    directions /= np.linalg.norm(directions, axis=1, keepdims=True)
    projections = vectors @ directions.T  # n × k

    # Quantize each dimension to n_bins levels
    # Map from actual range to [0, n_bins-1]
    mins = projections.min(axis=0)
    maxs = projections.max(axis=0)
    ranges = maxs - mins
    ranges[ranges == 0] = 1
    quantized = ((projections - mins) / ranges * (n_bins - 0.001)).astype(np.int32)
    quantized = np.clip(quantized, 0, n_bins - 1)

    # Convert to hash via mixed-radix
    hashes = np.zeros(n, dtype=np.int64)
    multiplier = 1
    for dim in range(k_dims):
        hashes += quantized[:, dim].astype(np.int64) * multiplier
        multiplier *= n_bins

    return hashes, directions, mins, ranges, quantized


def proj_grid_query(queries, directions, mins, ranges, n_bins):
    projections = queries @ directions.T
    quantized = ((projections - mins) / ranges * (n_bins - 0.001)).astype(np.int32)
    quantized = np.clip(quantized, 0, n_bins - 1)

    hashes = np.zeros(len(queries), dtype=np.int64)
    k_dims = directions.shape[0]
    multiplier = 1
    for dim in range(k_dims):
        hashes += quantized[:, dim].astype(np.int64) * multiplier
        multiplier *= n_bins

    return hashes, quantized


# ============================================================
# Baseline: Random Codebook VQ
# ============================================================

def random_codebook(vectors, n_centroids, rng):
    dims = vectors.shape[1]
    centroids = rng.randn(n_centroids, dims).astype(np.float32)
    centroids /= np.linalg.norm(centroids, axis=1, keepdims=True)
    sims = vectors @ centroids.T
    labels = np.argmax(sims, axis=1)
    return labels, centroids


# ============================================================
# Universal cluster-based evaluator
# ============================================================

def eval_cluster_adaptive(vectors, queries, gt, labels, n_buckets,
                          probe_order_fn, block_size, top_k,
                          target_recalls, method_name):
    """
    Evaluate any hash in cluster-based mode.
    probe_order_fn(qi) → ordered list of bucket IDs to probe.
    """
    n = len(vectors)
    n_blocks = (n + block_size - 1) // block_size

    # Sort by label, build block index
    order = np.argsort(labels)
    sorted_labels = labels[order]

    bucket_to_blocks = defaultdict(set)
    block_orig = []
    for i in range(n_blocks):
        start = i * block_size
        end = min(start + block_size, n)
        block_orig.append(set(order[start:end]))
        for cl in np.unique(sorted_labels[start:end]):
            bucket_to_blocks[cl].add(i)

    results = {}
    for target in target_recalls:
        probes_list = []
        blocks_list = []

        for qi in range(len(queries)):
            gt_set = set(gt[qi][:top_k])
            probe_seq = probe_order_fn(qi)

            blocks_scanned = set()
            found = set()
            probes_used = 0

            for bucket_id in probe_seq:
                probes_used += 1
                if bucket_id in bucket_to_blocks:
                    for bi in bucket_to_blocks[bucket_id]:
                        if bi not in blocks_scanned:
                            blocks_scanned.add(bi)
                            found.update(block_orig[bi] & gt_set)
                if len(found) / top_k >= target:
                    break

            probes_list.append(probes_used)
            blocks_list.append(len(blocks_scanned))

        avg_probes = np.mean(probes_list)
        p95_probes = np.percentile(probes_list, 95)
        avg_blocks = np.mean(blocks_list)
        skip = (1 - avg_blocks / n_blocks) * 100
        tag = f"{target:.0%}"
        results[target] = (skip, avg_probes, p95_probes, avg_blocks)

        print(
            f"  {method_name} recall≥{tag:<4s}  "
            f"|  probes: avg={avg_probes:5.0f} p95={p95_probes:5.0f}  "
            f"|  skip={skip:5.1f}%  blocks={avg_blocks:5.0f}/{n_blocks}"
        )

    return results


# ============================================================
# Main
# ============================================================

def main():
    dims = 768
    n_vectors = 200000
    n_blocks = (n_vectors + BLOCK_SIZE - 1) // BLOCK_SIZE
    target_recalls = [0.50, 0.70, 0.90, 0.95]

    spreads = [0.01, 0.03, 0.05, 0.10, 0.15, 0.30]

    for spread in spreads:
        rng = np.random.RandomState(RANDOM_SEED)

        print(f"\n{'#'*90}")
        print(f"# {n_vectors} vectors, {dims}D, spread={spread}, blocks={n_blocks}")
        angle = np.degrees(np.arctan(spread * np.sqrt(dims)))
        print(f"# Cluster angular spread ≈ {angle:.1f}°")
        print(f"{'#'*90}")

        vectors, queries = generate_clustered_data(n_vectors, dims, spread=spread)

        # Measure intra-cluster cosine similarity
        rng_temp = np.random.RandomState(RANDOM_SEED)
        centers = rng_temp.randn(200, dims).astype(np.float32)
        centers /= np.linalg.norm(centers, axis=1, keepdims=True)
        sample_sims = []
        for _ in range(1000):
            ci = rng_temp.randint(0, 200)
            v1 = centers[ci] + rng_temp.randn(dims).astype(np.float32) * spread
            v2 = centers[ci] + rng_temp.randn(dims).astype(np.float32) * spread
            v1 /= np.linalg.norm(v1)
            v2 /= np.linalg.norm(v2)
            sample_sims.append(np.dot(v1, v2))
        avg_sim = np.mean(sample_sims)
        print(f"# Avg intra-cluster cosine similarity: {avg_sim:.4f}")

        print("Computing ground truth top-10 ...", end=" ", flush=True)
        t0 = time.time()
        gt = ground_truth_topk(vectors, queries, k=TOP_K)
        print(f"({time.time()-t0:.1f}s)")

        # === BASELINE: Random Codebook VQ ===
        print(f"\n--- BASELINE: Random Codebook VQ (782 centroids) ---")
        t0 = time.time()
        rc_labels, rc_centroids = random_codebook(vectors, n_blocks, rng)
        dt = time.time() - t0
        print(f"  Hash time: {dt:.2f}s, unique buckets: {len(np.unique(rc_labels))}")

        def rc_probe_order(qi):
            q = queries[qi]
            dists = np.linalg.norm(rc_centroids - q, axis=1)
            return np.argsort(dists)

        eval_cluster_adaptive(
            vectors, queries, gt, rc_labels, n_blocks,
            rc_probe_order, BLOCK_SIZE, TOP_K, target_recalls,
            "RandomCodebook-782c"
        )

        # === A: SimHash cluster-based ===
        for n_bits in [10, 12]:
            n_buckets_sh = 2 ** n_bits
            print(f"\n--- A: SimHash-{n_bits}bit cluster-based ({n_buckets_sh} buckets) ---")
            t0 = time.time()
            sh_hashes, sh_hyperplanes = simhash_cluster(vectors, n_bits, rng)
            dt = time.time() - t0
            print(f"  Hash time: {dt:.2f}s, unique: {len(np.unique(sh_hashes))}")

            sh_q_hashes, sh_q_magnitudes = simhash_query(queries, sh_hyperplanes)

            def sh_probe_order(qi, _hashes=sh_q_hashes, _mags=sh_q_magnitudes,
                               _n_bits=n_bits):
                qh = _hashes[qi]
                mags = _mags[qi]
                # Order bits by confidence (weakest first = most likely to flip)
                bit_order = np.argsort(mags)

                # Generate probes in order of likelihood:
                # Start with exact match, then flip weakest bit, etc.
                probes = [qh]
                seen = {qh}

                # Hamming-1: flip each bit, ordered by weakness
                for b in bit_order:
                    h = qh ^ (1 << b)
                    if h not in seen:
                        probes.append(h)
                        seen.add(h)

                # Hamming-2: flip pairs, ordered by combined weakness
                pairs = []
                for i in range(len(bit_order)):
                    for j in range(i + 1, len(bit_order)):
                        pairs.append((mags[bit_order[i]] + mags[bit_order[j]],
                                      bit_order[i], bit_order[j]))
                pairs.sort()
                for _, b1, b2 in pairs:
                    h = qh ^ (1 << b1) ^ (1 << b2)
                    if h not in seen:
                        probes.append(h)
                        seen.add(h)

                # Hamming-3
                triples = []
                for i in range(len(bit_order)):
                    for j in range(i + 1, len(bit_order)):
                        for k in range(j + 1, len(bit_order)):
                            triples.append((
                                mags[bit_order[i]] + mags[bit_order[j]] + mags[bit_order[k]],
                                bit_order[i], bit_order[j], bit_order[k]))
                triples.sort()
                for _, b1, b2, b3 in triples:
                    h = qh ^ (1 << b1) ^ (1 << b2) ^ (1 << b3)
                    if h not in seen:
                        probes.append(h)
                        seen.add(h)

                return probes

            eval_cluster_adaptive(
                vectors, queries, gt, sh_hashes, n_buckets_sh,
                sh_probe_order, BLOCK_SIZE, TOP_K, target_recalls,
                f"SimHash-{n_bits}bit-cluster"
            )

        # === B: PQ-Hash ===
        for m_sub, k_cent in [(3, 10), (4, 6), (2, 28)]:
            n_buckets_pq = k_cent ** m_sub
            print(f"\n--- B: PQ-Hash m={m_sub} K={k_cent} ({n_buckets_pq} buckets) ---")
            t0 = time.time()
            pq_hashes, pq_codebooks, pq_sub_labels = pq_hash(
                vectors, m_sub, k_cent, rng)
            dt = time.time() - t0
            print(f"  Hash time: {dt:.2f}s, unique: {len(np.unique(pq_hashes))}")

            pq_q_hashes, pq_q_sub_labels, pq_q_dist_tables = pq_query(
                queries, pq_codebooks, m_sub, k_cent)

            def pq_probe_order(qi, _sub_labels=pq_q_sub_labels,
                               _dist_tables=pq_q_dist_tables,
                               _m=m_sub, _k=k_cent):
                # Rank all possible PQ codes by sum of subspace distances
                # Start with primary code, then expand
                primary = _sub_labels[qi].copy()
                dt = _dist_tables[qi]  # m × K

                # For each subspace, rank centroids by distance
                sub_rankings = []
                for s in range(_m):
                    order = np.argsort(dt[s])
                    sub_rankings.append(order)

                # Generate probes: start with primary, then change one subspace at a time
                # ordered by the cost of changing
                probes = []
                primary_hash = 0
                mult = 1
                for s in range(_m):
                    primary_hash += int(primary[s]) * mult
                    mult *= _k
                probes.append(primary_hash)
                seen = {primary_hash}

                # Single-subspace changes (cheapest)
                changes = []
                for s in range(_m):
                    for rank_idx in range(1, _k):
                        alt = sub_rankings[s][rank_idx]
                        cost = dt[s, alt] - dt[s, primary[s]]
                        changes.append((cost, s, alt))
                changes.sort()

                for cost, s, alt in changes:
                    code = primary.copy()
                    code[s] = alt
                    h = 0
                    mult = 1
                    for ss in range(_m):
                        h += int(code[ss]) * mult
                        mult *= _k
                    if h not in seen:
                        probes.append(h)
                        seen.add(h)

                # Two-subspace changes
                changes2 = []
                for i in range(len(changes)):
                    for j in range(i + 1, min(len(changes), 30)):
                        c1, s1, a1 = changes[i]
                        c2, s2, a2 = changes[j]
                        if s1 == s2:
                            continue
                        changes2.append((c1 + c2, s1, a1, s2, a2))
                changes2.sort()

                for cost, s1, a1, s2, a2 in changes2[:200]:
                    code = primary.copy()
                    code[s1] = a1
                    code[s2] = a2
                    h = 0
                    mult = 1
                    for ss in range(_m):
                        h += int(code[ss]) * mult
                        mult *= _k
                    if h not in seen:
                        probes.append(h)
                        seen.add(h)

                return probes

            eval_cluster_adaptive(
                vectors, queries, gt, pq_hashes, n_buckets_pq,
                pq_probe_order, BLOCK_SIZE, TOP_K, target_recalls,
                f"PQ-m{m_sub}K{k_cent}"
            )

        # === C: Projection + Grid ===
        for k_dims, n_bins in [(3, 10), (4, 6)]:
            n_buckets_pg = n_bins ** k_dims
            print(f"\n--- C: ProjGrid k={k_dims} B={n_bins} ({n_buckets_pg} buckets) ---")
            t0 = time.time()
            pg_hashes, pg_dirs, pg_mins, pg_ranges, pg_quant = proj_grid_hash(
                vectors, k_dims, n_bins, rng)
            dt = time.time() - t0
            print(f"  Hash time: {dt:.2f}s, unique: {len(np.unique(pg_hashes))}")

            pg_q_hashes, pg_q_quant = proj_grid_query(
                queries, pg_dirs, pg_mins, pg_ranges, n_bins)

            def pg_probe_order(qi, _quant=pg_q_quant, _k=k_dims, _b=n_bins):
                primary = _quant[qi]

                # Generate probes: primary cell, then neighboring cells
                # ordered by Manhattan distance in grid
                primary_hash = 0
                mult = 1
                for d in range(_k):
                    primary_hash += int(primary[d]) * mult
                    mult *= _b

                probes = [primary_hash]
                seen = {primary_hash}

                # Generate all cells within Manhattan distance 1, 2, 3
                from itertools import product as iproduct
                for max_dist in range(1, _b):
                    offsets = range(-max_dist, max_dist + 1)
                    for delta in iproduct(offsets, repeat=_k):
                        if sum(abs(d) for d in delta) != max_dist:
                            continue
                        cell = primary + np.array(delta)
                        if np.any(cell < 0) or np.any(cell >= _b):
                            continue
                        h = 0
                        mult = 1
                        for d in range(_k):
                            h += int(cell[d]) * mult
                            mult *= _b
                        if h not in seen:
                            probes.append(h)
                            seen.add(h)

                return probes

            eval_cluster_adaptive(
                vectors, queries, gt, pg_hashes, n_buckets_pg,
                pg_probe_order, BLOCK_SIZE, TOP_K, target_recalls,
                f"ProjGrid-k{k_dims}B{n_bins}"
            )

    # Summary
    print(f"\n{'='*90}")
    print("SUMMARY")
    print(f"{'='*90}")
    print("""
Method              | Cost per INSERT | Training? | Buckets
--------------------|----------------|-----------|--------
RandomCodebook-782c | O(782 × d)     | No        | 782
SimHash-10bit       | O(10 × d)      | No        | 1024
SimHash-12bit       | O(12 × d)      | No        | 4096
PQ-m3K10            | O(30 × d)      | No        | 1000
PQ-m4K6             | O(24 × d)      | No        | 1296
PQ-m2K28            | O(56 × d)      | No        | 784
ProjGrid-k3B10      | O(3 × d)       | No        | 1000
ProjGrid-k4B6       | O(4 × d)       | No        | 1296

Key question: at spread=0.01-0.05 (realistic), which hash achieves
>90% skip at ≥90% recall with fewest probes?
""")


if __name__ == "__main__":
    main()
