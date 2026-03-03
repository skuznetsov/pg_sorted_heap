#!/usr/bin/env python3
"""
PoC #3: Locality-preserving hash → 1D sort → zone map pruning.

Key idea: If we map 768D vectors to a 1D scalar that preserves locality,
then pg_sorted_heap's zone map machinery gives us 99.9998% skip rate
on scalar range queries — applied to vector search.

Tests:
1. SimHash: random hyperplanes → bit string → int64
2. Random projection → single scalar (simplest LSH)
3. PCA → low-dim → Hilbert curve → int64
4. Multi-probe: multiple hashes, scan union of ranges
"""

import numpy as np
import time
from sklearn.cluster import MiniBatchKMeans
from sklearn.decomposition import PCA

RANDOM_SEED = 42
N_QUERIES = 200
BLOCK_SIZE = 256  # vectors per page


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
# Hash functions
# ============================================================

def simhash(vectors, n_bits, rng):
    """SimHash: sign of random projections → bit string → int."""
    dims = vectors.shape[1]
    hyperplanes = rng.randn(dims, n_bits).astype(np.float32)
    projections = vectors @ hyperplanes  # n x n_bits
    bits = (projections > 0).astype(np.int64)
    # Convert to integer (use first 63 bits to stay in int64)
    use_bits = min(n_bits, 63)
    hashes = np.zeros(len(vectors), dtype=np.int64)
    for b in range(use_bits):
        hashes |= bits[:, b] << b
    return hashes


def random_projection_scalar(vectors, rng):
    """Single random projection → scalar."""
    dims = vectors.shape[1]
    direction = rng.randn(dims).astype(np.float32)
    direction /= np.linalg.norm(direction)
    return vectors @ direction


def pca_hilbert_hash(vectors, n_components, rng):
    """PCA → low dim → pseudo-Hilbert via Z-order (Morton code)."""
    pca = PCA(n_components=n_components, random_state=RANDOM_SEED)
    reduced = pca.fit_transform(vectors)

    # Quantize each dimension to 0..255 (8 bits)
    mins = reduced.min(axis=0)
    maxs = reduced.max(axis=0)
    ranges = maxs - mins
    ranges[ranges == 0] = 1
    quantized = ((reduced - mins) / ranges * 255).astype(np.uint64).clip(0, 255)

    # Z-order: interleave bits of all dimensions
    # Use min(n_components * 8, 63) bits for int64
    max_total_bits = 63
    bits_per_dim = max(1, max_total_bits // n_components)
    hashes = np.zeros(len(vectors), dtype=np.int64)
    bit_pos = 0
    for b in range(bits_per_dim):
        for d in range(n_components):
            if bit_pos >= 63:
                break
            hashes |= ((quantized[:, d] >> b) & 1).astype(np.int64) << bit_pos
            bit_pos += 1
    return hashes, pca


def multi_random_projection(vectors, n_projections, rng):
    """Multiple random projections → multiple scalars."""
    dims = vectors.shape[1]
    directions = rng.randn(n_projections, dims).astype(np.float32)
    norms = np.linalg.norm(directions, axis=1, keepdims=True)
    directions /= norms
    return vectors @ directions.T  # n x n_projections


# ============================================================
# Evaluate hash quality for zone map pruning
# ============================================================

def eval_hash_skip_rate(vectors, queries, gt, hashes_db, hashes_q,
                         block_size, top_k, method_name):
    """
    Sort vectors by hash. Build blocks. For each query, scan blocks
    nearest to query hash (expanding outward). Measure blocks needed
    to find true top-k.
    """
    n = len(vectors)
    n_blocks = (n + block_size - 1) // block_size

    # Sort by hash
    order = np.argsort(hashes_db)
    sorted_hashes = hashes_db[order]

    # Build blocks with min/max hash (zone map)
    block_mins = []
    block_maxs = []
    block_orig_indices = []
    for i in range(n_blocks):
        start = i * block_size
        end = min(start + block_size, n)
        block_h = sorted_hashes[start:end]
        block_mins.append(block_h[0])
        block_maxs.append(block_h[-1])
        block_orig_indices.append(order[start:end])

    block_mins = np.array(block_mins)
    block_maxs = np.array(block_maxs)

    # For each query: find blocks whose hash range overlaps with query hash ± window
    # Expand window until we find all top-k
    blocks_scanned_list = []
    vectors_scanned_list = []

    for qi in range(len(queries)):
        qh = hashes_q[qi]
        gt_set = set(gt[qi][:top_k])

        # Strategy: sort blocks by distance of their hash midpoint to query hash
        block_mids = (block_mins + block_maxs) / 2.0
        block_dist = np.abs(block_mids - qh)
        block_order = np.argsort(block_dist)

        found = set()
        scanned = 0

        for bi in block_order:
            scanned += 1
            for idx in block_orig_indices[bi]:
                if idx in gt_set:
                    found.add(idx)
            if len(found) >= top_k:
                break

        blocks_scanned_list.append(scanned)
        vectors_scanned_list.append(scanned * block_size)

    avg_blocks = np.mean(blocks_scanned_list)
    p50_blocks = np.median(blocks_scanned_list)
    p95_blocks = np.percentile(blocks_scanned_list, 95)
    skip_rate = (1 - avg_blocks / n_blocks) * 100
    scan_rate = avg_blocks / n_blocks * 100

    print(
        f"  {method_name:<35s}  |  "
        f"blocks: avg={avg_blocks:6.1f} p50={p50_blocks:5.0f} p95={p95_blocks:5.0f} / {n_blocks}  |  "
        f"skip={skip_rate:5.1f}%  scan={scan_rate:5.1f}%"
    )
    return skip_rate


def eval_multiprobe_skip_rate(vectors, queries, gt, all_hashes_db, all_hashes_q,
                                block_size, top_k, method_name):
    """
    Multi-probe: for each projection, sort and find candidate blocks.
    Union candidates across all projections.
    """
    n = len(vectors)
    n_blocks = (n + block_size - 1) // block_size
    n_probes = all_hashes_db.shape[1]

    # Build sorted index for each projection
    probe_data = []
    for p in range(n_probes):
        hashes = all_hashes_db[:, p]
        order = np.argsort(hashes)
        sorted_hashes = hashes[order]

        block_orig = []
        block_mids = []
        for i in range(n_blocks):
            start = i * block_size
            end = min(start + block_size, n)
            block_orig.append(order[start:end])
            block_mids.append((sorted_hashes[start] + sorted_hashes[end-1]) / 2.0)
        probe_data.append((block_orig, np.array(block_mids)))

    # For each query: gather candidates from each probe, measure total blocks
    total_blocks_list = []

    for qi in range(len(queries)):
        gt_set = set(gt[qi][:top_k])
        found = set()
        unique_blocks_read = set()

        # Round-robin across probes, scan nearest block from each
        # Until all top-k found
        probe_cursors = []
        for p in range(n_probes):
            qh = all_hashes_q[qi, p]
            block_mids = probe_data[p][1]
            block_order = np.argsort(np.abs(block_mids - qh))
            probe_cursors.append(iter(block_order))

        while len(found) < top_k:
            for p in range(n_probes):
                if len(found) >= top_k:
                    break
                try:
                    bi = next(probe_cursors[p])
                except StopIteration:
                    continue
                block_key = (p, bi)
                if block_key in unique_blocks_read:
                    continue
                unique_blocks_read.add(block_key)
                for idx in probe_data[p][0][bi]:
                    if idx in gt_set:
                        found.add(idx)

        total_blocks_list.append(len(unique_blocks_read))

    avg_blocks = np.mean(total_blocks_list)
    p50_blocks = np.median(total_blocks_list)
    p95_blocks = np.percentile(total_blocks_list, 95)
    # Total possible blocks across all probes
    total_possible = n_blocks * n_probes
    skip_rate = (1 - avg_blocks / total_possible) * 100
    # But more meaningful: fraction of DATA scanned (each probe has same data)
    # worst case: all blocks from different probes = avg_blocks * block_size unique vectors
    # best case: same vectors found in all probes
    data_scanned = avg_blocks * block_size / n * 100

    print(
        f"  {method_name:<35s}  |  "
        f"blocks: avg={avg_blocks:6.1f} p50={p50_blocks:5.0f} p95={p95_blocks:5.0f} / {n_blocks}×{n_probes}  |  "
        f"data≈{data_scanned:5.1f}%"
    )
    return data_scanned


# ============================================================
# Main
# ============================================================

def main():
    rng = np.random.RandomState(RANDOM_SEED)

    for dims in [768, 1536]:
        n_vectors = 200000
        print(f"\n{'#'*70}")
        print(f"# {n_vectors} vectors, {dims} dims, block_size={BLOCK_SIZE}")
        print(f"{'#'*70}")

        vectors, queries = generate_clustered_data(n_vectors, dims)

        print("Computing ground truth top-10 ...", end=" ", flush=True)
        t0 = time.time()
        gt = ground_truth_topk(vectors, queries, k=10)
        print(f"({time.time()-t0:.1f}s)")

        top_k = 10

        print(f"\n--- Single hash → sort → zone map (top-{top_k}) ---")

        # 1. SimHash (various bit widths)
        for n_bits in [8, 16, 32, 63]:
            h_db = simhash(vectors, n_bits, rng)
            h_q = simhash(queries, n_bits, rng)
            eval_hash_skip_rate(vectors, queries, gt, h_db, h_q,
                                BLOCK_SIZE, top_k, f"SimHash-{n_bits}bit")

        # 2. Single random projection
        h_db = random_projection_scalar(vectors, rng)
        h_q = random_projection_scalar(queries, rng)
        eval_hash_skip_rate(vectors, queries, gt, h_db, h_q,
                            BLOCK_SIZE, top_k, "RandomProj-1D")

        # 3. PCA → Z-order
        for n_comp in [4, 8, 16, 32]:
            if n_comp >= dims:
                continue
            h_db, pca = pca_hilbert_hash(vectors, n_comp, rng)
            reduced_q = pca.transform(queries)
            # Same quantization for queries
            mins = pca.transform(vectors).min(axis=0)
            maxs = pca.transform(vectors).max(axis=0)
            ranges = maxs - mins
            ranges[ranges == 0] = 1
            quantized_q = ((reduced_q - mins) / ranges * 255).astype(np.uint64).clip(0, 255)
            max_total_bits = 63
            bits_per_dim = max(1, max_total_bits // n_comp)
            h_q = np.zeros(len(queries), dtype=np.int64)
            bit_pos = 0
            for b in range(bits_per_dim):
                for d in range(n_comp):
                    if bit_pos >= 63:
                        break
                    h_q |= ((quantized_q[:, d] >> b) & 1).astype(np.int64) << bit_pos
                    bit_pos += 1
            eval_hash_skip_rate(vectors, queries, gt, h_db, h_q,
                                BLOCK_SIZE, top_k, f"PCA-{n_comp}D→Z-order")

        # 4. Multi-probe: multiple random projections
        print(f"\n--- Multi-probe: N projections × sorted copies (top-{top_k}) ---")
        for n_probes in [2, 4, 8, 16, 32]:
            h_db_multi = multi_random_projection(vectors, n_probes, rng)
            h_q_multi = multi_random_projection(queries, n_probes, rng)
            eval_multiprobe_skip_rate(vectors, queries, gt,
                                       h_db_multi, h_q_multi,
                                       BLOCK_SIZE, top_k,
                                       f"MultiProbe-{n_probes}proj")

    print(f"\n{'='*70}")
    print("ANALYSIS")
    print(f"{'='*70}")
    print("""
If any single-hash method achieves >90% skip rate:
  → Viable: sort by hash, zone map prunes blocks, pg_sorted_heap machinery works.

If multi-probe achieves <10% data scanned:
  → Viable with N sorted copies (trade space for speed).
  → Each copy = same data sorted by different hash.
  → Table AM stores N orderings, query probes all N.

If nothing works:
  → Locality-preserving hash insufficient in 768+ dims.
  → Graph index (HNSW/DiskANN) remains necessary for ANN.
""")


if __name__ == "__main__":
    main()
