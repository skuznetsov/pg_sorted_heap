#!/usr/bin/env python3
"""
PoC: Measure block-level centroid pruning skip rate on real embeddings.

Downloads real ANN benchmark datasets (or generates realistic synthetic data),
organizes vectors into page-sized blocks, and measures what fraction of blocks
can be skipped using centroid + radius bounding sphere pruning.

Key question: Is block-level spatial pruning viable in 768+ dimensions?
"""

import numpy as np
import os
import sys
import time
import h5py
import urllib.request
from sklearn.cluster import MiniBatchKMeans
from sklearn.random_projection import GaussianRandomProjection

# ============================================================
# Configuration
# ============================================================
BLOCK_SIZES = [64, 128, 256, 512]       # vectors per page/block
TOP_K_VALUES = [1, 10, 50, 100]          # k for top-k queries
N_QUERIES = 200                          # number of test queries
PROJECTION_DIMS = [0, 8, 16, 32, 64]    # 0 = no projection (full dims)
RANDOM_SEED = 42

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)


# ============================================================
# Dataset loading
# ============================================================

def download_ann_benchmark(name, url):
    """Download an ANN benchmark dataset in HDF5 format."""
    path = os.path.join(DATA_DIR, f"{name}.hdf5")
    if os.path.exists(path):
        print(f"  Using cached {path}")
        return path
    print(f"  Downloading {name} from {url} ...")
    urllib.request.urlretrieve(url, path)
    print(f"  Saved to {path}")
    return path


def load_dataset(name):
    """Load dataset, returns (train_vectors, query_vectors, true_neighbors, dims)."""
    datasets = {
        # Real embedding datasets from ann-benchmarks
        "glove-100": "http://ann-benchmarks.com/glove-100-angular.hdf5",
        "nytimes-256": "http://ann-benchmarks.com/nytimes-256-angular.hdf5",
        "deep-image-96": "http://ann-benchmarks.com/deep-image-96-angular.hdf5",
        "gist-960": "http://ann-benchmarks.com/gist-960-euclidean.hdf5",
    }

    if name not in datasets:
        raise ValueError(f"Unknown dataset: {name}. Available: {list(datasets.keys())}")

    path = download_ann_benchmark(name, datasets[name])
    f = h5py.File(path, "r")
    train = np.array(f["train"])
    test = np.array(f["test"])
    neighbors = np.array(f["neighbors"]) if "neighbors" in f else None
    dims = train.shape[1]
    print(f"  Loaded {name}: {train.shape[0]} train, {test.shape[0]} test, {dims} dims")
    return train, test, neighbors, dims


def generate_synthetic(n_vectors, dims, n_clusters=100):
    """Generate synthetic embeddings on a low-dimensional manifold."""
    rng = np.random.RandomState(RANDOM_SEED)
    # Create cluster centers on a manifold
    centers = rng.randn(n_clusters, dims).astype(np.float32)
    centers /= np.linalg.norm(centers, axis=1, keepdims=True)
    # Generate points around centers with varying spread
    labels = rng.randint(0, n_clusters, n_vectors)
    spread = 0.15  # controls cluster tightness
    vectors = centers[labels] + rng.randn(n_vectors, dims).astype(np.float32) * spread
    # Normalize (angular distance)
    vectors /= np.linalg.norm(vectors, axis=1, keepdims=True)
    # Generate queries from same distribution
    q_labels = rng.randint(0, n_clusters, N_QUERIES)
    queries = centers[q_labels] + rng.randn(N_QUERIES, dims).astype(np.float32) * spread
    queries /= np.linalg.norm(queries, axis=1, keepdims=True)
    print(f"  Generated synthetic: {n_vectors} vectors, {dims} dims, {n_clusters} clusters")
    return vectors, queries, None, dims


# ============================================================
# Block organization
# ============================================================

def organize_blocks_random(vectors, block_size):
    """Assign vectors to blocks in insertion order (no clustering)."""
    n = len(vectors)
    n_blocks = (n + block_size - 1) // block_size
    blocks = []
    for i in range(n_blocks):
        start = i * block_size
        end = min(start + block_size, n)
        blocks.append(vectors[start:end])
    return blocks


def organize_blocks_clustered(vectors, block_size):
    """Cluster vectors by similarity, then assign to blocks."""
    n = len(vectors)
    n_blocks = (n + block_size - 1) // block_size
    n_clusters = min(n_blocks, n // 2)

    if n_clusters < 2:
        return organize_blocks_random(vectors, block_size)

    kmeans = MiniBatchKMeans(
        n_clusters=n_clusters,
        batch_size=min(10000, n),
        random_state=RANDOM_SEED,
        n_init=1,
    )
    labels = kmeans.fit_predict(vectors)

    # Sort by cluster label to physically group similar vectors
    order = np.argsort(labels)
    sorted_vectors = vectors[order]

    blocks = []
    for i in range(n_blocks):
        start = i * block_size
        end = min(start + block_size, n)
        blocks.append(sorted_vectors[start:end])
    return blocks


# ============================================================
# Centroid + radius computation
# ============================================================

def compute_block_bounds(blocks):
    """Compute centroid and radius for each block."""
    bounds = []
    for block in blocks:
        centroid = block.mean(axis=0)
        # Radius = max L2 distance from centroid to any vector in block
        dists = np.linalg.norm(block - centroid, axis=1)
        radius = float(dists.max())
        bounds.append((centroid, radius))
    return bounds


def compute_block_bounds_projected(blocks, projector):
    """Compute centroid and radius in projected space."""
    bounds = []
    for block in blocks:
        projected = projector.transform(block)
        centroid = projected.mean(axis=0)
        dists = np.linalg.norm(projected - centroid, axis=1)
        radius = float(dists.max())
        bounds.append((centroid, radius))
    return bounds


# ============================================================
# Skip rate measurement
# ============================================================

def measure_skip_rate(blocks, bounds, queries, top_k, use_projection=False, projector=None):
    """
    For each query, simulate a scan with block pruning:
    1. First pass: compute distance to all block centroids
    2. Sort blocks by centroid distance (nearest first)
    3. Scan blocks in order, maintaining top-k candidates
    4. Skip blocks where centroid_dist - radius > current k-th best distance
    """
    n_queries = len(queries)
    total_blocks = len(blocks)
    total_skipped = 0
    total_scanned = 0
    total_vectors_scanned = 0
    total_vectors = sum(len(b) for b in blocks)

    for qi in range(n_queries):
        q = queries[qi]

        if use_projection and projector is not None:
            q_proj = projector.transform(q.reshape(1, -1))[0]
        else:
            q_proj = q

        # Compute distance from query to each block centroid
        centroid_dists = []
        for bi, (centroid, radius) in enumerate(bounds):
            if use_projection and projector is not None:
                d = float(np.linalg.norm(q_proj - centroid))
            else:
                d = float(np.linalg.norm(q - centroid))
            centroid_dists.append((d, radius, bi))

        # Sort by centroid distance (scan nearest blocks first)
        centroid_dists.sort(key=lambda x: x[0])

        # Scan with pruning
        current_k_dist = float("inf")
        k_dists = []  # heap of top-k distances
        skipped = 0
        scanned = 0
        vectors_scanned = 0

        for c_dist, radius, bi in centroid_dists:
            # Triangle inequality: closest possible vector in block
            # is at distance (c_dist - radius). If that's farther than
            # current k-th best, skip.
            lower_bound = c_dist - radius
            if lower_bound > current_k_dist and len(k_dists) >= top_k:
                skipped += 1
                continue

            # Scan this block
            scanned += 1
            block_vectors = blocks[bi]
            dists = np.linalg.norm(block_vectors - q, axis=1)
            vectors_scanned += len(block_vectors)

            for d in dists:
                if len(k_dists) < top_k:
                    k_dists.append(d)
                    if len(k_dists) == top_k:
                        k_dists.sort()
                        current_k_dist = k_dists[-1]
                elif d < current_k_dist:
                    k_dists[-1] = d
                    k_dists.sort()
                    current_k_dist = k_dists[-1]

        total_skipped += skipped
        total_scanned += scanned
        total_vectors_scanned += vectors_scanned

    avg_skip_rate = total_skipped / (n_queries * total_blocks) * 100
    avg_scan_rate = total_vectors_scanned / (n_queries * total_vectors) * 100
    return avg_skip_rate, avg_scan_rate, total_scanned / n_queries


# ============================================================
# Main experiment
# ============================================================

def run_experiment(vectors, queries, dataset_name, dims):
    """Run full experiment matrix."""
    print(f"\n{'='*70}")
    print(f"DATASET: {dataset_name} ({len(vectors)} vectors, {dims} dims)")
    print(f"{'='*70}")

    # Normalize vectors for angular distance
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms[norms == 0] = 1
    vectors = vectors / norms

    norms = np.linalg.norm(queries, axis=1, keepdims=True)
    norms[norms == 0] = 1
    queries = queries / norms

    # Limit queries
    if len(queries) > N_QUERIES:
        queries = queries[:N_QUERIES]

    results = []

    for block_size in BLOCK_SIZES:
        if block_size > len(vectors) // 4:
            continue

        print(f"\n--- Block size: {block_size} vectors/block ---")

        # Random layout (insertion order)
        blocks_random = organize_blocks_random(vectors, block_size)
        bounds_random = compute_block_bounds(blocks_random)

        # Clustered layout (k-means sorted)
        print(f"  Clustering {len(vectors)} vectors ...", end=" ", flush=True)
        t0 = time.time()
        blocks_clustered = organize_blocks_clustered(vectors, block_size)
        bounds_clustered = compute_block_bounds(blocks_clustered)
        print(f"({time.time()-t0:.1f}s)")

        for top_k in TOP_K_VALUES:
            if top_k > len(vectors) // 10:
                continue

            # --- Random layout ---
            skip_r, scan_r, avg_blocks_r = measure_skip_rate(
                blocks_random, bounds_random, queries, top_k
            )

            # --- Clustered layout ---
            skip_c, scan_c, avg_blocks_c = measure_skip_rate(
                blocks_clustered, bounds_clustered, queries, top_k
            )

            print(
                f"  top-{top_k:>3d}  |  "
                f"random: skip={skip_r:5.1f}% scan={scan_r:5.1f}%  |  "
                f"clustered: skip={skip_c:5.1f}% scan={scan_c:5.1f}%  |  "
                f"blocks: {avg_blocks_r:.0f}→{avg_blocks_c:.0f}/{len(blocks_random)}"
            )

            results.append({
                "dataset": dataset_name,
                "dims": dims,
                "block_size": block_size,
                "top_k": top_k,
                "n_blocks": len(blocks_random),
                "skip_random": skip_r,
                "skip_clustered": skip_c,
                "scan_random": scan_r,
                "scan_clustered": scan_c,
            })

        # --- Test random projection on clustered layout ---
        for proj_dim in PROJECTION_DIMS:
            if proj_dim == 0 or proj_dim >= dims:
                continue

            projector = GaussianRandomProjection(
                n_components=proj_dim, random_state=RANDOM_SEED
            )
            projector.fit(vectors[:1000])
            bounds_proj = compute_block_bounds_projected(blocks_clustered, projector)

            top_k = 10
            skip_p, scan_p, avg_blocks_p = measure_skip_rate(
                blocks_clustered, bounds_proj, queries, top_k,
                use_projection=True, projector=projector
            )
            print(
                f"  top-10 projected→{proj_dim:>2d}d  |  "
                f"clustered+proj: skip={skip_p:5.1f}% scan={scan_p:5.1f}%"
            )

    return results


def print_summary(all_results):
    """Print summary table."""
    print(f"\n{'='*70}")
    print("SUMMARY: Block Skip Rate (%) — Clustered Layout")
    print(f"{'='*70}")
    print(f"{'Dataset':<20s} {'Dims':>5s} {'BlkSz':>5s} {'top-1':>7s} {'top-10':>7s} {'top-50':>7s} {'top-100':>7s}")
    print("-" * 70)

    by_dataset_block = {}
    for r in all_results:
        key = (r["dataset"], r["block_size"])
        if key not in by_dataset_block:
            by_dataset_block[key] = {}
        by_dataset_block[key][r["top_k"]] = r["skip_clustered"]

    for (ds, bs), topks in sorted(by_dataset_block.items()):
        dims = next(r["dims"] for r in all_results if r["dataset"] == ds)
        vals = [f"{topks.get(k, 0):6.1f}%" for k in [1, 10, 50, 100]]
        print(f"{ds:<20s} {dims:>5d} {bs:>5d} {'  '.join(vals)}")


def main():
    np.random.seed(RANDOM_SEED)
    all_results = []

    # Real datasets from ann-benchmarks
    real_datasets = [
        "deep-image-96",     # 96 dims, ~10M train (we'll subsample)
        "glove-100",         # 100 dims, 1.2M train
        "nytimes-256",       # 256 dims, 290K train
        "gist-960",          # 960 dims, 1M train — closest to real embeddings
    ]

    for ds_name in real_datasets:
        try:
            vectors, queries, neighbors, dims = load_dataset(ds_name)
            # Subsample large datasets for PoC speed
            if len(vectors) > 500000:
                idx = np.random.choice(len(vectors), 500000, replace=False)
                vectors = vectors[idx]
                print(f"  Subsampled to {len(vectors)} vectors")
            results = run_experiment(vectors, queries, ds_name, dims)
            all_results.extend(results)
        except Exception as e:
            print(f"  Skipping {ds_name}: {e}")

    # Synthetic high-dim (768, 1536) to simulate OpenAI/Cohere embeddings
    for dims in [768, 1536]:
        vectors, queries, _, d = generate_synthetic(200000, dims, n_clusters=200)
        results = run_experiment(vectors, queries, f"synthetic-{dims}", d)
        all_results.extend(results)

    print_summary(all_results)

    # Verdict
    print(f"\n{'='*70}")
    print("VERDICT")
    print(f"{'='*70}")
    clustered_skips = [r["skip_clustered"] for r in all_results if r["top_k"] == 10]
    random_skips = [r["skip_random"] for r in all_results if r["top_k"] == 10]
    if clustered_skips:
        avg_c = np.mean(clustered_skips)
        avg_r = np.mean(random_skips)
        print(f"Average block skip rate (top-10, clustered): {avg_c:.1f}%")
        print(f"Average block skip rate (top-10, random):    {avg_r:.1f}%")
        print(f"Clustering improvement: {avg_c - avg_r:+.1f}pp")
        if avg_c > 50:
            print(">> VIABLE: >50% skip rate. Architecture is sound.")
        elif avg_c > 30:
            print(">> PROMISING: 30-50% skip rate. Worth pursuing with optimizations.")
        elif avg_c > 10:
            print(">> MARGINAL: 10-30% skip rate. Needs graph overlay or better pruning.")
        else:
            print(">> NOT VIABLE: <10% skip rate. Centroid pruning insufficient alone.")


if __name__ == "__main__":
    main()
