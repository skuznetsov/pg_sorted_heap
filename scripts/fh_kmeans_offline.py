#!/usr/bin/env python3
"""
Offline FH-space k-means segmentation prototype.

Loads vectors from PG, applies FH rotation+equalization matching the C engine,
runs k-means in FH equalized-rotated space, writes a permuted store file
in engine-compatible format for benchmarking via SQL nprobe parameter.

Usage:
    python3 scripts/fh_kmeans_offline.py \
        --dsn "dbname=fh_test" \
        --table gutenberg_local \
        --column embedding \
        --store /tmp/fh_gutenberg_kmeans.store \
        --seed 42 --group-size 4 --n-clusters 26

Then benchmark:
    psql -d fh_test -c "SELECT * FROM flashhadamard_store_scan(
        '/tmp/fh_gutenberg_kmeans.store', q, 10, 12, 42, 4, 4);"
"""

import argparse
import math
import struct
import sys
import time

import numpy as np

try:
    import psycopg2
except ImportError:
    print("pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Splitmix64 — must match C fh_splitmix64 exactly
# ---------------------------------------------------------------------------

MASK64 = (1 << 64) - 1

def splitmix64(state: int) -> tuple[int, int]:
    """Returns (next_state, output). state is uint64."""
    state = (state + 0x9E3779B97F4A7C15) & MASK64
    z = state
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & MASK64
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & MASK64
    z = z ^ (z >> 31)
    return state, z

def generate_perm_signs(dim: int, seed: int) -> tuple[np.ndarray, np.ndarray]:
    """Replicate C fh_generate_perm_signs using splitmix64."""
    state = seed & MASK64
    perm = list(range(dim))
    for i in range(dim - 1, 0, -1):
        state, r = splitmix64(state)
        j = r % (i + 1)
        perm[i], perm[j] = perm[j], perm[i]
    signs = np.empty(dim, dtype=np.float32)
    for i in range(dim):
        state, r = splitmix64(state)
        signs[i] = 1.0 if (r & 1) else -1.0
    return np.array(perm, dtype=np.int32), signs

# ---------------------------------------------------------------------------
# FWHT (Fast Walsh-Hadamard Transform)
# ---------------------------------------------------------------------------

def fwht_inplace(data: np.ndarray):
    """In-place normalized FWHT on 1D array (length must be power of 2)."""
    n = len(data)
    h = 1
    while h < n:
        for i in range(0, n, 2 * h):
            for j in range(i, i + h):
                x = data[j]
                y = data[j + h]
                data[j] = x + y
                data[j + h] = x - y
        h <<= 1
    norm = 1.0 / math.sqrt(n)
    data *= norm

def fwht_batch(data: np.ndarray, block_size: int):
    """In-place FWHT on each block column of a 2D array."""
    # Use scipy if available for speed, otherwise fall back
    n = data.shape[0]
    h = 1
    while h < block_size:
        for i in range(0, block_size, 2 * h):
            for j in range(i, i + h):
                x = data[:, j].copy()
                y = data[:, j + h].copy()
                data[:, j] = x + y
                data[:, j + h] = x - y
        h <<= 1
    data *= 1.0 / math.sqrt(block_size)

def power_of_two_blocks(dim: int) -> list[int]:
    """Decompose dim into power-of-two blocks."""
    blocks = []
    remaining = dim
    while remaining > 0:
        b = 1
        while b * 2 <= remaining:
            b *= 2
        blocks.append(b)
        remaining -= b
    return blocks

def rotate_batch(vecs: np.ndarray, perm: np.ndarray, signs: np.ndarray,
                 blocks: list[int]) -> np.ndarray:
    """Apply structured block-Hadamard rotation to batch of vectors."""
    mixed = vecs[:, perm] * signs[np.newaxis, :]
    out = np.empty_like(mixed, dtype=np.float32)
    offset = 0
    for block in blocks:
        chunk = mixed[:, offset:offset + block].copy()
        fwht_batch(chunk, block)
        out[:, offset:offset + block] = chunk
        offset += block
    return out

# ---------------------------------------------------------------------------
# Group RMS equalization
# ---------------------------------------------------------------------------

def compute_group_scales(rotated: np.ndarray, group_size: int) -> tuple[np.ndarray, np.ndarray]:
    """Compute per-group RMS scales and expanded array."""
    dim = rotated.shape[1]
    n_groups = (dim + group_size - 1) // group_size
    group_scales = np.empty(n_groups, dtype=np.float32)
    expanded = np.empty(dim, dtype=np.float32)
    for g in range(n_groups):
        gs = g * group_size
        ge = min(gs + group_size, dim)
        block = rotated[:, gs:ge]
        scale = float(np.sqrt(np.mean(block * block)))
        scale = max(scale, 1e-4)
        group_scales[g] = scale
        expanded[gs:ge] = scale
    return group_scales, expanded

# ---------------------------------------------------------------------------
# Lloyd-Max centers (must match C fh_lloyd_max_16)
# ---------------------------------------------------------------------------

LLOYD_MAX_16 = np.array([
    -2.1519927, -1.5341205, -1.1503494, -0.8326452,
    -0.5485528, -0.2822760, -0.0248825,  0.2279585,
     0.4809854,  0.7405728,  1.0137205,  1.3106381,
     1.6481531,  2.0637655,  2.6476993,  3.7169876,
], dtype=np.float32)

LLOYD_MAX_BOUNDS_17 = np.array([
    -np.inf,
    -1.8430566, -1.3422349, -0.9914973, -0.6905990,
    -0.4154144, -0.1535793,  0.1015380,  0.3544720,
     0.6107791,  0.8771467,  1.1621793,  1.4793956,
     1.8559593,  2.3557324,  3.1823435,
     np.inf
], dtype=np.float32)

def digitize_4bit(equalized: np.ndarray) -> np.ndarray:
    """Quantize equalized values to 4-bit Lloyd-Max codes [0..15]."""
    return np.digitize(equalized, LLOYD_MAX_BOUNDS_17[1:-1]).astype(np.uint8)

# ---------------------------------------------------------------------------
# Packed nibble layout (must match C transposed format)
# ---------------------------------------------------------------------------

def pack_nibbles(codes: np.ndarray) -> np.ndarray:
    """Pack 4-bit codes into nibble pairs (row-major). codes: [n_rows, dim]."""
    n_rows, dim = codes.shape
    n_bytes = (dim + 1) // 2
    packed = np.zeros((n_rows, n_bytes), dtype=np.uint8)
    for d in range(dim):
        bi = d // 2
        if d % 2 == 0:
            packed[:, bi] |= (codes[:, d] & 0x0F)
        else:
            packed[:, bi] |= ((codes[:, d] & 0x0F) << 4)
    return packed

def transpose_packed(packed: np.ndarray) -> np.ndarray:
    """Transpose packed codes: [n_rows, n_bytes] -> [n_bytes, n_rows] (column-major)."""
    return np.ascontiguousarray(packed.T)

# ---------------------------------------------------------------------------
# SQ8 encoding
# ---------------------------------------------------------------------------

def sq8_encode(unit_vecs: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """SQ8 scalar quantize unit vectors. Returns (codes, mins, scales)."""
    mins = unit_vecs.min(axis=0).astype(np.float32)
    maxs = unit_vecs.max(axis=0).astype(np.float32)
    ranges = maxs - mins
    ranges = np.maximum(ranges, 1e-8)
    scales = (ranges / 255.0).astype(np.float32)
    codes = np.clip(np.round((unit_vecs - mins[np.newaxis, :]) / scales[np.newaxis, :]), 0, 255).astype(np.uint8)
    return codes, mins, scales

# ---------------------------------------------------------------------------
# Store writer (matches C fh_store_write binary format)
# ---------------------------------------------------------------------------

FH_STORE_MAGIC = 0x46484D31
FH_STORE_VERSION = 2
FH_SEGMENT_SIZE = 4096
FH_MAX_CENTERS = 16
FH_MAX_GROUPS = 512
BLCKSZ = 8192

def write_store(path: str, dim: int, n_rows: int, seed: int, group_size: int,
                group_scales: np.ndarray, centers: np.ndarray,
                sq8_mins: np.ndarray, sq8_scales: np.ndarray,
                packed_t: np.ndarray, sq8_codes: np.ndarray,
                norms: np.ndarray, centroids: np.ndarray, n_segments: int):
    """Write store file in engine-compatible binary format."""
    n_bytes = (dim + 1) // 2
    n_groups = (dim + group_size - 1) // group_size

    # Build meta page (8192 bytes)
    meta_page = bytearray(BLCKSZ)

    # FHFilePageHeader at offset 0
    struct.pack_into('<IHHII', meta_page, 0,
                     FH_STORE_MAGIC, 0, 0, 0, 0)  # magic, page_type, reserved, offset, length

    # FHMetaPageDataV2 at offset 16
    meta_off = 16
    # 8 int32 fields
    struct.pack_into('<IiiiiiiI', meta_page, meta_off,
                     FH_STORE_MAGIC, FH_STORE_VERSION, dim, n_rows, n_bytes,
                     group_size, n_groups, seed)
    meta_off += 32

    # 5 int64 fields (placeholders, filled later)
    off_sq8_params_pos = meta_off
    struct.pack_into('<qqqqq', meta_page, meta_off, 0, 0, 0, 0, 0)
    meta_off += 40

    # 2 int32 fields
    n_segments_pos = meta_off
    struct.pack_into('<ii', meta_page, meta_off, n_segments, FH_SEGMENT_SIZE)
    meta_off += 8

    # 1 int64 field (off_end placeholder)
    off_end_pos = meta_off
    struct.pack_into('<q', meta_page, meta_off, 0)
    meta_off += 8

    # float centers[16]
    c = np.zeros(FH_MAX_CENTERS, dtype=np.float32)
    c[:len(centers)] = centers
    meta_page[meta_off:meta_off + FH_MAX_CENTERS * 4] = c.tobytes()
    meta_off += FH_MAX_CENTERS * 4

    # float group_scales[512]
    gs = np.zeros(FH_MAX_GROUPS, dtype=np.float32)
    gs[:min(n_groups, FH_MAX_GROUPS)] = group_scales[:min(n_groups, FH_MAX_GROUPS)]
    meta_page[meta_off:meta_off + FH_MAX_GROUPS * 4] = gs.tobytes()

    # Write file
    with open(path, 'wb') as f:
        f.write(meta_page)

        # Section 1: sq8_params (mins + scales)
        sq8_params_offset = BLCKSZ
        f.write(sq8_mins.astype(np.float32).tobytes())
        f.write(sq8_scales.astype(np.float32).tobytes())

        # Section 2: packed_t (transposed packed codes)
        packed_offset = sq8_params_offset + dim * 4 * 2
        f.write(packed_t.tobytes())

        # Section 3: sq8_codes
        sq8_offset = packed_offset + packed_t.nbytes
        f.write(sq8_codes.astype(np.uint8).tobytes())

        # Section 4: norms
        norm_offset = sq8_offset + sq8_codes.nbytes
        f.write(norms.astype(np.float32).tobytes())

        # Section 5: centroids
        centroid_offset = norm_offset + n_rows * 4
        if centroids is not None and n_segments > 0:
            f.write(centroids.astype(np.float32).tobytes())

        off_end = centroid_offset + (n_segments * dim * 4 if centroids is not None else 0)

        # Rewrite meta page with offsets
        struct.pack_into('<qqqqq', meta_page, off_sq8_params_pos,
                         sq8_params_offset, packed_offset, sq8_offset,
                         norm_offset, centroid_offset)
        struct.pack_into('<ii', meta_page, n_segments_pos, n_segments, FH_SEGMENT_SIZE)
        struct.pack_into('<q', meta_page, off_end_pos, off_end)

        f.seek(0)
        f.write(meta_page)

    print(f"Wrote store: {path} ({off_end} bytes, {n_rows} rows, {dim}D, {n_segments} segments)")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Offline FH-space k-means segmentation")
    parser.add_argument("--dsn", default="dbname=fh_test", help="PG DSN")
    parser.add_argument("--table", default="gutenberg_local", help="Source table")
    parser.add_argument("--column", default="embedding", help="Embedding column")
    parser.add_argument("--store", default="/tmp/fh_gutenberg_kmeans.store", help="Output store path")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--group-size", type=int, default=4)
    parser.add_argument("--n-clusters", type=int, default=26, help="Number of k-means clusters (= segments)")
    parser.add_argument("--kmeans-iters", type=int, default=20, help="Lloyd's iterations")
    args = parser.parse_args()

    t0 = time.time()

    # 1. Load vectors from PG
    print(f"Loading vectors from {args.table}.{args.column}...")
    conn = psycopg2.connect(args.dsn)
    cur = conn.cursor()
    cur.execute(f"SELECT {args.column} FROM {args.table}")
    rows = cur.fetchall()
    conn.close()

    # Parse vectors (pgvector format: '[1.0,2.0,...]')
    vecs = []
    for (v,) in rows:
        if isinstance(v, str):
            v = v.strip('[]')
            vecs.append([float(x) for x in v.split(',')])
        elif isinstance(v, (list, tuple)):
            vecs.append([float(x) for x in v])
        elif isinstance(v, np.ndarray):
            vecs.append(v.tolist())
        else:
            # binary format — try memoryview
            vecs.append(list(np.frombuffer(bytes(v), dtype=np.float32)))
    vecs = np.array(vecs, dtype=np.float32)
    n_rows, dim = vecs.shape
    print(f"Loaded {n_rows} × {dim}D vectors in {time.time()-t0:.1f}s")

    # 2. Normalize
    norms = np.linalg.norm(vecs, axis=1).astype(np.float32)
    norms = np.maximum(norms, 1e-12)
    unit = vecs / norms[:, np.newaxis]

    # 3. Generate perm/signs matching C engine (splitmix64)
    print(f"Generating perm/signs (seed={args.seed}, splitmix64)...")
    perm, signs = generate_perm_signs(dim, args.seed)
    blocks = power_of_two_blocks(dim)
    print(f"  blocks: {blocks}")

    # 4. Rotate
    print("Rotating (FH structured block-Hadamard)...")
    t1 = time.time()
    rotated = rotate_batch(unit, perm, signs, blocks) * math.sqrt(dim)
    print(f"  rotated in {time.time()-t1:.1f}s")

    # 5. Group RMS equalization
    group_scales, expanded = compute_group_scales(rotated, args.group_size)
    equalized = rotated / expanded[np.newaxis, :]
    print(f"  group_scales: {group_scales.shape}, range [{group_scales.min():.3f}, {group_scales.max():.3f}]")

    # 6. K-means in FH equalized space
    n_clusters = args.n_clusters
    print(f"Running k-means (k={n_clusters}, iters={args.kmeans_iters})...")
    t2 = time.time()

    # Initialize centroids: random sample
    rng = np.random.default_rng(args.seed + 7777)
    init_idx = rng.choice(n_rows, size=n_clusters, replace=False)
    centroids_km = equalized[init_idx].copy()

    for it in range(args.kmeans_iters):
        # Assign: find nearest centroid for each row
        # For large n_rows, use batched distance computation
        assignments = np.empty(n_rows, dtype=np.int32)
        batch_size = 4096
        for start in range(0, n_rows, batch_size):
            end = min(start + batch_size, n_rows)
            dists = np.sum((equalized[start:end, np.newaxis, :] - centroids_km[np.newaxis, :, :]) ** 2, axis=2)
            assignments[start:end] = np.argmin(dists, axis=1)

        # Update centroids
        new_centroids = np.zeros_like(centroids_km)
        counts = np.zeros(n_clusters, dtype=np.int64)
        for c in range(n_clusters):
            mask = assignments == c
            count = np.sum(mask)
            if count > 0:
                new_centroids[c] = equalized[mask].mean(axis=0)
                counts[c] = count

        # Check convergence
        shift = np.sqrt(np.sum((new_centroids - centroids_km) ** 2, axis=1)).max()
        centroids_km = new_centroids

        if (it + 1) % 5 == 0 or it == 0 or shift < 1e-4:
            print(f"  iter {it+1}: max_shift={shift:.4f}, cluster_sizes: "
                  f"min={counts.min()} avg={counts.mean():.0f} max={counts.max()}")

        if shift < 1e-4:
            print(f"  converged at iter {it+1}")
            break

    print(f"  k-means done in {time.time()-t2:.1f}s")

    # 7. Build permutation: sort by cluster assignment
    # Within each cluster, sort by distance to centroid (optional, for locality)
    perm_order = np.argsort(assignments, kind='stable')
    print(f"  permutation built, {n_clusters} clusters")

    # 8. Reorder data
    equalized_perm = equalized[perm_order]
    norms_perm = norms[perm_order]
    unit_perm = unit[perm_order]

    # 9. Pack codes from permuted equalized data
    codes = digitize_4bit(equalized_perm)
    packed = pack_nibbles(codes)
    packed_t = transpose_packed(packed)
    print(f"  packed: {packed.shape} → transposed: {packed_t.shape}")

    # 10. SQ8 encode from permuted unit vectors
    sq8_codes, sq8_mins, sq8_scales = sq8_encode(unit_perm)
    print(f"  SQ8: {sq8_codes.shape}")

    # 11. Compute segment centroids from permuted data
    n_seg = (n_rows + FH_SEGMENT_SIZE - 1) // FH_SEGMENT_SIZE
    seg_centroids = np.zeros((n_seg, dim), dtype=np.float32)
    for s in range(n_seg):
        rs = s * FH_SEGMENT_SIZE
        re = min(rs + FH_SEGMENT_SIZE, n_rows)
        # Centroid in equalized space (decoded from codes)
        seg_codes = codes[rs:re]
        decoded = LLOYD_MAX_16[seg_codes]
        seg_centroids[s] = decoded.mean(axis=0)
    print(f"  segment centroids: {seg_centroids.shape}")

    # 12. Write store
    write_store(
        args.store, dim, n_rows, args.seed, args.group_size,
        group_scales, LLOYD_MAX_16,
        sq8_mins, sq8_scales,
        packed_t, sq8_codes, norms_perm,
        seg_centroids, n_seg
    )

    total = time.time() - t0
    print(f"\nTotal time: {total:.1f}s")
    print(f"\nBenchmark with:")
    print(f"  psql -d fh_test -c \"\\i sql/flashhadamard_experimental.sql\"")
    print(f"  # Then run recall sweep (see scripts/fh_recall_sweep.sql)")

if __name__ == "__main__":
    main()
