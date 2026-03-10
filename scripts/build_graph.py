#!/usr/bin/env python3
"""
Minimum Viable Routed KNN Graph Builder for svec_graph_scan.

Builds a navigable graph from the main sorted-heap table's sketch embeddings.

Algorithm:
  1. Read all sketches (hsvec) and partition assignments from the main table.
  2. Compute centroid distance matrix → find 2 nearest partitions per partition.
  3. For each node, candidate pool = own partition + 2 nearest partitions.
  4. Select top-M neighbors by cosine distance from the candidate pool.
  5. Apply diversification pruning (HNSW-style heuristic).
  6. Bounded symmetrize: if A→B, add B→A if B has room (< M_max).
  7. Relabel nids by partition (locality-aware: ordered by distance to medoid).
  8. Select medoid per partition as entry point.
  9. Write graph_nodes and graph_entries tables.

Usage:
  python3 scripts/build_graph.py [--dsn DSN] [--table TABLE] [--M 16] [--seed 42]

Metrics guarantee: uses cosine distance on float16 sketches, same as runtime.
"""

import argparse
import sys
import time
import struct
import numpy as np
import psycopg2


def parse_hsvec_binary(data: memoryview) -> np.ndarray:
    """Parse hsvec binary format: 4-byte varlena header + 4-byte dim + float16 data."""
    # PostgreSQL sends hsvec in binary as: vl_len(4) + dim(int32) + half[dim]
    # But via text protocol we get text representation.
    # We'll use text protocol and parse the text.
    raise NotImplementedError("Use text protocol")


def parse_hsvec_text(text: str) -> np.ndarray:
    """Parse hsvec text representation '[v1,v2,...]' to float16 array."""
    text = text.strip('[]')
    if not text:
        return np.array([], dtype=np.float16)
    return np.array([float(x) for x in text.split(',')], dtype=np.float16)


def cosine_distance_f16(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine distance on float16 arrays. Matches runtime cosine_distance_f32_f16
    when query is also f16 (builder uses sketch-to-sketch, not query-to-sketch)."""
    # Compute in float64 to match C implementation
    a64 = a.astype(np.float64)
    b64 = b.astype(np.float64)
    dot = np.dot(a64, b64)
    na = np.linalg.norm(a64)
    nb = np.linalg.norm(b64)
    if na == 0 or nb == 0:
        return float('nan')
    sim = dot / (na * nb)
    sim = max(-1.0, min(1.0, sim))
    return 1.0 - sim


def batch_cosine_distances(query: np.ndarray, candidates: np.ndarray) -> np.ndarray:
    """Compute cosine distances from one query to many candidates. All float16 input,
    float64 computation."""
    q = query.astype(np.float64)
    C = candidates.astype(np.float64)
    dots = C @ q
    norms_c = np.linalg.norm(C, axis=1)
    norm_q = np.linalg.norm(q)
    denom = norms_c * norm_q
    denom[denom == 0] = 1e-10
    sims = dots / denom
    np.clip(sims, -1.0, 1.0, out=sims)
    return 1.0 - sims


def diversified_select(node_vec: np.ndarray, candidate_vecs: np.ndarray,
                       candidate_ids: np.ndarray, dists: np.ndarray,
                       M: int) -> np.ndarray:
    """HNSW-style diversification: select M neighbors that are both close to the node
    and diverse (not too close to each other). Returns selected candidate indices.
    Vectorized inner loop for speed."""
    order = np.argsort(dists)
    selected = []
    # Pre-normalize for fast cosine via dot product
    cands_f64 = candidate_vecs.astype(np.float64)
    norms = np.linalg.norm(cands_f64, axis=1, keepdims=True)
    norms[norms == 0] = 1e-10
    cands_normed = cands_f64 / norms

    selected_normed = np.empty((M, cands_normed.shape[1]), dtype=np.float64)
    n_sel = 0

    for idx in order:
        if n_sel >= M:
            break

        cand_dist = dists[idx]
        cand_n = cands_normed[idx]

        # Check against all selected so far (vectorized)
        if n_sel > 0:
            sims = selected_normed[:n_sel] @ cand_n
            inter_dists = 1.0 - sims
            if np.any(inter_dists < cand_dist):
                continue

        selected.append(idx)
        selected_normed[n_sel] = cand_n
        n_sel += 1

    # If pruning was too aggressive, fill up with best remaining
    if n_sel < M:
        selected_set = set(selected)
        for idx in order:
            if n_sel >= M:
                break
            if idx not in selected_set:
                selected.append(idx)
                n_sel += 1

    return np.array(selected[:M])


def main():
    parser = argparse.ArgumentParser(description='Build routed KNN graph')
    parser.add_argument('--dsn', default='host=localhost dbname=cogniformerus user=postgres',
                        help='PostgreSQL connection string')
    parser.add_argument('--table', default='gutenberg_gptoss_sh',
                        help='Main table name')
    parser.add_argument('--graph-table', default='graph_nodes',
                        help='Output graph table name')
    parser.add_argument('--entry-table', default='graph_entries',
                        help='Output entry points table name')
    parser.add_argument('--sketch-col', default='sketch',
                        help='Sketch column name in graph_nodes (for reading existing sketches)')
    parser.add_argument('--M', type=int, default=16,
                        help='Target number of neighbors per node')
    parser.add_argument('--M-max', type=int, default=0,
                        help='Max degree after symmetrize (default: M*2)')
    parser.add_argument('--n-adjacent', type=int, default=2,
                        help='Number of nearest partitions to include in candidate pool')
    parser.add_argument('--prune', action='store_true', default=True,
                        help='Apply diversification pruning (default: on)')
    parser.add_argument('--no-prune', action='store_true',
                        help='Disable diversification pruning')
    parser.add_argument('--seed', type=int, default=42,
                        help='Random seed for reproducibility')
    parser.add_argument('--sketch-dim', type=int, default=384,
                        help='Truncate embedding to this dim for sketch (bootstrap mode)')
    parser.add_argument('--bootstrap', action='store_true',
                        help='Build graph from scratch (main table only, no pre-existing graph_nodes)')
    parser.add_argument('--csv-input', default='',
                        help='Read from pre-exported CSV instead of database')
    parser.add_argument('--csv-output', default='',
                        help='Write result CSV instead of database')
    parser.add_argument('--dry-run', action='store_true',
                        help='Compute graph but do not write to database')
    args = parser.parse_args()

    if args.no_prune:
        args.prune = False
    if args.M_max == 0:
        args.M_max = args.M * 2

    np.random.seed(args.seed)

    import builtins
    _print = builtins.print
    def print(*a, **kw):
        kw.setdefault('flush', True)
        _print(*a, **kw)

    print(f"=== Graph Builder v1: minimum viable routed KNN ===")
    print(f"Table: {args.table}, M={args.M}, M_max={args.M_max}, "
          f"n_adjacent={args.n_adjacent}, prune={args.prune}, seed={args.seed}")

    # ---- Step 1: Read data ----
    t0 = time.time()
    print("\n[1/8] Reading data...")

    if args.csv_input:
        # Fast path: read from pre-exported CSV
        # Format: nid, partition_id, sketch_text, src_id, src_tid
        import csv
        print(f"  Reading from CSV: {args.csv_input}")
        rows = []
        with open(args.csv_input, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                rows.append(row)
        n = len(rows)
        print(f"  Loaded {n} rows from CSV")

        old_nids = np.array([int(r[0]) for r in rows], dtype=np.int32)
        partition_ids = np.array([int(r[1]) for r in rows], dtype=np.int16)
        sketches_text = [r[2] for r in rows]
        src_ids = [r[3] for r in rows]
        src_tids = [r[4] for r in rows]
    else:
        # Database path
        conn = psycopg2.connect(args.dsn)
        conn.set_session(autocommit=True)
        cur = conn.cursor()

        if args.bootstrap:
            # Bootstrap: build from main table directly (no pre-existing graph_nodes)
            sdim = args.sketch_dim
            print(f"  Bootstrap mode: reading from {args.table}, sketch_dim={sdim}")
            cur.execute(f"""
                SELECT row_number() OVER (ORDER BY partition_id, id) - 1 AS nid,
                       partition_id,
                       ('[' || array_to_string(
                           (string_to_array(trim(both '[]' from embedding::text), ','))[1:{sdim}],
                           ',') || ']') AS sketch_text,
                       id::text AS src_id,
                       ctid::text AS src_tid
                FROM {args.table}
                ORDER BY partition_id, id
            """)
        else:
            # Rebuild: read from existing graph_nodes + main table for partition_id.
            # Join on ctid to get the exact main-table row (src_tid is set during
            # initial build and is stable until VACUUM FULL / rewrite).
            print(f"  Rebuild mode: reading from {args.graph_table} + {args.table}")
            cur.execute(f"""
                SELECT g.nid, m.partition_id, g.sketch::text, g.src_id, g.src_tid::text
                FROM {args.graph_table} g
                JOIN {args.table} m ON m.ctid = g.src_tid
                ORDER BY g.nid
            """)

        rows = cur.fetchall()
        n = len(rows)
        print(f"  Loaded {n} rows from database")

        old_nids = np.array([int(r[0]) for r in rows], dtype=np.int32)
        partition_ids = np.array([int(r[1]) for r in rows], dtype=np.int16)
        sketches_text = [r[2] for r in rows]
        src_ids = [r[3] for r in rows]
        src_tids = [r[4] for r in rows]

    print("  Parsing sketches...")
    sketches = np.array([parse_hsvec_text(s) for s in sketches_text], dtype=np.float16)
    sketch_dim = sketches.shape[1]
    print(f"  Sketch dim: {sketch_dim}, partitions: {len(np.unique(partition_ids))}, n={n}")
    print(f"  Read time: {time.time() - t0:.1f}s")

    # ---- Step 2: Centroid distance matrix ----
    t1 = time.time()
    print("\n[2/8] Computing centroid distance matrix...")

    unique_parts = np.unique(partition_ids)
    n_parts = len(unique_parts)

    # Compute actual centroid per partition (mean of sketches)
    centroids = np.zeros((n_parts, sketch_dim), dtype=np.float64)
    part_to_idx = {int(p): i for i, p in enumerate(unique_parts)}
    part_nodes = {}  # partition_idx -> list of node indices

    for i in range(n):
        pidx = part_to_idx[int(partition_ids[i])]
        centroids[pidx] += sketches[i].astype(np.float64)
        if pidx not in part_nodes:
            part_nodes[pidx] = []
        part_nodes[pidx].append(i)

    for pidx in range(n_parts):
        cnt = len(part_nodes.get(pidx, []))
        if cnt > 0:
            centroids[pidx] /= cnt

    # Compute pairwise centroid distances
    centroid_dists = np.zeros((n_parts, n_parts), dtype=np.float64)
    for i in range(n_parts):
        for j in range(i + 1, n_parts):
            d = 1.0 - np.dot(centroids[i], centroids[j]) / (
                max(np.linalg.norm(centroids[i]), 1e-10) *
                max(np.linalg.norm(centroids[j]), 1e-10))
            centroid_dists[i, j] = d
            centroid_dists[j, i] = d

    # For each partition, find N nearest partitions
    adjacent = {}  # partition_idx -> list of adjacent partition_idxs
    for pidx in range(n_parts):
        dists = centroid_dists[pidx].copy()
        dists[pidx] = float('inf')
        nearest = np.argsort(dists)[:args.n_adjacent]
        adjacent[pidx] = list(nearest)

    print(f"  Centroid matrix time: {time.time() - t1:.1f}s")

    # ---- Step 3: Build candidate pools and select neighbors ----
    t2 = time.time()
    print(f"\n[3/8] Building KNN graph (M={args.M}, prune={args.prune})...")

    neighbors = [None] * n  # neighbors[i] = list of node indices

    for pidx in range(n_parts):
        nodes_in_part = part_nodes.get(pidx, [])
        if not nodes_in_part:
            continue

        # Build candidate pool: own partition + adjacent partitions
        pool_indices = list(nodes_in_part)
        for adj_pidx in adjacent[pidx]:
            pool_indices.extend(part_nodes.get(adj_pidx, []))
        pool_indices = np.array(pool_indices, dtype=np.int32)
        pool_sketches = sketches[pool_indices]

        # Pre-normalize pool for fast batch cosine
        pool_f64 = pool_sketches.astype(np.float64)
        pool_norms = np.linalg.norm(pool_f64, axis=1, keepdims=True)
        pool_norms[pool_norms == 0] = 1e-10
        pool_normed = pool_f64 / pool_norms

        # Process all nodes in this partition as a batch
        part_sketch_f64 = sketches[np.array(nodes_in_part)].astype(np.float64)
        part_norms = np.linalg.norm(part_sketch_f64, axis=1, keepdims=True)
        part_norms[part_norms == 0] = 1e-10
        part_normed = part_sketch_f64 / part_norms

        # Batch distance matrix: (n_part_nodes, n_pool)
        sim_matrix = part_normed @ pool_normed.T
        dist_matrix = 1.0 - sim_matrix

        for local_i, node_idx in enumerate(nodes_in_part):
            dists = dist_matrix[local_i].copy()

            # Exclude self
            self_mask = pool_indices == node_idx
            dists[self_mask] = float('inf')

            if args.prune:
                sel = diversified_select(
                    sketches[node_idx], pool_sketches, pool_indices, dists, args.M)
                neighbors[node_idx] = pool_indices[sel].tolist()
            else:
                topk = np.argsort(dists)[:args.M]
                neighbors[node_idx] = pool_indices[topk].tolist()

        done_count = sum(1 for x in neighbors if x is not None)
        print(f"  Partition {pidx}/{n_parts}: {len(nodes_in_part)} nodes, "
              f"pool={len(pool_indices)}, total {done_count}/{n} ({100*done_count/n:.0f}%)")

    print(f"\n  KNN build time: {time.time() - t2:.1f}s")

    # ---- Step 4: Bounded symmetrize ----
    t3 = time.time()
    print(f"\n[4/8] Bounded symmetrize (M_max={args.M_max})...")

    # Count degree before symmetrize
    pre_avg_deg = np.mean([len(nb) for nb in neighbors if nb is not None])

    for i in range(n):
        if neighbors[i] is None:
            neighbors[i] = []
        for j in neighbors[i]:
            if i not in neighbors[j]:
                if len(neighbors[j]) < args.M_max:
                    neighbors[j].append(i)

    post_degs = [len(nb) for nb in neighbors]
    print(f"  Degree: before={pre_avg_deg:.1f}, after avg={np.mean(post_degs):.1f}, "
          f"min={min(post_degs)}, max={max(post_degs)}")
    print(f"  Symmetrize time: {time.time() - t3:.1f}s")

    # ---- Step 5: Find medoids ----
    t4 = time.time()
    print("\n[5/8] Finding medoids per partition...")

    medoids = {}  # partition_idx -> node_index
    for pidx in range(n_parts):
        nodes = part_nodes.get(pidx, [])
        if not nodes:
            continue
        nodes_arr = np.array(nodes)
        part_sketches = sketches[nodes_arr]
        centroid = centroids[pidx].astype(np.float64)

        # Medoid = node closest to centroid
        dists = np.zeros(len(nodes))
        for k, ni in enumerate(nodes):
            sv = sketches[ni].astype(np.float64)
            dot = np.dot(sv, centroid)
            na = np.linalg.norm(sv)
            nb = np.linalg.norm(centroid)
            if na > 0 and nb > 0:
                dists[k] = 1.0 - dot / (na * nb)
            else:
                dists[k] = float('inf')

        medoid_local = np.argmin(dists)
        medoids[pidx] = nodes[medoid_local]

    print(f"  Found {len(medoids)} medoids")
    print(f"  Medoid time: {time.time() - t4:.1f}s")

    # ---- Step 6: Relabel nids by locality ----
    t5 = time.time()
    print("\n[6/8] Relabeling nids by locality (partition blocks, BFS from medoid)...")

    old_to_new = np.full(n, -1, dtype=np.int32)
    new_nid = 0

    for pidx in range(n_parts):
        nodes = part_nodes.get(pidx, [])
        if not nodes:
            continue

        # Order within partition: BFS from medoid through graph edges
        medoid = medoids.get(pidx)
        if medoid is None:
            medoid = nodes[0]

        nodes_set = set(nodes)
        visited = set()
        queue = [medoid]
        ordered = []
        visited.add(medoid)

        while queue:
            node = queue.pop(0)
            ordered.append(node)
            for nb in (neighbors[node] or []):
                if nb not in visited and nb in nodes_set:
                    visited.add(nb)
                    queue.append(nb)

        # Any nodes not reached by BFS (disconnected within partition)
        for node in nodes:
            if node not in visited:
                ordered.append(node)

        for node in ordered:
            old_to_new[node] = new_nid
            new_nid += 1

    assert new_nid == n, f"Relabeling error: {new_nid} != {n}"

    # Remap neighbor lists (sorted by new nid for batch scan locality)
    new_neighbors = [None] * n
    for old_idx in range(n):
        new_idx = old_to_new[old_idx]
        new_neighbors[new_idx] = sorted(int(old_to_new[nb]) for nb in neighbors[old_idx])

    print(f"  Relabel time: {time.time() - t5:.1f}s")

    # ---- Step 7: Prepare output data ----
    t6 = time.time()
    print("\n[7/8] Preparing output...")

    # Build new_idx -> old_idx mapping for data lookup
    new_to_old = np.argsort(old_to_new)

    # Medoid entries: store the actual node sketch (not centroid)
    entry_data = []
    for pidx in range(n_parts):
        medoid_old = medoids.get(pidx)
        if medoid_old is None:
            continue
        medoid_new = int(old_to_new[medoid_old])
        entry_data.append((medoid_new, sketches_text[medoid_old]))

    # Graph stats
    total_edges = sum(len(nb) for nb in new_neighbors)
    avg_deg = total_edges / n
    cross_part = 0
    for new_idx in range(n):
        old_idx = new_to_old[new_idx]
        node_part = partition_ids[old_idx]
        for nb_new in new_neighbors[new_idx]:
            nb_old = new_to_old[nb_new]
            if partition_ids[nb_old] != node_part:
                cross_part += 1

    print(f"  Total edges: {total_edges}, avg degree: {avg_deg:.1f}")
    print(f"  Cross-partition edges: {cross_part} ({100*cross_part/total_edges:.1f}%)")
    print(f"  Entry points: {len(entry_data)}")

    if args.dry_run:
        print("\n  DRY RUN — not writing.")
        print(f"\n  Total time: {time.time() - t0:.1f}s")
        return

    # ---- Step 8: Write output ----
    t7 = time.time()

    if args.csv_output:
        import csv
        print(f"\n[8/8] Writing CSV to {args.csv_output}...")
        graph_csv = args.csv_output
        entries_csv = args.csv_output.replace('.csv', '_entries.csv')

        with open(graph_csv, 'w', newline='') as f:
            writer = csv.writer(f)
            for new_idx in range(n):
                old_idx = int(new_to_old[new_idx])
                nbrs = '{' + ','.join(str(x) for x in new_neighbors[new_idx]) + '}'
                writer.writerow([new_idx, sketches_text[old_idx], nbrs,
                                src_ids[old_idx], src_tids[old_idx]])

        with open(entries_csv, 'w', newline='') as f:
            writer = csv.writer(f)
            for entry_nid, sketch_text in entry_data:
                writer.writerow([entry_nid, sketch_text])

        print(f"  Graph: {graph_csv} ({n} rows)")
        print(f"  Entries: {entries_csv} ({len(entry_data)} rows)")
        print(f"  Write time: {time.time() - t7:.1f}s")
        print(f"\n=== Done. Total time: {time.time() - t0:.1f}s ===")
        return

    print(f"\n[8/8] Writing to database...")

    conn.set_session(autocommit=False)

    # Drop and recreate graph table
    cur.execute(f"DROP TABLE IF EXISTS {args.graph_table} CASCADE")
    cur.execute(f"""
        CREATE TABLE {args.graph_table} (
            nid          int4 PRIMARY KEY,
            sketch       hsvec NOT NULL,
            neighbors    int4[] NOT NULL,
            src_id       text NOT NULL,
            src_tid      tid NOT NULL
        )
    """)

    # Batch insert
    batch_size = 1000
    for batch_start in range(0, n, batch_size):
        batch_end = min(batch_start + batch_size, n)
        values = []
        for new_idx in range(batch_start, batch_end):
            old_idx = int(new_to_old[new_idx])
            nid = new_idx
            sketch = sketches_text[old_idx]
            nbrs = '{' + ','.join(str(x) for x in new_neighbors[new_idx]) + '}'
            sid = src_ids[old_idx]
            stid = src_tids[old_idx]
            values.append(cur.mogrify("(%s, %s::hsvec, %s::int4[], %s, %s::tid)",
                                       (nid, sketch, nbrs, sid, stid)).decode())
        cur.execute(f"INSERT INTO {args.graph_table} (nid, sketch, neighbors, src_id, src_tid) VALUES " +
                    ','.join(values))

        if batch_start % (batch_size * 10) == 0:
            print(f"  Written {batch_end}/{n} nodes...", end='\r')

    print(f"\n  Nodes written: {n}")

    # Create covering index
    cur.execute(f"""
        CREATE UNIQUE INDEX {args.graph_table}_cover
        ON {args.graph_table} (nid) INCLUDE (sketch, neighbors, src_tid)
    """)
    print("  Covering index created")

    # Write entry points
    cur.execute(f"DROP TABLE IF EXISTS {args.entry_table} CASCADE")
    cur.execute(f"""
        CREATE TABLE {args.entry_table} (
            entry_nid  int4 PRIMARY KEY,
            centroid   hsvec NOT NULL
        )
    """)
    for entry_nid, sketch_text in entry_data:
        cur.execute(f"INSERT INTO {args.entry_table} (entry_nid, centroid) VALUES (%s, %s::hsvec)",
                    (entry_nid, sketch_text))
    print(f"  Entry points written: {len(entry_data)}")

    conn.commit()

    # VACUUM outside transaction
    conn.set_session(autocommit=True)
    cur.execute(f"VACUUM ANALYZE {args.graph_table}")
    cur.execute(f"VACUUM ANALYZE {args.entry_table}")
    print("  VACUUM ANALYZE done")

    print(f"  Write time: {time.time() - t7:.1f}s")
    print(f"\n=== Done. Total time: {time.time() - t0:.1f}s ===")
    print(f"\nParams: M={args.M}, M_max={args.M_max}, n_adjacent={args.n_adjacent}, "
          f"prune={args.prune}, seed={args.seed}")
    print(f"Stats: nodes={n}, avg_deg={avg_deg:.1f}, cross_part={100*cross_part/total_edges:.1f}%, "
          f"entries={len(entry_data)}")

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()
