#!/usr/bin/env python3
"""
Build hierarchical HNSW graph on sketches (faiss), with optional full-vector L0.

Extracts adjacency lists per level from a faiss IndexHNSWFlat and writes to
PostgreSQL sidecar tables for the hierarchical PG prototype.

Tables created:
  <prefix>_meta   (entry_nid int4, max_level int2)
  <prefix>_l0     (nid int4 PK, sketch hsvec(D)|svec(D), neighbors int4[],
                   src_id text, src_tid tid)
  <prefix>_l1     (nid int4 PK, sketch hsvec(D), neighbors int4[])
  <prefix>_l2 ... same for each upper level present

With --full-vectors, L0 stores full svec(D) from the main sorted_heap table
instead of reduced hsvec sketches. This enables exact distance computation
during L0 traversal for 100% recall at the cost of larger L0 cache (~1.2 GB
for 103K x 2880-dim vs ~93 MB with sketches).

Usage:
  python3 scripts/build_hnsw_graph.py \
    --dsn 'host=localhost port=15432 dbname=cogniformerus user=postgres password=postgres' \
    --source-table gutenberg_gptoss_sh_graph \
    --prefix gutenberg_gptoss_hnsw \
    --M 16 --ef-construction 200 \
    --sketch-dim 384

  # Hybrid L0: full vectors for L0, sketches for upper levels
  python3 scripts/build_hnsw_graph.py \
    --dsn '...' --source-table gutenberg_gptoss_sh_graph \
    --prefix gutenberg_gptoss_hnsw \
    --M 16 --ef-construction 200 \
    --full-vectors --main-table gutenberg_gptoss_sh
"""
from __future__ import annotations

import argparse
import time
from typing import Any

import faiss
import numpy as np
import psycopg2
import psycopg2.extras

DEFAULT_SKETCH_DIM = 384


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

def load_source(cur, source_table: str) -> tuple[np.ndarray, list[int], list[str], list[bytes]]:
    """Load sketches + nids + src_ids + src_tids from existing graph table."""
    print(f"[1/4] Loading from {source_table}...", flush=True)
    cur.execute(
        f"SELECT nid, sketch::text, src_id, src_tid::text FROM {source_table} ORDER BY nid"
    )
    rows = cur.fetchall()
    nids     = [r[0] for r in rows]
    sketches = np.array([[float(x) for x in r[1][1:-1].split(",")]
                         for r in rows], dtype=np.float32)
    src_ids  = [r[2] for r in rows]
    src_tids = [r[3] for r in rows]  # tid as text, e.g. "(123,4)"
    print(f"      {len(nids):,} rows, shape {sketches.shape}")
    return sketches, nids, src_ids, src_tids


def load_full_vectors(cur, main_table: str, nids: list[int],
                      src_ids_by_nid: dict[int, str]) -> dict[int, str]:
    """Load full svec embeddings from the main sorted_heap table.

    Returns nid → "[f,f,...,f]" literal for svec INSERT.
    """
    print(f"      Loading full vectors from {main_table}...", flush=True)
    # Build id→nid mapping
    id_to_nid = {sid: nid for nid, sid in src_ids_by_nid.items()}

    cur.execute(f"SELECT id, embedding::text FROM {main_table}")
    full_vecs: dict[int, str] = {}
    n_loaded = 0
    for row_id, emb_text in cur:
        nid = id_to_nid.get(row_id)
        if nid is None:
            continue
        full_vecs[nid] = emb_text  # already "[f,f,...,f]" from svec::text
        n_loaded += 1
        if n_loaded % 20000 == 0:
            print(f"        {n_loaded:,}/{len(nids):,}", end="\r", flush=True)

    print(f"      Loaded {n_loaded:,} full vectors ({len(full_vecs):,} matched)")
    return full_vecs


# ---------------------------------------------------------------------------
# Build faiss HNSW
# ---------------------------------------------------------------------------

def build_faiss_hnsw(sketches: np.ndarray, M: int, ef_construction: int) -> faiss.IndexHNSWFlat:
    """Build HNSW with cosine metric (normalize + inner product)."""
    print(f"[2/4] Building faiss HNSW (M={M}, ef_construction={ef_construction})...", flush=True)
    t0 = time.time()

    # L2-normalize → inner product == cosine similarity
    norms = np.linalg.norm(sketches, axis=1, keepdims=True)
    vecs  = sketches / np.maximum(norms, 1e-10)

    dim = sketches.shape[1]
    index = faiss.IndexHNSWFlat(dim, M, faiss.METRIC_INNER_PRODUCT)
    index.hnsw.efConstruction = ef_construction
    index.add(vecs)

    hnsw = index.hnsw
    print(f"      built in {time.time()-t0:.1f}s")
    print(f"      max_level={hnsw.max_level}  entry={hnsw.entry_point}")

    # levels[i] = 1-based count of levels for node i (max_level_i = levels[i] - 1)
    levels_arr = faiss.vector_to_array(hnsw.levels)
    for l in range(hnsw.max_level + 1):
        count = int((levels_arr >= l + 1).sum())  # nodes with max_level >= l
        print(f"      level {l}: {count:,} nodes")

    return index


# ---------------------------------------------------------------------------
# Extract adjacency lists
# ---------------------------------------------------------------------------

def extract_adjacency(index: faiss.IndexHNSWFlat, nids: list[int]) \
        -> tuple[int, int, dict[int, dict[int, list[int]]]]:
    """
    Returns (entry_nid, max_level, adj):
      adj[faiss_idx][level] = [neighbor_faiss_idx, ...]
    Only nodes present at level l are included at that level.
    """
    print("[3/4] Extracting adjacency lists...", flush=True)
    hnsw     = index.hnsw
    nb       = faiss.vector_to_array(hnsw.neighbors).copy()   # int32
    levels_a = faiss.vector_to_array(hnsw.levels).copy()      # int32
    offsets  = faiss.vector_to_array(hnsw.offsets).copy()     # size_t/uint64

    ntotal    = index.ntotal
    entry_nid = nids[hnsw.entry_point]
    max_level = int(hnsw.max_level)

    adj: dict[int, dict[int, list[int]]] = {}

    # Use faiss's own cum_nb_neighbors(l) for correct offsets per level.
    # levels[i] = 1-based count (node exists at levels 0 .. levels[i]-1)

    for fi in range(ntotal):
        n_lev = int(levels_a[fi])   # 1-based count
        off   = offsets[fi]

        nid = nids[fi]
        adj[nid] = {}
        for l in range(n_lev):      # levels 0 .. n_lev-1
            start = off + hnsw.cum_nb_neighbors(l)
            end_  = off + hnsw.cum_nb_neighbors(l + 1)
            raw   = nb[start:end_]
            valid = raw[raw != -1]
            adj[nid][l] = [nids[int(x)] for x in valid]

    # Count edges per level
    for l in range(max_level + 1):
        total_edges = sum(len(v.get(l, [])) for v in adj.values())
        nodes_at_l  = sum(1 for v in adj.values() if l in v)
        print(f"      level {l}: {nodes_at_l:,} nodes, {total_edges:,} directed edges")

    return entry_nid, max_level, adj


# ---------------------------------------------------------------------------
# Write to PostgreSQL
# ---------------------------------------------------------------------------

def create_tables(cur, prefix: str, max_level: int,
                  sketch_dim: int = DEFAULT_SKETCH_DIM,
                  l0_full_dim: int = 0) -> None:
    """Create sidecar tables.

    l0_full_dim > 0: L0 uses svec(l0_full_dim) instead of hsvec(sketch_dim).
    """
    cur.execute(f"DROP TABLE IF EXISTS {prefix}_meta CASCADE")
    cur.execute(f"""
        CREATE TABLE {prefix}_meta (
            entry_nid  int4,
            max_level  int2
        )
    """)

    # Level 0: includes src_id + src_tid for final rerank
    l0_type = f"svec({l0_full_dim})" if l0_full_dim > 0 else f"hsvec({sketch_dim})"
    cur.execute(f"DROP TABLE IF EXISTS {prefix}_l0 CASCADE")
    cur.execute(f"""
        CREATE TABLE {prefix}_l0 (
            nid        int4    PRIMARY KEY,
            sketch     {l0_type},
            neighbors  int4[],
            src_id     text,
            src_tid    tid
        )
    """)
    # Covering index only for hsvec (small sketches); svec(2880) exceeds btree
    # page size limit (8KB).  Full vectors are TOAST'd so we just use the PK.
    if l0_full_dim == 0:
        cur.execute(f"""
            CREATE INDEX {prefix.split('.')[-1]}_l0_cover
            ON {prefix}_l0 (nid) INCLUDE (sketch, neighbors, src_tid)
        """)

    for l in range(1, max_level + 1):
        cur.execute(f"DROP TABLE IF EXISTS {prefix}_l{l} CASCADE")
        cur.execute(f"""
            CREATE TABLE {prefix}_l{l} (
                nid       int4    PRIMARY KEY,
                sketch    hsvec({sketch_dim}),
                neighbors int4[]
            )
        """)
        cur.execute(f"""
            CREATE INDEX {prefix.split('.')[-1]}_l{l}_cover
            ON {prefix}_l{l} (nid) INCLUDE (sketch, neighbors)
        """)


def write_tables(
    cur,
    conn,
    prefix: str,
    entry_nid: int,
    max_level: int,
    adj: dict[int, dict[int, list[int]]],
    sketches_by_nid: dict[int, str],   # nid → "[f,f,...]" literal
    src_ids_by_nid: dict[int, str],
    src_tids_by_nid: dict[int, str],
    l0_vecs_by_nid: dict[int, str] | None = None,  # nid → full svec literal
    batch: int = 2000,
) -> None:
    print(f"[4/4] Writing to PostgreSQL...", flush=True)

    # Meta
    cur.execute(f"INSERT INTO {prefix}_meta VALUES (%s, %s)", (entry_nid, max_level))

    def pg_int4_array(lst: list[int]) -> str:
        return "{" + ",".join(str(x) for x in lst) + "}"

    # Level 0
    use_full = l0_vecs_by_nid is not None
    l0_cast = "svec" if use_full else "hsvec"
    t0 = time.time()
    rows_l0 = []
    for nid, level_nbrs in adj.items():
        if 0 not in level_nbrs:
            continue
        vec_literal = l0_vecs_by_nid[nid] if use_full else sketches_by_nid[nid]
        rows_l0.append((
            nid,
            vec_literal,
            pg_int4_array(level_nbrs[0]),
            src_ids_by_nid[nid],
            src_tids_by_nid[nid],
        ))

    for i in range(0, len(rows_l0), batch):
        psycopg2.extras.execute_values(
            cur,
            f"INSERT INTO {prefix}_l0 (nid, sketch, neighbors, src_id, src_tid) "
            f"VALUES %s",
            rows_l0[i:i+batch],
            template=f"(%s, %s::{l0_cast}, %s::int4[], %s, %s::tid)",
        )
        conn.commit()
    label = f"L0 ({'svec full' if use_full else 'hsvec sketch'})"
    print(f"      {label}: {len(rows_l0):,} rows  ({time.time()-t0:.1f}s)")

    # Upper levels
    for l in range(1, max_level + 1):
        t0 = time.time()
        rows_l = []
        for nid, level_nbrs in adj.items():
            if l not in level_nbrs:
                continue
            rows_l.append((
                nid,
                sketches_by_nid[nid],
                pg_int4_array(level_nbrs[l]),
            ))
        for i in range(0, len(rows_l), batch):
            psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO {prefix}_l{l} (nid, sketch, neighbors) VALUES %s",
                rows_l[i:i+batch],
                template=f"(%s, %s::hsvec, %s::int4[])",
            )
            conn.commit()
        print(f"      L{l}: {len(rows_l):,} rows  ({time.time()-t0:.1f}s)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dsn",
        default="host=localhost port=15432 dbname=cogniformerus user=postgres password=postgres",
    )
    parser.add_argument("--source-table",    default="gutenberg_gptoss_sh_graph")
    parser.add_argument("--prefix",          default="public.gutenberg_gptoss_hnsw")
    parser.add_argument("--M",               type=int, default=16)
    parser.add_argument("--ef-construction", type=int, default=200)
    parser.add_argument("--sketch-dim",      type=int, default=0,
                        help="Expected sketch dimension (0 = infer from data)")
    parser.add_argument("--full-vectors",    action="store_true",
                        help="Store full svec in L0 (hybrid mode: exact L0, sketch upper)")
    parser.add_argument("--main-table",      default="",
                        help="Main sorted_heap table with svec embeddings (required with --full-vectors)")
    args = parser.parse_args()

    if args.full_vectors and not args.main_table:
        parser.error("--full-vectors requires --main-table")

    conn = psycopg2.connect(args.dsn)
    conn.autocommit = False
    cur  = conn.cursor()

    sketches, nids, src_ids, src_tids = load_source(cur, args.source_table)

    sketch_dim = args.sketch_dim if args.sketch_dim > 0 else sketches.shape[1]
    if sketches.shape[1] != sketch_dim:
        parser.error(f"--sketch-dim={args.sketch_dim} but data has {sketches.shape[1]} dims")

    index = build_faiss_hnsw(sketches, args.M, args.ef_construction)
    entry_nid, max_level, adj = extract_adjacency(index, nids)

    # Build lookup dicts
    sketches_by_nid = {
        nids[i]: "[" + ",".join(f"{v:.8g}" for v in sketches[i]) + "]"
        for i in range(len(nids))
    }
    src_ids_by_nid  = {nids[i]: src_ids[i]  for i in range(len(nids))}
    src_tids_by_nid = {nids[i]: src_tids[i] for i in range(len(nids))}

    # Load full vectors for hybrid L0
    l0_vecs_by_nid = None
    l0_full_dim = 0
    if args.full_vectors:
        l0_vecs_by_nid = load_full_vectors(cur, args.main_table, nids, src_ids_by_nid)
        # Infer dimension from first vector
        sample = next(iter(l0_vecs_by_nid.values()))
        l0_full_dim = sample.count(",") + 1
        print(f"      L0 full vector dimension: {l0_full_dim}")

    create_tables(cur, args.prefix, max_level, sketch_dim, l0_full_dim)
    conn.commit()

    write_tables(
        cur, conn, args.prefix,
        entry_nid, max_level, adj,
        sketches_by_nid, src_ids_by_nid, src_tids_by_nid,
        l0_vecs_by_nid=l0_vecs_by_nid,
    )

    mode = "hybrid (svec L0 + hsvec upper)" if args.full_vectors else "sketch-only (hsvec)"
    print(f"\nDone. Mode: {mode}")
    print(f"Tables: {args.prefix}_meta, _l0 .. _l{max_level}")
    print(f"Entry point: nid={entry_nid}, max_level={max_level}")
    conn.close()


if __name__ == "__main__":
    main()
