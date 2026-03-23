#!/usr/bin/env python3
"""
Synthetic Qdrant HNSW benchmark matching the local sorted_hnsw/pgvector harness.

Usage:
  python3 scripts/bench_qdrant_synthetic.py --rows 10000 --queries 20 --dim 384 --k 10 --ef 64

Requires a running Qdrant server, for example:
  docker run -d --rm --name qdrant-bench -p 6333:6333 qdrant/qdrant:v1.13.2
"""

from __future__ import annotations

import argparse
import math
import statistics
import time
import uuid

from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance
from qdrant_client.http.models import PointStruct
from qdrant_client.http.models import SearchParams
from qdrant_client.http.models import VectorParams


def vec_for(g: int, dim: int) -> list[float]:
    return [
        (((g * (((d * 17) % 97) + 1)) + (d * 13)) % 1000) / 1000.0
        for d in range(1, dim + 1)
    ]


def qvec_for(qid: int, rows: int, dim: int) -> list[float]:
    return [
        (((((qid + rows) * (((d * 19) % 89) + 3)) + (d * 29)) % 1000) / 1000.0)
        for d in range(1, dim + 1)
    ]


def cosine_dist(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0.0 or nb == 0.0:
        return 1.0
    return 1.0 - dot / (na * nb)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:6333")
    ap.add_argument("--rows", type=int, default=10000)
    ap.add_argument("--queries", type=int, default=20)
    ap.add_argument("--dim", type=int, default=384)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--ef", type=int, default=64)
    ap.add_argument("--passes", type=int, default=3)
    args = ap.parse_args()

    client = QdrantClient(url=args.url, timeout=60, check_compatibility=False)
    name = "bench_" + uuid.uuid4().hex[:8]

    if client.collection_exists(name):
        client.delete_collection(name)

    client.create_collection(
        collection_name=name,
        vectors_config=VectorParams(size=args.dim, distance=Distance.COSINE),
    )

    try:
        points: list[PointStruct] = []
        all_vecs: dict[int, list[float]] = {}

        for i in range(1, args.rows + 1):
            vec = vec_for(i, args.dim)
            all_vecs[i] = vec
            points.append(PointStruct(id=i, vector=vec))
            if len(points) == 256:
                client.upsert(collection_name=name, points=points, wait=True)
                points = []
        if points:
            client.upsert(collection_name=name, points=points, wait=True)

        queries = [qvec_for(i, args.rows, args.dim) for i in range(1, args.queries + 1)]
        gt: list[list[int]] = []
        for q in queries:
            gt.append(sorted(all_vecs.keys(), key=lambda i: cosine_dist(all_vecs[i], q))[: args.k])

        params = SearchParams(hnsw_ef=args.ef, exact=False)

        for q in queries:
            client.query_points(collection_name=name, query=q, limit=args.k, search_params=params)

        passes: list[tuple[float, float, float]] = []
        for _ in range(args.passes):
            ms: list[float] = []
            recalls: list[float] = []
            for qi, q in enumerate(queries):
                t0 = time.perf_counter()
                res = client.query_points(
                    collection_name=name, query=q, limit=args.k, search_params=params
                ).points
                ms.append((time.perf_counter() - t0) * 1000.0)
                ids = [p.id for p in res]
                recalls.append(len(set(ids) & set(gt[qi])) / args.k * 100.0)
            passes.append((statistics.median(ms), statistics.fmean(ms), statistics.fmean(recalls)))

        print(f"qdrant_passes={passes}")
        print(
            "qdrant|p50_ms=%.3f|avg_ms=%.3f|recall_at_%d=%.1f"
            % (
                statistics.median(p[0] for p in passes),
                statistics.median(p[1] for p in passes),
                args.k,
                statistics.median(p[2] for p in passes),
            )
        )
        info = client.get_collection(name)
        print(f"qdrant_collection|points={info.points_count}")
    finally:
        client.delete_collection(name)


if __name__ == "__main__":
    main()
