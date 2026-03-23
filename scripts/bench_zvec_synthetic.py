#!/usr/bin/env python3
"""
Synthetic zvec HNSW benchmark matching the local sorted_hnsw/pgvector harness.

Usage:
  python3 scripts/bench_zvec_synthetic.py --rows 10000 --queries 20 --dim 384 --k 10 --ef 64
"""

from __future__ import annotations

import argparse
import math
import os
import shutil
import statistics
import tempfile
import time

import zvec


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


def dir_size_bytes(path: str) -> int:
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            total += os.path.getsize(os.path.join(root, name))
    return total


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=10000)
    ap.add_argument("--queries", type=int, default=20)
    ap.add_argument("--dim", type=int, default=384)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--ef", type=int, default=64)
    ap.add_argument("--m", type=int, default=16)
    ap.add_argument("--ef-construction", type=int, default=64)
    ap.add_argument("--passes", type=int, default=3)
    ap.add_argument("--memory-limit-mb", type=int, default=1024)
    args = ap.parse_args()

    zvec.init(memory_limit_mb=args.memory_limit_mb)

    base = tempfile.mkdtemp(prefix="zvec_bench_")
    path = os.path.join(base, "bench")

    try:
        schema = zvec.CollectionSchema(
            name="bench",
            vectors=zvec.VectorSchema(
                name="embedding",
                data_type=zvec.DataType.VECTOR_FP32,
                dimension=args.dim,
                index_param=zvec.HnswIndexParam(
                    metric_type=zvec.MetricType.COSINE,
                    m=args.m,
                    ef_construction=args.ef_construction,
                ),
            ),
        )
        coll = zvec.create_and_open(path, schema)

        docs: list[zvec.Doc] = []
        all_vecs: dict[int, list[float]] = {}
        for i in range(1, args.rows + 1):
            vec = vec_for(i, args.dim)
            all_vecs[i] = vec
            docs.append(zvec.Doc(id=str(i), vectors={"embedding": vec}))
            if len(docs) == 256:
                coll.insert(docs)
                docs = []
        if docs:
            coll.insert(docs)
        coll.flush()

        queries = [qvec_for(i, args.rows, args.dim) for i in range(1, args.queries + 1)]
        gt: list[list[str]] = []
        for q in queries:
            gt.append([str(i) for i in sorted(all_vecs.keys(), key=lambda i: cosine_dist(all_vecs[i], q))[: args.k]])

        query_param = zvec.HnswQueryParam(ef=args.ef)

        for q in queries:
            coll.query(zvec.VectorQuery("embedding", vector=q, param=query_param), topk=args.k)

        passes: list[tuple[float, float, float]] = []
        for _ in range(args.passes):
            ms: list[float] = []
            recalls: list[float] = []
            for qi, q in enumerate(queries):
                t0 = time.perf_counter()
                res = coll.query(zvec.VectorQuery("embedding", vector=q, param=query_param), topk=args.k)
                ms.append((time.perf_counter() - t0) * 1000.0)
                ids = [doc.id for doc in res]
                recalls.append(len(set(ids) & set(gt[qi])) / args.k * 100.0)
            passes.append((statistics.median(ms), statistics.fmean(ms), statistics.fmean(recalls)))

        print(f"zvec_passes={passes}")
        print(
            "zvec|p50_ms=%.3f|avg_ms=%.3f|recall_at_%d=%.1f"
            % (
                statistics.median(p[0] for p in passes),
                statistics.median(p[1] for p in passes),
                args.k,
                statistics.median(p[2] for p in passes),
            )
        )
        print(f"zvec_collection|bytes={dir_size_bytes(path)}")
    finally:
        shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":
    main()
