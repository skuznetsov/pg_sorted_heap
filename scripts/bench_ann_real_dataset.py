#!/usr/bin/env python3
"""
Benchmark pg_sorted_heap sorted_hnsw, pgvector, zvec, and Qdrant on a sampled
real ANN-Benchmarks dataset.

Default dataset is nytimes-256-angular. The script downloads the dataset once
into /tmp, samples a deterministic subset, computes ground truth via exact
PostgreSQL `svec` heap search on that sample, then benchmarks each engine on
the same vectors and queries.
"""

from __future__ import annotations

import argparse
import io
import os
import shutil
import socket
import statistics
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import h5py
import numpy as np
import psycopg2
import requests
import zvec
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance
from qdrant_client.http.models import HnswConfigDiff
from qdrant_client.http.models import PointStruct
from qdrant_client.http.models import SearchParams
from qdrant_client.http.models import VectorParams


DATASETS = {
    "nytimes-256": {
        "url": "https://ann-benchmarks.com/nytimes-256-angular.hdf5",
        "metric": "cosine",
        "dim": 256,
    },
    "glove-100": {
        "url": "https://ann-benchmarks.com/glove-100-angular.hdf5",
        "metric": "cosine",
        "dim": 100,
    },
}


def median_ms(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def avg_ms(values: list[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def ensure_download(url: str, dst: Path) -> Path:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() and dst.stat().st_size > 0:
        return dst
    headers = {"User-Agent": "Mozilla/5.0 (Codex benchmark harness)"}
    with requests.get(url, headers=headers, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dst, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    return dst


def load_sample(dataset_name: str, sample_size: int, query_count: int, seed: int, cache_dir: Path) -> tuple[np.ndarray, np.ndarray]:
    meta = DATASETS[dataset_name]
    path = ensure_download(meta["url"], cache_dir / f"{dataset_name}.hdf5")
    with h5py.File(path, "r") as f:
        train = np.array(f["train"], dtype=np.float32)
        test = np.array(f["test"], dtype=np.float32)

    rng = np.random.default_rng(seed)
    if sample_size > train.shape[0]:
        raise ValueError(f"sample_size={sample_size} exceeds train size {train.shape[0]}")
    if query_count > test.shape[0]:
        raise ValueError(f"query_count={query_count} exceeds test size {test.shape[0]}")

    train_idx = np.sort(rng.choice(train.shape[0], size=sample_size, replace=False))
    query_idx = np.sort(rng.choice(test.shape[0], size=query_count, replace=False))
    return train[train_idx], test[query_idx]


def recall_at_k(found_ids: list[int], gt_ids: list[int], k: int) -> float:
    if k <= 0:
        return 0.0
    return len(set(found_ids) & set(gt_ids)) / float(k) * 100.0


def vector_literal(vec: np.ndarray) -> str:
    return "[" + ",".join(repr(float(np.float32(x))) for x in vec.tolist()) + "]"


def pick_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@dataclass
class PgRunResult:
    exact_p50: float
    exact_avg: float
    exact_ids: list[list[int]]
    sorted_p50: float
    sorted_avg: float
    sorted_recall: float
    pgv_p50: float
    pgv_avg: float
    pgv_recall: float
    sh_index_size: str
    pgv_index_size: str
    sh_total_size: str
    pgv_total_size: str


def run_pg_benchmark(
    root_dir: Path,
    vectors: np.ndarray,
    queries: np.ndarray,
    k: int,
    pgv_storage: str,
    pgv_ef: int,
    sh_ef: int,
) -> PgRunResult:
    dim = vectors.shape[1]
    tmp = Path(tempfile.mkdtemp(prefix="ann_real_pg_", dir="/tmp"))
    pg_bindir = subprocess.check_output(["pg_config", "--bindir"], text=True).strip()
    port = pick_port()

    try:
        subprocess.run(
            ["make", "-C", str(root_dir), "install"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            [f"{pg_bindir}/initdb", "-D", str(tmp / "data"), "-A", "trust", "--no-locale"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        with open(tmp / "data" / "postgresql.conf", "a", encoding="utf-8") as f:
            f.write(
                "shared_buffers = 256MB\n"
                "listen_addresses = ''\n"
                "fsync = on\n"
                "max_wal_size = 1GB\n"
                "log_min_messages = warning\n"
                "shared_preload_libraries = 'pg_sorted_heap'\n"
            )
        subprocess.run(
            [
                f"{pg_bindir}/pg_ctl",
                "-D",
                str(tmp / "data"),
                "-l",
                str(tmp / "postmaster.log"),
                "-o",
                f"-k {tmp} -p {port}",
                "start",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        conn = psycopg2.connect(host=str(tmp), port=port, dbname="postgres")
        conn.autocommit = True
        cur = conn.cursor()
        try:
            cur.execute("CREATE EXTENSION pg_sorted_heap")
            cur.execute("CREATE EXTENSION vector")

            pgv_type = f"{pgv_storage}({dim})"
            pgv_opclass = "vector_cosine_ops" if pgv_storage == "vector" else "halfvec_cosine_ops"

            cur.execute(f"CREATE TABLE bench_sh(id int PRIMARY KEY, v svec({dim}))")
            cur.execute(f"CREATE TABLE bench_pgv(id int PRIMARY KEY, v {pgv_type})")

            sh_buf = io.StringIO()
            pgv_buf = io.StringIO()
            for i, vec in enumerate(vectors, start=1):
                lit = vector_literal(vec)
                sh_buf.write(f"{i}\t{lit}\n")
                pgv_buf.write(f"{i}\t{lit}\n")
            sh_buf.seek(0)
            pgv_buf.seek(0)

            cur.copy_from(sh_buf, "bench_sh", columns=("id", "v"))
            cur.copy_from(pgv_buf, "bench_pgv", columns=("id", "v"))

            cur.execute("CREATE INDEX bench_sh_idx ON bench_sh USING sorted_hnsw(v) WITH (m=16, ef_construction=64)")
            cur.execute(f"CREATE INDEX bench_pgv_idx ON bench_pgv USING hnsw (v {pgv_opclass}) WITH (m=16, ef_construction=64)")
            cur.execute("ANALYZE bench_sh")
            cur.execute("ANALYZE bench_pgv")
            cur.execute("SET jit = off")

            query_rows = [(i + 1, vector_literal(q)) for i, q in enumerate(queries)]
            cur.execute("CREATE TEMP TABLE bench_queries(qid int, q_s svec, q_p text)")
            q_buf = io.StringIO()
            for qid, lit in query_rows:
                q_buf.write(f"{qid}\t{lit}\t{lit}\n")
            q_buf.seek(0)
            cur.copy_from(q_buf, "bench_queries", columns=("qid", "q_s", "q_p"))
            cur.execute(f"ALTER TABLE bench_queries ALTER COLUMN q_s TYPE svec({dim}) USING q_s::svec")
            cur.execute(f"ALTER TABLE bench_queries ALTER COLUMN q_p TYPE {pgv_type} USING q_p::{pgv_type}")

            exact_ms: list[float] = []
            exact_ids: list[list[int]] = []
            sorted_ms: list[float] = []
            sorted_recall_parts: list[float] = []
            pgv_ms: list[float] = []
            pgv_recall_parts: list[float] = []

            cur.execute("SET enable_seqscan = on")
            cur.execute("SET enable_indexscan = off")
            cur.execute("SET enable_bitmapscan = off")
            for qid, lit in query_rows:
                cur.execute(f"SELECT id FROM bench_sh ORDER BY v <=> %s::svec LIMIT {k}", (lit,))
                t0 = time.perf_counter()
                cur.execute(f"SELECT id FROM bench_sh ORDER BY v <=> %s::svec LIMIT {k}", (lit,))
                ids = [row[0] for row in cur.fetchall()]
                exact_ms.append((time.perf_counter() - t0) * 1000.0)
                exact_ids.append(ids)

            cur.execute("SET enable_seqscan = off")
            cur.execute("SET enable_indexscan = on")
            cur.execute("SET enable_bitmapscan = on")
            cur.execute("SET sorted_hnsw.shared_cache = on")
            cur.execute(f"SET sorted_hnsw.ef_search = {sh_ef}")
            for qid, lit in query_rows:
                cur.execute(f"SELECT id FROM bench_sh ORDER BY v <=> %s::svec LIMIT {k}", (lit,))
                t0 = time.perf_counter()
                cur.execute(f"SELECT id FROM bench_sh ORDER BY v <=> %s::svec LIMIT {k}", (lit,))
                ids = [row[0] for row in cur.fetchall()]
                sorted_ms.append((time.perf_counter() - t0) * 1000.0)
                sorted_recall_parts.append(recall_at_k(ids, exact_ids[qid - 1], k))

            cur.execute("SET enable_seqscan = off")
            cur.execute(f"SET hnsw.ef_search = {pgv_ef}")
            for qid, lit in query_rows:
                cur.execute(f"SELECT id FROM bench_pgv ORDER BY v <=> %s::{pgv_type} LIMIT {k}", (lit,))
                t0 = time.perf_counter()
                cur.execute(f"SELECT id FROM bench_pgv ORDER BY v <=> %s::{pgv_type} LIMIT {k}", (lit,))
                ids = [row[0] for row in cur.fetchall()]
                pgv_ms.append((time.perf_counter() - t0) * 1000.0)
                pgv_recall_parts.append(recall_at_k(ids, exact_ids[qid - 1], k))

            cur.execute(
                """
                SELECT
                  pg_size_pretty(pg_relation_size('bench_sh_idx'::regclass)),
                  pg_size_pretty(pg_relation_size('bench_pgv_idx'::regclass)),
                  pg_size_pretty(pg_total_relation_size('bench_sh'::regclass)),
                  pg_size_pretty(pg_total_relation_size('bench_pgv'::regclass))
                """
            )
            sh_index_size, pgv_index_size, sh_total_size, pgv_total_size = cur.fetchone()

            return PgRunResult(
                exact_p50=median_ms(exact_ms),
                exact_avg=avg_ms(exact_ms),
                exact_ids=exact_ids,
                sorted_p50=median_ms(sorted_ms),
                sorted_avg=avg_ms(sorted_ms),
                sorted_recall=avg_ms(sorted_recall_parts),
                pgv_p50=median_ms(pgv_ms),
                pgv_avg=avg_ms(pgv_ms),
                pgv_recall=avg_ms(pgv_recall_parts),
                sh_index_size=sh_index_size,
                pgv_index_size=pgv_index_size,
                sh_total_size=sh_total_size,
                pgv_total_size=pgv_total_size,
            )
        finally:
            cur.close()
            conn.close()
    finally:
        subprocess.run(
            [f"{pg_bindir}/pg_ctl", "-D", str(tmp / "data"), "-m", "immediate", "stop"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        shutil.rmtree(tmp, ignore_errors=True)


@dataclass
class SimpleResult:
    p50: float
    avg: float
    recall: float
    aux: str = ""


def run_zvec_benchmark(vectors: np.ndarray, queries: np.ndarray, gt_ids: list[list[int]], k: int, ef: int, passes: int = 3) -> SimpleResult:
    dim = vectors.shape[1]
    base = tempfile.mkdtemp(prefix="ann_real_zvec_", dir="/tmp")
    path = os.path.join(base, "bench")
    try:
        zvec.init(memory_limit_mb=1024)
        schema = zvec.CollectionSchema(
            name="bench",
            vectors=zvec.VectorSchema(
                name="embedding",
                data_type=zvec.DataType.VECTOR_FP32,
                dimension=dim,
                index_param=zvec.HnswIndexParam(
                    metric_type=zvec.MetricType.COSINE,
                    m=16,
                    ef_construction=64,
                ),
            ),
        )
        coll = zvec.create_and_open(path, schema)
        docs: list[zvec.Doc] = []
        for i, vec in enumerate(vectors, start=1):
            docs.append(zvec.Doc(id=str(i), vectors={"embedding": vec.tolist()}))
            if len(docs) == 256:
                coll.insert(docs)
                docs = []
        if docs:
            coll.insert(docs)
        coll.flush()
        param = zvec.HnswQueryParam(ef=ef)
        for q in queries:
            coll.query(zvec.VectorQuery("embedding", vector=q.tolist(), param=param), topk=k)
        p50s = []
        avgs = []
        recalls = []
        for _ in range(passes):
            ms = []
            recall_parts = []
            for qi, q in enumerate(queries):
                t0 = time.perf_counter()
                res = coll.query(zvec.VectorQuery("embedding", vector=q.tolist(), param=param), topk=k)
                ms.append((time.perf_counter() - t0) * 1000.0)
                ids = [int(doc.id) for doc in res]
                recall_parts.append(recall_at_k(ids, gt_ids[qi], k))
            p50s.append(median_ms(ms))
            avgs.append(avg_ms(ms))
            recalls.append(avg_ms(recall_parts))
        size_bytes = sum((Path(root) / name).stat().st_size for root, _, files in os.walk(path) for name in files)
        return SimpleResult(median_ms(p50s), median_ms(avgs), median_ms(recalls), aux=f"bytes={size_bytes}")
    finally:
        shutil.rmtree(base, ignore_errors=True)


def ensure_qdrant() -> tuple[QdrantClient, bool]:
    client = QdrantClient(url="http://127.0.0.1:6333", timeout=60, check_compatibility=False)
    started = False
    try:
        client.get_collections()
        return client, started
    except Exception:
        subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            ["docker", "run", "-d", "--rm", "--name", "qdrant-bench", "-p", "6333:6333", "qdrant/qdrant:v1.13.2"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        for _ in range(20):
            try:
                client = QdrantClient(url="http://127.0.0.1:6333", timeout=60, check_compatibility=False)
                client.get_collections()
                started = True
                return client, started
            except Exception:
                time.sleep(1.0)
        raise RuntimeError("failed to start/connect to Qdrant on 127.0.0.1:6333")


def run_qdrant_benchmark(vectors: np.ndarray, queries: np.ndarray, gt_ids: list[list[int]], k: int, ef: int, passes: int = 3) -> SimpleResult:
    client, started = ensure_qdrant()
    name = "ann_real_" + uuid.uuid4().hex[:8]
    try:
        client.create_collection(
            collection_name=name,
            vectors_config=VectorParams(size=vectors.shape[1], distance=Distance.COSINE),
            hnsw_config=HnswConfigDiff(m=16, ef_construct=64),
        )
        points = []
        for i, vec in enumerate(vectors, start=1):
            points.append(PointStruct(id=i, vector=vec.tolist()))
            if len(points) == 256:
                client.upsert(collection_name=name, points=points, wait=True)
                points = []
        if points:
            client.upsert(collection_name=name, points=points, wait=True)
        params = SearchParams(hnsw_ef=ef, exact=False)
        for q in queries:
            client.query_points(collection_name=name, query=q.tolist(), limit=k, search_params=params)

        p50s = []
        avgs = []
        recalls = []
        for _ in range(passes):
            ms = []
            recall_parts = []
            for qi, q in enumerate(queries):
                t0 = time.perf_counter()
                res = client.query_points(collection_name=name, query=q.tolist(), limit=k, search_params=params).points
                ms.append((time.perf_counter() - t0) * 1000.0)
                ids = [int(p.id) for p in res]
                recall_parts.append(recall_at_k(ids, gt_ids[qi], k))
            p50s.append(median_ms(ms))
            avgs.append(avg_ms(ms))
            recalls.append(avg_ms(recall_parts))
        info = client.get_collection(name)
        return SimpleResult(median_ms(p50s), median_ms(avgs), median_ms(recalls), aux=f"points={info.points_count}")
    finally:
        try:
            client.delete_collection(name)
        except Exception:
            pass
        if started:
            subprocess.run(["docker", "stop", "qdrant-bench"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def print_result(method: str, p50: float, avg: float, recall: float, k: int, extra: str = "") -> None:
    suffix = f"|{extra}" if extra else ""
    print(f"{method}|p50_ms={p50:.3f}|avg_ms={avg:.3f}|recall_at_{k}={recall:.1f}{suffix}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="nytimes-256", choices=sorted(DATASETS))
    ap.add_argument("--sample-size", type=int, default=10000)
    ap.add_argument("--queries", type=int, default=20)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--cache-dir", default="/tmp/ann_benchmarks")
    ap.add_argument("--pgv-storage", default="vector", choices=["vector", "halfvec"])
    ap.add_argument("--pgv-ef", type=int, default=64)
    ap.add_argument("--sh-ef", type=int, default=96)
    ap.add_argument("--zvec-ef", type=int, default=64)
    ap.add_argument("--qdrant-ef", type=int, default=64)
    ap.add_argument("--skip-zvec", action="store_true")
    ap.add_argument("--skip-qdrant", action="store_true")
    args = ap.parse_args()

    vectors, queries = load_sample(args.dataset, args.sample_size, args.queries, args.seed, Path(args.cache_dir))
    print("============================================================")
    print("real-dataset ANN benchmark")
    print("============================================================")
    print(f"dataset:     {args.dataset}")
    print(f"sample_size: {args.sample_size}")
    print(f"queries:     {args.queries}")
    print(f"dim:         {vectors.shape[1]}")
    print(f"k:           {args.k}")
    print(f"pgv_storage: {args.pgv_storage}")
    print(f"pgv_ef:      {args.pgv_ef}")
    print(f"sh_ef:       {args.sh_ef}")
    print(f"zvec_ef:     {args.zvec_ef}")
    print(f"qdrant_ef:   {args.qdrant_ef}")
    print()

    root_dir = Path(__file__).resolve().parent.parent
    pg = run_pg_benchmark(root_dir, vectors, queries, args.k, args.pgv_storage, args.pgv_ef, args.sh_ef)
    print_result("exact_heap", pg.exact_p50, pg.exact_avg, 100.0, args.k)
    print_result("sorted_hnsw", pg.sorted_p50, pg.sorted_avg, pg.sorted_recall, args.k, extra=f"index={pg.sh_index_size}")
    print_result(
        f"pgvector_hnsw_{args.pgv_storage}",
        pg.pgv_p50,
        pg.pgv_avg,
        pg.pgv_recall,
        args.k,
        extra=f"index={pg.pgv_index_size}",
    )
    print(
        f"pg_sizes|sorted_hnsw_index={pg.sh_index_size}|pgvector_index={pg.pgv_index_size}"
        f"|bench_sh_total={pg.sh_total_size}|bench_pgv_total={pg.pgv_total_size}"
    )

    if not args.skip_zvec:
        zres = run_zvec_benchmark(vectors, queries, pg.exact_ids, args.k, args.zvec_ef)
        print_result("zvec", zres.p50, zres.avg, zres.recall, args.k, extra=zres.aux)

    if not args.skip_qdrant:
        qres = run_qdrant_benchmark(vectors, queries, pg.exact_ids, args.k, args.qdrant_ef)
        print_result("qdrant", qres.p50, qres.avg, qres.recall, args.k, extra=qres.aux)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
