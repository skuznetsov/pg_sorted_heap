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
import importlib.util
import io
import os
import shutil
import socket
import statistics
import subprocess
import sys
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

REPO_ROOT = Path(__file__).resolve().parent.parent
TURBOQUANT_BENCH_PATH = REPO_ROOT / "scripts" / "bench_turboquant_retrieval.py"


def median_ms(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def avg_ms(values: list[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def p95_ms(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1))))
    return ordered[idx]


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


def load_vector_files(vectors_path: Path, queries_path: Path | None) -> tuple[np.ndarray, np.ndarray]:
    if vectors_path.suffix == ".npz":
        blob = np.load(vectors_path)
        if "base" not in blob:
            raise ValueError(f"{vectors_path} must contain a 'base' array")
        base = np.asarray(blob["base"], dtype=np.float32)
        if queries_path is None:
            if "queries" not in blob:
                raise ValueError(f"{vectors_path} must contain a 'queries' array when --query-vectors is not set")
            queries = np.asarray(blob["queries"], dtype=np.float32)
        else:
            queries = np.asarray(np.load(queries_path), dtype=np.float32)
    else:
        base = np.asarray(np.load(vectors_path), dtype=np.float32)
        if queries_path is None:
            raise ValueError("--query-vectors is required for .npy base input")
        queries = np.asarray(np.load(queries_path), dtype=np.float32)

    if base.ndim != 2 or queries.ndim != 2:
        raise ValueError("base and query vectors must be 2-D arrays")
    if base.shape[1] != queries.shape[1]:
        raise ValueError(f"dimension mismatch: base={base.shape[1]} query={queries.shape[1]}")
    return base, queries


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


def load_turboquant_bench_module():
    spec = importlib.util.spec_from_file_location("bench_turboquant_retrieval", TURBOQUANT_BENCH_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {TURBOQUANT_BENCH_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def choose_pq_m(dim: int, requested: int) -> int:
    if requested > 0:
        if dim % requested != 0:
            raise ValueError(f"dim={dim} must be divisible by ivfpq_m={requested}")
        return requested
    for candidate in (16, 12, 10, 8, 4, 2, 1):
        if candidate <= dim and dim % candidate == 0:
            return candidate
    return 1


@dataclass
class IvfPqResult:
    p50: float
    p95: float
    avg: float
    recall: float
    train_ms: float
    build_ms: float
    compact_ms: float
    table_size: str
    codebook_size: str
    aux: str


@dataclass
class FlashHadamardResult:
    p50: float
    p95: float
    avg: float
    hit1: float
    recall: float
    encode_ms: float
    bytes_per_vec: float
    metadata_kb: float
    aux: str


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
    ivfpq: IvfPqResult | None = None
    flashhadamard: FlashHadamardResult | None = None


def run_pg_benchmark(
    root_dir: Path,
    vectors: np.ndarray,
    queries: np.ndarray,
    k: int,
    pgv_storage: str,
    pgv_ef: int,
    sh_ef: int,
    enable_ivfpq: bool = False,
    ivfpq_nlist: int = 256,
    ivfpq_nprobe: int = 16,
    ivfpq_m: int = 0,
    ivfpq_rerank_topk: int = 200,
    ivfpq_train_iter: int = 10,
    ivfpq_max_train: int = 10000,
    enable_flashhadamard: bool = False,
    flashhadamard_group_size: int = 16,
    seed: int = 42,
) -> PgRunResult:
    dim = vectors.shape[1]
    tmp = Path(tempfile.mkdtemp(prefix="ann_real_pg_", dir="/tmp"))
    pg_bindir = subprocess.check_output(["pg_config", "--bindir"], text=True).strip()
    port = pick_port()
    resolved_ivfpq_m = choose_pq_m(dim, ivfpq_m) if enable_ivfpq else 0
    if enable_ivfpq:
        if ivfpq_nlist <= 0:
            raise ValueError("ivfpq_nlist must be positive")
        if ivfpq_nprobe <= 0:
            raise ValueError("ivfpq_nprobe must be positive")
        if ivfpq_nprobe > ivfpq_nlist:
            raise ValueError("ivfpq_nprobe must be <= ivfpq_nlist")
        if ivfpq_nlist > vectors.shape[0]:
            raise ValueError(f"ivfpq_nlist={ivfpq_nlist} exceeds sample size {vectors.shape[0]}")
        if ivfpq_rerank_topk < 0:
            raise ValueError("ivfpq_rerank_topk must be non-negative")
        if 0 < ivfpq_rerank_topk < k:
            raise ValueError("ivfpq_rerank_topk must be zero or >= k")
        if ivfpq_train_iter <= 0:
            raise ValueError("ivfpq_train_iter must be positive")
        if ivfpq_max_train < 256:
            raise ValueError("ivfpq_max_train must be at least 256")
    if enable_flashhadamard and flashhadamard_group_size <= 0:
        raise ValueError("flashhadamard_group_size must be positive")

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

            ivfpq_result: IvfPqResult | None = None
            if enable_ivfpq:
                t0 = time.perf_counter()
                cur.execute(
                    "SELECT svec_ivf_train(%s, %s, %s, %s)",
                    ("SELECT v FROM bench_sh", ivfpq_nlist, ivfpq_train_iter, ivfpq_max_train),
                )
                ivf_cb_id = int(cur.fetchone()[0])
                cur.execute(
                    "SELECT svec_pq_train_residual(%s, %s, %s, %s, %s)",
                    ("SELECT v FROM bench_sh", resolved_ivfpq_m, ivf_cb_id, ivfpq_train_iter, ivfpq_max_train),
                )
                pq_cb_id = int(cur.fetchone()[0])
                train_ms = (time.perf_counter() - t0) * 1000.0

                t0 = time.perf_counter()
                cur.execute(
                    f"""
                    CREATE TABLE bench_ivfpq(
                      id text,
                      partition_id int2 GENERATED ALWAYS AS (
                        svec_ivf_assign(embedding, {ivf_cb_id})
                      ) STORED,
                      embedding svec({dim}),
                      pq_code bytea GENERATED ALWAYS AS (
                        svec_pq_encode_residual(
                          embedding,
                          svec_ivf_assign(embedding, {ivf_cb_id}),
                          {pq_cb_id},
                          {ivf_cb_id}
                        )
                      ) STORED,
                      PRIMARY KEY(partition_id, id)
                    ) USING sorted_heap
                    """
                )
                cur.execute("INSERT INTO bench_ivfpq(id, embedding) SELECT id::text, v FROM bench_sh")
                build_ms = (time.perf_counter() - t0) * 1000.0

                t0 = time.perf_counter()
                cur.execute("SELECT sorted_heap_compact('bench_ivfpq'::regclass)")
                compact_ms = (time.perf_counter() - t0) * 1000.0

                ivfpq_ms: list[float] = []
                ivfpq_recall_parts: list[float] = []
                for qid, lit in query_rows:
                    cur.execute(
                        """
                        SELECT id
                        FROM svec_ann_scan(
                          'bench_ivfpq'::regclass, %s::svec, %s, %s, %s, %s, %s,
                          'pq_code', '', 0
                        )
                        """,
                        (lit, ivfpq_nprobe, k, ivfpq_rerank_topk, pq_cb_id, ivf_cb_id),
                    )
                    t0 = time.perf_counter()
                    cur.execute(
                        """
                        SELECT id
                        FROM svec_ann_scan(
                          'bench_ivfpq'::regclass, %s::svec, %s, %s, %s, %s, %s,
                          'pq_code', '', 0
                        )
                        """,
                        (lit, ivfpq_nprobe, k, ivfpq_rerank_topk, pq_cb_id, ivf_cb_id),
                    )
                    ids = [int(row[0]) for row in cur.fetchall()]
                    ivfpq_ms.append((time.perf_counter() - t0) * 1000.0)
                    ivfpq_recall_parts.append(recall_at_k(ids, exact_ids[qid - 1], k))

                cur.execute(
                    """
                    SELECT
                      pg_size_pretty(pg_total_relation_size('bench_ivfpq'::regclass)),
                      pg_size_pretty(COALESCE((
                        SELECT sum(pg_total_relation_size(c.oid))::bigint
                        FROM pg_class c
                        WHERE c.relname IN (
                          '_ivf_meta', '_ivf_centroids',
                          '_pq_codebook_meta', '_pq_codebooks'
                        )
                      ), 0))
                    """
                )
                ivfpq_table_size, ivfpq_codebook_size = cur.fetchone()
                ivfpq_result = IvfPqResult(
                    p50=median_ms(ivfpq_ms),
                    p95=p95_ms(ivfpq_ms),
                    avg=avg_ms(ivfpq_ms),
                    recall=avg_ms(ivfpq_recall_parts),
                    train_ms=train_ms,
                    build_ms=build_ms,
                    compact_ms=compact_ms,
                    table_size=ivfpq_table_size,
                    codebook_size=ivfpq_codebook_size,
                    aux=(
                        f"nlist={ivfpq_nlist}|nprobe={ivfpq_nprobe}|m={resolved_ivfpq_m}"
                        f"|rerank_topk={ivfpq_rerank_topk}"
                    ),
                )

            flash_result: FlashHadamardResult | None = None
            if enable_flashhadamard:
                bench = load_turboquant_bench_module()
                base_norm = bench.normalize_rows(vectors.astype(np.float32, copy=False))
                query_norm = bench.normalize_rows(queries.astype(np.float32, copy=False))
                method = bench.TurboQuantBlock32PackedTopKMethod(
                    4,
                    seed,
                    group_size=flashhadamard_group_size,
                )

                t0 = time.perf_counter()
                method.fit(base_norm)
                encode_ms = (time.perf_counter() - t0) * 1000.0

                fh_ms: list[float] = []
                fh_hit1_parts: list[float] = []
                fh_recall_parts: list[float] = []
                for qi, query in enumerate(query_norm):
                    t0 = time.perf_counter()
                    found = method.search(query, k)
                    fh_ms.append((time.perf_counter() - t0) * 1000.0)
                    found_ids = (found + 1).astype(np.int64).tolist()
                    gt = exact_ids[qi]
                    fh_hit1_parts.append(100.0 if found_ids and gt and found_ids[0] == gt[0] else 0.0)
                    fh_recall_parts.append(recall_at_k(found_ids, gt, k))

                flash_result = FlashHadamardResult(
                    p50=median_ms(fh_ms),
                    p95=p95_ms(fh_ms),
                    avg=avg_ms(fh_ms),
                    hit1=avg_ms(fh_hit1_parts),
                    recall=avg_ms(fh_recall_parts),
                    encode_ms=encode_ms,
                    bytes_per_vec=method.bytes_per_vec(),
                    metadata_kb=method.metadata_bytes() / 1024.0,
                    aux=f"group_size={flashhadamard_group_size}|bits=4|backend={bench.packed_adc_backend_name()}",
                )

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
                ivfpq=ivfpq_result,
                flashhadamard=flash_result,
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
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--dataset", choices=sorted(DATASETS), help="ANN-Benchmarks dataset name (default: nytimes-256)")
    src.add_argument("--vectors", type=Path, help="Base vectors (.npy or .npz with key 'base')")
    ap.add_argument("--query-vectors", type=Path, help="Query vectors (.npy); optional for .npz with key 'queries'")
    ap.add_argument("--sample-size", type=int, default=10000)
    ap.add_argument("--queries", type=int, default=20)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--cache-dir", default="/tmp/ann_benchmarks")
    ap.add_argument("--pgv-storage", default="vector", choices=["vector", "halfvec"])
    ap.add_argument("--pgv-ef", type=int, default=64)
    ap.add_argument("--sh-ef", type=int, default=96)
    ap.add_argument("--enable-ivfpq", action="store_true")
    ap.add_argument("--ivfpq-nlist", type=int, default=256)
    ap.add_argument("--ivfpq-nprobe", type=int, default=16)
    ap.add_argument("--ivfpq-m", type=int, default=0, help="PQ subvector count; 0 chooses a divisor of dim")
    ap.add_argument("--ivfpq-rerank-topk", type=int, default=200)
    ap.add_argument("--ivfpq-train-iter", type=int, default=10)
    ap.add_argument("--ivfpq-max-train", type=int, default=10000)
    ap.add_argument("--enable-flashhadamard", action="store_true")
    ap.add_argument("--flashhadamard-group-size", type=int, default=16)
    ap.add_argument("--zvec-ef", type=int, default=64)
    ap.add_argument("--qdrant-ef", type=int, default=64)
    ap.add_argument("--skip-zvec", action="store_true")
    ap.add_argument("--skip-qdrant", action="store_true")
    args = ap.parse_args()

    if args.vectors is not None:
        vectors, queries = load_vector_files(args.vectors, args.query_vectors)
        source = str(args.vectors)
    else:
        dataset = args.dataset or "nytimes-256"
        vectors, queries = load_sample(dataset, args.sample_size, args.queries, args.seed, Path(args.cache_dir))
        source = dataset

    print("============================================================")
    print("real-dataset ANN benchmark")
    print("============================================================")
    print(f"source:      {source}")
    print(f"base_rows:   {vectors.shape[0]}")
    print(f"queries:     {queries.shape[0]}")
    print(f"dim:         {vectors.shape[1]}")
    print(f"k:           {args.k}")
    print(f"pgv_storage: {args.pgv_storage}")
    print(f"pgv_ef:      {args.pgv_ef}")
    print(f"sh_ef:       {args.sh_ef}")
    print(f"ivfpq:       {'on' if args.enable_ivfpq else 'off'}")
    if args.enable_ivfpq:
        print(
            f"ivfpq_cfg:   nlist={args.ivfpq_nlist} nprobe={args.ivfpq_nprobe} "
            f"m={args.ivfpq_m or 'auto'} rerank_topk={args.ivfpq_rerank_topk}"
        )
    print(f"flashh:     {'on' if args.enable_flashhadamard else 'off'}")
    if args.enable_flashhadamard:
        print(f"flashh_cfg: group_size={args.flashhadamard_group_size} bits=4")
    print(f"zvec_ef:     {args.zvec_ef}")
    print(f"qdrant_ef:   {args.qdrant_ef}")
    print()

    root_dir = Path(__file__).resolve().parent.parent
    pg = run_pg_benchmark(
        root_dir,
        vectors,
        queries,
        args.k,
        args.pgv_storage,
        args.pgv_ef,
        args.sh_ef,
        enable_ivfpq=args.enable_ivfpq,
        ivfpq_nlist=args.ivfpq_nlist,
        ivfpq_nprobe=args.ivfpq_nprobe,
        ivfpq_m=args.ivfpq_m,
        ivfpq_rerank_topk=args.ivfpq_rerank_topk,
        ivfpq_train_iter=args.ivfpq_train_iter,
        ivfpq_max_train=args.ivfpq_max_train,
        enable_flashhadamard=args.enable_flashhadamard,
        flashhadamard_group_size=args.flashhadamard_group_size,
        seed=args.seed,
    )
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
    if pg.ivfpq is not None:
        print_result(
            "ivfpq_residual",
            pg.ivfpq.p50,
            pg.ivfpq.avg,
            pg.ivfpq.recall,
            args.k,
            extra=(
                f"{pg.ivfpq.aux}|p95_ms={pg.ivfpq.p95:.3f}|train_ms={pg.ivfpq.train_ms:.1f}"
                f"|build_ms={pg.ivfpq.build_ms:.1f}|compact_ms={pg.ivfpq.compact_ms:.1f}"
                f"|table={pg.ivfpq.table_size}|codebooks={pg.ivfpq.codebook_size}"
            ),
        )
    if pg.flashhadamard is not None:
        print_result(
            f"flashhadamard{args.flashhadamard_group_size}_packed",
            pg.flashhadamard.p50,
            pg.flashhadamard.avg,
            pg.flashhadamard.recall,
            args.k,
            extra=(
                f"{pg.flashhadamard.aux}|p95_ms={pg.flashhadamard.p95:.3f}"
                f"|hit1={pg.flashhadamard.hit1:.1f}|encode_ms={pg.flashhadamard.encode_ms:.1f}"
                f"|bytes_per_vec={pg.flashhadamard.bytes_per_vec:.1f}"
                f"|metadata_kb={pg.flashhadamard.metadata_kb:.1f}"
            ),
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
