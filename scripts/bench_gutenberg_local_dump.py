#!/usr/bin/env python3
"""
Restore the Gutenberg benchmark subset from a local custom dump and benchmark
exact heap, sorted_hnsw, pgvector, zvec, and Qdrant on the same query set.

The script restores only the benchmark-relevant tables from the dump:
  - gutenberg_gptoss
  - gutenberg_gptoss_sh
  - bench_gptoss_queries
  - bench_hnsw_gt
  - IVF/PQ metadata tables needed by gutenberg_gptoss_sh generated columns

Ground truth is recomputed locally via exact heap search on gutenberg_gptoss_sh
for the query subset present in bench_hnsw_gt. The stored gt table is used only
to select the historical 50-query benchmark subset and as a restore sanity
check.
"""

from __future__ import annotations

import argparse
import os
import shlex
import shutil
import socket
import statistics
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import psycopg2
import zvec
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance
from qdrant_client.http.models import HnswConfigDiff
from qdrant_client.http.models import PointStruct
from qdrant_client.http.models import SearchParams
from qdrant_client.http.models import VectorParams


WANTED_RESTORE_OBJECTS = {
    "_ivf_meta",
    "_ivf_centroids",
    "_pq_codebook_meta",
    "_pq_codebooks",
    "bench_gptoss_queries",
    "bench_hnsw_gt",
    "gutenberg_gptoss",
    "gutenberg_gptoss_sh",
}


@dataclass
class PgRunResult:
    exact_p50: float
    exact_avg: float
    sorted_p50: float
    sorted_avg: float
    sorted_recall: float
    pgv_p50: float
    pgv_avg: float
    pgv_recall: float
    exact_ids: list[list[str]]
    query_literals: list[str]
    sh_index_size: str
    pgv_index_size: str
    sh_total_size: str
    pgv_total_size: str
    gt_table_exact_match_pct: float


@dataclass
class SimpleResult:
    p50: float
    avg: float
    recall: float
    aux: str = ""


def median_ms(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def avg_ms(values: list[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def recall_at_k(found_ids: list[str], gt_ids: list[str], k: int) -> float:
    if k <= 0:
        return 0.0
    return len(set(found_ids) & set(gt_ids)) / float(k) * 100.0


def pick_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def vector_literal_from_text(text: str) -> np.ndarray:
    inner = text.strip()
    if inner.startswith("[") and inner.endswith("]"):
        inner = inner[1:-1]
    return np.fromstring(inner, sep=",", dtype=np.float32)


def build_restore_list(dump_path: Path, list_path: Path) -> None:
    toc = subprocess.check_output(["pg_restore", "-l", str(dump_path)], text=True)
    lines: list[str] = []
    for line in toc.splitlines():
        if not line or line.startswith(";"):
            lines.append(line)
            continue
        object_name = None
        if " TABLE public " in line:
            object_name = line.split(" TABLE public ", 1)[1].split()[0]
        elif " TABLE DATA public " in line:
            object_name = line.split(" TABLE DATA public ", 1)[1].split()[0]

        if object_name in WANTED_RESTORE_OBJECTS:
            lines.append(line)
    list_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def init_temp_cluster(root_dir: Path, port: int, tmp_root: Path, install_cmd: list[str] | None = None) -> tuple[Path, str]:
    pg_bindir = subprocess.check_output(["pg_config", "--bindir"], text=True).strip()
    tmp = Path(tempfile.mkdtemp(prefix="gutenberg_local_", dir=str(tmp_root)))

    if install_cmd is None:
        install_cmd = ["make", "-C", str(root_dir), "install"]

    subprocess.run(
        install_cmd,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    subprocess.run(
        [f"{pg_bindir}/initdb", "-D", str(tmp / "data"), "-A", "trust", "--no-locale", "--encoding=UTF8"],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    with open(tmp / "data" / "postgresql.conf", "a", encoding="utf-8") as f:
        f.write(
            "shared_buffers = 512MB\n"
            "listen_addresses = ''\n"
            "fsync = on\n"
            "max_wal_size = 10GB\n"
            "checkpoint_timeout = 1h\n"
            "autovacuum = off\n"
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
    subprocess.run(
        [f"{pg_bindir}/createdb", "-h", str(tmp), "-p", str(port), "cogniformerus"],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return tmp, pg_bindir


def stop_temp_cluster(tmp: Path, pg_bindir: str) -> None:
    subprocess.run(
        [f"{pg_bindir}/pg_ctl", "-D", str(tmp / "data"), "-m", "immediate", "stop"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    shutil.rmtree(tmp, ignore_errors=True)


def restore_subset(tmp: Path, pg_bindir: str, port: int, dump_path: Path) -> None:
    subprocess.run(
        [
            f"{pg_bindir}/psql",
            "-h",
            str(tmp),
            "-p",
            str(port),
            "-d",
            "cogniformerus",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            "CREATE EXTENSION vector; CREATE EXTENSION pg_sorted_heap;",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    list_path = tmp / "restore.list"
    build_restore_list(dump_path, list_path)
    subprocess.run(
        [
            f"{pg_bindir}/pg_restore",
            "-h",
            str(tmp),
            "-p",
            str(port),
            "-d",
            "cogniformerus",
            "--no-owner",
            "-L",
            str(list_path),
            str(dump_path),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    subprocess.run(
        [
            f"{pg_bindir}/psql",
            "-h",
            str(tmp),
            "-p",
            str(port),
            "-d",
            "cogniformerus",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            "ANALYZE public.gutenberg_gptoss; ANALYZE public.gutenberg_gptoss_sh; ANALYZE public.bench_gptoss_queries; ANALYZE public.bench_hnsw_gt;",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def run_pg_benchmark(tmp: Path, port: int, k: int, pgv_ef: int, sh_ef: int) -> PgRunResult:
    conn = psycopg2.connect(host=str(tmp), port=port, dbname="cogniformerus")
    conn.autocommit = True
    conn.set_client_encoding("UTF8")
    cur = conn.cursor()
    try:
        cur.execute("SET jit = off")
        cur.execute(
            """
            SELECT q.qid, q.qvec::text, gt.gt_ids
            FROM public.bench_gptoss_queries q
            JOIN public.bench_hnsw_gt gt USING (qid)
            ORDER BY q.qid
            """
        )
        qrows = cur.fetchall()
        query_literals = [row[1] for row in qrows]
        gt_table_ids = [list(row[2]) for row in qrows]

        exact_ms: list[float] = []
        exact_ids: list[list[str]] = []

        cur.execute("SET enable_seqscan = on")
        cur.execute("SET enable_indexscan = off")
        cur.execute("SET enable_bitmapscan = off")
        for lit in query_literals:
            cur.execute(f"SELECT id FROM public.gutenberg_gptoss_sh ORDER BY embedding <=> %s::svec LIMIT {k}", (lit,))
            t0 = time.perf_counter()
            cur.execute(f"SELECT id FROM public.gutenberg_gptoss_sh ORDER BY embedding <=> %s::svec LIMIT {k}", (lit,))
            ids = [row[0] for row in cur.fetchall()]
            exact_ms.append((time.perf_counter() - t0) * 1000.0)
            exact_ids.append(ids)

        gt_matches = sum(1 for exact, stored in zip(exact_ids, gt_table_ids) if exact == stored)
        gt_match_pct = 100.0 * gt_matches / len(exact_ids) if exact_ids else 0.0

        cur.execute("CREATE INDEX gutenberg_gptoss_emb_idx ON public.gutenberg_gptoss USING hnsw (embedding halfvec_cosine_ops) WITH (m=16, ef_construction=100)")
        cur.execute("ANALYZE public.gutenberg_gptoss")
        cur.execute("CREATE INDEX gutenberg_gptoss_sh_shnsw_idx ON public.gutenberg_gptoss_sh USING sorted_hnsw (embedding) WITH (m=16, ef_construction=64)")
        cur.execute("ANALYZE public.gutenberg_gptoss_sh")

        sorted_ms: list[float] = []
        sorted_recall_parts: list[float] = []
        cur.execute("SET enable_seqscan = off")
        cur.execute("SET enable_indexscan = on")
        cur.execute("SET enable_bitmapscan = on")
        cur.execute("SET sorted_hnsw.shared_cache = on")
        cur.execute(f"SET sorted_hnsw.ef_search = {sh_ef}")
        for qi, lit in enumerate(query_literals):
            cur.execute(f"SELECT id FROM public.gutenberg_gptoss_sh ORDER BY embedding <=> %s::svec LIMIT {k}", (lit,))
            t0 = time.perf_counter()
            cur.execute(f"SELECT id FROM public.gutenberg_gptoss_sh ORDER BY embedding <=> %s::svec LIMIT {k}", (lit,))
            ids = [row[0] for row in cur.fetchall()]
            sorted_ms.append((time.perf_counter() - t0) * 1000.0)
            sorted_recall_parts.append(recall_at_k(ids, exact_ids[qi], k))

        pgv_ms: list[float] = []
        pgv_recall_parts: list[float] = []
        cur.execute(f"SET hnsw.ef_search = {pgv_ef}")
        for qi, lit in enumerate(query_literals):
            cur.execute(f"SELECT id FROM public.gutenberg_gptoss ORDER BY embedding <=> %s::halfvec(2880) LIMIT {k}", (lit,))
            t0 = time.perf_counter()
            cur.execute(f"SELECT id FROM public.gutenberg_gptoss ORDER BY embedding <=> %s::halfvec(2880) LIMIT {k}", (lit,))
            ids = [row[0] for row in cur.fetchall()]
            pgv_ms.append((time.perf_counter() - t0) * 1000.0)
            pgv_recall_parts.append(recall_at_k(ids, exact_ids[qi], k))

        cur.execute(
            """
            SELECT
              pg_size_pretty(pg_relation_size('public.gutenberg_gptoss_sh_shnsw_idx'::regclass)),
              pg_size_pretty(pg_relation_size('public.gutenberg_gptoss_emb_idx'::regclass)),
              pg_size_pretty(pg_total_relation_size('public.gutenberg_gptoss_sh'::regclass)),
              pg_size_pretty(pg_total_relation_size('public.gutenberg_gptoss'::regclass))
            """
        )
        sh_index_size, pgv_index_size, sh_total_size, pgv_total_size = cur.fetchone()

        return PgRunResult(
            exact_p50=median_ms(exact_ms),
            exact_avg=avg_ms(exact_ms),
            sorted_p50=median_ms(sorted_ms),
            sorted_avg=avg_ms(sorted_ms),
            sorted_recall=avg_ms(sorted_recall_parts),
            pgv_p50=median_ms(pgv_ms),
            pgv_avg=avg_ms(pgv_ms),
            pgv_recall=avg_ms(pgv_recall_parts),
            exact_ids=exact_ids,
            query_literals=query_literals,
            sh_index_size=sh_index_size,
            pgv_index_size=pgv_index_size,
            sh_total_size=sh_total_size,
            pgv_total_size=pgv_total_size,
            gt_table_exact_match_pct=gt_match_pct,
        )
    finally:
        cur.close()
        conn.close()


def export_vectors_and_queries(tmp: Path, port: int, out_dir: Path) -> tuple[Path, Path]:
    pg_bindir = subprocess.check_output(["pg_config", "--bindir"], text=True).strip()
    vectors_tsv = out_dir / "gutenberg_vectors.tsv"
    queries_tsv = out_dir / "gutenberg_queries.tsv"

    subprocess.run(
        [
            f"{pg_bindir}/psql",
            "-h",
            str(tmp),
            "-p",
            str(port),
            "-d",
            "cogniformerus",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            "\\copy (SELECT id, embedding::text FROM public.gutenberg_gptoss_sh ORDER BY id) TO '" + str(vectors_tsv) + "'",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    subprocess.run(
        [
            f"{pg_bindir}/psql",
            "-h",
            str(tmp),
            "-p",
            str(port),
            "-d",
            "cogniformerus",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            "\\copy (SELECT q.qid, q.qvec::text FROM public.bench_gptoss_queries q JOIN public.bench_hnsw_gt gt USING (qid) ORDER BY q.qid) TO '" + str(queries_tsv) + "'",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return vectors_tsv, queries_tsv


def iter_vector_tsv(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            ident, vec_text = line.split("\t", 1)
            yield ident, vec_text


def load_query_tsv(path: Path) -> list[np.ndarray]:
    out: list[np.ndarray] = []
    for _, vec_text in iter_vector_tsv(path):
        out.append(vector_literal_from_text(vec_text))
    return out


def run_zvec_benchmark(
    vectors_tsv: Path,
    query_vecs: list[np.ndarray],
    gt_ids: list[list[str]],
    k: int,
    ef: int,
    passes: int = 3,
    memory_limit_mb: int = 8192,
) -> SimpleResult:
    base = tempfile.mkdtemp(prefix="gutenberg_zvec_", dir="/tmp")
    path = os.path.join(base, "bench")
    try:
        zvec.init(memory_limit_mb=memory_limit_mb)
        schema = zvec.CollectionSchema(
            name="bench",
            vectors=zvec.VectorSchema(
                name="embedding",
                data_type=zvec.DataType.VECTOR_FP32,
                dimension=2880,
                index_param=zvec.HnswIndexParam(
                    metric_type=zvec.MetricType.COSINE,
                    m=16,
                    ef_construction=64,
                ),
            ),
        )
        coll = zvec.create_and_open(path, schema)
        batch: list[zvec.Doc] = []
        id_map: dict[str, str] = {}
        for idx, (ident, vec_text) in enumerate(iter_vector_tsv(vectors_tsv), start=1):
            doc_id = str(idx)
            id_map[doc_id] = ident
            batch.append(zvec.Doc(id=doc_id, vectors={"embedding": vector_literal_from_text(vec_text).tolist()}))
            if len(batch) == 128:
                coll.insert(batch)
                batch = []
        if batch:
            coll.insert(batch)
        coll.flush()

        param = zvec.HnswQueryParam(ef=ef)
        for q in query_vecs:
            coll.query(zvec.VectorQuery("embedding", vector=q.tolist(), param=param), topk=k)

        p50s = []
        avgs = []
        recalls = []
        for _ in range(passes):
            ms = []
            recall_parts = []
            for qi, q in enumerate(query_vecs):
                t0 = time.perf_counter()
                res = coll.query(zvec.VectorQuery("embedding", vector=q.tolist(), param=param), topk=k)
                ms.append((time.perf_counter() - t0) * 1000.0)
                ids = [id_map[doc.id] for doc in res]
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
            ["docker", "run", "-d", "--rm", "--name", "qdrant-bench", "-p", "6333:6333", "qdrant/qdrant:v1.13.2"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        for _ in range(30):
            try:
                client = QdrantClient(url="http://127.0.0.1:6333", timeout=60, check_compatibility=False)
                client.get_collections()
                started = True
                return client, started
            except Exception:
                time.sleep(1.0)
        raise RuntimeError("failed to start/connect to Qdrant on 127.0.0.1:6333")


def run_qdrant_benchmark(vectors_tsv: Path, query_vecs: list[np.ndarray], gt_ids: list[list[str]], k: int, ef: int, passes: int = 3) -> SimpleResult:
    client, started = ensure_qdrant()
    name = "gutenberg_local_" + uuid.uuid4().hex[:8]
    try:
        client.create_collection(
            collection_name=name,
            vectors_config=VectorParams(size=2880, distance=Distance.COSINE),
            hnsw_config=HnswConfigDiff(m=16, ef_construct=64),
        )
        batch: list[PointStruct] = []
        for idx, (_, vec_text) in enumerate(iter_vector_tsv(vectors_tsv), start=1):
            batch.append(PointStruct(id=idx, vector=vector_literal_from_text(vec_text).tolist(), payload={"doc_id": idx}))
            if len(batch) == 64:
                client.upsert(collection_name=name, points=batch, wait=True)
                batch = []
        if batch:
            client.upsert(collection_name=name, points=batch, wait=True)

        id_map: dict[int, str] = {}
        for idx, (ident, _) in enumerate(iter_vector_tsv(vectors_tsv), start=1):
            id_map[idx] = ident

        params = SearchParams(hnsw_ef=ef, exact=False)
        for q in query_vecs:
            client.query_points(collection_name=name, query=q.tolist(), limit=k, search_params=params)

        p50s = []
        avgs = []
        recalls = []
        for _ in range(passes):
            ms = []
            recall_parts = []
            for qi, q in enumerate(query_vecs):
                t0 = time.perf_counter()
                res = client.query_points(collection_name=name, query=q.tolist(), limit=k, search_params=params).points
                ms.append((time.perf_counter() - t0) * 1000.0)
                ids = [id_map[int(p.id)] for p in res]
                recall_parts.append(recall_at_k(ids, gt_ids[qi], k))
            p50s.append(median_ms(ms))
            avgs.append(avg_ms(ms))
            recalls.append(avg_ms(recall_parts))

        return SimpleResult(median_ms(p50s), median_ms(avgs), median_ms(recalls), aux=f"points={len(id_map)}")
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
    ap.add_argument("--dump", default="/tmp/cogniformerus_backup/cogniformerus_backup.dump")
    ap.add_argument("--tmp-root", default="/tmp")
    ap.add_argument("--port", type=int, default=65471)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--pgv-ef", type=int, default=64)
    ap.add_argument("--sh-ef", type=int, default=96)
    ap.add_argument("--zvec-ef", type=int, default=64)
    ap.add_argument("--zvec-memory-limit-mb", type=int, default=8192)
    ap.add_argument("--qdrant-ef", type=int, default=64)
    ap.add_argument("--install-cmd", default="")
    ap.add_argument("--skip-zvec", action="store_true")
    ap.add_argument("--skip-qdrant", action="store_true")
    args = ap.parse_args()

    root_dir = Path(__file__).resolve().parent.parent
    dump_path = Path(args.dump).resolve()
    if not dump_path.exists():
        raise FileNotFoundError(dump_path)

    tmp_root = Path(args.tmp_root).resolve()
    install_cmd = shlex.split(args.install_cmd) if args.install_cmd else None
    tmp, pg_bindir = init_temp_cluster(root_dir, args.port, tmp_root, install_cmd)
    try:
        restore_subset(tmp, pg_bindir, args.port, dump_path)

        print("============================================================")
        print("gutenberg local dump benchmark")
        print("============================================================")
        print(f"dump:      {dump_path}")
        print(f"port:      {args.port}")
        print(f"k:         {args.k}")
        print(f"pgv_ef:    {args.pgv_ef}")
        print(f"sh_ef:     {args.sh_ef}")
        print(f"zvec_ef:   {args.zvec_ef}")
        print(f"qdrant_ef: {args.qdrant_ef}")
        print()

        pg = run_pg_benchmark(tmp, args.port, args.k, args.pgv_ef, args.sh_ef)
        print_result("exact_heap", pg.exact_p50, pg.exact_avg, 100.0, args.k)
        print_result("sorted_hnsw", pg.sorted_p50, pg.sorted_avg, pg.sorted_recall, args.k, extra=f"index={pg.sh_index_size}")
        print_result("pgvector_hnsw_halfvec", pg.pgv_p50, pg.pgv_avg, pg.pgv_recall, args.k, extra=f"index={pg.pgv_index_size}")
        print(
            f"pg_sizes|sorted_hnsw_index={pg.sh_index_size}|pgvector_index={pg.pgv_index_size}"
            f"|bench_sh_total={pg.sh_total_size}|bench_pgv_total={pg.pgv_total_size}"
        )
        print(f"gt_sanity|bench_hnsw_gt_exact_match_pct={pg.gt_table_exact_match_pct:.1f}")

        if not args.skip_zvec or not args.skip_qdrant:
            export_dir = Path(tempfile.mkdtemp(prefix="gutenberg_export_", dir="/tmp"))
            try:
                vectors_tsv, queries_tsv = export_vectors_and_queries(tmp, args.port, export_dir)
                query_vecs = load_query_tsv(queries_tsv)
                if not args.skip_zvec:
                    zres = run_zvec_benchmark(
                        vectors_tsv,
                        query_vecs,
                        pg.exact_ids,
                        args.k,
                        args.zvec_ef,
                        memory_limit_mb=args.zvec_memory_limit_mb,
                    )
                    print_result("zvec", zres.p50, zres.avg, zres.recall, args.k, extra=zres.aux)
                if not args.skip_qdrant:
                    qres = run_qdrant_benchmark(vectors_tsv, query_vecs, pg.exact_ids, args.k, args.qdrant_ef)
                    print_result("qdrant", qres.p50, qres.avg, qres.recall, args.k, extra=qres.aux)
            finally:
                shutil.rmtree(export_dir, ignore_errors=True)

        return 0
    finally:
        stop_temp_cluster(tmp, pg_bindir)


if __name__ == "__main__":
    raise SystemExit(main())
