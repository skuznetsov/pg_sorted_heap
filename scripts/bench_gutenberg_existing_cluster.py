#!/usr/bin/env python3
"""
Benchmark Gutenberg ANN engines against an already restored local cluster.

This keeps the dataset fixed and lets us compare sorted_hnsw, pgvector, zvec,
and Qdrant on the same sampled query set and the same exact svec ground truth,
without paying restore noise on every run.
"""

from __future__ import annotations

import argparse
import shutil
import tempfile
import time
from pathlib import Path

import numpy as np
import psycopg2
import zvec
from qdrant_client.http.models import PointStruct
from qdrant_client.http.models import SearchParams

import bench_gutenberg_fixed_graph as fixed
import bench_gutenberg_local_dump as local_bench


def ensure_pg_objects(
    cur: psycopg2.extensions.cursor,
    sh_ef_construction: int,
    pgv_ef_construction: int,
    need_svec: bool,
    need_hsvec: bool,
    need_pgv: bool,
) -> tuple[str, str, str]:
    sh_index_size = "skipped"
    hs_index_size = "skipped"
    pgv_index_size = "skipped"

    if need_hsvec:
        cur.execute("SELECT to_regclass('public.gutenberg_gptoss_hs') IS NOT NULL")
        if not cur.fetchone()[0]:
            fixed.prepare_hs_table(cur)

    if need_svec:
        cur.execute("SELECT to_regclass('public.gutenberg_gptoss_sh_shnsw_idx') IS NOT NULL")
        if not cur.fetchone()[0]:
            cur.execute(
                f"CREATE INDEX gutenberg_gptoss_sh_shnsw_idx ON public.gutenberg_gptoss_sh "
                f"USING sorted_hnsw (embedding) WITH (m=16, ef_construction={sh_ef_construction})"
            )
            cur.execute("ANALYZE public.gutenberg_gptoss_sh")
        cur.execute("SELECT pg_size_pretty(pg_relation_size('public.gutenberg_gptoss_sh_shnsw_idx'::regclass))")
        sh_index_size = cur.fetchone()[0]

    if need_hsvec:
        cur.execute("SELECT to_regclass('public.gutenberg_gptoss_hs_shnsw_idx') IS NOT NULL")
        if not cur.fetchone()[0]:
            cur.execute(
                f"CREATE INDEX gutenberg_gptoss_hs_shnsw_idx ON public.gutenberg_gptoss_hs "
                f"USING sorted_hnsw (embedding hsvec_cosine_ops) WITH (m=16, ef_construction={sh_ef_construction})"
            )
            cur.execute("ANALYZE public.gutenberg_gptoss_hs")
        cur.execute("SELECT pg_size_pretty(pg_relation_size('public.gutenberg_gptoss_hs_shnsw_idx'::regclass))")
        hs_index_size = cur.fetchone()[0]

    if need_pgv:
        cur.execute("SELECT to_regclass('public.gutenberg_gptoss_emb_idx') IS NOT NULL")
        if not cur.fetchone()[0]:
            cur.execute(
                f"CREATE INDEX gutenberg_gptoss_emb_idx ON public.gutenberg_gptoss "
                f"USING hnsw (embedding halfvec_cosine_ops) WITH (m=16, ef_construction={pgv_ef_construction})"
            )
            cur.execute("ANALYZE public.gutenberg_gptoss")
        cur.execute("SELECT pg_size_pretty(pg_relation_size('public.gutenberg_gptoss_emb_idx'::regclass))")
        pgv_index_size = cur.fetchone()[0]

    return sh_index_size, hs_index_size, pgv_index_size


def measure_sql_ann(
    host: str,
    port: int,
    dbname: str,
    table_name: str,
    cast_name: str,
    query_specs: list[fixed.QuerySpec],
    exact_ids: list[list[str]],
    k: int,
    ef_search: int,
    set_sql: str,
) -> tuple[float, float, float]:
    conn = psycopg2.connect(host=host, port=port, dbname=dbname)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute("SET jit = off")
        cur.execute("SET enable_seqscan = off")
        cur.execute("SET enable_indexscan = on")
        cur.execute("SET enable_bitmapscan = on")
        cur.execute(set_sql.format(ef_search=ef_search))

        ms: list[float] = []
        recall_parts: list[float] = []
        for qi, (source_id, lit) in enumerate(query_specs):
            if source_id is None:
                sql = f"SELECT id FROM {table_name} ORDER BY embedding <=> %s::{cast_name} LIMIT {k}"
                params = (lit,)
            else:
                sql = (
                    f"SELECT id FROM {table_name} "
                    f"WHERE id <> %s ORDER BY embedding <=> %s::{cast_name} LIMIT {k}"
                )
                params = (source_id, lit)
            cur.execute(sql, params)
            t0 = time.perf_counter()
            cur.execute(sql, params)
            ids = [row[0] for row in cur.fetchall()]
            ms.append((time.perf_counter() - t0) * 1000.0)
            recall_parts.append(local_bench.recall_at_k(ids, exact_ids[qi], k))
        return local_bench.median_ms(ms), local_bench.avg_ms(ms), local_bench.avg_ms(recall_parts)
    finally:
        cur.close()
        conn.close()


def export_vectors_tsv(host: str, port: int, dbname: str, out_dir: Path) -> Path:
    out = out_dir / "gutenberg_vectors.tsv"
    conn = psycopg2.connect(host=host, port=port, dbname=dbname)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute("SELECT id, embedding::text FROM public.gutenberg_gptoss_sh ORDER BY id")
        with open(out, "w", encoding="utf-8") as f:
            for doc_id, vec_text in cur.fetchall():
                f.write(f"{doc_id}\t{vec_text}\n")
    finally:
        cur.close()
        conn.close()
    return out


def measure_zvec_existing(
    vectors_tsv: Path,
    query_specs: list[fixed.QuerySpec],
    exact_ids: list[list[str]],
    k: int,
    ef: int,
    passes: int,
    memory_limit_mb: int,
) -> local_bench.SimpleResult:
    base = tempfile.mkdtemp(prefix="gutenberg_zvec_existing_", dir="/tmp")
    path = Path(base) / "bench"
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
        coll = zvec.create_and_open(str(path), schema)
        batch: list[zvec.Doc] = []
        id_map: dict[str, str] = {}
        for idx, (ident, vec_text) in enumerate(local_bench.iter_vector_tsv(vectors_tsv), start=1):
            doc_id = str(idx)
            id_map[doc_id] = ident
            batch.append(zvec.Doc(id=doc_id, vectors={"embedding": local_bench.vector_literal_from_text(vec_text).tolist()}))
            if len(batch) == 128:
                coll.insert(batch)
                batch = []
        if batch:
            coll.insert(batch)
        coll.flush()

        param = zvec.HnswQueryParam(ef=ef)
        for _, lit in query_specs[: min(10, len(query_specs))]:
            q = local_bench.vector_literal_from_text(lit)
            coll.query(zvec.VectorQuery("embedding", vector=q.tolist(), param=param), topk=k + 1)

        p50s: list[float] = []
        avgs: list[float] = []
        recalls: list[float] = []
        for _ in range(passes):
            ms: list[float] = []
            recall_parts: list[float] = []
            for qi, (source_id, lit) in enumerate(query_specs):
                q = local_bench.vector_literal_from_text(lit)
                t0 = time.perf_counter()
                res = coll.query(zvec.VectorQuery("embedding", vector=q.tolist(), param=param), topk=k + 1)
                ms.append((time.perf_counter() - t0) * 1000.0)
                ids = [id_map[doc.id] for doc in res]
                if source_id is not None:
                    ids = [doc_id for doc_id in ids if doc_id != source_id]
                ids = ids[:k]
                recall_parts.append(local_bench.recall_at_k(ids, exact_ids[qi], k))
            p50s.append(local_bench.median_ms(ms))
            avgs.append(local_bench.avg_ms(ms))
            recalls.append(local_bench.avg_ms(recall_parts))

        size_bytes = sum((Path(root) / name).stat().st_size for root, _, files in shutil.os.walk(path) for name in files)
        return local_bench.SimpleResult(
            local_bench.median_ms(p50s),
            local_bench.median_ms(avgs),
            local_bench.median_ms(recalls),
            aux=f"bytes={size_bytes}",
        )
    finally:
        shutil.rmtree(base, ignore_errors=True)


def measure_qdrant_existing(
    vectors_tsv: Path,
    query_specs: list[fixed.QuerySpec],
    exact_ids: list[list[str]],
    k: int,
    ef: int,
    passes: int,
) -> local_bench.SimpleResult:
    client, started = local_bench.ensure_qdrant()
    name = "gutenberg_existing_" + next(tempfile._get_candidate_names())[:8]
    try:
        client.create_collection(
            collection_name=name,
            vectors_config=local_bench.VectorParams(size=2880, distance=local_bench.Distance.COSINE),
            hnsw_config=local_bench.HnswConfigDiff(m=16, ef_construct=64),
        )
        batch: list[PointStruct] = []
        id_map: dict[int, str] = {}
        for idx, (ident, vec_text) in enumerate(local_bench.iter_vector_tsv(vectors_tsv), start=1):
            id_map[idx] = ident
            batch.append(
                PointStruct(
                    id=idx,
                    vector=local_bench.vector_literal_from_text(vec_text).tolist(),
                    payload={"doc_id": ident},
                )
            )
            if len(batch) == 64:
                client.upsert(collection_name=name, points=batch, wait=True)
                batch = []
        if batch:
            client.upsert(collection_name=name, points=batch, wait=True)

        params = SearchParams(hnsw_ef=ef, exact=False)
        for _, lit in query_specs[: min(10, len(query_specs))]:
            q = local_bench.vector_literal_from_text(lit)
            client.query_points(collection_name=name, query=q.tolist(), limit=k + 1, search_params=params)

        p50s: list[float] = []
        avgs: list[float] = []
        recalls: list[float] = []
        for _ in range(passes):
            ms: list[float] = []
            recall_parts: list[float] = []
            for qi, (source_id, lit) in enumerate(query_specs):
                q = local_bench.vector_literal_from_text(lit)
                t0 = time.perf_counter()
                res = client.query_points(
                    collection_name=name,
                    query=q.tolist(),
                    limit=k + 1,
                    search_params=params,
                )
                ms.append((time.perf_counter() - t0) * 1000.0)
                ids = [point.payload["doc_id"] for point in res.points]
                if source_id is not None:
                    ids = [doc_id for doc_id in ids if doc_id != source_id]
                ids = ids[:k]
                recall_parts.append(local_bench.recall_at_k(ids, exact_ids[qi], k))
            p50s.append(local_bench.median_ms(ms))
            avgs.append(local_bench.avg_ms(ms))
            recalls.append(local_bench.avg_ms(recall_parts))

        info = client.get_collection(name)
        return local_bench.SimpleResult(
            local_bench.median_ms(p50s),
            local_bench.median_ms(avgs),
            local_bench.median_ms(recalls),
            aux=f"points={info.points_count}",
        )
    finally:
        try:
            client.delete_collection(name)
        except Exception:
            pass
        if started:
            local_bench.subprocess.run(
                ["docker", "stop", "qdrant-bench"],
                stdout=local_bench.subprocess.DEVNULL,
                stderr=local_bench.subprocess.DEVNULL,
            )


def print_result(method: str, p50: float, avg: float, recall: float, k: int, extra: str = "") -> None:
    suffix = f"|{extra}" if extra else ""
    print(f"{method}|p50_ms={p50:.3f}|avg_ms={avg:.3f}|recall_at_{k}={recall:.1f}{suffix}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--dbname", default="cogniformerus")
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--query-count", type=int, default=500)
    ap.add_argument("--query-source", choices=("bench", "sample"), default="sample")
    ap.add_argument("--sample-seed", default="gutenberg-existing-1")
    ap.add_argument("--sh-ef", type=int, default=32)
    ap.add_argument("--pgv-ef", type=int, default=64)
    ap.add_argument("--zvec-ef", type=int, default=64)
    ap.add_argument("--zvec-memory-limit-mb", type=int, default=8192)
    ap.add_argument("--qdrant-ef", type=int, default=64)
    ap.add_argument("--sh-ef-construction", type=int, default=200)
    ap.add_argument("--pgv-ef-construction", type=int, default=100)
    ap.add_argument("--passes", type=int, default=1)
    ap.add_argument("--skip-svec", action="store_true")
    ap.add_argument("--skip-hsvec", action="store_true")
    ap.add_argument("--skip-pgvector", action="store_true")
    ap.add_argument("--skip-zvec", action="store_true")
    ap.add_argument("--skip-qdrant", action="store_true")
    args = ap.parse_args()

    if args.skip_svec and args.skip_hsvec and args.skip_pgvector and args.skip_zvec and args.skip_qdrant:
        raise SystemExit("all engines were skipped")

    conn = psycopg2.connect(host=args.host, port=args.port, dbname=args.dbname)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute("SET jit = off")
        query_specs = fixed.load_query_specs(cur, args.query_count, args.query_source, args.sample_seed)
        exact_p50, exact_avg, exact_ids = fixed.compute_exact_gt(cur, query_specs, args.k)
        sh_index_size, hs_index_size, pgv_index_size = ensure_pg_objects(
            cur,
            args.sh_ef_construction,
            args.pgv_ef_construction,
            need_svec=not args.skip_svec,
            need_hsvec=not args.skip_hsvec,
            need_pgv=not args.skip_pgvector,
        )
    finally:
        cur.close()
        conn.close()

    print("============================================================")
    print("gutenberg existing-cluster benchmark")
    print("============================================================")
    print(f"host:              {args.host}")
    print(f"port:              {args.port}")
    print(f"dbname:            {args.dbname}")
    print(f"k:                 {args.k}")
    print(f"query_count:       {args.query_count}")
    print(f"query_source:      {args.query_source}")
    if args.query_source == "sample":
        print(f"sample_seed:       {args.sample_seed}")
    print(f"sh_ef:             {args.sh_ef}")
    print(f"pgv_ef:            {args.pgv_ef}")
    print(f"zvec_ef:           {args.zvec_ef}")
    print(f"qdrant_ef:         {args.qdrant_ef}")
    print(f"passes:            {args.passes}")
    print()

    print_result("exact_heap", exact_p50, exact_avg, 100.0, args.k)

    if not args.skip_svec:
        p50, avg, recall = measure_sql_ann(
            args.host, args.port, args.dbname,
            "public.gutenberg_gptoss_sh", "svec",
            query_specs, exact_ids, args.k, args.sh_ef,
            "SET sorted_hnsw.shared_cache = on; SET sorted_hnsw.ef_search = {ef_search}",
        )
        print_result("sorted_hnsw_svec", p50, avg, recall, args.k, extra=f"index={sh_index_size}")

    if not args.skip_hsvec:
        p50, avg, recall = measure_sql_ann(
            args.host, args.port, args.dbname,
            "public.gutenberg_gptoss_hs", "hsvec",
            query_specs, exact_ids, args.k, args.sh_ef,
            "SET sorted_hnsw.shared_cache = on; SET sorted_hnsw.ef_search = {ef_search}",
        )
        print_result("sorted_hnsw_hsvec", p50, avg, recall, args.k, extra=f"index={hs_index_size}")

    if not args.skip_pgvector:
        p50, avg, recall = measure_sql_ann(
            args.host, args.port, args.dbname,
            "public.gutenberg_gptoss", "halfvec(2880)",
            query_specs, exact_ids, args.k, args.pgv_ef,
            "SET hnsw.ef_search = {ef_search}",
        )
        print_result("pgvector_hnsw_halfvec", p50, avg, recall, args.k, extra=f"index={pgv_index_size}")

    if not args.skip_zvec or not args.skip_qdrant:
        out_dir = Path(tempfile.mkdtemp(prefix="gutenberg_existing_bench_", dir="/tmp"))
        try:
            vectors_tsv = export_vectors_tsv(args.host, args.port, args.dbname, out_dir)
            if not args.skip_zvec:
                zres = measure_zvec_existing(
                    vectors_tsv, query_specs, exact_ids, args.k,
                    args.zvec_ef, args.passes, args.zvec_memory_limit_mb,
                )
                print_result("zvec", zres.p50, zres.avg, zres.recall, args.k, extra=zres.aux)
            if not args.skip_qdrant:
                qres = measure_qdrant_existing(
                    vectors_tsv, query_specs, exact_ids, args.k,
                    args.qdrant_ef, args.passes,
                )
                print_result("qdrant", qres.p50, qres.avg, qres.recall, args.k, extra=qres.aux)
        finally:
            shutil.rmtree(out_dir, ignore_errors=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
