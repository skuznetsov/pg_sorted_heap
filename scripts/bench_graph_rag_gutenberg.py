#!/usr/bin/env python3
"""
Benchmark GraphRAG helpers on a real-text Gutenberg paragraph graph.

Dataset shape:
  - relation 1: book -> paragraph ("contains")
  - relation 2: paragraph -> next paragraph ("next")

This keeps the GraphRAG contract close to a real text graph without requiring
an external embedding server. Embeddings are deterministic lexical hash vectors
derived from paragraph text, so repeated runs are stable and the ANN seed step
still has text-locality instead of pure random noise.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import math
import re
import shlex
import shutil
import subprocess
import statistics
import tempfile
import time
import uuid
from pathlib import Path

import bench_graph_rag as base
import zvec
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance
from qdrant_client.http.models import HnswConfigDiff
from qdrant_client.http.models import PointStruct
from qdrant_client.http.models import SearchParams
from qdrant_client.http.models import VectorParams

TOKEN_RE = re.compile(r"[A-Za-z0-9']+")


def tokenize(text: str) -> list[str]:
    return [m.group(0).lower() for m in TOKEN_RE.finditer(text)]


def lexical_hash_vector(text: str, dim: int) -> str:
    acc = [0.0] * dim
    tokens = tokenize(text)
    if not tokens:
        tokens = [text.lower() or "_"]

    for token in tokens[:256]:
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        block = digest
        for i in range(dim):
            if i > 0 and i % 256 == 0:
                block = hashlib.sha256(block).digest()
            bit = (block[(i // 8) % len(block)] >> (i % 8)) & 1
            acc[i] += 1.0 if bit else -1.0

    norm = math.sqrt(sum(v * v for v in acc))
    if norm < 1e-8:
        vals = [0.0] * dim
    else:
        vals = [v / norm for v in acc]
    return "[" + ",".join(f"{v:.6f}" for v in vals) + "]"


def extract_paragraphs(text: str, max_paragraphs: int, skip_paragraphs: int) -> list[str]:
    paragraphs: list[str] = []
    for para in text.split("\n\n"):
        clean = " ".join(line.strip() for line in para.splitlines() if line.strip())
        if 50 < len(clean) < 2000:
            paragraphs.append(clean)
    return paragraphs[skip_paragraphs : skip_paragraphs + max_paragraphs]


def list_books(gutenberg_path: Path, max_books: int) -> list[Path]:
    return sorted(gutenberg_path.glob("*.txt"))[:max_books]


def generate_csv_from_gutenberg(
    path: Path,
    gutenberg_path: Path,
    max_books: int,
    max_paragraphs: int,
    skip_paragraphs: int,
    dim: int,
) -> tuple[int, int, int, int]:
    rows = 0
    contains_rows = 0
    next_rows = 0
    next_node_id = 1

    books = list_books(gutenberg_path, max_books)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for book_idx, book_path in enumerate(books, start=1):
            book_id = book_idx
            text = book_path.read_text(encoding="utf-8", errors="ignore")
            paragraphs = extract_paragraphs(text, max_paragraphs, skip_paragraphs)
            para_ids: list[int] = []

            for para in paragraphs:
                para_id = max_books + next_node_id
                next_node_id += 1
                para_ids.append(para_id)

                w.writerow(
                    [
                        book_id,
                        1,
                        para_id,
                        lexical_hash_vector(para, dim),
                        para,
                    ]
                )
                rows += 1
                contains_rows += 1

            for idx in range(len(paragraphs) - 1):
                next_para = paragraphs[idx + 1]
                w.writerow(
                    [
                        para_ids[idx],
                        2,
                        para_ids[idx + 1],
                        lexical_hash_vector(next_para, dim),
                        next_para,
                    ]
                )
                rows += 1
                next_rows += 1

    return rows, len(books), contains_rows, next_rows


def load_relation_queries(cur, query_count: int, relation_id: int) -> list[tuple[int, int, str]]:
    cur.execute(
        """
        SELECT entity_id, relation_id, embedding::text
        FROM facts_heap
        WHERE relation_id = %s
        ORDER BY entity_id, target_id
        LIMIT %s
        """,
        (relation_id, query_count),
    )
    return [(row[0], row[1], row[2]) for row in cur.fetchall()]


def vector_list_from_literal(lit: str) -> list[float]:
    inner = lit.strip()[1:-1]
    if not inner:
        return []
    return [float(x) for x in inner.split(",")]


def bootstrap_pgvector(cur, csv_path: Path, dim: int, ef_construction: int) -> None:
    cur.execute("CREATE EXTENSION vector")
    cur.execute(
        f"""
        CREATE TABLE facts_pgv (
            entity_id   int4 NOT NULL,
            relation_id int2 NOT NULL,
            target_id   int4 NOT NULL,
            embedding   vector({dim}) NOT NULL,
            payload     text NOT NULL,
            PRIMARY KEY (entity_id, relation_id, target_id)
        )
        """
    )
    with open(csv_path, "r", encoding="utf-8") as f:
        cur.copy_expert(
            """
            COPY facts_pgv (entity_id, relation_id, target_id, embedding, payload)
            FROM STDIN WITH (FORMAT csv)
            """,
            f,
        )
    cur.execute(
        f"""
        CREATE INDEX facts_pgv_ann_idx
        ON facts_pgv USING hnsw (embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = {ef_construction})
        """
    )
    cur.execute("ANALYZE facts_pgv")


def build_zvec_collection(
    csv_path: Path,
    dim: int,
    ef_construction: int,
    memory_limit_mb: int,
) -> tuple[object, dict[str, int], str]:
    zvec.init(memory_limit_mb=memory_limit_mb)
    base_dir = tempfile.mkdtemp(prefix="graph_rag_gutenberg_zvec_", dir="/tmp")
    path = str(Path(base_dir) / "bench")
    schema = zvec.CollectionSchema(
        name="bench",
        vectors=zvec.VectorSchema(
            name="embedding",
            data_type=zvec.DataType.VECTOR_FP32,
            dimension=dim,
            index_param=zvec.HnswIndexParam(
                metric_type=zvec.MetricType.COSINE,
                m=16,
                ef_construction=ef_construction,
            ),
        ),
    )
    coll = zvec.create_and_open(path, schema)
    id_to_target: dict[str, int] = {}
    docs: list[zvec.Doc] = []
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        for idx, row in enumerate(csv.reader(f), start=1):
            target_id = int(row[2])
            doc_id = str(idx)
            id_to_target[doc_id] = target_id
            docs.append(zvec.Doc(id=doc_id, vectors={"embedding": vector_list_from_literal(row[3])}))
            if len(docs) == 128:
                coll.insert(docs)
                docs = []
    if docs:
        coll.insert(docs)
    coll.flush()
    return coll, id_to_target, base_dir


def unique_ints_in_order(values: list[int]) -> list[int]:
    out: list[int] = []
    seen: set[int] = set()
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def map_zvec_results_to_targets(results, id_to_target: dict[str, int]) -> list[int]:
    missing = [doc.id for doc in results if doc.id not in id_to_target]
    if missing:
        sample = missing[:8]
        raise RuntimeError(
            f"zvec returned unmapped doc ids (sample={sample}, total_missing={len(missing)})"
        )
    return unique_ints_in_order([id_to_target[doc.id] for doc in results])


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


def build_qdrant_collection(
    csv_path: Path,
    dim: int,
    ef_construction: int,
) -> tuple[QdrantClient, bool, str, dict[int, int]]:
    client, started = ensure_qdrant()
    name = "graph_rag_gutenberg_" + uuid.uuid4().hex[:8]
    client.create_collection(
        collection_name=name,
        vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
        hnsw_config=HnswConfigDiff(m=16, ef_construct=ef_construction),
    )
    batch: list[PointStruct] = []
    id_to_target: dict[int, int] = {}
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        for idx, row in enumerate(csv.reader(f), start=1):
            target_id = int(row[2])
            id_to_target[idx] = target_id
            batch.append(PointStruct(id=idx, vector=vector_list_from_literal(row[3]), payload={"target_id": target_id}))
            if len(batch) == 64:
                client.upsert(collection_name=name, points=batch, wait=True)
                batch = []
    if batch:
        client.upsert(collection_name=name, points=batch, wait=True)
    return client, started, name, id_to_target


def measure_external_graph_rag_case(
    cur,
    case_name: str,
    queries: list[tuple[int, int, str]],
    runs: int,
    seed_fn,
    sql_template: str,
    sql_params_fn,
) -> tuple[float, float, float, float, str, int]:
    all_total_ms: list[float] = []
    all_hits: list[int] = []
    all_reads: list[int] = []
    root = ""
    rowcount = 0

    for run_idx in range(runs):
        for qi, query in enumerate(queries):
            qvec = vector_list_from_literal(query[2])
            t0 = time.perf_counter()
            seeds = seed_fn(qvec)
            ext_ms = (time.perf_counter() - t0) * 1000.0
            sql, params = sql_params_fn(query, seeds)
            sql_ms, hits, reads, root = base.explain_json(cur, sql, params)
            all_total_ms.append(ext_ms + sql_ms)
            all_hits.append(hits)
            all_reads.append(reads)

            if run_idx == 0 and qi == 0:
                cur.execute(sql, params)
                rowcount = len(cur.fetchall())

    return (
        statistics.median(all_total_ms),
        statistics.fmean(all_total_ms),
        statistics.fmean(all_hits),
        statistics.fmean(all_reads),
        root or "Limit",
        rowcount,
    )


def verify_helper_filtered_2hop_rerank_equivalence(
    cur, table_name: str, queries: list[tuple[int, int, str]], ann_k: int, top_k: int
) -> None:
    sql = f"""
    WITH ann AS MATERIALIZED (
        SELECT target_id
        FROM {table_name}
        ORDER BY embedding <=> %s::svec
        LIMIT {ann_k}
    ),
    seeds AS MATERIALIZED (
        SELECT DISTINCT target_id FROM ann
    ),
    helper AS (
        WITH hop1 AS MATERIALIZED (
            SELECT DISTINCT target_id
            FROM sorted_heap_expand_ids('{table_name}'::regclass, ARRAY(SELECT target_id FROM seeds), %s::int4, 0)
        )
        SELECT entity_id, relation_id, target_id, round(distance::numeric, 6) AS distance
        FROM sorted_heap_expand_rerank('{table_name}'::regclass, ARRAY(SELECT target_id FROM hop1), %s::svec, {top_k}, %s::int4, 0)
    ),
    hop1_sql AS MATERIALIZED (
        SELECT DISTINCT target_id
        FROM {table_name}
        WHERE entity_id = ANY (ARRAY(SELECT target_id FROM seeds))
          AND relation_id = %s
    ),
    expanded AS MATERIALIZED (
        SELECT *
        FROM {table_name}
        WHERE entity_id = ANY (ARRAY(SELECT target_id FROM hop1_sql))
          AND relation_id = %s
    ),
    sql_baseline AS (
        SELECT entity_id, relation_id, target_id,
               round((embedding <=> %s::svec)::numeric, 6) AS distance
        FROM expanded
        ORDER BY embedding <=> %s::svec, entity_id, relation_id, target_id
        LIMIT {top_k}
    )
    SELECT count(*) FROM (
        (SELECT * FROM helper EXCEPT ALL SELECT * FROM sql_baseline)
        UNION ALL
        (SELECT * FROM sql_baseline EXCEPT ALL SELECT * FROM helper)
    ) diff
    """

    for idx, query in enumerate(queries, start=1):
        cur.execute(
            sql,
            (query[2], query[1], query[2], query[1], query[1], query[1], query[2], query[2]),
        )
        diff_rows = cur.fetchone()[0]
        if diff_rows != 0:
            raise RuntimeError(
                f"sorted_heap_expand_rerank(2hop filtered) mismatch on {table_name} query#{idx}: diff_rows={diff_rows}"
            )


def verify_helper_filtered_2hop_fused_equivalence(
    cur, table_name: str, queries: list[tuple[int, int, str]], ann_k: int, top_k: int
) -> None:
    sql = f"""
    WITH ann AS MATERIALIZED (
        SELECT target_id
        FROM {table_name}
        ORDER BY embedding <=> %s::svec
        LIMIT {ann_k}
    ),
    seeds AS MATERIALIZED (
        SELECT DISTINCT target_id FROM ann
    ),
    helper AS (
        SELECT entity_id, relation_id, target_id, round(distance::numeric, 6) AS distance
        FROM sorted_heap_expand_twohop_rerank('{table_name}'::regclass, ARRAY(SELECT target_id FROM seeds), %s::svec, {top_k}, %s::int4, %s::int4, 0)
    ),
    hop1_sql AS MATERIALIZED (
        SELECT DISTINCT target_id
        FROM {table_name}
        WHERE entity_id = ANY (ARRAY(SELECT target_id FROM seeds))
          AND relation_id = %s
    ),
    expanded AS MATERIALIZED (
        SELECT *
        FROM {table_name}
        WHERE entity_id = ANY (ARRAY(SELECT target_id FROM hop1_sql))
          AND relation_id = %s
    ),
    sql_baseline AS (
        SELECT entity_id, relation_id, target_id,
               round((embedding <=> %s::svec)::numeric, 6) AS distance
        FROM expanded
        ORDER BY embedding <=> %s::svec, entity_id, relation_id, target_id
        LIMIT {top_k}
    )
    SELECT count(*) FROM (
        (SELECT * FROM helper EXCEPT ALL SELECT * FROM sql_baseline)
        UNION ALL
        (SELECT * FROM sql_baseline EXCEPT ALL SELECT * FROM helper)
    ) diff
    """

    for idx, query in enumerate(queries, start=1):
        cur.execute(
            sql,
            (query[2], query[2], query[1], query[1], query[1], query[1], query[2], query[2]),
        )
        diff_rows = cur.fetchone()[0]
        if diff_rows != 0:
            raise RuntimeError(
                f"sorted_heap_expand_twohop_rerank mismatch on {table_name} query#{idx}: diff_rows={diff_rows}"
            )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gutenberg-path", default=str(Path.home() / "Projects/ML/cogniversum_v2/gutenberg_cache"))
    ap.add_argument("--tmp-root", default="/tmp")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--max-books", type=int, default=64)
    ap.add_argument("--max-paragraphs", type=int, default=128)
    ap.add_argument("--skip-paragraphs", type=int, default=8)
    ap.add_argument("--dim", type=int, default=32)
    ap.add_argument("--query-count", type=int, default=16)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--ann-k", type=int, default=32)
    ap.add_argument("--top-k", type=int, default=10)
    ap.add_argument("--ef-search", type=int, default=32)
    ap.add_argument("--ef-construction", type=int, default=64)
    ap.add_argument("--pgv-ef-search", type=int, default=64)
    ap.add_argument("--skip-pgvector", action="store_true")
    ap.add_argument("--zvec-ef", type=int, default=64)
    ap.add_argument("--zvec-memory-limit-mb", type=int, default=8192)
    ap.add_argument("--skip-zvec", action="store_true")
    ap.add_argument("--qdrant-ef", type=int, default=64)
    ap.add_argument("--skip-qdrant", action="store_true")
    ap.add_argument("--shared-buffers-mb", type=int, default=64)
    ap.add_argument("--backend-mode", choices=("fresh", "reuse"), default="fresh")
    ap.add_argument("--install-cmd", default="")
    ap.add_argument("--keep-temp", action="store_true")
    args = ap.parse_args()

    gutenberg_path = Path(args.gutenberg_path).expanduser().resolve()
    if not gutenberg_path.is_dir():
        raise SystemExit(f"missing gutenberg path: {gutenberg_path}")

    root_dir = Path(__file__).resolve().parent.parent
    tmp_root = Path(args.tmp_root).resolve()
    port = args.port or base.pick_port()
    install_cmd = shlex.split(args.install_cmd) if args.install_cmd else None
    tmp, pg_bindir = base.init_temp_cluster(root_dir, port, tmp_root, args.shared_buffers_mb, install_cmd)
    csv_path = tmp / "facts_gutenberg.csv"
    zvec_dir: str | None = None
    qdrant_client: QdrantClient | None = None
    qdrant_started = False
    qdrant_name: str | None = None

    try:
        rows, book_count, contains_rows, next_rows = generate_csv_from_gutenberg(
            csv_path,
            gutenberg_path,
            args.max_books,
            args.max_paragraphs,
            args.skip_paragraphs,
            args.dim,
        )
        conn = base.connect(tmp, port)
        cur = conn.cursor()
        try:
            cur.execute("SET jit = off")
            cur.execute("SET sorted_hnsw.shared_cache = off")
            cur.execute(f"SET sorted_hnsw.ef_search = {args.ef_search}")
            base.bootstrap_schema(cur, args.dim)
            base.load_data(cur, csv_path)
            base.build_indexes(cur, args.ef_construction)
            if not args.skip_pgvector:
                bootstrap_pgvector(cur, csv_path, args.dim, args.ef_construction)
            zvec_coll = None
            zvec_target_map: dict[str, int] = {}
            if not args.skip_zvec:
                zvec_coll, zvec_target_map, zvec_dir = build_zvec_collection(
                    csv_path,
                    args.dim,
                    args.ef_construction,
                    args.zvec_memory_limit_mb,
                )
            qdrant_target_map: dict[int, int] = {}
            if not args.skip_qdrant:
                qdrant_client, qdrant_started, qdrant_name, qdrant_target_map = build_qdrant_collection(
                    csv_path,
                    args.dim,
                    args.ef_construction,
                )
            queries = load_relation_queries(cur, args.query_count, 2)
            if len(queries) < args.query_count:
                raise RuntimeError(f"not enough relation=2 query rows: got {len(queries)}")

            cases = [
                base.QueryCase(
                    "seed_expand_rel_in",
                    f"""
                    WITH ann AS MATERIALIZED (
                        SELECT target_id
                        FROM {{table}}
                        ORDER BY embedding <=> %s::svec
                        LIMIT {args.ann_k}
                    ),
                    seeds AS MATERIALIZED (
                        SELECT DISTINCT target_id FROM ann
                    )
                    SELECT *
                    FROM {{table}}
                    WHERE entity_id = ANY (ARRAY(SELECT target_id FROM seeds))
                      AND relation_id = %s
                    """,
                    lambda q: (q[2], q[1]),
                ),
                base.QueryCase(
                    "seed_expand_rerank_rel_in",
                    f"""
                    WITH ann AS MATERIALIZED (
                        SELECT target_id
                        FROM {{table}}
                        ORDER BY embedding <=> %s::svec
                        LIMIT {args.ann_k}
                    ),
                    seeds AS MATERIALIZED (
                        SELECT DISTINCT target_id FROM ann
                    ),
                    expanded AS MATERIALIZED (
                        SELECT *
                        FROM {{table}}
                        WHERE entity_id = ANY (ARRAY(SELECT target_id FROM seeds))
                          AND relation_id = %s
                    )
                    SELECT *
                    FROM expanded
                    ORDER BY embedding <=> %s::svec
                    LIMIT {args.top_k}
                    """,
                    lambda q: (q[2], q[1], q[2]),
                ),
                base.QueryCase(
                    "seed_expand2_rerank_rel_in",
                    f"""
                    WITH ann AS MATERIALIZED (
                        SELECT target_id
                        FROM {{table}}
                        ORDER BY embedding <=> %s::svec
                        LIMIT {args.ann_k}
                    ),
                    seeds AS MATERIALIZED (
                        SELECT DISTINCT target_id FROM ann
                    ),
                    hop1 AS MATERIALIZED (
                        SELECT DISTINCT target_id
                        FROM {{table}}
                        WHERE entity_id = ANY (ARRAY(SELECT target_id FROM seeds))
                          AND relation_id = %s
                    ),
                    expanded AS MATERIALIZED (
                        SELECT *
                        FROM {{table}}
                        WHERE entity_id = ANY (ARRAY(SELECT target_id FROM hop1))
                          AND relation_id = %s
                    )
                    SELECT *
                    FROM expanded
                    ORDER BY embedding <=> %s::svec
                    LIMIT {args.top_k}
                    """,
                    lambda q: (q[2], q[1], q[1], q[2]),
                ),
            ]
            helper_cases = [
                base.QueryCase(
                    "seed_expand_rel_fn",
                    f"""
                    WITH ann AS MATERIALIZED (
                        SELECT target_id
                        FROM {{table}}
                        ORDER BY embedding <=> %s::svec
                        LIMIT {args.ann_k}
                    ),
                    seeds AS MATERIALIZED (
                        SELECT DISTINCT target_id FROM ann
                    )
                    SELECT *
                    FROM sorted_heap_expand_ids('{{table}}'::regclass, ARRAY(SELECT target_id FROM seeds), %s::int4, 0)
                    """,
                    lambda q: (q[2], q[1]),
                ),
                base.QueryCase(
                    "seed_expand_rerank_rel_topk_fn",
                    f"""
                    WITH ann AS MATERIALIZED (
                        SELECT target_id
                        FROM {{table}}
                        ORDER BY embedding <=> %s::svec
                        LIMIT {args.ann_k}
                    ),
                    seeds AS MATERIALIZED (
                        SELECT DISTINCT target_id FROM ann
                    )
                    SELECT *
                    FROM sorted_heap_expand_rerank('{{table}}'::regclass, ARRAY(SELECT target_id FROM seeds), %s::svec, {args.top_k}, %s::int4, 0)
                    """,
                    lambda q: (q[2], q[2], q[1]),
                ),
                base.QueryCase(
                    "seed_graph_rag_rel_scan_fn",
                    f"""
                    SELECT *
                    FROM sorted_heap_graph_rag_scan('{{table}}'::regclass, %s::svec, {args.ann_k}, {args.top_k}, %s::int4, 0)
                    """,
                    lambda q: (q[2], q[1]),
                ),
                base.QueryCase(
                    "seed_expand2_rerank_rel_topk_fn",
                    f"""
                    WITH ann AS MATERIALIZED (
                        SELECT target_id
                        FROM {{table}}
                        ORDER BY embedding <=> %s::svec
                        LIMIT {args.ann_k}
                    ),
                    seeds AS MATERIALIZED (
                        SELECT DISTINCT target_id FROM ann
                    ),
                    hop1 AS MATERIALIZED (
                        SELECT DISTINCT target_id
                        FROM sorted_heap_expand_ids('{{table}}'::regclass, ARRAY(SELECT target_id FROM seeds), %s::int4, 0)
                    )
                    SELECT *
                    FROM sorted_heap_expand_rerank('{{table}}'::regclass, ARRAY(SELECT target_id FROM hop1), %s::svec, {args.top_k}, %s::int4, 0)
                    """,
                    lambda q: (q[2], q[1], q[2], q[1]),
                ),
                base.QueryCase(
                    "seed_expand2_rerank_rel_twohop_fn",
                    f"""
                    WITH ann AS MATERIALIZED (
                        SELECT target_id
                        FROM {{table}}
                        ORDER BY embedding <=> %s::svec
                        LIMIT {args.ann_k}
                    ),
                    seeds AS MATERIALIZED (
                        SELECT DISTINCT target_id FROM ann
                    )
                    SELECT *
                    FROM sorted_heap_expand_twohop_rerank('{{table}}'::regclass, ARRAY(SELECT target_id FROM seeds), %s::svec, {args.top_k}, %s::int4, %s::int4, 0)
                    """,
                    lambda q: (q[2], q[2], q[1], q[1]),
                ),
            ]
            pgvector_cases = [
                base.QueryCase(
                    "seed_expand_rel_pgv",
                    f"""
                    WITH ann AS MATERIALIZED (
                        SELECT target_id
                        FROM facts_pgv
                        ORDER BY embedding <=> %s::vector({args.dim})
                        LIMIT {args.ann_k}
                    ),
                    seeds AS MATERIALIZED (
                        SELECT DISTINCT target_id FROM ann
                    )
                    SELECT *
                    FROM facts_heap
                    WHERE entity_id = ANY (ARRAY(SELECT target_id FROM seeds))
                      AND relation_id = %s
                    """,
                    lambda q: (q[2], q[1]),
                ),
                base.QueryCase(
                    "seed_expand_rerank_rel_pgv",
                    f"""
                    WITH ann AS MATERIALIZED (
                        SELECT target_id
                        FROM facts_pgv
                        ORDER BY embedding <=> %s::vector({args.dim})
                        LIMIT {args.ann_k}
                    ),
                    seeds AS MATERIALIZED (
                        SELECT DISTINCT target_id FROM ann
                    ),
                    expanded AS MATERIALIZED (
                        SELECT *
                        FROM facts_heap
                        WHERE entity_id = ANY (ARRAY(SELECT target_id FROM seeds))
                          AND relation_id = %s
                    )
                    SELECT *
                    FROM expanded
                    ORDER BY embedding <=> %s::svec
                    LIMIT {args.top_k}
                    """,
                    lambda q: (q[2], q[1], q[2]),
                ),
                base.QueryCase(
                    "seed_expand2_rerank_rel_pgv",
                    f"""
                    WITH ann AS MATERIALIZED (
                        SELECT target_id
                        FROM facts_pgv
                        ORDER BY embedding <=> %s::vector({args.dim})
                        LIMIT {args.ann_k}
                    ),
                    seeds AS MATERIALIZED (
                        SELECT DISTINCT target_id FROM ann
                    ),
                    hop1 AS MATERIALIZED (
                        SELECT DISTINCT target_id
                        FROM facts_heap
                        WHERE entity_id = ANY (ARRAY(SELECT target_id FROM seeds))
                          AND relation_id = %s
                    ),
                    expanded AS MATERIALIZED (
                        SELECT *
                        FROM facts_heap
                        WHERE entity_id = ANY (ARRAY(SELECT target_id FROM hop1))
                          AND relation_id = %s
                    )
                    SELECT *
                    FROM expanded
                    ORDER BY embedding <=> %s::svec
                    LIMIT {args.top_k}
                    """,
                    lambda q: (q[2], q[1], q[1], q[2]),
                ),
            ]

            print("============================================================")
            print("graph rag Gutenberg benchmark")
            print("============================================================")
            print(f"port:             {port}")
            print(f"gutenberg_path:   {gutenberg_path}")
            print(f"books:            {book_count}")
            print(f"max_paragraphs:   {args.max_paragraphs}")
            print(f"skip_paragraphs:  {args.skip_paragraphs}")
            print(f"dim:              {args.dim}")
            print(f"rows:             {rows}")
            print(f"contains_rows:    {contains_rows}")
            print(f"next_rows:        {next_rows}")
            print(f"query_count:      {args.query_count}")
            print(f"runs:             {args.runs}")
            print(f"ann_k:            {args.ann_k}")
            print(f"top_k:            {args.top_k}")
            print(f"ef_search:        {args.ef_search}")
            print(f"ef_construction:  {args.ef_construction}")
            print(f"pgv_ef_search:    {args.pgv_ef_search}")
            print(f"pgvector:         {'off' if args.skip_pgvector else 'on'}")
            print(f"zvec_ef:          {args.zvec_ef}")
            print(f"zvec:             {'off' if args.skip_zvec else 'on'}")
            print(f"qdrant_ef:        {args.qdrant_ef}")
            print(f"qdrant:           {'off' if args.skip_qdrant else 'on'}")
            print(f"shared_buffers:   {args.shared_buffers_mb}MB")
            print(f"backend_mode:     {args.backend_mode}")
            print()

            if args.backend_mode == "fresh":
                cur.close()
                conn.close()
                conn = base.connect(tmp, port)
                cur = conn.cursor()

            cur.execute("SET jit = off")
            cur.execute("SET sorted_hnsw.shared_cache = off")
            cur.execute(f"SET sorted_hnsw.ef_search = {args.ef_search}")
            base.verify_helper_filtered_equivalence(cur, "facts_sh", queries, args.ann_k)
            base.verify_helper_filtered_rerank_equivalence(cur, "facts_sh", queries, args.ann_k, args.top_k)
            base.verify_graph_rag_scan_filtered_equivalence(cur, "facts_sh", queries, args.ann_k, args.top_k)
            verify_helper_filtered_2hop_rerank_equivalence(cur, "facts_sh", queries, args.ann_k, args.top_k)
            verify_helper_filtered_2hop_fused_equivalence(cur, "facts_sh", queries, args.ann_k, args.top_k)

            for table in ("facts_heap", "facts_sh"):
                for case in cases:
                    print(f"running|table={table}|case={case.name}", flush=True)
                    p50, avg, hits, reads, root, rowcount = base.measure_case(cur, table, case, queries, args.runs)
                    base.print_result(table, case.name, p50, avg, hits, reads, root, rowcount)
                if table == "facts_sh":
                    for case in helper_cases:
                        print(f"running|table={table}|case={case.name}", flush=True)
                        p50, avg, hits, reads, root, rowcount = base.measure_case(cur, table, case, queries, args.runs)
                        base.print_result(table, case.name, p50, avg, hits, reads, root, rowcount)

            sql_expand = """
                WITH seeds AS MATERIALIZED (
                    SELECT DISTINCT unnest(%s::int4[]) AS target_id
                )
                SELECT *
                FROM facts_heap
                WHERE entity_id = ANY (ARRAY(SELECT target_id FROM seeds))
                  AND relation_id = %s
            """
            sql_rerank = f"""
                WITH seeds AS MATERIALIZED (
                    SELECT DISTINCT unnest(%s::int4[]) AS target_id
                ),
                expanded AS MATERIALIZED (
                    SELECT *
                    FROM facts_heap
                    WHERE entity_id = ANY (ARRAY(SELECT target_id FROM seeds))
                      AND relation_id = %s
                )
                SELECT *
                FROM expanded
                ORDER BY embedding <=> %s::svec
                LIMIT {args.top_k}
            """
            sql_rerank_2hop = f"""
                WITH seeds AS MATERIALIZED (
                    SELECT DISTINCT unnest(%s::int4[]) AS target_id
                ),
                hop1 AS MATERIALIZED (
                    SELECT DISTINCT target_id
                    FROM facts_heap
                    WHERE entity_id = ANY (ARRAY(SELECT target_id FROM seeds))
                      AND relation_id = %s
                ),
                expanded AS MATERIALIZED (
                    SELECT *
                    FROM facts_heap
                    WHERE entity_id = ANY (ARRAY(SELECT target_id FROM hop1))
                      AND relation_id = %s
                )
                SELECT *
                FROM expanded
                ORDER BY embedding <=> %s::svec
                LIMIT {args.top_k}
            """

            if not args.skip_pgvector:
                cur.execute(f"SET hnsw.ef_search = {args.pgv_ef_search}")
                for case in pgvector_cases:
                    print(f"running|table=facts_pgv|case={case.name}", flush=True)
                    p50, avg, hits, reads, root, rowcount = base.measure_case(cur, "facts_pgv", case, queries, args.runs)
                    base.print_result("facts_pgv", case.name, p50, avg, hits, reads, root, rowcount)

            if not args.skip_zvec and zvec_coll is not None:
                param = zvec.HnswQueryParam(ef=args.zvec_ef)

                def zvec_seed_fn(qvec: list[float]) -> list[int]:
                    res = zvec_coll.query(zvec.VectorQuery("embedding", vector=qvec, param=param), topk=args.ann_k)
                    return map_zvec_results_to_targets(res, zvec_target_map)

                print("running|table=facts_zvec|case=seed_expand_rel_zvec", flush=True)
                p50, avg, hits, reads, root, rowcount = measure_external_graph_rag_case(
                    cur,
                    "seed_expand_rel_zvec",
                    queries,
                    args.runs,
                    zvec_seed_fn,
                    sql_expand,
                    lambda q, seeds: (sql_expand, (seeds, q[1])),
                )
                base.print_result("facts_zvec", "seed_expand_rel_zvec", p50, avg, hits, reads, root, rowcount)

                print("running|table=facts_zvec|case=seed_expand_rerank_rel_zvec", flush=True)
                p50, avg, hits, reads, root, rowcount = measure_external_graph_rag_case(
                    cur,
                    "seed_expand_rerank_rel_zvec",
                    queries,
                    args.runs,
                    zvec_seed_fn,
                    sql_rerank,
                    lambda q, seeds: (sql_rerank, (seeds, q[1], q[2])),
                )
                base.print_result("facts_zvec", "seed_expand_rerank_rel_zvec", p50, avg, hits, reads, root, rowcount)

                print("running|table=facts_zvec|case=seed_expand2_rerank_rel_zvec", flush=True)
                p50, avg, hits, reads, root, rowcount = measure_external_graph_rag_case(
                    cur,
                    "seed_expand2_rerank_rel_zvec",
                    queries,
                    args.runs,
                    zvec_seed_fn,
                    sql_rerank_2hop,
                    lambda q, seeds: (sql_rerank_2hop, (seeds, q[1], q[1], q[2])),
                )
                base.print_result("facts_zvec", "seed_expand2_rerank_rel_zvec", p50, avg, hits, reads, root, rowcount)

            if not args.skip_qdrant and qdrant_client is not None and qdrant_name is not None:
                params = SearchParams(hnsw_ef=args.qdrant_ef, exact=False)

                def qdrant_seed_fn(qvec: list[float]) -> list[int]:
                    res = qdrant_client.query_points(
                        collection_name=qdrant_name,
                        query=qvec,
                        limit=args.ann_k,
                        search_params=params,
                    ).points
                    return unique_ints_in_order([qdrant_target_map[int(p.id)] for p in res])

                print("running|table=facts_qdrant|case=seed_expand_rel_qdrant", flush=True)
                p50, avg, hits, reads, root, rowcount = measure_external_graph_rag_case(
                    cur,
                    "seed_expand_rel_qdrant",
                    queries,
                    args.runs,
                    qdrant_seed_fn,
                    sql_expand,
                    lambda q, seeds: (sql_expand, (seeds, q[1])),
                )
                base.print_result("facts_qdrant", "seed_expand_rel_qdrant", p50, avg, hits, reads, root, rowcount)

                print("running|table=facts_qdrant|case=seed_expand_rerank_rel_qdrant", flush=True)
                p50, avg, hits, reads, root, rowcount = measure_external_graph_rag_case(
                    cur,
                    "seed_expand_rerank_rel_qdrant",
                    queries,
                    args.runs,
                    qdrant_seed_fn,
                    sql_rerank,
                    lambda q, seeds: (sql_rerank, (seeds, q[1], q[2])),
                )
                base.print_result("facts_qdrant", "seed_expand_rerank_rel_qdrant", p50, avg, hits, reads, root, rowcount)

                print("running|table=facts_qdrant|case=seed_expand2_rerank_rel_qdrant", flush=True)
                p50, avg, hits, reads, root, rowcount = measure_external_graph_rag_case(
                    cur,
                    "seed_expand2_rerank_rel_qdrant",
                    queries,
                    args.runs,
                    qdrant_seed_fn,
                    sql_rerank_2hop,
                    lambda q, seeds: (sql_rerank_2hop, (seeds, q[1], q[1], q[2])),
                )
                base.print_result("facts_qdrant", "seed_expand2_rerank_rel_qdrant", p50, avg, hits, reads, root, rowcount)
        finally:
            cur.close()
            conn.close()
        return 0
    finally:
        if qdrant_client is not None and qdrant_name is not None:
            try:
                qdrant_client.delete_collection(qdrant_name)
            except Exception:
                pass
        if qdrant_started:
            subprocess.run(["docker", "stop", "qdrant-bench"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if zvec_dir is not None:
            shutil.rmtree(zvec_dir, ignore_errors=True)
        if args.keep_temp:
            print(f"keep_temp:        {tmp}", flush=True)
        else:
            base.stop_temp_cluster(tmp, pg_bindir)


if __name__ == "__main__":
    raise SystemExit(main())
