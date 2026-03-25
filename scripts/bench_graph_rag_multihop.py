#!/usr/bin/env python3
"""
Benchmark GraphRAG on a cogniformerus-like multihop fact graph.

Dataset shape:
  - relation 1: person -> parent
  - relation 2: parent -> city

Queries:
  - "Where does Person_i's parent live?"

This is intentionally closer to the current cogniformerus multihop benchmark
than the paragraph-adjacency Gutenberg graph. It measures both latency and
retrieval quality (hit@1 / hit@k for the expected city fact).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import math
import random
import re
import shlex
import shutil
import statistics
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import bench_graph_rag as base
import bench_graph_rag_gutenberg as gut

REL_PARENT = 1
REL_CITY = 2
ENTITY_TOKEN_RE = re.compile(r"^(person|parent|city)_\d+$")
STOP_TOKENS = {"where", "does", "the", "a", "an", "of", "in"}


@dataclass(frozen=True)
class MultiHopQuery:
    person_id: int
    parent_id: int
    city_id: int
    query_text: str
    query_vec: str


def normalized_tokens(text: str) -> list[str]:
    out: list[str] = []
    for token in gut.tokenize(text):
        token = token.lower()
        if token.endswith("'s"):
            token = token[:-2]
        if token:
            out.append(token)
    return out


def token_weight(token: str) -> float:
    if token in STOP_TOKENS:
        return 0.0
    if ENTITY_TOKEN_RE.match(token):
        return 8.0
    if token in {"parent", "lives", "live", "city"}:
        return 2.5
    return 1.0


def lexical_hash_vector(text: str, dim: int) -> str:
    acc = [0.0] * dim
    tokens = normalized_tokens(text)
    if not tokens:
        tokens = [text.lower() or "_"]

    for token in tokens[:256]:
        weight = token_weight(token)
        if weight == 0.0:
            continue
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        block = digest
        for i in range(dim):
            if i > 0 and i % 256 == 0:
                block = hashlib.sha256(block).digest()
            bit = (block[(i // 8) % len(block)] >> (i % 8)) & 1
            acc[i] += weight if bit else -weight

    norm = math.sqrt(sum(v * v for v in acc))
    if norm < 1e-8:
        vals = [0.0] * dim
    else:
        vals = [v / norm for v in acc]
    return "[" + ",".join(f"{v:.6f}" for v in vals) + "]"


def generate_csv(path: Path, num_pairs: int, dim: int, seed: int) -> dict[int, str]:
    rng = random.Random(seed)
    city_names: dict[int, str] = {}

    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for i in range(1, num_pairs + 1):
            person_id = i
            parent_id = num_pairs + i
            city_id = (2 * num_pairs) + i

            person = f"Person_{i}"
            parent = f"Parent_{i}"
            city = f"City_{rng.randrange(1_000_000)}"
            city_names[city_id] = city

            fact_parent = f"{person} has parent {parent}."
            fact_city = f"{parent} lives in {city}."

            w.writerow(
                [
                    person_id,
                    REL_PARENT,
                    parent_id,
                    lexical_hash_vector(fact_parent, dim),
                    fact_parent,
                ]
            )
            w.writerow(
                [
                    parent_id,
                    REL_CITY,
                    city_id,
                    lexical_hash_vector(fact_city, dim),
                    fact_city,
                ]
            )

    return city_names


def build_queries(num_pairs: int, query_count: int, dim: int, seed: int) -> list[MultiHopQuery]:
    if query_count > num_pairs:
        raise ValueError(f"query_count {query_count} exceeds num_pairs {num_pairs}")

    rng = random.Random(seed ^ 0x5A17)
    sample = rng.sample(range(1, num_pairs + 1), query_count)
    queries: list[MultiHopQuery] = []

    for i in sample:
        person = f"Person_{i}"
        query_text = f"Where does the parent of {person} live?"
        queries.append(
            MultiHopQuery(
                person_id=i,
                parent_id=num_pairs + i,
                city_id=(2 * num_pairs) + i,
                query_text=query_text,
                query_vec=lexical_hash_vector(query_text, dim),
            )
        )

    return queries


def load_queries_from_db(cur, query_count: int, num_pairs: int, dim: int, seed: int) -> list[MultiHopQuery]:
    queries = build_queries(num_pairs, query_count, dim, seed)

    # Sanity-check that expected city facts exist in the loaded dataset.
    cur.execute(
        """
        SELECT count(*)
        FROM facts_heap
        WHERE relation_id = %s
          AND entity_id BETWEEN %s AND %s
        """,
        (REL_CITY, num_pairs + 1, 2 * num_pairs),
    )
    if cur.fetchone()[0] != num_pairs:
        raise RuntimeError("unexpected multihop dataset shape in facts_heap")

    return queries


def verify_helper_twohop_equivalence(cur, table_name: str, queries: list[MultiHopQuery], ann_k: int, top_k: int) -> None:
    sql = f"""
    WITH ann AS MATERIALIZED (
        SELECT entity_id
        FROM {table_name}
        ORDER BY embedding <=> %s::svec
        LIMIT {ann_k}
    ),
    seeds AS MATERIALIZED (
        SELECT DISTINCT entity_id FROM ann
    ),
    helper AS (
        SELECT entity_id, relation_id, target_id, round(distance::numeric, 6) AS distance
        FROM sorted_heap_expand_twohop_rerank(
            '{table_name}'::regclass,
            ARRAY(SELECT entity_id FROM seeds),
            %s::svec,
            {top_k},
            {REL_PARENT},
            {REL_CITY},
            0
        )
    ),
    hop1_sql AS MATERIALIZED (
        SELECT DISTINCT target_id
        FROM {table_name}
        WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
          AND relation_id = {REL_PARENT}
    ),
    expanded AS MATERIALIZED (
        SELECT *
        FROM {table_name}
        WHERE entity_id = ANY (ARRAY(SELECT target_id FROM hop1_sql))
          AND relation_id = {REL_CITY}
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
        cur.execute(sql, (query.query_vec, query.query_vec, query.query_vec, query.query_vec))
        diff_rows = cur.fetchone()[0]
        if diff_rows != 0:
            raise RuntimeError(
                f"sorted_heap_expand_twohop_rerank mismatch on {table_name} query#{idx}: diff_rows={diff_rows}"
            )


def verify_graph_rag_twohop_scan_equivalence(cur, table_name: str, queries: list[MultiHopQuery], ann_k: int, top_k: int) -> None:
    sql = f"""
    WITH helper AS (
        SELECT entity_id, relation_id, target_id, round(distance::numeric, 6) AS distance
        FROM sorted_heap_graph_rag_twohop_scan(
            '{table_name}'::regclass,
            %s::svec,
            {ann_k},
            {top_k},
            {REL_PARENT},
            {REL_CITY},
            0
        )
    ),
    ann AS MATERIALIZED (
        SELECT entity_id
        FROM {table_name}
        ORDER BY embedding <=> %s::svec
        LIMIT {ann_k}
    ),
    seeds AS MATERIALIZED (
        SELECT DISTINCT entity_id FROM ann
    ),
    hop1_sql AS MATERIALIZED (
        SELECT DISTINCT target_id
        FROM {table_name}
        WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
          AND relation_id = {REL_PARENT}
    ),
    expanded AS MATERIALIZED (
        SELECT *
        FROM {table_name}
        WHERE entity_id = ANY (ARRAY(SELECT target_id FROM hop1_sql))
          AND relation_id = {REL_CITY}
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
        cur.execute(sql, (query.query_vec, query.query_vec, query.query_vec, query.query_vec))
        diff_rows = cur.fetchone()[0]
        if diff_rows != 0:
            raise RuntimeError(
                f"sorted_heap_graph_rag_twohop_scan mismatch on {table_name} query#{idx}: diff_rows={diff_rows}"
            )


def measure_quality(cur, table_name: str, case: base.QueryCase, queries: list[MultiHopQuery]) -> tuple[float, float, float]:
    sql = case.sql_template.format(table=table_name)
    hit1 = 0
    hitk = 0
    total_rows = 0

    for query in queries:
        params = case.params_builder(query)
        cur.execute(sql, params)
        rows = cur.fetchall()
        total_rows += len(rows)
        targets = [int(row[2]) for row in rows]
        if targets and targets[0] == query.city_id:
            hit1 += 1
        if query.city_id in targets:
            hitk += 1

    n = len(queries)
    return (
        (hit1 * 100.0) / n,
        (hitk * 100.0) / n,
        total_rows / n if n else 0.0,
    )


def measure_external_quality(cur, queries: list[MultiHopQuery], seed_fn, sql_params_fn) -> tuple[float, float, float]:
    hit1 = 0
    hitk = 0
    total_rows = 0

    for query in queries:
        qvec = gut.vector_list_from_literal(query.query_vec)
        seeds = seed_fn(qvec)
        sql, params = sql_params_fn(query, seeds)
        cur.execute(sql, params)
        rows = cur.fetchall()
        total_rows += len(rows)
        targets = [int(row[2]) for row in rows]
        if targets and targets[0] == query.city_id:
            hit1 += 1
        if query.city_id in targets:
            hitk += 1

    n = len(queries)
    return (
        (hit1 * 100.0) / n,
        (hitk * 100.0) / n,
        total_rows / n if n else 0.0,
    )


def measure_external_case(cur, queries: list[MultiHopQuery], runs: int, seed_fn, sql_params_fn) -> tuple[float, float, float, float, str, int]:
    all_total_ms: list[float] = []
    all_hits: list[int] = []
    all_reads: list[int] = []
    root = ""
    rowcount = 0

    for run_idx in range(runs):
        for qi, query in enumerate(queries):
            qvec = gut.vector_list_from_literal(query.query_vec)
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


def build_exact_seed_fn(cur, ann_k: int, dim: int):
    sql = f"""
        SELECT entity_id
        FROM facts_heap
        ORDER BY embedding <=> %s::svec
        LIMIT {ann_k}
    """

    def exact_seed_fn(qvec: list[float]) -> list[int]:
        lit = "[" + ",".join(f"{v:.6f}" for v in qvec[:dim]) + "]"
        cur.execute(sql, (lit,))
        return gut.unique_ints_in_order([int(row[0]) for row in cur.fetchall()])

    exact_seed_fn.expects_vector_list = True
    return exact_seed_fn


def build_ann_seed_fn(cur, ann_k: int, table_name: str):
    sql = f"""
        SELECT entity_id
        FROM {table_name}
        ORDER BY embedding <=> %s::svec
        LIMIT {ann_k}
    """

    def ann_seed_fn(qvec_literal: str) -> list[int]:
        cur.execute(sql, (qvec_literal,))
        return gut.unique_ints_in_order([int(row[0]) for row in cur.fetchall()])

    ann_seed_fn.expects_vector_list = False
    return ann_seed_fn


def measure_seed_diagnostics(cur, queries: list[MultiHopQuery], seed_fn, ann_k: int) -> tuple[float, float, float]:
    seed_person_hits = 0
    expanded_city_hits = 0
    rank_sum = 0.0
    rank_count = 0

    expanded_sql = f"""
        WITH seeds AS MATERIALIZED (
            SELECT DISTINCT unnest(%s::int4[]) AS entity_id
        ),
        hop1 AS MATERIALIZED (
            SELECT DISTINCT target_id
            FROM facts_heap
            WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
              AND relation_id = {REL_PARENT}
        )
        SELECT EXISTS (
            SELECT 1
            FROM facts_heap
            WHERE entity_id = ANY (ARRAY(SELECT target_id FROM hop1))
              AND relation_id = {REL_CITY}
              AND target_id = %s
        )
    """

    for query in queries:
        seed_arg = query.query_vec
        if getattr(seed_fn, "expects_vector_list", False):
            seed_arg = gut.vector_list_from_literal(query.query_vec)
        seeds = seed_fn(seed_arg)
        if query.person_id in seeds:
            seed_person_hits += 1
            rank_sum += seeds.index(query.person_id) + 1
            rank_count += 1

        cur.execute(expanded_sql, (seeds, query.city_id))
        if bool(cur.fetchone()[0]):
            expanded_city_hits += 1

    n = len(queries)
    return (
        (seed_person_hits * 100.0) / n if n else 0.0,
        (expanded_city_hits * 100.0) / n if n else 0.0,
        (rank_sum / rank_count) if rank_count else float(ann_k + 1),
    )


def build_zvec_collection_entity_seed(
    csv_path: Path,
    dim: int,
    ef_construction: int,
    memory_limit_mb: int,
) -> tuple[object, dict[str, int], str]:
    gut.zvec.init(memory_limit_mb=memory_limit_mb)
    base_dir = Path(tempfile.mkdtemp(prefix="graph_rag_multihop_zvec_", dir="/tmp"))
    path = str(base_dir / "bench")
    schema = gut.zvec.CollectionSchema(
        name="bench",
        vectors=gut.zvec.VectorSchema(
            name="embedding",
            data_type=gut.zvec.DataType.VECTOR_FP32,
            dimension=dim,
            index_param=gut.zvec.HnswIndexParam(
                metric_type=gut.zvec.MetricType.COSINE,
                m=16,
                ef_construction=ef_construction,
            ),
        ),
    )
    coll = gut.zvec.create_and_open(path, schema)
    id_to_entity: dict[str, int] = {}
    docs: list = []
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        for idx, row in enumerate(csv.reader(f), start=1):
            entity_id = int(row[0])
            doc_id = str(idx)
            id_to_entity[doc_id] = entity_id
            docs.append(gut.zvec.Doc(id=doc_id, vectors={"embedding": gut.vector_list_from_literal(row[3])}))
            if len(docs) == 128:
                coll.insert(docs)
                docs = []
    if docs:
        coll.insert(docs)
    coll.flush()
    return coll, id_to_entity, str(base_dir)


def build_qdrant_collection_entity_seed(
    csv_path: Path,
    dim: int,
    ef_construction: int,
) -> tuple[object, bool, str, dict[int, int]]:
    client, started = gut.ensure_qdrant()
    name = "graph_rag_multihop_" + hashlib.sha1(str(csv_path).encode("utf-8")).hexdigest()[:8]
    client.create_collection(
        collection_name=name,
        vectors_config=gut.VectorParams(size=dim, distance=gut.Distance.COSINE),
        hnsw_config=gut.HnswConfigDiff(m=16, ef_construct=ef_construction),
    )
    batch = []
    id_to_entity: dict[int, int] = {}
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        for idx, row in enumerate(csv.reader(f), start=1):
            entity_id = int(row[0])
            id_to_entity[idx] = entity_id
            batch.append(gut.PointStruct(id=idx, vector=gut.vector_list_from_literal(row[3]), payload={"entity_id": entity_id}))
            if len(batch) == 64:
                client.upsert(collection_name=name, points=batch, wait=True)
                batch = []
    if batch:
        client.upsert(collection_name=name, points=batch, wait=True)
    return client, started, name, id_to_entity


def print_result(
    table: str,
    case: str,
    p50: float,
    avg: float,
    hits: float,
    reads: float,
    root: str,
    rows: int,
    hit1: float,
    hitk: float,
    avg_rows: float,
) -> None:
    print(
        f"{table}|{case}|p50_ms={p50:.3f}|avg_ms={avg:.3f}|shared_hit={hits:.1f}|shared_read={reads:.1f}|"
        f"root={root}|rows={rows}|hit1_pct={hit1:.1f}|hitk_pct={hitk:.1f}|avg_rows={avg_rows:.2f}"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tmp-root", default="/tmp")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--num-pairs", type=int, default=10000)
    ap.add_argument("--query-count", type=int, default=64)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--dim", type=int, default=32)
    ap.add_argument("--ann-k", type=int, default=32)
    ap.add_argument("--top-k", type=int, default=10)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--ef-search", type=int, default=32)
    ap.add_argument("--ef-construction", type=int, default=64)
    ap.add_argument("--m", type=int, default=16)
    ap.add_argument("--pgv-ef-search", type=int, default=64)
    ap.add_argument("--skip-pgvector", action="store_true")
    ap.add_argument("--zvec-ef", type=int, default=64)
    ap.add_argument("--zvec-memory-limit-mb", type=int, default=8192)
    ap.add_argument("--skip-zvec", action="store_true")
    ap.add_argument("--qdrant-ef", type=int, default=64)
    ap.add_argument("--skip-qdrant", action="store_true")
    ap.add_argument("--shared-buffers-mb", type=int, default=64)
    ap.add_argument("--backend-mode", choices=("fresh", "reuse"), default="fresh")
    ap.add_argument("--exact-seed-diagnostics", action="store_true")
    ap.add_argument("--install-cmd", default="")
    ap.add_argument("--keep-temp", action="store_true")
    args = ap.parse_args()

    root_dir = Path(__file__).resolve().parent.parent
    tmp_root = Path(args.tmp_root).resolve()
    port = args.port or base.pick_port()
    install_cmd = shlex.split(args.install_cmd) if args.install_cmd else None
    tmp, pg_bindir = base.init_temp_cluster(root_dir, port, tmp_root, args.shared_buffers_mb, install_cmd)
    csv_path = tmp / "facts_multihop.csv"
    zvec_dir: str | None = None
    qdrant_client = None
    qdrant_started = False
    qdrant_name: str | None = None

    try:
        city_names = generate_csv(csv_path, args.num_pairs, args.dim, args.seed)

        conn = base.connect(tmp, port)
        cur = conn.cursor()
        try:
            cur.execute("SET jit = off")
            cur.execute("SET sorted_hnsw.shared_cache = off")
            cur.execute(f"SET sorted_hnsw.ef_search = {args.ef_search}")
            base.bootstrap_schema(cur, args.dim)
            base.load_data(cur, csv_path)
            base.build_indexes(cur, args.ef_construction, m=args.m)
            if not args.skip_pgvector:
                gut.bootstrap_pgvector(cur, csv_path, args.dim, args.ef_construction)

            zvec_coll = None
            zvec_target_map: dict[str, int] = {}
            if not args.skip_zvec:
                zvec_coll, zvec_target_map, zvec_dir = build_zvec_collection_entity_seed(
                    csv_path,
                    args.dim,
                    args.ef_construction,
                    args.zvec_memory_limit_mb,
                )

            qdrant_target_map: dict[int, int] = {}
            if not args.skip_qdrant:
                qdrant_client, qdrant_started, qdrant_name, qdrant_target_map = build_qdrant_collection_entity_seed(
                    csv_path,
                    args.dim,
                    args.ef_construction,
                )

            queries = load_queries_from_db(cur, args.query_count, args.num_pairs, args.dim, args.seed)
            if len(queries) != args.query_count:
                raise RuntimeError(f"unexpected query count: {len(queries)}")

            sql_twohop = base.QueryCase(
                "seed_expand2_rerank_rel_in",
                f"""
                WITH ann AS MATERIALIZED (
                    SELECT entity_id
                    FROM {{table}}
                    ORDER BY embedding <=> %s::svec
                    LIMIT {args.ann_k}
                ),
                seeds AS MATERIALIZED (
                    SELECT DISTINCT entity_id FROM ann
                ),
                hop1 AS MATERIALIZED (
                    SELECT DISTINCT target_id
                    FROM {{table}}
                    WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
                      AND relation_id = {REL_PARENT}
                ),
                expanded AS MATERIALIZED (
                    SELECT *
                    FROM {{table}}
                    WHERE entity_id = ANY (ARRAY(SELECT target_id FROM hop1))
                      AND relation_id = {REL_CITY}
                )
                SELECT *
                FROM expanded
                ORDER BY embedding <=> %s::svec
                LIMIT {args.top_k}
                """,
                lambda q: (q.query_vec, q.query_vec),
            )
            helper_twohop = base.QueryCase(
                "seed_expand2_rerank_rel_twohop_fn",
                f"""
                WITH ann AS MATERIALIZED (
                    SELECT entity_id
                    FROM {{table}}
                    ORDER BY embedding <=> %s::svec
                    LIMIT {args.ann_k}
                ),
                seeds AS MATERIALIZED (
                    SELECT DISTINCT entity_id FROM ann
                )
                SELECT *
                FROM sorted_heap_expand_twohop_rerank(
                    '{{table}}'::regclass,
                    ARRAY(SELECT entity_id FROM seeds),
                    %s::svec,
                    {args.top_k},
                    {REL_PARENT},
                    {REL_CITY},
                    0
                )
                """,
                lambda q: (q.query_vec, q.query_vec),
            )
            composed_twohop = base.QueryCase(
                "seed_expand2_rerank_rel_topk_fn",
                f"""
                WITH ann AS MATERIALIZED (
                    SELECT entity_id
                    FROM {{table}}
                    ORDER BY embedding <=> %s::svec
                    LIMIT {args.ann_k}
                ),
                seeds AS MATERIALIZED (
                    SELECT DISTINCT entity_id FROM ann
                ),
                hop1 AS MATERIALIZED (
                    SELECT DISTINCT target_id
                    FROM sorted_heap_expand_ids('{{table}}'::regclass, ARRAY(SELECT entity_id FROM seeds), {REL_PARENT}, 0)
                )
                SELECT *
                FROM sorted_heap_expand_rerank(
                    '{{table}}'::regclass,
                    ARRAY(SELECT target_id FROM hop1),
                    %s::svec,
                    {args.top_k},
                    {REL_CITY},
                    0
                )
                """,
                lambda q: (q.query_vec, q.query_vec),
            )
            wrapper_twohop = base.QueryCase(
                "seed_graph_rag_twohop_scan_fn",
                f"""
                SELECT *
                FROM sorted_heap_graph_rag_twohop_scan(
                    '{{table}}'::regclass,
                    %s::svec,
                    {args.ann_k},
                    {args.top_k},
                    {REL_PARENT},
                    {REL_CITY},
                    0
                )
                """,
                lambda q: (q.query_vec,),
            )
            pgvector_twohop = base.QueryCase(
                "seed_expand2_rerank_rel_pgv",
                f"""
                WITH ann AS MATERIALIZED (
                    SELECT entity_id
                    FROM facts_pgv
                    ORDER BY embedding <=> %s::vector({args.dim})
                    LIMIT {args.ann_k}
                ),
                seeds AS MATERIALIZED (
                    SELECT DISTINCT entity_id FROM ann
                ),
                hop1 AS MATERIALIZED (
                    SELECT DISTINCT target_id
                    FROM facts_heap
                    WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
                      AND relation_id = {REL_PARENT}
                ),
                expanded AS MATERIALIZED (
                    SELECT *
                    FROM facts_heap
                    WHERE entity_id = ANY (ARRAY(SELECT target_id FROM hop1))
                      AND relation_id = {REL_CITY}
                )
                SELECT *
                FROM expanded
                ORDER BY embedding <=> %s::svec
                LIMIT {args.top_k}
                """,
                lambda q: (q.query_vec, q.query_vec),
            )

            sql_rerank_2hop = f"""
                WITH seeds AS MATERIALIZED (
                    SELECT DISTINCT unnest(%s::int4[]) AS entity_id
                ),
                hop1 AS MATERIALIZED (
                    SELECT DISTINCT target_id
                    FROM facts_heap
                    WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
                      AND relation_id = {REL_PARENT}
                ),
                expanded AS MATERIALIZED (
                    SELECT *
                    FROM facts_heap
                    WHERE entity_id = ANY (ARRAY(SELECT target_id FROM hop1))
                      AND relation_id = {REL_CITY}
                )
                SELECT *
                FROM expanded
                ORDER BY embedding <=> %s::svec
                LIMIT {args.top_k}
            """
            helper_rerank_2hop = f"""
                SELECT *
                FROM sorted_heap_expand_twohop_rerank(
                    'facts_sh'::regclass,
                    %s::int4[],
                    %s::svec,
                    {args.top_k},
                    {REL_PARENT},
                    {REL_CITY},
                    0
                )
            """

            print("============================================================")
            print("graph rag multihop fact benchmark")
            print("============================================================")
            print(f"port:             {port}")
            print(f"num_pairs:        {args.num_pairs}")
            print(f"dim:              {args.dim}")
            print(f"rows:             {args.num_pairs * 2}")
            print(f"query_count:      {args.query_count}")
            print(f"runs:             {args.runs}")
            print(f"ann_k:            {args.ann_k}")
            print(f"top_k:            {args.top_k}")
            print(f"ef_search:        {args.ef_search}")
            print(f"ef_construction:  {args.ef_construction}")
            print(f"m:                {args.m}")
            print(f"pgv_ef_search:    {args.pgv_ef_search}")
            print(f"pgvector:         {'off' if args.skip_pgvector else 'on'}")
            print(f"zvec:             {'off' if args.skip_zvec else 'on'}")
            print(f"qdrant:           {'off' if args.skip_qdrant else 'on'}")
            print(f"shared_buffers:   {args.shared_buffers_mb}MB")
            print(f"backend_mode:     {args.backend_mode}")
            print(f"exact_seed_diag:  {'on' if args.exact_seed_diagnostics else 'off'}")
            print(f"sample_city:      {next(iter(city_names.values())) if city_names else 'n/a'}")
            print()

            if args.backend_mode == "fresh":
                cur.close()
                conn.close()
                conn = base.connect(tmp, port)
                cur = conn.cursor()

            cur.execute("SET jit = off")
            cur.execute("SET sorted_hnsw.shared_cache = off")
            cur.execute(f"SET sorted_hnsw.ef_search = {args.ef_search}")
            ann_seed_fn = build_ann_seed_fn(cur, args.ann_k, "facts_sh")
            exact_seed_fn = build_exact_seed_fn(cur, args.ann_k, args.dim)
            verify_helper_twohop_equivalence(cur, "facts_sh", queries, args.ann_k, args.top_k)
            verify_graph_rag_twohop_scan_equivalence(cur, "facts_sh", queries, args.ann_k, args.top_k)

            cases: list[tuple[str, str, base.QueryCase]] = [
                ("facts_heap", "facts_heap", sql_twohop),
                ("facts_sh", "facts_sh", sql_twohop),
                ("facts_sh", "facts_sh", composed_twohop),
                ("facts_sh", "facts_sh", helper_twohop),
                ("facts_sh", "facts_sh", wrapper_twohop),
            ]

            for label, table, case in cases:
                print(f"running|table={label}|case={case.name}", flush=True)
                p50, avg, hits, reads, root, rowcount = base.measure_case(cur, table, case, queries, args.runs)
                hit1, hitk, avg_rows = measure_quality(cur, table, case, queries)
                print_result(label, case.name, p50, avg, hits, reads, root, rowcount, hit1, hitk, avg_rows)

            if args.exact_seed_diagnostics:
                print("running|table=facts_heap|case=seed_expand2_rerank_rel_exact_seed", flush=True)
                p50, avg, hits, reads, root, rowcount = measure_external_case(
                    cur,
                    queries,
                    args.runs,
                    exact_seed_fn,
                    lambda q, seeds: (sql_rerank_2hop, (seeds, q.query_vec)),
                )
                hit1, hitk, avg_rows = measure_external_quality(
                    cur,
                    queries,
                    exact_seed_fn,
                    lambda q, seeds: (sql_rerank_2hop, (seeds, q.query_vec)),
                )
                print_result(
                    "facts_heap",
                    "seed_expand2_rerank_rel_exact_seed",
                    p50,
                    avg,
                    hits,
                    reads,
                    root,
                    rowcount,
                    hit1,
                    hitk,
                    avg_rows,
                )

                print("running|table=facts_sh|case=seed_expand2_rerank_rel_twohop_exact_seed", flush=True)
                p50, avg, hits, reads, root, rowcount = measure_external_case(
                    cur,
                    queries,
                    args.runs,
                    exact_seed_fn,
                    lambda q, seeds: (helper_rerank_2hop, (seeds, q.query_vec)),
                )
                hit1, hitk, avg_rows = measure_external_quality(
                    cur,
                    queries,
                    exact_seed_fn,
                    lambda q, seeds: (helper_rerank_2hop, (seeds, q.query_vec)),
                )
                print_result(
                    "facts_sh",
                    "seed_expand2_rerank_rel_twohop_exact_seed",
                    p50,
                    avg,
                    hits,
                    reads,
                    root,
                    rowcount,
                    hit1,
                    hitk,
                    avg_rows,
                )

                ann_seed_person, ann_expanded_city, ann_avg_rank = measure_seed_diagnostics(
                    cur,
                    queries,
                    ann_seed_fn,
                    args.ann_k,
                )
                print(
                    f"diagnostic|seed_mode=ann|seed_person_pct={ann_seed_person:.1f}|"
                    f"expanded_city_pct={ann_expanded_city:.1f}|avg_person_rank={ann_avg_rank:.2f}"
                )
                exact_seed_person, exact_expanded_city, exact_avg_rank = measure_seed_diagnostics(
                    cur,
                    queries,
                    exact_seed_fn,
                    args.ann_k,
                )
                print(
                    f"diagnostic|seed_mode=exact|seed_person_pct={exact_seed_person:.1f}|"
                    f"expanded_city_pct={exact_expanded_city:.1f}|avg_person_rank={exact_avg_rank:.2f}"
                )

            if not args.skip_pgvector:
                cur.execute(f"SET hnsw.ef_search = {args.pgv_ef_search}")
                print(f"running|table=facts_pgv|case={pgvector_twohop.name}", flush=True)
                p50, avg, hits, reads, root, rowcount = base.measure_case(
                    cur, "facts_pgv", pgvector_twohop, queries, args.runs
                )
                hit1, hitk, avg_rows = measure_quality(cur, "facts_pgv", pgvector_twohop, queries)
                print_result("facts_pgv", pgvector_twohop.name, p50, avg, hits, reads, root, rowcount, hit1, hitk, avg_rows)

            if not args.skip_zvec and zvec_coll is not None:
                param = gut.zvec.HnswQueryParam(ef=args.zvec_ef)

                def zvec_seed_fn(qvec: list[float]) -> list[int]:
                    res = zvec_coll.query(gut.zvec.VectorQuery("embedding", vector=qvec, param=param), topk=args.ann_k)
                    return gut.map_zvec_results_to_targets(res, zvec_target_map)

                print("running|table=facts_zvec|case=seed_expand2_rerank_rel_zvec", flush=True)
                p50, avg, hits, reads, root, rowcount = measure_external_case(
                    cur,
                    queries,
                    args.runs,
                    zvec_seed_fn,
                    lambda q, seeds: (sql_rerank_2hop, (seeds, q.query_vec)),
                )
                hit1, hitk, avg_rows = measure_external_quality(
                    cur,
                    queries,
                    zvec_seed_fn,
                    lambda q, seeds: (sql_rerank_2hop, (seeds, q.query_vec)),
                )
                print_result("facts_zvec", "seed_expand2_rerank_rel_zvec", p50, avg, hits, reads, root, rowcount, hit1, hitk, avg_rows)

            if not args.skip_qdrant and qdrant_client is not None and qdrant_name is not None:
                params = gut.SearchParams(hnsw_ef=args.qdrant_ef, exact=False)

                def qdrant_seed_fn(qvec: list[float]) -> list[int]:
                    res = qdrant_client.query_points(
                        collection_name=qdrant_name,
                        query=qvec,
                        limit=args.ann_k,
                        search_params=params,
                    ).points
                    return gut.unique_ints_in_order([qdrant_target_map[int(p.id)] for p in res])

                print("running|table=facts_qdrant|case=seed_expand2_rerank_rel_qdrant", flush=True)
                p50, avg, hits, reads, root, rowcount = measure_external_case(
                    cur,
                    queries,
                    args.runs,
                    qdrant_seed_fn,
                    lambda q, seeds: (sql_rerank_2hop, (seeds, q.query_vec)),
                )
                hit1, hitk, avg_rows = measure_external_quality(
                    cur,
                    queries,
                    qdrant_seed_fn,
                    lambda q, seeds: (sql_rerank_2hop, (seeds, q.query_vec)),
                )
                print_result(
                    "facts_qdrant",
                    "seed_expand2_rerank_rel_qdrant",
                    p50,
                    avg,
                    hits,
                    reads,
                    root,
                    rowcount,
                    hit1,
                    hitk,
                    avg_rows,
                )
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
