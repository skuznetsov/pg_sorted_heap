#!/usr/bin/env python3
"""
Prototype benchmark for GraphRAG-style retrieval on current pg_sorted_heap.

It compares a normal heap table (with btree PK) against a sorted_heap table
with the same logical schema, using:
  - 1-hop entity lookups
  - 1-hop entity + relation lookups
  - 2-hop SQL expansion
  - vector-seed + graph expansion SQL

The point is to falsify whether current sorted storage + zone-map pruning is
already enough for a useful GraphRAG MVP before designing a new C helper.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import shlex
import shutil
import socket
import statistics
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

import psycopg2
from psycopg2.extensions import cursor as Cursor

PAYLOAD_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789"


@dataclass
class QueryCase:
    name: str
    sql_template: str
    params_builder: callable


def pick_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def init_temp_cluster(
    root_dir: Path,
    port: int,
    tmp_root: Path,
    shared_buffers_mb: int,
    install_cmd: list[str] | None = None,
    max_wal_size_gb: int = 4,
    maintenance_work_mem_mb: int = 0,
) -> tuple[Path, str]:
    pg_bindir = subprocess.check_output(["pg_config", "--bindir"], text=True).strip()
    tmp = Path(tempfile.mkdtemp(prefix="graph_rag_", dir=str(tmp_root)))

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
            f"shared_buffers = {shared_buffers_mb}MB\n"
            "listen_addresses = ''\n"
            "fsync = on\n"
            f"max_wal_size = {max_wal_size_gb}GB\n"
            "checkpoint_timeout = 1h\n"
            "autovacuum = off\n"
            "jit = off\n"
            "log_min_messages = warning\n"
            "shared_preload_libraries = 'pg_sorted_heap'\n"
        )
        if maintenance_work_mem_mb > 0:
            f.write(f"maintenance_work_mem = {maintenance_work_mem_mb}MB\n")

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
        [f"{pg_bindir}/createdb", "-h", str(tmp), "-p", str(port), "bench"],
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


def connect(tmp: Path, port: int):
    conn = psycopg2.connect(host=str(tmp), port=port, dbname="bench")
    conn.autocommit = True
    return conn


def vec_literal(entity_id: int, relation_id: int, target_id: int, dim: int) -> str:
    vals = []
    for d in range(dim):
        x = (
            math.sin(entity_id * 0.013 * (d + 1))
            + math.cos(target_id * 0.017 * (d + 1))
            + relation_id * 0.031
        )
        vals.append(f"{x:.6f}")
    return "[" + ",".join(vals) + "]"


def payload_text(entity_id: int, relation_id: int, target_id: int, payload_bytes: int) -> str:
    base = f"fact:{entity_id}:{relation_id}:{target_id}"
    if payload_bytes <= 0 or len(base) >= payload_bytes:
        return base

    remaining = payload_bytes - len(base) - 1
    seed = ((entity_id * 48271) ^ (relation_id * 997) ^ (target_id * 811)) & 0xFFFFFFFF
    chunk = []
    for i in range(64):
        seed = (seed * 1664525 + 1013904223 + i) & 0xFFFFFFFF
        chunk.append(PAYLOAD_ALPHABET[seed % len(PAYLOAD_ALPHABET)])
    filler = "".join(chunk)
    repeats = (remaining + len(filler) - 1) // len(filler)
    return base + "|" + (filler * repeats)[:remaining]


def generate_csv(path: Path, entities: int, degree: int, relations: int, dim: int, payload_bytes: int) -> int:
    rows = 0
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for entity_id in range(1, entities + 1):
            for slot in range(degree):
                relation_id = (slot % relations) + 1
                target_id = ((entity_id * 48271 + relation_id * 997 + slot * 811) % entities) + 1
                payload = payload_text(entity_id, relation_id, target_id, payload_bytes)
                w.writerow([entity_id, relation_id, target_id, vec_literal(entity_id, relation_id, target_id, dim), payload])
                rows += 1
    return rows


def bootstrap_schema(cur: Cursor, dim: int) -> None:
    cur.execute("CREATE EXTENSION pg_sorted_heap")
    cur.execute(
        f"""
        CREATE TABLE facts_heap (
            entity_id   int4 NOT NULL,
            relation_id int2 NOT NULL,
            target_id   int4 NOT NULL,
            embedding   svec({dim}) NOT NULL,
            payload     text NOT NULL,
            PRIMARY KEY (entity_id, relation_id, target_id)
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE facts_sh (
            entity_id   int4 NOT NULL,
            relation_id int2 NOT NULL,
            target_id   int4 NOT NULL,
            embedding   svec({dim}) NOT NULL,
            payload     text NOT NULL,
            PRIMARY KEY (entity_id, relation_id, target_id)
        ) USING sorted_heap
        """
    )


def load_data_fileobj(cur: Cursor, src, retain_heap: bool = True, post_load_op: str = "compact") -> None:
    if post_load_op not in ("compact", "merge", "none"):
        raise ValueError(f"unsupported post_load_op: {post_load_op}")

    def apply_post_load_op() -> None:
        if post_load_op == "compact":
            cur.execute("SELECT sorted_heap_compact('facts_sh'::regclass)")
        elif post_load_op == "merge":
            cur.execute("SELECT sorted_heap_merge('facts_sh'::regclass)")

    if not retain_heap:
        cur.copy_expert(
            """
            COPY facts_sh (entity_id, relation_id, target_id, embedding, payload)
            FROM STDIN WITH (FORMAT csv)
            """,
            src,
        )
        apply_post_load_op()
        cur.execute("ANALYZE facts_sh")
        cur.execute("DROP TABLE facts_heap")
        return

    cur.copy_expert(
        """
        COPY facts_heap (entity_id, relation_id, target_id, embedding, payload)
        FROM STDIN WITH (FORMAT csv)
        """,
        src,
    )
    cur.execute(
        """
        INSERT INTO facts_sh (entity_id, relation_id, target_id, embedding, payload)
        SELECT entity_id, relation_id, target_id, embedding, payload
        FROM facts_heap
        ORDER BY entity_id, relation_id, target_id
        """
    )
    apply_post_load_op()
    cur.execute("ANALYZE facts_sh")
    if retain_heap:
        cur.execute("ANALYZE facts_heap")
    else:
        cur.execute("DROP TABLE facts_heap")


def load_data(cur: Cursor, csv_path: Path, retain_heap: bool = True, post_load_op: str = "compact") -> None:
    with open(csv_path, "r", encoding="utf-8") as f:
        load_data_fileobj(cur, f, retain_heap=retain_heap, post_load_op=post_load_op)


def build_indexes(
    cur: Cursor,
    ef_construction: int,
    m: int = 16,
    build_heap_index: bool = True,
    build_sorted_heap_index: bool = True,
) -> None:
    if build_heap_index:
        cur.execute(
            f"CREATE INDEX facts_heap_ann_idx ON facts_heap USING sorted_hnsw (embedding) WITH (m = {m}, ef_construction = {ef_construction})"
        )
    if build_sorted_heap_index:
        cur.execute(
            f"CREATE INDEX facts_sh_ann_idx ON facts_sh USING sorted_hnsw (embedding) WITH (m = {m}, ef_construction = {ef_construction})"
        )
    if build_heap_index:
        cur.execute("ANALYZE facts_heap")
    cur.execute("ANALYZE facts_sh")


def load_queries(cur: Cursor, query_count: int) -> list[tuple[int, int, str]]:
    cur.execute(
        """
        SELECT entity_id, relation_id, embedding::text
        FROM facts_heap
        ORDER BY entity_id, relation_id, target_id
        LIMIT %s
        """,
        (query_count,),
    )
    return [(row[0], row[1], row[2]) for row in cur.fetchall()]


def plan_metrics(plan: dict) -> tuple[int, int]:
    hit = int(plan.get("Shared Hit Blocks", 0))
    read = int(plan.get("Shared Read Blocks", 0))
    for child in plan.get("Plans", []):
        ch, cr = plan_metrics(child)
        hit += ch
        read += cr
    return hit, read


def summarize_root(plan: dict) -> str:
    node = plan.get("Node Type", "")
    provider = plan.get("Custom Plan Provider")
    if provider:
        return f"{node}:{provider}"
    return node


def explain_json(cur: Cursor, sql: str, params: tuple) -> tuple[float, int, int, str]:
    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}", params)
    raw = cur.fetchone()[0][0]
    exec_ms = float(raw["Execution Time"])
    hit, read = plan_metrics(raw["Plan"])
    root = summarize_root(raw["Plan"])
    return exec_ms, hit, read, root


def verify_helper_equivalence(cur: Cursor, table_name: str, queries: list[tuple[int, int, str]], ann_k: int) -> None:
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
        SELECT entity_id, relation_id, target_id
        FROM sorted_heap_expand_ids('{table_name}'::regclass, ARRAY(SELECT target_id FROM seeds), NULL, 0)
    ),
    sql_baseline AS (
        SELECT entity_id, relation_id, target_id
        FROM {table_name}
        WHERE entity_id = ANY (ARRAY(SELECT target_id FROM seeds))
    )
    SELECT count(*) FROM (
        (SELECT * FROM helper EXCEPT ALL SELECT * FROM sql_baseline)
        UNION ALL
        (SELECT * FROM sql_baseline EXCEPT ALL SELECT * FROM helper)
    ) diff
    """

    for idx, query in enumerate(queries, start=1):
        cur.execute(sql, (query[2],))
        diff_rows = cur.fetchone()[0]
        if diff_rows != 0:
            raise RuntimeError(
                f"sorted_heap_expand_ids mismatch on {table_name} query#{idx}: diff_rows={diff_rows}"
            )


def verify_helper_rerank_equivalence(
    cur: Cursor, table_name: str, queries: list[tuple[int, int, str]], ann_k: int, top_k: int
) -> None:
    # Compare against a materialized expanded baseline, not a direct
    # filtered ORDER BY <=> LIMIT query on the base table. sorted_hnsw's
    # automatic ordered path is intentionally costed out for extra base quals.
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
        FROM sorted_heap_expand_rerank('{table_name}'::regclass, ARRAY(SELECT target_id FROM seeds), %s::svec, {top_k}, NULL, 0)
    ),
    expanded AS MATERIALIZED (
        SELECT *
        FROM {table_name}
        WHERE entity_id = ANY (ARRAY(SELECT target_id FROM seeds))
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
        cur.execute(sql, (query[2], query[2], query[2], query[2]))
        diff_rows = cur.fetchone()[0]
        if diff_rows != 0:
            raise RuntimeError(
                f"sorted_heap_expand_rerank mismatch on {table_name} query#{idx}: diff_rows={diff_rows}"
            )


def verify_graph_rag_scan_equivalence(
    cur: Cursor, table_name: str, queries: list[tuple[int, int, str]], ann_k: int, top_k: int
) -> None:
    sql = f"""
    WITH helper AS (
        SELECT entity_id, relation_id, target_id, round(distance::numeric, 6) AS distance
        FROM sorted_heap_graph_rag_scan('{table_name}'::regclass, %s::svec, {ann_k}, {top_k}, NULL, 0)
    ),
    ann AS MATERIALIZED (
        SELECT target_id
        FROM {table_name}
        ORDER BY embedding <=> %s::svec
        LIMIT {ann_k}
    ),
    seeds AS MATERIALIZED (
        SELECT DISTINCT target_id FROM ann
    ),
    expanded AS MATERIALIZED (
        SELECT *
        FROM {table_name}
        WHERE entity_id = ANY (ARRAY(SELECT target_id FROM seeds))
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
        cur.execute(sql, (query[2], query[2], query[2], query[2]))
        diff_rows = cur.fetchone()[0]
        if diff_rows != 0:
            raise RuntimeError(
                f"sorted_heap_graph_rag_scan mismatch on {table_name} query#{idx}: diff_rows={diff_rows}"
            )


def verify_helper_filtered_equivalence(
    cur: Cursor, table_name: str, queries: list[tuple[int, int, str]], ann_k: int
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
        SELECT entity_id, relation_id, target_id
        FROM sorted_heap_expand_ids('{table_name}'::regclass, ARRAY(SELECT target_id FROM seeds), %s::int4, 0)
    ),
    sql_baseline AS (
        SELECT entity_id, relation_id, target_id
        FROM {table_name}
        WHERE entity_id = ANY (ARRAY(SELECT target_id FROM seeds))
          AND relation_id = %s
    )
    SELECT count(*) FROM (
        (SELECT * FROM helper EXCEPT ALL SELECT * FROM sql_baseline)
        UNION ALL
        (SELECT * FROM sql_baseline EXCEPT ALL SELECT * FROM helper)
    ) diff
    """

    for idx, query in enumerate(queries, start=1):
        cur.execute(sql, (query[2], query[1], query[1]))
        diff_rows = cur.fetchone()[0]
        if diff_rows != 0:
            raise RuntimeError(
                f"sorted_heap_expand_ids(filtered) mismatch on {table_name} query#{idx}: diff_rows={diff_rows}"
            )


def verify_helper_filtered_rerank_equivalence(
    cur: Cursor, table_name: str, queries: list[tuple[int, int, str]], ann_k: int, top_k: int
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
        FROM sorted_heap_expand_rerank('{table_name}'::regclass, ARRAY(SELECT target_id FROM seeds), %s::svec, {top_k}, %s::int4, 0)
    ),
    expanded AS MATERIALIZED (
        SELECT *
        FROM {table_name}
        WHERE entity_id = ANY (ARRAY(SELECT target_id FROM seeds))
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
        cur.execute(sql, (query[2], query[2], query[1], query[1], query[2], query[2]))
        diff_rows = cur.fetchone()[0]
        if diff_rows != 0:
            raise RuntimeError(
                f"sorted_heap_expand_rerank(filtered) mismatch on {table_name} query#{idx}: diff_rows={diff_rows}"
            )


def verify_graph_rag_scan_filtered_equivalence(
    cur: Cursor, table_name: str, queries: list[tuple[int, int, str]], ann_k: int, top_k: int
) -> None:
    sql = f"""
    WITH helper AS (
        SELECT entity_id, relation_id, target_id, round(distance::numeric, 6) AS distance
        FROM sorted_heap_graph_rag_scan('{table_name}'::regclass, %s::svec, {ann_k}, {top_k}, %s::int4, 0)
    ),
    ann AS MATERIALIZED (
        SELECT target_id
        FROM {table_name}
        ORDER BY embedding <=> %s::svec
        LIMIT {ann_k}
    ),
    seeds AS MATERIALIZED (
        SELECT DISTINCT target_id FROM ann
    ),
    expanded AS MATERIALIZED (
        SELECT *
        FROM {table_name}
        WHERE entity_id = ANY (ARRAY(SELECT target_id FROM seeds))
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
        cur.execute(sql, (query[2], query[1], query[2], query[1], query[2], query[2]))
        diff_rows = cur.fetchone()[0]
        if diff_rows != 0:
            raise RuntimeError(
                f"sorted_heap_graph_rag_scan(filtered) mismatch on {table_name} query#{idx}: diff_rows={diff_rows}"
            )


def measure_case(
    cur: Cursor,
    table_name: str,
    case: QueryCase,
    queries: list[tuple[int, int, str]],
    runs: int,
) -> tuple[float, float, float, float, str, int]:
    all_times: list[float] = []
    all_hits: list[int] = []
    all_reads: list[int] = []
    root = ""
    rowcount = 0

    sql = case.sql_template.format(table=table_name)
    params = case.params_builder(queries[0])
    cur.execute(sql, params)
    rowcount = len(cur.fetchall())

    for _ in range(runs):
        for q in queries:
            params = case.params_builder(q)
            t, h, r, root = explain_json(cur, sql, params)
            all_times.append(t)
            all_hits.append(h)
            all_reads.append(r)
    return (
        statistics.median(all_times),
        statistics.fmean(all_times),
        statistics.fmean(all_hits),
        statistics.fmean(all_reads),
        root,
        rowcount,
    )


def print_result(table: str, case: str, p50: float, avg: float, hits: float, reads: float, root: str, rows: int) -> None:
    print(
        f"{table}|{case}|p50_ms={p50:.3f}|avg_ms={avg:.3f}|shared_hit={hits:.1f}|shared_read={reads:.1f}|root={root}|rows={rows}"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tmp-root", default="/tmp")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--entities", type=int, default=5000)
    ap.add_argument("--degree", type=int, default=8)
    ap.add_argument("--relations", type=int, default=4)
    ap.add_argument("--dim", type=int, default=32)
    ap.add_argument("--payload-bytes", type=int, default=0)
    ap.add_argument("--query-count", type=int, default=16)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--ann-k", type=int, default=16)
    ap.add_argument("--top-k", type=int, default=10)
    ap.add_argument("--ef-search", type=int, default=32)
    ap.add_argument("--ef-construction", type=int, default=64)
    ap.add_argument("--m", type=int, default=16)
    ap.add_argument("--shared-buffers-mb", type=int, default=512)
    ap.add_argument("--backend-mode", choices=("fresh", "reuse"), default="fresh")
    ap.add_argument("--install-cmd", default="")
    ap.add_argument("--keep-temp", action="store_true")
    args = ap.parse_args()

    root_dir = Path(__file__).resolve().parent.parent
    tmp_root = Path(args.tmp_root).resolve()
    port = args.port or pick_port()
    install_cmd = shlex.split(args.install_cmd) if args.install_cmd else None
    tmp, pg_bindir = init_temp_cluster(root_dir, port, tmp_root, args.shared_buffers_mb, install_cmd)
    csv_path = tmp / "facts.csv"

    try:
        rows = generate_csv(csv_path, args.entities, args.degree, args.relations, args.dim, args.payload_bytes)
        conn = connect(tmp, port)
        cur = conn.cursor()
        try:
            cur.execute("SET jit = off")
            cur.execute("SET sorted_hnsw.shared_cache = off")
            cur.execute(f"SET sorted_hnsw.ef_search = {args.ef_search}")
            bootstrap_schema(cur, args.dim)
            load_data(cur, csv_path)
            build_indexes(cur, args.ef_construction, m=args.m)
            queries = load_queries(cur, args.query_count)

            cases = [
                QueryCase(
                    "hop1_entity",
                    "SELECT * FROM {table} WHERE entity_id = %s",
                    lambda q: (q[0],),
                ),
                QueryCase(
                    "hop1_entity_relation",
                    "SELECT * FROM {table} WHERE entity_id = %s AND relation_id = %s",
                    lambda q: (q[0], q[1]),
                ),
                QueryCase(
                    "hop2_join",
                    """
                    WITH hop1 AS MATERIALIZED (
                        SELECT target_id
                        FROM {table}
                        WHERE entity_id = %s
                    )
                    SELECT f2.*
                    FROM {table} f2
                    JOIN hop1 h ON f2.entity_id = h.target_id
                    """,
                    lambda q: (q[0],),
                ),
                QueryCase(
                    "hop2_in",
                    """
                    WITH hop1 AS MATERIALIZED (
                        SELECT target_id
                        FROM {table}
                        WHERE entity_id = %s
                    )
                    SELECT *
                    FROM {table}
                    WHERE entity_id = ANY (ARRAY(SELECT target_id FROM hop1))
                    """,
                    lambda q: (q[0],),
                ),
                QueryCase(
                    "seed_expand_join",
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
                    SELECT f.*
                    FROM {{table}} f
                    JOIN seeds s ON f.entity_id = s.target_id
                    """,
                    lambda q: (q[2],),
                ),
                QueryCase(
                    "seed_expand_in",
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
                    """,
                    lambda q: (q[2],),
                ),
                QueryCase(
                    "seed_expand_rerank_join",
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
                        SELECT f.*
                        FROM {{table}} f
                        JOIN seeds s ON f.entity_id = s.target_id
                    )
                    SELECT *
                    FROM expanded
                    ORDER BY embedding <=> %s::svec
                    LIMIT {args.top_k}
                    """,
                    lambda q: (q[2], q[2]),
                ),
                QueryCase(
                    "seed_expand_rerank_in",
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
                    )
                    SELECT *
                    FROM expanded
                    ORDER BY embedding <=> %s::svec
                    LIMIT {args.top_k}
                    """,
                    lambda q: (q[2], q[2]),
                ),
                QueryCase(
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
                QueryCase(
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
            ]
            helper_cases = [
                QueryCase(
                    "seed_expand_fn",
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
                    FROM sorted_heap_expand_ids('{{table}}'::regclass, ARRAY(SELECT target_id FROM seeds), NULL, 0)
                    """,
                    lambda q: (q[2],),
                ),
                QueryCase(
                    "seed_expand_rerank_fn",
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
                    FROM sorted_heap_expand_ids('{{table}}'::regclass, ARRAY(SELECT target_id FROM seeds), NULL, 0)
                    ORDER BY embedding <=> %s::svec
                    LIMIT {args.top_k}
                    """,
                    lambda q: (q[2], q[2]),
                ),
                QueryCase(
                    "seed_expand_rerank_topk_fn",
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
                    FROM sorted_heap_expand_rerank('{{table}}'::regclass, ARRAY(SELECT target_id FROM seeds), %s::svec, {args.top_k}, NULL, 0)
                    """,
                    lambda q: (q[2], q[2]),
                ),
                QueryCase(
                    "seed_graph_rag_scan_fn",
                    f"""
                    SELECT *
                    FROM sorted_heap_graph_rag_scan('{{table}}'::regclass, %s::svec, {args.ann_k}, {args.top_k}, NULL, 0)
                    """,
                    lambda q: (q[2],),
                ),
                QueryCase(
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
                QueryCase(
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
                QueryCase(
                    "seed_graph_rag_rel_scan_fn",
                    f"""
                    SELECT *
                    FROM sorted_heap_graph_rag_scan('{{table}}'::regclass, %s::svec, {args.ann_k}, {args.top_k}, %s::int4, 0)
                    """,
                    lambda q: (q[2], q[1]),
                ),
            ]

            print("============================================================")
            print("graph rag prototype benchmark")
            print("============================================================")
            print(f"port:             {port}")
            print(f"entities:         {args.entities}")
            print(f"degree:           {args.degree}")
            print(f"relations:        {args.relations}")
            print(f"dim:              {args.dim}")
            print(f"payload_bytes:    {args.payload_bytes}")
            print(f"rows:             {rows}")
            print(f"query_count:      {args.query_count}")
            print(f"runs:             {args.runs}")
            print(f"ann_k:            {args.ann_k}")
            print(f"top_k:            {args.top_k}")
            print(f"ef_search:        {args.ef_search}")
            print(f"ef_construction:  {args.ef_construction}")
            print(f"m:                {args.m}")
            print(f"shared_buffers:   {args.shared_buffers_mb}MB")
            print(f"backend_mode:     {args.backend_mode}")
            print()

            if args.backend_mode == "fresh":
                cur.close()
                conn.close()
                conn = connect(tmp, port)
                cur = conn.cursor()

            cur.execute("SET jit = off")
            cur.execute("SET sorted_hnsw.shared_cache = off")
            cur.execute(f"SET sorted_hnsw.ef_search = {args.ef_search}")
            verify_helper_equivalence(cur, "facts_sh", queries, args.ann_k)
            verify_helper_rerank_equivalence(cur, "facts_sh", queries, args.ann_k, args.top_k)
            verify_graph_rag_scan_equivalence(cur, "facts_sh", queries, args.ann_k, args.top_k)
            verify_helper_filtered_equivalence(cur, "facts_sh", queries, args.ann_k)
            verify_helper_filtered_rerank_equivalence(cur, "facts_sh", queries, args.ann_k, args.top_k)
            verify_graph_rag_scan_filtered_equivalence(cur, "facts_sh", queries, args.ann_k, args.top_k)

            for table in ("facts_heap", "facts_sh"):
                for case in cases:
                    print(f"running|table={table}|case={case.name}", flush=True)
                    p50, avg, hits, reads, root, rowcount = measure_case(cur, table, case, queries, args.runs)
                    print_result(table, case.name, p50, avg, hits, reads, root, rowcount)
                if table == "facts_sh":
                    for case in helper_cases:
                        print(f"running|table={table}|case={case.name}", flush=True)
                        p50, avg, hits, reads, root, rowcount = measure_case(cur, table, case, queries, args.runs)
                        print_result(table, case.name, p50, avg, hits, reads, root, rowcount)
        finally:
            cur.close()
            conn.close()
        return 0
    finally:
        if args.keep_temp:
            print(f"keep_temp:        {tmp}", flush=True)
        else:
            stop_temp_cluster(tmp, pg_bindir)


if __name__ == "__main__":
    raise SystemExit(main())
