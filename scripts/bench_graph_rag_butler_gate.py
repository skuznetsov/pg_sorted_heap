#!/usr/bin/env python3
"""
Smoke-check GraphRAG on the real Butler gate seed corpus from cogniformerus.

This is intentionally not a publishable benchmark frontier. The corpus is tiny:
7 graph facts and 2 positive multihop queries. Its purpose is narrower:

1. verify that the current path-aware GraphRAG helpers work on the actual
   Butler gate fact shape, not only on synthetic chains;
2. provide a repo-owned, reproducible smoke harness for that corpus.
"""

from __future__ import annotations

import argparse
import csv
import json
import shlex
import statistics
from dataclasses import dataclass
from pathlib import Path

import bench_graph_rag as base
import bench_graph_rag_multihop as mh


@dataclass(frozen=True)
class GateQuery:
    label: str
    prompt: str
    query_vec: str
    seed_entity_id: int
    expected_target_id: int
    hop1_relation_id: int
    hop2_relation_id: int


def load_fixture(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_csv_and_queries(fixture: dict, csv_path: Path, dim: int) -> tuple[list[GateQuery], int]:
    graph_facts = [fact for fact in fixture["facts"] if fact.get("include_in_graph", True)]
    relation_ids: dict[str, int] = {}
    entity_ids: dict[str, int] = {}

    def ensure_entity(name: str) -> int:
        if name not in entity_ids:
            entity_ids[name] = len(entity_ids) + 1
        return entity_ids[name]

    def ensure_relation(name: str) -> int:
        if name not in relation_ids:
            relation_ids[name] = len(relation_ids) + 1
        return relation_ids[name]

    rows = 0
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for fact in graph_facts:
            entity_id = ensure_entity(fact["entity"])
            target_id = ensure_entity(fact["target"])
            relation_id = ensure_relation(fact["relation"])
            w.writerow(
                [
                    entity_id,
                    relation_id,
                    target_id,
                    mh.lexical_hash_vector(fact["text"], dim),
                    fact["text"],
                ]
            )
            rows += 1

    queries: list[GateQuery] = []
    for item in fixture["multihop_queries"]:
        queries.append(
            GateQuery(
                label=item["label"],
                prompt=item["prompt"],
                query_vec=mh.lexical_hash_vector(item["prompt"], dim),
                seed_entity_id=ensure_entity(item["seed_entity"]),
                expected_target_id=ensure_entity(item["expected_target"]),
                hop1_relation_id=ensure_relation(item["hop1_relation"]),
                hop2_relation_id=ensure_relation(item["hop2_relation"]),
            )
        )

    return queries, rows


def verify_fixture_shape(cur, queries: list[GateQuery]) -> None:
    for query in queries:
        cur.execute(
            """
            WITH hop1 AS MATERIALIZED (
                SELECT target_id
                FROM facts_heap
                WHERE entity_id = %s
                  AND relation_id = %s
            )
            SELECT count(*)
            FROM facts_heap
            WHERE entity_id = ANY (ARRAY(SELECT target_id FROM hop1))
              AND relation_id = %s
              AND target_id = %s
            """,
            (
                query.seed_entity_id,
                query.hop1_relation_id,
                query.hop2_relation_id,
                query.expected_target_id,
            ),
        )
        if cur.fetchone()[0] != 1:
            raise RuntimeError(f"fixture chain missing for {query.label}")


def verify_helper_path_equivalence(cur, table_name: str, queries: list[GateQuery], ann_k: int, top_k: int) -> None:
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
        FROM sorted_heap_expand_twohop_path_rerank(
            '{table_name}'::regclass,
            ARRAY(SELECT entity_id FROM seeds),
            %s::svec,
            {top_k},
            %s::int4,
            %s::int4,
            0
        )
    ),
    hop1_sql AS MATERIALIZED (
        SELECT DISTINCT ON (target_id)
               target_id AS hop1_target_id,
               (embedding <=> %s::svec) AS hop1_distance
        FROM {table_name}
        WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
          AND relation_id = %s
        ORDER BY target_id, embedding <=> %s::svec, entity_id
    ),
    sql_baseline AS (
        SELECT hop2.entity_id,
               hop2.relation_id,
               hop2.target_id,
               round(((hop2.embedding <=> %s::svec) + hop1_sql.hop1_distance)::numeric, 6) AS distance
        FROM {table_name} hop2
        JOIN hop1_sql ON hop1_sql.hop1_target_id = hop2.entity_id
        WHERE hop2.relation_id = %s
        ORDER BY ((hop2.embedding <=> %s::svec) + hop1_sql.hop1_distance),
                 hop2.entity_id, hop2.relation_id, hop2.target_id
        LIMIT {top_k}
    )
    SELECT count(*) FROM (
        (SELECT * FROM helper EXCEPT ALL SELECT * FROM sql_baseline)
        UNION ALL
        (SELECT * FROM sql_baseline EXCEPT ALL SELECT * FROM helper)
    ) diff
    """

    for query in queries:
        cur.execute(
            sql,
            (
                query.query_vec,
                query.query_vec,
                query.hop1_relation_id,
                query.hop2_relation_id,
                query.query_vec,
                query.hop1_relation_id,
                query.query_vec,
                query.query_vec,
                query.hop2_relation_id,
                query.query_vec,
            ),
        )
        diff_rows = cur.fetchone()[0]
        if diff_rows != 0:
            raise RuntimeError(
                f"sorted_heap_expand_twohop_path_rerank mismatch on {table_name} {query.label}: diff_rows={diff_rows}"
            )


def verify_wrapper_path_equivalence(cur, table_name: str, queries: list[GateQuery], ann_k: int, top_k: int) -> None:
    sql = f"""
    WITH helper AS (
        SELECT entity_id, relation_id, target_id, round(distance::numeric, 6) AS distance
        FROM sorted_heap_graph_rag_twohop_path_scan(
            '{table_name}'::regclass,
            %s::svec,
            {ann_k},
            {top_k},
            %s::int4,
            %s::int4,
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
        SELECT DISTINCT ON (target_id)
               target_id AS hop1_target_id,
               (embedding <=> %s::svec) AS hop1_distance
        FROM {table_name}
        WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
          AND relation_id = %s
        ORDER BY target_id, embedding <=> %s::svec, entity_id
    ),
    sql_baseline AS (
        SELECT hop2.entity_id,
               hop2.relation_id,
               hop2.target_id,
               round(((hop2.embedding <=> %s::svec) + hop1_sql.hop1_distance)::numeric, 6) AS distance
        FROM {table_name} hop2
        JOIN hop1_sql ON hop1_sql.hop1_target_id = hop2.entity_id
        WHERE hop2.relation_id = %s
        ORDER BY ((hop2.embedding <=> %s::svec) + hop1_sql.hop1_distance),
                 hop2.entity_id, hop2.relation_id, hop2.target_id
        LIMIT {top_k}
    )
    SELECT count(*) FROM (
        (SELECT * FROM helper EXCEPT ALL SELECT * FROM sql_baseline)
        UNION ALL
        (SELECT * FROM sql_baseline EXCEPT ALL SELECT * FROM helper)
    ) diff
    """

    for query in queries:
        cur.execute(
            sql,
            (
                query.query_vec,
                query.hop1_relation_id,
                query.hop2_relation_id,
                query.query_vec,
                query.query_vec,
                query.hop1_relation_id,
                query.query_vec,
                query.query_vec,
                query.hop2_relation_id,
                query.query_vec,
            ),
        )
        diff_rows = cur.fetchone()[0]
        if diff_rows != 0:
            raise RuntimeError(
                f"sorted_heap_graph_rag_twohop_path_scan mismatch on {table_name} {query.label}: diff_rows={diff_rows}"
            )


def measure_case(cur, table_name: str, case: base.QueryCase, queries: list[GateQuery], runs: int) -> tuple[float, float, float, float, str, int]:
    sql = case.sql_template.format(table=table_name)
    total_ms: list[float] = []
    hits: list[int] = []
    reads: list[int] = []
    root = ""
    rowcount = 0

    for run_idx in range(runs):
        for query_idx, query in enumerate(queries):
            params = case.params_builder(query)
            exec_ms, hit, read, root = base.explain_json(cur, sql, params)
            total_ms.append(exec_ms)
            hits.append(hit)
            reads.append(read)
            if run_idx == 0 and query_idx == 0:
                cur.execute(sql, params)
                rowcount = len(cur.fetchall())

    return (
        statistics.median(total_ms),
        statistics.fmean(total_ms),
        statistics.fmean(hits),
        statistics.fmean(reads),
        root or "Limit",
        rowcount,
    )


def measure_quality(cur, table_name: str, case: base.QueryCase, queries: list[GateQuery]) -> tuple[float, float, float]:
    sql = case.sql_template.format(table=table_name)
    hit1 = 0
    hitk = 0
    total_rows = 0

    for query in queries:
        cur.execute(sql, case.params_builder(query))
        rows = cur.fetchall()
        total_rows += len(rows)
        targets = [int(row[2]) for row in rows]
        if targets and targets[0] == query.expected_target_id:
            hit1 += 1
        if query.expected_target_id in targets:
            hitk += 1

    n = len(queries)
    return (
        (hit1 * 100.0) / n if n else 0.0,
        (hitk * 100.0) / n if n else 0.0,
        total_rows / n if n else 0.0,
    )


def print_result(table: str, case: str, p50: float, avg: float, hits: float, reads: float, root: str, rows: int, hit1: float, hitk: float, avg_rows: float) -> None:
    print(
        f"{table}|{case}|p50_ms={p50:.3f}|avg_ms={avg:.3f}|shared_hit={hits:.1f}|shared_read={reads:.1f}|"
        f"root={root}|rows={rows}|hit1_pct={hit1:.1f}|hitk_pct={hitk:.1f}|avg_rows={avg_rows:.2f}"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fixture", default=str(Path(__file__).resolve().parent / "fixtures" / "graph_rag_butler_gate_seed.json"))
    ap.add_argument("--tmp-root", default="/tmp")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--dim", type=int, default=384)
    ap.add_argument("--ann-k", type=int, default=4)
    ap.add_argument("--top-k", type=int, default=4)
    ap.add_argument("--ef-search", type=int, default=64)
    ap.add_argument("--ef-construction", type=int, default=200)
    ap.add_argument("--m", type=int, default=24)
    ap.add_argument("--shared-buffers-mb", type=int, default=64)
    ap.add_argument("--backend-mode", choices=("fresh", "reuse"), default="fresh")
    ap.add_argument("--install-cmd", default="")
    ap.add_argument("--keep-temp", action="store_true")
    args = ap.parse_args()

    root_dir = Path(__file__).resolve().parent.parent
    fixture_path = Path(args.fixture).resolve()
    tmp_root = Path(args.tmp_root).resolve()
    port = args.port or base.pick_port()
    install_cmd = shlex.split(args.install_cmd) if args.install_cmd else None
    tmp, pg_bindir = base.init_temp_cluster(root_dir, port, tmp_root, args.shared_buffers_mb, install_cmd)
    csv_path = tmp / "facts_butler_gate.csv"

    try:
        fixture = load_fixture(fixture_path)
        queries, rowcount = build_csv_and_queries(fixture, csv_path, args.dim)

        conn = base.connect(tmp, port)
        cur = conn.cursor()
        try:
            cur.execute("SET jit = off")
            cur.execute("SET sorted_hnsw.shared_cache = off")
            cur.execute(f"SET sorted_hnsw.ef_search = {args.ef_search}")
            base.bootstrap_schema(cur, args.dim)
            base.load_data(cur, csv_path)
            base.build_indexes(cur, args.ef_construction, m=args.m)
            verify_fixture_shape(cur, queries)

            if args.backend_mode == "fresh":
                cur.close()
                conn.close()
                conn = base.connect(tmp, port)
                cur = conn.cursor()

            cur.execute("SET jit = off")
            cur.execute("SET sorted_hnsw.shared_cache = off")
            cur.execute(f"SET sorted_hnsw.ef_search = {args.ef_search}")

            verify_helper_path_equivalence(cur, "facts_sh", queries, args.ann_k, args.top_k)
            verify_wrapper_path_equivalence(cur, "facts_sh", queries, args.ann_k, args.top_k)

            sql_pathsum = base.QueryCase(
                "seed_expand2_pathsum_gate_in",
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
                    SELECT DISTINCT ON (target_id)
                           target_id AS hop1_target_id,
                           (embedding <=> %s::svec) AS hop1_distance
                    FROM {{table}}
                    WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
                      AND relation_id = %s
                    ORDER BY target_id, embedding <=> %s::svec, entity_id
                ),
                expanded AS MATERIALIZED (
                    SELECT hop2.*,
                           ((hop2.embedding <=> %s::svec) + hop1.hop1_distance) AS path_distance
                    FROM {{table}} hop2
                    JOIN hop1 ON hop1.hop1_target_id = hop2.entity_id
                    WHERE hop2.relation_id = %s
                )
                SELECT *
                FROM expanded
                ORDER BY path_distance, entity_id, relation_id, target_id
                LIMIT {args.top_k}
                """,
                lambda q: (
                    q.query_vec,
                    q.query_vec,
                    q.hop1_relation_id,
                    q.query_vec,
                    q.query_vec,
                    q.hop2_relation_id,
                ),
            )

            helper_path = base.QueryCase(
                "seed_expand2_pathsum_gate_fn",
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
                FROM sorted_heap_expand_twohop_path_rerank(
                    '{{table}}'::regclass,
                    ARRAY(SELECT entity_id FROM seeds),
                    %s::svec,
                    {args.top_k},
                    %s::int4,
                    %s::int4,
                    0
                )
                """,
                lambda q: (
                    q.query_vec,
                    q.query_vec,
                    q.hop1_relation_id,
                    q.hop2_relation_id,
                ),
            )

            wrapper_path = base.QueryCase(
                "seed_graph_rag_twohop_path_gate_fn",
                f"""
                SELECT *
                FROM sorted_heap_graph_rag_twohop_path_scan(
                    '{{table}}'::regclass,
                    %s::svec,
                    {args.ann_k},
                    {args.top_k},
                    %s::int4,
                    %s::int4,
                    0
                )
                """,
                lambda q: (
                    q.query_vec,
                    q.hop1_relation_id,
                    q.hop2_relation_id,
                ),
            )

            print("============================================================")
            print("graph rag butler gate smoke")
            print("============================================================")
            print(f"fixture:          {fixture_path}")
            print(f"port:             {port}")
            print(f"dim:              {args.dim}")
            print(f"rows:             {rowcount}")
            print(f"queries:          {len(queries)}")
            print(f"runs:             {args.runs}")
            print(f"ann_k:            {args.ann_k}")
            print(f"top_k:            {args.top_k}")
            print(f"ef_search:        {args.ef_search}")
            print(f"ef_construction:  {args.ef_construction}")
            print(f"m:                {args.m}")
            print(f"shared_buffers:   {args.shared_buffers_mb}MB")
            print(f"backend_mode:     {args.backend_mode}")
            print()

            for label, prompt in [(q.label, q.prompt) for q in queries]:
                print(f"query|label={label}|prompt={prompt}")
            print()

            cases: list[tuple[str, str, base.QueryCase]] = [
                ("facts_heap", "facts_heap", sql_pathsum),
                ("facts_sh", "facts_sh", sql_pathsum),
                ("facts_sh", "facts_sh", helper_path),
                ("facts_sh", "facts_sh", wrapper_path),
            ]

            for label, table, case in cases:
                print(f"running|table={label}|case={case.name}", flush=True)
                p50, avg, hits, reads, root, rows = measure_case(cur, table, case, queries, args.runs)
                hit1, hitk, avg_rows = measure_quality(cur, table, case, queries)
                print_result(label, case.name, p50, avg, hits, reads, root, rows, hit1, hitk, avg_rows)
        finally:
            cur.close()
            conn.close()
    finally:
        if args.keep_temp:
            print(f"kept_temp={tmp}")
        else:
            base.stop_temp_cluster(tmp, pg_bindir)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
