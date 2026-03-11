#!/usr/bin/env python3
"""
Local reproducible ANN benchmark harness for the synthetic bench_nomic dataset.

Measures exact ground truth vs:
  - svec_graph_scan
  - svec_ann_scan

The goal is reproducibility for local optimization work, not a generic
benchmark framework.
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time
from dataclasses import dataclass

import psycopg2
from psycopg2 import sql


@dataclass
class QueryCase:
    qid: int
    qvec_text: str


def parse_int_list(value: str) -> list[int]:
    return [int(part.strip()) for part in value.split(",") if part.strip()]


def median_ms(values: list[float]) -> float:
    if not values:
        return 0.0
    return statistics.median(values)


def avg_ms(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def fetch_queries(cur, query_table: str, query_limit: int) -> list[QueryCase]:
    cur.execute(
        sql.SQL(
            """
        SELECT qid, qvec::text
        FROM {}
        ORDER BY qid
        LIMIT %s
        """
        ).format(sql.Identifier(query_table)),
        (query_limit,),
    )
    return [QueryCase(qid=row[0], qvec_text=row[1]) for row in cur.fetchall()]


def fetch_exact_gt(cur, exact_table: str, qvec_text: str, k: int) -> list[str]:
    cur.execute(
        sql.SQL(
            """
        SELECT id::text
        FROM {}
        ORDER BY embedding <=> %s::halfvec
        LIMIT %s
        """
        ).format(sql.Identifier(exact_table)),
        (qvec_text, k),
    )
    return [row[0] for row in cur.fetchall()]


def run_graph(
    cur,
    main_table: str,
    graph_table: str,
    entry_table: str,
    qvec_text: str,
    ef_search: int,
    k: int,
    rerank_topk: int,
) -> list[str]:
    cur.execute(
        """
        SELECT id
        FROM svec_graph_scan(%s::regclass, %s::svec, %s, %s, %s, %s, %s)
        """,
        (main_table, qvec_text, graph_table, ef_search, k, rerank_topk, entry_table),
    )
    return [row[0] for row in cur.fetchall()]


def run_ivf(
    cur,
    main_table: str,
    qvec_text: str,
    nprobe: int,
    k: int,
    rerank_topk: int,
    cb_id: int,
    ivf_cb_id: int,
    pq_column: str,
) -> list[str]:
    cur.execute(
        """
        SELECT id
        FROM svec_ann_scan(
            %s::regclass, %s::svec, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (main_table, qvec_text, nprobe, k, rerank_topk, cb_id, ivf_cb_id,
         pq_column, "", 0),
    )
    return [row[0] for row in cur.fetchall()]


def timed_ids(fn) -> tuple[list[str], float]:
    t0 = time.perf_counter()
    ids = fn()
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    return ids, elapsed_ms


def overlap_at_k(found: list[str], gt: list[str]) -> int:
    gt_set = set(gt)
    return sum(1 for item in found if item in gt_set)


def autodetect_codebooks(cur) -> tuple[int, int]:
    cur.execute("SELECT cb_id, ivf_cb_id FROM public._pq_codebook_meta ORDER BY cb_id LIMIT 1")
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("no rows in public._pq_codebook_meta; pass --cb-id/--ivf-cb-id explicitly")
    return int(row[0]), int(row[1])


def print_method_table(title: str, rows: list[tuple[str, float, float]]) -> None:
    print(f"\n{title}")
    print("-" * len(title))
    print(f"{'setting':<18} {'p50_ms':>10} {'avg_recall@10':>15}")
    for setting, p50, recall in rows:
        print(f"{setting:<18} {p50:>10.3f} {recall:>15.2f}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Local ANN benchmark for bench_nomic")
    parser.add_argument("--dsn", default="host=/tmp port=65432 dbname=bench_nomic")
    parser.add_argument("--exact-table", default="bench_nomic_train_8k")
    parser.add_argument("--graph-main-table", default="bench_nomic_8k")
    parser.add_argument("--graph-table", default="bench_nomic_graph")
    parser.add_argument("--entry-table", default="bench_nomic_graph_entries")
    parser.add_argument("--query-table", default="bench_nomic_query_200")
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--query-limit", type=int, default=20)
    parser.add_argument("--graph-efs", default="32,64,128,256,512,1024")
    parser.add_argument("--graph-rerank-topk", type=int, default=0)
    parser.add_argument("--ivf-nprobes", default="10,20,40")
    parser.add_argument("--ivf-rerank-topk", type=int, default=500)
    parser.add_argument("--cb-id", type=int, default=0)
    parser.add_argument("--ivf-cb-id", type=int, default=-1)
    parser.add_argument("--pq-column", default="pq_code")
    parser.add_argument("--warmup", type=int, default=1)
    args = parser.parse_args()

    graph_efs = parse_int_list(args.graph_efs)
    ivf_nprobes = parse_int_list(args.ivf_nprobes)

    conn = psycopg2.connect(args.dsn)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    try:
        if args.cb_id == 0 or args.ivf_cb_id < 0:
            auto_cb_id, auto_ivf_cb_id = autodetect_codebooks(cur)
            if args.cb_id == 0:
                args.cb_id = auto_cb_id
            if args.ivf_cb_id < 0:
                args.ivf_cb_id = auto_ivf_cb_id

        queries = fetch_queries(cur, args.query_table, args.query_limit)
        if not queries:
            raise RuntimeError("no queries loaded")

        exact_latencies: list[float] = []
        gt_by_qid: dict[int, list[str]] = {}
        for case in queries:
            gt_ids, gt_ms = timed_ids(
                lambda case=case: fetch_exact_gt(cur, args.exact_table, case.qvec_text, args.k)
            )
            gt_by_qid[case.qid] = gt_ids
            exact_latencies.append(gt_ms)

        graph_rows: list[tuple[str, float, float]] = []
        for ef in graph_efs:
            latencies: list[float] = []
            recalls: list[int] = []
            for case in queries:
                for _ in range(args.warmup):
                    run_graph(
                        cur, args.graph_main_table, args.graph_table, args.entry_table,
                        case.qvec_text, ef, args.k, args.graph_rerank_topk,
                    )
                ids, ms = timed_ids(
                    lambda case=case, ef=ef: run_graph(
                        cur, args.graph_main_table, args.graph_table, args.entry_table,
                        case.qvec_text, ef, args.k, args.graph_rerank_topk,
                    )
                )
                latencies.append(ms)
                recalls.append(overlap_at_k(ids, gt_by_qid[case.qid]))
            graph_rows.append((f"ef={ef}", median_ms(latencies), avg_ms(recalls)))

        ivf_rows: list[tuple[str, float, float]] = []
        for nprobe in ivf_nprobes:
            latencies = []
            recalls = []
            for case in queries:
                for _ in range(args.warmup):
                    run_ivf(
                        cur, args.graph_main_table, case.qvec_text, nprobe, args.k,
                        args.ivf_rerank_topk, args.cb_id, args.ivf_cb_id, args.pq_column,
                    )
                ids, ms = timed_ids(
                    lambda case=case, nprobe=nprobe: run_ivf(
                        cur, args.graph_main_table, case.qvec_text, nprobe, args.k,
                        args.ivf_rerank_topk, args.cb_id, args.ivf_cb_id, args.pq_column,
                    )
                )
                latencies.append(ms)
                recalls.append(overlap_at_k(ids, gt_by_qid[case.qid]))
            ivf_rows.append((f"nprobe={nprobe}", median_ms(latencies), avg_ms(recalls)))

        print("bench_nomic local ANN benchmark")
        print("=============================")
        print(f"dsn={args.dsn}")
        print(f"queries={len(queries)} k={args.k} warmup={args.warmup}")
        print(f"exact_table={args.exact_table} graph_table={args.graph_table} entry_table={args.entry_table}")
        print(f"cb_id={args.cb_id} ivf_cb_id={args.ivf_cb_id} pq_column={args.pq_column}")

        print("\nExact baseline")
        print("--------------")
        print(f"p50_ms={median_ms(exact_latencies):.3f}")

        print_method_table("Graph scan", graph_rows)
        print_method_table("IVF-PQ", ivf_rows)
        return 0
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
