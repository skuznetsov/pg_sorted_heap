#!/usr/bin/env python3
"""
Sweep the sorted_hnsw seed frontier on the multihop fact benchmark.

Unlike repeated ad hoc invocations of bench_graph_rag_multihop.py, this script
reuses one loaded corpus per ef_construction and measures multiple ann_k /
ef_search points on the same dataset shape.
"""

from __future__ import annotations

import argparse
import shutil
import tempfile
from pathlib import Path

import bench_graph_rag as base
import bench_graph_rag_multihop as mh


def parse_int_list(raw: str) -> list[int]:
    vals: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        vals.append(int(part))
    if not vals:
        raise ValueError("expected at least one integer")
    return vals


def bootstrap_schema_noext(cur, dim: int) -> None:
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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tmp-root", default="/tmp")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--num-pairs", type=int, default=5000)
    ap.add_argument("--query-count", type=int, default=64)
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--dim", type=int, default=384)
    ap.add_argument("--top-k", type=int, default=10)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--ann-ks", default="64,96,128")
    ap.add_argument("--ef-searches", default="64,96,128")
    ap.add_argument("--ef-constructions", default="200")
    ap.add_argument("--ms", default="16")
    ap.add_argument("--shared-buffers-mb", type=int, default=64)
    ap.add_argument("--backend-mode", choices=("fresh", "reuse"), default="fresh")
    ap.add_argument("--verify-first", action="store_true")
    ap.add_argument("--keep-temp", action="store_true")
    args = ap.parse_args()

    ann_ks = parse_int_list(args.ann_ks)
    ef_searches = parse_int_list(args.ef_searches)
    ef_constructions = parse_int_list(args.ef_constructions)
    ms = parse_int_list(args.ms)

    root_dir = Path(__file__).resolve().parent.parent
    tmp_root = Path(args.tmp_root).resolve()
    port = args.port or base.pick_port()
    tmp, pg_bindir = base.init_temp_cluster(root_dir, port, tmp_root, args.shared_buffers_mb)
    csv_path = tmp / "facts_multihop.csv"

    try:
        mh.generate_csv(csv_path, args.num_pairs, args.dim, args.seed)
        queries = mh.build_queries(args.num_pairs, args.query_count, args.dim, args.seed)

        print("============================================================")
        print("graph rag multihop sweep")
        print("============================================================")
        print(f"num_pairs:        {args.num_pairs}")
        print(f"dim:              {args.dim}")
        print(f"rows:             {args.num_pairs * 2}")
        print(f"query_count:      {args.query_count}")
        print(f"runs:             {args.runs}")
        print(f"ann_ks:           {','.join(str(v) for v in ann_ks)}")
        print(f"ef_searches:      {','.join(str(v) for v in ef_searches)}")
        print(f"ef_constructions: {','.join(str(v) for v in ef_constructions)}")
        print(f"ms:               {','.join(str(v) for v in ms)}")
        print(f"shared_buffers:   {args.shared_buffers_mb}MB")
        print(f"backend_mode:     {args.backend_mode}")
        print()
        print("m|ef_construction|ef_search|ann_k|table|case|p50_ms|avg_ms|hit1_pct|hitk_pct|avg_rows")

        for m in ms:
            for ef_construction in ef_constructions:
                conn = base.connect(tmp, port)
                cur = conn.cursor()
                try:
                    cur.execute("CREATE EXTENSION IF NOT EXISTS pg_sorted_heap")
                    cur.execute("DROP TABLE IF EXISTS facts_pgv")
                    cur.execute("DROP TABLE IF EXISTS facts_sh")
                    cur.execute("DROP TABLE IF EXISTS facts_heap")

                    cur.execute("SET jit = off")
                    cur.execute("SET sorted_hnsw.shared_cache = off")
                    bootstrap_schema_noext(cur, args.dim)
                    base.load_data(cur, csv_path)
                    base.build_indexes(cur, ef_construction, m=m)

                    for ef_search in ef_searches:
                        if args.backend_mode == "fresh":
                            cur.close()
                            conn.close()
                            conn = base.connect(tmp, port)
                            cur = conn.cursor()

                        cur.execute("SET jit = off")
                        cur.execute("SET sorted_hnsw.shared_cache = off")
                        cur.execute(f"SET sorted_hnsw.ef_search = {ef_search}")

                        for ann_k in ann_ks:
                            sql_twohop = base.QueryCase(
                                "seed_expand2_rerank_rel_in",
                                f"""
                                WITH ann AS MATERIALIZED (
                                    SELECT entity_id
                                    FROM {{table}}
                                    ORDER BY embedding <=> %s::svec
                                    LIMIT {ann_k}
                                ),
                                seeds AS MATERIALIZED (
                                    SELECT DISTINCT entity_id FROM ann
                                ),
                                hop1 AS MATERIALIZED (
                                    SELECT DISTINCT target_id
                                    FROM {{table}}
                                    WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
                                      AND relation_id = {mh.REL_PARENT}
                                ),
                                expanded AS MATERIALIZED (
                                    SELECT *
                                    FROM {{table}}
                                    WHERE entity_id = ANY (ARRAY(SELECT target_id FROM hop1))
                                      AND relation_id = {mh.REL_CITY}
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
                                    LIMIT {ann_k}
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
                                    {mh.REL_PARENT},
                                    {mh.REL_CITY},
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
                                    {ann_k},
                                    {args.top_k},
                                    {mh.REL_PARENT},
                                    {mh.REL_CITY},
                                    0
                                )
                                """,
                                lambda q: (q.query_vec,),
                            )

                            if args.verify_first and m == ms[0] and ef_construction == ef_constructions[0] and ef_search == ef_searches[0] and ann_k == ann_ks[0]:
                                mh.verify_helper_twohop_equivalence(cur, "facts_sh", queries, ann_k, args.top_k)
                                mh.verify_graph_rag_twohop_scan_equivalence(cur, "facts_sh", queries, ann_k, args.top_k)

                            cases: list[tuple[str, str, base.QueryCase]] = [
                                ("facts_heap", "facts_heap", sql_twohop),
                                ("facts_sh", "facts_sh", helper_twohop),
                                ("facts_sh", "facts_sh", wrapper_twohop),
                            ]

                            for label, table, case in cases:
                                p50, avg, _hits, _reads, _root, _rowcount = base.measure_case(
                                    cur, table, case, queries, args.runs
                                )
                                hit1, hitk, avg_rows = mh.measure_quality(cur, table, case, queries)
                                print(
                                    f"{m}|{ef_construction}|{ef_search}|{ann_k}|{label}|{case.name}|"
                                    f"{p50:.3f}|{avg:.3f}|{hit1:.1f}|{hitk:.1f}|{avg_rows:.2f}"
                                )
                finally:
                    cur.close()
                    conn.close()

        return 0
    finally:
        if args.keep_temp:
            print(f"keep_temp:        {tmp}", flush=True)
        else:
            base.stop_temp_cluster(tmp, pg_bindir)


if __name__ == "__main__":
    raise SystemExit(main())
