#!/usr/bin/env python3
"""
Run a same-session sorted_hnsw build + first-query falsifier against an
already-restored Gutenberg cluster.

This avoids the restore cost of repro_gutenberg_same_session_hnsw.py and
targets the real suspicious path directly:
  - prepare hsvec shadow table if requested
  - CREATE INDEX USING sorted_hnsw in the current backend
  - run the first ordered scans in that same backend with no reconnect
"""

from __future__ import annotations

import argparse

import psycopg2

import bench_gutenberg_fixed_graph as fixed


def run_same_session_existing(
    host: str,
    port: int,
    dbname: str,
    k: int,
    query_count: int,
    query_source: str,
    sample_seed: str,
    sh_ef_construction: int,
    sh_ef_search: int,
    include_svec: bool,
    include_hsvec: bool,
    rebuild_hs_table: bool,
    verbose_queries: bool,
) -> int:
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        application_name="repro_gutenberg_same_session_existing",
    )
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute("SET jit = off")
        query_specs = fixed.load_query_specs(cur, query_count, query_source, sample_seed)

        if include_hsvec:
            cur.execute("SELECT to_regclass('public.gutenberg_gptoss_hs') IS NOT NULL")
            hs_exists = bool(cur.fetchone()[0])
            if rebuild_hs_table or not hs_exists:
                fixed.prepare_hs_table(cur)

        fixed.build_sorted_hnsw_indexes(
            cur,
            sh_ef_construction,
            include_svec=include_svec,
            include_hsvec=include_hsvec,
        )

        cur.execute("SET enable_seqscan = off")
        cur.execute("SET enable_indexscan = on")
        cur.execute("SET enable_bitmapscan = on")
        cur.execute("SET sorted_hnsw.shared_cache = on")
        cur.execute(f"SET sorted_hnsw.ef_search = {sh_ef_search}")

        print("============================================================")
        print("gutenberg same-session sorted_hnsw repro (existing cluster)")
        print("============================================================")
        print(f"host:              {host}")
        print(f"port:              {port}")
        print(f"dbname:            {dbname}")
        print(f"k:                 {k}")
        print(f"query_count:       {query_count}")
        print(f"query_source:      {query_source}")
        if query_source == "sample":
            print(f"sample_seed:       {sample_seed}")
        print(f"sh_ef_construction:{sh_ef_construction}")
        print(f"sh_ef_search:      {sh_ef_search}")
        print()

        for label, table_name, cast_name in (
            ("svec", "public.gutenberg_gptoss_sh", "svec"),
            ("hsvec", "public.gutenberg_gptoss_hs", "hsvec"),
        ):
            if label == "svec" and not include_svec:
                continue
            if label == "hsvec" and not include_hsvec:
                continue

            print(f"phase={label}|status=starting")
            for qi, (source_id, lit) in enumerate(query_specs, start=1):
                if source_id is None:
                    sql = f"SELECT id FROM {table_name} ORDER BY embedding <=> %s::{cast_name} LIMIT {k}"
                    params = (lit,)
                else:
                    sql = (
                        f"SELECT id FROM {table_name} "
                        f"WHERE id <> %s ORDER BY embedding <=> %s::{cast_name} LIMIT {k}"
                    )
                    params = (source_id, lit)
                if verbose_queries:
                    print(f"phase={label}|status=query_start|qi={qi}", flush=True)
                cur.execute(sql, params)
                if verbose_queries:
                    print(f"phase={label}|status=query_ok|qi={qi}", flush=True)
                elif qi == 1:
                    print(f"phase={label}|status=first_query_ok", flush=True)
            print(f"phase={label}|status=all_queries_ok|count={len(query_specs)}")

        return 0
    finally:
        cur.close()
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--dbname", default="cogniformerus")
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--query-count", type=int, default=50)
    ap.add_argument("--query-source", choices=("bench", "sample"), default="bench")
    ap.add_argument("--sample-seed", default="gutenberg-same-session-existing-1")
    ap.add_argument("--sh-ef-construction", type=int, default=200)
    ap.add_argument("--sh-ef-search", type=int, default=32)
    ap.add_argument("--skip-svec", action="store_true")
    ap.add_argument("--skip-hsvec", action="store_true")
    ap.add_argument("--rebuild-hs-table", action="store_true")
    ap.add_argument("--verbose-queries", action="store_true")
    args = ap.parse_args()

    if args.skip_svec and args.skip_hsvec:
        raise SystemExit("at least one of --skip-svec/--skip-hsvec must be false")

    return run_same_session_existing(
        args.host,
        args.port,
        args.dbname,
        args.k,
        args.query_count,
        args.query_source,
        args.sample_seed,
        args.sh_ef_construction,
        args.sh_ef_search,
        include_svec=not args.skip_svec,
        include_hsvec=not args.skip_hsvec,
        rebuild_hs_table=args.rebuild_hs_table,
        verbose_queries=args.verbose_queries,
    )


if __name__ == "__main__":
    raise SystemExit(main())
