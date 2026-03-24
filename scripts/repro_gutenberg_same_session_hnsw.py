#!/usr/bin/env python3
"""
Reproduce first-query same-session ordered scans on restored Gutenberg data.

This intentionally keeps exact GT computation, hsvec shadow-table creation,
index builds, and the first sorted_hnsw ordered scans in one backend session.
Use it as a falsifier for same-session cache/build interaction bugs.
"""

from __future__ import annotations

import argparse
import shlex
from pathlib import Path

import psycopg2

import bench_gutenberg_fixed_graph as fixed


def run_same_session_repro(
    tmp: Path,
    port: int,
    k: int,
    query_count: int,
    query_source: str,
    sample_seed: str,
    sh_ef_construction: int,
    sh_ef_search: int,
    include_svec: bool,
    include_hsvec: bool,
    verbose_queries: bool,
) -> int:
    conn = psycopg2.connect(host=str(tmp), port=port, dbname="cogniformerus")
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute("SET jit = off")
        query_specs = fixed.load_query_specs(cur, query_count, query_source, sample_seed)
        fixed.compute_exact_gt(cur, query_specs, k)
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
        print("gutenberg same-session sorted_hnsw repro")
        print("============================================================")
        print(f"dump:              {args.dump}")
        print(f"port:              {port}")
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
    except Exception as exc:
        print(f"status=error|type={type(exc).__name__}|message={exc}")
        raise
    finally:
        cur.close()
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dump", default="/tmp/cogniformerus_backup/cogniformerus_backup.dump")
    ap.add_argument("--tmp-root", default="/tmp")
    ap.add_argument("--port", type=int, default=0, help="0 picks a free port automatically")
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--query-count", type=int, default=50)
    ap.add_argument("--query-source", choices=("bench", "sample"), default="bench")
    ap.add_argument("--sample-seed", default="gutenberg-same-session-1")
    ap.add_argument("--sh-ef-construction", type=int, default=64)
    ap.add_argument("--sh-ef-search", type=int, default=32)
    ap.add_argument("--skip-svec", action="store_true")
    ap.add_argument("--skip-hsvec", action="store_true")
    ap.add_argument("--keep-temp", action="store_true")
    ap.add_argument("--verbose-queries", action="store_true")
    ap.add_argument("--install-cmd", default="")
    global args
    args = ap.parse_args()

    if args.skip_svec and args.skip_hsvec:
        raise SystemExit("at least one of --skip-svec/--skip-hsvec must be false")

    root_dir = Path(__file__).resolve().parent.parent
    dump_path = Path(args.dump).resolve()
    if not dump_path.exists():
        raise FileNotFoundError(dump_path)

    port = args.port or fixed.pick_port()
    install_cmd = shlex.split(args.install_cmd) if args.install_cmd else None
    tmp_root = Path(args.tmp_root).resolve()

    tmp, pg_bindir = fixed.init_temp_cluster(root_dir, port, tmp_root, install_cmd)
    print(f"tmp:               {tmp}", flush=True)
    print(f"port:              {port}", flush=True)
    try:
        fixed.restore_subset(tmp, pg_bindir, port, dump_path)
        return run_same_session_repro(
            tmp,
            port,
            args.k,
            args.query_count,
            args.query_source,
            args.sample_seed,
            args.sh_ef_construction,
            args.sh_ef_search,
            include_svec=not args.skip_svec,
            include_hsvec=not args.skip_hsvec,
            verbose_queries=args.verbose_queries,
        )
    finally:
        if args.keep_temp:
            print(f"keep_temp:         {tmp}", flush=True)
        else:
            fixed.stop_temp_cluster(tmp, pg_bindir)


if __name__ == "__main__":
    raise SystemExit(main())
