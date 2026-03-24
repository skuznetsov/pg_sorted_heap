#!/usr/bin/env python3
"""
Restore the Gutenberg benchmark subset once, build sorted_hnsw indexes once,
then measure multiple query-time ef_search points on that fixed on-disk graph.

This avoids graph-shape noise from rebuilding HNSW for every operating point.
"""

from __future__ import annotations

import argparse
import shlex
import shutil
import socket
import statistics
import subprocess
import tempfile
import time
from pathlib import Path

import psycopg2


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


def median_ms(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def avg_ms(values: list[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def recall_at_k(found_ids: list[str], gt_ids: list[str], k: int) -> float:
    if k <= 0:
        return 0.0
    return len(set(found_ids) & set(gt_ids)) / float(k) * 100.0


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


def pick_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def init_temp_cluster(root_dir: Path, port: int, tmp_root: Path, install_cmd: list[str] | None = None) -> tuple[Path, str]:
    pg_bindir = subprocess.check_output(["pg_config", "--bindir"], text=True).strip()
    tmp = Path(tempfile.mkdtemp(prefix="gutenberg_fixed_", dir=str(tmp_root)))

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
            "ANALYZE public.gutenberg_gptoss_sh; ANALYZE public.bench_gptoss_queries; ANALYZE public.bench_hnsw_gt;",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def load_query_literals(cur: psycopg2.extensions.cursor, query_count: int) -> list[str]:
    cur.execute(
        """
        SELECT q.qvec::text
        FROM public.bench_gptoss_queries q
        JOIN public.bench_hnsw_gt gt USING (qid)
        ORDER BY q.qid
        LIMIT %s
        """,
        (query_count,),
    )
    return [row[0] for row in cur.fetchall()]


def compute_exact_gt(cur: psycopg2.extensions.cursor, query_literals: list[str], k: int) -> tuple[float, float, list[list[str]]]:
    exact_ms: list[float] = []
    exact_ids: list[list[str]] = []
    cur.execute("SET enable_seqscan = on")
    cur.execute("SET enable_indexscan = off")
    cur.execute("SET enable_bitmapscan = off")
    for lit in query_literals:
        cur.execute(f"SELECT id FROM public.gutenberg_gptoss_sh ORDER BY embedding <=> %s::svec LIMIT {k}", (lit,))
        t0 = time.perf_counter()
        cur.execute(f"SELECT id FROM public.gutenberg_gptoss_sh ORDER BY embedding <=> %s::svec LIMIT {k}", (lit,))
        exact_ids.append([row[0] for row in cur.fetchall()])
        exact_ms.append((time.perf_counter() - t0) * 1000.0)
    return median_ms(exact_ms), avg_ms(exact_ms), exact_ids


def prepare_hs_table(cur: psycopg2.extensions.cursor) -> None:
    cur.execute("DROP TABLE IF EXISTS public.gutenberg_gptoss_hs CASCADE")
    cur.execute(
        """
        CREATE TABLE public.gutenberg_gptoss_hs (
          id text PRIMARY KEY,
          embedding hsvec(2880)
        )
        """
    )
    cur.execute(
        """
        INSERT INTO public.gutenberg_gptoss_hs (id, embedding)
        SELECT id, embedding::hsvec
        FROM public.gutenberg_gptoss_sh
        """
    )
    cur.execute("ANALYZE public.gutenberg_gptoss_hs")


def build_sorted_hnsw_indexes(cur: psycopg2.extensions.cursor, sh_ef_construction: int, include_svec: bool, include_hsvec: bool) -> tuple[str, str]:
    sh_index_size = "skipped"
    hs_index_size = "skipped"
    if include_svec:
        cur.execute("DROP INDEX IF EXISTS public.gutenberg_gptoss_sh_shnsw_idx")
        cur.execute(
            f"CREATE INDEX gutenberg_gptoss_sh_shnsw_idx ON public.gutenberg_gptoss_sh "
            f"USING sorted_hnsw (embedding) WITH (m=16, ef_construction={sh_ef_construction})"
        )
        cur.execute("ANALYZE public.gutenberg_gptoss_sh")
        cur.execute("SELECT pg_size_pretty(pg_relation_size('public.gutenberg_gptoss_sh_shnsw_idx'::regclass))")
        sh_index_size = cur.fetchone()[0]
    if include_hsvec:
        cur.execute("DROP INDEX IF EXISTS public.gutenberg_gptoss_hs_shnsw_idx")
        cur.execute(
            f"CREATE INDEX gutenberg_gptoss_hs_shnsw_idx ON public.gutenberg_gptoss_hs "
            f"USING sorted_hnsw (embedding hsvec_cosine_ops) WITH (m=16, ef_construction={sh_ef_construction})"
        )
        cur.execute("ANALYZE public.gutenberg_gptoss_hs")
        cur.execute("SELECT pg_size_pretty(pg_relation_size('public.gutenberg_gptoss_hs_shnsw_idx'::regclass))")
        hs_index_size = cur.fetchone()[0]
    return sh_index_size, hs_index_size


def measure_sorted_hnsw_pass(
    conn: psycopg2.extensions.connection,
    table_name: str,
    cast_name: str,
    query_literals: list[str],
    exact_ids: list[list[str]],
    k: int,
    ef_search: int,
    shared_cache: bool,
) -> tuple[float, float, float]:
    cur = conn.cursor()
    try:
        cur.execute("SET jit = off")
        cur.execute("SET enable_seqscan = off")
        cur.execute("SET enable_indexscan = on")
        cur.execute("SET enable_bitmapscan = on")
        cur.execute(f"SET sorted_hnsw.shared_cache = {'on' if shared_cache else 'off'}")
        cur.execute(f"SET sorted_hnsw.ef_search = {ef_search}")

        sql = f"SELECT id FROM {table_name} ORDER BY embedding <=> %s::{cast_name} LIMIT {k}"
        ms: list[float] = []
        recall_parts: list[float] = []

        for qi, lit in enumerate(query_literals):
            cur.execute(sql, (lit,))
            t0 = time.perf_counter()
            cur.execute(sql, (lit,))
            ids = [row[0] for row in cur.fetchall()]
            ms.append((time.perf_counter() - t0) * 1000.0)
            recall_parts.append(recall_at_k(ids, exact_ids[qi], k))

        return median_ms(ms), avg_ms(ms), avg_ms(recall_parts)
    finally:
        cur.close()


def measure_fixed_graph(
    tmp: Path,
    port: int,
    table_name: str,
    cast_name: str,
    query_literals: list[str],
    exact_ids: list[list[str]],
    k: int,
    ef_search: int,
    repeats: int,
    backend_mode: str,
    shared_cache: bool,
) -> tuple[float, float, float]:
    p50s: list[float] = []
    avgs: list[float] = []
    recalls: list[float] = []

    if backend_mode == "reuse":
        conn = psycopg2.connect(host=str(tmp), port=port, dbname="cogniformerus")
        conn.autocommit = True
        try:
            for _ in range(repeats):
                p50, avg, recall = measure_sorted_hnsw_pass(
                    conn, table_name, cast_name, query_literals, exact_ids,
                    k, ef_search, shared_cache
                )
                p50s.append(p50)
                avgs.append(avg)
                recalls.append(recall)
        finally:
            conn.close()
    else:
        for _ in range(repeats):
            conn = psycopg2.connect(host=str(tmp), port=port, dbname="cogniformerus")
            conn.autocommit = True
            try:
                p50, avg, recall = measure_sorted_hnsw_pass(
                    conn, table_name, cast_name, query_literals, exact_ids,
                    k, ef_search, shared_cache
                )
                p50s.append(p50)
                avgs.append(avg)
                recalls.append(recall)
            finally:
                conn.close()

    return median_ms(p50s), median_ms(avgs), median_ms(recalls)


def print_result(method: str, p50: float, avg: float, recall: float, k: int, extra: str = "") -> None:
    suffix = f"|{extra}" if extra else ""
    print(f"{method}|p50_ms={p50:.3f}|avg_ms={avg:.3f}|recall_at_{k}={recall:.1f}{suffix}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dump", default="/tmp/cogniformerus_backup/cogniformerus_backup.dump")
    ap.add_argument("--tmp-root", default="/tmp")
    ap.add_argument("--port", type=int, default=0, help="0 picks a free port automatically")
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--efs", default="32,48,64,96", help="comma-separated sorted_hnsw ef_search values")
    ap.add_argument("--sh-ef-construction", type=int, default=64)
    ap.add_argument("--query-count", type=int, default=50)
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--backend-mode", choices=("fresh", "reuse"), default="reuse")
    ap.add_argument("--shared-cache", choices=("on", "off"), default="on")
    ap.add_argument("--skip-svec", action="store_true")
    ap.add_argument("--skip-hsvec", action="store_true")
    ap.add_argument("--install-cmd", default="")
    args = ap.parse_args()

    if args.skip_svec and args.skip_hsvec:
        raise SystemExit("at least one of --skip-svec/--skip-hsvec must be false")

    root_dir = Path(__file__).resolve().parent.parent
    dump_path = Path(args.dump).resolve()
    if not dump_path.exists():
        raise FileNotFoundError(dump_path)

    port = args.port or pick_port()
    efs = [int(part) for part in args.efs.split(",") if part.strip()]
    install_cmd = shlex.split(args.install_cmd) if args.install_cmd else None
    tmp_root = Path(args.tmp_root).resolve()

    tmp, pg_bindir = init_temp_cluster(root_dir, port, tmp_root, install_cmd)
    try:
        restore_subset(tmp, pg_bindir, port, dump_path)
        conn = psycopg2.connect(host=str(tmp), port=port, dbname="cogniformerus")
        conn.autocommit = True
        cur = conn.cursor()
        try:
            query_literals = load_query_literals(cur, args.query_count)
            exact_p50, exact_avg, exact_ids = compute_exact_gt(cur, query_literals, args.k)
            prepare_hs_table(cur)
            svec_index_size, hsvec_index_size = build_sorted_hnsw_indexes(
                cur,
                args.sh_ef_construction,
                include_svec=not args.skip_svec,
                include_hsvec=not args.skip_hsvec,
            )
        finally:
            cur.close()
            conn.close()

        print("============================================================")
        print("gutenberg fixed-graph benchmark")
        print("============================================================")
        print(f"dump:              {dump_path}")
        print(f"port:              {port}")
        print(f"k:                 {args.k}")
        print(f"query_count:       {args.query_count}")
        print(f"sh_ef_construction:{args.sh_ef_construction}")
        print(f"efs:               {','.join(str(v) for v in efs)}")
        print(f"backend_mode:      {args.backend_mode}")
        print(f"shared_cache:      {args.shared_cache}")
        print(f"repeats:           {args.repeats}")
        print()

        print_result("exact_heap", exact_p50, exact_avg, 100.0, args.k)
        for ef in efs:
            if not args.skip_svec:
                p50, avg, recall = measure_fixed_graph(
                    tmp,
                    port,
                    "public.gutenberg_gptoss_sh",
                    "svec",
                    query_literals,
                    exact_ids,
                    args.k,
                    ef,
                    args.repeats,
                    args.backend_mode,
                    args.shared_cache == "on",
                )
                print_result(
                    "sorted_hnsw_svec",
                    p50,
                    avg,
                    recall,
                    args.k,
                    extra=f"ef_search={ef}|index={svec_index_size}",
                )

            if not args.skip_hsvec:
                p50, avg, recall = measure_fixed_graph(
                    tmp,
                    port,
                    "public.gutenberg_gptoss_hs",
                    "hsvec",
                    query_literals,
                    exact_ids,
                    args.k,
                    ef,
                    args.repeats,
                    args.backend_mode,
                    args.shared_cache == "on",
                )
                print_result(
                    "sorted_hnsw_hsvec",
                    p50,
                    avg,
                    recall,
                    args.k,
                    extra=f"ef_search={ef}|index={hsvec_index_size}",
                )
        return 0
    finally:
        stop_temp_cluster(tmp, pg_bindir)


if __name__ == "__main__":
    raise SystemExit(main())
