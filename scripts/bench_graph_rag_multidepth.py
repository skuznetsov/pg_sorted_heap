#!/usr/bin/env python3
"""
Benchmark fact-shaped GraphRAG across path depth 1..N.

Dataset shape:
  - relation 1: person -> hop1
  - relation 2: hop1 -> hop2
  - ...
  - relation N: hop(N-1) -> hopN

This harness measures how the ANN-seeded path-aware contract scales with depth
for:
  - heap SQL baseline
  - sorted_heap SQL baseline
  - sorted_heap_graph_rag(... relation_path := ARRAY[...], score_mode := 'path')
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import random
import shlex
import statistics
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import bench_graph_rag as base


@dataclass(frozen=True)
class DeepQuery:
    person_id: int
    vectors: dict[int, str]


META_NAME = "multidepth_meta.json"


def meta_path(tmp: Path) -> Path:
    return tmp / META_NAME


def write_meta(tmp: Path, payload: dict) -> None:
    meta_path(tmp).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_meta(tmp: Path) -> dict:
    return json.loads(meta_path(tmp).read_text(encoding="utf-8"))


def normalize(vals: list[float]) -> list[float]:
    norm = math.sqrt(sum(v * v for v in vals))
    return [v / norm for v in vals]


def person_base(person_id: int, dim: int) -> list[float]:
    return [
        math.sin(person_id * 0.013 * (idx + 1)) +
        0.7 * math.cos(person_id * 0.017 * (idx + 1))
        for idx in range(dim)
    ]


def hop_base(hop: int, dim: int) -> list[float]:
    return [
        0.6 * math.sin((1000 + hop) * 0.019 * (idx + 1)) +
        0.4 * math.cos((2000 + hop) * 0.023 * (idx + 1))
        for idx in range(dim)
    ]


def vec_to_svec(vals: list[float]) -> str:
    return "[" + ",".join(f"{v:.6f}" for v in normalize(vals)) + "]"


def generate_csv(path: Path, num_pairs: int, max_depth: int, dim: int, hop_weight: float) -> None:
    hop_cache = {h: hop_base(h, dim) for h in range(1, max_depth + 1)}

    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        for person_id in range(1, num_pairs + 1):
            person_vals = person_base(person_id, dim)
            prev = person_id
            for hop in range(1, max_depth + 1):
                target_id = hop * num_pairs + person_id
                vals = [
                    person_vals[idx] + hop_weight * hop_cache[hop][idx]
                    for idx in range(dim)
                ]
                payload = f"Person_{person_id} rel_{hop} node_{hop}_{person_id}."
                writer.writerow([prev, hop, target_id, vec_to_svec(vals), payload])
                prev = target_id


class FactCsvStream(io.TextIOBase):
    def __init__(self, num_pairs: int, max_depth: int, dim: int, hop_weight: float) -> None:
        self.num_pairs = num_pairs
        self.max_depth = max_depth
        self.dim = dim
        self.hop_weight = hop_weight
        self.hop_cache = {h: hop_base(h, dim) for h in range(1, max_depth + 1)}
        self.person_id = 1
        self.hop = 1
        self.person_vals: list[float] | None = None
        self.buf = ""
        self.done = False

    def readable(self) -> bool:
        return True

    def _next_line(self) -> str | None:
        if self.person_id > self.num_pairs:
            return None

        if self.person_vals is None:
            self.person_vals = person_base(self.person_id, self.dim)

        hop = self.hop
        prev = self.person_id if hop == 1 else (hop - 1) * self.num_pairs + self.person_id
        target_id = hop * self.num_pairs + self.person_id
        vals = [
            self.person_vals[idx] + self.hop_weight * self.hop_cache[hop][idx]
            for idx in range(self.dim)
        ]
        payload = f"Person_{self.person_id} rel_{hop} node_{hop}_{self.person_id}."
        line = f'{prev},{hop},{target_id},"{vec_to_svec(vals)}",{payload}\n'

        if self.hop == self.max_depth:
            self.person_id += 1
            self.hop = 1
            self.person_vals = None
        else:
            self.hop += 1

        return line

    def read(self, size: int = -1) -> str:
        if self.done and not self.buf:
            return ""
        if size is None or size < 0:
            size = 1 << 20
        while len(self.buf) < size and not self.done:
            line = self._next_line()
            if line is None:
                self.done = True
                break
            self.buf += line
        out = self.buf[:size]
        self.buf = self.buf[size:]
        return out


def build_queries(num_pairs: int, query_count: int, max_depth: int, dim: int, seed: int, hop_weight: float) -> list[DeepQuery]:
    if query_count > num_pairs:
        raise ValueError(f"query_count {query_count} exceeds num_pairs {num_pairs}")

    rng = random.Random(seed)
    sample = rng.sample(range(1, num_pairs + 1), query_count)
    person_cache = {i: person_base(i, dim) for i in sample}
    hop_cache = {h: hop_base(h, dim) for h in range(1, max_depth + 1)}
    queries: list[DeepQuery] = []

    for person_id in sample:
        vectors: dict[int, str] = {}
        for depth in range(1, max_depth + 1):
            vals = [
                person_cache[person_id][idx] +
                hop_weight * sum(hop_cache[hop][idx] for hop in range(1, depth + 1))
                for idx in range(dim)
            ]
            vectors[depth] = vec_to_svec(vals)
        queries.append(DeepQuery(person_id=person_id, vectors=vectors))

    return queries


def relation_path_literal(depth: int) -> str:
    return "ARRAY[" + ",".join(str(hop) for hop in range(1, depth + 1)) + "]"


def make_sql_path_case(depth: int, ann_k: int, top_k: int) -> base.QueryCase:
    ctes = [
        "ann AS MATERIALIZED ("
        "SELECT entity_id FROM {table} ORDER BY embedding <=> %s::svec LIMIT "
        f"{ann_k})",
        "seeds AS MATERIALIZED (SELECT DISTINCT entity_id FROM ann)",
        "hop1 AS MATERIALIZED ("
        "SELECT t.entity_id, t.relation_id, t.target_id, t.payload, "
        "t.target_id AS node_1, (t.embedding <=> %s::svec) AS path_distance "
        "FROM {table} t "
        "WHERE t.entity_id = ANY (ARRAY(SELECT entity_id FROM seeds)) "
        "AND t.relation_id = 1)",
    ]

    for hop in range(2, depth + 1):
        ctes.append(
            f"hop{hop} AS MATERIALIZED ("
            "SELECT t.entity_id, t.relation_id, t.target_id, t.payload, "
            f"t.target_id AS node_{hop}, "
            f"prev.path_distance + (t.embedding <=> %s::svec) AS path_distance "
            f"FROM {{table}} t "
            f"JOIN hop{hop - 1} prev ON prev.node_{hop - 1} = t.entity_id "
            f"WHERE t.relation_id = {hop})"
        )

    sql = (
        "WITH " + ",\n".join(ctes) +
        f"\nSELECT entity_id, relation_id, target_id, payload, path_distance AS distance "
        f"FROM hop{depth} "
        "ORDER BY path_distance, entity_id, relation_id, target_id "
        f"LIMIT {top_k}"
    )
    return base.QueryCase(
        f"sql_path_depth_{depth}",
        sql,
        lambda q: tuple([q.vectors[depth]] * (depth + 1)),
    )


def make_unified_case(depth: int, ann_k: int, top_k: int) -> base.QueryCase:
    path = relation_path_literal(depth)
    return base.QueryCase(
        f"graph_rag_path_depth_{depth}",
        f"""
        SELECT *
        FROM sorted_heap_graph_rag(
          '{{table}}'::regclass,
          %s::svec,
          relation_path := {path},
          ann_k := {ann_k},
          top_k := {top_k},
          score_mode := 'path',
          limit_rows := 0
        )
        """,
        lambda q: (q.vectors[depth],),
    )


def measure_quality(cur, table_name: str, case: base.QueryCase, queries: list[DeepQuery], num_pairs: int, depth: int) -> tuple[float, float]:
    sql = case.sql_template.format(table=table_name)
    hit1 = 0
    hitk = 0

    for query in queries:
        cur.execute(sql, case.params_builder(query))
        rows = cur.fetchall()
        targets = [row[2] for row in rows]
        expected = depth * num_pairs + query.person_id
        if targets:
            if targets[0] == expected:
                hit1 += 1
            if expected in targets:
                hitk += 1

    n = len(queries)
    return ((hit1 * 100.0) / n if n else 0.0, (hitk * 100.0) / n if n else 0.0)


def verify_unified_equivalence(cur, queries: list[DeepQuery], depth: int, ann_k: int, top_k: int) -> None:
    helper_case = make_unified_case(depth, ann_k, top_k)
    sql_case = make_sql_path_case(depth, ann_k, top_k)
    helper_sql = helper_case.sql_template.format(table="facts_sh")
    sql_sql = sql_case.sql_template.format(table="facts_sh")

    for idx, query in enumerate(queries, start=1):
        cur.execute(helper_sql, helper_case.params_builder(query))
        helper_rows = cur.fetchall()
        cur.execute(sql_sql, sql_case.params_builder(query))
        sql_rows = cur.fetchall()

        helper_simple = [
            (row[0], row[1], row[2], row[3], round(float(row[4]), 6))
            for row in helper_rows
        ]
        sql_simple = [
            (row[0], row[1], row[2], row[3], round(float(row[4]), 6))
            for row in sql_rows
        ]
        if helper_simple != sql_simple:
            raise RuntimeError(
                f"sorted_heap_graph_rag(path depth={depth}) mismatch on query#{idx}: "
                f"helper={helper_simple} sql={sql_simple}"
            )


def print_result(table: str, case: str, depth: int, p50: float, avg: float, hits: float, reads: float, root: str, rows: int, hit1: float, hitk: float) -> None:
    print(
        "result|"
        f"table={table}|case={case}|depth={depth}|"
        f"p50_ms={p50:.3f}|avg_ms={avg:.3f}|"
        f"shared_hits={hits:.1f}|shared_reads={reads:.1f}|"
        f"root={root}|rows={rows}|hit1_pct={hit1:.1f}|hitk_pct={hitk:.1f}"
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tmp-root", default="/tmp")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--num-pairs", type=int, default=5000)
    ap.add_argument("--max-depth", type=int, default=5)
    ap.add_argument("--query-count", type=int, default=32)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--dim", type=int, default=384)
    ap.add_argument("--hop-weight", type=float, default=0.15)
    ap.add_argument("--ann-k", type=int, default=64)
    ap.add_argument("--top-k", type=int, default=10)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--ef-search", type=int, default=128)
    ap.add_argument("--ef-construction", type=int, default=200)
    ap.add_argument("--m", type=int, default=24)
    ap.add_argument("--shared-buffers-mb", type=int, default=64)
    ap.add_argument("--max-wal-size-gb", type=int, default=4)
    ap.add_argument("--maintenance-work-mem-mb", type=int, default=0)
    ap.add_argument("--table-scope", choices=("all", "sorted_heap_only"), default="all")
    ap.add_argument(
        "--stop-after",
        choices=("none", "generate_csv", "load_data", "build_indexes", "analyze"),
        default="none",
    )
    ap.add_argument("--backend-mode", choices=("fresh", "reuse"), default="fresh")
    ap.add_argument("--install-cmd", default="")
    ap.add_argument("--reuse-temp", default="")
    ap.add_argument("--keep-temp", action="store_true")
    ap.add_argument("--stream-copy", action="store_true")
    args = ap.parse_args()

    root_dir = Path(__file__).resolve().parent.parent
    tmp_root = Path(args.tmp_root).resolve()
    install_cmd = shlex.split(args.install_cmd) if args.install_cmd else None
    reusing = bool(args.reuse_temp)
    if reusing:
        tmp = Path(args.reuse_temp).resolve()
        meta = read_meta(tmp)
        pg_bindir = subprocess.check_output(["pg_config", "--bindir"], text=True).strip()
        port = int(meta["port"])
        num_pairs = int(meta["num_pairs"])
        max_depth = int(meta["max_depth"])
        dim = int(meta["dim"])
        build_hop_weight = float(meta.get("hop_weight", args.hop_weight))
        build_ef_construction = int(meta.get("ef_construction", args.ef_construction))
        build_m = int(meta.get("m", args.m))
        build_table_scope = str(meta.get("table_scope", args.table_scope))
        build_heap_retained = bool(meta.get("heap_retained", True))
        build_stream_copy = bool(meta.get("stream_copy", False))
        build_shared_buffers_mb = int(meta.get("shared_buffers_mb", args.shared_buffers_mb))
        build_max_wal_size_gb = int(meta.get("max_wal_size_gb", args.max_wal_size_gb))
        build_maintenance_work_mem_mb = int(meta.get("maintenance_work_mem_mb", args.maintenance_work_mem_mb))
        csv_path = tmp / "facts_multidepth.csv"
    else:
        port = args.port or base.pick_port()
        tmp, pg_bindir = base.init_temp_cluster(
            root_dir,
            port,
            tmp_root,
            args.shared_buffers_mb,
            install_cmd,
            max_wal_size_gb=args.max_wal_size_gb,
            maintenance_work_mem_mb=args.maintenance_work_mem_mb,
        )
        num_pairs = args.num_pairs
        max_depth = args.max_depth
        dim = args.dim
        build_hop_weight = args.hop_weight
        build_ef_construction = args.ef_construction
        build_m = args.m
        build_table_scope = args.table_scope
        build_heap_retained = build_table_scope == "all"
        build_stream_copy = args.stream_copy
        build_shared_buffers_mb = args.shared_buffers_mb
        build_max_wal_size_gb = args.max_wal_size_gb
        build_maintenance_work_mem_mb = args.maintenance_work_mem_mb
        csv_path = tmp / "facts_multidepth.csv"
        write_meta(
            tmp,
            {
                "port": port,
                "num_pairs": num_pairs,
                "max_depth": max_depth,
                "dim": dim,
                "hop_weight": build_hop_weight,
                "ef_construction": build_ef_construction,
                "m": build_m,
                "table_scope": build_table_scope,
                "heap_retained": build_heap_retained,
                "stream_copy": build_stream_copy,
                "shared_buffers_mb": build_shared_buffers_mb,
                "max_wal_size_gb": build_max_wal_size_gb,
                "maintenance_work_mem_mb": build_maintenance_work_mem_mb,
                "stage_reached": "init",
            },
        )

    try:
        if not reusing:
            if build_stream_copy:
                print(
                    "stage|name=generate_csv|elapsed_s=0.000|csv_bytes=0|streamed=true",
                    flush=True,
                )
                csv_bytes = 0
            else:
                t_gen_start = time.perf_counter()
                generate_csv(csv_path, num_pairs, max_depth, dim, build_hop_weight)
                t_gen_end = time.perf_counter()
                csv_bytes = csv_path.stat().st_size
                print(
                    "stage|name=generate_csv|"
                    f"elapsed_s={t_gen_end - t_gen_start:.3f}|"
                    f"csv_bytes={csv_bytes}",
                    flush=True,
                )
            write_meta(
                tmp,
                {
                    **read_meta(tmp),
                    "stage_reached": "generate_csv",
                    "csv_bytes": csv_bytes,
                },
            )
            if args.stop_after == "generate_csv":
                return

        t_query_start = time.perf_counter()
        queries = build_queries(num_pairs, args.query_count, max_depth, dim, args.seed, build_hop_weight)
        t_query_end = time.perf_counter()
        print(
            "stage|name=build_queries|"
            f"elapsed_s={t_query_end - t_query_start:.3f}|"
            f"queries={len(queries)}",
            flush=True,
        )

        conn = base.connect(tmp, port)
        cur = conn.cursor()
        try:
            cur.execute("SET jit = off")
            cur.execute("SET sorted_hnsw.shared_cache = off")
            cur.execute(f"SET sorted_hnsw.ef_search = {args.ef_search}")
            if not reusing:
                base.bootstrap_schema(cur, dim)

            if not reusing:
                t_load_start = time.perf_counter()
                if build_stream_copy:
                    base.load_data_fileobj(
                        cur,
                        FactCsvStream(num_pairs, max_depth, dim, build_hop_weight),
                        retain_heap=build_heap_retained,
                    )
                else:
                    base.load_data(cur, csv_path, retain_heap=build_heap_retained)
                t_load_end = time.perf_counter()
                csv_removed = False
                if not build_stream_copy and csv_path.exists():
                    csv_path.unlink()
                    csv_removed = True
                print(
                    "stage|name=load_data|"
                    f"elapsed_s={t_load_end - t_load_start:.3f}|"
                    f"csv_removed={str(csv_removed).lower()}|"
                    f"streamed={str(build_stream_copy).lower()}",
                    flush=True,
                )
                write_meta(
                    tmp,
                    {
                        **read_meta(tmp),
                        "stage_reached": "load_data",
                        "csv_removed_after_load": csv_removed,
                    },
                )
                if args.stop_after == "load_data":
                    return

            if not reusing:
                t_build_start = time.perf_counter()
                base.build_indexes(
                    cur,
                    build_ef_construction,
                    m=build_m,
                    build_heap_index=(build_table_scope == "all"),
                    build_sorted_heap_index=True,
                )
                t_build_end = time.perf_counter()
                print(
                    "stage|name=build_indexes|"
                    f"elapsed_s={t_build_end - t_build_start:.3f}",
                    flush=True,
                )
                write_meta(tmp, {**read_meta(tmp), "stage_reached": "build_indexes"})
                if args.stop_after == "build_indexes":
                    return
            else:
                meta = read_meta(tmp)
                if meta.get("stage_reached") not in {"build_indexes", "analyze"}:
                    raise RuntimeError(
                        f"reuse-temp requires a temp cluster stopped after build_indexes/analyze, got stage={meta.get('stage_reached')}"
                    )

            t_analyze_start = time.perf_counter()
            if build_heap_retained:
                cur.execute("ANALYZE facts_heap")
            cur.execute("ANALYZE facts_sh")
            t_analyze_end = time.perf_counter()
            print(
                "stage|name=analyze|"
                f"elapsed_s={t_analyze_end - t_analyze_start:.3f}",
                flush=True,
            )
            write_meta(tmp, {**read_meta(tmp), "stage_reached": "analyze"})
            if args.stop_after == "analyze":
                return

            if args.backend_mode == "fresh":
                cur.close()
                conn.close()
                conn = base.connect(tmp, port)
                cur = conn.cursor()
                cur.execute("SET jit = off")
                cur.execute("SET sorted_hnsw.shared_cache = off")
                cur.execute(f"SET sorted_hnsw.ef_search = {args.ef_search}")

            print("============================================================")
            print("graph rag multidepth benchmark")
            print("============================================================")
            print(f"port:             {port}")
            print(f"num_pairs:        {num_pairs}")
            print(f"max_depth:        {max_depth}")
            print(f"rows:             {num_pairs * max_depth}")
            print(f"dim:              {dim}")
            print(f"hop_weight:       {build_hop_weight}")
            print(f"query_count:      {args.query_count}")
            print(f"runs:             {args.runs}")
            print(f"ann_k:            {args.ann_k}")
            print(f"top_k:            {args.top_k}")
            print(f"ef_search:        {args.ef_search}")
            print(f"ef_construction:  {build_ef_construction}")
            print(f"m:                {build_m}")
            print(f"shared_buffers:   {build_shared_buffers_mb}MB")
            print(f"max_wal_size:     {build_max_wal_size_gb}GB")
            print(f"maintenance_work_mem: {build_maintenance_work_mem_mb}MB")
            print(f"table_scope:      {build_table_scope}")
            print(f"heap_retained:    {'yes' if build_heap_retained else 'no'}")
            print(f"reuse_temp:       {str(tmp) if reusing else 'no'}")
            print(f"backend_mode:     {args.backend_mode}")
            print()

            for depth in range(1, max_depth + 1):
                if 3 <= depth <= 5:
                    verify_unified_equivalence(cur, queries, depth, args.ann_k, args.top_k)

                cases: list[tuple[str, base.QueryCase]]
                if build_table_scope == "all":
                    cases = [
                        ("facts_heap", make_sql_path_case(depth, args.ann_k, args.top_k)),
                        ("facts_sh", make_sql_path_case(depth, args.ann_k, args.top_k)),
                        ("facts_sh", make_unified_case(depth, args.ann_k, args.top_k)),
                    ]
                else:
                    cases = [
                        ("facts_sh", make_sql_path_case(depth, args.ann_k, args.top_k)),
                        ("facts_sh", make_unified_case(depth, args.ann_k, args.top_k)),
                    ]

                for table, case in cases:
                    print(f"running|table={table}|case={case.name}|depth={depth}", flush=True)
                    p50, avg, hits, reads, root, rows = base.measure_case(cur, table, case, queries, args.runs)
                    hit1, hitk = measure_quality(cur, table, case, queries, num_pairs, depth)
                    print_result(table, case.name, depth, p50, avg, hits, reads, root, rows, hit1, hitk)
        finally:
            cur.close()
            conn.close()
    finally:
        if args.keep_temp or reusing:
            print(f"kept_temp_cluster={tmp}")
        else:
            base.stop_temp_cluster(tmp, pg_bindir)


if __name__ == "__main__":
    main()
