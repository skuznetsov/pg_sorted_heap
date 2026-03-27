#!/usr/bin/env python3
"""
Benchmark segmented fact-shaped GraphRAG across multiple sorted_heap shards.

The current GraphRAG C helpers require a concrete sorted_heap table, not a
partitioned-table parent. This harness measures a practical first-step
segmentation strategy:

  1. split the synthetic multidepth corpus into N independent sorted_heap shards
  2. build one sorted_hnsw index per shard
  3. route each query either:
       - to all shards (worst-case fanout), or
       - to the owning shard only (synthetic lower bound / perfect pruning)
  4. merge shard-local top-k rows globally by path distance

This benchmark can compare the older Python-side shard fanout/merge path
against the SQL-level segmented beta wrapper.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import shlex
import statistics
import subprocess
import time
from pathlib import Path

import bench_graph_rag as base
import bench_graph_rag_multidepth as md


META_NAME = "multidepth_segmented_meta.json"
SEGMENT_ROUTE_NAME = "segmented_bench"
SEGMENT_EXACT_ROUTE_NAME = "segmented_exact_bench"


def meta_path(tmp: Path) -> Path:
    return tmp / META_NAME


def write_meta(tmp: Path, payload: dict) -> None:
    meta_path(tmp).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_meta(tmp: Path) -> dict:
    return json.loads(meta_path(tmp).read_text(encoding="utf-8"))


def shard_table_name(shard_id: int) -> str:
    return f"facts_sh_s{shard_id}"


def shard_index_name(shard_id: int) -> str:
    return f"{shard_table_name(shard_id)}_ann_idx"


def shard_for_person(person_id: int, num_pairs: int, shards: int) -> int:
    return min(shards - 1, ((person_id - 1) * shards) // num_pairs)


def shard_person_bounds(shard_id: int, num_pairs: int, shards: int) -> tuple[int, int]:
    start = (shard_id * num_pairs) // shards + 1
    end = ((shard_id + 1) * num_pairs) // shards
    return start, end


def generate_sharded_csvs(
    tmp: Path,
    num_pairs: int,
    max_depth: int,
    dim: int,
    hop_weight: float,
    shards: int,
) -> tuple[list[Path], list[int]]:
    shard_paths = [tmp / f"facts_multidepth_s{shard_id}.csv" for shard_id in range(shards)]
    shard_counts = [0 for _ in range(shards)]
    handles = []
    writers = []
    hop_cache = {h: md.hop_base(h, dim) for h in range(1, max_depth + 1)}

    try:
        for path in shard_paths:
            handle = open(path, "w", encoding="utf-8", newline="")
            handles.append(handle)
            writers.append(csv.writer(handle))

        for person_id in range(1, num_pairs + 1):
            shard_id = shard_for_person(person_id, num_pairs, shards)
            person_vals = md.person_base(person_id, dim)
            writer = writers[shard_id]
            prev = person_id

            for hop in range(1, max_depth + 1):
                target_id = hop * num_pairs + person_id
                vals = [
                    person_vals[idx] + hop_weight * hop_cache[hop][idx]
                    for idx in range(dim)
                ]
                payload = f"Person_{person_id} rel_{hop} node_{hop}_{person_id}."
                writer.writerow([prev, hop, target_id, md.vec_to_svec(vals), payload])
                prev = target_id
                shard_counts[shard_id] += 1
    finally:
        for handle in handles:
            handle.close()

    return shard_paths, shard_counts


class ShardedFactCsvStream(io.TextIOBase):
    def __init__(
        self,
        shard_id: int,
        num_pairs: int,
        max_depth: int,
        dim: int,
        hop_weight: float,
        shards: int,
    ) -> None:
        self.shard_id = shard_id
        self.num_pairs = num_pairs
        self.max_depth = max_depth
        self.dim = dim
        self.hop_weight = hop_weight
        self.shards = shards
        self.hop_cache = {h: md.hop_base(h, dim) for h in range(1, max_depth + 1)}
        self.person_start, self.person_end = shard_person_bounds(shard_id, num_pairs, shards)
        self.person_id = self.person_start
        self.hop = 1
        self.person_vals: list[float] | None = None
        self.buf = ""
        self.done = False

    def readable(self) -> bool:
        return True

    def _next_line(self) -> str | None:
        if self.person_id > self.person_end:
            return None

        if self.person_vals is None:
            self.person_vals = md.person_base(self.person_id, self.dim)

        hop = self.hop
        prev = self.person_id if hop == 1 else (hop - 1) * self.num_pairs + self.person_id
        target_id = hop * self.num_pairs + self.person_id
        vals = [
            self.person_vals[idx] + self.hop_weight * self.hop_cache[hop][idx]
            for idx in range(self.dim)
        ]
        payload = f"Person_{self.person_id} rel_{hop} node_{hop}_{self.person_id}."
        line = f'{prev},{hop},{target_id},"{md.vec_to_svec(vals)}",{payload}\n'

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


def bootstrap_sharded_schema(cur, dim: int, shards: int) -> None:
    cur.execute("CREATE EXTENSION pg_sorted_heap")
    for shard_id in range(shards):
        cur.execute(
            f"""
            CREATE TABLE {shard_table_name(shard_id)} (
                entity_id   int4 NOT NULL,
                relation_id int2 NOT NULL,
                target_id   int4 NOT NULL,
                embedding   svec({dim}) NOT NULL,
                payload     text NOT NULL,
                PRIMARY KEY (entity_id, relation_id, target_id)
            ) USING sorted_heap
            """
        )


def load_sharded_data(cur, shard_paths: list[Path], post_load_op: str) -> None:
    if post_load_op not in ("compact", "merge", "none"):
        raise ValueError(f"unsupported post_load_op: {post_load_op}")

    for shard_id, path in enumerate(shard_paths):
        with open(path, "r", encoding="utf-8") as f:
            cur.copy_expert(
                f"""
                COPY {shard_table_name(shard_id)} (
                    entity_id, relation_id, target_id, embedding, payload
                )
                FROM STDIN WITH (FORMAT csv)
                """,
                f,
            )

        if post_load_op == "compact":
            cur.execute(f"SELECT sorted_heap_compact('{shard_table_name(shard_id)}'::regclass)")
        elif post_load_op == "merge":
            cur.execute(f"SELECT sorted_heap_merge('{shard_table_name(shard_id)}'::regclass)")

        cur.execute(f"ANALYZE {shard_table_name(shard_id)}")
        path.unlink()


def load_sharded_data_stream(
    cur,
    num_pairs: int,
    max_depth: int,
    dim: int,
    hop_weight: float,
    shards: int,
    post_load_op: str,
) -> list[int]:
    if post_load_op not in ("compact", "merge", "none"):
        raise ValueError(f"unsupported post_load_op: {post_load_op}")

    shard_counts: list[int] = []
    for shard_id in range(shards):
        start, end = shard_person_bounds(shard_id, num_pairs, shards)
        person_count = max(0, end - start + 1)
        shard_counts.append(person_count * max_depth)
        cur.copy_expert(
            f"""
            COPY {shard_table_name(shard_id)} (
                entity_id, relation_id, target_id, embedding, payload
            )
            FROM STDIN WITH (FORMAT csv)
            """,
            ShardedFactCsvStream(shard_id, num_pairs, max_depth, dim, hop_weight, shards),
        )

        if post_load_op == "compact":
            cur.execute(f"SELECT sorted_heap_compact('{shard_table_name(shard_id)}'::regclass)")
        elif post_load_op == "merge":
            cur.execute(f"SELECT sorted_heap_merge('{shard_table_name(shard_id)}'::regclass)")

        cur.execute(f"ANALYZE {shard_table_name(shard_id)}")

    return shard_counts


def register_segment_ranges(cur, num_pairs: int, shards: int, route_name: str) -> None:
    cur.execute("SELECT sorted_heap_graph_segment_unregister(%s)", (route_name,))
    for shard_id in range(shards):
        start, end = shard_person_bounds(shard_id, num_pairs, shards)
        cur.execute(
            "SELECT sorted_heap_graph_segment_register(%s, %s::regclass, %s, %s)",
            (route_name, shard_table_name(shard_id), start, end),
        )


def register_exact_segment_keys(cur, shards: int, route_name: str) -> None:
    cur.execute("SELECT sorted_heap_graph_exact_unregister(%s)", (route_name,))
    for shard_id in range(shards):
        cur.execute(
            "SELECT sorted_heap_graph_exact_register(%s, %s, %s::regclass, %s)",
            (route_name, f"shard_{shard_id}", shard_table_name(shard_id), 0),
        )


def build_sharded_indexes(
    cur,
    shards: int,
    ef_construction: int,
    m: int,
    build_sq8: str,
) -> None:
    if build_sq8 not in ("default", "on", "off"):
        raise ValueError(f"unsupported build_sq8: {build_sq8}")

    if build_sq8 != "default":
        cur.execute(f"SET sorted_hnsw.build_sq8 = {'on' if build_sq8 == 'on' else 'off'}")
    try:
        for shard_id in range(shards):
            cur.execute(
                f"""
                CREATE INDEX {shard_index_name(shard_id)}
                ON {shard_table_name(shard_id)}
                USING sorted_hnsw (embedding)
                WITH (m = {m}, ef_construction = {ef_construction})
                """
            )
            cur.execute(f"ANALYZE {shard_table_name(shard_id)}")
    finally:
        if build_sq8 != "default":
            cur.execute("RESET sorted_hnsw.build_sq8")


def route_tables(query: md.DeepQuery, num_pairs: int, shards: int, route_mode: str) -> list[str]:
    if route_mode == "all":
        return [shard_table_name(shard_id) for shard_id in range(shards)]
    if route_mode == "exact":
        shard_id = shard_for_person(query.person_id, num_pairs, shards)
        return [shard_table_name(shard_id)]
    raise ValueError(f"unsupported route_mode: {route_mode}")


def regclass_array_literal(tables: list[str]) -> str:
    return "ARRAY[" + ",".join(f"'{table}'::regclass" for table in tables) + "]"


def merged_shard_rows(
    cur,
    tables: list[str],
    num_pairs: int,
    shards: int,
    depth: int,
    person_id: int,
    query_vec: str,
    ann_k: int,
    top_k: int,
    merge_mode: str,
) -> list[tuple[int, int, int, str, float]]:
    if merge_mode == "python":
        case = md.make_unified_case(depth, ann_k, top_k)
        rows: list[tuple[int, int, int, str, float]] = []

        for table in tables:
            sql = case.sql_template.format(table=table)
            cur.execute(sql, (query_vec,))
            rows.extend(cur.fetchall())

        rows.sort(key=lambda row: (float(row[4]), row[0], row[1], row[2]))
        return rows[:top_k]

    if merge_mode == "sql":
        path = md.relation_path_literal(depth)
        sql = f"""
        SELECT entity_id, relation_id, target_id, payload, distance
        FROM sorted_heap_graph_rag_segmented(
          {regclass_array_literal(tables)},
          %s::svec,
          relation_path := {path},
          ann_k := {ann_k},
          top_k := {top_k},
          score_mode := 'path',
          limit_rows := 0
        )
        ORDER BY distance, entity_id, relation_id, target_id
        """
        cur.execute(sql, (query_vec,))
        return cur.fetchall()

    if merge_mode == "routed":
        path = md.relation_path_literal(depth)
        sql = f"""
        SELECT entity_id, relation_id, target_id, payload, distance
        FROM sorted_heap_graph_rag_routed(
          %s,
          %s,
          %s::svec,
          relation_path := {path},
          ann_k := {ann_k},
          top_k := {top_k},
          score_mode := 'path',
          limit_rows := 0
        )
        ORDER BY distance, entity_id, relation_id, target_id
        """
        cur.execute(sql, (SEGMENT_ROUTE_NAME, person_id, query_vec))
        return cur.fetchall()

    if merge_mode == "routed_exact":
        path = md.relation_path_literal(depth)
        route_key = f"shard_{shard_for_person(person_id, num_pairs, shards)}"
        sql = f"""
        SELECT entity_id, relation_id, target_id, payload, distance
        FROM sorted_heap_graph_rag_routed_exact(
          %s,
          %s,
          %s::svec,
          relation_path := {path},
          ann_k := {ann_k},
          top_k := {top_k},
          score_mode := 'path',
          limit_rows := 0
        )
        ORDER BY distance, entity_id, relation_id, target_id
        """
        cur.execute(sql, (SEGMENT_EXACT_ROUTE_NAME, route_key, query_vec))
        return cur.fetchall()

    raise ValueError(f"unsupported merge_mode: {merge_mode}")


def measure_segmented_case(
    cur,
    queries: list[md.DeepQuery],
    num_pairs: int,
    shards: int,
    depth: int,
    ann_k: int,
    top_k: int,
    runs: int,
    route_mode: str,
    merge_mode: str,
) -> tuple[float, float, float, float, float]:
    latencies: list[float] = []
    returned_rows: list[int] = []
    hit1 = 0
    hitk = 0

    for _ in range(runs):
        for query in queries:
            tables = route_tables(query, num_pairs, shards, route_mode)
            t0 = time.perf_counter()
            rows = merged_shard_rows(
                cur,
                tables,
                num_pairs,
                shards,
                depth,
                query.person_id,
                query.vectors[depth],
                ann_k,
                top_k,
                merge_mode,
            )
            t1 = time.perf_counter()

            expected = depth * num_pairs + query.person_id
            targets = [row[2] for row in rows]

            latencies.append((t1 - t0) * 1000.0)
            returned_rows.append(len(rows))
            if targets:
                if targets[0] == expected:
                    hit1 += 1
                if expected in targets:
                    hitk += 1

    n = len(queries) * runs
    if n == 0:
        return 0.0, 0.0, 0.0, 0.0, 0.0
    return (
        statistics.median(latencies),
        statistics.fmean(latencies),
        statistics.fmean(returned_rows),
        (hit1 * 100.0) / n,
        (hitk * 100.0) / n,
    )


def print_stage(name: str, elapsed_s: float, extra: str = "") -> None:
    suffix = f"|{extra}" if extra else ""
    print(f"stage|name={name}|elapsed_s={elapsed_s:.3f}{suffix}", flush=True)


def print_result(
    route_mode: str,
    merge_mode: str,
    shards: int,
    depth: int,
    p50: float,
    avg: float,
    rows: float,
    hit1: float,
    hitk: float,
) -> None:
    print(
        "result|"
        "table=segmented|"
        "case=graph_rag_path|"
        f"route={route_mode}|"
        f"merge={merge_mode}|"
        f"shards={shards}|"
        f"depth={depth}|"
        f"p50_ms={p50:.3f}|"
        f"avg_ms={avg:.3f}|"
        f"rows={rows:.1f}|"
        f"hit1_pct={hit1:.1f}|"
        f"hitk_pct={hitk:.1f}"
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
    ap.add_argument("--build-sq8", choices=("default", "on", "off"), default="default")
    ap.add_argument("--shared-buffers-mb", type=int, default=64)
    ap.add_argument("--max-wal-size-gb", type=int, default=4)
    ap.add_argument("--maintenance-work-mem-mb", type=int, default=0)
    ap.add_argument("--post-load-op", choices=("compact", "merge", "none"), default="compact")
    ap.add_argument("--shards", type=int, default=4)
    ap.add_argument("--route", choices=("all", "exact", "both"), default="both")
    ap.add_argument(
        "--merge-mode",
        choices=("python", "sql", "routed", "routed_exact"),
        default="python",
    )
    ap.add_argument("--backend-mode", choices=("fresh", "reuse"), default="fresh")
    ap.add_argument(
        "--stop-after",
        choices=("none", "generate_csv", "load_data", "build_indexes", "analyze"),
        default="none",
    )
    ap.add_argument("--install-cmd", default="")
    ap.add_argument("--reuse-temp", default="")
    ap.add_argument("--keep-temp", action="store_true")
    ap.add_argument("--stream-copy", action="store_true")
    args = ap.parse_args()

    if args.shards < 1:
        raise ValueError("--shards must be >= 1")
    if args.merge_mode in {"routed", "routed_exact"} and args.route == "all":
        raise ValueError(f"--merge-mode {args.merge_mode} requires --route exact or --route both")

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
        hop_weight = float(meta["hop_weight"])
        ef_construction = int(meta["ef_construction"])
        m = int(meta["m"])
        build_sq8 = str(meta["build_sq8"])
        shared_buffers_mb = int(meta["shared_buffers_mb"])
        max_wal_size_gb = int(meta["max_wal_size_gb"])
        maintenance_work_mem_mb = int(meta["maintenance_work_mem_mb"])
        post_load_op = str(meta["post_load_op"])
        stream_copy = bool(meta.get("stream_copy", False))
        shards = int(meta["shards"])
        shard_counts = [int(v) for v in meta["shard_counts"]]
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
        hop_weight = args.hop_weight
        ef_construction = args.ef_construction
        m = args.m
        build_sq8 = args.build_sq8
        shared_buffers_mb = args.shared_buffers_mb
        max_wal_size_gb = args.max_wal_size_gb
        maintenance_work_mem_mb = args.maintenance_work_mem_mb
        post_load_op = args.post_load_op
        stream_copy = args.stream_copy
        shards = args.shards
        shard_counts: list[int] = []
        write_meta(
            tmp,
            {
                "port": port,
                "num_pairs": num_pairs,
                "max_depth": max_depth,
                "dim": dim,
                "hop_weight": hop_weight,
                "ef_construction": ef_construction,
                "m": m,
                "build_sq8": build_sq8,
                "shared_buffers_mb": shared_buffers_mb,
                "max_wal_size_gb": max_wal_size_gb,
                "maintenance_work_mem_mb": maintenance_work_mem_mb,
                "post_load_op": post_load_op,
                "stream_copy": stream_copy,
                "shards": shards,
                "shard_counts": shard_counts,
                "stage_reached": "init",
            },
        )

    try:
        shard_paths: list[Path] = []
        if not reusing:
            if stream_copy:
                csv_bytes = 0
                print_stage("generate_csv", 0.0, f"csv_bytes=0|shards={shards}|streamed=true")
            else:
                t_gen_start = time.perf_counter()
                shard_paths, shard_counts = generate_sharded_csvs(
                    tmp, num_pairs, max_depth, dim, hop_weight, shards
                )
                t_gen_end = time.perf_counter()
                csv_bytes = sum(path.stat().st_size for path in shard_paths)
                print_stage(
                    "generate_csv",
                    t_gen_end - t_gen_start,
                    f"csv_bytes={csv_bytes}|shards={shards}",
                )
            write_meta(
                tmp,
                {
                    **read_meta(tmp),
                    "stage_reached": "generate_csv",
                    "csv_bytes": csv_bytes,
                    "shard_counts": shard_counts,
                },
            )
            if args.stop_after == "generate_csv":
                return
        else:
            meta = read_meta(tmp)
            shard_counts = [int(v) for v in meta["shard_counts"]]

        t_query_start = time.perf_counter()
        queries = md.build_queries(
            num_pairs,
            args.query_count,
            max_depth,
            dim,
            args.seed,
            hop_weight,
        )
        t_query_end = time.perf_counter()
        print_stage("build_queries", t_query_end - t_query_start, f"queries={len(queries)}")

        conn = base.connect(tmp, port)
        cur = conn.cursor()
        try:
            cur.execute("SET jit = off")
            cur.execute("SET sorted_hnsw.shared_cache = off")
            cur.execute(f"SET sorted_hnsw.ef_search = {args.ef_search}")
            if not reusing:
                bootstrap_sharded_schema(cur, dim, shards)

                t_load_start = time.perf_counter()
                if stream_copy:
                    shard_counts = load_sharded_data_stream(
                        cur,
                        num_pairs,
                        max_depth,
                        dim,
                        hop_weight,
                        shards,
                        post_load_op,
                    )
                else:
                    load_sharded_data(cur, shard_paths, post_load_op)
                t_load_end = time.perf_counter()
                print_stage(
                    "load_data",
                    t_load_end - t_load_start,
                    f"csv_removed={'false' if stream_copy else 'true'}|streamed={'true' if stream_copy else 'false'}",
                )
                register_segment_ranges(cur, num_pairs, shards, SEGMENT_ROUTE_NAME)
                register_exact_segment_keys(cur, shards, SEGMENT_EXACT_ROUTE_NAME)
                write_meta(tmp, {**read_meta(tmp), "stage_reached": "load_data", "shard_counts": shard_counts})
                if args.stop_after == "load_data":
                    return

                t_build_start = time.perf_counter()
                build_sharded_indexes(cur, shards, ef_construction, m, build_sq8)
                t_build_end = time.perf_counter()
                print_stage("build_indexes", t_build_end - t_build_start)
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
            for shard_id in range(shards):
                cur.execute(f"ANALYZE {shard_table_name(shard_id)}")
            t_analyze_end = time.perf_counter()
            print_stage("analyze", t_analyze_end - t_analyze_start)
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
            print("graph rag multidepth segmented benchmark")
            print("============================================================")
            print(f"port:             {port}")
            print(f"num_pairs:        {num_pairs}")
            print(f"max_depth:        {max_depth}")
            print(f"rows:             {num_pairs * max_depth}")
            print(f"dim:              {dim}")
            print(f"hop_weight:       {hop_weight}")
            print(f"query_count:      {args.query_count}")
            print(f"runs:             {args.runs}")
            print(f"ann_k:            {args.ann_k}")
            print(f"top_k:            {args.top_k}")
            print(f"ef_search:        {args.ef_search}")
            print(f"ef_construction:  {ef_construction}")
            print(f"m:                {m}")
            print(f"build_sq8:        {build_sq8}")
            print(f"shards:           {shards}")
            print(f"route:            {args.route}")
            print(f"merge_mode:       {args.merge_mode}")
            print(f"post_load_op:     {post_load_op}")
            print(f"stream_copy:      {'yes' if stream_copy else 'no'}")
            print(f"reuse_temp:       {str(tmp) if reusing else 'no'}")
            print(f"shard_rows:       {','.join(str(count) for count in shard_counts)}")
            print("")

            routes = ["all", "exact"] if args.route == "both" else [args.route]
            for route_mode in routes:
                if args.merge_mode in {"routed", "routed_exact"} and route_mode != "exact":
                    continue
                for depth in range(1, max_depth + 1):
                    print(
                        f"running|table=segmented|route={route_mode}|depth={depth}|shards={shards}",
                        flush=True,
                    )
                    p50, avg, rows, hit1, hitk = measure_segmented_case(
                        cur,
                        queries,
                        num_pairs,
                        shards,
                        depth,
                        args.ann_k,
                        args.top_k,
                        args.runs,
                        route_mode,
                        args.merge_mode,
                    )
                    print_result(route_mode, args.merge_mode, shards, depth, p50, avg, rows, hit1, hitk)
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
