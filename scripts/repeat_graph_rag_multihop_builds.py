#!/usr/bin/env python3
"""
Run multiple independent GraphRAG multihop benchmark builds and summarize
build-to-build variance for selected cases.

This wraps bench_graph_rag_multihop.py so each repeat gets a fresh temp cluster
and a fresh HNSW build. It is intentionally higher-level than
sweep_graph_rag_multihop.py, which reuses one build per ef_construction.
"""

from __future__ import annotations

import argparse
import statistics
import subprocess
import sys
from pathlib import Path


def parse_case_list(raw: str) -> list[str]:
    cases = [part.strip() for part in raw.split(",") if part.strip()]
    if not cases:
        raise ValueError("expected at least one case")
    return cases


def parse_metric_line(line: str) -> tuple[str, dict[str, float | str]] | None:
    line = line.strip()
    if not line or "|" not in line or "=" not in line:
        return None

    parts = line.split("|")
    if len(parts) < 3:
        return None

    table = parts[0]
    case = parts[1]
    if not table or not case:
        return None

    metrics: dict[str, float | str] = {"table": table, "case": case}
    for part in parts[2:]:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        try:
            metrics[key] = float(value)
        except ValueError:
            metrics[key] = value
    return f"{table}|{case}", metrics


def aggregate(values: list[float]) -> tuple[float, float, float, float]:
    return (
        statistics.median(values),
        min(values),
        max(values),
        statistics.fmean(values),
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument(
        "--cases",
        default="facts_sh|seed_expand2_rerank_rel_twohop_path_fn,"
        "facts_sh|seed_graph_rag_twohop_path_scan_fn,"
        "facts_pgv|seed_expand2_rerank_rel_pathsum_pgv,"
        "facts_zvec|seed_expand2_rerank_rel_pathsum_zvec,"
        "facts_qdrant|seed_expand2_rerank_rel_pathsum_qdrant",
    )
    ap.add_argument("--tmp-root", default="/tmp")
    ap.add_argument("--port-base", type=int, default=65400)
    ap.add_argument("--num-pairs", type=int, default=5000)
    ap.add_argument("--query-count", type=int, default=64)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--dim", type=int, default=384)
    ap.add_argument("--ann-k", type=int, default=64)
    ap.add_argument("--top-k", type=int, default=10)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--ef-search", type=int, default=128)
    ap.add_argument("--ef-construction", type=int, default=200)
    ap.add_argument("--m", type=int, default=24)
    ap.add_argument("--pgv-ef-search", type=int, default=64)
    ap.add_argument("--zvec-ef", type=int, default=64)
    ap.add_argument("--zvec-memory-limit-mb", type=int, default=8192)
    ap.add_argument("--qdrant-ef", type=int, default=64)
    ap.add_argument("--shared-buffers-mb", type=int, default=64)
    ap.add_argument("--backend-mode", choices=("fresh", "reuse"), default="fresh")
    ap.add_argument("--install-cmd", default="")
    ap.add_argument("--skip-pgvector", action="store_true")
    ap.add_argument("--skip-zvec", action="store_true")
    ap.add_argument("--skip-qdrant", action="store_true")
    args = ap.parse_args()

    if args.repeats < 1:
        raise ValueError("repeats must be >= 1")
    if args.port_base < 1024:
        raise ValueError("port-base must be >= 1024")
    if args.port_base + args.repeats > 65535:
        raise ValueError(
            f"invalid port range: port-base={args.port_base} repeats={args.repeats} exceeds 65535"
        )

    root_dir = Path(__file__).resolve().parent.parent
    bench_script = root_dir / "scripts" / "bench_graph_rag_multihop.py"
    selected_cases = set(parse_case_list(args.cases))

    all_runs: dict[str, list[dict[str, float | str]]] = {case: [] for case in selected_cases}

    print("============================================================")
    print("graph rag multihop repeated-build protocol")
    print("============================================================")
    print(f"repeats:          {args.repeats}")
    print(f"num_pairs:        {args.num_pairs}")
    print(f"dim:              {args.dim}")
    print(f"query_count:      {args.query_count}")
    print(f"runs_per_build:   {args.runs}")
    print(f"ann_k:            {args.ann_k}")
    print(f"ef_search:        {args.ef_search}")
    print(f"ef_construction:  {args.ef_construction}")
    print(f"m:                {args.m}")
    print(f"backend_mode:     {args.backend_mode}")
    print(f"cases:            {','.join(sorted(selected_cases))}")
    print()

    for repeat in range(1, args.repeats + 1):
        port = args.port_base + repeat
        cmd = [
            sys.executable,
            str(bench_script),
            "--tmp-root",
            args.tmp_root,
            "--port",
            str(port),
            "--num-pairs",
            str(args.num_pairs),
            "--query-count",
            str(args.query_count),
            "--runs",
            str(args.runs),
            "--dim",
            str(args.dim),
            "--ann-k",
            str(args.ann_k),
            "--top-k",
            str(args.top_k),
            "--seed",
            str(args.seed),
            "--ef-search",
            str(args.ef_search),
            "--ef-construction",
            str(args.ef_construction),
            "--m",
            str(args.m),
            "--pgv-ef-search",
            str(args.pgv_ef_search),
            "--zvec-ef",
            str(args.zvec_ef),
            "--zvec-memory-limit-mb",
            str(args.zvec_memory_limit_mb),
            "--qdrant-ef",
            str(args.qdrant_ef),
            "--shared-buffers-mb",
            str(args.shared_buffers_mb),
            "--backend-mode",
            args.backend_mode,
        ]
        if args.install_cmd:
            cmd.extend(["--install-cmd", args.install_cmd])
        if args.skip_pgvector:
            cmd.append("--skip-pgvector")
        if args.skip_zvec:
            cmd.append("--skip-zvec")
        if args.skip_qdrant:
            cmd.append("--skip-qdrant")

        proc = subprocess.run(cmd, check=False, text=True, capture_output=True)
        if proc.returncode != 0:
            sys.stderr.write(f"repeat_failed|repeat={repeat}|port={port}|returncode={proc.returncode}\n")
            if proc.stdout:
                sys.stderr.write("stdout:\n")
                sys.stderr.write(proc.stdout)
                if not proc.stdout.endswith("\n"):
                    sys.stderr.write("\n")
            if proc.stderr:
                sys.stderr.write("stderr:\n")
                sys.stderr.write(proc.stderr)
                if not proc.stderr.endswith("\n"):
                    sys.stderr.write("\n")
            raise SystemExit(proc.returncode)
        print(f"repeat={repeat}|port={port}")
        for line in proc.stdout.splitlines():
            parsed = parse_metric_line(line)
            if parsed is None:
                continue
            key, metrics = parsed
            if key not in selected_cases:
                continue
            all_runs[key].append(metrics)
            print(line)
        print()

    print("summary|case|metric|median|min|max|mean")
    for case in sorted(selected_cases):
        runs = all_runs[case]
        if not runs:
            print(f"summary|{case}|missing|0|0|0|0")
            continue

        for metric in ("p50_ms", "hit1_pct", "hitk_pct", "avg_rows"):
            vals = [float(run[metric]) for run in runs if metric in run]
            if not vals:
                continue
            median, min_v, max_v, mean_v = aggregate(vals)
            print(
                f"summary|{case}|{metric}|"
                f"{median:.3f}|{min_v:.3f}|{max_v:.3f}|{mean_v:.3f}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
