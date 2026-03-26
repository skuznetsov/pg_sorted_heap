#!/usr/bin/env python3
"""
Run multiple independent GraphRAG code-corpus benchmark builds and summarize
build-to-build variance for selected cases.

This wraps bench_graph_rag_code_corpus.py so each repeat gets a fresh temp
cluster and a fresh HNSW build. It runs both generic and code-aware embedding
variants, because the current frontier differs by embedding mode.
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


def run_mode(
    bench_script: Path,
    tmp_root: str,
    port: int,
    args,
    embedding_mode: str,
) -> subprocess.CompletedProcess[str]:
    cmd = [
        sys.executable,
        str(bench_script),
        "--tmp-root",
        tmp_root,
        "--port",
        str(port),
        "--runs",
        str(args.runs),
        "--dim",
        str(args.dim),
        "--ann-k",
        str(args.ann_k),
        "--top-k",
        str(args.top_k),
        "--ef-search",
        str(args.ef_search),
        "--ef-construction",
        str(args.ef_construction),
        "--m",
        str(args.m),
        "--shared-buffers-mb",
        str(args.shared_buffers_mb),
        "--backend-mode",
        args.backend_mode,
        "--embedding-mode",
        embedding_mode,
    ]
    if args.cogniformerus_root:
        cmd.extend(["--cogniformerus-root", args.cogniformerus_root])
    if args.source_dir:
        cmd.extend(["--source-dir", args.source_dir])
    if args.question_source:
        cmd.extend(["--question-source", args.question_source])
    if args.install_cmd:
        cmd.extend(["--install-cmd", args.install_cmd])
    if args.question_filter:
        cmd.extend(["--question-filter", args.question_filter])
    if embedding_mode == "generic":
        cmd.extend(["--case-filter", args.generic_case_filter])
    else:
        cmd.extend(["--case-filter", args.code_aware_case_filter])

    return subprocess.run(cmd, check=False, text=True, capture_output=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--tmp-root", default="/tmp")
    ap.add_argument("--port-base", type=int, default=65300)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--dim", type=int, default=384)
    ap.add_argument("--ann-k", type=int, default=16)
    ap.add_argument("--top-k", type=int, default=4)
    ap.add_argument("--ef-search", type=int, default=64)
    ap.add_argument("--ef-construction", type=int, default=200)
    ap.add_argument("--m", type=int, default=24)
    ap.add_argument("--shared-buffers-mb", type=int, default=64)
    ap.add_argument("--backend-mode", choices=("fresh", "reuse"), default="fresh")
    ap.add_argument("--cogniformerus-root", default="")
    ap.add_argument("--source-dir", default="")
    ap.add_argument("--question-source", default="")
    ap.add_argument("--question-filter", default="")
    ap.add_argument("--install-cmd", default="")
    ap.add_argument(
        "--generic-cases",
        default="facts_sh|prompt_summary_snippet_py,facts_sh|prompt_symbol_summary_snippet_py",
    )
    ap.add_argument(
        "--code-aware-cases",
        default="facts_sh|prompt_summary_snippet_py,facts_sh|prompt_symbol_summary_snippet_py",
    )
    args = ap.parse_args()

    if args.repeats < 1:
        raise ValueError("repeats must be >= 1")
    if args.port_base < 1024:
        raise ValueError("port-base must be >= 1024")
    if args.port_base + args.repeats * 2 > 65535:
        raise ValueError(
            f"invalid port range: port-base={args.port_base} repeats={args.repeats} exceeds 65535"
        )

    root_dir = Path(__file__).resolve().parent.parent
    bench_script = root_dir / "scripts" / "bench_graph_rag_code_corpus.py"
    generic_cases = set(parse_case_list(args.generic_cases))
    code_aware_cases = set(parse_case_list(args.code_aware_cases))
    args.generic_case_filter = "(" + "|".join(case.split("|", 1)[1] for case in sorted(generic_cases)) + ")"
    args.code_aware_case_filter = "(" + "|".join(case.split("|", 1)[1] for case in sorted(code_aware_cases)) + ")"

    all_runs: dict[str, list[dict[str, float | str]]] = {}
    for case in generic_cases:
        all_runs[f"generic|{case}"] = []
    for case in code_aware_cases:
        all_runs[f"code_aware|{case}"] = []

    print("============================================================")
    print("graph rag code corpus repeated-build protocol")
    print("============================================================")
    print(f"repeats:               {args.repeats}")
    print(f"runs_per_build:        {args.runs}")
    print(f"dim:                   {args.dim}")
    print(f"ann_k:                 {args.ann_k}")
    print(f"top_k:                 {args.top_k}")
    print(f"ef_search:             {args.ef_search}")
    print(f"ef_construction:       {args.ef_construction}")
    print(f"m:                     {args.m}")
    print(f"backend_mode:          {args.backend_mode}")
    print(f"generic_cases:         {','.join(sorted(generic_cases))}")
    print(f"code_aware_cases:      {','.join(sorted(code_aware_cases))}")
    if args.question_filter:
        print(f"question_filter:       {args.question_filter}")
    print()

    for repeat in range(1, args.repeats + 1):
        generic_port = args.port_base + (repeat - 1) * 2 + 1
        code_aware_port = generic_port + 1

        for mode, port, selected_cases in (
            ("generic", generic_port, generic_cases),
            ("code_aware", code_aware_port, code_aware_cases),
        ):
            proc = run_mode(bench_script, args.tmp_root, port, args, mode)
            if proc.returncode != 0:
                sys.stderr.write(
                    f"repeat_failed|repeat={repeat}|mode={mode}|port={port}|returncode={proc.returncode}\n"
                )
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

            print(f"repeat={repeat}|mode={mode}|port={port}")
            for line in proc.stdout.splitlines():
                parsed = parse_metric_line(line)
                if parsed is None:
                    continue
                key, metrics = parsed
                if key not in selected_cases:
                    continue
                all_runs[f"{mode}|{key}"].append(metrics)
                print(f"{mode}|{line}")
            print()

    print("summary|mode|table|case|metric|median|min|max|mean")
    for mode in ("generic", "code_aware"):
        selected_cases = generic_cases if mode == "generic" else code_aware_cases
        for case in sorted(selected_cases):
            runs = all_runs[f"{mode}|{case}"]
            table, case_name = case.split("|", 1)
            if not runs:
                print(f"summary|{mode}|{table}|{case_name}|missing|0|0|0|0")
                continue

            for metric in ("p50_ms", "keyword_pct", "full_pct", "avg_rows"):
                vals = [float(run[metric]) for run in runs if metric in run]
                if not vals:
                    continue
                median, min_v, max_v, mean_v = aggregate(vals)
                print(
                    f"summary|{mode}|{table}|{case_name}|{metric}|"
                    f"{median:.3f}|{min_v:.3f}|{max_v:.3f}|{mean_v:.3f}"
                )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
