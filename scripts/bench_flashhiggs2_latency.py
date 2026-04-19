#!/usr/bin/env python3
"""Measure in-memory FlashHadamard vs FlashHIGGS2 search latency after fit."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import random
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np


REPO_ROOT = Path(__file__).resolve().parents[1]
BENCH_PATH = REPO_ROOT / "scripts" / "bench_turboquant_retrieval.py"


def load_bench_module() -> Any:
    spec = importlib.util.spec_from_file_location("bench_turboquant_retrieval", BENCH_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {BENCH_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--vectors", type=Path, help="Base vectors (.npy or .npz with key 'base')")
    src.add_argument("--pg", action="store_true", help="Read vectors from PostgreSQL SQL queries")
    ap.add_argument("--pg-dsn", help="PostgreSQL DSN; defaults to TURBOQUANT_PG_DSN")
    ap.add_argument("--queries", type=Path, help="Query vectors (.npy) for --vectors input")
    ap.add_argument("--base-sql", help="SQL returning one vector column for base vectors")
    ap.add_argument("--query-sql", help="SQL returning one vector column for query vectors")
    ap.add_argument("--shared-sql", help="SQL returning one vector column for holdout mode")
    ap.add_argument("--query-count", type=int, default=50)
    ap.add_argument("--query-limit", type=int, default=0, help="Limit loaded queries after input")
    ap.add_argument("--metric", choices=("cosine", "ip"), default="cosine")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--k", type=int, default=20)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--warmup-runs", type=int, default=1)
    ap.add_argument("--higgs-grid-samples", type=int, default=200_000)
    ap.add_argument("--json-out", type=Path)
    return ap


def load_vectors(bench: Any, args: argparse.Namespace) -> tuple[np.ndarray, np.ndarray, str]:
    if args.vectors:
        base, queries = bench.load_numpy_dataset(args.vectors, args.queries)
        source = str(args.vectors)
    else:
        dsn = args.pg_dsn or os.environ.get("TURBOQUANT_PG_DSN")
        if not dsn:
            raise SystemExit("--pg requires --pg-dsn or TURBOQUANT_PG_DSN")
        if args.shared_sql:
            if args.base_sql or args.query_sql:
                raise SystemExit("--shared-sql cannot be combined with --base-sql/--query-sql")
            shared = bench.load_pg_query_vectors(dsn, args.shared_sql)
            base, queries = bench.split_shared_vectors(shared, args.query_count, args.seed)
            source = "postgresql(shared_sql)"
        else:
            if not args.base_sql or not args.query_sql:
                raise SystemExit("--base-sql and --query-sql are required with --pg")
            base = bench.load_pg_query_vectors(dsn, args.base_sql)
            queries = bench.load_pg_query_vectors(dsn, args.query_sql)
            source = "postgresql"
    if args.query_limit > 0:
        queries = queries[: args.query_limit]
    return base, queries, source


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    return float(np.percentile(np.asarray(values, dtype=np.float64), pct))


def summarize(name: str, latencies: list[float]) -> dict[str, float | str | int]:
    return {
        "method": name,
        "count": len(latencies),
        "p50_ms": percentile(latencies, 50),
        "p90_ms": percentile(latencies, 90),
        "p95_ms": percentile(latencies, 95),
        "avg_ms": float(sum(latencies) / max(1, len(latencies))),
        "min_ms": float(min(latencies)) if latencies else 0.0,
        "max_ms": float(max(latencies)) if latencies else 0.0,
    }


def measure_method(method: Any, queries: np.ndarray, k: int, runs: int) -> list[float]:
    latencies: list[float] = []
    for _ in range(runs):
        for query in queries:
            t0 = time.perf_counter()
            found = method.search(query, k)
            latencies.append((time.perf_counter() - t0) * 1000.0)
            if len(found) == 0:
                raise RuntimeError(f"{method.name} returned no results")
    return latencies


def main() -> int:
    args = build_arg_parser().parse_args()
    if args.runs <= 0 or args.warmup_runs < 0:
        raise SystemExit("--runs must be positive and --warmup-runs must be non-negative")
    bench = load_bench_module()
    base, queries, source = load_vectors(bench, args)
    if args.metric == "cosine":
        base = bench.normalize_rows(base)
        queries = bench.normalize_rows(queries)

    scalar = bench.TurboQuantBlock32PackedTopKMethod(4, args.seed, group_size=16)
    higgs2 = bench.TurboQuantBlockHiggs2PackedMethod(
        4,
        args.seed,
        group_size=16,
        grid_samples=args.higgs_grid_samples,
    )
    methods = [("flashhadamard16", scalar), ("flashhiggs2", higgs2)]
    fit_ms: dict[str, float] = {}
    for name, method in methods:
        t0 = time.perf_counter()
        method.fit(base)
        fit_ms[name] = (time.perf_counter() - t0) * 1000.0

    rng = random.Random(args.seed)
    for _, method in methods:
        measure_method(method, queries, args.k, args.warmup_runs)

    latencies = {name: [] for name, _ in methods}
    for _ in range(args.runs):
        order = methods[:]
        rng.shuffle(order)
        for name, method in order:
            latencies[name].extend(measure_method(method, queries, args.k, 1))

    rows = []
    for name, _ in methods:
        row = summarize(name, latencies[name])
        row["fit_ms"] = fit_ms[name]
        rows.append(row)

    print(
        f"flashhiggs2 latency | source={source} metric={args.metric} "
        f"base={base.shape[0]} queries={queries.shape[0]} dim={base.shape[1]} "
        f"k={args.k} runs={args.runs} warmup_runs={args.warmup_runs}"
    )
    print(
        f"{'method':<18} {'fit_ms':>10} {'count':>7} {'p50_ms':>9} "
        f"{'p90_ms':>9} {'p95_ms':>9} {'avg_ms':>9} {'max_ms':>9}"
    )
    for row in rows:
        print(
            f"{row['method']:<18} {row['fit_ms']:>10.1f} {row['count']:>7} "
            f"{row['p50_ms']:>9.3f} {row['p90_ms']:>9.3f} {row['p95_ms']:>9.3f} "
            f"{row['avg_ms']:>9.3f} {row['max_ms']:>9.3f}"
        )
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(
            json.dumps(
                {
                    "source": source,
                    "metric": args.metric,
                    "base_count": int(base.shape[0]),
                    "query_count": int(queries.shape[0]),
                    "dim": int(base.shape[1]),
                    "k": args.k,
                    "runs": args.runs,
                    "warmup_runs": args.warmup_runs,
                    "results": rows,
                },
                indent=2,
            )
            + "\n"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
