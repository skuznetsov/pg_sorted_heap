#!/usr/bin/env python3
"""Run a reusable-fit FlashHadamard vs FlashHIGGS2 SQ8-rerank M sweep."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
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


def parse_m_values(raw: str) -> list[int]:
    values = sorted({int(part.strip()) for part in raw.split(",") if part.strip()})
    if not values or values[0] <= 0:
        raise argparse.ArgumentTypeError("--m-values must contain positive integers")
    return values


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--vectors", type=Path, help="Base vectors (.npy or .npz with key 'base')")
    src.add_argument("--pg", action="store_true", help="Read vectors from PostgreSQL SQL queries")
    ap.add_argument("--pg-dsn", help="PostgreSQL DSN for SQL vector input; defaults to TURBOQUANT_PG_DSN")
    ap.add_argument("--queries", type=Path, help="Query vectors (.npy) for --vectors input")
    ap.add_argument("--base-sql", help="SQL returning one vector column for base vectors")
    ap.add_argument("--query-sql", help="SQL returning one vector column for query vectors")
    ap.add_argument("--shared-sql", help="SQL returning one vector column for repeated holdout evaluation")
    ap.add_argument("--metric", choices=("cosine", "ip"), default="cosine")
    ap.add_argument("--query-count", type=int, default=50)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--m-values", type=parse_m_values, default=parse_m_values("8,10,12,14,16,18,20"))
    ap.add_argument("--higgs-grid-samples", type=int, default=200_000)
    ap.add_argument("--json-out", type=Path)
    return ap


def load_vectors(bench: Any, args: argparse.Namespace) -> tuple[np.ndarray, np.ndarray, str]:
    if args.vectors:
        base, queries = bench.load_numpy_dataset(args.vectors, args.queries)
        return base, queries, str(args.vectors)
    dsn = args.pg_dsn or os.environ.get("TURBOQUANT_PG_DSN")
    if not dsn:
        raise SystemExit("--pg requires --pg-dsn or TURBOQUANT_PG_DSN")
    if args.shared_sql:
        if args.base_sql or args.query_sql:
            raise SystemExit("--shared-sql cannot be combined with --base-sql/--query-sql")
        shared = bench.load_pg_query_vectors(dsn, args.shared_sql)
        base, queries = bench.split_shared_vectors(shared, args.query_count, args.seed)
        return base, queries, "postgresql(shared_sql)"
    if not args.base_sql or not args.query_sql:
        raise SystemExit("--base-sql and --query-sql are required with --pg-dsn")
    base = bench.load_pg_query_vectors(dsn, args.base_sql)
    queries = bench.load_pg_query_vectors(dsn, args.query_sql)
    return base, queries, "postgresql"


def fit_sq8(normalized_base: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    col_min = normalized_base.min(axis=0).astype(np.float32)
    col_max = normalized_base.max(axis=0).astype(np.float32)
    col_range = np.maximum(col_max - col_min, 1e-8)
    codes = np.clip(np.rint((normalized_base - col_min) / col_range * 255.0), 0, 255).astype(np.uint8)
    return codes, col_min, col_range / 255.0


def rerank_sq8(
    shortlist_ids: np.ndarray,
    query: np.ndarray,
    sq8_codes: np.ndarray,
    sq8_mins: np.ndarray,
    sq8_scales: np.ndarray,
    k: int,
) -> np.ndarray:
    q_norm = query / max(np.linalg.norm(query), 1e-12)
    decoded = sq8_codes[shortlist_ids].astype(np.float32) * sq8_scales + sq8_mins
    scores = decoded @ q_norm
    return shortlist_ids[np.argsort(scores)[::-1][:k]]


def eval_inner_sweep(
    bench: Any,
    name: str,
    inner: Any,
    base: np.ndarray,
    queries: np.ndarray,
    gt_ids: list[np.ndarray],
    m_values: list[int],
    k: int,
    sq8_codes: np.ndarray,
    sq8_mins: np.ndarray,
    sq8_scales: np.ndarray,
) -> list[dict[str, float | str]]:
    t0 = time.perf_counter()
    inner.fit(base)
    encode_ms = (time.perf_counter() - t0) * 1000.0
    max_m = max(m_values)
    rows: list[dict[str, float | str]] = []
    per_m_hits = {m: [] for m in m_values}
    per_m_recalls = {m: [] for m in m_values}
    per_m_latencies = {m: [] for m in m_values}

    for query, gt in zip(queries, gt_ids, strict=True):
        q0 = time.perf_counter()
        shortlist = inner.search(query, max_m)
        shortlist_ms = (time.perf_counter() - q0) * 1000.0
        for m in m_values:
            r0 = time.perf_counter()
            found = rerank_sq8(shortlist[:m], query, sq8_codes, sq8_mins, sq8_scales, k)
            per_m_latencies[m].append(shortlist_ms + (time.perf_counter() - r0) * 1000.0)
            per_m_hits[m].append(bench.hit_at_1(found, gt))
            per_m_recalls[m].append(bench.recall_at_k(found, gt, k))

    for m in m_values:
        rows.append(
            {
                "method": f"{name}_sq8rerank{m}",
                "M": float(m),
                "encode_ms": encode_ms,
                "p50_ms": bench.median_ms(per_m_latencies[m]),
                "avg_ms": bench.avg_ms(per_m_latencies[m]),
                "hit1": bench.avg_ms(per_m_hits[m]),
                "recall_at_k": bench.avg_ms(per_m_recalls[m]),
            }
        )
    return rows


def main() -> int:
    args = build_arg_parser().parse_args()
    bench = load_bench_module()
    base, queries, source = load_vectors(bench, args)
    if args.metric == "cosine":
        base = bench.normalize_rows(base)
        queries = bench.normalize_rows(queries)
    gt_ids, _ = bench.exact_ground_truth(base, queries, args.k)
    normalized_base = bench.normalize_rows(base)
    sq8_codes, sq8_mins, sq8_scales = fit_sq8(normalized_base)

    scalar = bench.TurboQuantBlock32PackedTopKMethod(4, args.seed, group_size=16)
    higgs2 = bench.TurboQuantBlockHiggs2PackedMethod(
        4,
        args.seed,
        group_size=16,
        grid_samples=args.higgs_grid_samples,
    )
    rows = []
    rows.extend(
        eval_inner_sweep(
            bench, "flashhadamard16", scalar, base, queries, gt_ids, args.m_values,
            args.k, sq8_codes, sq8_mins, sq8_scales,
        )
    )
    rows.extend(
        eval_inner_sweep(
            bench, "flashhiggs2", higgs2, base, queries, gt_ids, args.m_values,
            args.k, sq8_codes, sq8_mins, sq8_scales,
        )
    )

    print(
        f"flashhiggs2 sweep | source={source} metric={args.metric} "
        f"base={base.shape[0]} queries={queries.shape[0]} dim={base.shape[1]} "
        f"k={args.k} m_values={','.join(map(str, args.m_values))}"
    )
    print(f"{'method':<34} {'M':>4} {'encode_ms':>11} {'p50_ms':>9} {'avg_ms':>9} {'hit@1':>8} {'recall@k':>10}")
    for row in rows:
        print(
            f"{row['method']:<34} {row['M']:>4.0f} {row['encode_ms']:>11.1f} "
            f"{row['p50_ms']:>9.3f} {row['avg_ms']:>9.3f} {row['hit1']:>8.2f} "
            f"{row['recall_at_k']:>10.2f}"
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
                    "m_values": args.m_values,
                    "results": rows,
                },
                indent=2,
            )
            + "\n"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
