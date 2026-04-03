#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import pathlib
import statistics
import sys
import time
from types import SimpleNamespace

import numpy as np


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
BENCH_PATH = REPO_ROOT / "scripts" / "bench_turboquant_retrieval.py"


def load_bench_module():
    spec = importlib.util.spec_from_file_location("bench_turboquant_retrieval", BENCH_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {BENCH_PATH}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Screen packed TurboQuant helper lanes on a real SQL-backed set")
    ap.add_argument("--pg-dsn", required=True)
    ap.add_argument("--base-sql", required=True)
    ap.add_argument("--query-sql", required=True)
    ap.add_argument("--metric", choices=("cosine", "ip"), default="cosine")
    ap.add_argument("--methods", default="turboquant_blockhadamard_packed4,turboquant_blockhadamard_packed4_topk")
    ap.add_argument("--parity-against", default="turboquant_blockhadamard_packed4")
    ap.add_argument("--turbo-bits", type=int, default=4)
    ap.add_argument("--seed", type=int, default=123)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--query-limit", type=int, default=0)
    return ap.parse_args()


def build_method(mod, name: str, bits: int, seed: int):
    mapping = {
        "turboquant_blockhadamard_packed4": lambda: mod.TurboQuantBlockHadamardPackedMethod(bits, seed),
        "turboquant_blockhadamard_packed4_topk": lambda: mod.TurboQuantBlockHadamardPackedTopKMethod(bits, seed),
        "turboquant_block16_packed4": lambda: mod.TurboQuantBlock32PackedMethod(bits, seed, group_size=16),
        "turboquant_block32_packed4": lambda: mod.TurboQuantBlock32PackedMethod(bits, seed),
        "turboquant_block32_dither_packed4": lambda: mod.TurboQuantBlock32DitherPackedMethod(bits, seed),
    }
    if name not in mapping:
        raise SystemExit(f"unsupported screening method: {name}")
    return mapping[name]()


def tie_only_set_diff(mod, method, query: np.ndarray, ref: np.ndarray, got: np.ndarray, tol: float = 1e-6) -> bool:
    if not isinstance(method, mod.TurboQuantBlockHadamardPackedMethod):
        return False
    q_rot = mod.structured_block_hadamard_vec(query, method.perm, method.signs, method.blocks)
    coeffs = (q_rot / np.sqrt(method.dim)).astype(np.float32, copy=False)
    scores = mod.packed_lookup_scores_blockhadamard_packed4_transposed(
        method.packed_codes_t,
        coeffs,
        method.centers,
        method.norms,
    )
    boundary = min(float(scores[int(ref[-1])]), float(scores[int(got[-1])]))
    xor = np.setxor1d(ref, got)
    return all(abs(float(scores[int(idx)]) - boundary) <= tol for idx in xor.tolist())


def p50_ms(samples: list[float]) -> float:
    if not samples:
        return 0.0
    return float(np.percentile(np.asarray(samples, dtype=np.float64), 50))


def main() -> int:
    args = parse_args()
    mod = load_bench_module()
    base = mod.load_pg_query_vectors(args.pg_dsn, args.base_sql)
    queries = mod.load_pg_query_vectors(args.pg_dsn, args.query_sql)
    if args.query_limit > 0:
        queries = queries[: args.query_limit]
    if base.ndim != 2 or queries.ndim != 2:
        raise SystemExit("base/query vectors must be 2-D")
    if base.shape[1] != queries.shape[1]:
        raise SystemExit("dimension mismatch between base and queries")
    if args.metric == "cosine":
        base = mod.normalize_rows(base)
        queries = mod.normalize_rows(queries)

    method_names = [name.strip() for name in args.methods.split(",") if name.strip()]
    if not method_names:
        raise SystemExit("--methods must not be empty")
    if args.parity_against and args.parity_against not in method_names:
        method_names.insert(0, args.parity_against)

    results: dict[str, dict[str, float | int | bool]] = {}
    id_cache: dict[str, list[np.ndarray]] = {}
    method_objs: dict[str, object] = {}
    for name in method_names:
        method = build_method(mod, name, args.turbo_bits, args.seed)
        method_objs[name] = method
        t0 = time.perf_counter()
        method.fit(base)
        encode_ms = (time.perf_counter() - t0) * 1000.0
        latencies: list[float] = []
        ids: list[np.ndarray] = []
        for query in queries:
            q0 = time.perf_counter()
            top_ids = method.search(query, args.k)
            latencies.append((time.perf_counter() - q0) * 1000.0)
            ids.append(np.asarray(top_ids, dtype=np.int32))
        id_cache[name] = ids
        results[name] = {
            "encode_ms": encode_ms,
            "p50_ms": p50_ms(latencies),
            "avg_ms": float(statistics.fmean(latencies)),
            "queries": len(ids),
        }

    if args.parity_against:
        baseline = id_cache[args.parity_against]
        for name in method_names:
            if name == args.parity_against:
                results[name]["order_diff_queries"] = 0
                results[name]["set_diff_queries"] = 0
                continue
            order_diff_queries = 0
            set_diff_queries = 0
            tie_only_set_diff_queries = 0
            for query_idx, (ref, got) in enumerate(zip(baseline, id_cache[name], strict=True)):
                if not np.array_equal(ref, got):
                    order_diff_queries += 1
                    if np.setxor1d(ref, got).size != 0:
                        set_diff_queries += 1
                        if tie_only_set_diff(mod, method_objs[args.parity_against], queries[query_idx], ref, got):
                            tie_only_set_diff_queries += 1
            results[name]["order_diff_queries"] = order_diff_queries
            results[name]["set_diff_queries"] = set_diff_queries
            results[name]["tie_only_set_diff_queries"] = tie_only_set_diff_queries

    print(
        f"packed turboquant screening | metric={args.metric} base={base.shape[0]} "
        f"queries={queries.shape[0]} dim={base.shape[1]} k={args.k}"
    )
    print("Results")
    print("=======")
    print("method                              encode_ms    p50_ms    avg_ms  order_diff  set_diff  tie_only")
    for name in method_names:
        row = results[name]
        order_diff = row.get("order_diff_queries")
        set_diff = row.get("set_diff_queries")
        tie_only = row.get("tie_only_set_diff_queries")
        order_text = "-" if order_diff is None else str(int(order_diff))
        set_text = "-" if set_diff is None else str(int(set_diff))
        tie_text = "-" if tie_only is None else str(int(tie_only))
        print(
            f"{name:<34} {float(row['encode_ms']):>10.1f} {float(row['p50_ms']):>9.3f} "
            f"{float(row['avg_ms']):>9.3f} {order_text:>11} {set_text:>9} {tie_text:>9}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
