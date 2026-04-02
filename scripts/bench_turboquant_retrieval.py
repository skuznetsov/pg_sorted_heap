#!/usr/bin/env python3
"""
Offline retrieval evaluator for TurboQuant-style experimental compression.

This script intentionally stays outside the stable sorted_hnsw / GraphRAG path.
It compares a few reversible retrieval-side baselines on the same vectors:

  - float32 exact reference
  - fp16 baseline
  - sq8_linear baseline
  - pq_kmeans baseline
  - turboquant_mse experimental path

TurboQuant v1 in this harness covers only the MSE-oriented first stage:
random orthogonal rotation + coordinate-wise scalar quantization on rotated
coordinates. It does NOT yet implement the inner-product residual/QJL stage.

Supported inputs:
  - ANN-Benchmarks HDF5 datasets (downloaded on demand)
  - generic .npy / .npz files
  - PostgreSQL SQL queries that return one vector column castable to text
  - PostgreSQL shared-SQL holdout evaluation for tiny real datasets

Examples:
  "$(./scripts/find_vector_python.sh)" ./scripts/bench_turboquant_retrieval.py \
    --dataset glove-100 --sample-size 4000 --query-count 50 --k 10

  "$(./scripts/find_vector_python.sh)" ./scripts/bench_turboquant_retrieval.py \
    --pg-dsn 'postgres://sergey@127.0.0.1/postgres' \
    --base-sql "SELECT embedding::text FROM my_table ORDER BY id LIMIT 2000" \
    --query-sql "SELECT embedding::text FROM my_queries ORDER BY qid LIMIT 50"

  "$(./scripts/find_vector_python.sh)" ./scripts/bench_turboquant_retrieval.py \
    --pg-dsn 'postgres://sergey@127.0.0.1/postgres' \
    --shared-sql "SELECT embedding::text FROM my_table ORDER BY id LIMIT 200" \
    --query-count 20 --folds 5
"""

from __future__ import annotations

import argparse
import io
import json
import math
import statistics
import time
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Callable

import h5py
import numpy as np
import psycopg2
import requests
from scipy.stats import norm
from sklearn.cluster import MiniBatchKMeans


DATASETS = {
    "nytimes-256": {
        "url": "https://ann-benchmarks.com/nytimes-256-angular.hdf5",
        "metric": "cosine",
    },
    "glove-100": {
        "url": "https://ann-benchmarks.com/glove-100-angular.hdf5",
        "metric": "cosine",
    },
}


def median_ms(values: list[float]) -> float:
    return statistics.median(values) if values else 0.0


def avg_ms(values: list[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def ensure_download(url: str, dst: Path) -> Path:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() and dst.stat().st_size > 0:
        return dst
    headers = {"User-Agent": "Mozilla/5.0 (Codex TurboQuant evaluator)"}
    with requests.get(url, headers=headers, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dst, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    return dst


def parse_vector_text(value: object) -> np.ndarray:
    if isinstance(value, np.ndarray):
        return value.astype(np.float32, copy=False)
    if isinstance(value, (list, tuple)):
        return np.asarray(value, dtype=np.float32)
    if not isinstance(value, str):
        raise TypeError(f"unsupported vector value type: {type(value)!r}")
    text = value.strip()
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1]
    elif text.startswith("{") and text.endswith("}"):
        text = text[1:-1]
    if not text:
        return np.zeros(0, dtype=np.float32)
    arr = np.fromstring(text, sep=",", dtype=np.float32)
    if arr.size == 0:
        raise ValueError(f"failed to parse vector text: {value[:80]!r}")
    return arr


def normalize_rows(x: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(x, axis=1, keepdims=True)
    norms = np.maximum(norms, 1e-12)
    return x / norms


def topk_indices(scores: np.ndarray, k: int) -> np.ndarray:
    if k >= scores.shape[0]:
        return np.argsort(-scores)
    idx = np.argpartition(-scores, k - 1)[:k]
    return idx[np.argsort(-scores[idx])]


def recall_at_k(found: np.ndarray, gt: np.ndarray, k: int) -> float:
    if k <= 0:
        return 0.0
    return len(set(found[:k].tolist()) & set(gt[:k].tolist())) / float(k) * 100.0


def hit_at_1(found: np.ndarray, gt: np.ndarray) -> float:
    if found.size == 0 or gt.size == 0:
        return 0.0
    return 100.0 if int(found[0]) == int(gt[0]) else 0.0


def load_ann_dataset(
    dataset_name: str,
    sample_size: int,
    query_count: int,
    seed: int,
    cache_dir: Path,
) -> tuple[np.ndarray, np.ndarray, str]:
    meta = DATASETS[dataset_name]
    path = ensure_download(meta["url"], cache_dir / f"{dataset_name}.hdf5")
    with h5py.File(path, "r") as f:
        train = np.array(f["train"], dtype=np.float32)
        test = np.array(f["test"], dtype=np.float32)
    rng = np.random.default_rng(seed)
    if sample_size > train.shape[0]:
        raise ValueError(f"sample_size={sample_size} exceeds train size {train.shape[0]}")
    if query_count > test.shape[0]:
        raise ValueError(f"query_count={query_count} exceeds test size {test.shape[0]}")
    train_idx = np.sort(rng.choice(train.shape[0], size=sample_size, replace=False))
    query_idx = np.sort(rng.choice(test.shape[0], size=query_count, replace=False))
    return train[train_idx], test[query_idx], meta["metric"]


def load_numpy_dataset(path: Path, queries_path: Path | None) -> tuple[np.ndarray, np.ndarray]:
    if path.suffix == ".npz":
        blob = np.load(path)
        if "base" not in blob:
            raise ValueError(f"{path} must contain 'base'")
        if queries_path is None:
            if "queries" not in blob:
                raise ValueError(f"{path} must contain 'queries' when --queries is not set")
            queries = np.array(blob["queries"], dtype=np.float32)
        else:
            queries = np.array(np.load(queries_path), dtype=np.float32)
        base = np.array(blob["base"], dtype=np.float32)
        return base, queries
    base = np.array(np.load(path), dtype=np.float32)
    if queries_path is None:
        raise ValueError("--queries is required for .npy input")
    queries = np.array(np.load(queries_path), dtype=np.float32)
    return base, queries


def load_pg_query_vectors(dsn: str, sql_text: str) -> np.ndarray:
    conn = psycopg2.connect(dsn)
    conn.set_session(autocommit=True)
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql_text)
            rows = cur.fetchall()
        finally:
            cur.close()
    finally:
        conn.close()
    if not rows:
        raise ValueError("query returned no rows")
    vecs = [parse_vector_text(row[0]) for row in rows]
    dim = vecs[0].size
    if any(v.size != dim for v in vecs):
        raise ValueError("inconsistent vector dimensionality in SQL result")
    return np.vstack(vecs).astype(np.float32, copy=False)


def auto_pq_m(dim: int, preferred: int) -> int:
    upper = min(preferred if preferred > 0 else 16, dim)
    for candidate in range(upper, 0, -1):
        if dim % candidate == 0:
            return candidate
    raise ValueError(f"unable to resolve pq_m for dim={dim}")


def exact_ground_truth(base: np.ndarray, queries: np.ndarray, k: int) -> tuple[list[np.ndarray], list[float]]:
    gt_ids: list[np.ndarray] = []
    latencies: list[float] = []
    for q in queries:
        t0 = time.perf_counter()
        scores = base @ q
        gt_ids.append(topk_indices(scores, k))
        latencies.append((time.perf_counter() - t0) * 1000.0)
    return gt_ids, latencies


@lru_cache(maxsize=None)
def gaussian_lloyd_max(bits: int, n_iter: int = 64) -> tuple[np.ndarray, np.ndarray]:
    if bits <= 0 or bits > 8:
        raise ValueError("bits must be in [1, 8] for this evaluator")
    levels = 1 << bits
    probs = (np.arange(levels, dtype=np.float64) + 0.5) / levels
    centers = norm.ppf(probs)
    for _ in range(n_iter):
        bounds = np.empty(levels + 1, dtype=np.float64)
        bounds[0] = -np.inf
        bounds[-1] = np.inf
        bounds[1:-1] = 0.5 * (centers[:-1] + centers[1:])
        updated = np.empty_like(centers)
        for idx in range(levels):
            a = bounds[idx]
            b = bounds[idx + 1]
            mass = norm.cdf(b) - norm.cdf(a)
            if mass <= 1e-12:
                updated[idx] = centers[idx]
                continue
            updated[idx] = (norm.pdf(a) - norm.pdf(b)) / mass
        if np.max(np.abs(updated - centers)) < 1e-7:
            centers = updated
            break
        centers = updated
    bounds = np.empty(levels + 1, dtype=np.float64)
    bounds[0] = -np.inf
    bounds[-1] = np.inf
    bounds[1:-1] = 0.5 * (centers[:-1] + centers[1:])
    return centers.astype(np.float32), bounds.astype(np.float32)


def random_orthogonal(dim: int, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    g = rng.standard_normal((dim, dim), dtype=np.float32)
    q, r = np.linalg.qr(g)
    signs = np.sign(np.diag(r))
    signs[signs == 0] = 1.0
    q = q * signs[np.newaxis, :]
    return q.astype(np.float32, copy=False)


@dataclass
class EvalResult:
    method: str
    bits_per_dim: float
    bytes_per_vec: float
    metadata_kb: float
    encode_ms: float
    search_p50_ms: float
    search_avg_ms: float
    hit1: float
    recall_at_k: float


def average_eval_results(rows: list[EvalResult]) -> EvalResult:
    if not rows:
        raise ValueError("rows must not be empty")
    first = rows[0]
    return EvalResult(
        method=first.method,
        bits_per_dim=avg_ms([row.bits_per_dim for row in rows]),
        bytes_per_vec=avg_ms([row.bytes_per_vec for row in rows]),
        metadata_kb=avg_ms([row.metadata_kb for row in rows]),
        encode_ms=avg_ms([row.encode_ms for row in rows]),
        search_p50_ms=avg_ms([row.search_p50_ms for row in rows]),
        search_avg_ms=avg_ms([row.search_avg_ms for row in rows]),
        hit1=avg_ms([row.hit1 for row in rows]),
        recall_at_k=avg_ms([row.recall_at_k for row in rows]),
    )


def split_shared_vectors(vectors: np.ndarray, query_count: int, seed: int) -> tuple[np.ndarray, np.ndarray]:
    total = vectors.shape[0]
    if query_count <= 0:
        raise ValueError("query_count must be positive for shared holdout mode")
    if query_count >= total:
        raise ValueError(f"query_count={query_count} must be smaller than shared set size {total}")
    rng = np.random.default_rng(seed)
    query_idx = np.sort(rng.choice(total, size=query_count, replace=False))
    query_mask = np.zeros(total, dtype=bool)
    query_mask[query_idx] = True
    base = vectors[~query_mask]
    queries = vectors[query_mask]
    return base.astype(np.float32, copy=False), queries.astype(np.float32, copy=False)


class RetrievalMethod:
    def __init__(self, name: str) -> None:
        self.name = name

    def fit(self, base: np.ndarray) -> None:
        raise NotImplementedError

    def search(self, query: np.ndarray, k: int) -> np.ndarray:
        raise NotImplementedError

    def bytes_per_vec(self) -> float:
        raise NotImplementedError

    def metadata_bytes(self) -> int:
        return 0

    def bits_per_dim(self, dim: int) -> float:
        return self.bytes_per_vec() * 8.0 / float(dim)


class Fp16Method(RetrievalMethod):
    def __init__(self) -> None:
        super().__init__("fp16")
        self.base16: np.ndarray | None = None

    def fit(self, base: np.ndarray) -> None:
        self.base16 = base.astype(np.float16)

    def search(self, query: np.ndarray, k: int) -> np.ndarray:
        scores = self.base16.astype(np.float32) @ query
        return topk_indices(scores, k)

    def bytes_per_vec(self) -> float:
        if self.base16 is None:
            raise RuntimeError("fit must run first")
        return float(self.base16.shape[1] * 2)


class Sq8LinearMethod(RetrievalMethod):
    def __init__(self) -> None:
        super().__init__("sq8_linear")
        self.codes: np.ndarray | None = None
        self.scales: np.ndarray | None = None

    def fit(self, base: np.ndarray) -> None:
        max_abs = np.max(np.abs(base), axis=0)
        max_abs = np.maximum(max_abs, 1e-8)
        self.scales = (max_abs / 127.0).astype(np.float32)
        self.codes = np.clip(np.rint(base / self.scales), -127, 127).astype(np.int8)

    def search(self, query: np.ndarray, k: int) -> np.ndarray:
        approx = self.codes.astype(np.float32) * self.scales
        scores = approx @ query
        return topk_indices(scores, k)

    def bytes_per_vec(self) -> float:
        if self.codes is None:
            raise RuntimeError("fit must run first")
        return float(self.codes.shape[1])

    def metadata_bytes(self) -> int:
        if self.scales is None:
            return 0
        return int(self.scales.nbytes)


class PQKMeansMethod(RetrievalMethod):
    def __init__(self, m: int, bits: int, max_train: int, seed: int) -> None:
        super().__init__("pq_kmeans")
        self.m = m
        self.bits = bits
        self.max_train = max_train
        self.seed = seed
        self.ksub = 1 << bits
        self.dsub = 0
        self.codebooks: list[np.ndarray] = []
        self.codes: np.ndarray | None = None

    def fit(self, base: np.ndarray) -> None:
        dim = base.shape[1]
        if dim % self.m != 0:
            raise ValueError(f"dim={dim} must be divisible by pq_m={self.m}")
        self.dsub = dim // self.m
        train = base
        if base.shape[0] > self.max_train:
            rng = np.random.default_rng(self.seed)
            idx = np.sort(rng.choice(base.shape[0], size=self.max_train, replace=False))
            train = base[idx]
        all_codes = np.empty((base.shape[0], self.m), dtype=np.uint8)
        self.codebooks = []
        for part in range(self.m):
            start = part * self.dsub
            stop = start + self.dsub
            n_clusters = min(self.ksub, train.shape[0])
            km = MiniBatchKMeans(
                n_clusters=n_clusters,
                random_state=self.seed + part,
                n_init=1,
                batch_size=min(8192, max(1024, n_clusters * 8)),
                max_iter=100,
                reassignment_ratio=0.0,
            )
            km.fit(train[:, start:stop])
            centers = km.cluster_centers_.astype(np.float32)
            self.codebooks.append(centers)
            labels = km.predict(base[:, start:stop])
            all_codes[:, part] = labels.astype(np.uint8)
        self.codes = all_codes

    def search(self, query: np.ndarray, k: int) -> np.ndarray:
        tables = []
        for part, centers in enumerate(self.codebooks):
            start = part * self.dsub
            stop = start + self.dsub
            tables.append(centers @ query[start:stop])
        scores = np.zeros(self.codes.shape[0], dtype=np.float32)
        for part, table in enumerate(tables):
            scores += table[self.codes[:, part]]
        return topk_indices(scores, k)

    def bytes_per_vec(self) -> float:
        return float(self.m)

    def metadata_bytes(self) -> int:
        return sum(int(cb.nbytes) for cb in self.codebooks)


class TurboQuantMSEMethod(RetrievalMethod):
    def __init__(self, bits: int, seed: int) -> None:
        super().__init__("turboquant_mse")
        self.bits = bits
        self.seed = seed
        self.rotation: np.ndarray | None = None
        self.codes: np.ndarray | None = None
        self.norms: np.ndarray | None = None
        self.decoded_rot: np.ndarray | None = None
        self.centers: np.ndarray | None = None
        self.bounds: np.ndarray | None = None
        self.dim = 0

    def fit(self, base: np.ndarray) -> None:
        self.dim = base.shape[1]
        self.rotation = random_orthogonal(self.dim, self.seed)
        self.centers, self.bounds = gaussian_lloyd_max(self.bits)
        norms = np.linalg.norm(base, axis=1).astype(np.float32)
        unit = base / np.maximum(norms[:, None], 1e-12)
        rotated = (unit @ self.rotation) * math.sqrt(self.dim)
        codes = np.digitize(rotated, self.bounds[1:-1], right=False).astype(np.uint8)
        self.codes = codes
        self.norms = norms
        self.decoded_rot = self.centers[codes] / math.sqrt(self.dim)

    def search(self, query: np.ndarray, k: int) -> np.ndarray:
        q_rot = query @ self.rotation
        scores = self.decoded_rot @ q_rot
        scores *= self.norms
        return topk_indices(scores, k)

    def bytes_per_vec(self) -> float:
        if self.codes is None:
            raise RuntimeError("fit must run first")
        return float(self.codes.shape[1] * self.bits / 8.0 + 4.0)

    def metadata_bytes(self) -> int:
        total = 0
        if self.rotation is not None:
            total += int(self.rotation.nbytes)
        if self.centers is not None:
            total += int(self.centers.nbytes)
        if self.bounds is not None:
            total += int(self.bounds.nbytes)
        return total


def evaluate_method(
    method: RetrievalMethod,
    base: np.ndarray,
    queries: np.ndarray,
    gt_ids: list[np.ndarray],
    k: int,
) -> EvalResult:
    t0 = time.perf_counter()
    method.fit(base)
    encode_ms = (time.perf_counter() - t0) * 1000.0

    latencies: list[float] = []
    hit1_parts: list[float] = []
    recall_parts: list[float] = []
    for q, gt in zip(queries, gt_ids, strict=True):
        q0 = time.perf_counter()
        found = method.search(q, k)
        latencies.append((time.perf_counter() - q0) * 1000.0)
        hit1_parts.append(hit_at_1(found, gt))
        recall_parts.append(recall_at_k(found, gt, k))

    dim = base.shape[1]
    return EvalResult(
        method=method.name,
        bits_per_dim=method.bits_per_dim(dim),
        bytes_per_vec=method.bytes_per_vec(),
        metadata_kb=method.metadata_bytes() / 1024.0,
        encode_ms=encode_ms,
        search_p50_ms=median_ms(latencies),
        search_avg_ms=avg_ms(latencies),
        hit1=avg_ms(hit1_parts),
        recall_at_k=avg_ms(recall_parts),
    )


def print_results(title: str, rows: list[EvalResult]) -> None:
    print(title)
    print("=" * len(title))
    print(
        f"{'method':<16} {'bits/dim':>9} {'bytes/vec':>10} {'meta_kb':>10} "
        f"{'encode_ms':>11} {'p50_ms':>9} {'avg_ms':>9} {'hit@1':>8} {'recall@k':>10}"
    )
    for row in rows:
        print(
            f"{row.method:<16} {row.bits_per_dim:>9.2f} {row.bytes_per_vec:>10.1f} "
            f"{row.metadata_kb:>10.1f} {row.encode_ms:>11.1f} {row.search_p50_ms:>9.3f} "
            f"{row.search_avg_ms:>9.3f} {row.hit1:>8.2f} {row.recall_at_k:>10.2f}"
        )


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Offline TurboQuant retrieval evaluator")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--dataset", choices=sorted(DATASETS.keys()))
    src.add_argument("--vectors", type=Path, help="Base vectors (.npy or .npz with key 'base')")
    src.add_argument("--pg-dsn", help="PostgreSQL DSN for SQL-driven vector input")

    ap.add_argument("--queries", type=Path, help="Query vectors (.npy) for --vectors input")
    ap.add_argument("--base-sql", help="SQL returning one vector column for base vectors")
    ap.add_argument("--query-sql", help="SQL returning one vector column for query vectors")
    ap.add_argument("--shared-sql", help="SQL returning one vector column for repeated holdout evaluation")
    ap.add_argument("--metric", choices=("cosine", "ip"), default="cosine")
    ap.add_argument("--json-out", type=Path, help="Write structured JSON summary to this path")
    ap.add_argument("--cache-dir", type=Path, default=Path("/tmp/ann_real_cache"))
    ap.add_argument("--sample-size", type=int, default=4000)
    ap.add_argument("--query-count", type=int, default=50)
    ap.add_argument("--folds", type=int, default=1, help="Repeated holdout folds for --shared-sql mode")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--turbo-bits", type=int, default=4)
    ap.add_argument("--pq-m", type=int, default=0, help="PQ subvector count (0=auto)")
    ap.add_argument("--pq-bits", type=int, default=8)
    ap.add_argument("--pq-max-train", type=int, default=20000)
    ap.add_argument("--skip-fp16", action="store_true")
    ap.add_argument("--skip-sq8", action="store_true")
    ap.add_argument("--skip-pq", action="store_true")
    ap.add_argument("--skip-turbo", action="store_true")
    return ap


def evaluate_rows(base: np.ndarray, queries: np.ndarray, metric: str, args: argparse.Namespace) -> list[EvalResult]:
    if base.ndim != 2 or queries.ndim != 2:
        raise SystemExit("base and query vectors must be 2-D arrays")
    if base.shape[1] != queries.shape[1]:
        raise SystemExit(f"dimension mismatch: base={base.shape[1]} query={queries.shape[1]}")
    if base.shape[0] < args.k:
        raise SystemExit(f"base size {base.shape[0]} must be >= k={args.k}")

    if metric == "cosine":
        base = normalize_rows(base)
        queries = normalize_rows(queries)

    gt_ids, exact_latencies = exact_ground_truth(base, queries, args.k)

    rows = [
        EvalResult(
            method="float32_exact",
            bits_per_dim=32.0,
            bytes_per_vec=float(base.shape[1] * 4),
            metadata_kb=0.0,
            encode_ms=0.0,
            search_p50_ms=median_ms(exact_latencies),
            search_avg_ms=avg_ms(exact_latencies),
            hit1=100.0,
            recall_at_k=100.0,
        )
    ]

    methods: list[RetrievalMethod] = []
    if not args.skip_fp16:
        methods.append(Fp16Method())
    if not args.skip_sq8:
        methods.append(Sq8LinearMethod())
    if not args.skip_pq:
        methods.append(PQKMeansMethod(auto_pq_m(base.shape[1], args.pq_m), args.pq_bits, args.pq_max_train, args.seed))
    if not args.skip_turbo:
        methods.append(TurboQuantMSEMethod(args.turbo_bits, args.seed))

    for method in methods:
        rows.append(evaluate_method(method, base, queries, gt_ids, args.k))
    return rows


def eval_result_to_dict(row: EvalResult) -> dict[str, float | str]:
    return {
        "method": row.method,
        "bits_per_dim": row.bits_per_dim,
        "bytes_per_vec": row.bytes_per_vec,
        "metadata_kb": row.metadata_kb,
        "encode_ms": row.encode_ms,
        "search_p50_ms": row.search_p50_ms,
        "search_avg_ms": row.search_avg_ms,
        "hit1": row.hit1,
        "recall_at_k": row.recall_at_k,
    }


def main() -> int:
    args = build_arg_parser().parse_args()

    folds = 1
    shared: np.ndarray | None = None
    if args.dataset:
        base, queries, metric = load_ann_dataset(
            args.dataset,
            args.sample_size,
            args.query_count,
            args.seed,
            args.cache_dir,
        )
    elif args.vectors:
        if args.queries is None:
            raise SystemExit("--queries is required with --vectors")
        base, queries = load_numpy_dataset(args.vectors, args.queries)
        metric = args.metric
    else:
        if args.shared_sql:
            if args.base_sql or args.query_sql:
                raise SystemExit("--shared-sql cannot be combined with --base-sql/--query-sql")
            if args.folds <= 0:
                raise SystemExit("--folds must be positive")
            shared = load_pg_query_vectors(args.pg_dsn, args.shared_sql)
            base, queries = split_shared_vectors(shared, args.query_count, args.seed)
            folds = args.folds
        else:
            if not args.base_sql or not args.query_sql:
                raise SystemExit("--base-sql and --query-sql are required with --pg-dsn")
            base = load_pg_query_vectors(args.pg_dsn, args.base_sql)
            queries = load_pg_query_vectors(args.pg_dsn, args.query_sql)
        metric = args.metric

    if args.shared_sql:
        per_fold_rows: list[list[EvalResult]] = []
        for fold_idx in range(args.folds):
            fold_base, fold_queries = split_shared_vectors(shared, args.query_count, args.seed + fold_idx)
            per_fold_rows.append(evaluate_rows(fold_base, fold_queries, metric, args))
        rows = [
            average_eval_results([fold_rows[row_idx] for fold_rows in per_fold_rows])
            for row_idx in range(len(per_fold_rows[0]))
        ]
        source_name = "postgresql(shared_sql)"
    else:
        rows = evaluate_rows(base, queries, metric, args)
        source_name = args.dataset or (str(args.vectors) if args.vectors else None) or "postgresql"

    base_count = int(base.shape[0])
    query_count = int(queries.shape[0])
    dim = int(base.shape[1])
    print(
        f"turboquant offline retrieval eval | source={source_name} metric={metric} "
        f"base={base_count} queries={query_count} dim={dim} k={args.k}"
    )
    if folds > 1:
        print(f"holdout_folds={folds} query_count={query_count} seed={args.seed}")
    print_results("Results", rows)
    print("\nCaveat: turboquant_mse here implements only the first-stage MSE path.")
    print("It does not yet include the residual 1-bit QJL inner-product correction stage.")
    if args.json_out:
        payload = {
            "source": source_name,
            "metric": metric,
            "base_count": base_count,
            "query_count": query_count,
            "dim": dim,
            "k": args.k,
            "seed": args.seed,
            "folds": folds,
            "results": [eval_result_to_dict(row) for row in rows],
            "caveat": "turboquant_mse implements only the first-stage MSE path; residual 1-bit QJL correction is not included",
        }
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
