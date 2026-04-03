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
  - turboquant_blockhadamard experimental path
  - turboquant_blockhadamard_whitened experimental path
  - turboquant_blockhadamard_block32 experimental path
  - turboquant_prod experimental path

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
import ctypes
import io
import json
import math
import os
import platform
import statistics
import subprocess
import sys
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

BYTE_CODES = np.arange(256, dtype=np.uint8)
BYTE_LO_NIBBLES = (BYTE_CODES & 0x0F).astype(np.intp, copy=False)
BYTE_HI_NIBBLES = (BYTE_CODES >> 4).astype(np.intp, copy=False)


def packed_adc_helper_path() -> Path:
    suffix = ".dylib" if sys.platform == "darwin" else ".so"
    return Path(__file__).resolve().parent.parent / "build" / f"turboquant_packed_adc{suffix}"


def build_packed_adc_helper(dst: Path) -> None:
    src = Path(__file__).resolve().parent / "turboquant_packed_adc.c"
    dst.parent.mkdir(parents=True, exist_ok=True)
    cc = os.environ.get("CC", "cc")
    cmd = [cc, "-O3", "-std=c99"]
    if platform.system() == "Darwin":
        cmd.extend(["-dynamiclib", "-o", str(dst), str(src)])
    else:
        cmd.extend(["-shared", "-fPIC", "-pthread", "-o", str(dst), str(src)])
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


@lru_cache(maxsize=1)
def load_packed_adc_helper() -> ctypes.CDLL | None:
    if os.environ.get("TURBOQUANT_DISABLE_C_HELPER") == "1":
        return None
    dst = packed_adc_helper_path()
    src = Path(__file__).resolve().parent / "turboquant_packed_adc.c"
    try:
        if not dst.exists() or dst.stat().st_mtime < src.stat().st_mtime:
            build_packed_adc_helper(dst)
        lib = ctypes.CDLL(str(dst))
    except Exception:
        return None
    lib.turboquant_packed_adc_scores_f32.argtypes = [
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.POINTER(ctypes.c_float),
        ctypes.POINTER(ctypes.c_float),
        ctypes.c_size_t,
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_float),
    ]
    lib.turboquant_packed_adc_scores_f32.restype = None
    lib.turboquant_packed_adc_scores_t_f32.argtypes = [
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.POINTER(ctypes.c_float),
        ctypes.POINTER(ctypes.c_float),
        ctypes.c_size_t,
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_float),
    ]
    lib.turboquant_packed_adc_scores_t_f32.restype = None
    lib.turboquant_packed_adc_scores_t_mt_f32.argtypes = [
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.POINTER(ctypes.c_float),
        ctypes.POINTER(ctypes.c_float),
        ctypes.c_size_t,
        ctypes.c_size_t,
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_float),
    ]
    lib.turboquant_packed_adc_scores_t_mt_f32.restype = None
    lib.turboquant_blockhadamard_packed4_scores_t_f32.argtypes = [
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.POINTER(ctypes.c_float),
        ctypes.POINTER(ctypes.c_float),
        ctypes.POINTER(ctypes.c_float),
        ctypes.c_size_t,
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_float),
    ]
    lib.turboquant_blockhadamard_packed4_scores_t_f32.restype = None
    lib.turboquant_blockhadamard_packed4_scores_t_mt_f32.argtypes = [
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.POINTER(ctypes.c_float),
        ctypes.POINTER(ctypes.c_float),
        ctypes.POINTER(ctypes.c_float),
        ctypes.c_size_t,
        ctypes.c_size_t,
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_float),
    ]
    lib.turboquant_blockhadamard_packed4_scores_t_mt_f32.restype = None
    lib.turboquant_blockhadamard_packed4_topk_t_mt_f32.argtypes = [
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.POINTER(ctypes.c_float),
        ctypes.POINTER(ctypes.c_float),
        ctypes.POINTER(ctypes.c_float),
        ctypes.c_size_t,
        ctypes.c_size_t,
        ctypes.c_size_t,
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_int32),
        ctypes.POINTER(ctypes.c_float),
    ]
    lib.turboquant_blockhadamard_packed4_topk_t_mt_f32.restype = None
    lib.turboquant_blockhadamard_packed4_profile_reset.argtypes = []
    lib.turboquant_blockhadamard_packed4_profile_reset.restype = None
    lib.turboquant_blockhadamard_packed4_profile_get.argtypes = [
        ctypes.POINTER(ctypes.c_double),
        ctypes.POINTER(ctypes.c_double),
        ctypes.POINTER(ctypes.c_uint64),
    ]
    lib.turboquant_blockhadamard_packed4_profile_get.restype = None
    lib.turboquant_blockhadamard_packed4_topk_profile_get.argtypes = [
        ctypes.POINTER(ctypes.c_double),
        ctypes.POINTER(ctypes.c_double),
        ctypes.POINTER(ctypes.c_double),
        ctypes.POINTER(ctypes.c_uint64),
    ]
    lib.turboquant_blockhadamard_packed4_topk_profile_get.restype = None
    return lib


def packed_adc_backend_name() -> str:
    return "c-helper" if load_packed_adc_helper() is not None else "python-fallback"


def packed_adc_thread_count() -> int:
    raw = os.environ.get("TURBOQUANT_ADC_THREADS")
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            return 1
    return max(1, min(8, os.cpu_count() or 1))


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


def power_of_two_blocks(dim: int) -> list[int]:
    blocks: list[int] = []
    remaining = dim
    while remaining > 0:
        block = 1 << (remaining.bit_length() - 1)
        blocks.append(block)
        remaining -= block
    return blocks


def fwht_rows(block: np.ndarray) -> np.ndarray:
    out = block.astype(np.float32, copy=True)
    width = out.shape[1]
    if width <= 1:
        return out
    h = 1
    while h < width:
        reshaped = out.reshape(out.shape[0], -1, 2 * h)
        left = reshaped[:, :, :h].copy()
        right = reshaped[:, :, h : 2 * h].copy()
        reshaped[:, :, :h] = left + right
        reshaped[:, :, h : 2 * h] = left - right
        out = reshaped.reshape(out.shape[0], width)
        h *= 2
    out /= math.sqrt(width)
    return out


def fwht_vec(block: np.ndarray) -> np.ndarray:
    out = block.astype(np.float32, copy=True)
    width = out.shape[0]
    if width <= 1:
        return out
    h = 1
    while h < width:
        reshaped = out.reshape(-1, 2 * h)
        left = reshaped[:, :h].copy()
        right = reshaped[:, h : 2 * h].copy()
        reshaped[:, :h] = left + right
        reshaped[:, h : 2 * h] = left - right
        out = reshaped.reshape(width)
        h *= 2
    out /= math.sqrt(width)
    return out


def structured_block_hadamard(
    x: np.ndarray,
    perm: np.ndarray,
    signs: np.ndarray,
    blocks: list[int],
) -> np.ndarray:
    if x.ndim != 2:
        raise ValueError("structured_block_hadamard expects a 2-D array")
    mixed = x[:, perm] * signs[np.newaxis, :]
    out = np.empty_like(mixed, dtype=np.float32)
    offset = 0
    for block in blocks:
        chunk = mixed[:, offset : offset + block]
        out[:, offset : offset + block] = fwht_rows(chunk)
        offset += block
    return out


def structured_block_hadamard_vec(
    x: np.ndarray,
    perm: np.ndarray,
    signs: np.ndarray,
    blocks: list[int],
) -> np.ndarray:
    if x.ndim != 1:
        raise ValueError("structured_block_hadamard_vec expects a 1-D array")
    mixed = x[perm] * signs
    out = np.empty_like(mixed, dtype=np.float32)
    offset = 0
    for block in blocks:
        chunk = mixed[offset : offset + block]
        out[offset : offset + block] = fwht_vec(chunk)
        offset += block
    return out


def maybe_store_norms(norms: np.ndarray) -> np.ndarray | None:
    if np.allclose(norms, 1.0, atol=1e-4):
        return None
    return norms.astype(np.float32, copy=False)


def grouped_rms_scales(rotated: np.ndarray, group_size: int) -> tuple[np.ndarray, np.ndarray]:
    dim = rotated.shape[1]
    n_groups = (dim + group_size - 1) // group_size
    group_scales = np.empty(n_groups, dtype=np.float32)
    expanded = np.empty(dim, dtype=np.float32)
    group_idx = 0
    for start in range(0, dim, group_size):
        stop = min(dim, start + group_size)
        block = rotated[:, start:stop]
        scale = float(np.sqrt(np.mean(block * block)))
        scale = max(scale, 1e-4)
        group_scales[group_idx] = scale
        expanded[start:stop] = scale
        group_idx += 1
    return group_scales, expanded


def power_compand(x: np.ndarray, beta: float) -> np.ndarray:
    return np.sign(x) * np.power(np.abs(x), beta, dtype=np.float32)


def power_decompand(x: np.ndarray, beta: float) -> np.ndarray:
    inv_beta = 1.0 / beta
    return np.sign(x) * np.power(np.abs(x), inv_beta, dtype=np.float32)


def symmetric_int_levels(bits: int) -> tuple[int, int]:
    max_abs = max(1, (1 << (bits - 1)) - 1)
    levels = 2 * max_abs + 1
    return max_abs, levels


def uniform_quantize(x: np.ndarray, bits: int, clip: float) -> tuple[np.ndarray, np.ndarray, float]:
    max_abs, levels = symmetric_int_levels(bits)
    step = (2.0 * clip) / float(levels - 1)
    scaled = np.clip(x / step, -max_abs, max_abs)
    codes = np.rint(scaled).astype(np.int16)
    decoded = codes.astype(np.float32) * step
    return codes, decoded, step


def dithered_uniform_quantize(
    x: np.ndarray, bits: int, clip: float, seed: int
) -> tuple[np.ndarray, np.ndarray, float]:
    max_abs, levels = symmetric_int_levels(bits)
    step = (2.0 * clip) / float(levels - 1)
    rng = np.random.default_rng(seed)
    dither = rng.uniform(-0.5 * step, 0.5 * step, size=x.shape).astype(np.float32)
    scaled = np.clip((x + dither) / step, -max_abs, max_abs)
    codes = np.rint(scaled).astype(np.int16)
    decoded = codes.astype(np.float32) * step - dither
    return codes, decoded, step


def nearest_d4_rows(x: np.ndarray, max_abs: int) -> np.ndarray:
    rounded = np.clip(np.rint(x), -max_abs, max_abs).astype(np.int16)
    parity = (rounded.sum(axis=1) & 1).astype(bool)
    if not np.any(parity):
        return rounded

    odd_idx = np.flatnonzero(parity)
    for row_idx in odd_idx:
        row = rounded[row_idx].copy()
        target = x[row_idx]
        best_coord = 0
        best_delta = 0
        best_penalty = float("inf")
        for coord in range(row.shape[0]):
            current = int(row[coord])
            preferred = 1 if target[coord] >= current else -1
            for delta in (preferred, -preferred):
                candidate = current + delta
                if candidate < -max_abs or candidate > max_abs:
                    continue
                penalty = abs(target[coord] - candidate)
                if penalty < best_penalty:
                    best_penalty = penalty
                    best_coord = coord
                    best_delta = delta
        row[best_coord] = np.int16(int(row[best_coord]) + best_delta)
        rounded[row_idx] = row
    return rounded


def d4_quantize_rows(x: np.ndarray, bits: int, clip: float) -> tuple[np.ndarray, np.ndarray, float]:
    max_abs, levels = symmetric_int_levels(bits)
    step = (2.0 * clip) / float(levels - 1)
    q = np.clip(x / step, -max_abs, max_abs)
    if q.shape[1] % 4 != 0:
        raise ValueError("D4 quantization requires dimensions divisible by 4")
    flat = q.reshape(-1, 4)
    lattice = nearest_d4_rows(flat, max_abs=max_abs)
    decoded = lattice.astype(np.float32).reshape(x.shape) * step
    return lattice.reshape(x.shape), decoded, step


def two_pass_block_hadamard(
    x: np.ndarray,
    perm1: np.ndarray,
    signs1: np.ndarray,
    perm2: np.ndarray,
    signs2: np.ndarray,
    blocks: list[int],
) -> np.ndarray:
    return structured_block_hadamard(
        structured_block_hadamard(x, perm1, signs1, blocks),
        perm2,
        signs2,
        blocks,
    )


def pack_nibbles(codes: np.ndarray) -> np.ndarray:
    if codes.ndim != 2:
        raise ValueError("pack_nibbles expects a 2-D array")
    if codes.dtype != np.uint8:
        codes = codes.astype(np.uint8, copy=False)
    n_rows, dim = codes.shape
    packed = np.zeros((n_rows, (dim + 1) // 2), dtype=np.uint8)
    packed[:, : codes[:, 0::2].shape[1]] |= codes[:, 0::2] & 0x0F
    packed[:, : codes[:, 1::2].shape[1]] |= (codes[:, 1::2] & 0x0F) << 4
    return packed


def transpose_packed_codes(packed_codes: np.ndarray) -> np.ndarray:
    if packed_codes.ndim != 2:
        raise ValueError("transpose_packed_codes expects a 2-D array")
    return np.ascontiguousarray(packed_codes.T)


def nibble_pair_lut(lo_values: np.ndarray, hi_values: np.ndarray | None = None) -> np.ndarray:
    table = lo_values[BYTE_LO_NIBBLES].astype(np.float32, copy=False)
    if hi_values is not None:
        table = table + hi_values[BYTE_HI_NIBBLES]
    return table.astype(np.float32, copy=False)


def packed_lookup_scores_python(
    packed_codes: np.ndarray,
    byte_tables: np.ndarray,
    norms: np.ndarray | None = None,
) -> np.ndarray:
    scores = np.zeros(packed_codes.shape[0], dtype=np.float32)
    chunk_size = 64
    for start in range(0, byte_tables.shape[0], chunk_size):
        stop = min(start + chunk_size, len(byte_tables))
        table_block = byte_tables[start:stop]
        code_block = packed_codes[:, start:stop].T.astype(np.intp, copy=False)
        scores += np.take_along_axis(table_block, code_block, axis=1).sum(axis=0, dtype=np.float32)
    if norms is not None:
        scores *= norms
    return scores


def splitmix64(values: np.ndarray) -> np.ndarray:
    mask = np.uint64(0xFFFFFFFFFFFFFFFF)
    z = (values + np.uint64(0x9E3779B97F4A7C15)) & mask
    z = ((z ^ (z >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)) & mask
    z = ((z ^ (z >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)) & mask
    return z ^ (z >> np.uint64(31))


def deterministic_dither(dim: int, seed: int, step: float) -> np.ndarray:
    idx = np.arange(dim, dtype=np.uint64) + np.uint64(seed)
    mixed = splitmix64(idx)
    unit = ((mixed >> np.uint64(11)).astype(np.float64) / float(1 << 53)).astype(np.float32)
    return ((unit - 0.5) * step).astype(np.float32)


def score_luts_to_byte_tables(score_luts: np.ndarray) -> np.ndarray:
    if score_luts.ndim != 2:
        raise ValueError("score_luts_to_byte_tables expects a 2-D array")
    lo_tables = np.ascontiguousarray(score_luts[0::2][:, BYTE_LO_NIBBLES], dtype=np.float32)
    if score_luts.shape[0] % 2 == 0:
        hi_tables = np.ascontiguousarray(score_luts[1::2][:, BYTE_HI_NIBBLES], dtype=np.float32)
        tables = lo_tables + hi_tables
    else:
        tables = lo_tables
        if score_luts.shape[0] > 1:
            hi_tables = np.ascontiguousarray(score_luts[1::2][:, BYTE_HI_NIBBLES], dtype=np.float32)
            tables[:-1] += hi_tables
    return tables


def packed_lookup_scores(
    packed_codes: np.ndarray,
    byte_tables: np.ndarray,
    norms: np.ndarray | None = None,
) -> np.ndarray:
    if byte_tables.ndim != 2 or byte_tables.shape[1] != 256:
        raise ValueError("packed_lookup_scores expects byte_tables shaped [n_bytes, 256]")
    lib = load_packed_adc_helper()
    if lib is None:
        return packed_lookup_scores_python(packed_codes, byte_tables, norms)
    packed_codes = np.ascontiguousarray(packed_codes, dtype=np.uint8)
    byte_tables = np.ascontiguousarray(byte_tables, dtype=np.float32)
    norms_arr = None if norms is None else np.ascontiguousarray(norms, dtype=np.float32)
    scores = np.empty(packed_codes.shape[0], dtype=np.float32)
    norm_ptr = (
        ctypes.cast(0, ctypes.POINTER(ctypes.c_float))
        if norms_arr is None
        else norms_arr.ctypes.data_as(ctypes.POINTER(ctypes.c_float))
    )
    lib.turboquant_packed_adc_scores_f32(
        packed_codes.ctypes.data_as(ctypes.POINTER(ctypes.c_uint8)),
        byte_tables.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
        norm_ptr,
        ctypes.c_size_t(packed_codes.shape[0]),
        ctypes.c_size_t(packed_codes.shape[1]),
        scores.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
    )
    return scores


def packed_lookup_scores_transposed_python(
    packed_codes_t: np.ndarray,
    byte_tables: np.ndarray,
    norms: np.ndarray | None = None,
) -> np.ndarray:
    scores = np.zeros(packed_codes_t.shape[1], dtype=np.float32)
    for byte_idx in range(packed_codes_t.shape[0]):
        scores += byte_tables[byte_idx][packed_codes_t[byte_idx]]
    if norms is not None:
        scores *= norms
    return scores


def packed_lookup_scores_transposed(
    packed_codes_t: np.ndarray,
    byte_tables: np.ndarray,
    norms: np.ndarray | None = None,
) -> np.ndarray:
    if byte_tables.ndim != 2 or byte_tables.shape[1] != 256:
        raise ValueError("packed_lookup_scores_transposed expects byte_tables shaped [n_bytes, 256]")
    lib = load_packed_adc_helper()
    if lib is None:
        return packed_lookup_scores_transposed_python(packed_codes_t, byte_tables, norms)
    packed_codes_t = np.ascontiguousarray(packed_codes_t, dtype=np.uint8)
    byte_tables = np.ascontiguousarray(byte_tables, dtype=np.float32)
    norms_arr = None if norms is None else np.ascontiguousarray(norms, dtype=np.float32)
    scores = np.empty(packed_codes_t.shape[1], dtype=np.float32)
    norm_ptr = (
        ctypes.cast(0, ctypes.POINTER(ctypes.c_float))
        if norms_arr is None
        else norms_arr.ctypes.data_as(ctypes.POINTER(ctypes.c_float))
    )
    n_rows = packed_codes_t.shape[1]
    n_bytes = packed_codes_t.shape[0]
    thread_count = packed_adc_thread_count()
    if thread_count > 1 and n_rows >= 16384 and n_bytes >= 256:
        lib.turboquant_packed_adc_scores_t_mt_f32(
            packed_codes_t.ctypes.data_as(ctypes.POINTER(ctypes.c_uint8)),
            byte_tables.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
            norm_ptr,
            ctypes.c_size_t(n_rows),
            ctypes.c_size_t(n_bytes),
            ctypes.c_size_t(thread_count),
            scores.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
        )
    else:
        lib.turboquant_packed_adc_scores_t_f32(
            packed_codes_t.ctypes.data_as(ctypes.POINTER(ctypes.c_uint8)),
            byte_tables.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
            norm_ptr,
            ctypes.c_size_t(n_rows),
            ctypes.c_size_t(n_bytes),
            scores.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
        )
    return scores


def packed_lookup_scores_blockhadamard_packed4_transposed(
    packed_codes_t: np.ndarray,
    coeffs: np.ndarray,
    centers: np.ndarray,
    norms: np.ndarray | None = None,
) -> np.ndarray:
    lib = load_packed_adc_helper()
    if lib is None:
        score_luts = coeffs[:, None] * centers[None, :]
        return packed_lookup_scores_transposed(
            packed_codes_t,
            score_luts_to_byte_tables(score_luts),
            norms,
        )
    packed_codes_t = np.ascontiguousarray(packed_codes_t, dtype=np.uint8)
    coeffs = np.ascontiguousarray(coeffs, dtype=np.float32)
    centers = np.ascontiguousarray(centers, dtype=np.float32)
    norms_arr = None if norms is None else np.ascontiguousarray(norms, dtype=np.float32)
    scores = np.empty(packed_codes_t.shape[1], dtype=np.float32)
    norm_ptr = (
        ctypes.cast(0, ctypes.POINTER(ctypes.c_float))
        if norms_arr is None
        else norms_arr.ctypes.data_as(ctypes.POINTER(ctypes.c_float))
    )
    n_rows = packed_codes_t.shape[1]
    dim = coeffs.shape[0]
    thread_count = packed_adc_thread_count()
    if thread_count > 1 and n_rows >= 16384 and packed_codes_t.shape[0] >= 256:
        lib.turboquant_blockhadamard_packed4_scores_t_mt_f32(
            packed_codes_t.ctypes.data_as(ctypes.POINTER(ctypes.c_uint8)),
            coeffs.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
            centers.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
            norm_ptr,
            ctypes.c_size_t(n_rows),
            ctypes.c_size_t(dim),
            ctypes.c_size_t(thread_count),
            scores.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
        )
    else:
        lib.turboquant_blockhadamard_packed4_scores_t_f32(
            packed_codes_t.ctypes.data_as(ctypes.POINTER(ctypes.c_uint8)),
            coeffs.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
            centers.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
            norm_ptr,
            ctypes.c_size_t(n_rows),
            ctypes.c_size_t(dim),
            scores.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
    )
    return scores


def packed_topk_blockhadamard_packed4_transposed(
    packed_codes_t: np.ndarray,
    coeffs: np.ndarray,
    centers: np.ndarray,
    norms: np.ndarray | None,
    k: int,
) -> np.ndarray:
    lib = load_packed_adc_helper()
    if lib is None:
        scores = packed_lookup_scores_blockhadamard_packed4_transposed(
            packed_codes_t, coeffs, centers, norms
        )
        return topk_indices(scores, k)
    packed_codes_t = np.ascontiguousarray(packed_codes_t, dtype=np.uint8)
    coeffs = np.ascontiguousarray(coeffs, dtype=np.float32)
    centers = np.ascontiguousarray(centers, dtype=np.float32)
    norms_arr = None if norms is None else np.ascontiguousarray(norms, dtype=np.float32)
    norm_ptr = (
        ctypes.cast(0, ctypes.POINTER(ctypes.c_float))
        if norms_arr is None
        else norms_arr.ctypes.data_as(ctypes.POINTER(ctypes.c_float))
    )
    out_ids = np.empty(k, dtype=np.int32)
    out_scores = np.empty(k, dtype=np.float32)
    lib.turboquant_blockhadamard_packed4_topk_t_mt_f32(
        packed_codes_t.ctypes.data_as(ctypes.POINTER(ctypes.c_uint8)),
        coeffs.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
        centers.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
        norm_ptr,
        ctypes.c_size_t(packed_codes_t.shape[1]),
        ctypes.c_size_t(coeffs.shape[0]),
        ctypes.c_size_t(packed_adc_thread_count()),
        ctypes.c_size_t(k),
        out_ids.ctypes.data_as(ctypes.POINTER(ctypes.c_int32)),
        out_scores.ctypes.data_as(ctypes.POINTER(ctypes.c_float)),
    )
    return out_ids[out_ids >= 0]


def reset_blockhadamard_packed4_profile() -> None:
    lib = load_packed_adc_helper()
    if lib is None:
        return
    lib.turboquant_blockhadamard_packed4_profile_reset()


def get_blockhadamard_packed4_profile() -> dict[str, float | int] | None:
    lib = load_packed_adc_helper()
    if lib is None:
        return None
    build_ms = ctypes.c_double()
    score_ms = ctypes.c_double()
    calls = ctypes.c_uint64()
    lib.turboquant_blockhadamard_packed4_profile_get(
        ctypes.byref(build_ms),
        ctypes.byref(score_ms),
        ctypes.byref(calls),
    )
    return {
        "c_build_ms_total": float(build_ms.value),
        "c_score_ms_total": float(score_ms.value),
        "c_calls": int(calls.value),
    }


def get_blockhadamard_packed4_topk_profile() -> dict[str, float | int] | None:
    lib = load_packed_adc_helper()
    if lib is None:
        return None
    build_ms = ctypes.c_double()
    score_ms = ctypes.c_double()
    merge_ms = ctypes.c_double()
    calls = ctypes.c_uint64()
    lib.turboquant_blockhadamard_packed4_topk_profile_get(
        ctypes.byref(build_ms),
        ctypes.byref(score_ms),
        ctypes.byref(merge_ms),
        ctypes.byref(calls),
    )
    return {
        "c_build_ms_total": float(build_ms.value),
        "c_score_ms_total": float(score_ms.value),
        "c_merge_ms_total": float(merge_ms.value),
        "c_calls": int(calls.value),
    }


def expand_group_scales(group_scales: np.ndarray, dim: int, group_size: int) -> np.ndarray:
    return np.repeat(group_scales, group_size)[:dim].astype(np.float32, copy=False)


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
    stage_profile: dict[str, float | int] | None = None


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
        stage_profile=first.stage_profile,
    )


def nonfinite_row_count(vectors: np.ndarray) -> int:
    return int((~np.isfinite(vectors).all(axis=1)).sum())


def drop_nonfinite_rows(vectors: np.ndarray) -> tuple[np.ndarray, int]:
    mask = np.isfinite(vectors).all(axis=1)
    dropped = int((~mask).sum())
    return vectors[mask].astype(np.float32, copy=False), dropped


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

    def reset_profile(self) -> None:
        return None

    def profile_summary(self) -> dict[str, float | int] | None:
        return None


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
        self.norms = maybe_store_norms(norms)
        self.decoded_rot = self.centers[codes] / math.sqrt(self.dim)

    def search(self, query: np.ndarray, k: int) -> np.ndarray:
        q_rot = query @ self.rotation
        scores = self.decoded_rot @ q_rot
        if self.norms is not None:
            scores *= self.norms
        return topk_indices(scores, k)

    def bytes_per_vec(self) -> float:
        if self.codes is None:
            raise RuntimeError("fit must run first")
        norm_bytes = 4.0 if self.norms is not None else 0.0
        return float(self.codes.shape[1] * self.bits / 8.0 + norm_bytes)

    def metadata_bytes(self) -> int:
        total = 0
        if self.rotation is not None:
            total += int(self.rotation.nbytes)
        if self.centers is not None:
            total += int(self.centers.nbytes)
        if self.bounds is not None:
            total += int(self.bounds.nbytes)
        return total


class TurboQuantBlockHadamardMethod(RetrievalMethod):
    def __init__(self, bits: int, seed: int) -> None:
        super().__init__("turboquant_blockhadamard")
        self.bits = bits
        self.seed = seed
        self.perm: np.ndarray | None = None
        self.signs: np.ndarray | None = None
        self.blocks: list[int] = []
        self.codes: np.ndarray | None = None
        self.norms: np.ndarray | None = None
        self.decoded_rot: np.ndarray | None = None
        self.centers: np.ndarray | None = None
        self.bounds: np.ndarray | None = None
        self.dim = 0

    def fit(self, base: np.ndarray) -> None:
        self.dim = base.shape[1]
        rng = np.random.default_rng(self.seed)
        self.perm = rng.permutation(self.dim).astype(np.int32)
        self.signs = rng.choice(np.array([-1.0, 1.0], dtype=np.float32), size=self.dim, replace=True)
        self.blocks = power_of_two_blocks(self.dim)
        self.centers, self.bounds = gaussian_lloyd_max(self.bits)
        norms = np.linalg.norm(base, axis=1).astype(np.float32)
        unit = base / np.maximum(norms[:, None], 1e-12)
        rotated = structured_block_hadamard(unit, self.perm, self.signs, self.blocks) * math.sqrt(self.dim)
        codes = np.digitize(rotated, self.bounds[1:-1], right=False).astype(np.uint8)
        self.codes = codes
        self.norms = maybe_store_norms(norms)
        self.decoded_rot = self.centers[codes] / math.sqrt(self.dim)

    def search(self, query: np.ndarray, k: int) -> np.ndarray:
        q_rot = structured_block_hadamard_vec(query, self.perm, self.signs, self.blocks)
        scores = self.decoded_rot @ q_rot
        if self.norms is not None:
            scores *= self.norms
        return topk_indices(scores, k)

    def bytes_per_vec(self) -> float:
        if self.codes is None:
            raise RuntimeError("fit must run first")
        norm_bytes = 4.0 if self.norms is not None else 0.0
        return float(self.codes.shape[1] * self.bits / 8.0 + norm_bytes)

    def metadata_bytes(self) -> int:
        # Seed-derived structured transform: only tiny config needs storing.
        return 16


class TurboQuantBlockHadamardPackedMethod(RetrievalMethod):
    def __init__(self, bits: int, seed: int) -> None:
        super().__init__("turboquant_blockhadamard_packed4")
        self.bits = bits
        self.seed = seed
        self.perm: np.ndarray | None = None
        self.signs: np.ndarray | None = None
        self.blocks: list[int] = []
        self.packed_codes: np.ndarray | None = None
        self.packed_codes_t: np.ndarray | None = None
        self.norms: np.ndarray | None = None
        self.centers: np.ndarray | None = None
        self.dim = 0
        self.transform_ms_total = 0.0
        self.transform_calls = 0

    def fit(self, base: np.ndarray) -> None:
        if self.bits != 4:
            raise ValueError("turboquant_blockhadamard_packed4 currently requires --turbo-bits=4")
        load_packed_adc_helper()
        self.dim = base.shape[1]
        rng = np.random.default_rng(self.seed)
        self.perm = rng.permutation(self.dim).astype(np.int32)
        self.signs = rng.choice(np.array([-1.0, 1.0], dtype=np.float32), size=self.dim, replace=True)
        self.blocks = power_of_two_blocks(self.dim)
        self.centers, bounds = gaussian_lloyd_max(self.bits)
        norms = np.linalg.norm(base, axis=1).astype(np.float32)
        unit = base / np.maximum(norms[:, None], 1e-12)
        rotated = structured_block_hadamard(unit, self.perm, self.signs, self.blocks) * math.sqrt(self.dim)
        codes = np.digitize(rotated, bounds[1:-1], right=False).astype(np.uint8)
        self.packed_codes = pack_nibbles(codes)
        self.packed_codes_t = transpose_packed_codes(self.packed_codes)
        self.norms = maybe_store_norms(norms)
        self.transform_ms_total = 0.0
        self.transform_calls = 0

    def search(self, query: np.ndarray, k: int) -> np.ndarray:
        if self.packed_codes_t is None or self.centers is None:
            raise RuntimeError("fit must run first")
        t0 = time.perf_counter()
        q_rot = structured_block_hadamard_vec(query, self.perm, self.signs, self.blocks)
        self.transform_ms_total += (time.perf_counter() - t0) * 1000.0
        self.transform_calls += 1
        coeffs = (q_rot / math.sqrt(self.dim)).astype(np.float32, copy=False)
        scores = packed_lookup_scores_blockhadamard_packed4_transposed(
            self.packed_codes_t,
            coeffs,
            self.centers,
            self.norms,
        )
        return topk_indices(scores, k)

    def bytes_per_vec(self) -> float:
        if self.packed_codes is None:
            raise RuntimeError("fit must run first")
        norm_bytes = 4.0 if self.norms is not None else 0.0
        return float(self.packed_codes.shape[1] + norm_bytes)

    def metadata_bytes(self) -> int:
        total = 16
        if self.centers is not None:
            total += int(self.centers.nbytes)
        return total

    def reset_profile(self) -> None:
        self.transform_ms_total = 0.0
        self.transform_calls = 0
        reset_blockhadamard_packed4_profile()

    def profile_summary(self) -> dict[str, float | int] | None:
        helper_profile = get_blockhadamard_packed4_profile()
        if helper_profile is None:
            return None
        calls = max(1, int(helper_profile["c_calls"]))
        return {
            "query_transform_ms_total": self.transform_ms_total,
            "query_transform_ms_per_query": self.transform_ms_total / max(1, self.transform_calls),
            "c_build_ms_total": float(helper_profile["c_build_ms_total"]),
            "c_build_ms_per_query": float(helper_profile["c_build_ms_total"]) / calls,
            "c_score_ms_total": float(helper_profile["c_score_ms_total"]),
            "c_score_ms_per_query": float(helper_profile["c_score_ms_total"]) / calls,
            "c_calls": int(helper_profile["c_calls"]),
        }


class TurboQuantBlockHadamardPackedTopKMethod(TurboQuantBlockHadamardPackedMethod):
    def __init__(self, bits: int, seed: int) -> None:
        super().__init__(bits, seed)
        self.name = "turboquant_blockhadamard_packed4_topk"

    def search(self, query: np.ndarray, k: int) -> np.ndarray:
        if self.packed_codes_t is None or self.centers is None:
            raise RuntimeError("fit must run first")
        t0 = time.perf_counter()
        q_rot = structured_block_hadamard_vec(query, self.perm, self.signs, self.blocks)
        self.transform_ms_total += (time.perf_counter() - t0) * 1000.0
        self.transform_calls += 1
        coeffs = (q_rot / math.sqrt(self.dim)).astype(np.float32, copy=False)
        return packed_topk_blockhadamard_packed4_transposed(
            self.packed_codes_t,
            coeffs,
            self.centers,
            self.norms,
            k,
        )

    def profile_summary(self) -> dict[str, float | int] | None:
        helper_profile = get_blockhadamard_packed4_topk_profile()
        if helper_profile is None:
            return None
        calls = max(1, int(helper_profile["c_calls"]))
        return {
            "query_transform_ms_total": self.transform_ms_total,
            "query_transform_ms_per_query": self.transform_ms_total / max(1, self.transform_calls),
            "c_build_ms_total": float(helper_profile["c_build_ms_total"]),
            "c_build_ms_per_query": float(helper_profile["c_build_ms_total"]) / calls,
            "c_score_ms_total": float(helper_profile["c_score_ms_total"]),
            "c_score_ms_per_query": float(helper_profile["c_score_ms_total"]) / calls,
            "c_merge_ms_total": float(helper_profile["c_merge_ms_total"]),
            "c_merge_ms_per_query": float(helper_profile["c_merge_ms_total"]) / calls,
            "c_calls": int(helper_profile["c_calls"]),
        }


class TurboQuantBlock32PackedMethod(TurboQuantBlockHadamardPackedMethod):
    """Packed blockhadamard with group-32 RMS scaling.

    Same C helper as plain blockhadamard_packed4 — group scales are folded
    into query coefficients at search time, so the packed codes and LUT
    scoring path are identical.
    """

    def __init__(self, bits: int, seed: int, group_size: int = 32) -> None:
        super().__init__(bits, seed)
        self.name = f"turboquant_block{group_size}_packed4"
        self.group_size = group_size
        self.group_scales: np.ndarray | None = None
        self._expanded_scales: np.ndarray | None = None

    def fit(self, base: np.ndarray) -> None:
        if self.bits != 4:
            raise ValueError(f"{self.name} currently requires --turbo-bits=4")
        load_packed_adc_helper()
        self.dim = base.shape[1]
        rng = np.random.default_rng(self.seed)
        self.perm = rng.permutation(self.dim).astype(np.int32)
        self.signs = rng.choice(np.array([-1.0, 1.0], dtype=np.float32), size=self.dim, replace=True)
        self.blocks = power_of_two_blocks(self.dim)
        self.centers, bounds = gaussian_lloyd_max(self.bits)
        norms = np.linalg.norm(base, axis=1).astype(np.float32)
        unit = base / np.maximum(norms[:, None], 1e-12)
        rotated = structured_block_hadamard(unit, self.perm, self.signs, self.blocks) * math.sqrt(self.dim)
        group_scales, expanded_scales = grouped_rms_scales(rotated, self.group_size)
        equalized = rotated / expanded_scales
        codes = np.digitize(equalized, bounds[1:-1], right=False).astype(np.uint8)
        self.packed_codes = pack_nibbles(codes)
        self.packed_codes_t = transpose_packed_codes(self.packed_codes)
        self.norms = maybe_store_norms(norms)
        self.group_scales = group_scales
        self._expanded_scales = expand_group_scales(group_scales, self.dim, self.group_size)
        self.transform_ms_total = 0.0
        self.transform_calls = 0

    def search(self, query: np.ndarray, k: int) -> np.ndarray:
        if self.packed_codes_t is None or self.centers is None or self._expanded_scales is None:
            raise RuntimeError("fit must run first")
        t0 = time.perf_counter()
        q_rot = structured_block_hadamard_vec(query, self.perm, self.signs, self.blocks)
        self.transform_ms_total += (time.perf_counter() - t0) * 1000.0
        self.transform_calls += 1
        # Fold pre-expanded group scales into query coefficients
        coeffs = (q_rot * self._expanded_scales / math.sqrt(self.dim)).astype(np.float32, copy=False)
        scores = packed_lookup_scores_blockhadamard_packed4_transposed(
            self.packed_codes_t,
            coeffs,
            self.centers,
            self.norms,
        )
        return topk_indices(scores, k)

    def metadata_bytes(self) -> int:
        total = 24
        if self.centers is not None:
            total += int(self.centers.nbytes)
        if self.group_scales is not None:
            total += int(self.group_scales.nbytes)
        return total


class TurboQuantBlock32DitherPackedMethod(TurboQuantBlock32PackedMethod):
    """Packed block32 with dithered encoding.

    Encode with subtractive dither (better codes) but score via standard
    packed ADC on the codes, ignoring the per-row dither correction term.
    This tests whether dither's primary benefit is in CODE quality rather
    than the decode correction.
    """

    def __init__(self, bits: int, seed: int, group_size: int = 32, clip: float = 3.0) -> None:
        super().__init__(bits, seed, group_size)
        self.name = f"turboquant_block{group_size}_dither_packed4"
        self.clip = clip

    def fit(self, base: np.ndarray) -> None:
        if self.bits != 4:
            raise ValueError(f"{self.name} currently requires --turbo-bits=4")
        load_packed_adc_helper()
        self.dim = base.shape[1]
        rng = np.random.default_rng(self.seed)
        self.perm = rng.permutation(self.dim).astype(np.int32)
        self.signs = rng.choice(np.array([-1.0, 1.0], dtype=np.float32), size=self.dim, replace=True)
        self.blocks = power_of_two_blocks(self.dim)
        norms = np.linalg.norm(base, axis=1).astype(np.float32)
        unit = base / np.maximum(norms[:, None], 1e-12)
        rotated = structured_block_hadamard(unit, self.perm, self.signs, self.blocks) * math.sqrt(self.dim)
        group_scales, expanded_scales = grouped_rms_scales(rotated, self.group_size)
        equalized = rotated / expanded_scales
        # Dithered uniform quantization for better code assignment
        max_abs, n_levels = symmetric_int_levels(self.bits)
        step = (2.0 * self.clip) / float(n_levels - 1)
        dither_rng = np.random.default_rng(self.seed + 17)
        dither = dither_rng.uniform(-0.5 * step, 0.5 * step, size=equalized.shape).astype(np.float32)
        scaled = np.clip((equalized + dither) / step, -max_abs, max_abs)
        codes_signed = np.rint(scaled).astype(np.int16)
        # Unsigned codes 0..14 for nibble packing (15 levels from 4-bit symmetric)
        codes_unsigned = np.clip(codes_signed + max_abs, 0, 2 * max_abs).astype(np.uint8)
        self.packed_codes = pack_nibbles(codes_unsigned)
        self.packed_codes_t = transpose_packed_codes(self.packed_codes)
        self.norms = maybe_store_norms(norms)
        self.group_scales = group_scales
        self._expanded_scales = expand_group_scales(group_scales, self.dim, self.group_size)
        # Centers for uniform quantization: code_unsigned=i → value=(i-max_abs)*step
        # The C helper computes coeff[d] * centers[level], so centers must be
        # the decoded values WITHOUT group_scale (scale is folded into coeffs).
        self.centers = np.array([(i - max_abs) * step for i in range(2 * max_abs + 1)],
                                dtype=np.float32)
        # Pad to 16 entries if needed (4-bit = max 16 levels, we have 15)
        if len(self.centers) < 16:
            self.centers = np.pad(self.centers, (0, 16 - len(self.centers)),
                                  constant_values=0.0)
        self.transform_ms_total = 0.0
        self.transform_calls = 0


class TurboQuantBlockHadamardWhitenedMethod(RetrievalMethod):
    def __init__(self, bits: int, seed: int) -> None:
        super().__init__("turboquant_blockhadamard_whitened")
        self.bits = bits
        self.seed = seed
        self.perm: np.ndarray | None = None
        self.signs: np.ndarray | None = None
        self.blocks: list[int] = []
        self.codes: np.ndarray | None = None
        self.norms: np.ndarray | None = None
        self.decoded_rot: np.ndarray | None = None
        self.centers: np.ndarray | None = None
        self.bounds: np.ndarray | None = None
        self.scales: np.ndarray | None = None
        self.dim = 0

    def fit(self, base: np.ndarray) -> None:
        self.dim = base.shape[1]
        rng = np.random.default_rng(self.seed)
        self.perm = rng.permutation(self.dim).astype(np.int32)
        self.signs = rng.choice(np.array([-1.0, 1.0], dtype=np.float32), size=self.dim, replace=True)
        self.blocks = power_of_two_blocks(self.dim)
        self.centers, self.bounds = gaussian_lloyd_max(self.bits)
        norms = np.linalg.norm(base, axis=1).astype(np.float32)
        unit = base / np.maximum(norms[:, None], 1e-12)
        rotated = structured_block_hadamard(unit, self.perm, self.signs, self.blocks) * math.sqrt(self.dim)
        scales = np.std(rotated, axis=0).astype(np.float32)
        scales = np.maximum(scales, 1e-4)
        whitened = rotated / scales
        codes = np.digitize(whitened, self.bounds[1:-1], right=False).astype(np.uint8)
        self.codes = codes
        self.norms = maybe_store_norms(norms)
        self.scales = scales
        self.decoded_rot = (self.centers[codes] * scales) / math.sqrt(self.dim)

    def search(self, query: np.ndarray, k: int) -> np.ndarray:
        q_rot = structured_block_hadamard(query[np.newaxis, :], self.perm, self.signs, self.blocks)[0]
        scores = self.decoded_rot @ q_rot
        if self.norms is not None:
            scores *= self.norms
        return topk_indices(scores, k)

    def bytes_per_vec(self) -> float:
        if self.codes is None:
            raise RuntimeError("fit must run first")
        norm_bytes = 4.0 if self.norms is not None else 0.0
        return float(self.codes.shape[1] * self.bits / 8.0 + norm_bytes)

    def metadata_bytes(self) -> int:
        total = 16
        if self.scales is not None:
            total += int(self.scales.nbytes)
        return total


class TurboQuantBlockHadamardBlockwiseMethod(RetrievalMethod):
    def __init__(self, bits: int, seed: int, group_size: int = 32) -> None:
        super().__init__(f"turboquant_blockhadamard_block{group_size}")
        self.bits = bits
        self.seed = seed
        self.group_size = group_size
        self.perm: np.ndarray | None = None
        self.signs: np.ndarray | None = None
        self.blocks: list[int] = []
        self.codes: np.ndarray | None = None
        self.norms: np.ndarray | None = None
        self.decoded_rot: np.ndarray | None = None
        self.centers: np.ndarray | None = None
        self.bounds: np.ndarray | None = None
        self.group_scales: np.ndarray | None = None
        self.dim = 0

    def fit(self, base: np.ndarray) -> None:
        self.dim = base.shape[1]
        rng = np.random.default_rng(self.seed)
        self.perm = rng.permutation(self.dim).astype(np.int32)
        self.signs = rng.choice(np.array([-1.0, 1.0], dtype=np.float32), size=self.dim, replace=True)
        self.blocks = power_of_two_blocks(self.dim)
        self.centers, self.bounds = gaussian_lloyd_max(self.bits)
        norms = np.linalg.norm(base, axis=1).astype(np.float32)
        unit = base / np.maximum(norms[:, None], 1e-12)
        rotated = structured_block_hadamard(unit, self.perm, self.signs, self.blocks) * math.sqrt(self.dim)
        group_scales, expanded_scales = grouped_rms_scales(rotated, self.group_size)
        equalized = rotated / expanded_scales
        codes = np.digitize(equalized, self.bounds[1:-1], right=False).astype(np.uint8)
        self.codes = codes
        self.norms = maybe_store_norms(norms)
        self.group_scales = group_scales
        self.decoded_rot = (self.centers[codes] * expanded_scales) / math.sqrt(self.dim)

    def search(self, query: np.ndarray, k: int) -> np.ndarray:
        q_rot = structured_block_hadamard(query[np.newaxis, :], self.perm, self.signs, self.blocks)[0]
        scores = self.decoded_rot @ q_rot
        if self.norms is not None:
            scores *= self.norms
        return topk_indices(scores, k)

    def bytes_per_vec(self) -> float:
        if self.codes is None:
            raise RuntimeError("fit must run first")
        norm_bytes = 4.0 if self.norms is not None else 0.0
        return float(self.codes.shape[1] * self.bits / 8.0 + norm_bytes)

    def metadata_bytes(self) -> int:
        total = 16
        if self.group_scales is not None:
            total += int(self.group_scales.nbytes)
        return total


class TurboQuantBlockHadamardTwoPassMethod(RetrievalMethod):
    def __init__(self, bits: int, seed: int) -> None:
        super().__init__("turboquant_blockhadamard_twopass")
        self.bits = bits
        self.seed = seed
        self.perm1: np.ndarray | None = None
        self.signs1: np.ndarray | None = None
        self.perm2: np.ndarray | None = None
        self.signs2: np.ndarray | None = None
        self.blocks: list[int] = []
        self.codes: np.ndarray | None = None
        self.norms: np.ndarray | None = None
        self.decoded_rot: np.ndarray | None = None
        self.centers: np.ndarray | None = None
        self.bounds: np.ndarray | None = None
        self.dim = 0

    def fit(self, base: np.ndarray) -> None:
        self.dim = base.shape[1]
        rng1 = np.random.default_rng(self.seed)
        rng2 = np.random.default_rng(self.seed + 1)
        self.perm1 = rng1.permutation(self.dim).astype(np.int32)
        self.signs1 = rng1.choice(np.array([-1.0, 1.0], dtype=np.float32), size=self.dim, replace=True)
        self.perm2 = rng2.permutation(self.dim).astype(np.int32)
        self.signs2 = rng2.choice(np.array([-1.0, 1.0], dtype=np.float32), size=self.dim, replace=True)
        self.blocks = power_of_two_blocks(self.dim)
        self.centers, self.bounds = gaussian_lloyd_max(self.bits)
        norms = np.linalg.norm(base, axis=1).astype(np.float32)
        unit = base / np.maximum(norms[:, None], 1e-12)
        rotated = two_pass_block_hadamard(
            unit,
            self.perm1,
            self.signs1,
            self.perm2,
            self.signs2,
            self.blocks,
        ) * math.sqrt(self.dim)
        codes = np.digitize(rotated, self.bounds[1:-1], right=False).astype(np.uint8)
        self.codes = codes
        self.norms = maybe_store_norms(norms)
        self.decoded_rot = self.centers[codes] / math.sqrt(self.dim)

    def search(self, query: np.ndarray, k: int) -> np.ndarray:
        q_rot = two_pass_block_hadamard(
            query[np.newaxis, :],
            self.perm1,
            self.signs1,
            self.perm2,
            self.signs2,
            self.blocks,
        )[0]
        scores = self.decoded_rot @ q_rot
        if self.norms is not None:
            scores *= self.norms
        return topk_indices(scores, k)

    def bytes_per_vec(self) -> float:
        if self.codes is None:
            raise RuntimeError("fit must run first")
        norm_bytes = 4.0 if self.norms is not None else 0.0
        return float(self.codes.shape[1] * self.bits / 8.0 + norm_bytes)

    def metadata_bytes(self) -> int:
        return 32


class TurboQuantBlockwiseCompandedMethod(RetrievalMethod):
    def __init__(self, bits: int, seed: int, group_size: int = 32, beta: float = 0.75, clip: float = 3.0) -> None:
        super().__init__(f"turboquant_block{group_size}_compand")
        self.bits = bits
        self.seed = seed
        self.group_size = group_size
        self.beta = beta
        self.clip = clip
        self.perm: np.ndarray | None = None
        self.signs: np.ndarray | None = None
        self.blocks: list[int] = []
        self.codes: np.ndarray | None = None
        self.norms: np.ndarray | None = None
        self.decoded_rot: np.ndarray | None = None
        self.group_scales: np.ndarray | None = None
        self.dim = 0

    def fit(self, base: np.ndarray) -> None:
        self.dim = base.shape[1]
        rng = np.random.default_rng(self.seed)
        self.perm = rng.permutation(self.dim).astype(np.int32)
        self.signs = rng.choice(np.array([-1.0, 1.0], dtype=np.float32), size=self.dim, replace=True)
        self.blocks = power_of_two_blocks(self.dim)
        norms = np.linalg.norm(base, axis=1).astype(np.float32)
        unit = base / np.maximum(norms[:, None], 1e-12)
        rotated = structured_block_hadamard(unit, self.perm, self.signs, self.blocks) * math.sqrt(self.dim)
        group_scales, expanded_scales = grouped_rms_scales(rotated, self.group_size)
        equalized = rotated / expanded_scales
        companded = power_compand(equalized, self.beta)
        codes, decoded_companded, _ = uniform_quantize(companded, self.bits, self.clip)
        self.codes = codes
        self.norms = maybe_store_norms(norms)
        self.group_scales = group_scales
        self.decoded_rot = (power_decompand(decoded_companded, self.beta) * expanded_scales) / math.sqrt(self.dim)

    def search(self, query: np.ndarray, k: int) -> np.ndarray:
        q_rot = structured_block_hadamard(query[np.newaxis, :], self.perm, self.signs, self.blocks)[0]
        scores = self.decoded_rot @ q_rot
        if self.norms is not None:
            scores *= self.norms
        return topk_indices(scores, k)

    def bytes_per_vec(self) -> float:
        if self.codes is None:
            raise RuntimeError("fit must run first")
        norm_bytes = 4.0 if self.norms is not None else 0.0
        return float(self.codes.shape[1] * self.bits / 8.0 + norm_bytes)

    def metadata_bytes(self) -> int:
        total = 24
        if self.group_scales is not None:
            total += int(self.group_scales.nbytes)
        return total


class TurboQuantBlockwiseDitheredMethod(RetrievalMethod):
    def __init__(self, bits: int, seed: int, group_size: int = 32, clip: float = 3.0) -> None:
        super().__init__(f"turboquant_block{group_size}_dither")
        self.bits = bits
        self.seed = seed
        self.group_size = group_size
        self.clip = clip
        self.perm: np.ndarray | None = None
        self.signs: np.ndarray | None = None
        self.blocks: list[int] = []
        self.codes: np.ndarray | None = None
        self.norms: np.ndarray | None = None
        self.decoded_rot: np.ndarray | None = None
        self.group_scales: np.ndarray | None = None
        self.dim = 0

    def fit(self, base: np.ndarray) -> None:
        self.dim = base.shape[1]
        rng = np.random.default_rng(self.seed)
        self.perm = rng.permutation(self.dim).astype(np.int32)
        self.signs = rng.choice(np.array([-1.0, 1.0], dtype=np.float32), size=self.dim, replace=True)
        self.blocks = power_of_two_blocks(self.dim)
        norms = np.linalg.norm(base, axis=1).astype(np.float32)
        unit = base / np.maximum(norms[:, None], 1e-12)
        rotated = structured_block_hadamard(unit, self.perm, self.signs, self.blocks) * math.sqrt(self.dim)
        group_scales, expanded_scales = grouped_rms_scales(rotated, self.group_size)
        equalized = rotated / expanded_scales
        codes, decoded_equalized, _ = dithered_uniform_quantize(equalized, self.bits, self.clip, self.seed + 17)
        self.codes = codes
        self.norms = maybe_store_norms(norms)
        self.group_scales = group_scales
        self.decoded_rot = (decoded_equalized * expanded_scales) / math.sqrt(self.dim)

    def search(self, query: np.ndarray, k: int) -> np.ndarray:
        q_rot = structured_block_hadamard(query[np.newaxis, :], self.perm, self.signs, self.blocks)[0]
        scores = self.decoded_rot @ q_rot
        if self.norms is not None:
            scores *= self.norms
        return topk_indices(scores, k)

    def bytes_per_vec(self) -> float:
        if self.codes is None:
            raise RuntimeError("fit must run first")
        norm_bytes = 4.0 if self.norms is not None else 0.0
        return float(self.codes.shape[1] * self.bits / 8.0 + norm_bytes)

    def metadata_bytes(self) -> int:
        total = 24
        if self.group_scales is not None:
            total += int(self.group_scales.nbytes)
        return total


class TurboQuantBlockwiseDimDitherPackedMethod(RetrievalMethod):
    def __init__(self, bits: int, seed: int, group_size: int = 32, clip: float = 3.0) -> None:
        super().__init__(f"turboquant_block{group_size}_dimdither_packed4")
        self.bits = bits
        self.seed = seed
        self.group_size = group_size
        self.clip = clip
        self.perm: np.ndarray | None = None
        self.signs: np.ndarray | None = None
        self.blocks: list[int] = []
        self.packed_codes: np.ndarray | None = None
        self.norms: np.ndarray | None = None
        self.group_scales: np.ndarray | None = None
        self._decoded_levels: np.ndarray | None = None
        self.dim = 0

    def fit(self, base: np.ndarray) -> None:
        if self.bits != 4:
            raise ValueError(f"{self.name} currently requires --turbo-bits=4")
        load_packed_adc_helper()
        self.dim = base.shape[1]
        rng = np.random.default_rng(self.seed)
        self.perm = rng.permutation(self.dim).astype(np.int32)
        self.signs = rng.choice(np.array([-1.0, 1.0], dtype=np.float32), size=self.dim, replace=True)
        self.blocks = power_of_two_blocks(self.dim)
        norms = np.linalg.norm(base, axis=1).astype(np.float32)
        unit = base / np.maximum(norms[:, None], 1e-12)
        rotated = structured_block_hadamard(unit, self.perm, self.signs, self.blocks) * math.sqrt(self.dim)
        group_scales, expanded_scales = grouped_rms_scales(rotated, self.group_size)
        equalized = rotated / expanded_scales
        max_abs, levels = symmetric_int_levels(self.bits)
        step = (2.0 * self.clip) / float(levels - 1)
        dither = deterministic_dither(self.dim, self.seed + 17, step)
        scaled = np.clip((equalized + dither[np.newaxis, :]) / step, -max_abs, max_abs)
        signed_codes = np.rint(scaled).astype(np.int16)
        stored_codes = (signed_codes + max_abs).astype(np.uint8)
        decoded_levels = np.zeros(16, dtype=np.float32)
        decoded_levels[:levels] = (np.arange(levels, dtype=np.float32) - float(max_abs)) * step
        self.packed_codes = pack_nibbles(stored_codes)
        self.norms = maybe_store_norms(norms)
        self.group_scales = group_scales
        self._decoded_levels = decoded_levels

    def search(self, query: np.ndarray, k: int) -> np.ndarray:
        if self.packed_codes is None or self.group_scales is None or self._decoded_levels is None:
            raise RuntimeError("fit must run first")
        q_rot = structured_block_hadamard(query[np.newaxis, :], self.perm, self.signs, self.blocks)[0]
        expanded_scales = expand_group_scales(self.group_scales, self.dim, self.group_size)
        coeffs = (expanded_scales * q_rot / math.sqrt(self.dim)).astype(np.float32, copy=False)
        # The deterministic dim-only dither contributes a query-constant bias, so
        # packed search can omit it without changing ranking.
        score_luts = coeffs[:, None] * self._decoded_levels[None, :]
        scores = packed_lookup_scores(self.packed_codes, score_luts_to_byte_tables(score_luts), self.norms)
        return topk_indices(scores, k)

    def bytes_per_vec(self) -> float:
        if self.packed_codes is None:
            raise RuntimeError("fit must run first")
        norm_bytes = 4.0 if self.norms is not None else 0.0
        return float(self.packed_codes.shape[1] + norm_bytes)

    def metadata_bytes(self) -> int:
        total = 24
        if self.group_scales is not None:
            total += int(self.group_scales.nbytes)
        return total


class TurboQuantBlockwiseD4Method(RetrievalMethod):
    def __init__(self, bits: int, seed: int, group_size: int = 32, clip: float = 3.0) -> None:
        super().__init__(f"turboquant_block{group_size}_d4")
        self.bits = bits
        self.seed = seed
        self.group_size = group_size
        self.clip = clip
        self.perm: np.ndarray | None = None
        self.signs: np.ndarray | None = None
        self.blocks: list[int] = []
        self.codes: np.ndarray | None = None
        self.norms: np.ndarray | None = None
        self.decoded_rot: np.ndarray | None = None
        self.group_scales: np.ndarray | None = None
        self.dim = 0

    def fit(self, base: np.ndarray) -> None:
        self.dim = base.shape[1]
        rng = np.random.default_rng(self.seed)
        self.perm = rng.permutation(self.dim).astype(np.int32)
        self.signs = rng.choice(np.array([-1.0, 1.0], dtype=np.float32), size=self.dim, replace=True)
        self.blocks = power_of_two_blocks(self.dim)
        norms = np.linalg.norm(base, axis=1).astype(np.float32)
        unit = base / np.maximum(norms[:, None], 1e-12)
        rotated = structured_block_hadamard(unit, self.perm, self.signs, self.blocks) * math.sqrt(self.dim)
        group_scales, expanded_scales = grouped_rms_scales(rotated, self.group_size)
        equalized = rotated / expanded_scales
        codes, decoded_equalized, _ = d4_quantize_rows(equalized, self.bits, self.clip)
        self.codes = codes
        self.norms = maybe_store_norms(norms)
        self.group_scales = group_scales
        self.decoded_rot = (decoded_equalized * expanded_scales) / math.sqrt(self.dim)

    def search(self, query: np.ndarray, k: int) -> np.ndarray:
        q_rot = structured_block_hadamard(query[np.newaxis, :], self.perm, self.signs, self.blocks)[0]
        scores = self.decoded_rot @ q_rot
        if self.norms is not None:
            scores *= self.norms
        return topk_indices(scores, k)

    def bytes_per_vec(self) -> float:
        if self.codes is None:
            raise RuntimeError("fit must run first")
        norm_bytes = 4.0 if self.norms is not None else 0.0
        return float(self.codes.shape[1] * self.bits / 8.0 + norm_bytes)

    def metadata_bytes(self) -> int:
        total = 24
        if self.group_scales is not None:
            total += int(self.group_scales.nbytes)
        return total


class TurboQuantTwoPassBlockwiseDitheredMethod(RetrievalMethod):
    def __init__(self, bits: int, seed: int, group_size: int = 32, clip: float = 3.0) -> None:
        super().__init__(f"turboquant_twopass_block{group_size}_dither")
        self.bits = bits
        self.seed = seed
        self.group_size = group_size
        self.clip = clip
        self.perm1: np.ndarray | None = None
        self.signs1: np.ndarray | None = None
        self.perm2: np.ndarray | None = None
        self.signs2: np.ndarray | None = None
        self.blocks: list[int] = []
        self.codes: np.ndarray | None = None
        self.norms: np.ndarray | None = None
        self.decoded_rot: np.ndarray | None = None
        self.group_scales: np.ndarray | None = None
        self.dim = 0

    def fit(self, base: np.ndarray) -> None:
        self.dim = base.shape[1]
        rng1 = np.random.default_rng(self.seed)
        rng2 = np.random.default_rng(self.seed + 1)
        self.perm1 = rng1.permutation(self.dim).astype(np.int32)
        self.signs1 = rng1.choice(np.array([-1.0, 1.0], dtype=np.float32), size=self.dim, replace=True)
        self.perm2 = rng2.permutation(self.dim).astype(np.int32)
        self.signs2 = rng2.choice(np.array([-1.0, 1.0], dtype=np.float32), size=self.dim, replace=True)
        self.blocks = power_of_two_blocks(self.dim)
        norms = np.linalg.norm(base, axis=1).astype(np.float32)
        unit = base / np.maximum(norms[:, None], 1e-12)
        rotated = two_pass_block_hadamard(
            unit,
            self.perm1,
            self.signs1,
            self.perm2,
            self.signs2,
            self.blocks,
        ) * math.sqrt(self.dim)
        group_scales, expanded_scales = grouped_rms_scales(rotated, self.group_size)
        equalized = rotated / expanded_scales
        codes, decoded_equalized, _ = dithered_uniform_quantize(equalized, self.bits, self.clip, self.seed + 29)
        self.codes = codes
        self.norms = maybe_store_norms(norms)
        self.group_scales = group_scales
        self.decoded_rot = (decoded_equalized * expanded_scales) / math.sqrt(self.dim)

    def search(self, query: np.ndarray, k: int) -> np.ndarray:
        q_rot = two_pass_block_hadamard(
            query[np.newaxis, :],
            self.perm1,
            self.signs1,
            self.perm2,
            self.signs2,
            self.blocks,
        )[0]
        scores = self.decoded_rot @ q_rot
        if self.norms is not None:
            scores *= self.norms
        return topk_indices(scores, k)

    def bytes_per_vec(self) -> float:
        if self.codes is None:
            raise RuntimeError("fit must run first")
        norm_bytes = 4.0 if self.norms is not None else 0.0
        return float(self.codes.shape[1] * self.bits / 8.0 + norm_bytes)

    def metadata_bytes(self) -> int:
        total = 40
        if self.group_scales is not None:
            total += int(self.group_scales.nbytes)
        return total


class TurboQuantTwoPassBlockwiseMethod(RetrievalMethod):
    def __init__(self, bits: int, seed: int, group_size: int = 32) -> None:
        super().__init__(f"turboquant_twopass_block{group_size}")
        self.bits = bits
        self.seed = seed
        self.group_size = group_size
        self.perm1: np.ndarray | None = None
        self.signs1: np.ndarray | None = None
        self.perm2: np.ndarray | None = None
        self.signs2: np.ndarray | None = None
        self.blocks: list[int] = []
        self.codes: np.ndarray | None = None
        self.norms: np.ndarray | None = None
        self.decoded_rot: np.ndarray | None = None
        self.centers: np.ndarray | None = None
        self.bounds: np.ndarray | None = None
        self.group_scales: np.ndarray | None = None
        self.dim = 0

    def fit(self, base: np.ndarray) -> None:
        self.dim = base.shape[1]
        rng1 = np.random.default_rng(self.seed)
        rng2 = np.random.default_rng(self.seed + 1)
        self.perm1 = rng1.permutation(self.dim).astype(np.int32)
        self.signs1 = rng1.choice(np.array([-1.0, 1.0], dtype=np.float32), size=self.dim, replace=True)
        self.perm2 = rng2.permutation(self.dim).astype(np.int32)
        self.signs2 = rng2.choice(np.array([-1.0, 1.0], dtype=np.float32), size=self.dim, replace=True)
        self.blocks = power_of_two_blocks(self.dim)
        self.centers, self.bounds = gaussian_lloyd_max(self.bits)
        norms = np.linalg.norm(base, axis=1).astype(np.float32)
        unit = base / np.maximum(norms[:, None], 1e-12)
        rotated = two_pass_block_hadamard(
            unit,
            self.perm1,
            self.signs1,
            self.perm2,
            self.signs2,
            self.blocks,
        ) * math.sqrt(self.dim)
        group_scales, expanded_scales = grouped_rms_scales(rotated, self.group_size)
        equalized = rotated / expanded_scales
        codes = np.digitize(equalized, self.bounds[1:-1], right=False).astype(np.uint8)
        self.codes = codes
        self.norms = maybe_store_norms(norms)
        self.group_scales = group_scales
        self.decoded_rot = (self.centers[codes] * expanded_scales) / math.sqrt(self.dim)

    def search(self, query: np.ndarray, k: int) -> np.ndarray:
        q_rot = two_pass_block_hadamard(
            query[np.newaxis, :],
            self.perm1,
            self.signs1,
            self.perm2,
            self.signs2,
            self.blocks,
        )[0]
        scores = self.decoded_rot @ q_rot
        if self.norms is not None:
            scores *= self.norms
        return topk_indices(scores, k)

    def bytes_per_vec(self) -> float:
        if self.codes is None:
            raise RuntimeError("fit must run first")
        norm_bytes = 4.0 if self.norms is not None else 0.0
        return float(self.codes.shape[1] * self.bits / 8.0 + norm_bytes)

    def metadata_bytes(self) -> int:
        total = 32
        if self.group_scales is not None:
            total += int(self.group_scales.nbytes)
        return total


class TurboQuantProdMethod(RetrievalMethod):
    def __init__(self, bits: int, seed: int) -> None:
        super().__init__("turboquant_prod")
        if bits < 2:
            raise ValueError("turboquant_prod requires turbo_bits >= 2")
        self.bits = bits
        self.seed = seed
        self.mse_bits = bits - 1
        self.rotation: np.ndarray | None = None
        self.qjl_proj: np.ndarray | None = None
        self.codes: np.ndarray | None = None
        self.norms: np.ndarray | None = None
        self.decoded_rot: np.ndarray | None = None
        self.centers: np.ndarray | None = None
        self.bounds: np.ndarray | None = None
        self.residual_signs: np.ndarray | None = None
        self.residual_norms: np.ndarray | None = None
        self.dim = 0

    def fit(self, base: np.ndarray) -> None:
        self.dim = base.shape[1]
        self.rotation = random_orthogonal(self.dim, self.seed)
        self.qjl_proj = np.random.default_rng(self.seed + 1).standard_normal(
            (self.dim, self.dim), dtype=np.float32
        )
        self.centers, self.bounds = gaussian_lloyd_max(self.mse_bits)
        norms = np.linalg.norm(base, axis=1).astype(np.float32)
        unit = base / np.maximum(norms[:, None], 1e-12)
        rotated_unit = unit @ self.rotation
        scaled_rotated = rotated_unit * math.sqrt(self.dim)
        codes = np.digitize(scaled_rotated, self.bounds[1:-1], right=False).astype(np.uint8)
        decoded_rot = self.centers[codes] / math.sqrt(self.dim)
        residual = rotated_unit - decoded_rot
        residual_norms = np.linalg.norm(residual, axis=1).astype(np.float32)
        normalized_residual = residual / np.maximum(residual_norms[:, None], 1e-12)
        residual_signs = np.sign(normalized_residual @ self.qjl_proj.T).astype(np.int8)
        residual_signs[residual_signs == 0] = 1

        self.codes = codes
        self.norms = norms
        self.decoded_rot = decoded_rot.astype(np.float32, copy=False)
        self.residual_signs = residual_signs
        self.residual_norms = residual_norms

    def search(self, query: np.ndarray, k: int) -> np.ndarray:
        q_rot = query @ self.rotation
        mse_scores = self.decoded_rot @ q_rot
        qjl_query = np.sqrt(np.pi / 2.0) / float(self.dim) * (self.qjl_proj @ q_rot)
        qjl_scores = self.residual_signs.astype(np.float32) @ qjl_query
        scores = (mse_scores + self.residual_norms * qjl_scores) * self.norms
        return topk_indices(scores, k)

    def bytes_per_vec(self) -> float:
        if self.codes is None:
            raise RuntimeError("fit must run first")
        mse_bytes = self.codes.shape[1] * self.mse_bits / 8.0
        qjl_bytes = self.codes.shape[1] / 8.0
        return float(mse_bytes + qjl_bytes + 4.0 + 4.0)

    def metadata_bytes(self) -> int:
        total = 0
        if self.rotation is not None:
            total += int(self.rotation.nbytes)
        if self.qjl_proj is not None:
            total += int(self.qjl_proj.nbytes)
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
    profile_packed_stages: bool = False,
) -> EvalResult:
    t0 = time.perf_counter()
    method.fit(base)
    encode_ms = (time.perf_counter() - t0) * 1000.0

    latencies: list[float] = []
    hit1_parts: list[float] = []
    recall_parts: list[float] = []
    if profile_packed_stages:
        method.reset_profile()
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
        stage_profile=method.profile_summary() if profile_packed_stages else None,
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
    profiled = [row for row in rows if row.stage_profile]
    if profiled:
        print("\nStage profile")
        print("=============")
        for row in profiled:
            profile = row.stage_profile or {}
            print(
                f"{row.method}: transform={profile.get('query_transform_ms_per_query', 0.0):.3f} ms/query "
                f"c_build={profile.get('c_build_ms_per_query', 0.0):.3f} ms/query "
                f"c_score={profile.get('c_score_ms_per_query', 0.0):.3f} ms/query "
                f"c_merge={profile.get('c_merge_ms_per_query', 0.0):.3f} ms/query "
                f"calls={int(profile.get('c_calls', 0))}"
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
    ap.add_argument("--drop-nonfinite", action="store_true", help="Drop rows containing NaN/Inf instead of failing")
    ap.add_argument("--cache-dir", type=Path, default=Path("/tmp/ann_real_cache"))
    ap.add_argument("--sample-size", type=int, default=4000)
    ap.add_argument("--query-count", type=int, default=50)
    ap.add_argument("--folds", type=int, default=1, help="Repeated holdout folds for --shared-sql mode")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--turbo-bits", type=int, default=4)
    ap.add_argument("--profile-packed-stages", action="store_true", help="Print packed blockhadamard stage timings")
    ap.add_argument("--pq-m", type=int, default=0, help="PQ subvector count (0=auto)")
    ap.add_argument("--pq-bits", type=int, default=8)
    ap.add_argument("--pq-max-train", type=int, default=20000)
    ap.add_argument(
        "--methods",
        help=(
            "Comma-separated approximate method allowlist. "
            "When set, selects exact methods explicitly and overrides skip/turbo-research defaults. "
            "float32_exact is always included."
        ),
    )
    ap.add_argument("--skip-fp16", action="store_true")
    ap.add_argument("--skip-sq8", action="store_true")
    ap.add_argument("--skip-pq", action="store_true")
    ap.add_argument("--skip-turbo", action="store_true")
    ap.add_argument(
        "--turbo-research",
        action="store_true",
        help="Include additional no-codebook research comparators (compander, dither, twopass, D4).",
    )
    return ap


def parse_method_allowlist(value: str | None) -> list[str] | None:
    if value is None:
        return None
    names = [part.strip() for part in value.split(",") if part.strip()]
    if not names:
        raise SystemExit("--methods must name at least one method")
    return names


def method_factories(base: np.ndarray, args: argparse.Namespace) -> list[tuple[str, Callable[[], RetrievalMethod]]]:
    return [
        ("fp16", lambda: Fp16Method()),
        ("sq8_linear", lambda: Sq8LinearMethod()),
        ("pq_kmeans", lambda: PQKMeansMethod(auto_pq_m(base.shape[1], args.pq_m), args.pq_bits, args.pq_max_train, args.seed)),
        ("turboquant_mse", lambda: TurboQuantMSEMethod(args.turbo_bits, args.seed)),
        ("turboquant_blockhadamard", lambda: TurboQuantBlockHadamardMethod(args.turbo_bits, args.seed)),
        ("turboquant_blockhadamard_packed4", lambda: TurboQuantBlockHadamardPackedMethod(args.turbo_bits, args.seed)),
        ("turboquant_blockhadamard_packed4_topk", lambda: TurboQuantBlockHadamardPackedTopKMethod(args.turbo_bits, args.seed)),
        ("turboquant_block32_packed4", lambda: TurboQuantBlock32PackedMethod(args.turbo_bits, args.seed)),
        ("turboquant_block32_dither_packed4", lambda: TurboQuantBlock32DitherPackedMethod(args.turbo_bits, args.seed)),
        ("turboquant_blockhadamard_whitened", lambda: TurboQuantBlockHadamardWhitenedMethod(args.turbo_bits, args.seed)),
        ("turboquant_blockhadamard_block32", lambda: TurboQuantBlockHadamardBlockwiseMethod(args.turbo_bits, args.seed)),
        ("turboquant_blockhadamard_twopass", lambda: TurboQuantBlockHadamardTwoPassMethod(args.turbo_bits, args.seed)),
        ("turboquant_twopass_block32", lambda: TurboQuantTwoPassBlockwiseMethod(args.turbo_bits, args.seed)),
        ("turboquant_block32_compand", lambda: TurboQuantBlockwiseCompandedMethod(args.turbo_bits, args.seed)),
        ("turboquant_block32_dither", lambda: TurboQuantBlockwiseDitheredMethod(args.turbo_bits, args.seed)),
        ("turboquant_block32_dimdither_packed4", lambda: TurboQuantBlockwiseDimDitherPackedMethod(args.turbo_bits, args.seed)),
        ("turboquant_block32_d4", lambda: TurboQuantBlockwiseD4Method(args.turbo_bits, args.seed)),
        ("turboquant_twopass_block32_dither", lambda: TurboQuantTwoPassBlockwiseDitheredMethod(args.turbo_bits, args.seed)),
        ("turboquant_prod", lambda: TurboQuantProdMethod(args.turbo_bits, args.seed)),
    ]


def default_method_names(args: argparse.Namespace) -> list[str]:
    names: list[str] = []
    if not args.skip_fp16:
        names.append("fp16")
    if not args.skip_sq8:
        names.append("sq8_linear")
    if not args.skip_pq:
        names.append("pq_kmeans")
    if not args.skip_turbo:
        names.extend(
            [
                "turboquant_mse",
                "turboquant_blockhadamard",
                "turboquant_blockhadamard_whitened",
                "turboquant_blockhadamard_block32",
            ]
        )
        if args.turbo_research:
            names.extend(
                [
                    "turboquant_blockhadamard_twopass",
                    "turboquant_twopass_block32",
                    "turboquant_block32_compand",
                    "turboquant_block32_dither",
                    "turboquant_block32_d4",
                    "turboquant_twopass_block32_dither",
                ]
            )
        if args.turbo_bits >= 2:
            names.append("turboquant_prod")
    return names


def build_methods(base: np.ndarray, args: argparse.Namespace) -> list[RetrievalMethod]:
    factories = method_factories(base, args)
    factory_map = {name: factory for name, factory in factories}
    requested = parse_method_allowlist(args.methods)
    selected_names = requested if requested is not None else default_method_names(args)
    missing = [name for name in selected_names if name not in factory_map]
    if missing:
        available = ", ".join(name for name, _ in factories)
        raise SystemExit(
            f"unknown --methods entries: {', '.join(missing)}; available methods: {available}"
        )
    methods: list[RetrievalMethod] = []
    for name in selected_names:
        if name == "turboquant_prod" and args.turbo_bits < 2:
            raise SystemExit("turboquant_prod requires --turbo-bits >= 2")
        if name in {"turboquant_blockhadamard_packed4", "turboquant_blockhadamard_packed4_topk", "turboquant_block32_packed4", "turboquant_block32_dither_packed4", "turboquant_block32_dimdither_packed4"} and args.turbo_bits != 4:
            raise SystemExit(f"{name} currently requires --turbo-bits=4")
        methods.append(factory_map[name]())
    return methods


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

    methods = build_methods(base, args)
    for method in methods:
        rows.append(evaluate_method(method, base, queries, gt_ids, args.k, args.profile_packed_stages))
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
    input_nonfinite_rows = 0
    dropped_nonfinite_rows = 0
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
            input_nonfinite_rows = nonfinite_row_count(shared)
            if input_nonfinite_rows:
                if not args.drop_nonfinite:
                    raise SystemExit(
                        f"shared vector set contains {input_nonfinite_rows} non-finite rows; rerun with --drop-nonfinite to filter them"
                    )
                shared, dropped_nonfinite_rows = drop_nonfinite_rows(shared)
            base, queries = split_shared_vectors(shared, args.query_count, args.seed)
            folds = args.folds
        else:
            if not args.base_sql or not args.query_sql:
                raise SystemExit("--base-sql and --query-sql are required with --pg-dsn")
            base = load_pg_query_vectors(args.pg_dsn, args.base_sql)
            queries = load_pg_query_vectors(args.pg_dsn, args.query_sql)
            input_nonfinite_rows = nonfinite_row_count(base) + nonfinite_row_count(queries)
            if input_nonfinite_rows:
                if not args.drop_nonfinite:
                    raise SystemExit(
                        f"base/query vectors contain {input_nonfinite_rows} non-finite rows; rerun with --drop-nonfinite to filter them"
                    )
                base, dropped_base = drop_nonfinite_rows(base)
                queries, dropped_queries = drop_nonfinite_rows(queries)
                dropped_nonfinite_rows = dropped_base + dropped_queries
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
    if input_nonfinite_rows:
        print(
            f"nonfinite_rows_detected={input_nonfinite_rows} "
            f"nonfinite_rows_dropped={dropped_nonfinite_rows}"
        )
    if any("packed4" in row.method for row in rows):
        helper_path = packed_adc_helper_path()
        helper_note = str(helper_path) if helper_path.exists() else "unavailable"
        print(
            f"packed_adc_backend={packed_adc_backend_name()} "
            f"threads={packed_adc_thread_count()} helper={helper_note}"
        )
    print_results("Results", rows)
    print("\nCaveat: turboquant_mse here implements only the first-stage MSE path.")
    print("It does not yet include the residual 1-bit QJL inner-product correction stage.")
    print(
        "turboquant_blockhadamard uses a seed-derived sign+permutation+block-Hadamard "
        "transform to cut rotation metadata."
    )
    print(
        "turboquant_blockhadamard_whitened adds per-dimension variance scaling "
        "on top of the structured block-Hadamard transform."
    )
    print(
        "turboquant_blockhadamard_block32 adds coarse blockwise RMS scaling "
        "on top of the structured block-Hadamard transform."
    )
    print(
        "Packed4 research lanes reuse the same scalar quantizers but switch search to byte-packed "
        "ADC-style lookup tables; they are kernel-shape prototypes, not optimized kernels."
    )
    if args.turbo_research:
        print(
            "turboquant research lanes add no-codebook experiments: twopass structured mixing, "
            "twopass+block scaling, blockwise companding, blockwise subtractive dither, "
            "blockwise D4 lattice rounding, and a twopass+dither combination."
        )
    if args.turbo_bits >= 2:
        print(
            "turboquant_prod uses a second-stage QJL residual correction "
            "with a dense Gaussian projection in this evaluator."
        )
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
            "input_nonfinite_rows": input_nonfinite_rows,
            "dropped_nonfinite_rows": dropped_nonfinite_rows,
            "results": [eval_result_to_dict(row) for row in rows],
            "caveat": (
                "turboquant_mse implements only the first-stage MSE path; "
                "turboquant_prod adds a dense-Gaussian QJL residual correction stage"
            ),
        }
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
