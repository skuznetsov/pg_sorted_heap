#!/usr/bin/env python3
"""
Experiment 0: Can pg_sorted_heap serve KV-sized blocks fast enough
to be a plausible warm-tier backing store?

Measures exact spill/restore latency only. No model quality, no routing
quality, no llama.cpp integration. Sketch column is synthetic placeholder.

Pass/fail gates (local same-machine):
  - read-by-id p50 <= 2ms
  - read-by-id + decode p50 <= 5ms
  - sketch top-k + fetch p50 <= 8ms
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
import time

import numpy as np
import psycopg2


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="KV offload Experiment 0: storage/retrieval feasibility")
    ap.add_argument("--pg-dsn", default=os.environ.get(
        "KV_EXP0_PG_DSN",
        "postgres://cogniformerus:cogniformerus@127.0.0.1:30432/cogniformerus"))
    ap.add_argument("--blocks", type=int, default=1000)
    ap.add_argument("--payload-kb", type=int, default=32, help="Payload size in KB (32 or 128)")
    ap.add_argument("--sketch-dim", type=int, default=64, help="Sketch vector dimensionality")
    ap.add_argument("--sketch-k", type=int, default=5, help="Top-k for sketch search")
    ap.add_argument("--sessions", type=int, default=4, help="Number of sessions to mix")
    ap.add_argument("--layers", type=int, default=12)
    ap.add_argument("--warmup-reads", type=int, default=50)
    ap.add_argument("--read-samples", type=int, default=200)
    ap.add_argument("--drop-table", action="store_true", help="Drop and recreate table")
    ap.add_argument("--skip-write", action="store_true", help="Skip write phase (reuse existing data)")
    ap.add_argument("--skip-sketch", action="store_true", help="Skip sketch search phase")
    return ap.parse_args()


SCHEMA = """
CREATE TABLE IF NOT EXISTS kv_blocks_exp0 (
    session_id  int NOT NULL,
    layer_id    smallint NOT NULL,
    block_seq   int NOT NULL,
    token_start int NOT NULL,
    token_count smallint NOT NULL DEFAULT 256,
    sketch      vector({sketch_dim}),
    payload     bytea NOT NULL,
    PRIMARY KEY (session_id, layer_id, block_seq)
);
"""

SCHEMA_HEAP = """
CREATE TABLE IF NOT EXISTS kv_blocks_exp0_heap (
    session_id  int NOT NULL,
    layer_id    smallint NOT NULL,
    block_seq   int NOT NULL,
    token_start int NOT NULL,
    token_count smallint NOT NULL DEFAULT 256,
    sketch      vector({sketch_dim}),
    payload     bytea NOT NULL,
    PRIMARY KEY (session_id, layer_id, block_seq)
) USING sorted_heap;
"""


def p50(values: list[float]) -> float:
    return float(np.percentile(values, 50)) if values else 0.0


def p95(values: list[float]) -> float:
    return float(np.percentile(values, 95)) if values else 0.0


def generate_block(payload_bytes: int, sketch_dim: int, rng: np.random.Generator) -> tuple[bytes, np.ndarray]:
    payload = rng.integers(0, 256, size=payload_bytes, dtype=np.uint8).tobytes()
    sketch = rng.standard_normal(sketch_dim).astype(np.float32)
    sketch /= max(np.linalg.norm(sketch), 1e-8)
    return payload, sketch


def vector_literal(v: np.ndarray) -> str:
    return "[" + ",".join(f"{x:.6f}" for x in v) + "]"


def run_experiment(args: argparse.Namespace, table: str, use_heap: bool) -> dict:
    conn = psycopg2.connect(args.pg_dsn)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    payload_bytes = args.payload_kb * 1024
    rng = np.random.default_rng(42)
    results: dict[str, float | int | str] = {"table": table, "heap": use_heap}

    # Setup
    if args.drop_table:
        cur.execute(f"DROP TABLE IF EXISTS {table}")
    schema = SCHEMA_HEAP if use_heap else SCHEMA
    cur.execute(schema.format(sketch_dim=args.sketch_dim))

    # Write phase
    if not args.skip_write:
        write_times: list[float] = []
        block_id = 0
        for sid in range(args.sessions):
            blocks_per_session = args.blocks // args.sessions
            for seq in range(blocks_per_session):
                layer = seq % args.layers
                payload, sketch = generate_block(payload_bytes, args.sketch_dim, rng)
                t0 = time.perf_counter()
                cur.execute(
                    f"INSERT INTO {table} (session_id, layer_id, block_seq, token_start, token_count, sketch, payload) "
                    f"VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                    (sid, layer, seq, seq * 256, 256, vector_literal(sketch), psycopg2.Binary(payload))
                )
                write_times.append((time.perf_counter() - t0) * 1000.0)
                block_id += 1
        results["write_p50_ms"] = round(p50(write_times), 3)
        results["write_p95_ms"] = round(p95(write_times), 3)
        results["blocks_written"] = block_id
    else:
        cur.execute(f"SELECT count(*) FROM {table}")
        results["blocks_written"] = cur.fetchone()[0]

    # Table size
    cur.execute(f"SELECT pg_total_relation_size('{table}')")
    total_bytes = cur.fetchone()[0]
    results["table_size_mb"] = round(total_bytes / 1024 / 1024, 2)
    results["bytes_per_block"] = round(total_bytes / max(1, results["blocks_written"]))

    # Read-by-id phase (warmup then measure)
    all_keys = []
    cur.execute(f"SELECT session_id, layer_id, block_seq FROM {table} ORDER BY random() LIMIT {args.warmup_reads + args.read_samples}")
    for row in cur.fetchall():
        all_keys.append(row)

    # Warmup
    for sid, lid, bseq in all_keys[:args.warmup_reads]:
        cur.execute(f"SELECT payload FROM {table} WHERE session_id=%s AND layer_id=%s AND block_seq=%s", (sid, lid, bseq))
        cur.fetchone()

    # Measure read-by-id
    read_times: list[float] = []
    for sid, lid, bseq in all_keys[args.warmup_reads:]:
        t0 = time.perf_counter()
        cur.execute(f"SELECT payload FROM {table} WHERE session_id=%s AND layer_id=%s AND block_seq=%s", (sid, lid, bseq))
        row = cur.fetchone()
        read_times.append((time.perf_counter() - t0) * 1000.0)

    results["read_id_p50_ms"] = round(p50(read_times), 3)
    results["read_id_p95_ms"] = round(p95(read_times), 3)

    # Measure read-by-id + decode (memoryview → numpy)
    decode_times: list[float] = []
    for sid, lid, bseq in all_keys[args.warmup_reads:]:
        t0 = time.perf_counter()
        cur.execute(f"SELECT payload FROM {table} WHERE session_id=%s AND layer_id=%s AND block_seq=%s", (sid, lid, bseq))
        row = cur.fetchone()
        raw = bytes(row[0])
        arr = np.frombuffer(raw, dtype=np.uint8).astype(np.float16)  # SQ8→FP16 proxy
        decode_times.append((time.perf_counter() - t0) * 1000.0)

    results["read_decode_p50_ms"] = round(p50(decode_times), 3)
    results["read_decode_p95_ms"] = round(p95(decode_times), 3)

    # Sketch search phase
    if not args.skip_sketch:
        # Create index if needed
        idx_name = f"{table}_sketch_idx"
        cur.execute(f"""
            SELECT 1 FROM pg_indexes WHERE indexname = '{idx_name}'
        """)
        if not cur.fetchone():
            print(f"  building HNSW index on {table}.sketch ...", flush=True)
            t0 = time.perf_counter()
            cur.execute(f"CREATE INDEX {idx_name} ON {table} USING hnsw (sketch vector_cosine_ops)")
            idx_ms = (time.perf_counter() - t0) * 1000.0
            results["sketch_index_build_ms"] = round(idx_ms, 1)

        # Generate random query sketches
        query_sketches = [rng.standard_normal(args.sketch_dim).astype(np.float32) for _ in range(args.read_samples)]
        for qs in query_sketches:
            qs /= max(np.linalg.norm(qs), 1e-8)

        # Warmup
        for qs in query_sketches[:args.warmup_reads]:
            cur.execute(
                f"SELECT session_id, layer_id, block_seq, payload FROM {table} ORDER BY sketch <=> %s LIMIT %s",
                (vector_literal(qs), args.sketch_k)
            )
            cur.fetchall()

        # Measure sketch search + fetch
        sketch_times: list[float] = []
        for qs in query_sketches[args.warmup_reads:]:
            t0 = time.perf_counter()
            cur.execute(
                f"SELECT session_id, layer_id, block_seq, payload FROM {table} ORDER BY sketch <=> %s LIMIT %s",
                (vector_literal(qs), args.sketch_k)
            )
            rows = cur.fetchall()
            sketch_times.append((time.perf_counter() - t0) * 1000.0)

        results["sketch_topk_p50_ms"] = round(p50(sketch_times), 3)
        results["sketch_topk_p95_ms"] = round(p95(sketch_times), 3)

    cur.close()
    conn.close()
    return results


def verdict(results: dict) -> str:
    read_p50 = results.get("read_id_p50_ms", 999)
    decode_p50 = results.get("read_decode_p50_ms", 999)
    sketch_p50 = results.get("sketch_topk_p50_ms", 999)

    if read_p50 > 15 or decode_p50 > 15:
        return "HARD FAIL"
    if read_p50 > 5 or decode_p50 > 10:
        return "SOFT FAIL"
    if sketch_p50 > 15:
        return "SOFT FAIL (sketch)"
    if read_p50 <= 2 and decode_p50 <= 5 and sketch_p50 <= 8:
        return "PASS"
    return "MARGINAL"


def print_results(results: dict) -> None:
    v = verdict(results)
    print(f"\n{'='*60}")
    print(f"Table: {results['table']}  heap={results['heap']}  verdict={v}")
    print(f"{'='*60}")
    print(f"  blocks:           {results['blocks_written']}")
    print(f"  table size:       {results['table_size_mb']} MB")
    print(f"  bytes/block:      {results['bytes_per_block']}")
    if "write_p50_ms" in results:
        print(f"  write p50/p95:    {results['write_p50_ms']:.3f} / {results['write_p95_ms']:.3f} ms")
    print(f"  read-by-id p50/p95:    {results['read_id_p50_ms']:.3f} / {results['read_id_p95_ms']:.3f} ms")
    print(f"  read+decode p50/p95:   {results['read_decode_p50_ms']:.3f} / {results['read_decode_p95_ms']:.3f} ms")
    if "sketch_topk_p50_ms" in results:
        print(f"  sketch top-k p50/p95:  {results['sketch_topk_p50_ms']:.3f} / {results['sketch_topk_p95_ms']:.3f} ms")
    if "sketch_index_build_ms" in results:
        print(f"  sketch index build:    {results['sketch_index_build_ms']:.1f} ms")
    print(f"  VERDICT: {v}")


def main() -> int:
    args = parse_args()
    print(f"KV offload Experiment 0 | blocks={args.blocks} payload={args.payload_kb}KB "
          f"sketch_dim={args.sketch_dim} sessions={args.sessions}")

    # Run on plain heap first (baseline)
    args_plain = argparse.Namespace(**vars(args))
    args_plain.drop_table = True
    print("\n--- Plain heap (baseline) ---")
    plain = run_experiment(args_plain, "kv_blocks_exp0", use_heap=False)
    print_results(plain)

    # Run on sorted_heap
    args_heap = argparse.Namespace(**vars(args))
    args_heap.drop_table = True
    print("\n--- sorted_heap ---")
    heap = run_experiment(args_heap, "kv_blocks_exp0_heap", use_heap=True)
    print_results(heap)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
