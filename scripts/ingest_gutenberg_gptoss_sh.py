#!/usr/bin/env python3
"""
Ingest Gutenberg text files into pg_sorted_heap vector tables.

Creates:
  gutenberg_gptoss        -- pgvector HNSW table (halfvec 2880)
  gutenberg_gptoss_sh     -- sorted_heap IVF-PQ table (svec 2880)
  gutenberg_gptoss_sh_sketch -- sketch sidecar (hsvec 384)
  bench_gptoss_queries    -- query table (qid, qvec)

Requires llama-server in embedding mode on EMBED_SERVER (default port 8090).

Usage:
  python3 scripts/ingest_gutenberg_gptoss_sh.py \\
    --corpus-dir ~/Projects/ML/cogniversum_v2/gutenberg_cache \\
    --dsn 'host=localhost port=15432 dbname=cogniformerus user=postgres password=postgres' \\
    --phase all

Phases:
  ingest  -- chunk, embed, insert into hnsw + plain sh tables
  train   -- train IVF+PQ, rebuild sh table with generated cols, compact, build sketch
  query   -- populate bench_gptoss_queries from random sample
  all     -- run all three phases in order
"""
from __future__ import annotations

import argparse
import hashlib
import sys
import time
from pathlib import Path
from typing import Iterator

from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
import psycopg2.extras
import requests


# ---------------------------------------------------------------------------
# Embedding
# ---------------------------------------------------------------------------

def _embed_single(text: str, server: str, max_words: int = 200) -> list[float] | None:
    """Embed one text item, truncating to max_words."""
    safe = " ".join(text.split()[:max_words])
    try:
        resp = requests.post(
            f"{server}/v1/embeddings",
            json={"input": safe, "model": "gpt-oss"},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()["data"][0]["embedding"]
    except Exception:
        return None


def get_embeddings_batch(texts: list[str], server: str,
                         workers: int = 4) -> list[list[float] | None]:
    """Embed texts using concurrent single requests (better GPU utilization)."""
    max_words = 120
    safe_texts = [" ".join(t.split()[:max_words]) for t in texts]
    results: list[list[float] | None] = [None] * len(texts)

    def _embed_idx(i_text):
        idx, text = i_text
        return idx, _embed_single(text, server, max_words=max_words)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for idx, emb in ex.map(_embed_idx, enumerate(safe_texts)):
            results[idx] = emb
    return results


def embedding_literal(values: list[float]) -> str:
    return "[" + ",".join(f"{v:.8g}" for v in values) + "]"


# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------

def clean_text(text: str) -> str:
    import re
    text = re.sub(r"[^\S\n]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def chunk_text(text: str, chunk_words: int, stride_words: int) -> Iterator[tuple[int, str]]:
    words = text.split()
    i = 0
    idx = 0
    while i < len(words):
        chunk = " ".join(words[i : i + chunk_words])
        yield idx, chunk
        idx += 1
        i += stride_words
        if i + chunk_words > len(words) and i < len(words):
            # last partial chunk already captured above if stride < chunk_words
            break


def batched(lst: list, n: int):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def table_exists(cur, table: str) -> bool:
    parts = table.split(".", 1)
    schema, tname = (parts[0], parts[1]) if len(parts) == 2 else ("public", parts[0])
    cur.execute(
        "SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = %s",
        (schema, tname),
    )
    return cur.fetchone() is not None


def column_exists(cur, table: str, col: str) -> bool:
    tname = table.split(".")[-1]
    cur.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_name = %s AND column_name = %s",
        (tname, col),
    )
    return cur.fetchone() is not None


def row_count(cur, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# Phase: ingest
# ---------------------------------------------------------------------------

def phase_ingest(args, conn) -> int:
    cur = conn.cursor()

    # HNSW table: pgvector halfvec(2880) with HNSW index
    print("[ingest] Creating hnsw table...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {args.hnsw_table} (
            id           text PRIMARY KEY,
            embedding    halfvec(2880),
            chunk_text   text,
            source_file  text
        )
    """)
    cur.execute(f"""
        CREATE INDEX IF NOT EXISTS gutenberg_gptoss_emb_idx
        ON {args.hnsw_table}
        USING hnsw (embedding halfvec_cosine_ops)
        WITH (m = 16, ef_construction = 100)
    """)
    conn.commit()

    # Plain SH table (no generated cols yet — codebook not trained)
    print("[ingest] Creating plain sorted_heap table (pre-train)...")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {args.sh_table} (
            id           text PRIMARY KEY,
            embedding    svec(2880),
            chunk_text   text,
            source_file  text
        )
    """)
    conn.commit()

    corpus_dir = Path(args.corpus_dir).expanduser()
    files = sorted(corpus_dir.glob("*.txt"))
    if not files:
        print(f"ERROR: no .txt files in {corpus_dir}", file=sys.stderr)
        return 1
    print(f"[ingest] {len(files)} text files")

    # Build chunk list
    all_chunks: list[tuple[str, str, str]] = []  # (id, text, source)
    for fpath in files:
        source = fpath.name
        try:
            raw = fpath.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            print(f"  skip {source}: {e}")
            continue
        text = clean_text(raw)
        for chunk_idx, chunk in chunk_text(text, args.chunk_size, args.chunk_stride):
            h = hashlib.md5(f"{source}:{chunk_idx}".encode()).hexdigest()[:12]
            chunk_id = f"{fpath.stem}:{chunk_idx}:{h}"
            all_chunks.append((chunk_id, chunk, source))

    print(f"[ingest] Total chunks: {len(all_chunks)}")

    # Find already-inserted IDs
    cur.execute(f"SELECT id FROM {args.hnsw_table}")
    existing_ids = {r[0] for r in cur.fetchall()}
    pending = [(cid, ctxt, src) for cid, ctxt, src in all_chunks if cid not in existing_ids]
    print(f"[ingest] Pending: {len(pending)}  (already done: {len(existing_ids)})")

    if not pending:
        print("[ingest] Nothing to do.")
        return 0

    t0 = time.time()
    inserted = 0

    for batch in batched(pending, args.batch_size):
        ids    = [b[0] for b in batch]
        texts  = [b[1] for b in batch]
        sources = [b[2] for b in batch]

        try:
            embeddings = get_embeddings_batch(texts, args.embed_server,
                                              workers=args.workers)
        except Exception as e:
            print(f"  embed error: {e} — skipping batch")
            continue

        valid = [(ids[i], texts[i], sources[i], embeddings[i])
                 for i in range(len(batch)) if embeddings[i] is not None]
        if not valid:
            continue

        for _attempt in range(5):
            try:
                psycopg2.extras.execute_values(
                    cur,
                    f"INSERT INTO {args.hnsw_table} (id, embedding, chunk_text, source_file) "
                    f"VALUES %s ON CONFLICT (id) DO NOTHING",
                    [(r[0], embedding_literal(r[3]), r[1], r[2]) for r in valid],
                    template="(%s, %s::halfvec, %s, %s)",
                )
                psycopg2.extras.execute_values(
                    cur,
                    f"INSERT INTO {args.sh_table} (id, embedding, chunk_text, source_file) "
                    f"VALUES %s ON CONFLICT (id) DO NOTHING",
                    [(r[0], embedding_literal(r[3]), r[1], r[2]) for r in valid],
                    template="(%s, %s::svec, %s, %s)",
                )
                conn.commit()
                break
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                print(f"  DB error ({e}), reconnecting...")
                time.sleep(5)
                try:
                    conn = psycopg2.connect(args.dsn)
                    conn.autocommit = False
                    cur = conn.cursor()
                except Exception:
                    time.sleep(10)

        inserted += len(valid)
        elapsed = time.time() - t0
        rate = inserted / elapsed if elapsed > 0 else 0
        remaining = len(pending) - inserted
        eta = f"{remaining/rate:.0f}s" if rate > 0 else "?"
        print(f"  {inserted}/{len(pending)}  {rate:.1f} rows/s  eta {eta}")

    print(f"[ingest] Done. {inserted} rows in {time.time()-t0:.1f}s")
    return 0


# ---------------------------------------------------------------------------
# Phase: train
# ---------------------------------------------------------------------------

def phase_train(args, conn) -> int:
    cur = conn.cursor()

    n = row_count(cur, args.sh_table)
    print(f"[train] {n} rows in {args.sh_table}")

    # IVF codebook
    if not table_exists(cur, "_ivf_meta"):
        print("[train] Training IVF (nlist=256)...")
        t0 = time.time()
        cur.execute(f"""
            SELECT svec_ivf_train(
                'SELECT embedding FROM {args.sh_table}', 256)
        """)
        ivf_id = cur.fetchone()[0]
        conn.commit()
        print(f"  ivf_cb_id={ivf_id}  ({time.time()-t0:.1f}s)")
    else:
        print("[train] IVF codebook already exists, skipping.")

    # PQ codebook
    if not table_exists(cur, "_pq_codebook_meta"):
        print("[train] Training residual PQ (M=720)...")
        t0 = time.time()
        cur.execute(f"""
            SELECT svec_pq_train_residual(
                'SELECT embedding FROM {args.sh_table}', 720, 1)
        """)
        pq_id = cur.fetchone()[0]
        conn.commit()
        print(f"  pq_cb_id={pq_id}  ({time.time()-t0:.1f}s)")
    else:
        print("[train] PQ codebook already exists, skipping.")

    # Rebuild sh_table with generated columns if needed
    if not column_exists(cur, args.sh_table, "partition_id"):
        sh_old = args.sh_table.split(".")[-1] + "_plain"
        print(f"[train] Rebuilding {args.sh_table} with generated cols...")
        t0 = time.time()
        cur.execute(f"ALTER TABLE {args.sh_table} RENAME TO {sh_old}")
        cur.execute(f"""
            CREATE TABLE {args.sh_table} (
                id           text,
                partition_id int2 GENERATED ALWAYS AS (
                                 svec_ivf_assign(embedding, 1)) STORED,
                embedding    svec(2880),
                pq_code      bytea GENERATED ALWAYS AS (
                                 svec_pq_encode_residual(
                                     embedding,
                                     svec_ivf_assign(embedding, 1),
                                     1, 1)) STORED,
                chunk_text   text,
                source_file  text,
                PRIMARY KEY (partition_id, id)
            ) USING sorted_heap
        """)
        cur.execute(f"""
            INSERT INTO {args.sh_table} (id, embedding, chunk_text, source_file)
            SELECT id, embedding, chunk_text, source_file FROM {sh_old}
        """)
        n_copied = cur.rowcount
        conn.commit()
        print(f"  copied {n_copied} rows ({time.time()-t0:.1f}s)")
        cur.execute(f"DROP TABLE {sh_old}")
        conn.commit()
    else:
        print(f"[train] {args.sh_table} already has partition_id, skipping rebuild.")

    # Compact
    print("[train] Compacting...")
    t0 = time.time()
    cur.execute(f"SELECT sorted_heap_compact('{args.sh_table}')")
    conn.commit()
    print(f"  done ({time.time()-t0:.1f}s)")

    # Sketch sidecar
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {args.sketch_table} (
            partition_id int2,
            id           text,
            sketch       hsvec(384),
            PRIMARY KEY (partition_id, id)
        )
    """)
    conn.commit()

    cur.execute(f"SELECT COUNT(*) FROM {args.sketch_table}")
    sk = cur.fetchone()[0]
    if sk == 0:
        print("[train] Building sketch table (first 384 dims)...")
        t0 = time.time()
        cur.execute(f"""
            INSERT INTO {args.sketch_table} (partition_id, id, sketch)
            SELECT
                partition_id, id,
                (
                    '[' || array_to_string(
                        (string_to_array(
                            trim(both '[]' from embedding::text), ','))[1:384],
                        ','
                    ) || ']'
                )::hsvec
            FROM {args.sh_table}
        """)
        n_sk = cur.rowcount
        conn.commit()
        print(f"  {n_sk} sketch rows ({time.time()-t0:.1f}s)")
    else:
        print(f"[train] Sketch table already has {sk} rows, skipping.")

    return 0


# ---------------------------------------------------------------------------
# Phase: query
# ---------------------------------------------------------------------------

def phase_query(args, conn) -> int:
    cur = conn.cursor()

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {args.query_table} (
            qid  text PRIMARY KEY,
            qvec svec(2880)
        )
    """)
    conn.commit()

    cur.execute(f"SELECT COUNT(*) FROM {args.query_table}")
    q_count = cur.fetchone()[0]
    if q_count >= args.n_queries:
        print(f"[query] Already have {q_count} queries, skipping.")
        return 0

    print(f"[query] Sampling {args.n_queries} queries from {args.sh_table}...")
    cur.execute(f"""
        SELECT id, embedding::text
        FROM {args.sh_table}
        ORDER BY random()
        LIMIT %s
    """, (args.n_queries,))
    rows = cur.fetchall()

    psycopg2.extras.execute_values(
        cur,
        f"INSERT INTO {args.query_table} (qid, qvec) VALUES %s ON CONFLICT DO NOTHING",
        rows,
        template="(%s, %s::svec)",
    )
    conn.commit()
    print(f"[query] {len(rows)} queries inserted.")
    return 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--corpus-dir",
                        default="~/Projects/ML/cogniversum_v2/gutenberg_cache")
    parser.add_argument("--dsn",
                        default="host=localhost port=15432 dbname=cogniformerus "
                                "user=postgres password=postgres")
    parser.add_argument("--embed-server", default="http://127.0.0.1:8090")
    parser.add_argument("--hnsw-table",   default="public.gutenberg_gptoss")
    parser.add_argument("--sh-table",     default="public.gutenberg_gptoss_sh")
    parser.add_argument("--sketch-table", default="public.gutenberg_gptoss_sh_sketch")
    parser.add_argument("--query-table",  default="public.bench_gptoss_queries")
    parser.add_argument("--chunk-size",   type=int, default=400)
    parser.add_argument("--chunk-stride", type=int, default=200)
    parser.add_argument("--n-queries",    type=int, default=200)
    parser.add_argument("--batch-size",   type=int, default=8)
    parser.add_argument("--workers",      type=int, default=4,
                        help="Concurrent embedding requests to llama-server")
    parser.add_argument("--phase",
                        choices=["ingest", "train", "query", "all"],
                        default="all")
    args = parser.parse_args()

    conn = psycopg2.connect(args.dsn)
    conn.autocommit = False

    rc = 0
    if args.phase in ("ingest", "all"):
        rc = rc or phase_ingest(args, conn)
    if args.phase in ("train", "all"):
        rc = rc or phase_train(args, conn)
    if args.phase in ("query", "all"):
        rc = rc or phase_query(args, conn)

    conn.close()
    return rc


if __name__ == "__main__":
    sys.exit(main())
