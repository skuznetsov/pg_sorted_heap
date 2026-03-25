#!/usr/bin/env python3
"""
Reproduce the zvec Gutenberg threshold bug seen in the real-text GraphRAG runs.

This script builds lexical-hash Gutenberg paragraph graphs of increasing size
using the same generator as scripts/bench_graph_rag_gutenberg.py, then checks
whether zvec starts returning empty/unmapped doc ids for a fixed top-k query.

The current known failure signature is:
  - dim=32
  - topk>=16
  - Gutenberg slice around 58k rows
  - first observed bad probe is query #10 with empty doc ids
"""

from __future__ import annotations

import argparse
import csv
import shutil
import sys
import tempfile
from pathlib import Path

import zvec

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bench_graph_rag_gutenberg as gut  # noqa: E402


def run_one(
    gutenberg_path: Path,
    max_books: int,
    max_paragraphs: int,
    skip_paragraphs: int,
    dim: int,
    ef_construction: int,
    ef_search: int,
    topk: int,
    query_count: int,
    memory_limit_mb: int,
) -> tuple[int, int | None, list[str] | None]:
    tmpdir = Path(tempfile.mkdtemp(prefix="zvec_guten_repro_", dir="/tmp"))
    csv_path = tmpdir / "facts.csv"
    base_dir: str | None = None

    try:
        rows, _, _, _ = gut.generate_csv_from_gutenberg(
            csv_path,
            gutenberg_path,
            max_books,
            max_paragraphs,
            skip_paragraphs,
            dim,
        )
        coll, idmap, base_dir = gut.build_zvec_collection(
            csv_path,
            dim,
            ef_construction,
            memory_limit_mb,
        )

        queries: list[list[float]] = []
        with open(csv_path, "r", encoding="utf-8", newline="") as f:
            for row in csv.reader(f):
                if int(row[1]) == 2:
                    queries.append(gut.vector_list_from_literal(row[3]))
                    if len(queries) == query_count:
                        break

        param = zvec.HnswQueryParam(ef=ef_search)
        for qi, qvec in enumerate(queries, start=1):
            res = coll.query(zvec.VectorQuery("embedding", vector=qvec, param=param), topk=topk)
            ids = [doc.id for doc in res]
            bad = [doc_id for doc_id in ids if doc_id not in idmap]
            if bad:
                return rows, qi, bad[:8]

        return rows, None, None
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
        if base_dir is not None:
            shutil.rmtree(base_dir, ignore_errors=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gutenberg-path", default=str(Path.home() / "Projects/ML/cogniversum_v2/gutenberg_cache"))
    ap.add_argument("--dim", type=int, default=32)
    ap.add_argument("--topk", type=int, default=16)
    ap.add_argument("--ef-search", type=int, default=64)
    ap.add_argument("--ef-construction", type=int, default=64)
    ap.add_argument("--query-count", type=int, default=16)
    ap.add_argument("--skip-paragraphs", type=int, default=8)
    ap.add_argument("--memory-limit-mb", type=int, default=8192)
    ap.add_argument(
        "--configs",
        default="64x256,80x256,96x256,112x256,128x256",
        help="Comma-separated configs as booksxparagraphs, e.g. 64x256,128x256",
    )
    args = ap.parse_args()

    gutenberg_path = Path(args.gutenberg_path).expanduser().resolve()
    if not gutenberg_path.is_dir():
        raise SystemExit(f"missing gutenberg path: {gutenberg_path}")

    print("============================================================")
    print("zvec Gutenberg threshold repro")
    print("============================================================")
    print(f"gutenberg_path:   {gutenberg_path}")
    print(f"dim:              {args.dim}")
    print(f"topk:             {args.topk}")
    print(f"ef_search:        {args.ef_search}")
    print(f"ef_construction:  {args.ef_construction}")
    print(f"query_count:      {args.query_count}")
    print(f"skip_paragraphs:  {args.skip_paragraphs}")
    print(f"memory_limit_mb:  {args.memory_limit_mb}")

    for item in args.configs.split(","):
        item = item.strip()
        if not item:
            continue
        books_s, paras_s = item.split("x", 1)
        max_books = int(books_s)
        max_paragraphs = int(paras_s)
        rows, bad_query, sample = run_one(
            gutenberg_path,
            max_books,
            max_paragraphs,
            args.skip_paragraphs,
            args.dim,
            args.ef_construction,
            args.ef_search,
            args.topk,
            args.query_count,
            args.memory_limit_mb,
        )
        status = "ok" if bad_query is None else "bad"
        print(
            f"THRESH|config={max_books}x{max_paragraphs}|rows={rows}"
            f"|status={status}|first_bad_query={bad_query}|sample={sample}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
