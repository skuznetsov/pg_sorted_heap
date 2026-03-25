#!/usr/bin/env python3
"""
Reproduce the non-monotonic zvec retrieval bug on a synthetic FP32 corpus.

This complements scripts/repro_zvec_gutenberg_threshold.py. The Gutenberg
reproducer falsified the "just a GraphRAG corpus artifact" theory only
partially; this script checks whether the same engine path can return
empty/unmapped document ids on a plain synthetic collection too.

Current observed signature:
  - dim=32
  - ef_search=64
  - topk=7 already sufficient to trigger the bug
  - a compact failing case exists at 4,950 rows
  - failures are non-monotonic with row count
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path

import zvec

sys.path.insert(0, str(Path(__file__).resolve().parent))
import bench_zvec_synthetic as synth  # noqa: E402


def run_one(
    rows: int,
    dim: int,
    topk: int,
    ef_search: int,
    ef_construction: int,
    m: int,
    query_count: int,
    memory_limit_mb: int,
) -> tuple[int | None, list[str] | None]:
    tmpdir = Path(tempfile.mkdtemp(prefix="zvec_synth_repro_", dir="/tmp"))
    path = tmpdir / "bench"

    try:
        schema = zvec.CollectionSchema(
            name="bench",
            vectors=zvec.VectorSchema(
                name="embedding",
                data_type=zvec.DataType.VECTOR_FP32,
                dimension=dim,
                index_param=zvec.HnswIndexParam(
                    metric_type=zvec.MetricType.COSINE,
                    m=m,
                    ef_construction=ef_construction,
                ),
            ),
        )
        coll = zvec.create_and_open(str(path), schema)

        batch: list[zvec.Doc] = []
        idmap: set[str] = set()
        for i in range(1, rows + 1):
            batch.append(zvec.Doc(id=str(i), vectors={"embedding": synth.vec_for(i, dim)}))
            idmap.add(str(i))
            if len(batch) == 128:
                coll.insert(batch)
                batch = []
        if batch:
            coll.insert(batch)
        coll.flush()

        param = zvec.HnswQueryParam(ef=ef_search)
        for qi in range(1, query_count + 1):
            qvec = synth.qvec_for(qi, rows, dim)
            res = coll.query(zvec.VectorQuery("embedding", vector=qvec, param=param), topk=topk)
            ids = [doc.id for doc in res]
            bad = [doc_id for doc_id in ids if doc_id not in idmap]
            if bad:
                return qi, bad[:8]

        return None, None
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dim", type=int, default=32)
    ap.add_argument("--topk", type=int, default=7)
    ap.add_argument("--ef-search", type=int, default=64)
    ap.add_argument("--ef-construction", type=int, default=64)
    ap.add_argument("--m", type=int, default=16)
    ap.add_argument("--query-count", type=int, default=20)
    ap.add_argument("--memory-limit-mb", type=int, default=8192)
    ap.add_argument(
        "--rows",
        default="4900,4950,5000,7000,7500,7800,7900,24000,30000",
        help="Comma-separated synthetic row counts",
    )
    args = ap.parse_args()

    zvec.init(memory_limit_mb=args.memory_limit_mb)

    print("============================================================")
    print("zvec synthetic threshold repro")
    print("============================================================")
    print(f"dim:              {args.dim}")
    print(f"topk:             {args.topk}")
    print(f"ef_search:        {args.ef_search}")
    print(f"ef_construction:  {args.ef_construction}")
    print(f"m:                {args.m}")
    print(f"query_count:      {args.query_count}")
    print(f"memory_limit_mb:  {args.memory_limit_mb}")

    for item in args.rows.split(","):
        item = item.strip()
        if not item:
            continue
        rows = int(item)
        bad_query, sample = run_one(
            rows,
            args.dim,
            args.topk,
            args.ef_search,
            args.ef_construction,
            args.m,
            args.query_count,
            args.memory_limit_mb,
        )
        status = "ok" if bad_query is None else "bad"
        print(
            f"SYNTH_THRESH|rows={rows}|status={status}"
            f"|first_bad_query={bad_query}|sample={sample}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
