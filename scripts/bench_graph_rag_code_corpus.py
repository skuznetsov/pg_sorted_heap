#!/usr/bin/env python3
"""
Benchmark a real code-corpus GraphRAG shape on cogniformerus source files.

Dataset:
  - each file is an entity
  - each chunk in that file is a fact row:
      entity_id=file_id, relation_id=HAS_CHUNK, target_id=chunk_id
  - embedding/payload are derived from the real source chunk text

Queries:
  - actual CrossFile questions parsed from cogniformerus/bin/butler_code_test.cr

The benchmark asks a narrow question:

  Does file-seeded GraphRAG expansion help real code-question retrieval
  compared to direct ANN over raw chunks?

Quality metric:
  - keyword coverage percentage over the retrieved payload union
  - full-hit percentage: all expected keywords covered
"""

from __future__ import annotations

import argparse
import csv
import re
import shlex
import statistics
from dataclasses import dataclass
from pathlib import Path

import bench_graph_rag as base
import bench_graph_rag_multihop as mh

REL_HAS_CHUNK = 1
CHUNK_WINDOW = 800
CHUNK_OVERLAP = 200

QUESTION_START_RE = re.compile(r"Question\.new\(")
QUESTION_Q_RE = re.compile(r'q:\s*"(.*)",\s*$')
QUESTION_TOPIC_RE = re.compile(r'topic:\s*"(.*)",\s*$')
QUESTION_TIER_RE = re.compile(r"tier:\s*Tier::([A-Za-z_]+),\s*$")
QUOTED_RE = re.compile(r'"([^"]+)"')


@dataclass(frozen=True)
class CodeQuestion:
    label: str
    prompt: str
    keywords: tuple[str, ...]
    query_vec: str


@dataclass(frozen=True)
class CodeChunk:
    file_id: int
    chunk_id: int
    global_id: int
    file_path: str
    text: str


def default_cogniformerus_root(repo_root: Path) -> Path:
    return repo_root.parent.parent / "Crystal" / "cogniformerus"


def normalize_prompt(raw: str) -> str:
    return raw.replace("#{name}", "Jarvis")


def parse_crossfile_questions(question_path: Path, dim: int) -> list[CodeQuestion]:
    lines = question_path.read_text(encoding="utf-8").splitlines()
    questions: list[CodeQuestion] = []
    in_block = False
    raw_q: str | None = None
    keywords: list[str] = []
    topic: str | None = None
    tier: str | None = None

    for line in lines:
        stripped = line.strip()
        if not in_block and QUESTION_START_RE.search(stripped):
            in_block = True
            raw_q = None
            keywords = []
            topic = None
            tier = None
            continue

        if not in_block:
            continue

        q_match = QUESTION_Q_RE.search(stripped)
        if q_match:
            raw_q = q_match.group(1)
            continue

        if stripped.startswith("keywords:"):
            keywords = QUOTED_RE.findall(stripped)
            continue

        topic_match = QUESTION_TOPIC_RE.search(stripped)
        if topic_match:
            topic = topic_match.group(1)
            continue

        tier_match = QUESTION_TIER_RE.search(stripped)
        if tier_match:
            tier = tier_match.group(1)
            continue

        if stripped == "),":
            in_block = False
            if tier == "CrossFile" and raw_q and topic and keywords:
                prompt = normalize_prompt(raw_q)
                questions.append(
                    CodeQuestion(
                        label=topic,
                        prompt=prompt,
                        keywords=tuple(keywords),
                        query_vec=mh.lexical_hash_vector(prompt, dim),
                    )
                )

    if not questions:
        raise RuntimeError(f"no CrossFile questions parsed from {question_path}")
    return questions


def chunk_source_file(path: Path, src_dir: Path, file_id: int, window: int, overlap: int, start_chunk_id: int) -> tuple[list[CodeChunk], int]:
    content = path.read_text(encoding="utf-8")
    relative = str(path.relative_to(src_dir))

    if len(content) <= window:
        enriched = f"# File: {relative}\n{content}"
        return [CodeChunk(file_id=file_id, chunk_id=0, global_id=start_chunk_id, file_path=relative, text=enriched)], start_chunk_id + 1

    chunks: list[CodeChunk] = []
    stride = max(1, window - overlap)
    pos = 0
    local_chunk_id = 0
    global_chunk_id = start_chunk_id

    while pos < len(content):
        chunk_end = min(pos + window, len(content))
        chunk_text = content[pos:chunk_end]
        enriched = f"# File: {relative}\n{chunk_text}"
        chunks.append(
            CodeChunk(
                file_id=file_id,
                chunk_id=local_chunk_id,
                global_id=global_chunk_id,
                file_path=relative,
                text=enriched,
            )
        )
        local_chunk_id += 1
        global_chunk_id += 1
        if chunk_end >= len(content):
            break
        pos += stride

    return chunks, global_chunk_id


def build_code_csv(src_dir: Path, csv_path: Path, dim: int, window: int, overlap: int) -> tuple[int, int]:
    files = sorted(src_dir.rglob("*.cr"))
    if not files:
        raise RuntimeError(f"no Crystal source files found under {src_dir}")

    next_chunk_id = 1
    rowcount = 0
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for file_id, path in enumerate(files, start=1):
            chunks, next_chunk_id = chunk_source_file(path, src_dir, file_id, window, overlap, next_chunk_id)
            for chunk in chunks:
                w.writerow(
                    [
                        chunk.file_id,
                        REL_HAS_CHUNK,
                        chunk.global_id,
                        mh.lexical_hash_vector(chunk.text, dim),
                        chunk.text,
                    ]
                )
                rowcount += 1

    return len(files), rowcount


def verify_helper_equivalence(cur, table_name: str, questions: list[CodeQuestion], ann_k: int, top_k: int) -> None:
    sql = f"""
    WITH ann AS MATERIALIZED (
        SELECT entity_id
        FROM {table_name}
        ORDER BY embedding <=> %s::svec
        LIMIT {ann_k}
    ),
    seeds AS MATERIALIZED (
        SELECT DISTINCT entity_id FROM ann
    ),
    helper AS (
        SELECT entity_id, relation_id, target_id, round(distance::numeric, 6) AS distance
        FROM sorted_heap_expand_rerank(
            '{table_name}'::regclass,
            ARRAY(SELECT entity_id FROM seeds),
            %s::svec,
            {top_k},
            {REL_HAS_CHUNK},
            0
        )
    ),
    expanded AS MATERIALIZED (
        SELECT *
        FROM {table_name}
        WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
          AND relation_id = {REL_HAS_CHUNK}
    ),
    sql_baseline AS (
        SELECT entity_id, relation_id, target_id, round((embedding <=> %s::svec)::numeric, 6) AS distance
        FROM expanded
        ORDER BY embedding <=> %s::svec, entity_id, relation_id, target_id
        LIMIT {top_k}
    )
    SELECT count(*) FROM (
        (SELECT * FROM helper EXCEPT ALL SELECT * FROM sql_baseline)
        UNION ALL
        (SELECT * FROM sql_baseline EXCEPT ALL SELECT * FROM helper)
    ) diff
    """

    for idx, question in enumerate(questions, start=1):
        cur.execute(sql, (question.query_vec, question.query_vec, question.query_vec, question.query_vec))
        diff_rows = cur.fetchone()[0]
        if diff_rows != 0:
            raise RuntimeError(
                f"sorted_heap_expand_rerank mismatch on {table_name} question#{idx}: diff_rows={diff_rows}"
            )


def keyword_coverage(rows: list[tuple], keywords: tuple[str, ...]) -> tuple[float, bool]:
    if not keywords:
        return 100.0, True
    haystack = "\n".join(str(row[4]).lower() for row in rows)
    matched = 0
    for kw in keywords:
        if kw.lower() in haystack:
            matched += 1
    pct = (matched * 100.0) / len(keywords)
    return pct, matched == len(keywords)


def payload_index_from_description(description) -> int:
    for idx, col in enumerate(description):
        name = getattr(col, "name", None) or col[0]
        if name == "payload":
            return idx
    raise RuntimeError("query result does not contain a payload column")


def measure_case(cur, table_name: str, case: base.QueryCase, questions: list[CodeQuestion], runs: int) -> tuple[float, float, float, float, str, int]:
    sql = case.sql_template.format(table=table_name)
    total_ms: list[float] = []
    hits: list[int] = []
    reads: list[int] = []
    root = ""
    rowcount = 0

    for run_idx in range(runs):
        for q_idx, question in enumerate(questions):
            params = case.params_builder(question)
            exec_ms, hit, read, root = base.explain_json(cur, sql, params)
            total_ms.append(exec_ms)
            hits.append(hit)
            reads.append(read)
            if run_idx == 0 and q_idx == 0:
                cur.execute(sql, params)
                rowcount = len(cur.fetchall())

    return (
        statistics.median(total_ms),
        statistics.fmean(total_ms),
        statistics.fmean(hits),
        statistics.fmean(reads),
        root or "Limit",
        rowcount,
    )


def measure_quality(cur, table_name: str, case: base.QueryCase, questions: list[CodeQuestion]) -> tuple[float, float, float]:
    sql = case.sql_template.format(table=table_name)
    full_hits = 0
    total_rows = 0
    coverage_scores: list[float] = []

    for question in questions:
        cur.execute(sql, case.params_builder(question))
        payload_idx = payload_index_from_description(cur.description)
        rows = cur.fetchall()
        total_rows += len(rows)
        payload_rows = [str(row[payload_idx]) for row in rows]
        coverage_pct, full_hit = keyword_coverage([(None, None, None, None, payload) for payload in payload_rows], question.keywords)
        coverage_scores.append(coverage_pct)
        if full_hit:
            full_hits += 1

    n = len(questions)
    return (
        statistics.fmean(coverage_scores) if coverage_scores else 0.0,
        (full_hits * 100.0) / n if n else 0.0,
        total_rows / n if n else 0.0,
    )


def print_result(table: str, case: str, p50: float, avg: float, hits: float, reads: float, root: str, rows: int, keyword_pct: float, full_pct: float, avg_rows: float) -> None:
    print(
        f"{table}|{case}|p50_ms={p50:.3f}|avg_ms={avg:.3f}|shared_hit={hits:.1f}|shared_read={reads:.1f}|"
        f"root={root}|rows={rows}|keyword_pct={keyword_pct:.1f}|full_pct={full_pct:.1f}|avg_rows={avg_rows:.2f}"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cogniformerus-root", default="")
    ap.add_argument("--source-dir", default="")
    ap.add_argument("--question-source", default="")
    ap.add_argument("--tmp-root", default="/tmp")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--dim", type=int, default=384)
    ap.add_argument("--ann-k", type=int, default=16)
    ap.add_argument("--top-k", type=int, default=8)
    ap.add_argument("--ef-search", type=int, default=64)
    ap.add_argument("--ef-construction", type=int, default=200)
    ap.add_argument("--m", type=int, default=24)
    ap.add_argument("--shared-buffers-mb", type=int, default=64)
    ap.add_argument("--backend-mode", choices=("fresh", "reuse"), default="fresh")
    ap.add_argument("--chunk-window", type=int, default=CHUNK_WINDOW)
    ap.add_argument("--chunk-overlap", type=int, default=CHUNK_OVERLAP)
    ap.add_argument("--install-cmd", default="")
    ap.add_argument("--keep-temp", action="store_true")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    cogniformerus_root = Path(args.cogniformerus_root).resolve() if args.cogniformerus_root else default_cogniformerus_root(repo_root).resolve()
    source_dir = Path(args.source_dir).resolve() if args.source_dir else (cogniformerus_root / "src" / "cogniformerus").resolve()
    question_source = Path(args.question_source).resolve() if args.question_source else (cogniformerus_root / "bin" / "butler_code_test.cr").resolve()

    if not source_dir.exists():
        raise RuntimeError(f"source dir not found: {source_dir}")
    if not question_source.exists():
        raise RuntimeError(f"question source not found: {question_source}")

    tmp_root = Path(args.tmp_root).resolve()
    port = args.port or base.pick_port()
    install_cmd = shlex.split(args.install_cmd) if args.install_cmd else None
    tmp, pg_bindir = base.init_temp_cluster(repo_root, port, tmp_root, args.shared_buffers_mb, install_cmd)
    csv_path = tmp / "facts_code_corpus.csv"

    try:
        questions = parse_crossfile_questions(question_source, args.dim)
        file_count, rowcount = build_code_csv(source_dir, csv_path, args.dim, args.chunk_window, args.chunk_overlap)

        conn = base.connect(tmp, port)
        cur = conn.cursor()
        try:
            cur.execute("SET jit = off")
            cur.execute("SET sorted_hnsw.shared_cache = off")
            cur.execute(f"SET sorted_hnsw.ef_search = {args.ef_search}")
            base.bootstrap_schema(cur, args.dim)
            base.load_data(cur, csv_path)
            base.build_indexes(cur, args.ef_construction, m=args.m)

            if args.backend_mode == "fresh":
                cur.close()
                conn.close()
                conn = base.connect(tmp, port)
                cur = conn.cursor()

            cur.execute("SET jit = off")
            cur.execute("SET sorted_hnsw.shared_cache = off")
            cur.execute(f"SET sorted_hnsw.ef_search = {args.ef_search}")

            verify_helper_equivalence(cur, "facts_sh", questions, args.ann_k, args.top_k)

            direct_ann = base.QueryCase(
                "direct_ann",
                f"""
                SELECT *
                FROM {{table}}
                WHERE relation_id = {REL_HAS_CHUNK}
                ORDER BY embedding <=> %s::svec, entity_id, relation_id, target_id
                LIMIT {args.top_k}
                """,
                lambda q: (q.query_vec,),
            )

            seed_expand_sql = base.QueryCase(
                "seed_file_expand_in",
                f"""
                WITH ann AS MATERIALIZED (
                    SELECT entity_id
                    FROM {{table}}
                    ORDER BY embedding <=> %s::svec
                    LIMIT {args.ann_k}
                ),
                seeds AS MATERIALIZED (
                    SELECT DISTINCT entity_id FROM ann
                ),
                expanded AS MATERIALIZED (
                    SELECT *
                    FROM {{table}}
                    WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
                      AND relation_id = {REL_HAS_CHUNK}
                )
                SELECT *
                FROM expanded
                ORDER BY embedding <=> %s::svec, entity_id, relation_id, target_id
                LIMIT {args.top_k}
                """,
                lambda q: (q.query_vec, q.query_vec),
            )

            seed_expand_fn = base.QueryCase(
                "seed_file_expand_fn",
                f"""
                WITH ann AS MATERIALIZED (
                    SELECT entity_id
                    FROM {{table}}
                    ORDER BY embedding <=> %s::svec
                    LIMIT {args.ann_k}
                ),
                seeds AS MATERIALIZED (
                    SELECT DISTINCT entity_id FROM ann
                )
                SELECT *
                FROM sorted_heap_expand_rerank(
                    '{{table}}'::regclass,
                    ARRAY(SELECT entity_id FROM seeds),
                    %s::svec,
                    {args.top_k},
                    {REL_HAS_CHUNK},
                    0
                )
                """,
                lambda q: (q.query_vec, q.query_vec),
            )

            print("============================================================")
            print("graph rag code corpus")
            print("============================================================")
            print(f"cogniformerus_root: {cogniformerus_root}")
            print(f"source_dir:         {source_dir}")
            print(f"question_source:    {question_source}")
            print(f"port:               {port}")
            print(f"files:              {file_count}")
            print(f"rows:               {rowcount}")
            print(f"queries:            {len(questions)}")
            print(f"runs:               {args.runs}")
            print(f"dim:                {args.dim}")
            print(f"ann_k:              {args.ann_k}")
            print(f"top_k:              {args.top_k}")
            print(f"ef_search:          {args.ef_search}")
            print(f"ef_construction:    {args.ef_construction}")
            print(f"m:                  {args.m}")
            print(f"shared_buffers:     {args.shared_buffers_mb}MB")
            print(f"backend_mode:       {args.backend_mode}")
            print()

            for question in questions:
                print(f"query|label={question.label}|keywords={','.join(question.keywords)}|prompt={question.prompt}")
            print()

            cases: list[tuple[str, str, base.QueryCase]] = [
                ("facts_heap", "facts_heap", direct_ann),
                ("facts_sh", "facts_sh", direct_ann),
                ("facts_heap", "facts_heap", seed_expand_sql),
                ("facts_sh", "facts_sh", seed_expand_sql),
                ("facts_sh", "facts_sh", seed_expand_fn),
            ]

            for label, table, case in cases:
                print(f"running|table={label}|case={case.name}", flush=True)
                p50, avg, hits, reads, root, rows = measure_case(cur, table, case, questions, args.runs)
                keyword_pct, full_pct, avg_rows = measure_quality(cur, table, case, questions)
                print_result(label, case.name, p50, avg, hits, reads, root, rows, keyword_pct, full_pct, avg_rows)
        finally:
            cur.close()
            conn.close()
    finally:
        if args.keep_temp:
            print(f"kept_temp={tmp}")
        else:
            base.stop_temp_cluster(tmp, pg_bindir)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
