#!/usr/bin/env python3
"""
Benchmark a real code-corpus GraphRAG shape on cogniformerus source files.

Dataset:
  - each file is an entity
  - each chunk in that file is a fact row:
      entity_id=file_id, relation_id=HAS_CHUNK, target_id=chunk_id
  - local require edges become graph rows:
      entity_id=file_id, relation_id=REQUIRES_FILE, target_id=required_file_id
  - embedding/payload are derived from the real source chunk text

Queries:
  - actual CrossFile questions parsed from cogniformerus/bin/butler_code_test.cr

The benchmark asks a narrow question:

  Do real file-level relations from the code graph help real code-question
  retrieval, or is plain file-seeded expansion already enough?

Quality metric:
  - keyword coverage percentage over the retrieved payload union
  - full-hit percentage: all expected keywords covered
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import math
import re
import shlex
import statistics
from dataclasses import dataclass
from pathlib import Path

import bench_graph_rag as base
import bench_graph_rag_multihop as mh

REL_HAS_CHUNK = 1
REL_REQUIRES_FILE = 2
REL_FILE_SUMMARY = 3
CHUNK_WINDOW = 800
CHUNK_OVERLAP = 200

QUESTION_START_RE = re.compile(r"Question\.new\(")
QUESTION_Q_RE = re.compile(r'q:\s*"(.*)",\s*$')
QUESTION_TOPIC_RE = re.compile(r'topic:\s*"(.*)",\s*$')
QUESTION_TIER_RE = re.compile(r"tier:\s*Tier::([A-Za-z_]+),\s*$")
QUOTED_RE = re.compile(r'"([^"]+)"')
REQUIRE_RE = re.compile(r'^require\s+"([^"]+)"')
PROMPT_TERM_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*|\d+")
CODE_TOKEN_RE = re.compile(r"[A-Za-z0-9_']+")
CAMEL_SPLIT_RE = re.compile(r"[A-Z]+(?=[A-Z][a-z]|\b)|[A-Z]?[a-z]+|\d+")

PROMPT_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "between",
    "by",
    "does",
    "for",
    "from",
    "handle",
    "happens",
    "how",
    "in",
    "is",
    "jarvis",
    "of",
    "or",
    "the",
    "to",
    "what",
    "when",
    "which",
    "with",
}


@dataclass(frozen=True)
class CodeQuestion:
    label: str
    prompt: str
    keywords: tuple[str, ...]
    query_vec: str
    oracle_file_ids: tuple[int, ...] = ()


@dataclass(frozen=True)
class CodeChunk:
    file_id: int
    chunk_id: int
    global_id: int
    file_path: str
    text: str


@dataclass(frozen=True)
class QuestionQuality:
    label: str
    keyword_pct: float
    full_hit: bool
    rows: int


@dataclass(frozen=True)
class QuestionPayloadRow:
    label: str
    row_idx: int
    payload: str


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


def file_summary_text(relative: str, chunks: list[CodeChunk], token_budget: int = 256) -> str:
    parts = [f"# File Summary: {relative}"]
    token_count = 0

    for chunk in chunks:
        snippet = chunk.text.replace("\n", " ")
        words = snippet.split()
        if not words:
            continue
        take = min(len(words), max(0, token_budget - token_count))
        if take <= 0:
            break
        parts.append(" ".join(words[:take]))
        token_count += take
        if token_count >= token_budget:
            break

    return "\n".join(parts)


def list_source_files(src_dir: Path) -> list[Path]:
    files = sorted(src_dir.rglob("*.cr"))
    if not files:
        raise RuntimeError(f"no Crystal source files found under {src_dir}")
    return files


def build_oracle_seed_map(files: list[Path], questions: list[CodeQuestion], ann_k: int) -> dict[str, tuple[int, ...]]:
    file_texts = [(idx, path.read_text(encoding="utf-8").lower()) for idx, path in enumerate(files, start=1)]
    seed_map: dict[str, tuple[int, ...]] = {}

    for question in questions:
        scored: list[tuple[int, int]] = []
        for file_id, text in file_texts:
            score = sum(1 for kw in question.keywords if kw.lower() in text)
            if score > 0:
                scored.append((score, file_id))

        scored.sort(key=lambda item: (-item[0], item[1]))
        seeds = tuple(file_id for _, file_id in scored[:ann_k])
        if not seeds and file_texts:
            seeds = (file_texts[0][0],)
        seed_map[question.label] = seeds

    return seed_map


def split_code_token(token: str) -> list[str]:
    token = token.strip("_")
    if not token:
        return []

    parts: list[str] = []
    underscore_parts = [part for part in re.split(r"_+", token) if part]
    for part in underscore_parts:
        camel_parts = CAMEL_SPLIT_RE.findall(part)
        if camel_parts:
            parts.extend(camel_parts)
        else:
            parts.append(part)
    return [part.lower() for part in parts if part]


def code_aware_tokens(text: str) -> list[str]:
    out: list[str] = []
    for raw in CODE_TOKEN_RE.findall(text):
        lowered = raw.lower()
        out.append(lowered)
        pieces = split_code_token(raw)
        for piece in pieces:
            if piece != lowered:
                out.append(piece)
    return out


def code_aware_lexical_hash_vector(text: str, dim: int) -> str:
    acc = [0.0] * dim
    tokens = code_aware_tokens(text)
    if not tokens:
        tokens = [text.lower() or "_"]

    for token in tokens[:512]:
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        block = digest
        for i in range(dim):
            if i > 0 and i % 256 == 0:
                block = hashlib.sha256(block).digest()
            bit = (block[(i // 8) % len(block)] >> (i % 8)) & 1
            acc[i] += 1.0 if bit else -1.0

    norm = math.sqrt(sum(v * v for v in acc))
    if norm < 1e-8:
        vals = [0.0] * dim
    else:
        vals = [v / norm for v in acc]
    return "[" + ",".join(f"{v:.6f}" for v in vals) + "]"


def vectorize_text(text: str, dim: int, mode: str) -> str:
    if mode == "generic":
        return mh.lexical_hash_vector(text, dim)
    if mode == "code_aware":
        return code_aware_lexical_hash_vector(text, dim)
    raise ValueError(f"unknown embedding mode: {mode}")


def extract_prompt_terms(prompt: str) -> tuple[str, ...]:
    terms: list[str] = []
    seen: set[str] = set()

    for token in PROMPT_TERM_RE.findall(prompt):
        lowered = token.lower()
        if lowered in PROMPT_STOPWORDS:
            continue
        if len(lowered) <= 2 and not lowered.isdigit():
            continue
        if lowered in seen:
            continue
        seen.add(lowered)
        terms.append(lowered)

    return tuple(terms)


def resolve_local_require_targets(path: Path, src_dir: Path, rel_to_id: dict[str, int]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    parent = path.parent

    for line in path.read_text(encoding="utf-8").splitlines():
        match = REQUIRE_RE.match(line.strip())
        if not match:
            continue

        target = match.group(1)
        if not target.startswith("."):
            continue

        if target.endswith("/*"):
            base = (parent / target[:-2]).resolve()
            if not base.exists() or not base.is_dir():
                continue
            candidates = sorted(base.glob("*.cr"))
        else:
            base = (parent / target).resolve()
            candidate = base if base.suffix == ".cr" else base.with_suffix(".cr")
            candidates = [candidate] if candidate.exists() else []

        for candidate in candidates:
            try:
                relative = str(candidate.relative_to(src_dir))
            except ValueError:
                continue
            if relative not in rel_to_id:
                continue
            if relative not in seen:
                seen.add(relative)
                out.append(relative)

    return out


def build_code_csv(src_dir: Path, csv_path: Path, dim: int, window: int, overlap: int, embedding_mode: str) -> tuple[int, int, int, int]:
    files = list_source_files(src_dir)
    rel_to_id = {str(path.relative_to(src_dir)): idx for idx, path in enumerate(files, start=1)}

    next_chunk_id = 1
    rowcount = 0
    edge_count = 0
    summary_count = 0
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for file_id, path in enumerate(files, start=1):
            relative = str(path.relative_to(src_dir))
            chunks, next_chunk_id = chunk_source_file(path, src_dir, file_id, window, overlap, next_chunk_id)
            summary_text = file_summary_text(relative, chunks)
            w.writerow(
                [
                    file_id,
                    REL_FILE_SUMMARY,
                    file_id,
                    vectorize_text(summary_text, dim, embedding_mode),
                    summary_text,
                ]
            )
            rowcount += 1
            summary_count += 1
            for chunk in chunks:
                w.writerow(
                    [
                        chunk.file_id,
                        REL_HAS_CHUNK,
                        chunk.global_id,
                        vectorize_text(chunk.text, dim, embedding_mode),
                        chunk.text,
                    ]
                )
                rowcount += 1
            for required_rel in resolve_local_require_targets(path, src_dir, rel_to_id):
                required_id = rel_to_id[required_rel]
                edge_text = f"# Require: {relative} -> {required_rel}"
                w.writerow(
                        [
                            file_id,
                            REL_REQUIRES_FILE,
                            required_id,
                            vectorize_text(edge_text, dim, embedding_mode),
                            edge_text,
                        ]
                    )
                rowcount += 1
                edge_count += 1

    return len(files), rowcount, edge_count, summary_count


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


def verify_twohop_equivalence(cur, table_name: str, questions: list[CodeQuestion], ann_k: int, top_k: int) -> None:
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
        FROM sorted_heap_expand_twohop_rerank(
            '{table_name}'::regclass,
            ARRAY(SELECT entity_id FROM seeds),
            %s::svec,
            {top_k},
            {REL_REQUIRES_FILE},
            {REL_HAS_CHUNK},
            0
        )
    ),
    hop1 AS MATERIALIZED (
        SELECT DISTINCT target_id
        FROM {table_name}
        WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
          AND relation_id = {REL_REQUIRES_FILE}
    ),
    expanded AS MATERIALIZED (
        SELECT *
        FROM {table_name}
        WHERE entity_id = ANY (ARRAY(SELECT target_id FROM hop1))
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
                f"sorted_heap_expand_twohop_rerank mismatch on {table_name} question#{idx}: diff_rows={diff_rows}"
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


def measure_quality_details(cur, table_name: str, case: base.QueryCase, questions: list[CodeQuestion]) -> list[QuestionQuality]:
    sql = case.sql_template.format(table=table_name)
    out: list[QuestionQuality] = []

    for question in questions:
        cur.execute(sql, case.params_builder(question))
        payload_idx = payload_index_from_description(cur.description)
        rows = cur.fetchall()
        payload_rows = [str(row[payload_idx]) for row in rows]
        keyword_pct, full_hit = keyword_coverage(
            [(None, None, None, None, payload) for payload in payload_rows], question.keywords
        )
        out.append(
            QuestionQuality(
                label=question.label,
                keyword_pct=keyword_pct,
                full_hit=full_hit,
                rows=len(rows),
            )
        )

    return out


def measure_payload_details(cur, table_name: str, case: base.QueryCase, questions: list[CodeQuestion]) -> list[QuestionPayloadRow]:
    sql = case.sql_template.format(table=table_name)
    out: list[QuestionPayloadRow] = []

    for question in questions:
        cur.execute(sql, case.params_builder(question))
        payload_idx = payload_index_from_description(cur.description)
        rows = cur.fetchall()
        for idx, row in enumerate(rows, start=1):
            payload = " ".join(str(row[payload_idx]).split())
            out.append(
                QuestionPayloadRow(
                    label=question.label,
                    row_idx=idx,
                    payload=payload[:240],
                )
            )

    return out


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
    ap.add_argument("--embedding-mode", choices=("generic", "code_aware"), default="generic")
    ap.add_argument("--chunk-window", type=int, default=CHUNK_WINDOW)
    ap.add_argument("--chunk-overlap", type=int, default=CHUNK_OVERLAP)
    ap.add_argument("--case-filter", default="")
    ap.add_argument("--question-filter", default="")
    ap.add_argument("--report-questions", action="store_true")
    ap.add_argument("--report-payloads", action="store_true")
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
        questions = [
            CodeQuestion(
                label=q.label,
                prompt=q.prompt,
                keywords=q.keywords,
                query_vec=vectorize_text(q.prompt, args.dim, args.embedding_mode),
                oracle_file_ids=q.oracle_file_ids,
            )
            for q in questions
        ]
        source_files = list_source_files(source_dir)
        oracle_seed_map = build_oracle_seed_map(source_files, questions, args.ann_k)
        questions = [
            CodeQuestion(
                label=q.label,
                prompt=q.prompt,
                keywords=q.keywords,
                query_vec=q.query_vec,
                oracle_file_ids=oracle_seed_map[q.label],
            )
            for q in questions
        ]
        question_filter = re.compile(args.question_filter) if args.question_filter else None
        if question_filter is not None:
            questions = [
                q for q in questions
                if question_filter.search(q.label) or question_filter.search(q.prompt)
            ]
        if not questions:
            raise RuntimeError("question filter removed all questions")
        file_count, rowcount, edge_count, summary_count = build_code_csv(
            source_dir,
            csv_path,
            args.dim,
            args.chunk_window,
            args.chunk_overlap,
            args.embedding_mode,
        )

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
            verify_twohop_equivalence(cur, "facts_sh", questions, args.ann_k, args.top_k)

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

            summary_seed_expand_sql = base.QueryCase(
                "summary_seed_expand_in",
                f"""
                WITH ann AS MATERIALIZED (
                    SELECT entity_id
                    FROM {{table}}
                    WHERE relation_id = {REL_FILE_SUMMARY}
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

            summary_seed_expand_fn = base.QueryCase(
                "summary_seed_expand_fn",
                f"""
                WITH ann AS MATERIALIZED (
                    SELECT entity_id
                    FROM {{table}}
                    WHERE relation_id = {REL_FILE_SUMMARY}
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

            summary_output_sql = base.QueryCase(
                "seed_file_summary_output_in",
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
                FROM {{table}}
                WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
                  AND relation_id = {REL_FILE_SUMMARY}
                ORDER BY embedding <=> %s::svec, entity_id, relation_id, target_id
                LIMIT {args.top_k}
                """,
                lambda q: (q.query_vec, q.query_vec),
            )

            prompt_summary_rerank_sql = base.QueryCase(
                "prompt_summary_rerank_in",
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
                summaries AS MATERIALIZED (
                    SELECT *
                    FROM {{table}}
                    WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
                      AND relation_id = {REL_FILE_SUMMARY}
                )
                SELECT *
                FROM summaries
                ORDER BY (
                    SELECT count(*)
                    FROM unnest(%s::text[]) kw
                    WHERE position(lower(kw) in lower(payload)) > 0
                ) DESC,
                embedding <=> %s::svec,
                entity_id, relation_id, target_id
                LIMIT {args.top_k}
                """,
                lambda q: (q.query_vec, list(extract_prompt_terms(q.prompt)), q.query_vec),
            )

            summary_seed_summary_output_sql = base.QueryCase(
                "summary_seed_summary_output_in",
                f"""
                WITH ann AS MATERIALIZED (
                    SELECT entity_id
                    FROM {{table}}
                    WHERE relation_id = {REL_FILE_SUMMARY}
                    ORDER BY embedding <=> %s::svec
                    LIMIT {args.ann_k}
                ),
                seeds AS MATERIALIZED (
                    SELECT DISTINCT entity_id FROM ann
                )
                SELECT *
                FROM {{table}}
                WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
                  AND relation_id = {REL_FILE_SUMMARY}
                ORDER BY embedding <=> %s::svec, entity_id, relation_id, target_id
                LIMIT {args.top_k}
                """,
                lambda q: (q.query_vec, q.query_vec),
            )

            prompt_summary_seed_rerank_sql = base.QueryCase(
                "prompt_summary_seed_rerank_in",
                f"""
                WITH ann AS MATERIALIZED (
                    SELECT entity_id
                    FROM {{table}}
                    WHERE relation_id = {REL_FILE_SUMMARY}
                    ORDER BY embedding <=> %s::svec
                    LIMIT {args.ann_k}
                ),
                seeds AS MATERIALIZED (
                    SELECT DISTINCT entity_id FROM ann
                ),
                summaries AS MATERIALIZED (
                    SELECT *
                    FROM {{table}}
                    WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
                      AND relation_id = {REL_FILE_SUMMARY}
                )
                SELECT *
                FROM summaries
                ORDER BY (
                    SELECT count(*)
                    FROM unnest(%s::text[]) kw
                    WHERE position(lower(kw) in lower(payload)) > 0
                ) DESC,
                embedding <=> %s::svec,
                entity_id, relation_id, target_id
                LIMIT {args.top_k}
                """,
                lambda q: (q.query_vec, list(extract_prompt_terms(q.prompt)), q.query_vec),
            )

            prompt_summary_chunk_hybrid_sql = base.QueryCase(
                "prompt_summary_chunk_hybrid_in",
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
                summary_ranked AS MATERIALIZED (
                    SELECT
                        s.*,
                        (
                            SELECT count(*)
                            FROM unnest(%s::text[]) kw
                            WHERE position(lower(kw) in lower(s.payload)) > 0
                        ) AS lexical_hits,
                        (s.embedding <=> %s::svec) AS semantic_distance
                    FROM {{table}} s
                    WHERE s.entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
                      AND s.relation_id = {REL_FILE_SUMMARY}
                    ORDER BY lexical_hits DESC, semantic_distance, entity_id
                    LIMIT {max(1, (args.top_k + 1) // 2)}
                ),
                best_chunks AS MATERIALIZED (
                    SELECT entity_id, relation_id, target_id, embedding, payload, lexical_hits, semantic_distance
                    FROM (
                        SELECT
                            c.entity_id,
                            c.relation_id,
                            c.target_id,
                            c.embedding,
                            c.payload,
                            (
                                SELECT count(*)
                                FROM unnest(%s::text[]) kw
                                WHERE position(lower(kw) in lower(c.payload)) > 0
                            ) AS lexical_hits,
                            (c.embedding <=> %s::svec) AS semantic_distance,
                            row_number() OVER (
                                PARTITION BY c.entity_id
                                ORDER BY (
                                    SELECT count(*)
                                    FROM unnest(%s::text[]) kw
                                    WHERE position(lower(kw) in lower(c.payload)) > 0
                                ) DESC,
                                c.embedding <=> %s::svec,
                                c.target_id
                            ) AS chunk_rank
                        FROM {{table}} c
                        WHERE c.entity_id = ANY (ARRAY(SELECT entity_id FROM summary_ranked))
                          AND c.relation_id = {REL_HAS_CHUNK}
                    ) ranked_chunks
                    WHERE chunk_rank = 1
                ),
                combined AS (
                    SELECT entity_id, relation_id, target_id, embedding, payload, lexical_hits, semantic_distance, 0 AS row_kind
                    FROM summary_ranked
                    UNION ALL
                    SELECT entity_id, relation_id, target_id, embedding, payload, lexical_hits, semantic_distance, 1 AS row_kind
                    FROM best_chunks
                )
                SELECT entity_id, relation_id, target_id, embedding, payload
                FROM combined
                ORDER BY lexical_hits DESC, semantic_distance, row_kind, entity_id, relation_id, target_id
                LIMIT {args.top_k}
                """,
                lambda q: (
                    q.query_vec,
                    list(extract_prompt_terms(q.prompt)),
                    q.query_vec,
                    list(extract_prompt_terms(q.prompt)),
                    q.query_vec,
                    list(extract_prompt_terms(q.prompt)),
                    q.query_vec,
                ),
            )

            prompt_summary_seed_chunk_hybrid_sql = base.QueryCase(
                "prompt_summary_seed_chunk_hybrid_in",
                f"""
                WITH ann AS MATERIALIZED (
                    SELECT entity_id
                    FROM {{table}}
                    WHERE relation_id = {REL_FILE_SUMMARY}
                    ORDER BY embedding <=> %s::svec
                    LIMIT {args.ann_k}
                ),
                seeds AS MATERIALIZED (
                    SELECT DISTINCT entity_id FROM ann
                ),
                summary_ranked AS MATERIALIZED (
                    SELECT
                        s.*,
                        (
                            SELECT count(*)
                            FROM unnest(%s::text[]) kw
                            WHERE position(lower(kw) in lower(s.payload)) > 0
                        ) AS lexical_hits,
                        (s.embedding <=> %s::svec) AS semantic_distance
                    FROM {{table}} s
                    WHERE s.entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
                      AND s.relation_id = {REL_FILE_SUMMARY}
                    ORDER BY lexical_hits DESC, semantic_distance, entity_id
                    LIMIT {max(1, (args.top_k + 1) // 2)}
                ),
                best_chunks AS MATERIALIZED (
                    SELECT entity_id, relation_id, target_id, embedding, payload, lexical_hits, semantic_distance
                    FROM (
                        SELECT
                            c.entity_id,
                            c.relation_id,
                            c.target_id,
                            c.embedding,
                            c.payload,
                            (
                                SELECT count(*)
                                FROM unnest(%s::text[]) kw
                                WHERE position(lower(kw) in lower(c.payload)) > 0
                            ) AS lexical_hits,
                            (c.embedding <=> %s::svec) AS semantic_distance,
                            row_number() OVER (
                                PARTITION BY c.entity_id
                                ORDER BY (
                                    SELECT count(*)
                                    FROM unnest(%s::text[]) kw
                                    WHERE position(lower(kw) in lower(c.payload)) > 0
                                ) DESC,
                                c.embedding <=> %s::svec,
                                c.target_id
                            ) AS chunk_rank
                        FROM {{table}} c
                        WHERE c.entity_id = ANY (ARRAY(SELECT entity_id FROM summary_ranked))
                          AND c.relation_id = {REL_HAS_CHUNK}
                    ) ranked_chunks
                    WHERE chunk_rank = 1
                ),
                combined AS (
                    SELECT entity_id, relation_id, target_id, embedding, payload, lexical_hits, semantic_distance, 0 AS row_kind
                    FROM summary_ranked
                    UNION ALL
                    SELECT entity_id, relation_id, target_id, embedding, payload, lexical_hits, semantic_distance, 1 AS row_kind
                    FROM best_chunks
                )
                SELECT entity_id, relation_id, target_id, embedding, payload
                FROM combined
                ORDER BY lexical_hits DESC, semantic_distance, row_kind, entity_id, relation_id, target_id
                LIMIT {args.top_k}
                """,
                lambda q: (
                    q.query_vec,
                    list(extract_prompt_terms(q.prompt)),
                    q.query_vec,
                    list(extract_prompt_terms(q.prompt)),
                    q.query_vec,
                    list(extract_prompt_terms(q.prompt)),
                    q.query_vec,
                ),
            )

            prompt_summary_seed_chunk_hybrid_s3_sql = base.QueryCase(
                "prompt_summary_seed_chunk_hybrid_s3_in",
                f"""
                WITH ann AS MATERIALIZED (
                    SELECT entity_id
                    FROM {{table}}
                    WHERE relation_id = {REL_FILE_SUMMARY}
                    ORDER BY embedding <=> %s::svec
                    LIMIT {args.ann_k}
                ),
                seeds AS MATERIALIZED (
                    SELECT DISTINCT entity_id FROM ann
                ),
                summary_candidates AS MATERIALIZED (
                    SELECT
                        s.*,
                        (
                            SELECT count(*)
                            FROM unnest(%s::text[]) kw
                            WHERE position(lower(kw) in lower(s.payload)) > 0
                        ) AS lexical_hits,
                        (s.embedding <=> %s::svec) AS semantic_distance
                    FROM {{table}} s
                    WHERE s.entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
                      AND s.relation_id = {REL_FILE_SUMMARY}
                    ORDER BY lexical_hits DESC, semantic_distance, entity_id
                    LIMIT {max(1, args.top_k - 1)}
                ),
                summary_output AS MATERIALIZED (
                    SELECT entity_id, relation_id, target_id, embedding, payload, lexical_hits, semantic_distance, 0 AS row_kind
                    FROM summary_candidates
                ),
                best_chunks AS MATERIALIZED (
                    SELECT entity_id, relation_id, target_id, embedding, payload, lexical_hits, semantic_distance, 1 AS row_kind
                    FROM (
                        SELECT
                            c.entity_id,
                            c.relation_id,
                            c.target_id,
                            c.embedding,
                            c.payload,
                            (
                                SELECT count(*)
                                FROM unnest(%s::text[]) kw
                                WHERE position(lower(kw) in lower(c.payload)) > 0
                            ) AS lexical_hits,
                            (c.embedding <=> %s::svec) AS semantic_distance,
                            row_number() OVER (
                                PARTITION BY c.entity_id
                                ORDER BY (
                                    SELECT count(*)
                                    FROM unnest(%s::text[]) kw
                                    WHERE position(lower(kw) in lower(c.payload)) > 0
                                ) DESC,
                                c.embedding <=> %s::svec,
                                c.target_id
                            ) AS chunk_rank
                        FROM {{table}} c
                        WHERE c.entity_id = ANY (
                            ARRAY(
                                SELECT entity_id
                                FROM summary_candidates
                                ORDER BY lexical_hits DESC, semantic_distance, entity_id
                                LIMIT 1
                            )
                        )
                          AND c.relation_id = {REL_HAS_CHUNK}
                    ) ranked_chunks
                    WHERE chunk_rank = 1
                ),
                combined AS (
                    SELECT * FROM summary_output
                    UNION ALL
                    SELECT * FROM best_chunks
                )
                SELECT entity_id, relation_id, target_id, embedding, payload
                FROM combined
                ORDER BY row_kind, lexical_hits DESC, semantic_distance, entity_id, relation_id, target_id
                LIMIT {args.top_k}
                """,
                lambda q: (
                    q.query_vec,
                    list(extract_prompt_terms(q.prompt)),
                    q.query_vec,
                    list(extract_prompt_terms(q.prompt)),
                    q.query_vec,
                    list(extract_prompt_terms(q.prompt)),
                    q.query_vec,
                ),
            )

            prompt_summary_chunk_hybrid_s1_sql = base.QueryCase(
                "prompt_summary_chunk_hybrid_s1_in",
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
                summary_candidates AS MATERIALIZED (
                    SELECT
                        s.*,
                        (
                            SELECT count(*)
                            FROM unnest(%s::text[]) kw
                            WHERE position(lower(kw) in lower(s.payload)) > 0
                        ) AS lexical_hits,
                        (s.embedding <=> %s::svec) AS semantic_distance
                    FROM {{table}} s
                    WHERE s.entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
                      AND s.relation_id = {REL_FILE_SUMMARY}
                    ORDER BY lexical_hits DESC, semantic_distance, entity_id
                    LIMIT {max(1, args.top_k - 1)}
                ),
                summary_output AS MATERIALIZED (
                    SELECT entity_id, relation_id, target_id, embedding, payload, lexical_hits, semantic_distance, 0 AS row_kind
                    FROM summary_candidates
                    ORDER BY lexical_hits DESC, semantic_distance, entity_id
                    LIMIT 1
                ),
                best_chunks AS MATERIALIZED (
                    SELECT entity_id, relation_id, target_id, embedding, payload, lexical_hits, semantic_distance, 1 AS row_kind
                    FROM (
                        SELECT
                            c.entity_id,
                            c.relation_id,
                            c.target_id,
                            c.embedding,
                            c.payload,
                            (
                                SELECT count(*)
                                FROM unnest(%s::text[]) kw
                                WHERE position(lower(kw) in lower(c.payload)) > 0
                            ) AS lexical_hits,
                            (c.embedding <=> %s::svec) AS semantic_distance,
                            row_number() OVER (
                                PARTITION BY c.entity_id
                                ORDER BY (
                                    SELECT count(*)
                                    FROM unnest(%s::text[]) kw
                                    WHERE position(lower(kw) in lower(c.payload)) > 0
                                ) DESC,
                                c.embedding <=> %s::svec,
                                c.target_id
                            ) AS chunk_rank
                        FROM {{table}} c
                        WHERE c.entity_id = ANY (ARRAY(SELECT entity_id FROM summary_candidates))
                          AND c.relation_id = {REL_HAS_CHUNK}
                    ) ranked_chunks
                    WHERE chunk_rank = 1
                ),
                combined AS (
                    SELECT * FROM summary_output
                    UNION ALL
                    SELECT * FROM best_chunks
                )
                SELECT entity_id, relation_id, target_id, embedding, payload
                FROM combined
                ORDER BY row_kind, lexical_hits DESC, semantic_distance, entity_id, relation_id, target_id
                LIMIT {args.top_k}
                """,
                lambda q: (
                    q.query_vec,
                    list(extract_prompt_terms(q.prompt)),
                    q.query_vec,
                    list(extract_prompt_terms(q.prompt)),
                    q.query_vec,
                    list(extract_prompt_terms(q.prompt)),
                    q.query_vec,
                ),
            )

            prompt_summary_chunk_hybrid_s3_sql = base.QueryCase(
                "prompt_summary_chunk_hybrid_s3_in",
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
                summary_candidates AS MATERIALIZED (
                    SELECT
                        s.*,
                        (
                            SELECT count(*)
                            FROM unnest(%s::text[]) kw
                            WHERE position(lower(kw) in lower(s.payload)) > 0
                        ) AS lexical_hits,
                        (s.embedding <=> %s::svec) AS semantic_distance
                    FROM {{table}} s
                    WHERE s.entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
                      AND s.relation_id = {REL_FILE_SUMMARY}
                    ORDER BY lexical_hits DESC, semantic_distance, entity_id
                    LIMIT {max(1, args.top_k - 1)}
                ),
                summary_output AS MATERIALIZED (
                    SELECT entity_id, relation_id, target_id, embedding, payload, lexical_hits, semantic_distance, 0 AS row_kind
                    FROM summary_candidates
                ),
                best_chunks AS MATERIALIZED (
                    SELECT entity_id, relation_id, target_id, embedding, payload, lexical_hits, semantic_distance, 1 AS row_kind
                    FROM (
                        SELECT
                            c.entity_id,
                            c.relation_id,
                            c.target_id,
                            c.embedding,
                            c.payload,
                            (
                                SELECT count(*)
                                FROM unnest(%s::text[]) kw
                                WHERE position(lower(kw) in lower(c.payload)) > 0
                            ) AS lexical_hits,
                            (c.embedding <=> %s::svec) AS semantic_distance,
                            row_number() OVER (
                                PARTITION BY c.entity_id
                                ORDER BY (
                                    SELECT count(*)
                                    FROM unnest(%s::text[]) kw
                                    WHERE position(lower(kw) in lower(c.payload)) > 0
                                ) DESC,
                                c.embedding <=> %s::svec,
                                c.target_id
                            ) AS chunk_rank
                        FROM {{table}} c
                        WHERE c.entity_id = ANY (
                            ARRAY(
                                SELECT entity_id
                                FROM summary_candidates
                                ORDER BY lexical_hits DESC, semantic_distance, entity_id
                                LIMIT 1
                            )
                        )
                          AND c.relation_id = {REL_HAS_CHUNK}
                    ) ranked_chunks
                    WHERE chunk_rank = 1
                ),
                combined AS (
                    SELECT * FROM summary_output
                    UNION ALL
                    SELECT * FROM best_chunks
                )
                SELECT entity_id, relation_id, target_id, embedding, payload
                FROM combined
                ORDER BY row_kind, lexical_hits DESC, semantic_distance, entity_id, relation_id, target_id
                LIMIT {args.top_k}
                """,
                lambda q: (
                    q.query_vec,
                    list(extract_prompt_terms(q.prompt)),
                    q.query_vec,
                    list(extract_prompt_terms(q.prompt)),
                    q.query_vec,
                    list(extract_prompt_terms(q.prompt)),
                    q.query_vec,
                ),
            )

            prompt_summary_chunk_local2_sql = base.QueryCase(
                "prompt_summary_chunk_local2_in",
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
                summary_candidates AS MATERIALIZED (
                    SELECT
                        s.*,
                        (
                            SELECT count(*)
                            FROM unnest(%s::text[]) kw
                            WHERE position(lower(kw) in lower(s.payload)) > 0
                        ) AS lexical_hits,
                        (s.embedding <=> %s::svec) AS semantic_distance
                    FROM {{table}} s
                    WHERE s.entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
                      AND s.relation_id = {REL_FILE_SUMMARY}
                    ORDER BY lexical_hits DESC, semantic_distance, entity_id
                    LIMIT {max(1, args.top_k - 2)}
                ),
                summary_output AS MATERIALIZED (
                    SELECT entity_id, relation_id, target_id, embedding, payload, lexical_hits, semantic_distance, 0 AS row_kind
                    FROM summary_candidates
                ),
                chunk_anchor AS MATERIALIZED (
                    SELECT entity_id, target_id AS anchor_target_id
                    FROM (
                        SELECT
                            c.entity_id,
                            c.target_id,
                            (
                                SELECT count(*)
                                FROM unnest(%s::text[]) kw
                                WHERE position(lower(kw) in lower(c.payload)) > 0
                            ) AS lexical_hits,
                            (c.embedding <=> %s::svec) AS semantic_distance,
                            row_number() OVER (
                                ORDER BY (
                                    SELECT count(*)
                                    FROM unnest(%s::text[]) kw
                                    WHERE position(lower(kw) in lower(c.payload)) > 0
                                ) DESC,
                                c.embedding <=> %s::svec,
                                c.target_id
                            ) AS local_rank
                        FROM {{table}} c
                        WHERE c.entity_id = ANY (
                            ARRAY(
                                SELECT entity_id
                                FROM summary_candidates
                                ORDER BY lexical_hits DESC, semantic_distance, entity_id
                                LIMIT 1
                            )
                        )
                          AND c.relation_id = {REL_HAS_CHUNK}
                    ) ranked_anchor
                    WHERE local_rank = 1
                ),
                local_chunks AS MATERIALIZED (
                    SELECT entity_id, relation_id, target_id, embedding, payload, lexical_hits, semantic_distance, 1 AS row_kind
                    FROM (
                        SELECT
                            c.entity_id,
                            c.relation_id,
                            c.target_id,
                            c.embedding,
                            c.payload,
                            (
                                SELECT count(*)
                                FROM unnest(%s::text[]) kw
                                WHERE position(lower(kw) in lower(c.payload)) > 0
                            ) AS lexical_hits,
                            (c.embedding <=> %s::svec) AS semantic_distance,
                            abs(c.target_id - a.anchor_target_id) AS local_distance,
                            row_number() OVER (
                                ORDER BY
                                    abs(c.target_id - a.anchor_target_id),
                                    (
                                        SELECT count(*)
                                        FROM unnest(%s::text[]) kw
                                        WHERE position(lower(kw) in lower(c.payload)) > 0
                                    ) DESC,
                                    c.embedding <=> %s::svec,
                                    c.target_id
                            ) AS local_rank
                        FROM {{table}} c
                        JOIN chunk_anchor a
                          ON a.entity_id = c.entity_id
                        WHERE c.relation_id = {REL_HAS_CHUNK}
                    ) ranked_chunks
                    WHERE local_rank <= 2
                ),
                combined AS (
                    SELECT * FROM summary_output
                    UNION ALL
                    SELECT * FROM local_chunks
                )
                SELECT entity_id, relation_id, target_id, embedding, payload
                FROM combined
                ORDER BY row_kind, lexical_hits DESC, semantic_distance, entity_id, relation_id, target_id
                LIMIT {args.top_k}
                """,
                lambda q: (
                    q.query_vec,
                    list(extract_prompt_terms(q.prompt)),
                    q.query_vec,
                    list(extract_prompt_terms(q.prompt)),
                    q.query_vec,
                    list(extract_prompt_terms(q.prompt)),
                    q.query_vec,
                    list(extract_prompt_terms(q.prompt)),
                    q.query_vec,
                    list(extract_prompt_terms(q.prompt)),
                    q.query_vec,
                ),
            )

            oracle_seed_expand_in = base.QueryCase(
                "oracle_seed_expand_in",
                f"""
                SELECT *
                FROM {{table}}
                WHERE entity_id = ANY (%s::int4[])
                  AND relation_id = {REL_HAS_CHUNK}
                ORDER BY embedding <=> %s::svec, entity_id, relation_id, target_id
                LIMIT {args.top_k}
                """,
                lambda q: (list(q.oracle_file_ids), q.query_vec),
            )

            oracle_seed_expand_fn = base.QueryCase(
                "oracle_seed_expand_fn",
                f"""
                SELECT *
                FROM sorted_heap_expand_rerank(
                    '{{table}}'::regclass,
                    %s::int4[],
                    %s::svec,
                    {args.top_k},
                    {REL_HAS_CHUNK},
                    0
                )
                """,
                lambda q: (list(q.oracle_file_ids), q.query_vec),
            )

            keyword_rerank_sql = base.QueryCase(
                "oracle_keyword_rerank_in",
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
                ORDER BY (
                    SELECT count(*)
                    FROM unnest(%s::text[]) kw
                    WHERE position(lower(kw) in lower(payload)) > 0
                ) DESC,
                embedding <=> %s::svec,
                entity_id, relation_id, target_id
                LIMIT {args.top_k}
                """,
                lambda q: (q.query_vec, list(q.keywords), q.query_vec),
            )

            prompt_lexical_rerank_sql = base.QueryCase(
                "prompt_lexical_rerank_in",
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
                ORDER BY (
                    SELECT count(*)
                    FROM unnest(%s::text[]) kw
                    WHERE position(lower(kw) in lower(payload)) > 0
                ) DESC,
                embedding <=> %s::svec,
                entity_id, relation_id, target_id
                LIMIT {args.top_k}
                """,
                lambda q: (q.query_vec, list(extract_prompt_terms(q.prompt)), q.query_vec),
            )

            prompt_diverse_rerank_sql = base.QueryCase(
                "prompt_diverse_rerank_in",
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
                ),
                scored AS MATERIALIZED (
                    SELECT
                        expanded.*,
                        (
                            SELECT count(*)
                            FROM unnest(%s::text[]) kw
                            WHERE position(lower(kw) in lower(expanded.payload)) > 0
                        ) AS lexical_hits,
                        (expanded.embedding <=> %s::svec) AS semantic_distance
                    FROM expanded
                ),
                ranked AS MATERIALIZED (
                    SELECT
                        scored.*,
                        row_number() OVER (
                            PARTITION BY entity_id
                            ORDER BY lexical_hits DESC, semantic_distance, target_id
                        ) AS file_rank
                    FROM scored
                )
                SELECT entity_id, relation_id, target_id, embedding, payload
                FROM ranked
                ORDER BY file_rank, lexical_hits DESC, semantic_distance, entity_id, relation_id, target_id
                LIMIT {args.top_k}
                """,
                lambda q: (q.query_vec, list(extract_prompt_terms(q.prompt)), q.query_vec),
            )

            dependency_twohop_sql = base.QueryCase(
                "seed_require_twohop_in",
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
                hop1 AS MATERIALIZED (
                    SELECT DISTINCT target_id
                    FROM {{table}}
                    WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
                      AND relation_id = {REL_REQUIRES_FILE}
                ),
                expanded AS MATERIALIZED (
                    SELECT *
                    FROM {{table}}
                    WHERE entity_id = ANY (ARRAY(SELECT target_id FROM hop1))
                      AND relation_id = {REL_HAS_CHUNK}
                )
                SELECT *
                FROM expanded
                ORDER BY embedding <=> %s::svec, entity_id, relation_id, target_id
                LIMIT {args.top_k}
                """,
                lambda q: (q.query_vec, q.query_vec),
            )

            dependency_twohop_fn = base.QueryCase(
                "seed_require_twohop_fn",
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
                FROM sorted_heap_expand_twohop_rerank(
                    '{{table}}'::regclass,
                    ARRAY(SELECT entity_id FROM seeds),
                    %s::svec,
                    {args.top_k},
                    {REL_REQUIRES_FILE},
                    {REL_HAS_CHUNK},
                    0
                )
                """,
                lambda q: (q.query_vec, q.query_vec),
            )

            seed_plus_require_sql = base.QueryCase(
                "seed_file_plus_require_in",
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
                hop1 AS MATERIALIZED (
                    SELECT DISTINCT target_id AS entity_id
                    FROM {{table}}
                    WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
                      AND relation_id = {REL_REQUIRES_FILE}
                ),
                expanded_entities AS MATERIALIZED (
                    SELECT entity_id FROM seeds
                    UNION
                    SELECT entity_id FROM hop1
                ),
                expanded AS MATERIALIZED (
                    SELECT *
                    FROM {{table}}
                    WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM expanded_entities))
                      AND relation_id = {REL_HAS_CHUNK}
                )
                SELECT *
                FROM expanded
                ORDER BY embedding <=> %s::svec, entity_id, relation_id, target_id
                LIMIT {args.top_k}
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
            print(f"require_edges:      {edge_count}")
            print(f"summary_rows:       {summary_count}")
            print(f"queries:            {len(questions)}")
            print(f"runs:               {args.runs}")
            print(f"dim:                {args.dim}")
            print(f"embedding_mode:     {args.embedding_mode}")
            print(f"ann_k:              {args.ann_k}")
            print(f"top_k:              {args.top_k}")
            print(f"ef_search:          {args.ef_search}")
            print(f"ef_construction:    {args.ef_construction}")
            print(f"m:                  {args.m}")
            print(f"shared_buffers:     {args.shared_buffers_mb}MB")
            print(f"backend_mode:       {args.backend_mode}")
            if args.question_filter:
                print(f"question_filter:    {args.question_filter}")
            print()

            for question in questions:
                oracle_ids = ",".join(str(v) for v in question.oracle_file_ids)
                prompt_terms = ",".join(extract_prompt_terms(question.prompt))
                print(
                    f"query|label={question.label}|keywords={','.join(question.keywords)}|"
                    f"prompt_terms={prompt_terms}|oracle_files={oracle_ids}|prompt={question.prompt}"
                )
            print()

            cases: list[tuple[str, str, base.QueryCase]] = [
                ("facts_heap", "facts_heap", direct_ann),
                ("facts_sh", "facts_sh", direct_ann),
                ("facts_heap", "facts_heap", seed_expand_sql),
                ("facts_sh", "facts_sh", seed_expand_sql),
                ("facts_sh", "facts_sh", seed_expand_fn),
                ("facts_heap", "facts_heap", prompt_diverse_rerank_sql),
                ("facts_sh", "facts_sh", prompt_diverse_rerank_sql),
                ("facts_heap", "facts_heap", prompt_lexical_rerank_sql),
                ("facts_sh", "facts_sh", prompt_lexical_rerank_sql),
                ("facts_heap", "facts_heap", keyword_rerank_sql),
                ("facts_sh", "facts_sh", keyword_rerank_sql),
                ("facts_heap", "facts_heap", summary_seed_expand_sql),
                ("facts_sh", "facts_sh", summary_seed_expand_sql),
                ("facts_sh", "facts_sh", summary_seed_expand_fn),
                ("facts_heap", "facts_heap", summary_output_sql),
                ("facts_sh", "facts_sh", summary_output_sql),
                ("facts_heap", "facts_heap", prompt_summary_rerank_sql),
                ("facts_sh", "facts_sh", prompt_summary_rerank_sql),
                ("facts_heap", "facts_heap", summary_seed_summary_output_sql),
                ("facts_sh", "facts_sh", summary_seed_summary_output_sql),
                ("facts_heap", "facts_heap", prompt_summary_seed_rerank_sql),
                ("facts_sh", "facts_sh", prompt_summary_seed_rerank_sql),
                ("facts_heap", "facts_heap", prompt_summary_chunk_hybrid_sql),
                ("facts_sh", "facts_sh", prompt_summary_chunk_hybrid_sql),
                ("facts_heap", "facts_heap", prompt_summary_chunk_hybrid_s1_sql),
                ("facts_sh", "facts_sh", prompt_summary_chunk_hybrid_s1_sql),
                ("facts_heap", "facts_heap", prompt_summary_chunk_hybrid_s3_sql),
                ("facts_sh", "facts_sh", prompt_summary_chunk_hybrid_s3_sql),
                ("facts_heap", "facts_heap", prompt_summary_chunk_local2_sql),
                ("facts_sh", "facts_sh", prompt_summary_chunk_local2_sql),
                ("facts_heap", "facts_heap", prompt_summary_seed_chunk_hybrid_sql),
                ("facts_sh", "facts_sh", prompt_summary_seed_chunk_hybrid_sql),
                ("facts_heap", "facts_heap", prompt_summary_seed_chunk_hybrid_s3_sql),
                ("facts_sh", "facts_sh", prompt_summary_seed_chunk_hybrid_s3_sql),
                ("facts_heap", "facts_heap", oracle_seed_expand_in),
                ("facts_sh", "facts_sh", oracle_seed_expand_in),
                ("facts_sh", "facts_sh", oracle_seed_expand_fn),
                ("facts_heap", "facts_heap", seed_plus_require_sql),
                ("facts_sh", "facts_sh", seed_plus_require_sql),
                ("facts_heap", "facts_heap", dependency_twohop_sql),
                ("facts_sh", "facts_sh", dependency_twohop_sql),
                ("facts_sh", "facts_sh", dependency_twohop_fn),
            ]

            case_filter = re.compile(args.case_filter) if args.case_filter else None
            if case_filter is not None:
                cases = [
                    item for item in cases
                    if case_filter.search(item[0]) or case_filter.search(item[2].name)
                ]

            for label, table, case in cases:
                print(f"running|table={label}|case={case.name}", flush=True)
                p50, avg, hits, reads, root, rows = measure_case(cur, table, case, questions, args.runs)
                keyword_pct, full_pct, avg_rows = measure_quality(cur, table, case, questions)
                print_result(label, case.name, p50, avg, hits, reads, root, rows, keyword_pct, full_pct, avg_rows)
                if args.report_questions:
                    for detail in measure_quality_details(cur, table, case, questions):
                        print(
                            f"detail|table={label}|case={case.name}|label={detail.label}|"
                            f"keyword_pct={detail.keyword_pct:.1f}|full_hit={'1' if detail.full_hit else '0'}|rows={detail.rows}"
                        )
                if args.report_payloads:
                    for detail in measure_payload_details(cur, table, case, questions):
                        print(
                            f"payload|table={label}|case={case.name}|label={detail.label}|"
                            f"row={detail.row_idx}|text={detail.payload}"
                        )
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
