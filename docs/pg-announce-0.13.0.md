# pg_sorted_heap 0.13.0 released

I am pleased to announce `pg_sorted_heap 0.13.0`, a PostgreSQL extension for
physically sorted heap storage, zone-map pruning, planner-integrated vector
search, and a narrow fact-shaped GraphRAG query surface.

Repository:

https://github.com/skuznetsov/pg_sorted_heap

Release:

https://github.com/skuznetsov/pg_sorted_heap/releases/tag/v0.13.0

## What is pg_sorted_heap?

`pg_sorted_heap` is a PostgreSQL extension that adds:

- `sorted_heap`: a table access method that keeps rows physically ordered by
  primary key and prunes heap blocks with per-page zone maps.
- `sorted_hnsw`: a planner-integrated HNSW index access method for built-in
  vector types.
- `svec` and `hsvec`: float32 and float16 vector types for PostgreSQL.
- Fact-shaped GraphRAG helpers for retrieving and reranking graph-shaped facts
  directly inside PostgreSQL.

The storage side is aimed at workloads where physical locality matters:
time-series data, event logs, IoT readings, ordered IDs, and large tables with
range predicates on primary-key columns.

The vector and GraphRAG side is aimed at applications that want retrieval to
stay inside PostgreSQL instead of introducing a separate vector-search sidecar.

## What is new in 0.13.0?

The main change in `0.13.0` is that the narrow fact-shaped GraphRAG contract is
now part of the stable release surface.

The stable GraphRAG API includes:

- `sorted_heap_graph_rag(...)`
- `sorted_heap_graph_register(...)`
- `sorted_heap_graph_config(...)`
- `sorted_heap_graph_unregister(...)`
- `sorted_heap_graph_rag_stats()`
- `sorted_heap_graph_rag_reset_stats()`

This contract is intentionally narrow. It is designed for fact tables clustered
by `(entity_id, relation_id, target_id)`, or by an equivalent registered alias
mapping. Queries start with ANN seed retrieval, expand through one or more
relation hops, and then exact-rerank the expanded candidate set.

Example:

    SELECT *
    FROM sorted_heap_graph_rag(
        'facts'::regclass,
        '[0.1,0.2,0.3,...]'::svec,
        relation_path := ARRAY[1, 2],
        ann_k := 64,
        top_k := 10,
        score_mode := 'path'
    );

`0.13.0` also adds a stable routed GraphRAG entry point for multi-shard or
multi-tenant application flows:

- `sorted_heap_graph_route(...)`
- `sorted_heap_graph_route_plan(...)`

This gives applications one dispatcher for exact-key routing, range routing,
profiles, policies, and defaults.

## Other release highlights

`0.13.0` also includes:

- Schema registration for non-canonical fact tables, so GraphRAG can be used
  with existing column names.
- Backend-local GraphRAG stage stats: seed count, expanded rows, reranked rows,
  returned rows, and per-stage timing.
- Lifecycle hardening across extension upgrade, dump/restore, crash recovery,
  concurrent online compact, and concurrent online merge.
- A shared-cache correctness fix for `sorted_hnsw` multi-index workloads.
- `sorted_hnsw.build_sq8`, an opt-in low-memory index-build mode for
  constrained builders.
- An experimental FlashHadamard retrieval lane. This is documented and tested,
  but it is not the default ANN path and not part of the stable GraphRAG
  contract.

## Benchmark snapshots

These are workload-specific benchmark snapshots from the release notes, not
universal performance claims.

AWS Gutenberg workload, about `104K x 2880D`, top-10:

- `sorted_hnsw (svec)`: `1.287 ms`, `100.0% Recall@10`
- `sorted_hnsw (hsvec)`: `1.404 ms`, `100.0% Recall@10`
- pgvector `halfvec`: `2.031 ms`, `99.8% Recall@10`

AWS fact-shaped multihop GraphRAG workload, `5K` chains, `384D`:

- `sorted_heap_expand_twohop_path_rerank()`: median `0.962 ms`
- `sorted_heap_graph_rag_twohop_path_scan()`: median `1.025 ms`
- pgvector parity row: median `1.434 ms`
- Qdrant parity row: median `3.355 ms`

## Installation

Requirements:

- PostgreSQL 16, 17, or 18
- Standard PGXS build toolchain with `pg_config` in `PATH`

Build from source:

    git clone https://github.com/skuznetsov/pg_sorted_heap.git
    cd pg_sorted_heap
    make
    make install

Enable the extension:

    CREATE EXTENSION pg_sorted_heap;

Upgrade an existing installation:

    ALTER EXTENSION pg_sorted_heap UPDATE TO '0.13.0';

## Minimal examples

Create a physically sorted table:

    CREATE TABLE events (
        id      bigint PRIMARY KEY,
        ts      timestamptz,
        payload text
    ) USING sorted_heap;

    SELECT sorted_heap_compact('events'::regclass);

Create a planner-integrated HNSW index:

    CREATE TABLE documents (
        id        bigserial PRIMARY KEY,
        embedding svec(384),
        content   text
    );

    CREATE INDEX documents_embedding_idx
    ON documents USING sorted_hnsw (embedding)
    WITH (m = 16, ef_construction = 200);

    SET sorted_hnsw.ef_search = 96;

    SELECT id, content
    FROM documents
    ORDER BY embedding <=> '[0.1,0.2,0.3,...]'::svec
    LIMIT 10;

Create a stable fact-shaped GraphRAG table:

    CREATE TABLE facts (
        entity_id   int4,
        relation_id int2,
        target_id   int4,
        embedding   svec(384),
        payload     text,
        PRIMARY KEY (entity_id, relation_id, target_id)
    ) USING sorted_heap;

    CREATE INDEX facts_embedding_idx
    ON facts USING sorted_hnsw (embedding)
    WITH (m = 24, ef_construction = 200);

## Verification

The `0.13.0` release-candidate checks include:

- SQL regression coverage for `pg_sorted_heap`, `sorted_hnsw`, and `graph_rag`
- GraphRAG lifecycle coverage for upgrade and dump/restore
- crash recovery checks
- concurrent online-operation checks
- `pg_upgrade` coverage from PostgreSQL 16 to 18 and from 17 to 18
- `sorted_hnsw` chunked/shared-cache integration checks

For local verification:

    make test-release

For the narrower GraphRAG release bundle:

    make test-graphrag-release

## Links

- Repository: https://github.com/skuznetsov/pg_sorted_heap
- Documentation: https://skuznetsov.github.io/pg_sorted_heap/
- Release notes: https://github.com/skuznetsov/pg_sorted_heap/releases/tag/v0.13.0
- Issues: https://github.com/skuznetsov/pg_sorted_heap/issues
