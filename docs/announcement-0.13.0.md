---
layout: default
title: Announcement Pack 0.13.0
nav_order: 11
---

# Announcement Pack 0.13.0

This page is the repo-owned outreach pack for `v0.13.0`.

The goal is to keep one canonical messaging source for:

- GitHub release follow-up posts
- LinkedIn
- Hacker News
- Reddit
- PostgreSQL community posts
- targeted outreach such as `mempalace`

## Core message

`pg_sorted_heap v0.13.0` promotes the narrow fact-shaped GraphRAG contract to
the stable release surface.

The stable story for this release is:

- planner-integrated `sorted_hnsw` for `svec` and `hsvec`
- stable fact-shaped GraphRAG entry points
- stable routed GraphRAG dispatcher for multi-shard app flows
- lifecycle hardening across upgrade, dump/restore, crash recovery, and
  concurrent online operations

The experimental story for this release is:

- FlashHadamard as a documented research lane
- not the default ANN path
- not the stable GraphRAG contract

## Canonical proof points

Use these numbers consistently.

### Vector search

AWS Gutenberg, `~104K x 2880D`, top-10:

- `sorted_hnsw (svec)`: `1.287 ms`, `100.0% Recall@10`
- `sorted_hnsw (hsvec)`: `1.404 ms`, `100.0% Recall@10`
- pgvector `halfvec`: `2.031 ms`, `99.8% Recall@10`

### Stable fact-shaped GraphRAG

AWS fact-shaped multihop GraphRAG, `5K` chains, `384D`:

- `sorted_heap_expand_twohop_path_rerank()`: median `0.962 ms`
- `sorted_heap_graph_rag_twohop_path_scan()`: median `1.025 ms`
- pgvector parity row: median `1.434 ms`
- Qdrant parity row: median `3.355 ms`

### Release verification

Release gates for `0.13.0`:

- full `CI` green on PostgreSQL `16`, `17`, and `18`
- `pg_upgrade 16→18` and `17→18` green
- local `make test-release` green on `2026-04-07`

## What to claim

- `0.13.0` makes the narrow fact-shaped GraphRAG surface stable
- `sorted_hnsw` is the default ANN path
- routed GraphRAG now has one stable app-facing dispatcher
- the release is lifecycle-hardened, not just benchmarked

## What not to claim

- do not say FlashHadamard is production-default
- do not say `pg_sorted_heap` is a general-purpose GraphRAG framework for all
  graph workloads
- do not say the code-corpus retrieval contracts are stable
- do not say the current FlashHadamard branch is the headline of `0.13.0`

## Release blurb

Use this short blurb when a site needs one paragraph.

> `pg_sorted_heap v0.13.0` makes the narrow fact-shaped GraphRAG contract
> stable inside PostgreSQL. The release also hardens routed GraphRAG,
> planner-integrated `sorted_hnsw` search for `svec`/`hsvec`, and lifecycle
> coverage across upgrade, dump/restore, crash recovery, and concurrent online
> operations. FlashHadamard remains included as an experimental research lane,
> not the default ANN path.

## LinkedIn

### LinkedIn post

> We shipped `pg_sorted_heap v0.13.0`.
>
> The main change in this release is that the narrow fact-shaped GraphRAG
> contract is now part of the stable surface inside PostgreSQL.
>
> In practical terms, `0.13.0` brings together:
>
> - `sorted_heap` as a sorted table access method
> - `sorted_hnsw` as the planner-integrated ANN path for `svec` and `hsvec`
> - stable fact-shaped GraphRAG entry points
> - a stable routed GraphRAG dispatcher for multi-shard application flows
>
> Some current anchors:
>
> - Gutenberg `~104K x 2880D`: `sorted_hnsw (hsvec)` at `1.404 ms`, `100.0% Recall@10`
> - same corpus: pgvector `halfvec` at `2.031 ms`, `99.8% Recall@10`
> - fact-shaped multihop GraphRAG (`5K` chains, `384D`): `0.962 ms` median on the path-aware helper
>
> A big part of this release was also lifecycle hardening:
>
> - extension upgrade
> - dump/restore
> - crash recovery
> - concurrent online operations
> - shared-cache correctness
> - CI green on PostgreSQL 16, 17, and 18, including `pg_upgrade 16 -> 18` and `17 -> 18`
>
> FlashHadamard is included in `0.13.0`, but still explicitly experimental.
> The stable headline of this release is GraphRAG + planner-integrated ANN
> inside PostgreSQL.
>
> Release: https://github.com/skuznetsov/pg_sorted_heap/releases/tag/v0.13.0
> Repo: https://github.com/skuznetsov/pg_sorted_heap

### LinkedIn short version

> We shipped `pg_sorted_heap v0.13.0`.
>
> This release makes the narrow fact-shaped GraphRAG contract stable inside
> PostgreSQL, with `sorted_hnsw` as the planner-integrated ANN path for
> `svec`/`hsvec` and `sorted_heap_graph_route(...)` as the stable routed
> dispatcher.
>
> Current anchors:
>
> - Gutenberg `~104K x 2880D`: `1.404 ms`, `100.0% Recall@10` (`hsvec`)
> - fact-shaped multihop GraphRAG (`5K` chains, `384D`): `0.962 ms` median
>
> FlashHadamard is in the release, but remains experimental.
>
> Release: https://github.com/skuznetsov/pg_sorted_heap/releases/tag/v0.13.0

## Hacker News

### Suggested title options

- `Show HN: pg_sorted_heap 0.13.0, GraphRAG and ANN inside PostgreSQL`
- `Show HN: pg_sorted_heap 0.13.0 — stable fact-shaped GraphRAG in Postgres`
- `Show HN: a PostgreSQL extension for sorted storage, ANN, and GraphRAG`

### Suggested body

> We shipped `pg_sorted_heap v0.13.0`.
>
> It is a PostgreSQL extension with:
>
> - a sorted table access method (`sorted_heap`)
> - planner-integrated ANN search (`sorted_hnsw`) for `svec` and `hsvec`
> - a now-stable narrow fact-shaped GraphRAG contract
> - a stable routed GraphRAG dispatcher for multi-shard application flows
>
> The stable GraphRAG surface is intentionally narrow:
>
> - fact rows clustered by `(entity_id, relation_id, target_id)` or registered aliases
> - one-hop through multi-hop retrieval via `relation_path`
> - exact rerank on the expanded candidate set
>
> Current benchmark anchors:
>
> - Gutenberg `~104K x 2880D`: `sorted_hnsw (hsvec)` `1.404 ms`, `100.0% Recall@10`
> - same corpus: pgvector `halfvec` `2.031 ms`, `99.8% Recall@10`
> - fact-shaped multihop GraphRAG (`5K` chains, `384D`): `0.962 ms` median on the path-aware helper
>
> This release also focused heavily on lifecycle hardening:
>
> - extension upgrade
> - dump/restore
> - crash recovery
> - concurrent online operations
> - shared-cache correctness
> - CI green on PostgreSQL 16, 17, and 18, including `pg_upgrade 16→18` and `17→18`
>
> FlashHadamard is in the repo and release, but still explicitly experimental.
> The stable headline of `0.13.0` is GraphRAG + planner-integrated ANN inside
> PostgreSQL.
>
> Repo: https://github.com/skuznetsov/pg_sorted_heap
> Release: https://github.com/skuznetsov/pg_sorted_heap/releases/tag/v0.13.0

## Reddit

### r/PostgreSQL draft

Title:

`pg_sorted_heap v0.13.0: stable fact-shaped GraphRAG + planner-integrated ANN inside PostgreSQL`

Body:

> We shipped `pg_sorted_heap v0.13.0`.
>
> This release makes the narrow fact-shaped GraphRAG surface stable and keeps
> the default ANN path inside PostgreSQL through `sorted_hnsw` for `svec` and
> `hsvec`.
>
> Release highlights:
>
> - stable `sorted_heap_graph_rag(...)`
> - stable routed dispatcher: `sorted_heap_graph_route(...)`
> - lifecycle hardening across upgrade, dump/restore, crash recovery, and
>   concurrent online operations
> - planner-integrated ANN instead of a sidecar search path
>
> Current benchmark anchors:
>
> - Gutenberg `~104K x 2880D`: `sorted_hnsw (hsvec)` `1.404 ms`, `100.0% Recall@10`
> - pgvector `halfvec`: `2.031 ms`, `99.8% Recall@10`
>
> FlashHadamard is included as experimental only; it is not the stable release
> headline.
>
> Repo: https://github.com/skuznetsov/pg_sorted_heap
> Release: https://github.com/skuznetsov/pg_sorted_heap/releases/tag/v0.13.0

### r/LocalLLaMA draft

Title:

`pg_sorted_heap 0.13.0: stable GraphRAG in PostgreSQL, plus an experimental FlashHadamard lane`

Body:

> We shipped `pg_sorted_heap v0.13.0`.
>
> The stable part of the release is a narrow fact-shaped GraphRAG contract
> inside PostgreSQL, backed by planner-integrated ANN (`sorted_hnsw`) for
> `svec` and `hsvec`.
>
> Current anchors:
>
> - Gutenberg `~104K x 2880D`: `1.404 ms`, `100.0% Recall@10` on `hsvec`
> - fact-shaped multihop GraphRAG (`5K` chains, `384D`): `0.962 ms` median
>
> FlashHadamard is also in the repo, but still explicitly experimental. We are
> not positioning it as the default retrieval path in this release.
>
> Repo: https://github.com/skuznetsov/pg_sorted_heap
> Release: https://github.com/skuznetsov/pg_sorted_heap/releases/tag/v0.13.0

## PostgreSQL community

### Short mailing-list / forum note

> We shipped `pg_sorted_heap v0.13.0`.
>
> The main change is that the narrow fact-shaped GraphRAG contract is now part
> of the stable release surface, alongside `sorted_heap` and the
> planner-integrated `sorted_hnsw` ANN path for `svec` and `hsvec`.
>
> This release also adds lifecycle hardening across extension upgrade,
> dump/restore, crash recovery, concurrent online operations, and
> `pg_upgrade 16→18` and `17→18`.
>
> Release: https://github.com/skuznetsov/pg_sorted_heap/releases/tag/v0.13.0
> Repo: https://github.com/skuznetsov/pg_sorted_heap

## mempalace outreach

Use this only after the `0.13.0` release is public.

### Suggested discussion opener

> We just shipped `pg_sorted_heap v0.13.0`, which is a PostgreSQL extension for
> sorted storage, planner-integrated ANN, and a stable fact-shaped GraphRAG
> surface inside Postgres.
>
> We also built a working `mempalace` PostgreSQL backend spike in our fork that
> preserves result quality on a small corpus and keeps ChromaDB as the default.
>
> We are not proposing a backend switch by surprise. The concrete question is:
> would you be interested in a small abstraction-first PR or discussion around
> optional backend support?
>
> If yes, we can share:
>
> - the fork branch
> - exact benchmark numbers
> - the current integration surface and caveats

## Posting order

Recommended sequence:

1. LinkedIn
2. Hacker News
3. r/PostgreSQL
4. PostgreSQL community note
5. targeted `mempalace` discussion

Reason:

- LinkedIn is low-friction and establishes the release publicly
- HN needs the cleanest positioning and benefits from the fresh release link
- PostgreSQL channels care more about lifecycle and operator credibility
- `mempalace` outreach is stronger after the release is visibly public
