# Stability Matrix

This document separates the public release surface from lower-level helpers,
research lanes, and compatibility paths. The goal is to make adoption safer:
users should know which APIs are expected to be stable, which APIs are useful
but still evolving, and which APIs are primarily present for benchmarks or
backward compatibility.

## Stability levels

| Level | Contract | Compatibility expectation | Recommended for apps |
|---|---|---|---|
| Stable | Primary product API with regression and lifecycle coverage | Backward-compatible within the extension release line | Yes |
| Beta | Useful and tested, but still subject to shape changes | Best-effort compatibility; may be promoted after more use | Only with pinned version |
| Experimental | Research or benchmark lane | May change or disappear between releases | No |
| Legacy/manual | Older compatibility path or manual workflow | Kept for migration and comparison when practical | Prefer stable alternatives |

## Stable

| Surface | What it provides | Main evidence |
|---|---|---|
| `sorted_heap` table access method | Physically sorted heap storage with zone-map pruning | Core regression, dump/restore, crash recovery, TOAST, ALTER TABLE, online operation tests |
| `sorted_heap_compact(...)`, `sorted_heap_merge(...)` | Offline maintenance with full rewrite semantics | Regression and crash-recovery coverage |
| `sorted_heap_compact_online(...)`, `sorted_heap_merge_online(...)` | Non-blocking maintenance with trigger-based change replay | Concurrent DML tests, crash-recovery tests |
| `sorted_heap_bulk_load_ordered(...)` and append-run witness helpers | Trusted ordered bulk ingestion plus fail-closed observational run metadata | API-stable docs and witness bulk-load benchmark smoke |
| Zone-map inspection and rebuild helpers | Operational visibility and recovery | Regression and lifecycle tests |
| Zone-map metadata overlap probe | Fail-open empty-result proof for first-key `int8` ranges | Regression and documented no-row-return contract |
| `sorted_heap_scan_stats(...)` and reset helpers | Runtime scan observability | Regression and dedicated selftests |
| `svec` and `hsvec` vector types | Native vector storage for the extension | Regression and vector benchmark coverage |
| `sorted_hnsw` Index AM | Planner-integrated KNN over `svec`/`hsvec` with exact rerank | `sorted_hnsw` regression, chunked-cache test, benchmark harness |
| `sorted_hnsw_scan_stats()` and `sorted_hnsw_reset_stats()` | Backend-local ANN/top-up/exact-fallback attribution | `sorted_hnsw` regression and benchmark smoke |
| Stable fact-shaped GraphRAG wrapper | ANN seed retrieval plus fact-graph expansion and rerank | GraphRAG lifecycle, crash, concurrent, and release tests |
| Routed GraphRAG dispatcher and plan helper | Stable high-level route resolution and introspection | GraphRAG lifecycle and release tests |
| Partition maintenance helpers | Parent-to-leaf maintenance convenience with fail-closed behavior | Partition lock and regression tests |

## Beta

| Surface | Why beta | Promotion condition |
|---|---|---|
| Lower-level GraphRAG expand/rerank functions | Valuable building blocks, but easier to misuse than the high-level wrappers | More app-level usage and a narrower recommended recipe |
| Lower-level routed GraphRAG catalog/resolve helpers | Operationally useful, but mostly implementation detail for the stable route wrapper | Stable operator workflows around route management |
| Code-corpus/snippet GraphRAG contracts in benchmark logic | Workload-specific semantics are still being refined | Dedicated SQL contract and reproducible benchmark set |
| Partition-aware vector helper surface | Useful for large partitioned datasets, still being tuned around local/global `k` behavior | Broader benchmark matrix and planner guidance |

## Experimental

| Surface | Why experimental | Safe use |
|---|---|---|
| FlashHadamard SQL lane | Research retrieval/quantization path, not the default serving path | Benchmarks and design experiments only |
| FlashHadamard file-backed segment store | Uses a separate storage lane with experimental threading assumptions | Local experiments with explicit benchmark notes |
| TurboQuant/FlashHadamard Make targets | Research harnesses, not release validation gates | Run via `make research-help` |
| Large-vector sublinear sketches | Design/spec work, not a stable execution contract | Compare against stable `sorted_hnsw` and exact baselines |

## Legacy/manual

| Surface | Replacement | Notes |
|---|---|---|
| `svec_ann_scan(...)` / `svec_ann_search(...)` IVF-PQ path | `sorted_hnsw` for planner-integrated KNN | Kept for comparison and manual workflows |
| Sidecar HNSW scan functions | `sorted_hnsw` Index AM | Use only when reproducing older experiments |
| Historical extension SQL install files | Current install plus upgrade scripts | Required for PostgreSQL extension upgrade compatibility |

## Release gates

Stable-surface changes should pass the strongest relevant subset of:

- `make installcheck REGRESS=pg_sorted_heap`
- `make installcheck REGRESS=sorted_hnsw`
- `make installcheck REGRESS=graph_rag`
- `make test-dump-restore`
- `make test-pg-upgrade`
- `make test-crash-recovery`
- `make test-concurrent`
- `make test-hnsw-chunked-cache`
- `make test-graphrag-release`
- `make test-release`

Experimental-surface changes should still compile and should include a focused
benchmark or smoke test, but they are not promoted to stable solely because a
research benchmark passes.
