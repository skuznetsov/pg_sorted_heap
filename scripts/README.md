# Scripts Directory Map

The scripts are intentionally kept in one flat directory for compatibility with
existing Makefile targets and external copy/paste commands. Use this map to
find the right category without relying on filename memory.

## Release gates

These scripts are the main script-backed pieces of the product-quality safety
net. `make test-release` also runs Makefile-level gates such as
`policy-safety-selftest` and GraphRAG `installcheck`, so treat the Makefile as
the source of truth for exact release composition.

- `run_pg_core_regression_smoke.sh`
- `test_dump_restore.sh`
- `test_toast_and_concurrent_compact.sh`
- `test_alter_table.sh`
- `test_partition_lock_behavior.sh`
- `test_crash_recovery.sh`
- `test_concurrent_online_ops.sh`
- `test_hnsw_chunked_cache.sh`
- `test_pg_upgrade.sh`
- `test_graph_rag_lifecycle.sh`
- `test_graph_rag_crash_recovery.sh`
- `test_graph_rag_concurrent.sh`

Related Makefile-level release gates:

- `policy-safety-selftest`
- GraphRAG `installcheck REGRESS='pg_sorted_heap sorted_hnsw graph_rag'`

## Product benchmarks

These scripts compare stable or beta product surfaces.

- `bench_sorted_heap.sh`
- `bench_crud_comparison.sh`
- `bench_sorted_hnsw_vs_pgvector.sh`
- `bench_partitioned_sorted_hnsw.sh`
- `bench_ann_matrix_offline_smoke.sh`
- `bench_nomic_local_ann.py`

## Research and experimental benchmarks

These scripts support FlashHadamard, TurboQuant, KV-routing, and exploratory
benchmark lanes. They are not release gates.

- `bench_turboquant_retrieval.py`
- `bench_turboquant_packed_screen.py`
- `bench_flashhiggs2_sweep.py`
- `bench_fh_kernel.c`
- `bench_fh_int16_compare.sql`
- `bench_kv_routing_exp1a.py`
- `bench_gutenberg_k8s_ann.sh`
- `bench_graph_rag_multidepth_segmented_aws.sh`
- `repeat_graph_rag_code_corpus_builds_aws.sh`
- `repeat_graph_rag_multihop_builds_aws.sh`

## Policy, planner, and selftest harnesses

These scripts validate workflow contracts, planner-cost sentinels, and
long-running policy-review probes. They are valuable for development, but not
the first place new users should start.

- `run_unnest_ab_*`
- `selftest_run_unnest_ab_*`
- `compare_unnest_ab_*`
- `benchmark_unnest_ab_*`
- `accumulate_unnest_ab_*`
- `derive_unnest_ab_*`
- `run_planner_cost_probe.sh`
- `selftest_run_planner_probe_with_summary.sh`
- `compare_planner_probe_logsets.sh`
- `summarize_planner_probe_log.sh`

## Operational helpers

- `find_vector_python.sh`
- `test_shared_relation_scan_stats.sh`
- `test_graph_builder.sh`
- `repro_shared_cache_multihop.sh`
- `tmp-clean` is exposed through the Makefile rather than a standalone script.
