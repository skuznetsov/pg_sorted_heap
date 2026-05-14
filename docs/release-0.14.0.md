---
layout: default
title: Release 0.14.0
nav_order: 10
---

# Release 0.14.0

This release hardens the `0.14.0` line around insertion locality, native
partitioning, and planner-visible vector search while keeping experimental
research paths out of the stable contract.

## Highlights

- `clustered_pk_index` accepts one or two integer key columns for
  directed-placement workloads. `(tenant_id, id)` style layouts no longer
  collapse placement to the first column.
- Composite zone-map metadata tracks first-key block ranges plus a bounded
  second-key summary where it is safe to do so.
- `sorted_heap_zonemap_may_match_int8(regclass, bigint, bigint)` provides a
  fail-open first-key range probe. It returns `false` only when valid metadata
  proves a range cannot match.
- Native PostgreSQL partitioned parents have explicit sorted-heap maintenance
  helpers and partition-aware vector search helper paths.
- `sorted_hnsw` is planner-visible through the Index AM path for covered KNN
  shapes, including parent `Merge Append` over leaf HNSW indexes.
- GraphRAG release gates cover lifecycle, dump/restore, crash recovery, and
  concurrent online maintenance with KNN/GraphRAG readers.
- `sorted_heap_bulk_load_ordered(...)` adds a witness-bearing ordered bulk-load
  helper for deterministic ingestion experiments. It is a stable helper API,
  not a hidden table-AM behavior change.

## Stable boundaries

- `sorted_heap` table storage, compaction, merge, zone-map pruning, online
  compact/merge, and scan stats are stable release surfaces.
- `svec`/`hsvec`, `sorted_hnsw`, and the documented GraphRAG wrapper/dispatcher
  APIs are stable release surfaces.
- Partition support is explicit: use the provided partition maintenance and
  route/helper APIs when operating on partitioned parents.
- FlashHadamard, TurboQuant, and large-vector research sketches remain
  experimental. They are useful for comparison and research, but they are not
  part of the `0.14.0` stable contract.

## Upgrade

```text
ALTER EXTENSION pg_sorted_heap UPDATE TO '0.14.0';
```

The upgrade installs new SQL helpers, catalogs, and planner support functions.
Existing tables do not need rewrites just to load the new extension version.
Run normal maintenance or the partition helpers when you want refreshed
physical order, zone-map metadata, or vector indexes after data changes.

## Verification

The `0.14.0` stabilization pass was verified locally on PostgreSQL 18. The
compatibility follow-up also rechecked PostgreSQL 16 and both supported
upgrade paths to PostgreSQL 18:

- `make installcheck REGRESS='pg_sorted_heap sorted_hnsw graph_rag'`
- `make test-release`
- `make installcheck PG_CONFIG=/opt/homebrew/opt/postgresql@16/bin/pg_config REGRESS='pg_sorted_heap sorted_hnsw graph_rag'`
- `PG_OLD_VERSION=16 PG_NEW_VERSION=18 ./scripts/test_pg_upgrade.sh /private/tmp 65476 65477`
- `./scripts/test_pg_upgrade.sh /private/tmp 65496 65497`
- `bash ./scripts/selftest_release_archive_contract.sh /private/tmp`
- archive dry-run: `git archive HEAD`, then `make install` and `make installcheck`
  from the unpacked archive on PostgreSQL 18
- `./scripts/bench_witness_bulk_load.sh /private/tmp 65511 8192 512 512,2048,8192 128`
- `./scripts/bench_partitioned_sorted_hnsw.sh /private/tmp 65492 3 200 6 8 5 8 16 8 32`
- `bash ./scripts/bench_ann_matrix_offline_smoke.sh /private/tmp 320 3 8 3`

Small smoke-benchmark observations from this pass:

- Witness bulk-load locality improved from `15.398` average blocks per tenant
  for unsorted COPY to `1.320` for full/window-8192 ordering on the 8K-row
  smoke corpus, with similar insert time.
- Partitioned `sorted_hnsw` selected/all-leaf helper paths and parent
  `Merge Append` returned `1.0000` recall on the small deterministic smoke.
- The ANN matrix smoke emitted the expected rows for exact heap,
  `sorted_hnsw`, pgvector HNSW, IVFPQ, and FlashHadamard. The tiny offline
  corpus is a surface-regression smoke, not a quality benchmark.
