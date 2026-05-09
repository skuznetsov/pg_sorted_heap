---
layout: default
title: Functional Gap Specs
nav_order: 13
---

# Functional Gap Specs

This document tracks the next functional gaps after the `0.13.0` release
surface. It is intentionally contract-first: each gap should get a small spec,
acceptance tests, and only then implementation.

## Gap Inventory

### G0. Composite-PK pruning quality

Current state:

- The zone map stores the first two PK columns.
- Initial direct leaf probe on a compacted `(tenant_id, id)` sorted_heap table
  produced `Custom Scan (SortedHeapScan)`, but scanned `308 of 310` blocks for
  `tenant_id = 1 AND id BETWEEN 100 AND 110`.
- The zone map entries contained second-column ranges, so this is not a
  storage-layout absence. The likely gap is in the sorted-prefix range search:
  it uses the first column as the main binary-search key and does not yet make
  the lexicographic `(col1, col2)` bound tight enough.
- Implemented first-pass fix: when column 1 is fixed by inclusive equality and
  column 2 has a range/equality bound, the sorted-prefix candidate span is
  refined with existing second-column zone-map overlap checks.
- Regression `SH21B` now guards `tenant_id = 1 AND id BETWEEN 100 AND 110`
  with a tight scanned-block threshold and verifies count correctness.
- Regression `SH21B-3` now guards that an unsorted tail remains conservative
  for fixed-column-1 and bounded-column-2 predicates, preserving correctness
  after tail inserts.
- Regression `SH21B` now has a dedicated first-column-only range guard, so the
  first-pass composite refinement does not regress the original col1 pruning
  path.
- Regression `SH21B-3` now guards generic prepared runtime composite bounds
  under `force_generic_plan`, so executor-startup parameter resolution keeps the
  same tight pruning shape as constant `(tenant_id, id)` predicates.
- `docs/spec-composite-pk-pruning.md` documents the first-pass contract and
  remaining lexicographic follow-ups.

Risk:

- Partitioning and fact-shaped GraphRAG commonly use composite keys such as
  `(tenant_id, id)` or `(entity_id, relation_id, target_id)`. If second-column
  pruning is weak, we keep correctness but give back much of the locality win.

Target direction:

- Continue from the first-pass refinement toward true lexicographic binary
  search for the sorted prefix path only if planning-time refinement over a
  large tenant span becomes measurable overhead and the zone-map metadata can
  prove lexicographic endpoint ordering.
- Keep linear/tail pruning conservative, but make compacted prefix queries with
  equality on column 1 and range/equality on column 2 map to tight block ranges.

### G1. Declarative partitioning support

Current state:

- `sorted_heap` works as a concrete table access method on physical tables.
- Each physical `sorted_heap` relation owns its own block-0 meta page, zone
  map, sorted-prefix state, compact/merge lifecycle, and optional
  `sorted_hnsw` indexes.
- PostgreSQL declarative partitioned parents now have first-pass explicit
  status and maintenance helpers:
  `sorted_heap_partition_status(...)`,
  `sorted_heap_compact_partitions(...)`,
  `sorted_heap_merge_partitions(...)`, and
  `sorted_heap_rebuild_zonemap_partitions(...)`.
- Verified probe: sorted_heap leaves under a declarative partitioned parent can
  be created and compacted, and a direct leaf query can use
  `SortedHeapScan`.
- First-pass planner fix: the same covered predicate shape issued through the
  partitioned parent now reaches `SortedHeapScan` on the pruned leaf.
- Regression `SH23` covers parent status, leaf-by-leaf compact, fail-closed
  unsupported heap/no-PK leaf behavior, explicit skip mode, empty partition
  parents, nested partition trees, and parent-query `SortedHeapScan` for
  single-leaf and multi-leaf range-partition shapes, including a generic
  prepared runtime-bound parent query.
- GraphRAG routing already supports multiple concrete shard relations, but it
  does not automatically dispatch across a PostgreSQL partitioned-table parent.

Risk:

- Treating partitioning as one broad feature would mix storage maintenance,
  planner pruning, index fanout, and GraphRAG global merge semantics.

Target direction:

- Make partition support explicit and incremental:
  1. leaf tables first: done;
  2. parent-level maintenance helpers: first pass done;
  3. parent/leaf observability: first pass done;
  4. parent-query `SortedHeapScan` planner support: covered single-leaf and
     multi-leaf range shapes plus generic prepared runtime-bound shape done;
  5. optional GraphRAG/ANN parent dispatch later.

### G2. Huge-table compaction operating model

Current state:

- `sorted_heap_compact(regclass)` is a rewrite-and-swap operation similar in
  operational shape to `CLUSTER` or `VACUUM FULL`.
- `sorted_heap_merge(regclass)` avoids re-sorting an already sorted prefix, but
  still rewrites into a new relation.
- Online compact/merge reduce blocking time, not temporary disk-space
  requirements.
- `docs/spec-huge-table-compaction.md` now documents the first-pass operating
  model: concrete operations rewrite one relation; partition helpers bound the
  rewrite unit to one leaf.

Risk:

- A user may expect compaction to be an in-place defragmentation operation with
  no second-copy space requirement. That is not the current contract.

Target direction:

- For very large logical tables, keep partition-scoped compaction as the
  default operational story.
- Longer term: explore segment-level compaction inside a relation, but do not
  promise this until crash-safety and index semantics are specified.

### G3. Filtered ANN with `sorted_hnsw`

Current state:

- `sorted_hnsw` is intentionally narrow: base-relation
  `ORDER BY embedding <=> query LIMIT k`.
- It avoids planning for unbounded KNN and for extra base-table quals that can
  under-return candidates.
- GraphRAG wrappers handle some filtered retrieval patterns outside the generic
  Index AM path.
- `docs/spec-filtered-ann.md` now separates filtered retrieval into
  pre-filter/materialize, ANN-overfetch/exact-rerank, and
  partition/routing-aware global-merge modes.
- `sorted_hnsw` regression now has explicit planner-safety guards for
  unbounded KNN, `LIMIT > sorted_hnsw.ef_search`, and filtered KNN shapes.
- `sorted_hnsw_partition_search_status(...)` now exposes underfill metadata for
  routed partition search without changing the row-returning API.
- `sorted_hnsw_partition_search(..., exact_fallback := true)` now lets callers
  explicitly replace an underfilled selected-leaf ANN pool with exact rerank
  over the same selected leaves.
- `sorted_hnsw_partition_search(...)` now uses a C SRF over the leaf-local
  `sorted_hnsw` Index AM rather than PL/pgSQL dynamic fanout, preserving the
  public SQL contract while removing selected-leaf orchestration overhead.

Risk:

- Users will expect pgvector-like filtered ANN behavior from a normal-looking
  index.

Target direction:

- Keep the transparent `sorted_hnsw` planner guard intact until helper-level
  filtered contracts prove underfill handling and global merge semantics.
- Implement helper-level modes from `docs/spec-filtered-ann.md` before
  promoting any filtered ANN path into the planner.
- Treat exact fallback as an explicit selected-leaf helper mode, not a generic
  `WHERE`-qual planner mode.
- Keep transparent filtered ANN planner support gated behind explicit
  underfill/global-merge semantics. The C partition helper closes the
  route-first helper overhead gap, but it does not by itself make arbitrary
  `WHERE`-qualified KNN safe.

### G4. Parent-level observability

Current state:

- `sorted_heap_zonemap_stats(regclass)` reports one concrete relation.
- `sorted_heap_partition_status(parent)` is the first row-returning
  observability surface. It accepts a partitioned parent or a concrete
  sorted_heap table and reports leaf AM, PK presence, zone-map validity,
  sorted-prefix pages, zone-map entries, overflow pages, and relation size.
- `sorted_heap_partition_index_status(parent)` now reports leaf index health:
  index AM, valid/ready/live flags, primary/unique flags, and convenience
  booleans for btree and `sorted_hnsw`.
- `sorted_heap_scan_stats_by_relation()` now reports backend-local
  relation-aware scan counters for same-backend diagnostics, and shared
  relation-aware counters when the extension is preloaded.
- `sorted_heap_partition_scan_stats(parent)` now rolls those counters up to
  sorted_heap leaves under a partitioned parent or concrete table.
- Aggregate scan stats are global/per-backend counters, not per partition.
- GraphRAG aggregate stats are backend-local last-call stats.
- Routed/segmented GraphRAG now also exposes backend-local per-shard last-call
  stats with `source_rel` identity.

Risk:

- On partitioned deployments, `SortedHeapScan` counters now have parent leaf
  rollups and routed GraphRAG calls have backend-local `source_rel` execution
  rows. Broader persistent telemetry is still deliberately out of scope.

Target direction:

- Treat `sorted_heap_partition_status(...)` as the storage-state baseline.
- Treat `sorted_heap_partition_index_status(...)` as the index-health baseline.
- Add broader row-returning parent observability only when it can reuse
  relation-aware scan stats or `sorted_heap_graph_route_last_stats()` without
  relabeling backend-local diagnostics as persistent telemetry. See
  `docs/spec-parent-runtime-observability.md`.

### G5. Online compact/merge restrictions for lossy PKs

Current state:

- Online compact/merge rejects UUID/text/varchar PKs because the replay key is
  currently lossy `int64`.
- SH13 regression covers the fail-closed online compact/merge behavior for
  UUID, text, and varchar PKs, including the concrete operation/type rejection
  message.
- `docs/spec-online-lossy-pk.md` documents the required future design:
  pruning keys may stay lossy, but online replay identity must be lossless.

Risk:

- The offline path works, so the online limitation may surprise users.

Target direction:

- Replace the replay map key with a lossless Datum/composite key representation
  or a serialized binary key, with full-key equality after hash lookup.
- Keep current fail-closed behavior until then.

### G6. `pg_dump` / `pg_restore` zone-map ergonomics

Current state:

- Data restores correctly.
- Zone-map pruning needs compact/merge after restore.
- Existing docs now call this out for concrete relations and partitioned
  parents; HNSW sidecars still need rebuild because restored heap TIDs change.
- `sorted_heap_restore_plan(parent default NULL)` now reports concrete
  sorted_heap tables/leaves that need compact/merge after restore and flags
  `sorted_hnsw` indexes that should be rebuilt after `pg_restore`.

Risk:

- Users may see correct but slower queries after restore and not understand why.

Target direction:

- Keep the explicit restore checklist in operator docs.
- Keep `sorted_heap_restore_plan(...)` read-only; correctness does not depend
  on it, but it reduces post-restore operator guesswork.

### G7. Zone-map-only / index-only-like fast paths

Current state:

- `SortedHeapScan` uses zone maps to choose heap block ranges, then returns
  heap tuples via the normal table scan path.
- Zone maps store page-level min/max for the first two PK columns. They do not
  store tuple values, TIDs, or MVCC visibility information.
- `docs/spec-zone-map-only-fast-paths.md` now defines the boundary: current
  zone maps can skip pages or prove empty overlap, but cannot implement a true
  PostgreSQL `Index Only Scan`.

Risk:

- Calling this "index-only" overstates the feature and invites an unsafe
  implementation that bypasses heap visibility or returns rows from lossy page
  metadata.

Target direction:

- Keep `0.13` as heap-backed `SortedHeapScan`.
- If needed later, choose explicitly between metadata-only fast paths and a
  covering value-bearing sidecar / Index AM contract.

### G8. Large-vector sublinear search revival

Current state:

- `sorted_hnsw` is the stable planner-integrated vector default.
- IVF-PQ remains available as a legacy/manual `svec_ann_scan(...)` path.
- FlashHadamard is a documented experimental packed exhaustive lane.
- Existing FlashHadamard notes refute IVF/pruning as the next default win at
  `103K x 2880D`, but do not refute sublinear routing at `500K+` / `1M+`.
- `docs/spec-large-vector-sublinear.md` now defines the benchmark gate for any
  IVF-PQ, SQ8, or SQ4 revival.
- `make bench-large-vector-synthetic` now provides the reproducible synthetic
  baseline entry point. Its defaults are smoke-sized; large-scale runs override
  `BENCH_LARGE_VECTOR_ROWS`, `BENCH_LARGE_VECTOR_QUERIES`, and
  `BENCH_LARGE_VECTOR_DIM`.
- `scripts/bench_ann_real_dataset.py --enable-ivfpq` adds an opt-in residual
  IVF-PQ row for real-corpus runs. It remains off by default because training,
  generated-code insertion, and compaction are part of the measured cost.
- `scripts/bench_ann_real_dataset.py --enable-flashhadamard` adds an opt-in
  offline FlashHadamard packed exhaustive row on the same vectors, queries, and
  exact PostgreSQL ground truth. It is a comparator, not PostgreSQL storage
  lifecycle integration.
- The same harness accepts local `.npy` / `.npz` vector inputs so smoke tests
  can exercise the full matrix without downloading external ANN-Benchmarks
  files.
- `make bench-ann-matrix-offline-smoke` wraps that local-input path and checks
  exact heap, `sorted_hnsw`, pgvector HNSW, residual IVF-PQ, and FlashHadamard
  packed exhaustive in one deterministic smoke run.

Risk:

- Promoting IVF-PQ or SQ4 without a scale gate would weaken the product story:
  it would add complexity while competing with stronger current defaults on the
  validated `103K` operating point.

Target direction:

- Keep IVF-PQ as explicit/manual until a large-scale harness proves a winning
  region against `sorted_hnsw` and FlashHadamard.
- Use the synthetic harness first to establish exact/sorted_hnsw/pgvector and
  optional DiskANN baselines before adding IVF-PQ or FlashHadamard rows.
- Use the real-dataset `--enable-ivfpq` row to gate any claim that residual
  IVF-PQ has a winning region; do not infer it from synthetic data alone.
- Use the real-dataset `--enable-flashhadamard` row as the packed exhaustive
  quality/footprint comparator before promoting any sublinear lane.
- Prefer SQ8 before SQ4 for productized candidate scanning unless a 4-bit
  FlashHadamard-derived path proves equal quality at lower footprint.

### G9. SIMD ADC and pgvectorscale DiskANN comparison

Current state:

- PQ ADC has a scalar lookup path and a C-level `svec_ann_scan(...)` path that
  avoids per-row fmgr overhead.
- FlashHadamard has an experimental NEON int16 path and a kernel lab with AVX2
  candidates; existing notes refute AVX2 int16 and keep AVX2 gather as
  microbench-only.
- pgvectorscale StreamingDiskANN is an external graph-index baseline with SBQ,
  query rescoring, label filtering, and relaxed-order behavior.
- `docs/spec-simd-adc-diskann.md` now separates local ADC kernel optimization
  from product-level DiskANN benchmarking.
- `scripts/bench_sorted_hnsw_vs_pgvector.sh` now reports strict-order mode and
  optional pgvectorscale DiskANN rows. If `vectorscale` is absent, it emits a
  skip note and keeps the local benchmark runnable.

Risk:

- A microbench-only SIMD win can disappear in PostgreSQL executor overhead.
- A DiskANN comparison can be misleading if it omits strict-order behavior,
  query rescoring, build settings, or footprint.

Target direction:

- Use the optional pgvectorscale harness row for apples-to-apples DiskANN
  comparisons when the extension is installed.
- Only integrate SIMD kernels behind scalar parity gates and platform guards.

## Prioritization

| Priority | Gap | Reason |
|----------|-----|--------|
| P0 | G0 composite-PK pruning quality | First-pass fixed-col1/bounded-col2 path landed; const, generic runtime, first-col-only, and tail correctness guarded; full lexicographic search remains benchmark-gated follow-up |
| P0 | G1 declarative partitioning support | Directly affects huge-table operating model and interview/product story |
| P0 | G2 huge-table compaction model | Needed to explain free-space requirements honestly |
| P1 | G4 parent-level observability | Storage, index-health, local/shared scan rollups, and GraphRAG source-rel route stats landed; broader persistent telemetry remains out of scope |
| P1 | G3 filtered ANN | Expected by vector-search users, but broader than storage |
| P2 | G7 zone-map-only / index-only-like fast paths | Spec boundary landed; real row-returning path requires a covering sidecar |
| P2 | G8 large-vector sublinear search revival | Benchmark-gated; do not reopen refuted 103K pruning as a default |
| P2 | G9 SIMD ADC and pgvectorscale DiskANN comparison | Benchmark-gated; external baseline needs versioned settings and strict-order note |
| P2 | G5 online lossy-PK support | Useful, but current fail-closed behavior is acceptable |
| P2 | G6 restore ergonomics | Checklist and read-only restore plan helper landed |

## Quadrumvirate Notes

Cassandra:

- Likely failure mode: collapsing partition support, GraphRAG routing, and ANN
  fanout into one over-broad implementation.
- Likely failure mode: making parent functions silently skip unsupported leaves,
  hiding operational drift.
- Likely failure mode: implementing parent maintenance while leaving
  composite-key pruning weak, so partitioned tenant tables work operationally
  but not fast enough.

Daedalus:

- Reframe from "support partitioned tables" to "define exact contracts for
  leaf storage, parent maintenance, and cross-leaf query merge".

Maieutic:

- Refuted assumption: PostgreSQL planner will automatically produce the same
  `SortedHeapScan` path through a partitioned parent that it produces on the
  leaf directly. Probe result: direct leaf query used `SortedHeapScan`; parent
  query did not.
- Assumption: parent-level compact can be operationally useful even if it
  locks one leaf at a time. Falsifier: lock behavior or transaction semantics
  require a different procedure-style API.
- Refuted then partially fixed assumption: current two-column zone map gives
  tight pruning for `(tenant_id, id)` compacted tables. Probe result before the
  fix: `id BETWEEN 100 AND 110` under `tenant_id = 1` scanned most leaf blocks.
  `SH21B` now guards the fixed-col1/bounded-col2 path.

Adversary:

- Parent APIs must not accidentally operate on foreign partitions or
  non-sorted_heap leaves without an explicit policy.
- Parent APIs must report partial failure clearly.
- Cross-leaf ANN/GraphRAG needs global top-k exact merge; local top-k concat is
  not a sufficient correctness contract.
