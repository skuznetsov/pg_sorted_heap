# Changelog

## 0.13.0 (in development)

### GraphRAG release status

- The narrow fact-shaped GraphRAG contract is now the intended stable
  `0.13` release surface:
  - `sorted_heap_graph_rag(...)`
  - `sorted_heap_graph_register(...)`
  - `sorted_heap_graph_config(...)`
  - `sorted_heap_graph_unregister(...)`
  - `sorted_heap_graph_rag_stats()`
  - `sorted_heap_graph_rag_reset_stats()`
- Lower-level helper/wrapper building blocks remain beta:
  - `sorted_heap_expand_ids(...)`
  - `sorted_heap_expand_rerank(...)`
  - `sorted_heap_expand_twohop_rerank(...)`
  - `sorted_heap_expand_twohop_path_rerank(...)`
  - `sorted_heap_graph_rag_scan(...)`
  - `sorted_heap_graph_rag_twohop_scan(...)`
  - `sorted_heap_graph_rag_twohop_path_scan(...)`
- Code-corpus snippet/symbol/lexical retrieval contracts remain
  benchmark/reference logic, not the stable SQL surface.
- Added `make test-graphrag-release` to run the full GraphRAG release-candidate
  bundle:
  - SQL regression
  - lifecycle
  - crash recovery
  - concurrent online-operation coverage
- Added `make test-release` to run the broader `0.13` extension release bundle:
  - core regression smoke
  - policy/doc contract selftests
  - dump/restore, TOAST, DDL, crash recovery, concurrent online ops
  - `pg_upgrade`
  - the narrower `make test-graphrag-release` bundle
- Clarified the GraphRAG docs around:
  - `limit_rows` as a work cap rather than a final result-count override
  - one-hop `score_mode := 'path'` being intentionally equivalent to
    `endpoint`

### sorted_hnsw shared cache fix

- Fixed a multi-index shared-cache corruption bug where
  `shnsw_shared_scan_cache_attach()` held bare pointers into shared
  memory. A subsequent publish for a different index overwrote the shared
  region, silently corrupting the first index's cached HNSW graph.
- The attach path now deep-copies all bulk data (L0 neighbors, SQ8
  vectors, upper-level neighbor slabs) into local palloc'd buffers.
- Added a multi-index overwrite regression phase (B5) to
  `scripts/test_hnsw_chunked_cache.sh`.
- Verified: `shared_cache=on` and `off` produce identical retrieval
  quality on the 5K x 384D and 10K x 384D multihop benchmarks.
  `shared_cache=off` is no longer needed as a correctness workaround.

### GraphRAG syntax unification

- Added `sorted_heap_graph_rag(...)` as the new unified fact-shaped GraphRAG
  entry point.
- The new syntax accepts:
  - `relation_path := ARRAY[hop]` for one-hop retrieval
  - `relation_path := ARRAY[hop1, hop2]` for two-hop retrieval
  - `relation_path := ARRAY[hop1, hop2, ...]` for explicit multi-hop retrieval
  - `score_mode := 'endpoint' | 'path'`
- One-hop semantics are now aligned with the fact-graph contract:
  ANN seed selection is on `entity_id`, not `target_id`.
- Added regression coverage for:
  - one-hop unified syntax
  - two-hop endpoint-scored unified syntax
  - two-hop path-aware unified syntax
  - generic path-aware multihop syntax
- Added `docs/graphrag-0.13-plan.md` to separate the narrow stable `0.13`
  target from the broader experimental code-GraphRAG surface.

### GraphRAG schema registration

- Added:
  - `sorted_heap_graph_register(...)`
  - `sorted_heap_graph_config(...)`
  - `sorted_heap_graph_unregister(...)`
- GraphRAG helpers and wrappers can now run against non-canonical fact-table
  schemas as long as the mapped columns still satisfy the fact contract:
  `int4 / int2 / int4 / svec / text`.
- Added regression coverage for an alias schema using:
  - `src_id`
  - `edge_type`
  - `dst_id`
  - `vec`
  - `body`

### GraphRAG lifecycle hardening

- Added `pg_extension_config_dump(...)` coverage for
  `sorted_heap_graph_registry`, so registered GraphRAG mappings survive
  `pg_dump` / `pg_restore`.
- Added `scripts/test_graph_rag_lifecycle.sh` and `make test-graphrag-lifecycle`
  to verify:
  - `0.12.0 -> 0.13.0` extension upgrade
  - alias-schema registration
  - registry persistence across dump/restore
  - persistence of segmented/routed GraphRAG registries across dump/restore:
    - shared shard metadata
    - shared `segment_labels`
    - range routing
    - exact-key routing
    - route policies
    - route profiles
    - route defaults
    - effective default `segment_labels`
  - post-restore GraphRAG query correctness on registered alias schemas
  - post-restore GraphRAG query correctness on routed/default-backed segmented
    GraphRAG queries
- Added `scripts/test_graph_rag_crash_recovery.sh` and
  `make test-graphrag-crash` to verify crash recovery for:
  - committed registered GraphRAG tables
  - crash during insert into a registered/indexed graph table
  - crash during compact on a registered graph table

### GraphRAG observability

- Added:
  - `sorted_heap_graph_rag_stats()`
  - `sorted_heap_graph_rag_reset_stats()`
- GraphRAG now exposes backend-local last-call stats for:
  - seed count
  - expanded row count
  - reranked row count
  - returned row count
  - ANN / expand / rerank / total timing
- Added regression coverage for:
  - direct helper observability via `sorted_heap_expand_rerank(...)`
  - unified wrapper observability via `sorted_heap_graph_rag(...)`
- The reported `api` field reflects the concrete top-level GraphRAG execution
  path, so unified wrapper calls report the underlying C path they dispatched
  to.

### GraphRAG scale harnesses

- Added `--hop-weight` to `scripts/bench_graph_rag_multidepth.py` so large
  synthetic multihop runs can vary the relative hop contribution without
  changing the SQL/GraphRAG contract.
- Added `sorted_hnsw.build_sq8` for constrained-memory index builds.
  - builds the graph from SQ8-compressed build vectors instead of a full
    float32 build slab
  - costs an extra heap scan during `CREATE INDEX`
  - first bounded local result on `1M x 64D`:
    - `build_indexes`: `48.606 s -> 46.541 s`
    - depth-5 unified GraphRAG stayed `87.5% / 100.0%`
- Added `scripts/bench_graph_rag_multidepth_aws.sh` to run the synthetic
  multi-hop depth benchmark on a remote AWS host using the same sync/install
  pattern as the existing multihop AWS runners.
- Added `scripts/bench_graph_rag_multidepth_segmented.py` to benchmark the
  first partitioning/segmentation path using multiple concrete `sorted_heap`
  shards plus harness-side fanout and global rerank.
- Added `scripts/bench_graph_rag_multidepth_segmented_aws.sh` so the same
  segmented benchmark can be run on a constrained remote host without manual
  repo sync/install steps.
- Added `docs/graphrag-segmentation-plan.md` to separate the post-`0.13`
  large-scale architecture from the `0.13` release surface.
- Added larger-scale benchmark notes for:
  - local `1M`-row measured query latency on the synthetic multidepth graph
  - local and AWS `10M`-row build-bound envelopes, where generation/load now
    survive but the first practical frontier remains `sorted_hnsw` build time
  - retained-temp query-only sweeps on the AWS `10M x 32D` cheap-build point,
    which showed that raising `ef_search`/`ann_k` as high as `256/256` still
    does not recover depth-5 quality on the same weak graph
  - a follow-up `1M x 32D` calibration showing that wider query budgets
    (`ann_k=256`, `top_k=32`) can recover `96.9% hit@k` on smaller graphs even
    with a cheaper build
  - a stronger AWS `10M x 32D` falsifier showing that even exact heap seeds
    still return `0.0% / 0.0%` at `ann_k=256`, `top_k=32`, so the remaining
    problem there is the low-dimensional scale contract, not just HNSW build
    quality
  - a local `1M x 64D` calibration showing the same widened contract reaches
    `65.6% hit@1 / 96.9% hit@k` and ANN matches exact seeds there
  - a follow-up `10M x 64D` allocator diagnosis showing the failure came from
    the old contiguous local L0 scan-cache slabs, not from HNSW graph build
    itself
  - a chunked local scan-cache fix that replaces giant local `l0_neighbors`
    and `sq8_data` allocations with page-backed storage for build seeding and
    `shnsw_load_cache()`
  - a constrained-memory AWS `10M x 64D` monolithic rerun on the same
    `4 vCPU / 8 GiB` host with `sorted_hnsw.build_sq8 = on` and
    `hop_weight = 0.05` that now completes:
    - `load_data`: `787.809 s`
    - `build_indexes`: `846.795 s`
  - the first retained query-only pass on that exact built graph showing that
    the monolithic path is now viable but still not the final speed story:
    - depth 1 unified GraphRAG: `840.607 ms`, `100.0% / 100.0%`
    - depth 5 unified GraphRAG: `2084.155 ms`, `75.0% / 100.0%`
    - depth-2+ quality stayed aligned with the SQL baseline, but latency
      remained about `2x` slower than the SQL path baseline
  - so the current `10M x 64D` frontier is no longer build survival or
    quality drift; it is monolithic query cost, which pushes the next scale
    branch toward segmentation + pruning
  - a loader fast path for `sorted_heap_only` multidepth runs that copies
    directly into `facts_sh` before `sorted_heap_compact(...)` instead of
    staging through `facts_heap`; bounded local checks held the same compacted
    depth-5 quality/latency while improving ingest by about `10%`
    (`6.321 s` -> `5.638 s` at `200K` rows, `31.392 s` -> `28.231 s` at
    `1M` rows)
  - a new multidepth harness knob `--post-load-op compact|merge|none` to
    compare post-load maintenance strategies on the same synthetic graph
  - bounded local evidence that keeps `compact` as the default:
    `none` is much slower at query time, while `merge` is viable but does not
    materially beat `compact` on the larger `1M` load point (`28.142 s`
    versus `28.108 s`)
  - an opt-in stage-breakdown path `--report-stage-stats` for the multidepth
    harness, backed by `sorted_heap_graph_rag_stats()`
  - a local `1M x 64D` lower-hop stage diagnosis showing that the widened
    multihop path is ANN-bound, not expansion-bound: at depth 5 the unified
    path took `110.507 ms` end-to-end, of which about `109.178 ms` was ANN,
    `0.691 ms` expansion, and `0.011 ms` rerank
  - the first local segmented `1M x 64D` GraphRAG point (`8` shards,
    `build_sq8=on`) showing:
    - all-shard fanout preserves quality but is slower than the monolith
      (`87.677 ms` vs `50.104 ms` at depth 1, `142.472 ms` vs `121.524 ms`
      at depth 5)
    - exact routing to the owning shard is the real partitioning win
      (`10.574 ms` at depth 1, `16.822 ms` at depth 5, stable `100.0% /
      100.0%`)
    - so the next scale contract must be "segmentation + pruning", not just
      "more shards"
  - the first full AWS segmented `10M x 64D` rerun on the same constrained
    `4 vCPU / 8 GiB` host using streamed shard load and `build_sq8=on`:
    - `generate_csv`: `0.000 s`
    - `load_data`: `500.474 s`
    - `build_indexes`: `784.778 s`
    - `route=all` matched the old monolithic query envelope almost exactly
      (`898.440 ms` at depth 1, `2093.652 ms` at depth 5)
    - `route=exact` was the real scale win
      (`126.057 ms` at depth 1, `258.766 ms` at depth 5, stable
      `100.0% / 100.0%` at depth 5)
    - so the constrained-memory large-scale direction is now much clearer:
      productize segmented routing, not broad all-shard fanout
  - the first SQL-level segmented reference path:
    - added `sorted_heap_graph_rag_segmented(regclass[], ...)`
    - it executes `sorted_heap_graph_rag(...)` across a caller-supplied shard
      set and merges shard-local top-k rows in SQL
    - local segmented smoke confirmed the SQL merge path matches the older
      Python merge path on quality/row counts with similar latency
    - routing/pruning still stays outside the extension for now; this wrapper
      only productizes fanout/merge
  - the first metadata-driven routed GraphRAG reference path:
    - added `sorted_heap_graph_segment_register(...)`,
      `sorted_heap_graph_segment_config(...)`,
      `sorted_heap_graph_segment_resolve(...)`, and
      `sorted_heap_graph_segment_unregister(...)`
    - added `sorted_heap_graph_rag_routed(...)` on top of the segmented
      wrapper
    - this beta surface lets callers register shard ranges once and then
      route by a supplied `int8` key before segmented GraphRAG fanout/merge
    - local routed smoke showed the routed path matches exact-route segmented
      SQL quality/row counts with only small extra lookup overhead
  - the exact-key routed companion for tenant / KB style routing:
    - added `sorted_heap_graph_exact_register(...)`,
      `sorted_heap_graph_exact_config(...)`,
      `sorted_heap_graph_exact_resolve(...)`, and
      `sorted_heap_graph_exact_unregister(...)`
    - added `sorted_heap_graph_rag_routed_exact(...)`
    - local exact-key smoke stayed aligned with the exact-route segmented SQL
      merge path (`0.202 ms` vs `0.183 ms` at depth 5, both `100.0% / 100.0%`)
  - the first richer shard-group filter on top of routed segmentation:
    - both range-routed and exact-key routed registries now accept an optional
      `segment_group` label
    - both config/resolve functions now accept optional `segment_groups text[]`
      filters
    - both routed wrappers now accept optional `segment_groups := ARRAY[...]`
      to narrow candidate shards before segmented GraphRAG fanout/merge
    - when `segment_groups` is present, its array order now becomes the shard
      preference order before bounded fanout is applied
    - this is the first beta surface for hot/sealed or relation-family shard
      pruning without changing the GraphRAG scoring contract
  - the first registry-backed policy layer for shard-group preference:
    - added `sorted_heap_graph_route_policy_register(...)`,
      `sorted_heap_graph_route_policy_config(...)`,
      `sorted_heap_graph_route_policy_groups(...)`, and
      `sorted_heap_graph_route_policy_unregister(...)`
    - added `sorted_heap_graph_rag_routed_policy(...)` and
      `sorted_heap_graph_rag_routed_exact_policy(...)`
    - this keeps hot/sealed preference in route metadata instead of repeating
      raw `segment_groups := ARRAY[...]` literals in every query
  - the first second routing dimension on top of that policy layer:
    - both range-routed and exact-key shard registries now accept an optional
      `relation_family` label
    - both config/resolve functions now accept optional
      `relation_family := ...` filtering
    - both raw and policy-backed routed wrappers now accept optional
      `relation_family := ...` to narrow candidate shards after route
      resolution but before segmented GraphRAG fanout/merge
    - regression coverage now proves route+family and route+policy+family
      filtering for both range and exact-key routing
  - the first route-profile convenience layer on top of that:
    - added `sorted_heap_graph_route_profile_register(...)`,
      `sorted_heap_graph_route_profile_config(...)`,
      `sorted_heap_graph_route_profile_resolve(...)`, and
      `sorted_heap_graph_route_profile_unregister(...)`
    - added `sorted_heap_graph_rag_routed_profile(...)` and
      `sorted_heap_graph_rag_routed_exact_profile(...)`
    - this now stores either:
      - `policy_name + relation_family + fanout_limit`, or
      - inline `segment_groups + relation_family + fanout_limit`
    - so one route profile can be self-contained without a separate
      `sorted_heap_graph_route_policy_registry` row
    - regression coverage now proves both profile-backed wrappers match the
      existing sealed/right routed baselines
  - the first default-profile operator layer on top of routed profiles:
    - added `sorted_heap_graph_route_default_register(...)`,
      `sorted_heap_graph_route_default_config(...)`,
      `sorted_heap_graph_route_default_resolve(...)`, and
      `sorted_heap_graph_route_default_unregister(...)`
    - added `sorted_heap_graph_rag_routed_default(...)` and
      `sorted_heap_graph_rag_routed_exact_default(...)`
    - this binds one default profile per route so callers no longer need to
      pass `profile_name` on every query
    - regression coverage now proves both default-backed wrappers match the
      same sealed/right routed baselines as the explicit profile paths
  - a shared shard-metadata cleanup under the routed beta surface:
    - added `sorted_heap_graph_segment_meta_register(...)`,
      `sorted_heap_graph_segment_meta_config(...)`, and
      `sorted_heap_graph_segment_meta_unregister(...)`
    - range-routed and exact-key routed config/resolve paths now fall back to
      shared per-shard `segment_group` / `relation_family` metadata when the
      route row leaves those labels `NULL`
    - row-local routed metadata still overrides shared shard metadata when
      both are present
    - regression coverage now proves both routed wrappers work when those
      labels live only in the shared shard-metadata registry
  - the first multi-valued shard-label filter on top of that:
    - shared shard metadata now also accepts optional `segment_labels text[]`
    - range/exact config, catalog, and resolve functions now accept optional
      `segment_labels := ARRAY[...]` filters
    - raw, policy-backed, profile-backed, and default-backed routed wrappers
      now propagate that filter without changing GraphRAG scoring semantics
    - route profiles and route/default catalogs now expose profile-level and
      default effective `segment_labels`
    - regression coverage now proves label-based pruning through the shared
      shard-metadata path for both range and exact-key routing
  - a first operator-facing shard catalog on top of that:
    - added `sorted_heap_graph_segment_catalog(...)` and
      `sorted_heap_graph_exact_catalog(...)`
    - both show route-local metadata, shared shard metadata, effective
      resolved metadata, optional shared/effective `segment_labels`, and
      per-column source markers (`route|shared|unset`)
    - this is introspection-only and does not change the routed GraphRAG
      execution contract
  - a first operator-facing route-profile catalog on top of that:
    - added `sorted_heap_graph_route_profile_catalog(...)`
    - it shows profile-local `policy_name`, inline `segment_groups`,
      policy-backed `segment_groups`, effective group order,
      `segment_groups_source` (`inline|policy|unset`), `relation_family`,
      `fanout_limit`, optional profile-level `segment_labels`, and whether the
      profile is the current route default
    - this is introspection-only and does not change the routed GraphRAG
      execution contract
  - a first route-level operator summary on top of that:
    - added `sorted_heap_graph_route_catalog(...)`
    - it shows one row per route with range-shard count, exact-binding count,
      policy/profile counts, and the effective default-profile contract,
      including default `segment_labels`
    - this is introspection-only and does not change the routed GraphRAG
      execution contract

### sorted_hnsw build optimization

- Fixed a real build-time memory-safety bug in `src/hnsw_build.c`: reverse
  link insertion intentionally overflows a neighbor list by one entry before
  `shrink_connections()` prunes it, but the in-memory neighbor arrays were
  previously allocated to only `max_nbrs` slots.
- The build now allocates `max_nbrs + 1` slots for those transient reverse
  inserts, removing the out-of-bounds write.
- Local reproducer after the fix:
  - `40K` pairs / `200K` rows / `64D` / `m=16` / `ef_construction=64`
  - default contract `hop_weight=0.15`: `5/5` build-only passes
  - lowered-hop contract `hop_weight=0.05`: `3/3` build-only passes
- Removed the per-search `visited[]` allocation/zeroing from the hot HNSW
  build loop in `src/hnsw_build.c` and replaced it with a reusable visit-mark
  array.
- On the local `500K x 32D` diagnostic point (`m=8`, `ef_construction=8`),
  that reduced total `CREATE INDEX` time from about `18.1-18.7 s` to
  `2.8-3.0 s`, with the isolated graph-construction phase dropping from about
  `18.27 s` to `2.42-2.59 s`.
- With that optimization in place, the AWS `10M x 32D` cheap-build scale run
  progressed from "stuck in CREATE INDEX" to the first real `10M` query pass.

### GraphRAG concurrent online-operation hardening

- Added `scripts/test_graph_rag_concurrent.sh` and
  `make test-graphrag-concurrent` to verify registered alias-schema fact
  graphs under:
  - concurrent `INSERT` / `UPDATE` / `DELETE`
  - concurrent GraphRAG queries
  - concurrent `sorted_hnsw` KNN queries
  - `sorted_heap_compact_online(...)`
  - `sorted_heap_merge_online(...)`
- The new harness verifies that:
  - GraphRAG alias mappings remain registered
  - the deterministic helper signature remains stable across online operations
  - the unified GraphRAG wrapper remains callable and non-empty
  - `sorted_hnsw` indexes stay valid and usable
  - backend-local GraphRAG observability still reports non-empty stage stats

### GraphRAG larger real-corpus verification

- Refreshed the `0.13` GraphRAG plan and benchmark docs with a larger in-repo
  `cogniformerus` transfer gate.
- Verified that the old tiny-budget code-corpus point (`top_k=4`) drifts on
  the full `183`-file `cogniformerus` repository to about `87%` keyword
  coverage and `66.7%` full hits.
- Verified that increasing only the final result budget to `top_k=8` restores
  repeated-build stable `100.0% / 100.0%` on that larger in-repo Crystal
  corpus for both:
  - generic `prompt_summary_snippet_py`
  - code-aware `prompt_symbol_summary_snippet_py`
- Added mixed-language code-corpus support to the benchmark harness via:
  - JSON question fixtures
  - configurable source extensions
  - quoted C/C++ include-edge extraction
- Added the first real `~/Projects/C` adversary gate on `pycdc` using
  `scripts/fixtures/graph_rag_pycdc_questions.json`.
- Verified on `pycdc` that:
  - the fast generic point is repeated-build stable but partial
    (`90.0% / 60.0%`)
  - the code-aware helper-backed compact include rescue is repeated-build
    stable at `100.0% / 100.0%`
- Added the first archive-side adversary gate on `~/SrcArchives/apple/ninja/src`
  using `scripts/fixtures/graph_rag_ninja_questions.json`.
- Verified on `ninja/src` that:
  - the plain generic `prompt_summary_snippet_py` path is repeated-build stable
    at `100.0% / 100.0%` once the final result budget is raised to `top_k=12`
  - the code-aware `prompt_summary_snippet_py` path remains partial
    (`85.0% / 80.0%`) on the same corpus
- The scoped `0.13` larger real-corpus gate now spans:
  - `~/Projects/Crystal`
  - `~/Projects/C`
  - `~/SrcArchives`

## 0.12.0 (2026-03-26)

### Release documentation pass

- Public docs now split the surface into:
  - **stable**: `sorted_heap` table AM and `sorted_hnsw` Index AM
  - **beta**: GraphRAG helper/wrapper API
  - **legacy/manual**: IVF-PQ and sidecar HNSW paths
- README performance summary was narrowed to representative rows and had the
  stale narrow-range comparison removed.
- README, `docs/index.md`, `docs/vector-search.md`, and `docs/limitations.md`
  now document the current `sorted_hnsw` ordered-scan contract:
  base-relation `ORDER BY embedding <=> query LIMIT k`, with explicit notes
  about `LIMIT`, `ef_search`, and filtered-query caveats.
- `docs/api.md` now includes:
  - `sorted_hnsw.ef_search`
  - `sorted_hnsw.sq8`
  - stable `sorted_hnsw` usage examples
  - beta GraphRAG function reference and usage examples
- `docs/benchmarks.md` now labels GraphRAG benchmark sections as beta-facing.

## 0.10.0 (2026-03-14)

Documentation release: comprehensive rewrite of README with use-case examples,
updated benchmarks, and usage guides. No SQL or C changes from 0.9.15.

## 0.9.15 (2026-03-13)

### Scan planner fixes

- **Prepared-mode OLTP cliff fix**: Path B cost estimator now includes
  uncovered tail pages (pages beyond zone map entries created by UPDATEs).
  Previously the generic plan estimated 1 block but scanned 90+, keeping
  Custom Scan over Index Scan. Mixed OLTP: 56 → 28K tps.
- **DML planning skip**: `set_rel_pathlist` hook bails out immediately for
  UPDATE/DELETE on the result relation, avoiding bounds extraction and range
  computation overhead. UPDATE +10%, DELETE+INSERT reaches heap parity.
- **SCAN-1 regression test**: verifies prepared statement generic plan uses
  Index Scan (not Custom Scan) after UPDATEs create uncovered tail pages.

### UPDATE path optimization

- **Remove `slot_getallattrs` from UPDATE/INSERT zone map path**:
  `zonemap_update_entry` only reads PK columns via `slot_getattr` (lazy
  deform). The prior `slot_getallattrs` call unnecessarily materialized all
  columns including wide svec vectors on every UPDATE/INSERT. Removing it
  eliminates ~530 bytes of needless deform per tuple for svec(128) tables.
  UPDATE vec col: 74% → 102% (parity). Mixed OLTP: 42% → 83%.
- **Lazy update maintenance** (`sorted_heap.lazy_update = on`): opt-in mode
  that skips per-UPDATE zone map maintenance. First UPDATE on a covered page
  clears `SHM_FLAG_ZONEMAP_VALID` on disk; planner falls back to Index Scan.
  INSERT keeps eager maintenance. Compact/merge restores zone map pruning.
  UPDATE non-vec: 46% → 100%. Mixed OLTP: 83% → 97%.
- **SCAN-2 regression test**: verifies lazy mode invalidation → Index Scan
  fallback → correct data → compact restores Custom Scan pruning.

### CRUD performance contract (500K rows, svec(128), prepared mode)

| Operation | eager / heap | lazy / heap | Notes |
|-----------|-------------|-------------|-------|
| SELECT PK | 85% | 85% | Index Scan via btree |
| SELECT range 1000 | 97% | — | Custom Scan pruning (eager only) |
| Bulk INSERT | 100% | 100% | Always eager |
| DELETE + INSERT | 63% | 63% | INSERT always eager |
| UPDATE non-vec | 46% | **100%** | Lazy skips zone map flush |
| UPDATE vec col | 102% | **100%** | Parity both modes |
| Mixed OLTP | 83% | **97%** | Near-parity with lazy |

Eager mode (default) maintains zone maps on every UPDATE for scan pruning.
Lazy mode (`sorted_heap.lazy_update = on`) trades scan pruning for UPDATE
parity with heap. Compact/merge restores pruning. Recommended for
write-heavy workloads where point lookups use Index Scan anyway.

### HNSW sidecar search (`svec_hnsw_scan`)

- **7-arg interface**: added `rerank1_topk` parameter for optional dense r1
  pre-filter via `{prefix}_r1` sidecar table (nid int4 PK, rerank_vec hsvec).
  The 6-arg form continues to work via `PG_NARGS()`.
- **Session-local L0 cache** (`sorted_heap.hnsw_cache_l0 = on`): seqscans L0
  once per session (~95ms build, ~100MB for 103K nodes). Upper levels (L1–L4)
  cached separately (~6MB). OID-based invalidation on DDL.
- **ARM NEON SIMD** for `cosine_distance_f32_f16`: vectorized mixed-precision
  distance with `vld1q_f16` → `vcvt_f32_f16` → `vfmaq_f32`, processing 8 f16
  elements per iteration. Precomputed query self-norm (`cosine_distance_f32_f16_prenorm`)
  eliminates redundant norm_a accumulation in beam search (4 FMAs/iter vs 6).
  Compile-time guard: `__aarch64__ && __ARM_NEON && HSVEC_NATIVE_FP16`.
  Scalar fallback on x86. 27–36% speedup across all operating points.
- **Beam search micro-optimizations** (22–29% additional speedup on cache paths):
  - Visited bitset: replaced `bool[]` (1 byte/node, 103KB) with `uint64[]`
    bitset (1 bit/node, 12.9KB). Fits in L1 cache; faster membership tests.
  - Neighbor prefetch: `__builtin_prefetch` on next unvisited neighbor's cache
    node before computing distance on current. Hides L2/L3 latency for the
    908-byte cache nodes scattered across the 96MB cache array.
  - Cache-only upper search: `hnsw_search_cached()` bypasses
    `hnsw_open_level`/`hnsw_close_level` for warm upper-level caches. Eliminates
    `table_open` + `index_open` + `index_beginscan` overhead per level.
    Upper traversal: 0.17ms → 0.04ms (−74%).
- **Recommended operating points** (103K x 2880-dim, hsvec(384) sketch):
  - Balanced: ef=96 rk=48 → 0.70ms p50, 96.8% recall@10
  - Quality: ef=96 rk=0 → 1.15ms p50, 98.4% recall@10
  - Latency: ef=64 rk=32 → 0.50ms p50, 92.8% recall@10
  Measured with `shared_buffers=512MB` (pod 2Gi), isolated per-config protocol
  (warmup pass + measure pass, no cross-config TOAST sharing). pgvector HNSW
  under same conditions: 1.70ms p50 (ef=64). Cold first-call latency is 2–3×
  higher due to TOAST page faults; `shared_buffers` must be sized to hold the
  rerank working set (~27MB for 50 queries at rk=48).
- **r1 verdict**: marginal on warm pools. At ef>=96 the btree overhead exceeds
  TOAST savings. Useful only in cold-TOAST scenarios.
- **Tests**: ANN-15 (basic HNSW), ANN-15b (bit-identical distances across cache
  states), ANN-16 (r1 sidecar), ANN-17 (r1 absent graceful skip), ANN-18
  (relcache invalidation).
- **Adaptive ef** (`sorted_heap.hnsw_ef_patience`): patience-based early
  termination for L0 beam search. When set to N > 0, the search stops after N
  consecutive node expansions that don't improve the result set. `ef` becomes
  the maximum budget; easy queries converge sooner.
  Not beneficial for rk=0 (quality mode) where TOAST reads scale with ef.
- **Sketch dimension sweep** (103K x 2880-dim Nomic, hsvec 384/512/768):
  recall@10 identical (98.2% at ef=96/rk=48, 93.6% at ef=64/rk=32) across all
  three dimensions; latency within noise (~1.2ms/q). First 384 dims via MRL
  prefix truncation already capture all discriminative power for this model.
  Recall ceiling is in navigation (graph quality / ef budget), not sketch
  fidelity. **384-dim is the correct sketch size.**
- **Builder**: `build_hnsw_graph.py` now accepts `--sketch-dim` (infers from
  data by default). Make target: `make build-hnsw-bench-nomic`.

### Storage: persisted sorted prefix (S2)

- **Meta page v7**: `shm_sorted_prefix_pages` persisted in meta page header.
  Split from `shm_padding` (struct size unchanged, backward compatible: v7
  reader treats v6 as prefix=0).
- **O(1) prefix detection**: `detect_sorted_prefix` returns persisted value
  when available, falls back to O(n) zone map scan for pre-v7 tables.
- **Conservative shrink paths**: `tuple_update` override shrinks prefix when
  update lands on a prefix page. `zonemap_update_entry` detects both
  min-decrease and max-increase-with-overlap on prefix pages.
- **Merge restore**: `rebuild_zonemap_internal` and merge early-exit both
  persist the recomputed prefix.
- **Characterization** (`scripts/bench_s2_prefix.sql`):
  - Append-only: prefix survives 100% at all fillfactors
  - ff=100 + updates: 98% survival (non-HOT goes to tail)
  - ff<100 + updates/recycle: prefix collapses (accepted tradeoff)
  - Merge always restores
- **Tests**: SH22-1 through SH22-5 covering compact, append, recycle insert,
  merge restore, and UPDATE-induced prefix shrink.

## 0.9.14 (2026-03-12)

- `svec_hnsw_scan`: 6-arg hierarchical HNSW search via PG sidecar tables.
  Top-down descent through upper levels, beam search at L0 with hsvec sketches,
  exact rerank via main table TOAST vectors.

## 0.9.13

- `svec_graph_scan`: flat NSW graph search with btree-backed sidecar.
- IVF-PQ three-stage rerank with sketch sidecar (`svec_ann_scan`).
