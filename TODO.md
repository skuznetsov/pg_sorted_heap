# pg_sorted_heap — Project Status

PostgreSQL extension providing the `sorted_heap` table access method:
physically sorted storage with zone-map-based scan pruning.

## Architecture

```
COPY / multi_insert
    → sort batch by PK
    → delegate to heap
    → update per-page zone map in meta page (block 0)

sorted_heap_compact(regclass)
    → CLUSTER-based full rewrite
    → rebuild zone map
    → set SHM_FLAG_ZONEMAP_VALID

SELECT ... WHERE pk_col <op> const
    → set_rel_pathlist_hook
    → extract PK bounds from baserestrictinfo
    → compute block range from zone map
    → CustomPath (SortedHeapScan) with pruned cost
    → heap_setscanlimits(start, nblocks) — physical I/O skip
    → per-block zone map check in ExecCustomScan

Parallel scan (large tables):
    → add_partial_path with parallel_aware=true
    → Gather wraps SortedHeapScan
    → PG parallel table scan API distributes blocks among workers
    → each worker applies per-block zone map pruning independently
```

## Source Files

| File | Lines | Purpose |
|------|------:|---------|
| `sorted_heap.h` | 181 | Meta page layout, zone map structs (v5/v6), SortedHeapRelInfo |
| `sorted_heap.c` | 2452 | Table AM: sorted multi_insert, zone map persistence, compact, merge, vacuum |
| `sorted_heap_scan.c` | 1547 | Custom scan provider: planner hook, ExecScan, parallel scan, multi-col pruning, runtime params |
| `sorted_heap_online.c` | 1053 | Online compact + online merge: trigger, copy, replay, swap |
| `pg_sorted_heap.c` | 1537 | Extension entry point, legacy clustered index AM, GUC registration |
| `sql/pg_sorted_heap.sql` | 2073 | Regression tests (SH1–SH17) |
| `expected/pg_sorted_heap.out` | 3152 | Expected test output |
| `scripts/test_concurrent_online_ops.sh` | 264 | Concurrent DML + online compact/merge (ephemeral cluster) |
| `scripts/test_crash_recovery.sh` | 335 | Crash recovery scenarios (pg_ctl stop -m immediate) |
| `scripts/test_toast_and_concurrent_compact.sh` | 338 | TOAST integrity + concurrent online compact guard |
| `scripts/test_alter_table.sh` | 357 | ALTER TABLE on sorted_heap (ADD/DROP/RENAME/ALTER TYPE/PK, concurrent DDL) |
| `scripts/test_dump_restore.sh` | 176 | pg_dump/restore lifecycle test (data, TOAST, indexes, zone map) |
| `scripts/bench_sorted_heap.sh` | 411 | sorted_heap vs heap+btree vs seqscan comparative benchmark (multi-client) |

## Completed Phases

### Phase 1 — Table AM Skeleton
Basic `sorted_heap` AM that delegates everything to heap.

### Phase 2 — PK Auto-Detection + Sorted Bulk Insert
- Auto-detect PK via `pg_index` catalog scan
- Cache PK info in backend-local hash table (`SortedHeapRelInfo`)
- Sort each `multi_insert` batch by PK columns before delegating to heap
- Result: physically sorted runs within each COPY batch

### Phase 3 — Persistent Zone Maps
- Meta page (block 0) with `SortedHeapMetaPageData` in special space
- Per-page `(min, max)` of first PK column (int2/int4/int8)
- Zone map updated during `multi_insert` via GenericXLog
- Up to 500 entries (8024 bytes, fits in special space)
- `sorted_heap_zonemap_stats()` SQL function for inspection
- Staleness flag (`SHM_FLAG_ZONEMAP_STALE`) for INSERT-after-COPY detection

### Phase 4 — CLUSTER-Based Compaction + Zone Map Rebuild
- `sorted_heap_compact(regclass)` — full table rewrite via `cluster_rel()`
- Rebuilds zone map from scratch after compaction
- `sorted_heap_rebuild_zonemap_sql()` for manual rebuild
- Result: globally sorted table with accurate zone map

### Phase 5 — Scan Pruning via Custom Scan Provider
- `set_rel_pathlist_hook` → `SortedHeapScan` CustomPath
- Extract PK bounds from WHERE clause (=, <, <=, >, >=, BETWEEN)
- Map operator OIDs to btree strategies via `get_op_opfamily_strategy()`
- Compute contiguous block range from zone map overlap
- `heap_setscanlimits()` for physical I/O skip
- Per-block zone map check in ExecCustomScan for non-contiguous pruning
- `SHM_FLAG_ZONEMAP_VALID` flag: cleared on first INSERT after compact,
  set during compact/rebuild — prevents stale pruning
- Uncovered pages (beyond zone map capacity) included in scan unless
  upper bound falls within covered range
- EXPLAIN output: "Zone Map: N of M blocks (pruned P)"

## Benchmark Results

PostgreSQL 18, Apple M-series (12 CPU, 64 GB RAM), zone map v6.
PG config: shared_buffers=4GB, work_mem=256MB, maintenance_work_mem=2GB.

`scripts/bench_sorted_heap.sh` — ephemeral cluster, 3-way comparison:
sorted_heap (zone map), heap+btree (standard), heap seqscan (no index).

### EXPLAIN ANALYZE: Execution Time + Buffer Reads (warm cache)

True query performance without pgbench overhead. Average of 5 runs.

**1M rows**

| Query | sorted_heap | heap+btree | heap seqscan |
|-------|------------|-----------|-------------|
| Point (1 row) | 0.035ms / 1 buf | 0.046ms / 7 bufs | 15.2ms / 6,370 bufs |
| Narrow (100) | 0.043ms / 2 bufs | 0.067ms / 8 bufs | 16.2ms / 6,370 bufs |
| Medium (5K) | 0.434ms / 33 bufs | 0.492ms / 52 bufs | 16.1ms / 6,370 bufs |
| Wide (100K) | 7.5ms / 638 bufs | 8.9ms / 917 bufs | 17.4ms / 6,370 bufs |

**10M rows**

| Query | sorted_heap | heap+btree | heap seqscan |
|-------|------------|-----------|-------------|
| Point (1 row) | 0.034ms / 1 buf | 0.047ms / 7 bufs | 117.9ms / 63,695 bufs |
| Narrow (100) | 0.037ms / 1 buf | 0.062ms / 7 bufs | 130.9ms / 63,695 bufs |
| Medium (5K) | 0.435ms / 32 bufs | 0.549ms / 51 bufs | 131.0ms / 63,695 bufs |
| Wide (100K) | 7.6ms / 638 bufs | 8.8ms / 917 bufs | 131.4ms / 63,695 bufs |

**100M rows**

| Query | sorted_heap | heap+btree | heap seqscan |
|-------|------------|-----------|-------------|
| Point (1 row) | 0.045ms / 1 buf | 0.506ms / 8 bufs | 1,190ms / 519,906 bufs |
| Narrow (100) | 0.166ms / 2 bufs | 0.144ms / 9 bufs | 1,325ms / 520,782 bufs |
| Medium (5K) | 0.479ms / 38 bufs | 0.812ms / 58 bufs | 1,326ms / 519,857 bufs |
| Wide (100K) | 7.9ms / 737 bufs | 10.1ms / 1,017 bufs | 1,405ms / 518,896 bufs |

sorted_heap reads fewer blocks than btree for all query types. Zone map
prunes to exact block range; btree traverses 3-4 index pages per lookup.
At 100M rows, sorted_heap point query reads 1 buffer vs 8 for btree.

### INSERT (rows/sec)

| Scale | sorted_heap | heap+btree | heap (no idx) | compact |
|------:|------------:|-----------:|--------------:|--------:|
| 1M | 923K | 961K | 1.91M | 0.3s |
| 10M | 908K | 901K | 1.65M | 3.1s |
| 100M | 840K | 1.22M | 2.22M | 41.3s |

### Table Size

| Scale | sorted_heap | heap+btree (table + index) | heap (no idx) |
|------:|------------:|---------------------------:|--------------:|
| 1M | 71 MB | 71 MB (50 + 21) | 50 MB |
| 10M | 714 MB | 712 MB (498 + 214) | 498 MB |
| 100M | 7.8 GB | 7.8 GB (5.7 + 2.1) | 5.7 GB |

Sizes unchanged — same data, same row width.

### pgbench Throughput (queries/sec, 1 client, 10s)

Includes pgbench overhead (connection, planning). Useful for comparing
relative throughput under sustained load, not absolute query latency.

**Simple mode** (`-M simple`): each query parsed, planned, and executed separately.

| Query | 1M sh / btree | 10M sh / btree | 100M sh / btree |
|-------|-------------:|--------------:|---------------:|
| Point (1 row) | 28.4K / 38.0K | 29.1K / 41.4K | 18.7K / 4.6K |
| Narrow (100) | 19.6K / 24.4K | 21.8K / 27.6K | 7.1K / 5.5K |
| Medium (5K) | 3.1K / 3.7K | 3.4K / 4.8K | 2.1K / 1.6K |
| Wide (100K) | 198 / 290 | 200 / 286 | 163 / 144 |

**Prepared mode** (`-M prepared`): query planned once, re-executed with parameters.

| Query | 1M sh / btree | 10M sh / btree | 100M sh / btree |
|-------|-------------:|--------------:|---------------:|
| Point (1 row) | 46.9K / 59.4K | 46.5K / 58.0K | 32.6K / 43.6K |
| Narrow (100) | 22.3K / 29.1K | 22.5K / 28.8K | 17.9K / 18.1K |
| Medium (5K) | 3.4K / 5.1K | 3.4K / 4.8K | 2.4K / 2.4K |
| Wide (100K) | 295 / 289 | 293 / 286 | 168 / 157 |

### Observations

- **EXPLAIN ANALYZE** shows sorted_heap reads fewer blocks than btree
  at every selectivity level. At 10M rows: point query reads 1 block
  (vs 7 for btree, 63,695 for seqscan). At 100M: 1 block vs 8 for btree,
  519,906 for seqscan. Zone map prunes to exact contiguous block range.
- **Prepared mode** — runtime parameter resolution enables generic plans for
  sorted_heap. Point query TPS at 10M: 46.5K prepared vs 29.1K simple (+60%).
  At 100M: 32.6K prepared vs 18.7K simple (+74%). Narrow/medium/wide reach
  parity with btree at 100M. Wide (100K row) queries show sorted_heap
  slightly ahead at all scales since execution cost dominates.
- **Simple mode** — at 1M/10M btree shows higher TPS because per-query
  planning overhead dominates when execution time is sub-millisecond.
  At 100M sorted_heap wins all query types: point 4x (18.7K vs 4.6K),
  narrow +29%, medium +28%, wide +13% — btree's 2.1 GB index exceeds
  shared_buffers, while sorted_heap's zone map lookup is O(1) in memory.
- **INSERT** — comparable at scale; heap without index is fastest (~2M/s)
  since there is no index maintenance or batch sorting overhead.
- **Storage** — nearly identical (sorted_heap trades btree index for
  meta+overflow pages). Heap without index is smallest (no index overhead).

## Known Limitations

- Zone map capacity: unlimited (v6 format). 250 entries in meta page +
  overflow pages linked via next_block chain (254 entries/page).
- Zone map tracks first two PK columns (col1 + col2). Supported types:
  int2/int4/int8, timestamp, timestamptz, date, uuid, text/varchar
  (text requires `COLLATE "C"`). UUID/text use lossy first-8-byte mapping
  (conservative pruning for values sharing long prefixes).
- Online compact/merge (`_online` variants) not supported for UUID/text/varchar
  PKs due to lossy int64 hash key. Use regular compact/merge instead.
- Single-row INSERT into a covered page updates zone map in-place
  (preserving scan pruning). INSERT into an uncovered page invalidates
  scan pruning until next compact.
- TOAST: sorted_heap delegates TOAST storage entirely to heap. Large
  values (>2KB) survive all rewrite paths (compact, merge, online
  compact, online merge). Tested with 4KB payloads, 25K rows.
- `sorted_heap_compact()` acquires AccessExclusiveLock — blocks all
  concurrent reads and writes. Use `sorted_heap_compact_online()` for
  non-blocking compaction.
- Concurrent online compact/merge on the same table: second session
  receives "relation already exists" error (implicit guard via log table
  name `_sh_compact_log_<oid>`). First session completes normally;
  re-run succeeds after cleanup. No explicit advisory lock needed.
- ALTER TABLE: ADD COLUMN, DROP COLUMN (non-PK), RENAME COLUMN (including PK),
  ALTER TYPE (non-PK, triggers table rewrite) all work correctly. Zone map
  survives via relcache invalidation callback (`pk_probed=false` triggers
  re-probe). PostgreSQL preserves attnums on DROP COLUMN. Table rewrite
  (ALTER TYPE) creates new meta page; compact restores zone map. DROP PK
  disables pruning; re-ADD PK + compact re-enables it. Tested: 33 checks.
- `heap_setscanlimits()` only supports contiguous block ranges.
  Non-contiguous pruning handled per-block in ExecCustomScan (still reads
  pages, but skips tuple processing).

### Phase 6 — Production Hardening
- GUC `sorted_heap.enable_scan_pruning` (default on)
- Timestamp/date PK support in zone map
- INSERT-after-compact zone map updates
- EXPLAIN ANALYZE counters (Scanned Blocks, Pruned Blocks)
- Shared memory scan statistics (`sorted_heap_scan_stats()`)

### Phase 7 — Online Compact
- `sorted_heap_compact_online(regclass)` — PROCEDURE (use with CALL)
- Trigger-based change capture (pg_repack-style)
- Phase 1: UNLOGGED log table + AFTER ROW trigger (committed mid-call)
- Phase 2: Index scan in PK order → bulk copy to new table
  (ShareUpdateExclusiveLock — concurrent reads and writes allowed)
- Phase 3: Replay log entries, up to 10 convergence passes
- AccessExclusiveLock only for brief final filenode swap
- PK→TID hash table for O(1) replay lookups
- Zone map rebuilt on new table before swap

### Phase 8 — Multi-Column Zone Map
- Zone map v5: 32-byte entries (col1 + col2 min/max per page)
- Composite PK `(a, b)` prunes on both columns (AND semantics)
- v4 backward compatibility: load expands 16→32 byte entries,
  flush writes v4 format, compact/rebuild upgrades to v5
- Supported col2 types: int2/int4/int8, timestamp, timestamptz, date
- Non-trackable col2 degrades gracefully (col1-only pruning)
- Meta page capacity: 250 entries (v5) vs 500 (v4)
- Overflow capacity: 255 entries/page (v5) vs 509 (v4)

### Phase 9 — Parallel Custom Scan
- Parallel-aware SortedHeapScan via PostgreSQL's parallel table scan API
- Planner adds partial path (`add_partial_path`) when `compute_parallel_worker`
  returns > 0 workers; Gather node wraps parallel SortedHeapScan
- DSM callbacks: `EstimateDSM`, `InitializeDSM`, `ReInitializeDSM`,
  `InitializeWorker` — use `table_parallelscan_initialize` / `table_beginscan_parallel`
- Workers coordinate block distribution through PG's native mechanism;
  each worker applies per-block zone map pruning independently
- Fix: `sorted_heap_get_zm_entry()` used hardcoded `SORTED_HEAP_ZONEMAP_CACHE_MAX`
  (500) as cache/overflow boundary — incorrect for v5 format (250 entries).
  Changed to `info->zm_nentries` so overflow lookup adapts to format version.

### Production Hardening (post-Phase 9)
- multi_insert zone map boundary: capped at `SORTED_HEAP_ZONEMAP_MAX` (250)
  to match flush capacity and read path. Previously used `SORTED_HEAP_ZONEMAP_CACHE_MAX`
  (500), causing writes to entries that were never flushed to disk.
- Overflow page bounds check: `overflow_npages` clamped to
  `SORTED_HEAP_OVERFLOW_MAX_PAGES` (32) on both v4 and v5 read paths,
  preventing OOM from corrupted on-disk metadata.
- SH10 test suite: zone map overflow boundary correctness (INSERT into
  overflow range, re-compact verification).

### Phase 10 — Incremental Merge Compaction
- `sorted_heap_merge(regclass)` — two-way merge avoiding full CLUSTER rewrite
- Sorted prefix detection: scan zone map entries for monotonicity
  (`entry[i+1].min >= entry[i].max`); works regardless of `SHM_FLAG_ZONEMAP_VALID`
  since compacted entries remain accurate after subsequent INSERTs
- Stream A: sequential heap scan of sorted prefix (no index needed)
- Stream B: tuplesort of unsorted tail via PG's tuplesort API
- Merge loop: compare PK columns via SortSupport, write winner to new
  relation via `heap->tuple_insert` (bypasses sorted_heap AM sorting)
- Zone map rebuilt on new table via `sorted_heap_rebuild_zonemap_internal`
- Atomic filenode swap via `finish_heap_swap`
- AccessExclusiveLock for entire operation (same as compact)
- Edge cases: empty table → early return; already sorted → early return;
  never compacted (no valid prefix) → full tuplesort fallback;
  zone map doesn't cover trailing pages → uncovered pages go to tail
- SH11 test suite: normal merge with overlapping data, zone map validity,
  scan pruning after merge, re-compact verification, already-sorted,
  never-compacted fallback, empty table

### Phase 11 — Online Merge Compaction
- `sorted_heap_merge_online(regclass)` — PROCEDURE (use with CALL)
- Non-blocking variant of merge using trigger-based change capture
  (same pg_repack-style approach as `sorted_heap_compact_online`)
- Phase 0b: Detect prefix under ShareUpdateExclusiveLock; early exit
  for empty/already-sorted tables (no SPI/log table overhead)
- Phase 1: UNLOGGED log table + AFTER ROW trigger (committed mid-call)
- Phase 2: Prefix seq scan + tail tuplesort merge → new table under
  ShareUpdateExclusiveLock (concurrent reads and writes allowed)
- PK→TID hash populated during merge for O(1) replay lookups
- Phase 2b: Replay loop (up to 10 convergence passes)
- Phase 3: AccessExclusiveLock only for brief final replay + swap
- Re-detect prefix in Phase 2 under lock eliminates TOCTOU race
- Exported `sorted_heap_detect_sorted_prefix` and `sorted_heap_zonemap_load`
  from sorted_heap.c for cross-file use
- SH12 test suite: normal online merge, physical sort verification,
  zone map validity, scan pruning, already-sorted, empty table,
  never-compacted fallback

### Phase 12 — Zone Map Support for UUID and Text Types
- UUID PK: first 8 bytes as big-endian uint64, sign-flipped to int64
  (lossy but order-preserving; UUIDs sharing first 8 bytes collapse)
- TEXT/VARCHAR PK: first 8 bytes zero-padded, same uint64→int64 conversion.
  Requires `COLLATE "C"` (byte order = sort order). Non-C collation
  degrades gracefully (`zm_usable = false`, no zone map, no scan pruning)
- Zone map rebuild probe replaced: `Int32GetDatum(0)` probe caused NULL
  dereference for pointer-based types; now uses direct typid switch
- Online compact/merge blocked for UUID/text/varchar PKs — the int8
  log table and PK→TID hash use lossy int64 representation, causing
  hash collisions and incorrect replay
- Collation check uses `C_COLLATION_OID` from `pg_collation_d.h`
  (avoids ICU header dependency from `pg_locale.h`)
- SH13 test suite: UUID zone map + scan pruning, text/C zone map +
  scan pruning, online compact/merge blocked, varchar zone map

### Phase 13 — Production Hardening: Unlimited Zone Map, Autovacuum Integration

**Phase 13A: Unlimited Zone Map Capacity (v6 format)**
- Removed 65 MB / 8,410-page zone map capacity limit
- v6 overflow page format: 16-byte header + 254 entries + `shmo_next_block` pointer
- Linked list of overflow pages: first 32 referenced from meta page, then
  chained via `shmo_next_block` (no hard cap on overflow pages)
- Dynamic `repalloc`-based entry allocation in rebuild (no fixed-size buffer)
- v5 backward compatibility: `SortedHeapOverflowPageDataV5` struct preserved,
  version check in `sorted_heap_zonemap_load()` handles both formats
- `SORTED_HEAP_VERSION` bumped to 6; `zm_overflow_npages` widened to uint32
- SH14 test suite: 170K wide rows (>9,400 pages), overflow chain verification,
  scan pruning on high page numbers

**Phase 13B: WAL Audit**
- All zone map write paths verified crash-safe (GenericXLog or `log_newpage`)
- No code changes required

**Phase 13C: Autovacuum Zone Map Rebuild**
- Override `relation_vacuum` in sorted_heap AM routine
- After heap vacuum completes, check `SHM_FLAG_ZONEMAP_VALID` flag
- Rebuild zone map automatically when flag is not set
- GUC `sorted_heap.vacuum_rebuild_zonemap` (default on) for control
- Safety check: skip rebuild when relation has 0 data pages (post-truncate)
- SH15 test suite: vacuum rebuild, GUC on/off, scan pruning after rebuild

**CustomScan ExecScan fix (PG 18 compatibility)**
- PG 18 changed `ExecCustomScan` to call `methods->ExecCustomScan` directly
  (no longer wraps in `ExecScan`), so projection/qual evaluation must be
  handled by the extension
- Restructured to split scan logic: `sorted_heap_scan_next()` (access method
  with zone map pruning) + `sorted_heap_scan_recheck()` (EPQ), wrapped by
  `ExecScan()` which handles quals and projection
- Fixed SEGFAULT with WindowAgg/ModifyTable plans and protocol field count
  errors with direct Limit→CustomScan plans

**SH16: Secondary Index Preservation**
- SH16 test suite: secondary indexes (btree on non-PK columns) survive all
  four rewrite paths: compact, merge, online compact, online merge
- Index build delegated to heap via `rd_tableam` swap trick
- Compact uses `cluster_rel()` (rebuilds indexes); merge/online variants
  use `finish_heap_swap()` (updates index filenodes)
- DML (UPDATE/DELETE) with secondary indexes verified
- UNIQUE constraints on secondary indexes enforced correctly after
  compact, merge, online compact, and online merge

**Bug Fix: NULL tg_newtuple in Online Compact Trigger**
- `sorted_heap_compact_trigger()` used `tg_newtuple` for INSERT events,
  but PostgreSQL sets `tg_newtuple = NULL` for INSERT (and DELETE) triggers.
  Only UPDATE populates `tg_newtuple` with the new row; INSERT and DELETE
  store the affected row in `tg_trigtuple`.
- Caused SIGSEGV (signal 11) on every concurrent INSERT during online
  compact/merge. Also crashed on every INSERT after crash recovery
  (trigger persists, log table reset from UNLOGGED INIT fork).
- Fix: use `tg_newtuple` only for UPDATE; use `tg_trigtuple` for INSERT
  and DELETE.
- Added defensive `nblocks` check in `sorted_heap_tuple_insert` before
  reading meta page for zone map invalidation.

**Concurrent Workload Tests** (`scripts/test_concurrent_online_ops.sh`)
- Ephemeral PG cluster with 50K-row sorted_heap table + secondary index
- Test A: online compact with 4 background DML workers
  (INSERT / UPDATE / DELETE / SELECT) running for 15 seconds
- Test B: online merge with same 4 workers
- 6 checks: count > 0, no duplicate PKs, secondary index consistency
  (each verified after compact and after merge)
- Workers: ~400+ ops each, no crashes, no data corruption

**Crash Recovery Tests** (`scripts/test_crash_recovery.sh`)
- 4 scenarios, each in its own ephemeral PG cluster:
  1. Crash during COPY — pre-crash committed data survives, no dup PKs
  2. Crash after compact — zone map persists via GenericXLog WAL replay,
     scan pruning works after recovery
  3. Crash during zone map rebuild — table accessible, re-rebuild succeeds
  4. Crash during online compact Phase 2 — original table intact, zone map
     preserved, no orphaned log tables, compact succeeds post-recovery
- 15 checks, all pass. `checkpoint_timeout = 30s` (PG 18 minimum).

**TOAST Data Integrity Tests** (`scripts/test_toast_and_concurrent_compact.sh`, Area 1)
- 4KB payload (`repeat('x', 4000)`) forces TOAST storage (>2KB threshold)
- sorted_heap delegates TOAST entirely to heap via `heap->multi_insert`
- Verified through all 5 rewrite paths: COPY, compact, online compact,
  merge, online merge — payload length preserved, no data corruption
- TOAST table existence confirmed via `pg_class.reltoastrelid`
- 15 checks, all pass

**Concurrent Online Compact Guard** (`scripts/test_toast_and_concurrent_compact.sh`, Area 2)
- Log table name uses only relid (`_sh_compact_log_%u`), so a second
  concurrent online operation on the same table hits "already exists"
  error from `CREATE TABLE` — implicit but effective guard
- Tested: compact x2, merge x2, compact vs merge (cross-operation)
- After first session completes and cleans up, re-run succeeds
- No crashes (SIGSEGV/SIGBUS/SIGABRT) in any scenario
- 11 checks, all pass

**ALTER TABLE Tests** (`scripts/test_alter_table.sh`)
- ADD COLUMN (no default + with default), DROP COLUMN (non-PK),
  RENAME COLUMN (non-PK + PK), ALTER TYPE (non-PK, table rewrite)
- INSERT, compact, online compact after all DDL — no crashes
- Secondary index after DDL — consistent with seqscan
- DROP PK → pruning disabled, DML works; re-ADD PK + compact → pruning restored
- 33 checks, all pass

### Runtime Parameter Resolution (Prepared Statements)
- `sorted_heap_extract_bounds()` now accepts both `Const` and `Param` nodes
- Two-path architecture in the planner:
  - **Path A** (all Const): block range computed at plan time (unchanged)
  - **Path B** (has Param): expressions stored in `custom_exprs`, bounds
    deferred to executor via `ExecInitExprList` + `ExecEvalExprSwitchContext`
- `sorted_heap_resolve_runtime_bounds()`: evaluates Param expressions at
  executor startup, merges with Const-only baseline, computes block range
- `sorted_heap_apply_bound()`: shared helper for both plan-time and runtime
  bound application (eliminates ~100 lines of duplicated switch logic)
- Rescan support: re-evaluates runtime bounds on each rescan (NestLoop)
- Cost estimation for Path B uses `clauselist_selectivity()` for generic
  plan adoption after ~5 executions
- EXPLAIN shows "N total blocks (runtime bounds)" for Path B without ANALYZE;
  with ANALYZE shows actual resolved range as usual
- Path A/B detection via `list_length(custom_private)` (2 vs 3 elements)
- SH17 regression tests: point, range, mixed Const+Param prepared queries
  under `force_generic_plan`, correctness cross-checks against non-prepared

## Operational Notes

### pg_dump / pg_restore
Data is restored via COPY → `multi_insert`, which rebuilds physical data but
does not set `SHM_FLAG_ZONEMAP_VALID`. Run `sorted_heap_compact()` (or
`sorted_heap_merge()`) after restore to re-enable scan pruning. Tested:
10 checks (data integrity, TOAST, secondary indexes, zone map rebuild).

### Logical Replication
Subscribers receive changes via the apply worker, which uses standard DML.
Initial table sync uses COPY. Same as pg_dump: run compact after initial sync
to build zone map on the subscriber.

### Connection Poolers
Prepared statement mode (PgBouncer `pool_mode=transaction`) works correctly —
runtime parameter resolution handles generic plans. No special configuration
needed.

### pg_upgrade
Untested. Expected to work: pg_upgrade copies data files as-is (meta page and
overflow pages are standard 8KB blocks), and the extension .so is loaded on
startup via `shared_preload_libraries` (not needed for sorted_heap — it's loaded
on first use via `CREATE EXTENSION`).

### Extension Upgrade Path
No `pg_sorted_heap--0.1.0--0.2.0.sql` upgrade script exists. Currently requires
`DROP EXTENSION` + `CREATE EXTENSION` for version changes (data preserved in
tables, zone map rebuilt on next compact).

## Vector Search Track

Extending sorted_heap for approximate nearest neighbor (ANN) search.
The key idea: use sorted_heap's zone-map pruning on hash columns
(generated from vector embeddings) for coarse filtering, then PQ codes
for fast distance estimation without TOAST reads.

### Source Files (Vector)

| File | Lines | Purpose |
|------|------:|---------|
| `svec.h` | 35 | Svec struct (pgvector-compatible layout), macros |
| `svec.c` | 280 | svec type I/O, typmod, cosine distance operator `<=>` |
| `sorted_vector_hash.c` | 590 | SimHash, VQ, RVQ, RPVQ, CVQ hash functions |
| `pq.h` | 36 | PQ constants (PQ_MAX_M, PQ_KSUB=256), function declarations |
| `pq.c` | 860 | PQ training (k-means), encode, split ADC (distance_table + adc_lookup) |

### Vector Hash Functions (SimHash, VQ, RVQ, RPVQ, CVQ)

Five hash functions for mapping high-dimensional vectors to int2 bucket IDs,
used as generated columns for zone-map-based scan pruning:

- `sorted_vector_hash(svec, seed)` — SimHash (random hyperplanes, Hamming-like)
- `sorted_vector_vq(svec, n_centroids, seed)` — Vector Quantization (random centroids)
- `sorted_vector_rvq(svec, n_centroids, seed)` — Residual VQ (two-stage)
- `sorted_vector_rpvq(svec, n_centroids, seed)` — Random Projection + VQ
- `sorted_vector_cvq(svec, ref, n_centroids, seed)` — Composite VQ (diff from reference)
- `sorted_vector_cvq_probe(svec, ref, n_centroids, n_probes, seed)` — Multi-probe CVQ

### svec Type (Own Vector Type)

Custom PostgreSQL vector type replacing pgvector dependency:
- Binary-compatible with pgvector's `vector` struct layout
- Same `[x,y,z]` text I/O format
- Cosine distance operator `<=>`
- `STORAGE = external` for TOAST
- Typmod support: `svec(768)` for dimension checking

### Product Quantization (PQ)

Compact vector encoding for fast ANN search without TOAST reads.

**Training:**
- `svec_pq_train(source_query, M, n_iter, max_samples)` → codebook ID
- k-means clustering on M subvector groups (Ksub=256 centroids each)
- Codebook stored in `_pq_codebook_meta` + `_pq_codebooks` tables
- Backend-local codebook cache for cross-query reuse

**Encoding:**
- `svec_pq_encode(svec, cb_id)` → M-byte bytea PQ code
- For dim=2880, M=180: each vector compressed to 180 bytes (64× reduction)

**Distance computation (split ADC):**
- `svec_pq_distance_table(query, cb_id)` → M×256 float distance table (once per query)
- `svec_pq_adc_lookup(dist_table, code)` → float8 estimated L2² distance (O(M) per candidate)
- Split design avoids recomputing distance table per row (573× speedup over naive per-row)

**Memory context fix:**
SPI's `procCxt` is deleted by `SPI_finish()`. Any `palloc` between `SPI_connect()`
and `SPI_finish()` becomes a dangling pointer. Fixed by allocating long-lived data
(sample_data, indices) in the caller's `func_ctx` via explicit `MemoryContextSwitchTo`.

### ANN Benchmark Results

103K vectors, svec(2880) / halfvec(2880), 1Gi k8s pod, PostgreSQL 18.

**PQ vs pgvector HNSW (20 queries, recall@10):**

| Method | Recall@10 | Avg latency | Index size |
|---|---|---|---|
| Exact brute-force | 10.0/10 | 996 ms | — |
| pgvector HNSW ef=40 | 9.7/10 | 17 ms | 806 MB |
| pgvector HNSW ef=100 | 9.7/10 | 14 ms | 806 MB |
| pgvector HNSW ef=200 | 9.7/10 | 20 ms | 806 MB |
| PQ two-phase pool=200 | 10.0/10 | 60 ms | 27 MB |

**PQ recall by reranking pool size (10 queries):**

| Pool size | Recall@10 |
|---|---|
| 50 | 93% |
| 100 | 99% |
| 200 | 100% |
| 500 | 100% |

**PQ two-phase timing breakdown (single query):**

| Phase | Time | Description |
|---|---|---|
| Distance table | 15.6 ms | Precompute M×256 distances (once per query) |
| ADC scan | 25.5 ms | Scan 103K PQ codes, sort top-200 |
| Exact rerank | 22.8 ms | Detoast + cosine for 200 candidates |
| Total | 63.9 ms | 16× faster than brute-force |

**Key tradeoffs:**
- HNSW: 3-4× faster latency, but 30× larger index (stores full vectors)
- PQ: higher recall (100% vs 97%), 30× smaller index, scales better to large datasets
- At 1M vectors: HNSW ~8 GB vs PQ ~260 MB

### Multi-Hash Filter PoC (Counterproductive)

Tested two-table architecture with multi-hash filter (h1×h2×h3×h4) for
tighter candidate selection. Result: **filter hurts recall without improving
latency** — the problem is TOAST read latency, not candidate count.
PQ approach directly addresses this by eliminating TOAST reads for filtering.

## Possible Future Work

- Index-only scan equivalent using zone map
- Online compact/merge support for UUID/text PKs (requires log format redesign)
- Extension upgrade SQL scripts for version transitions
- pg_upgrade testing with two major PG versions
- IVF-PQ: use SimHash or VQ partitioning + PQ codes for sub-linear scan on large datasets
- Scalar quantization (SQ8/SQ4) as lighter alternative to PQ for lower dimensions
- SIMD-accelerated ADC lookup (NEON/AVX2 for distance table precomputation)
- pgvectorscale DiskANN comparison (requires Rust/PGRX build)
