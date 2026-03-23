# Design: `sorted_hnsw` Shared Scan Cache

## Status

Implemented in:

- `f33d620` `perf: share decoded sorted_hnsw scan cache across backends`
- `27e77a3` `perf: cost sorted_hnsw shared-cache startup separately`

The original upper-level-only payload was a useful falsifier, but it only cut
fresh 50k/384D scans by about 10% and missed the bar. The implemented version
therefore promotes the shared slot to a full decoded immutable scan snapshot.

## Problem

`sorted_hnsw` already keeps a decoded scan cache per backend in
[`src/sorted_hnsw.c`](/Users/sergey/Projects/C/clustered_pg/src/sorted_hnsw.c),
keyed by `{relid, relfilenode, cache_gen}`.

That solves repeated scans inside one backend, but every new backend still pays
to rebuild the same immutable scan-side state:

- SQ8 min/scale arrays
- upper-level adjacency
- upper-level `nid -> index` lookup tables
- scan-cache control structs

The new fixed-graph harness can now measure the gap directly:

- `fresh` mode: one ordered scan per new backend
- `reuse` mode: one warmup query, then repeated scans in a single backend

Representative pre-shared-cache 384D result:

- `fresh avg_ms = 0.5803`
- `reuse avg_ms = 0.3007`

So roughly half of the observed fresh-backend latency is still startup work, not
the search itself.

## Goal

Reduce cross-backend cold-start cost for ordered `sorted_hnsw` scans without
changing search semantics or weakening crash safety.

## Non-goals

- Do not change on-disk page layout.
- Do not make writable-path behavior depend on shared-memory mutation.
- Do not share exact rerank heap/TOAST state.
- Do not start with a general-purpose multi-index cache manager if a smaller
  falsifier can prove or kill the idea first.

## Current architecture

Today each backend does:

1. Read metapage.
2. Read SQ8 auxiliary pages.
3. Decode upper levels into backend-local `palloc()` allocations.
4. Build upper-level lookup tables.
5. Keep L0 decoded lazily per backend.

That means `fresh` mode repeatedly duplicates the same immutable decode work.

## Implemented Phase A: single-slot shared immutable cache

Start with one experimental shared cache slot, effective only when the
extension is loaded via `shared_preload_libraries`.

Why a single slot first:

- it is enough to validate the measured bottleneck on the current benchmark
- it keeps invalidation and memory budgeting simple
- it gives a hard falsifier before building a more complex multi-slot cache

### What to share

Share immutable decoded scan-side data:

- SQ8 mins
- SQ8 scales
- decoded L0 node headers
- decoded L0 neighbor slab
- decoded L0 SQ8 vectors
- upper-level adjacency
- upper-level `nid -> index` arrays
- immutable header fields: `M`, `dim`, `n_nodes`, `entry_nid`, `max_level`,
  `cache_gen`, `relid`, `locator`

Keep these backend-local:

- L0 decoded pages and neighbor arrays
- visited bitsets
- candidate/result scratch
- writable-path insert helpers
- rerank heap/slot state

### Why this split

The upper-level-only split was safe but insufficient. The dominant remaining
fresh-backend tax was the decoded L0 snapshot itself, so the implemented slot
stores the full immutable decoded graph and relies on `cache_gen` invalidation
to keep writable-path updates from reusing stale state.

## Shared memory layout

Use the existing shared-memory hook pattern already present in
[`src/sorted_heap_scan.c`](/Users/sergey/Projects/C/clustered_pg/src/sorted_heap_scan.c).

Add:

- one control struct in main shared memory
- one fixed-size payload arena reserved at startup
- one LWLock protecting publish/replace of the slot

Use offsets inside the payload arena rather than raw `palloc()`-style pointer
ownership.

### Control struct sketch

```c
typedef struct ShnswSharedScanCacheCtl
{
    slock_t mutex_or_lwlock;
    bool valid;
    Oid relid;
    RelFileLocator locator;
    uint64 cache_gen;
    int32 dim;
    int16 M;
    int16 max_level;
    int32 n_nodes;
    int32 entry_nid;

    Size payload_bytes;
    Size sq8_mins_off;
    Size sq8_scales_off;
    Size upper_entries_off[SHNSW_MAX_LEVELS];
    Size upper_nbr_idx_off[SHNSW_MAX_LEVELS];
    int32 upper_count[SHNSW_MAX_LEVELS];
} ShnswSharedScanCacheCtl;
```

### Initial payload budget

Start with a fixed budget just for the experimental slot.

Current payload budget: `64 MB`.

If the decoded snapshot does not fit, publication bails out and the backend
falls back to the normal private decode path.

## Backend behavior

On scan-cache lookup:

1. Read `cache_gen` from the metapage.
2. Check the shared slot under lock.
3. If `{relid, locator, cache_gen}` match and payload is present:
   - attach backend-local lightweight wrappers to shared immutable arrays
   - skip SQ8/upper decode
4. Otherwise:
   - build the normal backend-local cache
   - materialize any deferred L0 pages
   - if the shared slot is empty or stale and the payload fits, publish the
     decoded immutable snapshot into shared memory

On insert, vacuum tombstone, rebuild, relfilenode swap, or reindex:

- metapage already bumps `cache_gen`
- next backend lookup sees mismatch and treats the shared slot as stale
- stale slots are replaced lazily on the next eligible scan

## Correctness constraints

- shared cache must never survive a `cache_gen` mismatch
- any `cache_gen` mismatch forces a miss
- payload publication must be all-or-nothing
- fallback to backend-local decode must remain correct when shmem is disabled,
  full, or stale

## Falsifier

The branch cleared the original gate:

- `fresh` 50k/384D fixed-graph average:
  - `shared_cache=off -> 1.9213 ms`
  - `shared_cache=on  -> 0.6337 ms`
- warm same-backend 50k/384D:
  - `shared_cache=off -> 0.2180 ms`
  - `shared_cache=on  -> 0.0917 ms`
- `make installcheck` green
- `scripts/test_sorted_hnsw_crash_recovery.sh` green
- fresh-backend insert invalidation check:
  - nearest before insert `833:0.000258`
  - nearest after insert `2001:0.000000`

That is enough to keep the branch and stop treating it as speculative.

## Validation plan

Use the new benchmark split from
[`scripts/bench_sorted_hnsw_fixed_graph.sh`](/Users/sergey/Projects/C/clustered_pg/scripts/bench_sorted_hnsw_fixed_graph.sh):

- `fresh`: cross-backend cold-start cost
- `reuse`: steady-state search cost

Required checks:

- `make installcheck REGRESS='pg_sorted_heap sorted_hnsw'`
- `./scripts/test_sorted_hnsw_crash_recovery.sh /tmp <port>`
- `./scripts/bench_sorted_hnsw_fixed_graph.sh /tmp <port> 2000 8 384 fresh on off`
- `./scripts/bench_sorted_hnsw_fixed_graph.sh /tmp <port> 2000 8 384 fresh on on`
- `./scripts/bench_sorted_hnsw_fixed_graph.sh /tmp <port> 2000 8 384 reuse on off`
- `./scripts/bench_sorted_hnsw_fixed_graph.sh /tmp <port> 2000 8 384 reuse on on`

## If Phase A works

Only then consider:

- more than one shared slot
- LRU/size-based replacement
- optional shared L0 decode for read-mostly indexes
- explicit observability counters for shared hits/misses/publishes

## If Phase A fails

Stop local cache-manager work and accept that the next performance branch is
either:

- a larger shared-cache redesign with DSA/DSM, or
- planner/policy work that avoids paying fresh-backend cost in short-lived
  sessions
