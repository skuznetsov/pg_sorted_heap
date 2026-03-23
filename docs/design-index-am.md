# Design: HNSW Index Access Method (`sorted_hnsw`)

## Goal

Replace the current sidecar-table + `svec_hnsw_scan()` function with a proper
PostgreSQL Index AM so that the planner can automatically choose HNSW index
scans for `ORDER BY embedding <=> query LIMIT K` queries.

```sql
-- Creation
CREATE INDEX ON documents USING sorted_hnsw (embedding svec_cosine_ops)
  WITH (m = 16, ef_construction = 200);

-- Query (planner chooses index scan automatically)
SELECT * FROM documents ORDER BY embedding <=> query_vec LIMIT 10;

-- Explicit control
SET sorted_hnsw.ef_search = 96;
SET sorted_hnsw.sq8 = on;
```

## Current Architecture (Sidecar Model)

```
gutenberg_gptoss_sh          (sorted_heap table, main data)
  |
  +-- gutenberg_gptoss_hnsw_meta    (entry_nid, max_level)
  +-- gutenberg_gptoss_hnsw_l0      (nid, sketch/svec, neighbors[], src_id, src_tid)
  +-- gutenberg_gptoss_hnsw_l1..l4  (nid, sketch, neighbors[])

Session-local cache (per-backend):
  +-- L0 SQ8 cache (~297MB for 103K x 2880-dim, streaming two-pass build)
  +-- Upper level cache (~6MB)
  +-- Build time: ~95ms (warm) to ~3.4s (cold TOAST, 2Gi pod)

Search path:
  1. User calls svec_hnsw_scan() explicitly
  2. Cache built on first call (streaming SQ8 two-pass)
  3. Upper levels navigated via cache
  4. L0 navigated via SQ8 cache
  5. Top-K reranked via TOAST fetch of original svec
```

### What works
- Sub-millisecond search (0.49-1.99ms on AWS ARM64)
- SQ8 4x memory compression
- Streaming two-pass build (no OOM on 2Gi pods)
- NEON SIMD for distance computation

### What doesn't work
- Manual function call instead of idiomatic SQL
- No planner integration (no cost estimation, no join optimization)
- No incremental updates (full rebuild required)
- No VACUUM/DELETE support
- No crash recovery for graph mutations
- Session-local cache rebuilt per connection

## Proposed Architecture

### Storage Layout

The index uses PostgreSQL's standard relation storage (not special pages).
Each index tuple stores a graph node:

```
Index relation pages:
  Page 0: Metapage (fixed-size, fits in one 8KB page)
    - magic (uint32)
    - entry_nid (int32)
    - max_level (int16)
    - M (int16), ef_construction (int16)
    - vector_dim (int32)
    - node_count (int32)
    - sq8_aux_start (BlockNumber)  -- first page of SQ8 metadata
    - sq8_aux_npages (int32)       -- number of SQ8 metadata pages
    - flags (uint32)               -- SQ8_VALID, NEEDS_REINDEX, etc.

  Pages 1..S: SQ8 metadata pages
    Store sq8_mins[dim] and sq8_scales[dim] as float32 arrays.
    For dim=2880: 2 × 2880 × 4 = 23,040 bytes → 3 pages (8KB each).
    Linked via next_block pointer in page special space.
    Read once at scan start, cached in scan state.

  Pages S+1..N: L0 node pages
    Each tuple: (nid int32, heap_tid tid, neighbors int32[2*M], sq8_vec uint8[dim])
    Packed format, no standard index tuple header overhead for vector data.
    For dim=2880, M=16: each node = 4 + 6 + 128 + 2880 = 3018 bytes.
    ~2 nodes per 8KB page → ~52K pages for 103K nodes.

  Pages N+1..N+K: Upper level pages (levels 1..max_level)
    Each tuple: (nid int32, neighbors int32[2*M], hsvec float16[sketch_dim])
    Much fewer nodes per level (exponential falloff).
    For sketch_dim=384, M=16: each node = 4 + 128 + 768 = 900 bytes.
    ~8 nodes per page.

  Index size estimate (103K x 2880-dim, M=16):
    Metapage: 1 page
    SQ8 metadata: 3 pages
    L0: ~52K pages = ~400MB
    Upper levels: ~1K pages = ~8MB
    Total: ~408MB on disk
```

Key design choice: **SQ8 vectors stored inline in index tuples** at L0.
This eliminates the session-local cache entirely — the index IS the cache.
Upper levels store hsvec sketches (smaller, used only for navigation).

Note on SQ8 metadata: the per-dimension min/max arrays (23KB for 2880-dim)
do NOT fit in the metapage. They are stored in dedicated auxiliary pages
referenced from the metapage. This is analogous to how GiST stores
internal union values in separate pages.

### Advantages over sidecar model
- No session-local cache build (instant first query, vs 95ms-3.4s per connection)
- Shared across all backends (OS page cache, shared_buffers)
- Standard VACUUM/visibility semantics
- WAL-logged (crash safe)
- Planner can estimate costs
- Incremental INSERT/DELETE (vs full rebuild)

### Trade-off
- Index size on disk: ~408MB for 103K x 2880-dim (SQ8 L0 + upper levels)
  vs 0 bytes on disk currently (sidecar tables ~99MB for L0 + ~7MB upper)
- Eliminates ~297MB per-connection RAM overhead for session-local cache
- More I/O per query (read from shared_buffers vs local malloc cache),
  but shared_buffers hit rate should be high for repeated workloads

## Index AM Callbacks

### Required (IndexAmRoutine)

```c
/* Handler */
IndexAmRoutine *sorted_hnsw_handler(PG_FUNCTION_ARGS);

/* Build */
IndexBuildResult *sorted_hnsw_build(Relation heap, Relation index,
                                     IndexInfo *indexInfo);
void sorted_hnsw_buildempty(Relation index);

/* Insert */
bool sorted_hnsw_insert(Relation index, Datum *values, bool *isnull,
                         ItemPointer heap_tid, Relation heap,
                         IndexUniqueCheck checkUnique,
                         bool indexUnchanged,
                         IndexInfo *indexInfo);

/* Scan */
IndexScanDesc sorted_hnsw_beginscan(Relation index, int nkeys, int norderbys);
void sorted_hnsw_rescan(IndexScanDesc scan, ScanKey keys, int nkeys,
                         ScanKey orderbys, int norderbys);
bool sorted_hnsw_gettuple(IndexScanDesc scan, ScanDirection direction);
void sorted_hnsw_endscan(IndexScanDesc scan);

/* Cost */
void sorted_hnsw_costestimate(PlannerInfo *root, IndexPath *path,
                               double loop_count,
                               Cost *indexStartupCost,
                               Cost *indexTotalCost,
                               Selectivity *indexSelectivity,
                               double *indexCorrelation,
                               double *indexPages);

/* Maintenance */
IndexBulkDeleteResult *sorted_hnsw_bulkdelete(IndexVacuumInfo *info,
                                               IndexBulkDeleteResult *stats,
                                               IndexBulkDeleteCallback callback,
                                               void *callback_state);
IndexBulkDeleteResult *sorted_hnsw_vacuumcleanup(IndexVacuumInfo *info,
                                                  IndexBulkDeleteResult *stats);

/* Validation */
bool sorted_hnsw_validate(Oid opclassoid);

/* Properties */
bool sorted_hnsw_amproperty(Oid index_oid, int attno,
                             IndexAMProperty prop, const char *propname,
                             bool *res, bool *isnull);

/* Options */
bytea *sorted_hnsw_options(Datum reloptions, bool validate);
```

### Not needed initially
- `amcanparallel` — false (serial search is already sub-ms)
- `amcanmulticol` — false (single vector column)
- `amcanunique` — false
- `amcanorderbyop` — **true** (this is how ORDER BY <=> works)

## Operator Class

```sql
CREATE OPERATOR CLASS svec_cosine_ops
  FOR TYPE svec USING sorted_hnsw AS
    OPERATOR 1 <=> (svec, svec) FOR ORDER BY float_ops,
    FUNCTION 1 svec_cosine_distance(svec, svec);
```

The `<=>` operator already exists in our extension. We register it as an
ordering operator (not equality/comparison) so the planner knows this index
supports `ORDER BY col <=> const LIMIT K`.

## Build Strategy

### ambuild (bulk)

```
1. Sequential scan of heap, extract all (tid, embedding) pairs
2. Normalize vectors
3. Build HNSW in memory using our own C implementation
   (port the faiss HNSW algorithm to C, or call faiss via FFI)
4. Compute SQ8 min/max per dimension (pass 1 of streaming approach)
5. Write metapage
6. Write L0 pages: for each node, quantize to SQ8, pack with neighbors + tid
7. Write upper level pages
8. WAL-log all pages via GenericXLog
```

For 103K x 2880-dim: expected build time ~15-30s (faiss builds in 12s,
plus I/O for writing index pages).

Open question: implement HNSW construction in C or link faiss as a build
dependency? Options:

**Option A: C implementation (recommended)**
- No external dependencies
- Full control over memory layout
- Can stream directly to index pages
- Moderate complexity (~500-800 lines for HNSW insert + neighbor selection)
- HNSW construction is well-documented (Malkov & Yashunin 2018)

**Option B: faiss FFI**
- Faster to implement initially
- Adds C++ dependency (faiss) to build
- Need to extract adjacency lists from faiss internal structures
- Not portable to all platforms

### aminsert (incremental)

```
1. Quantize new vector to SQ8 using stored min/max from metapage
2. Pick random level (geometric distribution, p = 1/ln(M))
3. Find entry point from metapage
4. Greedy search from top to level+1 (using upper level sketches)
5. At each level from level down to 0:
   a. Search ef_construction nearest neighbors
   b. Select M best neighbors (heuristic: prefer diverse directions)
   c. Update neighbor lists (may require page modifications)
   d. WAL-log modified pages
6. If new node level > max_level, update metapage entry point
```

Concern: SQ8 min/max may drift as new vectors arrive with values outside
the original range. Options:
- Recompute min/max periodically (REINDEX)
- Use conservative initial range (quantile-based with headroom)
- Track drift in metapage, trigger REINDEX warning when >5% OOR

## Scan Strategy

### beginscan / gettuple

The scan must handle `ORDER BY embedding <=> $1 LIMIT K` patterns.
PostgreSQL passes the ordering operator via `scan->orderByData`.

```
beginscan:
  - Allocate scan state (priority queue, visited set)
  - Read metapage (entry_nid, max_level)
  - Pin frequently accessed pages in buffer cache

rescan:
  - Extract query vector from orderByData[0]
  - Precompute query SQ8 representation (or keep float32)
  - Run HNSW search:
    1. Navigate upper levels using hsvec sketch distances
    2. Navigate L0 using SQ8 distances (inline in index tuples)
    3. Collect top ef_search candidates
    4. Sort by SQ8 distance
  - Store result TIDs in scan state

gettuple:
  - Pop next TID from sorted result list
  - Set scan->xs_heaptid and xs_recheck = false (distance is exact)
  - Return true
  - When exhausted, return false
```

### Reranking and amcanorderbyop Contract

**Critical constraint:** when `amcanorderbyop = true`, the executor trusts
that the AM emits tuples in final distance order. The `ORDER BY ... LIMIT K`
plan does NOT re-sort — it simply takes the first K tuples from the index
scan. Therefore the AM MUST return tuples in exact distance order.

**Design rule for Phase 1:** the AM always reranks internally and returns
tuples in exact svec cosine distance order. The flow:

```
gettuple called repeatedly by executor:
  1. On first call: run HNSW search, collect ef_search candidates (TIDs)
  2. For each candidate TID:
     a. Fetch heap tuple
     b. Check MVCC visibility (snapshot check)
     c. If visible: compute exact svec <=> distance, add to result set
     d. If dead/invisible: skip (do not count toward results)
  3. If result set has fewer than ef_search visible tuples AND
     the HNSW search was not exhaustive:
     → Continue exploring the graph (expand beam) to find more
       live candidates, up to max_candidates = ef_search * 2 total
       graph nodes visited. This handles moderate churn.
  4. Sort visible results by exact distance
  5. Store sorted result list in scan state
  6. Return tuples one at a time in exact order

LIMIT K in executor simply stops calling gettuple after K rows.
```

**Dead candidate / churn policy:**

After UPDATE/DELETE, some HNSW nodes point to dead heap tuples. The scan
must handle this gracefully:

- **Visibility check:** every candidate TID is checked against the active
  snapshot before computing exact distance. Dead tuples are skipped.
- **Top-up exploration:** if too many candidates are dead, the initial
  ef_search batch may yield fewer than K live results. The AM continues
  HNSW exploration (expanding the beam) until either K live results are
  found or a hard limit (2 × ef_search nodes visited) is reached.
- **Underflow behavior:** if even after top-up the AM cannot find K live
  tuples, it returns fewer than K rows. The executor handles this
  correctly (LIMIT K returns at most K, not exactly K).
- **Cost implication:** high churn increases scan cost. The cost estimator
  accounts for this using `pg_stat_user_tables.n_dead_tup` to inflate
  the expected number of nodes visited.
- **REINDEX recommendation:** when dead_ratio > 20%, VACUUM logs a
  WARNING suggesting REINDEX to rebuild the graph without dead nodes.

**Strict rule for automatic index scan path:** the AM exact-reranks ALL
`ef_search` candidates collected during HNSW traversal. Every candidate
gets an exact svec cosine distance computed from the heap tuple. The
returned tuples are sorted by exact distance. There is no lossy cutoff.

This means the automatic index path always pays the full rerank cost
(ef_search TOAST reads). For ef_search=96 with 2880-dim svec (~11.5KB
each), that is ~1.1MB of TOAST I/O per query. On warm cache this adds
~1-2ms. This is the price of correctness.

**No `rerank_topk` GUC in the index scan path.** The `rerank_topk`
optimization (rerank fewer candidates to save TOAST reads) remains
available only via the explicit `svec_hnsw_scan()` function, which
does not claim ordered-index semantics. Users who prefer speed over
strict ordering use the function; users who want planner integration
and correct ORDER BY semantics use the index.

This separation is clean:
- `CREATE INDEX USING sorted_hnsw` → exact ordered results, planner-integrated
- `svec_hnsw_scan()` → tunable speed/recall tradeoff, manual invocation

## Cost Estimation

The cost model must account for three distinct phases:

1. **Index navigation** (SQ8 distance in index pages)
2. **Heap fetch + visibility** (random I/O to heap + MVCC check)
3. **Exact rerank** (TOAST detoast + float32 cosine distance)

Phase 3 is the dominant cost for high-dimensional vectors and cannot
be omitted — the new exact-rerank contract guarantees it always runs.

```c
void sorted_hnsw_costestimate(...)
{
    double ef        = sorted_hnsw_ef_search;   /* GUC, default 96 */
    int    dim       = index->rd_att->atts[0]->atttypmod;  /* vector dimension */
    double dead_frac = get_dead_tuple_fraction(rel);  /* from pg_stat */
    double visited   = ef * 1.5;        /* avg graph nodes touched during search */
    double live_cand = ef;              /* candidates collected */
    double topup     = live_cand * dead_frac;  /* extra nodes for dead-tuple top-up */
    double total_vis = visited + topup;

    /* Phase 1: Index navigation (SQ8 distances on index pages)
     * Each SQ8 distance: dim bytes loaded + dim/16 NEON ops
     * Index pages are ~2 nodes/page for 2880-dim, so ~visited/2 page reads */
    double nav_cpu  = total_vis * dim * cpu_operator_cost / 100.0;
    double nav_io   = (total_vis / 2.0) * random_page_cost * 0.2;  /* mostly cached */

    /* Phase 2: Heap fetch + visibility check for each candidate
     * Random heap page read per candidate (may hit buffer cache) */
    double heap_cpu  = live_cand * cpu_tuple_cost;
    double heap_io   = live_cand * random_page_cost * 0.3;  /* partial cache hits */

    /* Phase 3: Exact rerank — dominant cost
     * Each candidate: detoast svec (~dim*4 bytes from TOAST, ~ceil(dim*4/2000)
     * TOAST chunks) + float32 cosine distance (dim FMA ops)
     * For dim=2880: ~6 TOAST chunks per vector, ~11.5KB */
    double toast_chunks = ceil((double)(dim * 4) / 2000.0);
    double rerank_io    = live_cand * toast_chunks * random_page_cost * 0.5;
    double rerank_cpu   = live_cand * dim * cpu_operator_cost / 50.0;

    *indexStartupCost = 0;  /* first tuple requires full search */
    *indexTotalCost   = nav_cpu + nav_io +
                        heap_cpu + heap_io +
                        rerank_io + rerank_cpu;

    /* Selectivity: ordered scan returns at most LIMIT rows */
    *indexSelectivity = (path->indexorderbys != NIL) ?
        Min(1.0, (double) ef / rel->tuples) : 1.0;

    *indexCorrelation = 0.0;  /* no physical correlation */
    *indexPages = RelationGetNumberOfBlocks(index);
}
```

**Cost breakdown example** (103K × 2880-dim, ef=96, warm cache):

| Phase | What | Estimated |
|-------|------|-----------|
| Navigation | 144 SQ8 distances on ~72 index pages | ~0.5ms |
| Heap fetch | 96 random heap page reads | ~0.3ms |
| Exact rerank | 96 × 6 TOAST chunks + 96 cosine | ~1.5ms |
| **Total** | | **~2.3ms** |

Measured p50 on AWS: 1.37ms (rk=20 with only 20 TOAST reads) to 2.99ms
(rk=0 with all 96 TOAST reads). The cost model correctly identifies
exact rerank as the dominant term.

**Planner behavior:** with this cost model, the planner will prefer
HNSW index scan over sequential scan when `LIMIT K` is small. For
large LIMIT or no LIMIT, the index cost exceeds seq scan and the
planner correctly falls back. The dead_frac term ensures the cost
increases realistically under churn.

## VACUUM / DELETE

### bulkdelete

Mark deleted nodes in the graph. Two strategies:

**Strategy A: Lazy deletion (recommended for v1)**
- Mark node as deleted (set a flag bit in the index tuple)
- Don't modify neighbor lists
- Search skips deleted nodes
- REINDEX cleans up and rebuilds

**Strategy B: Eager deletion**
- Remove node from all neighbor lists
- Reconnect neighbors (expensive: may need to re-search for replacements)
- Complex WAL logging for multi-page updates
- Better for high-churn workloads

For v1, Strategy A. Most ANN workloads are append-heavy with rare deletes.

### vacuumcleanup

- Count deleted vs total nodes
- If deleted_ratio > threshold (e.g., 20%), suggest REINDEX
- Update metapage statistics

## WAL / Crash Recovery

All page modifications use **GenericXLog** (same as our sorted_heap AM):

```c
GenericXLogState *state = GenericXLogStart(index);
Page page = GenericXLogRegisterBuffer(state, buf, 0);
/* modify page */
GenericXLogFinish(state);
```

This gives us crash safety without a custom WAL resource manager.
GenericXLog logs full page images, which is inefficient for small
modifications (e.g., updating one neighbor list), but correct and simple.

Future optimization: custom WAL rmgr for delta-encoded neighbor updates.
Not needed for v1.

## GUCs

| GUC | Type | Default | Description |
|-----|------|---------|-------------|
| `sorted_hnsw.ef_search` | int | 96 | Beam width during search (more = better recall, slower) |
| `sorted_hnsw.sq8` | bool | on | Use SQ8 quantized L0 (off = float32 in index, 4x larger) |

Note: no `rerank_topk` GUC for the index scan path. All ef_search candidates
are exact-reranked. The `rerank_topk` optimization remains in `svec_hnsw_scan()`.

## Reloptions (per-index)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `m` | int | 16 | HNSW M parameter (max edges per node per level) |
| `ef_construction` | int | 200 | Build-time beam width |

## Implementation Phases

### Phase 1: Index AM MVP (build + scan + insert)

A PostgreSQL index on a writable table MUST accept new tuples via
`aminsert` after `CREATE INDEX`. Without this, DML on indexed tables
either breaks or silently produces stale results. Therefore Phase 1
includes incremental insert.

- `amhandler`, `ambuild`, `ambuildempty`
- `aminsert` with HNSW node insertion algorithm
- `beginscan`, `rescan`, `gettuple`, `endscan`
- `costestimate`, `validate`, `options`
- Operator class `svec_cosine_ops`
- Build: HNSW construction in C (port from faiss algorithm)
- SQ8 inline in L0 index tuples, SQ8 metadata in auxiliary pages
- GenericXLog for crash safety
- GUC: `ef_search` (no `rerank_topk` — all candidates exact-reranked)
- SQ8 drift tracking: metapage flag when new vectors exceed min/max range,
  REINDEX recommended via WARNING when >5% of nodes are out-of-range
- `bulkdelete` / `vacuumcleanup`: lazy deletion (tombstone bit on node,
  search skips deleted nodes, REINDEX to reclaim space)

Deliverable: `CREATE INDEX ... USING sorted_hnsw` works on normal
writable tables. `ORDER BY <=> LIMIT K` uses index scan automatically.
INSERT adds new nodes to the graph incrementally. DELETE marks nodes
as tombstones. REINDEX rebuilds the full graph.

### Phase 2: Production hardening
- Concurrent INSERT stress testing
- SQ8 range auto-expansion (extend min/max without REINDEX)
- VACUUM efficiency (reclaim pages with all-deleted nodes)
- Buffer pool prefetch patterns for L0 traversal
- CREATE INDEX CONCURRENTLY support

### Phase 3: Optimization
- Pin hot upper-level pages in shared_buffers
- Batch insert mode (for bulk loading after initial CREATE INDEX)
- Parallel ambuild (PG's parallel index build infrastructure)
- Experimental shared scan cache: see `docs/design-sorted-hnsw-shared-cache.md`

## Files

| File | Purpose |
|------|---------|
| `src/sorted_hnsw.c` | Index AM handler, build, scan, cost, vacuum |
| `src/sorted_hnsw.h` | Page layout, node structs, constants |
| `src/hnsw_build.c` | HNSW construction algorithm (C port) |
| `src/hnsw_search.c` | HNSW search with SQ8 (moved from pq.c) |

Estimated size: ~3000-4000 lines of new C code.

## Migration Path

1. Ship Phase 1 as v0.12.0
2. Keep `svec_hnsw_scan()` function as deprecated compatibility layer
3. Sidecar tables remain supported (for users who built graphs externally)
4. New users use `CREATE INDEX USING sorted_hnsw`

## Decisions (resolved from review)

1. **HNSW construction in C** (not faiss FFI). No external C++ dependency.
   Expect a substantial module (~800-1200 lines), not a trivial port.
   Malkov & Yashunin 2018 algorithm with shrink-pruning heuristic.

2. **Index size budget**: ~408MB on disk for 103K x 2880-dim is acceptable.
   This replaces ~297MB per-connection session-local cache. Net win for
   multi-connection production deployments.

3. **hsvec for upper levels** in Phase 1. Matches the proven search path.
   Switch to svec only if profiling shows upper navigation is a recall
   bottleneck (unlikely — upper levels are traversed quickly).

4. **CREATE INDEX CONCURRENTLY**: deferred to Phase 2. Phase 1 uses
   standard exclusive lock during build.

5. **amcanorderbyop**: RESOLVED. The AM always reranks internally with
   exact svec distance and returns tuples in final exact order.
   Approximate-only mode (`rerank_topk=0`) remains in `svec_hnsw_scan()`
   function but is NOT available via automatic index scan path.

## Open Questions

1. **Node page layout**: with dim=2880 and M=16, each L0 node is ~3KB.
   Only 2 nodes fit per 8KB page. This is ~50% page utilization. Options:
   a) Accept the waste (simple implementation)
   b) Use variable-length item layout with continuation tuples
   c) Reduce SQ8 dim (prefix truncation at index level)
   Leaning toward (a) for Phase 1 simplicity.

2. **SQ8 drift on INSERT**: when new vectors have values outside the
   original min/max range, SQ8 quantization clips to 0 or 255. Options:
   a) Track OOR count in metapage, emit WARNING at >5%, REINDEX to fix
   b) Auto-expand range (requires re-quantizing all existing nodes — expensive)
   c) Use conservative initial range (p0.1 / p99.9 quantiles + 10% headroom)
   Leaning toward (a) + (c) for Phase 1.

3. **Buffer pool pressure**: 408MB of index pages in shared_buffers competes
   with heap/TOAST pages. Need to benchmark whether the index scan path
   achieves comparable latency to the current malloc-based session cache.
   May need `__builtin_prefetch` patterns for L0 page traversal.
