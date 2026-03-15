---
layout: default
title: SQL API
nav_order: 5
---

# SQL API Reference

## Compaction

### `sorted_heap_compact(regclass)`

Rewrites the table in globally sorted PK order and rebuilds the zone map.
Acquires `AccessExclusiveLock`.

```sql
SELECT sorted_heap_compact('events'::regclass);
```

### `sorted_heap_compact_online(regclass)`

Non-blocking compaction using trigger-based change capture. Concurrent
reads and writes continue during the operation.

```sql
CALL sorted_heap_compact_online('events'::regclass);
```

### `sorted_heap_merge(regclass)`

Incremental merge: detects the already-sorted prefix and only re-sorts the
unsorted tail. 50--90% faster than full compact when data is partially sorted.
Acquires `AccessExclusiveLock`.

```sql
SELECT sorted_heap_merge('events'::regclass);
```

### `sorted_heap_merge_online(regclass)`

Non-blocking variant of merge with the same three-phase approach as
`sorted_heap_compact_online`.

```sql
CALL sorted_heap_merge_online('events'::regclass);
```

---

## Zone map

### `sorted_heap_zonemap_stats(regclass)`

Returns a text summary of the zone map: format version, number of entries,
validity flags, and overflow page chain.

```sql
SELECT sorted_heap_zonemap_stats('events'::regclass);
```

Example output:

```
v6 nentries=1946 flags=valid,sorted overflow_pages=7
```

### `sorted_heap_rebuild_zonemap(regclass)`

Forces a full zone map rebuild by scanning all tuples. Useful after
bulk operations that invalidate the zone map.

```sql
SELECT sorted_heap_rebuild_zonemap('events'::regclass);
```

---

## Monitoring

### `sorted_heap_scan_stats()`

Returns scan statistics as a record: total scans, blocks scanned, blocks
pruned, and stats source (shared memory or per-backend).

```sql
SELECT * FROM sorted_heap_scan_stats();
```

```
 total_scans | blocks_scanned | blocks_pruned | source
-------------+----------------+---------------+---------
         142 |           284  |        276012 | shmem
```

### `sorted_heap_reset_stats()`

Resets the scan statistics counters.

```sql
SELECT sorted_heap_reset_stats();
```

---

## Trigger

### `sorted_heap_compact_trigger()`

A trigger function used internally by the online compact/merge procedures
to capture changes during non-blocking operations. Not intended for
direct use.

---

## Scan pruning

### IN / ANY support

SortedHeapScan prunes blocks for `IN` and `= ANY(array)` queries on the
leading primary key column. Each block's zone map entry is checked against
the sorted list of target values using O(log K) binary search — blocks that
contain no matching values are skipped entirely.

Both literal arrays and parameterized arrays (generic prepared statements)
are supported, including LATERAL/NestLoop runtime parameters.

```sql
-- Literal IN-list — pruned at plan time
SELECT * FROM events WHERE id IN (100, 200, 300);

-- Literal ANY — same pruning
SELECT * FROM events WHERE id = ANY(ARRAY[100, 200, 300]);

-- Generic prepared statement — pruned at execution time
PREPARE q(int[]) AS SELECT * FROM events WHERE id = ANY($1);
SET plan_cache_mode = force_generic_plan;
EXECUTE q(ARRAY[100, 200, 300]);

-- LATERAL join with runtime array — pruned per outer row
SELECT o.i, s.cnt
FROM (SELECT g AS i, make_arr(g) AS arr FROM generate_series(1,10) g) o
CROSS JOIN LATERAL (
    SELECT count(*) AS cnt FROM events WHERE id = ANY(o.arr)
) s;
```

The scan computes a bounding box (min/max of the array values) to limit the
block range, then applies per-block IN-value filtering within that range.

For LATERAL/NestLoop joins, runtime array parameters (`PARAM_EXEC`) are
resolved at first rescan when outer values become available.

---

## Configuration (GUCs)

### `sorted_heap.enable_scan_pruning`

| Property | Value |
|----------|-------|
| Type | boolean |
| Default | `on` |
| Context | user (SET) |

Enables or disables zone map scan pruning for sorted_heap tables. When
disabled, queries fall back to sequential scan.

```sql
SET sorted_heap.enable_scan_pruning = off;
```

### `sorted_heap.vacuum_rebuild_zonemap`

| Property | Value |
|----------|-------|
| Type | boolean |
| Default | `on` |
| Context | user (SET) |

When enabled, VACUUM automatically rebuilds an invalid zone map, re-enabling
scan pruning without a manual compact step.

```sql
SET sorted_heap.vacuum_rebuild_zonemap = off;
```

### `sorted_heap.lazy_update`

| Property | Value |
|----------|-------|
| Type | boolean |
| Default | `off` |
| Context | user (SET, SET LOCAL, ALTER SYSTEM) |

When enabled, the first UPDATE on a covered page invalidates the zone map on
disk. All subsequent UPDATEs skip zone map maintenance entirely. The planner
falls back to Index Scan. INSERT always uses eager maintenance regardless.
Compact or merge restores zone map pruning.

```sql
-- Per session
SET sorted_heap.lazy_update = on;

-- Per transaction
SET LOCAL sorted_heap.lazy_update = on;

-- Globally
ALTER SYSTEM SET sorted_heap.lazy_update = on;
SELECT pg_reload_conf();
```

### `sorted_heap.hnsw_cache_l0`

| Property | Value |
|----------|-------|
| Type | boolean |
| Default | `off` |
| Context | user (SET) |

Enables session-local cache for HNSW sidecar tables. On first
`svec_hnsw_scan` call, L0 is loaded via sequential scan (~95ms build,
~100 MB for 100K nodes). Upper levels (L1--L4) cached separately (~6 MB).
Cache is evicted on DDL (relcache invalidation).

```sql
SET sorted_heap.hnsw_cache_l0 = on;
```

### `sorted_heap.hnsw_ef_patience`

| Property | Value |
|----------|-------|
| Type | integer |
| Default | `0` (disabled) |
| Context | user (SET) |

Patience-based early termination for L0 beam search. When set to N > 0, the
search stops after N consecutive expansions that don't improve the result set.
`ef_search` becomes the maximum budget.

```sql
SET sorted_heap.hnsw_ef_patience = 20;
```

### `sorted_heap.ann_timing`

| Property | Value |
|----------|-------|
| Type | boolean |
| Default | `off` |
| Context | user (SET) |

Enables per-query timing breakdown for `svec_ann_scan`, `svec_graph_scan`,
and `svec_hnsw_scan`. Output is emitted at `DEBUG1` log level.

```sql
SET sorted_heap.ann_timing = on;
SET client_min_messages = debug1;
```

---

## Vector search

See the [Vector Search guide](vector-search) for a full tutorial.

### Training permissions

All training functions (`svec_ann_train`, `svec_pq_train`, `svec_pq_train_residual`,
`svec_ivf_train`) create internal metadata tables in the extension schema on first
call. The calling role must have `CREATE` privilege on the extension schema, or be
the extension owner / superuser.

### `svec_ann_train(source_query, nlist, m, n_iter, max_samples)`

Trains both IVF centroids and raw PQ codebook from a SQL query returning
`svec` vectors. Returns `(ivf_cb_id, pq_cb_id)`.

```sql
SELECT * FROM svec_ann_train(
    'SELECT embedding FROM my_table',
    nlist := 64, m := 192);
```

### `svec_pq_train_residual(source_query, m, ivf_cb_id, n_iter, max_samples)`

Trains a residual PQ codebook on `(vector − nearest IVF centroid)` residuals.
Higher recall than raw PQ at no additional storage cost.

```sql
SELECT svec_pq_train_residual(
    'SELECT embedding FROM my_table',
    m := 192, ivf_cb_id := 1);
```

### `svec_ann_scan(tbl, query, nprobe, lim, rerank_topk, cb_id, ivf_cb_id, pq_column)`

C-level IVF-PQ scan — fastest path. Performs IVF probe, PQ ADC, top-K selection,
and optional exact cosine reranking in a single C function call.

```sql
-- PQ-only (fastest)
SELECT * FROM svec_ann_scan('my_table', query_vec,
    nprobe := 3, lim := 10, cb_id := 2, ivf_cb_id := 1);

-- With exact reranking
SELECT * FROM svec_ann_scan('my_table', query_vec,
    nprobe := 10, lim := 10, rerank_topk := 200,
    cb_id := 2, ivf_cb_id := 1);
```

| Parameter | Default | Description |
|---|---|---|
| tbl | — | Table name (regclass) |
| query | — | Query vector (svec) |
| nprobe | 10 | Number of IVF partitions to probe |
| lim | 10 | Number of results to return |
| rerank_topk | 0 | If > 0, rerank this many PQ candidates with exact cosine |
| cb_id | 1 | PQ codebook ID |
| ivf_cb_id | 0 | IVF codebook ID (> 0 enables residual PQ mode) |
| pq_column | 'pq_code' | Name of the PQ code column |

### `svec_ann_search(tbl, query, nprobe, lim, rerank_topk, cb_id)`

SQL-level IVF-PQ search. Same interface as `svec_ann_scan` but implemented in
PL/pgSQL. Useful for debugging and when `svec_ann_scan` is not available.

### `svec_ivf_assign(vec, cb_id)`

Returns the nearest IVF centroid ID for a vector. Used in generated columns
to assign rows to partitions.

### `svec_ivf_probe(vec, nprobe, cb_id)`

Returns an array of the `nprobe` nearest IVF centroid IDs. Used in WHERE
clauses to filter candidates.

### `svec_pq_encode(vec, cb_id)` / `svec_pq_encode_residual(vec, centroid_id, pq_cb_id, ivf_cb_id)`

Encode a vector as an M-byte PQ code. The residual variant encodes
`(vec − centroid)` for use with residual PQ codebooks.
