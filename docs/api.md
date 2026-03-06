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

---

## Vector search

See the [Vector Search guide](vector-search) for a full tutorial.

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
