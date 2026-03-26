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

Patience-based early termination for L0 beam search. Applies to both
`svec_hnsw_scan()` and `sorted_hnsw` ordered index scans. When set to N > 0,
the search stops after N consecutive expansions that don't improve the result
set. `ef_search` becomes the maximum budget.

```sql
SET sorted_heap.hnsw_ef_patience = 20;
```

### `sorted_hnsw.ef_search`

| Property | Value |
|----------|-------|
| Type | integer |
| Default | `96` |
| Context | user (SET) |

Beam width for planner-integrated `sorted_hnsw` ordered index scans.
Higher values increase candidate exploration and usually improve recall at the
cost of latency.

```sql
SET sorted_hnsw.ef_search = 128;
```

### `sorted_hnsw.sq8`

| Property | Value |
|----------|-------|
| Type | boolean |
| Default | `on` |
| Context | user (SET) |

Controls SQ8 quantization in `sorted_hnsw` L0 storage and scan distance
evaluation. Leave this enabled for the current release path unless you are
doing low-level experiments.

```sql
SET sorted_hnsw.sq8 = on;
```

### `sorted_hnsw.shared_cache`

| Property | Value |
|----------|-------|
| Type | boolean |
| Default | `on` |
| Context | user (SET) |

Enables the preloaded shared decoded scan cache for ordered `sorted_hnsw`
index scans. Effective only when `pg_sorted_heap` is loaded via
`shared_preload_libraries`; otherwise scans fall back to backend-local cache
builds. When active, fresh backends can attach to a shared immutable snapshot
keyed by `{relid, relfilenode, cache_gen}` instead of rebuilding the decoded
graph privately.

```sql
SET sorted_hnsw.shared_cache = on;
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

### `sorted_hnsw` Index AM

The stable ANN path in this release is the planner-integrated HNSW access
method. It supports both `svec` and `hsvec`.

```sql
CREATE TABLE items (
    id        bigserial PRIMARY KEY,
    embedding svec(384),
    body      text
);

CREATE INDEX items_embedding_idx
ON items USING sorted_hnsw (embedding)
WITH (m = 16, ef_construction = 200);

SET sorted_hnsw.ef_search = 32;

SELECT id, body
FROM items
ORDER BY embedding <=> '[0.1,0.2,0.3,...]'::svec
LIMIT 10;
```

Compact-storage variant:

```sql
CREATE TABLE items_compact (
    id        bigserial PRIMARY KEY,
    embedding hsvec(384),
    body      text
);

CREATE INDEX items_compact_embedding_idx
ON items_compact USING sorted_hnsw (embedding hsvec_cosine_ops)
WITH (m = 16, ef_construction = 200);
```

Current contract:

- planner-integrated ordered scan for base-relation
  `ORDER BY embedding <=> query LIMIT k`
- not chosen when there is no `LIMIT`, when `LIMIT > sorted_hnsw.ef_search`,
  or when extra base-table quals would make the path under-return candidates
- exact rerank happens inside the index scan
- `sorted_hnsw.shared_cache = on` is most useful when
  `shared_preload_libraries = 'pg_sorted_heap'`

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

---

## GraphRAG (beta)

These functions are intended for fact-shaped retrieval over a `sorted_heap`
table clustered by `(entity_id, relation_id, target_id)`. The stable release
story for GraphRAG is still beta: the API is usable and benchmarked, but the
best operating point depends on workload shape and scoring contract.

Recommended schema shape:

```sql
CREATE TABLE facts (
    entity_id   int4,
    relation_id int2,
    target_id   int4,
    embedding   svec(384),
    payload     text,
    PRIMARY KEY (entity_id, relation_id, target_id)
) USING sorted_heap;

CREATE INDEX facts_embedding_idx
ON facts USING sorted_hnsw (embedding)
WITH (m = 24, ef_construction = 200);
```

### `sorted_heap_graph_rag(rel, query, relation_path, ann_k, top_k, score_mode, limit_rows)`

Preferred fact-shaped GraphRAG entry point.

- `relation_path := ARRAY[1]`
  - one-hop expansion
  - ANN seed on `entity_id`
  - exact rerank on the endpoint facts
- `relation_path := ARRAY[1, 2], score_mode := 'endpoint'`
  - two-hop expansion
  - rerank by the final-hop endpoint only
- `relation_path := ARRAY[1, 2], score_mode := 'path'`
  - two-hop expansion
  - path-aware rerank using hop-1 and hop-2 evidence together

Current constraints:

- `relation_path` must be a one-dimensional `int4[]` of length `1` or `2`
- supported `score_mode` values are `endpoint` and `path`
- current schema contract still expects canonical fact columns:
  `entity_id`, `relation_id`, `target_id`, `embedding`, `payload`

```sql
SET sorted_hnsw.ef_search = 128;

SELECT *
FROM sorted_heap_graph_rag(
    'facts'::regclass,
    '[0.1,0.2,0.3,...]'::svec,
    relation_path := ARRAY[1, 2],
    ann_k := 64,
    top_k := 10,
    score_mode := 'path'
);
```

### `sorted_heap_expand_ids(rel, seed_ids, relation_filter, limit_rows)`

Expands known entity seeds into fact rows without reranking.

```sql
SELECT *
FROM sorted_heap_expand_ids(
    'facts'::regclass,
    ARRAY[101, 202],
    relation_filter := 1
);
```

### `sorted_heap_expand_rerank(rel, seed_ids, query, top_k, relation_filter, limit_rows)`

One-hop expansion followed by exact rerank on the expanded candidates.

```sql
SELECT *
FROM sorted_heap_expand_rerank(
    'facts'::regclass,
    ARRAY[101, 202],
    '[0.1,0.2,0.3,...]'::svec,
    top_k := 10,
    relation_filter := 1
);
```

### `sorted_heap_expand_twohop_rerank(rel, seed_ids, query, top_k, hop1_relation_filter, hop2_relation_filter, limit_rows)`

Two-hop expansion with rerank on the final candidate set.

### `sorted_heap_expand_twohop_path_rerank(rel, seed_ids, query, top_k, hop1_relation_filter, hop2_relation_filter, limit_rows)`

Two-hop expansion with path-aware rerank using hop-1 and hop-2 evidence
together. This is the stronger current contract for fact-shaped multihop
retrieval.

```sql
SELECT *
FROM sorted_heap_expand_twohop_path_rerank(
    'facts'::regclass,
    ARRAY[101, 202],
    '[0.1,0.2,0.3,...]'::svec,
    top_k := 10,
    hop1_relation_filter := 1,
    hop2_relation_filter := 2
);
```

### `sorted_heap_graph_rag_scan(rel, query, ann_k, top_k, relation_filter, limit_rows)`

Lower-level one-hop wrapper retained for backward compatibility and
target-seeded graph shapes. This wrapper seeds one-hop expansion from
ANN-selected `target_id` values, so it is not the preferred fact-graph
contract.

### `sorted_heap_graph_rag_twohop_scan(rel, query, ann_k, top_k, hop1_relation_filter, hop2_relation_filter, limit_rows)`

Lower-level endpoint-scored two-hop wrapper. `sorted_heap_graph_rag(...)`
with `relation_path := ARRAY[hop1, hop2], score_mode := 'endpoint'`
is the preferred higher-level syntax.

### `sorted_heap_graph_rag_twohop_path_scan(rel, query, ann_k, top_k, hop1_relation_filter, hop2_relation_filter, limit_rows)`

Lower-level path-aware two-hop wrapper. `sorted_heap_graph_rag(...)` with
`relation_path := ARRAY[hop1, hop2], score_mode := 'path'` is the preferred
higher-level syntax.

```sql
SET sorted_hnsw.ef_search = 128;

SELECT *
FROM sorted_heap_graph_rag(
    'facts'::regclass,
    '[0.1,0.2,0.3,...]'::svec,
    relation_path := ARRAY[1, 2],
    ann_k := 64,
    top_k := 10,
    score_mode := 'path'
);
```
