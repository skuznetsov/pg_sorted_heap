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

### Partition-scoped maintenance

For declarative partitioned tables, use the explicit parent helpers instead of
calling the concrete-table functions on the parent. They recurse to concrete
leaves and operate one leaf at a time.

```sql
SELECT *
FROM sorted_heap_partition_status('events_parent'::regclass);

SELECT *
FROM sorted_heap_partition_index_status('events_parent'::regclass);

SELECT *
FROM sorted_heap_compact_partitions('events_parent'::regclass);

SELECT *
FROM sorted_heap_merge_partitions('events_parent'::regclass);

SELECT *
FROM sorted_heap_rebuild_zonemap_partitions('events_parent'::regclass);
```

Before running a maintenance operation on a large partition tree, use the
read-only plan helper:

```sql
SELECT *
FROM sorted_heap_partition_maintenance_plan(
    'events_parent'::regclass,
    operation := 'compact');
```

The plan returns one row per concrete leaf with:

- `status = 'would_run'` for supported sorted_heap leaves with a primary key;
- `status = 'blocked'` for unsupported leaves or sorted_heap leaves without a
  primary key;
- `lock_mode`, currently `AccessExclusiveLock` for the concrete operation;
- `tablespace_oid`, `tablespace_name`, and `tablespace_location`, so operators
  can join the rewrite estimate to their tablespace or filesystem monitoring;
- `relation_size_bytes` and `estimated_temp_bytes`. For `compact` and `merge`,
  the temp estimate is the current leaf size; for `rebuild_zonemap`, heap
  rewrite temp is `0`.

By default, maintenance helpers fail before doing work if any leaf is not a
`sorted_heap` table or if a sorted_heap leaf has no primary key. Pass `false`
as the second argument to explicitly skip unsupported leaves:

```sql
SELECT *
FROM sorted_heap_compact_partitions('events_parent'::regclass, false);
```

Locking and disk-space model:

- `sorted_heap_compact_partitions(...)` calls `sorted_heap_compact(...)` on each
  leaf, so each processed leaf takes `AccessExclusiveLock` while it is
  rewritten.
- `sorted_heap_merge_partitions(...)` calls `sorted_heap_merge(...)` on each
  leaf and has the same lock class.
- The rewrite needs temporary disk headroom for the leaf being processed, not
  for the whole logical parent at once.
- PostgreSQL does not expose a portable SQL-level free-space metric; use the
  reported tablespace fields with OS or platform monitoring when checking
  available bytes.
- These helpers are not atomic across all leaves. If a later leaf fails,
  already processed leaves remain processed.
- See [Huge-Table Compaction Operating Model](spec-huge-table-compaction) for
  the detailed rewrite/free-space contract.

`sorted_heap_partition_index_status(parent)` returns one row per leaf index
for a partitioned parent or concrete sorted_heap table. It reports the index AM,
valid/ready/live flags, primary/unique flags, and convenience booleans for
`sorted_hnsw` and btree indexes. Leaves without indexes are still represented
with `NULL` index fields, so the output can be used as a health checklist.

Manual lock-behavior smoke:

```bash
make test-partition-lock
```

### Partition-aware vector search

Use `sorted_hnsw_partition_search(...)` when vector search should run across a
partitioned parent or an explicit subset of leaves:

```sql
SELECT leaf_relid, distance, row_data
FROM sorted_hnsw_partition_search(
    'documents_parent'::regclass,
    'embedding',
    '[0.1,0.2,0.3,...]',
    top_k := 10,
    local_k := 32,
    leaf_relids := ARRAY['documents_2026_05'::regclass]);
```

Contract:

- each selected leaf runs local `ORDER BY embedding <=> query LIMIT local_k`;
- the helper unions local candidate pools and globally reranks by exact
  distance;
- `local_k` must be at least `top_k`, otherwise a dense leaf could contribute
  more true global winners than the local pool preserves;
- `local_k` must be no larger than `sorted_hnsw.ef_search`, so each local scan
  remains inside the planner-integrated `sorted_hnsw` contract;
- each selected sorted_heap leaf must have a valid, ready, non-expression,
  non-partial leaf-local `sorted_hnsw` index on `vector_column`; the helper
  fails closed instead of silently falling back to exact sort;
- every explicit `leaf_relids` entry must be a concrete leaf under `parent`;
  wrong-parent relations are rejected instead of being ignored;
- `leaf_relids` is optional. When omitted, all supported sorted_heap leaves
  under the parent participate.

The query argument is text and is cast to the actual vector type of each leaf
column (`svec` or `hsvec`). The result is intentionally generic:
`row_data jsonb` carries the source row because partitioned parents can have
arbitrary table shapes.

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

These counters are not keyed by relation. For partitioned deployments, use
`sorted_heap_partition_status(...)` and
`sorted_heap_partition_index_status(...)` for per-leaf state; do not interpret
`sorted_heap_scan_stats()` as parent-level runtime telemetry. The future
runtime-observability contract is tracked in
[Parent Runtime Observability](spec-parent-runtime-observability).

### `sorted_heap_scan_stats_by_relation()`

Returns backend-local scan statistics keyed by concrete relation:

```sql
SELECT *
FROM sorted_heap_scan_stats_by_relation();
```

Returned fields:

- `relid`: concrete relation that executed `SortedHeapScan`
- `relname`: current relation name, or `NULL` if the relation was dropped after
  the counter was recorded
- `total_scans`: number of `SortedHeapScan` executions in this backend
- `blocks_scanned`: heap blocks visited by those scans
- `blocks_pruned`: heap blocks skipped by zone-map pruning
- `source`: currently always `local`

This is the first relation-aware runtime surface. It is intentionally
backend-local and reset by `sorted_heap_reset_stats()`. It is safe to join to
`sorted_heap_partition_status(parent)` for same-backend diagnostics, but it is
not cluster-wide telemetry.

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

### `sorted_hnsw.build_sq8`

| Property | Value |
|----------|-------|
| Type | boolean |
| Default | `off` |
| Context | user (SET) |

Builds `sorted_hnsw` from SQ8-compressed build vectors instead of keeping a
full float32 build slab resident. This lowers build-time memory materially for
large indexes at the cost of an extra heap scan and possible graph-quality
loss on some corpora.

The narrow verified point so far:

- local `1M x 64D` multidepth build (`m=16`, `ef_construction=64`)
- `build_indexes`: `48.606 s -> 46.541 s`
- depth-5 unified GraphRAG stayed `87.5% / 100.0%`

The memory saving is by construction:

- float32 build slab: `4 * N * D` bytes
- SQ8 build slab: `1 * N * D` bytes

So, for example:

- `10M x 64D`: about `2.56 GiB -> 0.64 GiB`
- `10M x 384D`: about `15.36 GiB -> 3.84 GiB`

```sql
SET sorted_hnsw.build_sq8 = on;
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

## GraphRAG

The stable `0.13` GraphRAG surface is intentionally narrow: fact-shaped
retrieval over a `sorted_heap` table clustered by
`(entity_id, relation_id, target_id)`, or by an equivalent registered alias
mapping.

Stable API:

- `sorted_heap_graph_rag(...)`
- `sorted_heap_graph_register(...)`
- `sorted_heap_graph_config(...)`
- `sorted_heap_graph_unregister(...)`
- `sorted_heap_graph_rag_stats()`
- `sorted_heap_graph_rag_reset_stats()`

Beta API:

- `sorted_heap_expand_ids(...)`
- `sorted_heap_expand_rerank(...)`
- `sorted_heap_expand_twohop_rerank(...)`
- `sorted_heap_expand_twohop_path_rerank(...)`
- `sorted_heap_expand_multihop_rerank(...)`
- `sorted_heap_expand_multihop_path_rerank(...)`
- `sorted_heap_graph_rag_scan(...)`
- `sorted_heap_graph_rag_twohop_scan(...)`
- `sorted_heap_graph_rag_twohop_path_scan(...)`
- `sorted_heap_graph_rag_multihop_scan(...)`
- `sorted_heap_graph_rag_multihop_path_scan(...)`

The stable contract covers fact-shaped retrieval with explicit per-hop relation
sequences. Broader code-corpus snippet/symbol/lexical retrieval recipes remain
benchmark-side reference logic, not SQL-stable product surface.

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

Canonical fact columns work out of the box. If a fact table uses different
names, register the mapping once and then keep using the same GraphRAG API.

### `sorted_heap_graph_register(rel, entity_column, relation_column, target_column, embedding_column, payload_column)`

Registers a non-canonical fact-table schema for GraphRAG helpers and wrappers.
All mapped columns must still be type-compatible with the fact-graph contract:
`int4 / int2 / int4 / svec / text`.

```sql
SELECT sorted_heap_graph_register(
    'facts_alias'::regclass,
    entity_column := 'src_id',
    relation_column := 'edge_type',
    target_column := 'dst_id',
    embedding_column := 'vec',
    payload_column := 'body'
);
```

### `sorted_heap_graph_config(rel)`

Returns the current GraphRAG mapping for a relation, including whether it was
explicitly registered or is still using the canonical default names.

### `sorted_heap_graph_unregister(rel)`

Removes a previously registered mapping and restores the canonical-name
defaults for that relation.

### `sorted_heap_graph_rag_stats()`

Returns backend-local statistics for the last GraphRAG helper or wrapper call.
This is intended for stage-level observability while tuning seed breadth,
expansion width, and rerank cost.

Returned fields:

- `calls`: number of top-level GraphRAG calls seen by the current backend
- `api`: concrete GraphRAG execution path for the last call
- `seed_count`: ANN or explicit seed count
- `expanded_rows`: rows collected during graph expansion
- `reranked_rows`: rows considered by the exact rerank stage
- `returned_rows`: rows emitted to the caller
- `ann_ms`: ANN seed stage time in milliseconds
- `expand_ms`: graph expansion time in milliseconds
- `rerank_ms`: final rerank / emit time in milliseconds
- `total_ms`: aggregate of the recorded stages

`api` reports the top-level C execution path, not necessarily the SQL wrapper
name. For example, a unified `sorted_heap_graph_rag(...)` call with
`relation_path := ARRAY[1, 2], score_mode := 'path'` will report
`sorted_heap_graph_rag_twohop_path_scan`.

These stats are backend-local and aggregate the last top-level GraphRAG call.
They are not per-shard or per-partition telemetry.

```sql
SELECT * FROM sorted_heap_graph_rag_stats();
```

### `sorted_heap_graph_rag_reset_stats()`

Resets the backend-local GraphRAG stats counters.

```sql
SELECT sorted_heap_graph_rag_reset_stats();
```

### `sorted_heap_graph_rag(rel, query, relation_path, ann_k, top_k, score_mode, limit_rows)`

Preferred fact-shaped GraphRAG entry point.

- `relation_path := ARRAY[1]`
  - one-hop expansion
  - ANN seed on `entity_id`
  - exact rerank on the endpoint facts
  - `score_mode := 'endpoint'` and `score_mode := 'path'` are intentionally
    equivalent here because there is only one hop
- `relation_path := ARRAY[1, 2], score_mode := 'endpoint'`
  - two-hop expansion
  - rerank by the final-hop endpoint only
- `relation_path := ARRAY[1, 2], score_mode := 'path'`
  - two-hop expansion
  - path-aware rerank using hop-1 and hop-2 evidence together
- `relation_path := ARRAY[1, 2, 3, ...]`
  - multi-hop expansion
  - each array element is the relation filter for that hop
  - `score_mode := 'endpoint'` ranks only the final hop
  - `score_mode := 'path'` accumulates distance across the whole path

Current constraints:

- `relation_path` must be a non-empty one-dimensional `int4[]`
- supported `score_mode` values are `endpoint` and `path`
- `limit_rows := 0` means unlimited expansion/rerank work
- positive `limit_rows` values cap rows collected or scanned inside the
  current GraphRAG helper stages; this is a work bound, not a final result
  count override
- canonical fact columns (`entity_id`, `relation_id`, `target_id`,
  `embedding`, `payload`) need no extra setup
- non-canonical schemas must be registered first with
  `sorted_heap_graph_register(...)`
- current regression and benchmark coverage verifies the generic path-aware
  contract through synthetic depth `5`

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

### `sorted_heap_graph_rag_segmented(rels, query, relation_path, ann_k, top_k, score_mode, limit_rows)`

Beta segmented GraphRAG wrapper.

- `rels` is the candidate shard list to search
- each shard is queried via `sorted_heap_graph_rag(...)`
- shard-local rows are merged globally by `(distance, entity_id, relation_id,
  target_id)`
- `limit_rows` keeps the same per-shard work-cap semantics as
  `sorted_heap_graph_rag(...)`; it does not replace `top_k` as the final merge
  limit
- this does **not** solve routing for you; the caller still chooses which
  shard subset to query

This is the first SQL-level segmented reference path for large-scale
fact-shaped GraphRAG. It is useful when routing/pruning already exists in the
application or in metadata tables, and you want to move shard fanout/merge out
of benchmark code and into SQL.

Partitioned-parent contract:

- `sorted_heap_graph_rag(...)` is a concrete-relation API. Do not use a
  declarative partition parent as an implicit global graph.
- For partitioned or tenant-sharded facts, register each concrete leaf/shard
  with `sorted_heap_graph_register(...)` and call
  `sorted_heap_graph_route(...)` or `sorted_heap_graph_rag_segmented(...)`
  over the selected leaves.
- Fanout is explicit. `sorted_heap_graph_route_plan(...)` shows the concrete
  shard list before execution.
- The merge step is global over the routed shard-local result sets:
  `global_top_k = top_k(sort(union(shard_local_top_k)))`. It is not local
  top-k concatenation.
- `fanout_limit` narrows the selected shard list before GraphRAG execution. If
  correctness requires every possibly relevant shard, keep `fanout_limit := 0`
  or use a profile that includes all required shards.
- ANN seed retrieval, `ann_k`, and `limit_rows` remain per-shard work bounds;
  they do not become a transparent cross-partition planner path.

```sql
SELECT source_rel, entity_id, relation_id, target_id, payload, distance
FROM sorted_heap_graph_rag_segmented(
    ARRAY['facts_2025q1'::regclass, 'facts_2025q2'::regclass],
    '[0.1,0.2,0.3,...]'::svec,
    relation_path := ARRAY[1, 2],
    ann_k := 64,
    top_k := 10,
    score_mode := 'path'
);
```

### `sorted_heap_graph_segment_meta_register(rel, segment_group, relation_family, segment_labels)`

Registers shared per-shard routing metadata.

- `rel` is one concrete shard relation
- `segment_group` is optional shared shard labeling such as `hot` or `sealed`
- `relation_family` is optional shared shard labeling such as `claims`,
  `citations`, or `right`
- `segment_labels` is optional shared multi-valued shard labeling such as
  `ARRAY['sealed','archive']`
- this is mainly for reducing repeated registry data when several route rows
  point at the same shard
- if a routed row also stores `segment_group` or `relation_family`, the
  row-local value wins over the shared metadata

### `sorted_heap_graph_segment_meta_config(rel)`

Lists current shared per-shard metadata rows.

### `sorted_heap_graph_segment_meta_unregister(rel)`

Deletes one shared per-shard metadata row, or all rows when `rel` is `NULL`.

### `sorted_heap_graph_segment_register(route_name, rel, route_min, route_max, segment_group, relation_family)`

Registers a shard in the beta segment-routing registry.

- `route_name` groups shards into one logical routed graph
- `route_min` / `route_max` define an inclusive `int8` range
- `segment_group` is an optional shard label such as `hot`, `sealed`, or a
  lifecycle tier
- `relation_family` is an optional second routing label such as
  `claims`, `citations`, or `right`
- either label may be left `NULL` and resolved later from
  `sorted_heap_graph_segment_meta_registry`
- overlapping ranges are allowed

### `sorted_heap_graph_segment_config(route_name, segment_groups, relation_family, segment_labels)`

Lists the current registered shard ranges for one route group, ordered by
group, range, and relation.

- `segment_groups := NULL` means "all groups"
- non-`NULL` `text[]` filters to matching shard labels only
- `relation_family := NULL` means "all families"
- non-`NULL` `relation_family` narrows rows to one family value
- `segment_labels := NULL` means "all label sets"
- non-`NULL` `segment_labels` requires the effective shared shard labels to
  contain all supplied labels
- effective labels come from the route row first, then from
  `sorted_heap_graph_segment_meta_registry` for the same shard when the
  route-local label is `NULL`
- when `segment_groups` is non-`NULL`, its array order also becomes the
  preferred group order in the result set

### `sorted_heap_graph_segment_catalog(route_name, segment_groups, relation_family, segment_labels)`

Lists range-routed shard rows with both raw and effective metadata.

Returned fields include:

- route-local values:
  - `route_segment_group`
  - `route_relation_family`
- shared shard metadata:
  - `shared_segment_group`
  - `shared_relation_family`
  - `shared_segment_labels`
- effective resolved values:
  - `effective_segment_group`
  - `effective_relation_family`
  - `effective_segment_labels`
- source markers:
  - `segment_group_source`
  - `relation_family_source`
  - `segment_labels_source`

Source markers are one of:

- `route`
- `shared`
- `unset`

This is an introspection helper only. It does not affect shard routing or
GraphRAG scoring.

### `sorted_heap_graph_segment_resolve(route_name, route_value, fanout_limit, segment_groups, relation_family, segment_labels)`

Resolves candidate shards for a route value.

- matches rows where `route_value BETWEEN route_min AND route_max`
- optionally filters to `segment_group = ANY(segment_groups)`
- optionally filters to `relation_family = <supplied family>`
- optionally filters to shards whose shared `segment_labels` contain all
  supplied labels
- effective labels come from the route row first, then from
  `sorted_heap_graph_segment_meta_registry` for the same shard when the
  route-local label is `NULL`
- when `segment_groups` is non-`NULL`, its array order is preferred before the
  usual narrower-range ordering
- orders narrower ranges first
- `fanout_limit := 0` means "all matching shards"

### `sorted_heap_graph_segment_unregister(route_name, rel)`

Deletes one shard from a route group, or all shards for that route group when
`rel` is `NULL`.

### `sorted_heap_graph_route_policy_register(route_name, policy_name, segment_groups)`

Registers a named shard-group preference policy.

- `route_name` scopes the policy to one routed graph
- `policy_name` is an application-facing label such as `prefer_hot`
- `segment_groups` is a non-empty `text[]`
- array order is preserved and becomes the preference order used by the
  policy-backed wrappers

### `sorted_heap_graph_route_policy_config(route_name, policy_name)`

Lists registered shard-group policies for one route group.

### `sorted_heap_graph_route_policy_groups(route_name, policy_name)`

Returns the stored `segment_groups text[]` for one named policy.

- raises an error if the policy does not exist

### `sorted_heap_graph_route_policy_unregister(route_name, policy_name)`

Deletes one named policy, or all policies under that route group when
`policy_name` is `NULL`.

### `sorted_heap_graph_route_profile_register(route_name, profile_name, policy_name, segment_groups, relation_family, fanout_limit, segment_labels)`

Registers a named routed profile that bundles the current beta routing knobs.

- `route_name` scopes the profile to one routed graph
- `profile_name` is an application-facing name such as `sealed_claims`
- `policy_name` may be `NULL`; when non-`NULL` it must name a policy already
  registered under the same route
- `segment_groups` may be `NULL`; when non-`NULL` it must be a non-empty
  one-dimensional `text[]`
- `policy_name` and `segment_groups` cannot both be non-`NULL`
- `relation_family` may be `NULL`
- `fanout_limit` must be `>= 0`
- `segment_labels` may be `NULL`; when non-`NULL` it must be a non-empty
  one-dimensional `text[]`

### `sorted_heap_graph_route_profile_config(route_name, profile_name)`

Lists registered routed profiles for one route group.

### `sorted_heap_graph_route_profile_resolve(route_name, profile_name)`

Resolves one routed profile into:

- `policy_name`
- `segment_groups`
- `relation_family`
- `fanout_limit`
- `segment_labels`

This is mainly useful for inspection/testing. Query execution should normally
go through the profile-backed wrappers below.

### `sorted_heap_graph_route_profile_unregister(route_name, profile_name)`

Deletes one routed profile, or all profiles under that route group when
`profile_name` is `NULL`.

### `sorted_heap_graph_route_default_register(route_name, profile_name)`

Registers the default routed profile for one route.

- `route_name` scopes the default to one routed graph
- `profile_name` must already exist in
  `sorted_heap_graph_route_profile_registry` under the same route
- later registrations replace the previous default for that route

### `sorted_heap_graph_route_default_config(route_name)`

Lists current default-profile bindings.

### `sorted_heap_graph_route_default_resolve(route_name)`

Returns the `profile_name` currently marked as default for one route.

This is mainly useful for inspection/testing. Query execution should normally
go through the default-backed wrappers below.

### `sorted_heap_graph_route_profile_catalog(route_name, profile_name)`

Lists routed profiles with both raw and effective shard-group metadata.

Returned fields include:

- profile-local values:
  - `policy_name`
  - `inline_segment_groups`
- policy-backed values:
  - `policy_segment_groups`
- effective resolved values:
  - `effective_segment_groups`
  - `relation_family`
  - `fanout_limit`
  - `segment_labels`
- source/default markers:
  - `segment_groups_source`
  - `is_default`

`segment_groups_source` is one of:

- `inline`
- `policy`
- `unset`

This is an introspection helper only. It does not affect shard routing or
GraphRAG scoring.

### `sorted_heap_graph_route_catalog(route_name)`

Lists one summary row per route across the current segmented/routed control
plane.

Returned fields include:

- control-plane counts:
  - `range_shard_count`
  - `exact_binding_count`
  - `policy_count`
  - `profile_count`
- effective default-profile state:
  - `default_profile_name`
  - `default_effective_segment_groups`
  - `default_segment_groups_source`
  - `default_relation_family`
  - `default_fanout_limit`
  - `default_segment_labels`

This is the top-level operator summary:

- range shards are counted from `sorted_heap_graph_segment_registry`
- exact bindings are counted from `sorted_heap_graph_exact_registry`
- policies and profiles are counted from their corresponding registries
- default-profile fields come from the effective profile/default catalog layer

This is an introspection helper only. It does not affect shard routing or
GraphRAG scoring.

### `sorted_heap_graph_route_default_unregister(route_name)`

Deletes one default-profile binding, or all bindings when `route_name` is
`NULL`.

### `sorted_heap_graph_rag_routed(route_name, route_value, query, relation_path, ann_k, top_k, score_mode, limit_rows, fanout_limit, segment_groups, relation_family, segment_labels)`

Beta routed GraphRAG wrapper.

- resolves candidate shards from `sorted_heap_graph_segment_registry`
- optionally narrows those shards by `segment_group`
- optionally narrows those shards by `relation_family`
- optionally narrows those shards by shared `segment_labels`
- when `segment_groups` is non-`NULL`, its array order is the shard preference
  order before `fanout_limit` is applied
- delegates to `sorted_heap_graph_rag_segmented(...)`
- preserves the same GraphRAG scoring contract after routing

This is the first metadata-driven routing surface for segmented GraphRAG. It
does not try to infer a route from the vector query itself; the caller supplies
the route value.

```sql
SELECT sorted_heap_graph_segment_register('tenant_facts', 'facts_hot'::regclass, 1, 1000, 'hot', 'claims');
SELECT sorted_heap_graph_segment_register('tenant_facts', 'facts_cold'::regclass, 1, 1000, 'sealed', 'claims');

SELECT source_rel, entity_id, relation_id, target_id, payload, distance
FROM sorted_heap_graph_rag_routed(
    'tenant_facts',
    812,
    '[0.1,0.2,0.3,...]'::svec,
    relation_path := ARRAY[1, 2],
    ann_k := 64,
    top_k := 10,
    score_mode := 'path',
    segment_groups := ARRAY['hot'],
    relation_family := 'claims'
);
```

### `sorted_heap_graph_rag_routed_policy(route_name, route_value, policy_name, query, relation_path, ann_k, top_k, score_mode, limit_rows, fanout_limit, relation_family, segment_labels)`

Beta routed GraphRAG wrapper with registry-backed shard-group policy lookup.

- resolves `segment_groups` from `sorted_heap_graph_route_policy_registry`
- delegates to `sorted_heap_graph_rag_routed(...)`
- can still add a per-query `relation_family` filter on top of the stored
  shard-group order
- can also add a per-query `segment_labels` filter on top of the stored
  shard-group order
- keeps the same routing and GraphRAG scoring semantics

```sql
SELECT sorted_heap_graph_route_policy_register(
    'tenant_facts',
    'prefer_hot',
    ARRAY['hot', 'sealed']
);

SELECT source_rel, entity_id, relation_id, target_id, payload, distance
FROM sorted_heap_graph_rag_routed_policy(
    'tenant_facts',
    812,
    'prefer_hot',
    '[0.1,0.2,0.3,...]'::svec,
    relation_path := ARRAY[1, 2],
    ann_k := 64,
    top_k := 10,
    score_mode := 'path',
    fanout_limit := 1,
    relation_family := 'claims'
);
```

### `sorted_heap_graph_rag_routed_profile(route_name, route_value, profile_name, query, relation_path, ann_k, top_k, score_mode, limit_rows)`

Beta routed GraphRAG wrapper with registry-backed profile lookup.

- resolves `policy_name`, `relation_family`, `fanout_limit`, and optional
  `segment_labels` from
  `sorted_heap_graph_route_profile_registry`
- delegates to `sorted_heap_graph_rag_routed(...)` with inline
  `segment_groups` when the profile stores them directly
- otherwise delegates to `sorted_heap_graph_rag_routed_policy(...)` when the
  profile names a policy
- otherwise delegates to `sorted_heap_graph_rag_routed(...)`
- keeps the same routing and GraphRAG scoring semantics

```sql
SELECT sorted_heap_graph_route_profile_register(
    'tenant_facts',
    'sealed_claims',
    NULL,
    ARRAY['sealed', 'hot'],
    'claims',
    1
);

SELECT source_rel, entity_id, relation_id, target_id, payload, distance
FROM sorted_heap_graph_rag_routed_profile(
    'tenant_facts',
    812,
    'sealed_claims',
    '[0.1,0.2,0.3,...]'::svec,
    relation_path := ARRAY[1, 2],
    ann_k := 64,
    top_k := 10,
    score_mode := 'path'
);
```

### `sorted_heap_graph_rag_routed_default(route_name, route_value, query, relation_path, ann_k, top_k, score_mode, limit_rows)`

Beta routed GraphRAG wrapper with registry-backed default-profile lookup.

- resolves `profile_name` from `sorted_heap_graph_route_default_registry`
- delegates to `sorted_heap_graph_rag_routed_profile(...)`
- keeps the same routing and GraphRAG scoring semantics

```sql
SELECT sorted_heap_graph_route_default_register(
    'tenant_facts',
    'sealed_claims'
);

SELECT source_rel, entity_id, relation_id, target_id, payload, distance
FROM sorted_heap_graph_rag_routed_default(
    'tenant_facts',
    812,
    '[0.1,0.2,0.3,...]'::svec,
    relation_path := ARRAY[1, 2],
    ann_k := 64,
    top_k := 10,
    score_mode := 'path'
);
```

### `sorted_heap_graph_route(route_name, query, relation_path, route_key, route_value, profile_name, policy_name, ann_k, top_k, score_mode, limit_rows, fanout_limit, segment_groups, relation_family, segment_labels)`

Unified routed GraphRAG entry point over the existing beta exact/range
wrappers.

Resolution order:

1. exactly one of `route_key` or `route_value`
2. at most one of `profile_name` or `policy_name`
3. explicit `profile_name`
4. explicit `policy_name`
5. explicit call-site routing overrides
   - `fanout_limit`
   - `segment_groups`
   - `relation_family`
   - `segment_labels`
6. route default profile, if one exists
7. base exact/range routed wrapper

Important constraints:

- `profile_name` cannot be combined with call-site routing overrides
- `policy_name` cannot be combined with `segment_groups`
- defaults never override explicit call-site routing knobs
- this wrapper reuses the existing routed GraphRAG paths; it does not define
  a new GraphRAG scoring model
- this is the recommended parent/shard fanout API for GraphRAG; declarative
  partition parents should be resolved to concrete leaves first rather than
  passed as implicit global GraphRAG relations

```sql
SELECT source_rel, entity_id, relation_id, target_id, payload, distance
FROM sorted_heap_graph_route(
    'tenant_facts',
    '[0.1,0.2,0.3,...]'::svec,
    relation_path := ARRAY[1, 2],
    route_key := 'kb_alpha',
    ann_k := 64,
    top_k := 10,
    score_mode := 'path'
);
```

### `sorted_heap_graph_route_plan(route_name, route_key, route_value, profile_name, policy_name, fanout_limit, segment_groups, relation_family, segment_labels)`

Explain helper for `sorted_heap_graph_route(...)`.

Returns:

- `route_kind`
- `resolution_path`
- `used_profile_name`
- `used_policy_name`
- `used_default`
- `effective_fanout_limit`
- `effective_segment_groups`
- `effective_relation_family`
- `effective_segment_labels`
- `candidate_shards`

This function uses the same routing precedence as `sorted_heap_graph_route(...)`
but does not execute GraphRAG.

```sql
SELECT *
FROM sorted_heap_graph_route_plan(
    'tenant_facts',
    route_key := 'kb_alpha'
);
```

### `sorted_heap_graph_exact_register(route_name, route_key, rel, priority, segment_group, relation_family)`

Registers an exact-key shard mapping in the beta exact-routing registry.

- `route_name` groups shards into one logical routed graph
- `route_key` is an exact text key such as a tenant id or knowledge-base id
- multiple shards may share the same key
- `priority` orders those shards when one key fans out to several shards
- `segment_group` is an optional shard label such as `hot` or `sealed`
- `relation_family` is an optional second routing label such as
  `claims`, `citations`, or `right`
- either label may be left `NULL` and resolved later from
  `sorted_heap_graph_segment_meta_registry`

### `sorted_heap_graph_exact_config(route_name, route_key, segment_groups, relation_family, segment_labels)`

Lists the current exact-key shard mappings ordered by
`(route_name, route_key, priority desc, segment_group, relation_family, rel)`.

- when `segment_groups` is non-`NULL`, its array order becomes the preferred
  group order before per-shard priority
- `relation_family := NULL` means "all families"
- non-`NULL` `relation_family` narrows rows to one family value
- `segment_labels := NULL` means "all label sets"
- non-`NULL` `segment_labels` requires the effective shared shard labels to
  contain all supplied labels
- effective labels come from the exact-route row first, then from
  `sorted_heap_graph_segment_meta_registry` for the same shard when the
  route-local label is `NULL`

### `sorted_heap_graph_exact_catalog(route_name, route_key, segment_groups, relation_family, segment_labels)`

Lists exact-key routed shard rows with both raw and effective metadata.

Returned fields include:

- route-local values:
  - `route_segment_group`
  - `route_relation_family`
- shared shard metadata:
  - `shared_segment_group`
  - `shared_relation_family`
  - `shared_segment_labels`
- effective resolved values:
  - `effective_segment_group`
  - `effective_relation_family`
  - `effective_segment_labels`
- source markers:
  - `segment_group_source`
  - `relation_family_source`
  - `segment_labels_source`

Source markers are one of:

- `route`
- `shared`
- `unset`

This is an introspection helper only. It does not affect shard routing or
GraphRAG scoring.

### `sorted_heap_graph_exact_resolve(route_name, route_key, fanout_limit, segment_groups, relation_family, segment_labels)`

Resolves candidate shards for an exact route key.

- matches rows where `route_key = <supplied key>`
- optionally filters to `segment_group = ANY(segment_groups)`
- optionally filters to `relation_family = <supplied family>`
- optionally filters to shards whose shared `segment_labels` contain all
  supplied labels
- effective labels come from the exact-route row first, then from
  `sorted_heap_graph_segment_meta_registry` for the same shard when the
  route-local label is `NULL`
- when `segment_groups` is non-`NULL`, its array order is preferred before the
  usual `priority DESC` ordering
- orders by `priority DESC, rel`
- `fanout_limit := 0` means "all matching shards"

### `sorted_heap_graph_exact_unregister(route_name, route_key, rel)`

Deletes exact-key shard mappings.

- `route_key := NULL` deletes every key under that route group
- `rel := NULL` deletes all matching shard rows for the selected route/key

### `sorted_heap_graph_rag_routed_exact(route_name, route_key, query, relation_path, ann_k, top_k, score_mode, limit_rows, fanout_limit, segment_groups, relation_family, segment_labels)`

Beta exact-key routed GraphRAG wrapper.

- resolves candidate shards from `sorted_heap_graph_exact_registry`
- optionally narrows those shards by `segment_group`
- optionally narrows those shards by `relation_family`
- optionally narrows those shards by shared `segment_labels`
- when `segment_groups` is non-`NULL`, its array order is the shard preference
  order before `fanout_limit` is applied
- delegates to `sorted_heap_graph_rag_segmented(...)`
- keeps the same GraphRAG scoring contract after routing

This is the stronger fit for tenant-id / knowledge-base-id routing than the
range-based wrapper.

```sql
SELECT sorted_heap_graph_exact_register('tenant_facts', 'kb_alpha', 'facts_hot'::regclass, 100, 'hot', 'claims');
SELECT sorted_heap_graph_exact_register('tenant_facts', 'kb_alpha', 'facts_cold'::regclass, 50, 'sealed', 'claims');

SELECT source_rel, entity_id, relation_id, target_id, payload, distance
FROM sorted_heap_graph_rag_routed_exact(
    'tenant_facts',
    'kb_alpha',
    '[0.1,0.2,0.3,...]'::svec,
    relation_path := ARRAY[1, 2],
    ann_k := 64,
    top_k := 10,
    score_mode := 'path',
    segment_groups := ARRAY['hot'],
    relation_family := 'claims'
);
```

### `sorted_heap_graph_rag_routed_exact_policy(route_name, route_key, policy_name, query, relation_path, ann_k, top_k, score_mode, limit_rows, fanout_limit, relation_family, segment_labels)`

Beta exact-key routed GraphRAG wrapper with registry-backed shard-group policy
lookup.

- resolves `segment_groups` from `sorted_heap_graph_route_policy_registry`
- delegates to `sorted_heap_graph_rag_routed_exact(...)`
- can still add a per-query `relation_family` filter on top of the stored
  shard-group order
- can also add a per-query `segment_labels` filter on top of the stored
  shard-group order
- keeps the same routing and GraphRAG scoring semantics

```sql
SELECT sorted_heap_graph_route_policy_register(
    'tenant_facts',
    'prefer_hot',
    ARRAY['hot', 'sealed']
);

SELECT source_rel, entity_id, relation_id, target_id, payload, distance
FROM sorted_heap_graph_rag_routed_exact_policy(
    'tenant_facts',
    'kb_alpha',
    'prefer_hot',
    '[0.1,0.2,0.3,...]'::svec,
    relation_path := ARRAY[1, 2],
    ann_k := 64,
    top_k := 10,
    score_mode := 'path',
    fanout_limit := 1,
    relation_family := 'claims'
);
```

### `sorted_heap_graph_rag_routed_exact_profile(route_name, route_key, profile_name, query, relation_path, ann_k, top_k, score_mode, limit_rows)`

Beta exact-key routed GraphRAG wrapper with registry-backed profile lookup.

- resolves `policy_name`, `relation_family`, `fanout_limit`, and optional
  `segment_labels` from
  `sorted_heap_graph_route_profile_registry`
- delegates to `sorted_heap_graph_rag_routed_exact(...)` with inline
  `segment_groups` when the profile stores them directly
- otherwise delegates to `sorted_heap_graph_rag_routed_exact_policy(...)`
  when the profile names a policy
- otherwise delegates to `sorted_heap_graph_rag_routed_exact(...)`
- keeps the same routing and GraphRAG scoring semantics

```sql
SELECT sorted_heap_graph_route_profile_register(
    'tenant_facts',
    'sealed_claims',
    NULL,
    ARRAY['sealed', 'hot'],
    'claims',
    1
);

SELECT source_rel, entity_id, relation_id, target_id, payload, distance
FROM sorted_heap_graph_rag_routed_exact_profile(
    'tenant_facts',
    'kb_alpha',
    'sealed_claims',
    '[0.1,0.2,0.3,...]'::svec,
    relation_path := ARRAY[1, 2],
    ann_k := 64,
    top_k := 10,
    score_mode := 'path'
);
```

### `sorted_heap_graph_rag_routed_exact_default(route_name, route_key, query, relation_path, ann_k, top_k, score_mode, limit_rows)`

Beta exact-key routed GraphRAG wrapper with registry-backed default-profile
lookup.

- resolves `profile_name` from `sorted_heap_graph_route_default_registry`
- delegates to `sorted_heap_graph_rag_routed_exact_profile(...)`
- keeps the same routing and GraphRAG scoring semantics

```sql
SELECT sorted_heap_graph_route_default_register(
    'tenant_facts',
    'sealed_claims'
);

SELECT source_rel, entity_id, relation_id, target_id, payload, distance
FROM sorted_heap_graph_rag_routed_exact_default(
    'tenant_facts',
    'kb_alpha',
    '[0.1,0.2,0.3,...]'::svec,
    relation_path := ARRAY[1, 2],
    ann_k := 64,
    top_k := 10,
    score_mode := 'path'
);
```

### Lower-level GraphRAG building blocks (beta)

### `sorted_heap_expand_ids(rel, seed_ids, relation_filter, limit_rows)`

Expands known entity seeds into fact rows without reranking.

- `limit_rows := 0` means unlimited
- positive `limit_rows` stops expansion after that many matching rows have
  been collected

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

- `limit_rows` caps the expansion and rerank scan work before top-k selection
- `top_k` still controls the final output size

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

- `limit_rows` caps work in the hop-expansion and final rerank scan stages

### `sorted_heap_expand_twohop_path_rerank(rel, seed_ids, query, top_k, hop1_relation_filter, hop2_relation_filter, limit_rows)`

Two-hop expansion with path-aware rerank using hop-1 and hop-2 evidence
together. This is the stronger current contract for fact-shaped multihop
retrieval.

- `limit_rows` caps work in the hop-expansion and final path-rerank scan
  stages

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

- `limit_rows` keeps the same work-cap semantics as the lower-level helpers

### `sorted_heap_graph_rag_twohop_scan(rel, query, ann_k, top_k, hop1_relation_filter, hop2_relation_filter, limit_rows)`

Lower-level endpoint-scored two-hop wrapper. `sorted_heap_graph_rag(...)`
with `relation_path := ARRAY[hop1, hop2], score_mode := 'endpoint'`
is the preferred higher-level syntax.

- `limit_rows` keeps the same work-cap semantics as the underlying helper path

### `sorted_heap_graph_rag_twohop_path_scan(rel, query, ann_k, top_k, hop1_relation_filter, hop2_relation_filter, limit_rows)`

Lower-level path-aware two-hop wrapper. `sorted_heap_graph_rag(...)` with
`relation_path := ARRAY[hop1, hop2], score_mode := 'path'` is the preferred
higher-level syntax.

- `limit_rows` keeps the same work-cap semantics as the underlying helper path

### `sorted_heap_expand_multihop_rerank(rel, seed_ids, query, top_k, relation_path, limit_rows)`

Lower-level endpoint-scored multi-hop helper. `relation_path` is the explicit
per-hop relation sequence.

- `limit_rows` caps work at each hop expansion plus the final rerank scan

### `sorted_heap_expand_multihop_path_rerank(rel, seed_ids, query, top_k, relation_path, limit_rows)`

Lower-level path-aware multi-hop helper. This accumulates distance across the
full explicit `relation_path`.

- `limit_rows` caps work at each hop expansion plus the final path-rerank scan

### `sorted_heap_graph_rag_multihop_scan(rel, query, ann_k, top_k, relation_path, limit_rows)`

Lower-level ANN-seeded multi-hop wrapper for endpoint-scored retrieval.

- `limit_rows` keeps the same work-cap semantics as the underlying helper path

### `sorted_heap_graph_rag_multihop_path_scan(rel, query, ann_k, top_k, relation_path, limit_rows)`

Lower-level ANN-seeded multi-hop wrapper for path-aware retrieval.

- `limit_rows` keeps the same work-cap semantics as the underlying helper path

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

---

## Routed GraphRAG: operator recipe

This section describes the recommended app-facing flow for multi-shard
GraphRAG with routing. Use `sorted_heap_graph_route(...)` for queries and
`sorted_heap_graph_route_plan(...)` for inspection. The lower-level
`_routed`, `_routed_exact`, `_routed_policy`, `_routed_profile`, and
`_routed_default` wrappers remain lower-level beta building blocks.

### What to call in an app

| Task | Function |
|------|----------|
| **Query** (app code) | `sorted_heap_graph_route(...)` |
| **Inspect routing** (operator/debug) | `sorted_heap_graph_route_plan(...)` |
| Setup: register shard metadata | `sorted_heap_graph_register(...)` per shard |
| Setup: register exact routes | `sorted_heap_graph_exact_register(...)` |
| Setup: register range routes | `sorted_heap_graph_segment_register(...)` |
| Setup: optional profile/default | `sorted_heap_graph_route_profile_register(...)` + `sorted_heap_graph_route_default_register(...)` |

### Exact-key routing (tenant / knowledge-base)

**Setup (once per deployment):**

```sql
-- 1. Create shard tables with HNSW indexes
CREATE TABLE facts_shard_a (...) USING sorted_heap;
CREATE TABLE facts_shard_b (...) USING sorted_heap;
-- load data, compact, create sorted_hnsw indexes, ANALYZE

-- 2. Register graph schema on each shard
SELECT sorted_heap_graph_register('facts_shard_a'::regclass,
  entity_column := 'entity_id', relation_column := 'relation_id',
  target_column := 'target_id', embedding_column := 'embedding',
  payload_column := 'payload');
SELECT sorted_heap_graph_register('facts_shard_b'::regclass, ...);

-- 3. Map tenant keys to shards
SELECT sorted_heap_graph_exact_register('tenants', 'acme',
  'facts_shard_a'::regclass, 100);
SELECT sorted_heap_graph_exact_register('tenants', 'globex',
  'facts_shard_b'::regclass, 100);
```

**Query (app code):**

```sql
SELECT entity_id, relation_id, target_id, payload, distance
FROM sorted_heap_graph_route(
  'tenants',                        -- route name
  query_embedding,                  -- svec
  ARRAY[1, 2],                      -- relation path (2-hop)
  route_key := 'acme',              -- tenant key
  ann_k := 64,
  top_k := 10,
  score_mode := 'path'
);
```

**Inspect (operator):**

```sql
SELECT * FROM sorted_heap_graph_route_plan(
  'tenants', route_key := 'acme');
-- route_kind | resolution_path | candidate_shards
-- exact      | base            | {facts_shard_a}
```

### Range routing (time-window / ID-range)

**Setup:**

```sql
-- Map ID ranges to shards
SELECT sorted_heap_graph_segment_register('id_range',
  'facts_shard_a'::regclass, 1, 100000);
SELECT sorted_heap_graph_segment_register('id_range',
  'facts_shard_b'::regclass, 100001, 200000);
```

**Query:**

```sql
SELECT entity_id, relation_id, target_id, payload, distance
FROM sorted_heap_graph_route(
  'id_range',
  query_embedding,
  ARRAY[1, 2],
  route_value := 42000,             -- routes to shard_a
  ann_k := 64,
  top_k := 10
);
```

### Adding a default profile

Profiles bundle routing knobs so app code doesn't need to pass them:

```sql
-- Register a profile that filters by segment group and relation family
SELECT sorted_heap_graph_route_profile_register(
  'tenants', 'production',
  NULL,                             -- no policy
  ARRAY['active'],                  -- segment_groups filter
  'core',                           -- relation_family
  0,                                -- fanout_limit
  ARRAY['verified']                 -- segment_labels
);

-- Set as the default for this route
SELECT sorted_heap_graph_route_default_register('tenants', 'production');
```

Now `sorted_heap_graph_route('tenants', ..., route_key := 'acme')` will
automatically use the 'production' profile (step 6 in the resolution
order) unless the caller explicitly provides a different profile, policy,
or routing overrides.

### Lower-level building blocks (not recommended for app code)

The following functions are internal dispatch targets used by
`sorted_heap_graph_route(...)`. They remain available for advanced use
cases but are not the recommended app entry point:

- `sorted_heap_graph_rag_routed(...)` / `sorted_heap_graph_rag_routed_exact(...)`
- `sorted_heap_graph_rag_routed_policy(...)` / `sorted_heap_graph_rag_routed_exact_policy(...)`
- `sorted_heap_graph_rag_routed_profile(...)` / `sorted_heap_graph_rag_routed_exact_profile(...)`
- `sorted_heap_graph_rag_routed_default(...)` / `sorted_heap_graph_rag_routed_exact_default(...)`
- `sorted_heap_graph_rag_segmented(...)`
