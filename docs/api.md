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
- this does **not** solve routing for you; the caller still chooses which
  shard subset to query

This is the first SQL-level segmented reference path for large-scale
fact-shaped GraphRAG. It is useful when routing/pruning already exists in the
application or in metadata tables, and you want to move shard fanout/merge out
of benchmark code and into SQL.

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

### `sorted_heap_graph_segment_register(route_name, rel, route_min, route_max)`

Registers a shard in the beta segment-routing registry.

- `route_name` groups shards into one logical routed graph
- `route_min` / `route_max` define an inclusive `int8` range
- overlapping ranges are allowed

### `sorted_heap_graph_segment_config(route_name)`

Lists the current registered shard ranges for one route group, ordered by
range and relation.

### `sorted_heap_graph_segment_resolve(route_name, route_value, fanout_limit)`

Resolves candidate shards for a route value.

- matches rows where `route_value BETWEEN route_min AND route_max`
- orders narrower ranges first
- `fanout_limit := 0` means "all matching shards"

### `sorted_heap_graph_segment_unregister(route_name, rel)`

Deletes one shard from a route group, or all shards for that route group when
`rel` is `NULL`.

### `sorted_heap_graph_rag_routed(route_name, route_value, query, relation_path, ann_k, top_k, score_mode, limit_rows, fanout_limit)`

Beta routed GraphRAG wrapper.

- resolves candidate shards from `sorted_heap_graph_segment_registry`
- delegates to `sorted_heap_graph_rag_segmented(...)`
- preserves the same GraphRAG scoring contract after routing

This is the first metadata-driven routing surface for segmented GraphRAG. It
does not try to infer a route from the vector query itself; the caller supplies
the route value.

```sql
SELECT sorted_heap_graph_segment_register('tenant_facts', 'facts_t1'::regclass, 1, 1000);
SELECT sorted_heap_graph_segment_register('tenant_facts', 'facts_t2'::regclass, 1001, 2000);

SELECT source_rel, entity_id, relation_id, target_id, payload, distance
FROM sorted_heap_graph_rag_routed(
    'tenant_facts',
    812,
    '[0.1,0.2,0.3,...]'::svec,
    relation_path := ARRAY[1, 2],
    ann_k := 64,
    top_k := 10,
    score_mode := 'path'
);
```

### `sorted_heap_graph_exact_register(route_name, route_key, rel, priority)`

Registers an exact-key shard mapping in the beta exact-routing registry.

- `route_name` groups shards into one logical routed graph
- `route_key` is an exact text key such as a tenant id or knowledge-base id
- multiple shards may share the same key
- `priority` orders those shards when one key fans out to several shards

### `sorted_heap_graph_exact_config(route_name, route_key)`

Lists the current exact-key shard mappings ordered by
`(route_name, route_key, priority desc, rel)`.

### `sorted_heap_graph_exact_resolve(route_name, route_key, fanout_limit)`

Resolves candidate shards for an exact route key.

- matches rows where `route_key = <supplied key>`
- orders by `priority DESC, rel`
- `fanout_limit := 0` means "all matching shards"

### `sorted_heap_graph_exact_unregister(route_name, route_key, rel)`

Deletes exact-key shard mappings.

- `route_key := NULL` deletes every key under that route group
- `rel := NULL` deletes all matching shard rows for the selected route/key

### `sorted_heap_graph_rag_routed_exact(route_name, route_key, query, relation_path, ann_k, top_k, score_mode, limit_rows, fanout_limit)`

Beta exact-key routed GraphRAG wrapper.

- resolves candidate shards from `sorted_heap_graph_exact_registry`
- delegates to `sorted_heap_graph_rag_segmented(...)`
- keeps the same GraphRAG scoring contract after routing

This is the stronger fit for tenant-id / knowledge-base-id routing than the
range-based wrapper.

```sql
SELECT sorted_heap_graph_exact_register('tenant_facts', 'kb_alpha', 'facts_hot'::regclass, 100);
SELECT sorted_heap_graph_exact_register('tenant_facts', 'kb_alpha', 'facts_cold'::regclass, 50);

SELECT source_rel, entity_id, relation_id, target_id, payload, distance
FROM sorted_heap_graph_rag_routed_exact(
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

### `sorted_heap_expand_multihop_rerank(rel, seed_ids, query, top_k, relation_path, limit_rows)`

Lower-level endpoint-scored multi-hop helper. `relation_path` is the explicit
per-hop relation sequence.

### `sorted_heap_expand_multihop_path_rerank(rel, seed_ids, query, top_k, relation_path, limit_rows)`

Lower-level path-aware multi-hop helper. This accumulates distance across the
full explicit `relation_path`.

### `sorted_heap_graph_rag_multihop_scan(rel, query, ann_k, top_k, relation_path, limit_rows)`

Lower-level ANN-seeded multi-hop wrapper for endpoint-scored retrieval.

### `sorted_heap_graph_rag_multihop_path_scan(rel, query, ann_k, top_k, relation_path, limit_rows)`

Lower-level ANN-seeded multi-hop wrapper for path-aware retrieval.

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
