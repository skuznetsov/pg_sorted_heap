---
layout: default
title: Stable API
nav_order: 4
---

# Stable API

This page lists the compact application-facing API. For every function,
configuration knob, and lower-level helper, use the full [SQL API Reference](api).

## Storage and maintenance

```sql
CREATE TABLE events (
    id bigint PRIMARY KEY,
    payload text
) USING sorted_heap;
```

| API | Use |
|---|---|
| `sorted_heap_compact(regclass)` | Offline full rewrite into globally sorted PK order |
| `sorted_heap_compact_online(regclass)` | Online compaction with trigger-based change replay |
| `sorted_heap_merge(regclass)` | Offline sorted-prefix plus unsorted-tail merge |
| `sorted_heap_merge_online(regclass)` | Online merge |
| `sorted_heap_bulk_load_ordered(regclass, source_sql, order_by, analyze_after := false, key_columns := NULL)` | Trusted ordered bulk ingestion with observational run witness |
| `sorted_heap_rebuild_zonemap(regclass)` | Manual zone-map rebuild |

## Partition maintenance

| API | Use |
|---|---|
| `sorted_heap_partition_status(parent)` | Check concrete leaves under a partitioned parent |
| `sorted_heap_partition_index_status(parent)` | Check leaf index state |
| `sorted_heap_partition_maintenance_plan(parent, operation)` | Read-only lock and rewrite estimate |
| `sorted_heap_compact_partitions(parent, fail_on_unsupported := true)` | Compact concrete sorted_heap leaves |
| `sorted_heap_merge_partitions(parent, fail_on_unsupported := true)` | Merge concrete sorted_heap leaves |
| `sorted_heap_rebuild_zonemap_partitions(parent, fail_on_unsupported := true)` | Rebuild leaf zone maps |

## Observability

| API | Use |
|---|---|
| `sorted_heap_zonemap_stats(regclass)` | Inspect zone-map validity and coverage |
| `sorted_heap_zonemap_may_match_int8(regclass, bigint, bigint)` | Fail-open metadata probe for first-key int8 range overlap |
| `sorted_heap_append_run_status(regclass default NULL)` | Inspect ordered bulk-load append-run witnesses and stale/current status |
| `sorted_heap_append_run_plan(regclass)` | Dry-run append-run summary; never authorizes merge in this release |
| `sorted_heap_append_run_invalidate(regclass default NULL)` | Mark append-run witnesses invalid |
| `sorted_heap_append_run_cleanup(regclass default NULL)` | Delete invalid, relfilenode-stale, or orphaned append-run witnesses |
| `sorted_heap_restore_plan(parent default NULL)` | Post-restore maintenance checklist |
| `sorted_heap_scan_stats()` | Shared/global scan pruning counters |
| `sorted_heap_scan_stats_by_relation()` | Per-relation scan pruning counters |
| `sorted_heap_partition_scan_stats(parent)` | Partition rollup for scan pruning counters |
| `sorted_heap_reset_stats()` | Reset sorted_heap scan counters |

## Vector search

```sql
CREATE TABLE documents (
    id bigint PRIMARY KEY,
    embedding svec(384),
    content text
);

CREATE INDEX documents_embedding_idx
ON documents USING sorted_hnsw (embedding)
WITH (m = 16, ef_construction = 200);

SELECT id, content
FROM documents
ORDER BY embedding <=> '[0.1,0.2,0.3,...]'::svec
LIMIT 10;
```

| API | Use |
|---|---|
| `svec(dim)` | Float32 vector storage |
| `hsvec(dim)` | Float16 vector storage |
| `sorted_hnsw` | Planner-integrated KNN Index AM |
| `sorted_hnsw_scan_stats()` | Backend-local ANN/top-up/exact-fallback attribution |
| `sorted_hnsw_reset_stats()` | Reset backend-local sorted_hnsw scan counters |

## Stable GraphRAG

```sql
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

| API | Use |
|---|---|
| `sorted_heap_graph_register(...)` | Register non-canonical fact column names |
| `sorted_heap_graph_config(regclass)` | Inspect fact-graph registration |
| `sorted_heap_graph_unregister(regclass)` | Remove fact-graph registration |
| `sorted_heap_graph_rag(...)` | Stable fact-shaped GraphRAG query |
| `sorted_heap_graph_rag_stats()` | GraphRAG execution counters |
| `sorted_heap_graph_rag_reset_stats()` | Reset GraphRAG counters |

## Routed GraphRAG

| API | Use |
|---|---|
| `sorted_heap_graph_route(...)` | Stable routed query dispatcher |
| `sorted_heap_graph_route_plan(...)` | Explain which route/default/profile/policy would be used |
| `sorted_heap_graph_route_last_stats()` | Last routed GraphRAG execution summary |

Canonical setup helpers:

| API family | Use |
|---|---|
| `sorted_heap_graph_exact_*` | Exact-key route registration |
| `sorted_heap_graph_segment_*` | Range/segment route registration |
| `sorted_heap_graph_route_profile_*` | Profile registration and resolution |
| `sorted_heap_graph_route_default_*` | Default profile registration |
| `sorted_heap_graph_route_policy_*` | Policy/group registration |
| `sorted_heap_graph_segment_meta_*` | Segment metadata registration |

## Stable configuration

| GUC | Default | Use |
|---|---:|---|
| `sorted_heap.enable_scan_pruning` | `on` | Enable sorted_heap custom scan pruning |
| `sorted_heap.vacuum_rebuild_zonemap` | `off` | Rebuild zone maps during VACUUM |
| `sorted_heap.lazy_update` | `off` | Defer costly eager update maintenance |
| `sorted_hnsw.ef_search` | `64` | Runtime HNSW search breadth |
| `sorted_hnsw.shared_cache` | `on` | Shared decoded graph cache when preloaded |
| `sorted_hnsw.sq8` | `on` | SQ8 decoded cache representation |
| `sorted_hnsw.build_sq8` | `off` | Low-memory index build mode |
