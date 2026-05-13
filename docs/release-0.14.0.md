---
layout: default
title: Release 0.14.0
nav_order: 10
---

# Release 0.14.0

This release focuses on insertion locality and safe metadata-only pruning
helpers.

## Highlights

- `clustered_pk_index` now accepts one or two integer key columns for
  directed-placement workloads.
- `clustered_heap` insert placement keys its backend-local block map by the
  full one- or two-column key, so `(tenant_id, id)` style layouts are no longer
  collapsed to the first column.
- `sorted_heap_zonemap_may_match_int8(regclass, bigint, bigint)` adds a
  fail-open metadata probe for first-key range overlap. It returns `false`
  only when valid zone-map metadata proves an `int8` range cannot match.
- Append-zone run acceleration is documented as a future proof-bearing
  contract rather than implemented unsafely from page min/max metadata.

## Upgrade

```text
ALTER EXTENSION pg_sorted_heap UPDATE TO '0.14.0';
```

The upgrade adds the metadata-only zone-map probe. Existing tables do not need
rewrites for the new helper.

## Verification

The release path is covered by:

- `make installcheck REGRESS='pg_sorted_heap sorted_hnsw graph_rag'`
- `make test-graphrag-release`
