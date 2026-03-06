---
layout: default
title: Limitations
nav_order: 7
---

# Limitations

## Supported PK types

The zone map tracks the first **two** PK columns. Supported types:

| Type | Zone map support |
|------|-----------------|
| int2, int4, int8 | Full |
| timestamp, timestamptz | Full |
| date | Full |
| uuid | Lossy (first 8 bytes; UUIDs sharing a prefix may not be pruned) |
| text, varchar | Lossy, requires `COLLATE "C"` (byte order must equal sort order) |

Non-C collation text/varchar columns are not tracked. The table still works
but queries on those columns will not benefit from scan pruning.

---

## Online compact/merge restrictions

`sorted_heap_compact_online` and `sorted_heap_merge_online` are **not
supported** for tables with UUID, text, or varchar primary keys. The lossy
int64 hash representation causes collisions during change replay.

Use the offline variants (`sorted_heap_compact`, `sorted_heap_merge`) instead.

---

## UPDATE behavior

UPDATE does not re-sort tuples. After many updates, the physical order may
drift from PK order. Run `sorted_heap_compact` or `sorted_heap_merge`
periodically on write-heavy tables.

---

## Zone map validity

- After compact or rebuild, the zone map is marked **valid** and scan pruning
  is active.
- Single-row INSERTs into pages already covered by the zone map update it
  in place (pruning stays active).
- INSERTs into pages beyond zone map coverage invalidate the flag. VACUUM
  with `sorted_heap.vacuum_rebuild_zonemap = on` (default) automatically
  rebuilds it.

---

## Block range pruning

`heap_setscanlimits()` supports only **contiguous** block ranges. For
non-contiguous distributions (e.g., after many random inserts without
compaction), the scan reads intervening pages but skips tuple processing
on pages outside the bounds.

---

## Locking

| Operation | Lock level |
|-----------|-----------|
| `sorted_heap_compact` | AccessExclusiveLock (blocks all access) |
| `sorted_heap_merge` | AccessExclusiveLock |
| `sorted_heap_compact_online` | ShareUpdateExclusiveLock during copy; brief AccessExclusiveLock for swap |
| `sorted_heap_merge_online` | Same as compact\_online |

Only one online compact/merge can run on a table at a time. A second
concurrent attempt will fail.

---

## Data migration

- **pg_dump / pg_restore:** the zone map needs a compact after restore to
  re-enable scan pruning.
- **pg_upgrade 17 to 18:** tested and verified. Data files (including zone map)
  are copied as-is.

---

## ALTER TABLE

Most ALTER TABLE operations work correctly:

| Operation | Zone map impact |
|-----------|----------------|
| ADD COLUMN | No impact |
| DROP COLUMN (non-PK) | No impact |
| RENAME COLUMN | No impact (including PK columns) |
| ALTER TYPE (non-PK) | Table rewrite; compact restores zone map |
| DROP PRIMARY KEY | Disables pruning; re-add PK + compact to restore |
