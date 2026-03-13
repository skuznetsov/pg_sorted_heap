# S1: Storage Redesign — Persisted Sorted Prefix

## Problem Statement

After compaction (`sorted_heap_merge` / `sorted_heap_merge_online`), the entire
table is physically sorted by PK. Subsequent INSERTs append new pages to the
end, breaking monotonicity only at the boundary between "compacted" and "new"
pages. The current code has no memory of where that boundary is.

### Current behavior

1. `sorted_heap_detect_sorted_prefix` (sorted_heap.c:2354) scans **all** zone
   map entries from the start, checking `entry[i+1].min >= entry[i].max`. This
   is O(n_entries) and re-derived every time `merge` runs.

2. `SHM_FLAG_ZM_SORTED` is cleared on **every** INSERT flush
   (sorted_heap.c:991), even if the insert only appended pages beyond the
   existing sorted prefix. This pessimistically disables binary search scan
   pruning for the entire table.

3. `SHM_FLAG_ZONEMAP_VALID` (scan pruning flag) is also cleared by INSERTs
   that can't be tracked in the zone map (overflow). This disables all
   zone-map-based scan pruning until the next VACUUM rebuild.

4. The merge path sorts the **entire tail** (everything after the first
   overlap). If 1000 pages were compacted and 5 new pages were appended, all 5
   are tuplesorted even though they might already be internally sorted — the
   system doesn't know where "compacted" ends and "appended" begins.

### Consequences

| Symptom | Root cause |
|---------|-----------|
| Merge re-scans zone map entries every run | No persisted prefix count |
| Binary search disabled after 1 INSERT | Flag cleared globally |
| Tail tuplesort includes pages that are already sorted runs | No boundary between compacted prefix and append zone |
| Scan pruning lost after overflow insert | VALID flag is all-or-nothing |

## AM Surface Analysis

sorted_heap AM (sorted_heap.c:169-196) starts from a **copy** of the heap
AM routine and overrides only these entry points:

| Override | What it does |
|----------|-------------|
| `tuple_insert` | Delegates to `heap->tuple_insert`, then updates zone map |
| `multi_insert` | Sorts batch by PK, delegates to `heap->multi_insert`, updates zone map |
| `relation_set_new_filelocator` | Meta page init |
| `relation_nontransactional_truncate` | Meta page reset |
| `relation_copy_data` / `relation_copy_for_cluster` | DDL lifecycle |
| `index_build_range_scan` / `index_validate_scan` | Delegates to heap with AM swap |
| `relation_vacuum` | Zone map rebuild when invalid |

**Not overridden (inherited from heap):**
- `tuple_update` — heap-owned. HOT rewrites in-place. Non-HOT allocates on
  a different page via FSM. **PK is NOT immutable in PostgreSQL** — PK columns
  are updatable. The online compact trigger explicitly handles PK-changing
  updates as `D(old_pk) + I(new_pk)` (sorted_heap_online.c:259-270).
- `tuple_delete` — heap-owned. Marks tuple dead. Creates free space for FSM.

### Write paths that can break prefix monotonicity

The prefix invariant is `entry[i].max <= entry[i+1].min` for all `i` in
`[0, prefix)`. An entry for page P is broken when either:
- P's `zme_min` decreases below the previous page's `zme_max` (overlap left)
- P's `zme_max` increases above the next page's `zme_min` (overlap right)

Exhaustive list of paths that can cause this:

1. **`tuple_insert`** — heap chooses page via FSM/extend. If FSM offers a
   compacted page, a new PK value outside the page's [min, max] widens the
   entry in either direction.

2. **`multi_insert`** — same mechanism. Batch is sorted by PK before
   `heap->multi_insert`, but heap placement is still FSM-driven.

3. **`tuple_update` (non-HOT, same PK)** — heap places new version on a
   different page via FSM. Since PK unchanged, the new page gets a tuple
   whose PK may be outside that page's zone map range. **However:** we do
   not override `tuple_update`, so `sorted_heap_zonemap_update_entry` is
   never called. The zone map becomes stale — page B's true range is wider
   than tracked. This is invisible to the prefix check.

4. **`tuple_update` (PK change)** — PostgreSQL allows PK updates. The new
   tuple version has a different PK and lands on a page chosen by FSM. This
   is equivalent to an untracked INSERT of a new PK value — same invisible
   zone map staleness as case 3, but worse because the new PK can be
   arbitrarily different from the old one.

5. **`tuple_update` (HOT)** — in-place on the same page. PK may or may not
   change, but the tuple stays on the same page. If PK changed, the page's
   true [min, max] may widen, but again we have no hook.

6. **`tuple_delete`** — marks dead, no placement. Cannot directly break
   prefix. Creates free space for FSM.

**Summary:** cases 1-2 are covered by our `tuple_insert`/`multi_insert`
overrides. Cases 3-5 are NOT covered because we don't override
`tuple_update`. The zone map becomes stale without any notification to the
prefix tracking.

## Proposed Design

### Core idea: persist `sorted_prefix_pages` in the meta page

After compaction, store the count of sorted data pages as a new field in
`SortedHeapMetaPageData`:

```c
typedef struct SortedHeapMetaPageData
{
    uint32      shm_magic;
    uint32      shm_version;            /* bump to 7 */
    uint32      shm_flags;
    Oid         shm_pk_index_oid;
    uint16      shm_zonemap_nentries;
    uint16      shm_overflow_npages;
    Oid         shm_zonemap_pk_typid;
    Oid         shm_zonemap_pk_typid2;
    uint16      shm_sorted_prefix_pages;  /* NEW: pages 1..N known sorted */
    uint16      shm_padding;
    ...
};
```

### Invariant and contract

`shm_sorted_prefix_pages` is a **shrinkable lower bound** of the zone map
monotonicity boundary.

- **Compaction increases it** (offline merge: `= total_data_pages`;
  online merge: `= detect_sorted_prefix_scan(new_info)` post-rebuild).
- **Insert paths decrease it** when zone map entry within prefix widens to
  create overlap (min decrease OR max increase).
- **Update paths shrink it** conservatively via the new `tuple_update`
  override: when a tuple lands on a prefix page, shrink to that page boundary.
- **Value 0** means "no prefix known" — full zone map scan fallback. This is
  the safe default for pre-v7 tables and fresh tables.

The prefix count is a **persistent cache**. It can always be re-derived from
zone map entries (the current `detect_sorted_prefix` scan), but persisting it
avoids the O(n) scan on the hot path.

### What changes

#### 1. Compaction sets the prefix count

**Offline `sorted_heap_merge`:** writes a fully sorted table, then rebuilds
the zone map. After `finish_heap_swap`, the new table is entirely sorted:

```
shm_sorted_prefix_pages = total_data_pages_of_new_rel
shm_flags |= SHM_FLAG_ZM_SORTED
shm_flags |= SHM_FLAG_ZONEMAP_VALID
```

**Online `sorted_heap_merge_online`:** the sequence is:
1. Copy merged data into `new_rel` (sorted)
2. Replay loop: apply log entries via `heap->tuple_insert` into `new_rel`
3. Final replay under AccessExclusiveLock
4. `sorted_heap_rebuild_zonemap_internal(new_rel, ...)`
5. `finish_heap_swap`

Replayed inserts (step 2-3) use `heap->tuple_insert` into `new_rel`, which
appends pages without zone map tracking. These pages form an unsorted tail.
After zone map rebuild (step 4), the zone map covers ALL pages including
replayed ones, but the replayed pages may break monotonicity.

**Therefore:** after zone map rebuild, run `detect_sorted_prefix_scan` on the
rebuilt zone map to find the actual prefix length. This is the value to
persist — NOT `total_data_pages`.

```
prefix = detect_sorted_prefix_scan(new_info);
shm_sorted_prefix_pages = prefix;
if (prefix >= total_data_pages)
    shm_flags |= SHM_FLAG_ZM_SORTED;
```

This is correct even when replay added zero changes (prefix = total).

#### 2. Invalidation on insert paths (tuple_insert, multi_insert)

Both insert overrides call `sorted_heap_zonemap_update_entry` after heap
placement. Currently, only min-decrease sets `zm_sorted = false`
(sorted_heap.c:1149). But prefix monotonicity can also break when max
increases past the next entry's min.

We add prefix shrink logic for **both** directions in
`sorted_heap_zonemap_update_entry`:

```c
if (key < e->zme_min)
{
    e->zme_min = key;
    changed = true;
    info->zm_sorted = false;

    /* Overlap left: this entry's min may now be < prev entry's max */
    if (zmidx < info->sorted_prefix_pages)
        sorted_heap_shrink_prefix(rel, info, zmidx);
}
if (key > e->zme_max)
{
    e->zme_max = key;
    changed = true;

    /* Overlap right: this entry's max may now be > next entry's min.
     * Check if we actually broke monotonicity before shrinking.      */
    if (zmidx + 1 < info->sorted_prefix_pages)
    {
        SortedHeapZoneMapEntry *next = sorted_heap_get_zm_entry(info, zmidx + 1);
        if (next->zme_min != PG_INT64_MAX && key > next->zme_min)
        {
            info->zm_sorted = false;
            sorted_heap_shrink_prefix(rel, info, zmidx + 1);
        }
    }
}
```

`sorted_heap_shrink_prefix(rel, info, boundary)` sets
`info->sorted_prefix_pages = boundary` and persists to the meta page.

Note: for max-increase, we only shrink when there is an actual overlap with
the next entry (not unconditionally). This avoids pessimistic shrinking when
the max grows but still doesn't overlap.

#### 3. Invalidation on update path (tuple_update) — new override

We **must** override `tuple_update` to protect the prefix. Without a hook,
any update (PK-changing or not, HOT or non-HOT) can silently widen a prefix
page's true range without the zone map or prefix tracking knowing.

The override is minimal:

```c
static TM_Result
sorted_heap_tuple_update(Relation rel, ItemPointer otid,
                         TupleTableSlot *slot, CommandId cid,
                         Snapshot snapshot, Snapshot crosscheck,
                         bool wait, TM_FailureData *tmfd,
                         LockTupleMode *lockmode,
                         TU_UpdateIndexes *update_indexes)
{
    const TableAmRoutine *heap = GetHeapamTableAmRoutine();
    TM_Result result;

    result = heap->tuple_update(rel, otid, slot, cid, snapshot,
                                crosscheck, wait, tmfd,
                                lockmode, update_indexes);

    if (result == TM_Ok)
    {
        SortedHeapRelInfo *info = sorted_heap_get_relinfo(rel);

        if (info->sorted_prefix_pages > 0)
        {
            BlockNumber new_blk = ItemPointerGetBlockNumber(&slot->tts_tid);
            uint32      zmidx = (new_blk >= 1) ? (new_blk - 1) : 0;

            if (new_blk >= 1 && zmidx < info->sorted_prefix_pages)
            {
                /* Tuple landed on a prefix page. We can't cheaply tell
                 * whether the PK changed or whether the range widened,
                 * so conservatively shrink to this page boundary.
                 *
                 * This fires rarely: non-HOT updates to prefix pages
                 * are uncommon after compaction (pages are full). */
                sorted_heap_shrink_prefix(rel, info, zmidx);
            }
        }

        /* Zone map update for the new page (same as tuple_insert) */
        if (info->zm_scan_valid && info->zm_usable && new_blk >= 1)
        {
            uint32 zmidx2 = new_blk - 1;
            if (zmidx2 < info->zm_total_entries)
            {
                slot_getallattrs(slot);
                if (sorted_heap_zonemap_update_entry(info, zmidx2, slot))
                {
                    if (zmidx2 < info->zm_nentries)
                        sorted_heap_zonemap_flush(rel, info);
                    else
                    {
                        uint32 ovfl_idx = zmidx2 - info->zm_nentries;
                        uint32 page_idx = ovfl_idx /
                            SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE;
                        if (page_idx < info->zm_overflow_npages)
                            sorted_heap_zonemap_flush_overflow_page(
                                rel, info, page_idx);
                    }
                    sorted_heap_clear_sorted_flag(rel, info);
                }
            }
            else
                sorted_heap_clear_sorted_flag(rel, info);
        }
    }

    return result;
}
```

And in the AM init:

```c
sorted_heap_am_routine.tuple_update = sorted_heap_tuple_update;
```

This has two effects:
1. Conservative prefix shrink when any update lands on a prefix page
2. Zone map update for the new tuple's page (currently missing — this is
   an existing gap that the prefix work surfaces)

The shrink is conservative: it fires on any update to a prefix page, even
if PK is unchanged and the range didn't widen. Frequency depends on table
state:
- **Post-compaction (full pages):** prefix pages are near fillfactor=100, so
  non-HOT updates place the new version on a DIFFERENT page (no room on the
  original). The shrink fires only when FSM recycles a prefix page that had
  dead tuples (from DELETE + VACUUM) — uncommon right after compaction.
- **Low-fillfactor or heavily-updated tables:** HOT updates and FSM recycling
  on prefix pages can be frequent, which may collapse `sorted_prefix_pages`
  aggressively toward 0. This is correct (safe lower bound) but pessimistic.
  The next compaction restores the prefix.

**Tradeoff:** the conservative shrink is worst-case O(1)-per-update overhead
with no false positives (prefix never exceeds actual monotonic range). The
cost is that write-heavy workloads on prefix pages may lose the prefix
benefit between compactions. This is acceptable for S2 — a tighter
`tuple_update` override that extracts and compares PK values is a possible
S2+ refinement if benchmarks show excessive prefix collapse.

**HOT updates:** the `slot->tts_tid` after a HOT update points to the same
page as `otid`. If `zmidx < sorted_prefix_pages`, the shrink fires, but this
is conservative — the actual range likely didn't widen.

#### 4. `detect_sorted_prefix` becomes O(1)

```c
BlockNumber
sorted_heap_detect_sorted_prefix(SortedHeapRelInfo *info)
{
    /* Fast path: persisted prefix from last compaction */
    if (info->sorted_prefix_pages > 0)
        return info->sorted_prefix_pages;

    /* Fallback: scan zone map entries (pre-v7 tables, or no compaction yet) */
    return sorted_heap_detect_sorted_prefix_scan(info);
}
```

#### 5. Scan pruning: binary search in prefix, linear in tail

Currently `SHM_FLAG_ZM_SORTED` enables binary search over ALL zone map
entries. With persisted prefix, we can binary search within
`[0, sorted_prefix_pages)` and linear scan `[sorted_prefix_pages, total)`,
even when the global sorted flag is cleared.

This restores partial binary search after INSERTs without a full rebuild.
Implementation: in the scan pruning path, check
`sorted_prefix_pages > 0 && target falls within prefix range` -> binary
search; else linear scan as today.

#### 6. Incremental merge: smarter tail handling

Current: tail = everything after first zone map overlap.
New: tail = everything after `sorted_prefix_pages`.

If the append zone itself contains sorted runs (from sorted multi_insert
batches), we can detect sub-runs and use a multi-way merge instead of full
tuplesort. This is a **future** optimization (not S2 scope), but the prefix
metadata makes it possible.

### Meta page version bump

- Current version: 6 (v5 overflow + v6 linked-list chain)
- New version: 7
- Backward compatibility: v7 reader treats v6 meta page as
  `sorted_prefix_pages = 0` (no prefix known -> full zone map scan fallback)
- v6 reader ignores the new field (it's in padding space)

**Space check:** the header layout is:
```
magic(4) + version(4) + flags(4) + pk_oid(4) + nentries(2) + overflow_npages(2)
+ pk_typid(4) + pk_typid2(4) + padding(4) = 32 bytes
```

`shm_padding` is `uint32` (4 bytes). Split into
`shm_sorted_prefix_pages (uint16)` + `shm_padding2 (uint16)`. Zone map
array offset unchanged.

## Scope

### S2 (prototype): minimum viable persisted prefix

1. Add `shm_sorted_prefix_pages` to meta page (version 7)
2. Add `sorted_prefix_pages` to `SortedHeapRelInfo`, load in `zonemap_load`
3. Set in `sorted_heap_merge` post-swap (`= total_data_pages_of_new_rel`)
4. Set in `sorted_heap_merge_online` post-zone-map-rebuild
   (`= detect_sorted_prefix_scan(new_info)`)
5. Add `sorted_heap_shrink_prefix()` — shrink + persist
6. Wire shrink into `sorted_heap_zonemap_update_entry` for BOTH min-decrease
   and max-increase-with-overlap (covers `tuple_insert` and `multi_insert`)
7. Add `sorted_heap_tuple_update` override — conservative prefix shrink +
   zone map update for the new tuple's page
8. Use in `detect_sorted_prefix` as O(1) fast path
9. Reset to 0 on `relation_nontransactional_truncate`
10. Expose in `pg_sorted_heap_observability()`
11. Regression tests:
    a. compact -> verify prefix = total -> INSERT beyond -> prefix unchanged
    b. INSERT that lands on compacted page (via DELETE + VACUUM + INSERT to
       recycle space) -> prefix shrank
    c. UPDATE with PK change on prefix page -> prefix shrank
    d. merge -> prefix = total again
12. Debug-build Assert in `relation_vacuum` zone map rebuild: verify
    persisted prefix <= actual monotonic prefix from scan

### S2+ (follow-up): scan pruning enhancement

13. Binary search within prefix zone map entries during scan pruning
14. `SHM_FLAG_ZM_SORTED` semantics: represent "entire table sorted" vs
    prefix-only sorted (may need a new flag `SHM_FLAG_PREFIX_VALID`)

### Not in scope

- Multi-way merge of sorted runs in tail
- Per-page "compacted" bit
- Compaction scheduling / auto-compact

## Risks

| Risk | Mitigation |
|------|-----------|
| FSM offers compacted page to insert, max grows past next min | Shrink on max-increase-with-overlap in `zonemap_update_entry` |
| `tuple_update` places on prefix page (PK change or not) | New `tuple_update` override: conservative shrink |
| HOT update on prefix page triggers unnecessary shrink | Conservative but safe; HOT on full pages is rare post-compact |
| `merge_online` replay creates unsorted tail | `detect_sorted_prefix_scan` post-rebuild, not total pages |
| v7 meta page unreadable by old code | Field is in padding bytes; old code ignores it |
| prefix > actual sorted range after DDL | Reset to 0 on TRUNCATE; re-derived by merge before use |
| `tuple_update` override adds per-update overhead | Only checks 1 integer comparison + 1 branch; negligible |

## Verification

1. **Unit:** regression tests covering full lifecycle (compact, append-only
   insert, insert on recycled prefix page, PK-changing update, merge)
2. **Bench:** gutenberg corpus — compact, insert 1000 rows, merge — compare
   merge NOTICE output (prefix/tail page counts) with/without persisted prefix
3. **Stress:** debug-build Assert in vacuum zone map rebuild — run the full
   test suite to verify prefix <= actual monotonic prefix
4. **Update safety:** test PK-changing UPDATE, non-HOT UPDATE, HOT UPDATE on
   prefix pages -> verify prefix shrinks correctly via observability
5. **`tuple_update` regression:** run existing test suite to verify the new
   override doesn't change any existing behavior (it's pass-through + tracking)
