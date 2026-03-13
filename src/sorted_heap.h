#ifndef SORTED_HEAP_H
#define SORTED_HEAP_H

#include "postgres.h"
#include "fmgr.h"
#include "access/attnum.h"
#include "access/tableam.h"
#include "port/atomics.h"
#include "storage/block.h"

#define SORTED_HEAP_MAGIC		0x534F5254	/* 'SORT' */
#define SORTED_HEAP_VERSION		7
#define SORTED_HEAP_META_BLOCK	0
#define SORTED_HEAP_MAX_KEYS	INDEX_MAX_KEYS
#define SORTED_HEAP_ZONEMAP_MAX	250		/* v5/v6 on-disk meta page entries */
#define SORTED_HEAP_ZONEMAP_CACHE_MAX 500	/* in-memory cache entries (supports v4-v6) */

/* Meta page overflow slots: block numbers stored directly in meta page */
#define SORTED_HEAP_META_OVERFLOW_SLOTS		32

/* v6 overflow pages: 254 entries + next_block pointer (linked list) */
#define SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE 254
/* No hard cap on overflow pages — linked list extends beyond meta slots */

/* v5 backward compatibility */
#define SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE_V5 255

/* v4 backward compatibility constants */
#define SORTED_HEAP_ZONEMAP_MAX_V4				500
#define SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE_V4 509

/* Flag bits for shm_flags */
#define SORTED_HEAP_FLAG_ZONEMAP_STALE	0x0001
#define SHM_FLAG_ZONEMAP_VALID			0x0002	/* zone map safe for scan pruning */
#define SHM_FLAG_ZM_SORTED				0x0004	/* zone map entries monotonic (binary search ok) */

/*
 * Per-page zone map entry: min/max of PK columns as int64.
 * Column 1 always tracked. Column 2 tracked when composite PK is usable.
 * Sentinel: zme_min == PG_INT64_MAX means "no data tracked".
 * For column 2: zme_min2 == PG_INT64_MAX means "column 2 not tracked".
 */
typedef struct SortedHeapZoneMapEntry
{
	int64		zme_min;	/* column 1 min */
	int64		zme_max;	/* column 1 max */
	int64		zme_min2;	/* column 2 min (PG_INT64_MAX = not tracked) */
	int64		zme_max2;	/* column 2 max */
} SortedHeapZoneMapEntry;	/* 32 bytes */

/*
 * Meta page data stored in the special space of page 0.
 * Data pages (>= 1) use standard heap page format with no special space.
 *
 * v5 size: 32 header + 250 * 32 entries + 128 overflow = 8160 bytes.
 * Fits within max special space of 8168 bytes.
 */
typedef struct SortedHeapMetaPageData
{
	uint32		shm_magic;
	uint32		shm_version;
	uint32		shm_flags;
	Oid			shm_pk_index_oid;		/* cached PK index OID */
	uint16		shm_zonemap_nentries;	/* valid zone map entry count (in meta page) */
	uint16		shm_overflow_npages;	/* number of overflow pages */
	Oid			shm_zonemap_pk_typid;	/* type of first PK column */
	Oid			shm_zonemap_pk_typid2;	/* type of second PK column (v5+) */
	uint16		shm_sorted_prefix_pages;	/* v7: persisted sorted prefix count */
	uint16		shm_padding;			/* align entries to 8 bytes */
	/* 32 bytes of header above */
	SortedHeapZoneMapEntry shm_zonemap[SORTED_HEAP_ZONEMAP_MAX];
	/* overflow page block numbers (128 bytes) */
	BlockNumber	shm_overflow_blocks[SORTED_HEAP_META_OVERFLOW_SLOTS];
} SortedHeapMetaPageData;

/*
 * v5 overflow page: 8-byte header + 255 × 32-byte entries = 8168 bytes.
 * Kept for reading pre-v6 tables.
 */
typedef struct SortedHeapOverflowPageDataV5
{
	uint32		shmo_magic;			/* SORTED_HEAP_MAGIC */
	uint16		shmo_nentries;		/* entries in this page */
	uint16		shmo_page_index;	/* 0-based index among overflow pages */
	SortedHeapZoneMapEntry shmo_entries[SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE_V5];
} SortedHeapOverflowPageDataV5;

/*
 * v6 overflow page: 16-byte header + 254 × 32-byte entries = 8144 bytes.
 * Adds shmo_next_block for linked-list chain beyond meta page slots.
 */
typedef struct SortedHeapOverflowPageData
{
	uint32		shmo_magic;			/* SORTED_HEAP_MAGIC */
	uint16		shmo_nentries;		/* entries in this page */
	uint16		shmo_page_index;	/* 0-based index among overflow pages */
	BlockNumber	shmo_next_block;	/* next overflow page, or InvalidBlockNumber */
	uint32		shmo_padding;		/* align entries to 8 bytes */
	SortedHeapZoneMapEntry shmo_entries[SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE];
} SortedHeapOverflowPageData;

/*
 * Per-relation PK info + zone map cache, backend-local hash table.
 * Populated lazily on first multi_insert call.
 */
typedef struct SortedHeapRelInfo
{
	Oid			relid;								/* hash key */
	bool		pk_probed;							/* true after first lookup */
	Oid			pk_index_oid;						/* PK index OID, or InvalidOid */
	int			nkeys;								/* number of PK columns */
	AttrNumber	attNums[SORTED_HEAP_MAX_KEYS];
	Oid			sortOperators[SORTED_HEAP_MAX_KEYS];
	Oid			sortCollations[SORTED_HEAP_MAX_KEYS];
	bool		nullsFirst[SORTED_HEAP_MAX_KEYS];

	/* Zone map cache */
	bool		zm_usable;			/* first PK col is int2/4/8/timestamp/date */
	bool		zm_loaded;			/* zone map read from meta page */
	bool		zm_scan_valid;		/* zone map valid for scan pruning */
	bool		zm_sorted;			/* zone map entries monotonically sorted */
	bool		zm_disk_sorted_cleared;	/* on-disk SHM_FLAG_ZM_SORTED already cleared */
	Oid			zm_pk_typid;		/* type of first PK column */
	bool		zm_col2_usable;		/* second PK col is int2/4/8/timestamp/date */
	Oid			zm_pk_typid2;		/* type of second PK column */
	uint16		zm_nentries;		/* entries in cache (max CACHE_MAX) */
	SortedHeapZoneMapEntry zm_entries[SORTED_HEAP_ZONEMAP_CACHE_MAX];

	/* Overflow zone map (for tables > 500 data pages) */
	SortedHeapZoneMapEntry *zm_overflow;	/* palloc'd, or NULL */
	uint32		zm_overflow_nentries;		/* entries in overflow pages */
	uint32		zm_overflow_alloc;			/* allocated slots in zm_overflow */
	uint32		zm_total_entries;			/* zm_nentries + zm_overflow_nentries */
	uint32		zm_overflow_npages;			/* number of overflow pages */
	BlockNumber *zm_overflow_blocks;		/* palloc'd, or NULL; one per overflow page */
	uint64		zm_gen;						/* generation when zone map was loaded */

	/* Persisted sorted prefix (v7+) */
	uint16		sorted_prefix_pages;		/* pages 1..N known sorted */
} SortedHeapRelInfo;

/*
 * Inline helper to access zone map entry by global index.
 * Entries 0..zm_nentries-1 are in the cache array; the rest in overflow.
 * (v4 stores up to 500 in cache, v5 stores up to 250.)
 */
static inline SortedHeapZoneMapEntry *
sorted_heap_get_zm_entry(SortedHeapRelInfo *info, uint32 idx)
{
	if (idx < info->zm_nentries)
		return &info->zm_entries[idx];
	return &info->zm_overflow[idx - info->zm_nentries];
}

extern Datum sorted_heap_tableam_handler(PG_FUNCTION_ARGS);
extern Datum sorted_heap_zonemap_stats(PG_FUNCTION_ARGS);
extern Datum sorted_heap_compact(PG_FUNCTION_ARGS);
extern Datum sorted_heap_rebuild_zonemap_sql(PG_FUNCTION_ARGS);
extern void sorted_heap_relcache_callback(Datum arg, Oid relid);

/* Exported for sorted_heap_scan.c */
extern TableAmRoutine sorted_heap_am_routine;
extern SortedHeapRelInfo *sorted_heap_get_relinfo(Relation rel);
extern bool sorted_heap_key_to_int64(Datum value, Oid typid, int64 *out);
extern void sorted_heap_scan_init(void);
extern Datum sorted_heap_scan_stats(PG_FUNCTION_ARGS);
extern Datum sorted_heap_reset_stats(PG_FUNCTION_ARGS);
extern Datum sorted_heap_compact_trigger(PG_FUNCTION_ARGS);
extern Datum sorted_heap_compact_online(PG_FUNCTION_ARGS);
extern Datum sorted_heap_merge(PG_FUNCTION_ARGS);
extern Datum sorted_heap_merge_online(PG_FUNCTION_ARGS);
extern BlockNumber sorted_heap_detect_sorted_prefix(SortedHeapRelInfo *info);
extern void sorted_heap_bump_zm_generation(void);
extern uint64 sorted_heap_read_zm_generation(void);
extern void sorted_heap_zonemap_load(Relation rel, SortedHeapRelInfo *info);
extern void sorted_heap_rebuild_zonemap_internal(Relation rel, Oid pk_typid,
												 AttrNumber pk_attnum,
												 Oid pk_typid2,
												 AttrNumber pk_attnum2);

/* Shared memory stats (cluster-wide when loaded via shared_preload_libraries) */
typedef struct SortedHeapSharedStats
{
	pg_atomic_uint64 total_scans;
	pg_atomic_uint64 blocks_scanned;
	pg_atomic_uint64 blocks_pruned;
	pg_atomic_uint64 zm_generation;		/* bumped on any zone map mutation */
} SortedHeapSharedStats;

/* GUC variables */
extern bool sorted_heap_enable_scan_pruning;
extern bool sorted_heap_vacuum_rebuild_zonemap;
extern bool sorted_heap_ann_timing;
extern bool sorted_heap_hnsw_cache_l0;
extern void sorted_heap_hnsw_relcache_invalidate(Oid relid);

#endif							/* SORTED_HEAP_H */
