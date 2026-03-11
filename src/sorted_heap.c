/*
 * sorted_heap.c
 *
 * Sorted heap table access method — Phase 3 (persistent zone maps).
 *
 * Uses standard heap page format.  Block 0 carries a meta page with
 * SortedHeapMetaPageData in special space; data lives on pages >= 1.
 * Single-row inserts delegate to heap (zero overhead).
 * multi_insert (COPY path) sorts each batch by PK before delegating
 * to heap, producing physically sorted runs.  After placement, per-page
 * min/max of the first PK column (int2/4/8 only) are recorded in a
 * persistent zone map stored in the meta page.
 * Scans, deletes, updates, and vacuum all delegate to heap.
 */
#include "postgres.h"

#include "access/generic_xlog.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/stratnum.h"
#include "access/tableam.h"
#include "access/xloginsert.h"
#include "catalog/index.h"
#include "commands/cluster.h"
#include "catalog/pg_index.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "storage/smgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/sortsupport.h"
#include "utils/tuplesort.h"
#include "utils/uuid.h"
#include "catalog/pg_collation_d.h"
#include "executor/tuptable.h"

#include "sorted_heap.h"

/* ----------------------------------------------------------------
 *  v4 backward compatibility structures
 *
 *  v4 used 16-byte zone map entries (col1 only). v5 widened to 32 bytes
 *  (col1 + col2). These structs allow reading v4 on-disk pages.
 * ---------------------------------------------------------------- */
typedef struct SortedHeapZoneMapEntryV4
{
	int64		zme_min;
	int64		zme_max;
} SortedHeapZoneMapEntryV4;		/* 16 bytes */

typedef struct SortedHeapMetaPageDataV4
{
	uint32		shm_magic;
	uint32		shm_version;
	uint32		shm_flags;
	Oid			shm_pk_index_oid;
	uint16		shm_zonemap_nentries;
	uint16		shm_overflow_npages;
	Oid			shm_zonemap_pk_typid;
	/* 24 bytes of header */
	SortedHeapZoneMapEntryV4 shm_zonemap[SORTED_HEAP_ZONEMAP_MAX_V4];
	BlockNumber	shm_overflow_blocks[SORTED_HEAP_META_OVERFLOW_SLOTS];
} SortedHeapMetaPageDataV4;

typedef struct SortedHeapOverflowPageDataV4
{
	uint32		shmo_magic;
	uint16		shmo_nentries;
	uint16		shmo_page_index;
	SortedHeapZoneMapEntryV4 shmo_entries[SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE_V4];
} SortedHeapOverflowPageDataV4;

PG_FUNCTION_INFO_V1(sorted_heap_tableam_handler);
PG_FUNCTION_INFO_V1(sorted_heap_zonemap_stats);
PG_FUNCTION_INFO_V1(sorted_heap_compact);
PG_FUNCTION_INFO_V1(sorted_heap_rebuild_zonemap_sql);
PG_FUNCTION_INFO_V1(sorted_heap_merge);

/* ----------------------------------------------------------------
 *  Forward declarations
 * ---------------------------------------------------------------- */
static void sorted_heap_init_meta_page_smgr(const RelFileLocator *rlocator,
											ProcNumber backend, bool need_wal);
static void sorted_heap_relinfo_invalidate(Oid relid);
/* sorted_heap_zonemap_load is declared in sorted_heap.h (non-static) */
static void sorted_heap_zonemap_flush(Relation rel, SortedHeapRelInfo *info);
/* sorted_heap_rebuild_zonemap_internal is declared in sorted_heap.h (non-static) */

static void sorted_heap_relation_set_new_filelocator(Relation rel,
													 const RelFileLocator *rlocator,
													 char persistence,
													 TransactionId *freezeXid,
													 MultiXactId *minmulti);
static void sorted_heap_relation_nontransactional_truncate(Relation rel);
static void sorted_heap_relation_copy_data(Relation rel,
										   const RelFileLocator *newrlocator);
static void sorted_heap_relation_copy_for_cluster(Relation OldTable,
												  Relation NewTable,
												  Relation OldIndex,
												  bool use_sort,
												  TransactionId OldestXmin,
												  TransactionId *xid_cutoff,
												  MultiXactId *multi_cutoff,
												  double *num_tuples,
												  double *tups_vacuumed,
												  double *tups_recently_dead);
static void sorted_heap_tuple_insert(Relation rel, TupleTableSlot *slot,
									 CommandId cid, int options,
									 struct BulkInsertStateData *bistate);
static void sorted_heap_multi_insert(Relation rel, TupleTableSlot **slots,
									 int nslots, CommandId cid, int options,
									 struct BulkInsertStateData *bistate);
static double sorted_heap_index_build_range_scan(Relation tableRelation,
												 Relation indexRelation,
												 IndexInfo *indexInfo,
												 bool allow_sync,
												 bool anyvisible,
												 bool progress,
												 BlockNumber start_blockno,
												 BlockNumber numblocks,
												 IndexBuildCallback callback,
												 void *callback_state,
												 TableScanDesc scan);
static void sorted_heap_index_validate_scan(Relation tableRelation,
											Relation indexRelation,
											IndexInfo *indexInfo,
											Snapshot snapshot,
											ValidateIndexState *state);
static void sorted_heap_relation_vacuum(Relation rel,
										struct VacuumParams *params,
										BufferAccessStrategy bstrategy);

/* ----------------------------------------------------------------
 *  Static state
 * ---------------------------------------------------------------- */
static bool sorted_heap_am_initialized = false;
TableAmRoutine sorted_heap_am_routine;
static HTAB *sorted_heap_relinfo_hash = NULL;

/* GUC: rebuild zone map during VACUUM when invalid */
bool sorted_heap_vacuum_rebuild_zonemap = true;

/* ----------------------------------------------------------------
 *  Handler + initialization
 * ---------------------------------------------------------------- */
static void
sorted_heap_init_routine(void)
{
	const TableAmRoutine *heap;

	if (sorted_heap_am_initialized)
		return;

	heap = GetHeapamTableAmRoutine();
	if (heap == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("heap table access method is unavailable")));

	sorted_heap_am_routine = *heap;
	sorted_heap_am_routine.type = T_TableAmRoutine;

	/* DDL lifecycle */
	sorted_heap_am_routine.relation_set_new_filelocator =
		sorted_heap_relation_set_new_filelocator;
	sorted_heap_am_routine.relation_nontransactional_truncate =
		sorted_heap_relation_nontransactional_truncate;
	sorted_heap_am_routine.relation_copy_data =
		sorted_heap_relation_copy_data;
	sorted_heap_am_routine.relation_copy_for_cluster =
		sorted_heap_relation_copy_for_cluster;

	/* Single-row insert — invalidates zone map valid flag */
	sorted_heap_am_routine.tuple_insert = sorted_heap_tuple_insert;

	/* Bulk insert — sort batch by PK + update zone map */
	sorted_heap_am_routine.multi_insert = sorted_heap_multi_insert;

	/* Index build — needs rd_tableam swap to delegate to heap */
	sorted_heap_am_routine.index_build_range_scan =
		sorted_heap_index_build_range_scan;
	sorted_heap_am_routine.index_validate_scan =
		sorted_heap_index_validate_scan;

	/* Vacuum — rebuild zone map when invalid */
	sorted_heap_am_routine.relation_vacuum =
		sorted_heap_relation_vacuum;

	sorted_heap_am_initialized = true;
}

Datum
sorted_heap_tableam_handler(PG_FUNCTION_ARGS)
{
	sorted_heap_init_routine();
	PG_RETURN_POINTER(&sorted_heap_am_routine);
}

/* ----------------------------------------------------------------
 *  Key conversion utility
 * ---------------------------------------------------------------- */
bool
sorted_heap_key_to_int64(Datum value, Oid typid, int64 *out)
{
	switch (typid)
	{
		case INT2OID:
			*out = (int64) DatumGetInt16(value);
			return true;
		case INT4OID:
			*out = (int64) DatumGetInt32(value);
			return true;
		case INT8OID:
			*out = DatumGetInt64(value);
			return true;
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			*out = DatumGetInt64(value);	/* int64 microseconds */
			return true;
		case DATEOID:
			*out = (int64) DatumGetInt32(value);	/* DateADT is int32 */
			return true;
		case UUIDOID:
		{
			pg_uuid_t  *uuid = DatumGetUUIDP(value);
			unsigned char *d = uuid->data;
			uint64		hi;

			/* First 8 bytes as big-endian uint64, sign-flipped for ordering */
			hi = ((uint64) d[0] << 56) | ((uint64) d[1] << 48) |
				 ((uint64) d[2] << 40) | ((uint64) d[3] << 32) |
				 ((uint64) d[4] << 24) | ((uint64) d[5] << 16) |
				 ((uint64) d[6] << 8)  | (uint64) d[7];
			*out = (int64) (hi ^ ((uint64) 1 << 63));
			return true;
		}
		case TEXTOID:
		case VARCHAROID:
		{
			text	   *txt = DatumGetTextPP(value);
			int			len = VARSIZE_ANY_EXHDR(txt);
			char	   *data = VARDATA_ANY(txt);
			unsigned char buf[8];
			uint64		val64;

			/* First 8 bytes zero-padded, as big-endian uint64, sign-flipped */
			memset(buf, 0, 8);
			memcpy(buf, data, Min(len, 8));
			val64 = ((uint64) buf[0] << 56) | ((uint64) buf[1] << 48) |
					((uint64) buf[2] << 40) | ((uint64) buf[3] << 32) |
					((uint64) buf[4] << 24) | ((uint64) buf[5] << 16) |
					((uint64) buf[6] << 8)  | (uint64) buf[7];
			*out = (int64) (val64 ^ ((uint64) 1 << 63));
			return true;
		}
		default:
			return false;
	}
}

/* ----------------------------------------------------------------
 *  PK detection infrastructure
 *
 *  Per-relation cache of PK columns, sort operators, and zone map.
 *  Populated lazily on first multi_insert.  Invalidated by
 *  relcache callback when indexes are created/dropped.
 * ---------------------------------------------------------------- */
static void
sorted_heap_ensure_relinfo_hash(void)
{
	HASHCTL ctl;

	if (sorted_heap_relinfo_hash != NULL)
		return;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(SortedHeapRelInfo);
	ctl.hcxt = TopMemoryContext;
	sorted_heap_relinfo_hash = hash_create("sorted_heap relinfo",
										   32, &ctl,
										   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

SortedHeapRelInfo *
sorted_heap_get_relinfo(Relation rel)
{
	Oid				relid = RelationGetRelid(rel);
	SortedHeapRelInfo *info;
	bool			found;

	sorted_heap_ensure_relinfo_hash();

	info = hash_search(sorted_heap_relinfo_hash, &relid, HASH_ENTER, &found);
	if (!found)
	{
		info->pk_probed = false;
		info->pk_index_oid = InvalidOid;
		info->nkeys = 0;
		info->zm_usable = false;
		info->zm_loaded = false;
		info->zm_sorted = false;
		info->zm_disk_sorted_cleared = false;
		info->zm_pk_typid = InvalidOid;
		info->zm_nentries = 0;
		info->zm_overflow = NULL;
		info->zm_overflow_nentries = 0;
		info->zm_overflow_alloc = 0;
		info->zm_total_entries = 0;
		info->zm_overflow_npages = 0;
		info->zm_overflow_blocks = NULL;
		info->zm_col2_usable = false;
		info->zm_pk_typid2 = InvalidOid;
		info->zm_gen = 0;
	}
	else
	{
		/*
		 * Cross-backend cache invalidation: if another backend mutated
		 * the zone map, our cached zm_sorted / zm_scan_valid may be stale.
		 * Force reload from disk on generation mismatch.
		 */
		uint64	current_gen = sorted_heap_read_zm_generation();

		if (current_gen != 0 && info->zm_gen != current_gen)
			info->zm_loaded = false;
	}

	if (!info->pk_probed)
	{
		Oid		pk_oid;

		/* Ensure index list is loaded */
		if (!rel->rd_indexvalid)
			RelationGetIndexList(rel);

		pk_oid = rel->rd_pkindex;

		if (OidIsValid(pk_oid))
		{
			Relation	idxrel;
			int			nkeys;
			int			i;
			bool		usable = true;

			idxrel = index_open(pk_oid, AccessShareLock);
			nkeys = idxrel->rd_index->indnkeyatts;

			for (i = 0; i < nkeys; i++)
			{
				AttrNumber		attnum;
				int16			opt;
				bool			reverse;
				StrategyNumber	strat;
				Oid				sortop;

				attnum = idxrel->rd_index->indkey.values[i];
				if (attnum == 0)
				{
					/* Expression column — can't sort */
					usable = false;
					break;
				}
				info->attNums[i] = attnum;

				opt = idxrel->rd_indoption[i];
				reverse = (opt & INDOPTION_DESC) != 0;
				info->nullsFirst[i] = (opt & INDOPTION_NULLS_FIRST) != 0;

				strat = reverse ? BTGreaterStrategyNumber
								: BTLessStrategyNumber;
				sortop = get_opfamily_member(idxrel->rd_opfamily[i],
											 idxrel->rd_opcintype[i],
											 idxrel->rd_opcintype[i],
											 strat);
				if (!OidIsValid(sortop))
				{
					usable = false;
					break;
				}
				info->sortOperators[i] = sortop;
				info->sortCollations[i] = idxrel->rd_indcollation[i];
			}

			if (usable)
			{
				Oid		first_col_typid;

				info->pk_index_oid = pk_oid;
				info->nkeys = nkeys;

				/* Determine zone map usability from first PK column type */
				first_col_typid = TupleDescAttr(RelationGetDescr(rel),
												info->attNums[0] - 1)->atttypid;
				info->zm_pk_typid = first_col_typid;
				info->zm_usable = (first_col_typid == INT2OID ||
								   first_col_typid == INT4OID ||
								   first_col_typid == INT8OID ||
								   first_col_typid == TIMESTAMPOID ||
								   first_col_typid == TIMESTAMPTZOID ||
								   first_col_typid == DATEOID ||
								   first_col_typid == UUIDOID);

				/* TEXT/VARCHAR: usable only with C/POSIX collation */
				if (!info->zm_usable &&
					(first_col_typid == TEXTOID ||
					 first_col_typid == VARCHAROID))
				{
					Oid		collation = TupleDescAttr(RelationGetDescr(rel),
													  info->attNums[0] - 1)->attcollation;

					if (collation == C_COLLATION_OID)
						info->zm_usable = true;
				}

				/* Detect column 2 usability for composite PK */
				info->zm_col2_usable = false;
				info->zm_pk_typid2 = InvalidOid;
				if (nkeys >= 2)
				{
					Oid		second_col_typid;

					second_col_typid = TupleDescAttr(RelationGetDescr(rel),
													 info->attNums[1] - 1)->atttypid;
					if (second_col_typid == INT2OID ||
						second_col_typid == INT4OID ||
						second_col_typid == INT8OID ||
						second_col_typid == TIMESTAMPOID ||
						second_col_typid == TIMESTAMPTZOID ||
						second_col_typid == DATEOID ||
						second_col_typid == UUIDOID)
					{
						info->zm_col2_usable = true;
						info->zm_pk_typid2 = second_col_typid;
					}
					else if (second_col_typid == TEXTOID ||
							 second_col_typid == VARCHAROID)
					{
						Oid		coll2 = TupleDescAttr(RelationGetDescr(rel),
													  info->attNums[1] - 1)->attcollation;

						if (coll2 == C_COLLATION_OID)
						{
							info->zm_col2_usable = true;
							info->zm_pk_typid2 = second_col_typid;
						}
					}
				}
			}
			else
			{
				info->pk_index_oid = InvalidOid;
				info->nkeys = 0;
				info->zm_usable = false;
				info->zm_col2_usable = false;
				info->zm_pk_typid2 = InvalidOid;
			}

			index_close(idxrel, AccessShareLock);
		}
		else
		{
			info->pk_index_oid = InvalidOid;
			info->nkeys = 0;
			info->zm_usable = false;
		}

		info->pk_probed = true;
	}

	/* Auto-load zone map if usable PK and not yet loaded */
	if (info->zm_usable && !info->zm_loaded)
	{
		BlockNumber nblocks = RelationGetNumberOfBlocks(rel);

		if (nblocks > 1)		/* meta page + at least 1 data page */
			sorted_heap_zonemap_load(rel, info);
	}

	return info;
}

/*
 * Relcache invalidation callback.
 *
 * When an index is created or dropped, PG fires relcache invalidation
 * for the parent table.  We clear pk_probed so the next multi_insert
 * re-discovers the (possibly new) PK.  Also clear zm_loaded so the
 * zone map is re-read from disk.
 */
void
sorted_heap_relcache_callback(Datum arg, Oid relid)
{
	if (sorted_heap_relinfo_hash == NULL)
		return;

	if (OidIsValid(relid))
	{
		SortedHeapRelInfo *info;

		info = hash_search(sorted_heap_relinfo_hash, &relid,
						   HASH_FIND, NULL);
		if (info != NULL)
		{
			info->pk_probed = false;
			info->zm_loaded = false;
			if (info->zm_overflow)
			{
				pfree(info->zm_overflow);
				info->zm_overflow = NULL;
			}
			if (info->zm_overflow_blocks)
			{
				pfree(info->zm_overflow_blocks);
				info->zm_overflow_blocks = NULL;
			}
			info->zm_overflow_nentries = 0;
			info->zm_overflow_alloc = 0;
			info->zm_total_entries = 0;
		}
	}
	else
	{
		/* Invalidate all entries */
		HASH_SEQ_STATUS status;
		SortedHeapRelInfo *info;

		hash_seq_init(&status, sorted_heap_relinfo_hash);
		while ((info = hash_seq_search(&status)) != NULL)
		{
			info->pk_probed = false;
			info->zm_loaded = false;
			if (info->zm_overflow)
			{
				pfree(info->zm_overflow);
				info->zm_overflow = NULL;
			}
			if (info->zm_overflow_blocks)
			{
				pfree(info->zm_overflow_blocks);
				info->zm_overflow_blocks = NULL;
			}
			info->zm_overflow_nentries = 0;
			info->zm_overflow_alloc = 0;
			info->zm_total_entries = 0;
		}
	}
}

/*
 * Remove entry from cache on DDL (CREATE TABLE, TRUNCATE).
 */
static void
sorted_heap_relinfo_invalidate(Oid relid)
{
	SortedHeapRelInfo *info;

	if (sorted_heap_relinfo_hash == NULL)
		return;

	info = hash_search(sorted_heap_relinfo_hash, &relid, HASH_FIND, NULL);
	if (info != NULL)
	{
		if (info->zm_overflow)
		{
			pfree(info->zm_overflow);
			info->zm_overflow = NULL;
		}
		if (info->zm_overflow_blocks)
		{
			pfree(info->zm_overflow_blocks);
			info->zm_overflow_blocks = NULL;
		}
	}

	hash_search(sorted_heap_relinfo_hash, &relid, HASH_REMOVE, NULL);
}

/* ----------------------------------------------------------------
 *  Zone map load / flush
 * ---------------------------------------------------------------- */

/*
 * Load zone map from meta page into relinfo cache.
 * Handles v2/v3 meta pages gracefully, and v4 backward compatibility
 * (16-byte entries expanded to 32-byte v5 format).
 */
void
sorted_heap_zonemap_load(Relation rel, SortedHeapRelInfo *info)
{
	Buffer		metabuf;
	Page		metapage;
	char	   *special;
	uint32		magic;
	uint32		version;

	metabuf = ReadBufferExtended(rel, MAIN_FORKNUM, SORTED_HEAP_META_BLOCK,
								 RBM_NORMAL, NULL);
	LockBuffer(metabuf, BUFFER_LOCK_SHARE);
	metapage = BufferGetPage(metabuf);

	special = (char *) PageGetSpecialPointer(metapage);

	/* Read magic and version from common header prefix */
	memcpy(&magic, special, sizeof(uint32));
	memcpy(&version, special + sizeof(uint32), sizeof(uint32));

	if (magic != SORTED_HEAP_MAGIC || version < 3)
	{
		info->zm_nentries = 0;
		info->zm_scan_valid = false;
		info->zm_overflow_nentries = 0;
		info->zm_overflow_alloc = 0;
		info->zm_total_entries = 0;
		info->zm_overflow_npages = 0;
		info->zm_loaded = true;
		info->zm_gen = sorted_heap_read_zm_generation();
		UnlockReleaseBuffer(metabuf);
		return;
	}

	if (version <= 4)
	{
		/* v3/v4 format: 16-byte entries, 24-byte header */
		SortedHeapMetaPageDataV4 *meta4 =
			(SortedHeapMetaPageDataV4 *) special;
		uint16		n4 = Min(meta4->shm_zonemap_nentries,
							 SORTED_HEAP_ZONEMAP_MAX_V4);
		uint16		cache_n = Min(n4, SORTED_HEAP_ZONEMAP_CACHE_MAX);
		uint16		overflow_npages = 0;
		BlockNumber	ovfl_blocks[SORTED_HEAP_META_OVERFLOW_SLOTS];
		int			i;

		/* Expand 16→32 byte entries into cache */
		for (i = 0; i < cache_n; i++)
		{
			info->zm_entries[i].zme_min = meta4->shm_zonemap[i].zme_min;
			info->zm_entries[i].zme_max = meta4->shm_zonemap[i].zme_max;
			info->zm_entries[i].zme_min2 = PG_INT64_MAX;	/* not tracked */
			info->zm_entries[i].zme_max2 = PG_INT64_MIN;
		}
		info->zm_nentries = cache_n;
		info->zm_scan_valid =
			(meta4->shm_flags & SHM_FLAG_ZONEMAP_VALID) != 0;
		info->zm_sorted = false;	/* v3/v4 format predates sorted flag */
		info->zm_disk_sorted_cleared = false;

		if (version >= 4)
		{
			overflow_npages = Min(meta4->shm_overflow_npages,
								  SORTED_HEAP_META_OVERFLOW_SLOTS);
			memcpy(ovfl_blocks, meta4->shm_overflow_blocks,
				   overflow_npages * sizeof(BlockNumber));
		}
		info->zm_overflow_npages = overflow_npages;
		info->zm_total_entries = cache_n;

		UnlockReleaseBuffer(metabuf);

		/* Read v4 overflow pages if present */
		if (overflow_npages > 0)
		{
			uint32	max_overflow = (uint32) overflow_npages *
				SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE_V4;
			uint32	total_overflow = 0;
			int		p;

			if (info->zm_overflow)
			{
				pfree(info->zm_overflow);
				info->zm_overflow = NULL;
			}

			info->zm_overflow = (SortedHeapZoneMapEntry *)
				MemoryContextAllocZero(TopMemoryContext,
									   max_overflow *
									   sizeof(SortedHeapZoneMapEntry));

			for (p = 0; p < overflow_npages; p++)
			{
				Buffer		ovfl_buf;
				Page		ovfl_page;
				SortedHeapOverflowPageDataV4 *ovfl;

				if (ovfl_blocks[p] == InvalidBlockNumber)
					break;

				ovfl_buf = ReadBufferExtended(rel, MAIN_FORKNUM,
											  ovfl_blocks[p], RBM_NORMAL,
											  NULL);
				LockBuffer(ovfl_buf, BUFFER_LOCK_SHARE);
				ovfl_page = BufferGetPage(ovfl_buf);
				ovfl = (SortedHeapOverflowPageDataV4 *)
					PageGetSpecialPointer(ovfl_page);

				if (ovfl->shmo_magic == SORTED_HEAP_MAGIC &&
					ovfl->shmo_nentries > 0)
				{
					uint16	ne = Min(ovfl->shmo_nentries,
									 SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE_V4);
					int		j;

					for (j = 0; j < ne; j++)
					{
						info->zm_overflow[total_overflow].zme_min =
							ovfl->shmo_entries[j].zme_min;
						info->zm_overflow[total_overflow].zme_max =
							ovfl->shmo_entries[j].zme_max;
						info->zm_overflow[total_overflow].zme_min2 =
							PG_INT64_MAX;
						info->zm_overflow[total_overflow].zme_max2 =
							PG_INT64_MIN;
						total_overflow++;
					}
				}

				UnlockReleaseBuffer(ovfl_buf);
			}

			info->zm_overflow_nentries = total_overflow;
			info->zm_total_entries = cache_n + total_overflow;
		}

		info->zm_loaded = true;
		info->zm_gen = sorted_heap_read_zm_generation();
		return;
	}

	/* v5/v6 format: 32-byte entries, 32-byte header */
	{
		SortedHeapMetaPageData *meta =
			(SortedHeapMetaPageData *) special;
		uint16		n = Min(meta->shm_zonemap_nentries,
							SORTED_HEAP_ZONEMAP_MAX);
		uint16		meta_ovfl_npages = Min(meta->shm_overflow_npages,
										   SORTED_HEAP_META_OVERFLOW_SLOTS);

		/* Copy v5/v6 entries directly (already 32 bytes) */
		info->zm_nentries = n;
		memcpy(info->zm_entries, meta->shm_zonemap,
			   n * sizeof(SortedHeapZoneMapEntry));
		info->zm_scan_valid =
			(meta->shm_flags & SHM_FLAG_ZONEMAP_VALID) != 0;
		info->zm_sorted =
			(meta->shm_flags & SHM_FLAG_ZM_SORTED) != 0;
		info->zm_disk_sorted_cleared = false;

		info->zm_overflow_npages = meta_ovfl_npages;
		info->zm_total_entries = n;

		/* Read overflow pages if present */
		if (meta_ovfl_npages > 0)
		{
			BlockNumber	meta_ovfl_blks[SORTED_HEAP_META_OVERFLOW_SLOTS];
			uint32		total_overflow = 0;
			uint32		alloc_overflow;
			uint32		entries_per_page;
			uint32		alloc_pages;
			uint32		actual_npages = 0;
			int			p;
			bool		is_v6 = (version >= 6);

			entries_per_page = is_v6 ?
				SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE :
				SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE_V5;

			/* Initial allocation for meta-slot pages */
			alloc_overflow = (uint32) meta_ovfl_npages * entries_per_page;
			alloc_pages = meta_ovfl_npages + 16;	/* room for chain */

			memcpy(meta_ovfl_blks, meta->shm_overflow_blocks,
				   meta_ovfl_npages * sizeof(BlockNumber));

			UnlockReleaseBuffer(metabuf);

			if (info->zm_overflow)
			{
				pfree(info->zm_overflow);
				info->zm_overflow = NULL;
			}
			if (info->zm_overflow_blocks)
			{
				pfree(info->zm_overflow_blocks);
				info->zm_overflow_blocks = NULL;
			}

			info->zm_overflow = (SortedHeapZoneMapEntry *)
				MemoryContextAllocZero(TopMemoryContext,
									   alloc_overflow *
									   sizeof(SortedHeapZoneMapEntry));
			info->zm_overflow_blocks = (BlockNumber *)
				MemoryContextAlloc(TopMemoryContext,
								   alloc_pages * sizeof(BlockNumber));

			/* Phase 1: read overflow pages referenced by meta page */
			for (p = 0; p < meta_ovfl_npages; p++)
			{
				Buffer		ovfl_buf;
				Page		ovfl_page;
				uint16		ne;

				if (meta_ovfl_blks[p] == InvalidBlockNumber)
					break;

				info->zm_overflow_blocks[actual_npages++] = meta_ovfl_blks[p];

				ovfl_buf = ReadBufferExtended(rel, MAIN_FORKNUM,
											  meta_ovfl_blks[p], RBM_NORMAL,
											  NULL);
				LockBuffer(ovfl_buf, BUFFER_LOCK_SHARE);
				ovfl_page = BufferGetPage(ovfl_buf);

				if (is_v6)
				{
					SortedHeapOverflowPageData *ovfl =
						(SortedHeapOverflowPageData *)
						PageGetSpecialPointer(ovfl_page);

					if (ovfl->shmo_magic == SORTED_HEAP_MAGIC &&
						ovfl->shmo_nentries > 0)
					{
						ne = Min(ovfl->shmo_nentries,
								 SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE);
						memcpy(&info->zm_overflow[total_overflow],
							   ovfl->shmo_entries,
							   ne * sizeof(SortedHeapZoneMapEntry));
						total_overflow += ne;
					}
				}
				else
				{
					SortedHeapOverflowPageDataV5 *ovfl =
						(SortedHeapOverflowPageDataV5 *)
						PageGetSpecialPointer(ovfl_page);

					if (ovfl->shmo_magic == SORTED_HEAP_MAGIC &&
						ovfl->shmo_nentries > 0)
					{
						ne = Min(ovfl->shmo_nentries,
								 SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE_V5);
						memcpy(&info->zm_overflow[total_overflow],
							   ovfl->shmo_entries,
							   ne * sizeof(SortedHeapZoneMapEntry));
						total_overflow += ne;
					}
				}

				UnlockReleaseBuffer(ovfl_buf);
			}

			/* Phase 2 (v6 only): follow next_block chain */
			if (is_v6 && meta_ovfl_npages > 0)
			{
				BlockNumber	last_meta_blk =
					meta_ovfl_blks[meta_ovfl_npages - 1];
				BlockNumber	next_blk = InvalidBlockNumber;
				Buffer		ovfl_buf;
				Page		ovfl_page;
				SortedHeapOverflowPageData *ovfl;

				/* Read next_block from last meta-slot page */
				if (last_meta_blk != InvalidBlockNumber)
				{
					ovfl_buf = ReadBufferExtended(rel, MAIN_FORKNUM,
												  last_meta_blk,
												  RBM_NORMAL, NULL);
					LockBuffer(ovfl_buf, BUFFER_LOCK_SHARE);
					ovfl_page = BufferGetPage(ovfl_buf);
					ovfl = (SortedHeapOverflowPageData *)
						PageGetSpecialPointer(ovfl_page);
					next_blk = ovfl->shmo_next_block;
					UnlockReleaseBuffer(ovfl_buf);
				}

				while (next_blk != InvalidBlockNumber)
				{
					uint16	ne;

					/* Grow allocation if needed */
					if (total_overflow + entries_per_page > alloc_overflow)
					{
						alloc_overflow = alloc_overflow * 2 + entries_per_page;
						info->zm_overflow = (SortedHeapZoneMapEntry *)
							repalloc(info->zm_overflow,
									 alloc_overflow *
									 sizeof(SortedHeapZoneMapEntry));
						/* Zero new portion */
						memset(&info->zm_overflow[total_overflow], 0,
							   (alloc_overflow - total_overflow) *
							   sizeof(SortedHeapZoneMapEntry));
					}
					if (actual_npages >= alloc_pages)
					{
						alloc_pages = alloc_pages * 2;
						info->zm_overflow_blocks = (BlockNumber *)
							repalloc(info->zm_overflow_blocks,
									 alloc_pages * sizeof(BlockNumber));
					}

					info->zm_overflow_blocks[actual_npages++] = next_blk;

					ovfl_buf = ReadBufferExtended(rel, MAIN_FORKNUM,
												  next_blk, RBM_NORMAL,
												  NULL);
					LockBuffer(ovfl_buf, BUFFER_LOCK_SHARE);
					ovfl_page = BufferGetPage(ovfl_buf);
					ovfl = (SortedHeapOverflowPageData *)
						PageGetSpecialPointer(ovfl_page);

					if (ovfl->shmo_magic != SORTED_HEAP_MAGIC)
					{
						UnlockReleaseBuffer(ovfl_buf);
						break;
					}

					ne = Min(ovfl->shmo_nentries,
							 SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE);
					if (ne > 0)
					{
						memcpy(&info->zm_overflow[total_overflow],
							   ovfl->shmo_entries,
							   ne * sizeof(SortedHeapZoneMapEntry));
						total_overflow += ne;
					}

					next_blk = ovfl->shmo_next_block;
					UnlockReleaseBuffer(ovfl_buf);
				}
			}

			info->zm_overflow_npages = actual_npages;
			info->zm_overflow_nentries = total_overflow;
			info->zm_overflow_alloc = alloc_overflow;
			info->zm_total_entries = n + total_overflow;
			info->zm_loaded = true;
			info->zm_gen = sorted_heap_read_zm_generation();
			return;		/* already released metabuf */
		}
	}

	info->zm_loaded = true;
	info->zm_gen = sorted_heap_read_zm_generation();
	UnlockReleaseBuffer(metabuf);
}

/*
 * Flush zone map from relinfo cache to meta page via GenericXLog.
 * Version-aware: writes v4 (16-byte) or v5 (32-byte) entries
 * depending on the on-disk format.
 */
static void
sorted_heap_zonemap_flush(Relation rel, SortedHeapRelInfo *info)
{
	Buffer				metabuf;
	Page				metapage;
	GenericXLogState   *state;
	char			   *special;
	uint32				version;

	metabuf = ReadBufferExtended(rel, MAIN_FORKNUM, SORTED_HEAP_META_BLOCK,
								 RBM_NORMAL, NULL);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(rel);
	metapage = GenericXLogRegisterBuffer(state, metabuf, 0);
	special = (char *) PageGetSpecialPointer(metapage);

	/* Read on-disk version */
	memcpy(&version, special + sizeof(uint32), sizeof(uint32));

	if (version >= 5)
	{
		/* v5: write 32-byte entries directly */
		SortedHeapMetaPageData *meta = (SortedHeapMetaPageData *) special;
		uint16		n = Min(info->zm_nentries, SORTED_HEAP_ZONEMAP_MAX);

		Assert(meta->shm_magic == SORTED_HEAP_MAGIC);

		meta->shm_zonemap_nentries = n;
		meta->shm_zonemap_pk_typid = info->zm_pk_typid;
		meta->shm_zonemap_pk_typid2 = info->zm_pk_typid2;
		meta->shm_flags &= ~SHM_FLAG_ZM_SORTED;	/* INSERT may break monotonicity */
		memcpy(meta->shm_zonemap, info->zm_entries,
			   n * sizeof(SortedHeapZoneMapEntry));
	}
	else
	{
		/* v4/v3: write 16-byte entries (col1 only) */
		SortedHeapMetaPageDataV4 *meta4 = (SortedHeapMetaPageDataV4 *) special;
		uint16		n = Min(info->zm_nentries, SORTED_HEAP_ZONEMAP_MAX_V4);
		int			i;

		Assert(meta4->shm_magic == SORTED_HEAP_MAGIC);

		meta4->shm_zonemap_nentries = n;
		meta4->shm_zonemap_pk_typid = info->zm_pk_typid;

		for (i = 0; i < n; i++)
		{
			meta4->shm_zonemap[i].zme_min = info->zm_entries[i].zme_min;
			meta4->shm_zonemap[i].zme_max = info->zm_entries[i].zme_max;
		}
	}

	GenericXLogFinish(state);
	UnlockReleaseBuffer(metabuf);

	/* Notify other backends that zone map changed */
	sorted_heap_bump_zm_generation();
}

/*
 * Clear SHM_FLAG_ZM_SORTED on the meta page if it's currently set.
 * Called when monotonicity is lost (overflow update, uncovered insert).
 *
 * Uses zm_disk_sorted_cleared to track whether we have already persisted
 * the flag clear to disk in this session, avoiding redundant buffer reads
 * on repeat calls (e.g. per-tuple uncovered-page path in multi_insert).
 */
static void
sorted_heap_clear_sorted_flag(Relation rel, SortedHeapRelInfo *info)
{
	Buffer				metabuf;
	Page				metapage;
	SortedHeapMetaPageData *meta;

	info->zm_sorted = false;

	if (info->zm_disk_sorted_cleared)
		return;		/* already persisted to disk this session */

	metabuf = ReadBufferExtended(rel, MAIN_FORKNUM, SORTED_HEAP_META_BLOCK,
								 RBM_NORMAL, NULL);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	metapage = BufferGetPage(metabuf);
	meta = (SortedHeapMetaPageData *) PageGetSpecialPointer(metapage);

	if (meta->shm_flags & SHM_FLAG_ZM_SORTED)
	{
		GenericXLogState *state = GenericXLogStart(rel);

		metapage = GenericXLogRegisterBuffer(state, metabuf, 0);
		meta = (SortedHeapMetaPageData *) PageGetSpecialPointer(metapage);
		meta->shm_flags &= ~SHM_FLAG_ZM_SORTED;
		GenericXLogFinish(state);
	}

	info->zm_disk_sorted_cleared = true;

	UnlockReleaseBuffer(metabuf);
	sorted_heap_bump_zm_generation();
}

/*
 * Flush a single overflow page from the in-memory cache to disk.
 * page_idx is the 0-based index into info->zm_overflow_blocks[].
 * Only touches one buffer — much cheaper than a full zonemap_flush.
 */
static void
sorted_heap_zonemap_flush_overflow_page(Relation rel,
										SortedHeapRelInfo *info,
										uint32 page_idx)
{
	BlockNumber		blkno;
	Buffer			buf;
	Page			page;
	GenericXLogState *state;
	SortedHeapOverflowPageData *ovfl;
	uint32			entry_start;	/* overflow-relative index */
	uint32			count;

	Assert(page_idx < info->zm_overflow_npages);
	blkno = info->zm_overflow_blocks[page_idx];
	Assert(blkno != InvalidBlockNumber);

	/* Compute which overflow entries this page covers */
	entry_start = (uint32) page_idx * SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE;
	count = Min(SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE,
				info->zm_overflow_nentries - entry_start);

	buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(rel);
	page = GenericXLogRegisterBuffer(state, buf, 0);
	ovfl = (SortedHeapOverflowPageData *) PageGetSpecialPointer(page);

	Assert(ovfl->shmo_magic == SORTED_HEAP_MAGIC);

	ovfl->shmo_nentries = count;
	memcpy(ovfl->shmo_entries, &info->zm_overflow[entry_start],
		   count * sizeof(SortedHeapZoneMapEntry));

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);

	sorted_heap_bump_zm_generation();
}

/*
 * Update a zone map entry for the given block, creating or widening
 * min/max as needed.  Returns true if the entry was modified.
 * Works for both inline (zmidx < zm_nentries) and overflow entries.
 */
static bool
sorted_heap_zonemap_update_entry(SortedHeapRelInfo *info,
								 uint32 zmidx,
								 TupleTableSlot *slot)
{
	SortedHeapZoneMapEntry *e;
	Datum		val;
	bool		isnull;
	int64		key;
	bool		changed = false;

	if (zmidx >= info->zm_total_entries)
		return false;	/* beyond coverage — caller handles */

	val = slot_getattr(slot, info->attNums[0], &isnull);
	if (isnull)
		return false;
	if (!sorted_heap_key_to_int64(val, info->zm_pk_typid, &key))
		return false;

	e = sorted_heap_get_zm_entry(info, zmidx);

	if (e->zme_min == PG_INT64_MAX)
	{
		/* First tuple tracked on this page */
		e->zme_min = key;
		e->zme_max = key;
		changed = true;
	}
	else
	{
		if (key < e->zme_min)
		{
			e->zme_min = key;
			changed = true;
			info->zm_sorted = false;	/* min decreased — monotonicity broken */
		}
		if (key > e->zme_max)
		{
			e->zme_max = key;
			changed = true;
		}
	}

	/* Track column 2 */
	if (info->zm_col2_usable)
	{
		Datum	val2;
		bool	isnull2;
		int64	key2;

		val2 = slot_getattr(slot, info->attNums[1], &isnull2);
		if (!isnull2 &&
			sorted_heap_key_to_int64(val2, info->zm_pk_typid2, &key2))
		{
			if (e->zme_min2 == PG_INT64_MAX)
			{
				e->zme_min2 = key2;
				e->zme_max2 = key2;
				changed = true;
			}
			else
			{
				if (key2 < e->zme_min2)
				{
					e->zme_min2 = key2;
					changed = true;
				}
				if (key2 > e->zme_max2)
				{
					e->zme_max2 = key2;
					changed = true;
				}
			}
		}
	}

	return changed;
}

/* ----------------------------------------------------------------
 *  Zone map rebuild — full table scan
 *
 *  Scans all tuples in a relation, computes per-page min/max of the
 *  first PK column, and writes the result to the meta page.  Used by
 *  relation_copy_for_cluster (CLUSTER path) and the standalone
 *  sorted_heap_rebuild_zonemap() SQL function.
 * ---------------------------------------------------------------- */
void
sorted_heap_rebuild_zonemap_internal(Relation rel, Oid pk_typid,
									 AttrNumber pk_attnum,
									 Oid pk_typid2,
									 AttrNumber pk_attnum2)
{
	SortedHeapZoneMapEntry *entries;
	uint32			nentries = 0;
	uint32			max_entries;
	uint32			alloc_entries;
	TableScanDesc	scan;
	TupleTableSlot *slot;
	Buffer			metabuf;
	Page			metapage;
	GenericXLogState *gxlog_state;
	SortedHeapMetaPageData *meta;
	uint16			meta_nentries;
	uint32			overflow_npages = 0;
	BlockNumber		overflow_blocks[SORTED_HEAP_META_OVERFLOW_SLOTS];
	bool			track_col2 = OidIsValid(pk_typid2);

	/* Only supported PK types get zone maps.
	 * Cannot probe with sorted_heap_key_to_int64(Int32GetDatum(0), ...)
	 * because pointer-based types (UUID, text) would dereference NULL. */
	switch (pk_typid)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case DATEOID:
		case UUIDOID:
		case TEXTOID:
		case VARCHAROID:
			break;		/* supported */
		default:
			return;		/* not supported */
	}

	/*
	 * Dynamic allocation: start with a reasonable initial size, grow as needed.
	 * No hard cap — zone map covers all data pages in the relation.
	 */
	alloc_entries = Max(SORTED_HEAP_ZONEMAP_MAX +
						(uint32) SORTED_HEAP_META_OVERFLOW_SLOTS *
						SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE,
						1024);
	max_entries = alloc_entries;

	entries = (SortedHeapZoneMapEntry *)
		palloc(max_entries * sizeof(SortedHeapZoneMapEntry));

	/* Initialize to sentinel */
	for (uint32 i = 0; i < max_entries; i++)
	{
		entries[i].zme_min = PG_INT64_MAX;
		entries[i].zme_max = PG_INT64_MIN;
		entries[i].zme_min2 = PG_INT64_MAX;
		entries[i].zme_max2 = PG_INT64_MIN;
	}

	/* Scan all tuples, build per-page min/max */
	slot = table_slot_create(rel, NULL);
	scan = table_beginscan(rel, SnapshotAny, 0, NULL);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		BlockNumber		blk;
		uint32			zmidx;
		Datum			val;
		bool			isnull;
		int64			key;
		SortedHeapZoneMapEntry *e;

		blk = ItemPointerGetBlockNumber(&slot->tts_tid);
		if (blk < 1)
			continue;			/* skip meta page */
		zmidx = blk - 1;

		/* Grow entries array if needed */
		if (zmidx >= max_entries)
		{
			uint32	new_max = Max(max_entries * 2, zmidx + 1);

			entries = (SortedHeapZoneMapEntry *)
				repalloc(entries, new_max * sizeof(SortedHeapZoneMapEntry));

			for (uint32 i = max_entries; i < new_max; i++)
			{
				entries[i].zme_min = PG_INT64_MAX;
				entries[i].zme_max = PG_INT64_MIN;
				entries[i].zme_min2 = PG_INT64_MAX;
				entries[i].zme_max2 = PG_INT64_MIN;
			}
			max_entries = new_max;
		}

		val = slot_getattr(slot, pk_attnum, &isnull);
		if (isnull)
			continue;
		if (!sorted_heap_key_to_int64(val, pk_typid, &key))
			continue;

		e = &entries[zmidx];
		if (e->zme_min == PG_INT64_MAX)
		{
			e->zme_min = key;
			e->zme_max = key;
		}
		else
		{
			if (key < e->zme_min)
				e->zme_min = key;
			if (key > e->zme_max)
				e->zme_max = key;
		}

		/* Track column 2 min/max */
		if (track_col2)
		{
			Datum	val2;
			bool	isnull2;
			int64	key2;

			val2 = slot_getattr(slot, pk_attnum2, &isnull2);
			if (!isnull2 &&
				sorted_heap_key_to_int64(val2, pk_typid2, &key2))
			{
				if (e->zme_min2 == PG_INT64_MAX)
				{
					e->zme_min2 = key2;
					e->zme_max2 = key2;
				}
				else
				{
					if (key2 < e->zme_min2)
						e->zme_min2 = key2;
					if (key2 > e->zme_max2)
						e->zme_max2 = key2;
				}
			}
		}

		if (zmidx >= nentries)
			nentries = zmidx + 1;
	}

	table_endscan(scan);
	ExecDropSingleTupleTableSlot(slot);

	/* Split entries: first 250 go to meta page, rest to overflow pages */
	meta_nentries = Min(nentries, SORTED_HEAP_ZONEMAP_MAX);

	/* Initialize overflow block numbers */
	for (int i = 0; i < SORTED_HEAP_META_OVERFLOW_SLOTS; i++)
		overflow_blocks[i] = InvalidBlockNumber;

	/* Create overflow pages if needed (v6: linked list, no hard cap) */
	if (nentries > SORTED_HEAP_ZONEMAP_MAX)
	{
		uint32		overflow_entries = nentries - SORTED_HEAP_ZONEMAP_MAX;
		SMgrRelation srel = RelationGetSmgr(rel);
		BlockNumber	next_blk = smgrnblocks(srel, MAIN_FORKNUM);
		RelFileLocator rlocator = rel->rd_locator;
		BlockNumber *all_ovfl_blocks;

		overflow_npages =
			(overflow_entries + SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE - 1) /
			SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE;

		/* No hard cap — allocate block number tracking for all pages */
		all_ovfl_blocks = (BlockNumber *)
			palloc(overflow_npages * sizeof(BlockNumber));

		/* Pass 1: allocate and write all overflow pages */
		for (uint32 p = 0; p < overflow_npages; p++)
		{
			PGAlignedBlock	aligned_buf;
			Page			ovfl_page;
			SortedHeapOverflowPageData *ovfl;
			uint32			start = SORTED_HEAP_ZONEMAP_MAX +
				(uint32) p * SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE;
			uint32			count = Min(SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE,
										nentries - start);

			ovfl_page = (Page) aligned_buf.data;
			PageInit(ovfl_page, BLCKSZ,
					 sizeof(SortedHeapOverflowPageData));

			/* Mark page as full so heap never uses it */
			((PageHeader) ovfl_page)->pd_lower =
				((PageHeader) ovfl_page)->pd_upper;

			ovfl = (SortedHeapOverflowPageData *)
				PageGetSpecialPointer(ovfl_page);
			ovfl->shmo_magic = SORTED_HEAP_MAGIC;
			ovfl->shmo_nentries = count;
			ovfl->shmo_page_index = p;
			ovfl->shmo_next_block = InvalidBlockNumber;	/* set in pass 2 */
			ovfl->shmo_padding = 0;
			memcpy(ovfl->shmo_entries, &entries[start],
				   count * sizeof(SortedHeapZoneMapEntry));

			/* WAL-log, then checksum, then write */
			log_newpage(&rlocator, MAIN_FORKNUM, next_blk,
						ovfl_page, true);
			PageSetChecksumInplace(ovfl_page, next_blk);
			smgrextend(srel, MAIN_FORKNUM, next_blk,
					   aligned_buf.data, false);

			all_ovfl_blocks[p] = next_blk;
			next_blk++;
		}

		/*
		 * Pass 2: set next_block pointers for pages beyond meta slots.
		 * Pages 0..min(overflow_npages,32)-1 are referenced from meta page.
		 * If overflow_npages > 32, page 31's next_block → page 32,
		 * page 32's next_block → page 33, etc.
		 */
		if (overflow_npages > SORTED_HEAP_META_OVERFLOW_SLOTS)
		{
			uint32	chain_start = SORTED_HEAP_META_OVERFLOW_SLOTS - 1;

			for (uint32 p = chain_start; p < overflow_npages; p++)
			{
				Buffer		ovfl_buf;
				Page		ovfl_page;
				GenericXLogState *ovfl_state;
				SortedHeapOverflowPageData *ovfl;

				ovfl_buf = ReadBufferExtended(rel, MAIN_FORKNUM,
											  all_ovfl_blocks[p],
											  RBM_NORMAL, NULL);
				LockBuffer(ovfl_buf, BUFFER_LOCK_EXCLUSIVE);

				ovfl_state = GenericXLogStart(rel);
				ovfl_page = GenericXLogRegisterBuffer(ovfl_state,
													  ovfl_buf, 0);
				ovfl = (SortedHeapOverflowPageData *)
					PageGetSpecialPointer(ovfl_page);

				if (p + 1 < overflow_npages)
					ovfl->shmo_next_block = all_ovfl_blocks[p + 1];
				else
					ovfl->shmo_next_block = InvalidBlockNumber;

				GenericXLogFinish(ovfl_state);
				UnlockReleaseBuffer(ovfl_buf);
			}
		}

		/* Copy first 32 (or fewer) block numbers to meta page array */
		for (uint32 p = 0; p < Min(overflow_npages,
								   SORTED_HEAP_META_OVERFLOW_SLOTS); p++)
			overflow_blocks[p] = all_ovfl_blocks[p];

		pfree(all_ovfl_blocks);
	}

	/* Write zone map to meta page */
	metabuf = ReadBufferExtended(rel, MAIN_FORKNUM, SORTED_HEAP_META_BLOCK,
								 RBM_NORMAL, NULL);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

	gxlog_state = GenericXLogStart(rel);
	metapage = GenericXLogRegisterBuffer(gxlog_state, metabuf, 0);
	meta = (SortedHeapMetaPageData *) PageGetSpecialPointer(metapage);
	meta->shm_zonemap_nentries = meta_nentries;
	meta->shm_zonemap_pk_typid = pk_typid;
	meta->shm_zonemap_pk_typid2 = pk_typid2;
	meta->shm_flags |= SHM_FLAG_ZONEMAP_VALID;

	/* Check if entries are monotonically sorted (enables binary search) */
	{
		bool	is_sorted = true;
		int64	prev_max = PG_INT64_MIN;

		for (uint32 j = 0; j < nentries && is_sorted; j++)
		{
			if (entries[j].zme_min == PG_INT64_MAX)
				continue;			/* skip empty pages */
			if (entries[j].zme_min < prev_max)
				is_sorted = false;
			prev_max = entries[j].zme_max;
		}
		if (is_sorted)
			meta->shm_flags |= SHM_FLAG_ZM_SORTED;
		else
			meta->shm_flags &= ~SHM_FLAG_ZM_SORTED;
	}

	memcpy(meta->shm_zonemap, entries,
		   meta_nentries * sizeof(SortedHeapZoneMapEntry));

	/* Write overflow metadata (meta page stores up to 32 block numbers) */
	meta->shm_overflow_npages = Min(overflow_npages,
									SORTED_HEAP_META_OVERFLOW_SLOTS);
	memcpy(meta->shm_overflow_blocks, overflow_blocks,
		   sizeof(overflow_blocks));

	GenericXLogFinish(gxlog_state);
	UnlockReleaseBuffer(metabuf);

	pfree(entries);

	/* Invalidate relinfo cache so next access re-reads */
	sorted_heap_relinfo_invalidate(RelationGetRelid(rel));
	sorted_heap_bump_zm_generation();
}

/* ----------------------------------------------------------------
 *  Meta page initialization via smgr
 *
 *  During relation_set_new_filelocator (CREATE TABLE / TRUNCATE),
 *  rel->rd_locator still points to the OLD filenode.  We bypass the
 *  buffer manager and write the meta page directly to the correct
 *  locator using smgrextend + log_newpage.
 * ---------------------------------------------------------------- */
static void
sorted_heap_init_meta_page_smgr(const RelFileLocator *rlocator,
								ProcNumber backend, bool need_wal)
{
	SMgrRelation		srel;
	PGAlignedBlock		buf;
	Page				page;
	SortedHeapMetaPageData *meta;
	RelFileLocator		rlocator_copy;

	srel = smgropen(*rlocator, backend);

	page = (Page) buf.data;
	PageInit(page, BLCKSZ, sizeof(SortedHeapMetaPageData));

	/* Mark page as full so heap never tries to use block 0 for data */
	((PageHeader) page)->pd_lower = ((PageHeader) page)->pd_upper;

	meta = (SortedHeapMetaPageData *) PageGetSpecialPointer(page);
	meta->shm_magic = SORTED_HEAP_MAGIC;
	meta->shm_version = SORTED_HEAP_VERSION;
	meta->shm_flags = 0;
	meta->shm_pk_index_oid = InvalidOid;
	meta->shm_zonemap_nentries = 0;
	meta->shm_overflow_npages = 0;
	meta->shm_zonemap_pk_typid = InvalidOid;
	meta->shm_zonemap_pk_typid2 = InvalidOid;
	meta->shm_padding = 0;

	/* Initialize zone map entries to sentinel */
	for (int i = 0; i < SORTED_HEAP_ZONEMAP_MAX; i++)
	{
		meta->shm_zonemap[i].zme_min = PG_INT64_MAX;
		meta->shm_zonemap[i].zme_max = PG_INT64_MIN;
		meta->shm_zonemap[i].zme_min2 = PG_INT64_MAX;
		meta->shm_zonemap[i].zme_max2 = PG_INT64_MIN;
	}

	/* Initialize overflow block pointers */
	for (int i = 0; i < SORTED_HEAP_META_OVERFLOW_SLOTS; i++)
		meta->shm_overflow_blocks[i] = InvalidBlockNumber;

	/* WAL-log first (sets LSN on page), then checksum, then write */
	if (need_wal)
	{
		rlocator_copy = *rlocator;
		log_newpage(&rlocator_copy, MAIN_FORKNUM,
					SORTED_HEAP_META_BLOCK, page, true);
	}

	PageSetChecksumInplace(page, SORTED_HEAP_META_BLOCK);
	smgrextend(srel, MAIN_FORKNUM, SORTED_HEAP_META_BLOCK,
			   buf.data, false);
}

/* ----------------------------------------------------------------
 *  DDL lifecycle callbacks
 * ---------------------------------------------------------------- */
static void
sorted_heap_relation_set_new_filelocator(Relation rel,
										 const RelFileLocator *rlocator,
										 char persistence,
										 TransactionId *freezeXid,
										 MultiXactId *minmulti)
{
	const TableAmRoutine *heap = GetHeapamTableAmRoutine();

	heap->relation_set_new_filelocator(rel, rlocator, persistence,
									   freezeXid, minmulti);

	/* Write meta page to the NEW file using its locator directly */
	sorted_heap_init_meta_page_smgr(rlocator, rel->rd_backend,
									persistence == RELPERSISTENCE_PERMANENT);

	sorted_heap_relinfo_invalidate(RelationGetRelid(rel));
}

static void
sorted_heap_relation_nontransactional_truncate(Relation rel)
{
	const TableAmRoutine *heap = GetHeapamTableAmRoutine();

	heap->relation_nontransactional_truncate(rel);
	sorted_heap_relinfo_invalidate(RelationGetRelid(rel));
}

static void
sorted_heap_relation_copy_data(Relation rel,
							   const RelFileLocator *newrlocator)
{
	const TableAmRoutine *heap = GetHeapamTableAmRoutine();

	heap->relation_copy_data(rel, newrlocator);
}

/* ----------------------------------------------------------------
 *  Vacuum callback — delegate to heap, then rebuild zone map if invalid
 * ---------------------------------------------------------------- */
static void
sorted_heap_relation_vacuum(Relation rel, struct VacuumParams *params,
							BufferAccessStrategy bstrategy)
{
	const TableAmRoutine *heap = GetHeapamTableAmRoutine();

	/* Step 1: delegate to heap vacuum (actual tuple cleanup) */
	heap->relation_vacuum(rel, params, bstrategy);

	/* Step 2: rebuild zone map if invalid and GUC enabled */
	if (sorted_heap_vacuum_rebuild_zonemap &&
		RelationGetNumberOfBlocks(rel) > SORTED_HEAP_META_BLOCK)
	{
		Buffer		metabuf;
		Page		metapage;
		SortedHeapMetaPageData *meta;
		bool		need_rebuild = false;

		metabuf = ReadBufferExtended(rel, MAIN_FORKNUM,
									 SORTED_HEAP_META_BLOCK, RBM_NORMAL,
									 NULL);
		LockBuffer(metabuf, BUFFER_LOCK_SHARE);
		metapage = BufferGetPage(metabuf);
		meta = (SortedHeapMetaPageData *) PageGetSpecialPointer(metapage);

		if (meta->shm_magic == SORTED_HEAP_MAGIC &&
			!(meta->shm_flags & SHM_FLAG_ZONEMAP_VALID))
			need_rebuild = true;

		UnlockReleaseBuffer(metabuf);

		if (need_rebuild)
		{
			SortedHeapRelInfo *info = sorted_heap_get_relinfo(rel);

			if (info->zm_usable)
				sorted_heap_rebuild_zonemap_internal(rel,
					info->zm_pk_typid, info->attNums[0],
					info->zm_col2_usable ? info->zm_pk_typid2 : InvalidOid,
					info->zm_col2_usable ? info->attNums[1] : 0);
		}
	}
}

static void
sorted_heap_relation_copy_for_cluster(Relation OldTable,
									  Relation NewTable,
									  Relation OldIndex,
									  bool use_sort,
									  TransactionId OldestXmin,
									  TransactionId *xid_cutoff,
									  MultiXactId *multi_cutoff,
									  double *num_tuples,
									  double *tups_vacuumed,
									  double *tups_recently_dead)
{
	const TableAmRoutine *heap = GetHeapamTableAmRoutine();
	SortedHeapRelInfo *old_info;

	/* Let heap copy data (sorted by OldIndex if use_sort) */
	heap->relation_copy_for_cluster(OldTable, NewTable, OldIndex,
									use_sort, OldestXmin,
									xid_cutoff, multi_cutoff,
									num_tuples, tups_vacuumed,
									tups_recently_dead);

	/*
	 * Rebuild zone map on NewTable from actual page contents.
	 * NewTable has no indexes yet (PG rebuilds them after this callback),
	 * so we get PK metadata from OldTable which has the same schema.
	 */
	old_info = sorted_heap_get_relinfo(OldTable);
	if (old_info->zm_usable)
		sorted_heap_rebuild_zonemap_internal(NewTable,
											 old_info->zm_pk_typid,
											 old_info->attNums[0],
											 old_info->zm_pk_typid2,
											 old_info->zm_col2_usable ?
											 old_info->attNums[1] : 0);
}

/* ----------------------------------------------------------------
 *  Sorted multi_insert
 *
 *  If a PK exists, sort the incoming batch of slot pointers by PK
 *  using qsort_arg + SortSupport, then delegate to heap's
 *  multi_insert.  After placement, update zone map with per-page
 *  min/max of the first PK column.
 * ---------------------------------------------------------------- */

/* Comparison context passed through qsort_arg */
typedef struct SortedHeapCmpCtx
{
	SortedHeapRelInfo *info;
	SortSupportData   *sortKeys;
} SortedHeapCmpCtx;

static int
sorted_heap_cmp_slots(const void *a, const void *b, void *arg)
{
	SortedHeapCmpCtx *ctx = (SortedHeapCmpCtx *) arg;
	TupleTableSlot *sa = *(TupleTableSlot *const *) a;
	TupleTableSlot *sb = *(TupleTableSlot *const *) b;
	int		i;

	for (i = 0; i < ctx->info->nkeys; i++)
	{
		Datum	val1,
				val2;
		bool	null1,
				null2;
		int		cmp;

		val1 = slot_getattr(sa, ctx->info->attNums[i], &null1);
		val2 = slot_getattr(sb, ctx->info->attNums[i], &null2);

		cmp = ApplySortComparator(val1, null1, val2, null2,
								  &ctx->sortKeys[i]);
		if (cmp != 0)
			return cmp;
	}
	return 0;
}

static void
sorted_heap_multi_insert(Relation rel, TupleTableSlot **slots,
						 int nslots, CommandId cid, int options,
						 struct BulkInsertStateData *bistate)
{
	const TableAmRoutine *heap = GetHeapamTableAmRoutine();
	SortedHeapRelInfo *info;

	info = sorted_heap_get_relinfo(rel);

	/* Phase 2: sort batch by PK */
	if (OidIsValid(info->pk_index_oid) && nslots > 1)
	{
		SortSupportData *sortKeys;
		SortedHeapCmpCtx ctx;
		int				i;

		sortKeys = palloc0(sizeof(SortSupportData) * info->nkeys);
		for (i = 0; i < info->nkeys; i++)
		{
			sortKeys[i].ssup_cxt = CurrentMemoryContext;
			sortKeys[i].ssup_collation = info->sortCollations[i];
			sortKeys[i].ssup_nulls_first = info->nullsFirst[i];
			sortKeys[i].ssup_attno = info->attNums[i];
			PrepareSortSupportFromOrderingOp(info->sortOperators[i],
											  &sortKeys[i]);
		}

		ctx.info = info;
		ctx.sortKeys = sortKeys;

		qsort_arg(slots, nslots, sizeof(TupleTableSlot *),
				   sorted_heap_cmp_slots, &ctx);

		pfree(sortKeys);
	}

	/* Delegate to heap */
	heap->multi_insert(rel, slots, nslots, cid, options, bistate);

	/* Phase 3: update zone map from placed tuples */
	if (info->zm_usable)
	{
		bool	inline_dirty = false;
		bool   *ovfl_page_dirty = NULL;
		int		i;

		if (!info->zm_loaded)
			sorted_heap_zonemap_load(rel, info);

		/* Track which overflow pages need flushing */
		if (info->zm_overflow_npages > 0)
			ovfl_page_dirty = palloc0(sizeof(bool) * info->zm_overflow_npages);

		for (i = 0; i < nslots; i++)
		{
			BlockNumber				blk;
			uint32					zmidx;
			Datum					val;
			bool					isnull;
			int64					key;
			SortedHeapZoneMapEntry *e;

			blk = ItemPointerGetBlockNumber(&slots[i]->tts_tid);
			if (blk < 1)
				continue;		/* skip meta page */
			zmidx = blk - 1;	/* data block 1 → index 0 */

			if (zmidx < SORTED_HEAP_ZONEMAP_CACHE_MAX)
			{
				/*
				 * Inline entry — can grow zm_nentries up to CACHE_MAX.
				 * This handles both initial population and updates.
				 */
				val = slot_getattr(slots[i], info->attNums[0], &isnull);
				if (isnull)
					continue;
				if (!sorted_heap_key_to_int64(val, info->zm_pk_typid, &key))
					continue;

				e = &info->zm_entries[zmidx];
				if (e->zme_min == PG_INT64_MAX)
				{
					e->zme_min = key;
					e->zme_max = key;
				}
				else
				{
					if (key < e->zme_min)
					{
						e->zme_min = key;
						sorted_heap_clear_sorted_flag(rel, info);
					}
					if (key > e->zme_max)
						e->zme_max = key;
				}

				if (info->zm_col2_usable)
				{
					Datum	val2;
					bool	isnull2;
					int64	key2;

					val2 = slot_getattr(slots[i], info->attNums[1], &isnull2);
					if (!isnull2 &&
						sorted_heap_key_to_int64(val2, info->zm_pk_typid2, &key2))
					{
						if (e->zme_min2 == PG_INT64_MAX)
						{
							e->zme_min2 = key2;
							e->zme_max2 = key2;
						}
						else
						{
							if (key2 < e->zme_min2)
								e->zme_min2 = key2;
							if (key2 > e->zme_max2)
								e->zme_max2 = key2;
						}
					}
				}

				if (zmidx >= info->zm_nentries)
					info->zm_nentries = zmidx + 1;

				inline_dirty = true;
			}
			else if (zmidx < info->zm_total_entries)
			{
				/*
				 * Overflow entry within loaded coverage.
				 * Update in-place and mark the overflow page dirty.
				 */
				if (sorted_heap_zonemap_update_entry(info, zmidx, slots[i]))
				{
					uint32 ovfl_idx = zmidx - info->zm_nentries;
					uint32 page_idx = ovfl_idx /
						SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE;

					if (page_idx < info->zm_overflow_npages)
						ovfl_page_dirty[page_idx] = true;
				}
			}
			else
			{
				/*
				 * Page beyond current zone map coverage.  Don't
				 * invalidate — the scan path handles uncovered pages
				 * conservatively (includes them in the scan range).
				 */
				sorted_heap_clear_sorted_flag(rel, info);
			}
		}

		if (inline_dirty)
			sorted_heap_zonemap_flush(rel, info);

		/* Flush only the overflow pages that changed */
		if (ovfl_page_dirty)
		{
			for (i = 0; i < (int) info->zm_overflow_npages; i++)
			{
				if (ovfl_page_dirty[i])
					sorted_heap_zonemap_flush_overflow_page(rel, info, i);
			}
			pfree(ovfl_page_dirty);

			/*
			 * Persist cleared sorted flag to disk if overflow updates
			 * broke monotonicity.  (Inline flush already clears the
			 * on-disk flag unconditionally.)
			 */
			sorted_heap_clear_sorted_flag(rel, info);
		}
	}
}

/* ----------------------------------------------------------------
 *  Observability: sorted_heap_zonemap_stats(regclass) → text
 * ---------------------------------------------------------------- */
Datum
sorted_heap_zonemap_stats(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Relation	rel;
	Buffer		metabuf;
	Page		metapage;
	SortedHeapMetaPageData *meta;
	StringInfoData buf;
	int			show;
	uint32		on_disk_version;
	uint16		on_disk_nentries;
	uint16		on_disk_ovfl_npages;
	SortedHeapZoneMapEntry first_entries[5];
	int			n_first_entries;
	BlockNumber	last_meta_ovfl_blk = InvalidBlockNumber;

	rel = table_open(relid, AccessShareLock);

	metabuf = ReadBufferExtended(rel, MAIN_FORKNUM, SORTED_HEAP_META_BLOCK,
								 RBM_NORMAL, NULL);
	LockBuffer(metabuf, BUFFER_LOCK_SHARE);
	metapage = BufferGetPage(metabuf);
	meta = (SortedHeapMetaPageData *) PageGetSpecialPointer(metapage);

	/* Save values before releasing buffer */
	on_disk_version = meta->shm_version;

	initStringInfo(&buf);
	if (on_disk_version >= 5)
	{
		on_disk_nentries = meta->shm_zonemap_nentries;
		on_disk_ovfl_npages = meta->shm_overflow_npages;

		{
			const char *flags_str;
			uint32		f = meta->shm_flags;
			bool		fv = (f & SHM_FLAG_ZONEMAP_VALID) != 0;
			bool		fs = (f & SHM_FLAG_ZM_SORTED) != 0;

			if (fv && fs)
				flags_str = "valid,sorted";
			else if (fv)
				flags_str = "valid";
			else if (fs)
				flags_str = "sorted";
			else
				flags_str = "0";

			appendStringInfo(&buf, "version=%u nentries=%u pk_typid=%u"
							 " pk_typid2=%u flags=%s overflow_pages=%u",
							 meta->shm_version,
							 (unsigned) meta->shm_zonemap_nentries,
							 (unsigned) meta->shm_zonemap_pk_typid,
							 (unsigned) meta->shm_zonemap_pk_typid2,
							 flags_str,
							 (unsigned) meta->shm_overflow_npages);
		}

		/* Save first entries and last overflow block for after release */
		n_first_entries = Min(on_disk_nentries, 5);
		memcpy(first_entries, meta->shm_zonemap,
			   n_first_entries * sizeof(SortedHeapZoneMapEntry));

		if (on_disk_ovfl_npages > 0)
			last_meta_ovfl_blk =
				meta->shm_overflow_blocks[on_disk_ovfl_npages - 1];

		UnlockReleaseBuffer(metabuf);

		/* v6: count chain pages beyond meta slots */
		if (on_disk_version >= 6 && on_disk_ovfl_npages > 0 &&
			last_meta_ovfl_blk != InvalidBlockNumber)
		{
			uint32		total_overflow = on_disk_ovfl_npages;
			BlockNumber	next_blk;
			Buffer		ovfl_buf;
			Page		ovfl_page;
			SortedHeapOverflowPageData *ovfl;

			ovfl_buf = ReadBufferExtended(rel, MAIN_FORKNUM,
										  last_meta_ovfl_blk,
										  RBM_NORMAL, NULL);
			LockBuffer(ovfl_buf, BUFFER_LOCK_SHARE);
			ovfl_page = BufferGetPage(ovfl_buf);
			ovfl = (SortedHeapOverflowPageData *)
				PageGetSpecialPointer(ovfl_page);
			next_blk = ovfl->shmo_next_block;
			UnlockReleaseBuffer(ovfl_buf);

			while (next_blk != InvalidBlockNumber)
			{
				total_overflow++;
				ovfl_buf = ReadBufferExtended(rel, MAIN_FORKNUM,
											  next_blk, RBM_NORMAL, NULL);
				LockBuffer(ovfl_buf, BUFFER_LOCK_SHARE);
				ovfl_page = BufferGetPage(ovfl_buf);
				ovfl = (SortedHeapOverflowPageData *)
					PageGetSpecialPointer(ovfl_page);
				next_blk = ovfl->shmo_next_block;
				UnlockReleaseBuffer(ovfl_buf);
			}

			if (total_overflow > on_disk_ovfl_npages)
				appendStringInfo(&buf, " total_overflow_pages=%u",
								 total_overflow);
		}

		/* Show first few entries */
		for (int i = 0; i < n_first_entries; i++)
		{
			appendStringInfo(&buf, " [%d:" INT64_FORMAT ".." INT64_FORMAT,
							 i + 1,
							 first_entries[i].zme_min,
							 first_entries[i].zme_max);
			if (first_entries[i].zme_min2 != PG_INT64_MAX)
				appendStringInfo(&buf, " c2:" INT64_FORMAT ".." INT64_FORMAT,
								 first_entries[i].zme_min2,
								 first_entries[i].zme_max2);
			appendStringInfoChar(&buf, ']');
		}
	}
	else
	{
		/* v4 format: read via v4 struct to avoid misaligned access */
		SortedHeapMetaPageDataV4 *meta4 =
			(SortedHeapMetaPageDataV4 *) meta;

		appendStringInfo(&buf, "version=%u nentries=%u pk_typid=%u flags=%u"
						 " overflow_pages=%u",
						 meta4->shm_version,
						 (unsigned) meta4->shm_zonemap_nentries,
						 (unsigned) meta4->shm_zonemap_pk_typid,
						 meta4->shm_flags,
						 (meta4->shm_version >= 4) ?
						 (unsigned) meta4->shm_overflow_npages : 0);

		show = Min(meta4->shm_zonemap_nentries, 5);
		for (int i = 0; i < show; i++)
		{
			appendStringInfo(&buf, " [%d:" INT64_FORMAT ".." INT64_FORMAT "]",
							 i + 1,
							 meta4->shm_zonemap[i].zme_min,
							 meta4->shm_zonemap[i].zme_max);
		}

		UnlockReleaseBuffer(metabuf);
	}

	table_close(rel, AccessShareLock);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/* ----------------------------------------------------------------
 *  Index build support — rd_tableam swap trick
 *
 *  Heap's index_build_range_scan checks rd_tableam internally and
 *  takes optimized paths for heap.  We temporarily swap to heap AM
 *  so the build succeeds, then restore.
 * ---------------------------------------------------------------- */
static double
sorted_heap_index_build_range_scan(Relation tableRelation,
								   Relation indexRelation,
								   IndexInfo *indexInfo,
								   bool allow_sync,
								   bool anyvisible,
								   bool progress,
								   BlockNumber start_blockno,
								   BlockNumber numblocks,
								   IndexBuildCallback callback,
								   void *callback_state,
								   TableScanDesc scan)
{
	const TableAmRoutine *heap;
	const TableAmRoutine *old_tableam;
	double		result;

	if (tableRelation == NULL || indexRelation == NULL || indexInfo == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap index_build_range_scan requires valid arguments")));

	heap = GetHeapamTableAmRoutine();
	old_tableam = tableRelation->rd_tableam;

	tableRelation->rd_tableam = heap;
	PG_TRY();
	{
		result = heap->index_build_range_scan(tableRelation,
											  indexRelation,
											  indexInfo,
											  allow_sync,
											  anyvisible,
											  progress,
											  start_blockno,
											  numblocks,
											  callback,
											  callback_state,
											  scan);
	}
	PG_CATCH();
	{
		tableRelation->rd_tableam = old_tableam;
		PG_RE_THROW();
	}
	PG_END_TRY();

	tableRelation->rd_tableam = old_tableam;
	return result;
}

static void
sorted_heap_index_validate_scan(Relation tableRelation,
								Relation indexRelation,
								IndexInfo *indexInfo,
								Snapshot snapshot,
								ValidateIndexState *state)
{
	const TableAmRoutine *heap;
	const TableAmRoutine *old_tableam;

	if (tableRelation == NULL || indexRelation == NULL || indexInfo == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap index_validate_scan requires valid arguments")));

	heap = GetHeapamTableAmRoutine();
	old_tableam = tableRelation->rd_tableam;

	tableRelation->rd_tableam = heap;
	PG_TRY();
	{
		heap->index_validate_scan(tableRelation,
								  indexRelation,
								  indexInfo,
								  snapshot,
								  state);
	}
	PG_CATCH();
	{
		tableRelation->rd_tableam = old_tableam;
		PG_RE_THROW();
	}
	PG_END_TRY();

	tableRelation->rd_tableam = old_tableam;
}

/* ----------------------------------------------------------------
 *  tuple_insert — incremental zone map update
 *
 *  Delegates to heap, then either:
 *  (a) updates the zone map entry in-place if the tuple landed in
 *      a block covered by the zone map (inline or overflow),
 *      preserving scan pruning validity; or
 *  (b) does nothing if the tuple landed outside zone map coverage
 *      (scan path handles uncovered pages conservatively).
 * ---------------------------------------------------------------- */
static void
sorted_heap_tuple_insert(Relation rel, TupleTableSlot *slot,
						 CommandId cid, int options,
						 struct BulkInsertStateData *bistate)
{
	const TableAmRoutine *heap = GetHeapamTableAmRoutine();
	SortedHeapRelInfo *info;

	/* Let heap do the actual insert */
	heap->tuple_insert(rel, slot, cid, options, bistate);

	info = sorted_heap_get_relinfo(rel);
	if (info->zm_scan_valid && info->zm_usable)
	{
		BlockNumber		blk = ItemPointerGetBlockNumber(&slot->tts_tid);
		uint32			zmidx = (blk >= 1) ? (blk - 1) : 0;

		if (blk < 1)
			return;		/* meta page, nothing to track */

		slot_getallattrs(slot);

		if (zmidx < info->zm_total_entries)
		{
			/*
			 * Block within zone map coverage (inline or overflow).
			 * Update entry in-place.
			 */
			if (sorted_heap_zonemap_update_entry(info, zmidx, slot))
			{
				if (zmidx < info->zm_nentries)
				{
					sorted_heap_zonemap_flush(rel, info);
				}
				else
				{
					uint32 ovfl_idx = zmidx - info->zm_nentries;
					uint32 page_idx = ovfl_idx /
						SORTED_HEAP_OVERFLOW_ENTRIES_PER_PAGE;

					if (page_idx < info->zm_overflow_npages)
						sorted_heap_zonemap_flush_overflow_page(
							rel, info, page_idx);
				}
				sorted_heap_clear_sorted_flag(rel, info);
			}
			/* Zone map stays valid — pruning preserved */
		}
		else
		{
			/*
			 * Page beyond current zone map coverage.  Don't invalidate —
			 * the scan path handles uncovered pages conservatively by
			 * including them in the scan range.
			 */
			sorted_heap_clear_sorted_flag(rel, info);
		}
	}
}

/* ----------------------------------------------------------------
 *  sorted_heap_compact(regclass) → void
 *
 *  Convenience wrapper: finds the PK index and runs CLUSTER via
 *  cluster_rel().  Data is rewritten in global PK order with a
 *  fresh zone map built by relation_copy_for_cluster.
 * ---------------------------------------------------------------- */
Datum
sorted_heap_compact(PG_FUNCTION_ARGS)
{
	Oid				relid = PG_GETARG_OID(0);
	Relation		rel;
	Oid				pk_index_oid;
	ClusterParams	params;

	/* Verify ownership — only table owner may compact */
	if (!object_ownercheck(RelationRelationId, relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE, get_rel_name(relid));

	/* Open with lightweight lock to discover PK index */
	rel = table_open(relid, AccessShareLock);

	if (rel->rd_tableam != &sorted_heap_am_routine)
	{
		table_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a sorted_heap table",
						RelationGetRelationName(rel))));
	}

	if (!rel->rd_indexvalid)
		RelationGetIndexList(rel);
	pk_index_oid = rel->rd_pkindex;
	table_close(rel, AccessShareLock);

	if (!OidIsValid(pk_index_oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("\"%s\" has no primary key",
						RelationGetRelationName(rel))));

	ereport(NOTICE,
			(errmsg("sorted_heap_compact acquires AccessExclusiveLock"),
			 errhint("Schedule during maintenance windows. "
					 "Concurrent reads and writes are blocked.")));

	memset(&params, 0, sizeof(params));
#if PG_VERSION_NUM < 180000
	/* PG 17: cluster_rel takes Oid and acquires lock internally */
	cluster_rel(relid, pk_index_oid, &params);
#else
	/* PG 18: cluster_rel takes Relation; it closes the relation */
	rel = table_open(relid, AccessExclusiveLock);
	cluster_rel(rel, pk_index_oid, &params);
#endif

	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 *  sorted_heap_rebuild_zonemap(regclass) → void
 *
 *  Rebuild zone map from actual page contents without rewriting
 *  data.  Useful after VACUUM or when zone map becomes stale.
 * ---------------------------------------------------------------- */
Datum
sorted_heap_rebuild_zonemap_sql(PG_FUNCTION_ARGS)
{
	Oid				relid = PG_GETARG_OID(0);
	Relation		rel;
	SortedHeapRelInfo *info;

	/* Verify ownership */
	if (!object_ownercheck(RelationRelationId, relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE, get_rel_name(relid));

	rel = table_open(relid, AccessShareLock);

	if (rel->rd_tableam != &sorted_heap_am_routine)
	{
		table_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a sorted_heap table",
						RelationGetRelationName(rel))));
	}

	info = sorted_heap_get_relinfo(rel);
	if (info->zm_usable)
		sorted_heap_rebuild_zonemap_internal(rel, info->zm_pk_typid,
											 info->attNums[0],
											 info->zm_pk_typid2,
											 info->zm_col2_usable ?
											 info->attNums[1] : 0);

	table_close(rel, AccessShareLock);
	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 *  sorted_heap_detect_sorted_prefix
 *
 *  Scan zone map entries from the start. The longest initial sequence
 *  where entry[i+1].min >= entry[i].max (no overlap) is the sorted
 *  prefix. Zone map must be valid. If invalid, returns 0 (entire
 *  table goes through tuplesort).
 *
 *  Returns the number of data pages in the sorted prefix (0-based
 *  entry index corresponds to data page = block entry_index + 1).
 * ---------------------------------------------------------------- */
BlockNumber
sorted_heap_detect_sorted_prefix(SortedHeapRelInfo *info)
{
	uint32		i;
	int64		prev_max;
	SortedHeapZoneMapEntry *entry;

	/*
	 * We use zone map entries to detect monotonicity regardless of the
	 * zm_scan_valid flag. The entries for compacted pages are accurate
	 * even after subsequent INSERTs clear the flag. If the zone map
	 * hasn't been populated at all, prefix is 0 (full tuplesort fallback).
	 */
	if (info->zm_total_entries == 0)
		return 0;

	/* First entry is always part of the prefix */
	entry = sorted_heap_get_zm_entry(info, 0);

	/* Skip sentinel entries (no data tracked) */
	if (entry->zme_min == PG_INT64_MAX)
		return 0;

	prev_max = entry->zme_max;

	for (i = 1; i < info->zm_total_entries; i++)
	{
		entry = sorted_heap_get_zm_entry(info, i);

		/* Sentinel = empty page, skip */
		if (entry->zme_min == PG_INT64_MAX)
			continue;

		/* Overlap detected: this entry's min < previous max */
		if (entry->zme_min < prev_max)
			return i;

		prev_max = entry->zme_max;
	}

	/* All entries are sorted */
	return info->zm_total_entries;
}

/* ----------------------------------------------------------------
 *  sorted_heap_merge(regclass) → void
 *
 *  Incremental merge compaction. Detects the sorted prefix from
 *  zone map monotonicity, sequential-scans it (no index needed),
 *  tuplesorts only the unsorted tail, two-way merges into a new
 *  relation.
 *
 *  Benefits over full compact: sequential I/O for sorted prefix,
 *  smaller tuplesort (only unsorted tail), no btree traversal.
 * ---------------------------------------------------------------- */
Datum
sorted_heap_merge(PG_FUNCTION_ARGS)
{
	Oid				relid = PG_GETARG_OID(0);
	Relation		rel;
	SortedHeapRelInfo *info;
	Oid				table_am_oid;
	BlockNumber		total_blocks;
	BlockNumber		total_data_pages;
	BlockNumber		prefix_pages;
	BlockNumber		tail_nblocks;
	Oid				new_relid;
	Relation		new_rel;
	const TableAmRoutine *heap;
	TableScanDesc	prefix_scan = NULL;
	TableScanDesc	tail_scan = NULL;
	TupleTableSlot *prefix_slot;
	TupleTableSlot *tail_slot;
	Tuplesortstate *tupstate = NULL;
	SortSupportData *sortkeys = NULL;
	double			ntuples = 0;
	int				nkeys;
	int				k;
	bool			prefix_valid;
	bool			tail_valid;

	/* Verify ownership */
	if (!object_ownercheck(RelationRelationId, relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE, get_rel_name(relid));

	/* Open with lightweight lock to validate */
	rel = table_open(relid, AccessShareLock);

	if (rel->rd_tableam != &sorted_heap_am_routine)
	{
		table_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a sorted_heap table",
						RelationGetRelationName(rel))));
	}

	if (!rel->rd_indexvalid)
		RelationGetIndexList(rel);
	if (!OidIsValid(rel->rd_pkindex))
	{
		table_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("\"%s\" has no primary key",
						RelationGetRelationName(rel))));
	}

	table_am_oid = rel->rd_rel->relam;
	table_close(rel, AccessShareLock);

	ereport(NOTICE,
			(errmsg("sorted_heap_merge acquires AccessExclusiveLock"),
			 errhint("Schedule during maintenance windows. "
					 "Concurrent reads and writes are blocked.")));

	/* Reopen with exclusive lock */
	rel = table_open(relid, AccessExclusiveLock);
	info = sorted_heap_get_relinfo(rel);

	/* Force zone map reload under exclusive lock */
	info->zm_loaded = false;
	sorted_heap_zonemap_load(rel, info);

	total_blocks = RelationGetNumberOfBlocks(rel);

	/* Empty or single-page table: nothing to merge */
	if (total_blocks <= 1)
	{
		ereport(NOTICE, (errmsg("sorted_heap_merge: table is empty")));
		table_close(rel, AccessExclusiveLock);
		PG_RETURN_VOID();
	}

	total_data_pages = total_blocks - 1;	/* exclude meta page (block 0) */
	prefix_pages = sorted_heap_detect_sorted_prefix(info);

	/*
	 * Already fully sorted?  Only early-exit when the prefix covers ALL
	 * data pages.  If the zone map doesn't cover some trailing pages
	 * (e.g. after INSERT added pages beyond zone map capacity), those
	 * uncovered pages must go through the tail path.
	 */
	if (prefix_pages >= total_data_pages)
	{
		ereport(NOTICE,
				(errmsg("sorted_heap_merge: table is already sorted (%u pages)",
						(unsigned) total_data_pages)));
		table_close(rel, AccessExclusiveLock);
		PG_RETURN_VOID();
	}

	tail_nblocks = total_data_pages - prefix_pages;

	ereport(NOTICE,
			(errmsg("sorted_heap_merge: %u prefix pages (sequential scan), "
					"%u tail pages (tuplesort)",
					(unsigned) prefix_pages, (unsigned) tail_nblocks)));

	/* Create new heap relation (same schema) */
	new_relid = make_new_heap(relid, InvalidOid, table_am_oid,
							  RELPERSISTENCE_PERMANENT,
							  AccessExclusiveLock);
	new_rel = table_open(new_relid, AccessExclusiveLock);
	heap = GetHeapamTableAmRoutine();

	nkeys = info->nkeys;

	/* Prepare SortSupport keys for merge comparison */
	sortkeys = palloc0(sizeof(SortSupportData) * nkeys);
	for (k = 0; k < nkeys; k++)
	{
		SortSupport ssup = &sortkeys[k];

		ssup->ssup_cxt = CurrentMemoryContext;
		ssup->ssup_collation = info->sortCollations[k];
		ssup->ssup_nulls_first = info->nullsFirst[k];
		ssup->ssup_attno = info->attNums[k];
		PrepareSortSupportFromOrderingOp(info->sortOperators[k], ssup);
	}

	/* Create tuple slots */
	prefix_slot = MakeSingleTupleTableSlot(RelationGetDescr(rel),
										   &TTSOpsBufferHeapTuple);
	tail_slot = MakeSingleTupleTableSlot(RelationGetDescr(rel),
										 &TTSOpsMinimalTuple);

	/*
	 * Stream A: sequential scan of sorted prefix (blocks 1..prefix_pages).
	 * If prefix_pages == 0, we skip this entirely.
	 */
	if (prefix_pages > 0)
	{
		prefix_scan = table_beginscan(rel, GetTransactionSnapshot(),
									  0, NULL);
		heap_setscanlimits(prefix_scan,
						   1, prefix_pages);
		prefix_valid = table_scan_getnextslot(prefix_scan,
											  ForwardScanDirection,
											  prefix_slot);
	}
	else
	{
		prefix_valid = false;
	}

	/*
	 * Stream B: tuplesort of unsorted tail (blocks prefix_pages+1..end).
	 * Read tail tuples, feed into tuplesort, then sort.
	 */
	{
		TupleTableSlot *scan_slot;
		AttrNumber	   *attNums = palloc(sizeof(AttrNumber) * nkeys);
		Oid			   *sortOps = palloc(sizeof(Oid) * nkeys);
		Oid			   *sortColls = palloc(sizeof(Oid) * nkeys);
		bool		   *nullsFirst = palloc(sizeof(bool) * nkeys);

		for (k = 0; k < nkeys; k++)
		{
			attNums[k] = info->attNums[k];
			sortOps[k] = info->sortOperators[k];
			sortColls[k] = info->sortCollations[k];
			nullsFirst[k] = info->nullsFirst[k];
		}

		tupstate = tuplesort_begin_heap(RelationGetDescr(rel),
										nkeys, attNums, sortOps,
										sortColls, nullsFirst,
										maintenance_work_mem,
										NULL, TUPLESORT_NONE);

		tail_scan = table_beginscan(rel, GetTransactionSnapshot(),
									0, NULL);
		heap_setscanlimits(tail_scan,
						   1 + prefix_pages, tail_nblocks);

		scan_slot = MakeSingleTupleTableSlot(RelationGetDescr(rel),
											 &TTSOpsBufferHeapTuple);

		while (table_scan_getnextslot(tail_scan, ForwardScanDirection,
									  scan_slot))
		{
			tuplesort_puttupleslot(tupstate, scan_slot);
		}

		ExecDropSingleTupleTableSlot(scan_slot);
		table_endscan(tail_scan);
		tail_scan = NULL;

		tuplesort_performsort(tupstate);

		pfree(attNums);
		pfree(sortOps);
		pfree(sortColls);
		pfree(nullsFirst);
	}

	/* Get first sorted tail tuple */
	tail_valid = tuplesort_gettupleslot(tupstate, true, true,
										tail_slot, NULL);

	/*
	 * Two-way merge: compare prefix_slot vs tail_slot by PK,
	 * write winner to new_rel via heap AM (bypasses sorted_heap
	 * zone map updates).
	 */
	while (prefix_valid || tail_valid)
	{
		bool		use_prefix;

		CHECK_FOR_INTERRUPTS();

		if (!prefix_valid)
			use_prefix = false;
		else if (!tail_valid)
			use_prefix = true;
		else
		{
			/* Compare PK columns */
			int		cmp = 0;

			for (k = 0; k < nkeys; k++)
			{
				Datum	d1, d2;
				bool	n1, n2;

				d1 = slot_getattr(prefix_slot, info->attNums[k], &n1);
				d2 = slot_getattr(tail_slot, info->attNums[k], &n2);
				cmp = ApplySortComparator(d1, n1, d2, n2, &sortkeys[k]);
				if (cmp != 0)
					break;
			}
			use_prefix = (cmp <= 0);
		}

		if (use_prefix)
		{
			heap->tuple_insert(new_rel, prefix_slot,
							   GetCurrentCommandId(true), 0, NULL);
			ntuples++;
			prefix_valid = table_scan_getnextslot(prefix_scan,
												  ForwardScanDirection,
												  prefix_slot);
		}
		else
		{
			heap->tuple_insert(new_rel, tail_slot,
							   GetCurrentCommandId(true), 0, NULL);
			ntuples++;
			tail_valid = tuplesort_gettupleslot(tupstate, true, true,
												tail_slot, NULL);
		}
	}

	/* Cleanup */
	if (prefix_scan)
		table_endscan(prefix_scan);
	tuplesort_end(tupstate);
	ExecDropSingleTupleTableSlot(prefix_slot);
	ExecDropSingleTupleTableSlot(tail_slot);
	pfree(sortkeys);

	/* Rebuild zone map on new table */
	if (info->zm_usable)
		sorted_heap_rebuild_zonemap_internal(new_rel, info->zm_pk_typid,
											 info->attNums[0],
											 info->zm_pk_typid2,
											 info->zm_col2_usable ?
											 info->attNums[1] : 0);

	table_close(new_rel, NoLock);
	table_close(rel, NoLock);

	/* Atomic swap of filenodes */
	finish_heap_swap(relid, new_relid,
					 false,		/* not system catalog */
					 false,		/* no toast swap by content */
					 false,		/* no constraint check */
					 true,		/* is_internal */
					 InvalidTransactionId,
					 InvalidMultiXactId,
					 RELPERSISTENCE_PERMANENT);

	ereport(NOTICE,
			(errmsg("sorted_heap_merge: completed (%.0f tuples, "
					"%u prefix + %u tail pages)",
					ntuples, (unsigned) prefix_pages,
					(unsigned) tail_nblocks)));

	PG_RETURN_VOID();
}
