#include "postgres.h"

#include "access/amapi.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/stratnum.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "commands/vacuum.h"
#include "catalog/index.h"
#include "nodes/execnodes.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/errcodes.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "storage/itemptr.h"
#include "utils/selfuncs.h"
#include "sorted_heap.h"
#include <inttypes.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_sorted_heap_version);
PG_FUNCTION_INFO_V1(pg_sorted_heap_observability);
PG_FUNCTION_INFO_V1(pg_sorted_heap_tableam_handler);
PG_FUNCTION_INFO_V1(pg_sorted_heap_pkidx_handler);
PG_FUNCTION_INFO_V1(pg_sorted_heap_locator_pack);
PG_FUNCTION_INFO_V1(pg_sorted_heap_locator_major);
PG_FUNCTION_INFO_V1(pg_sorted_heap_locator_minor);
PG_FUNCTION_INFO_V1(pg_sorted_heap_locator_to_hex);
PG_FUNCTION_INFO_V1(pg_sorted_heap_locator_pack_int8);
PG_FUNCTION_INFO_V1(pg_sorted_heap_locator_cmp);
PG_FUNCTION_INFO_V1(pg_sorted_heap_locator_advance_major);
PG_FUNCTION_INFO_V1(pg_sorted_heap_locator_next_minor);

#define CLUSTERED_PG_EXTENSION_VERSION "0.9.8"
#define CLUSTERED_PG_OBS_API_VERSION 1

typedef struct ClusteredPgStats
{
	uint64		observability_calls;
	uint64		costestimate_calls;
	uint64		insert_calls;
	uint64		insert_errors;
	uint64		vacuumcleanup_calls;
} ClusteredPgStats;

static ClusteredPgStats pg_sorted_heap_stats = {0};

typedef struct ClusteredLocator
{
	uint64		major_key;
	uint64		minor_key;
} ClusteredLocator;

static bool			pg_sorted_heap_clustered_heapam_initialized = false;
static TableAmRoutine pg_sorted_heap_clustered_heapam_routine;

typedef struct ClusteredPgPkidxBuildState
{
	Relation	heapRelation;
	IndexInfo  *indexInfo;
	int64		index_tuples;
} ClusteredPgPkidxBuildState;


/*
 * Zone map for directed placement: maps minor_key -> BlockNumber so that
 * tuple_insert can direct rows with the same clustering key to the same
 * heap block, achieving physical clustering at insertion time.
 */
typedef struct ClusteredPgZoneMapBlockKey
{
	int64		minor_key;
} ClusteredPgZoneMapBlockKey;

typedef struct ClusteredPgZoneMapBlockEntry
{
	ClusteredPgZoneMapBlockKey key;
	BlockNumber	block;
} ClusteredPgZoneMapBlockEntry;

typedef struct ClusteredPgZoneMapRelInfo
{
	Oid			relid;			/* hash key */
	AttrNumber	key_attnum;		/* heap attribute number of clustering key */
	Oid			key_typid;		/* INT2OID, INT4OID, or INT8OID */
	HTAB	   *block_map;		/* minor_key -> BlockNumber */
	bool		initialized;	/* true once clustering index found */
	bool		probed;			/* true after first index-list scan attempt */
} ClusteredPgZoneMapRelInfo;

static HTAB	   *pg_sorted_heap_zone_map_rels = NULL;
static Oid		pg_sorted_heap_pkidx_am_oid_cache = InvalidOid;

/* Sort helper for multi_insert key grouping */
typedef struct ClusteredPgMultiInsertKeySlot
{
	int64		key;
	int			idx;
	bool		valid;
} ClusteredPgMultiInsertKeySlot;

/* Saved original heap callbacks for delegation */
static void (*pg_sorted_heap_heap_tuple_insert_orig)(Relation rel,
												   TupleTableSlot *slot,
												   CommandId cid,
												   int options,
												   struct BulkInsertStateData *bistate) = NULL;
static void (*pg_sorted_heap_heap_multi_insert_orig)(Relation rel,
												   TupleTableSlot **slots,
												   int nslots,
												   CommandId cid,
												   int options,
												   struct BulkInsertStateData *bistate) = NULL;


static bool pg_sorted_heap_pkidx_int_key_to_int64(Datum value, Oid valueType,
												int64 *minor_key);
static ClusteredPgZoneMapRelInfo *pg_sorted_heap_zone_map_get_relinfo(Relation rel);
static void pg_sorted_heap_zone_map_invalidate(Oid relid);
static void pg_sorted_heap_relcache_callback(Datum arg, Oid relid);
static void pg_sorted_heap_clustered_heap_tuple_insert(Relation rel,
													TupleTableSlot *slot,
													CommandId cid, int options,
													struct BulkInsertStateData *bistate);
static void pg_sorted_heap_clustered_heap_multi_insert(Relation rel,
													TupleTableSlot **slots,
													int nslots,
													CommandId cid, int options,
													struct BulkInsertStateData *bistate);
static int pg_sorted_heap_multi_insert_key_cmp(const void *a, const void *b);

static void pg_sorted_heap_pack_u64_be(uint8_t *dst, uint64 src);
static uint64 pg_sorted_heap_unpack_u64_be(const uint8_t *src);
static void pg_sorted_heap_validate_locator_len(bytea *locator);
static bool pg_sorted_heap_pkidx_insert(Relation indexRelation, Datum *values,
									 bool *isnull, ItemPointer heap_tid,
									 Relation heapRelation,
									 IndexUniqueCheck checkUnique,
									 bool indexUnchanged, IndexInfo *indexInfo);
#if PG_VERSION_NUM >= 180000
static CompareType pg_sorted_heap_pkidx_translate_strategy(StrategyNumber strategy,
													 Oid opfamily);
static StrategyNumber pg_sorted_heap_pkidx_translate_cmptype(CompareType cmptype,
													 Oid opfamily);
#endif
static void pg_sorted_heap_clustered_heap_relation_set_new_filelocator(Relation rel,
														const RelFileLocator *rlocator,
														char persistence,
														TransactionId *freezeXid,
														MultiXactId *minmulti);
static void pg_sorted_heap_clustered_heap_relation_nontransactional_truncate(Relation rel);
static double pg_sorted_heap_clustered_heap_index_build_range_scan(
	Relation tableRelation,
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
static void pg_sorted_heap_clustered_heap_index_validate_scan(
	Relation tableRelation,
	Relation indexRelation,
	IndexInfo *indexInfo,
	Snapshot snapshot,
	ValidateIndexState *state);
static void pg_sorted_heap_clustered_heap_relation_copy_data(Relation rel,
														const RelFileLocator *newrlocator);
static void pg_sorted_heap_clustered_heap_relation_copy_for_cluster(Relation OldTable,
															 Relation NewTable,
															 Relation OldIndex,
															 bool use_sort,
															 TransactionId OldestXmin,
															 TransactionId *xid_cutoff,
															 MultiXactId *multi_cutoff,
															 double *num_tuples,
															 double *tups_vacuumed,
															 double *tups_recently_dead);
static void pg_sorted_heap_clustered_heap_init_tableam_routine(void);



static void
pg_sorted_heap_clustered_heap_relation_set_new_filelocator(Relation rel,
														const RelFileLocator *rlocator,
														char persistence,
														TransactionId *freezeXid,
														MultiXactId *minmulti)
{
	const TableAmRoutine *heap = GetHeapamTableAmRoutine();

	if (heap == NULL || heap->relation_set_new_filelocator == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("heap table access method is unavailable")));

	heap->relation_set_new_filelocator(rel, rlocator, persistence, freezeXid, minmulti);

	pg_sorted_heap_zone_map_invalidate(RelationGetRelid(rel));
}

static void
pg_sorted_heap_clustered_heap_relation_nontransactional_truncate(Relation rel)
{
	const TableAmRoutine *heap = GetHeapamTableAmRoutine();

	if (heap == NULL || heap->relation_nontransactional_truncate == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("heap table access method is unavailable")));

	heap->relation_nontransactional_truncate(rel);
	pg_sorted_heap_zone_map_invalidate(RelationGetRelid(rel));
}

static double
pg_sorted_heap_clustered_heap_index_build_range_scan(Relation tableRelation,
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
	const TableAmRoutine *old_tableam = tableRelation ? tableRelation->rd_tableam : NULL;
	double result;

	if (tableRelation == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("clustered_heap index_build_range_scan requires a valid relation")));

	if (indexRelation == NULL || indexInfo == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("clustered_heap index_build_range_scan requires valid index relation and index info")));

	heap = GetHeapamTableAmRoutine();
	if (heap == NULL || heap->index_build_range_scan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("heap table access method build callback is unavailable")));

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
pg_sorted_heap_clustered_heap_index_validate_scan(Relation tableRelation,
								Relation indexRelation,
								IndexInfo *indexInfo,
								Snapshot snapshot,
								ValidateIndexState *state)
{
	const TableAmRoutine *heap;
	const TableAmRoutine *old_tableam = tableRelation ? tableRelation->rd_tableam : NULL;

	if (tableRelation == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("clustered_heap index_validate_scan requires a valid relation")));

	if (indexRelation == NULL || indexInfo == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("clustered_heap index_validate_scan requires valid index relation and index info")));

	heap = GetHeapamTableAmRoutine();
	if (heap == NULL || heap->index_validate_scan == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("heap table access method validate callback is unavailable")));

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

static void
pg_sorted_heap_clustered_heap_relation_copy_data(Relation rel,
								   const RelFileLocator *newrlocator)
{
	const TableAmRoutine *heap = GetHeapamTableAmRoutine();

	if (heap == NULL || heap->relation_copy_data == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("heap table access method is unavailable")));

	heap->relation_copy_data(rel, newrlocator);
	pg_sorted_heap_zone_map_invalidate(RelationGetRelid(rel));
}

static void
pg_sorted_heap_clustered_heap_relation_copy_for_cluster(Relation OldTable,
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

	if (heap == NULL || heap->relation_copy_for_cluster == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("heap table access method is unavailable")));

	heap->relation_copy_for_cluster(OldTable,
									NewTable,
									OldIndex,
									use_sort,
									OldestXmin,
									xid_cutoff,
									multi_cutoff,
									num_tuples,
									tups_vacuumed,
									tups_recently_dead);
	pg_sorted_heap_zone_map_invalidate(RelationGetRelid(OldTable));
}

/* Maximum distinct keys tracked per relation before resetting zone map */
#define CLUSTERED_PG_ZONE_MAP_MAX_KEYS 1048576

/* Maximum distinct relations tracked in zone map before full reset */
#define CLUSTERED_PG_ZONE_MAP_MAX_RELS 256

/* ----------------------------------------------------------------
 * Zone map: directed placement for physical clustering at INSERT time.
 *
 * On first tuple_insert for a relation, discover the clustered_pk_index
 * (if any), extract the key column number, and build an in-memory
 * minor_key -> BlockNumber map.  For each subsequent insert, look up
 * the key in the zone map and hint the heap via RelationSetTargetBlock.
 * ----------------------------------------------------------------
 */

/*
 * Invalidate zone map for a relation.  Called from lifecycle hooks
 * (truncate, new filelocator, copy_data, copy_for_cluster) to prevent
 * stale block references after physical storage changes.
 */
static void
pg_sorted_heap_zone_map_invalidate(Oid relid)
{
	ClusteredPgZoneMapRelInfo *info;

	if (pg_sorted_heap_zone_map_rels == NULL)
		return;

	info = hash_search(pg_sorted_heap_zone_map_rels, &relid, HASH_FIND, NULL);
	if (info != NULL)
	{
		if (info->block_map != NULL)
			hash_destroy(info->block_map);
		info->block_map = NULL;
		info->initialized = false;
		hash_search(pg_sorted_heap_zone_map_rels, &relid, HASH_REMOVE, NULL);
	}
}

/*
 * Relcache invalidation callback: clears zone map negative cache so that
 * newly created clustered_pk_index indexes are discovered on next insert.
 */
static void
pg_sorted_heap_relcache_callback(Datum arg, Oid relid)
{
	if (pg_sorted_heap_zone_map_rels == NULL)
		return;

	if (OidIsValid(relid))
	{
		pg_sorted_heap_zone_map_invalidate(relid);
	}
	else
	{
		/* InvalidOid = full invalidation: destroy all entries */
		HASH_SEQ_STATUS status;
		ClusteredPgZoneMapRelInfo *entry;

		hash_seq_init(&status, pg_sorted_heap_zone_map_rels);
		while ((entry = hash_seq_search(&status)) != NULL)
		{
			if (entry->block_map != NULL)
				hash_destroy(entry->block_map);
		}
		hash_destroy(pg_sorted_heap_zone_map_rels);
		pg_sorted_heap_zone_map_rels = NULL;
	}
}

static Oid
pg_sorted_heap_get_pkidx_am_oid(void)
{
	if (!OidIsValid(pg_sorted_heap_pkidx_am_oid_cache))
		pg_sorted_heap_pkidx_am_oid_cache = get_am_oid("clustered_pk_index", true);
	return pg_sorted_heap_pkidx_am_oid_cache;
}

static ClusteredPgZoneMapRelInfo *
pg_sorted_heap_zone_map_get_relinfo(Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	ClusteredPgZoneMapRelInfo *info;
	bool		found;

	/* Create top-level hash on first call */
	if (pg_sorted_heap_zone_map_rels == NULL)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(ClusteredPgZoneMapRelInfo);
		ctl.hcxt = TopMemoryContext;
		pg_sorted_heap_zone_map_rels = hash_create("pg_sorted_heap zone map rels",
												 16, &ctl,
												 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	/*
	 * Overflow guard: if tracking too many relations (e.g. after many
	 * CREATE/DROP cycles), destroy and recreate the top-level HTAB.
	 * This also cleans up zombie entries for dropped tables.
	 */
	if (hash_get_num_entries(pg_sorted_heap_zone_map_rels) >=
		CLUSTERED_PG_ZONE_MAP_MAX_RELS)
	{
		HASH_SEQ_STATUS status;
		ClusteredPgZoneMapRelInfo *entry;

		hash_seq_init(&status, pg_sorted_heap_zone_map_rels);
		while ((entry = hash_seq_search(&status)) != NULL)
		{
			if (entry->block_map != NULL)
				hash_destroy(entry->block_map);
		}
		hash_destroy(pg_sorted_heap_zone_map_rels);
		{
			HASHCTL		ctl;

			memset(&ctl, 0, sizeof(ctl));
			ctl.keysize = sizeof(Oid);
			ctl.entrysize = sizeof(ClusteredPgZoneMapRelInfo);
			ctl.hcxt = TopMemoryContext;
			pg_sorted_heap_zone_map_rels = hash_create("pg_sorted_heap zone map rels",
													 16, &ctl,
													 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		}
	}

	info = hash_search(pg_sorted_heap_zone_map_rels, &relid, HASH_ENTER, &found);
	if (!found)
	{
		info->relid = relid;
		info->key_attnum = InvalidAttrNumber;
		info->key_typid = InvalidOid;
		info->block_map = NULL;
		info->initialized = false;
		info->probed = false;
	}

	if (!info->initialized && !info->probed)
	{
		Oid			pkidx_am = pg_sorted_heap_get_pkidx_am_oid();
		List	   *indexlist;
		ListCell   *lc;

		info->probed = true;

		if (!OidIsValid(pkidx_am))
			return info;

		indexlist = RelationGetIndexList(rel);
		foreach(lc, indexlist)
		{
			Oid			indexoid = lfirst_oid(lc);
			Relation	indexrel = index_open(indexoid, AccessShareLock);

			if (indexrel->rd_rel->relam == pkidx_am &&
				indexrel->rd_index->indnatts >= 1)
			{
				AttrNumber	heap_attnum = indexrel->rd_index->indkey.values[0];
				TupleDesc	idxdesc = RelationGetDescr(indexrel);

				if (heap_attnum > 0 && idxdesc->natts > 0)
				{
					HASHCTL		ctl;

					info->key_attnum = heap_attnum;
					info->key_typid = TupleDescAttr(idxdesc, 0)->atttypid;

					memset(&ctl, 0, sizeof(ctl));
					ctl.keysize = sizeof(ClusteredPgZoneMapBlockKey);
					ctl.entrysize = sizeof(ClusteredPgZoneMapBlockEntry);
					ctl.hcxt = TopMemoryContext;
					info->block_map = hash_create("pg_sorted_heap zone block map",
												  256, &ctl,
												  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
					info->initialized = true;
				}
				index_close(indexrel, AccessShareLock);
				break;
			}
			index_close(indexrel, AccessShareLock);
		}
		list_free(indexlist);
	}

	return info;
}

/*
 * Reset zone map block_map if it exceeds the max key limit.
 * Prevents unbounded memory growth in TopMemoryContext for
 * high-cardinality workloads.
 */
static void
pg_sorted_heap_zone_map_check_overflow(ClusteredPgZoneMapRelInfo *relinfo)
{
	if (relinfo->block_map != NULL &&
		hash_get_num_entries(relinfo->block_map) >=
		CLUSTERED_PG_ZONE_MAP_MAX_KEYS)
	{
		HASHCTL		ctl;

		hash_destroy(relinfo->block_map);
		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(ClusteredPgZoneMapBlockKey);
		ctl.entrysize = sizeof(ClusteredPgZoneMapBlockEntry);
		ctl.hcxt = TopMemoryContext;
		relinfo->block_map = hash_create("pg_sorted_heap zone block map",
										 256, &ctl,
										 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
}

static void
pg_sorted_heap_clustered_heap_tuple_insert(Relation rel, TupleTableSlot *slot,
										 CommandId cid, int options,
										 struct BulkInsertStateData *bistate)
{
	ClusteredPgZoneMapRelInfo *relinfo;
	int64		minor_key = 0;
	bool		key_valid = false;

	relinfo = pg_sorted_heap_zone_map_get_relinfo(rel);

	if (relinfo != NULL && relinfo->initialized)
	{
		Datum	val;
		bool	isnull;

		val = slot_getattr(slot, relinfo->key_attnum, &isnull);
		if (!isnull &&
			pg_sorted_heap_pkidx_int_key_to_int64(val, relinfo->key_typid,
												&minor_key))
		{
			ClusteredPgZoneMapBlockKey mapkey;
			ClusteredPgZoneMapBlockEntry *entry;

			mapkey.minor_key = minor_key;
			entry = hash_search(relinfo->block_map, &mapkey, HASH_FIND, NULL);
			if (entry != NULL)
			{
				/*
				 * Validate block is still within the relation's file.
				 * VACUUM can truncate trailing blocks, leaving stale
				 * zone map entries.  Evict and skip if invalid.
				 */
				if (entry->block < RelationGetNumberOfBlocks(rel))
					RelationSetTargetBlock(rel, entry->block);
				else
					hash_search(relinfo->block_map, &mapkey,
								HASH_REMOVE, NULL);
			}

			key_valid = true;
		}
	}

	/* Delegate to standard heap insert */
	pg_sorted_heap_heap_tuple_insert_orig(rel, slot, cid, options, bistate);

	/* Record actual placement in zone map */
	if (key_valid && relinfo != NULL && relinfo->block_map != NULL)
	{
		BlockNumber		actual_block = ItemPointerGetBlockNumber(&slot->tts_tid);
		ClusteredPgZoneMapBlockKey mapkey;
		ClusteredPgZoneMapBlockEntry *entry;
		bool			found;

		pg_sorted_heap_zone_map_check_overflow(relinfo);

		mapkey.minor_key = minor_key;
		entry = hash_search(relinfo->block_map, &mapkey, HASH_ENTER, &found);
		entry->block = actual_block;
	}
}

static int
pg_sorted_heap_multi_insert_key_cmp(const void *a, const void *b)
{
	const ClusteredPgMultiInsertKeySlot *ka = (const ClusteredPgMultiInsertKeySlot *) a;
	const ClusteredPgMultiInsertKeySlot *kb = (const ClusteredPgMultiInsertKeySlot *) b;

	/* Invalid keys sort to end */
	if (!ka->valid && !kb->valid) return 0;
	if (!ka->valid) return 1;
	if (!kb->valid) return -1;

	if (ka->key < kb->key) return -1;
	if (ka->key > kb->key) return 1;
	return 0;
}

/*
 * Threshold: if a multi_insert batch has more than this many distinct keys,
 * skip sort+group (too expensive) and fall back to lightweight placement
 * that just sets target for the first slot and records all placements.
 */
#define CLUSTERED_PG_MULTI_INSERT_GROUP_THRESHOLD 64

static void
pg_sorted_heap_clustered_heap_multi_insert(Relation rel, TupleTableSlot **slots,
										 int nslots, CommandId cid, int options,
										 struct BulkInsertStateData *bistate)
{
	ClusteredPgZoneMapRelInfo *relinfo;
	ClusteredPgMultiInsertKeySlot *ks;
	TupleTableSlot **sorted_slots;
	int			pos;
	int			i;
	int			distinct_keys;
	int64		prev_key;
	bool		prev_valid;

	relinfo = pg_sorted_heap_zone_map_get_relinfo(rel);

	/* No directed placement possible: delegate directly */
	if (relinfo == NULL || !relinfo->initialized || nslots <= 0)
	{
		pg_sorted_heap_heap_multi_insert_orig(rel, slots, nslots,
											cid, options, bistate);
		return;
	}

	/* Extract clustering key from every slot and count distinct keys */
	ks = palloc(nslots * sizeof(ClusteredPgMultiInsertKeySlot));
	distinct_keys = 0;
	prev_key = 0;
	prev_valid = false;

	for (i = 0; i < nslots; i++)
	{
		Datum	val;
		bool	isnull;

		ks[i].idx = i;
		val = slot_getattr(slots[i], relinfo->key_attnum, &isnull);
		if (!isnull &&
			pg_sorted_heap_pkidx_int_key_to_int64(val, relinfo->key_typid,
												&ks[i].key))
			ks[i].valid = true;
		else
		{
			ks[i].key = 0;
			ks[i].valid = false;
		}

		/* Approximate distinct key count (exact would need a hash) */
		if (ks[i].valid && (!prev_valid || ks[i].key != prev_key))
		{
			distinct_keys++;
			prev_key = ks[i].key;
			prev_valid = true;
		}
	}

	/*
	 * Fast path: if too many distinct keys in this batch, skip sort+group.
	 * Just hint with the first valid key and insert in one call.
	 * The zone map still records placements for future batches.
	 */
	if (distinct_keys > CLUSTERED_PG_MULTI_INSERT_GROUP_THRESHOLD)
	{
		/* Set target block for first valid key */
		for (i = 0; i < nslots; i++)
		{
			if (ks[i].valid)
			{
				ClusteredPgZoneMapBlockKey mapkey;
				ClusteredPgZoneMapBlockEntry *entry;

				mapkey.minor_key = ks[i].key;
				entry = hash_search(relinfo->block_map, &mapkey,
									HASH_FIND, NULL);
				if (entry != NULL)
				{
					if (entry->block < RelationGetNumberOfBlocks(rel))
						RelationSetTargetBlock(rel, entry->block);
					else
						hash_search(relinfo->block_map, &mapkey,
									HASH_REMOVE, NULL);
				}
				break;
			}
		}

		pg_sorted_heap_heap_multi_insert_orig(rel, slots, nslots,
											cid, options, bistate);

		/*
		 * Record placements efficiently: sort ks by key (lightweight
		 * 12-byte elements), then record only one representative slot
		 * per distinct key.  This reduces hash_search calls from nslots
		 * to distinct_keys.
		 */
		if (relinfo->block_map != NULL)
		{
			pg_sorted_heap_zone_map_check_overflow(relinfo);

			qsort(ks, nslots, sizeof(ClusteredPgMultiInsertKeySlot),
				  pg_sorted_heap_multi_insert_key_cmp);

			for (i = 0; i < nslots; )
			{
				int64	key = ks[i].key;
				bool	valid = ks[i].valid;
				int		last_idx = ks[i].idx;

				while (i < nslots &&
					   ks[i].valid == valid &&
					   (!valid || ks[i].key == key))
				{
					last_idx = ks[i].idx;
					i++;
				}

				if (valid)
				{
					BlockNumber blk;
					ClusteredPgZoneMapBlockKey mk;
					ClusteredPgZoneMapBlockEntry *e;
					bool	found;

					blk = ItemPointerGetBlockNumber(&slots[last_idx]->tts_tid);
					mk.minor_key = key;
					e = hash_search(relinfo->block_map, &mk,
									HASH_ENTER, &found);
					e->block = blk;
				}
			}
		}

		pfree(ks);
		return;
	}

	/* Sort by key so same-key slots are adjacent */
	qsort(ks, nslots, sizeof(ClusteredPgMultiInsertKeySlot),
		  pg_sorted_heap_multi_insert_key_cmp);

	/* Build reordered slot pointer array */
	sorted_slots = palloc(nslots * sizeof(TupleTableSlot *));
	for (i = 0; i < nslots; i++)
		sorted_slots[i] = slots[ks[i].idx];

	/* Process one key group at a time */
	pos = 0;
	while (pos < nslots)
	{
		int		group_start = pos;
		int64	group_key = ks[pos].key;
		bool	group_valid = ks[pos].valid;
		int		group_size;

		while (pos < nslots &&
			   ks[pos].valid == group_valid &&
			   (!group_valid || ks[pos].key == group_key))
			pos++;

		group_size = pos - group_start;

		if (group_valid)
		{
			ClusteredPgZoneMapBlockKey mapkey;
			ClusteredPgZoneMapBlockEntry *entry;

			mapkey.minor_key = group_key;
			entry = hash_search(relinfo->block_map, &mapkey,
								HASH_FIND, NULL);

			/* Release bistate buffer pin so target block takes effect */
			if (bistate != NULL)
				ReleaseBulkInsertStatePin(bistate);

			if (entry != NULL)
			{
				if (entry->block < RelationGetNumberOfBlocks(rel))
					RelationSetTargetBlock(rel, entry->block);
				else
					hash_search(relinfo->block_map, &mapkey,
								HASH_REMOVE, NULL);
			}
		}

		pg_sorted_heap_heap_multi_insert_orig(rel, sorted_slots + group_start,
											group_size, cid, options, bistate);

		/* Record last-used block for this key in zone map */
		if (group_valid && relinfo->block_map != NULL)
		{
			BlockNumber last_block;

			pg_sorted_heap_zone_map_check_overflow(relinfo);

			last_block = ItemPointerGetBlockNumber(
				&sorted_slots[group_start + group_size - 1]->tts_tid);

			if (BlockNumberIsValid(last_block))
			{
				ClusteredPgZoneMapBlockKey mk;
				ClusteredPgZoneMapBlockEntry *e;
				bool	found;

				mk.minor_key = group_key;
				e = hash_search(relinfo->block_map, &mk,
								HASH_ENTER, &found);
				e->block = last_block;
			}
		}
	}

	pfree(sorted_slots);
	pfree(ks);
}

static void
pg_sorted_heap_clustered_heap_init_tableam_routine(void)
{
	const TableAmRoutine *heap;

	if (pg_sorted_heap_clustered_heapam_initialized)
		return;

	heap = GetHeapamTableAmRoutine();
	if (heap == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("heap table access method is unavailable")));

	pg_sorted_heap_clustered_heapam_routine = *heap;
	pg_sorted_heap_clustered_heapam_routine.type = T_TableAmRoutine;
	pg_sorted_heap_clustered_heapam_routine.relation_set_new_filelocator =
		pg_sorted_heap_clustered_heap_relation_set_new_filelocator;
	pg_sorted_heap_clustered_heapam_routine.relation_nontransactional_truncate =
		pg_sorted_heap_clustered_heap_relation_nontransactional_truncate;
	pg_sorted_heap_clustered_heapam_routine.index_build_range_scan =
		pg_sorted_heap_clustered_heap_index_build_range_scan;
	pg_sorted_heap_clustered_heapam_routine.index_validate_scan =
		pg_sorted_heap_clustered_heap_index_validate_scan;
	pg_sorted_heap_clustered_heapam_routine.relation_copy_data =
		pg_sorted_heap_clustered_heap_relation_copy_data;
	pg_sorted_heap_clustered_heapam_routine.relation_copy_for_cluster =
		pg_sorted_heap_clustered_heap_relation_copy_for_cluster;

	/* Directed placement: override insert paths to steer rows by key */
	pg_sorted_heap_heap_tuple_insert_orig = heap->tuple_insert;
	pg_sorted_heap_clustered_heapam_routine.tuple_insert =
		pg_sorted_heap_clustered_heap_tuple_insert;

	pg_sorted_heap_heap_multi_insert_orig = heap->multi_insert;
	pg_sorted_heap_clustered_heapam_routine.multi_insert =
		pg_sorted_heap_clustered_heap_multi_insert;

	pg_sorted_heap_clustered_heapam_initialized = true;
}

static bool
pg_sorted_heap_pkidx_int_key_to_int64(Datum value, Oid valueType, int64 *minor_key)
{
	switch (valueType)
	{
		case INT2OID:
			*minor_key = (int64) DatumGetInt16(value);
			return true;
		case INT4OID:
			*minor_key = (int64) DatumGetInt32(value);
			return true;
		case INT8OID:
			*minor_key = DatumGetInt64(value);
			return true;
		default:
			return false;
	}
}

static bool
pg_sorted_heap_pkidx_extract_minor_key(Relation indexRelation, Datum *values,
									bool *isnull, int64 *minor_key)
{
	TupleDesc	tupdesc;

	if (values == NULL || isnull == NULL || minor_key == NULL)
		return false;
	if (indexRelation == NULL)
		return false;

	tupdesc = RelationGetDescr(indexRelation);
	if (tupdesc == NULL || tupdesc->natts == 0)
		return false;

	if (isnull[0])
		return false;

	return pg_sorted_heap_pkidx_int_key_to_int64(values[0],
											  TupleDescAttr(tupdesc, 0)->atttypid,
											  minor_key);
}

static IndexBulkDeleteResult *
pg_sorted_heap_pkidx_init_bulkdelete_stats(IndexVacuumInfo *info,
										IndexBulkDeleteResult *stats)
{
	IndexBulkDeleteResult *result = stats;

	if (result == NULL)
		result = palloc0_object(IndexBulkDeleteResult);

	if (info != NULL)
	{
		result->estimated_count = info->estimated_count;
		if (info->estimated_count)
			result->num_index_tuples = info->num_heap_tuples;
	}

	return result;
}

static void
pg_sorted_heap_pkidx_build_callback(Relation indexRelation, ItemPointer heap_tid,
                                 Datum *values, bool *isnull, bool tupleIsAlive,
                                 void *state)
{
	ClusteredPgPkidxBuildState *buildstate = (ClusteredPgPkidxBuildState *) state;
	int64		minor_key = 0;

	if (buildstate == NULL || indexRelation == NULL || buildstate->indexInfo == NULL)
		return;
	if (!tupleIsAlive)
		return;
	if (!pg_sorted_heap_pkidx_extract_minor_key(indexRelation, values, isnull, &minor_key))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("pg_sorted_heap build path does not support this index key"),
				 errhint("clustered_pk_index supports exactly one key attribute of types int2, int4 or int8.")));
	buildstate->index_tuples++;
}


static void
pg_sorted_heap_pack_u64_be(uint8_t *dst, uint64 src)
{
	int			i;

	for (i = 0; i < 8; i++)
		dst[i] = (uint8_t) (src >> (56 - (i * 8)));
}

static uint64
pg_sorted_heap_unpack_u64_be(const uint8_t *src)
{
	uint64		result = 0;
	int			i;

	for (i = 0; i < 8; i++)
		result = (result << 8) | (uint64) src[i];

	return result;
}
Datum
pg_sorted_heap_locator_pack(PG_FUNCTION_ARGS)
{
	int64		major = PG_GETARG_INT64(0);
	int64		minor = PG_GETARG_INT64(1);
	bytea	   *locator;
	uint8	   *payload;

	locator = palloc(VARHDRSZ + (int) sizeof(ClusteredLocator));
	SET_VARSIZE(locator, VARHDRSZ + (int) sizeof(ClusteredLocator));
	payload = (uint8 *) VARDATA(locator);

	pg_sorted_heap_pack_u64_be(payload, (uint64) major);
	pg_sorted_heap_pack_u64_be(payload + 8, (uint64) minor);

	PG_RETURN_BYTEA_P(locator);
}

Datum
pg_sorted_heap_locator_pack_int8(PG_FUNCTION_ARGS)
{
	int64		pk = PG_GETARG_INT64(0);
	bytea	   *locator;
	uint8	   *payload;

	locator = palloc(VARHDRSZ + (int) sizeof(ClusteredLocator));
	SET_VARSIZE(locator, VARHDRSZ + (int) sizeof(ClusteredLocator));
	payload = (uint8 *) VARDATA(locator);

	pg_sorted_heap_pack_u64_be(payload, 0);
	pg_sorted_heap_pack_u64_be(payload + 8, (uint64) pk);

	PG_RETURN_BYTEA_P(locator);
}

static void
pg_sorted_heap_validate_locator_len(bytea *locator)
{
	if (VARSIZE_ANY_EXHDR(locator) != (int) sizeof(ClusteredLocator))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("clustered locator must be exactly %zu bytes",
						sizeof(ClusteredLocator))));
}

Datum
pg_sorted_heap_locator_major(PG_FUNCTION_ARGS)
{
	bytea	   *locator = PG_GETARG_BYTEA_P(0);
	const uint8 *payload = (const uint8 *) VARDATA_ANY(locator);

	pg_sorted_heap_validate_locator_len(locator);
	PG_RETURN_INT64((int64) pg_sorted_heap_unpack_u64_be(payload));
}

Datum
pg_sorted_heap_locator_minor(PG_FUNCTION_ARGS)
{
	bytea	   *locator = PG_GETARG_BYTEA_P(0);
	const uint8 *payload = (const uint8 *) VARDATA_ANY(locator);

	pg_sorted_heap_validate_locator_len(locator);
	PG_RETURN_INT64((int64) pg_sorted_heap_unpack_u64_be(payload + 8));
}

Datum
pg_sorted_heap_locator_to_hex(PG_FUNCTION_ARGS)
{
	bytea	   *locator = PG_GETARG_BYTEA_P(0);
	uint64		major_key;
	uint64		minor_key;
	const uint8 *payload = (const uint8 *) VARDATA_ANY(locator);
	char	   *text_repr;

	pg_sorted_heap_validate_locator_len(locator);

	major_key = pg_sorted_heap_unpack_u64_be(payload);
	minor_key = pg_sorted_heap_unpack_u64_be(payload + 8);

	text_repr = psprintf("%016" PRIX64 ":%016" PRIX64,
						 (unsigned long long) major_key,
						 (unsigned long long) minor_key);

	PG_RETURN_TEXT_P(cstring_to_text(text_repr));
}

Datum
pg_sorted_heap_locator_cmp(PG_FUNCTION_ARGS)
{
	bytea	   *a = PG_GETARG_BYTEA_P(0);
	bytea	   *b = PG_GETARG_BYTEA_P(1);
	const uint8 *pa = (const uint8 *) VARDATA_ANY(a);
	const uint8 *pb = (const uint8 *) VARDATA_ANY(b);
	int64		a_major, a_minor;
	int64		b_major, b_minor;

	pg_sorted_heap_validate_locator_len(a);
	pg_sorted_heap_validate_locator_len(b);

	a_major = (int64) pg_sorted_heap_unpack_u64_be(pa);
	a_minor = (int64) pg_sorted_heap_unpack_u64_be(pa + 8);
	b_major = (int64) pg_sorted_heap_unpack_u64_be(pb);
	b_minor = (int64) pg_sorted_heap_unpack_u64_be(pb + 8);

	if (a_major < b_major)
		PG_RETURN_INT32(-1);
	if (a_major > b_major)
		PG_RETURN_INT32(1);
	if (a_minor < b_minor)
		PG_RETURN_INT32(-1);
	if (a_minor > b_minor)
		PG_RETURN_INT32(1);

	PG_RETURN_INT32(0);
}

Datum
pg_sorted_heap_locator_advance_major(PG_FUNCTION_ARGS)
{
	bytea	   *locator = PG_GETARG_BYTEA_P(0);
	int64		delta = PG_GETARG_INT64(1);
	const uint8 *payload = (const uint8 *) VARDATA_ANY(locator);
	uint64		major;
	uint64		minor;
	bytea	   *moved;
	uint8	   *out;

	pg_sorted_heap_validate_locator_len(locator);

	major = pg_sorted_heap_unpack_u64_be(payload);
	minor = pg_sorted_heap_unpack_u64_be(payload + 8);

	{
		int64	signed_major = (int64) major;
		int64	result = signed_major + delta;

		if ((delta > 0 && result < signed_major) ||
			(delta < 0 && result > signed_major))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("major locator overflow")));
		major = (uint64) result;
	}

	moved = palloc(VARHDRSZ + (int) sizeof(ClusteredLocator));
	SET_VARSIZE(moved, VARHDRSZ + (int) sizeof(ClusteredLocator));
	out = (uint8 *) VARDATA(moved);
	pg_sorted_heap_pack_u64_be(out, major);
	pg_sorted_heap_pack_u64_be(out + 8, minor);

	PG_RETURN_BYTEA_P(moved);
}

Datum
pg_sorted_heap_locator_next_minor(PG_FUNCTION_ARGS)
{
	bytea	   *locator = PG_GETARG_BYTEA_P(0);
	int64		delta = PG_GETARG_INT64(1);
	const uint8 *payload = (const uint8 *) VARDATA_ANY(locator);
	uint64		major;
	int64		minor;
	int64		next_minor;
	bytea	   *moved;
	uint8	   *out;

	pg_sorted_heap_validate_locator_len(locator);

	major = pg_sorted_heap_unpack_u64_be(payload);
	minor = (int64) pg_sorted_heap_unpack_u64_be(payload + 8);

	next_minor = minor + delta;
	if ((delta > 0 && next_minor < minor) ||
		(delta < 0 && next_minor > minor))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("minor locator overflow")));

	moved = palloc(VARHDRSZ + (int) sizeof(ClusteredLocator));
	SET_VARSIZE(moved, VARHDRSZ + (int) sizeof(ClusteredLocator));
	out = (uint8 *) VARDATA(moved);
	pg_sorted_heap_pack_u64_be(out, major);
	pg_sorted_heap_pack_u64_be(out + 8, (uint64) next_minor);

	PG_RETURN_BYTEA_P(moved);
}

/*
 * Simple extension identity.
 */
Datum
pg_sorted_heap_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text("pg_sorted_heap " CLUSTERED_PG_EXTENSION_VERSION));
}

Datum
pg_sorted_heap_observability(PG_FUNCTION_ARGS)
{
	char		text_buf[512];

	pg_sorted_heap_stats.observability_calls++;

	snprintf(text_buf, sizeof(text_buf),
			 "pg_sorted_heap=%s api=%d "
			 "counters={observability=%" PRIu64 ",costestimate=%" PRIu64
			 ",index_inserts=%" PRIu64 ",insert_errors=%" PRIu64
			 ",vacuumcleanup=%" PRIu64 "}",
			 CLUSTERED_PG_EXTENSION_VERSION,
			 CLUSTERED_PG_OBS_API_VERSION,
			 (unsigned long long) pg_sorted_heap_stats.observability_calls,
			 (unsigned long long) pg_sorted_heap_stats.costestimate_calls,
			 (unsigned long long) pg_sorted_heap_stats.insert_calls,
			 (unsigned long long) pg_sorted_heap_stats.insert_errors,
			 (unsigned long long) pg_sorted_heap_stats.vacuumcleanup_calls);

	PG_RETURN_TEXT_P(cstring_to_text(text_buf));
}

/*
 * Table AM handler: bootstrap phase delegates to heapam implementation.
 *
 * Current behavior keeps heap semantics, but exposes a dedicated clustered table
 * AM entry point so future locator-aware hooks can be layered in safely.
 */
Datum
pg_sorted_heap_tableam_handler(PG_FUNCTION_ARGS)
{
	/*
	 * Expose clustered_heap wrapper routine: core tuple semantics still delegate
	 * to heap, while clustered metadata lifecycle hooks stay active.
	 */
	pg_sorted_heap_clustered_heap_init_tableam_routine();
	PG_RETURN_POINTER(&pg_sorted_heap_clustered_heapam_routine);
}

/*
 * Index AM handler: minimal skeleton AM used as a safe extension point for
 * incremental development of a clustered index method.
 */

static IndexBuildResult *
pg_sorted_heap_pkidx_build(Relation heapRelation, Relation indexRelation,
						IndexInfo *indexInfo)
{
	ClusteredPgPkidxBuildState buildstate;
	IndexBuildResult *result;

	if (heapRelation == NULL || indexRelation == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("clustered_pk_index ambuild requires valid relations"),
				 errhint("Call CREATE INDEX on a valid relation.")));

	buildstate.heapRelation = heapRelation;
	buildstate.indexInfo = indexInfo;
	buildstate.index_tuples = 0;

	if (indexInfo == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("clustered_pk_index ambuild requires index metadata"),
				 errhint("Call CREATE INDEX with a valid catalog state.")));

	if (indexInfo->ii_NumIndexAttrs != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("clustered_pk_index ambuild supports exactly one key attribute"),
				 errhint("Create a single-column index for the first iteration.")));

	result = palloc0_object(IndexBuildResult);

	result->heap_tuples = table_index_build_scan(heapRelation,
													indexRelation,
													indexInfo,
												(indexInfo == NULL || !indexInfo->ii_Concurrent),
													false,
													pg_sorted_heap_pkidx_build_callback,
													(void *) &buildstate,
													NULL);
	result->index_tuples = (double) buildstate.index_tuples;

	return result;
}

static void
pg_sorted_heap_pkidx_buildempty(Relation indexRelation)
{
	/* No-op: metadata lives in zone map (in-memory) */
	(void) indexRelation;
}

static bool
pg_sorted_heap_pkidx_insert(Relation indexRelation, Datum *values, bool *isnull,
					ItemPointer heap_tid, Relation heapRelation,
					IndexUniqueCheck checkUnique, bool indexUnchanged,
					IndexInfo *indexInfo)
{
	int64		minor_key;

	(void)checkUnique;
	(void)indexUnchanged;
	pg_sorted_heap_stats.insert_calls++;

	if (indexInfo == NULL || indexInfo->ii_NumIndexAttrs != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("clustered_pk_index supports exactly one key attribute"),
				 errhint("Create a single-column index for the first iteration.")));

	if (!pg_sorted_heap_pkidx_extract_minor_key(indexRelation, values, isnull, &minor_key))
			ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("clustered_pk_index currently supports only int2/int4/int8 index key types"),
						 errdetail("Index key is NULL, missing, or has unsupported type.")));

	return true;
}

static IndexBulkDeleteResult *
pg_sorted_heap_pkidx_bulkdelete(IndexVacuumInfo *info,
						IndexBulkDeleteResult *stats,
						IndexBulkDeleteCallback callback,
						void *callback_state)
{
	(void)callback;
	(void)callback_state;
	return pg_sorted_heap_pkidx_init_bulkdelete_stats(info, stats);
}

static IndexBulkDeleteResult *
pg_sorted_heap_pkidx_vacuumcleanup(IndexVacuumInfo *info,
							IndexBulkDeleteResult *stats)
{
	if (info != NULL && !info->analyze_only && info->index != NULL &&
		info->index->rd_index != NULL)
		pg_sorted_heap_stats.vacuumcleanup_calls++;

	return pg_sorted_heap_pkidx_init_bulkdelete_stats(info, stats);
}

static void
pg_sorted_heap_pkidx_costestimate(struct PlannerInfo *root, struct IndexPath *path,
							double loop_count, Cost *startup_cost,
							Cost *total_cost, Selectivity *selectivity,
							double *correlation, double *pages)
{
	/*
	 * This index AM does not support scans (amgettuple=NULL, amgetbitmap=NULL).
	 * Return prohibitively high cost so the planner never selects it.
	 */
	pg_sorted_heap_stats.costestimate_calls++;

	(void) root;
	(void) loop_count;

	*startup_cost = 1.0e10;
	*total_cost = 1.0e10;
	*selectivity = 1.0;
	*correlation = 0.0;
	*pages = (path != NULL && path->indexinfo != NULL) ? 1.0 : 1.0;
}

static bool
pg_sorted_heap_pkidx_validate(Oid opclassoid)
{
	(void)opclassoid;
	return true;
}

#if PG_VERSION_NUM >= 180000
static CompareType
pg_sorted_heap_pkidx_translate_strategy(StrategyNumber strategy, Oid opfamily)
{
	(void) opfamily;

	switch (strategy)
	{
		case BTLessStrategyNumber:
			return COMPARE_LT;
		case BTLessEqualStrategyNumber:
			return COMPARE_LE;
		case BTEqualStrategyNumber:
			return COMPARE_EQ;
		case BTGreaterEqualStrategyNumber:
			return COMPARE_GE;
		case BTGreaterStrategyNumber:
			return COMPARE_GT;
		default:
			return COMPARE_INVALID;
	}
}

static StrategyNumber
pg_sorted_heap_pkidx_translate_cmptype(CompareType cmptype, Oid opfamily)
{
	(void) opfamily;

	switch (cmptype)
	{
		case COMPARE_LT:
			return BTLessStrategyNumber;
		case COMPARE_LE:
			return BTLessEqualStrategyNumber;
		case COMPARE_EQ:
			return BTEqualStrategyNumber;
		case COMPARE_GE:
			return BTGreaterEqualStrategyNumber;
		case COMPARE_GT:
			return BTGreaterStrategyNumber;
		default:
			return InvalidStrategy;
	}
}
#endif

Datum
pg_sorted_heap_pkidx_handler(PG_FUNCTION_ARGS)
{
	static const IndexAmRoutine amroutine = {
		.type = T_IndexAmRoutine,
		.amstrategies = 5,
		.amsupport = 1,
		.amcanorder = false,
		.amcanorderbyop = false,
#if PG_VERSION_NUM >= 180000
		.amcanhash = false,
		.amconsistentequality = true,
		.amconsistentordering = true,
#endif
		.amcanbackward = false,
		.amcanunique = false,
		.amcanmulticol = false,
		.amoptionalkey = false,
		.amsearcharray = false,
		.amsearchnulls = false,
		.amstorage = false,
		.amclusterable = true,
		.ampredlocks = false,
		.amcanparallel = false,
		.amcanbuildparallel = false,
		.amcaninclude = false,
		.amusemaintenanceworkmem = false,
		.amsummarizing = false,
		.amparallelvacuumoptions = VACUUM_OPTION_NO_PARALLEL,
		.amkeytype = InvalidOid,

		.ambuild = pg_sorted_heap_pkidx_build,
		.ambuildempty = pg_sorted_heap_pkidx_buildempty,
		.aminsert = pg_sorted_heap_pkidx_insert,
		.ambulkdelete = pg_sorted_heap_pkidx_bulkdelete,
		.amvacuumcleanup = pg_sorted_heap_pkidx_vacuumcleanup,
		.amcanreturn = NULL,
#if PG_VERSION_NUM >= 180000
		.amgettreeheight = NULL,
#endif
		.amcostestimate = pg_sorted_heap_pkidx_costestimate,
		.amoptions = NULL,
		.amvalidate = pg_sorted_heap_pkidx_validate,
		.ambeginscan = NULL,
		.amrescan = NULL,
		.amgettuple = NULL,
		.amgetbitmap = NULL,
		.amendscan = NULL,
		.ammarkpos = NULL,
		.amrestrpos = NULL,
#if PG_VERSION_NUM >= 180000
		.amtranslatestrategy = pg_sorted_heap_pkidx_translate_strategy,
		.amtranslatecmptype = pg_sorted_heap_pkidx_translate_cmptype,
#endif
	};
	IndexAmRoutine *result;

	result = (IndexAmRoutine *) palloc(sizeof(IndexAmRoutine));
	*result = amroutine;
	PG_RETURN_POINTER(result);
}

void
_PG_init(void)
{
	DefineCustomBoolVariable("sorted_heap.enable_scan_pruning",
							 "Enable zone map scan pruning for sorted_heap tables.",
							 NULL,
							 &sorted_heap_enable_scan_pruning,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("sorted_heap.vacuum_rebuild_zonemap",
							 "Rebuild zone map during VACUUM when invalid.",
							 NULL,
							 &sorted_heap_vacuum_rebuild_zonemap,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("sorted_heap.ann_timing",
							 "Log svec_ann_scan phase timing via DEBUG1.",
							 NULL,
							 &sorted_heap_ann_timing,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("sorted_heap.hnsw_cache_l0",
							 "Cache HNSW L0 nodes in session memory to "
							 "eliminate per-node btree overhead. "
							 "Uses ~908 bytes per node (86 MB for 103K nodes). "
							 "Immutable sidecar tables only; no in-place update detection.",
							 NULL,
							 &sorted_heap_hnsw_cache_l0,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	MarkGUCPrefixReserved("sorted_heap");

	CacheRegisterRelcacheCallback(pg_sorted_heap_relcache_callback, (Datum) 0);
	CacheRegisterRelcacheCallback(sorted_heap_relcache_callback, (Datum) 0);
	sorted_heap_scan_init();
}
