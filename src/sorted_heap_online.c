/*
 * sorted_heap_online.c
 *
 * Online (non-blocking) compaction for sorted_heap tables.
 *
 * Uses a pg_repack-style trigger-based approach:
 *   Phase 1: Create log table + AFTER trigger to capture concurrent DML
 *   Phase 2: Copy old table → new table in PK order (ShareUpdateExclusiveLock)
 *   Phase 3: Replay captured changes, brief AccessExclusiveLock for swap
 *
 * During phases 1-2, concurrent SELECTs and DML proceed normally.
 * AccessExclusiveLock is held only for the final filenode swap.
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/multixact.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "commands/cluster.h"
#include "commands/defrem.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/sortsupport.h"
#include "utils/tuplesort.h"
#include "utils/uuid.h"

#include "sorted_heap.h"

/* ----------------------------------------------------------------
 *  PK → TID hash table for fast lookups in new table
 * ---------------------------------------------------------------- */
typedef struct PKTidEntry
{
	int64			pk_val;		/* hash key */
	ItemPointerData tid;		/* location in new table */
	char			status;		/* STATUS_IN_USE */
} PKTidEntry;

#define SH_COMPACT_MAX_PASSES	10

/* ----------------------------------------------------------------
 *  Log table + trigger management
 * ---------------------------------------------------------------- */

/*
 * Create the log table and AFTER trigger.  Commits the DDL so concurrent
 * backends can see the trigger immediately.
 *
 * Returns the log table's schema-qualified name (palloc'd).
 */
static char *
create_log_infrastructure(Oid relid, const char *relname,
						  AttrNumber pk_attnum)
{
	StringInfoData sql;
	char	   *log_table_name;
	const char *quoted_log_table;
	const char *quoted_relname;
	char	   *schema_name;
	const char *quoted_schema;
	int			ret;

	/* Generate unique log table name — safe, derived from Oid */
	log_table_name = psprintf("_sh_compact_log_%u", relid);
	quoted_log_table = quote_identifier(log_table_name);

	/* Schema-qualify the target table for the trigger */
	schema_name = get_namespace_name(get_rel_namespace(relid));
	quoted_schema = quote_identifier(schema_name);
	quoted_relname = quote_identifier(get_rel_name(relid));

	initStringInfo(&sql);

	/* Create unlogged log table in the target relation's schema */
	appendStringInfo(&sql,
					 "CREATE UNLOGGED TABLE %s.%s ("
					 "  id bigserial,"
					 "  action char(1) NOT NULL,"
					 "  pk_val int8 NOT NULL"
					 ")",
					 quoted_schema, quoted_log_table);
	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		elog(ERROR, "sorted_heap: CREATE TABLE for log failed: %d", ret);
	resetStringInfo(&sql);

	/* Index on id for efficient ordered reads */
	appendStringInfo(&sql,
					 "CREATE INDEX ON %s.%s (id)",
					 quoted_schema, quoted_log_table);
	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		elog(ERROR, "sorted_heap: CREATE INDEX on log table failed: %d", ret);
	resetStringInfo(&sql);

	/* Install AFTER trigger to capture concurrent DML */
	appendStringInfo(&sql,
					 "CREATE TRIGGER _sh_compact_trigger "
					 "AFTER INSERT OR UPDATE OR DELETE ON %s.%s "
					 "FOR EACH ROW EXECUTE FUNCTION "
					 "sorted_heap_compact_trigger('%s', '%d', '%s')",
					 quoted_schema,
					 quoted_relname,
					 log_table_name,	/* trigger arg 0: table name */
					 pk_attnum,			/* trigger arg 1: PK attnum */
					 schema_name);		/* trigger arg 2: schema name */
	ret = SPI_execute(sql.data, false, 0);
	if (ret != SPI_OK_UTILITY)
		elog(ERROR, "sorted_heap: CREATE TRIGGER failed: %d", ret);
	pfree(sql.data);
	pfree(schema_name);

	/* Commit so trigger is visible to other backends */
	SPI_commit();
	SPI_start_transaction();

	return log_table_name;
}

/*
 * Drop the log table and trigger.  Best-effort cleanup.
 */
static void
drop_log_infrastructure(Oid relid, const char *log_table_name)
{
	StringInfoData sql;
	char	   *schema_name;

	schema_name = get_namespace_name(get_rel_namespace(relid));

	initStringInfo(&sql);

	/* Drop trigger first */
	appendStringInfo(&sql,
					 "DROP TRIGGER IF EXISTS _sh_compact_trigger ON %s.%s",
					 quote_identifier(schema_name),
					 quote_identifier(get_rel_name(relid)));
	SPI_execute(sql.data, false, 0);
	resetStringInfo(&sql);

	/* Drop log table (schema-qualified) */
	appendStringInfo(&sql,
					 "DROP TABLE IF EXISTS %s.%s",
					 quote_identifier(schema_name),
					 quote_identifier(log_table_name));
	SPI_execute(sql.data, false, 0);

	pfree(sql.data);
	pfree(schema_name);
}

/* ----------------------------------------------------------------
 *  Trigger function: capture DML into log table
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(sorted_heap_compact_trigger);

Datum
sorted_heap_compact_trigger(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	char	   *log_table_name;
	AttrNumber	pk_attnum;
	HeapTuple	tuple;
	Datum		pk_datum;
	bool		isnull;
	int64		pk_val;
	Oid			pk_typid;
	StringInfoData sql;
	Oid			argtypes[2];
	Datum		values[2];
	char		nulls[2];

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "sorted_heap_compact_trigger: not called by trigger manager");

	if (!TRIGGER_FIRED_AFTER(trigdata->tg_event))
		elog(ERROR, "sorted_heap_compact_trigger: must be AFTER trigger");

	if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		elog(ERROR, "sorted_heap_compact_trigger: must be FOR EACH ROW");

	/* Parse trigger arguments */
	if (trigdata->tg_trigger->tgnargs != 3)
		elog(ERROR, "sorted_heap_compact_trigger: expected 3 args (log_table, pk_attnum, schema)");

	log_table_name = trigdata->tg_trigger->tgargs[0];
	pk_attnum = atoi(trigdata->tg_trigger->tgargs[1]);

	/* Get PK type from tuple descriptor */
	pk_typid = TupleDescAttr(trigdata->tg_relation->rd_att,
							 pk_attnum - 1)->atttypid;

	/* Insert into log table via SPI (schema-qualified) */
	{
		char   *log_schema_name = trigdata->tg_trigger->tgargs[2];

		SPI_connect();

		initStringInfo(&sql);
		appendStringInfo(&sql,
						 "INSERT INTO %s.%s (action, pk_val) VALUES ($1, $2)",
						 quote_identifier(log_schema_name),
						 quote_identifier(log_table_name));
	}

	argtypes[0] = CHAROID;
	argtypes[1] = INT8OID;
	nulls[0] = ' ';
	nulls[1] = ' ';

	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
	{
		/*
		 * UPDATE: extract both old and new PK values.  If the PK changed,
		 * log D(old_pk) + I(new_pk) so replay removes the old row from
		 * new_rel and inserts the new one.  If PK is unchanged, log a
		 * single U(pk) as before.
		 */
		int64	old_pk_val, new_pk_val;

		pk_datum = heap_getattr(trigdata->tg_trigtuple, pk_attnum,
								trigdata->tg_relation->rd_att, &isnull);
		if (isnull)
			elog(ERROR, "sorted_heap_compact_trigger: old PK column is NULL");
		if (!sorted_heap_key_to_int64(pk_datum, pk_typid, &old_pk_val))
			elog(ERROR, "sorted_heap_compact_trigger: unsupported PK type %u",
				 pk_typid);

		pk_datum = heap_getattr(trigdata->tg_newtuple, pk_attnum,
								trigdata->tg_relation->rd_att, &isnull);
		if (isnull)
			elog(ERROR, "sorted_heap_compact_trigger: new PK column is NULL");
		if (!sorted_heap_key_to_int64(pk_datum, pk_typid, &new_pk_val))
			elog(ERROR, "sorted_heap_compact_trigger: unsupported PK type %u",
				 pk_typid);

		if (old_pk_val != new_pk_val)
		{
			/* PK changed: D(old) + I(new) */
			values[0] = CharGetDatum('D');
			values[1] = Int64GetDatum(old_pk_val);
			SPI_execute_with_args(sql.data, 2, argtypes, values, nulls,
								 false, 0);

			values[0] = CharGetDatum('I');
			values[1] = Int64GetDatum(new_pk_val);
			SPI_execute_with_args(sql.data, 2, argtypes, values, nulls,
								 false, 0);
		}
		else
		{
			/* PK unchanged: single U */
			values[0] = CharGetDatum('U');
			values[1] = Int64GetDatum(new_pk_val);
			SPI_execute_with_args(sql.data, 2, argtypes, values, nulls,
								 false, 0);
		}
	}
	else if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
	{
		tuple = trigdata->tg_trigtuple;
		pk_datum = heap_getattr(tuple, pk_attnum,
								trigdata->tg_relation->rd_att, &isnull);
		if (isnull)
			elog(ERROR, "sorted_heap_compact_trigger: PK column is NULL");
		if (!sorted_heap_key_to_int64(pk_datum, pk_typid, &pk_val))
			elog(ERROR, "sorted_heap_compact_trigger: unsupported PK type %u",
				 pk_typid);

		values[0] = CharGetDatum('I');
		values[1] = Int64GetDatum(pk_val);
		SPI_execute_with_args(sql.data, 2, argtypes, values, nulls,
							 false, 0);
	}
	else
	{
		/* DELETE */
		tuple = trigdata->tg_trigtuple;
		pk_datum = heap_getattr(tuple, pk_attnum,
								trigdata->tg_relation->rd_att, &isnull);
		if (isnull)
			elog(ERROR, "sorted_heap_compact_trigger: PK column is NULL");
		if (!sorted_heap_key_to_int64(pk_datum, pk_typid, &pk_val))
			elog(ERROR, "sorted_heap_compact_trigger: unsupported PK type %u",
				 pk_typid);

		values[0] = CharGetDatum('D');
		values[1] = Int64GetDatum(pk_val);
		SPI_execute_with_args(sql.data, 2, argtypes, values, nulls,
							 false, 0);
	}

	pfree(sql.data);

	SPI_finish();

	return PointerGetDatum(NULL);
}

/* ----------------------------------------------------------------
 *  Copy phase: index scan in PK order → new table
 * ---------------------------------------------------------------- */
static double
sorted_heap_copy_sorted(Relation old_rel, Relation new_rel,
						Relation pk_index, Snapshot snapshot,
						HTAB *pk_tid_map, AttrNumber pk_attnum,
						Oid pk_typid)
{
	IndexScanDesc iscan;
	TupleTableSlot *slot;
	double		ntuples = 0;
	const TableAmRoutine *heap = GetHeapamTableAmRoutine();

#if PG_VERSION_NUM < 180000
	iscan = index_beginscan(old_rel, pk_index, snapshot, 0, 0);
#else
	iscan = index_beginscan(old_rel, pk_index, snapshot, NULL, 0, 0);
#endif
	index_rescan(iscan, NULL, 0, NULL, 0);

	slot = table_slot_create(old_rel, NULL);

	while (index_getnext_slot(iscan, ForwardScanDirection, slot))
	{
		Datum		val;
		bool		isnull;
		int64		key;

		/* Insert into new table via heap AM (skip zone map updates) */
		heap->tuple_insert(new_rel, slot, GetCurrentCommandId(true),
						   0, NULL);

		/* Track PK → TID mapping for replay */
		slot_getallattrs(slot);
		val = slot_getattr(slot, pk_attnum, &isnull);
		if (!isnull && sorted_heap_key_to_int64(val, pk_typid, &key))
		{
			PKTidEntry *entry;
			bool		found;

			entry = hash_search(pk_tid_map, &key, HASH_ENTER, &found);
			entry->pk_val = key;
			ItemPointerCopy(&slot->tts_tid, &entry->tid);
		}

		ntuples++;

		/* Allow interrupts for long copies */
		CHECK_FOR_INTERRUPTS();
	}

	ExecDropSingleTupleTableSlot(slot);
	index_endscan(iscan);

	return ntuples;
}

/* ----------------------------------------------------------------
 *  Replay phase: apply log entries to new table
 * ---------------------------------------------------------------- */
static int64
sorted_heap_replay_log(Relation old_rel, Relation new_rel,
					   const char *log_table_name,
					   const char *log_schema_name,
					   int64 *last_processed_id,
					   HTAB *pk_tid_map,
					   AttrNumber pk_attnum, Oid pk_typid,
					   Oid pk_index_oid)
{
	StringInfoData sql;
	int64		processed = 0;
	int			ret;
	uint64		i;
	const TableAmRoutine *heap = GetHeapamTableAmRoutine();

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT id, action, pk_val FROM %s.%s "
					 "WHERE id > %lld ORDER BY id",
					 quote_identifier(log_schema_name),
					 quote_identifier(log_table_name),
					 (long long) *last_processed_id);

	ret = SPI_execute(sql.data, true, 0);
	pfree(sql.data);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "sorted_heap_replay_log: SPI_execute failed: %d", ret);

	for (i = 0; i < SPI_processed; i++)
	{
		HeapTuple	spi_tuple = SPI_tuptable->vals[i];
		TupleDesc	spi_desc = SPI_tuptable->tupdesc;
		bool		isnull;
		int64		log_id;
		char		action;
		int64		pk_val;

		log_id = DatumGetInt64(SPI_getbinval(spi_tuple, spi_desc, 1, &isnull));
		action = DatumGetChar(SPI_getbinval(spi_tuple, spi_desc, 2, &isnull));
		pk_val = DatumGetInt64(SPI_getbinval(spi_tuple, spi_desc, 3, &isnull));

		/* For DELETE or UPDATE: remove old version from new table */
		if (action == 'D' || action == 'U')
		{
			PKTidEntry *entry;

			entry = hash_search(pk_tid_map, &pk_val, HASH_FIND, NULL);
			if (entry != NULL)
			{
				simple_heap_delete(new_rel, &entry->tid);
				hash_search(pk_tid_map, &pk_val, HASH_REMOVE, NULL);
			}
		}

		/* For INSERT or UPDATE: copy current version from old table */
		if (action == 'I' || action == 'U')
		{
			Relation	pk_index;
			IndexScanDesc iscan;
			ScanKeyData skey[1];
			TupleTableSlot *slot;
			Datum		pk_datum;

			/* Build scan key for the PK value */
			pk_datum = Int64GetDatum(pk_val);

			/* Convert int64 back to native PK type for index scan */
			switch (pk_typid)
			{
				case INT2OID:
					pk_datum = Int16GetDatum((int16) pk_val);
					break;
				case INT4OID:
					pk_datum = Int32GetDatum((int32) pk_val);
					break;
				case INT8OID:
					pk_datum = Int64GetDatum(pk_val);
					break;
				case TIMESTAMPOID:
				case TIMESTAMPTZOID:
					pk_datum = Int64GetDatum(pk_val);
					break;
				case DATEOID:
					pk_datum = Int32GetDatum((int32) pk_val);
					break;
				default:
					elog(ERROR, "sorted_heap_replay_log: unsupported PK type %u",
						 pk_typid);
			}

			{
				Oid		opclass = GetDefaultOpClass(pk_typid, BTREE_AM_OID);
				Oid		opfamily = get_opclass_family(opclass);
				Oid		eq_opr = get_opfamily_member(opfamily, pk_typid,
													 pk_typid,
													 BTEqualStrategyNumber);
				RegProcedure eq_proc = get_opcode(eq_opr);

				ScanKeyInit(&skey[0],
							1,		/* first index column */
							BTEqualStrategyNumber,
							eq_proc,
							pk_datum);
			}

			pk_index = index_open(pk_index_oid, AccessShareLock);
#if PG_VERSION_NUM < 180000
			iscan = index_beginscan(old_rel, pk_index,
									GetActiveSnapshot(), 1, 0);
#else
			iscan = index_beginscan(old_rel, pk_index,
									GetActiveSnapshot(), NULL, 1, 0);
#endif
			index_rescan(iscan, skey, 1, NULL, 0);

			slot = table_slot_create(old_rel, NULL);

			if (index_getnext_slot(iscan, ForwardScanDirection, slot))
			{
				PKTidEntry *entry;
				bool		found;

				/* Insert into new table */
				heap->tuple_insert(new_rel, slot,
								   GetCurrentCommandId(true), 0, NULL);

				/* Update PK→TID map */
				entry = hash_search(pk_tid_map, &pk_val, HASH_ENTER, &found);
				entry->pk_val = pk_val;
				ItemPointerCopy(&slot->tts_tid, &entry->tid);
			}
			/* If row not found in old_rel, it was deleted — skip */

			ExecDropSingleTupleTableSlot(slot);
			index_endscan(iscan);
			index_close(pk_index, AccessShareLock);
		}

		*last_processed_id = log_id;
		processed++;

		CHECK_FOR_INTERRUPTS();
	}

	return processed;
}

/* ----------------------------------------------------------------
 *  Main entry point: sorted_heap_compact_online(regclass)
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(sorted_heap_compact_online);

Datum
sorted_heap_compact_online(PG_FUNCTION_ARGS)
{
	Oid				relid = PG_GETARG_OID(0);
	Relation		rel;
	SortedHeapRelInfo *info;
	AttrNumber		pk_attnum;
	Oid				pk_typid;
	Oid				pk_index_oid;
	Oid				table_am_oid;
	char		   *log_table_name = NULL;
	char		   *log_schema_name = NULL;
	Oid				new_relid = InvalidOid;
	HASHCTL			hashctl;
	HTAB		   *pk_tid_map;
	int64			last_id = 0;
	double			ntuples;
	int				pass;

	/* Verify ownership */
	if (!object_ownercheck(RelationRelationId, relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE, get_rel_name(relid));

	/* Phase 1: Validate and collect PK info */
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
	if (!OidIsValid(info->pk_index_oid))
	{
		table_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("\"%s\" has no primary key",
						RelationGetRelationName(rel))));
	}

	pk_attnum = info->attNums[0];
	pk_typid = info->zm_pk_typid;
	pk_index_oid = info->pk_index_oid;
	table_am_oid = rel->rd_rel->relam;
	table_close(rel, AccessShareLock);

	/* Block online compact for lossy PK types (UUID, text) —
	 * the int8 log table and PK→TID hash cause collisions */
	if (pk_typid == UUIDOID || pk_typid == TEXTOID || pk_typid == VARCHAROID)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("online compact is not supported for %s primary keys",
						format_type_be(pk_typid)),
				 errhint("Use sorted_heap_compact() instead.")));

	/* Resolve schema for log table placement */
	log_schema_name = get_namespace_name(get_rel_namespace(relid));

	ereport(NOTICE,
			(errmsg("online compact: starting for \"%s\"",
					get_rel_name(relid)),
			 errhint("Concurrent reads and writes are allowed. "
					 "Brief exclusive lock at the end for swap.")));

	/* Use non-atomic SPI so we can commit DDL mid-function */
	SPI_connect_ext(SPI_OPT_NONATOMIC);

	PG_TRY();
	{
		Relation	new_rel;
		Relation	pk_index;
		Snapshot	snapshot;

		/* Phase 1b: Create log infrastructure (commits to make visible) */
		log_table_name = create_log_infrastructure(relid,
												   get_rel_name(relid),
												   pk_attnum);

		/* Phase 1c: Create new heap (same schema as old) */
		new_relid = make_new_heap(relid, InvalidOid, table_am_oid,
								  RELPERSISTENCE_PERMANENT,
								  AccessShareLock);

		/* Initialize PK → TID hash table */
		memset(&hashctl, 0, sizeof(hashctl));
		hashctl.keysize = sizeof(int64);
		hashctl.entrysize = sizeof(PKTidEntry);
		hashctl.hcxt = CurrentMemoryContext;
		pk_tid_map = hash_create("pk_tid_map", 1024, &hashctl,
								 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

		/* Phase 2: Copy data (ShareUpdateExclusiveLock allows concurrent DML) */
		rel = table_open(relid, ShareUpdateExclusiveLock);
		new_rel = table_open(new_relid, AccessExclusiveLock);
		pk_index = index_open(pk_index_oid, AccessShareLock);
		snapshot = GetTransactionSnapshot();

		ntuples = sorted_heap_copy_sorted(rel, new_rel, pk_index, snapshot,
										  pk_tid_map, pk_attnum, pk_typid);

		ereport(NOTICE,
				(errmsg("online compact: copied %.0f tuples", ntuples)));

		index_close(pk_index, AccessShareLock);
		table_close(new_rel, NoLock);

		/* Phase 2b: Replay loop until convergence */
		for (pass = 0; pass < SH_COMPACT_MAX_PASSES; pass++)
		{
			int64	replayed;

			new_rel = table_open(new_relid, RowExclusiveLock);
			replayed = sorted_heap_replay_log(rel, new_rel, log_table_name,
											  log_schema_name,
											  &last_id, pk_tid_map,
											  pk_attnum, pk_typid,
											  pk_index_oid);
			table_close(new_rel, NoLock);

			if (replayed == 0)
				break;

			ereport(NOTICE,
					(errmsg("online compact: pass %d replayed %lld changes",
							pass + 1, (long long) replayed)));
		}

		/* Release ShareUpdateExclusiveLock before upgrading */
		table_close(rel, ShareUpdateExclusiveLock);

		/* Phase 3: Final swap under AccessExclusiveLock */
		rel = table_open(relid, AccessExclusiveLock);
		new_rel = table_open(new_relid, AccessExclusiveLock);

		/* Final replay: process any last changes */
		sorted_heap_replay_log(rel, new_rel, log_table_name,
							   log_schema_name,
							   &last_id, pk_tid_map,
							   pk_attnum, pk_typid, pk_index_oid);

		/* Rebuild zone map on new table */
		if (info->zm_usable)
			sorted_heap_rebuild_zonemap_internal(new_rel, pk_typid, pk_attnum,
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

		/* Cleanup: drop trigger and log table */
		drop_log_infrastructure(relid, log_table_name);

		hash_destroy(pk_tid_map);

		ereport(NOTICE,
				(errmsg("online compact: completed for \"%s\" (%.0f tuples)",
						get_rel_name(relid), ntuples)));
	}
	PG_CATCH();
	{
		/* Best-effort cleanup */
		if (log_table_name != NULL)
		{
			PG_TRY();
			{
				drop_log_infrastructure(relid, log_table_name);
			}
			PG_CATCH();
			{
				/* Ignore cleanup errors */
			}
			PG_END_TRY();
		}
		SPI_finish();
		PG_RE_THROW();
	}
	PG_END_TRY();

	SPI_finish();
	PG_RETURN_VOID();
}

/* ----------------------------------------------------------------
 *  Online merge: copy phase helper
 *
 *  Prefix sequential scan + tail tuplesort, two-way merge into
 *  new_rel.  Populates pk_tid_map for replay phase.
 * ---------------------------------------------------------------- */
static double
sorted_heap_copy_merged(Relation old_rel, Relation new_rel,
						Snapshot snapshot, HTAB *pk_tid_map,
						SortedHeapRelInfo *info,
						BlockNumber prefix_pages,
						BlockNumber tail_nblocks)
{
	const TableAmRoutine *heap = GetHeapamTableAmRoutine();
	int				nkeys = info->nkeys;
	AttrNumber		pk_attnum = info->attNums[0];
	Oid				pk_typid = info->zm_pk_typid;
	double			ntuples = 0;
	SortSupportData *sortkeys;
	TupleTableSlot *prefix_slot;
	TupleTableSlot *tail_slot;
	TableScanDesc	prefix_scan = NULL;
	Tuplesortstate *tupstate;
	bool			prefix_valid = false;
	bool			tail_valid;
	int				k;

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
	prefix_slot = MakeSingleTupleTableSlot(RelationGetDescr(old_rel),
										   &TTSOpsBufferHeapTuple);
	tail_slot = MakeSingleTupleTableSlot(RelationGetDescr(old_rel),
										 &TTSOpsMinimalTuple);

	/* Stream A: sequential scan of sorted prefix */
	if (prefix_pages > 0)
	{
		prefix_scan = table_beginscan(old_rel, snapshot, 0, NULL);
		heap_setscanlimits(prefix_scan, 1, prefix_pages);
		prefix_valid = table_scan_getnextslot(prefix_scan,
											  ForwardScanDirection,
											  prefix_slot);
	}

	/* Stream B: tuplesort of unsorted tail */
	{
		TupleTableSlot *scan_slot;
		TableScanDesc	tail_scan;
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

		tupstate = tuplesort_begin_heap(RelationGetDescr(old_rel),
										nkeys, attNums, sortOps,
										sortColls, nullsFirst,
										maintenance_work_mem,
										NULL, TUPLESORT_NONE);

		tail_scan = table_beginscan(old_rel, snapshot, 0, NULL);
		heap_setscanlimits(tail_scan, 1 + prefix_pages, tail_nblocks);

		scan_slot = MakeSingleTupleTableSlot(RelationGetDescr(old_rel),
											 &TTSOpsBufferHeapTuple);

		while (table_scan_getnextslot(tail_scan, ForwardScanDirection,
									  scan_slot))
		{
			tuplesort_puttupleslot(tupstate, scan_slot);
		}

		ExecDropSingleTupleTableSlot(scan_slot);
		table_endscan(tail_scan);
		tuplesort_performsort(tupstate);

		pfree(attNums);
		pfree(sortOps);
		pfree(sortColls);
		pfree(nullsFirst);
	}

	/* Get first sorted tail tuple */
	tail_valid = tuplesort_gettupleslot(tupstate, true, true,
										tail_slot, NULL);

	/* Two-way merge with PK→TID tracking */
	while (prefix_valid || tail_valid)
	{
		TupleTableSlot *winner;
		bool			use_prefix;

		CHECK_FOR_INTERRUPTS();

		if (!prefix_valid)
			use_prefix = false;
		else if (!tail_valid)
			use_prefix = true;
		else
		{
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

		winner = use_prefix ? prefix_slot : tail_slot;
		heap->tuple_insert(new_rel, winner,
						   GetCurrentCommandId(true), 0, NULL);

		/* Track PK → TID for replay */
		{
			Datum	val;
			bool	isnull;
			int64	key;

			slot_getallattrs(winner);
			val = slot_getattr(winner, pk_attnum, &isnull);
			if (!isnull && sorted_heap_key_to_int64(val, pk_typid, &key))
			{
				PKTidEntry *entry;
				bool		found;

				entry = hash_search(pk_tid_map, &key, HASH_ENTER, &found);
				entry->pk_val = key;
				ItemPointerCopy(&winner->tts_tid, &entry->tid);
			}
		}
		ntuples++;

		/* Advance winner stream */
		if (use_prefix)
			prefix_valid = table_scan_getnextslot(prefix_scan,
												  ForwardScanDirection,
												  prefix_slot);
		else
			tail_valid = tuplesort_gettupleslot(tupstate, true, true,
												tail_slot, NULL);
	}

	/* Cleanup */
	if (prefix_scan)
		table_endscan(prefix_scan);
	tuplesort_end(tupstate);
	ExecDropSingleTupleTableSlot(prefix_slot);
	ExecDropSingleTupleTableSlot(tail_slot);
	pfree(sortkeys);

	return ntuples;
}

/* ----------------------------------------------------------------
 *  sorted_heap_merge_online(regclass) → void
 *
 *  Non-blocking incremental merge compaction.  Uses trigger-based
 *  change capture (same as compact_online) with the merge strategy
 *  (prefix seq scan + tail tuplesort) from sorted_heap_merge.
 *
 *  ShareUpdateExclusiveLock during copy allows concurrent DML.
 *  AccessExclusiveLock only for brief final swap.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(sorted_heap_merge_online);

Datum
sorted_heap_merge_online(PG_FUNCTION_ARGS)
{
	Oid				relid = PG_GETARG_OID(0);
	Relation		rel;
	SortedHeapRelInfo *info;
	AttrNumber		pk_attnum;
	Oid				pk_typid;
	Oid				pk_index_oid;
	Oid				table_am_oid;
	BlockNumber		total_blocks;
	BlockNumber		total_data_pages;
	BlockNumber		prefix_pages;
	BlockNumber		tail_nblocks;
	char		   *log_table_name = NULL;
	char		   *log_schema_name = NULL;
	Oid				new_relid = InvalidOid;
	HASHCTL			hashctl;
	HTAB		   *pk_tid_map;
	int64			last_id = 0;
	double			ntuples;
	int				pass;

	/* Verify ownership */
	if (!object_ownercheck(RelationRelationId, relid, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE, get_rel_name(relid));

	/* Phase 0: Validate */
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

	if (!OidIsValid(pk_index_oid))
	{
		table_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("\"%s\" has no primary key",
						RelationGetRelationName(rel))));
	}

	info = sorted_heap_get_relinfo(rel);
	pk_attnum = info->attNums[0];
	pk_typid = info->zm_pk_typid;
	table_am_oid = rel->rd_rel->relam;
	table_close(rel, AccessShareLock);

	/* Block online merge for lossy PK types (UUID, text) —
	 * the int8 log table and PK→TID hash cause collisions */
	if (pk_typid == UUIDOID || pk_typid == TEXTOID || pk_typid == VARCHAROID)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("online merge is not supported for %s primary keys",
						format_type_be(pk_typid)),
				 errhint("Use sorted_heap_merge() instead.")));

	/* Phase 0b: Detect prefix (early exit before SPI setup) */
	rel = table_open(relid, ShareUpdateExclusiveLock);
	info = sorted_heap_get_relinfo(rel);
	info->zm_loaded = false;
	sorted_heap_zonemap_load(rel, info);

	total_blocks = RelationGetNumberOfBlocks(rel);

	if (total_blocks <= 1)
	{
		ereport(NOTICE,
				(errmsg("online merge: table is empty")));
		table_close(rel, ShareUpdateExclusiveLock);
		PG_RETURN_VOID();
	}

	total_data_pages = total_blocks - 1;
	prefix_pages = sorted_heap_detect_sorted_prefix(info);

	if (prefix_pages >= total_data_pages)
	{
		ereport(NOTICE,
				(errmsg("online merge: table is already sorted (%u pages)",
						(unsigned) total_data_pages)));
		table_close(rel, ShareUpdateExclusiveLock);
		PG_RETURN_VOID();
	}

	table_close(rel, ShareUpdateExclusiveLock);

	/* Resolve schema for log table placement */
	log_schema_name = get_namespace_name(get_rel_namespace(relid));

	ereport(NOTICE,
			(errmsg("online merge: starting for \"%s\"",
					get_rel_name(relid)),
			 errhint("Concurrent reads and writes are allowed. "
					 "Brief exclusive lock at the end for swap.")));

	/* Phase 1: SPI + log infrastructure */
	SPI_connect_ext(SPI_OPT_NONATOMIC);

	PG_TRY();
	{
		Relation	new_rel;
		Snapshot	snapshot;

		/* Phase 1b: Create log infrastructure (commits to make visible) */
		log_table_name = create_log_infrastructure(relid,
												   get_rel_name(relid),
												   pk_attnum);

		/* Phase 1c: Create new heap */
		new_relid = make_new_heap(relid, InvalidOid, table_am_oid,
								  RELPERSISTENCE_PERMANENT,
								  AccessShareLock);

		/* Initialize PK → TID hash table */
		memset(&hashctl, 0, sizeof(hashctl));
		hashctl.keysize = sizeof(int64);
		hashctl.entrysize = sizeof(PKTidEntry);
		hashctl.hcxt = CurrentMemoryContext;
		pk_tid_map = hash_create("pk_tid_map", 1024, &hashctl,
								 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

		/* Phase 2: Merge copy under ShareUpdateExclusiveLock */
		rel = table_open(relid, ShareUpdateExclusiveLock);
		new_rel = table_open(new_relid, AccessExclusiveLock);

		/* Re-detect prefix under lock (handles TOCTOU race) */
		info = sorted_heap_get_relinfo(rel);
		info->zm_loaded = false;
		sorted_heap_zonemap_load(rel, info);

		total_blocks = RelationGetNumberOfBlocks(rel);
		total_data_pages = total_blocks - 1;
		prefix_pages = sorted_heap_detect_sorted_prefix(info);
		tail_nblocks = total_data_pages - prefix_pages;

		snapshot = GetTransactionSnapshot();

		ntuples = sorted_heap_copy_merged(rel, new_rel, snapshot,
										  pk_tid_map, info,
										  prefix_pages, tail_nblocks);

		ereport(NOTICE,
				(errmsg("online merge: copied %.0f tuples "
						"(%u prefix + %u tail pages)",
						ntuples, (unsigned) prefix_pages,
						(unsigned) tail_nblocks)));

		table_close(new_rel, NoLock);

		/* Phase 2b: Replay loop until convergence */
		for (pass = 0; pass < SH_COMPACT_MAX_PASSES; pass++)
		{
			int64	replayed;

			new_rel = table_open(new_relid, RowExclusiveLock);
			replayed = sorted_heap_replay_log(rel, new_rel, log_table_name,
											  log_schema_name,
											  &last_id, pk_tid_map,
											  pk_attnum, pk_typid,
											  pk_index_oid);
			table_close(new_rel, NoLock);

			if (replayed == 0)
				break;

			ereport(NOTICE,
					(errmsg("online merge: pass %d replayed %lld changes",
							pass + 1, (long long) replayed)));
		}

		/* Phase 2c: Release ShareUpdateExclusiveLock before upgrading */
		table_close(rel, ShareUpdateExclusiveLock);

		/* Phase 3: Final swap under AccessExclusiveLock */
		rel = table_open(relid, AccessExclusiveLock);
		new_rel = table_open(new_relid, AccessExclusiveLock);

		/* Final replay: process any last changes */
		sorted_heap_replay_log(rel, new_rel, log_table_name,
							   log_schema_name,
							   &last_id, pk_tid_map,
							   pk_attnum, pk_typid, pk_index_oid);

		/* Rebuild zone map on new table */
		if (info->zm_usable)
			sorted_heap_rebuild_zonemap_internal(new_rel, pk_typid, pk_attnum,
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

		/* Cleanup: drop trigger and log table */
		drop_log_infrastructure(relid, log_table_name);

		hash_destroy(pk_tid_map);

		ereport(NOTICE,
				(errmsg("online merge: completed for \"%s\" (%.0f tuples)",
						get_rel_name(relid), ntuples)));
	}
	PG_CATCH();
	{
		/* Best-effort cleanup */
		if (log_table_name != NULL)
		{
			PG_TRY();
			{
				drop_log_infrastructure(relid, log_table_name);
			}
			PG_CATCH();
			{
				/* Ignore cleanup errors */
			}
			PG_END_TRY();
		}
		SPI_finish();
		PG_RE_THROW();
	}
	PG_END_TRY();

	SPI_finish();
	PG_RETURN_VOID();
}
