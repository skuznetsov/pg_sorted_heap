/* ----------------------------------------------------------------
 *  Narrow GraphRAG primitive: expand known entity IDs on sorted_heap.
 *
 *  Requires a relation with the columns:
 *    entity_id   int4
 *    relation_id int2
 *    target_id   int4
 *    embedding   svec
 *    payload     text
 *
 *  The helper deliberately keeps the surface area small: it reuses the
 *  zone-map range builder directly and emits fact rows for known seed IDs,
 *  avoiding planner/custom-scan setup for the narrow "expand these IDs"
 *  use case.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(sorted_heap_expand_ids);

Datum
sorted_heap_expand_ids(PG_FUNCTION_ARGS)
{
	ReturnSetInfo	   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Oid					rel_oid;
	ArrayType		   *seed_arr;
	bool				has_relation_filter;
	int32				relation_filter = 0;
	int32				limit_rows;
	Relation			rel;
	SortedHeapRelInfo  *info;
	SortedHeapGraphAttrs gattrs;
	AttrNumber			entity_att;
	AttrNumber			relation_att;
	AttrNumber			target_att;
	AttrNumber			embedding_att;
	AttrNumber			payload_att;
	const char		   *relname;
	Datum			   *seed_datums = NULL;
	bool			   *seed_nulls = NULL;
	int					nseed = 0;
	int64			   *seed_values = NULL;
	int					nseed_values = 0;
	SortedHeapScanBounds bounds;
	BlockNumber			total_blocks;
	SortedHeapScanRange *ranges = NULL;
	int					nranges = 0;
	BlockNumber			range_total = 0;
	TableScanDesc		scan;
	TupleTableSlot	   *slot;
	Snapshot			snapshot;
	int					range_idx;
	int32				n_out = 0;
	bool				obs_top_level;
	instr_time			t_expand_start;
	instr_time			t_expand_end;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("sorted_heap_expand_ids must be called in a set-returning context")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
	{
		InitMaterializedSRF(fcinfo, 0);
		PG_RETURN_NULL();
	}

	rel_oid = PG_GETARG_OID(0);
	seed_arr = PG_GETARG_ARRAYTYPE_P(1);
	has_relation_filter = !PG_ARGISNULL(2);
	if (has_relation_filter)
		relation_filter = PG_GETARG_INT32(2);
	limit_rows = PG_ARGISNULL(3) ? 0 : PG_GETARG_INT32(3);
	if (limit_rows < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_expand_ids: limit_rows must be >= 0")));
	if (has_relation_filter &&
		(relation_filter < PG_INT16_MIN || relation_filter > PG_INT16_MAX))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_heap_expand_ids: relation_filter %d is outside int2 range",
						relation_filter)));

	InitMaterializedSRF(fcinfo, 0);

	if (ARR_NDIM(seed_arr) == 0 || ArrayGetNItems(ARR_NDIM(seed_arr), ARR_DIMS(seed_arr)) == 0)
		PG_RETURN_NULL();

	if (ARR_NDIM(seed_arr) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("sorted_heap_expand_ids: seed_ids must be a one-dimensional int4[]")));
	if (ARR_ELEMTYPE(seed_arr) != INT4OID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("sorted_heap_expand_ids: seed_ids must be int4[]")));

	rel = table_open(rel_oid, AccessShareLock);
	relname = pstrdup(RelationGetRelationName(rel));
	if (rel->rd_tableam != &sorted_heap_am_routine)
	{
		table_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("sorted_heap_expand_ids: relation \"%s\" is not a sorted_heap table",
						relname)));
	}

	sorted_heap_graph_resolve_attrs(rel, "sorted_heap_expand_ids", &gattrs);
	entity_att = gattrs.entity_att;
	relation_att = gattrs.relation_att;
	target_att = gattrs.target_att;
	embedding_att = gattrs.embedding_att;
	payload_att = gattrs.payload_att;

	info = sorted_heap_get_relinfo(rel);

	deconstruct_array(seed_arr, INT4OID, 4, true, TYPALIGN_INT,
					  &seed_datums, &seed_nulls, &nseed);
	if (nseed <= 0)
	{
		table_close(rel, AccessShareLock);
		PG_RETURN_NULL();
	}

	seed_values = palloc(sizeof(int64) * nseed);
	for (int i = 0; i < nseed; i++)
	{
		if (seed_nulls[i])
			continue;
		seed_values[nseed_values++] = (int64) DatumGetInt32(seed_datums[i]);
	}
	if (nseed_values == 0)
	{
		table_close(rel, AccessShareLock);
		pfree(seed_values);
		PG_RETURN_NULL();
	}
	qsort(seed_values, nseed_values, sizeof(int64), sorted_heap_int64_cmp);
	obs_top_level = sorted_heap_graph_obs_begin("sorted_heap_expand_ids");
	if (obs_top_level)
		sorted_heap_graph_obs_add_seed(nseed_values, 0.0);
	INSTR_TIME_SET_CURRENT(t_expand_start);

	memset(&bounds, 0, sizeof(bounds));
	bounds.has_lo = true;
	bounds.has_hi = true;
	bounds.lo_inclusive = true;
	bounds.hi_inclusive = true;
	bounds.lo = seed_values[0];
	bounds.hi = seed_values[nseed_values - 1];
	if (has_relation_filter)
	{
		bounds.has_lo2 = true;
		bounds.has_hi2 = true;
		bounds.lo2_inclusive = true;
		bounds.hi2_inclusive = true;
		bounds.lo2 = relation_filter;
		bounds.hi2 = relation_filter;
	}

	total_blocks = RelationGetNumberOfBlocks(rel);
	if (info->zm_usable && info->zm_loaded && info->zm_total_entries > 0)
		sorted_heap_compute_scan_ranges(info, &bounds,
										seed_values, nseed_values,
										total_blocks,
										&ranges, &nranges, &range_total);

	scan = table_beginscan(rel, GetActiveSnapshot(), 0, NULL);
	slot = table_slot_create(rel, NULL);
	snapshot = GetActiveSnapshot();
	(void) snapshot;

	if (nranges == 0)
	{
		/* Fallback for invalid/unloaded zone map: scan whole relation. */
		heap_setscanlimits(scan, 1, total_blocks > 0 ? total_blocks - 1 : 0);
		nranges = 1;
	}

	for (range_idx = 0; range_idx < nranges; range_idx++)
	{
		BlockNumber range_start;
		BlockNumber range_nblocks;
		bool		range_sorted;
		BlockNumber last_blk = InvalidBlockNumber;

		if (ranges)
		{
			range_start = ranges[range_idx].start;
			range_nblocks = ranges[range_idx].nblocks;
			range_sorted = ranges[range_idx].sorted_prefix;
		}
		else
		{
			range_start = 1;
			range_nblocks = total_blocks > 0 ? total_blocks - 1 : 0;
			range_sorted = false;
		}

		table_rescan(scan, NULL);
		heap_setscanlimits(scan, range_start, range_nblocks);

		while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
		{
			BlockNumber	blk = ItemPointerGetBlockNumber(&slot->tts_tid);
			bool		isnull;
			Datum		entity_datum;
			int32		entity_id;
			int16		relation_id;
			int32		target_id;
			bool		zone_checked = false;

			if (blk != last_blk && info->zm_loaded &&
				blk >= 1 && (blk - 1) < info->zm_total_entries)
			{
				SortedHeapZoneMapEntry *e =
					sorted_heap_get_zm_entry(info, blk - 1);

				if (!zone_overlaps_in_values(e, seed_values, nseed_values))
				{
					last_blk = blk;
					continue;
				}
				if (has_relation_filter && !sorted_heap_zone_overlaps(e, &bounds))
				{
					last_blk = blk;
					continue;
				}
				zone_checked = true;
				last_blk = blk;
			}

			entity_datum = slot_getattr(slot, entity_att, &isnull);
			if (isnull)
				continue;
			entity_id = DatumGetInt32(entity_datum);
			if (!sorted_heap_value_in_set((int64) entity_id, seed_values, nseed_values))
				continue;

			relation_id = DatumGetInt16(slot_getattr(slot, relation_att, &isnull));
			if (isnull)
				continue;
			if (has_relation_filter && relation_id != relation_filter)
				continue;

			if (range_sorted && zone_checked && entity_id > bounds.hi)
				break;

			target_id = DatumGetInt32(slot_getattr(slot, target_att, &isnull));
			if (isnull)
				continue;

			{
				Datum values[5];
				bool nulls[5] = {false, false, false, false, false};

				values[0] = Int32GetDatum(entity_id);
				values[1] = Int16GetDatum(relation_id);
				values[2] = Int32GetDatum(target_id);
				values[3] = slot_getattr(slot, embedding_att, &nulls[3]);
				values[4] = slot_getattr(slot, payload_att, &nulls[4]);
				tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
									 values, nulls);
			}

			n_out++;
			if (limit_rows > 0 && n_out >= limit_rows)
				goto done;
		}
	}

done:
	INSTR_TIME_SET_CURRENT(t_expand_end);
	sorted_heap_graph_obs_add_expand(
		n_out,
		INSTR_TIME_GET_MILLISEC(t_expand_end) -
		INSTR_TIME_GET_MILLISEC(t_expand_start));
	sorted_heap_graph_obs_add_rerank(0, n_out, 0.0);
	if (ranges)
		pfree(ranges);
	if (seed_values)
		pfree(seed_values);
	if (slot)
		ExecDropSingleTupleTableSlot(slot);
	if (scan)
		table_endscan(scan);
	table_close(rel, AccessShareLock);
	sorted_heap_graph_route_obs_capture(rel_oid);
	sorted_heap_graph_obs_finish();

	PG_RETURN_NULL();
}

/* ----------------------------------------------------------------
 *  Narrow GraphRAG primitive: expand known entity IDs and rerank top-K
 *  in C, avoiding SQL materialization + sort of the entire expanded set.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(sorted_heap_expand_rerank);
PG_FUNCTION_INFO_V1(sorted_heap_expand_twohop_rerank);
PG_FUNCTION_INFO_V1(sorted_heap_expand_twohop_path_rerank);
PG_FUNCTION_INFO_V1(sorted_heap_expand_multihop_rerank);
PG_FUNCTION_INFO_V1(sorted_heap_expand_multihop_path_rerank);
PG_FUNCTION_INFO_V1(sorted_heap_graph_rag_scan);
PG_FUNCTION_INFO_V1(sorted_heap_graph_rag_twohop_scan);
PG_FUNCTION_INFO_V1(sorted_heap_graph_rag_twohop_path_scan);
PG_FUNCTION_INFO_V1(sorted_heap_graph_rag_multihop_scan);
PG_FUNCTION_INFO_V1(sorted_heap_graph_rag_multihop_path_scan);

Datum
sorted_heap_expand_rerank(PG_FUNCTION_ARGS)
{
	ReturnSetInfo	   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Oid					rel_oid;
	ArrayType		   *seed_arr;
	Svec			   *query;
	int32				top_k;
	bool				has_relation_filter;
	int32				relation_filter = 0;
	int32				limit_rows;
	Relation			rel;
	SortedHeapRelInfo  *info;
	SortedHeapGraphAttrs gattrs;
	AttrNumber			entity_att;
	AttrNumber			relation_att;
	AttrNumber			target_att;
	AttrNumber			embedding_att;
	AttrNumber			payload_att;
	const char		   *relname;
	Datum			   *seed_datums = NULL;
	bool			   *seed_nulls = NULL;
	int					nseed = 0;
	int64			   *seed_values = NULL;
	int					nseed_values = 0;
	bool				obs_top_level;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("sorted_heap_expand_rerank must be called in a set-returning context")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
	{
		InitMaterializedSRF(fcinfo, 0);
		PG_RETURN_NULL();
	}

	rel_oid = PG_GETARG_OID(0);
	seed_arr = PG_GETARG_ARRAYTYPE_P(1);
	query = PG_GETARG_SVEC_P(2);
	top_k = PG_GETARG_INT32(3);
	has_relation_filter = !PG_ARGISNULL(4);
	if (has_relation_filter)
		relation_filter = PG_GETARG_INT32(4);
	limit_rows = PG_ARGISNULL(5) ? 0 : PG_GETARG_INT32(5);

	if (top_k < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_expand_rerank: top_k must be >= 1")));
	if (limit_rows < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_expand_rerank: limit_rows must be >= 0")));
	if (has_relation_filter &&
		(relation_filter < PG_INT16_MIN || relation_filter > PG_INT16_MAX))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_heap_expand_rerank: relation_filter %d is outside int2 range",
						relation_filter)));

	InitMaterializedSRF(fcinfo, 0);

	if (ARR_NDIM(seed_arr) == 0 || ArrayGetNItems(ARR_NDIM(seed_arr), ARR_DIMS(seed_arr)) == 0)
		PG_RETURN_NULL();
	if (ARR_NDIM(seed_arr) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("sorted_heap_expand_rerank: seed_ids must be a one-dimensional int4[]")));
	if (ARR_ELEMTYPE(seed_arr) != INT4OID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("sorted_heap_expand_rerank: seed_ids must be int4[]")));

	rel = table_open(rel_oid, AccessShareLock);
	relname = pstrdup(RelationGetRelationName(rel));
	if (rel->rd_tableam != &sorted_heap_am_routine)
	{
		table_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("sorted_heap_expand_rerank: relation \"%s\" is not a sorted_heap table",
						relname)));
	}

	sorted_heap_graph_resolve_attrs(rel, "sorted_heap_expand_rerank", &gattrs);
	entity_att = gattrs.entity_att;
	relation_att = gattrs.relation_att;
	target_att = gattrs.target_att;
	embedding_att = gattrs.embedding_att;
	payload_att = gattrs.payload_att;

	info = sorted_heap_get_relinfo(rel);

	deconstruct_array(seed_arr, INT4OID, 4, true, TYPALIGN_INT,
					  &seed_datums, &seed_nulls, &nseed);
	if (nseed <= 0)
	{
		table_close(rel, AccessShareLock);
		PG_RETURN_NULL();
	}

	seed_values = palloc(sizeof(int64) * nseed);
	for (int i = 0; i < nseed; i++)
	{
		if (seed_nulls[i])
			continue;
		seed_values[nseed_values++] = (int64) DatumGetInt32(seed_datums[i]);
	}
	if (nseed_values == 0)
	{
		table_close(rel, AccessShareLock);
		pfree(seed_values);
		PG_RETURN_NULL();
	}
	qsort(seed_values, nseed_values, sizeof(int64), sorted_heap_int64_cmp);
	obs_top_level = sorted_heap_graph_obs_begin("sorted_heap_expand_rerank");
	if (obs_top_level)
		sorted_heap_graph_obs_add_seed(nseed_values, 0.0);

	sorted_heap_graph_emit_rerank(rsinfo,
								  rel, info,
								  entity_att, relation_att, target_att,
								  embedding_att, payload_att,
								  seed_values, nseed_values,
								  query, top_k,
								  has_relation_filter, (int16) relation_filter,
								  limit_rows);

	if (seed_values)
		pfree(seed_values);
	table_close(rel, AccessShareLock);
	sorted_heap_graph_route_obs_capture(rel_oid);
	sorted_heap_graph_obs_finish();

	PG_RETURN_NULL();
}

Datum
sorted_heap_expand_twohop_rerank(PG_FUNCTION_ARGS)
{
	ReturnSetInfo	   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Oid					rel_oid;
	ArrayType		   *seed_arr;
	Svec			   *query;
	int32				top_k;
	bool				has_hop1_filter;
	int32				hop1_filter = 0;
	bool				has_hop2_filter;
	int32				hop2_filter = 0;
	int32				limit_rows;
	Relation			rel;
	SortedHeapRelInfo  *info;
	SortedHeapGraphAttrs gattrs;
	AttrNumber			entity_att;
	AttrNumber			relation_att;
	AttrNumber			target_att;
	AttrNumber			embedding_att;
	AttrNumber			payload_att;
	const char		   *relname;
	Datum			   *seed_datums = NULL;
	bool			   *seed_nulls = NULL;
	int					nseed = 0;
	int64			   *seed_values = NULL;
	int					nseed_values = 0;
	int64			   *hop1_values = NULL;
	int					nhop1_values = 0;
	bool				obs_top_level;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("sorted_heap_expand_twohop_rerank must be called in a set-returning context")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
	{
		InitMaterializedSRF(fcinfo, 0);
		PG_RETURN_NULL();
	}

	rel_oid = PG_GETARG_OID(0);
	seed_arr = PG_GETARG_ARRAYTYPE_P(1);
	query = PG_GETARG_SVEC_P(2);
	top_k = PG_GETARG_INT32(3);
	has_hop1_filter = !PG_ARGISNULL(4);
	if (has_hop1_filter)
		hop1_filter = PG_GETARG_INT32(4);
	has_hop2_filter = !PG_ARGISNULL(5);
	if (has_hop2_filter)
		hop2_filter = PG_GETARG_INT32(5);
	limit_rows = PG_ARGISNULL(6) ? 0 : PG_GETARG_INT32(6);

	if (top_k < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_expand_twohop_rerank: top_k must be >= 1")));
	if (limit_rows < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_expand_twohop_rerank: limit_rows must be >= 0")));
	if (has_hop1_filter &&
		(hop1_filter < PG_INT16_MIN || hop1_filter > PG_INT16_MAX))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_heap_expand_twohop_rerank: hop1_relation_filter %d is outside int2 range",
						hop1_filter)));
	if (has_hop2_filter &&
		(hop2_filter < PG_INT16_MIN || hop2_filter > PG_INT16_MAX))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_heap_expand_twohop_rerank: hop2_relation_filter %d is outside int2 range",
						hop2_filter)));

	InitMaterializedSRF(fcinfo, 0);

	if (ARR_NDIM(seed_arr) == 0 || ArrayGetNItems(ARR_NDIM(seed_arr), ARR_DIMS(seed_arr)) == 0)
		PG_RETURN_NULL();
	if (ARR_NDIM(seed_arr) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("sorted_heap_expand_twohop_rerank: seed_ids must be a one-dimensional int4[]")));
	if (ARR_ELEMTYPE(seed_arr) != INT4OID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("sorted_heap_expand_twohop_rerank: seed_ids must be int4[]")));

	rel = table_open(rel_oid, AccessShareLock);
	relname = pstrdup(RelationGetRelationName(rel));
	if (rel->rd_tableam != &sorted_heap_am_routine)
	{
		table_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("sorted_heap_expand_twohop_rerank: relation \"%s\" is not a sorted_heap table",
						relname)));
	}

	sorted_heap_graph_resolve_attrs(rel, "sorted_heap_expand_twohop_rerank", &gattrs);
	entity_att = gattrs.entity_att;
	relation_att = gattrs.relation_att;
	target_att = gattrs.target_att;
	embedding_att = gattrs.embedding_att;
	payload_att = gattrs.payload_att;

	info = sorted_heap_get_relinfo(rel);

	deconstruct_array(seed_arr, INT4OID, 4, true, TYPALIGN_INT,
					  &seed_datums, &seed_nulls, &nseed);
	if (nseed <= 0)
	{
		table_close(rel, AccessShareLock);
		PG_RETURN_NULL();
	}

	seed_values = palloc(sizeof(int64) * nseed);
	for (int i = 0; i < nseed; i++)
	{
		if (seed_nulls[i])
			continue;
		seed_values[nseed_values++] = (int64) DatumGetInt32(seed_datums[i]);
	}
	if (nseed_values == 0)
	{
		table_close(rel, AccessShareLock);
		pfree(seed_values);
		PG_RETURN_NULL();
	}
	qsort(seed_values, nseed_values, sizeof(int64), sorted_heap_int64_cmp);
	obs_top_level = sorted_heap_graph_obs_begin("sorted_heap_expand_twohop_rerank");
	if (obs_top_level)
		sorted_heap_graph_obs_add_seed(nseed_values, 0.0);

	hop1_values = sorted_heap_graph_collect_targets(rel, info,
													entity_att, relation_att, target_att,
													seed_values, nseed_values,
													has_hop1_filter, (int16) hop1_filter,
													0, &nhop1_values);
	if (seed_values)
		pfree(seed_values);

	if (nhop1_values <= 0)
	{
		if (hop1_values)
			pfree(hop1_values);
		table_close(rel, AccessShareLock);
		sorted_heap_graph_route_obs_capture(rel_oid);
		sorted_heap_graph_obs_finish();
		PG_RETURN_NULL();
	}

	sorted_heap_graph_emit_rerank(rsinfo,
								  rel, info,
								  entity_att, relation_att, target_att,
								  embedding_att, payload_att,
								  hop1_values, nhop1_values,
								  query, top_k,
								  has_hop2_filter, (int16) hop2_filter,
								  limit_rows);

	if (hop1_values)
		pfree(hop1_values);
	table_close(rel, AccessShareLock);
	sorted_heap_graph_route_obs_capture(rel_oid);
	sorted_heap_graph_obs_finish();

	PG_RETURN_NULL();
}

Datum
sorted_heap_expand_twohop_path_rerank(PG_FUNCTION_ARGS)
{
	ReturnSetInfo	   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Oid					rel_oid;
	ArrayType		   *seed_arr;
	Svec			   *query;
	int32				top_k;
	bool				has_hop1_filter;
	int32				hop1_filter = 0;
	bool				has_hop2_filter;
	int32				hop2_filter = 0;
	int32				limit_rows;
	Relation			rel;
	SortedHeapRelInfo  *info;
	SortedHeapGraphAttrs gattrs;
	AttrNumber			entity_att;
	AttrNumber			relation_att;
	AttrNumber			target_att;
	AttrNumber			embedding_att;
	AttrNumber			payload_att;
	const char		   *relname;
	Datum			   *seed_datums = NULL;
	bool			   *seed_nulls = NULL;
	int					nseed = 0;
	int64			   *seed_values = NULL;
	int					nseed_values = 0;
	GraphRagTargetScore *hop1_scores = NULL;
	int					nhop1_scores = 0;
	bool				obs_top_level;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("sorted_heap_expand_twohop_path_rerank must be called in a set-returning context")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
	{
		InitMaterializedSRF(fcinfo, 0);
		PG_RETURN_NULL();
	}

	rel_oid = PG_GETARG_OID(0);
	seed_arr = PG_GETARG_ARRAYTYPE_P(1);
	query = PG_GETARG_SVEC_P(2);
	top_k = PG_GETARG_INT32(3);
	has_hop1_filter = !PG_ARGISNULL(4);
	if (has_hop1_filter)
		hop1_filter = PG_GETARG_INT32(4);
	has_hop2_filter = !PG_ARGISNULL(5);
	if (has_hop2_filter)
		hop2_filter = PG_GETARG_INT32(5);
	limit_rows = PG_ARGISNULL(6) ? 0 : PG_GETARG_INT32(6);

	if (top_k < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_expand_twohop_path_rerank: top_k must be >= 1")));
	if (limit_rows < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_expand_twohop_path_rerank: limit_rows must be >= 0")));
	if (has_hop1_filter &&
		(hop1_filter < PG_INT16_MIN || hop1_filter > PG_INT16_MAX))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_heap_expand_twohop_path_rerank: hop1_relation_filter %d is outside int2 range",
						hop1_filter)));
	if (has_hop2_filter &&
		(hop2_filter < PG_INT16_MIN || hop2_filter > PG_INT16_MAX))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_heap_expand_twohop_path_rerank: hop2_relation_filter %d is outside int2 range",
						hop2_filter)));

	InitMaterializedSRF(fcinfo, 0);

	if (ARR_NDIM(seed_arr) == 0 || ArrayGetNItems(ARR_NDIM(seed_arr), ARR_DIMS(seed_arr)) == 0)
		PG_RETURN_NULL();
	if (ARR_NDIM(seed_arr) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("sorted_heap_expand_twohop_path_rerank: seed_ids must be a one-dimensional int4[]")));
	if (ARR_ELEMTYPE(seed_arr) != INT4OID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("sorted_heap_expand_twohop_path_rerank: seed_ids must be int4[]")));

	rel = table_open(rel_oid, AccessShareLock);
	relname = pstrdup(RelationGetRelationName(rel));
	if (rel->rd_tableam != &sorted_heap_am_routine)
	{
		table_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("sorted_heap_expand_twohop_path_rerank: relation \"%s\" is not a sorted_heap table",
						relname)));
	}

	sorted_heap_graph_resolve_attrs(rel, "sorted_heap_expand_twohop_path_rerank", &gattrs);
	entity_att = gattrs.entity_att;
	relation_att = gattrs.relation_att;
	target_att = gattrs.target_att;
	embedding_att = gattrs.embedding_att;
	payload_att = gattrs.payload_att;

	info = sorted_heap_get_relinfo(rel);

	deconstruct_array(seed_arr, INT4OID, 4, true, TYPALIGN_INT,
					  &seed_datums, &seed_nulls, &nseed);
	if (nseed <= 0)
	{
		table_close(rel, AccessShareLock);
		PG_RETURN_NULL();
	}

	seed_values = palloc(sizeof(int64) * nseed);
	for (int i = 0; i < nseed; i++)
	{
		if (seed_nulls[i])
			continue;
		seed_values[nseed_values++] = (int64) DatumGetInt32(seed_datums[i]);
	}
	if (nseed_values == 0)
	{
		table_close(rel, AccessShareLock);
		pfree(seed_values);
		PG_RETURN_NULL();
	}
	qsort(seed_values, nseed_values, sizeof(int64), sorted_heap_int64_cmp);
	obs_top_level = sorted_heap_graph_obs_begin("sorted_heap_expand_twohop_path_rerank");
	if (obs_top_level)
		sorted_heap_graph_obs_add_seed(nseed_values, 0.0);

	hop1_scores = sorted_heap_graph_collect_target_scores(rel, info,
															entity_att, relation_att, target_att,
															embedding_att,
															seed_values, nseed_values,
															query,
															has_hop1_filter, (int16) hop1_filter,
															limit_rows, &nhop1_scores);
	if (seed_values)
		pfree(seed_values);

	if (nhop1_scores <= 0)
	{
		if (hop1_scores)
			pfree(hop1_scores);
		table_close(rel, AccessShareLock);
		sorted_heap_graph_route_obs_capture(rel_oid);
		sorted_heap_graph_obs_finish();
		PG_RETURN_NULL();
	}

	sorted_heap_graph_emit_twohop_path_rerank(rsinfo,
											 rel, info,
											 entity_att, relation_att, target_att,
											 embedding_att, payload_att,
											 hop1_scores, nhop1_scores,
											 query, top_k,
											 has_hop2_filter, (int16) hop2_filter,
											 limit_rows);

	if (hop1_scores)
		pfree(hop1_scores);
	table_close(rel, AccessShareLock);
	sorted_heap_graph_route_obs_capture(rel_oid);
	sorted_heap_graph_obs_finish();

	PG_RETURN_NULL();
}

Datum
sorted_heap_expand_multihop_rerank(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Oid rel_oid;
	ArrayType *seed_arr;
	Svec *query;
	int32 top_k;
	ArrayType *path_arr;
	int32 limit_rows;
	Relation rel;
	SortedHeapRelInfo *info;
	SortedHeapGraphAttrs gattrs;
	AttrNumber entity_att;
	AttrNumber relation_att;
	AttrNumber target_att;
	AttrNumber embedding_att;
	AttrNumber payload_att;
	const char *relname;
	Datum *seed_datums = NULL;
	bool *seed_nulls = NULL;
	int nseed = 0;
	int64 *seed_values = NULL;
	int nseed_values = 0;
	Datum *path_datums = NULL;
	bool *path_nulls = NULL;
	int npath = 0;
	int16 *path_filters = NULL;
	int64 *current_values = NULL;
	int current_count = 0;
	bool current_is_seed = false;
	bool obs_top_level;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("sorted_heap_expand_multihop_rerank must be called in a set-returning context")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) || PG_ARGISNULL(4))
	{
		InitMaterializedSRF(fcinfo, 0);
		PG_RETURN_NULL();
	}

	rel_oid = PG_GETARG_OID(0);
	seed_arr = PG_GETARG_ARRAYTYPE_P(1);
	query = PG_GETARG_SVEC_P(2);
	top_k = PG_GETARG_INT32(3);
	path_arr = PG_GETARG_ARRAYTYPE_P(4);
	limit_rows = PG_ARGISNULL(5) ? 0 : PG_GETARG_INT32(5);

	if (top_k < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_expand_multihop_rerank: top_k must be >= 1")));
	if (limit_rows < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_expand_multihop_rerank: limit_rows must be >= 0")));

	InitMaterializedSRF(fcinfo, 0);

	if (ARR_NDIM(seed_arr) == 0 || ArrayGetNItems(ARR_NDIM(seed_arr), ARR_DIMS(seed_arr)) == 0)
		PG_RETURN_NULL();
	if (ARR_NDIM(seed_arr) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("sorted_heap_expand_multihop_rerank: seed_ids must be a one-dimensional int4[]")));
	if (ARR_ELEMTYPE(seed_arr) != INT4OID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("sorted_heap_expand_multihop_rerank: seed_ids must be int4[]")));
	if (ARR_NDIM(path_arr) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("sorted_heap_expand_multihop_rerank: relation_path must be a one-dimensional int4[]")));
	if (ARR_ELEMTYPE(path_arr) != INT4OID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("sorted_heap_expand_multihop_rerank: relation_path must be int4[]")));

	deconstruct_array(path_arr, INT4OID, 4, true, TYPALIGN_INT,
					  &path_datums, &path_nulls, &npath);
	if (npath <= 0)
		PG_RETURN_NULL();
	path_filters = palloc(sizeof(int16) * npath);
	for (int i = 0; i < npath; i++)
	{
		int32 filter;

		if (path_nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("sorted_heap_expand_multihop_rerank: relation_path must not contain NULLs")));
		filter = DatumGetInt32(path_datums[i]);
		if (filter < PG_INT16_MIN || filter > PG_INT16_MAX)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("sorted_heap_expand_multihop_rerank: relation_path element %d is outside int2 range",
							filter)));
		path_filters[i] = (int16) filter;
	}

	rel = table_open(rel_oid, AccessShareLock);
	relname = pstrdup(RelationGetRelationName(rel));
	if (rel->rd_tableam != &sorted_heap_am_routine)
	{
		table_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("sorted_heap_expand_multihop_rerank: relation \"%s\" is not a sorted_heap table",
						relname)));
	}

	sorted_heap_graph_resolve_attrs(rel, "sorted_heap_expand_multihop_rerank", &gattrs);
	entity_att = gattrs.entity_att;
	relation_att = gattrs.relation_att;
	target_att = gattrs.target_att;
	embedding_att = gattrs.embedding_att;
	payload_att = gattrs.payload_att;
	info = sorted_heap_get_relinfo(rel);

	deconstruct_array(seed_arr, INT4OID, 4, true, TYPALIGN_INT,
					  &seed_datums, &seed_nulls, &nseed);
	if (nseed <= 0)
	{
		table_close(rel, AccessShareLock);
		PG_RETURN_NULL();
	}

	seed_values = palloc(sizeof(int64) * nseed);
	for (int i = 0; i < nseed; i++)
	{
		if (seed_nulls[i])
			continue;
		seed_values[nseed_values++] = (int64) DatumGetInt32(seed_datums[i]);
	}
	if (nseed_values == 0)
	{
		table_close(rel, AccessShareLock);
		pfree(seed_values);
		PG_RETURN_NULL();
	}
	qsort(seed_values, nseed_values, sizeof(int64), sorted_heap_int64_cmp);
	obs_top_level = sorted_heap_graph_obs_begin("sorted_heap_expand_multihop_rerank");
	if (obs_top_level)
		sorted_heap_graph_obs_add_seed(nseed_values, 0.0);

	current_values = seed_values;
	current_count = nseed_values;
	current_is_seed = true;

	for (int hop_idx = 0; hop_idx < npath - 1; hop_idx++)
	{
		int64 *next_values = NULL;
		int next_count = 0;

		next_values = sorted_heap_graph_collect_targets(rel, info,
														 entity_att, relation_att, target_att,
														 current_values, current_count,
														 true, path_filters[hop_idx],
														 limit_rows, &next_count);
		if (!current_is_seed && current_values)
			pfree(current_values);
		current_values = next_values;
		current_count = next_count;
		current_is_seed = false;

		if (current_count <= 0)
		{
			if (!current_is_seed && current_values)
				pfree(current_values);
			if (seed_values)
				pfree(seed_values);
			if (path_filters)
				pfree(path_filters);
			table_close(rel, AccessShareLock);
			sorted_heap_graph_route_obs_capture(rel_oid);
			sorted_heap_graph_obs_finish();
			PG_RETURN_NULL();
		}
	}

	sorted_heap_graph_emit_rerank(rsinfo,
								  rel, info,
								  entity_att, relation_att, target_att,
								  embedding_att, payload_att,
								  current_values, current_count,
								  query, top_k,
								  true, path_filters[npath - 1],
								  limit_rows);

	if (!current_is_seed && current_values)
		pfree(current_values);
	if (seed_values)
		pfree(seed_values);
	if (path_filters)
		pfree(path_filters);
	table_close(rel, AccessShareLock);
	sorted_heap_graph_route_obs_capture(rel_oid);
	sorted_heap_graph_obs_finish();

	PG_RETURN_NULL();
}

Datum
sorted_heap_expand_multihop_path_rerank(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Oid rel_oid;
	ArrayType *seed_arr;
	Svec *query;
	int32 top_k;
	ArrayType *path_arr;
	int32 limit_rows;
	Relation rel;
	SortedHeapRelInfo *info;
	SortedHeapGraphAttrs gattrs;
	AttrNumber entity_att;
	AttrNumber relation_att;
	AttrNumber target_att;
	AttrNumber embedding_att;
	AttrNumber payload_att;
	const char *relname;
	Datum *seed_datums = NULL;
	bool *seed_nulls = NULL;
	int nseed = 0;
	int64 *seed_values = NULL;
	int nseed_values = 0;
	Datum *path_datums = NULL;
	bool *path_nulls = NULL;
	int npath = 0;
	int16 *path_filters = NULL;
	GraphRagTargetScore *current_scores = NULL;
	int current_count = 0;
	bool obs_top_level;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("sorted_heap_expand_multihop_path_rerank must be called in a set-returning context")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) || PG_ARGISNULL(4))
	{
		InitMaterializedSRF(fcinfo, 0);
		PG_RETURN_NULL();
	}

	rel_oid = PG_GETARG_OID(0);
	seed_arr = PG_GETARG_ARRAYTYPE_P(1);
	query = PG_GETARG_SVEC_P(2);
	top_k = PG_GETARG_INT32(3);
	path_arr = PG_GETARG_ARRAYTYPE_P(4);
	limit_rows = PG_ARGISNULL(5) ? 0 : PG_GETARG_INT32(5);

	if (top_k < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_expand_multihop_path_rerank: top_k must be >= 1")));
	if (limit_rows < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_expand_multihop_path_rerank: limit_rows must be >= 0")));

	InitMaterializedSRF(fcinfo, 0);

	if (ARR_NDIM(seed_arr) == 0 || ArrayGetNItems(ARR_NDIM(seed_arr), ARR_DIMS(seed_arr)) == 0)
		PG_RETURN_NULL();
	if (ARR_NDIM(seed_arr) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("sorted_heap_expand_multihop_path_rerank: seed_ids must be a one-dimensional int4[]")));
	if (ARR_ELEMTYPE(seed_arr) != INT4OID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("sorted_heap_expand_multihop_path_rerank: seed_ids must be int4[]")));
	if (ARR_NDIM(path_arr) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("sorted_heap_expand_multihop_path_rerank: relation_path must be a one-dimensional int4[]")));
	if (ARR_ELEMTYPE(path_arr) != INT4OID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("sorted_heap_expand_multihop_path_rerank: relation_path must be int4[]")));

	deconstruct_array(path_arr, INT4OID, 4, true, TYPALIGN_INT,
					  &path_datums, &path_nulls, &npath);
	if (npath <= 0)
		PG_RETURN_NULL();
	path_filters = palloc(sizeof(int16) * npath);
	for (int i = 0; i < npath; i++)
	{
		int32 filter;

		if (path_nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("sorted_heap_expand_multihop_path_rerank: relation_path must not contain NULLs")));
		filter = DatumGetInt32(path_datums[i]);
		if (filter < PG_INT16_MIN || filter > PG_INT16_MAX)
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
					 errmsg("sorted_heap_expand_multihop_path_rerank: relation_path element %d is outside int2 range",
							filter)));
		path_filters[i] = (int16) filter;
	}

	rel = table_open(rel_oid, AccessShareLock);
	relname = pstrdup(RelationGetRelationName(rel));
	if (rel->rd_tableam != &sorted_heap_am_routine)
	{
		table_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("sorted_heap_expand_multihop_path_rerank: relation \"%s\" is not a sorted_heap table",
						relname)));
	}

	sorted_heap_graph_resolve_attrs(rel, "sorted_heap_expand_multihop_path_rerank", &gattrs);
	entity_att = gattrs.entity_att;
	relation_att = gattrs.relation_att;
	target_att = gattrs.target_att;
	embedding_att = gattrs.embedding_att;
	payload_att = gattrs.payload_att;
	info = sorted_heap_get_relinfo(rel);

	deconstruct_array(seed_arr, INT4OID, 4, true, TYPALIGN_INT,
					  &seed_datums, &seed_nulls, &nseed);
	if (nseed <= 0)
	{
		table_close(rel, AccessShareLock);
		PG_RETURN_NULL();
	}

	seed_values = palloc(sizeof(int64) * nseed);
	for (int i = 0; i < nseed; i++)
	{
		if (seed_nulls[i])
			continue;
		seed_values[nseed_values++] = (int64) DatumGetInt32(seed_datums[i]);
	}
	if (nseed_values == 0)
	{
		table_close(rel, AccessShareLock);
		pfree(seed_values);
		PG_RETURN_NULL();
	}
	qsort(seed_values, nseed_values, sizeof(int64), sorted_heap_int64_cmp);
	obs_top_level = sorted_heap_graph_obs_begin("sorted_heap_expand_multihop_path_rerank");
	if (obs_top_level)
		sorted_heap_graph_obs_add_seed(nseed_values, 0.0);

	current_scores = graph_rag_seed_scores_from_values(seed_values, nseed_values);
	current_count = nseed_values;
	pfree(seed_values);
	seed_values = NULL;

	for (int hop_idx = 0; hop_idx < npath - 1; hop_idx++)
	{
		GraphRagTargetScore *next_scores = NULL;
		int next_count = 0;

		next_scores = sorted_heap_graph_collect_scored_targets(rel, info,
																 entity_att, relation_att, target_att,
																 embedding_att,
																 current_scores, current_count,
																 query,
																 true, path_filters[hop_idx],
																 limit_rows, &next_count);
		if (current_scores)
			pfree(current_scores);
		current_scores = next_scores;
		current_count = next_count;

		if (current_count <= 0)
		{
			if (current_scores)
				pfree(current_scores);
			if (path_filters)
				pfree(path_filters);
			table_close(rel, AccessShareLock);
			sorted_heap_graph_route_obs_capture(rel_oid);
			sorted_heap_graph_obs_finish();
			PG_RETURN_NULL();
		}
	}

	sorted_heap_graph_emit_twohop_path_rerank(rsinfo,
											 rel, info,
											 entity_att, relation_att, target_att,
											 embedding_att, payload_att,
											 current_scores, current_count,
											 query, top_k,
											 true, path_filters[npath - 1],
											 limit_rows);

	if (current_scores)
		pfree(current_scores);
	if (path_filters)
		pfree(path_filters);
	table_close(rel, AccessShareLock);
	sorted_heap_graph_route_obs_capture(rel_oid);
	sorted_heap_graph_obs_finish();

	PG_RETURN_NULL();
}

/* ----------------------------------------------------------------
 *  GraphRAG convenience wrapper: ANN seed + expand + rerank in one call.
 *
 *  This is intentionally a narrow API wrapper. It uses SPI to collect ANN
 *  seed IDs from the base relation, then delegates the expansion+rereank
 *  step to sorted_heap_expand_rerank(...), which already owns the verified
 *  zone-map-driven implementation.
 * ---------------------------------------------------------------- */
Datum
sorted_heap_graph_rag_scan(PG_FUNCTION_ARGS)
{
	ReturnSetInfo	   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Oid					rel_oid;
	Svec			   *query;
	int32				ann_k;
	int32				top_k;
	bool				has_relation_filter;
	int32				relation_filter = 0;
	int32				limit_rows;
	Relation			rel;
	SortedHeapGraphAttrs gattrs;
	Oid					embedding_typid;
	char			   *schema_name;
	char			   *relname;
	const char		   *quoted_schema;
	const char		   *quoted_relname;
	const char		   *quoted_ext_schema;
	const char		   *quoted_target_col;
	const char		   *quoted_embedding_col;
	StringInfoData		seed_sql;
	StringInfoData		helper_sql;
	Oid					argtypes[6];
	Datum				values[6];
	char				nulls[6] = {' ', ' ', ' ', ' ', ' ', ' '};
	int					ret;
	ArrayType		   *seed_arr = NULL;
	bool				isnull;
	HeapTuple			tuple;
	TupleDesc			tupdesc;
	uint64				row_idx;
	instr_time			t_ann_start;
	instr_time			t_ann_end;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("sorted_heap_graph_rag_scan must be called in a set-returning context")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
	{
		InitMaterializedSRF(fcinfo, 0);
		PG_RETURN_NULL();
	}

	rel_oid = PG_GETARG_OID(0);
	query = PG_GETARG_SVEC_P(1);
	ann_k = PG_GETARG_INT32(2);
	top_k = PG_GETARG_INT32(3);
	has_relation_filter = !PG_ARGISNULL(4);
	if (has_relation_filter)
		relation_filter = PG_GETARG_INT32(4);
	limit_rows = PG_ARGISNULL(5) ? 0 : PG_GETARG_INT32(5);

	if (ann_k < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_graph_rag_scan: ann_k must be >= 1")));
	if (top_k < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_graph_rag_scan: top_k must be >= 1")));
	if (limit_rows < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_graph_rag_scan: limit_rows must be >= 0")));
	if (has_relation_filter &&
		(relation_filter < PG_INT16_MIN || relation_filter > PG_INT16_MAX))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_heap_graph_rag_scan: relation_filter %d is outside int2 range",
						relation_filter)));

	InitMaterializedSRF(fcinfo, 0);

	rel = table_open(rel_oid, AccessShareLock);
	relname = pstrdup(RelationGetRelationName(rel));
	if (rel->rd_tableam != &sorted_heap_am_routine)
	{
		table_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("sorted_heap_graph_rag_scan: relation \"%s\" is not a sorted_heap table",
						relname)));
	}

	sorted_heap_graph_resolve_attrs(rel, "sorted_heap_graph_rag_scan", &gattrs);
	embedding_typid = gattrs.embedding_typid;
	sorted_heap_graph_obs_begin("sorted_heap_graph_rag_scan");

	schema_name = get_namespace_name(get_rel_namespace(rel_oid));
	quoted_schema = quote_identifier(schema_name);
	quoted_relname = quote_identifier(relname);
	quoted_ext_schema = sorted_heap_get_ext_schema();
	quoted_target_col = quote_identifier(NameStr(gattrs.target_col));
	quoted_embedding_col = quote_identifier(NameStr(gattrs.embedding_col));

	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
	{
		table_close(rel, AccessShareLock);
		elog(ERROR, "sorted_heap_graph_rag_scan: SPI_connect failed: %d", ret);
	}

	initStringInfo(&seed_sql);
	appendStringInfo(&seed_sql,
					 "SELECT array_agg(%s::int4) "
					 "FROM (SELECT DISTINCT %s "
					 "      FROM (SELECT %s "
					 "            FROM %s.%s "
					 "            ORDER BY %s <=> $1 LIMIT %d) ann) seeds",
					 quoted_target_col, quoted_target_col, quoted_target_col,
					 quoted_schema, quoted_relname, quoted_embedding_col, ann_k);

	argtypes[0] = embedding_typid;
	values[0] = PointerGetDatum(query);
	INSTR_TIME_SET_CURRENT(t_ann_start);
	ret = SPI_execute_with_args(seed_sql.data, 1, argtypes, values, nulls,
								true, 1);
	INSTR_TIME_SET_CURRENT(t_ann_end);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		table_close(rel, AccessShareLock);
		elog(ERROR, "sorted_heap_graph_rag_scan: seed query failed: %d", ret);
	}

	if (SPI_processed > 0)
	{
		tuple = SPI_tuptable->vals[0];
		tupdesc = SPI_tuptable->tupdesc;
		values[0] = SPI_getbinval(tuple, tupdesc, 1, &isnull);
		if (!isnull)
			seed_arr = DatumGetArrayTypePCopy(values[0]);
		SPI_freetuptable(SPI_tuptable);
	}
	sorted_heap_graph_obs_add_seed(
		sorted_heap_graph_count_int4_array(seed_arr),
		INSTR_TIME_GET_MILLISEC(t_ann_end) -
		INSTR_TIME_GET_MILLISEC(t_ann_start));

	if (seed_arr == NULL)
	{
		SPI_finish();
		table_close(rel, AccessShareLock);
		sorted_heap_graph_route_obs_capture(rel_oid);
		sorted_heap_graph_obs_finish();
		PG_RETURN_NULL();
	}

	initStringInfo(&helper_sql);
	appendStringInfo(&helper_sql,
					 "SELECT entity_id, relation_id, target_id, payload, distance "
					 "FROM %s.sorted_heap_expand_rerank($1::regclass, $2::int4[], $3, $4::int4, $5::int4, $6::int4)",
					 quoted_ext_schema);

	argtypes[0] = REGCLASSOID;
	values[0] = ObjectIdGetDatum(rel_oid);
	argtypes[1] = INT4ARRAYOID;
	values[1] = PointerGetDatum(seed_arr);
	argtypes[2] = embedding_typid;
	values[2] = PointerGetDatum(query);
	argtypes[3] = INT4OID;
	values[3] = Int32GetDatum(top_k);
	argtypes[4] = INT4OID;
	values[4] = Int32GetDatum(relation_filter);
	nulls[4] = has_relation_filter ? ' ' : 'n';
	argtypes[5] = INT4OID;
	values[5] = Int32GetDatum(limit_rows);

	ret = SPI_execute_with_args(helper_sql.data, 6, argtypes, values, nulls,
								true, 0);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		table_close(rel, AccessShareLock);
		elog(ERROR, "sorted_heap_graph_rag_scan: helper query failed: %d", ret);
	}

	tupdesc = SPI_tuptable->tupdesc;
	for (row_idx = 0; row_idx < SPI_processed; row_idx++)
	{
		Datum out_values[5];
		bool out_nulls[5];
		int col;

		tuple = SPI_tuptable->vals[row_idx];
		for (col = 0; col < 5; col++)
			out_values[col] = SPI_getbinval(tuple, tupdesc, col + 1, &out_nulls[col]);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 out_values, out_nulls);
	}
	SPI_freetuptable(SPI_tuptable);
	SPI_finish();
	table_close(rel, AccessShareLock);
	sorted_heap_graph_route_obs_capture(rel_oid);
	sorted_heap_graph_obs_finish();

	PG_RETURN_NULL();
}

/* ----------------------------------------------------------------
 *  GraphRAG convenience wrapper for fact-shaped two-hop expansion:
 *  ANN seed on entity_id, hop1 expansion, then hop2 rerank in one call.
 * ---------------------------------------------------------------- */
Datum
sorted_heap_graph_rag_twohop_scan(PG_FUNCTION_ARGS)
{
	ReturnSetInfo	   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Oid					rel_oid;
	Svec			   *query;
	int32				ann_k;
	int32				top_k;
	bool				has_hop1_filter;
	int32				hop1_filter = 0;
	bool				has_hop2_filter;
	int32				hop2_filter = 0;
	int32				limit_rows;
	Relation			rel;
	SortedHeapGraphAttrs gattrs;
	Oid					embedding_typid;
	char			   *schema_name;
	char			   *relname;
	const char		   *quoted_schema;
	const char		   *quoted_relname;
	const char		   *quoted_ext_schema;
	const char		   *quoted_entity_col;
	const char		   *quoted_embedding_col;
	StringInfoData		seed_sql;
	StringInfoData		helper_sql;
	Oid					argtypes[7];
	Datum				values[7];
	char				nulls[7] = {' ', ' ', ' ', ' ', ' ', ' ', ' '};
	int					ret;
	ArrayType		   *seed_arr = NULL;
	bool				isnull;
	HeapTuple			tuple;
	TupleDesc			tupdesc;
	uint64				row_idx;
	instr_time			t_ann_start;
	instr_time			t_ann_end;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("sorted_heap_graph_rag_twohop_scan must be called in a set-returning context")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
	{
		InitMaterializedSRF(fcinfo, 0);
		PG_RETURN_NULL();
	}

	rel_oid = PG_GETARG_OID(0);
	query = PG_GETARG_SVEC_P(1);
	ann_k = PG_GETARG_INT32(2);
	top_k = PG_GETARG_INT32(3);
	has_hop1_filter = !PG_ARGISNULL(4);
	if (has_hop1_filter)
		hop1_filter = PG_GETARG_INT32(4);
	has_hop2_filter = !PG_ARGISNULL(5);
	if (has_hop2_filter)
		hop2_filter = PG_GETARG_INT32(5);
	limit_rows = PG_ARGISNULL(6) ? 0 : PG_GETARG_INT32(6);

	if (ann_k < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_graph_rag_twohop_scan: ann_k must be >= 1")));
	if (top_k < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_graph_rag_twohop_scan: top_k must be >= 1")));
	if (limit_rows < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_graph_rag_twohop_scan: limit_rows must be >= 0")));
	if (has_hop1_filter &&
		(hop1_filter < PG_INT16_MIN || hop1_filter > PG_INT16_MAX))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_heap_graph_rag_twohop_scan: hop1_relation_filter %d is outside int2 range",
						hop1_filter)));
	if (has_hop2_filter &&
		(hop2_filter < PG_INT16_MIN || hop2_filter > PG_INT16_MAX))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_heap_graph_rag_twohop_scan: hop2_relation_filter %d is outside int2 range",
						hop2_filter)));

	InitMaterializedSRF(fcinfo, 0);

	rel = table_open(rel_oid, AccessShareLock);
	relname = pstrdup(RelationGetRelationName(rel));
	if (rel->rd_tableam != &sorted_heap_am_routine)
	{
		table_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("sorted_heap_graph_rag_twohop_scan: relation \"%s\" is not a sorted_heap table",
						relname)));
	}

	sorted_heap_graph_resolve_attrs(rel, "sorted_heap_graph_rag_twohop_scan", &gattrs);
	embedding_typid = gattrs.embedding_typid;
	sorted_heap_graph_obs_begin("sorted_heap_graph_rag_twohop_scan");

	schema_name = get_namespace_name(get_rel_namespace(rel_oid));
	quoted_schema = quote_identifier(schema_name);
	quoted_relname = quote_identifier(relname);
	quoted_ext_schema = sorted_heap_get_ext_schema();
	quoted_entity_col = quote_identifier(NameStr(gattrs.entity_col));
	quoted_embedding_col = quote_identifier(NameStr(gattrs.embedding_col));

	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
	{
		table_close(rel, AccessShareLock);
		elog(ERROR, "sorted_heap_graph_rag_twohop_scan: SPI_connect failed: %d", ret);
	}

	initStringInfo(&seed_sql);
	appendStringInfo(&seed_sql,
					 "SELECT array_agg(%s::int4) "
					 "FROM (SELECT DISTINCT %s "
					 "      FROM (SELECT %s "
					 "            FROM %s.%s "
					 "            ORDER BY %s <=> $1 LIMIT %d) ann) seeds",
					 quoted_entity_col, quoted_entity_col, quoted_entity_col,
					 quoted_schema, quoted_relname, quoted_embedding_col, ann_k);

	argtypes[0] = embedding_typid;
	values[0] = PointerGetDatum(query);
	INSTR_TIME_SET_CURRENT(t_ann_start);
	ret = SPI_execute_with_args(seed_sql.data, 1, argtypes, values, nulls,
								true, 1);
	INSTR_TIME_SET_CURRENT(t_ann_end);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		table_close(rel, AccessShareLock);
		elog(ERROR, "sorted_heap_graph_rag_twohop_scan: seed query failed: %d", ret);
	}

	if (SPI_processed > 0)
	{
		tuple = SPI_tuptable->vals[0];
		tupdesc = SPI_tuptable->tupdesc;
		values[0] = SPI_getbinval(tuple, tupdesc, 1, &isnull);
		if (!isnull)
			seed_arr = DatumGetArrayTypePCopy(values[0]);
		SPI_freetuptable(SPI_tuptable);
	}
	sorted_heap_graph_obs_add_seed(
		sorted_heap_graph_count_int4_array(seed_arr),
		INSTR_TIME_GET_MILLISEC(t_ann_end) -
		INSTR_TIME_GET_MILLISEC(t_ann_start));

	if (seed_arr == NULL)
	{
		SPI_finish();
		table_close(rel, AccessShareLock);
		sorted_heap_graph_route_obs_capture(rel_oid);
		sorted_heap_graph_obs_finish();
		PG_RETURN_NULL();
	}

	initStringInfo(&helper_sql);
	appendStringInfo(&helper_sql,
					 "SELECT entity_id, relation_id, target_id, payload, distance "
					 "FROM %s.sorted_heap_expand_twohop_rerank("
					 "$1::regclass, $2::int4[], $3, $4::int4, $5::int4, $6::int4, $7::int4)",
					 quoted_ext_schema);

	argtypes[0] = REGCLASSOID;
	values[0] = ObjectIdGetDatum(rel_oid);
	argtypes[1] = INT4ARRAYOID;
	values[1] = PointerGetDatum(seed_arr);
	argtypes[2] = embedding_typid;
	values[2] = PointerGetDatum(query);
	argtypes[3] = INT4OID;
	values[3] = Int32GetDatum(top_k);
	argtypes[4] = INT4OID;
	values[4] = Int32GetDatum(hop1_filter);
	nulls[4] = has_hop1_filter ? ' ' : 'n';
	argtypes[5] = INT4OID;
	values[5] = Int32GetDatum(hop2_filter);
	nulls[5] = has_hop2_filter ? ' ' : 'n';
	argtypes[6] = INT4OID;
	values[6] = Int32GetDatum(limit_rows);

	ret = SPI_execute_with_args(helper_sql.data, 7, argtypes, values, nulls,
								true, 0);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		table_close(rel, AccessShareLock);
		elog(ERROR, "sorted_heap_graph_rag_twohop_scan: helper query failed: %d", ret);
	}

	tupdesc = SPI_tuptable->tupdesc;
	for (row_idx = 0; row_idx < SPI_processed; row_idx++)
	{
		Datum out_values[5];
		bool out_nulls[5];
		int col;

		tuple = SPI_tuptable->vals[row_idx];
		for (col = 0; col < 5; col++)
			out_values[col] = SPI_getbinval(tuple, tupdesc, col + 1, &out_nulls[col]);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 out_values, out_nulls);
	}
	SPI_freetuptable(SPI_tuptable);
	SPI_finish();
	table_close(rel, AccessShareLock);
	sorted_heap_graph_route_obs_capture(rel_oid);
	sorted_heap_graph_obs_finish();

	PG_RETURN_NULL();
}

Datum
sorted_heap_graph_rag_twohop_path_scan(PG_FUNCTION_ARGS)
{
	ReturnSetInfo	   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Oid					rel_oid;
	Svec			   *query;
	int32				ann_k;
	int32				top_k;
	bool				has_hop1_filter;
	int32				hop1_filter = 0;
	bool				has_hop2_filter;
	int32				hop2_filter = 0;
	int32				limit_rows;
	Relation			rel;
	SortedHeapGraphAttrs gattrs;
	Oid					embedding_typid;
	char			   *schema_name;
	char			   *relname;
	const char		   *quoted_schema;
	const char		   *quoted_relname;
	const char		   *quoted_ext_schema;
	const char		   *quoted_entity_col;
	const char		   *quoted_embedding_col;
	StringInfoData		seed_sql;
	StringInfoData		helper_sql;
	Oid					argtypes[7];
	Datum				values[7];
	char				nulls[7] = {' ', ' ', ' ', ' ', ' ', ' ', ' '};
	int					ret;
	ArrayType		   *seed_arr = NULL;
	bool				isnull;
	HeapTuple			tuple;
	TupleDesc			tupdesc;
	uint64				row_idx;
	instr_time			t_ann_start;
	instr_time			t_ann_end;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("sorted_heap_graph_rag_twohop_path_scan must be called in a set-returning context")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
	{
		InitMaterializedSRF(fcinfo, 0);
		PG_RETURN_NULL();
	}

	rel_oid = PG_GETARG_OID(0);
	query = PG_GETARG_SVEC_P(1);
	ann_k = PG_GETARG_INT32(2);
	top_k = PG_GETARG_INT32(3);
	has_hop1_filter = !PG_ARGISNULL(4);
	if (has_hop1_filter)
		hop1_filter = PG_GETARG_INT32(4);
	has_hop2_filter = !PG_ARGISNULL(5);
	if (has_hop2_filter)
		hop2_filter = PG_GETARG_INT32(5);
	limit_rows = PG_ARGISNULL(6) ? 0 : PG_GETARG_INT32(6);

	if (ann_k < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_graph_rag_twohop_path_scan: ann_k must be >= 1")));
	if (top_k < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_graph_rag_twohop_path_scan: top_k must be >= 1")));
	if (limit_rows < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_graph_rag_twohop_path_scan: limit_rows must be >= 0")));
	if (has_hop1_filter &&
		(hop1_filter < PG_INT16_MIN || hop1_filter > PG_INT16_MAX))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_heap_graph_rag_twohop_path_scan: hop1_relation_filter %d is outside int2 range",
						hop1_filter)));
	if (has_hop2_filter &&
		(hop2_filter < PG_INT16_MIN || hop2_filter > PG_INT16_MAX))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_heap_graph_rag_twohop_path_scan: hop2_relation_filter %d is outside int2 range",
						hop2_filter)));

	InitMaterializedSRF(fcinfo, 0);

	rel = table_open(rel_oid, AccessShareLock);
	relname = pstrdup(RelationGetRelationName(rel));
	if (rel->rd_tableam != &sorted_heap_am_routine)
	{
		table_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("sorted_heap_graph_rag_twohop_path_scan: relation \"%s\" is not a sorted_heap table",
						relname)));
	}

	sorted_heap_graph_resolve_attrs(rel, "sorted_heap_graph_rag_twohop_path_scan", &gattrs);
	embedding_typid = gattrs.embedding_typid;
	sorted_heap_graph_obs_begin("sorted_heap_graph_rag_twohop_path_scan");

	schema_name = get_namespace_name(get_rel_namespace(rel_oid));
	quoted_schema = quote_identifier(schema_name);
	quoted_relname = quote_identifier(relname);
	quoted_ext_schema = sorted_heap_get_ext_schema();
	quoted_entity_col = quote_identifier(NameStr(gattrs.entity_col));
	quoted_embedding_col = quote_identifier(NameStr(gattrs.embedding_col));

	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
	{
		table_close(rel, AccessShareLock);
		elog(ERROR, "sorted_heap_graph_rag_twohop_path_scan: SPI_connect failed: %d", ret);
	}

	initStringInfo(&seed_sql);
	appendStringInfo(&seed_sql,
					 "SELECT array_agg(%s::int4) "
					 "FROM (SELECT DISTINCT %s "
					 "      FROM (SELECT %s "
					 "            FROM %s.%s "
					 "            ORDER BY %s <=> $1 LIMIT %d) ann) seeds",
					 quoted_entity_col, quoted_entity_col, quoted_entity_col,
					 quoted_schema, quoted_relname, quoted_embedding_col, ann_k);

	argtypes[0] = embedding_typid;
	values[0] = PointerGetDatum(query);
	INSTR_TIME_SET_CURRENT(t_ann_start);
	ret = SPI_execute_with_args(seed_sql.data, 1, argtypes, values, nulls,
								true, 1);
	INSTR_TIME_SET_CURRENT(t_ann_end);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		table_close(rel, AccessShareLock);
		elog(ERROR, "sorted_heap_graph_rag_twohop_path_scan: seed query failed: %d", ret);
	}

	if (SPI_processed > 0)
	{
		tuple = SPI_tuptable->vals[0];
		tupdesc = SPI_tuptable->tupdesc;
		values[0] = SPI_getbinval(tuple, tupdesc, 1, &isnull);
		if (!isnull)
			seed_arr = DatumGetArrayTypePCopy(values[0]);
		SPI_freetuptable(SPI_tuptable);
	}
	sorted_heap_graph_obs_add_seed(
		sorted_heap_graph_count_int4_array(seed_arr),
		INSTR_TIME_GET_MILLISEC(t_ann_end) -
		INSTR_TIME_GET_MILLISEC(t_ann_start));

	if (seed_arr == NULL)
	{
		SPI_finish();
		table_close(rel, AccessShareLock);
		sorted_heap_graph_route_obs_capture(rel_oid);
		sorted_heap_graph_obs_finish();
		PG_RETURN_NULL();
	}

	initStringInfo(&helper_sql);
	appendStringInfo(&helper_sql,
					 "SELECT entity_id, relation_id, target_id, payload, distance "
					 "FROM %s.sorted_heap_expand_twohop_path_rerank("
					 "$1::regclass, $2::int4[], $3, $4::int4, $5::int4, $6::int4, $7::int4)",
					 quoted_ext_schema);

	argtypes[0] = REGCLASSOID;
	values[0] = ObjectIdGetDatum(rel_oid);
	argtypes[1] = INT4ARRAYOID;
	values[1] = PointerGetDatum(seed_arr);
	argtypes[2] = embedding_typid;
	values[2] = PointerGetDatum(query);
	argtypes[3] = INT4OID;
	values[3] = Int32GetDatum(top_k);
	argtypes[4] = INT4OID;
	values[4] = Int32GetDatum(hop1_filter);
	nulls[4] = has_hop1_filter ? ' ' : 'n';
	argtypes[5] = INT4OID;
	values[5] = Int32GetDatum(hop2_filter);
	nulls[5] = has_hop2_filter ? ' ' : 'n';
	argtypes[6] = INT4OID;
	values[6] = Int32GetDatum(limit_rows);

	ret = SPI_execute_with_args(helper_sql.data, 7, argtypes, values, nulls,
								true, 0);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		table_close(rel, AccessShareLock);
		elog(ERROR, "sorted_heap_graph_rag_twohop_path_scan: helper query failed: %d", ret);
	}

	tupdesc = SPI_tuptable->tupdesc;
	for (row_idx = 0; row_idx < SPI_processed; row_idx++)
	{
		Datum out_values[5];
		bool out_nulls[5];
		int col;

		tuple = SPI_tuptable->vals[row_idx];
		for (col = 0; col < 5; col++)
			out_values[col] = SPI_getbinval(tuple, tupdesc, col + 1, &out_nulls[col]);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 out_values, out_nulls);
	}
	SPI_freetuptable(SPI_tuptable);
	SPI_finish();
	table_close(rel, AccessShareLock);
	sorted_heap_graph_route_obs_capture(rel_oid);
	sorted_heap_graph_obs_finish();

	PG_RETURN_NULL();
}

Datum
sorted_heap_graph_rag_multihop_scan(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Oid rel_oid;
	Svec *query;
	int32 ann_k;
	int32 top_k;
	ArrayType *path_arr;
	int32 limit_rows;
	Relation rel;
	SortedHeapGraphAttrs gattrs;
	Oid embedding_typid;
	char *schema_name;
	char *relname;
	const char *quoted_schema;
	const char *quoted_relname;
	const char *quoted_ext_schema;
	const char *quoted_entity_col;
	const char *quoted_embedding_col;
	StringInfoData seed_sql;
	StringInfoData helper_sql;
	Oid argtypes[6];
	Datum values[6];
	char nulls[6] = {' ', ' ', ' ', ' ', ' ', ' '};
	int ret;
	ArrayType *seed_arr = NULL;
	bool isnull;
	HeapTuple tuple;
	TupleDesc tupdesc;
	uint64 row_idx;
	instr_time t_ann_start;
	instr_time t_ann_end;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("sorted_heap_graph_rag_multihop_scan must be called in a set-returning context")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(4))
	{
		InitMaterializedSRF(fcinfo, 0);
		PG_RETURN_NULL();
	}

	rel_oid = PG_GETARG_OID(0);
	query = PG_GETARG_SVEC_P(1);
	ann_k = PG_GETARG_INT32(2);
	top_k = PG_GETARG_INT32(3);
	path_arr = PG_GETARG_ARRAYTYPE_P(4);
	limit_rows = PG_ARGISNULL(5) ? 0 : PG_GETARG_INT32(5);

	if (ann_k < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_graph_rag_multihop_scan: ann_k must be >= 1")));
	if (top_k < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_graph_rag_multihop_scan: top_k must be >= 1")));
	if (limit_rows < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_graph_rag_multihop_scan: limit_rows must be >= 0")));
	if (ARR_NDIM(path_arr) != 1 || ARR_ELEMTYPE(path_arr) != INT4OID)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("sorted_heap_graph_rag_multihop_scan: relation_path must be a one-dimensional int4[]")));

	InitMaterializedSRF(fcinfo, 0);

	rel = table_open(rel_oid, AccessShareLock);
	relname = pstrdup(RelationGetRelationName(rel));
	if (rel->rd_tableam != &sorted_heap_am_routine)
	{
		table_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("sorted_heap_graph_rag_multihop_scan: relation \"%s\" is not a sorted_heap table",
						relname)));
	}

	sorted_heap_graph_resolve_attrs(rel, "sorted_heap_graph_rag_multihop_scan", &gattrs);
	embedding_typid = gattrs.embedding_typid;
	sorted_heap_graph_obs_begin("sorted_heap_graph_rag_multihop_scan");

	schema_name = get_namespace_name(get_rel_namespace(rel_oid));
	quoted_schema = quote_identifier(schema_name);
	quoted_relname = quote_identifier(relname);
	quoted_ext_schema = sorted_heap_get_ext_schema();
	quoted_entity_col = quote_identifier(NameStr(gattrs.entity_col));
	quoted_embedding_col = quote_identifier(NameStr(gattrs.embedding_col));

	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
	{
		table_close(rel, AccessShareLock);
		elog(ERROR, "sorted_heap_graph_rag_multihop_scan: SPI_connect failed: %d", ret);
	}

	initStringInfo(&seed_sql);
	appendStringInfo(&seed_sql,
					 "SELECT array_agg(%s::int4) "
					 "FROM (SELECT DISTINCT %s "
					 "      FROM (SELECT %s "
					 "            FROM %s.%s "
					 "            ORDER BY %s <=> $1 LIMIT %d) ann) seeds",
					 quoted_entity_col, quoted_entity_col, quoted_entity_col,
					 quoted_schema, quoted_relname, quoted_embedding_col, ann_k);

	argtypes[0] = embedding_typid;
	values[0] = PointerGetDatum(query);
	INSTR_TIME_SET_CURRENT(t_ann_start);
	ret = SPI_execute_with_args(seed_sql.data, 1, argtypes, values, nulls, true, 1);
	INSTR_TIME_SET_CURRENT(t_ann_end);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		table_close(rel, AccessShareLock);
		elog(ERROR, "sorted_heap_graph_rag_multihop_scan: seed query failed: %d", ret);
	}

	if (SPI_processed > 0)
	{
		tuple = SPI_tuptable->vals[0];
		tupdesc = SPI_tuptable->tupdesc;
		values[0] = SPI_getbinval(tuple, tupdesc, 1, &isnull);
		if (!isnull)
			seed_arr = DatumGetArrayTypePCopy(values[0]);
		SPI_freetuptable(SPI_tuptable);
	}
	sorted_heap_graph_obs_add_seed(
		sorted_heap_graph_count_int4_array(seed_arr),
		INSTR_TIME_GET_MILLISEC(t_ann_end) -
		INSTR_TIME_GET_MILLISEC(t_ann_start));

	if (seed_arr == NULL)
	{
		SPI_finish();
		table_close(rel, AccessShareLock);
		sorted_heap_graph_route_obs_capture(rel_oid);
		sorted_heap_graph_obs_finish();
		PG_RETURN_NULL();
	}

	initStringInfo(&helper_sql);
	appendStringInfo(&helper_sql,
					 "SELECT entity_id, relation_id, target_id, payload, distance "
					 "FROM %s.sorted_heap_expand_multihop_rerank("
					 "$1::regclass, $2::int4[], $3, $4::int4, $5::int4[], $6::int4)",
					 quoted_ext_schema);

	argtypes[0] = REGCLASSOID;
	values[0] = ObjectIdGetDatum(rel_oid);
	argtypes[1] = INT4ARRAYOID;
	values[1] = PointerGetDatum(seed_arr);
	argtypes[2] = embedding_typid;
	values[2] = PointerGetDatum(query);
	argtypes[3] = INT4OID;
	values[3] = Int32GetDatum(top_k);
	argtypes[4] = INT4ARRAYOID;
	values[4] = PointerGetDatum(path_arr);
	argtypes[5] = INT4OID;
	values[5] = Int32GetDatum(limit_rows);

	ret = SPI_execute_with_args(helper_sql.data, 6, argtypes, values, nulls, true, 0);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		table_close(rel, AccessShareLock);
		elog(ERROR, "sorted_heap_graph_rag_multihop_scan: helper query failed: %d", ret);
	}

	tupdesc = SPI_tuptable->tupdesc;
	for (row_idx = 0; row_idx < SPI_processed; row_idx++)
	{
		Datum out_values[5];
		bool out_nulls[5];

		tuple = SPI_tuptable->vals[row_idx];
		for (int col = 0; col < 5; col++)
			out_values[col] = SPI_getbinval(tuple, tupdesc, col + 1, &out_nulls[col]);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, out_values, out_nulls);
	}
	SPI_freetuptable(SPI_tuptable);
	SPI_finish();
	table_close(rel, AccessShareLock);
	sorted_heap_graph_route_obs_capture(rel_oid);
	sorted_heap_graph_obs_finish();

	PG_RETURN_NULL();
}

Datum
sorted_heap_graph_rag_multihop_path_scan(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Oid rel_oid;
	Svec *query;
	int32 ann_k;
	int32 top_k;
	ArrayType *path_arr;
	int32 limit_rows;
	Relation rel;
	SortedHeapGraphAttrs gattrs;
	Oid embedding_typid;
	char *schema_name;
	char *relname;
	const char *quoted_schema;
	const char *quoted_relname;
	const char *quoted_ext_schema;
	const char *quoted_entity_col;
	const char *quoted_embedding_col;
	StringInfoData seed_sql;
	StringInfoData helper_sql;
	Oid argtypes[6];
	Datum values[6];
	char nulls[6] = {' ', ' ', ' ', ' ', ' ', ' '};
	int ret;
	ArrayType *seed_arr = NULL;
	bool isnull;
	HeapTuple tuple;
	TupleDesc tupdesc;
	uint64 row_idx;
	instr_time t_ann_start;
	instr_time t_ann_end;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("sorted_heap_graph_rag_multihop_path_scan must be called in a set-returning context")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(4))
	{
		InitMaterializedSRF(fcinfo, 0);
		PG_RETURN_NULL();
	}

	rel_oid = PG_GETARG_OID(0);
	query = PG_GETARG_SVEC_P(1);
	ann_k = PG_GETARG_INT32(2);
	top_k = PG_GETARG_INT32(3);
	path_arr = PG_GETARG_ARRAYTYPE_P(4);
	limit_rows = PG_ARGISNULL(5) ? 0 : PG_GETARG_INT32(5);

	if (ann_k < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_graph_rag_multihop_path_scan: ann_k must be >= 1")));
	if (top_k < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_graph_rag_multihop_path_scan: top_k must be >= 1")));
	if (limit_rows < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_heap_graph_rag_multihop_path_scan: limit_rows must be >= 0")));
	if (ARR_NDIM(path_arr) != 1 || ARR_ELEMTYPE(path_arr) != INT4OID)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("sorted_heap_graph_rag_multihop_path_scan: relation_path must be a one-dimensional int4[]")));

	InitMaterializedSRF(fcinfo, 0);

	rel = table_open(rel_oid, AccessShareLock);
	relname = pstrdup(RelationGetRelationName(rel));
	if (rel->rd_tableam != &sorted_heap_am_routine)
	{
		table_close(rel, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("sorted_heap_graph_rag_multihop_path_scan: relation \"%s\" is not a sorted_heap table",
						relname)));
	}

	sorted_heap_graph_resolve_attrs(rel, "sorted_heap_graph_rag_multihop_path_scan", &gattrs);
	embedding_typid = gattrs.embedding_typid;
	sorted_heap_graph_obs_begin("sorted_heap_graph_rag_multihop_path_scan");

	schema_name = get_namespace_name(get_rel_namespace(rel_oid));
	quoted_schema = quote_identifier(schema_name);
	quoted_relname = quote_identifier(relname);
	quoted_ext_schema = sorted_heap_get_ext_schema();
	quoted_entity_col = quote_identifier(NameStr(gattrs.entity_col));
	quoted_embedding_col = quote_identifier(NameStr(gattrs.embedding_col));

	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
	{
		table_close(rel, AccessShareLock);
		elog(ERROR, "sorted_heap_graph_rag_multihop_path_scan: SPI_connect failed: %d", ret);
	}

	initStringInfo(&seed_sql);
	appendStringInfo(&seed_sql,
					 "SELECT array_agg(%s::int4) "
					 "FROM (SELECT DISTINCT %s "
					 "      FROM (SELECT %s "
					 "            FROM %s.%s "
					 "            ORDER BY %s <=> $1 LIMIT %d) ann) seeds",
					 quoted_entity_col, quoted_entity_col, quoted_entity_col,
					 quoted_schema, quoted_relname, quoted_embedding_col, ann_k);

	argtypes[0] = embedding_typid;
	values[0] = PointerGetDatum(query);
	INSTR_TIME_SET_CURRENT(t_ann_start);
	ret = SPI_execute_with_args(seed_sql.data, 1, argtypes, values, nulls, true, 1);
	INSTR_TIME_SET_CURRENT(t_ann_end);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		table_close(rel, AccessShareLock);
		elog(ERROR, "sorted_heap_graph_rag_multihop_path_scan: seed query failed: %d", ret);
	}

	if (SPI_processed > 0)
	{
		tuple = SPI_tuptable->vals[0];
		tupdesc = SPI_tuptable->tupdesc;
		values[0] = SPI_getbinval(tuple, tupdesc, 1, &isnull);
		if (!isnull)
			seed_arr = DatumGetArrayTypePCopy(values[0]);
		SPI_freetuptable(SPI_tuptable);
	}
	sorted_heap_graph_obs_add_seed(
		sorted_heap_graph_count_int4_array(seed_arr),
		INSTR_TIME_GET_MILLISEC(t_ann_end) -
		INSTR_TIME_GET_MILLISEC(t_ann_start));

	if (seed_arr == NULL)
	{
		SPI_finish();
		table_close(rel, AccessShareLock);
		sorted_heap_graph_route_obs_capture(rel_oid);
		sorted_heap_graph_obs_finish();
		PG_RETURN_NULL();
	}

	initStringInfo(&helper_sql);
	appendStringInfo(&helper_sql,
					 "SELECT entity_id, relation_id, target_id, payload, distance "
					 "FROM %s.sorted_heap_expand_multihop_path_rerank("
					 "$1::regclass, $2::int4[], $3, $4::int4, $5::int4[], $6::int4)",
					 quoted_ext_schema);

	argtypes[0] = REGCLASSOID;
	values[0] = ObjectIdGetDatum(rel_oid);
	argtypes[1] = INT4ARRAYOID;
	values[1] = PointerGetDatum(seed_arr);
	argtypes[2] = embedding_typid;
	values[2] = PointerGetDatum(query);
	argtypes[3] = INT4OID;
	values[3] = Int32GetDatum(top_k);
	argtypes[4] = INT4ARRAYOID;
	values[4] = PointerGetDatum(path_arr);
	argtypes[5] = INT4OID;
	values[5] = Int32GetDatum(limit_rows);

	ret = SPI_execute_with_args(helper_sql.data, 6, argtypes, values, nulls, true, 0);
	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		table_close(rel, AccessShareLock);
		elog(ERROR, "sorted_heap_graph_rag_multihop_path_scan: helper query failed: %d", ret);
	}

	tupdesc = SPI_tuptable->tupdesc;
	for (row_idx = 0; row_idx < SPI_processed; row_idx++)
	{
		Datum out_values[5];
		bool out_nulls[5];

		tuple = SPI_tuptable->vals[row_idx];
		for (int col = 0; col < 5; col++)
			out_values[col] = SPI_getbinval(tuple, tupdesc, col + 1, &out_nulls[col]);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, out_values, out_nulls);
	}
	SPI_freetuptable(SPI_tuptable);
	SPI_finish();
	table_close(rel, AccessShareLock);
	sorted_heap_graph_route_obs_capture(rel_oid);
	sorted_heap_graph_obs_finish();

	PG_RETURN_NULL();
}
