/* ================================================================
 *  svec_graph_scan: NSW graph search via btree-backed sidecar
 *
 *  svec_graph_scan(tbl regclass, query svec,
 *                  graph_table text,
 *                  ef_search int4 DEFAULT 64,
 *                  lim int4 DEFAULT 10,
 *                  rerank_topk int4 DEFAULT 0,
 *                  entry_table text DEFAULT '')
 *  RETURNS TABLE(id text, distance float8)
 *
 *  Performs greedy best-first search on a Navigable Small World
 *  graph stored as a regular table with btree PK on nid (int4).
 *
 *  Graph table expected schema:
 *    nid          int4 PRIMARY KEY
 *    sketch       hsvec        -- reduced-dim embedding (MRL prefix)
 *    neighbors    int4[]       -- neighbor node IDs
 *    src_id       text         -- id in main table (for output)
 *    src_tid      tid          -- ctid in main table (for exact rerank)
 *
 *  For best performance, create a covering index:
 *    CREATE UNIQUE INDEX ON graph_tbl (nid)
 *        INCLUDE (sketch, neighbors);
 *  and VACUUM the graph table so navigation uses index-only scan.
 *
 *  Pipeline:
 *    1. NSW greedy search using sketch distances (hsvec, reduced dim)
 *    2. Exact rerank: fetch full embedding from main table via src_tid
 *    3. Return top-lim by exact cosine distance
 * ================================================================ */

/* Minimum contiguous run length to use range scan instead of point probes */
#define GRAPH_BATCH_MIN 3

/* --- Contiguous nid run (for batch neighbor reads) --- */
typedef struct
{
	int32		lo;
	int32		hi;		/* inclusive */
} NidRange;

/* --- Graph search entry (used in both candidate and result heaps) --- */

typedef struct
{
	float8		dist;
	int32		nid;
} GraphEntry;

/* Min-heap: pop closest candidate first */
static void
graph_minheap_siftup(GraphEntry *heap, int i)
{
	while (i > 0)
	{
		int			parent = (i - 1) / 2;
		GraphEntry	tmp;

		if (heap[i].dist >= heap[parent].dist)
			break;
		tmp = heap[i];
		heap[i] = heap[parent];
		heap[parent] = tmp;
		i = parent;
	}
}

static void
graph_minheap_siftdown(GraphEntry *heap, int n, int i)
{
	for (;;)
	{
		int			smallest = i;
		int			left = 2 * i + 1;
		int			right = 2 * i + 2;
		GraphEntry	tmp;

		if (left < n && heap[left].dist < heap[smallest].dist)
			smallest = left;
		if (right < n && heap[right].dist < heap[smallest].dist)
			smallest = right;
		if (smallest == i)
			break;
		tmp = heap[i];
		heap[i] = heap[smallest];
		heap[smallest] = tmp;
		i = smallest;
	}
}

/* Max-heap: bounded result set, evict furthest */
static void
graph_maxheap_siftup(GraphEntry *heap, int i)
{
	while (i > 0)
	{
		int			parent = (i - 1) / 2;
		GraphEntry	tmp;

		if (heap[i].dist <= heap[parent].dist)
			break;
		tmp = heap[i];
		heap[i] = heap[parent];
		heap[parent] = tmp;
		i = parent;
	}
}

static void
graph_maxheap_siftdown(GraphEntry *heap, int n, int i)
{
	for (;;)
	{
		int			largest = i;
		int			left = 2 * i + 1;
		int			right = 2 * i + 2;
		GraphEntry	tmp;

		if (left < n && heap[left].dist > heap[largest].dist)
			largest = left;
		if (right < n && heap[right].dist > heap[largest].dist)
			largest = right;
		if (largest == i)
			break;
		tmp = heap[i];
		heap[i] = heap[largest];
		heap[largest] = tmp;
		i = largest;
	}
}

static int
cmp_graph_entry(const void *a, const void *b)
{
	float8	da = ((const GraphEntry *) a)->dist;
	float8	db = ((const GraphEntry *) b)->dist;

	if (da < db) return -1;
	if (da > db) return 1;
	return 0;
}

/* --- Graph result for rerank phase --- */

typedef struct
{
	int32			nid;
	float8			distance;
	char		   *src_id;
	ItemPointerData	src_tid;
} GraphResult;

static int
cmp_graph_result(const void *a, const void *b)
{
	float8	da = ((const GraphResult *) a)->distance;
	float8	db = ((const GraphResult *) b)->distance;

	if (da < db) return -1;
	if (da > db) return 1;
	return 0;
}

static int
cmp_graph_result_by_blk(const void *a, const void *b)
{
	const GraphResult *ra = (const GraphResult *) a;
	const GraphResult *rb = (const GraphResult *) b;

	if (!ItemPointerIsValid(&ra->src_tid))
		return 1;
	if (!ItemPointerIsValid(&rb->src_tid))
		return -1;

	{
		BlockNumber ba = ItemPointerGetBlockNumber(&ra->src_tid);
		BlockNumber bb = ItemPointerGetBlockNumber(&rb->src_tid);

		if (ba < bb) return -1;
		if (ba > bb) return 1;
	}
	return 0;
}

/*
 * Find a covering btree index on (nid) INCLUDE (sketch, neighbors).
 * Returns the index OID if found, InvalidOid otherwise.
 * The covering index must:
 *   - be btree
 *   - have exactly 1 key column matching the PK's first column
 *   - include "sketch" and "neighbors" as non-key (INCLUDE) columns
 */
static Oid
find_covering_graph_index(Relation graph_rel, AttrNumber pk_att,
						  AttrNumber att_sketch, AttrNumber att_nbrs)
{
	List	   *indexlist;
	ListCell   *lc;

	indexlist = RelationGetIndexList(graph_rel);

	foreach(lc, indexlist)
	{
		Oid			idx_oid = lfirst_oid(lc);
		Relation	idx;
		Form_pg_index idx_form;
		int			nkeyatts, natts;

		if (idx_oid == graph_rel->rd_pkindex)
			continue;		/* skip the PK itself */

		idx = index_open(idx_oid, AccessShareLock);
		idx_form = idx->rd_index;

		/* Must be btree, unique, valid, ready */
		if (idx->rd_rel->relam != BTREE_AM_OID ||
			!idx_form->indisunique ||
			!idx_form->indisvalid ||
			!idx_form->indisready)
		{
			index_close(idx, AccessShareLock);
			continue;
		}

		nkeyatts = idx_form->indnkeyatts;
		natts = idx_form->indnatts;

		/* Must have 1 key column (nid) + at least 2 INCLUDE columns */
		if (nkeyatts != 1 || natts < 3 ||
			idx_form->indkey.values[0] != pk_att)
		{
			index_close(idx, AccessShareLock);
			continue;
		}

		/* Check that sketch and neighbors are among the INCLUDE columns */
		{
			bool	has_sketch = false;
			bool	has_nbrs = false;
			int		i;

			for (i = nkeyatts; i < natts; i++)
			{
				AttrNumber a = idx_form->indkey.values[i];

				if (a == att_sketch)
					has_sketch = true;
				else if (a == att_nbrs)
					has_nbrs = true;
			}

			if (has_sketch && has_nbrs)
			{
				Oid result = idx_oid;

				index_close(idx, AccessShareLock);
				list_free(indexlist);
				return result;
			}
		}

		index_close(idx, AccessShareLock);
	}

	list_free(indexlist);
	return InvalidOid;
}

/* --- Main graph scan function --- */

PG_FUNCTION_INFO_V1(svec_graph_scan);
Datum
svec_graph_scan(PG_FUNCTION_ARGS)
{
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Oid				tbl_oid = PG_GETARG_OID(0);
	Svec		   *query = PG_GETARG_SVEC_P(1);
	char		   *graph_tbl_name = text_to_cstring(PG_GETARG_TEXT_PP(2));
	int				ef_search = PG_GETARG_INT32(3);
	int				lim = PG_GETARG_INT32(4);
	int				rerank_topk = PG_GETARG_INT32(5);
	char		   *entry_tbl_name = text_to_cstring(PG_GETARG_TEXT_PP(6));

	/* Graph table state */
	Oid				graph_oid;
	Relation		graph_rel;
	Relation		graph_pk;		/* PK or covering index */
	TupleTableSlot *graph_slot;
	TupleDesc		graph_td;
	Snapshot		snapshot;
	AttrNumber		g_att_nid;	/* heap attno of PK nid column */
	AttrNumber		g_att_sketch, g_att_nbrs, g_att_src_id, g_att_src_tid;
	IndexScanDesc	graph_iscan;		/* point probes (1 key: eq) */
	IndexScanDesc	range_iscan;		/* range scans (2 keys: ge+le) */
	ScanKeyData		graph_skey[1];
	ScanKeyData		range_skey[2];
	RegProcedure	graph_eq;
	RegProcedure	graph_ge;
	RegProcedure	graph_le;

	/* Index-only scan state (when covering index available) */
	bool			use_ios = false;
	Oid				pk_index_oid;	/* saved PK OID for rerank phase */
	int				ios_att_sketch = 0;	/* index attr pos for sketch */
	int				ios_att_nbrs = 0;	/* index attr pos for neighbors */
	int				ios_att_src_tid = 0;/* index attr pos for src_tid */

	/* Search state */
	GraphEntry	   *candidates;		/* min-heap */
	int				cand_size = 0;
	int				cand_cap;
	GraphEntry	   *res_heap;		/* max-heap, bounded to ef_search */
	int				res_size = 0;
	bool		   *visited;
	int				visited_cap;
	int				n_visited = 0;
	int				n_explored = 0;
	int				sketch_dim = 0;
	int				n_point_probes = 0;
	int				n_range_runs = 0;
	int				n_range_nodes = 0;

	/* Timing */
	instr_time		t_start, t_search, t_rerank, t_out;

	int				j;

	/* ---- Setup ---- */
	if (sorted_heap_ann_timing)
		INSTR_TIME_SET_CURRENT(t_start);

	if (ef_search < 1) ef_search = 1;
	if (lim < 1) lim = 1;
	if (ef_search < lim) ef_search = lim;

	/* ---- Open graph table ---- */
	graph_oid = DatumGetObjectId(
		DirectFunctionCall1(regclassin,
							CStringGetDatum(graph_tbl_name)));
	graph_rel = table_open(graph_oid, AccessShareLock);
	graph_td = RelationGetDescr(graph_rel);
	graph_slot = table_slot_create(graph_rel, NULL);
	snapshot = GetActiveSnapshot();

	/* Resolve columns */
	g_att_sketch = get_attnum(graph_oid, "sketch");
	g_att_nbrs = get_attnum(graph_oid, "neighbors");
	g_att_src_id = get_attnum(graph_oid, "src_id");
	g_att_src_tid = get_attnum(graph_oid, "src_tid");

	if (g_att_sketch == InvalidAttrNumber ||
		g_att_nbrs == InvalidAttrNumber)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("svec_graph_scan: graph table \"%s\" must have "
						"\"sketch\" (hsvec) and \"neighbors\" (int4[]) "
						"columns", graph_tbl_name)));

	/* Find PK index */
	if (!graph_rel->rd_indexvalid)
		RelationGetIndexList(graph_rel);
	if (!OidIsValid(graph_rel->rd_pkindex))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("svec_graph_scan: graph table \"%s\" has no "
						"primary key", graph_tbl_name)));
	pk_index_oid = graph_rel->rd_pkindex;

	/* Validate first PK column is int4 */
	{
		Relation	pk_tmp = index_open(graph_rel->rd_pkindex,
										 AccessShareLock);
		AttrNumber	pk_att = pk_tmp->rd_index->indkey.values[0];
		Oid			pk_typid = TupleDescAttr(graph_td,
											   pk_att - 1)->atttypid;
		Oid			oc, of;

		g_att_nid = pk_att;

		if (pk_typid != INT4OID)
		{
			const char *pk_colname = NameStr(
				TupleDescAttr(graph_td, pk_att - 1)->attname);

			index_close(pk_tmp, AccessShareLock);
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("svec_graph_scan: first PK column \"%s\" of "
							"\"%s\" must be int4, got %s",
							pk_colname, graph_tbl_name,
							format_type_be(pk_typid))));
		}

		oc = GetDefaultOpClass(pk_typid, BTREE_AM_OID);
		of = get_opclass_family(oc);
		graph_eq = get_opcode(
			get_opfamily_member(of, pk_typid, pk_typid,
								BTEqualStrategyNumber));
		graph_ge = get_opcode(
			get_opfamily_member(of, pk_typid, pk_typid,
								BTGreaterEqualStrategyNumber));
		graph_le = get_opcode(
			get_opfamily_member(of, pk_typid, pk_typid,
								BTLessEqualStrategyNumber));

		/* Try to find covering index: (nid) INCLUDE (sketch, neighbors) */
		{
			Oid	cover_oid = find_covering_graph_index(
				graph_rel, pk_att, g_att_sketch, g_att_nbrs);

			if (OidIsValid(cover_oid))
			{
				index_close(pk_tmp, AccessShareLock);
				graph_pk = index_open(cover_oid, AccessShareLock);
				use_ios = true;

				/* Map table attnums to index attribute positions */
				{
					Form_pg_index ci = graph_pk->rd_index;
					int			i;

					for (i = 0; i < ci->indnatts; i++)
					{
						if (ci->indkey.values[i] == g_att_sketch)
							ios_att_sketch = i + 1; /* 1-based */
						else if (ci->indkey.values[i] == g_att_nbrs)
							ios_att_nbrs = i + 1;
						else if (g_att_src_tid != InvalidAttrNumber &&
								 ci->indkey.values[i] == g_att_src_tid)
							ios_att_src_tid = i + 1;
					}
				}
			}
			else
			{
				graph_pk = pk_tmp;	/* use PK index (regular scan) */
			}
		}
	}

	/* All validation passed — initialize SRF and search structures */
	InitMaterializedSRF(fcinfo, 0);

	cand_cap = Max(ef_search * 32, 4096);
	candidates = palloc(sizeof(GraphEntry) * cand_cap);
	res_heap = palloc(sizeof(GraphEntry) * (ef_search + 1));

	visited_cap = Max((int) graph_rel->rd_rel->reltuples + 1024, 65536);
	visited = palloc0(sizeof(bool) * visited_cap);

	/* ---- Begin index scans ---- */
#if PG_VERSION_NUM < 180000
	graph_iscan = index_beginscan(graph_rel, graph_pk, snapshot, 1, 0);
#else
	graph_iscan = index_beginscan(graph_rel, graph_pk, snapshot,
								  NULL, 1, 0);
#endif

	/* Range scan descriptor (2 keys: ge + le) */
#if PG_VERSION_NUM < 180000
	range_iscan = index_beginscan(graph_rel, graph_pk, snapshot, 2, 0);
#else
	range_iscan = index_beginscan(graph_rel, graph_pk, snapshot,
								   NULL, 2, 0);
#endif

	if (use_ios)
	{
		graph_iscan->xs_want_itup = true;
		range_iscan->xs_want_itup = true;
	}

	/* ---- Entry points ---- */
	if (entry_tbl_name[0] != '\0')
	{
		/* Multi-entry: read entry table, pick closest centroids */
		Oid			entry_oid;
		Relation	entry_rel;
		TableScanDesc entry_scan;
		TupleTableSlot *entry_slot;
		AttrNumber	e_att_nid, e_att_centroid;
		int			n_entries = 0;
		int			max_seeds = 8;	/* use top-8 closest entry points */
		GraphEntry *entry_buf;
		int			entry_buf_cap = 256;

		entry_oid = DatumGetObjectId(
			DirectFunctionCall1(regclassin,
								CStringGetDatum(entry_tbl_name)));
		entry_rel = table_open(entry_oid, AccessShareLock);
		entry_slot = table_slot_create(entry_rel, NULL);

		e_att_nid = get_attnum(entry_oid, "entry_nid");
		e_att_centroid = get_attnum(entry_oid, "centroid");

		if (e_att_nid == InvalidAttrNumber ||
			e_att_centroid == InvalidAttrNumber)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("svec_graph_scan: entry table \"%s\" must have "
							"\"entry_nid\" (int4) and \"centroid\" (hsvec) "
							"columns", entry_tbl_name)));

		entry_buf = palloc(sizeof(GraphEntry) * entry_buf_cap);

		entry_scan = table_beginscan(entry_rel, snapshot, 0, NULL);

		while (table_scan_getnextslot(entry_scan, ForwardScanDirection,
									   entry_slot))
		{
			bool	isnull;
			Datum	nid_d, cent_d;
			int32	e_nid;
			Hsvec  *cent;
			float8	d;

			nid_d = slot_getattr(entry_slot, e_att_nid, &isnull);
			if (isnull)
			{
				ExecClearTuple(entry_slot);
				continue;
			}
			e_nid = DatumGetInt32(nid_d);

			cent_d = slot_getattr(entry_slot, e_att_centroid, &isnull);
			if (isnull)
			{
				ExecClearTuple(entry_slot);
				continue;
			}

			cent = (Hsvec *) PG_DETOAST_DATUM(cent_d);

			if (sketch_dim == 0)
			{
				if (cent->dim > query->dim)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_EXCEPTION),
							 errmsg("svec_graph_scan: centroid dim %d > "
									"query dim %d", cent->dim, query->dim)));
				sketch_dim = cent->dim;
			}

			d = cosine_distance_f32_f16(query->x, cent->x,
										 Min(cent->dim, query->dim));

			if (n_entries >= entry_buf_cap)
			{
				entry_buf_cap *= 2;
				entry_buf = repalloc(entry_buf,
									  sizeof(GraphEntry) * entry_buf_cap);
			}
			entry_buf[n_entries].dist = d;
			entry_buf[n_entries].nid = e_nid;
			n_entries++;

			ExecClearTuple(entry_slot);
		}

		table_endscan(entry_scan);
		ExecDropSingleTupleTableSlot(entry_slot);
		table_close(entry_rel, AccessShareLock);

		if (n_entries == 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("svec_graph_scan: entry table \"%s\" is empty",
							entry_tbl_name)));

		/* Sort by distance, seed from top max_seeds */
		qsort(entry_buf, n_entries, sizeof(GraphEntry), cmp_graph_entry);
		{
			int		n_seeds = Min(max_seeds, n_entries);
			int		i;

			for (i = 0; i < n_seeds; i++)
			{
				int32	e_nid = entry_buf[i].nid;
				float8	e_dist = entry_buf[i].dist;

				/* Mark visited */
				if (e_nid >= 0)
				{
					if (e_nid >= visited_cap)
					{
						int new_cap = Max(visited_cap * 2, e_nid + 1024);

						visited = repalloc(visited,
											sizeof(bool) * new_cap);
						memset(visited + visited_cap, 0,
							   sizeof(bool) * (new_cap - visited_cap));
						visited_cap = new_cap;
					}
					if (visited[e_nid])
						continue;
					visited[e_nid] = true;
					n_visited++;
				}

				/* Add to candidates min-heap */
				if (cand_size >= cand_cap)
				{
					cand_cap *= 2;
					candidates = repalloc(candidates,
										   sizeof(GraphEntry) * cand_cap);
				}
				candidates[cand_size].dist = e_dist;
				candidates[cand_size].nid = e_nid;
				cand_size++;
				graph_minheap_siftup(candidates, cand_size - 1);

				/* Add to results max-heap */
				if (res_size < ef_search)
				{
					res_heap[res_size].dist = e_dist;
					res_heap[res_size].nid = e_nid;
					res_size++;
					graph_maxheap_siftup(res_heap, res_size - 1);
				}
				else if (e_dist < res_heap[0].dist)
				{
					res_heap[0].dist = e_dist;
					res_heap[0].nid = e_nid;
					graph_maxheap_siftdown(res_heap, res_size, 0);
				}
			}
		}

		pfree(entry_buf);

		/* Need sketch_dim for navigation — read it from first graph node
		 * if centroids didn't set it (shouldn't happen, but be safe). */
		if (sketch_dim == 0)
			sketch_dim = query->dim;
	}
	else
	{
		/* Single entry point: nid=0 (original behavior) */
		int32		entry_nid = 0;
		bool		isnull;
		Datum		sk_d;
		Hsvec	   *sk;
		float8		d;

		ScanKeyInit(&graph_skey[0], 1, BTEqualStrategyNumber,
					graph_eq, Int32GetDatum(entry_nid));
		index_rescan(graph_iscan, graph_skey, 1, NULL, 0);

		if (use_ios)
		{
			ItemPointer tid;

			tid = index_getnext_tid(graph_iscan, ForwardScanDirection);
			if (tid == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_EXCEPTION),
						 errmsg("svec_graph_scan: entry point nid=0 not "
								"found in \"%s\"", graph_tbl_name)));

			sk_d = index_getattr(graph_iscan->xs_itup,
								  ios_att_sketch,
								  graph_iscan->xs_itupdesc,
								  &isnull);
		}
		else
		{
			if (!index_getnext_slot(graph_iscan, ForwardScanDirection,
									graph_slot))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_EXCEPTION),
						 errmsg("svec_graph_scan: entry point nid=0 not "
								"found in \"%s\"", graph_tbl_name)));

			sk_d = slot_getattr(graph_slot, g_att_sketch, &isnull);
		}

		if (isnull)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("svec_graph_scan: null sketch at nid=0")));

		sk = (Hsvec *) PG_DETOAST_DATUM(sk_d);
		if (sk->dim > query->dim)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("svec_graph_scan: sketch dim %d > query "
							"dim %d", sk->dim, query->dim)));

		sketch_dim = sk->dim;
		d = cosine_distance_f32_f16(query->x, sk->x, sketch_dim);
		if (!use_ios)
			ExecClearTuple(graph_slot);

		if (entry_nid < visited_cap)
			visited[entry_nid] = true;
		n_visited++;

		candidates[0].dist = d;
		candidates[0].nid = entry_nid;
		cand_size = 1;

		res_heap[0].dist = d;
		res_heap[0].nid = entry_nid;
		res_size = 1;
	}

	/* ---- NSW greedy search ---- */
	while (cand_size > 0)
	{
		float8		c_dist;
		int32		c_nid;
		float8		f_dist;
		bool		isnull;
		Datum		nbrs_d;
		ArrayType  *nbrs_arr;
		int32	   *nbrs;
		int			n_nbrs;
		int32	   *nbrs_copy;
		int			k;

		/* Pop closest candidate (min-heap root) */
		c_dist = candidates[0].dist;
		c_nid = candidates[0].nid;
		cand_size--;
		if (cand_size > 0)
		{
			candidates[0] = candidates[cand_size];
			graph_minheap_siftdown(candidates, cand_size, 0);
		}

		/* Convergence check */
		f_dist = res_heap[0].dist;	/* max-heap root = furthest */
		if (res_size >= ef_search && c_dist > f_dist)
			break;

		n_explored++;

		/* Read explored node's neighbors */
		ScanKeyInit(&graph_skey[0], 1, BTEqualStrategyNumber,
					graph_eq, Int32GetDatum(c_nid));
		index_rescan(graph_iscan, graph_skey, 1, NULL, 0);

		if (use_ios)
		{
			ItemPointer tid = index_getnext_tid(graph_iscan,
												 ForwardScanDirection);
			if (tid == NULL)
				continue;

			nbrs_d = index_getattr(graph_iscan->xs_itup,
									ios_att_nbrs,
									graph_iscan->xs_itupdesc,
									&isnull);
		}
		else
		{
			if (!index_getnext_slot(graph_iscan, ForwardScanDirection,
									graph_slot))
				continue;

			nbrs_d = slot_getattr(graph_slot, g_att_nbrs, &isnull);
		}

		if (isnull)
		{
			if (!use_ios) ExecClearTuple(graph_slot);
			continue;
		}

		nbrs_arr = DatumGetArrayTypeP(nbrs_d);
		nbrs = (int32 *) ARR_DATA_PTR(nbrs_arr);
		n_nbrs = ArrayGetNItems(ARR_NDIM(nbrs_arr), ARR_DIMS(nbrs_arr));

		/* Copy neighbors before clearing tuple */
		nbrs_copy = palloc(sizeof(int32) * n_nbrs);
		memcpy(nbrs_copy, nbrs, sizeof(int32) * n_nbrs);
		if (!use_ios) ExecClearTuple(graph_slot);

		/*
		 * Batch neighbor reads: neighbor lists are sorted by nid
		 * (from builder relabel).  Filter visited, detect contiguous
		 * runs, use range scan for runs >= GRAPH_BATCH_MIN.
		 */
		{
			int32	   *unvisited;
			int			n_unvisited = 0;
			NidRange   *runs;
			int			n_runs = 0;
			int32	   *singles;
			int			n_singles = 0;
			int			r, s;

			unvisited = palloc(sizeof(int32) * n_nbrs);

			/* Filter out visited and negative nids */
			for (k = 0; k < n_nbrs; k++)
			{
				int32	n_nid = nbrs_copy[k];

				if (n_nid < 0)
					continue;

				/* Expand visited bitmap if needed */
				if (n_nid >= visited_cap)
				{
					int	new_cap = Max(visited_cap * 2, n_nid + 1024);

					visited = repalloc(visited, sizeof(bool) * new_cap);
					memset(visited + visited_cap, 0,
						   sizeof(bool) * (new_cap - visited_cap));
					visited_cap = new_cap;
				}

				if (visited[n_nid])
					continue;

				unvisited[n_unvisited++] = n_nid;
			}

			/* Detect contiguous runs in sorted unvisited list */
			runs = palloc(sizeof(NidRange) * (n_unvisited + 1));
			singles = palloc(sizeof(int32) * n_unvisited);

			if (n_unvisited > 0)
			{
				int32	run_lo = unvisited[0];
				int32	run_hi = unvisited[0];

				for (k = 1; k < n_unvisited; k++)
				{
					if (unvisited[k] == run_hi + 1)
					{
						run_hi = unvisited[k];
					}
					else
					{
						/* Emit previous run */
						if (run_hi - run_lo + 1 >= GRAPH_BATCH_MIN)
						{
							runs[n_runs].lo = run_lo;
							runs[n_runs].hi = run_hi;
							n_runs++;
						}
						else
						{
							int32 i;
							for (i = run_lo; i <= run_hi; i++)
								singles[n_singles++] = i;
						}
						run_lo = unvisited[k];
						run_hi = unvisited[k];
					}
				}
				/* Emit last run */
				if (run_hi - run_lo + 1 >= GRAPH_BATCH_MIN)
				{
					runs[n_runs].lo = run_lo;
					runs[n_runs].hi = run_hi;
					n_runs++;
				}
				else
				{
					int32 i;
					for (i = run_lo; i <= run_hi; i++)
						singles[n_singles++] = i;
				}
			}

			/* Mark all unvisited as visited now */
			for (k = 0; k < n_unvisited; k++)
			{
				visited[unvisited[k]] = true;
				n_visited++;
			}

			/*
			 * Process contiguous runs via range scan:
			 *   nid >= run_lo AND nid <= run_hi
			 */
			for (r = 0; r < n_runs; r++)
			{
				n_range_runs++;

				ScanKeyInit(&range_skey[0], 1,
							BTGreaterEqualStrategyNumber,
							graph_ge, Int32GetDatum(runs[r].lo));
				ScanKeyInit(&range_skey[1], 1,
							BTLessEqualStrategyNumber,
							graph_le, Int32GetDatum(runs[r].hi));
				index_rescan(range_iscan, range_skey, 2, NULL, 0);

				for (;;)
				{
					Datum	sk_d;
					Hsvec  *sk;
					float8	n_dist;
					int32	n_nid;

					if (use_ios)
					{
						ItemPointer tid = index_getnext_tid(
							range_iscan, ForwardScanDirection);
						if (tid == NULL)
							break;

						/* Extract nid from index key attr 1 */
						{
							bool nid_null;
							Datum nid_d = index_getattr(
								range_iscan->xs_itup, 1,
								range_iscan->xs_itupdesc,
								&nid_null);
							n_nid = DatumGetInt32(nid_d);
						}

						sk_d = index_getattr(range_iscan->xs_itup,
											  ios_att_sketch,
											  range_iscan->xs_itupdesc,
											  &isnull);
					}
					else
					{
						if (!index_getnext_slot(range_iscan,
												ForwardScanDirection,
												graph_slot))
							break;

						{
							bool nid_null;
							Datum nid_d = slot_getattr(
								graph_slot, g_att_nid, &nid_null);
							n_nid = DatumGetInt32(nid_d);
						}

						sk_d = slot_getattr(graph_slot, g_att_sketch,
											&isnull);
					}

					n_range_nodes++;

					if (isnull)
					{
						if (!use_ios) ExecClearTuple(graph_slot);
						continue;
					}

					sk = (Hsvec *) PG_DETOAST_DATUM(sk_d);
					n_dist = cosine_distance_f32_f16(
						query->x, sk->x,
						Min(sk->dim, query->dim));
					if (!use_ios) ExecClearTuple(graph_slot);

					f_dist = (res_size >= ef_search)
						? res_heap[0].dist : DBL_MAX;

					if (n_dist < f_dist || res_size < ef_search)
					{
						if (cand_size >= cand_cap)
						{
							cand_cap *= 2;
							candidates = repalloc(candidates,
								sizeof(GraphEntry) * cand_cap);
						}
						candidates[cand_size].dist = n_dist;
						candidates[cand_size].nid = n_nid;
						cand_size++;
						graph_minheap_siftup(candidates,
											  cand_size - 1);

						if (res_size < ef_search)
						{
							res_heap[res_size].dist = n_dist;
							res_heap[res_size].nid = n_nid;
							res_size++;
							graph_maxheap_siftup(res_heap,
												  res_size - 1);
						}
						else if (n_dist < res_heap[0].dist)
						{
							res_heap[0].dist = n_dist;
							res_heap[0].nid = n_nid;
							graph_maxheap_siftdown(res_heap,
												   res_size, 0);
						}
					}
				}
			}

			/* Process singles via point probes */
			for (s = 0; s < n_singles; s++)
			{
				int32	n_nid = singles[s];
				Datum	sk_d;
				Hsvec  *sk;
				float8	n_dist;

				n_point_probes++;

				ScanKeyInit(&graph_skey[0], 1, BTEqualStrategyNumber,
							graph_eq, Int32GetDatum(n_nid));
				index_rescan(graph_iscan, graph_skey, 1, NULL, 0);

				if (use_ios)
				{
					ItemPointer tid = index_getnext_tid(
						graph_iscan, ForwardScanDirection);
					if (tid == NULL)
						continue;

					sk_d = index_getattr(graph_iscan->xs_itup,
										  ios_att_sketch,
										  graph_iscan->xs_itupdesc,
										  &isnull);
				}
				else
				{
					if (!index_getnext_slot(graph_iscan,
											ForwardScanDirection,
											graph_slot))
						continue;

					sk_d = slot_getattr(graph_slot, g_att_sketch,
										&isnull);
				}

				if (isnull)
				{
					if (!use_ios) ExecClearTuple(graph_slot);
					continue;
				}

				sk = (Hsvec *) PG_DETOAST_DATUM(sk_d);
				n_dist = cosine_distance_f32_f16(
					query->x, sk->x,
					Min(sk->dim, query->dim));
				if (!use_ios) ExecClearTuple(graph_slot);

				f_dist = (res_size >= ef_search)
					? res_heap[0].dist : DBL_MAX;

				if (n_dist < f_dist || res_size < ef_search)
				{
					if (cand_size >= cand_cap)
					{
						cand_cap *= 2;
						candidates = repalloc(candidates,
							sizeof(GraphEntry) * cand_cap);
					}
					candidates[cand_size].dist = n_dist;
					candidates[cand_size].nid = n_nid;
					cand_size++;
					graph_minheap_siftup(candidates,
										  cand_size - 1);

					if (res_size < ef_search)
					{
						res_heap[res_size].dist = n_dist;
						res_heap[res_size].nid = n_nid;
						res_size++;
						graph_maxheap_siftup(res_heap,
											  res_size - 1);
					}
					else if (n_dist < res_heap[0].dist)
					{
						res_heap[0].dist = n_dist;
						res_heap[0].nid = n_nid;
						graph_maxheap_siftdown(res_heap,
											   res_size, 0);
					}
				}
			}

			pfree(unvisited);
			pfree(runs);
			pfree(singles);
		}
		pfree(nbrs_copy);
	}

	index_endscan(graph_iscan);
	index_endscan(range_iscan);

	if (sorted_heap_ann_timing)
		INSTR_TIME_SET_CURRENT(t_search);

	/* ---- Rerank phase ---- */
	{
		GraphResult	   *gresults;
		bool			do_exact_rerank;
		int				n_out;

		gresults = palloc(sizeof(GraphResult) * res_size);
		for (j = 0; j < res_size; j++)
		{
			gresults[j].nid = res_heap[j].nid;
			gresults[j].distance = res_heap[j].dist;
			gresults[j].src_id = NULL;
			ItemPointerSetInvalid(&gresults[j].src_tid);
		}

		do_exact_rerank = (g_att_src_tid != InvalidAttrNumber);

		/* Pre-filter: narrow candidates by sketch distance before
		 * expensive exact rerank.  rerank_topk=0 means use all. */
		if (do_exact_rerank && rerank_topk > 0 && rerank_topk < res_size)
		{
			int effective = Max(rerank_topk, lim);

			qsort(gresults, res_size, sizeof(GraphResult),
				  cmp_graph_result);
			res_size = Min(effective, res_size);
		}

		/* Read src_tid for all candidates (needed for exact rerank).
		 * Defer src_id to after sort — only needed for final top-lim. */
		if (do_exact_rerank)
		{
			if (use_ios && ios_att_src_tid != 0)
			{
				/* IOS path: covering index includes src_tid */
#if PG_VERSION_NUM < 180000
				graph_iscan = index_beginscan(graph_rel, graph_pk,
											   snapshot, 1, 0);
#else
				graph_iscan = index_beginscan(graph_rel, graph_pk,
											   snapshot, NULL, 1, 0);
#endif
				graph_iscan->xs_want_itup = true;

				for (j = 0; j < res_size; j++)
				{
					bool		isnull;
					ItemPointer tid;

					ScanKeyInit(&graph_skey[0], 1, BTEqualStrategyNumber,
								graph_eq,
								Int32GetDatum(gresults[j].nid));
					index_rescan(graph_iscan, graph_skey, 1, NULL, 0);

					tid = index_getnext_tid(graph_iscan,
											ForwardScanDirection);
					if (tid == NULL)
						continue;

					{
						Datum	tid_d = index_getattr(
							graph_iscan->xs_itup, ios_att_src_tid,
							graph_iscan->xs_itupdesc, &isnull);

						if (!isnull)
							ItemPointerCopy(DatumGetItemPointer(tid_d),
											&gresults[j].src_tid);
					}
				}

				index_endscan(graph_iscan);
			}
			else
			{
				/* Heap path: PK index → heap for src_tid only */
				Relation	rerank_idx;
				bool		rerank_opened_pk = false;

				if (use_ios)
				{
					rerank_idx = index_open(pk_index_oid, AccessShareLock);
					rerank_opened_pk = true;
				}
				else
					rerank_idx = graph_pk;

#if PG_VERSION_NUM < 180000
				graph_iscan = index_beginscan(graph_rel, rerank_idx,
											   snapshot, 1, 0);
#else
				graph_iscan = index_beginscan(graph_rel, rerank_idx,
											   snapshot, NULL, 1, 0);
#endif

				for (j = 0; j < res_size; j++)
				{
					bool	isnull;

					ScanKeyInit(&graph_skey[0], 1, BTEqualStrategyNumber,
								graph_eq,
								Int32GetDatum(gresults[j].nid));
					index_rescan(graph_iscan, graph_skey, 1, NULL, 0);

					if (!index_getnext_slot(graph_iscan,
											ForwardScanDirection,
											graph_slot))
						continue;

					{
						Datum	tid_d = slot_getattr(graph_slot,
													 g_att_src_tid,
													 &isnull);

						if (!isnull)
							ItemPointerCopy(DatumGetItemPointer(tid_d),
											&gresults[j].src_tid);
					}

					ExecClearTuple(graph_slot);
				}

				index_endscan(graph_iscan);

				if (rerank_opened_pk)
					index_close(rerank_idx, AccessShareLock);
			}
		}

		/* Exact rerank via main table */
		if (do_exact_rerank)
		{
			Relation		main_rel;
			TupleTableSlot *main_slot;
			TupleDesc		main_td;
			AttrNumber		emb_attno;
			AttrNumber		id_attno;
			Oid				id_typid, typoutput;
			bool			typisvarlena;

			main_rel = table_open(tbl_oid, AccessShareLock);
			main_td = RelationGetDescr(main_rel);
			main_slot = table_slot_create(main_rel, NULL);

			emb_attno = get_attnum(tbl_oid, "embedding");
			id_attno = get_attnum(tbl_oid, "id");
			id_typid = TupleDescAttr(main_td,
									  id_attno - 1)->atttypid;
			getTypeOutputInfo(id_typid, &typoutput, &typisvarlena);

			/* Sort by block for sequential I/O */
			qsort(gresults, res_size, sizeof(GraphResult),
				  cmp_graph_result_by_blk);

			/* Prefetch initial batch */
			for (j = 0; j < Min(RERANK_PREFETCH_DISTANCE, res_size); j++)
			{
				if (ItemPointerIsValid(&gresults[j].src_tid))
					PrefetchBuffer(main_rel, MAIN_FORKNUM,
								   ItemPointerGetBlockNumber(
									   &gresults[j].src_tid));
			}

			for (j = 0; j < res_size; j++)
			{
				bool	isnull;
				Datum	emb_d;
				Svec   *candidate;

				if (!ItemPointerIsValid(&gresults[j].src_tid))
				{
					gresults[j].distance = DBL_MAX;
					continue;
				}

				/* Prefetch ahead */
				if (j + RERANK_PREFETCH_DISTANCE < res_size &&
					ItemPointerIsValid(
						&gresults[j + RERANK_PREFETCH_DISTANCE].src_tid))
					PrefetchBuffer(main_rel, MAIN_FORKNUM,
								   ItemPointerGetBlockNumber(
									   &gresults[j + RERANK_PREFETCH_DISTANCE].src_tid));

				if (!table_tuple_fetch_row_version(main_rel,
												   &gresults[j].src_tid,
												   snapshot, main_slot))
				{
					gresults[j].distance = DBL_MAX;
					continue;
				}

				emb_d = slot_getattr(main_slot, emb_attno, &isnull);
				if (!isnull)
				{
					candidate = DatumGetSvecP(emb_d);
					if (candidate->dim != query->dim)
						ereport(ERROR,
								(errcode(ERRCODE_DATA_EXCEPTION),
								 errmsg("svec_graph_scan: embedding dim "
										"%d != query dim %d",
										candidate->dim, query->dim)));
					gresults[j].distance =
						svec_cosine_distance_internal(query, candidate);
				}

				/* Read id from main table */
				{
					Datum	mid = slot_getattr(main_slot, id_attno,
											   &isnull);

					if (!isnull)
						gresults[j].src_id = pstrdup(
							OidOutputFunctionCall(typoutput, mid));
				}

				ExecClearTuple(main_slot);
			}

			ExecDropSingleTupleTableSlot(main_slot);
			table_close(main_rel, AccessShareLock);
		}

		if (sorted_heap_ann_timing)
			INSTR_TIME_SET_CURRENT(t_rerank);

		/* Sort by distance, keep top lim */
		qsort(gresults, res_size, sizeof(GraphResult), cmp_graph_result);
		n_out = Min(res_size, lim);

		/* Deferred src_id: when no exact rerank, read from graph table
		 * only for the final top-lim results. */
		if (!do_exact_rerank && g_att_src_id != InvalidAttrNumber)
		{
			Relation	srcid_idx;
			bool		srcid_opened_pk = false;
			Oid			g_typid;
			Oid			g_typout;
			bool		g_typisvar;

			g_typid = TupleDescAttr(graph_td,
									g_att_src_id - 1)->atttypid;
			getTypeOutputInfo(g_typid, &g_typout, &g_typisvar);

			if (use_ios)
			{
				srcid_idx = index_open(pk_index_oid, AccessShareLock);
				srcid_opened_pk = true;
			}
			else
				srcid_idx = graph_pk;

#if PG_VERSION_NUM < 180000
			graph_iscan = index_beginscan(graph_rel, srcid_idx,
										   snapshot, 1, 0);
#else
			graph_iscan = index_beginscan(graph_rel, srcid_idx,
										   snapshot, NULL, 1, 0);
#endif

			for (j = 0; j < n_out; j++)
			{
				bool	isnull;

				ScanKeyInit(&graph_skey[0], 1, BTEqualStrategyNumber,
							graph_eq, Int32GetDatum(gresults[j].nid));
				index_rescan(graph_iscan, graph_skey, 1, NULL, 0);

				if (!index_getnext_slot(graph_iscan, ForwardScanDirection,
										graph_slot))
					continue;

				{
					Datum	id_d = slot_getattr(graph_slot, g_att_src_id,
												 &isnull);

					if (!isnull)
						gresults[j].src_id = pstrdup(
							OidOutputFunctionCall(g_typout, id_d));
				}

				ExecClearTuple(graph_slot);
			}

			index_endscan(graph_iscan);

			if (srcid_opened_pk)
				index_close(srcid_idx, AccessShareLock);
		}

		/* ---- Output to tuplestore ---- */
		for (j = 0; j < n_out; j++)
		{
			Datum	values[2];
			bool	nulls[2] = {false, false};

			values[0] = CStringGetTextDatum(
				gresults[j].src_id ? gresults[j].src_id : "");
			values[1] = Float8GetDatum(gresults[j].distance);
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
								values, nulls);
		}

		pfree(gresults);
	}

	/* ---- Cleanup ---- */
	ExecDropSingleTupleTableSlot(graph_slot);
	index_close(graph_pk, AccessShareLock);
	table_close(graph_rel, AccessShareLock);

	pfree(candidates);
	pfree(res_heap);
	pfree(visited);

	if (sorted_heap_ann_timing)
	{
		INSTR_TIME_SET_CURRENT(t_out);
		elog(DEBUG1, "svec_graph_scan: search=%.3fms rerank=%.3fms "
			 "out=%.3fms total=%.3fms "
			 "(visited=%d explored=%d results=%d sketch_dim=%d "
			 "point_probes=%d range_runs=%d range_nodes=%d)",
			 INSTR_TIME_GET_MILLISEC(t_search) -
			 INSTR_TIME_GET_MILLISEC(t_start),
			 INSTR_TIME_GET_MILLISEC(t_rerank) -
			 INSTR_TIME_GET_MILLISEC(t_search),
			 INSTR_TIME_GET_MILLISEC(t_out) -
			 INSTR_TIME_GET_MILLISEC(t_rerank),
			 INSTR_TIME_GET_MILLISEC(t_out) -
			 INSTR_TIME_GET_MILLISEC(t_start),
			 n_visited, n_explored, res_size, sketch_dim,
			 n_point_probes, n_range_runs, n_range_nodes);
	}

	return (Datum) 0;
}

/* ================================================================
 *  svec_hnsw_scan: hierarchical HNSW search via PG sidecar tables
 *
 *  svec_hnsw_scan(tbl regclass, query svec, prefix text,
 *                 ef_search int4 DEFAULT 64,
 *                 lim int4 DEFAULT 10,
 *                 rerank_topk int4 DEFAULT 0)
 *  RETURNS TABLE(id text, distance float8)
 *
 *  Top-down HNSW search on sidecar tables built by build_hnsw_graph.py:
 *    {prefix}_meta   (entry_nid int4, max_level int2)
 *    {prefix}_l0     (nid int4 PK, sketch hsvec, neighbors int4[],
 *                    src_id text, src_tid tid)
 *    {prefix}_l1..lN (nid int4 PK, sketch hsvec, neighbors int4[])
 *
 *  Algorithm:
 *    1. Read entry_nid, max_level from {prefix}_meta
 *    2. For l = max_level..1: greedy ef=1 descent
 *    3. At L0: beam search with ef_search candidates
 *    4. Exact rerank via main table (same pattern as svec_graph_scan)
 * ================================================================ */

/* One open HNSW level table + its covering index + reusable scan state */
typedef struct HnswLevelTag
{
	Relation		rel;
	Relation		idx;		/* covering (IOS) or PK */
	bool			use_ios;
	TupleTableSlot *slot;
	Snapshot		snap;
	Oid				pk_oid;		/* original PK oid (for rerank after IOS) */
	AttrNumber		att_nid;
	AttrNumber		att_sketch;
	AttrNumber		att_nbrs;
	AttrNumber		att_src_id;		/* InvalidAttrNumber for L1+ */
	AttrNumber		att_src_tid;	/* InvalidAttrNumber for L1+ */
	/* 1-based index attribute positions (0 = not present) */
	int				ios_att_sketch;
	int				ios_att_nbrs;
	int				ios_att_src_tid;
	RegProcedure	eq_op;
	IndexScanDesc	iscan;
	ScanKeyData		skey[1];
} HnswLevel;

/*
 * Like find_covering_graph_index but accepts non-unique indexes.
 * Needed because build_hnsw_graph.py creates non-unique covering indexes.
 */
static Oid
find_covering_hnsw_index(Relation rel, AttrNumber pk_att,
						 AttrNumber att_sketch, AttrNumber att_nbrs)
{
	List	   *indexlist;
	ListCell   *lc;

	indexlist = RelationGetIndexList(rel);

	foreach(lc, indexlist)
	{
		Oid			idx_oid = lfirst_oid(lc);
		Relation	idx;
		Form_pg_index idx_form;
		int			nkeyatts, natts;

		idx      = index_open(idx_oid, AccessShareLock);
		idx_form = idx->rd_index;

		/* Must be btree, valid, ready */
		if (idx->rd_rel->relam != BTREE_AM_OID ||
			!idx_form->indisvalid ||
			!idx_form->indisready)
		{
			index_close(idx, AccessShareLock);
			continue;
		}

		nkeyatts = idx_form->indnkeyatts;
		natts    = idx_form->indnatts;

		/* 1 key column = pk_att, + at least 2 INCLUDE columns */
		if (nkeyatts != 1 || natts < 3 ||
			idx_form->indkey.values[0] != pk_att)
		{
			index_close(idx, AccessShareLock);
			continue;
		}

		{
			bool	has_sketch = false;
			bool	has_nbrs   = false;
			int		i;

			for (i = nkeyatts; i < natts; i++)
			{
				AttrNumber a = idx_form->indkey.values[i];

				if (a == att_sketch)
					has_sketch = true;
				else if (a == att_nbrs)
					has_nbrs = true;
			}

			if (has_sketch && has_nbrs)
			{
				Oid result = idx_oid;

				index_close(idx, AccessShareLock);
				list_free(indexlist);
				return result;
			}
		}

		index_close(idx, AccessShareLock);
	}

	list_free(indexlist);
	return InvalidOid;
}

static void
hnsw_open_level(HnswLevel *h, const char *tblname, Snapshot snap)
{
	Oid			tbl_oid;
	TupleDesc	td;
	AttrNumber	pk_att;
	Oid			pk_typid, oc, of;
	Relation	pk_tmp;
	Oid			cover_oid;

	tbl_oid = DatumGetObjectId(
		DirectFunctionCall1(regclassin, CStringGetDatum(tblname)));

	h->rel    = table_open(tbl_oid, AccessShareLock);
	td        = RelationGetDescr(h->rel);
	h->slot   = table_slot_create(h->rel, NULL);
	h->snap   = snap;
	h->use_ios         = false;
	h->ios_att_sketch  = 0;
	h->ios_att_nbrs    = 0;
	h->ios_att_src_tid = 0;

	h->att_nid    = get_attnum(tbl_oid, "nid");
	h->att_sketch = get_attnum(tbl_oid, "sketch");
	h->att_nbrs   = get_attnum(tbl_oid, "neighbors");
	h->att_src_id  = get_attnum(tbl_oid, "src_id");
	h->att_src_tid = get_attnum(tbl_oid, "src_tid");

	if (h->att_sketch == InvalidAttrNumber ||
		h->att_nbrs == InvalidAttrNumber)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("svec_hnsw_scan: table \"%s\" must have "
						"\"sketch\" (hsvec) and \"neighbors\" (int4[]) "
						"columns", tblname)));

	if (!h->rel->rd_indexvalid)
		RelationGetIndexList(h->rel);
	h->pk_oid = h->rel->rd_pkindex;
	if (!OidIsValid(h->pk_oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("svec_hnsw_scan: table \"%s\" has no primary key",
						tblname)));

	pk_tmp   = index_open(h->pk_oid, AccessShareLock);
	pk_att   = pk_tmp->rd_index->indkey.values[0];
	pk_typid = TupleDescAttr(td, pk_att - 1)->atttypid;
	oc = GetDefaultOpClass(pk_typid, BTREE_AM_OID);
	of = get_opclass_family(oc);
	h->eq_op = get_opcode(
		get_opfamily_member(of, pk_typid, pk_typid,
							BTEqualStrategyNumber));

	cover_oid = find_covering_hnsw_index(h->rel, pk_att,
										 h->att_sketch, h->att_nbrs);
	if (OidIsValid(cover_oid))
	{
		Form_pg_index	ci;
		int				i;

		index_close(pk_tmp, AccessShareLock);
		h->idx     = index_open(cover_oid, AccessShareLock);
		h->use_ios = true;
		ci = h->idx->rd_index;
		for (i = 0; i < ci->indnatts; i++)
		{
			AttrNumber a = ci->indkey.values[i];

			if (a == h->att_sketch)
				h->ios_att_sketch = i + 1;
			else if (a == h->att_nbrs)
				h->ios_att_nbrs = i + 1;
			else if (h->att_src_tid != InvalidAttrNumber &&
					 a == h->att_src_tid)
				h->ios_att_src_tid = i + 1;
		}
	}
	else
		h->idx = pk_tmp;

#if PG_VERSION_NUM < 180000
	h->iscan = index_beginscan(h->rel, h->idx, snap, 1, 0);
#else
	h->iscan = index_beginscan(h->rel, h->idx, snap, NULL, 1, 0);
#endif
	if (h->use_ios)
		h->iscan->xs_want_itup = true;
}

static void
hnsw_close_level(HnswLevel *h)
{
	index_endscan(h->iscan);
	ExecDropSingleTupleTableSlot(h->slot);
	index_close(h->idx, AccessShareLock);
	table_close(h->rel, AccessShareLock);
}

/* ---- Visited bitset helpers (1 bit per nid, growable) ---- */

static inline bool
visited_test(const uint64 *bits, int nwords, int32 nid)
{
	int w = nid / 64;

	if (nid < 0 || w >= nwords)
		return false;
	return (bits[w] & ((uint64) 1 << (nid % 64))) != 0;
}

static inline void
visited_set(uint64 **bits_p, int *nwords_p, int32 nid)
{
	int w = nid / 64;

	if (w >= *nwords_p)
	{
		int new_nwords = Max(*nwords_p * 2, w + 64);

		*bits_p = repalloc(*bits_p, sizeof(uint64) * new_nwords);
		memset(*bits_p + *nwords_p, 0,
			   sizeof(uint64) * (new_nwords - *nwords_p));
		*nwords_p = new_nwords;
	}
	(*bits_p)[w] |= ((uint64) 1 << (nid % 64));
}

/*
 * Compute cosine distance between query and a cached L0/upper-level node.
 * Dispatches to SQ8, f32, or f16 based on cache vector type.
 */
static inline float8
hnsw_cache_distance(const HnswL0Cache *cache, int32 nid,
					const float *qx, int qdim, double qnorm)
{
	const char *vec = cache->vec_data + (Size) nid * cache->vec_stride;
	int			dim = Min(cache->vec_dim, qdim);

	if (cache->vec_is_sq8)
		return cosine_distance_f32_sq8_prenorm(qx, (const uint8 *) vec,
											   cache->sq8_mins,
											   cache->sq8_scales,
											   dim, qnorm);
	else if (cache->vec_is_f32)
		return cosine_distance_f32_f32_prenorm(qx, (const float *) vec,
											   dim, qnorm);
	else
		return cosine_distance_f32_f16_prenorm(qx, (const half *) vec,
											   dim, qnorm);
}

/*
 * Cache-only beam search (no HnswLevel / no catalog access).
 *
 * Used for upper-level greedy descent when cache is already built.
 * Avoids hnsw_open_level / hnsw_close_level overhead entirely.
 */
static int
hnsw_search_cached(const HnswL0Cache *cache,
				   const float *qx, int qdim,
				   int32 *entry_nids, int n_entries, int ef,
				   GraphEntry *res_out,
				   int *n_visited_out,
				   double qnorm)
{
	GraphEntry *candidates;
	int			cand_size = 0;
	int			cand_cap;
	int			res_size  = 0;
	uint64	   *visited_bits;
	int			visited_nwords;
	int			n_visited = 0;
	int			i;

	cand_cap       = Max(ef * 32, 4096);
	candidates     = palloc(sizeof(GraphEntry) * cand_cap);
	visited_nwords = cache->max_nid / 64 + 2;
	visited_bits   = palloc0(sizeof(uint64) * visited_nwords);

	for (i = 0; i < n_entries; i++)
	{
		int32		ep = entry_nids[i];
		float8		d;

		if (ep < 0 || ep > cache->max_nid)
			continue;
		if (visited_test(visited_bits, visited_nwords, ep))
			continue;
		visited_set(&visited_bits, &visited_nwords, ep);
		n_visited++;

		d = hnsw_cache_distance(cache, ep, qx, qdim, qnorm);

		candidates[cand_size].dist = d;
		candidates[cand_size].nid  = ep;
		cand_size++;
		graph_minheap_siftup(candidates, cand_size - 1);

		if (res_size < ef)
		{
			res_out[res_size].dist = d;
			res_out[res_size].nid  = ep;
			res_size++;
			graph_maxheap_siftup(res_out, res_size - 1);
		}
		else if (d < res_out[0].dist)
		{
			res_out[0].dist = d;
			res_out[0].nid  = ep;
			graph_maxheap_siftdown(res_out, res_size, 0);
		}
	}

	while (cand_size > 0)
	{
		float8		c_dist;
		int32		c_nid;
		float8		f_dist;
		int			k;
		const HnswL0CacheNode *cn;

		c_dist = candidates[0].dist;
		c_nid  = candidates[0].nid;
		cand_size--;
		if (cand_size > 0)
		{
			candidates[0] = candidates[cand_size];
			graph_minheap_siftdown(candidates, cand_size, 0);
		}

		f_dist = (res_size >= ef) ? res_out[0].dist : DBL_MAX;
		if (res_size >= ef && c_dist > f_dist)
			break;

		if (c_nid < 0 || c_nid > cache->max_nid)
			continue;
		cn = &cache->nodes[c_nid];

		for (k = 0; k < (int) cn->nbr_count; k++)
		{
			int32	n_nid = cn->nbrs[k];
			float8	n_dist;

			if (n_nid < 0 || n_nid > cache->max_nid)
				continue;
			if (visited_test(visited_bits, visited_nwords, n_nid))
				continue;
			visited_set(&visited_bits, &visited_nwords, n_nid);
			n_visited++;

			/* Prefetch next unvisited neighbor */
			for (int pk = k + 1; pk < (int) cn->nbr_count; pk++)
			{
				int32 pnid = cn->nbrs[pk];

				if (pnid >= 0 && pnid <= cache->max_nid &&
					!visited_test(visited_bits, visited_nwords, pnid))
				{
					__builtin_prefetch(&cache->nodes[pnid], 0, 1);
					__builtin_prefetch(cache->vec_data +
									   (Size) pnid * cache->vec_stride, 0, 1);
					break;
				}
			}

			n_dist = hnsw_cache_distance(cache, n_nid, qx, qdim, qnorm);

			f_dist = (res_size >= ef) ? res_out[0].dist : DBL_MAX;
			if (n_dist < f_dist || res_size < ef)
			{
				if (cand_size >= cand_cap)
				{
					cand_cap  *= 2;
					candidates = repalloc(candidates,
										  sizeof(GraphEntry) * cand_cap);
				}
				candidates[cand_size].dist = n_dist;
				candidates[cand_size].nid  = n_nid;
				cand_size++;
				graph_minheap_siftup(candidates, cand_size - 1);

				if (res_size < ef)
				{
					res_out[res_size].dist = n_dist;
					res_out[res_size].nid  = n_nid;
					res_size++;
					graph_maxheap_siftup(res_out, res_size - 1);
				}
				else if (n_dist < res_out[0].dist)
				{
					res_out[0].dist = n_dist;
					res_out[0].nid  = n_nid;
					graph_maxheap_siftdown(res_out, res_size, 0);
				}
			}
		}
	}

	pfree(candidates);
	pfree(visited_bits);

	qsort(res_out, res_size, sizeof(GraphEntry), cmp_graph_entry);
	*n_visited_out = n_visited;
	return res_size;
}

/*
 * Beam search on one HNSW level.
 *
 * entry_nids[0..n_entries-1]: starting nodes.
 * ef: beam width (1 for greedy upper-level descent, ef_search for L0).
 * res_out: pre-allocated array of size ef+1.
 *
 * Returns number of results in res_out, sorted ascending by distance.
 */
static int
hnsw_search_level(HnswLevel *h,
				  const float *qx, int qdim,
				  int32 *entry_nids, int n_entries, int ef,
				  GraphEntry *res_out,
				  int *n_visited_out,
				  const HnswL0Cache *l0_cache,	/* NULL for upper levels */
				  int patience,					/* 0 = disabled */
				  double qnorm)					/* precomputed ||query||² */
{
	GraphEntry *candidates;
	int			cand_size = 0;
	int			cand_cap;
	int			res_size  = 0;
	uint64	   *visited_bits;		/* bitset: 1 bit per nid */
	int			visited_nwords;		/* number of uint64 words */
	int			n_visited = 0;
	int			i;

	int			stale_steps = 0;	/* adaptive ef: consecutive no-improvement expansions */

	cand_cap       = Max(ef * 32, 4096);
	candidates     = palloc(sizeof(GraphEntry) * cand_cap);
	visited_nwords = (l0_cache != NULL)
		? (l0_cache->max_nid / 64 + 2)
		: Max((int) h->rel->rd_rel->reltuples / 64 + 16, 1024);
	visited_bits   = palloc0(sizeof(uint64) * visited_nwords);

	/* ---- Seed from entry points ---- */
	for (i = 0; i < n_entries; i++)
	{
		int32		ep = entry_nids[i];
		float8		d;

		if (ep < 0)
			continue;

		if (visited_test(visited_bits, visited_nwords, ep))
			continue;
		visited_set(&visited_bits, &visited_nwords, ep);
		n_visited++;

		if (l0_cache != NULL && ep <= l0_cache->max_nid)
		{
			/* Cache fast path: dispatch f32 or f16 based on vec type */
			d = hnsw_cache_distance(l0_cache, ep, qx, qdim, qnorm);
		}
		else
		{
			/* Btree index path */
			bool	isnull;
			Datum	sk_d;
			Hsvec  *sk;

			ScanKeyInit(&h->skey[0], 1, BTEqualStrategyNumber,
						h->eq_op, Int32GetDatum(ep));
			index_rescan(h->iscan, h->skey, 1, NULL, 0);

			if (h->use_ios)
			{
				ItemPointer tid = index_getnext_tid(h->iscan,
													ForwardScanDirection);

				if (tid == NULL)
					continue;
				sk_d = index_getattr(h->iscan->xs_itup,
									 h->ios_att_sketch,
									 h->iscan->xs_itupdesc, &isnull);
			}
			else
			{
				if (!index_getnext_slot(h->iscan, ForwardScanDirection,
										h->slot))
					continue;
				sk_d = slot_getattr(h->slot, h->att_sketch, &isnull);
			}

			if (isnull)
			{
				if (!h->use_ios) ExecClearTuple(h->slot);
				continue;
			}
			sk = (Hsvec *) PG_DETOAST_DATUM(sk_d);
			d  = cosine_distance_f32_f16(qx, sk->x, Min(sk->dim, qdim));
			if (!h->use_ios) ExecClearTuple(h->slot);
		}

		candidates[cand_size].dist = d;
		candidates[cand_size].nid  = ep;
		cand_size++;
		graph_minheap_siftup(candidates, cand_size - 1);

		if (res_size < ef)
		{
			res_out[res_size].dist = d;
			res_out[res_size].nid  = ep;
			res_size++;
			graph_maxheap_siftup(res_out, res_size - 1);
		}
		else if (d < res_out[0].dist)
		{
			res_out[0].dist = d;
			res_out[0].nid  = ep;
			graph_maxheap_siftdown(res_out, res_size, 0);
		}
	}

	/* ---- Main beam search loop ---- */
	while (cand_size > 0)
	{
		float8		c_dist;
		int32		c_nid;
		float8		f_dist;
		int			k;
		bool		improved = false;

		c_dist = candidates[0].dist;
		c_nid  = candidates[0].nid;
		cand_size--;
		if (cand_size > 0)
		{
			candidates[0] = candidates[cand_size];
			graph_minheap_siftdown(candidates, cand_size, 0);
		}

		f_dist = (res_size >= ef) ? res_out[0].dist : DBL_MAX;
		if (res_size >= ef && c_dist > f_dist)
			break;

		if (l0_cache != NULL && c_nid >= 0 && c_nid <= l0_cache->max_nid)
		{
			/* ---- Cache fast path: no btree probes ---- */
			const HnswL0CacheNode *cn = &l0_cache->nodes[c_nid];
			const int32			  *nbrs_c = cn->nbrs;
			int					   n_nbrs_c = (int) cn->nbr_count;

			for (k = 0; k < n_nbrs_c; k++)
			{
				int32	n_nid = nbrs_c[k];
				float8	n_dist;

				if (n_nid < 0 || n_nid > l0_cache->max_nid)
					continue;

				if (visited_test(visited_bits, visited_nwords, n_nid))
					continue;
				visited_set(&visited_bits, &visited_nwords, n_nid);
				n_visited++;

				/* Prefetch next unvisited neighbor's cache node */
				for (int pk = k + 1; pk < n_nbrs_c; pk++)
				{
					int32 pnid = nbrs_c[pk];

					if (pnid >= 0 && pnid <= l0_cache->max_nid &&
						!visited_test(visited_bits, visited_nwords, pnid))
					{
						__builtin_prefetch(&l0_cache->nodes[pnid], 0, 1);
						__builtin_prefetch(l0_cache->vec_data +
										   (Size) pnid * l0_cache->vec_stride,
										   0, 1);
						break;
					}
				}

				n_dist = hnsw_cache_distance(l0_cache, n_nid, qx, qdim, qnorm);

				f_dist = (res_size >= ef) ? res_out[0].dist : DBL_MAX;
				if (n_dist < f_dist || res_size < ef)
				{
					if (cand_size >= cand_cap)
					{
						cand_cap  *= 2;
						candidates = repalloc(candidates,
											  sizeof(GraphEntry) * cand_cap);
					}
					candidates[cand_size].dist = n_dist;
					candidates[cand_size].nid  = n_nid;
					cand_size++;
					graph_minheap_siftup(candidates, cand_size - 1);

					if (res_size < ef)
					{
						res_out[res_size].dist = n_dist;
						res_out[res_size].nid  = n_nid;
						res_size++;
						graph_maxheap_siftup(res_out, res_size - 1);
						improved = true;
					}
					else if (n_dist < res_out[0].dist)
					{
						res_out[0].dist = n_dist;
						res_out[0].nid  = n_nid;
						graph_maxheap_siftdown(res_out, res_size, 0);
						improved = true;
					}
				}
			}
		}
		else
		{
			/* ---- Btree index path ---- */
			bool		isnull;
			Datum		nbrs_d;
			ArrayType  *nbrs_arr;
			int32	   *nbrs;
			int			n_nbrs;
			int32	   *nbrs_copy;

			ScanKeyInit(&h->skey[0], 1, BTEqualStrategyNumber,
						h->eq_op, Int32GetDatum(c_nid));
			index_rescan(h->iscan, h->skey, 1, NULL, 0);

			if (h->use_ios)
			{
				ItemPointer tid = index_getnext_tid(h->iscan,
													ForwardScanDirection);

				if (tid == NULL)
					continue;
				nbrs_d = index_getattr(h->iscan->xs_itup,
									   h->ios_att_nbrs,
									   h->iscan->xs_itupdesc, &isnull);
			}
			else
			{
				if (!index_getnext_slot(h->iscan, ForwardScanDirection,
										h->slot))
					continue;
				nbrs_d = slot_getattr(h->slot, h->att_nbrs, &isnull);
			}

			if (isnull)
			{
				if (!h->use_ios) ExecClearTuple(h->slot);
				continue;
			}

			nbrs_arr  = DatumGetArrayTypeP(nbrs_d);
			nbrs      = (int32 *) ARR_DATA_PTR(nbrs_arr);
			n_nbrs    = ArrayGetNItems(ARR_NDIM(nbrs_arr), ARR_DIMS(nbrs_arr));
			nbrs_copy = palloc(sizeof(int32) * n_nbrs);
			memcpy(nbrs_copy, nbrs, sizeof(int32) * n_nbrs);
			if (!h->use_ios) ExecClearTuple(h->slot);

			for (k = 0; k < n_nbrs; k++)
			{
				int32	n_nid = nbrs_copy[k];
				bool	nisnull;
				Datum	nsk_d;
				Hsvec  *nsk;
				float8	n_dist;

				if (n_nid < 0)
					continue;

				if (visited_test(visited_bits, visited_nwords, n_nid))
					continue;
				visited_set(&visited_bits, &visited_nwords, n_nid);
				n_visited++;

				ScanKeyInit(&h->skey[0], 1, BTEqualStrategyNumber,
							h->eq_op, Int32GetDatum(n_nid));
				index_rescan(h->iscan, h->skey, 1, NULL, 0);

				if (h->use_ios)
				{
					ItemPointer tid = index_getnext_tid(h->iscan,
														ForwardScanDirection);

					if (tid == NULL)
						continue;
					nsk_d = index_getattr(h->iscan->xs_itup,
										  h->ios_att_sketch,
										  h->iscan->xs_itupdesc, &nisnull);
				}
				else
				{
					if (!index_getnext_slot(h->iscan, ForwardScanDirection,
											h->slot))
						continue;
					nsk_d = slot_getattr(h->slot, h->att_sketch, &nisnull);
				}

				if (nisnull)
				{
					if (!h->use_ios) ExecClearTuple(h->slot);
					continue;
				}
				nsk    = (Hsvec *) PG_DETOAST_DATUM(nsk_d);
				n_dist = cosine_distance_f32_f16(qx, nsk->x,
												 Min(nsk->dim, qdim));
				if (!h->use_ios) ExecClearTuple(h->slot);

				f_dist = (res_size >= ef) ? res_out[0].dist : DBL_MAX;
				if (n_dist < f_dist || res_size < ef)
				{
					if (cand_size >= cand_cap)
					{
						cand_cap  *= 2;
						candidates = repalloc(candidates,
											  sizeof(GraphEntry) * cand_cap);
					}
					candidates[cand_size].dist = n_dist;
					candidates[cand_size].nid  = n_nid;
					cand_size++;
					graph_minheap_siftup(candidates, cand_size - 1);

					if (res_size < ef)
					{
						res_out[res_size].dist = n_dist;
						res_out[res_size].nid  = n_nid;
						res_size++;
						graph_maxheap_siftup(res_out, res_size - 1);
						improved = true;
					}
					else if (n_dist < res_out[0].dist)
					{
						res_out[0].dist = n_dist;
						res_out[0].nid  = n_nid;
						graph_maxheap_siftdown(res_out, res_size, 0);
						improved = true;
					}
				}
			}
			pfree(nbrs_copy);
		}

		/* ---- Adaptive ef: patience-based early termination ---- */
		if (patience > 0 && res_size >= ef)
		{
			if (improved)
				stale_steps = 0;
			else if (++stale_steps >= patience)
				break;
		}
	}

	pfree(candidates);
	pfree(visited_bits);

	/* Sort results ascending by distance */
	qsort(res_out, res_size, sizeof(GraphEntry), cmp_graph_entry);

	*n_visited_out = n_visited;
	return res_size;
}

PG_FUNCTION_INFO_V1(svec_hnsw_scan);
Datum
svec_hnsw_scan(PG_FUNCTION_ARGS)
{
	ReturnSetInfo  *rsinfo    = (ReturnSetInfo *) fcinfo->resultinfo;
	Oid				tbl_oid   = PG_GETARG_OID(0);
	Svec		   *query     = PG_GETARG_SVEC_P(1);
	char		   *prefix    = text_to_cstring(PG_GETARG_TEXT_PP(2));
	int				ef_search = PG_GETARG_INT32(3);
	int				lim       = PG_GETARG_INT32(4);
	int				rerank_topk  = PG_GETARG_INT32(5);
	int				rerank1_topk = (PG_NARGS() > 6) ? PG_GETARG_INT32(6) : 0;

	char		   *tblname;
	Snapshot		snapshot;
	int32			entry_nid = 0;
	int				max_level = 0;
	int32			ep;
	int				l;
	HnswLevel		l0;
	GraphEntry	   *l0_res;
	int				l0_res_size = 0;
	int				n_visited_upper = 0;
	int				n_visited_l0 = 0;
	int				j;
	instr_time		t_start, t_upper, t_l0, t_r1, t_rerank, t_out;
	const HnswL0Cache *active_cache = NULL;	/* set below if GUC on */
	double			cache_build_ms = 0.0;
	double			qnorm;			/* precomputed ||query||² */

	/* ---- Setup ---- */
	if (sorted_heap_ann_timing)
		INSTR_TIME_SET_CURRENT(t_start);

	if (ef_search < 1) ef_search = 1;
	if (lim < 1) lim = 1;
	if (ef_search < lim) ef_search = lim;

	qnorm = query_self_norm(query->x, query->dim);

	snapshot = GetActiveSnapshot();

	/* ---- Read meta table ---- */
	tblname = psprintf("%s_meta", prefix);
	{
		Oid				meta_oid;
		Relation		meta_rel;
		TableScanDesc	meta_scan;
		TupleTableSlot *meta_slot;
		AttrNumber		att_entry, att_maxlev;

		meta_oid  = DatumGetObjectId(
			DirectFunctionCall1(regclassin, CStringGetDatum(tblname)));
		meta_rel  = table_open(meta_oid, AccessShareLock);
		meta_slot = table_slot_create(meta_rel, NULL);
		meta_scan = table_beginscan(meta_rel, snapshot, 0, NULL);

		att_entry  = get_attnum(meta_oid, "entry_nid");
		att_maxlev = get_attnum(meta_oid, "max_level");

		if (table_scan_getnextslot(meta_scan, ForwardScanDirection,
								   meta_slot))
		{
			bool	isnull;

			entry_nid = DatumGetInt32(
				slot_getattr(meta_slot, att_entry, &isnull));
			max_level = (int) DatumGetInt16(
				slot_getattr(meta_slot, att_maxlev, &isnull));
			ExecClearTuple(meta_slot);
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_NO_DATA_FOUND),
					 errmsg("svec_hnsw_scan: \"%s\" is empty", tblname)));

		table_endscan(meta_scan);
		ExecDropSingleTupleTableSlot(meta_slot);
		table_close(meta_rel, AccessShareLock);
	}

	/* ---- Validation passed — init SRF and open L0 ---- */
	InitMaterializedSRF(fcinfo, 0);

	tblname = psprintf("%s_l0", prefix);
	hnsw_open_level(&l0, tblname, snapshot);

	/* ---- Session-local L0 cache (opt-in via sorted_heap.hnsw_cache_l0) ---- */
	if (sorted_heap_hnsw_cache_l0)
	{
		Oid l0_oid = RelationGetRelid(l0.rel);

		if (hnsw_l0_cache == NULL ||
			hnsw_l0_cache->l0_rel_oid != l0_oid)
		{
			/* Cache miss or stale: evict old, build fresh */
			evict_hnsw_l0_cache();
			hnsw_l0_cache = build_hnsw_l0_cache(l0.rel, l0_oid, snapshot);
		}
		active_cache  = hnsw_l0_cache;
		cache_build_ms = (hnsw_l0_cache->use_count == 0)
			? hnsw_l0_cache->build_ms : 0.0;
		hnsw_l0_cache->use_count++;
	}

	l0_res = palloc(sizeof(GraphEntry) * (ef_search + 1));

	/* ---- Greedy ef=1 descent through upper levels (L_max..L1) ---- */
	ep = entry_nid;
	for (l = max_level; l >= 1; l--)
	{
		GraphEntry	ul_res[2];	/* ef=1 → at most 1 result */
		int32		ep_arr[1];
		int			n_vis = 0;
		int			res_sz;
		int			cidx = l - 1;

		ep_arr[0] = ep;
		tblname = psprintf("%s_l%d", prefix, l);

		/*
		 * Fast path: if upper cache already built AND matches the
		 * current prefix's table OID, search from cache without
		 * opening/closing the relation.  OID check prevents cross-prefix
		 * cache reuse when different HNSW graphs share a backend.
		 */
		if (sorted_heap_hnsw_cache_l0 && cidx < HNSW_MAX_UPPER_LEVELS &&
			hnsw_upper_caches[cidx] != NULL)
		{
			Oid expected_oid;
			const char *dot = strchr(tblname, '.');

			if (dot)
			{
				char *schemaname = pnstrdup(tblname, dot - tblname);
				Oid   nsoid = LookupExplicitNamespace(schemaname, true);

				expected_oid = OidIsValid(nsoid)
					? get_relname_relid(dot + 1, nsoid)
					: InvalidOid;
			}
			else
				expected_oid = RelnameGetRelid(tblname);

			if (OidIsValid(expected_oid) &&
				hnsw_upper_caches[cidx]->l0_rel_oid == expected_oid)
			{
				res_sz = hnsw_search_cached(hnsw_upper_caches[cidx],
											query->x, query->dim,
											ep_arr, 1, 1, ul_res, &n_vis,
											qnorm);
				goto upper_done;
			}
			else
			{
				/* OID mismatch: evict stale cache */
				MemoryContextDelete(hnsw_upper_caches[cidx]->mctx);
				hnsw_upper_caches[cidx] = NULL;
			}
		}

		{
			/* Cold path: open relation, build cache if GUC on, search */
			HnswLevel	ul;
			const HnswL0Cache *ul_cache = NULL;

			hnsw_open_level(&ul, tblname, snapshot);

			if (sorted_heap_hnsw_cache_l0 && cidx < HNSW_MAX_UPPER_LEVELS)
			{
				Oid ul_oid = RelationGetRelid(ul.rel);

				hnsw_upper_caches[cidx] =
					build_hnsw_l0_cache(ul.rel, ul_oid, snapshot);
				ul_cache = hnsw_upper_caches[cidx];
			}

			res_sz = hnsw_search_level(&ul, query->x, query->dim,
									   ep_arr, 1, 1, ul_res, &n_vis,
									   ul_cache, 0, qnorm);
			hnsw_close_level(&ul);
		}
upper_done:
		n_visited_upper += n_vis;

		if (res_sz > 0)
			ep = ul_res[0].nid;
	}

	if (sorted_heap_ann_timing)
		INSTR_TIME_SET_CURRENT(t_upper);

	/* ---- Beam search at L0 ---- */
	{
		int32	ep_arr[1];
		int		n_vis = 0;

		ep_arr[0]   = ep;
		l0_res_size = hnsw_search_level(&l0, query->x, query->dim,
										ep_arr, 1, ef_search,
										l0_res, &n_vis, active_cache,
										sorted_heap_hnsw_ef_patience,
										qnorm);
		n_visited_l0 = n_vis;
	}

	if (sorted_heap_ann_timing)
		INSTR_TIME_SET_CURRENT(t_l0);

	/* ---- Rerank phase ---- */
	{
		GraphResult	   *gresults;
		bool			do_exact_rerank;
		int				n_out;

		gresults = palloc(sizeof(GraphResult) * l0_res_size);
		for (j = 0; j < l0_res_size; j++)
		{
			gresults[j].nid      = l0_res[j].nid;
			gresults[j].distance = l0_res[j].dist;
			gresults[j].src_id   = NULL;
			ItemPointerSetInvalid(&gresults[j].src_tid);
		}

		do_exact_rerank = (l0.att_src_tid != InvalidAttrNumber);

		/* Sketch pre-filter: l0_res already sorted ascending */
		if (do_exact_rerank && rerank_topk > 0 &&
			rerank_topk < l0_res_size)
		{
			int	effective = Max(rerank_topk, lim);

			l0_res_size = Min(effective, l0_res_size);
		}

		/* ---- Dense r1 rerank via _r1 sidecar (e.g. hsvec(768)) ---- *
		 * If rerank1_topk > 0 and the _r1 table exists, compute an
		 * intermediate hsvec cosine distance for every L0 candidate and
		 * keep only the closest rerank1_topk before expensive full-svec
		 * TOAST reads.  Gracefully skipped when _r1 is absent.
		 */
		if (rerank1_topk > 0 && l0_res_size > 0)
		{
			const char *dot;
			char	   *r1_tblname;
			Oid			r1_oid = InvalidOid;

			r1_tblname = psprintf("%s_r1", prefix);
			dot = strchr(prefix, '.');
			if (dot)
			{
				/* schema-qualified prefix: split at first '.' */
				char	   *schemaname = pnstrdup(prefix, dot - prefix);
				const char *relname    = psprintf("%s_r1", dot + 1);
				Oid			nsoid = LookupExplicitNamespace(schemaname,
															true /* missing_ok */);

				if (OidIsValid(nsoid))
					r1_oid = get_relname_relid(relname, nsoid);
			}
			else
				r1_oid = RelnameGetRelid(psprintf("%s_r1", prefix));

			if (OidIsValid(r1_oid))
			{
				Relation		r1_rel;
				TupleDesc		r1_td;
				TupleTableSlot *r1_slot;
				AttrNumber		r1_vec_attno;
				Relation		r1_pk;
				AttrNumber		r1_pk_att;
				Oid				r1_pk_typid, r1_oc, r1_of;
				RegProcedure	r1_eq_op;
				IndexScanDesc	r1_iscan;
				ScanKeyData		r1_skey[1];
				int				r1_effective;

				r1_rel       = table_open(r1_oid, AccessShareLock);
				r1_td        = RelationGetDescr(r1_rel);
				r1_slot      = table_slot_create(r1_rel, NULL);
				r1_vec_attno = get_attnum(r1_oid, "rerank_vec");

				if (r1_vec_attno == InvalidAttrNumber)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("svec_hnsw_scan: \"%s\" must have "
									"\"rerank_vec\" (hsvec) column",
									r1_tblname)));

				if (!r1_rel->rd_indexvalid)
					RelationGetIndexList(r1_rel);
				if (!OidIsValid(r1_rel->rd_pkindex))
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_OBJECT),
							 errmsg("svec_hnsw_scan: \"%s\" has no "
									"primary key", r1_tblname)));

				r1_pk       = index_open(r1_rel->rd_pkindex, AccessShareLock);
				r1_pk_att   = r1_pk->rd_index->indkey.values[0];
				r1_pk_typid = TupleDescAttr(r1_td, r1_pk_att - 1)->atttypid;
				r1_oc       = GetDefaultOpClass(r1_pk_typid, BTREE_AM_OID);
				r1_of       = get_opclass_family(r1_oc);
				r1_eq_op    = get_opcode(
					get_opfamily_member(r1_of, r1_pk_typid, r1_pk_typid,
										BTEqualStrategyNumber));

#if PG_VERSION_NUM < 180000
				r1_iscan = index_beginscan(r1_rel, r1_pk, snapshot, 1, 0);
#else
				r1_iscan = index_beginscan(r1_rel, r1_pk, snapshot, NULL, 1, 0);
#endif

				for (j = 0; j < l0_res_size; j++)
				{
					bool	isnull;
					Datum	vec_d;
					Hsvec  *rvec;

					ScanKeyInit(&r1_skey[0], 1, BTEqualStrategyNumber,
								r1_eq_op,
								Int32GetDatum(gresults[j].nid));
					index_rescan(r1_iscan, r1_skey, 1, NULL, 0);

					if (!index_getnext_slot(r1_iscan, ForwardScanDirection,
											r1_slot))
					{
						/* nid absent from _r1: sort to end */
						gresults[j].distance = DBL_MAX;
						continue;
					}

					vec_d = slot_getattr(r1_slot, r1_vec_attno, &isnull);
					if (!isnull)
					{
						rvec = (Hsvec *) PG_DETOAST_DATUM(vec_d);
						gresults[j].distance =
							cosine_distance_f32_f16(query->x, rvec->x,
													Min(rvec->dim,
														query->dim));
					}
					else
						gresults[j].distance = DBL_MAX;

					ExecClearTuple(r1_slot);
				}

				index_endscan(r1_iscan);
				ExecDropSingleTupleTableSlot(r1_slot);
				index_close(r1_pk, AccessShareLock);
				table_close(r1_rel, AccessShareLock);

				/* Sort by r1 distance, truncate to rerank1_effective */
				r1_effective = Max(rerank1_topk, lim);
				qsort(gresults, l0_res_size, sizeof(GraphResult),
					  cmp_graph_result);
				if (l0_res_size > r1_effective)
					l0_res_size = r1_effective;
			}
		}

		if (sorted_heap_ann_timing)
			INSTR_TIME_SET_CURRENT(t_r1);

		/* Read src_tid for all candidates */
		if (do_exact_rerank && active_cache != NULL)
		{
			/* Cache fast path: src_tid already loaded in HnswL0CacheNode */
			for (j = 0; j < l0_res_size; j++)
			{
				int32 nid = gresults[j].nid;

				if (nid >= 0 && nid <= active_cache->max_nid &&
					ItemPointerIsValid(&active_cache->nodes[nid].src_tid))
					ItemPointerCopy(&active_cache->nodes[nid].src_tid,
									&gresults[j].src_tid);
			}
		}
		else if (do_exact_rerank)
		{
			IndexScanDesc	src_iscan;
			ScanKeyData		src_skey[1];

			if (l0.use_ios && l0.ios_att_src_tid != 0)
			{
				/* IOS path: src_tid is in the covering index */
#if PG_VERSION_NUM < 180000
				src_iscan = index_beginscan(l0.rel, l0.idx,
											snapshot, 1, 0);
#else
				src_iscan = index_beginscan(l0.rel, l0.idx,
											snapshot, NULL, 1, 0);
#endif
				src_iscan->xs_want_itup = true;

				for (j = 0; j < l0_res_size; j++)
				{
					bool		isnull;
					ItemPointer tid;
					Datum		tid_d;

					ScanKeyInit(&src_skey[0], 1, BTEqualStrategyNumber,
								l0.eq_op,
								Int32GetDatum(gresults[j].nid));
					index_rescan(src_iscan, src_skey, 1, NULL, 0);

					tid = index_getnext_tid(src_iscan,
											ForwardScanDirection);
					if (tid == NULL)
						continue;

					tid_d = index_getattr(src_iscan->xs_itup,
										  l0.ios_att_src_tid,
										  src_iscan->xs_itupdesc,
										  &isnull);
					if (!isnull)
						ItemPointerCopy(DatumGetItemPointer(tid_d),
										&gresults[j].src_tid);
				}
				index_endscan(src_iscan);
			}
			else
			{
				/* Heap path: PK lookup → heap fetch for src_tid */
				Relation	src_idx;
				bool		src_pk = false;

				if (l0.use_ios)
				{
					src_idx = index_open(l0.pk_oid, AccessShareLock);
					src_pk  = true;
				}
				else
					src_idx = l0.idx;

#if PG_VERSION_NUM < 180000
				src_iscan = index_beginscan(l0.rel, src_idx,
											snapshot, 1, 0);
#else
				src_iscan = index_beginscan(l0.rel, src_idx,
											snapshot, NULL, 1, 0);
#endif
				for (j = 0; j < l0_res_size; j++)
				{
					bool	isnull;

					ScanKeyInit(&src_skey[0], 1, BTEqualStrategyNumber,
								l0.eq_op,
								Int32GetDatum(gresults[j].nid));
					index_rescan(src_iscan, src_skey, 1, NULL, 0);

					if (!index_getnext_slot(src_iscan,
											ForwardScanDirection,
											l0.slot))
						continue;

					{
						Datum	td = slot_getattr(l0.slot,
												   l0.att_src_tid,
												   &isnull);

						if (!isnull)
							ItemPointerCopy(DatumGetItemPointer(td),
											&gresults[j].src_tid);
					}
					ExecClearTuple(l0.slot);
				}
				index_endscan(src_iscan);
				if (src_pk)
					index_close(src_idx, AccessShareLock);
			}
		}

		/* Exact rerank via main table */
		if (do_exact_rerank)
		{
			Relation		main_rel;
			TupleTableSlot *main_slot;
			TupleDesc		main_td;
			AttrNumber		emb_attno, id_attno;
			Oid				id_typid, typoutput;
			bool			typisvarlena;

			main_rel  = table_open(tbl_oid, AccessShareLock);
			main_td   = RelationGetDescr(main_rel);
			main_slot = table_slot_create(main_rel, NULL);

			emb_attno = get_attnum(tbl_oid, "embedding");
			id_attno  = get_attnum(tbl_oid, "id");
			id_typid  = TupleDescAttr(main_td, id_attno - 1)->atttypid;
			getTypeOutputInfo(id_typid, &typoutput, &typisvarlena);

			/* Sort by block for sequential I/O */
			qsort(gresults, l0_res_size, sizeof(GraphResult),
				  cmp_graph_result_by_blk);

			/* Prefetch initial batch */
			for (j = 0; j < Min(RERANK_PREFETCH_DISTANCE,
								 l0_res_size); j++)
			{
				if (ItemPointerIsValid(&gresults[j].src_tid))
					PrefetchBuffer(main_rel, MAIN_FORKNUM,
								   ItemPointerGetBlockNumber(
									   &gresults[j].src_tid));
			}

			for (j = 0; j < l0_res_size; j++)
			{
				bool	isnull;
				Datum	emb_d;
				Svec   *candidate;

				if (!ItemPointerIsValid(&gresults[j].src_tid))
				{
					gresults[j].distance = DBL_MAX;
					continue;
				}

				if (j + RERANK_PREFETCH_DISTANCE < l0_res_size &&
					ItemPointerIsValid(
						&gresults[j + RERANK_PREFETCH_DISTANCE].src_tid))
					PrefetchBuffer(main_rel, MAIN_FORKNUM,
								   ItemPointerGetBlockNumber(
									   &gresults[j + RERANK_PREFETCH_DISTANCE].src_tid));

				if (!table_tuple_fetch_row_version(main_rel,
												   &gresults[j].src_tid,
												   snapshot, main_slot))
				{
					gresults[j].distance = DBL_MAX;
					continue;
				}

				emb_d = slot_getattr(main_slot, emb_attno, &isnull);
				if (!isnull)
				{
					candidate = DatumGetSvecP(emb_d);
					if (candidate->dim != query->dim)
						ereport(ERROR,
								(errcode(ERRCODE_DATA_EXCEPTION),
								 errmsg("svec_hnsw_scan: embedding dim "
										"%d != query dim %d",
										candidate->dim, query->dim)));
					gresults[j].distance =
						svec_cosine_distance_internal(query, candidate);
				}

				{
					Datum	mid = slot_getattr(main_slot, id_attno,
											   &isnull);

					if (!isnull)
						gresults[j].src_id = pstrdup(
							OidOutputFunctionCall(typoutput, mid));
				}
				ExecClearTuple(main_slot);
			}

			ExecDropSingleTupleTableSlot(main_slot);
			table_close(main_rel, AccessShareLock);
		}

		if (sorted_heap_ann_timing)
			INSTR_TIME_SET_CURRENT(t_rerank);

		/* Sort final results by distance, keep top lim */
		qsort(gresults, l0_res_size, sizeof(GraphResult),
			  cmp_graph_result);
		n_out = Min(l0_res_size, lim);

		/* Deferred src_id when no exact rerank */
		if (!do_exact_rerank && l0.att_src_id != InvalidAttrNumber)
		{
			IndexScanDesc	srcid_iscan;
			Relation		srcid_idx;
			bool			srcid_pk = false;
			ScanKeyData		srcid_skey[1];
			TupleDesc		g_td = RelationGetDescr(l0.rel);
			Oid				g_typid, g_typout;
			bool			g_typisvar;

			g_typid = TupleDescAttr(g_td,
									l0.att_src_id - 1)->atttypid;
			getTypeOutputInfo(g_typid, &g_typout, &g_typisvar);

			if (l0.use_ios)
			{
				srcid_idx = index_open(l0.pk_oid, AccessShareLock);
				srcid_pk  = true;
			}
			else
				srcid_idx = l0.idx;

#if PG_VERSION_NUM < 180000
			srcid_iscan = index_beginscan(l0.rel, srcid_idx,
										  snapshot, 1, 0);
#else
			srcid_iscan = index_beginscan(l0.rel, srcid_idx,
										  snapshot, NULL, 1, 0);
#endif
			for (j = 0; j < n_out; j++)
			{
				bool	isnull;

				ScanKeyInit(&srcid_skey[0], 1, BTEqualStrategyNumber,
							l0.eq_op,
							Int32GetDatum(gresults[j].nid));
				index_rescan(srcid_iscan, srcid_skey, 1, NULL, 0);

				if (!index_getnext_slot(srcid_iscan,
										ForwardScanDirection,
										l0.slot))
					continue;

				{
					Datum	id_d = slot_getattr(l0.slot,
												 l0.att_src_id, &isnull);

					if (!isnull)
						gresults[j].src_id = pstrdup(
							OidOutputFunctionCall(g_typout, id_d));
				}
				ExecClearTuple(l0.slot);
			}
			index_endscan(srcid_iscan);
			if (srcid_pk)
				index_close(srcid_idx, AccessShareLock);
		}

		/* ---- Output tuples ---- */
		for (j = 0; j < n_out; j++)
		{
			Datum	values[2];
			bool	nulls[2] = {false, false};

			values[0] = CStringGetTextDatum(
				gresults[j].src_id ? gresults[j].src_id : "");
			values[1] = Float8GetDatum(gresults[j].distance);
			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
								 values, nulls);
		}

		pfree(gresults);
	}

	/* ---- Cleanup ---- */
	hnsw_close_level(&l0);
	pfree(l0_res);

	if (sorted_heap_ann_timing)
	{
		INSTR_TIME_SET_CURRENT(t_out);
		elog(DEBUG1,
			 "svec_hnsw_scan: upper=%.3fms l0=%.3fms r1=%.3fms "
			 "rerank=%.3fms out=%.3fms total=%.3fms "
			 "(visited_upper=%d visited_l0=%d results=%d max_level=%d "
			 "cache=%s build_ms=%.1f use=%lld bytes=%lld)",
			 INSTR_TIME_GET_MILLISEC(t_upper) -
			 INSTR_TIME_GET_MILLISEC(t_start),
			 INSTR_TIME_GET_MILLISEC(t_l0) -
			 INSTR_TIME_GET_MILLISEC(t_upper),
			 INSTR_TIME_GET_MILLISEC(t_r1) -
			 INSTR_TIME_GET_MILLISEC(t_l0),
			 INSTR_TIME_GET_MILLISEC(t_rerank) -
			 INSTR_TIME_GET_MILLISEC(t_r1),
			 INSTR_TIME_GET_MILLISEC(t_out) -
			 INSTR_TIME_GET_MILLISEC(t_rerank),
			 INSTR_TIME_GET_MILLISEC(t_out) -
			 INSTR_TIME_GET_MILLISEC(t_start),
			 n_visited_upper, n_visited_l0, l0_res_size, max_level,
			 active_cache ? "hit" : "off",
			 cache_build_ms,
			 active_cache ? (long long) active_cache->use_count : 0LL,
			 active_cache ? (long long) active_cache->cache_bytes : 0LL);
	}

	return (Datum) 0;
}
