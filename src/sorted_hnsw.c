/*
 * sorted_hnsw.c
 *
 * HNSW Index Access Method for pg_sorted_heap.
 *
 * Phase 1 MVP: build + scan + insert + lazy delete.
 */
#include "sorted_hnsw.h"

#include "access/amapi.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "optimizer/cost.h"
#include "storage/bufmgr.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"
#include "optimizer/optimizer.h"

#include <float.h>
#include <math.h>

#include "svec.h"

/* ---- GUCs ---- */

int		sorted_hnsw_ef_search = 96;
bool	sorted_hnsw_sq8 = true;

static relopt_kind shnsw_relopt_kind = 0;

/* ---- Forward declarations ---- */

static IndexBuildResult *shnsw_build(Relation heap, Relation index,
									 IndexInfo *indexInfo);
static void shnsw_buildempty(Relation index);
static bool shnsw_insert(Relation index, Datum *values, bool *isnull,
						  ItemPointer heap_tid, Relation heap,
						  IndexUniqueCheck checkUnique,
						  bool indexUnchanged,
						  IndexInfo *indexInfo);
static IndexScanDesc shnsw_beginscan(Relation index, int nkeys,
									  int norderbys);
static void shnsw_rescan(IndexScanDesc scan, ScanKey keys, int nkeys,
						  ScanKey orderbys, int norderbys);
static bool shnsw_gettuple(IndexScanDesc scan, ScanDirection direction);
static void shnsw_endscan(IndexScanDesc scan);
static void shnsw_costestimate(PlannerInfo *root, IndexPath *path,
								double loop_count,
								Cost *indexStartupCost,
								Cost *indexTotalCost,
								Selectivity *indexSelectivity,
								double *indexCorrelation,
								double *indexPages);
static IndexBulkDeleteResult *shnsw_bulkdelete(IndexVacuumInfo *info,
												IndexBulkDeleteResult *stats,
												IndexBulkDeleteCallback callback,
												void *callback_state);
static IndexBulkDeleteResult *shnsw_vacuumcleanup(IndexVacuumInfo *info,
												   IndexBulkDeleteResult *stats);
static bool shnsw_validate(Oid opclassoid);
static bytea *shnsw_options(Datum reloptions, bool validate);

/* ================================================================
 * AM Handler
 * ================================================================ */

PG_FUNCTION_INFO_V1(sorted_hnsw_handler);
Datum
sorted_hnsw_handler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	/* AM properties */
	amroutine->amstrategies = 0;
	amroutine->amsupport = 1;			/* distance function */
	amroutine->amoptsprocnum = 0;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = true;	/* ORDER BY col <=> val */
	amroutine->amcanbackward = false;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = false;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amcanparallel = false;
	amroutine->amcanbuildparallel = false;
	amroutine->amcaninclude = false;
	amroutine->amusemaintenanceworkmem = true;
	amroutine->amsummarizing = false;
	amroutine->amparallelvacuumoptions = VACUUM_OPTION_NO_PARALLEL;
	amroutine->amkeytype = InvalidOid;

	/* Callbacks */
	amroutine->ambuild = shnsw_build;
	amroutine->ambuildempty = shnsw_buildempty;
	amroutine->aminsert = shnsw_insert;
	amroutine->ambulkdelete = shnsw_bulkdelete;
	amroutine->amvacuumcleanup = shnsw_vacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = shnsw_costestimate;
	amroutine->amoptions = shnsw_options;
	amroutine->amproperty = NULL;
	amroutine->ambuildphasename = NULL;
	amroutine->amvalidate = shnsw_validate;
	amroutine->amadjustmembers = NULL;
	amroutine->ambeginscan = shnsw_beginscan;
	amroutine->amrescan = shnsw_rescan;
	amroutine->amgettuple = shnsw_gettuple;
	amroutine->amgetbitmap = NULL;		/* no bitmap scans */
	amroutine->amendscan = shnsw_endscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}

/* ================================================================
 * GUC Registration (called from _PG_init in pg_sorted_heap.c)
 * ================================================================ */

void
sorted_hnsw_init(void)
{
	DefineCustomIntVariable(
		"sorted_hnsw.ef_search",
		"Beam width for HNSW index search",
		NULL,
		&sorted_hnsw_ef_search,
		96, 1, 10000,
		PGC_USERSET, 0,
		NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"sorted_hnsw.sq8",
		"Use SQ8 quantization in HNSW index L0",
		NULL,
		&sorted_hnsw_sq8,
		true,
		PGC_USERSET, 0,
		NULL, NULL, NULL);

	/* Register custom reloption kind for sorted_hnsw indexes */
	shnsw_relopt_kind = add_reloption_kind();
	add_int_reloption(shnsw_relopt_kind, "m",
					  "HNSW M parameter (max edges per node per layer)",
					  SHNSW_DEFAULT_M, 2, SHNSW_MAX_M,
					  AccessExclusiveLock);
	add_int_reloption(shnsw_relopt_kind, "ef_construction",
					  "Build-time beam width for HNSW graph construction",
					  SHNSW_DEFAULT_EF_CONSTRUCTION, 4, SHNSW_MAX_EF_CONSTRUCTION,
					  AccessExclusiveLock);
}

/* ================================================================
 * Reloptions
 * ================================================================ */

static bytea *
shnsw_options(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"m", RELOPT_TYPE_INT, offsetof(ShnswOptions, m)},
		{"ef_construction", RELOPT_TYPE_INT, offsetof(ShnswOptions, ef_construction)},
	};

	return (bytea *) build_reloptions(reloptions, validate,
									  shnsw_relopt_kind,
									  sizeof(ShnswOptions),
									  tab, lengthof(tab));
}

/* ================================================================
 * Build (ambuild)
 * ================================================================ */

/* ---- Helper: write one page to the index via GenericXLog ---- */
static void
shnsw_flush_page(Relation index, Buffer buf, Page src_page)
{
	Page target = BufferGetPage(buf);

	memcpy(target, src_page, BLCKSZ);
	MarkBufferDirty(buf);
	log_newpage_buffer(buf, true);
}

/* ---- SQ8 quantization helpers ---- */

static inline uint8
sq8_quantize(float val, float min_val, float scale)
{
	float	normalized;

	if (scale <= 0.0f)
		return 0;
	normalized = (val - min_val) / scale;
	if (normalized < 0.0f) normalized = 0.0f;
	if (normalized > 255.0f) normalized = 255.0f;
	return (uint8) (normalized + 0.5f);
}

static IndexBuildResult *
shnsw_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult   *result;
	MemoryContext		build_ctx;
	MemoryContext		old_ctx;
	HnswBuildState	   *graph;
	ShnswOptions	   *opts;
	int					M, ef_construction, dim;
	int					n_nodes;
	int					max_level;

	/* Vectors + TIDs collected from heap scan */
	float			   *vectors = NULL;
	ItemPointerData	   *tids = NULL;
	int					alloc_nodes = 0;

	/* SQ8 */
	float			   *sq8_mins = NULL;
	float			   *sq8_scales = NULL;

	/* Page writing */
	int					nodes_per_page;
	int					sq8_aux_npages;
	BlockNumber			next_blkno;
	int					i, d;

	/* Read reloptions */
	opts = (ShnswOptions *) index->rd_options;
	M = opts ? opts->m : SHNSW_DEFAULT_M;
	ef_construction = opts ? opts->ef_construction : SHNSW_DEFAULT_EF_CONSTRUCTION;

	/* Determine vector dimension from the indexed column's typmod */
	dim = TupleDescAttr(index->rd_att, 0)->atttypmod;
	if (dim <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("sorted_hnsw requires svec with explicit dimension, e.g. svec(768)")));

	build_ctx = AllocSetContextCreate(CurrentMemoryContext,
									  "sorted_hnsw build",
									  ALLOCSET_DEFAULT_SIZES);

	/* ---- Phase 1: Scan heap, collect vectors ---- */
	{
		TableScanDesc	scan;
		TupleTableSlot *slot;
		Snapshot		snapshot;
		int				vec_attno;

		elog(NOTICE, "sorted_hnsw: scanning heap for vectors (dim=%d)", dim);

		vec_attno = indexInfo->ii_IndexAttrNumbers[0];
		snapshot = RegisterSnapshot(GetLatestSnapshot());
		scan = table_beginscan(heap, snapshot, 0, NULL);
		slot = table_slot_create(heap, NULL);

		n_nodes = 0;
		alloc_nodes = 1024;
		old_ctx = MemoryContextSwitchTo(build_ctx);
		vectors = palloc(sizeof(float) * alloc_nodes * dim);
		tids = palloc(sizeof(ItemPointerData) * alloc_nodes);
		MemoryContextSwitchTo(old_ctx);

		while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
		{
			bool	isnull;
			Datum	val;
			Svec   *sv;

			CHECK_FOR_INTERRUPTS();

			val = slot_getattr(slot, vec_attno, &isnull);
			if (isnull)
				continue;

			sv = DatumGetSvecP(val);
			if (sv->dim != dim)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_EXCEPTION),
						 errmsg("vector dimension %d does not match index dimension %d",
								sv->dim, dim)));

			/* Grow arrays if needed */
			if (n_nodes >= alloc_nodes)
			{
				alloc_nodes *= 2;
				old_ctx = MemoryContextSwitchTo(build_ctx);
				vectors = repalloc(vectors, sizeof(float) * (Size)alloc_nodes * dim);
				tids = repalloc(tids, sizeof(ItemPointerData) * alloc_nodes);
				MemoryContextSwitchTo(old_ctx);
			}

			memcpy(vectors + (Size)n_nodes * dim, sv->x, sizeof(float) * dim);
			ItemPointerCopy(&slot->tts_tid, &tids[n_nodes]);
			n_nodes++;

			if (sv != (Svec *) DatumGetPointer(val))
				pfree(sv);

			ExecClearTuple(slot);
		}

		ExecDropSingleTupleTableSlot(slot);
		table_endscan(scan);
		UnregisterSnapshot(snapshot);

		elog(NOTICE, "sorted_hnsw: collected %d vectors", n_nodes);
	}

	if (n_nodes == 0)
	{
		/* Empty table — write empty metapage */
		shnsw_buildempty(index);
		result = palloc0(sizeof(IndexBuildResult));
		return result;
	}

	/* ---- Phase 2: Build HNSW graph in memory ---- */
	graph = shnsw_build_graph(vectors, tids, n_nodes, dim,
							  M, ef_construction, build_ctx);
	max_level = shnsw_build_max_level(graph);

	/* ---- Phase 3: Compute SQ8 min/max ---- */
	old_ctx = MemoryContextSwitchTo(build_ctx);
	sq8_mins = palloc(sizeof(float) * dim);
	sq8_scales = palloc(sizeof(float) * dim);
	MemoryContextSwitchTo(old_ctx);

	for (d = 0; d < dim; d++)
	{
		sq8_mins[d] = FLT_MAX;
		sq8_scales[d] = -FLT_MAX;	/* temporarily store max here */
	}
	for (i = 0; i < n_nodes; i++)
	{
		const float *v = vectors + (Size)i * dim;
		for (d = 0; d < dim; d++)
		{
			if (v[d] < sq8_mins[d]) sq8_mins[d] = v[d];
			if (v[d] > sq8_scales[d]) sq8_scales[d] = v[d];
		}
	}
	for (d = 0; d < dim; d++)
	{
		float range = sq8_scales[d] - sq8_mins[d];
		sq8_scales[d] = (range > 0.0f) ? (range / 255.0f) : 0.0f;
	}

	/* ---- Phase 4: Write index pages ---- */

	nodes_per_page = ShnswL0NodesPerPage(M, dim);
	if (nodes_per_page < 1)
		nodes_per_page = 1;

	/* SQ8 aux pages: need 2 * dim * sizeof(float) bytes */
	sq8_aux_npages = (int) ceil((2.0 * dim * sizeof(float)) /
								(BLCKSZ - MAXALIGN(SizeOfPageHeaderData) -
								 MAXALIGN(sizeof(ShnswPageOpaqueData))));
	if (sq8_aux_npages < 1)
		sq8_aux_npages = 1;

	elog(NOTICE, "sorted_hnsw: writing index pages (nodes_per_page=%d, sq8_aux=%d)",
		 nodes_per_page, sq8_aux_npages);

	next_blkno = 0;

	/* 4a: Reserve metapage (block 0) — filled at the end after all pages known */
	{
		Buffer		buf;
		Page		page;
		GenericXLogState *state;

		buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		state = GenericXLogStart(index);
		page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
		PageInit(page, BLCKSZ, sizeof(ShnswPageOpaqueData));
		GenericXLogFinish(state);
		UnlockReleaseBuffer(buf);
		next_blkno = 1;
	}

	/* 4b: SQ8 auxiliary pages */
	{
		Size	total_bytes = 2 * dim * sizeof(float);
		Size	offset = 0;
		int		p;
		Size	usable;

		float  *sq8_data;

		usable = BLCKSZ - MAXALIGN(SizeOfPageHeaderData) -
			MAXALIGN(sizeof(ShnswPageOpaqueData));

		/* Pack: first dim floats = mins, next dim floats = scales */
		sq8_data = palloc(total_bytes);
		memcpy(sq8_data, sq8_mins, sizeof(float) * dim);
		memcpy(sq8_data + dim, sq8_scales, sizeof(float) * dim);

		for (p = 0; p < sq8_aux_npages; p++)
		{
			Buffer		buf;
			Page		page;
			ShnswPageOpaque opaque;
			Size		chunk;

			buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW,
									 RBM_NORMAL, NULL);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

			page = palloc(BLCKSZ);
			PageInit(page, BLCKSZ, sizeof(ShnswPageOpaqueData));
			opaque = ShnswPageGetOpaque(page);
			opaque->shnsw_page_type = SHNSW_PAGE_SQ8_AUX;
			opaque->shnsw_level = 0;
			opaque->shnsw_next = (p + 1 < sq8_aux_npages) ?
				next_blkno + 1 : InvalidBlockNumber;

			chunk = Min(total_bytes - offset, usable);
			memcpy(PageGetContents(page), (char *)sq8_data + offset, chunk);

			shnsw_flush_page(index, buf, page);
			UnlockReleaseBuffer(buf);
			pfree(page);

			offset += chunk;
			next_blkno++;
		}
		pfree(sq8_data);
	}

	/* 4c: L0 node pages */
	{
		int		node_size = MAXALIGN(ShnswNodeSize(M, dim));
		int		page_nodes = 0;
		Buffer	buf = InvalidBuffer;
		Page	page = NULL;
		ShnswPageOpaque opaque;

		for (i = 0; i < n_nodes; i++)
		{
			HnswBuiltNode *bn = shnsw_build_get_node(graph, i);
			ShnswNodeHeader *nh;
			int32  *nbrs;
			uint8  *sq8;
			const float *vec;
			int		n;

			CHECK_FOR_INTERRUPTS();

			/* Start new page if needed */
			if (page == NULL || page_nodes >= nodes_per_page)
			{
				if (page != NULL)
				{
					shnsw_flush_page(index, buf, page);
					UnlockReleaseBuffer(buf);
					pfree(page);
				}

				buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW,
										 RBM_NORMAL, NULL);
				LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

				page = palloc0(BLCKSZ);
				PageInit(page, BLCKSZ, sizeof(ShnswPageOpaqueData));
				opaque = ShnswPageGetOpaque(page);
				opaque->shnsw_page_type = SHNSW_PAGE_L0;
				opaque->shnsw_level = 0;
				opaque->shnsw_next = InvalidBlockNumber;
				page_nodes = 0;
				next_blkno++;
			}

			/* Write node into page */
			nh = (ShnswNodeHeader *)
				((char *) PageGetContents(page) + page_nodes * node_size);

			nh->nid = bn->nid;
			ItemPointerCopy(&bn->heap_tid, &nh->heap_tid);
			nh->level = bn->level;
			nh->flags = 0;
			nh->padding = 0;

			/* Neighbors at level 0 */
			nbrs = ShnswNodeNeighbors(nh);
			n = (bn->n_neighbors && bn->n_neighbors[0] > 0) ?
				Min(bn->n_neighbors[0], 2 * M) : 0;
			nh->n_neighbors = n;
			for (d = 0; d < n; d++)
				nbrs[d] = bn->neighbors[0][d];
			for (d = n; d < 2 * M; d++)
				nbrs[d] = -1;

			/* SQ8 quantized vector */
			sq8 = ShnswNodeSQ8Vec(nh, M);
			vec = vectors + (Size)i * dim;
			for (d = 0; d < dim; d++)
				sq8[d] = sq8_quantize(vec[d], sq8_mins[d], sq8_scales[d]);

			page_nodes++;
		}

		/* Flush last page */
		if (page != NULL)
		{
			shnsw_flush_page(index, buf, page);
			UnlockReleaseBuffer(buf);
			pfree(page);
		}
	}

	/* 4d: Upper level pages */
	{
		int		lev;
		int		entries_per_page = ShnswUpperEntriesPerPage(M);
		int		entry_size = MAXALIGN(ShnswUpperEntrySize(M));
		BlockNumber upper_starts[SHNSW_MAX_LEVELS];
		int			upper_npages[SHNSW_MAX_LEVELS];

		memset(upper_starts, 0, sizeof(upper_starts));
		memset(upper_npages, 0, sizeof(upper_npages));

		if (entries_per_page < 1)
			entries_per_page = 1;

		for (lev = 1; lev <= max_level; lev++)
		{
			int		page_entries = 0;
			Buffer	ubuf = InvalidBuffer;
			Page	upage = NULL;
			ShnswPageOpaque uopaque;

			upper_starts[lev] = next_blkno;

			for (i = 0; i < n_nodes; i++)
			{
				HnswBuiltNode *bn = shnsw_build_get_node(graph, i);
				ShnswUpperEntry *ue;
				int32  *unbrs;
				int		n;

				if (bn->level < lev)
					continue;

				/* New page if needed */
				if (upage == NULL || page_entries >= entries_per_page)
				{
					if (upage != NULL)
					{
						shnsw_flush_page(index, ubuf, upage);
						UnlockReleaseBuffer(ubuf);
						pfree(upage);
					}
					ubuf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW,
											  RBM_NORMAL, NULL);
					LockBuffer(ubuf, BUFFER_LOCK_EXCLUSIVE);
					upage = palloc0(BLCKSZ);
					PageInit(upage, BLCKSZ, sizeof(ShnswPageOpaqueData));
					uopaque = ShnswPageGetOpaque(upage);
					uopaque->shnsw_page_type = SHNSW_PAGE_UPPER;
					uopaque->shnsw_level = lev;
					uopaque->shnsw_next = InvalidBlockNumber;
					page_entries = 0;
					next_blkno++;
				}

				ue = (ShnswUpperEntry *)
					((char *) PageGetContents(upage) + page_entries * entry_size);
				ue->nid = bn->nid;
				n = (bn->n_neighbors && lev <= bn->level && bn->n_neighbors[lev] > 0)
					? Min(bn->n_neighbors[lev], M) : 0;
				ue->n_neighbors = n;
				ue->padding = 0;
				unbrs = ShnswUpperEntryNeighbors(ue);
				for (d = 0; d < n; d++)
					unbrs[d] = bn->neighbors[lev][d];
				for (d = n; d < M; d++)
					unbrs[d] = -1;

				page_entries++;
			}

			if (upage != NULL)
			{
				shnsw_flush_page(index, ubuf, upage);
				UnlockReleaseBuffer(ubuf);
				pfree(upage);
			}

			upper_npages[lev] = next_blkno - upper_starts[lev];
		}

		/* Write metapage with all page ranges now known */
		{
			Buffer		buf;
			Page		page;
			ShnswMetaPageData *meta;
			ShnswPageOpaque opaque;
			BlockNumber l0_start_blk = 1 + sq8_aux_npages;

			buf = ReadBuffer(index, SHNSW_METAPAGE_BLKNO);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			page = BufferGetPage(buf);

			PageInit(page, BLCKSZ, sizeof(ShnswPageOpaqueData));
			opaque = ShnswPageGetOpaque(page);
			opaque->shnsw_page_type = SHNSW_PAGE_META;
			opaque->shnsw_level = 0;
			opaque->shnsw_next = InvalidBlockNumber;

			meta = (ShnswMetaPageData *) PageGetContents(page);
			memset(meta, 0, sizeof(ShnswMetaPageData));
			meta->shnsw_magic = SORTED_HNSW_MAGIC;
			meta->shnsw_version = SORTED_HNSW_VERSION;
			meta->shnsw_flags = SHNSW_FLAG_SQ8_VALID;
			meta->shnsw_m = M;
			meta->shnsw_ef_construction = ef_construction;
			meta->shnsw_dim = dim;
			meta->shnsw_node_count = n_nodes;
			meta->shnsw_entry_nid = shnsw_build_entry_nid(graph);
			meta->shnsw_max_level = max_level;
			meta->shnsw_sq8_start = 1;
			meta->shnsw_sq8_npages = sq8_aux_npages;
			meta->shnsw_l0_start = l0_start_blk;
			meta->shnsw_l0_npages = upper_starts[1] > 0 ?
				upper_starts[1] - l0_start_blk :
				next_blkno - l0_start_blk;
			for (lev = 1; lev < SHNSW_MAX_LEVELS; lev++)
			{
				meta->shnsw_upper_start[lev] = upper_starts[lev];
				meta->shnsw_upper_npages[lev] = upper_npages[lev];
			}
			MarkBufferDirty(buf);
			log_newpage_buffer(buf, true);
			UnlockReleaseBuffer(buf);
		}
	}

	elog(NOTICE, "sorted_hnsw: build complete. %d nodes, %d L0 pages, %d SQ8 aux pages",
		 n_nodes, (int)(next_blkno - 1 - sq8_aux_npages), sq8_aux_npages);

	MemoryContextDelete(build_ctx);

	result = palloc0(sizeof(IndexBuildResult));
	result->heap_tuples = n_nodes;
	result->index_tuples = n_nodes;
	return result;
}

static void
shnsw_buildempty(Relation index)
{
	/* Write an empty metapage for an unlogged index */
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	ShnswMetaPageData *meta;
	ShnswPageOpaque opaque;

	buf = ReadBufferExtended(index, INIT_FORKNUM, P_NEW, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);

	PageInit(page, BLCKSZ, sizeof(ShnswPageOpaqueData));
	opaque = ShnswPageGetOpaque(page);
	opaque->shnsw_page_type = SHNSW_PAGE_META;
	opaque->shnsw_level = 0;
	opaque->shnsw_next = InvalidBlockNumber;

	meta = (ShnswMetaPageData *) PageGetContents(page);
	memset(meta, 0, sizeof(ShnswMetaPageData));
	meta->shnsw_magic = SORTED_HNSW_MAGIC;
	meta->shnsw_version = SORTED_HNSW_VERSION;
	meta->shnsw_entry_nid = -1;
	meta->shnsw_max_level = -1;

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/* ================================================================
 * Insert (aminsert)
 * ================================================================ */

static bool
shnsw_insert(Relation index, Datum *values, bool *isnull,
			  ItemPointer heap_tid, Relation heap,
			  IndexUniqueCheck checkUnique,
			  bool indexUnchanged,
			  IndexInfo *indexInfo)
{
	/* TODO: Phase 1 incremental insert
	 * 1. Read metapage for params and entry point
	 * 2. Quantize vector to SQ8
	 * 3. Pick random level
	 * 4. Greedy search from entry to target level
	 * 5. At each level: search ef_construction neighbors, select M best
	 * 6. Write new node to L0 page (and upper pages if level > 0)
	 * 7. Update neighbor lists (GenericXLog)
	 * 8. Update metapage if new entry point
	 */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("sorted_hnsw insert not yet implemented")));

	return false;
}

/* ================================================================
 * Scan
 * ================================================================ */

/* ---- Scan-local index cache ---- */

typedef struct ShnswCacheNode
{
	ItemPointerData heap_tid;
	int16		n_neighbors;	/* L0 neighbor count */
	int16		level;
	uint8		flags;
	int32	   *neighbors;		/* L0 neighbors */
	/* SQ8 vector is at sq8_data + nid * dim */
} ShnswCacheNode;

/* Upper level neighbor list for one node at one level */
typedef struct ShnswUpperNbr
{
	int32		nid;
	int16		n_neighbors;
	int32	   *neighbors;
} ShnswUpperNbr;

typedef struct ShnswScanCache
{
	/* From metapage */
	int			M;
	int			dim;
	int			n_nodes;
	int			entry_nid;
	int			max_level;
	int			ef_search;

	/* SQ8 data */
	float	   *sq8_mins;
	float	   *sq8_scales;

	/* L0 nodes */
	ShnswCacheNode *nodes;		/* array[n_nodes] */
	uint8	   *sq8_data;		/* n_nodes * dim */

	/* Upper levels: array per level */
	ShnswUpperNbr **upper;		/* upper[lev] = array of entries */
	int		   *upper_count;	/* count per level */

	/* NID lookup for upper levels */
	int		  **upper_nbr_idx;	/* upper_nbr_idx[lev][nid] = index into upper[lev], or -1 */
} ShnswScanCache;

/* ---- SQ8 distance computation ---- */

static float
sq8_cosine_distance(const float *query, int query_dim,
					const uint8 *sq8_vec,
					const float *mins, const float *scales, int dim)
{
	double	dot = 0.0, norm_q = 0.0, norm_v = 0.0;
	int		d;
	int		use_dim = Min(query_dim, dim);

	for (d = 0; d < use_dim; d++)
	{
		double qd = (double) query[d];
		double vd = (double) mins[d] + (double) sq8_vec[d] * (double) scales[d];

		dot += qd * vd;
		norm_q += qd * qd;
		norm_v += vd * vd;
	}

	if (norm_q == 0.0 || norm_v == 0.0)
		return 2.0f;
	return (float)(1.0 - dot / (sqrt(norm_q) * sqrt(norm_v)));
}

/* ---- Load index into scan cache ---- */

static ShnswScanCache *
shnsw_load_cache(Relation index)
{
	ShnswScanCache *cache;
	Buffer		buf;
	Page		page;
	ShnswMetaPageData *meta;
	int			M, dim, n_nodes;
	int			i, lev;
	BlockNumber	sq8_start;
	int			sq8_npages;
	BlockNumber	l0_start;
	int			l0_npages;
	BlockNumber upper_starts[SHNSW_MAX_LEVELS];
	int			upper_npages_arr[SHNSW_MAX_LEVELS];

	cache = palloc0(sizeof(ShnswScanCache));

	/* Read metapage */
	buf = ReadBuffer(index, SHNSW_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	meta = (ShnswMetaPageData *) PageGetContents(page);

	if (meta->shnsw_magic != SORTED_HNSW_MAGIC)
		ereport(ERROR,
				(errmsg("sorted_hnsw: invalid index magic (got 0x%08X, expected 0x%08X, page_contents offset=%zu)",
						meta->shnsw_magic, SORTED_HNSW_MAGIC,
						(size_t)((char *)meta - (char *)page))));

	cache->M = M = meta->shnsw_m;
	cache->dim = dim = meta->shnsw_dim;
	cache->n_nodes = n_nodes = meta->shnsw_node_count;
	cache->entry_nid = meta->shnsw_entry_nid;
	cache->max_level = meta->shnsw_max_level;
	cache->ef_search = sorted_hnsw_ef_search;

	sq8_start = meta->shnsw_sq8_start;
	sq8_npages = meta->shnsw_sq8_npages;
	l0_start = meta->shnsw_l0_start;
	l0_npages = meta->shnsw_l0_npages;

	for (lev = 0; lev < SHNSW_MAX_LEVELS; lev++)
	{
		upper_starts[lev] = meta->shnsw_upper_start[lev];
		upper_npages_arr[lev] = meta->shnsw_upper_npages[lev];
	}

	UnlockReleaseBuffer(buf);

	if (n_nodes == 0)
		return cache;

	/* Load SQ8 mins/scales */
	{
		Size	total = 2 * dim * sizeof(float);
		char   *sq8_raw = palloc(total);
		Size	offset = 0;
		Size	usable = BLCKSZ - MAXALIGN(SizeOfPageHeaderData) -
			MAXALIGN(sizeof(ShnswPageOpaqueData));

		for (i = 0; i < sq8_npages; i++)
		{
			Size	chunk;

			buf = ReadBuffer(index, sq8_start + i);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);
			chunk = Min(total - offset, usable);
			memcpy(sq8_raw + offset, PageGetContents(page), chunk);
			UnlockReleaseBuffer(buf);
			offset += chunk;
		}

		cache->sq8_mins = palloc(sizeof(float) * dim);
		cache->sq8_scales = palloc(sizeof(float) * dim);
		memcpy(cache->sq8_mins, sq8_raw, sizeof(float) * dim);
		memcpy(cache->sq8_scales, sq8_raw + sizeof(float) * dim,
			   sizeof(float) * dim);
		pfree(sq8_raw);
	}

	/* Load L0 nodes */
	cache->nodes = palloc0(sizeof(ShnswCacheNode) * n_nodes);
	cache->sq8_data = palloc(n_nodes * (Size)dim);

	{
		int		node_size = MAXALIGN(ShnswNodeSize(M, dim));
		int		nodes_per_page = ShnswL0NodesPerPage(M, dim);
		int		nid_loaded = 0;

		if (nodes_per_page < 1) nodes_per_page = 1;

		for (i = 0; i < l0_npages && nid_loaded < n_nodes; i++)
		{
			int		j;
			int		page_count;

			buf = ReadBuffer(index, l0_start + i);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);

			page_count = Min(nodes_per_page, n_nodes - nid_loaded);
			for (j = 0; j < page_count; j++)
			{
				ShnswNodeHeader *nh = (ShnswNodeHeader *)
					((char *) PageGetContents(page) + j * node_size);
				int32		nid = nh->nid;
				ShnswCacheNode *cn;
				int32	   *src_nbrs;
				int			k;

				if (nid < 0 || nid >= n_nodes)
					continue;

				cn = &cache->nodes[nid];
				ItemPointerCopy(&nh->heap_tid, &cn->heap_tid);
				cn->level = nh->level;
				cn->n_neighbors = nh->n_neighbors;
				cn->flags = nh->flags;

				/* Copy L0 neighbors */
				cn->neighbors = palloc(sizeof(int32) * 2 * M);
				src_nbrs = ShnswNodeNeighbors(nh);
				for (k = 0; k < 2 * M; k++)
					cn->neighbors[k] = src_nbrs[k];

				/* Copy SQ8 vector */
				memcpy(cache->sq8_data + (Size)nid * dim,
					   ShnswNodeSQ8Vec(nh, M), dim);

				nid_loaded++;
			}
			UnlockReleaseBuffer(buf);
		}
	}

	/* Load upper levels */
	cache->upper = palloc0(sizeof(ShnswUpperNbr *) * (cache->max_level + 1));
	cache->upper_count = palloc0(sizeof(int) * (cache->max_level + 1));
	cache->upper_nbr_idx = palloc0(sizeof(int *) * (cache->max_level + 1));

	for (lev = 1; lev <= cache->max_level; lev++)
	{
		int		entries_per_page = ShnswUpperEntriesPerPage(M);
		int		entry_size = MAXALIGN(ShnswUpperEntrySize(M));
		int		total_entries = 0;
		int		alloc = 256;
		int		p;
		ShnswUpperNbr *entries;

		if (entries_per_page < 1) entries_per_page = 1;

		entries = palloc(sizeof(ShnswUpperNbr) * alloc);

		for (p = 0; p < upper_npages_arr[lev]; p++)
		{
			int		j;
			int		page_count;

			buf = ReadBuffer(index, upper_starts[lev] + p);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);

			page_count = entries_per_page;  /* may read beyond, entries self-validate */

			for (j = 0; j < page_count; j++)
			{
				ShnswUpperEntry *ue = (ShnswUpperEntry *)
					((char *) PageGetContents(page) + j * entry_size);
				int		k;

				if (ue->nid < 0 || ue->nid >= n_nodes || ue->n_neighbors < 0)
					break;		/* end of valid entries on this page */

				if (total_entries >= alloc)
				{
					alloc *= 2;
					entries = repalloc(entries, sizeof(ShnswUpperNbr) * alloc);
				}

				entries[total_entries].nid = ue->nid;
				entries[total_entries].n_neighbors = ue->n_neighbors;
				entries[total_entries].neighbors = palloc(sizeof(int32) * M);
				for (k = 0; k < Min(ue->n_neighbors, M); k++)
					entries[total_entries].neighbors[k] =
						ShnswUpperEntryNeighbors(ue)[k];
				total_entries++;
			}
			UnlockReleaseBuffer(buf);
		}

		cache->upper[lev] = entries;
		cache->upper_count[lev] = total_entries;

		/* Build NID → index lookup */
		cache->upper_nbr_idx[lev] = palloc(sizeof(int) * n_nodes);
		memset(cache->upper_nbr_idx[lev], -1, sizeof(int) * n_nodes);
		for (i = 0; i < total_entries; i++)
			cache->upper_nbr_idx[lev][entries[i].nid] = i;
	}

	return cache;
}

/* ---- Graph search using scan cache ---- */

typedef struct ScanCandidate
{
	float		dist;
	int32		nid;
} ScanCandidate;

static int
cmp_candidate_asc(const void *a, const void *b)
{
	float da = ((const ScanCandidate *)a)->dist;
	float db = ((const ScanCandidate *)b)->dist;
	if (da < db) return -1;
	if (da > db) return 1;
	return 0;
}

/*
 * Greedy search at one level using SQ8 distances from the cache.
 * Returns sorted candidates (ascending distance).
 */
static int
shnsw_search_level(ShnswScanCache *cache, const float *query,
				   int entry_nid, int ef, int level,
				   ScanCandidate *results, int max_results)
{
	bool	   *visited;
	ScanCandidate *candidates;	/* min-sorted working set */
	ScanCandidate *best;		/* max-sorted result set */
	int			n_cand = 0, n_best = 0;
	int			dim = cache->dim;
	int			b;

	visited = palloc0(sizeof(bool) * cache->n_nodes);
	candidates = palloc(sizeof(ScanCandidate) * (ef * 4 + 1));
	best = palloc(sizeof(ScanCandidate) * (ef + 1));

	/* Seed with entry point */
	{
		float d = sq8_cosine_distance(query, dim,
									  cache->sq8_data + (Size)entry_nid * dim,
									  cache->sq8_mins, cache->sq8_scales, dim);
		candidates[0].dist = d;
		candidates[0].nid = entry_nid;
		n_cand = 1;
		best[0].dist = d;
		best[0].nid = entry_nid;
		n_best = 1;
		visited[entry_nid] = true;
	}

	while (n_cand > 0)
	{
		/* Pop nearest candidate */
		ScanCandidate nearest;
		float	furthest_dist;
		int		nn, k;
		int		min_idx = 0;
		for (k = 1; k < n_cand; k++)
			if (candidates[k].dist < candidates[min_idx].dist)
				min_idx = k;
		nearest = candidates[min_idx];
		candidates[min_idx] = candidates[--n_cand];

		/* Check termination */
		furthest_dist = best[0].dist;
		for (k = 1; k < n_best; k++)
			if (best[k].dist > furthest_dist)
				furthest_dist = best[k].dist;

		if (nearest.dist > furthest_dist && n_best >= ef)
			break;

		/* Get neighbors at this level */
		if (level == 0)
		{
			ShnswCacheNode *cn = &cache->nodes[nearest.nid];
			nn = cn->n_neighbors;
			for (k = 0; k < nn; k++)
			{
				int32 nbr = cn->neighbors[k];
				float nbr_dist;

				if (nbr < 0 || nbr >= cache->n_nodes || visited[nbr])
					continue;
				visited[nbr] = true;

				nbr_dist = sq8_cosine_distance(
					query, dim,
					cache->sq8_data + (Size)nbr * dim,
					cache->sq8_mins, cache->sq8_scales, dim);

				/* Update best set */
				furthest_dist = best[0].dist;
				for (b = 1; b < n_best; b++)
					if (best[b].dist > furthest_dist)
						furthest_dist = best[b].dist;

				if (nbr_dist < furthest_dist || n_best < ef)
				{
					candidates[n_cand].dist = nbr_dist;
					candidates[n_cand].nid = nbr;
					n_cand++;

					if (n_best < ef)
					{
						best[n_best].dist = nbr_dist;
						best[n_best].nid = nbr;
						n_best++;
					}
					else
					{
						/* Replace furthest in best */
						int worst = 0;
						for (b = 1; b < n_best; b++)
							if (best[b].dist > best[worst].dist)
								worst = b;
						best[worst].dist = nbr_dist;
						best[worst].nid = nbr;
					}
				}
			}
		}
		else
		{
			/* Upper level: use upper_nbr_idx lookup */
			int idx = cache->upper_nbr_idx[level][nearest.nid];
			if (idx >= 0)
			{
				ShnswUpperNbr *un = &cache->upper[level][idx];
				nn = un->n_neighbors;
				for (k = 0; k < nn; k++)
				{
					int32 nbr = un->neighbors[k];
					float nbr_dist;

					if (nbr < 0 || nbr >= cache->n_nodes || visited[nbr])
						continue;
					visited[nbr] = true;

					nbr_dist = sq8_cosine_distance(
						query, dim,
						cache->sq8_data + (Size)nbr * dim,
						cache->sq8_mins, cache->sq8_scales, dim);

					furthest_dist = best[0].dist;
					for (b = 1; b < n_best; b++)
						if (best[b].dist > furthest_dist)
							furthest_dist = best[b].dist;

					if (nbr_dist < furthest_dist || n_best < ef)
					{
						candidates[n_cand].dist = nbr_dist;
						candidates[n_cand].nid = nbr;
						n_cand++;

						if (n_best < ef)
						{
							best[n_best].dist = nbr_dist;
							best[n_best].nid = nbr;
							n_best++;
						}
						else
						{
							int worst = 0;
							for (b = 1; b < n_best; b++)
								if (best[b].dist > best[worst].dist)
									worst = b;
							best[worst].dist = nbr_dist;
							best[worst].nid = nbr;
						}
					}
				}
			}
		}
	}

	/* Copy best to results */
	{
	int ret;
	qsort(best, n_best, sizeof(ScanCandidate), cmp_candidate_asc);
	ret = Min(n_best, max_results);
	memcpy(results, best, sizeof(ScanCandidate) * ret);

	pfree(visited);
	pfree(candidates);
	pfree(best);

	return ret;
	}
}

/* ---- Result entry for exact rerank ---- */

typedef struct ScanResult
{
	ItemPointerData tid;
	float8		exact_dist;
} ScanResult;

static int
cmp_result_asc(const void *a, const void *b)
{
	float8 da = ((const ScanResult *)a)->exact_dist;
	float8 db = ((const ScanResult *)b)->exact_dist;
	if (da < db) return -1;
	if (da > db) return 1;
	return 0;
}

/* ================================================================
 * Scan callbacks
 * ================================================================ */

typedef struct ShnswScanOpaqueData
{
	/* Results: sorted by exact distance */
	int			n_results;
	int			result_idx;		/* next to return */
	ItemPointerData *result_tids;
	float8	   *result_dists;

	bool		first_call;
} ShnswScanOpaqueData;

typedef ShnswScanOpaqueData *ShnswScanOpaque;

static IndexScanDesc
shnsw_beginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	ShnswScanOpaque so;

	scan = RelationGetIndexScan(index, nkeys, norderbys);

	so = (ShnswScanOpaque) palloc0(sizeof(ShnswScanOpaqueData));
	so->first_call = true;
	scan->opaque = so;

	return scan;
}

static void
shnsw_rescan(IndexScanDesc scan, ScanKey keys, int nkeys,
			  ScanKey orderbys, int norderbys)
{
	ShnswScanOpaque so = (ShnswScanOpaque) scan->opaque;

	so->first_call = true;
	so->n_results = 0;
	so->result_idx = 0;

	if (norderbys > 0 && orderbys)
		memmove(scan->orderByData, orderbys,
				sizeof(ScanKeyData) * norderbys);
}

static bool
shnsw_gettuple(IndexScanDesc scan, ScanDirection direction)
{
	ShnswScanOpaque so = (ShnswScanOpaque) scan->opaque;

	if (so->first_call)
	{
		Relation		index = scan->indexRelation;
		Relation		heap;
		Svec		   *query;
		ShnswScanCache *cache;
		ScanCandidate  *candidates;
		ScanResult	   *results;
		int				n_cand;
		int				n_results;
		int				ef;
		int				ep_nid;
		int				level;
		int				i;

		so->first_call = false;

		/* Extract query vector from ORDER BY operator */
		if (scan->numberOfOrderBys < 1)
		{
			so->n_results = 0;
			goto done_search;
		}
		query = DatumGetSvecP(scan->orderByData[0].sk_argument);

		/* Load index into cache */
		cache = shnsw_load_cache(index);
		if (cache->n_nodes == 0)
		{
			so->n_results = 0;
			goto done_search;
		}

		ef = cache->ef_search;
		ep_nid = cache->entry_nid;

		/* Navigate upper levels (ef=1, greedy) */
		for (level = cache->max_level; level >= 1; level--)
		{
			ScanCandidate one;
			int found = shnsw_search_level(cache, query->x, ep_nid,
										   1, level, &one, 1);
			if (found > 0)
				ep_nid = one.nid;
		}

		/* Search L0 with full ef */
		candidates = palloc(sizeof(ScanCandidate) * ef);
		n_cand = shnsw_search_level(cache, query->x, ep_nid,
									ef, 0, candidates, ef);

		/* Exact rerank: fetch heap tuples, compute svec <=> distance */
		heap = table_open(index->rd_index->indrelid, AccessShareLock);
		results = palloc(sizeof(ScanResult) * n_cand);
		n_results = 0;

		for (i = 0; i < n_cand; i++)
		{
			int32			nid = candidates[i].nid;
			ItemPointerData	htid;
			TupleTableSlot *slot;
			bool			found;

			ItemPointerCopy(&cache->nodes[nid].heap_tid, &htid);

			slot = table_slot_create(heap, NULL);
			found = table_tuple_fetch_row_version(heap, &htid,
												  GetActiveSnapshot(),
												  slot);
			if (found)
			{
				bool	isnull;
				Datum	val;
				Svec   *sv;

				val = slot_getattr(slot,
								   index->rd_index->indkey.values[0],
								   &isnull);
				if (!isnull)
				{
					sv = DatumGetSvecP(val);
					results[n_results].exact_dist =
						svec_cosine_distance_internal(query, sv);
					ItemPointerCopy(&htid, &results[n_results].tid);
					n_results++;
					if (sv != (Svec *) DatumGetPointer(val))
						pfree(sv);
				}
			}
			ExecDropSingleTupleTableSlot(slot);
		}

		table_close(heap, AccessShareLock);

		/* Sort by exact distance */
		qsort(results, n_results, sizeof(ScanResult), cmp_result_asc);

		/* Store in scan state */
		so->n_results = n_results;
		so->result_idx = 0;
		so->result_tids = palloc(sizeof(ItemPointerData) * n_results);
		so->result_dists = palloc(sizeof(float8) * n_results);
		for (i = 0; i < n_results; i++)
		{
			ItemPointerCopy(&results[i].tid, &so->result_tids[i]);
			so->result_dists[i] = results[i].exact_dist;
		}

		pfree(results);
		pfree(candidates);
		/* cache is in CurrentMemoryContext, freed when scan ends */

done_search:
		;
	}

	/* Return next result */
	if (so->result_idx < so->n_results)
	{
		scan->xs_heaptid = so->result_tids[so->result_idx];
		scan->xs_recheck = false;

		if (scan->numberOfOrderBys > 0)
		{
			scan->xs_orderbyvals[0] =
				Float8GetDatum(so->result_dists[so->result_idx]);
			scan->xs_orderbynulls[0] = false;
		}

		so->result_idx++;
		return true;
	}

	return false;
}

static void
shnsw_endscan(IndexScanDesc scan)
{
	ShnswScanOpaque so = (ShnswScanOpaque) scan->opaque;

	if (so->result_tids)
		pfree(so->result_tids);
	if (so->result_dists)
		pfree(so->result_dists);

	pfree(so);
	scan->opaque = NULL;
}

/* ================================================================
 * Cost Estimation
 * ================================================================ */

static void
shnsw_costestimate(PlannerInfo *root, IndexPath *path,
					double loop_count,
					Cost *indexStartupCost,
					Cost *indexTotalCost,
					Selectivity *indexSelectivity,
					double *indexCorrelation,
					double *indexPages)
{
	RelOptInfo *rel = path->path.parent;
	Relation	index = index_open(path->indexinfo->indexoid, NoLock);
	int			dim;
	double		ef;
	double		visited;
	double		live_cand;
	double		nav_cpu, nav_io;
	double		heap_cpu, heap_io;
	double		toast_chunks, rerank_io, rerank_cpu;

	/* Get vector dimension from index attribute typmod */
	dim = TupleDescAttr(index->rd_att, 0)->atttypmod;
	if (dim <= 0)
		dim = 768;	/* fallback */

	ef = (double) sorted_hnsw_ef_search;
	visited = ef * 1.5;
	live_cand = ef;

	/* Phase 1: Index navigation (SQ8 distances) */
	nav_cpu = visited * dim * cpu_operator_cost / 100.0;
	nav_io = (visited / 2.0) * random_page_cost * 0.2;

	/* Phase 2: Heap fetch + visibility */
	heap_cpu = live_cand * cpu_tuple_cost;
	heap_io = live_cand * random_page_cost * 0.3;

	/* Phase 3: Exact rerank (TOAST detoast + cosine) */
	toast_chunks = ceil((double)(dim * 4) / 2000.0);
	rerank_io = live_cand * toast_chunks * random_page_cost * 0.5;
	rerank_cpu = live_cand * dim * cpu_operator_cost / 50.0;

	*indexStartupCost = 0;
	*indexTotalCost = nav_cpu + nav_io +
					  heap_cpu + heap_io +
					  rerank_io + rerank_cpu;

	/* Ordered scan returns at most ef_search rows */
	*indexSelectivity = Min(1.0, ef / Max(rel->tuples, 1.0));

	*indexCorrelation = 0.0;
	*indexPages = RelationGetNumberOfBlocks(index);

	index_close(index, NoLock);
}

/* ================================================================
 * Vacuum
 * ================================================================ */

static IndexBulkDeleteResult *
shnsw_bulkdelete(IndexVacuumInfo *info,
				  IndexBulkDeleteResult *stats,
				  IndexBulkDeleteCallback callback,
				  void *callback_state)
{
	/* TODO: lazy deletion — mark nodes with SHNSW_NODE_DELETED flag */
	if (stats == NULL)
		stats = palloc0(sizeof(IndexBulkDeleteResult));

	return stats;
}

static IndexBulkDeleteResult *
shnsw_vacuumcleanup(IndexVacuumInfo *info,
					 IndexBulkDeleteResult *stats)
{
	if (stats == NULL)
		stats = palloc0(sizeof(IndexBulkDeleteResult));

	/* Report index size */
	stats->num_pages = RelationGetNumberOfBlocks(info->index);

	return stats;
}

/* ================================================================
 * Validate
 * ================================================================ */

static bool
shnsw_validate(Oid opclassoid)
{
	/* TODO: verify opclass has the required support functions */
	return true;
}
