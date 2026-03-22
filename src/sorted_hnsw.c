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

static IndexBuildResult *
shnsw_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;

	/* TODO: Phase 1 HNSW construction
	 * 1. Scan heap, collect all (tid, svec) pairs
	 * 2. Build HNSW in memory (C port of Malkov & Yashunin 2018)
	 * 3. Compute SQ8 min/max
	 * 4. Write metapage, SQ8 aux pages, L0 pages, upper pages
	 */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("sorted_hnsw index build not yet implemented")));

	result = palloc0(sizeof(IndexBuildResult));
	result->heap_tuples = 0;
	result->index_tuples = 0;
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

typedef struct ShnswScanOpaqueData
{
	/* Query vector (detoasted, kept for duration of scan) */
	Svec	   *query;

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

	/* Reset state for new scan */
	so->first_call = true;
	so->n_results = 0;
	so->result_idx = 0;
	if (so->query)
	{
		pfree(so->query);
		so->query = NULL;
	}

	/* Store ORDER BY key for gettuple */
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
		/* TODO: Phase 1 scan implementation
		 * 1. Extract query vector from scan->orderByData[0]
		 * 2. Read metapage
		 * 3. Navigate upper levels using hsvec sketches
		 * 4. Navigate L0 using SQ8 distances
		 * 5. Collect ef_search candidates
		 * 6. For each candidate:
		 *    a. Fetch heap tuple, check visibility
		 *    b. If visible: compute exact svec <=> distance
		 * 7. Sort by exact distance
		 * 8. Store in so->result_tids/result_dists
		 */
		so->first_call = false;

		/* Placeholder: no results until build is implemented */
		so->n_results = 0;
		so->result_idx = 0;
	}

	/* Return next result */
	if (so->result_idx < so->n_results)
	{
		scan->xs_heaptid = so->result_tids[so->result_idx];
		scan->xs_recheck = false;	/* distance is exact */

		if (scan->numberOfOrderBys > 0)
			scan->xs_orderbyvals[0] =
				Float8GetDatum(so->result_dists[so->result_idx]);
		scan->xs_orderbynulls[0] = false;

		so->result_idx++;
		return true;
	}

	return false;
}

static void
shnsw_endscan(IndexScanDesc scan)
{
	ShnswScanOpaque so = (ShnswScanOpaque) scan->opaque;

	if (so->query)
		pfree(so->query);
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
