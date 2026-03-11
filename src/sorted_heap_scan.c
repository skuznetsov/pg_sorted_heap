/*
 * sorted_heap_scan.c
 *
 * Custom scan provider for sorted_heap zone map pruning.
 *
 * Hooks into the planner via set_rel_pathlist_hook. When a query has
 * WHERE predicates on the first PK column of a sorted_heap table whose
 * zone map is valid (after COMPACT/REBUILD), we offer a CustomScan path
 * that restricts the heap scan to only matching blocks using
 * heap_setscanlimits().
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/stratnum.h"
#include "access/tableam.h"
#include "catalog/pg_am.h"
#include "catalog/pg_opclass.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#if PG_VERSION_NUM >= 180000
#include "commands/explain_format.h"
#endif
#include "executor/executor.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/pathnodes.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/restrictinfo.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "funcapi.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "access/parallel.h"

#include "sorted_heap.h"

/* Marker strategy for runtime IN-list array (Param) in runtime_meta */
#define SH_RUNTIME_IN_ARRAY  (-1)

/* ----------------------------------------------------------------
 *  Bounds extracted from WHERE clause
 * ---------------------------------------------------------------- */
typedef struct SortedHeapScanBounds
{
	bool		has_lo;
	bool		has_hi;
	bool		lo_inclusive;
	bool		hi_inclusive;
	int64		lo;
	int64		hi;
	/* Column 2 bounds (composite PK) */
	bool		has_lo2;
	bool		has_hi2;
	bool		lo2_inclusive;
	bool		hi2_inclusive;
	int64		lo2;
	int64		hi2;
} SortedHeapScanBounds;

/* ----------------------------------------------------------------
 *  Custom scan state
 * ---------------------------------------------------------------- */
typedef struct SortedHeapScanState
{
	CustomScanState css;
	TableScanDesc	heap_scan;
	SortedHeapScanBounds bounds;
	SortedHeapRelInfo *relinfo;
	BlockNumber		total_blocks;
	BlockNumber		scan_start;
	BlockNumber		scan_nblocks;
	/* Two-pass scan: sorted prefix + conservative tail */
	BlockNumber		prefix_pages;		/* sorted prefix length (entries) */
	BlockNumber		prefix_start;		/* prefix scan range */
	BlockNumber		prefix_nblocks;
	BlockNumber		tail_start;			/* tail scan range */
	BlockNumber		tail_nblocks;
	bool			in_tail_phase;		/* currently scanning tail */
	bool			two_pass;			/* prefix + tail mode active */
	/* Per-scan stats for EXPLAIN ANALYZE */
	BlockNumber		scanned_blocks;
	BlockNumber		pruned_blocks;
	BlockNumber		last_blk;			/* track block transitions */
	/* Parallel support: PG's parallel table scan descriptor in DSM */
	ParallelTableScanDesc pscan;		/* NULL for serial scans */
	/* Runtime parameter resolution (Path B — prepared statements) */
	bool			runtime_bounds;		/* true if bounds have Param nodes */
	int				n_runtime_exprs;
	List		   *runtime_exprstates;	/* ExprState* list */
	int			   *runtime_strategies;
	bool		   *runtime_is_col2;
	Oid			   *runtime_typids;
	SortedHeapScanBounds const_bounds;	/* Const-only baseline for rescan */
	bool			runtime_resolve_pending;	/* defer initial ParamExec resolution */
	/* IN-list values for per-block pruning (sorted, col1 only) */
	int				n_in_values;
	int64		   *in_values;		/* sorted array, NULL if no IN clause */
	/* Zone map generation at scan start — detect mid-scan invalidation */
	uint64			zm_gen_at_start;
	bool			zm_stale;		/* set if generation changed mid-scan */
} SortedHeapScanState;

/* ----------------------------------------------------------------
 *  Forward declarations
 * ---------------------------------------------------------------- */
static void sorted_heap_set_rel_pathlist(PlannerInfo *root,
										 RelOptInfo *rel,
										 Index rti,
										 RangeTblEntry *rte);
static bool sorted_heap_extract_bounds(RelOptInfo *rel,
									   AttrNumber pk_attno,
									   Oid pk_typid,
									   AttrNumber pk_attno2,
									   Oid pk_typid2,
									   SortedHeapScanBounds *bounds,
									   List **runtime_exprs,
									   List **runtime_meta,
									   List **pk_clauses,
									   List **in_values_out);
static void sorted_heap_apply_bound(SortedHeapScanBounds *bounds,
									int strategy, bool is_col2, int64 val);
static void sorted_heap_resolve_runtime_bounds(SortedHeapScanState *shstate);
static int sorted_heap_int64_cmp(const void *a, const void *b);
static void sorted_heap_compute_block_range(SortedHeapRelInfo *info,
											SortedHeapScanBounds *bounds,
											BlockNumber total_blocks,
											BlockNumber *start_block,
											BlockNumber *nblocks);
static bool sorted_heap_compute_two_pass_ranges(SortedHeapRelInfo *info,
												SortedHeapScanBounds *bounds,
												BlockNumber prefix_pages,
												BlockNumber total_blocks,
												BlockNumber *prefix_start,
												BlockNumber *prefix_nblocks,
												BlockNumber *tail_start,
												BlockNumber *tail_nblocks);
static bool sorted_heap_zone_overlaps(SortedHeapZoneMapEntry *e,
									  SortedHeapScanBounds *bounds);
static bool sorted_heap_exprs_need_deferred_runtime_resolve(List *exprs);

/* CustomPath callback */
static Plan *sorted_heap_plan_custom_path(PlannerInfo *root,
										  RelOptInfo *rel,
										  struct CustomPath *best_path,
										  List *tlist,
										  List *clauses,
										  List *custom_plans);

/* CustomScan callbacks */
static Node *sorted_heap_create_scan_state(CustomScan *cscan);
static void sorted_heap_begin_custom_scan(CustomScanState *node,
										  EState *estate, int eflags);
static TupleTableSlot *sorted_heap_scan_next(ScanState *ss);
#if PG_VERSION_NUM >= 180000
static bool sorted_heap_scan_recheck(ScanState *ss, TupleTableSlot *slot);
#endif
static TupleTableSlot *sorted_heap_exec_custom_scan(CustomScanState *node);
static void sorted_heap_end_custom_scan(CustomScanState *node);
static void sorted_heap_rescan_custom_scan(CustomScanState *node);
static void sorted_heap_explain_custom_scan(CustomScanState *node,
											List *ancestors,
											ExplainState *es);

/* Parallel support */
static Size sorted_heap_estimate_dsm(CustomScanState *node,
									 ParallelContext *pcxt);
static void sorted_heap_initialize_dsm(CustomScanState *node,
									   ParallelContext *pcxt,
									   void *coordinate);
static void sorted_heap_reinitialize_dsm(CustomScanState *node,
										 ParallelContext *pcxt,
										 void *coordinate);
static void sorted_heap_initialize_worker(CustomScanState *node,
										  shm_toc *toc,
										  void *coordinate);

/* ----------------------------------------------------------------
 *  Static state
 * ---------------------------------------------------------------- */
/* GUC: allow users to disable scan pruning at runtime */
bool sorted_heap_enable_scan_pruning = true;

/* Shared memory stats (cluster-wide when in shared_preload_libraries) */
static SortedHeapSharedStats *sh_shared_stats = NULL;

/* Backend-local fallback stats (used when shmem not available) */
static uint64 sh_local_scans = 0;
static uint64 sh_local_blocks_scanned = 0;
static uint64 sh_local_blocks_pruned = 0;

/* Hook chains */
static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook = NULL;
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static CustomPathMethods sorted_heap_path_methods = {
	.CustomName = "SortedHeapScan",
	.PlanCustomPath = sorted_heap_plan_custom_path,
};

static CustomScanMethods sorted_heap_plan_methods = {
	.CustomName = "SortedHeapScan",
	.CreateCustomScanState = sorted_heap_create_scan_state,
};

static CustomExecMethods sorted_heap_exec_methods = {
	.CustomName = "SortedHeapScan",
	.BeginCustomScan = sorted_heap_begin_custom_scan,
	.ExecCustomScan = sorted_heap_exec_custom_scan,
	.EndCustomScan = sorted_heap_end_custom_scan,
	.ReScanCustomScan = sorted_heap_rescan_custom_scan,
	.EstimateDSMCustomScan = sorted_heap_estimate_dsm,
	.InitializeDSMCustomScan = sorted_heap_initialize_dsm,
	.ReInitializeDSMCustomScan = sorted_heap_reinitialize_dsm,
	.InitializeWorkerCustomScan = sorted_heap_initialize_worker,
	.ExplainCustomScan = sorted_heap_explain_custom_scan,
};

/* ----------------------------------------------------------------
 *  Shared memory hooks
 * ---------------------------------------------------------------- */
static void
sorted_heap_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
	RequestAddinShmemSpace(MAXALIGN(sizeof(SortedHeapSharedStats)));
}

static void
sorted_heap_shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	sh_shared_stats = ShmemInitStruct("sorted_heap stats",
									  sizeof(SortedHeapSharedStats),
									  &found);
	if (!found)
	{
		pg_atomic_init_u64(&sh_shared_stats->total_scans, 0);
		pg_atomic_init_u64(&sh_shared_stats->blocks_scanned, 0);
		pg_atomic_init_u64(&sh_shared_stats->blocks_pruned, 0);
		pg_atomic_init_u64(&sh_shared_stats->zm_generation, 1);
	}
}

/*
 * Zone map generation counter — cross-backend cache invalidation.
 * Bumped by any zone map mutation; checked in sorted_heap_get_relinfo()
 * to detect stale per-backend caches.
 */
void
sorted_heap_bump_zm_generation(void)
{
	if (sh_shared_stats)
		pg_atomic_fetch_add_u64(&sh_shared_stats->zm_generation, 1);
}

uint64
sorted_heap_read_zm_generation(void)
{
	if (sh_shared_stats)
		return pg_atomic_read_u64(&sh_shared_stats->zm_generation);
	return 0;
}

static bool
sorted_heap_exprs_need_deferred_runtime_resolve(List *exprs)
{
	ListCell   *lc;

	foreach(lc, exprs)
	{
		Node   *expr = (Node *) lfirst(lc);

		if (expr != NULL &&
			IsA(expr, Param) &&
			((Param *) expr)->paramkind == PARAM_EXEC)
			return true;
	}

	return false;
}

/* ----------------------------------------------------------------
 *  Initialization — called from _PG_init()
 * ---------------------------------------------------------------- */
void
sorted_heap_scan_init(void)
{
	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = sorted_heap_set_rel_pathlist;
	RegisterCustomScanMethods(&sorted_heap_plan_methods);

	/* Shared memory hooks (only effective via shared_preload_libraries) */
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = sorted_heap_shmem_request;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = sorted_heap_shmem_startup;
}

/* ----------------------------------------------------------------
 *  Planner hook: offer SortedHeapScan path when applicable
 * ---------------------------------------------------------------- */
static void
sorted_heap_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
							 Index rti, RangeTblEntry *rte)
{
	Relation			table_rel;
	SortedHeapRelInfo  *info;
	SortedHeapScanBounds bounds;
	BlockNumber			start_block, nblocks, total_blocks;
	CustomPath		   *cpath;
	double				sel;
	bool				skip_narrow_dml;

	/* Chain to previous hook */
	if (prev_set_rel_pathlist_hook)
		prev_set_rel_pathlist_hook(root, rel, rti, rte);

	/* GUC kill switch */
	if (!sorted_heap_enable_scan_pruning)
		return;

	/* Only base relations with restrictions */
	if (rel->reloptkind != RELOPT_BASEREL)
		return;
	if (rte->rtekind != RTE_RELATION)
		return;
	if (rel->baserestrictinfo == NIL)
		return;

	/* Check if this is a sorted_heap table */
	table_rel = table_open(rte->relid, NoLock);
	if (table_rel->rd_tableam != &sorted_heap_am_routine)
	{
		table_close(table_rel, NoLock);
		return;
	}

	/* Load relinfo and check zone map validity */
	info = sorted_heap_get_relinfo(table_rel);
	if (!info->zm_usable || !info->zm_loaded || info->zm_total_entries == 0)
	{
		table_close(table_rel, NoLock);
		return;
	}

	if (!info->zm_scan_valid)
	{
		table_close(table_rel, NoLock);
		return;
	}

	/* Extract PK bounds from baserestrictinfo */
	{
		List	   *runtime_exprs = NIL;
		List	   *runtime_meta = NIL;
		List	   *pk_clauses = NIL;
		List	   *in_values = NIL;

		if (!sorted_heap_extract_bounds(rel, info->attNums[0],
										info->zm_pk_typid,
										info->zm_col2_usable ?
										info->attNums[1] : 0,
										info->zm_pk_typid2,
										&bounds,
										&runtime_exprs,
										&runtime_meta,
										&pk_clauses,
										&in_values))
		{
			table_close(table_rel, NoLock);
			return;
		}

		total_blocks = RelationGetNumberOfBlocks(table_rel);
		table_close(table_rel, NoLock);

		if (total_blocks <= 1)
			return;

		/*
		 * For UPDATE/DELETE targeting this relation, skip CustomScan when the
		 * block range is narrow (<=4 blocks).  IndexScan's direct TID access
		 * beats CustomScan's per-block filtering for point DML, while
		 * CustomScan's sequential I/O pattern still wins for wider ranges.
		 */
		skip_narrow_dml = (root->parse->commandType != CMD_SELECT &&
						   (int) rti == root->parse->resultRelation);

		/* Create CustomPath */
		cpath = makeNode(CustomPath);
		cpath->path.type = T_CustomPath;
		cpath->path.pathtype = T_CustomScan;
		cpath->path.parent = rel;
		cpath->path.pathtarget = rel->reltarget;
		cpath->path.param_info = NULL;
		cpath->path.parallel_aware = false;
		cpath->path.parallel_safe = rel->consider_parallel;
		cpath->path.parallel_workers = 0;
		cpath->path.pathkeys = NIL;
		cpath->flags = 0;
		cpath->methods = &sorted_heap_path_methods;

		if (runtime_exprs == NIL)
		{
			/* Path A: all Const — compute block range now */
			BlockNumber prefix_pages_local;
			BlockNumber pfx_start = 0, pfx_nblks = 0;
			BlockNumber tail_start_local = 0, tail_nblks = 0;
			bool		use_two_pass = false;

			prefix_pages_local = sorted_heap_detect_sorted_prefix(info);

			/*
			 * Try two-pass scan when we have a sorted prefix but the
			 * table also has unsorted tail pages (not fully sorted).
			 */
			if (prefix_pages_local > 0 &&
				prefix_pages_local < info->zm_total_entries + 1 &&
				!info->zm_sorted)
			{
				use_two_pass =
					sorted_heap_compute_two_pass_ranges(info, &bounds,
														prefix_pages_local,
														total_blocks,
														&pfx_start, &pfx_nblks,
														&tail_start_local,
														&tail_nblks);
			}

			if (use_two_pass)
			{
				nblocks = pfx_nblks + tail_nblks;
				start_block = pfx_start;	/* for fallback compat */
			}
			else
			{
				sorted_heap_compute_block_range(info, &bounds, total_blocks,
												&start_block, &nblocks);
				prefix_pages_local = 0;		/* signal: single-pass */
			}

			if (nblocks >= total_blocks)
				return;

			if (skip_narrow_dml && nblocks <= 4)
				return;

			sel = (double) nblocks / (double) total_blocks;
			cpath->path.rows = rel->rows;
			cpath->path.startup_cost = random_page_cost;
			cpath->path.total_cost = cpath->path.startup_cost +
				seq_page_cost * nblocks +
				cpu_tuple_cost * rel->tuples * sel +
				cpu_operator_cost * rel->tuples * sel;

			if (use_two_pass)
			{
				/* Add small cost for scan restart between passes */
				cpath->path.total_cost += random_page_cost;
			}

			/* Pack range + bounds into custom_private */
			{
				List *range_list = NIL;
				List *bounds_list = NIL;

				range_list = lappend_int(range_list, (int32) start_block);
				range_list = lappend_int(range_list, (int32) nblocks);
				range_list = lappend_int(range_list, (int32) total_blocks);
				/* Two-pass fields (4 extra ints) */
				range_list = lappend_int(range_list, (int32) prefix_pages_local);
				range_list = lappend_int(range_list, (int32) pfx_start);
				range_list = lappend_int(range_list, (int32) pfx_nblks);
				range_list = lappend_int(range_list, (int32) tail_start_local);
				range_list = lappend_int(range_list, (int32) tail_nblks);

				bounds_list = lappend_int(bounds_list, bounds.has_lo ? 1 : 0);
				bounds_list = lappend_int(bounds_list, bounds.has_hi ? 1 : 0);
				bounds_list = lappend_int(bounds_list, bounds.lo_inclusive ? 1 : 0);
				bounds_list = lappend_int(bounds_list, bounds.hi_inclusive ? 1 : 0);
				bounds_list = lappend_int(bounds_list, (int32) (bounds.lo >> 32));
				bounds_list = lappend_int(bounds_list, (int32) (bounds.lo & 0xFFFFFFFF));
				bounds_list = lappend_int(bounds_list, (int32) (bounds.hi >> 32));
				bounds_list = lappend_int(bounds_list, (int32) (bounds.hi & 0xFFFFFFFF));

				bounds_list = lappend_int(bounds_list, bounds.has_lo2 ? 1 : 0);
				bounds_list = lappend_int(bounds_list, bounds.has_hi2 ? 1 : 0);
				bounds_list = lappend_int(bounds_list, bounds.lo2_inclusive ? 1 : 0);
				bounds_list = lappend_int(bounds_list, bounds.hi2_inclusive ? 1 : 0);
				bounds_list = lappend_int(bounds_list, (int32) (bounds.lo2 >> 32));
				bounds_list = lappend_int(bounds_list, (int32) (bounds.lo2 & 0xFFFFFFFF));
				bounds_list = lappend_int(bounds_list, (int32) (bounds.hi2 >> 32));
				bounds_list = lappend_int(bounds_list, (int32) (bounds.hi2 & 0xFFFFFFFF));

				cpath->custom_private = list_make3(range_list, bounds_list,
											  in_values);
			}
		}
		else
		{
			/* Path B: has Params — defer block range to executor */
			Selectivity		pk_sel;
			List		   *meta_list = NIL;
			List		   *const_bounds_list = NIL;

			pk_sel = clauselist_selectivity(root, pk_clauses,
											0, JOIN_INNER, NULL);
			nblocks = (BlockNumber) clamp_row_est(total_blocks * pk_sel);
			if (nblocks < 1)
				nblocks = 1;

			if (skip_narrow_dml && nblocks <= 4)
				return;

			sel = (double) nblocks / (double) total_blocks;
			cpath->path.rows = rel->rows;
			cpath->path.startup_cost = random_page_cost;
			cpath->path.total_cost = cpath->path.startup_cost +
				seq_page_cost * nblocks +
				cpu_tuple_cost * rel->tuples * sel +
				cpu_operator_cost * rel->tuples * sel;

			/* Meta: [total_blocks, n_runtime_exprs] */
			meta_list = lappend_int(meta_list, (int32) total_blocks);
			meta_list = lappend_int(meta_list,
									list_length(runtime_exprs));

			/* Pack Const-only bounds (baseline for mixed Const+Param) */
			const_bounds_list = lappend_int(const_bounds_list, bounds.has_lo ? 1 : 0);
			const_bounds_list = lappend_int(const_bounds_list, bounds.has_hi ? 1 : 0);
			const_bounds_list = lappend_int(const_bounds_list, bounds.lo_inclusive ? 1 : 0);
			const_bounds_list = lappend_int(const_bounds_list, bounds.hi_inclusive ? 1 : 0);
			const_bounds_list = lappend_int(const_bounds_list, (int32) (bounds.lo >> 32));
			const_bounds_list = lappend_int(const_bounds_list, (int32) (bounds.lo & 0xFFFFFFFF));
			const_bounds_list = lappend_int(const_bounds_list, (int32) (bounds.hi >> 32));
			const_bounds_list = lappend_int(const_bounds_list, (int32) (bounds.hi & 0xFFFFFFFF));

			const_bounds_list = lappend_int(const_bounds_list, bounds.has_lo2 ? 1 : 0);
			const_bounds_list = lappend_int(const_bounds_list, bounds.has_hi2 ? 1 : 0);
			const_bounds_list = lappend_int(const_bounds_list, bounds.lo2_inclusive ? 1 : 0);
			const_bounds_list = lappend_int(const_bounds_list, bounds.hi2_inclusive ? 1 : 0);
			const_bounds_list = lappend_int(const_bounds_list, (int32) (bounds.lo2 >> 32));
			const_bounds_list = lappend_int(const_bounds_list, (int32) (bounds.lo2 & 0xFFFFFFFF));
			const_bounds_list = lappend_int(const_bounds_list, (int32) (bounds.hi2 >> 32));
			const_bounds_list = lappend_int(const_bounds_list, (int32) (bounds.hi2 & 0xFFFFFFFF));

			cpath->custom_private = list_make5(meta_list, runtime_meta,
											   const_bounds_list,
											   runtime_exprs,
											   in_values);
		}
	}

	/*
	 * Save fields we need for the parallel path BEFORE add_path(),
	 * because add_path() may pfree(cpath) if it's dominated by an
	 * existing path (e.g. SeqScan).  Using cpath after add_path()
	 * would be a use-after-free.
	 */
	{
		List	   *saved_custom_private = cpath->custom_private;
		Cardinality	saved_rows = cpath->path.rows;
		Cost		saved_total_cost = cpath->path.total_cost;

		add_path(rel, &cpath->path);
		/* cpath may be freed here — do NOT dereference it below */

		/* Also offer a parallel partial path if beneficial */
		if (rel->consider_parallel && nblocks > 0)
		{
			int		pw;

			pw = compute_parallel_worker(rel, (double) nblocks, -1,
										 max_parallel_workers_per_gather);
			if (pw > 0)
			{
				CustomPath *ppath = makeNode(CustomPath);

				ppath->path.type = T_CustomPath;
				ppath->path.pathtype = T_CustomScan;
				ppath->path.parent = rel;
				ppath->path.pathtarget = rel->reltarget;
				ppath->path.param_info = NULL;
				ppath->path.parallel_aware = true;
				ppath->path.parallel_safe = true;
				ppath->path.parallel_workers = pw;
				ppath->path.pathkeys = NIL;

				/* Per-worker cost: divide total among participants */
				ppath->path.rows = saved_rows;
				ppath->path.startup_cost = 0;
				ppath->path.total_cost = saved_total_cost / (pw + 1);

				ppath->flags = 0;
				ppath->methods = &sorted_heap_path_methods;
				ppath->custom_private = saved_custom_private;

				add_partial_path(rel, &ppath->path);
			}
		}
	}
}

/* ----------------------------------------------------------------
 *  Apply a single bound (strategy + value) to a SortedHeapScanBounds.
 *  Shared by plan-time Const extraction and runtime Param resolution.
 * ---------------------------------------------------------------- */
static void
sorted_heap_apply_bound(SortedHeapScanBounds *bounds,
						int strategy, bool is_col2, int64 val)
{
	if (!is_col2)
	{
		switch (strategy)
		{
			case BTEqualStrategyNumber:
				bounds->has_lo = true;
				bounds->lo = val;
				bounds->lo_inclusive = true;
				bounds->has_hi = true;
				bounds->hi = val;
				bounds->hi_inclusive = true;
				break;
			case BTLessStrategyNumber:
				if (!bounds->has_hi || val < bounds->hi ||
					(val == bounds->hi && bounds->hi_inclusive))
				{
					bounds->has_hi = true;
					bounds->hi = val;
					bounds->hi_inclusive = false;
				}
				break;
			case BTLessEqualStrategyNumber:
				if (!bounds->has_hi || val < bounds->hi)
				{
					bounds->has_hi = true;
					bounds->hi = val;
					bounds->hi_inclusive = true;
				}
				break;
			case BTGreaterStrategyNumber:
				if (!bounds->has_lo || val > bounds->lo ||
					(val == bounds->lo && bounds->lo_inclusive))
				{
					bounds->has_lo = true;
					bounds->lo = val;
					bounds->lo_inclusive = false;
				}
				break;
			case BTGreaterEqualStrategyNumber:
				if (!bounds->has_lo || val > bounds->lo)
				{
					bounds->has_lo = true;
					bounds->lo = val;
					bounds->lo_inclusive = true;
				}
				break;
			default:
				break;
		}
	}
	else
	{
		switch (strategy)
		{
			case BTEqualStrategyNumber:
				bounds->has_lo2 = true;
				bounds->lo2 = val;
				bounds->lo2_inclusive = true;
				bounds->has_hi2 = true;
				bounds->hi2 = val;
				bounds->hi2_inclusive = true;
				break;
			case BTLessStrategyNumber:
				if (!bounds->has_hi2 || val < bounds->hi2 ||
					(val == bounds->hi2 && bounds->hi2_inclusive))
				{
					bounds->has_hi2 = true;
					bounds->hi2 = val;
					bounds->hi2_inclusive = false;
				}
				break;
			case BTLessEqualStrategyNumber:
				if (!bounds->has_hi2 || val < bounds->hi2)
				{
					bounds->has_hi2 = true;
					bounds->hi2 = val;
					bounds->hi2_inclusive = true;
				}
				break;
			case BTGreaterStrategyNumber:
				if (!bounds->has_lo2 || val > bounds->lo2 ||
					(val == bounds->lo2 && bounds->lo2_inclusive))
				{
					bounds->has_lo2 = true;
					bounds->lo2 = val;
					bounds->lo2_inclusive = false;
				}
				break;
			case BTGreaterEqualStrategyNumber:
				if (!bounds->has_lo2 || val > bounds->lo2)
				{
					bounds->has_lo2 = true;
					bounds->lo2 = val;
					bounds->lo2_inclusive = true;
				}
				break;
			default:
				break;
		}
	}
}

/* ----------------------------------------------------------------
 *  Extract PK bounds from baserestrictinfo
 * ---------------------------------------------------------------- */
static bool
sorted_heap_extract_bounds(RelOptInfo *rel, AttrNumber pk_attno,
						   Oid pk_typid, AttrNumber pk_attno2,
						   Oid pk_typid2,
						   SortedHeapScanBounds *bounds,
						   List **runtime_exprs,
						   List **runtime_meta,
						   List **pk_clauses_out,
						   List **in_values_out)
{
	ListCell   *lc;
	Oid			opfamily;
	Oid			opcid;
	Oid			opfamily2 = InvalidOid;

	memset(bounds, 0, sizeof(SortedHeapScanBounds));
	*runtime_exprs = NIL;
	*runtime_meta = NIL;
	*pk_clauses_out = NIL;
	*in_values_out = NIL;

	/* Get btree opfamily for column 1 */
	opcid = GetDefaultOpClass(pk_typid, BTREE_AM_OID);
	if (!OidIsValid(opcid))
		return false;
	opfamily = get_opclass_family(opcid);
	if (!OidIsValid(opfamily))
		return false;

	/* Get btree opfamily for column 2 (if available) */
	if (OidIsValid(pk_typid2) && pk_attno2 != 0)
	{
		Oid		opcid2 = GetDefaultOpClass(pk_typid2, BTREE_AM_OID);

		if (OidIsValid(opcid2))
			opfamily2 = get_opclass_family(opcid2);
	}

	foreach(lc, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		OpExpr	   *opexpr;
		Var		   *var;
		Node	   *val_node;
		int			strategy;
		bool		varonleft;
		bool		is_const;
		bool		is_col2 = false;
		Oid			match_typid;

		/* Handle IN / = ANY(array) on PK column 1 */
		if (IsA(rinfo->clause, ScalarArrayOpExpr))
		{
			ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) rinfo->clause;
			Var		   *saop_var;
			Node	   *arr_node;
			ArrayType  *arr;
			Datum	   *elems;
			bool	   *nulls;
			int			nelems;
			int			saop_strategy;
			int			k;

			/* Only handle ANY (OR semantics), not ALL */
			if (!saop->useOr)
				continue;

			/* args: [0] = scalar (Var), [1] = array */
			if (list_length(saop->args) != 2)
				continue;
			if (!IsA(linitial(saop->args), Var))
				continue;
			saop_var = (Var *) linitial(saop->args);

			/* Must match PK column 1 */
			if (saop_var->varattno != pk_attno)
				continue;

			/* Must be equality operator */
			saop_strategy = get_op_opfamily_strategy(saop->opno, opfamily);
			if (saop_strategy != BTEqualStrategyNumber)
				continue;

			/* Array: Const (plan-time) or Param (deferred to executor) */
			arr_node = (Node *) lsecond(saop->args);
			if (IsA(arr_node, Param))
			{
				/* Param array — defer to executor (Path B) */
				*runtime_exprs = lappend(*runtime_exprs, arr_node);
				*runtime_meta = lappend_int(*runtime_meta, SH_RUNTIME_IN_ARRAY);
				*runtime_meta = lappend_int(*runtime_meta, 0);
				*runtime_meta = lappend_int(*runtime_meta, (int) pk_typid);
				*pk_clauses_out = lappend(*pk_clauses_out, rinfo);
				continue;
			}
			if (!IsA(arr_node, Const))
				continue;
			if (((Const *) arr_node)->constisnull)
				continue;

			arr = DatumGetArrayTypeP(((Const *) arr_node)->constvalue);
			{
				int16	typlen;
				bool	typbyval;
				char	typalign;

				get_typlenbyvalalign(pk_typid, &typlen, &typbyval, &typalign);
				deconstruct_array(arr, pk_typid, typlen, typbyval, typalign,
								  &elems, &nulls, &nelems);
			}

			if (nelems < 1)
				continue;

			/* Convert all non-null elements to int64, compute bounding box */
			{
				int64	val_min = PG_INT64_MAX;
				int64	val_max = PG_INT64_MIN;
				List   *vals = NIL;

				for (k = 0; k < nelems; k++)
				{
					int64	iv;

					if (nulls[k])
						continue;
					if (!sorted_heap_key_to_int64(elems[k], pk_typid, &iv))
						continue;
					vals = lappend_int(vals, (int32)(iv >> 32));
					vals = lappend_int(vals, (int32)(iv & 0xFFFFFFFF));
					if (iv < val_min) val_min = iv;
					if (iv > val_max) val_max = iv;
				}

				if (vals == NIL)
					continue;

				/* Apply bounding box as lo/hi bounds */
				sorted_heap_apply_bound(bounds, BTEqualStrategyNumber,
										false, val_min);
				if (val_max != val_min)
				{
					/* Widen to range [val_min, val_max] inclusive */
					bounds->hi = val_max;
					bounds->hi_inclusive = true;
				}

				*in_values_out = vals;
			}

			*pk_clauses_out = lappend(*pk_clauses_out, rinfo);
			continue;
		}

		if (!IsA(rinfo->clause, OpExpr))
			continue;

		opexpr = (OpExpr *) rinfo->clause;
		if (list_length(opexpr->args) != 2)
			continue;

		/* Check for Var op {Const|Param} or {Const|Param} op Var */
		if (IsA(linitial(opexpr->args), Var) &&
			(IsA(lsecond(opexpr->args), Const) ||
			 IsA(lsecond(opexpr->args), Param)))
		{
			var = (Var *) linitial(opexpr->args);
			val_node = (Node *) lsecond(opexpr->args);
			varonleft = true;
		}
		else if ((IsA(linitial(opexpr->args), Const) ||
				  IsA(linitial(opexpr->args), Param)) &&
				 IsA(lsecond(opexpr->args), Var))
		{
			val_node = (Node *) linitial(opexpr->args);
			var = (Var *) lsecond(opexpr->args);
			varonleft = false;
		}
		else
			continue;

		is_const = IsA(val_node, Const);

		/* Match to PK column 1 or column 2 */
		if (var->varattno == pk_attno)
		{
			is_col2 = false;
			match_typid = pk_typid;
		}
		else if (pk_attno2 != 0 && var->varattno == pk_attno2 &&
				 OidIsValid(opfamily2))
		{
			is_col2 = true;
			match_typid = pk_typid2;
		}
		else
			continue;

		if (is_const && ((Const *) val_node)->constisnull)
			continue;

		/* Determine btree strategy */
		strategy = get_op_opfamily_strategy(opexpr->opno,
											is_col2 ? opfamily2 : opfamily);
		if (strategy == 0)
			continue;

		/* If var is on right, flip strategy */
		if (!varonleft)
		{
			switch (strategy)
			{
				case BTLessStrategyNumber:
					strategy = BTGreaterStrategyNumber;
					break;
				case BTLessEqualStrategyNumber:
					strategy = BTGreaterEqualStrategyNumber;
					break;
				case BTGreaterStrategyNumber:
					strategy = BTLessStrategyNumber;
					break;
				case BTGreaterEqualStrategyNumber:
					strategy = BTLessEqualStrategyNumber;
					break;
			}
		}

		/* Collect matching RestrictInfo for selectivity estimation */
		*pk_clauses_out = lappend(*pk_clauses_out, rinfo);

		if (is_const)
		{
			/* Const: resolve at plan time */
			int64	int_val;

			if (!sorted_heap_key_to_int64(((Const *) val_node)->constvalue,
										  match_typid, &int_val))
				continue;
			sorted_heap_apply_bound(bounds, strategy, is_col2, int_val);
		}
		else
		{
			/* Param: defer to executor */
			*runtime_exprs = lappend(*runtime_exprs, val_node);
			*runtime_meta = lappend_int(*runtime_meta, strategy);
			*runtime_meta = lappend_int(*runtime_meta, is_col2 ? 1 : 0);
			*runtime_meta = lappend_int(*runtime_meta, (int) match_typid);
		}
	}

	return bounds->has_lo || bounds->has_hi ||
		   bounds->has_lo2 || bounds->has_hi2 ||
		   *runtime_exprs != NIL;
}

/* ----------------------------------------------------------------
 *  Binary search helpers for monotonic zone maps.
 *
 *  After compact, zone map entries have non-decreasing zme_min and
 *  zme_max values (data is physically sorted).  This enables O(log N)
 *  block range computation instead of O(N) linear scan.
 * ---------------------------------------------------------------- */

/*
 * Find first entry index where zme_max >= lo (or > lo if !inclusive).
 * Returns count if no such entry exists.
 */
static uint32
zm_bsearch_first(SortedHeapRelInfo *info, int64 lo, bool inclusive,
				 uint32 count)
{
	uint32	low = 0, high = count;

	while (low < high)
	{
		uint32	mid = low + (high - low) / 2;
		SortedHeapZoneMapEntry *e = sorted_heap_get_zm_entry(info, mid);
		bool	below;

		below = inclusive ? (e->zme_max < lo) : (e->zme_max <= lo);
		if (below)
			low = mid + 1;
		else
			high = mid;
	}
	return low;
}

/*
 * Find one-past-last entry index where zme_min <= hi (or < hi if !inclusive).
 * Returns 0 if no such entry exists.
 */
static uint32
zm_bsearch_last(SortedHeapRelInfo *info, int64 hi, bool inclusive,
				uint32 count)
{
	uint32	low = 0, high = count;

	while (low < high)
	{
		uint32	mid = low + (high - low) / 2;
		SortedHeapZoneMapEntry *e = sorted_heap_get_zm_entry(info, mid);
		bool	above;

		above = inclusive ? (e->zme_min > hi) : (e->zme_min >= hi);
		if (above)
			high = mid;
		else
			low = mid + 1;
	}
	return low;		/* one-past-last matching index */
}

/* ----------------------------------------------------------------
 *  Resolve runtime bounds at executor startup (Path B).
 *
 *  Evaluates Param expressions, merges with Const-only baseline bounds,
 *  and computes the block range from the zone map.
 * ---------------------------------------------------------------- */
static void
sorted_heap_resolve_runtime_bounds(SortedHeapScanState *shstate)
{
	ExprContext *econtext = shstate->css.ss.ps.ps_ExprContext;
	Relation	rel = shstate->css.ss.ss_currentRelation;
	ListCell   *lc;
	int			i;

	/* Start from Const-only baseline */
	shstate->bounds = shstate->const_bounds;

	/* Reset any previous IN-values (rescan may call this again) */
	if (shstate->in_values)
	{
		pfree(shstate->in_values);
		shstate->in_values = NULL;
	}
	shstate->n_in_values = 0;

	/* Evaluate each runtime Param expression and apply bound */
	i = 0;
	foreach(lc, shstate->runtime_exprstates)
	{
		ExprState  *exprstate = (ExprState *) lfirst(lc);
		bool		isnull;
		Datum		val;
		int64		int_val;

		val = ExecEvalExprSwitchContext(exprstate, econtext, &isnull);
		if (isnull)
		{
			i++;
			continue;
		}

		/* IN-list array Param: deconstruct and compute bounding box */
		if (shstate->runtime_strategies[i] == SH_RUNTIME_IN_ARRAY)
		{
			ArrayType  *arr = DatumGetArrayTypeP(val);
			Datum	   *elems;
			bool	   *nulls;
			int			nelems, k, nvalid = 0;
			int64		val_min = PG_INT64_MAX, val_max = PG_INT64_MIN;
			Oid			typid = shstate->runtime_typids[i];
			int16		typlen;
			bool		typbyval;
			char		typalign;

			get_typlenbyvalalign(typid, &typlen, &typbyval, &typalign);
			deconstruct_array(arr, typid, typlen, typbyval, typalign,
							  &elems, &nulls, &nelems);

			if (nelems > 0)
			{
				int64  *vals = palloc(sizeof(int64) * nelems);

				for (k = 0; k < nelems; k++)
				{
					int64	iv;

					if (nulls[k])
						continue;
					if (!sorted_heap_key_to_int64(elems[k], typid, &iv))
						continue;
					vals[nvalid++] = iv;
					if (iv < val_min) val_min = iv;
					if (iv > val_max) val_max = iv;
				}

				if (nvalid > 0)
				{
					sorted_heap_apply_bound(&shstate->bounds,
											BTEqualStrategyNumber,
											false, val_min);
					if (val_max != val_min)
					{
						shstate->bounds.hi = val_max;
						shstate->bounds.hi_inclusive = true;
					}

					qsort(vals, nvalid, sizeof(int64),
						  sorted_heap_int64_cmp);
					shstate->in_values = vals;
					shstate->n_in_values = nvalid;
				}
				else
				{
					pfree(vals);
				}
			}
			i++;
			continue;
		}

		/* Regular scalar Param: convert and apply as bound */
		if (sorted_heap_key_to_int64(val, shstate->runtime_typids[i], &int_val))
		{
			sorted_heap_apply_bound(&shstate->bounds,
									shstate->runtime_strategies[i],
									shstate->runtime_is_col2[i],
									int_val);
		}
		i++;
	}

	/* Compute block range from zone map using current relation size */
	shstate->total_blocks = RelationGetNumberOfBlocks(rel);
	sorted_heap_compute_block_range(shstate->relinfo, &shstate->bounds,
									shstate->total_blocks,
									&shstate->scan_start,
									&shstate->scan_nblocks);
}

/* ----------------------------------------------------------------
 *  Compute block range from zone map
 * ---------------------------------------------------------------- */
static void
sorted_heap_compute_block_range(SortedHeapRelInfo *info,
								SortedHeapScanBounds *bounds,
								BlockNumber total_blocks,
								BlockNumber *start_block,
								BlockNumber *nblocks)
{
	BlockNumber		first_match = total_blocks;
	BlockNumber		last_match = 0;
	uint32			i;
	uint32			zm_entries_count = info->zm_total_entries;
	BlockNumber		data_blocks;

	/*
	 * Compute effective data page count by excluding meta page and
	 * overflow pages from total_blocks.
	 */
	data_blocks = (total_blocks > 1 + info->zm_overflow_npages) ?
		total_blocks - 1 - info->zm_overflow_npages : 0;

	if (info->zm_sorted)
	{
		/*
		 * Binary search: O(log N) for monotonic zone map.
		 * Column 2 pruning is not applied here; the executor handles
		 * per-block column 2 checks during scan.
		 */
		uint32	first_idx = 0;
		uint32	last_idx_excl = zm_entries_count;

		if (bounds->has_lo)
			first_idx = zm_bsearch_first(info, bounds->lo,
										 bounds->lo_inclusive,
										 zm_entries_count);
		if (bounds->has_hi)
			last_idx_excl = zm_bsearch_last(info, bounds->hi,
											bounds->hi_inclusive,
											zm_entries_count);

		if (first_idx < last_idx_excl)
		{
			first_match = first_idx + 1;	/* +1 for meta page */
			last_match = last_idx_excl;		/* one-past = last block */
		}
	}
	else
	{
		/* Linear scan: O(N) fallback for non-monotonic zone map */
		for (i = 0; i < zm_entries_count; i++)
		{
			SortedHeapZoneMapEntry *e = sorted_heap_get_zm_entry(info, i);

			if (e->zme_min == PG_INT64_MAX)
				continue;			/* empty page */

			if (!sorted_heap_zone_overlaps(e, bounds))
				continue;			/* zone map says no match */

			if ((BlockNumber)(i + 1) < first_match)
				first_match = i + 1;	/* +1 for meta page */
			last_match = i + 1;
		}
	}

	/*
	 * Handle data pages beyond zone map capacity.  These have unknown
	 * content, so we must include them unless the upper bound falls
	 * entirely within the covered range.
	 */
	if (zm_entries_count < data_blocks)
	{
		bool		uncovered_safe_to_skip = false;
		BlockNumber first_uncovered = (BlockNumber) zm_entries_count + 1;

		/*
		 * Optimisation for sorted data: if the zone map is monotonic and
		 * the last covered entry has a finite max, and the query's upper
		 * bound is at or below that max, uncovered pages (which hold
		 * higher values) can't match.  Without monotonicity guarantee,
		 * uncovered pages may contain any values (e.g. inserts that
		 * landed on new pages with values within the covered range).
		 */
		if (info->zm_sorted && bounds->has_hi && zm_entries_count > 0)
		{
			SortedHeapZoneMapEntry *last_e =
				sorted_heap_get_zm_entry(info, zm_entries_count - 1);
			int64	last_max = last_e->zme_max;

			if (last_max != PG_INT64_MAX &&
				(bounds->hi_inclusive ? bounds->hi <= last_max
									 : bounds->hi < last_max))
				uncovered_safe_to_skip = true;
		}

		if (!uncovered_safe_to_skip)
		{
			/*
			 * Must scan all uncovered pages.  Use total_blocks - 1
			 * instead of data_blocks because new data pages can be
			 * appended after overflow pages by heap.  Overflow pages
			 * have no tuples (pd_lower == pd_upper) so scanning them
			 * is harmless.
			 */
			BlockNumber last_block = total_blocks - 1;

			if (first_uncovered < first_match)
				first_match = first_uncovered;
			if (last_block > last_match)
				last_match = last_block;
		}
	}

	if (first_match >= total_blocks)
	{
		/* No blocks match — minimal scan that finds nothing */
		*start_block = 1;
		*nblocks = 0;
	}
	else
	{
		*start_block = first_match;
		*nblocks = last_match - first_match + 1;
	}
}

/* ----------------------------------------------------------------
 *  Compute two-pass scan ranges: sorted prefix + conservative tail.
 *
 *  When the zone map has a sorted prefix (entries 0..prefix_pages-1
 *  are monotonically sorted) but the table also has unsorted tail
 *  pages, we split the scan into two contiguous passes:
 *    1. prefix pass: binary search on sorted entries → tight range
 *    2. tail pass: linear scan on remaining entries + uncovered pages
 *
 *  Returns true if two-pass mode is beneficial (prefix > 0 and
 *  tail exists). Returns false if single-pass is sufficient.
 * ---------------------------------------------------------------- */
static bool
sorted_heap_compute_two_pass_ranges(SortedHeapRelInfo *info,
									SortedHeapScanBounds *bounds,
									BlockNumber prefix_pages,
									BlockNumber total_blocks,
									BlockNumber *prefix_start,
									BlockNumber *prefix_nblocks,
									BlockNumber *tail_start,
									BlockNumber *tail_nblocks)
{
	BlockNumber		pfx_first = total_blocks;
	BlockNumber		pfx_last = 0;
	BlockNumber		tail_first = total_blocks;
	BlockNumber		tail_last = 0;
	uint32			zm_entries_count = info->zm_total_entries;
	BlockNumber		data_blocks;
	uint32			i;

	*prefix_start = 1;
	*prefix_nblocks = 0;
	*tail_start = 1;
	*tail_nblocks = 0;

	data_blocks = (total_blocks > 1 + info->zm_overflow_npages) ?
		total_blocks - 1 - info->zm_overflow_npages : 0;

	/*
	 * Phase 1: binary search over sorted prefix entries [0..prefix_pages-1].
	 */
	{
		uint32	pfx_limit = Min(prefix_pages, zm_entries_count);

		if (pfx_limit > 0)
		{
			uint32	pfx_first_idx = 0;
			uint32	pfx_last_idx = pfx_limit;

			if (bounds->has_lo)
				pfx_first_idx = zm_bsearch_first(info, bounds->lo,
												  bounds->lo_inclusive,
												  pfx_limit);
			if (bounds->has_hi)
				pfx_last_idx = zm_bsearch_last(info, bounds->hi,
												bounds->hi_inclusive,
												pfx_limit);

			if (pfx_first_idx < pfx_last_idx)
			{
				pfx_first = pfx_first_idx + 1;		/* +1 for meta page */
				pfx_last = pfx_last_idx;			/* one-past = last block */
			}
		}
	}

	/*
	 * Phase 2: linear scan over tail entries [prefix_pages..zm_entries_count-1].
	 */
	for (i = prefix_pages; i < zm_entries_count; i++)
	{
		SortedHeapZoneMapEntry *e = sorted_heap_get_zm_entry(info, i);

		if (e->zme_min == PG_INT64_MAX)
			continue;

		if (!sorted_heap_zone_overlaps(e, bounds))
			continue;

		if ((BlockNumber)(i + 1) < tail_first)
			tail_first = i + 1;
		tail_last = i + 1;
	}

	/*
	 * Phase 3: uncovered pages beyond zone map entries.
	 * These must go into the tail pass (unknown content).
	 */
	if (zm_entries_count < data_blocks)
	{
		BlockNumber first_uncovered = (BlockNumber) zm_entries_count + 1;
		BlockNumber last_block = total_blocks - 1;

		if (first_uncovered < tail_first)
			tail_first = first_uncovered;
		if (last_block > tail_last)
			tail_last = last_block;
	}

	/* Compute prefix range */
	if (pfx_first < total_blocks && pfx_last > 0)
	{
		*prefix_start = pfx_first;
		*prefix_nblocks = pfx_last - pfx_first + 1;
	}

	/* Compute tail range */
	if (tail_first < total_blocks && tail_last > 0)
	{
		*tail_start = tail_first;
		*tail_nblocks = tail_last - tail_first + 1;
	}

	return (*prefix_nblocks > 0 && *tail_nblocks > 0);
}

/* ----------------------------------------------------------------
 *  Check if a zone map entry overlaps with scan bounds
 * ---------------------------------------------------------------- */
static bool
sorted_heap_zone_overlaps(SortedHeapZoneMapEntry *e,
						  SortedHeapScanBounds *bounds)
{
	if (e->zme_min == PG_INT64_MAX)
		return false;

	/* Check column 1 lower bound: skip if entire page is below lo */
	if (bounds->has_lo)
	{
		if (bounds->lo_inclusive)
		{
			if (e->zme_max < bounds->lo)
				return false;
		}
		else
		{
			if (e->zme_max <= bounds->lo)
				return false;
		}
	}

	/* Check column 1 upper bound: skip if entire page is above hi */
	if (bounds->has_hi)
	{
		if (bounds->hi_inclusive)
		{
			if (e->zme_min > bounds->hi)
				return false;
		}
		else
		{
			if (e->zme_min >= bounds->hi)
				return false;
		}
	}

	/*
	 * Check column 2 bounds (AND semantics).
	 * Skip page if col2 data is tracked and proves no overlap.
	 * If col2 not tracked (sentinel), skip this check.
	 */
	if (e->zme_min2 != PG_INT64_MAX)
	{
		if (bounds->has_lo2)
		{
			if (bounds->lo2_inclusive)
			{
				if (e->zme_max2 < bounds->lo2)
					return false;
			}
			else
			{
				if (e->zme_max2 <= bounds->lo2)
					return false;
			}
		}

		if (bounds->has_hi2)
		{
			if (bounds->hi2_inclusive)
			{
				if (e->zme_min2 > bounds->hi2)
					return false;
			}
			else
			{
				if (e->zme_min2 >= bounds->hi2)
					return false;
			}
		}
	}

	return true;
}

/* ----------------------------------------------------------------
 *  qsort comparator for int64 (used to sort IN-values)
 * ---------------------------------------------------------------- */
static int
sorted_heap_int64_cmp(const void *a, const void *b)
{
	int64	va = *(const int64 *) a;
	int64	vb = *(const int64 *) b;

	if (va < vb) return -1;
	if (va > vb) return 1;
	return 0;
}

/* ----------------------------------------------------------------
 *  Check if any IN-value falls within a zone map entry's range.
 *  Values must be pre-sorted.  O(log K) per block via binary search.
 * ---------------------------------------------------------------- */
static bool
zone_overlaps_in_values(SortedHeapZoneMapEntry *e,
						int64 *values, int nvalues)
{
	int		lo = 0, hi = nvalues;

	if (e->zme_min == PG_INT64_MAX)
		return false;

	/* Binary search: find first value >= zme_min */
	while (lo < hi)
	{
		int mid = lo + (hi - lo) / 2;

		if (values[mid] < e->zme_min)
			lo = mid + 1;
		else
			hi = mid;
	}

	/* If that value exists and <= zme_max, there's overlap */
	return (lo < nvalues && values[lo] <= e->zme_max);
}

/* ----------------------------------------------------------------
 *  PlanCustomPath: convert CustomPath to CustomScan plan node
 * ---------------------------------------------------------------- */
static Plan *
sorted_heap_plan_custom_path(PlannerInfo *root, RelOptInfo *rel,
							 struct CustomPath *best_path,
							 List *tlist, List *clauses,
							 List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);

	cscan->scan.scanrelid = rel->relid;
	cscan->flags = best_path->flags;

	/*
	 * Detect Path B (runtime bounds): move runtime_exprs from
	 * custom_private[3] to custom_exprs so PG deep-copies Param
	 * nodes for generic plan caching.
	 *
	 * Path A: custom_private has 3 elements (range_list, bounds_list,
	 *         in_values)
	 * Path B: custom_private has 5 elements (meta, runtime_meta,
	 *         const_bounds, runtime_exprs, in_values)
	 */
	{
		if (list_length(best_path->custom_private) == 5)
		{
			cscan->custom_exprs = (List *) lfourth(best_path->custom_private);
			cscan->custom_private = list_make4(linitial(best_path->custom_private),
											   lsecond(best_path->custom_private),
											   lthird(best_path->custom_private),
											   list_nth(best_path->custom_private, 4));
		}
		else
		{
			cscan->custom_private = best_path->custom_private;
		}
	}

	cscan->custom_scan_tlist = NIL;
	cscan->custom_plans = NIL;
	cscan->scan.plan.targetlist = tlist;
	cscan->scan.plan.qual = extract_actual_clauses(clauses, false);
	cscan->methods = &sorted_heap_plan_methods;

	return &cscan->scan.plan;
}

/* ----------------------------------------------------------------
 *  CustomScan state creation
 * ---------------------------------------------------------------- */
static Node *
sorted_heap_create_scan_state(CustomScan *cscan)
{
	SortedHeapScanState *shstate;

	shstate = (SortedHeapScanState *) newNode(sizeof(SortedHeapScanState),
											  T_CustomScanState);
	shstate->css.methods = &sorted_heap_exec_methods;
	shstate->css.slotOps = &TTSOpsBufferHeapTuple;
	return (Node *) &shstate->css;
}

/* ----------------------------------------------------------------
 *  BeginCustomScan
 * ---------------------------------------------------------------- */
static void
sorted_heap_begin_custom_scan(CustomScanState *node, EState *estate,
							  int eflags)
{
	SortedHeapScanState *shstate = (SortedHeapScanState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	Relation	rel = node->ss.ss_currentRelation;

	/* Load relinfo for per-block zone map checks */
	shstate->relinfo = sorted_heap_get_relinfo(rel);

	/* Init per-scan stats and parallel state */
	shstate->scanned_blocks = 0;
	shstate->pruned_blocks = 0;
	shstate->last_blk = InvalidBlockNumber;
	shstate->pscan = NULL;
	shstate->runtime_bounds = false;
	shstate->runtime_resolve_pending = false;
	shstate->zm_gen_at_start = sorted_heap_read_zm_generation();
	shstate->zm_stale = false;

	/*
	 * Path A: custom_private has 3 elements (range_list, bounds_list,
	 *         in_values)
	 * Path B: custom_private has 4 elements (meta_list, runtime_meta,
	 *         const_bounds_list, in_values) — runtime_exprs moved to
	 *         custom_exprs by plan_custom_path.
	 */
	if (list_length(cscan->custom_private) == 4)
	{
		/*
		 * Path B: runtime bounds with Param nodes.
		 * custom_private = list_make4(meta, runtime_meta, const_bounds,
		 *                             in_values)
		 * custom_exprs = runtime Expr* nodes (Param/Const)
		 */
		List	   *meta_list = (List *) linitial(cscan->custom_private);
		List	   *runtime_meta = (List *) lsecond(cscan->custom_private);
		List	   *const_bounds_list = (List *) lthird(cscan->custom_private);
		int			n_runtime;
		int			i;
		ListCell   *lc;

		shstate->runtime_bounds = true;
		shstate->total_blocks = (BlockNumber) linitial_int(meta_list);
		n_runtime = lsecond_int(meta_list);
		shstate->n_runtime_exprs = n_runtime;

		/* Initialize ExprStates from custom_exprs */
		shstate->runtime_exprstates =
			ExecInitExprList(cscan->custom_exprs, &node->ss.ps);

		/* Unpack runtime metadata: 3 ints per expression (strategy, is_col2, typid) */
		shstate->runtime_strategies = palloc(sizeof(int) * n_runtime);
		shstate->runtime_is_col2 = palloc(sizeof(bool) * n_runtime);
		shstate->runtime_typids = palloc(sizeof(Oid) * n_runtime);

		i = 0;
		lc = list_head(runtime_meta);
		while (i < n_runtime && lc != NULL)
		{
			shstate->runtime_strategies[i] = lfirst_int(lc);
			lc = lnext(runtime_meta, lc);
			shstate->runtime_is_col2[i] = lfirst_int(lc) != 0;
			lc = lnext(runtime_meta, lc);
			shstate->runtime_typids[i] = (Oid) lfirst_int(lc);
			lc = lnext(runtime_meta, lc);
			i++;
		}

		/* Unpack Const-only baseline bounds */
		shstate->const_bounds.has_lo = list_nth_int(const_bounds_list, 0) != 0;
		shstate->const_bounds.has_hi = list_nth_int(const_bounds_list, 1) != 0;
		shstate->const_bounds.lo_inclusive = list_nth_int(const_bounds_list, 2) != 0;
		shstate->const_bounds.hi_inclusive = list_nth_int(const_bounds_list, 3) != 0;
		shstate->const_bounds.lo = ((int64) list_nth_int(const_bounds_list, 4) << 32) |
			((int64) (uint32) list_nth_int(const_bounds_list, 5));
		shstate->const_bounds.hi = ((int64) list_nth_int(const_bounds_list, 6) << 32) |
			((int64) (uint32) list_nth_int(const_bounds_list, 7));

		if (list_length(const_bounds_list) >= 16)
		{
			shstate->const_bounds.has_lo2 = list_nth_int(const_bounds_list, 8) != 0;
			shstate->const_bounds.has_hi2 = list_nth_int(const_bounds_list, 9) != 0;
			shstate->const_bounds.lo2_inclusive = list_nth_int(const_bounds_list, 10) != 0;
			shstate->const_bounds.hi2_inclusive = list_nth_int(const_bounds_list, 11) != 0;
			shstate->const_bounds.lo2 = ((int64) list_nth_int(const_bounds_list, 12) << 32) |
				((int64) (uint32) list_nth_int(const_bounds_list, 13));
			shstate->const_bounds.hi2 = ((int64) list_nth_int(const_bounds_list, 14) << 32) |
				((int64) (uint32) list_nth_int(const_bounds_list, 15));
		}
		else
		{
			shstate->const_bounds.has_lo2 = false;
			shstate->const_bounds.has_hi2 = false;
		}

		/*
		 * PARAM_EXEC values from NestLoop/LATERAL are not set yet during
		 * BeginCustomScan.  Defer their first resolution until rescan/execution.
		 */
		if (sorted_heap_exprs_need_deferred_runtime_resolve(cscan->custom_exprs))
			shstate->runtime_resolve_pending = true;
		else
			sorted_heap_resolve_runtime_bounds(shstate);
	}
	else
	{
		/*
		 * Path A: all Const bounds — block range computed at plan time.
		 * custom_private = list_make3(range_list, bounds_list, in_values)
		 */
		List	   *range_list = (List *) linitial(cscan->custom_private);
		List	   *bounds_list = (List *) lsecond(cscan->custom_private);

		shstate->scan_start = (BlockNumber) linitial_int(range_list);
		shstate->scan_nblocks = (BlockNumber) lsecond_int(range_list);
		shstate->total_blocks = (BlockNumber) lthird_int(range_list);

		/* Two-pass fields (present when range_list has 8 elements) */
		if (list_length(range_list) >= 8)
		{
			BlockNumber pp = (BlockNumber) list_nth_int(range_list, 3);

			shstate->prefix_pages = pp;
			shstate->prefix_start = (BlockNumber) list_nth_int(range_list, 4);
			shstate->prefix_nblocks = (BlockNumber) list_nth_int(range_list, 5);
			shstate->tail_start = (BlockNumber) list_nth_int(range_list, 6);
			shstate->tail_nblocks = (BlockNumber) list_nth_int(range_list, 7);
			shstate->two_pass = (pp > 0 && shstate->prefix_nblocks > 0 &&
								 shstate->tail_nblocks > 0);
		}
		else
		{
			shstate->two_pass = false;
			shstate->prefix_pages = 0;
		}
		shstate->in_tail_phase = false;

		/* Extract bounds */
		shstate->bounds.has_lo = list_nth_int(bounds_list, 0) != 0;
		shstate->bounds.has_hi = list_nth_int(bounds_list, 1) != 0;
		shstate->bounds.lo_inclusive = list_nth_int(bounds_list, 2) != 0;
		shstate->bounds.hi_inclusive = list_nth_int(bounds_list, 3) != 0;
		shstate->bounds.lo = ((int64) list_nth_int(bounds_list, 4) << 32) |
			((int64) (uint32) list_nth_int(bounds_list, 5));
		shstate->bounds.hi = ((int64) list_nth_int(bounds_list, 6) << 32) |
			((int64) (uint32) list_nth_int(bounds_list, 7));

		/* Column 2 bounds (indices 8-15) */
		if (list_length(bounds_list) >= 16)
		{
			shstate->bounds.has_lo2 = list_nth_int(bounds_list, 8) != 0;
			shstate->bounds.has_hi2 = list_nth_int(bounds_list, 9) != 0;
			shstate->bounds.lo2_inclusive = list_nth_int(bounds_list, 10) != 0;
			shstate->bounds.hi2_inclusive = list_nth_int(bounds_list, 11) != 0;
			shstate->bounds.lo2 = ((int64) list_nth_int(bounds_list, 12) << 32) |
				((int64) (uint32) list_nth_int(bounds_list, 13));
			shstate->bounds.hi2 = ((int64) list_nth_int(bounds_list, 14) << 32) |
				((int64) (uint32) list_nth_int(bounds_list, 15));
		}
		else
		{
			shstate->bounds.has_lo2 = false;
			shstate->bounds.has_hi2 = false;
		}
	}

	/*
	 * Unpack IN-values from custom_private (last element in both paths).
	 * Packed as pairs of int32 (hi32, lo32) representing int64 values.
	 */
	{
		List   *in_vals_packed;
		int		packed_len;

		if (list_length(cscan->custom_private) == 4)
			in_vals_packed = (List *) lfourth(cscan->custom_private);
		else
			in_vals_packed = (List *) lthird(cscan->custom_private);

		packed_len = list_length(in_vals_packed);
		if (packed_len >= 2)
		{
			int		nvals = packed_len / 2;
			int		i;

			shstate->in_values = palloc(sizeof(int64) * nvals);
			shstate->n_in_values = nvals;

			for (i = 0; i < nvals; i++)
			{
				int32 hi = list_nth_int(in_vals_packed, i * 2);
				int32 lo = list_nth_int(in_vals_packed, i * 2 + 1);

				shstate->in_values[i] = ((int64) hi << 32) |
					((int64) (uint32) lo);
			}

			/* Sort for binary search in zone_overlaps_in_values */
			qsort(shstate->in_values, nvals, sizeof(int64),
				  sorted_heap_int64_cmp);
		}
	}

	/*
	 * For parallel-aware scans, defer scan creation to the DSM
	 * callbacks (InitializeDSM / InitializeWorker) which will open a
	 * coordinated parallel scan.  For serial scans, open the heap scan
	 * now and restrict it to the pruned block range.
	 */
	if (cscan->scan.plan.parallel_aware)
	{
		shstate->heap_scan = NULL;
	}
	else
	{
		shstate->heap_scan = table_beginscan(rel, estate->es_snapshot,
											 0, NULL);
		if (shstate->runtime_bounds && shstate->runtime_resolve_pending)
		{
			/* Full scan for now; first rescan/execution will narrow it. */
		}
		else if (shstate->two_pass)
		{
			/* Start with prefix pass */
			heap_setscanlimits(shstate->heap_scan,
							   shstate->prefix_start,
							   shstate->prefix_nblocks);
		}
		else if (shstate->scan_nblocks > 0)
			heap_setscanlimits(shstate->heap_scan,
							   shstate->scan_start,
							   shstate->scan_nblocks);
		else
			heap_setscanlimits(shstate->heap_scan, 1, 0);
	}
}

/* ----------------------------------------------------------------
 *  Scan access method — return next zone-map-qualified scan tuple.
 *
 *  Called by ExecScan() as the "access method" callback.  Returns raw
 *  scan tuples from the heap with zone-map block pruning applied.
 *  Qual evaluation and projection are handled by ExecScan itself.
 * ---------------------------------------------------------------- */
static TupleTableSlot *
sorted_heap_scan_next(ScanState *ss)
{
	CustomScanState *node = (CustomScanState *) ss;
	SortedHeapScanState *shstate = (SortedHeapScanState *) node;
	TupleTableSlot *slot = ss->ss_ScanTupleSlot;
	SortedHeapRelInfo *info = shstate->relinfo;
	BlockNumber	current_nblocks;
	bool		skip_zone_check;
	bool		pk_prefilter;

	/*
	 * Determine the active scan range for skip_zone_check sizing.
	 * In two-pass mode, use the current phase's block count.
	 */
	current_nblocks = shstate->two_pass
		? (shstate->in_tail_phase ? shstate->tail_nblocks
								  : shstate->prefix_nblocks)
		: shstate->scan_nblocks;

	skip_zone_check = (current_nblocks <= 4 && shstate->n_in_values == 0);

	/*
	 * PK-ordered pre-filter: tuples within each page are in PK order
	 * when the data is sorted.  Active only in the sorted prefix:
	 * - single-pass fully-sorted: info->zm_sorted
	 * - two-pass prefix phase: !in_tail_phase
	 */
	pk_prefilter = shstate->two_pass
		? !shstate->in_tail_phase
		: info->zm_sorted;

	if (shstate->runtime_bounds && shstate->runtime_resolve_pending)
	{
		sorted_heap_resolve_runtime_bounds(shstate);
		shstate->runtime_resolve_pending = false;
		shstate->scanned_blocks = 0;
		shstate->pruned_blocks = 0;
		shstate->last_blk = InvalidBlockNumber;

		if (shstate->heap_scan)
		{
			table_rescan(shstate->heap_scan, NULL);

			if (shstate->scan_nblocks > 0)
				heap_setscanlimits(shstate->heap_scan,
							   shstate->scan_start,
							   shstate->scan_nblocks);
			else
				heap_setscanlimits(shstate->heap_scan, 1, 0);
		}

		current_nblocks = shstate->scan_nblocks;
		skip_zone_check = (current_nblocks <= 4 &&
						   shstate->n_in_values == 0);
	}

	for (;;)
	{
		while (table_scan_getnextslot(shstate->heap_scan,
									  ForwardScanDirection, slot))
		{
			BlockNumber blk = ItemPointerGetBlockNumber(&slot->tts_tid);
			bool		new_block = (blk != shstate->last_blk);
			bool		blk_has_zm;

			/* Track block transitions for EXPLAIN ANALYZE */
			if (new_block)
			{
				shstate->scanned_blocks++;
				shstate->last_blk = blk;

				if (!shstate->zm_stale &&
					shstate->zm_gen_at_start != 0)
				{
					uint64 cur = sorted_heap_read_zm_generation();
					if (cur != shstate->zm_gen_at_start)
						shstate->zm_stale = true;
				}
			}

			blk_has_zm = (blk >= 1 &&
						  (blk - 1) < info->zm_total_entries);

			/* Per-block zone map check for fine-grained pruning */
			if (!skip_zone_check && !shstate->zm_stale && blk_has_zm)
			{
				SortedHeapZoneMapEntry *e =
					sorted_heap_get_zm_entry(info, blk - 1);
				bool	overlaps;

				if (shstate->n_in_values > 0)
					overlaps = zone_overlaps_in_values(e, shstate->in_values,
													   shstate->n_in_values);
				else
					overlaps = sorted_heap_zone_overlaps(e, &shstate->bounds);

				if (!overlaps)
				{
					if (new_block)
						shstate->pruned_blocks++;
					continue;
				}
			}

			/*
			 * PK-ordered pre-filter: skip tuples below lower bound,
			 * stop early once past upper bound.  Only safe when data is
			 * sorted (full table sorted, or prefix phase of two-pass).
			 */
			if (pk_prefilter && !shstate->zm_stale && blk_has_zm)
			{
				bool	isnull;
				Datum	pk_datum;
				int64	pk_val;

				pk_datum = slot_getattr(slot, info->attNums[0], &isnull);
				if (!isnull &&
					sorted_heap_key_to_int64(pk_datum, info->zm_pk_typid,
											 &pk_val))
				{
					if (shstate->bounds.has_lo && pk_val < shstate->bounds.lo)
						continue;

					if (shstate->bounds.has_hi && pk_val > shstate->bounds.hi)
					{
						/*
						 * Past upper bound in sorted data.  In two-pass
						 * prefix phase, skip to tail; in single-pass or
						 * tail phase, we're done.
						 */
						if (shstate->two_pass && !shstate->in_tail_phase)
							goto transition_to_tail;
						return NULL;
					}
				}
			}

			return slot;
		}

		/*
		 * Current phase exhausted.  In two-pass mode, transition from
		 * prefix to tail phase by resetting the scan with tail limits.
		 */
		if (shstate->two_pass && !shstate->in_tail_phase &&
			shstate->tail_nblocks > 0)
		{
transition_to_tail:
			shstate->in_tail_phase = true;
			pk_prefilter = false;
			shstate->last_blk = InvalidBlockNumber;

			table_rescan(shstate->heap_scan, NULL);
			heap_setscanlimits(shstate->heap_scan,
							   shstate->tail_start,
							   shstate->tail_nblocks);

			current_nblocks = shstate->tail_nblocks;
			skip_zone_check = (current_nblocks <= 4 &&
							   shstate->n_in_values == 0);
			continue;
		}

		return NULL;
	}
}

/* ----------------------------------------------------------------
 *  EPQ recheck — always true (quals are evaluated by ExecScan)
 * ---------------------------------------------------------------- */
static bool
sorted_heap_scan_recheck(ScanState *ss, TupleTableSlot *slot)
{
	return true;
}

/* ----------------------------------------------------------------
 *  ExecCustomScan — delegates to ExecScan for qual + projection.
 *
 *  The executor calls methods->ExecCustomScan() directly in both
 *  PG 17 and PG 18 (no ExecScan wrapper), so we must always invoke
 *  ExecScan ourselves for qual evaluation and projection.
 * ---------------------------------------------------------------- */
static TupleTableSlot *
sorted_heap_exec_custom_scan(CustomScanState *node)
{
	return ExecScan(&node->ss,
					(ExecScanAccessMtd) sorted_heap_scan_next,
					(ExecScanRecheckMtd) sorted_heap_scan_recheck);
}

/* ----------------------------------------------------------------
 *  EndCustomScan
 * ---------------------------------------------------------------- */
static void
sorted_heap_end_custom_scan(CustomScanState *node)
{
	SortedHeapScanState *shstate = (SortedHeapScanState *) node;

	/* Accumulate stats: shared memory if available, local fallback always */
	sh_local_scans++;
	sh_local_blocks_scanned += shstate->scanned_blocks;
	sh_local_blocks_pruned += shstate->pruned_blocks;

	if (sh_shared_stats)
	{
		pg_atomic_fetch_add_u64(&sh_shared_stats->total_scans, 1);
		pg_atomic_fetch_add_u64(&sh_shared_stats->blocks_scanned,
								shstate->scanned_blocks);
		pg_atomic_fetch_add_u64(&sh_shared_stats->blocks_pruned,
								shstate->pruned_blocks);
	}

	if (shstate->heap_scan)
	{
		table_endscan(shstate->heap_scan);
		shstate->heap_scan = NULL;
	}
}

/* ----------------------------------------------------------------
 *  EstimateDSMCustomScan
 * ---------------------------------------------------------------- */
static Size
sorted_heap_estimate_dsm(CustomScanState *node, ParallelContext *pcxt)
{
	return table_parallelscan_estimate(node->ss.ss_currentRelation,
									   node->ss.ps.state->es_snapshot);
}

/* ----------------------------------------------------------------
 *  InitializeDSMCustomScan — leader sets up parallel table scan
 * ---------------------------------------------------------------- */
static void
sorted_heap_initialize_dsm(CustomScanState *node, ParallelContext *pcxt,
							void *coordinate)
{
	SortedHeapScanState *shstate = (SortedHeapScanState *) node;
	Relation	rel = node->ss.ss_currentRelation;
	ParallelTableScanDesc pscan = (ParallelTableScanDesc) coordinate;

	table_parallelscan_initialize(rel, pscan,
								  node->ss.ps.state->es_snapshot);
	shstate->pscan = pscan;

	/* Open leader's parallel scan */
	shstate->heap_scan = table_beginscan_parallel(rel, pscan);
}

/* ----------------------------------------------------------------
 *  ReInitializeDSMCustomScan — reset for rescan
 * ---------------------------------------------------------------- */
static void
sorted_heap_reinitialize_dsm(CustomScanState *node, ParallelContext *pcxt,
							  void *coordinate)
{
	SortedHeapScanState *shstate = (SortedHeapScanState *) node;
	Relation	rel = node->ss.ss_currentRelation;
	ParallelTableScanDesc pscan = (ParallelTableScanDesc) coordinate;

	table_parallelscan_reinitialize(rel, pscan);

	/* Reopen the leader's scan */
	if (shstate->heap_scan)
		table_endscan(shstate->heap_scan);
	shstate->heap_scan = table_beginscan_parallel(rel, pscan);
}

/* ----------------------------------------------------------------
 *  InitializeWorkerCustomScan — worker opens parallel scan
 * ---------------------------------------------------------------- */
static void
sorted_heap_initialize_worker(CustomScanState *node, shm_toc *toc,
							   void *coordinate)
{
	SortedHeapScanState *shstate = (SortedHeapScanState *) node;
	Relation	rel = node->ss.ss_currentRelation;
	ParallelTableScanDesc pscan = (ParallelTableScanDesc) coordinate;

	shstate->pscan = pscan;

	/* Open this worker's parallel scan */
	if (shstate->heap_scan)
		table_endscan(shstate->heap_scan);
	shstate->heap_scan = table_beginscan_parallel(rel, pscan);
}

/* ----------------------------------------------------------------
 *  ReScanCustomScan
 * ---------------------------------------------------------------- */
static void
sorted_heap_rescan_custom_scan(CustomScanState *node)
{
	SortedHeapScanState *shstate = (SortedHeapScanState *) node;

	/* Reset mid-scan staleness so rescan gets a fresh generation snapshot */
	shstate->zm_gen_at_start = sorted_heap_read_zm_generation();
	shstate->zm_stale = false;

	/* Path B: re-evaluate runtime bounds (params may change in NestLoop) */
	if (shstate->runtime_bounds)
	{
		sorted_heap_resolve_runtime_bounds(shstate);
		shstate->runtime_resolve_pending = false;
		shstate->scanned_blocks = 0;
		shstate->pruned_blocks = 0;
		shstate->last_blk = InvalidBlockNumber;
	}

	/* Reset two-pass state to prefix phase */
	shstate->in_tail_phase = false;

	if (shstate->heap_scan)
	{
		table_rescan(shstate->heap_scan, NULL);

		/* Re-apply scan limits (rescan resets rs_inited) */
		if (shstate->two_pass)
			heap_setscanlimits(shstate->heap_scan,
							   shstate->prefix_start,
							   shstate->prefix_nblocks);
		else if (shstate->scan_nblocks > 0)
			heap_setscanlimits(shstate->heap_scan,
							   shstate->scan_start,
							   shstate->scan_nblocks);
		else
			heap_setscanlimits(shstate->heap_scan, 1, 0);
	}
}

/* ----------------------------------------------------------------
 *  ExplainCustomScan
 * ---------------------------------------------------------------- */
static void
sorted_heap_explain_custom_scan(CustomScanState *node, List *ancestors,
								ExplainState *es)
{
	SortedHeapScanState *shstate = (SortedHeapScanState *) node;
	StringInfoData buf;

	initStringInfo(&buf);
	if (shstate->runtime_bounds && !es->analyze)
	{
		appendStringInfo(&buf, "%u total blocks (runtime bounds)",
						 shstate->total_blocks);
	}
	else if (shstate->two_pass)
	{
		appendStringInfo(&buf,
						 "prefix %u + tail %u of %u blocks (pruned %u)",
						 shstate->prefix_nblocks,
						 shstate->tail_nblocks,
						 shstate->total_blocks,
						 shstate->total_blocks -
						 shstate->prefix_nblocks - shstate->tail_nblocks);
	}
	else
	{
		appendStringInfo(&buf, "%u of %u blocks (pruned %u)",
						 shstate->scan_nblocks,
						 shstate->total_blocks,
						 shstate->total_blocks - shstate->scan_nblocks);
	}
	ExplainPropertyText("Zone Map", buf.data, es);
	pfree(buf.data);

	if (es->analyze)
	{
		ExplainPropertyInteger("Scanned Blocks", NULL,
							   shstate->scanned_blocks, es);
		ExplainPropertyInteger("Pruned Blocks", NULL,
							   shstate->pruned_blocks, es);
	}
}

/* ----------------------------------------------------------------
 *  SQL-callable scan stats function
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(sorted_heap_scan_stats);

Datum
sorted_heap_scan_stats(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[4];
	bool		nulls[4] = {false, false, false, false};
	HeapTuple	tuple;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	tupdesc = BlessTupleDesc(tupdesc);

	if (sh_shared_stats)
	{
		values[0] = Int64GetDatum((int64) pg_atomic_read_u64(&sh_shared_stats->total_scans));
		values[1] = Int64GetDatum((int64) pg_atomic_read_u64(&sh_shared_stats->blocks_scanned));
		values[2] = Int64GetDatum((int64) pg_atomic_read_u64(&sh_shared_stats->blocks_pruned));
		values[3] = CStringGetTextDatum("shared");
	}
	else
	{
		values[0] = Int64GetDatum((int64) sh_local_scans);
		values[1] = Int64GetDatum((int64) sh_local_blocks_scanned);
		values[2] = Int64GetDatum((int64) sh_local_blocks_pruned);
		values[3] = CStringGetTextDatum("local");
	}

	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/* ----------------------------------------------------------------
 *  SQL-callable stats reset function
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(sorted_heap_reset_stats);

Datum
sorted_heap_reset_stats(PG_FUNCTION_ARGS)
{
	if (sh_shared_stats)
	{
		pg_atomic_write_u64(&sh_shared_stats->total_scans, 0);
		pg_atomic_write_u64(&sh_shared_stats->blocks_scanned, 0);
		pg_atomic_write_u64(&sh_shared_stats->blocks_pruned, 0);
	}

	sh_local_scans = 0;
	sh_local_blocks_scanned = 0;
	sh_local_blocks_pruned = 0;

	PG_RETURN_VOID();
}
