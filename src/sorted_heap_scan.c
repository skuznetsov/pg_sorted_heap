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
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "access/parallel.h"

#include "sorted_heap.h"

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
									   List **pk_clauses);
static void sorted_heap_apply_bound(SortedHeapScanBounds *bounds,
									int strategy, bool is_col2, int64 val);
static void sorted_heap_resolve_runtime_bounds(SortedHeapScanState *shstate);
static void sorted_heap_compute_block_range(SortedHeapRelInfo *info,
											SortedHeapScanBounds *bounds,
											BlockNumber total_blocks,
											BlockNumber *start_block,
											BlockNumber *nblocks);
static bool sorted_heap_zone_overlaps(SortedHeapZoneMapEntry *e,
									  SortedHeapScanBounds *bounds);

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

		if (!sorted_heap_extract_bounds(rel, info->attNums[0],
										info->zm_pk_typid,
										info->zm_col2_usable ?
										info->attNums[1] : 0,
										info->zm_pk_typid2,
										&bounds,
										&runtime_exprs,
										&runtime_meta,
										&pk_clauses))
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
			sorted_heap_compute_block_range(info, &bounds, total_blocks,
											&start_block, &nblocks);

			if (nblocks >= total_blocks)
				return;

			if (skip_narrow_dml && nblocks <= 4)
				return;

			sel = (double) nblocks / (double) total_blocks;
			cpath->path.rows = rel->rows;
			cpath->path.startup_cost = random_page_cost;
			cpath->path.total_cost = seq_page_cost * nblocks +
				cpu_tuple_cost * rel->tuples * sel +
				cpu_operator_cost * rel->tuples * sel;

			/* Pack range + bounds into custom_private */
			{
				List *range_list = NIL;
				List *bounds_list = NIL;

				range_list = lappend_int(range_list, (int32) start_block);
				range_list = lappend_int(range_list, (int32) nblocks);
				range_list = lappend_int(range_list, (int32) total_blocks);

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

				cpath->custom_private = list_make2(range_list, bounds_list);
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
			cpath->path.total_cost = seq_page_cost * nblocks +
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

			cpath->custom_private = list_make4(meta_list, runtime_meta,
											   const_bounds_list,
											   runtime_exprs);
		}
	}

	add_path(rel, &cpath->path);

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
			ppath->path.rows = cpath->path.rows;
			ppath->path.startup_cost = 0;
			ppath->path.total_cost = cpath->path.total_cost / (pw + 1);

			ppath->flags = 0;
			ppath->methods = &sorted_heap_path_methods;
			ppath->custom_private = cpath->custom_private;

			add_partial_path(rel, &ppath->path);
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
						   List **pk_clauses_out)
{
	ListCell   *lc;
	Oid			opfamily;
	Oid			opcid;
	Oid			opfamily2 = InvalidOid;

	memset(bounds, 0, sizeof(SortedHeapScanBounds));
	*runtime_exprs = NIL;
	*runtime_meta = NIL;
	*pk_clauses_out = NIL;

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

	/* Evaluate each runtime Param expression and apply bound */
	i = 0;
	foreach(lc, shstate->runtime_exprstates)
	{
		ExprState  *exprstate = (ExprState *) lfirst(lc);
		bool		isnull;
		Datum		val;
		int64		int_val;

		val = ExecEvalExprSwitchContext(exprstate, econtext, &isnull);
		if (!isnull &&
			sorted_heap_key_to_int64(val, shstate->runtime_typids[i], &int_val))
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
		 * Optimisation for sorted data: if the last covered entry has a
		 * finite max, and the query's upper bound is at or below that max,
		 * uncovered pages (which hold higher values) can't match.
		 */
		if (bounds->has_hi && zm_entries_count > 0)
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
			/* Must scan all uncovered data pages (but not overflow pages) */
			BlockNumber last_data_block = data_blocks;

			if (first_uncovered < first_match)
				first_match = first_uncovered;
			if (last_data_block > last_match)
				last_match = last_data_block;
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
	 * Path A: custom_private has 2 elements (range_list, bounds_list)
	 * Path B: custom_private has 4 elements (meta, runtime_meta,
	 *         const_bounds, runtime_exprs)
	 */
	{
		if (list_length(best_path->custom_private) == 4)
		{
			cscan->custom_exprs = (List *) lfourth(best_path->custom_private);
			cscan->custom_private = list_make3(linitial(best_path->custom_private),
											   lsecond(best_path->custom_private),
											   lthird(best_path->custom_private));
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

	/*
	 * Path A: custom_private has 2 elements (range_list, bounds_list)
	 * Path B: custom_private has 3 elements (meta_list, runtime_meta,
	 *         const_bounds_list) — runtime_exprs moved to custom_exprs
	 *         by plan_custom_path.
	 */
	if (list_length(cscan->custom_private) == 3)
	{
		/*
		 * Path B: runtime bounds with Param nodes.
		 * custom_private = list_make3(meta_list, runtime_meta, const_bounds_list)
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

		/* Resolve runtime bounds: evaluate Params, compute block range */
		sorted_heap_resolve_runtime_bounds(shstate);
	}
	else
	{
		/*
		 * Path A: all Const bounds — block range computed at plan time.
		 * custom_private = list_make2(range_list, bounds_list)
		 */
		List	   *range_list = (List *) linitial(cscan->custom_private);
		List	   *bounds_list = (List *) lsecond(cscan->custom_private);

		shstate->scan_start = (BlockNumber) linitial_int(range_list);
		shstate->scan_nblocks = (BlockNumber) lsecond_int(range_list);
		shstate->total_blocks = (BlockNumber) lthird_int(range_list);

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
		if (shstate->scan_nblocks > 0)
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

	/*
	 * For narrow scans (<=4 blocks), heap_setscanlimits already restricts
	 * to the matching blocks.  The per-block zone map check is redundant
	 * because compute_block_range already verified overlap.
	 */
	bool		skip_zone_check = (shstate->scan_nblocks <= 4);

	/*
	 * For sorted zone maps (post-compact), tuples within each page are in
	 * PK order.  We can skip tuples before the lower bound and stop early
	 * once we pass the upper bound — avoiding both getnextslot and
	 * ExecScan qual evaluation for non-matching tuples.
	 */
	bool		pk_prefilter = info->zm_sorted;

	while (table_scan_getnextslot(shstate->heap_scan,
								  ForwardScanDirection, slot))
	{
		BlockNumber blk = ItemPointerGetBlockNumber(&slot->tts_tid);
		bool		new_block = (blk != shstate->last_blk);
		bool		blk_in_prefix;

		/* Track block transitions for EXPLAIN ANALYZE */
		if (new_block)
		{
			shstate->scanned_blocks++;
			shstate->last_blk = blk;
		}

		blk_in_prefix = (blk >= 1 &&
						 (blk - 1) < info->zm_total_entries);

		/* Per-block zone map check for fine-grained pruning */
		if (!skip_zone_check && blk_in_prefix)
		{
			SortedHeapZoneMapEntry *e =
				sorted_heap_get_zm_entry(info, blk - 1);

			if (!sorted_heap_zone_overlaps(e, &shstate->bounds))
			{
				if (new_block)
					shstate->pruned_blocks++;
				continue;
			}
		}

		/*
		 * PK-ordered pre-filter: skip tuples below lower bound, terminate
		 * scan once past upper bound.  Only safe for pages within the
		 * sorted prefix (covered by monotonic zone map entries).
		 */
		if (pk_prefilter && blk_in_prefix)
		{
			bool	isnull;
			Datum	pk_datum;
			int64	pk_val;

			pk_datum = slot_getattr(slot, info->attNums[0], &isnull);
			if (!isnull &&
				sorted_heap_key_to_int64(pk_datum, info->zm_pk_typid,
										 &pk_val))
			{
				/* Below lower bound — skip without qual evaluation */
				if (shstate->bounds.has_lo && pk_val < shstate->bounds.lo)
					continue;

				/*
				 * Past upper bound — all remaining tuples are higher,
				 * so terminate the scan immediately.
				 */
				if (shstate->bounds.has_hi && pk_val > shstate->bounds.hi)
					return NULL;
			}
		}

		return slot;
	}

	return NULL;
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

	/* Path B: re-evaluate runtime bounds (params may change in NestLoop) */
	if (shstate->runtime_bounds)
	{
		sorted_heap_resolve_runtime_bounds(shstate);
		shstate->scanned_blocks = 0;
		shstate->pruned_blocks = 0;
		shstate->last_blk = InvalidBlockNumber;
	}

	if (shstate->heap_scan)
	{
		table_rescan(shstate->heap_scan, NULL);

		/* Re-apply scan limits (rescan resets rs_inited) */
		if (shstate->scan_nblocks > 0)
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
