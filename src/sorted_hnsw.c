/*
 * sorted_hnsw.c
 *
 * HNSW Index Access Method for pg_sorted_heap.
 *
 * Phase 1 MVP: build + scan + insert + lazy delete.
 */
#include "sorted_hnsw.h"
#include "sorted_heap.h"

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
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"
#include "optimizer/optimizer.h"
#include "common/pg_prng.h"
#include "storage/smgr.h"

#include <float.h>
#include <math.h>

#include "svec.h"

/* ---- GUCs ---- */

int		sorted_hnsw_ef_search = 96;
bool	sorted_hnsw_sq8 = true;

static relopt_kind shnsw_relopt_kind = 0;

/* ---- Shared types used by both Insert and Scan ---- */

typedef struct ScanCandidate
{
	float		dist;
	int32		nid;
} ScanCandidate;

typedef struct ScanResult ScanResult;

/* ---- Types shared between Insert and Scan ---- */

typedef struct ShnswCacheNode
{
	ItemPointerData heap_tid;
	int16		n_neighbors;
	int16		level;
	uint8		flags;
	int32	   *neighbors;
} ShnswCacheNode;

typedef struct ShnswUpperNbr
{
	int32		nid;
	int16		n_neighbors;
	int32	   *neighbors;
} ShnswUpperNbr;

typedef struct ShnswScanCache
{
	int			M;
	int			dim;
	int			n_nodes;
	int			entry_nid;
	int			max_level;
	int			ef_search;
	float	   *sq8_mins;
	float	   *sq8_scales;
	ShnswCacheNode *nodes;
	int32	   *l0_neighbors;
	uint8	   *sq8_data;
	BlockNumber	l0_start;
	int			l0_npages;
	int			nodes_per_page;
	int			node_size;
	uint8	   *l0_page_loaded;
	ShnswUpperNbr **upper;
	int		   *upper_count;
	int		  **upper_nbr_idx;
} ShnswScanCache;

typedef struct ShnswScanCacheEntry
{
	Oid				relid;
	RelFileLocator	locator;
	uint64			cache_gen;
	MemoryContext	cache_ctx;
	ShnswScanCache *cache;
} ShnswScanCacheEntry;

static HTAB *shnsw_scan_cache_hash = NULL;
static bool shnsw_scan_cache_callback_registered = false;

/* Forward declarations */
static ShnswScanCache *shnsw_load_cache(Relation index);
static ShnswScanCache *shnsw_get_scan_cache(Relation index);
static void shnsw_ensure_scan_cache_hash(void);
static void shnsw_scan_cache_relcache_callback(Datum arg, Oid relid);
static void shnsw_scan_cache_invalidate(ShnswScanCacheEntry *entry);
static void shnsw_scan_cache_seed_from_build(Relation index,
											 HnswBuildState *graph,
											 const float *vectors,
											 const float *sq8_mins,
											 const float *sq8_scales,
											 int n_nodes, int dim, int M);
static void shnsw_scan_cache_record_insert(Relation index, uint64 cache_gen,
										   ItemPointer heap_tid,
										   int32 new_nid,
										   const int32 *selected,
										   const bool *reverse_added,
										   int n_sel,
										   const uint8 *sq8,
										   int M, int dim);
static bool shnsw_scan_cache_is_warm(Relation index);
static uint64 shnsw_read_cache_generation(Relation index);
static inline uint8 sq8_quantize(float val, float min_val, float scale);
static Size shnsw_l0_neighbors_bytes(int n_nodes, int M);
static int32 *shnsw_cache_l0_neighbors_slot(ShnswScanCache *cache, int32 nid);
static void shnsw_cache_refresh_l0_neighbor_ptrs(ShnswScanCache *cache,
												 int start_nid);
static void shnsw_cache_load_l0_page(Relation index, ShnswScanCache *cache,
									 int page_idx);
static bool shnsw_cache_ensure_node_loaded(Relation index,
										   ShnswScanCache *cache, int32 nid);
static int shnsw_search_level(Relation index, ShnswScanCache *cache,
							  const float *query,
							  int entry_nid, int ef, int level,
							  ScanCandidate *results, int max_results);
static int shnsw_rerank_candidates(Relation index, const ShnswScanCache *cache,
								   const Svec *query,
								   const ScanCandidate *candidates, int n_cand,
								   struct ScanResult *results, int max_results);

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
static void shnsw_write_empty_metapage(Relation index, ForkNumber forknum,
										int M, int ef_construction, int dim);
static bool shnsw_bootstrap_first_node(Relation index, const Svec *sv,
										ItemPointer heap_tid,
										int M, int ef_construction, int dim);

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

	if (!shnsw_scan_cache_callback_registered)
	{
		CacheRegisterRelcacheCallback(shnsw_scan_cache_relcache_callback,
									  (Datum) 0);
		shnsw_scan_cache_callback_registered = true;
	}
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

static void
shnsw_ensure_scan_cache_hash(void)
{
	HASHCTL ctl;

	if (shnsw_scan_cache_hash != NULL)
		return;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(ShnswScanCacheEntry);
	ctl.hcxt = TopMemoryContext;
	shnsw_scan_cache_hash = hash_create("sorted_hnsw scan cache",
										32, &ctl,
										HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static void
shnsw_scan_cache_invalidate(ShnswScanCacheEntry *entry)
{
	if (entry->cache_ctx != NULL)
	{
		MemoryContextDelete(entry->cache_ctx);
		entry->cache_ctx = NULL;
	}
	entry->cache = NULL;
	entry->cache_gen = 0;
	memset(&entry->locator, 0, sizeof(entry->locator));
}

static void
shnsw_scan_cache_relcache_callback(Datum arg, Oid relid)
{
	if (shnsw_scan_cache_hash == NULL)
		return;

	/*
	 * Keep relid-specific entries across relcache refreshes. The scan cache is
	 * already keyed by relfilenode locator + metapage cache generation, which
	 * are stricter than generic relcache invalidations and allow a freshly
	 * built graph to remain warm for the first same-session ordered scan after
	 * CREATE INDEX. Global invalidations still drop everything.
	 */
	if (!OidIsValid(relid))
	{
		HASH_SEQ_STATUS status;
		ShnswScanCacheEntry *entry;

		hash_seq_init(&status, shnsw_scan_cache_hash);
		while ((entry = hash_seq_search(&status)) != NULL)
			shnsw_scan_cache_invalidate(entry);
	}
}

static void
shnsw_scan_cache_seed_from_build(Relation index,
								 HnswBuildState *graph,
								 const float *vectors,
								 const float *sq8_mins,
								 const float *sq8_scales,
								 int n_nodes, int dim, int M)
{
	Oid					relid = RelationGetRelid(index);
	ShnswScanCacheEntry *entry;
	ShnswScanCache	   *cache;
	MemoryContext		cache_ctx;
	MemoryContext		old_ctx;
	bool				found;
	uint64				cache_gen;
	int					max_level;
	BlockNumber			l0_start;
	int					l0_npages;
	int					i, lev, d;

	shnsw_ensure_scan_cache_hash();
	{
		Buffer				buf;
		Page				page;
		ShnswMetaPageData  *meta;

		buf = ReadBuffer(index, SHNSW_METAPAGE_BLKNO);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		meta = (ShnswMetaPageData *) PageGetContents(page);
		cache_gen = meta->shnsw_cache_gen;
		l0_start = meta->shnsw_l0_start;
		l0_npages = meta->shnsw_l0_npages;
		UnlockReleaseBuffer(buf);
	}
	entry = hash_search(shnsw_scan_cache_hash, &relid, HASH_ENTER, &found);

	if (found)
		shnsw_scan_cache_invalidate(entry);

	cache_ctx = AllocSetContextCreate(TopMemoryContext,
									  "sorted_hnsw build cache",
									  ALLOCSET_DEFAULT_SIZES);
	old_ctx = MemoryContextSwitchTo(cache_ctx);

	cache = palloc0(sizeof(ShnswScanCache));
	max_level = shnsw_build_max_level(graph);
	cache->M = M;
	cache->dim = dim;
	cache->n_nodes = n_nodes;
	cache->entry_nid = shnsw_build_entry_nid(graph);
	cache->max_level = max_level;
	cache->ef_search = sorted_hnsw_ef_search;
	cache->l0_start = l0_start;
	cache->l0_npages = l0_npages;
	cache->nodes_per_page = ShnswL0NodesPerPage(M, dim);
	if (cache->nodes_per_page < 1)
		cache->nodes_per_page = 1;
	cache->node_size = MAXALIGN(ShnswNodeSize(M, dim));
	cache->l0_page_loaded = palloc0(Max(l0_npages, 1));

	cache->sq8_mins = palloc(sizeof(float) * dim);
	cache->sq8_scales = palloc(sizeof(float) * dim);
	memcpy(cache->sq8_mins, sq8_mins, sizeof(float) * dim);
	memcpy(cache->sq8_scales, sq8_scales, sizeof(float) * dim);

	cache->nodes = palloc0(sizeof(ShnswCacheNode) * n_nodes);
	cache->l0_neighbors = palloc(shnsw_l0_neighbors_bytes(n_nodes, M));
	cache->sq8_data = palloc((Size) n_nodes * dim);

	for (i = 0; i < n_nodes; i++)
	{
		HnswBuiltNode   *bn = shnsw_build_get_node(graph, i);
		ShnswCacheNode  *cn = &cache->nodes[i];
		int32		   *nbrs;
		int				n_l0;

		ItemPointerCopy(&bn->heap_tid, &cn->heap_tid);
		cn->level = bn->level;
		cn->flags = 0;
		n_l0 = Min(bn->n_neighbors[0], 2 * M);
		cn->n_neighbors = n_l0;
		cn->neighbors = shnsw_cache_l0_neighbors_slot(cache, i);
		nbrs = cn->neighbors;
		for (d = 0; d < n_l0; d++)
			nbrs[d] = bn->neighbors[0][d];
		for (d = n_l0; d < 2 * M; d++)
			nbrs[d] = -1;

		for (d = 0; d < dim; d++)
		{
			cache->sq8_data[(Size) i * dim + d] =
				sq8_quantize(vectors[(Size) i * dim + d],
							 sq8_mins[d], sq8_scales[d]);
		}
	}

	cache->upper = palloc0(sizeof(ShnswUpperNbr *) * (max_level + 1));
	cache->upper_count = palloc0(sizeof(int) * (max_level + 1));
	cache->upper_nbr_idx = palloc0(sizeof(int *) * (max_level + 1));

	for (lev = 1; lev <= max_level; lev++)
	{
		int count = 0;
		int pos = 0;

		for (i = 0; i < n_nodes; i++)
		{
			HnswBuiltNode *bn = shnsw_build_get_node(graph, i);
			if (bn->level >= lev)
				count++;
		}

		cache->upper[lev] = palloc0(sizeof(ShnswUpperNbr) * Max(count, 1));
		cache->upper_count[lev] = count;
		cache->upper_nbr_idx[lev] = palloc(sizeof(int) * n_nodes);
		memset(cache->upper_nbr_idx[lev], -1, sizeof(int) * n_nodes);

		for (i = 0; i < n_nodes; i++)
		{
			HnswBuiltNode *bn = shnsw_build_get_node(graph, i);
			ShnswUpperNbr *ue;
			int n_upper;

			if (bn->level < lev)
				continue;

			ue = &cache->upper[lev][pos];
			ue->nid = bn->nid;
			n_upper = Min(bn->n_neighbors[lev], M);
			ue->n_neighbors = n_upper;
			ue->neighbors = palloc(sizeof(int32) * M);
			for (d = 0; d < n_upper; d++)
				ue->neighbors[d] = bn->neighbors[lev][d];
			for (d = n_upper; d < M; d++)
				ue->neighbors[d] = -1;

			cache->upper_nbr_idx[lev][ue->nid] = pos;
			pos++;
		}
	}

	if (l0_npages > 0)
		memset(cache->l0_page_loaded, 1, l0_npages);

	entry->cache = cache;
	entry->cache_ctx = cache_ctx;
	entry->cache_gen = cache_gen;
	entry->locator = index->rd_locator;

	MemoryContextSwitchTo(old_ctx);
}

static bool
shnsw_scan_cache_is_warm(Relation index)
{
	uint64				cache_gen;
	ShnswScanCacheEntry *entry;

	if (shnsw_scan_cache_hash == NULL)
		return false;

	cache_gen = shnsw_read_cache_generation(index);
	entry = hash_search(shnsw_scan_cache_hash, &index->rd_id, HASH_FIND, NULL);

	return entry != NULL &&
		entry->cache != NULL &&
		entry->cache_ctx != NULL &&
		entry->cache_gen == cache_gen &&
		RelFileLocatorEquals(entry->locator, index->rd_locator);
}

static Size
shnsw_l0_neighbors_bytes(int n_nodes, int M)
{
	return (Size) Max(n_nodes, 1) * (Size) (2 * M) * sizeof(int32);
}

static int32 *
shnsw_cache_l0_neighbors_slot(ShnswScanCache *cache, int32 nid)
{
	return cache->l0_neighbors + (Size) nid * (Size) (2 * cache->M);
}

static void
shnsw_cache_refresh_l0_neighbor_ptrs(ShnswScanCache *cache, int start_nid)
{
	int nid;

	if (cache->l0_neighbors == NULL)
		return;

	for (nid = Max(start_nid, 0); nid < cache->n_nodes; nid++)
	{
		if (cache->nodes[nid].neighbors != NULL)
			cache->nodes[nid].neighbors =
				shnsw_cache_l0_neighbors_slot(cache, nid);
	}
}

static void
shnsw_cache_load_l0_page(Relation index, ShnswScanCache *cache, int page_idx)
{
	Buffer	buf;
	Page	page;
	int		j;
	int		page_count;

	if (cache->l0_page_loaded == NULL ||
		page_idx < 0 ||
		page_idx >= cache->l0_npages ||
		cache->l0_page_loaded[page_idx])
		return;

	buf = ReadBuffer(index, cache->l0_start + page_idx);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);

	page_count = Min(cache->nodes_per_page,
					 cache->n_nodes - page_idx * cache->nodes_per_page);
	for (j = 0; j < page_count; j++)
	{
		ShnswNodeHeader *nh = (ShnswNodeHeader *)
			((char *) PageGetContents(page) + j * cache->node_size);
		int32			nid = nh->nid;
		ShnswCacheNode *cn;
		int32		   *src_nbrs;

		if (nid < 0 || nid >= cache->n_nodes)
			continue;

		cn = &cache->nodes[nid];
		cn->neighbors = shnsw_cache_l0_neighbors_slot(cache, nid);

		ItemPointerCopy(&nh->heap_tid, &cn->heap_tid);
		cn->level = nh->level;
		cn->n_neighbors = nh->n_neighbors;
		cn->flags = nh->flags;

		src_nbrs = ShnswNodeNeighbors(nh);
		memcpy(cn->neighbors, src_nbrs, sizeof(int32) * 2 * cache->M);
		memcpy(cache->sq8_data + (Size) nid * cache->dim,
			   ShnswNodeSQ8Vec(nh, cache->M),
			   cache->dim);
	}

	UnlockReleaseBuffer(buf);
	cache->l0_page_loaded[page_idx] = 1;
}

static bool
shnsw_cache_ensure_node_loaded(Relation index, ShnswScanCache *cache, int32 nid)
{
	int page_idx;

	if (nid < 0 || nid >= cache->n_nodes)
		return false;

	if (cache->nodes[nid].neighbors != NULL)
		return true;

	if (cache->l0_page_loaded == NULL || cache->nodes_per_page <= 0)
		return false;

	page_idx = nid / cache->nodes_per_page;
	if (page_idx < 0 || page_idx >= cache->l0_npages)
		return false;

	shnsw_cache_load_l0_page(index, cache, page_idx);
	return cache->nodes[nid].neighbors != NULL;
}

static void
shnsw_scan_cache_record_insert(Relation index, uint64 cache_gen,
							   ItemPointer heap_tid,
							   int32 new_nid,
							   const int32 *selected,
							   const bool *reverse_added,
							   int n_sel,
							   const uint8 *sq8,
							   int M, int dim)
{
	Oid					relid = RelationGetRelid(index);
	ShnswScanCacheEntry *entry;
	ShnswScanCache	   *cache;
	MemoryContext		old_ctx;
	int					old_n_nodes;
	int					page_idx;
	int					lev;
	int32			   *nbrs;

	if (shnsw_scan_cache_hash == NULL)
		return;

	entry = hash_search(shnsw_scan_cache_hash, &relid, HASH_FIND, NULL);
	if (entry == NULL ||
		entry->cache == NULL ||
		entry->cache_ctx == NULL ||
		!RelFileLocatorEquals(entry->locator, index->rd_locator))
		return;

	cache = entry->cache;
	old_n_nodes = cache->n_nodes;
	if (new_nid != old_n_nodes)
	{
		shnsw_scan_cache_invalidate(entry);
		return;
	}

	old_ctx = MemoryContextSwitchTo(entry->cache_ctx);

	cache->nodes = repalloc(cache->nodes,
							sizeof(ShnswCacheNode) * (old_n_nodes + 1));
	memset(&cache->nodes[old_n_nodes], 0, sizeof(ShnswCacheNode));
	{
		int32 *old_neighbors = cache->l0_neighbors;

		cache->l0_neighbors = repalloc(cache->l0_neighbors,
									   shnsw_l0_neighbors_bytes(old_n_nodes + 1,
																 M));
		if (cache->l0_neighbors != old_neighbors)
			shnsw_cache_refresh_l0_neighbor_ptrs(cache, 0);
	}

	cache->sq8_data = repalloc(cache->sq8_data, (Size) (old_n_nodes + 1) * dim);
	memcpy(cache->sq8_data + (Size) old_n_nodes * dim, sq8, dim);
	page_idx = new_nid / cache->nodes_per_page;
	if (page_idx >= cache->l0_npages)
	{
		int old_pages = cache->l0_npages;
		int new_pages = page_idx + 1;

		cache->l0_page_loaded = repalloc(cache->l0_page_loaded, Max(new_pages, 1));
		memset(cache->l0_page_loaded + old_pages, 0, new_pages - old_pages);
		cache->l0_npages = new_pages;
	}
	cache->l0_page_loaded[page_idx] = 1;

	cache->nodes[old_n_nodes].neighbors =
		shnsw_cache_l0_neighbors_slot(cache, old_n_nodes);
	nbrs = cache->nodes[old_n_nodes].neighbors;
	cache->nodes[old_n_nodes].n_neighbors = n_sel;
	cache->nodes[old_n_nodes].level = 0;
	cache->nodes[old_n_nodes].flags = 0;
	ItemPointerCopy(heap_tid, &cache->nodes[old_n_nodes].heap_tid);
	for (lev = 0; lev < n_sel; lev++)
		nbrs[lev] = selected[lev];
	for (lev = n_sel; lev < 2 * M; lev++)
		nbrs[lev] = -1;

	for (lev = 1; lev <= cache->max_level; lev++)
	{
		cache->upper_nbr_idx[lev] = repalloc(cache->upper_nbr_idx[lev],
											 sizeof(int) * (old_n_nodes + 1));
		cache->upper_nbr_idx[lev][old_n_nodes] = -1;
	}

	for (lev = 0; lev < n_sel; lev++)
	{
		int32 target_nid = selected[lev];
		ShnswCacheNode *target;

		if (!reverse_added[lev] ||
			target_nid < 0 ||
			target_nid >= old_n_nodes)
			continue;

		target = &cache->nodes[target_nid];
		if (target->neighbors == NULL || target->n_neighbors >= 2 * M)
			continue;

		target->neighbors[target->n_neighbors] = new_nid;
		target->n_neighbors++;
	}

	cache->n_nodes = old_n_nodes + 1;
	entry->cache_gen = cache_gen;
	entry->locator = index->rd_locator;
	shnsw_cache_refresh_l0_neighbor_ptrs(cache, old_n_nodes);

	MemoryContextSwitchTo(old_ctx);
}

static uint64
shnsw_read_cache_generation(Relation index)
{
	Buffer	buf;
	Page	page;
	ShnswMetaPageData *meta;
	uint64	gen;

	buf = ReadBuffer(index, SHNSW_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	meta = (ShnswMetaPageData *) PageGetContents(page);
	gen = meta->shnsw_cache_gen;
	UnlockReleaseBuffer(buf);

	return gen;
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

static void
shnsw_page_set_payload_end(Page page, Size payload_bytes)
{
	PageHeader	header = (PageHeader) page;
	LocationIndex lower;

	lower = (LocationIndex) MAXALIGN(SizeOfPageHeaderData + payload_bytes);
	if (lower > header->pd_special)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("sorted_hnsw page payload exceeds page capacity")));
	header->pd_lower = lower;
}

static void
shnsw_write_empty_metapage(Relation index, ForkNumber forknum,
							 int M, int ef_construction, int dim)
{
	Buffer		buf;
	Page		page;
	ShnswMetaPageData *meta;
	ShnswPageOpaque opaque;

	buf = ReadBufferExtended(index, forknum, P_NEW, RBM_NORMAL, NULL);
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
	meta->shnsw_m = M;
	meta->shnsw_ef_construction = ef_construction;
	meta->shnsw_dim = dim;
	meta->shnsw_entry_nid = -1;
	meta->shnsw_max_level = -1;
	meta->shnsw_cache_gen = 1;
	shnsw_page_set_payload_end(page, sizeof(ShnswMetaPageData));

	MarkBufferDirty(buf);
	if (forknum == MAIN_FORKNUM)
		log_newpage_buffer(buf, true);
	UnlockReleaseBuffer(buf);
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
		/* Empty table — initialize the main-fork metapage so aminsert can bootstrap. */
		shnsw_write_empty_metapage(index, MAIN_FORKNUM, M, ef_construction, dim);
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

		buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);
		PageInit(page, BLCKSZ, sizeof(ShnswPageOpaqueData));
		MarkBufferDirty(buf);
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
			shnsw_page_set_payload_end(page, chunk);

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
			shnsw_page_set_payload_end(page, (Size) page_nodes * node_size);
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
				/* Mark all unused slots with nid=-1 sentinel */
				int s;
				for (s = page_entries; s < entries_per_page; s++)
				{
					ShnswUpperEntry *sentinel = (ShnswUpperEntry *)
						((char *) PageGetContents(upage) + s * entry_size);
					sentinel->nid = -1;
				}
				shnsw_page_set_payload_end(upage,
										   (Size) entries_per_page * entry_size);
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
			meta->shnsw_cache_gen = 1;
			for (lev = 1; lev < SHNSW_MAX_LEVELS; lev++)
			{
				meta->shnsw_upper_start[lev] = upper_starts[lev];
				meta->shnsw_upper_npages[lev] = upper_npages[lev];
			}
			shnsw_page_set_payload_end(page, sizeof(ShnswMetaPageData));
			MarkBufferDirty(buf);
			log_newpage_buffer(buf, true);
			UnlockReleaseBuffer(buf);
		}
	}

	elog(NOTICE, "sorted_hnsw: build complete. %d nodes, %d L0 pages, %d SQ8 aux pages",
		 n_nodes, (int)(next_blkno - 1 - sq8_aux_npages), sq8_aux_npages);

	/* Ensure all index pages are flushed to disk */
	FlushRelationBuffers(index);
	shnsw_scan_cache_seed_from_build(index, graph, vectors, sq8_mins, sq8_scales,
									 n_nodes, dim, M);

	MemoryContextDelete(build_ctx);

	result = palloc0(sizeof(IndexBuildResult));
	result->heap_tuples = n_nodes;
	result->index_tuples = n_nodes;
	return result;
}

static void
shnsw_buildempty(Relation index)
{
	int			dim;
	ShnswOptions *opts;
	int			M;
	int			ef_construction;

	opts = (ShnswOptions *) index->rd_options;
	M = opts ? opts->m : SHNSW_DEFAULT_M;
	ef_construction = opts ? opts->ef_construction : SHNSW_DEFAULT_EF_CONSTRUCTION;
	dim = TupleDescAttr(index->rd_att, 0)->atttypmod;
	if (dim <= 0)
		dim = 0;

	/* Core uses ambuildempty() for the init fork of unlogged indexes. */
	shnsw_write_empty_metapage(index, INIT_FORKNUM, M, ef_construction, dim);
}

static bool
shnsw_bootstrap_first_node(Relation index, const Svec *sv, ItemPointer heap_tid,
							 int M, int ef_construction, int dim)
{
	float		   *sq8_data;
	Size			total_bytes;
	Size			offset;
	Size			usable;
	int				sq8_aux_npages;
	BlockNumber		sq8_start;
	BlockNumber		l0_start;
	int				nodes_per_page;
	int				d;
	int				p;
	Buffer			buf;
	Page			page;
	ShnswPageOpaque opaque;
	ShnswNodeHeader *nh;
	uint8		   *sq8;
	Buffer			mbuf;
	Page			mpage;
	ShnswMetaPageData *meta;

	total_bytes = 2 * dim * sizeof(float);
	usable = BLCKSZ - MAXALIGN(SizeOfPageHeaderData) -
		MAXALIGN(sizeof(ShnswPageOpaqueData));
	sq8_aux_npages = (int) ceil((double) total_bytes / (double) usable);
	if (sq8_aux_npages < 1)
		sq8_aux_npages = 1;
	sq8_start = 1;
	l0_start = 1 + sq8_aux_npages;
	nodes_per_page = ShnswL0NodesPerPage(M, dim);
	if (nodes_per_page < 1)
		nodes_per_page = 1;

	/* mins = vector, scales = 0 so the first node quantizes to all-zeroes */
	sq8_data = palloc0(total_bytes);
	memcpy(sq8_data, sv->x, sizeof(float) * dim);

	offset = 0;
	for (p = 0; p < sq8_aux_npages; p++)
	{
		Size	chunk;

		buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		page = palloc0(BLCKSZ);
		PageInit(page, BLCKSZ, sizeof(ShnswPageOpaqueData));
		opaque = ShnswPageGetOpaque(page);
		opaque->shnsw_page_type = SHNSW_PAGE_SQ8_AUX;
		opaque->shnsw_level = 0;
		opaque->shnsw_next = (p + 1 < sq8_aux_npages) ? (sq8_start + p + 1) :
			InvalidBlockNumber;

		chunk = Min(total_bytes - offset, usable);
		memcpy(PageGetContents(page), (char *) sq8_data + offset, chunk);
		shnsw_page_set_payload_end(page, chunk);
		shnsw_flush_page(index, buf, page);
		UnlockReleaseBuffer(buf);
		pfree(page);
		offset += chunk;
	}
	pfree(sq8_data);

	buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	page = palloc0(BLCKSZ);
	PageInit(page, BLCKSZ, sizeof(ShnswPageOpaqueData));
	opaque = ShnswPageGetOpaque(page);
	opaque->shnsw_page_type = SHNSW_PAGE_L0;
	opaque->shnsw_level = 0;
	opaque->shnsw_next = InvalidBlockNumber;

	nh = (ShnswNodeHeader *) PageGetContents(page);
	nh->nid = 0;
	ItemPointerCopy(heap_tid, &nh->heap_tid);
	nh->level = 0;
	nh->n_neighbors = 0;
	nh->flags = 0;
	nh->padding = 0;
	for (d = 0; d < 2 * M; d++)
		ShnswNodeNeighbors(nh)[d] = -1;
	sq8 = ShnswNodeSQ8Vec(nh, M);
	memset(sq8, 0, dim);
	shnsw_page_set_payload_end(page, MAXALIGN(ShnswNodeSize(M, dim)));

	shnsw_flush_page(index, buf, page);
	UnlockReleaseBuffer(buf);
	pfree(page);

	mbuf = ReadBuffer(index, SHNSW_METAPAGE_BLKNO);
	LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
	mpage = BufferGetPage(mbuf);
	meta = (ShnswMetaPageData *) PageGetContents(mpage);
	memset(meta, 0, sizeof(ShnswMetaPageData));
	meta->shnsw_magic = SORTED_HNSW_MAGIC;
	meta->shnsw_version = SORTED_HNSW_VERSION;
	meta->shnsw_flags = SHNSW_FLAG_SQ8_VALID;
	meta->shnsw_m = M;
	meta->shnsw_ef_construction = ef_construction;
	meta->shnsw_dim = dim;
	meta->shnsw_node_count = 1;
	meta->shnsw_entry_nid = 0;
	meta->shnsw_max_level = 0;
	meta->shnsw_sq8_start = sq8_start;
	meta->shnsw_sq8_npages = sq8_aux_npages;
	meta->shnsw_l0_start = l0_start;
	meta->shnsw_l0_npages = 1;
	meta->shnsw_cache_gen = 1;
	shnsw_page_set_payload_end(mpage, sizeof(ShnswMetaPageData));
	MarkBufferDirty(mbuf);
	log_newpage_buffer(mbuf, false);
	UnlockReleaseBuffer(mbuf);

	return false;
}

/* ================================================================
 * Insert (aminsert)
 * ================================================================ */

/*
 * Helper: read an L0 node's neighbor list from its index page.
 * Returns pointer to the node header within the pinned buffer page.
 * Caller must UnlockReleaseBuffer(buf) after use.
 */
static ShnswNodeHeader *
shnsw_read_l0_node(Relation index, int32 nid, int M, int dim,
				   BlockNumber l0_start, int nodes_per_page,
				   Buffer *out_buf)
{
	BlockNumber	blk;
	int			offset_in_page;
	int			node_size;
	Page		page;

	node_size = MAXALIGN(ShnswNodeSize(M, dim));
	blk = l0_start + nid / nodes_per_page;
	offset_in_page = nid % nodes_per_page;

	*out_buf = ReadBuffer(index, blk);
	LockBuffer(*out_buf, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(*out_buf);

	return (ShnswNodeHeader *)
		((char *) PageGetContents(page) + offset_in_page * node_size);
}

/*
 * Helper: add nid as a neighbor to an existing node's neighbor list.
 * Reads the node's L0 page, modifies in place, marks dirty.
 * Returns true if added, false if neighbor list already full.
 */
static bool
shnsw_add_neighbor_on_page(Relation index, int32 target_nid, int32 new_nbr_nid,
						   int M, int dim, BlockNumber l0_start,
						   int nodes_per_page)
{
	Buffer		buf;
	ShnswNodeHeader *nh;
	int32	   *nbrs;
	int			max_nbrs = 2 * M;
	bool		added = false;

	nh = shnsw_read_l0_node(index, target_nid, M, dim, l0_start,
							nodes_per_page, &buf);

	nbrs = ShnswNodeNeighbors(nh);

	if (nh->n_neighbors < max_nbrs)
	{
		nbrs[nh->n_neighbors] = new_nbr_nid;
		nh->n_neighbors++;
		added = true;
		MarkBufferDirty(buf);
		log_newpage_buffer(buf, false);
	}

	UnlockReleaseBuffer(buf);
	return added;
}

static bool
shnsw_insert(Relation index, Datum *values, bool *isnull,
			  ItemPointer heap_tid, Relation heap,
			  IndexUniqueCheck checkUnique,
			  bool indexUnchanged,
			  IndexInfo *indexInfo)
{
	ShnswScanCache *cache;
	Svec		   *sv;
	int				dim, M, n_nodes, new_nid;
	int				ef_construction;
	int				node_level, level;
	int				ep_nid;
	int				nodes_per_page, node_size;
	BlockNumber		l0_start;
	float		   *query_vec;
	ScanCandidate  *cand_buf;
	int				n_cand;
	int32		   *selected;
	bool		   *reverse_added;
	uint8		   *new_sq8;
	int				d;
	Buffer			mbuf;
	Page			mpage;
	ShnswMetaPageData *meta;
	uint64			new_cache_gen;

	if (isnull[0])
		return false;

	sv = DatumGetSvecP(values[0]);

	/* Read metapage parameters first so empty indexes can bootstrap. */
	mbuf = ReadBuffer(index, SHNSW_METAPAGE_BLKNO);
	LockBuffer(mbuf, BUFFER_LOCK_SHARE);
	mpage = BufferGetPage(mbuf);
	meta = (ShnswMetaPageData *) PageGetContents(mpage);
	M = meta->shnsw_m;
	ef_construction = meta->shnsw_ef_construction;
	dim = meta->shnsw_dim;
	n_nodes = meta->shnsw_node_count;
	ep_nid = meta->shnsw_entry_nid;
	l0_start = meta->shnsw_l0_start;
	UnlockReleaseBuffer(mbuf);

	if (dim <= 0)
	{
		dim = TupleDescAttr(index->rd_att, 0)->atttypmod;
		if (dim <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("sorted_hnsw requires svec with explicit dimension")));
	}
	if (M <= 0)
		M = SHNSW_DEFAULT_M;
	if (ef_construction <= 0)
		ef_construction = SHNSW_DEFAULT_EF_CONSTRUCTION;

	if (sv->dim != dim)
	{
		ereport(WARNING,
				(errmsg("sorted_hnsw: insert vector dim %d != index dim %d, skipping",
						sv->dim, dim)));
		if (sv != (Svec *) DatumGetPointer(values[0]))
			pfree(sv);
		return false;
	}

	if (n_nodes == 0 || ep_nid < 0)
	{
		bool inserted;

		inserted = shnsw_bootstrap_first_node(index, sv, heap_tid,
											  M, ef_construction, dim);
		if (sv != (Svec *) DatumGetPointer(values[0]))
			pfree(sv);
		return inserted;
	}

	/* Load index cache */
	cache = shnsw_get_scan_cache(index);
	dim = cache->dim;
	M = cache->M;
	n_nodes = cache->n_nodes;
	ep_nid = cache->entry_nid;
	nodes_per_page = ShnswL0NodesPerPage(M, dim);
	node_size = MAXALIGN(ShnswNodeSize(M, dim));

	if (nodes_per_page < 1) nodes_per_page = 1;

	/* Read l0_start from metapage */
	{
		Buffer	mbuf2 = ReadBuffer(index, SHNSW_METAPAGE_BLKNO);
		Page	mpage2;
		ShnswMetaPageData *meta2;

		LockBuffer(mbuf2, BUFFER_LOCK_SHARE);
		mpage2 = BufferGetPage(mbuf2);
		meta2 = (ShnswMetaPageData *) PageGetContents(mpage2);
		l0_start = meta2->shnsw_l0_start;
		UnlockReleaseBuffer(mbuf2);
	}

	/* Prepare query vector (float32 copy for SQ8 distance computation) */
	query_vec = sv->x;

	/* Pick random level */
	{
		double ml = 1.0 / log((double) M);
		double r = pg_prng_double(&pg_global_prng_state);

		if (r < 1e-10) r = 1e-10;
		node_level = (int)(-log(r) * ml);
		if (node_level > SHNSW_MAX_LEVELS - 1)
			node_level = SHNSW_MAX_LEVELS - 1;
		/* For Phase 1, cap at L0 only to avoid complex upper-level page management */
		node_level = 0;
	}

	/* Navigate upper levels to find good entry point for L0 */
	for (level = cache->max_level; level >= 1; level--)
	{
		ScanCandidate one;
		int found;

		memset(&one, 0, sizeof(one));
		found = shnsw_search_level(index, cache, query_vec, ep_nid,
								   1, level, &one, 1);
		if (found > 0)
			ep_nid = one.nid;
	}

	/* Search L0 for ef_construction neighbors */
	{
		int ef = Min(sorted_hnsw_ef_search, 32);	/* use smaller ef for insert */

		cand_buf = palloc(sizeof(ScanCandidate) * ef);
		n_cand = shnsw_search_level(index, cache, query_vec, ep_nid,
									ef, 0, cand_buf, ef);
	}

	/* Select M best neighbors using simple closest-first (no heuristic for speed) */
	new_nid = n_nodes;	/* new node gets the next nid */
	selected = palloc(sizeof(int32) * 2 * M);
	reverse_added = palloc0(sizeof(bool) * 2 * M);
	new_sq8 = palloc(dim);
	{
		int n_sel = 0;
		int max_sel = 2 * M;

		/* cand_buf is already sorted by distance ascending */
		for (d = 0; d < Min(max_sel, n_cand); d++)
		{
			if (cand_buf[d].nid >= 0 && cand_buf[d].nid < n_nodes)
				selected[n_sel++] = cand_buf[d].nid;
		}

		/* Append new L0 node to the index */
		{
			BlockNumber new_blk;
			int			offset_in_page;
			Buffer		buf;
			Page		page;
			ShnswNodeHeader *nh;
			int32	   *nbrs;
			uint8	   *sq8;

			/* Check if there's room on the last L0 page */
			offset_in_page = new_nid % nodes_per_page;
			new_blk = l0_start + new_nid / nodes_per_page;

			if (offset_in_page == 0)
			{
				/* Need a new page */
				buf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW,
										 RBM_NORMAL, NULL);
			}
			else
			{
				buf = ReadBuffer(index, new_blk);
			}
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			page = BufferGetPage(buf);

			if (offset_in_page == 0)
			{
				/* Initialize new page */
				ShnswPageOpaque opaque;
				PageInit(page, BLCKSZ, sizeof(ShnswPageOpaqueData));
				opaque = ShnswPageGetOpaque(page);
				opaque->shnsw_page_type = SHNSW_PAGE_L0;
				opaque->shnsw_level = 0;
				opaque->shnsw_next = InvalidBlockNumber;
			}

			nh = (ShnswNodeHeader *)
				((char *) PageGetContents(page) + offset_in_page * node_size);
			nh->nid = new_nid;
			ItemPointerCopy(heap_tid, &nh->heap_tid);
			nh->level = 0;
			nh->n_neighbors = n_sel;
			nh->flags = 0;
			nh->padding = 0;

			nbrs = ShnswNodeNeighbors(nh);
			for (d = 0; d < n_sel; d++)
				nbrs[d] = selected[d];
			for (d = n_sel; d < 2 * M; d++)
				nbrs[d] = -1;

			/* SQ8 quantize */
			sq8 = ShnswNodeSQ8Vec(nh, M);
			for (d = 0; d < dim; d++)
			{
				new_sq8[d] = sq8_quantize(sv->x[d], cache->sq8_mins[d],
										  cache->sq8_scales[d]);
				sq8[d] = new_sq8[d];
			}
			shnsw_page_set_payload_end(page,
									   (Size) (offset_in_page + 1) * node_size);

			MarkBufferDirty(buf);
			log_newpage_buffer(buf, false);
			UnlockReleaseBuffer(buf);
		}

		/* Add reverse connections: for each selected neighbor, add new_nid */
		for (d = 0; d < n_sel; d++)
		{
			reverse_added[d] = shnsw_add_neighbor_on_page(index, selected[d], new_nid,
														  M, dim, l0_start, nodes_per_page);
		}

		/* Update metapage: increment node_count */
		{
			Buffer	mbuf = ReadBuffer(index, SHNSW_METAPAGE_BLKNO);
			Page	mpage;
			ShnswMetaPageData *meta;

			LockBuffer(mbuf, BUFFER_LOCK_EXCLUSIVE);
			mpage = BufferGetPage(mbuf);
			meta = (ShnswMetaPageData *) PageGetContents(mpage);
			meta->shnsw_node_count++;
			meta->shnsw_cache_gen++;
			new_cache_gen = meta->shnsw_cache_gen;
			shnsw_page_set_payload_end(mpage, sizeof(ShnswMetaPageData));
			MarkBufferDirty(mbuf);
			log_newpage_buffer(mbuf, false);
			UnlockReleaseBuffer(mbuf);
		}

		shnsw_scan_cache_record_insert(index, new_cache_gen, heap_tid, new_nid,
									   selected, reverse_added, n_sel,
									   new_sq8, M, dim);

		pfree(selected);
	}

	pfree(new_sq8);
	pfree(reverse_added);
	pfree(cand_buf);
	if (sv != (Svec *) DatumGetPointer(values[0]))
		pfree(sv);

	return false;	/* not unique */
}

/* ================================================================
 * Scan
 * ================================================================ */

/* (ShnswScanCache and related types defined at top of file) */

/* ---- SQ8 distance computation ---- */

static float
sq8_cosine_distance(const float *query, int query_dim,
					double query_inv_norm,
					const uint8 *sq8_vec,
					const float *mins, const float *scales, int dim)
{
	double	dot = 0.0, norm_v = 0.0;
	int		d;
	int		use_dim = Min(query_dim, dim);

	for (d = 0; d < use_dim; d++)
	{
		double qd = (double) query[d];
		double vd = (double) mins[d] + (double) sq8_vec[d] * (double) scales[d];

		dot += qd * vd;
		norm_v += vd * vd;
	}

	if (query_inv_norm == 0.0 || norm_v == 0.0)
		return 2.0f;
	return (float) (1.0 - (dot * query_inv_norm) / sqrt(norm_v));
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

	elog(DEBUG1, "shnsw_load_cache: loading %d nodes, dim=%d, M=%d, l0_start=%u, l0_npages=%d",
		 n_nodes, dim, M, l0_start, l0_npages);

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

	cache->l0_start = l0_start;
	cache->l0_npages = l0_npages;
	cache->nodes_per_page = ShnswL0NodesPerPage(M, dim);
	if (cache->nodes_per_page < 1)
		cache->nodes_per_page = 1;
	cache->node_size = MAXALIGN(ShnswNodeSize(M, dim));

	/* L0 nodes are decoded lazily page-by-page on first touch. */
	elog(DEBUG1, "shnsw_load_cache: SQ8 metadata loaded, deferring L0 decode");
	cache->nodes = palloc0(sizeof(ShnswCacheNode) * n_nodes);
	cache->l0_neighbors = palloc(shnsw_l0_neighbors_bytes(n_nodes, M));
	cache->sq8_data = palloc0(n_nodes * (Size) dim);
	cache->l0_page_loaded = palloc0(Max(l0_npages, 1));

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

		elog(DEBUG1, "shnsw_load_cache: loading upper level %d, start=%u, npages=%d, epp=%d",
			 lev, upper_starts[lev], upper_npages_arr[lev], entries_per_page);

		entries = palloc(sizeof(ShnswUpperNbr) * alloc);

		if (upper_npages_arr[lev] == 0 || upper_starts[lev] == 0)
		{
			cache->upper[lev] = entries;
			cache->upper_count[lev] = 0;
			cache->upper_nbr_idx[lev] = palloc(sizeof(int) * n_nodes);
			memset(cache->upper_nbr_idx[lev], -1, sizeof(int) * n_nodes);
			continue;
		}

		for (p = 0; p < upper_npages_arr[lev]; p++)
		{
			int		j;

			buf = ReadBuffer(index, upper_starts[lev] + p);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);

			for (j = 0; j < entries_per_page; j++)
			{
				ShnswUpperEntry *ue = (ShnswUpperEntry *)
					((char *) PageGetContents(page) + j * entry_size);
				int		k;

				/* End-of-data sentinel: nid=0 with n_neighbors=0 could be
				 * a real node, but padding byte will be non-zero for real
				 * entries written by build. Use a size check instead: */
				if (ue->n_neighbors < 0 || ue->n_neighbors > M)
					break;
				if (ue->nid < 0 || ue->nid >= n_nodes)
					break;

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

static ShnswScanCache *
shnsw_get_scan_cache(Relation index)
{
	Oid					relid = RelationGetRelid(index);
	uint64				cache_gen;
	ShnswScanCacheEntry *entry;
	bool				found;
	MemoryContext		old_ctx;

	shnsw_ensure_scan_cache_hash();
	cache_gen = shnsw_read_cache_generation(index);
	entry = hash_search(shnsw_scan_cache_hash, &relid, HASH_ENTER, &found);

	if (found &&
		entry->cache != NULL &&
		entry->cache_gen == cache_gen &&
		memcmp(&entry->locator, &index->rd_locator,
			   sizeof(RelFileLocator)) == 0)
		return entry->cache;

	if (found)
		shnsw_scan_cache_invalidate(entry);

	entry->cache_ctx = AllocSetContextCreate(TopMemoryContext,
											 "sorted_hnsw cached scan",
											 ALLOCSET_DEFAULT_SIZES);
	old_ctx = MemoryContextSwitchTo(entry->cache_ctx);
	entry->cache = shnsw_load_cache(index);
	MemoryContextSwitchTo(old_ctx);

	entry->cache_gen = cache_gen;
	entry->locator = index->rd_locator;

	return entry->cache;
}

/* ---- Graph search using scan cache ---- */
/* (ScanCandidate defined at top of file) */

static int
cmp_candidate_asc(const void *a, const void *b)
{
	float da = ((const ScanCandidate *)a)->dist;
	float db = ((const ScanCandidate *)b)->dist;
	if (da < db) return -1;
	if (da > db) return 1;
	return 0;
}

static inline bool
shnsw_visited_test(const uint64 *bits, int nwords, int32 nid)
{
	int w = nid / 64;

	if (nid < 0 || w >= nwords)
		return false;
	return (bits[w] & ((uint64) 1 << (nid % 64))) != 0;
}

static inline void
shnsw_visited_set(uint64 *bits, int32 nid)
{
	int w = nid / 64;

	bits[w] |= ((uint64) 1 << (nid % 64));
}

static inline void
shnsw_find_worst_candidate(const ScanCandidate *best, int n_best,
						   int *worst_idx, float *worst_dist)
{
	int i;
	int idx = 0;
	float dist;

	Assert(n_best > 0);

	dist = best[0].dist;
	for (i = 1; i < n_best; i++)
	{
		if (best[i].dist > dist)
		{
			idx = i;
			dist = best[i].dist;
		}
	}

	*worst_idx = idx;
	*worst_dist = dist;
}

/*
 * Greedy search at one level using SQ8 distances from the cache.
 * Returns sorted candidates (ascending distance).
 */
static int
shnsw_search_level(Relation index, ShnswScanCache *cache, const float *query,
				   int entry_nid, int ef, int level,
				   ScanCandidate *results, int max_results)
{
	uint64	   *visited_bits;
	int			visited_nwords;
	ScanCandidate *candidates;	/* min-sorted working set */
	ScanCandidate *best;		/* max-sorted result set */
	int			n_cand = 0, n_best = 0;
	int			dim;
	int			ret;
	int			cand_cap;
	int			worst_idx = 0;
	float		worst_dist = 0.0f;
	double		query_norm = 0.0;
	double		query_inv_norm = 0.0;
	int			patience;
	int			stale_steps = 0;
	int			q;

	dim = cache->dim;
	cand_cap = Max(ef * 8, 64);
	visited_nwords = Max(1, (cache->n_nodes + 63) / 64);
	patience = (level == 0) ? sorted_heap_hnsw_ef_patience : 0;
	for (q = 0; q < dim; q++)
	{
		double qd = (double) query[q];

		query_norm += qd * qd;
	}
	if (query_norm > 0.0)
		query_inv_norm = 1.0 / sqrt(query_norm);

	elog(DEBUG1, "shnsw_search_level: allocating cand_cap=%d n_nodes=%d", cand_cap, cache->n_nodes);
	visited_bits = palloc0(sizeof(uint64) * visited_nwords);
	elog(DEBUG1, "shnsw_search_level: visited=%p", visited_bits);
	candidates = palloc(sizeof(ScanCandidate) * cand_cap);
	elog(DEBUG1, "shnsw_search_level: candidates=%p", candidates);
	best = palloc(sizeof(ScanCandidate) * (ef + 2));
	elog(DEBUG1, "shnsw_search_level: best=%p, validating entry", best);

	/* Validate entry point */
	if (entry_nid < 0 || entry_nid >= cache->n_nodes)
	{
		pfree(visited_bits);
		pfree(candidates);
		pfree(best);
		return 0;
	}
	if (!shnsw_cache_ensure_node_loaded(index, cache, entry_nid))
	{
		elog(WARNING, "shnsw_search: entry nid=%d has NULL neighbors (n_nodes=%d)",
			 entry_nid, cache->n_nodes);
		pfree(visited_bits);
		pfree(candidates);
		pfree(best);
		return 0;
	}

	/* Seed with entry point */
	{
		float d;
		elog(DEBUG1, "shnsw_search: seeding entry_nid=%d, sq8_mins=%p, sq8_scales=%p, nodes[%d].nbrs=%p n_nbrs=%d",
			 entry_nid, cache->sq8_mins, cache->sq8_scales,
			 entry_nid, cache->nodes[entry_nid].neighbors,
			 cache->nodes[entry_nid].n_neighbors);
		d = sq8_cosine_distance(query, dim,
								query_inv_norm,
								cache->sq8_data + (Size)entry_nid * dim,
								cache->sq8_mins, cache->sq8_scales, dim);
		candidates[0].dist = d;
		candidates[0].nid = entry_nid;
		n_cand = 1;
		best[0].dist = d;
		best[0].nid = entry_nid;
		n_best = 1;
		worst_dist = d;
		worst_idx = 0;
		shnsw_visited_set(visited_bits, entry_nid);
	}

	while (n_cand > 0)
	{
		/* Pop nearest candidate */
		ScanCandidate nearest;
		int		nn, k;
		int		min_idx = 0;
		bool	improved = false;
		for (k = 1; k < n_cand; k++)
			if (candidates[k].dist < candidates[min_idx].dist)
				min_idx = k;
		nearest = candidates[min_idx];
		candidates[min_idx] = candidates[--n_cand];

		/* Check termination */
		if (nearest.dist > worst_dist && n_best >= ef)
			break;

		/* Get neighbors at this level */
		if (level == 0)
		{
			ShnswCacheNode *cn = &cache->nodes[nearest.nid];

			if (!shnsw_cache_ensure_node_loaded(index, cache, nearest.nid))
				continue;
			if (cn->neighbors == NULL)
				continue;	/* node not loaded, skip */
			nn = cn->n_neighbors;
			if (nn < 0 || nn > 2 * cache->M)
				nn = 0;		/* corrupted neighbor count, skip */
			for (k = 0; k < nn; k++)
			{
				int32 nbr = cn->neighbors[k];
				float nbr_dist;

				if (nbr < 0 || nbr >= cache->n_nodes ||
					shnsw_visited_test(visited_bits, visited_nwords, nbr))
					continue;
				shnsw_visited_set(visited_bits, nbr);

				if (!shnsw_cache_ensure_node_loaded(index, cache, nbr))
					continue;

				/* Skip deleted nodes */
				if (cache->nodes[nbr].flags & SHNSW_NODE_DELETED)
					continue;

				nbr_dist = sq8_cosine_distance(
					query, dim,
					query_inv_norm,
					cache->sq8_data + (Size)nbr * dim,
					cache->sq8_mins, cache->sq8_scales, dim);

				if (nbr_dist < worst_dist || n_best < ef)
				{
					if (n_cand >= cand_cap)
					{
						cand_cap *= 2;
						candidates = repalloc(candidates,
							sizeof(ScanCandidate) * cand_cap);
					}
					candidates[n_cand].dist = nbr_dist;
					candidates[n_cand].nid = nbr;
					n_cand++;

					if (n_best < ef)
					{
						best[n_best].dist = nbr_dist;
						best[n_best].nid = nbr;
						n_best++;
						improved = true;
						if (nbr_dist > worst_dist)
						{
							worst_dist = nbr_dist;
							worst_idx = n_best - 1;
						}
					}
					else
					{
						best[worst_idx].dist = nbr_dist;
						best[worst_idx].nid = nbr;
						shnsw_find_worst_candidate(best, n_best,
												   &worst_idx, &worst_dist);
						improved = true;
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

					if (nbr < 0 || nbr >= cache->n_nodes ||
						shnsw_visited_test(visited_bits, visited_nwords, nbr))
						continue;
					shnsw_visited_set(visited_bits, nbr);

					if (!shnsw_cache_ensure_node_loaded(index, cache, nbr))
						continue;

					if (cache->nodes[nbr].flags & SHNSW_NODE_DELETED)
						continue;

					nbr_dist = sq8_cosine_distance(
						query, dim,
						query_inv_norm,
						cache->sq8_data + (Size)nbr * dim,
						cache->sq8_mins, cache->sq8_scales, dim);

					if (nbr_dist < worst_dist || n_best < ef)
					{
						if (n_cand >= cand_cap)
						{
							cand_cap *= 2;
							candidates = repalloc(candidates,
								sizeof(ScanCandidate) * cand_cap);
						}
						candidates[n_cand].dist = nbr_dist;
						candidates[n_cand].nid = nbr;
						n_cand++;

					if (n_best < ef)
					{
						best[n_best].dist = nbr_dist;
						best[n_best].nid = nbr;
						n_best++;
						improved = true;
						if (nbr_dist > worst_dist)
						{
							worst_dist = nbr_dist;
							worst_idx = n_best - 1;
						}
						}
					else
					{
						best[worst_idx].dist = nbr_dist;
						best[worst_idx].nid = nbr;
						shnsw_find_worst_candidate(best, n_best,
												   &worst_idx, &worst_dist);
						improved = true;
					}
				}
			}
		}

		if (patience > 0 && n_best >= ef)
		{
			if (improved)
				stale_steps = 0;
			else if (++stale_steps >= patience)
				break;
		}
	}
	}

	/* Copy best to results */
	qsort(best, n_best, sizeof(ScanCandidate), cmp_candidate_asc);
	ret = Min(n_best, max_results);
	memcpy(results, best, sizeof(ScanCandidate) * ret);

	pfree(visited_bits);
	pfree(candidates);
	pfree(best);

	return ret;
}

/* ---- Result entry for exact rerank ---- */

struct ScanResult
{
	ItemPointerData tid;
	float8		exact_dist;
};

static int
cmp_result_asc(const void *a, const void *b)
{
	float8 da = ((const ScanResult *)a)->exact_dist;
	float8 db = ((const ScanResult *)b)->exact_dist;
	if (da < db) return -1;
	if (da > db) return 1;
	return 0;
}

static int
shnsw_rerank_candidates(Relation index, const ShnswScanCache *cache,
						   const Svec *query,
						   const ScanCandidate *candidates, int n_cand,
						   ScanResult *results, int max_results)
{
	Relation	heap;
	Snapshot	snap;
	TupleTableSlot *slot;
	int			i;
	int			n_results = 0;

	if (n_cand <= 0 || max_results <= 0)
		return 0;

	heap = table_open(index->rd_index->indrelid, AccessShareLock);
	snap = GetActiveSnapshot();
	slot = table_slot_create(heap, NULL);

	for (i = 0; i < n_cand && n_results < max_results; i++)
	{
		int32			nid = candidates[i].nid;
		ItemPointerData	htid;
		bool			fetched;

		if (nid < 0 || nid >= cache->n_nodes)
			continue;

		ItemPointerCopy(&cache->nodes[nid].heap_tid, &htid);

		if (!ItemPointerIsValid(&htid))
			continue;

		fetched = table_tuple_fetch_row_version(heap, &htid, snap, slot);
		if (fetched)
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

		ExecClearTuple(slot);
	}

	ExecDropSingleTupleTableSlot(slot);
	table_close(heap, AccessShareLock);

	return n_results;
}

/* ================================================================
 * Scan callbacks
 * ================================================================ */

typedef struct ShnswScanOpaqueData
{
	/* Per-scan memory that survives executor context churn and rescans */
	MemoryContext state_ctx;

	/* Deep-copied query vector (scan-owned, survives context resets) */
	Svec	   *query;

	/* Results: sorted by exact distance */
	int			n_results;
	int			result_idx;		/* next to return */
	ItemPointerData *result_tids;
	float8	   *result_dists;

	bool		first_call;
} ShnswScanOpaqueData;

typedef ShnswScanOpaqueData *ShnswScanOpaque;

static void
shnsw_reset_scan_state(ShnswScanOpaque so)
{
	MemoryContextReset(so->state_ctx);
	so->query = NULL;
	so->n_results = 0;
	so->result_idx = 0;
	so->result_tids = NULL;
	so->result_dists = NULL;
}

static bool
shnsw_copy_query_arg(ShnswScanOpaque so, ScanKey orderby)
{
	MemoryContext old_ctx;
	Svec	   *copy;

	if (orderby == NULL || (orderby->sk_flags & SK_ISNULL))
		return false;

	old_ctx = MemoryContextSwitchTo(so->state_ctx);
	copy = (Svec *) PG_DETOAST_DATUM_COPY(orderby->sk_argument);
	MemoryContextSwitchTo(old_ctx);

	so->query = copy;
	return true;
}

static IndexScanDesc
shnsw_beginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	ShnswScanOpaque so;

	scan = RelationGetIndexScan(index, nkeys, norderbys);

	/*
	 * Ordered scans write xs_orderbyvals/xs_orderbynulls on every returned
	 * tuple. Do not rely on RelationGetIndexScan() to initialize them for us;
	 * on PG18 these fields can be left as indeterminate garbage on first use.
	 * Fresh scan-owned arrays are cheap and avoid session-dependent crashes.
	 */
	if (norderbys > 0)
	{
		scan->xs_orderbyvals = palloc0(sizeof(Datum) * norderbys);
		scan->xs_orderbynulls = palloc0(sizeof(bool) * norderbys);
		memset(scan->xs_orderbynulls, true, sizeof(bool) * norderbys);
	}

	so = (ShnswScanOpaque) palloc0(sizeof(ShnswScanOpaqueData));
	so->state_ctx = AllocSetContextCreate(CurrentMemoryContext,
										  "sorted_hnsw scan state",
										  ALLOCSET_DEFAULT_SIZES);
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
	shnsw_reset_scan_state(so);

	if (norderbys > 0 && orderbys)
	{
		/* Deep-copy ORDER BY argument from the orderbys parameter
		 * (which the executor guarantees is valid at rescan time).
		 * Copy into scan-owned storage before it can be freed. */
		(void) shnsw_copy_query_arg(so, &orderbys[0]);

		memmove(scan->orderByData, orderbys,
				sizeof(ScanKeyData) * norderbys);
	}
}

static bool
shnsw_gettuple(IndexScanDesc scan, ScanDirection direction)
{
	ShnswScanOpaque so = (ShnswScanOpaque) scan->opaque;

	if (scan->numberOfOrderBys < 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("sorted_hnsw only supports ordered scans with ORDER BY distance")));

	if (so->first_call)
	{
		Relation		index = scan->indexRelation;
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
		MemoryContext	old_ctx = CurrentMemoryContext;
		MemoryContext	scan_ctx = NULL;

		so->first_call = false;

		/*
		 * Materialize the ORDER BY query into scan-owned memory. The executor's
		 * ScanKey storage is not ours to retain across context churn.
		 */
		if (so->query == NULL &&
			(scan->orderByData == NULL ||
			 !shnsw_copy_query_arg(so, &scan->orderByData[0])))
		{
			so->n_results = 0;
			goto done_search;
		}
		query = so->query;

		/* Use a scan-scoped memory context for cache allocations */
		scan_ctx = AllocSetContextCreate(CurrentMemoryContext,
										 "sorted_hnsw scan",
										 ALLOCSET_DEFAULT_SIZES);
		old_ctx = MemoryContextSwitchTo(scan_ctx);

		elog(DEBUG1, "sorted_hnsw scan: query dim=%d", query->dim);

		/* Load index into cache */
		cache = shnsw_get_scan_cache(index);
		if (cache->n_nodes == 0)
		{
			so->n_results = 0;
			goto done_search;
		}

		elog(DEBUG1, "sorted_hnsw scan: cache loaded, %d nodes, entry=%d, max_level=%d",
			 cache->n_nodes, cache->entry_nid, cache->max_level);

		ef = cache->ef_search;
		ep_nid = cache->entry_nid;

		/* Navigate upper levels (ef=1, greedy) */
		for (level = cache->max_level; level >= 1; level--)
		{
			ScanCandidate one;
			int found;

			memset(&one, 0, sizeof(one));
			found = shnsw_search_level(index, cache, query->x, ep_nid,
									   1, level, &one, 1);
			if (found > 0)
				ep_nid = one.nid;
			elog(DEBUG1, "sorted_hnsw scan: upper level %d → ep=%d",
				 level, ep_nid);
		}

		/* Search L0 with full ef */
		elog(DEBUG1, "sorted_hnsw scan: starting L0 search ep=%d ef=%d nodes=%d dim=%d sq8=%p",
			 ep_nid, ef, cache->n_nodes, cache->dim,
			 cache->sq8_data);
		candidates = palloc(sizeof(ScanCandidate) * ef);
		n_cand = shnsw_search_level(index, cache, query->x, ep_nid,
									ef, 0, candidates, ef);

		elog(DEBUG1, "sorted_hnsw scan: L0 search found %d candidates", n_cand);

		results = palloc(sizeof(ScanResult) * Max(ef, 1));
		n_results = shnsw_rerank_candidates(index, cache, query,
											candidates, n_cand,
											results, Max(ef, 1));

		/*
		 * Writable tables can leave dead heap tuples behind the graph. If the
		 * first ef-sized batch underflows after MVCC visibility checks, do one
		 * bounded top-up pass with a wider beam before returning fewer rows.
		 */
		if (n_results < Min(ef, cache->n_nodes))
		{
			int				topup_ef = Min(cache->n_nodes, ef * 2);

			if (topup_ef > ef)
			{
				ScanCandidate  *topup_candidates;
				ScanResult	   *topup_results;
				int				topup_n_cand;
				int				topup_n_results;

				topup_candidates = palloc(sizeof(ScanCandidate) * topup_ef);
				topup_n_cand = shnsw_search_level(index, cache, query->x, ep_nid,
												 topup_ef, 0,
												 topup_candidates, topup_ef);

				topup_results = palloc(sizeof(ScanResult) * topup_ef);
				topup_n_results = shnsw_rerank_candidates(index, cache, query,
														  topup_candidates,
														  topup_n_cand,
														  topup_results,
														  topup_ef);

				if (topup_n_results > n_results)
				{
					pfree(results);
					results = topup_results;
					n_results = topup_n_results;
				}
				else
					pfree(topup_results);

				pfree(topup_candidates);
			}
		}

		elog(DEBUG1, "sorted_hnsw scan: reranked %d results", n_results);

		/* Sort by exact distance */
		qsort(results, n_results, sizeof(ScanResult), cmp_result_asc);

		/* Store in scan state (allocate in outer context, survives scan_ctx delete) */
		so->n_results = n_results;
		so->result_idx = 0;
		MemoryContextSwitchTo(so->state_ctx);
		so->result_tids = palloc(sizeof(ItemPointerData) * Max(n_results, 1));
		so->result_dists = palloc(sizeof(float8) * Max(n_results, 1));
		MemoryContextSwitchTo(scan_ctx);
		for (i = 0; i < n_results; i++)
		{
			ItemPointerCopy(&results[i].tid, &so->result_tids[i]);
			so->result_dists[i] = results[i].exact_dist;
		}

		pfree(results);
		pfree(candidates);

done_search:
		if (scan_ctx != NULL)
		{
			MemoryContextSwitchTo(old_ctx);
			MemoryContextDelete(scan_ctx);
		}
	}

	/* Return next result */
	if (so->result_idx < so->n_results)
	{
		scan->xs_heaptid = so->result_tids[so->result_idx];
		scan->xs_recheck = false;

		if (scan->numberOfOrderBys > 0 &&
			scan->xs_orderbyvals != NULL &&
			scan->xs_orderbynulls != NULL)
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

	if (so->state_ctx != NULL)
		MemoryContextDelete(so->state_ctx);

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
	Relation	index;
	int			dim;
	int			M;
	int			sq8_npages;
	int			l0_npages;
	int			nodes_per_page;
	int			lev;
	double		ef;
	double		limit_tuples;
	double		visited;
	double		live_cand;
	double		nav_cpu;
	double		heap_cpu, heap_io;
	double		toast_chunks, rerank_io, rerank_cpu;
	double		startup_pages;
	double		l0_touch_pages;

	/*
	 * sorted_hnsw only supports ORDER BY distance scans. Cost plain index
	 * scans out of consideration so the planner will not pick this AM for
	 * unordered queries like COUNT(*), even with enable_seqscan=off.
	 */
	if (path->indexorderbys == NIL)
	{
		*indexStartupCost = 1.0e12;
		*indexTotalCost = 1.0e12;
		*indexSelectivity = 1.0;
		*indexCorrelation = 0.0;
		*indexPages = 0;
		return;
	}

	/*
	 * Phase 1 sorted_hnsw only produces up to ef_search ordered candidates
	 * per scan. Do not let the planner treat it as a general ORDER BY path
	 * when the query has no LIMIT or asks for more rows than the AM can
	 * faithfully return.
	 */
	limit_tuples = root->limit_tuples;
	if (limit_tuples < 0 || limit_tuples > (double) sorted_hnsw_ef_search)
	{
		*indexStartupCost = 1.0e12;
		*indexTotalCost = 1.0e12;
		*indexSelectivity = 1.0;
		*indexCorrelation = 0.0;
		*indexPages = 0;
		return;
	}

	index = index_open(path->indexinfo->indexoid, NoLock);

	dim = TupleDescAttr(index->rd_att, 0)->atttypmod;
	if (dim <= 0)
		dim = 768;	/* fallback */
	M = SHNSW_DEFAULT_M;
	sq8_npages = 0;
	l0_npages = 0;
	startup_pages = 1.0;	/* metapage */

	{
		Buffer				buf;
		Page				page;
		ShnswMetaPageData  *meta;

		buf = ReadBuffer(index, SHNSW_METAPAGE_BLKNO);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		meta = (ShnswMetaPageData *) PageGetContents(page);
		if (meta->shnsw_magic == SORTED_HNSW_MAGIC)
		{
			dim = meta->shnsw_dim > 0 ? meta->shnsw_dim : dim;
			M = meta->shnsw_m > 0 ? meta->shnsw_m : M;
			sq8_npages = Max(meta->shnsw_sq8_npages, 0);
			l0_npages = Max(meta->shnsw_l0_npages, 0);
			startup_pages += sq8_npages;
			for (lev = 1; lev < SHNSW_MAX_LEVELS; lev++)
				startup_pages += Max(meta->shnsw_upper_npages[lev], 0);
		}
		UnlockReleaseBuffer(buf);
	}

	ef = (double) sorted_hnsw_ef_search;

	/* Cap ef at actual table size — no point searching more nodes than exist */
	if (rel->tuples > 0 && ef > rel->tuples)
		ef = rel->tuples;

	visited = ef * 1.5;
	live_cand = ef;
	nodes_per_page = ShnswL0NodesPerPage(M, dim);
	if (nodes_per_page < 1)
		nodes_per_page = 1;
	l0_touch_pages = ceil(visited / (double) nodes_per_page);
	if (l0_npages > 0)
		l0_touch_pages = Min(l0_touch_pages, (double) l0_npages);
	startup_pages += l0_touch_pages;

	/*
	 * Phase 0: Cache load startup cost.
	 * Cold scans read the metapage, SQ8 aux pages, upper levels, and only the
	 * subset of L0 pages touched by the beam. Warm scans stay CPU-only.
	 */
	{
		if (shnsw_scan_cache_is_warm(index))
		{
			/*
			 * Backend-local decoded cache already exists and matches the
			 * current relfilenode/cache generation, so startup is CPU-only.
			 * Keep a small non-zero term so the planner still distinguishes
			 * warm scans from essentially free operators.
			 */
			*indexStartupCost = Max(1.0, startup_pages * cpu_operator_cost);
		}
		else
		{
			*indexStartupCost = startup_pages * seq_page_cost * 0.5;
		}
	}

	/* Phase 1: SQ8 navigation (CPU only, cache is in memory after load) */
	nav_cpu = visited * dim * cpu_operator_cost / 200.0;

	/* Phase 2: Heap fetch + visibility for each candidate */
	heap_cpu = live_cand * cpu_tuple_cost;
	heap_io = live_cand * seq_page_cost * 0.1;

	/* Phase 3: Exact rerank (cosine distance)
	 * For small dims: inline svec, no TOAST. For large dims: TOAST reads. */
	toast_chunks = (dim * 4 > 2000) ? ceil((double)(dim * 4) / 2000.0) : 0;
	rerank_io = live_cand * toast_chunks * random_page_cost * 0.3;
	rerank_cpu = live_cand * dim * cpu_operator_cost / 100.0;

	*indexTotalCost = *indexStartupCost +
					  nav_cpu +
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
	Relation	index = info->index;
	Buffer		mbuf;
	Page		mpage;
	ShnswMetaPageData *meta;
	int			M, dim, n_nodes;
	BlockNumber	l0_start;
	int			l0_npages;
	int			nodes_per_page, node_size;
	int			i;
	int			n_deleted = 0;

	if (stats == NULL)
		stats = palloc0(sizeof(IndexBulkDeleteResult));

	/* Read metapage */
	mbuf = ReadBuffer(index, SHNSW_METAPAGE_BLKNO);
	LockBuffer(mbuf, BUFFER_LOCK_SHARE);
	mpage = BufferGetPage(mbuf);
	meta = (ShnswMetaPageData *) PageGetContents(mpage);

	if (meta->shnsw_magic != SORTED_HNSW_MAGIC)
	{
		UnlockReleaseBuffer(mbuf);
		return stats;
	}

	M = meta->shnsw_m;
	dim = meta->shnsw_dim;
	n_nodes = meta->shnsw_node_count;
	l0_start = meta->shnsw_l0_start;
	l0_npages = meta->shnsw_l0_npages;
	UnlockReleaseBuffer(mbuf);

	if (n_nodes == 0)
		return stats;

	node_size = MAXALIGN(ShnswNodeSize(M, dim));
	nodes_per_page = ShnswL0NodesPerPage(M, dim);
	if (nodes_per_page < 1) nodes_per_page = 1;

	/* Scan L0 pages, mark deleted nodes */
	for (i = 0; i < l0_npages; i++)
	{
		Buffer	buf;
		Page	page;
		int		j;
		int		page_count;
		bool	page_dirty = false;

		buf = ReadBuffer(index, l0_start + i);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);

		page_count = Min(nodes_per_page, n_nodes - i * nodes_per_page);
		for (j = 0; j < page_count; j++)
		{
			ShnswNodeHeader *nh = (ShnswNodeHeader *)
				((char *) PageGetContents(page) + j * node_size);

			if (nh->nid < 0 || (nh->flags & SHNSW_NODE_DELETED))
				continue;

			/* Check if this heap TID should be deleted */
			if (callback(&nh->heap_tid, callback_state))
			{
				nh->flags |= SHNSW_NODE_DELETED;
				page_dirty = true;
				n_deleted++;
			}
		}

		if (page_dirty)
		{
			MarkBufferDirty(buf);
			log_newpage_buffer(buf, false);
		}
		UnlockReleaseBuffer(buf);
	}

	stats->tuples_removed = n_deleted;
	stats->num_index_tuples = n_nodes - n_deleted;

	if (n_deleted > 0)
	{
		Buffer		update_mbuf;
		Page		update_mpage;
		ShnswMetaPageData *update_meta;

		update_mbuf = ReadBuffer(index, SHNSW_METAPAGE_BLKNO);
		LockBuffer(update_mbuf, BUFFER_LOCK_EXCLUSIVE);
		update_mpage = BufferGetPage(update_mbuf);
		update_meta = (ShnswMetaPageData *) PageGetContents(update_mpage);
		update_meta->shnsw_cache_gen++;
		shnsw_page_set_payload_end(update_mpage, sizeof(ShnswMetaPageData));
		MarkBufferDirty(update_mbuf);
		log_newpage_buffer(update_mbuf, false);
		UnlockReleaseBuffer(update_mbuf);
	}

	return stats;
}

static IndexBulkDeleteResult *
shnsw_vacuumcleanup(IndexVacuumInfo *info,
					 IndexBulkDeleteResult *stats)
{
	if (stats == NULL)
		stats = palloc0(sizeof(IndexBulkDeleteResult));

	stats->num_pages = RelationGetNumberOfBlocks(info->index);

	/* If bulkdelete was not called (no dead tuples), count tuples */
	if (stats->num_index_tuples == 0)
	{
		Buffer		mbuf;
		Page		mpage;
		ShnswMetaPageData *meta;

		mbuf = ReadBuffer(info->index, SHNSW_METAPAGE_BLKNO);
		LockBuffer(mbuf, BUFFER_LOCK_SHARE);
		mpage = BufferGetPage(mbuf);
		meta = (ShnswMetaPageData *) PageGetContents(mpage);
		if (meta->shnsw_magic == SORTED_HNSW_MAGIC)
			stats->num_index_tuples = meta->shnsw_node_count;
		UnlockReleaseBuffer(mbuf);
	}

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
