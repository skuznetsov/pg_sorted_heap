/*
 * sorted_hnsw.h
 *
 * HNSW Index Access Method for pg_sorted_heap.
 *
 * Page layout:
 *   Page 0:        Metapage (entry point, params, SQ8 aux page refs)
 *   Pages 1..S:    SQ8 auxiliary pages (per-dim min/scale arrays)
 *   Pages S+1..N:  L0 node pages (nid, heap_tid, neighbors, sq8_vec)
 *   Pages N+1..:   Upper level pages (nid, neighbors, hsvec sketch)
 */
#ifndef SORTED_HNSW_H
#define SORTED_HNSW_H

#include "postgres.h"
#include "access/amapi.h"
#include "access/generic_xlog.h"
#include "access/genam.h"
#include "access/itup.h"
#include "access/tableam.h"
#include "nodes/pathnodes.h"
#include "storage/bufmgr.h"
#include "utils/relcache.h"

/* ---- Constants ---- */

#define SORTED_HNSW_MAGIC			0x48535748	/* "HSWH" */
#define SORTED_HNSW_VERSION			1

/* Maximum M (edges per node per layer) */
#define SHNSW_MAX_M					64
/* Maximum ef_construction */
#define SHNSW_MAX_EF_CONSTRUCTION	1000
/* Maximum HNSW levels (geometric dist: P(level>k) = (1/ln(M))^k) */
#define SHNSW_MAX_LEVELS			16
/* Maximum vector dimension */
#define SHNSW_MAX_DIM				16000

/* Metapage is always block 0 */
#define SHNSW_METAPAGE_BLKNO		0

/* Metapage flags */
#define SHNSW_FLAG_SQ8_VALID		0x0001
#define SHNSW_FLAG_NEEDS_REINDEX	0x0002

/* Special page types (stored in page special space) */
#define SHNSW_PAGE_META				1
#define SHNSW_PAGE_SQ8_AUX			2
#define SHNSW_PAGE_L0				3
#define SHNSW_PAGE_UPPER			4

/* ---- Page special data ---- */

typedef struct ShnswPageOpaqueData
{
	uint16		shnsw_page_type;	/* SHNSW_PAGE_* */
	uint16		shnsw_level;		/* 0 for L0, 1+ for upper */
	BlockNumber	shnsw_next;			/* next page in chain (for SQ8 aux) */
} ShnswPageOpaqueData;

typedef ShnswPageOpaqueData *ShnswPageOpaque;

#define ShnswPageGetOpaque(page) \
	((ShnswPageOpaque) PageGetSpecialPointer(page))

/* ---- Metapage ---- */

typedef struct ShnswMetaPageData
{
	uint32		shnsw_magic;
	uint16		shnsw_version;
	uint16		shnsw_flags;

	/* HNSW parameters */
	int16		shnsw_m;				/* M parameter */
	int16		shnsw_ef_construction;	/* build-time beam width */
	int32		shnsw_dim;				/* vector dimension */
	int32		shnsw_node_count;		/* total nodes in graph */
	int32		shnsw_entry_nid;		/* entry point node ID */
	int16		shnsw_max_level;		/* highest level in graph */

	/* SQ8 auxiliary page chain */
	BlockNumber	shnsw_sq8_start;		/* first SQ8 aux page */
	int32		shnsw_sq8_npages;		/* number of SQ8 aux pages */

	/* Level page ranges (start block for each level) */
	BlockNumber	shnsw_l0_start;			/* first L0 node page */
	int32		shnsw_l0_npages;		/* number of L0 pages */
	BlockNumber	shnsw_upper_start[SHNSW_MAX_LEVELS];
	int32		shnsw_upper_npages[SHNSW_MAX_LEVELS];

	/* SQ8 drift tracking */
	int32		shnsw_sq8_oor_count;	/* out-of-range insert count */
	uint64		shnsw_cache_gen;		/* bump on scan-visible graph changes */
} ShnswMetaPageData;

/* ---- L0 Node (packed in index pages) ---- */

/*
 * L0 node layout (variable size based on M and dim):
 *   int32       nid
 *   ItemPointerData heap_tid  (6 bytes)
 *   int16       level         (node's max level, for upper-level membership)
 *   int16       n_neighbors   (actual neighbor count, <= 2*M)
 *   uint8       flags         (SHNSW_NODE_DELETED etc.)
 *   uint8       padding
 *   int32       neighbors[2*M]  (fixed-size array, unused slots = -1)
 *   uint8       sq8_vec[dim]    (SQ8-quantized vector)
 *
 * We use a fixed-size layout per index to simplify page management.
 * Node size = 4 + 6 + 2 + 2 + 1 + 1 + 4*2*M + dim
 * For M=16, dim=2880: 16 + 128 + 2880 = 3024 bytes
 */

#define SHNSW_NODE_DELETED		0x01

typedef struct ShnswNodeHeader
{
	int32			nid;
	ItemPointerData	heap_tid;
	int16			level;
	int16			n_neighbors;
	uint8			flags;
	uint8			padding;
	/* followed by: int32 neighbors[2*M], then uint8 sq8_vec[dim] */
} ShnswNodeHeader;

#define SHNSW_NODE_HEADER_SIZE	(offsetof(ShnswNodeHeader, padding) + 1)

/* Access helpers — call with pointer to ShnswNodeHeader */
#define ShnswNodeNeighbors(node) \
	((int32 *) ((char *)(node) + sizeof(ShnswNodeHeader)))

#define ShnswNodeSQ8Vec(node, M) \
	((uint8 *) ((char *)(node) + sizeof(ShnswNodeHeader) + sizeof(int32) * 2 * (M)))

/* Total node size for given M and dim */
#define ShnswNodeSize(M, dim) \
	(sizeof(ShnswNodeHeader) + sizeof(int32) * 2 * (M) + (dim))

/* How many L0 nodes fit in one 8KB page (minus headers and special) */
#define ShnswL0NodesPerPage(M, dim) \
	((int)((BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - \
	        MAXALIGN(sizeof(ShnswPageOpaqueData))) / \
	       MAXALIGN(ShnswNodeSize(M, dim))))

/* ---- Upper Level Node ---- */

/*
 * Upper level entry: compact neighbor list only.
 * SQ8 vectors are looked up from L0 during scan.
 * Layout:
 *   int32       nid
 *   int16       n_neighbors
 *   int16       padding
 *   int32       neighbors[M]  (upper levels use M, not 2*M)
 */
typedef struct ShnswUpperEntry
{
	int32		nid;
	int16		n_neighbors;
	int16		padding;
	/* followed by: int32 neighbors[M] */
} ShnswUpperEntry;

#define ShnswUpperEntryNeighbors(entry) \
	((int32 *) ((char *)(entry) + sizeof(ShnswUpperEntry)))

#define ShnswUpperEntrySize(M) \
	(sizeof(ShnswUpperEntry) + sizeof(int32) * (M))

#define ShnswUpperEntriesPerPage(M) \
	((int)((BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - \
	        MAXALIGN(sizeof(ShnswPageOpaqueData))) / \
	       MAXALIGN(ShnswUpperEntrySize(M))))

/* ---- Reloptions ---- */

typedef struct ShnswOptions
{
	int32		vl_len_;		/* varlena header */
	int			m;
	int			ef_construction;
} ShnswOptions;

#define SHNSW_DEFAULT_M					16
#define SHNSW_DEFAULT_EF_CONSTRUCTION	200

/* ---- GUCs ---- */

extern int	sorted_hnsw_ef_search;
extern bool	sorted_hnsw_sq8;
extern bool	sorted_hnsw_build_sq8;
extern bool	sorted_hnsw_shared_cache;

/* ---- AM handler ---- */

extern IndexAmRoutine *sorted_hnsw_handler_internal(void);

/* Init function (called from _PG_init) */
extern void sorted_hnsw_init(void);

/* ---- Build graph (hnsw_build.c) ---- */

/* Opaque build state — defined in hnsw_build.c */
typedef struct HnswBuildState HnswBuildState;

typedef enum HnswBuildVectorMode
{
	SHNSW_BUILD_VECTOR_F32 = 0,
	SHNSW_BUILD_VECTOR_SQ8 = 1
} HnswBuildVectorMode;

/* In-memory node info, for reading back after build */
typedef struct HnswBuiltNode
{
	int32			nid;
	int16			level;
	ItemPointerData	heap_tid;
	int32		  **neighbors;		/* per-level neighbor arrays */
	int16		   *n_neighbors;	/* count per level */
} HnswBuiltNode;

extern HnswBuildState *shnsw_build_graph(float *vectors_f32,
										  const uint8 *vectors_sq8,
										  const float *sq8_mins,
										  const float *sq8_scales,
										  ItemPointer tids,
										  int n_nodes, int dim,
										  int M, int ef_construction,
										  HnswBuildVectorMode vector_mode,
										  MemoryContext build_ctx);

/* Accessors for build state */
extern int			shnsw_build_max_level(HnswBuildState *state);
extern int			shnsw_build_entry_nid(HnswBuildState *state);
extern HnswBuiltNode *shnsw_build_get_node(HnswBuildState *state, int nid);

#endif							/* SORTED_HNSW_H */
