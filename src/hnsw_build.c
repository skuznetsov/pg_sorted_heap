/*
 * hnsw_build.c
 *
 * In-memory HNSW graph construction for the sorted_hnsw Index AM.
 * Port of Malkov & Yashunin 2018 "Efficient and robust approximate
 * nearest neighbor using Hierarchical Navigable Small World graphs".
 *
 * The build produces an in-memory graph that is then serialized to
 * index pages by shnsw_build() in sorted_hnsw.c.
 */
#include "postgres.h"

#include <math.h>
#include <float.h>

#include "sorted_hnsw.h"
#include "svec.h"
#include "common/pg_prng.h"
#include "miscadmin.h"
#include "utils/memutils.h"

/* ---- In-memory graph representation ---- */

typedef struct HnswNode
{
	int32		nid;
	int16		level;			/* node's assigned level */
	ItemPointerData heap_tid;

	/* Neighbors per level: neighbors[lev] is an array of nids */
	int32	  **neighbors;		/* neighbors[0..level] */
	int16	   *n_neighbors;	/* count per level */
} HnswNode;

typedef struct HnswBuildState
{
	/* Parameters */
	int			M;				/* max neighbors per layer */
	int			M_max0;			/* max neighbors at layer 0 (= 2*M) */
	int			ef_construction;
	double		ml;				/* level multiplier: 1/ln(M) */
	int			dim;

	/* Graph */
	int			n_nodes;
	int			max_level;
	int			entry_nid;		/* entry point node */
	HnswNode   *nodes;			/* array[n_nodes] */

	/* Vectors (float32, row-major) */
	float	   *vectors;		/* n_nodes * dim */

	/* Reusable visitation marks for build-time searches */
	uint32	   *visit_marks;	/* array[n_nodes], zero means unvisited */
	uint32		visit_token;

	/* Memory */
	MemoryContext build_ctx;
} HnswBuildState;

/* ---- Priority queue (min-heap by distance) ---- */

typedef struct PQEntry
{
	float		dist;
	int32		nid;
} PQEntry;

typedef struct PQueue
{
	PQEntry    *entries;
	int			size;
	int			capacity;
	bool		is_max_heap;	/* false = min-heap, true = max-heap */
} PQueue;

static PQueue *
pq_create(int capacity, bool is_max_heap)
{
	PQueue *pq = palloc(sizeof(PQueue));
	pq->entries = palloc(sizeof(PQEntry) * capacity);
	pq->size = 0;
	pq->capacity = capacity;
	pq->is_max_heap = is_max_heap;
	return pq;
}

static inline bool
pq_cmp(PQueue *pq, int a, int b)
{
	if (pq->is_max_heap)
		return pq->entries[a].dist > pq->entries[b].dist;
	else
		return pq->entries[a].dist < pq->entries[b].dist;
}

static inline void
pq_swap(PQueue *pq, int a, int b)
{
	PQEntry tmp = pq->entries[a];
	pq->entries[a] = pq->entries[b];
	pq->entries[b] = tmp;
}

static void
pq_sift_up(PQueue *pq, int i)
{
	while (i > 0)
	{
		int parent = (i - 1) / 2;
		if (pq_cmp(pq, i, parent))
		{
			pq_swap(pq, i, parent);
			i = parent;
		}
		else
			break;
	}
}

static void
pq_sift_down(PQueue *pq, int i)
{
	while (true)
	{
		int best = i;
		int left = 2 * i + 1;
		int right = 2 * i + 2;

		if (left < pq->size && pq_cmp(pq, left, best))
			best = left;
		if (right < pq->size && pq_cmp(pq, right, best))
			best = right;
		if (best == i)
			break;
		pq_swap(pq, i, best);
		i = best;
	}
}

static void
pq_push(PQueue *pq, float dist, int32 nid)
{
	if (pq->size >= pq->capacity)
	{
		pq->capacity *= 2;
		pq->entries = repalloc(pq->entries, sizeof(PQEntry) * pq->capacity);
	}
	pq->entries[pq->size].dist = dist;
	pq->entries[pq->size].nid = nid;
	pq->size++;
	pq_sift_up(pq, pq->size - 1);
}

static PQEntry
pq_pop(PQueue *pq)
{
	PQEntry top = pq->entries[0];
	pq->size--;
	if (pq->size > 0)
	{
		pq->entries[0] = pq->entries[pq->size];
		pq_sift_down(pq, 0);
	}
	return top;
}

static inline float
pq_top_dist(PQueue *pq)
{
	return pq->entries[0].dist;
}

/* ---- Distance computation ---- */

static float
cosine_distance_f32(const float *a, const float *b, int dim)
{
	double dot = 0.0, norm_a = 0.0, norm_b = 0.0;
	int i;

	for (i = 0; i < dim; i++)
	{
		dot += (double)a[i] * (double)b[i];
		norm_a += (double)a[i] * (double)a[i];
		norm_b += (double)b[i] * (double)b[i];
	}

	if (norm_a == 0.0 || norm_b == 0.0)
		return 2.0f;

	return (float)(1.0 - dot / (sqrt(norm_a) * sqrt(norm_b)));
}

/* ---- Level selection ---- */

static int
select_level(double ml)
{
	/* Geometric distribution: level = floor(-ln(uniform) * ml) */
	double r = pg_prng_double(&pg_global_prng_state);

	if (r < 1e-10)
		r = 1e-10;
	return (int)(-log(r) * ml);
}

static inline uint32
next_visit_token(HnswBuildState *state)
{
	state->visit_token++;
	if (state->visit_token == 0)
	{
		memset(state->visit_marks, 0, sizeof(uint32) * (Size) state->n_nodes);
		state->visit_token = 1;
	}
	return state->visit_token;
}

/* ---- Core HNSW algorithms ---- */

/*
 * search_layer: greedy beam search at a single level.
 *
 * Returns a max-heap of the ef closest nodes found.
 * visited[] is a boolean array indexed by nid.
 */
static PQueue *
search_layer(HnswBuildState *state, const float *query,
			 int entry_nid, int ef, int level)
{
	PQueue	   *candidates;		/* min-heap: next to explore */
	PQueue	   *result;			/* max-heap: ef nearest */
	float		entry_dist;
	int			dim = state->dim;
	uint32		visit_token = next_visit_token(state);
	uint32	   *visited = state->visit_marks;

	entry_dist = cosine_distance_f32(query,
									 state->vectors + (Size)entry_nid * dim,
									 dim);

	candidates = pq_create(ef * 2, false);	/* min-heap */
	result = pq_create(ef + 1, true);		/* max-heap */

	pq_push(candidates, entry_dist, entry_nid);
	pq_push(result, entry_dist, entry_nid);
	visited[entry_nid] = visit_token;

	while (candidates->size > 0)
	{
		PQEntry		nearest;
		float		furthest_dist;
		HnswNode   *node;
		int			i;

		nearest = pq_pop(candidates);

		furthest_dist = pq_top_dist(result);
		if (nearest.dist > furthest_dist && result->size >= ef)
			break;

		node = &state->nodes[nearest.nid];

		/* Check level exists for this node */
		if (level > node->level)
			continue;

		for (i = 0; i < node->n_neighbors[level]; i++)
		{
			int32	nbr_nid = node->neighbors[level][i];
			float	nbr_dist;

			if (nbr_nid < 0 || nbr_nid >= state->n_nodes)
				continue;
			if (visited[nbr_nid] == visit_token)
				continue;
			visited[nbr_nid] = visit_token;

			nbr_dist = cosine_distance_f32(query,
										   state->vectors + (Size)nbr_nid * dim,
										   dim);

			furthest_dist = pq_top_dist(result);
			if (nbr_dist < furthest_dist || result->size < ef)
			{
				pq_push(candidates, nbr_dist, nbr_nid);
				pq_push(result, nbr_dist, nbr_nid);
				if (result->size > ef)
					pq_pop(result);		/* remove furthest */
			}
		}
	}

	pfree(candidates->entries);
	pfree(candidates);

	return result;
}

/*
 * select_neighbors_heuristic: Malkov's neighbor selection with pruning.
 *
 * From candidate set, select up to M neighbors that are diverse
 * (prefer candidates closer to the query than to existing selections).
 *
 * candidates: max-heap of candidates (modified in place)
 * M: max neighbors to return
 *
 * Returns array of selected nids and count.
 */
static int
select_neighbors_heuristic(HnswBuildState *state,
						   PQueue *candidates, int M,
						   int32 *out_nids)
{
	PQEntry    *sorted;
	int			n_cand;
	int			n_selected = 0;
	int			i, j;
	int			dim = state->dim;

	/* Extract all candidates sorted by distance (ascending) */
	n_cand = candidates->size;
	sorted = palloc(sizeof(PQEntry) * n_cand);
	for (i = 0; i < n_cand; i++)
		sorted[i] = candidates->entries[i];

	/* Sort by distance ascending */
	for (i = 0; i < n_cand - 1; i++)
		for (j = i + 1; j < n_cand; j++)
			if (sorted[j].dist < sorted[i].dist)
			{
				PQEntry tmp = sorted[i];
				sorted[i] = sorted[j];
				sorted[j] = tmp;
			}

	/* Heuristic: select candidates that are closer to query than to
	 * any already-selected neighbor (promotes diversity) */
	for (i = 0; i < n_cand && n_selected < M; i++)
	{
		float	cand_dist = sorted[i].dist;
		int32	cand_nid = sorted[i].nid;
		bool	good = true;

		for (j = 0; j < n_selected; j++)
		{
			float inter_dist = cosine_distance_f32(
				state->vectors + (Size)cand_nid * dim,
				state->vectors + (Size)out_nids[j] * dim,
				dim);

			if (inter_dist < cand_dist)
			{
				good = false;
				break;
			}
		}

		if (good)
			out_nids[n_selected++] = cand_nid;
	}

	pfree(sorted);
	return n_selected;
}

/*
 * Add a bidirectional edge between two nodes at a given level.
 */
static void
add_connection(HnswBuildState *state, int32 from_nid, int32 to_nid,
			   int level, int max_nbrs)
{
	HnswNode   *from = &state->nodes[from_nid];

	if (from->n_neighbors[level] < max_nbrs)
	{
		from->neighbors[level][from->n_neighbors[level]] = to_nid;
		from->n_neighbors[level]++;
	}
}

/*
 * Shrink a node's neighbor list at a given level to max_nbrs using
 * the heuristic pruning. Called when a neighbor list overflows.
 */
static void
shrink_connections(HnswBuildState *state, int32 nid, int level, int max_nbrs)
{
	HnswNode   *node = &state->nodes[nid];
	int			dim = state->dim;
	int			n = node->n_neighbors[level];
	PQueue	   *candidates;
	int32	   *new_nbrs;
	int			new_count;
	int			i;

	if (n <= max_nbrs)
		return;

	/* Build candidate set with distances to the node */
	candidates = pq_create(n, true);
	for (i = 0; i < n; i++)
	{
		int32	nbr = node->neighbors[level][i];
		float	d = cosine_distance_f32(
			state->vectors + (Size)nid * dim,
			state->vectors + (Size)nbr * dim,
			dim);
		pq_push(candidates, d, nbr);
	}

	new_nbrs = palloc(sizeof(int32) * max_nbrs);
	new_count = select_neighbors_heuristic(state, candidates, max_nbrs,
										   new_nbrs);

	/* Replace neighbor list */
	memcpy(node->neighbors[level], new_nbrs, sizeof(int32) * new_count);
	node->n_neighbors[level] = new_count;

	pfree(new_nbrs);
	pfree(candidates->entries);
	pfree(candidates);
}

/*
 * Insert one node into the HNSW graph.
 */
static void
hnsw_insert_node(HnswBuildState *state, int32 nid)
{
	int			node_level;
	int			level;
	int			ep_nid;
	int			max_level;
	int			dim = state->dim;
	const float *query = state->vectors + (Size)nid * dim;
	HnswNode   *node = &state->nodes[nid];
	int32	   *selected;

	node_level = select_level(state->ml);
	if (node_level > SHNSW_MAX_LEVELS - 1)
		node_level = SHNSW_MAX_LEVELS - 1;

	node->level = node_level;

	/* Allocate per-level neighbor arrays */
	node->neighbors = MemoryContextAllocZero(state->build_ctx,
											 sizeof(int32 *) * (node_level + 1));
	node->n_neighbors = MemoryContextAllocZero(state->build_ctx,
											   sizeof(int16) * (node_level + 1));
	for (level = 0; level <= node_level; level++)
	{
		int max_nbrs = (level == 0) ? state->M_max0 : state->M;
		node->neighbors[level] = MemoryContextAllocZero(
			state->build_ctx, sizeof(int32) * max_nbrs);
	}

	/* First node: just set as entry point */
	if (state->entry_nid < 0)
	{
		state->entry_nid = nid;
		state->max_level = node_level;
		return;
	}

	ep_nid = state->entry_nid;
	max_level = state->max_level;

	/* Phase 1: Greedy search from top to node_level+1 (single nearest) */
	for (level = max_level; level > node_level; level--)
	{
		PQueue *result = search_layer(state, query, ep_nid, 1, level);
		if (result->size > 0)
			ep_nid = result->entries[0].nid;
		pfree(result->entries);
		pfree(result);
	}

	/* Phase 2: Insert at each level from min(node_level, max_level) down to 0 */
	selected = palloc(sizeof(int32) * state->M_max0);

	for (level = Min(node_level, max_level); level >= 0; level--)
	{
		int		ef = state->ef_construction;
		int		max_nbrs = (level == 0) ? state->M_max0 : state->M;
		int		n_selected;
		int		i;

		PQueue *candidates = search_layer(state, query, ep_nid, ef, level);

		/* Select neighbors using heuristic */
		n_selected = select_neighbors_heuristic(state, candidates, max_nbrs,
												selected);

		/* Set neighbors for new node */
		memcpy(node->neighbors[level], selected, sizeof(int32) * n_selected);
		node->n_neighbors[level] = n_selected;

		/* Add reverse connections */
		for (i = 0; i < n_selected; i++)
		{
			add_connection(state, selected[i], nid, level, max_nbrs + 1);
			/* Shrink if overflow */
			if (state->nodes[selected[i]].n_neighbors[level] > max_nbrs)
				shrink_connections(state, selected[i], level, max_nbrs);
		}

		/* Use closest found as entry point for next level */
		if (candidates->size > 0)
		{
			/* Find min-dist in result (it's a max-heap) */
			float min_d = FLT_MAX;
			for (i = 0; i < candidates->size; i++)
			{
				if (candidates->entries[i].dist < min_d)
				{
					min_d = candidates->entries[i].dist;
					ep_nid = candidates->entries[i].nid;
				}
			}
		}

		pfree(candidates->entries);
		pfree(candidates);
	}

	pfree(selected);

	/* Update entry point if new node has higher level */
	if (node_level > state->max_level)
	{
		state->entry_nid = nid;
		state->max_level = node_level;
	}
}

/* ================================================================
 * Public interface: build the HNSW graph from vectors
 * ================================================================ */

/*
 * shnsw_build_graph: construct HNSW graph in memory.
 *
 * Input:
 *   vectors: float32 array, n_nodes * dim, row-major
 *   tids: heap ItemPointers for each node
 *   n_nodes, dim: counts
 *   M, ef_construction: HNSW parameters
 *
 * Output:
 *   Fills the HnswBuildState with graph topology.
 *   Caller uses this to write index pages.
 */
HnswBuildState *
shnsw_build_graph(float *vectors, ItemPointer tids,
				  int n_nodes, int dim,
				  int M, int ef_construction,
				  MemoryContext build_ctx)
{
	HnswBuildState *state;
	MemoryContext old_ctx;
	int			i;

	old_ctx = MemoryContextSwitchTo(build_ctx);

	state = palloc0(sizeof(HnswBuildState));
	state->M = M;
	state->M_max0 = 2 * M;
	state->ef_construction = ef_construction;
	state->ml = 1.0 / log((double)M);
	state->dim = dim;
	state->n_nodes = n_nodes;
	state->max_level = -1;
	state->entry_nid = -1;
	state->vectors = vectors;
	state->visit_marks = palloc0(sizeof(uint32) * (Size) n_nodes);
	state->visit_token = 0;
	state->build_ctx = build_ctx;

	/* Allocate node array */
	state->nodes = palloc0(sizeof(HnswNode) * n_nodes);
	for (i = 0; i < n_nodes; i++)
	{
		state->nodes[i].nid = i;
		ItemPointerCopy(&tids[i], &state->nodes[i].heap_tid);
	}

	MemoryContextSwitchTo(old_ctx);

	/* Insert nodes one by one */
	elog(NOTICE, "sorted_hnsw: building HNSW graph (%d nodes, dim=%d, M=%d, ef=%d)",
		 n_nodes, dim, M, ef_construction);

	for (i = 0; i < n_nodes; i++)
	{
		CHECK_FOR_INTERRUPTS();

		hnsw_insert_node(state, i);

		if ((i + 1) % 10000 == 0)
			elog(NOTICE, "sorted_hnsw: %d / %d nodes inserted (max_level=%d)",
				 i + 1, n_nodes, state->max_level);
	}

	elog(NOTICE, "sorted_hnsw: graph complete. entry=%d max_level=%d",
		 state->entry_nid, state->max_level);

	return state;
}

/* ---- Accessors for sorted_hnsw.c ---- */

int
shnsw_build_max_level(HnswBuildState *state)
{
	return state->max_level;
}

int
shnsw_build_entry_nid(HnswBuildState *state)
{
	return state->entry_nid;
}

HnswBuiltNode *
shnsw_build_get_node(HnswBuildState *state, int nid)
{
	/* Return pointer into the node array (caller must not free) */
	return (HnswBuiltNode *) &state->nodes[nid];
}
