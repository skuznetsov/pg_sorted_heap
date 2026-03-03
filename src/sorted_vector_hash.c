/*
 * sorted_vector_hash.c
 *
 * Locality-preserving hash functions for vector columns:
 *
 * 1. SimHash (sorted_vector_hash): 12-bit LSH for cosine similarity.
 *    Best for tight clusters (cosine sim > 0.9). O(12 × d) per hash.
 *
 * 2. Random Codebook VQ (sorted_vector_vq): assigns vector to nearest
 *    random centroid. Best for loose clusters. O(C × d) per hash.
 *    Codebook is cached per-backend for performance.
 *
 * 3. Residual VQ (sorted_vector_rvq): two-level VQ that splits large
 *    buckets uniformly. Coarse VQ → residual → normalize → fine VQ.
 *    hash = coarse_idx × n_fine + fine_idx. O((C1+C2) × d) per hash.
 *
 * 4. Random Projection VQ (sorted_vector_rpvq): project to low-D space
 *    then VQ. Solves the high-D degeneration where random centroids
 *    are orthogonal to the data cone. O(n_proj × d + C × n_proj).
 *
 * 5. Centered VQ (sorted_vector_cvq): subtract a reference vector
 *    (typically the dataset mean) before VQ. The reference removes the
 *    common component, revealing the spread. Requires a training step
 *    to compute the mean, but the hash function itself is IMMUTABLE.
 *
 * All are IMMUTABLE STRICT — safe for GENERATED ALWAYS AS columns.
 * Designed for use with pg_sorted_heap: store as generated column,
 * include in composite PK, compact → zone map on hash → ANN pruning.
 *
 * Supports the svec type natively (no pgvector dependency).
 */
#include "postgres.h"

#include "fmgr.h"
#include "catalog/pg_type_d.h"
#include "utils/array.h"
#include "utils/builtins.h"

#include <math.h>

#include "utils/memutils.h"

#include "svec.h"

/* Number of SimHash bits — yields 4096 buckets */
#define SIMHASH_N_BITS 12

/* ----------------------------------------------------------------
 *  Deterministic PRNG: splitmix64 → xoshiro256**
 * ---------------------------------------------------------------- */
static uint64
splitmix64(uint64 *state)
{
	uint64 z = (*state += 0x9e3779b97f4a7c15ULL);
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
	z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
	return z ^ (z >> 31);
}

typedef struct
{
	uint64 s[4];
} XoshiroState;

static inline uint64
rotl64(uint64 x, int k)
{
	return (x << k) | (x >> (64 - k));
}

static uint64
xoshiro_next(XoshiroState *state)
{
	uint64 *s = state->s;
	uint64 result = rotl64(s[1] * 5, 7) * 9;
	uint64 t = s[1] << 17;

	s[2] ^= s[0];
	s[3] ^= s[1];
	s[1] ^= s[2];
	s[0] ^= s[3];
	s[2] ^= t;
	s[3] = rotl64(s[3], 45);

	return result;
}

static void
xoshiro_seed(XoshiroState *state, int64 seed)
{
	uint64 s = (uint64) seed;

	state->s[0] = splitmix64(&s);
	state->s[1] = splitmix64(&s);
	state->s[2] = splitmix64(&s);
	state->s[3] = splitmix64(&s);
}

/*
 * Uniform double in [0, 1) from 53-bit mantissa.
 */
static double
xoshiro_uniform(XoshiroState *state)
{
	return (double)(xoshiro_next(state) >> 11) * 0x1.0p-53;
}

/*
 * Standard normal via Box-Muller.
 * Consumes 2 uniform values, produces 1 normal.
 * (We discard the second normal for simplicity; determinism preserved.)
 */
static double
xoshiro_normal(XoshiroState *state)
{
	double u1, u2;

	do {
		u1 = xoshiro_uniform(state);
	} while (u1 <= 1e-300);
	u2 = xoshiro_uniform(state);

	return sqrt(-2.0 * log(u1)) * cos(2.0 * M_PI * u2);
}

/* ----------------------------------------------------------------
 *  SimHash core: compute N_BITS sign bits from dot products
 *  with random Gaussian hyperplanes.
 * ---------------------------------------------------------------- */
static int16
compute_simhash(float *data, int ndim, int32 seed)
{
	XoshiroState rng;
	int16		hash = 0;
	int			bit;
	int			i;

	xoshiro_seed(&rng, (int64) seed);

	for (bit = 0; bit < SIMHASH_N_BITS; bit++)
	{
		double		dot = 0.0;

		for (i = 0; i < ndim; i++)
		{
			double r = xoshiro_normal(&rng);
			dot += (double) data[i] * r;
		}

		if (dot > 0.0)
			hash |= (1 << bit);
	}

	return hash;
}

/* ----------------------------------------------------------------
 *  SQL function: sorted_vector_hash(vector, int4) → int2
 *
 *  Accepts svec type.
 *  IMMUTABLE STRICT — safe for generated columns.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(sorted_vector_hash);

Datum
sorted_vector_hash(PG_FUNCTION_ARGS)
{
	Svec	   *vec = PG_GETARG_SVEC_P(0);
	int32		seed = PG_GETARG_INT32(1);

	PG_RETURN_INT16(compute_simhash(vec->x, vec->dim, seed));
}

/* ----------------------------------------------------------------
 *  SQL function: sorted_vector_hash_arr(float4[], int4) → int2
 *
 *  Accepts plain float4[] for environments without pgvector.
 *  IMMUTABLE STRICT — safe for generated columns.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(sorted_vector_hash_arr);

Datum
sorted_vector_hash_arr(PG_FUNCTION_ARGS)
{
	ArrayType  *arr = PG_GETARG_ARRAYTYPE_P(0);
	int32		seed = PG_GETARG_INT32(1);
	int			ndim;
	float4	   *data;

	if (ARR_NDIM(arr) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("sorted_vector_hash_arr: expected 1-dimensional array")));
	if (ARR_ELEMTYPE(arr) != FLOAT4OID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("sorted_vector_hash_arr: expected float4 array")));
	if (ARR_HASNULL(arr))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("sorted_vector_hash_arr: array must not contain NULLs")));

	ndim = ARR_DIMS(arr)[0];
	data = (float4 *) ARR_DATA_PTR(arr);

	PG_RETURN_INT16(compute_simhash(data, ndim, seed));
}

/* ================================================================
 *  Random Codebook VQ hash
 *
 *  Generates C random unit vectors (codebook) from seed,
 *  returns argmax(codebook · input_vector) as int2.
 *  Codebook is cached per-backend to avoid regeneration.
 * ================================================================ */

/* Backend-local codebook cache */
typedef struct
{
	int32		seed;
	int			n_centroids;
	int			dims;
	float	   *centroids;		/* C × D row-major, unit-normalized */
} CachedCodebook;

/* Separate caches: plain VQ, RVQ coarse level, RVQ fine level */
static CachedCodebook cached_cb = {0, 0, 0, NULL};
static CachedCodebook cached_rvq_coarse = {0, 0, 0, NULL};
static CachedCodebook cached_rvq_fine = {0, 0, 0, NULL};

/*
 * Ensure the given codebook cache matches the requested parameters.
 * If not, regenerate and cache in TopMemoryContext.
 */
static void
ensure_codebook(CachedCodebook *cb, int n_centroids, int dims, int32 seed)
{
	XoshiroState rng;
	int			c, d;
	MemoryContext oldctx;

	/* Cache hit? */
	if (cb->centroids != NULL &&
		cb->seed == seed &&
		cb->n_centroids == n_centroids &&
		cb->dims == dims)
		return;

	/* Free old codebook */
	if (cb->centroids != NULL)
	{
		pfree(cb->centroids);
		cb->centroids = NULL;
	}

	/* Allocate in TopMemoryContext so it survives across queries */
	oldctx = MemoryContextSwitchTo(TopMemoryContext);
	cb->centroids = palloc(sizeof(float) * n_centroids * dims);
	MemoryContextSwitchTo(oldctx);

	/* Generate random unit vectors */
	xoshiro_seed(&rng, (int64) seed);

	for (c = 0; c < n_centroids; c++)
	{
		float  *row = cb->centroids + c * dims;
		double	norm_sq = 0.0;
		double	inv_norm;

		for (d = 0; d < dims; d++)
		{
			row[d] = (float) xoshiro_normal(&rng);
			norm_sq += (double) row[d] * (double) row[d];
		}

		/* Normalize to unit length */
		inv_norm = 1.0 / sqrt(norm_sq);
		for (d = 0; d < dims; d++)
			row[d] = (float) ((double) row[d] * inv_norm);
	}

	cb->seed = seed;
	cb->n_centroids = n_centroids;
	cb->dims = dims;
}

/*
 * Compute VQ hash: argmax of dot products with cached codebook.
 */
/*
 * Find nearest centroid in given codebook, return index and optionally the
 * pointer to the winning centroid row.
 */
static int
find_nearest(CachedCodebook *cb, float *data, int ndim, float **winner_out)
{
	int		best_idx = 0;
	float	best_sim = -1e30f;
	int		c, d;

	for (c = 0; c < cb->n_centroids; c++)
	{
		float  *centroid = cb->centroids + c * ndim;
		float	sim = 0.0f;

		for (d = 0; d < ndim; d++)
			sim += data[d] * centroid[d];

		if (sim > best_sim)
		{
			best_sim = sim;
			best_idx = c;
		}
	}

	if (winner_out)
		*winner_out = cb->centroids + best_idx * ndim;

	return best_idx;
}

/*
 * Find top-K nearest centroids. Returns indices in out_indices[] sorted
 * by descending similarity. Caller must allocate out_indices[k].
 * Returns actual number of results (min(k, n_centroids)).
 */
static int
find_nearest_k(CachedCodebook *cb, float *data, int ndim, int k, int *out_indices)
{
	int		n = cb->n_centroids;
	int		actual_k = (k < n) ? k : n;
	int		c, d, i, j;

	/* Compute all similarities */
	float  *sims = palloc(sizeof(float) * n);
	int	   *idx = palloc(sizeof(int) * n);

	for (c = 0; c < n; c++)
	{
		float  *centroid = cb->centroids + c * ndim;
		float	sim = 0.0f;

		for (d = 0; d < ndim; d++)
			sim += data[d] * centroid[d];
		sims[c] = sim;
		idx[c] = c;
	}

	/* Partial selection sort for top-K (K is small, typically 1-10) */
	for (i = 0; i < actual_k; i++)
	{
		int		best = i;

		for (j = i + 1; j < n; j++)
		{
			if (sims[idx[j]] > sims[idx[best]])
				best = j;
		}
		if (best != i)
		{
			int tmp = idx[i];
			idx[i] = idx[best];
			idx[best] = tmp;
		}
		out_indices[i] = idx[i];
	}

	pfree(sims);
	pfree(idx);
	return actual_k;
}

static int16
compute_vq(float *data, int ndim, int n_centroids, int32 seed)
{
	ensure_codebook(&cached_cb, n_centroids, ndim, seed);
	return (int16) find_nearest(&cached_cb, data, ndim, NULL);
}

/* ----------------------------------------------------------------
 *  SQL function: sorted_vector_vq(vector, int4, int4) → int2
 *
 *  Random Codebook VQ: assign vector to nearest of n_centroids
 *  random unit vectors (deterministic from seed).
 *  IMMUTABLE STRICT PARALLEL SAFE — safe for generated columns.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(sorted_vector_vq);

Datum
sorted_vector_vq(PG_FUNCTION_ARGS)
{
	Svec	   *vec = PG_GETARG_SVEC_P(0);
	int32		n_centroids = PG_GETARG_INT32(1);
	int32		seed = PG_GETARG_INT32(2);

	if (n_centroids < 2 || n_centroids > 32000)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_vector_vq: n_centroids must be between 2 and 32000")));

	PG_RETURN_INT16(compute_vq(vec->x, vec->dim, n_centroids, seed));
}

/* ----------------------------------------------------------------
 *  SQL function: sorted_vector_vq_arr(float4[], int4, int4) → int2
 *
 *  Float4[] overload for environments without pgvector.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(sorted_vector_vq_arr);

Datum
sorted_vector_vq_arr(PG_FUNCTION_ARGS)
{
	ArrayType  *arr = PG_GETARG_ARRAYTYPE_P(0);
	int32		n_centroids = PG_GETARG_INT32(1);
	int32		seed = PG_GETARG_INT32(2);
	int			ndim;
	float4	   *data;

	if (ARR_NDIM(arr) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("sorted_vector_vq_arr: expected 1-dimensional array")));
	if (ARR_ELEMTYPE(arr) != FLOAT4OID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("sorted_vector_vq_arr: expected float4 array")));
	if (ARR_HASNULL(arr))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("sorted_vector_vq_arr: array must not contain NULLs")));

	if (n_centroids < 2 || n_centroids > 32000)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_vector_vq_arr: n_centroids must be between 2 and 32000")));

	ndim = ARR_DIMS(arr)[0];
	data = (float4 *) ARR_DATA_PTR(arr);

	PG_RETURN_INT16(compute_vq(data, ndim, n_centroids, seed));
}

/* ================================================================
 *  Residual VQ (RVQ) hash
 *
 *  Two-level VQ: coarse assignment → residual → fine assignment.
 *  hash = coarse_idx × n_fine + fine_idx
 *
 *  Residuals are more isotropic than raw vectors, so random VQ
 *  splits them much more uniformly — solves the "big bucket" problem
 *  where a single coarse bucket captures 60%+ of data in high-D.
 * ================================================================ */

static int16
compute_rvq(float *data, int ndim, int n_coarse, int n_fine, int32 seed)
{
	int		coarse_idx;
	int		fine_idx;
	float  *coarse_centroid;
	float  *residual;
	double	norm_sq;
	double	inv_norm;
	int		d;

	/* Coarse level: assign to nearest of n_coarse random centroids */
	ensure_codebook(&cached_rvq_coarse, n_coarse, ndim, seed);
	coarse_idx = find_nearest(&cached_rvq_coarse, data, ndim, &coarse_centroid);

	/* Compute residual = data - coarse_centroid, then normalize */
	residual = palloc(sizeof(float) * ndim);
	norm_sq = 0.0;
	for (d = 0; d < ndim; d++)
	{
		residual[d] = data[d] - coarse_centroid[d];
		norm_sq += (double) residual[d] * (double) residual[d];
	}

	/* Normalize residual to unit length (important: random codebook
	 * expects unit-ish vectors for meaningful dot-product comparison) */
	if (norm_sq > 1e-30)
	{
		inv_norm = 1.0 / sqrt(norm_sq);
		for (d = 0; d < ndim; d++)
			residual[d] = (float) ((double) residual[d] * inv_norm);
	}

	/* Fine level: assign residual to nearest of n_fine centroids
	 * using a different seed (seed + 1) for independent randomness */
	ensure_codebook(&cached_rvq_fine, n_fine, ndim, seed + 1);
	fine_idx = find_nearest(&cached_rvq_fine, residual, ndim, NULL);

	pfree(residual);

	return (int16) (coarse_idx * n_fine + fine_idx);
}

/* ----------------------------------------------------------------
 *  SQL function: sorted_vector_rvq(vector, int4, int4, int4) → int2
 *
 *  Residual VQ: two-level hash for uniform bucket distribution.
 *  hash = coarse_idx × n_fine + fine_idx
 *  IMMUTABLE STRICT PARALLEL SAFE — safe for generated columns.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(sorted_vector_rvq);

Datum
sorted_vector_rvq(PG_FUNCTION_ARGS)
{
	Svec	   *vec = PG_GETARG_SVEC_P(0);
	int32		n_coarse = PG_GETARG_INT32(1);
	int32		n_fine = PG_GETARG_INT32(2);
	int32		seed = PG_GETARG_INT32(3);

	if (n_coarse < 2 || n_coarse > 32000)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_vector_rvq: n_coarse must be between 2 and 32000")));
	if (n_fine < 2 || n_fine > 32000)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_vector_rvq: n_fine must be between 2 and 32000")));
	if ((int64) n_coarse * n_fine > 32767)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_vector_rvq: n_coarse * n_fine must fit in int2 (max 32767)")));

	PG_RETURN_INT16(compute_rvq(vec->x, vec->dim, n_coarse, n_fine, seed));
}

/* ----------------------------------------------------------------
 *  SQL function: sorted_vector_rvq_arr(float4[], int4, int4, int4) → int2
 *
 *  Float4[] overload for environments without pgvector.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(sorted_vector_rvq_arr);

Datum
sorted_vector_rvq_arr(PG_FUNCTION_ARGS)
{
	ArrayType  *arr = PG_GETARG_ARRAYTYPE_P(0);
	int32		n_coarse = PG_GETARG_INT32(1);
	int32		n_fine = PG_GETARG_INT32(2);
	int32		seed = PG_GETARG_INT32(3);
	int			ndim;
	float4	   *data;

	if (ARR_NDIM(arr) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("sorted_vector_rvq_arr: expected 1-dimensional array")));
	if (ARR_ELEMTYPE(arr) != FLOAT4OID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("sorted_vector_rvq_arr: expected float4 array")));
	if (ARR_HASNULL(arr))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("sorted_vector_rvq_arr: array must not contain NULLs")));

	if (n_coarse < 2 || n_coarse > 32000)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_vector_rvq_arr: n_coarse must be between 2 and 32000")));
	if (n_fine < 2 || n_fine > 32000)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_vector_rvq_arr: n_fine must be between 2 and 32000")));
	if ((int64) n_coarse * n_fine > 32767)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_vector_rvq_arr: n_coarse * n_fine must fit in int2 (max 32767)")));

	ndim = ARR_DIMS(arr)[0];
	data = (float4 *) ARR_DATA_PTR(arr);

	PG_RETURN_INT16(compute_rvq(data, ndim, n_coarse, n_fine, seed));
}

/* ================================================================
 *  Random Projection VQ (RPVQ)
 *
 *  Project high-D vector to n_proj dimensions using random Gaussian
 *  projections, then apply VQ in the low-D space. In high-D (e.g.
 *  2880), random centroids are nearly orthogonal to the data cone,
 *  making VQ useless. In low-D (e.g. 32), random centroids have
 *  ~10x more signal, enabling meaningful bucket separation.
 *
 *  Cost: O(n_proj × D + n_centroids × n_proj) per hash.
 *  For n_proj=32, D=2880, C=256: ~100K flops — 7x faster than
 *  plain VQ (740K flops).
 * ================================================================ */

static CachedCodebook cached_rpvq_proj = {0, 0, 0, NULL};
static CachedCodebook cached_rpvq_cb = {0, 0, 0, NULL};

static int16
compute_rpvq(float *data, int ndim, int n_proj, int n_centroids, int32 seed)
{
	float  *projected;
	int		i, d;
	int16	result;

	/* Projection matrix: n_proj random unit vectors in ndim dimensions */
	ensure_codebook(&cached_rpvq_proj, n_proj, ndim, seed);

	/* Project: y[i] = dot(data, proj_row[i]) */
	projected = palloc(sizeof(float) * n_proj);
	for (i = 0; i < n_proj; i++)
	{
		float  *proj_row = cached_rpvq_proj.centroids + i * ndim;
		float	dot = 0.0f;

		for (d = 0; d < ndim; d++)
			dot += data[d] * proj_row[d];
		projected[i] = dot;
	}

	/* Centroids in projected space: n_centroids unit vectors in n_proj dims */
	ensure_codebook(&cached_rpvq_cb, n_centroids, n_proj, seed + 1);

	/* Find nearest centroid in projected space */
	result = (int16) find_nearest(&cached_rpvq_cb, projected, n_proj, NULL);

	pfree(projected);
	return result;
}

/* ----------------------------------------------------------------
 *  SQL: sorted_vector_rpvq(vector, n_proj, n_centroids, seed) → int2
 *
 *  Random Projection VQ: project to n_proj dimensions, then VQ.
 *  Best for high-D vectors (>256D) where plain VQ degenerates.
 *  IMMUTABLE STRICT PARALLEL SAFE — safe for generated columns.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(sorted_vector_rpvq);

Datum
sorted_vector_rpvq(PG_FUNCTION_ARGS)
{
	Svec	   *vec = PG_GETARG_SVEC_P(0);
	int32		n_proj = PG_GETARG_INT32(1);
	int32		n_centroids = PG_GETARG_INT32(2);
	int32		seed = PG_GETARG_INT32(3);

	if (n_proj < 2 || n_proj > 4096)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_vector_rpvq: n_proj must be between 2 and 4096")));
	if (n_centroids < 2 || n_centroids > 32000)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_vector_rpvq: n_centroids must be between 2 and 32000")));

	PG_RETURN_INT16(compute_rpvq(vec->x, vec->dim, n_proj, n_centroids, seed));
}

/* ----------------------------------------------------------------
 *  SQL: sorted_vector_rpvq(float4[], n_proj, n_centroids, seed) → int2
 *
 *  Float4[] overload for environments without pgvector.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(sorted_vector_rpvq_arr);

Datum
sorted_vector_rpvq_arr(PG_FUNCTION_ARGS)
{
	ArrayType  *arr = PG_GETARG_ARRAYTYPE_P(0);
	int32		n_proj = PG_GETARG_INT32(1);
	int32		n_centroids = PG_GETARG_INT32(2);
	int32		seed = PG_GETARG_INT32(3);
	int			ndim;
	float4	   *data;

	if (ARR_NDIM(arr) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("sorted_vector_rpvq_arr: expected 1-dimensional array")));
	if (ARR_ELEMTYPE(arr) != FLOAT4OID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("sorted_vector_rpvq_arr: expected float4 array")));
	if (ARR_HASNULL(arr))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("sorted_vector_rpvq_arr: array must not contain NULLs")));

	if (n_proj < 2 || n_proj > 4096)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_vector_rpvq_arr: n_proj must be between 2 and 4096")));
	if (n_centroids < 2 || n_centroids > 32000)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_vector_rpvq_arr: n_centroids must be between 2 and 32000")));

	ndim = ARR_DIMS(arr)[0];
	data = (float4 *) ARR_DATA_PTR(arr);

	PG_RETURN_INT16(compute_rpvq(data, ndim, n_proj, n_centroids, seed));
}

/* ================================================================
 *  Centered VQ (CVQ)
 *
 *  Subtract a reference vector (typically the dataset mean) from the
 *  input before applying VQ. This removes the common component that
 *  dominates in high-D tightly-clustered data, revealing the spread.
 *
 *  Usage: compute avg(embedding) once after initial load, embed it as
 *  a literal in the GENERATED ALWAYS AS expression.
 *
 *  Cost: O(D + C × D) per hash — same as plain VQ plus the subtraction.
 * ================================================================ */

static CachedCodebook cached_cvq_cb = {0, 0, 0, NULL};

static int16
compute_cvq(float *data, float *ref, int ndim, int n_centroids, int32 seed)
{
	float  *centered;
	int		d;
	int16	result;

	centered = palloc(sizeof(float) * ndim);
	for (d = 0; d < ndim; d++)
		centered[d] = data[d] - ref[d];

	ensure_codebook(&cached_cvq_cb, n_centroids, ndim, seed);
	result = (int16) find_nearest(&cached_cvq_cb, centered, ndim, NULL);

	pfree(centered);
	return result;
}

/* ----------------------------------------------------------------
 *  SQL: sorted_vector_cvq(vector, vector, n_centroids, seed) → int2
 *
 *  Centered VQ: subtract reference vector, then VQ.
 *  Reference is typically avg(embedding) computed after initial load.
 *  IMMUTABLE STRICT PARALLEL SAFE — safe for generated columns.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(sorted_vector_cvq);

Datum
sorted_vector_cvq(PG_FUNCTION_ARGS)
{
	Svec	   *vec = PG_GETARG_SVEC_P(0);
	Svec	   *ref = PG_GETARG_SVEC_P(1);
	int32		n_centroids = PG_GETARG_INT32(2);
	int32		seed = PG_GETARG_INT32(3);

	if (vec->dim != ref->dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("sorted_vector_cvq: input and reference must have same dimensions")));
	if (n_centroids < 2 || n_centroids > 32000)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_vector_cvq: n_centroids must be between 2 and 32000")));

	PG_RETURN_INT16(compute_cvq(vec->x, ref->x, vec->dim, n_centroids, seed));
}

/* ----------------------------------------------------------------
 *  SQL: sorted_vector_cvq(float4[], float4[], n_centroids, seed) → int2
 *
 *  Float4[] overload for environments without pgvector.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(sorted_vector_cvq_arr);

Datum
sorted_vector_cvq_arr(PG_FUNCTION_ARGS)
{
	ArrayType  *arr = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *ref_arr = PG_GETARG_ARRAYTYPE_P(1);
	int32		n_centroids = PG_GETARG_INT32(2);
	int32		seed = PG_GETARG_INT32(3);
	int			ndim, ref_ndim;
	float4	   *data;
	float4	   *ref;

	if (ARR_NDIM(arr) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("sorted_vector_cvq_arr: expected 1-dimensional array")));
	if (ARR_ELEMTYPE(arr) != FLOAT4OID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("sorted_vector_cvq_arr: expected float4 array")));
	if (ARR_HASNULL(arr))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("sorted_vector_cvq_arr: array must not contain NULLs")));
	if (ARR_NDIM(ref_arr) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("sorted_vector_cvq_arr: expected 1-dimensional reference array")));
	if (ARR_ELEMTYPE(ref_arr) != FLOAT4OID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("sorted_vector_cvq_arr: expected float4 reference array")));

	ndim = ARR_DIMS(arr)[0];
	ref_ndim = ARR_DIMS(ref_arr)[0];
	if (ndim != ref_ndim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("sorted_vector_cvq_arr: input and reference must have same dimensions")));

	if (n_centroids < 2 || n_centroids > 32000)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_vector_cvq_arr: n_centroids must be between 2 and 32000")));

	data = (float4 *) ARR_DATA_PTR(arr);
	ref = (float4 *) ARR_DATA_PTR(ref_arr);

	PG_RETURN_INT16(compute_cvq(data, ref, ndim, n_centroids, seed));
}

/* ================================================================
 *  Multi-probe CVQ: return top-K nearest CVQ bucket IDs as int2[]
 *
 *  Usage: WHERE cvqhash = ANY(sorted_vector_cvq_probe(q, ref, 256, 3, 42))
 *  This scans the K nearest buckets, improving recall at the cost of
 *  scanning K× more data.
 * ================================================================ */
PG_FUNCTION_INFO_V1(sorted_vector_cvq_probe);

Datum
sorted_vector_cvq_probe(PG_FUNCTION_ARGS)
{
	Svec	   *vec = PG_GETARG_SVEC_P(0);
	Svec	   *ref = PG_GETARG_SVEC_P(1);
	int32		n_centroids = PG_GETARG_INT32(2);
	int32		n_probes = PG_GETARG_INT32(3);
	int32		seed = PG_GETARG_INT32(4);
	float	   *centered;
	int		   *indices;
	int			actual_k;
	int			d, i;
	Datum	   *elems;
	ArrayType  *result;

	if (vec->dim != ref->dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("sorted_vector_cvq_probe: input and reference must have same dimensions")));
	if (n_centroids < 2 || n_centroids > 32000)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_vector_cvq_probe: n_centroids must be between 2 and 32000")));
	if (n_probes < 1 || n_probes > n_centroids)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("sorted_vector_cvq_probe: n_probes must be between 1 and n_centroids")));

	/* Center the input */
	centered = palloc(sizeof(float) * vec->dim);
	for (d = 0; d < vec->dim; d++)
		centered[d] = vec->x[d] - ref->x[d];

	/* Ensure codebook and find top-K */
	ensure_codebook(&cached_cvq_cb, n_centroids, vec->dim, seed);
	indices = palloc(sizeof(int) * n_probes);
	actual_k = find_nearest_k(&cached_cvq_cb, centered, vec->dim, n_probes, indices);

	pfree(centered);

	/* Build int2[] result */
	elems = palloc(sizeof(Datum) * actual_k);
	for (i = 0; i < actual_k; i++)
		elems[i] = Int16GetDatum((int16) indices[i]);

	pfree(indices);

	result = construct_array(elems, actual_k, INT2OID, sizeof(int16), true, TYPALIGN_SHORT);
	pfree(elems);

	PG_RETURN_ARRAYTYPE_P(result);
}
