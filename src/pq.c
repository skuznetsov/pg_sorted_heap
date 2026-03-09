/*
 * pq.c
 *
 * Product Quantization for pg_sorted_heap / svec.
 *
 * Three SQL-callable functions:
 *
 * 1. svec_pq_train(source_query text, M int, n_iter int) → int
 *    Reads svec vectors from a SQL query, trains M sub-codebooks via k-means
 *    (Ksub=256 centroids each), stores in _pq_codebooks table. Returns
 *    number of centroids stored (M × 256).
 *
 * 2. svec_pq_encode(vec svec, cb_id int) → bytea
 *    Encodes a vector into M-byte PQ code using the trained codebook.
 *
 * 3. svec_pq_distance(query svec, code bytea, cb_id int) → float8
 *    Asymmetric Distance Computation: precomputes query-to-centroid L2
 *    distances for all M×256 entries, then sums distances using PQ codes.
 *    Returns estimated squared L2 distance.
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/stratnum.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/pg_am.h"
#include "commands/defrem.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "funcapi.h"
#include "catalog/pg_type_d.h"
#include "catalog/pg_namespace.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "storage/bufmgr.h"
#include "storage/itemptr.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/float.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include <float.h>
#include <math.h>
#include <string.h>

#include "svec.h"
#include "hsvec.h"
#include "pq.h"
#include "sorted_heap.h"

/* GUC: log svec_ann_scan phase timing via DEBUG1 */
bool sorted_heap_ann_timing = false;

/* ----------------------------------------------------------------
 *  Return the quoted schema name of the pg_sorted_heap extension.
 *  Used to schema-qualify internal metadata tables (_pq_*, _ivf_*).
 * ---------------------------------------------------------------- */
static const char *
get_ext_schema(void)
{
	Oid		ext_oid = get_extension_oid("pg_sorted_heap", false);

	return quote_identifier(get_namespace_name(get_extension_schema(ext_oid)));
}

/* ----------------------------------------------------------------
 *  Check that the current user has CREATE privilege on the extension
 *  schema.  Training functions create metadata tables (_pq_*, _ivf_*)
 *  lazily, so they need CREATE on the schema.
 * ---------------------------------------------------------------- */
static void
check_training_privileges(void)
{
	Oid		ext_oid = get_extension_oid("pg_sorted_heap", false);
	Oid		schema_oid = get_extension_schema(ext_oid);

	if (object_aclcheck(NamespaceRelationId, schema_oid,
						GetUserId(), ACL_CREATE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied: training requires CREATE privilege "
						"on schema \"%s\"",
						get_namespace_name(schema_oid)),
				 errhint("GRANT CREATE ON SCHEMA \"%s\" TO current_user; "
						 "or run training as the extension owner.",
						 get_namespace_name(schema_oid))));
}

/* ----------------------------------------------------------------
 *  L2-normalize a float vector in-place (for cosine ↔ L2 equivalence)
 * ---------------------------------------------------------------- */
static void
normalize_vector(float *v, int dim)
{
	double	norm = 0.0;
	float	inv;
	int		i;

	for (i = 0; i < dim; i++)
		norm += (double) v[i] * v[i];

	norm = sqrt(norm);
	if (!isfinite(norm))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("vector contains NaN or Inf values")));
	if (norm < 1e-30)
		return;					/* zero vector, leave as-is */

	inv = (float) (1.0 / norm);
	for (i = 0; i < dim; i++)
		v[i] *= inv;
}

/* ----------------------------------------------------------------
 *  Subquantizer reordering helpers (used by codebook cache + ADC scan)
 *
 *  qsort comparator: sorts sub-quantizer indices by descending
 *  spread so high-spread sub-quantizers are evaluated first,
 *  enabling earlier ADC termination.
 * ---------------------------------------------------------------- */
static float *pq_perm_spread;

static int
pq_perm_cmp(const void *a, const void *b)
{
	float sa = pq_perm_spread[*(const int *) a];
	float sb = pq_perm_spread[*(const int *) b];

	/* descending order */
	if (sa > sb) return -1;
	if (sa < sb) return 1;
	return 0;
}

/* ----------------------------------------------------------------
 *  Backend-local codebook cache
 * ---------------------------------------------------------------- */
typedef struct PQCodebookCache
{
	int			cb_id;			/* codebook ID from _pq_codebooks */
	int			M;				/* number of subvectors */
	int			dsub;			/* dimensions per subvector */
	int			total_dim;		/* M * dsub = original vector dimension */
	float	   *centroids;		/* M * 256 * dsub floats, row-major */
	int		   *perm;			/* sub-quantizer eval order (descending spread) */
} PQCodebookCache;

static PQCodebookCache cached_pq = {-1, 0, 0, 0, NULL, NULL};

/*
 * Load codebook from _pq_codebooks table into cache.
 */
static void
pq_load_codebook(int cb_id)
{
	int			ret;
	int			nrows;
	int			i;
	int			M = 0, dsub = 0;
	MemoryContext oldctx;
	char		sql[256];

	/* Cache hit? */
	if (cached_pq.centroids != NULL && cached_pq.cb_id == cb_id)
		return;

	/* Free old cache */
	if (cached_pq.centroids != NULL)
	{
		pfree(cached_pq.centroids);
		cached_pq.centroids = NULL;
		if (cached_pq.perm != NULL)
		{
			pfree(cached_pq.perm);
			cached_pq.perm = NULL;
		}
		cached_pq.cb_id = -1;
	}

	/* Read codebook metadata first */
	snprintf(sql, sizeof(sql),
			 "SELECT m, dsub FROM %s._pq_codebook_meta WHERE cb_id = %d",
			 get_ext_schema(), cb_id);

	SPI_connect();
	ret = SPI_execute(sql, true, 1);

	if (ret != SPI_OK_SELECT || SPI_processed != 1)
	{
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("PQ codebook %d not found in _pq_codebook_meta",
						cb_id)));
	}

	M = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
									SPI_tuptable->tupdesc, 1, &(bool){false}));
	dsub = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
									   SPI_tuptable->tupdesc, 2, &(bool){false}));
	SPI_finish();

	if (M < 1 || M > PQ_MAX_M || dsub < 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("invalid PQ codebook: M=%d, dsub=%d", M, dsub)));

	/* Read all centroids */
	snprintf(sql, sizeof(sql),
			 "SELECT sub_id, cent_id, centroid "
			 "FROM %s._pq_codebooks WHERE cb_id = %d "
			 "ORDER BY sub_id, cent_id",
			 get_ext_schema(), cb_id);

	SPI_connect();
	ret = SPI_execute(sql, true, 0);

	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to read PQ codebook %d", cb_id)));
	}

	nrows = (int) SPI_processed;

	if (nrows != M * PQ_KSUB)
	{
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("PQ codebook %d: expected %d rows, got %d",
						cb_id, M * PQ_KSUB, nrows)));
	}

	/* Allocate in TopMemoryContext for cross-query survival */
	oldctx = MemoryContextSwitchTo(TopMemoryContext);
	cached_pq.centroids = palloc(sizeof(float) * M * PQ_KSUB * dsub);
	MemoryContextSwitchTo(oldctx);

	for (i = 0; i < nrows; i++)
	{
		HeapTuple	tup = SPI_tuptable->vals[i];
		bool		isnull;
		int			sub_id, cent_id;
		Svec	   *cent;
		float	   *dest;

		sub_id = DatumGetInt32(SPI_getbinval(tup, SPI_tuptable->tupdesc,
											 1, &isnull));
		cent_id = DatumGetInt32(SPI_getbinval(tup, SPI_tuptable->tupdesc,
											  2, &isnull));
		cent = (Svec *) PG_DETOAST_DATUM(SPI_getbinval(tup,
														SPI_tuptable->tupdesc,
														3, &isnull));

		if (isnull || cent->dim != dsub)
		{
			SPI_finish();
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("PQ codebook %d: bad centroid at sub=%d, cent=%d",
							cb_id, sub_id, cent_id)));
		}

		dest = cached_pq.centroids + (sub_id * PQ_KSUB + cent_id) * dsub;
		memcpy(dest, cent->x, sizeof(float) * dsub);
	}

	SPI_finish();

	cached_pq.cb_id = cb_id;
	cached_pq.M = M;
	cached_pq.dsub = dsub;
	cached_pq.total_dim = M * dsub;

	/*
	 * Compute global sub-quantizer permutation by centroid variance.
	 *
	 * For each sub-quantizer, compute the total variance of its 256
	 * centroids across all dsub dimensions.  High-variance sub-quantizers
	 * have widely spread centroids, so their ADC distance contributions
	 * vary more across codes — evaluating them first makes early
	 * termination kick in sooner.
	 *
	 * Cost: O(M * 256 * dsub) — negligible, computed once at load time.
	 */
	{
		float	sub_spread[PQ_MAX_M];
		int		mi, ci, d;

		for (mi = 0; mi < M; mi++)
		{
			float  *cents = cached_pq.centroids
				+ (size_t) mi * PQ_KSUB * dsub;
			double	total_var = 0.0;

			for (d = 0; d < dsub; d++)
			{
				double	sum = 0.0, sum2 = 0.0;

				for (ci = 0; ci < PQ_KSUB; ci++)
				{
					double	v = (double) cents[ci * dsub + d];

					sum += v;
					sum2 += v * v;
				}
				total_var += sum2 / PQ_KSUB
					- (sum / PQ_KSUB) * (sum / PQ_KSUB);
			}
			sub_spread[mi] = (float) total_var;
		}

		oldctx = MemoryContextSwitchTo(TopMemoryContext);
		cached_pq.perm = palloc(sizeof(int) * M);
		MemoryContextSwitchTo(oldctx);

		for (mi = 0; mi < M; mi++)
			cached_pq.perm[mi] = mi;

		pq_perm_spread = sub_spread;
		qsort(cached_pq.perm, M, sizeof(int), pq_perm_cmp);
	}
}

/* ----------------------------------------------------------------
 *  Backend-local IVF centroid cache
 * ---------------------------------------------------------------- */
typedef struct IVFCentroidCache
{
	int			cb_id;			/* codebook ID from _ivf_meta */
	int			nlist;			/* number of centroids (partitions) */
	int			dim;			/* vector dimensionality */
	float	   *centroids;		/* nlist * dim floats, row-major */
} IVFCentroidCache;

static IVFCentroidCache cached_ivf = {-1, 0, 0, NULL};

/*
 * Load IVF centroids from _ivf_centroids table into cache.
 */
static void
ivf_load_centroids(int cb_id)
{
	int			ret;
	int			nrows;
	int			i;
	int			nlist = 0, dim = 0;
	MemoryContext oldctx;
	char		sql[256];

	/* Cache hit? */
	if (cached_ivf.centroids != NULL && cached_ivf.cb_id == cb_id)
		return;

	/* Free old cache */
	if (cached_ivf.centroids != NULL)
	{
		pfree(cached_ivf.centroids);
		cached_ivf.centroids = NULL;
		cached_ivf.cb_id = -1;
	}

	/* Read meta */
	snprintf(sql, sizeof(sql),
			 "SELECT nlist, dim FROM %s._ivf_meta WHERE cb_id = %d",
			 get_ext_schema(), cb_id);

	SPI_connect();
	ret = SPI_execute(sql, true, 1);

	if (ret != SPI_OK_SELECT || SPI_processed != 1)
	{
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("IVF codebook %d not found in _ivf_meta", cb_id)));
	}

	nlist = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
										SPI_tuptable->tupdesc, 1,
										&(bool){false}));
	dim = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
									  SPI_tuptable->tupdesc, 2,
									  &(bool){false}));
	SPI_finish();

	if (nlist < 1 || nlist > IVF_MAX_NLIST || dim < 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("invalid IVF codebook: nlist=%d, dim=%d",
						nlist, dim)));

	/* Read all centroids */
	snprintf(sql, sizeof(sql),
			 "SELECT centroid_id, centroid "
			 "FROM %s._ivf_centroids WHERE cb_id = %d "
			 "ORDER BY centroid_id",
			 get_ext_schema(), cb_id);

	SPI_connect();
	ret = SPI_execute(sql, true, 0);

	if (ret != SPI_OK_SELECT)
	{
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to read IVF centroids for cb_id %d",
						cb_id)));
	}

	nrows = (int) SPI_processed;

	if (nrows != nlist)
	{
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("IVF codebook %d: expected %d centroids, got %d",
						cb_id, nlist, nrows)));
	}

	/* Allocate in TopMemoryContext for cross-query survival */
	oldctx = MemoryContextSwitchTo(TopMemoryContext);
	cached_ivf.centroids = palloc(sizeof(float) * nlist * dim);
	MemoryContextSwitchTo(oldctx);

	for (i = 0; i < nrows; i++)
	{
		HeapTuple	tup = SPI_tuptable->vals[i];
		bool		isnull;
		int			centroid_id;
		Svec	   *cent;
		float	   *dest;

		centroid_id = DatumGetInt32(SPI_getbinval(tup,
												  SPI_tuptable->tupdesc,
												  1, &isnull));
		cent = (Svec *) PG_DETOAST_DATUM(SPI_getbinval(tup,
														SPI_tuptable->tupdesc,
														2, &isnull));

		if (isnull || cent->dim != dim)
		{
			SPI_finish();
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("IVF codebook %d: bad centroid at id=%d",
							cb_id, centroid_id)));
		}

		dest = cached_ivf.centroids + centroid_id * dim;
		memcpy(dest, cent->x, sizeof(float) * dim);
	}

	SPI_finish();

	cached_ivf.cb_id = cb_id;
	cached_ivf.nlist = nlist;
	cached_ivf.dim = dim;
}

/* ----------------------------------------------------------------
 *  K-means on a set of dsub-dimensional vectors
 * ---------------------------------------------------------------- */
void
kmeans_train(float *data, int npts, int dsub, int k, int n_iter,
			 float *centroids_out)
{
	int		   *assign;
	int		   *counts;
	float	   *sums;
	int			iter, i, j, c;

	assign = palloc(sizeof(int) * npts);
	counts = palloc(sizeof(int) * k);
	sums = palloc0(sizeof(float) * k * dsub);

	/* Initialize centroids: first k points (samples were pre-shuffled) */
	for (c = 0; c < k; c++)
	{
		if (c < npts)
			memcpy(centroids_out + c * dsub, data + c * dsub,
				   sizeof(float) * dsub);
		else
		{
			/* Duplicate first centroid if not enough points */
			memcpy(centroids_out + c * dsub, data, sizeof(float) * dsub);
		}
	}

	for (iter = 0; iter < n_iter; iter++)
	{
		/* Assign each point to nearest centroid (L2) */
		for (i = 0; i < npts; i++)
		{
			float  *pt = data + i * dsub;
			float	best_dist = 1e30f;
			int		best_c = 0;

			for (c = 0; c < k; c++)
			{
				float  *cen = centroids_out + c * dsub;
				float	dist = 0.0f;

				for (j = 0; j < dsub; j++)
				{
					float diff = pt[j] - cen[j];
					dist += diff * diff;
				}

				if (dist < best_dist)
				{
					best_dist = dist;
					best_c = c;
				}
			}
			assign[i] = best_c;
		}

		/* Recompute centroids */
		memset(counts, 0, sizeof(int) * k);
		memset(sums, 0, sizeof(float) * k * dsub);

		for (i = 0; i < npts; i++)
		{
			int		ci = assign[i];
			float  *pt = data + i * dsub;

			counts[ci]++;
			for (j = 0; j < dsub; j++)
				sums[ci * dsub + j] += pt[j];
		}

		for (c = 0; c < k; c++)
		{
			if (counts[c] > 0)
			{
				float inv = 1.0f / counts[c];
				for (j = 0; j < dsub; j++)
					centroids_out[c * dsub + j] = sums[c * dsub + j] * inv;
			}
			/* Empty clusters keep their previous centroid */
		}
	}

	pfree(assign);
	pfree(counts);
	pfree(sums);
}


/* ----------------------------------------------------------------
 *  svec_pq_train: train PQ codebook from data
 *
 *  svec_pq_train(source_query text, M int, n_iter int DEFAULT 10) → int
 *
 *  source_query must return rows with a single svec column.
 *  Trains M sub-codebooks with 256 centroids each via k-means.
 *  Stores results in _pq_codebook_meta and _pq_codebooks tables.
 *  Returns codebook ID.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(svec_pq_train);
Datum
svec_pq_train(PG_FUNCTION_ARGS)
{
	text	   *source_query = PG_GETARG_TEXT_PP(0);
	int			M = PG_GETARG_INT32(1);
	int			n_iter = PG_GETARG_INT32(2);
	int			max_samples = PG_GETARG_INT32(3);

	char	   *query_str;
	StringInfoData wrapped_query;
	int			ret;
	int			n_samples;
	int			dim, dsub;
	float	   *sample_data;	/* n_samples × dim */
	float	   *sub_data;		/* n_samples × dsub for one subvector */
	float	   *centroids;		/* 256 × dsub for one subvector */
	int			cb_id;
	int			m, i, j;
	char		sql[256];
	MemoryContext func_ctx;
	MemoryContext tmp_ctx;
	MemoryContext old_ctx;

	check_training_privileges();

	if (M < 1 || M > PQ_MAX_M)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("M must be between 1 and %d", PQ_MAX_M)));

	if (n_iter < 1 || n_iter > PQ_MAX_ITER)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("n_iter must be between 1 and %d", PQ_MAX_ITER)));

	if (max_samples < 256)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("max_samples must be at least 256")));

	query_str = text_to_cstring(source_query);
	func_ctx = CurrentMemoryContext;

	/*
	 * Wrap the user query with ORDER BY random() LIMIT max_samples.
	 * PostgreSQL uses a bounded heap sort for LIMIT, so only max_samples
	 * rows are held in memory at any time — O(K) instead of O(N).
	 * This eliminates the previous OOM on large tables.
	 */
	initStringInfo(&wrapped_query);
	appendStringInfo(&wrapped_query,
					 "SELECT * FROM (%s) _src ORDER BY random() LIMIT %d",
					 query_str, max_samples);

	/* Execute wrapped query — returns at most max_samples rows */
	SPI_connect();
	ret = SPI_execute(wrapped_query.data, true, 0);
	pfree(wrapped_query.data);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 errmsg("source query returned no rows")));
	}

	n_samples = (int) SPI_processed;

	/* Read first vector to get dimension */
	{
		bool	isnull;
		Datum	d;
		Svec   *first;

		d = SPI_getbinval(SPI_tuptable->vals[0],
						  SPI_tuptable->tupdesc, 1, &isnull);

		if (isnull)
		{
			SPI_finish();
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("NULL vector in training data")));
		}

		first = (Svec *) PG_DETOAST_DATUM(d);
		dim = first->dim;
		if (DatumGetPointer(d) != (Pointer) first)
			pfree(first);
	}

	if (dim % M != 0)
	{
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("vector dimension %d is not divisible by M=%d",
						dim, M)));
	}

	dsub = dim / M;

	ereport(NOTICE,
			(errmsg("PQ training: dim=%d, M=%d, dsub=%d, Ksub=%d, "
					"n_iter=%d, samples=%d",
					dim, M, dsub, PQ_KSUB, n_iter, n_samples)));

	/*
	 * Allocate sample_data in func_ctx, NOT in SPI's procCxt.
	 * SPI_finish() deletes procCxt, so anything allocated there becomes
	 * a dangling pointer.
	 */
	old_ctx = MemoryContextSwitchTo(func_ctx);
	sample_data = palloc(sizeof(float) * n_samples * dim);
	MemoryContextSwitchTo(old_ctx);

	/*
	 * Copy all returned vectors into contiguous float array.
	 * Rows are already randomly sampled by ORDER BY random() LIMIT.
	 */
	for (i = 0; i < n_samples; i++)
	{
		bool	isnull;
		Datum	d;
		Svec   *vec;

		d = SPI_getbinval(SPI_tuptable->vals[i],
						  SPI_tuptable->tupdesc, 1, &isnull);

		if (isnull)
		{
			SPI_finish();
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("NULL vector in training data at row %d", i)));
		}

		vec = (Svec *) PG_DETOAST_DATUM(d);

		if (vec->dim != dim)
		{
			SPI_finish();
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("inconsistent vector dim at row %d: "
							"expected %d, got %d",
							i, dim, vec->dim)));
		}

		memcpy(sample_data + i * dim, vec->x, sizeof(float) * dim);

		/* L2-normalize so PQ learns cosine-equivalent space */
		normalize_vector(sample_data + i * dim, dim);

		/* Free detoasted copy to prevent memory growth */
		if (DatumGetPointer(d) != (Pointer) vec)
			pfree(vec);
	}

	SPI_finish();

	ereport(NOTICE,
			(errmsg("PQ training: sampled %d vectors (%d MB), starting k-means...",
					n_samples,
					(int)((int64)n_samples * dim * sizeof(float) / (1024*1024)))));

	/* Train k-means for each subvector group */
	sub_data = palloc(sizeof(float) * n_samples * dsub);
	centroids = palloc(sizeof(float) * PQ_KSUB * dsub);

	/*
	 * Create temporary context for building INSERT SQL strings.
	 * Each batched INSERT is ~50KB; we reset after each SPI_execute.
	 */
	tmp_ctx = AllocSetContextCreate(func_ctx,
									"PQ training temp",
									ALLOCSET_DEFAULT_SIZES);

	/* Create tables and get codebook ID */
	SPI_connect();
	{
		const char *es = get_ext_schema();

		/* Create meta table if not exists */
		snprintf(sql, sizeof(sql),
				 "CREATE TABLE IF NOT EXISTS %s._pq_codebook_meta ("
				 "  cb_id int PRIMARY KEY,"
				 "  m int NOT NULL,"
				 "  dsub int NOT NULL,"
				 "  total_dim int NOT NULL"
				 ")", es);
		ret = SPI_execute(sql, false, 0);

		if (ret != SPI_OK_UTILITY)
		{
			SPI_finish();
			ereport(ERROR, (errmsg("failed to create _pq_codebook_meta")));
		}

		/* Create codebook table if not exists */
		snprintf(sql, sizeof(sql),
				 "CREATE TABLE IF NOT EXISTS %s._pq_codebooks ("
				 "  cb_id int NOT NULL,"
				 "  sub_id int NOT NULL,"
				 "  cent_id int NOT NULL,"
				 "  centroid %s.svec NOT NULL,"
				 "  PRIMARY KEY (cb_id, sub_id, cent_id)"
				 ")", es, es);
		ret = SPI_execute(sql, false, 0);

		if (ret != SPI_OK_UTILITY)
		{
			SPI_finish();
			ereport(ERROR, (errmsg("failed to create _pq_codebooks")));
		}

		/* Get next cb_id */
		snprintf(sql, sizeof(sql),
				 "SELECT COALESCE(MAX(cb_id), 0) + 1 FROM %s._pq_codebook_meta",
				 es);
		ret = SPI_execute(sql, true, 1);

		if (ret != SPI_OK_SELECT || SPI_processed != 1)
		{
			SPI_finish();
			ereport(ERROR, (errmsg("failed to get next cb_id")));
		}

		cb_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 1,
											&(bool){false}));

		/* Insert meta row */
		snprintf(sql, sizeof(sql),
				 "INSERT INTO %s._pq_codebook_meta (cb_id, m, dsub, total_dim) "
				 "VALUES (%d, %d, %d, %d)",
				 es, cb_id, M, dsub, dim);
	}
	ret = SPI_execute(sql, false, 0);

	if (ret != SPI_OK_INSERT)
	{
		SPI_finish();
		ereport(ERROR, (errmsg("failed to insert codebook meta")));
	}

	SPI_finish();

	/* Train each subvector group and store centroids */
	for (m = 0; m < M; m++)
	{
		int			offset = m * dsub;
		StringInfoData insert_buf;

		/* Extract subvector slice from all samples */
		for (i = 0; i < n_samples; i++)
			memcpy(sub_data + i * dsub,
				   sample_data + i * dim + offset,
				   sizeof(float) * dsub);

		/* Run k-means */
		kmeans_train(sub_data, n_samples, dsub, PQ_KSUB, n_iter,
					 centroids);

		if ((m + 1) % 10 == 0 || m == 0 || m == M - 1)
			ereport(NOTICE,
					(errmsg("PQ training: subvector %d/%d k-means done",
							m + 1, M)));

		/*
		 * Build a single multi-VALUES INSERT for all 256 centroids.
		 * This is ~256x faster than individual INSERTs and avoids SPI
		 * memory accumulation issues.
		 */
		old_ctx = MemoryContextSwitchTo(tmp_ctx);
		initStringInfo(&insert_buf);
		appendStringInfo(&insert_buf,
						 "INSERT INTO %s._pq_codebooks "
						 "(cb_id, sub_id, cent_id, centroid) VALUES ",
						 get_ext_schema());

		for (j = 0; j < PQ_KSUB; j++)
		{
			float  *cent = centroids + j * dsub;
			int		k;

			if (j > 0)
				appendStringInfoChar(&insert_buf, ',');

			appendStringInfo(&insert_buf, "(%d,%d,%d,'[", cb_id, m, j);
			for (k = 0; k < dsub; k++)
			{
				if (k > 0)
					appendStringInfoChar(&insert_buf, ',');
				appendStringInfo(&insert_buf, "%.9g", (double) cent[k]);
			}
			appendStringInfo(&insert_buf, "]'::%s.svec)", get_ext_schema());
		}

		MemoryContextSwitchTo(old_ctx);

		/* Execute the batched INSERT */
		SPI_connect();

		if ((m + 1) % 10 == 0 || m == 0 || m == M - 1)
			ereport(NOTICE,
					(errmsg("PQ training: subvector %d/%d inserting "
							"(%d bytes SQL)", m + 1, M,
							(int) strlen(insert_buf.data))));

		ret = SPI_execute(insert_buf.data, false, 0);
		SPI_finish();

		/* Free the large SQL string */
		MemoryContextReset(tmp_ctx);

		if (ret != SPI_OK_INSERT)
			ereport(ERROR,
					(errmsg("failed to insert centroids for sub=%d", m)));
	}

	pfree(sub_data);
	pfree(centroids);
	pfree(sample_data);
	MemoryContextDelete(tmp_ctx);

	/* Invalidate cache */
	cached_pq.cb_id = -1;

	ereport(NOTICE,
			(errmsg("PQ training complete: cb_id=%d, M=%d, dsub=%d, "
					"stored %d centroids",
					cb_id, M, dsub, M * PQ_KSUB)));

	PG_RETURN_INT32(cb_id);
}

/* ----------------------------------------------------------------
 *  svec_pq_train_residual: train PQ codebook on IVF residuals
 *
 *  svec_pq_train_residual(source_query text, M int, ivf_cb_id int,
 *                         n_iter int DEFAULT 10, max_samples int DEFAULT 10000) → int
 *
 *  Same as svec_pq_train but:
 *  1. Loads IVF centroids for ivf_cb_id
 *  2. For each sample: residual = vector - nearest_centroid (NO normalization)
 *  3. Trains k-means on residuals
 *  Stores residual flag in _pq_codebook_meta.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(svec_pq_train_residual);
Datum
svec_pq_train_residual(PG_FUNCTION_ARGS)
{
	text	   *source_query = PG_GETARG_TEXT_PP(0);
	int			M = PG_GETARG_INT32(1);
	int			ivf_cb_id = PG_GETARG_INT32(2);
	int			n_iter = PG_GETARG_INT32(3);
	int			max_samples = PG_GETARG_INT32(4);

	char	   *query_str;
	StringInfoData wrapped_query;
	int			ret;
	int			n_samples;
	int			dim, dsub, nlist;
	float	   *sample_data;
	float	   *sub_data;
	float	   *centroids;
	int			cb_id;
	int			m, i, j;
	char		sql[512];
	MemoryContext func_ctx;
	MemoryContext tmp_ctx;
	MemoryContext old_ctx;

	check_training_privileges();

	if (M < 1 || M > PQ_MAX_M)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("M must be between 1 and %d", PQ_MAX_M)));

	if (n_iter < 1 || n_iter > PQ_MAX_ITER)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("n_iter must be between 1 and %d", PQ_MAX_ITER)));

	if (max_samples < 256)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("max_samples must be at least 256")));

	/* Load IVF centroids first */
	ivf_load_centroids(ivf_cb_id);
	nlist = cached_ivf.nlist;

	query_str = text_to_cstring(source_query);
	func_ctx = CurrentMemoryContext;

	/* Sample vectors */
	initStringInfo(&wrapped_query);
	appendStringInfo(&wrapped_query,
					 "SELECT * FROM (%s) _src ORDER BY random() LIMIT %d",
					 query_str, max_samples);

	SPI_connect();
	ret = SPI_execute(wrapped_query.data, true, 0);
	pfree(wrapped_query.data);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 errmsg("source query returned no rows")));
	}

	n_samples = (int) SPI_processed;

	/* Read first vector to get dimension */
	{
		bool	isnull;
		Datum	d;
		Svec   *first;

		d = SPI_getbinval(SPI_tuptable->vals[0],
						  SPI_tuptable->tupdesc, 1, &isnull);
		if (isnull)
		{
			SPI_finish();
			ereport(ERROR, (errmsg("NULL vector in training data")));
		}

		first = (Svec *) PG_DETOAST_DATUM(d);
		dim = first->dim;
		if (DatumGetPointer(d) != (Pointer) first)
			pfree(first);
	}

	if (dim != cached_ivf.dim)
	{
		SPI_finish();
		ereport(ERROR,
				(errmsg("vector dim %d doesn't match IVF dim %d",
						dim, cached_ivf.dim)));
	}

	if (dim % M != 0)
	{
		SPI_finish();
		ereport(ERROR,
				(errmsg("vector dimension %d is not divisible by M=%d",
						dim, M)));
	}

	dsub = dim / M;

	ereport(NOTICE,
			(errmsg("Residual PQ training: dim=%d, M=%d, dsub=%d, "
					"ivf_cb=%d (nlist=%d), n_iter=%d, samples=%d",
					dim, M, dsub, ivf_cb_id, nlist, n_iter, n_samples)));

	/* Allocate in func_ctx */
	old_ctx = MemoryContextSwitchTo(func_ctx);
	sample_data = palloc(sizeof(float) * n_samples * dim);
	MemoryContextSwitchTo(old_ctx);

	/* Copy vectors and convert to residuals */
	for (i = 0; i < n_samples; i++)
	{
		bool	isnull;
		Datum	d;
		Svec   *vec;
		float  *dest;
		float	best_dist;
		int		best_c;

		d = SPI_getbinval(SPI_tuptable->vals[i],
						  SPI_tuptable->tupdesc, 1, &isnull);
		if (isnull)
		{
			SPI_finish();
			ereport(ERROR,
					(errmsg("NULL vector in training data at row %d", i)));
		}

		vec = (Svec *) PG_DETOAST_DATUM(d);
		if (vec->dim != dim)
		{
			SPI_finish();
			ereport(ERROR,
					(errmsg("inconsistent dim at row %d: expected %d, got %d",
							i, dim, vec->dim)));
		}

		dest = sample_data + i * dim;
		memcpy(dest, vec->x, sizeof(float) * dim);

		/* Find nearest IVF centroid */
		best_dist = 1e30f;
		best_c = 0;
		for (j = 0; j < nlist; j++)
		{
			float  *cent = cached_ivf.centroids + j * dim;
			float	dist = 0.0f;
			int		k;

			for (k = 0; k < dim; k++)
			{
				float diff = dest[k] - cent[k];

				dist += diff * diff;
			}
			if (dist < best_dist)
			{
				best_dist = dist;
				best_c = j;
			}
		}

		/* Subtract centroid → residual */
		{
			float  *cent = cached_ivf.centroids + best_c * dim;

			for (j = 0; j < dim; j++)
				dest[j] -= cent[j];
		}

		if (DatumGetPointer(d) != (Pointer) vec)
			pfree(vec);
	}

	SPI_finish();

	ereport(NOTICE,
			(errmsg("Residual PQ: computed %d residuals, starting k-means...",
					n_samples)));

	/* Train k-means on residuals (same as svec_pq_train from here) */
	sub_data = palloc(sizeof(float) * n_samples * dsub);
	centroids = palloc(sizeof(float) * PQ_KSUB * dsub);

	tmp_ctx = AllocSetContextCreate(func_ctx,
									"RPQ training temp",
									ALLOCSET_DEFAULT_SIZES);

	/* Create tables and get codebook ID */
	SPI_connect();
	{
		const char *es = get_ext_schema();

		snprintf(sql, sizeof(sql),
				 "CREATE TABLE IF NOT EXISTS %s._pq_codebook_meta ("
				 "  cb_id int PRIMARY KEY,"
				 "  m int NOT NULL,"
				 "  dsub int NOT NULL,"
				 "  total_dim int NOT NULL"
				 ")", es);
		ret = SPI_execute(sql, false, 0);

		/* Add residual columns if missing */
		snprintf(sql, sizeof(sql),
				 "ALTER TABLE %s._pq_codebook_meta "
				 "ADD COLUMN IF NOT EXISTS residual boolean DEFAULT false",
				 es);
		SPI_execute(sql, false, 0);
		snprintf(sql, sizeof(sql),
				 "ALTER TABLE %s._pq_codebook_meta "
				 "ADD COLUMN IF NOT EXISTS ivf_cb_id int",
				 es);
		SPI_execute(sql, false, 0);

		snprintf(sql, sizeof(sql),
				 "CREATE TABLE IF NOT EXISTS %s._pq_codebooks ("
				 "  cb_id int NOT NULL,"
				 "  sub_id int NOT NULL,"
				 "  cent_id int NOT NULL,"
				 "  centroid %s.svec NOT NULL,"
				 "  PRIMARY KEY (cb_id, sub_id, cent_id)"
				 ")", es, es);
		ret = SPI_execute(sql, false, 0);

		/* Get next cb_id */
		snprintf(sql, sizeof(sql),
				 "SELECT COALESCE(MAX(cb_id), 0) + 1 FROM %s._pq_codebook_meta",
				 es);
		ret = SPI_execute(sql, true, 1);

		if (ret != SPI_OK_SELECT || SPI_processed != 1)
		{
			SPI_finish();
			ereport(ERROR, (errmsg("failed to get next cb_id")));
		}

		cb_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 1,
											&(bool){false}));

		/* Insert meta row with residual flag */
		snprintf(sql, sizeof(sql),
				 "INSERT INTO %s._pq_codebook_meta "
				 "(cb_id, m, dsub, total_dim, residual, ivf_cb_id) "
				 "VALUES (%d, %d, %d, %d, true, %d)",
				 es, cb_id, M, dsub, dim, ivf_cb_id);
	}
	ret = SPI_execute(sql, false, 0);

	if (ret != SPI_OK_INSERT)
	{
		SPI_finish();
		ereport(ERROR, (errmsg("failed to insert codebook meta")));
	}

	SPI_finish();

	/* Train each subvector group and store centroids (identical to raw PQ) */
	for (m = 0; m < M; m++)
	{
		int			offset = m * dsub;
		StringInfoData insert_buf;

		for (i = 0; i < n_samples; i++)
			memcpy(sub_data + i * dsub,
				   sample_data + i * dim + offset,
				   sizeof(float) * dsub);

		kmeans_train(sub_data, n_samples, dsub, PQ_KSUB, n_iter,
					 centroids);

		if ((m + 1) % 10 == 0 || m == 0 || m == M - 1)
			ereport(NOTICE,
					(errmsg("Residual PQ: subvector %d/%d k-means done",
							m + 1, M)));

		old_ctx = MemoryContextSwitchTo(tmp_ctx);
		initStringInfo(&insert_buf);
		appendStringInfo(&insert_buf,
						 "INSERT INTO %s._pq_codebooks "
						 "(cb_id, sub_id, cent_id, centroid) VALUES ",
						 get_ext_schema());

		for (j = 0; j < PQ_KSUB; j++)
		{
			float  *cent = centroids + j * dsub;
			int		k;

			if (j > 0)
				appendStringInfoChar(&insert_buf, ',');

			appendStringInfo(&insert_buf, "(%d,%d,%d,'[", cb_id, m, j);
			for (k = 0; k < dsub; k++)
			{
				if (k > 0)
					appendStringInfoChar(&insert_buf, ',');
				appendStringInfo(&insert_buf, "%.9g", (double) cent[k]);
			}
			appendStringInfo(&insert_buf, "]'::%s.svec)", get_ext_schema());
		}

		MemoryContextSwitchTo(old_ctx);

		SPI_connect();

		if ((m + 1) % 10 == 0 || m == 0 || m == M - 1)
			ereport(NOTICE,
					(errmsg("Residual PQ: subvector %d/%d inserting "
							"(%d bytes SQL)", m + 1, M,
							(int) strlen(insert_buf.data))));

		ret = SPI_execute(insert_buf.data, false, 0);
		SPI_finish();

		MemoryContextReset(tmp_ctx);

		if (ret != SPI_OK_INSERT)
			ereport(ERROR,
					(errmsg("failed to insert centroids for sub=%d", m)));
	}

	pfree(sub_data);
	pfree(centroids);
	pfree(sample_data);
	MemoryContextDelete(tmp_ctx);

	cached_pq.cb_id = -1;

	ereport(NOTICE,
			(errmsg("Residual PQ training complete: cb_id=%d, M=%d, dsub=%d, "
					"ivf_cb=%d, stored %d centroids",
					cb_id, M, dsub, ivf_cb_id, M * PQ_KSUB)));

	PG_RETURN_INT32(cb_id);
}

/* ----------------------------------------------------------------
 *  svec_pq_encode_residual: encode vector residual to PQ code
 *
 *  svec_pq_encode_residual(vec svec, partition_id int2, pq_cb_id int,
 *                          ivf_cb_id int) → bytea
 *
 *  Computes residual = vec - centroid[partition_id], then PQ-encodes.
 *  No normalization (residual PQ operates in the original space).
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(svec_pq_encode_residual);
Datum
svec_pq_encode_residual(PG_FUNCTION_ARGS)
{
	Svec	   *vec = PG_GETARG_SVEC_P(0);
	int			partition_id = PG_GETARG_INT16(1);
	int			pq_cb_id = PG_GETARG_INT32(2);
	int			ivf_cb_id = PG_GETARG_INT32(3);
	int			M, dsub, dim;
	bytea	   *result;
	uint8	   *codes;
	int			m, j, c;
	float	   *residual;
	float	   *cent;

	pq_load_codebook(pq_cb_id);
	ivf_load_centroids(ivf_cb_id);

	M = cached_pq.M;
	dsub = cached_pq.dsub;
	dim = cached_pq.total_dim;

	if (vec->dim != dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("vector dimension %d doesn't match codebook "
						"(expected %d)", vec->dim, dim)));

	if (partition_id < 0 || partition_id >= cached_ivf.nlist)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("partition_id %d out of range [0, %d)",
						partition_id, cached_ivf.nlist)));

	/* Compute residual = vec - centroid[partition_id] */
	residual = palloc(sizeof(float) * dim);
	cent = cached_ivf.centroids + partition_id * dim;
	for (j = 0; j < dim; j++)
		residual[j] = vec->x[j] - cent[j];

	/* PQ-encode the residual */
	result = (bytea *) palloc(VARHDRSZ + M);
	SET_VARSIZE(result, VARHDRSZ + M);
	codes = (uint8 *) VARDATA(result);

	for (m = 0; m < M; m++)
	{
		float  *sub = residual + m * dsub;
		float  *cb_base = cached_pq.centroids + m * PQ_KSUB * dsub;
		float	best_dist = 1e30f;
		int		best_c = 0;

		for (c = 0; c < PQ_KSUB; c++)
		{
			float  *cen = cb_base + c * dsub;
			float	dist = 0.0f;

			for (j = 0; j < dsub; j++)
			{
				float diff = sub[j] - cen[j];

				dist += diff * diff;
			}

			if (dist < best_dist)
			{
				best_dist = dist;
				best_c = c;
			}
		}

		codes[m] = (uint8) best_c;
	}

	pfree(residual);
	PG_RETURN_BYTEA_P(result);
}

/* ----------------------------------------------------------------
 *  svec_pq_distance_table_residual: distance table for one centroid
 *
 *  svec_pq_distance_table_residual(query svec, centroid_id int2,
 *                                  pq_cb_id int, ivf_cb_id int) → bytea
 *
 *  Computes query_residual = query - centroid[centroid_id], then builds
 *  a distance table (M × 256 float array stored as bytea) from the
 *  query residual to all PQ centroids.
 *
 *  Used with svec_pq_adc_lookup for the ADC scan:
 *    ORDER BY svec_pq_adc_lookup(dt.t, m.pq_code)
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(svec_pq_distance_table_residual);
Datum
svec_pq_distance_table_residual(PG_FUNCTION_ARGS)
{
	Svec	   *query = PG_GETARG_SVEC_P(0);
	int			centroid_id = PG_GETARG_INT16(1);
	int			pq_cb_id = PG_GETARG_INT32(2);
	int			ivf_cb_id = PG_GETARG_INT32(3);
	int			M, dsub, dim;
	int			m, c, j;
	int			table_bytes;
	bytea	   *result;
	float	   *table;
	float	   *q_residual;
	float	   *cent;

	pq_load_codebook(pq_cb_id);
	ivf_load_centroids(ivf_cb_id);

	M = cached_pq.M;
	dsub = cached_pq.dsub;
	dim = cached_pq.total_dim;

	if (query->dim != dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("query dimension %d doesn't match codebook "
						"(expected %d)", query->dim, dim)));

	if (centroid_id < 0 || centroid_id >= cached_ivf.nlist)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("centroid_id %d out of range [0, %d)",
						centroid_id, cached_ivf.nlist)));

	/* Compute query residual = query - centroid[centroid_id] */
	q_residual = palloc(sizeof(float) * dim);
	cent = cached_ivf.centroids + centroid_id * dim;
	for (j = 0; j < dim; j++)
		q_residual[j] = query->x[j] - cent[j];

	/* Build distance table: M × 256 floats */
	table_bytes = sizeof(float) * M * PQ_KSUB;
	result = (bytea *) palloc(VARHDRSZ + table_bytes);
	SET_VARSIZE(result, VARHDRSZ + table_bytes);
	table = (float *) VARDATA(result);

	for (m = 0; m < M; m++)
	{
		float  *qsub = q_residual + m * dsub;
		float  *cb_base = cached_pq.centroids + m * PQ_KSUB * dsub;

		for (c = 0; c < PQ_KSUB; c++)
		{
			float  *cen = cb_base + c * dsub;
			float	d = 0.0f;

			for (j = 0; j < dsub; j++)
			{
				float diff = qsub[j] - cen[j];

				d += diff * diff;
			}
			table[m * PQ_KSUB + c] = d;
		}
	}

	pfree(q_residual);
	PG_RETURN_BYTEA_P(result);
}

/* ----------------------------------------------------------------
 *  svec_pq_encode: encode vector to PQ code
 *
 *  svec_pq_encode(vec svec, cb_id int) → bytea
 *
 *  For each subvector, find nearest centroid and store its index (0-255)
 *  as one byte. Result is M-byte bytea.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(svec_pq_encode);
Datum
svec_pq_encode(PG_FUNCTION_ARGS)
{
	Svec	   *vec = PG_GETARG_SVEC_P(0);
	int			cb_id = PG_GETARG_INT32(1);
	int			M, dsub, dim;
	bytea	   *result;
	uint8	   *codes;
	int			m, j, c;
	float	   *norm_vec;

	pq_load_codebook(cb_id);

	M = cached_pq.M;
	dsub = cached_pq.dsub;
	dim = cached_pq.total_dim;

	if (vec->dim != dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("vector dimension %d doesn't match codebook "
						"(expected %d)", vec->dim, dim)));

	/* L2-normalize a copy for cosine-equivalent encoding */
	norm_vec = palloc(sizeof(float) * dim);
	memcpy(norm_vec, vec->x, sizeof(float) * dim);
	normalize_vector(norm_vec, dim);

	result = (bytea *) palloc(VARHDRSZ + M);
	SET_VARSIZE(result, VARHDRSZ + M);
	codes = (uint8 *) VARDATA(result);

	for (m = 0; m < M; m++)
	{
		float  *sub = norm_vec + m * dsub;
		float  *cb_base = cached_pq.centroids + m * PQ_KSUB * dsub;
		float	best_dist = 1e30f;
		int		best_c = 0;

		for (c = 0; c < PQ_KSUB; c++)
		{
			float  *cent = cb_base + c * dsub;
			float	dist = 0.0f;

			for (j = 0; j < dsub; j++)
			{
				float diff = sub[j] - cent[j];
				dist += diff * diff;
			}

			if (dist < best_dist)
			{
				best_dist = dist;
				best_c = c;
			}
		}

		codes[m] = (uint8) best_c;
	}

	pfree(norm_vec);
	PG_RETURN_BYTEA_P(result);
}

/* ----------------------------------------------------------------
 *  svec_pq_distance: ADC distance estimation
 *
 *  svec_pq_distance(query svec, code bytea, cb_id int) → float8
 *
 *  Asymmetric Distance Computation:
 *  1. Precompute dist_table[m][c] = L2_sq(query_sub[m], centroid[m][c])
 *     for all M subvectors and 256 centroids
 *  2. Sum dist_table[m][code[m]] over all M subvectors
 *
 *  Returns squared L2 distance estimate.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(svec_pq_distance);
Datum
svec_pq_distance(PG_FUNCTION_ARGS)
{
	Svec	   *query = PG_GETARG_SVEC_P(0);
	bytea	   *code = PG_GETARG_BYTEA_PP(1);
	int			cb_id = PG_GETARG_INT32(2);
	int			M, dsub;
	int			code_len;
	uint8	   *codes;
	float	   *dist_table;		/* M × 256 precomputed distances */
	double		total_dist;
	int			m, c, j;

	pq_load_codebook(cb_id);

	M = cached_pq.M;
	dsub = cached_pq.dsub;

	if (query->dim != cached_pq.total_dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("query dimension %d doesn't match codebook "
						"(expected %d)", query->dim, cached_pq.total_dim)));

	code_len = VARSIZE_ANY_EXHDR(code);
	if (code_len != M)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("PQ code length %d doesn't match M=%d",
						code_len, M)));

	codes = (uint8 *) VARDATA_ANY(code);

	/* Precompute distance table: dist_table[m * 256 + c] */
	dist_table = palloc(sizeof(float) * M * PQ_KSUB);

	/* L2-normalize query for cosine-equivalent ADC */
	{
		float  *norm_q = palloc(sizeof(float) * query->dim);

		memcpy(norm_q, query->x, sizeof(float) * query->dim);
		normalize_vector(norm_q, query->dim);

		for (m = 0; m < M; m++)
		{
			float  *qsub = norm_q + m * dsub;
			float  *cb_base = cached_pq.centroids + m * PQ_KSUB * dsub;

			for (c = 0; c < PQ_KSUB; c++)
			{
				float  *cent = cb_base + c * dsub;
				float	dist = 0.0f;

				for (j = 0; j < dsub; j++)
				{
					float diff = qsub[j] - cent[j];
					dist += diff * diff;
				}

				dist_table[m * PQ_KSUB + c] = dist;
			}
		}

		pfree(norm_q);
	}

	/* Sum distances using PQ codes */
	total_dist = 0.0;
	for (m = 0; m < M; m++)
		total_dist += (double) dist_table[m * PQ_KSUB + codes[m]];

	pfree(dist_table);

	PG_RETURN_FLOAT8(total_dist);
}

/* ----------------------------------------------------------------
 *  svec_pq_distance_table: precompute ADC distance table
 *
 *  svec_pq_distance_table(query svec, cb_id int) → bytea
 *
 *  Computes dist_table[m][c] = L2_sq(query_sub[m], centroid[m][c])
 *  for all M subvectors and 256 centroids.  Returns as bytea
 *  (M × 256 float32 values = M × 1024 bytes).
 *
 *  This should be computed ONCE per query, then passed to
 *  svec_pq_adc_lookup() for each candidate.  This split avoids
 *  recomputing 180 × 256 × 16 distances per row.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(svec_pq_distance_table);
Datum
svec_pq_distance_table(PG_FUNCTION_ARGS)
{
	Svec	   *query = PG_GETARG_SVEC_P(0);
	int			cb_id = PG_GETARG_INT32(1);
	int			M, dsub;
	bytea	   *result;
	float	   *dt;
	int			m, c, j;

	pq_load_codebook(cb_id);

	M = cached_pq.M;
	dsub = cached_pq.dsub;

	if (query->dim != cached_pq.total_dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("query dimension %d doesn't match codebook "
						"(expected %d)", query->dim, cached_pq.total_dim)));

	result = (bytea *) palloc(VARHDRSZ + sizeof(float) * M * PQ_KSUB);
	SET_VARSIZE(result, VARHDRSZ + sizeof(float) * M * PQ_KSUB);
	dt = (float *) VARDATA(result);

	/* L2-normalize query for cosine-equivalent ADC */
	{
		float  *norm_q = palloc(sizeof(float) * query->dim);

		memcpy(norm_q, query->x, sizeof(float) * query->dim);
		normalize_vector(norm_q, query->dim);

		for (m = 0; m < M; m++)
		{
			float  *qsub = norm_q + m * dsub;
			float  *cb_base = cached_pq.centroids + m * PQ_KSUB * dsub;

			for (c = 0; c < PQ_KSUB; c++)
			{
				float  *cent = cb_base + c * dsub;
				float	dist = 0.0f;

				for (j = 0; j < dsub; j++)
				{
					float diff = qsub[j] - cent[j];
					dist += diff * diff;
				}

				dt[m * PQ_KSUB + c] = dist;
			}
		}

		pfree(norm_q);
	}

	PG_RETURN_BYTEA_P(result);
}

/* ----------------------------------------------------------------
 *  svec_pq_adc_lookup: fast ADC distance from precomputed table
 *
 *  svec_pq_adc_lookup(dist_table bytea, code bytea) → float8
 *
 *  Sums dist_table[m][code[m]] over all M subvectors.
 *  This is O(M) = 180 additions — sub-microsecond per call.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(svec_pq_adc_lookup);
Datum
svec_pq_adc_lookup(PG_FUNCTION_ARGS)
{
	bytea	   *dt_bytea = PG_GETARG_BYTEA_PP(0);
	bytea	   *code = PG_GETARG_BYTEA_PP(1);
	float	   *dt;
	uint8	   *codes;
	int			M;
	double		total_dist;
	int			m;

	dt = (float *) VARDATA_ANY(dt_bytea);
	codes = (uint8 *) VARDATA_ANY(code);
	M = VARSIZE_ANY_EXHDR(code);

	total_dist = 0.0;
	for (m = 0; m < M; m++)
		total_dist += (double) dt[m * PQ_KSUB + codes[m]];

	PG_RETURN_FLOAT8(total_dist);
}

/* ================================================================
 *  IVF (Inverted File Index) functions
 *
 *  These enable IVF-PQ approximate nearest neighbor search where
 *  sorted_heap physical clustering by PK prefix acts as the inverted
 *  file — no separate index structure needed.
 *
 *  svec_ivf_train:  train IVF centroids via k-means
 *  svec_ivf_assign: assign vector to nearest centroid (for GENERATED cols)
 *  svec_ivf_probe:  find nprobe nearest centroids for query-time search
 * ================================================================ */

/* Batch size for centroid INSERTs (high-dim vectors are large as text) */
#define IVF_INSERT_BATCH	50

/* ----------------------------------------------------------------
 *  svec_ivf_train: train IVF centroids from data
 *
 *  svec_ivf_train(source_query text, nlist int4,
 *                 n_iter int4 DEFAULT 10,
 *                 max_samples int4 DEFAULT 10000) → int4
 *
 *  source_query must return rows with a single svec column.
 *  Trains nlist centroids via k-means on full vectors.
 *  Stores results in _ivf_meta and _ivf_centroids tables.
 *  Returns codebook ID.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(svec_ivf_train);
Datum
svec_ivf_train(PG_FUNCTION_ARGS)
{
	text	   *source_query = PG_GETARG_TEXT_PP(0);
	int			nlist = PG_GETARG_INT32(1);
	int			n_iter = PG_GETARG_INT32(2);
	int			max_samples = PG_GETARG_INT32(3);

	char	   *query_str;
	StringInfoData wrapped_query;
	int			ret;
	int			n_samples;
	int			dim;
	float	   *sample_data;	/* n_samples * dim */
	float	   *centroids;		/* nlist * dim */
	int			cb_id;
	int			i;
	char		sql[256];
	MemoryContext func_ctx;
	MemoryContext tmp_ctx;
	MemoryContext old_ctx;

	check_training_privileges();

	if (nlist < 1 || nlist > IVF_MAX_NLIST)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("nlist must be between 1 and %d", IVF_MAX_NLIST)));

	if (n_iter < 1 || n_iter > PQ_MAX_ITER)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("n_iter must be between 1 and %d", PQ_MAX_ITER)));

	if (max_samples < nlist)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("max_samples must be at least nlist (%d)", nlist)));

	query_str = text_to_cstring(source_query);
	func_ctx = CurrentMemoryContext;

	/*
	 * Wrap the user query with ORDER BY random() LIMIT max_samples.
	 * PostgreSQL uses a bounded heap sort for LIMIT, so only max_samples
	 * rows are held in memory at any time — O(K) instead of O(N).
	 */
	initStringInfo(&wrapped_query);
	appendStringInfo(&wrapped_query,
					 "SELECT * FROM (%s) _src ORDER BY random() LIMIT %d",
					 query_str, max_samples);

	/* Execute wrapped query — returns at most max_samples rows */
	SPI_connect();
	ret = SPI_execute(wrapped_query.data, true, 0);
	pfree(wrapped_query.data);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 errmsg("source query returned no rows")));
	}

	n_samples = (int) SPI_processed;

	/* Read first vector to get dimension */
	{
		bool	isnull;
		Datum	d;
		Svec   *first;

		d = SPI_getbinval(SPI_tuptable->vals[0],
						  SPI_tuptable->tupdesc, 1, &isnull);

		if (isnull)
		{
			SPI_finish();
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("NULL vector in training data")));
		}

		first = (Svec *) PG_DETOAST_DATUM(d);
		dim = first->dim;
		if (DatumGetPointer(d) != (Pointer) first)
			pfree(first);
	}

	ereport(NOTICE,
			(errmsg("IVF training: dim=%d, nlist=%d, n_iter=%d, samples=%d",
					dim, nlist, n_iter, n_samples)));

	/* Allocate in func_ctx to survive SPI_finish */
	old_ctx = MemoryContextSwitchTo(func_ctx);
	sample_data = palloc(sizeof(float) * n_samples * dim);
	MemoryContextSwitchTo(old_ctx);

	/*
	 * Copy all returned vectors into contiguous float array.
	 * Rows are already randomly sampled by ORDER BY random() LIMIT.
	 */
	for (i = 0; i < n_samples; i++)
	{
		bool	isnull;
		Datum	d;
		Svec   *vec;

		d = SPI_getbinval(SPI_tuptable->vals[i],
						  SPI_tuptable->tupdesc, 1, &isnull);

		if (isnull)
		{
			SPI_finish();
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("NULL vector in training data at row %d", i)));
		}

		vec = (Svec *) PG_DETOAST_DATUM(d);

		if (vec->dim != dim)
		{
			SPI_finish();
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("inconsistent vector dim at row %d: "
							"expected %d, got %d",
							i, dim, vec->dim)));
		}

		memcpy(sample_data + i * dim, vec->x, sizeof(float) * dim);

		if (DatumGetPointer(d) != (Pointer) vec)
			pfree(vec);
	}

	SPI_finish();

	ereport(NOTICE,
			(errmsg("IVF training: sampled %d vectors (%d MB), "
					"starting k-means...",
					n_samples,
					(int)((int64)n_samples * dim * sizeof(float) / (1024*1024)))));

	/* Train k-means on full vectors */
	centroids = palloc(sizeof(float) * nlist * dim);
	kmeans_train(sample_data, n_samples, dim, nlist, n_iter, centroids);

	ereport(NOTICE,
			(errmsg("IVF training: k-means complete, storing %d centroids",
					nlist)));

	/* Create temp context for building INSERT SQL strings */
	tmp_ctx = AllocSetContextCreate(func_ctx,
									"IVF training temp",
									ALLOCSET_DEFAULT_SIZES);

	/* Create tables and get codebook ID */
	SPI_connect();
	{
		const char *es = get_ext_schema();

		snprintf(sql, sizeof(sql),
				 "CREATE TABLE IF NOT EXISTS %s._ivf_meta ("
				 "  cb_id int PRIMARY KEY,"
				 "  nlist int NOT NULL,"
				 "  dim int NOT NULL"
				 ")", es);
		ret = SPI_execute(sql, false, 0);

		if (ret != SPI_OK_UTILITY)
		{
			SPI_finish();
			ereport(ERROR, (errmsg("failed to create _ivf_meta")));
		}

		snprintf(sql, sizeof(sql),
				 "CREATE TABLE IF NOT EXISTS %s._ivf_centroids ("
				 "  cb_id int NOT NULL,"
				 "  centroid_id int NOT NULL,"
				 "  centroid %s.svec NOT NULL,"
				 "  PRIMARY KEY (cb_id, centroid_id)"
				 ")", es, es);
		ret = SPI_execute(sql, false, 0);

		if (ret != SPI_OK_UTILITY)
		{
			SPI_finish();
			ereport(ERROR, (errmsg("failed to create _ivf_centroids")));
		}

		/* Get next cb_id */
		snprintf(sql, sizeof(sql),
				 "SELECT COALESCE(MAX(cb_id), 0) + 1 FROM %s._ivf_meta",
				 es);
		ret = SPI_execute(sql, true, 1);

		if (ret != SPI_OK_SELECT || SPI_processed != 1)
		{
			SPI_finish();
			ereport(ERROR, (errmsg("failed to get next IVF cb_id")));
		}

		cb_id = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
											SPI_tuptable->tupdesc, 1,
											&(bool){false}));

		/* Insert meta row */
		snprintf(sql, sizeof(sql),
				 "INSERT INTO %s._ivf_meta (cb_id, nlist, dim) "
				 "VALUES (%d, %d, %d)",
				 es, cb_id, nlist, dim);
	}
	ret = SPI_execute(sql, false, 0);

	if (ret != SPI_OK_INSERT)
	{
		SPI_finish();
		ereport(ERROR, (errmsg("failed to insert IVF meta")));
	}

	SPI_finish();

	/*
	 * Insert centroids in batches of IVF_INSERT_BATCH to keep SQL strings
	 * manageable for high-dimensional vectors (dim=2880 -> ~17KB per centroid
	 * as text).
	 */
	{
		int		batch_start = 0;

		while (batch_start < nlist)
		{
			int				batch_end;
			int				b;
			StringInfoData	insert_buf;

			batch_end = batch_start + IVF_INSERT_BATCH;
			if (batch_end > nlist)
				batch_end = nlist;

			old_ctx = MemoryContextSwitchTo(tmp_ctx);
			initStringInfo(&insert_buf);
			appendStringInfo(&insert_buf,
							 "INSERT INTO %s._ivf_centroids "
							 "(cb_id, centroid_id, centroid) VALUES ",
							 get_ext_schema());

			for (b = batch_start; b < batch_end; b++)
			{
				float  *cent = centroids + b * dim;
				int		k;

				if (b > batch_start)
					appendStringInfoChar(&insert_buf, ',');

				appendStringInfo(&insert_buf, "(%d,%d,'[", cb_id, b);
				for (k = 0; k < dim; k++)
				{
					if (k > 0)
						appendStringInfoChar(&insert_buf, ',');
					appendStringInfo(&insert_buf, "%.9g", (double) cent[k]);
				}
				appendStringInfo(&insert_buf, "]'::%s.svec)", get_ext_schema());
			}

			MemoryContextSwitchTo(old_ctx);

			SPI_connect();
			ret = SPI_execute(insert_buf.data, false, 0);
			SPI_finish();

			MemoryContextReset(tmp_ctx);

			if (ret != SPI_OK_INSERT)
				ereport(ERROR,
						(errmsg("failed to insert IVF centroids batch %d-%d",
								batch_start, batch_end - 1)));

			batch_start = batch_end;
		}
	}

	pfree(centroids);
	pfree(sample_data);
	MemoryContextDelete(tmp_ctx);

	/* Invalidate IVF cache */
	cached_ivf.cb_id = -1;

	ereport(NOTICE,
			(errmsg("IVF training complete: cb_id=%d, nlist=%d, dim=%d",
					cb_id, nlist, dim)));

	PG_RETURN_INT32(cb_id);
}

/* ----------------------------------------------------------------
 *  svec_ivf_assign: assign vector to nearest IVF centroid
 *
 *  svec_ivf_assign(vec svec, cb_id int4) → int2
 *
 *  Linear scan of all centroids, returns nearest centroid_id.
 *  Declared IMMUTABLE for use in GENERATED ALWAYS AS columns.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(svec_ivf_assign);
Datum
svec_ivf_assign(PG_FUNCTION_ARGS)
{
	Svec	   *vec = PG_GETARG_SVEC_P(0);
	int			cb_id = PG_GETARG_INT32(1);
	int			nlist, dim;
	float		best_dist = 1e30f;
	int			best_c = 0;
	int			c, j;

	ivf_load_centroids(cb_id);

	nlist = cached_ivf.nlist;
	dim = cached_ivf.dim;

	if (vec->dim != dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("vector dimension %d doesn't match IVF codebook "
						"(expected %d)", vec->dim, dim)));

	for (c = 0; c < nlist; c++)
	{
		float  *cent = cached_ivf.centroids + c * dim;
		float	dist = 0.0f;

		for (j = 0; j < dim; j++)
		{
			float diff = vec->x[j] - cent[j];
			dist += diff * diff;
		}

		if (dist < best_dist)
		{
			best_dist = dist;
			best_c = c;
		}
	}

	PG_RETURN_INT16((int16) best_c);
}

/* ----------------------------------------------------------------
 *  svec_ivf_probe: find nprobe nearest IVF centroids for a query
 *
 *  svec_ivf_probe(query svec, nprobe int4, cb_id int4 DEFAULT 1) → int2[]
 *
 *  Computes L2 distance to all centroids, returns top-nprobe as
 *  int2[] via partial selection sort.
 *  Declared STABLE (reads IVF tables), PARALLEL SAFE.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(svec_ivf_probe);
Datum
svec_ivf_probe(PG_FUNCTION_ARGS)
{
	Svec	   *query = PG_GETARG_SVEC_P(0);
	int			nprobe = PG_GETARG_INT32(1);
	int			cb_id = PG_GETARG_INT32(2);
	int			nlist, dim;
	float	   *dists;		/* distance to each centroid */
	int		   *ids;		/* centroid IDs for sorting */
	int			actual_nprobe;
	Datum	   *elems;
	ArrayType  *result;
	int			c, j, p;

	ivf_load_centroids(cb_id);

	nlist = cached_ivf.nlist;
	dim = cached_ivf.dim;

	if (query->dim != dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("query dimension %d doesn't match IVF codebook "
						"(expected %d)", query->dim, dim)));

	if (nprobe < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("nprobe must be at least 1")));

	actual_nprobe = (nprobe < nlist) ? nprobe : nlist;

	/* Compute distances to all centroids */
	dists = palloc(sizeof(float) * nlist);
	ids = palloc(sizeof(int) * nlist);

	for (c = 0; c < nlist; c++)
	{
		float  *cent = cached_ivf.centroids + c * dim;
		float	dist = 0.0f;

		for (j = 0; j < dim; j++)
		{
			float diff = query->x[j] - cent[j];
			dist += diff * diff;
		}

		dists[c] = dist;
		ids[c] = c;
	}

	/* Partial selection sort: find top actual_nprobe nearest */
	for (p = 0; p < actual_nprobe; p++)
	{
		int		min_idx = p;
		float	min_dist = dists[p];

		for (c = p + 1; c < nlist; c++)
		{
			if (dists[c] < min_dist)
			{
				min_dist = dists[c];
				min_idx = c;
			}
		}

		if (min_idx != p)
		{
			float	tmp_d;
			int		tmp_i;

			/* Swap distances */
			tmp_d = dists[p];
			dists[p] = dists[min_idx];
			dists[min_idx] = tmp_d;

			/* Swap IDs */
			tmp_i = ids[p];
			ids[p] = ids[min_idx];
			ids[min_idx] = tmp_i;
		}
	}

	/* Build int2[] result */
	elems = palloc(sizeof(Datum) * actual_nprobe);
	for (p = 0; p < actual_nprobe; p++)
		elems[p] = Int16GetDatum((int16) ids[p]);

	pfree(dists);
	pfree(ids);

	result = construct_array(elems, actual_nprobe, INT2OID,
							 sizeof(int16), true, TYPALIGN_SHORT);
	pfree(elems);

	PG_RETURN_ARRAYTYPE_P(result);
}

/* ----------------------------------------------------------------
 *  svec_pq_adc: combined ADC with auto-cached distance table
 *
 *  svec_pq_adc(query svec, code bytea, cb_id int4 DEFAULT 1) → float8
 *
 *  On first call per scan, computes the full M×256 distance table
 *  (like svec_pq_distance_table).  On every subsequent row, reuses
 *  the cached table and does O(M) lookup (like svec_pq_adc_lookup).
 *
 *  The distance table is stored in fcinfo->flinfo->fn_extra, which
 *  persists across rows within a single query scan.  Cache is
 *  validated by comparing (cb_id, dim, first 4 floats of query).
 *
 *  Eliminates the CTE pattern for most use cases.
 * ---------------------------------------------------------------- */
typedef struct PQADCState
{
	int			cb_id;
	int			M;
	int			dim;
	float		query_sig[4];	/* first 4 floats for cache validation */
	float		dist_table[FLEXIBLE_ARRAY_MEMBER];	/* M × 256 floats */
} PQADCState;

PG_FUNCTION_INFO_V1(svec_pq_adc);
Datum
svec_pq_adc(PG_FUNCTION_ARGS)
{
	Svec	   *query = PG_GETARG_SVEC_P(0);
	bytea	   *code = PG_GETARG_BYTEA_PP(1);
	int			cb_id = PG_GETARG_INT32(2);
	PQADCState *state;
	uint8	   *codes;
	int			M;
	int			code_len;
	double		total_dist;
	int			m;
	int			sig_len;
	bool		need_recompute;

	state = (PQADCState *) fcinfo->flinfo->fn_extra;
	sig_len = (query->dim < 4) ? query->dim : 4;
	need_recompute = false;

	if (state == NULL)
		need_recompute = true;
	else if (state->cb_id != cb_id || state->dim != query->dim)
		need_recompute = true;
	else if (memcmp(state->query_sig, query->x,
					sizeof(float) * sig_len) != 0)
		need_recompute = true;

	if (need_recompute)
	{
		MemoryContext old_ctx;
		int			dsub, c, j, dim;
		float	   *norm_q;

		pq_load_codebook(cb_id);

		M = cached_pq.M;
		dsub = cached_pq.dsub;
		dim = cached_pq.total_dim;

		if (query->dim != dim)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("query dimension %d doesn't match codebook "
							"(expected %d)",
							query->dim, dim)));

		/* L2-normalize query for cosine-equivalent ADC */
		norm_q = palloc(sizeof(float) * dim);
		memcpy(norm_q, query->x, sizeof(float) * dim);
		normalize_vector(norm_q, dim);

		/* Allocate or reallocate state in fn_mcxt */
		old_ctx = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
		if (state != NULL && state->M != M)
		{
			pfree(state);
			state = NULL;
		}
		if (state == NULL)
		{
			state = palloc(offsetof(PQADCState, dist_table) +
						   sizeof(float) * M * PQ_KSUB);
		}
		MemoryContextSwitchTo(old_ctx);

		state->cb_id = cb_id;
		state->M = M;
		state->dim = query->dim;
		memcpy(state->query_sig, query->x, sizeof(float) * sig_len);

		/* Compute distance table using normalized query */
		for (m = 0; m < M; m++)
		{
			float  *qsub = norm_q + m * dsub;
			float  *cb_base = cached_pq.centroids + m * PQ_KSUB * dsub;

			for (c = 0; c < PQ_KSUB; c++)
			{
				float  *cent = cb_base + c * dsub;
				float	dist = 0.0f;

				for (j = 0; j < dsub; j++)
				{
					float diff = qsub[j] - cent[j];
					dist += diff * diff;
				}

				state->dist_table[m * PQ_KSUB + c] = dist;
			}
		}

		pfree(norm_q);
		fcinfo->flinfo->fn_extra = state;
	}

	/* Lookup: O(M) per row */
	codes = (uint8 *) VARDATA_ANY(code);
	M = state->M;
	code_len = VARSIZE_ANY_EXHDR(code);

	if (code_len != M)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("PQ code length %d doesn't match M=%d",
						code_len, M)));

	total_dist = 0.0;
	for (m = 0; m < M; m++)
		total_dist += (double) state->dist_table[m * PQ_KSUB + codes[m]];

	PG_RETURN_FLOAT8(total_dist);
}

/* ================================================================
 *  svec_ann_scan: C-level IVF-PQ scan (maximum throughput)
 *
 *  svec_ann_scan(tbl regclass, query svec,
 *                nprobe int4 DEFAULT 10, lim int4 DEFAULT 10,
 *                rerank_topk int4 DEFAULT 0, cb_id int4 DEFAULT 1)
 *  RETURNS TABLE(id text, distance float8)
 *
 *  Performs full IVF-PQ search in a single C function:
 *    1. IVF probe: find nearest nprobe centroids (no SQL overhead)
 *    2. SPI fetch: get candidate rows from probed partitions
 *    3. ADC scan: tight C loop over PQ codes (no per-row fmgr)
 *    4. Top-K selection via max-heap
 *    5. Optional exact cosine reranking of top candidates
 *
 *  Eliminates per-row function call overhead (~1μs/row) that
 *  dominates the SQL-level svec_pq_adc approach.
 * ================================================================ */

/* --- Mixed-precision cosine distance (float32 query × float16 sketch) --- */

static float8
cosine_distance_f32_f16(const float *a, const half *b, int dim)
{
	double		dot = 0.0,
				norm_a = 0.0,
				norm_b = 0.0;
	double		similarity;
	int			i;

	for (i = 0; i < dim; i++)
	{
		double		ai = (double) a[i];
		double		bi = (double) HalfToFloat4(b[i]);

		dot += ai * bi;
		norm_a += ai * ai;
		norm_b += bi * bi;
	}

	if (norm_a == 0.0 || norm_b == 0.0)
		return get_float8_nan();

	similarity = dot / (sqrt(norm_a) * sqrt(norm_b));
	if (similarity > 1.0)
		similarity = 1.0;
	else if (similarity < -1.0)
		similarity = -1.0;

	return 1.0 - similarity;
}

/* --- Max-heap for top-K selection --- */

typedef struct
{
	float			dist;
	ItemPointerData	tid;
	int16			partition_id;
} TopKEntry;

static void
topk_siftdown(TopKEntry *heap, int n, int i)
{
	for (;;)
	{
		int		largest = i;
		int		left = 2 * i + 1;
		int		right = 2 * i + 2;
		TopKEntry tmp;

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

static void
topk_siftup(TopKEntry *heap, int i)
{
	while (i > 0)
	{
		int		parent = (i - 1) / 2;
		TopKEntry tmp;

		if (heap[i].dist <= heap[parent].dist)
			break;

		tmp = heap[i];
		heap[i] = heap[parent];
		heap[parent] = tmp;
		i = parent;
	}
}

static void
topk_insert(TopKEntry *heap, int *heap_size, int max_k,
			float dist, ItemPointerData *tid, int16 partition_id)
{
	if (*heap_size < max_k)
	{
		heap[*heap_size].dist = dist;
		ItemPointerCopy(tid, &heap[*heap_size].tid);
		heap[*heap_size].partition_id = partition_id;
		(*heap_size)++;
		topk_siftup(heap, *heap_size - 1);
	}
	else if (dist < heap[0].dist)
	{
		heap[0].dist = dist;
		ItemPointerCopy(tid, &heap[0].tid);
		heap[0].partition_id = partition_id;
		topk_siftdown(heap, *heap_size, 0);
	}
}

/* --- Result sorting --- */

typedef struct
{
	char		   *id;
	float8			distance;
	ItemPointerData	tid;
	int16			partition_id;
} AnnResult;

static int
cmp_ann_result(const void *a, const void *b)
{
	float8	da = ((const AnnResult *) a)->distance;
	float8	db = ((const AnnResult *) b)->distance;

	if (da < db) return -1;
	if (da > db) return 1;
	return 0;
}

static int
cmp_ann_result_by_blk(const void *a, const void *b)
{
	BlockNumber ba = ItemPointerGetBlockNumber(&((const AnnResult *) a)->tid);
	BlockNumber bb = ItemPointerGetBlockNumber(&((const AnnResult *) b)->tid);

	if (ba < bb) return -1;
	if (ba > bb) return 1;
	return 0;
}

#define RERANK_PREFETCH_DISTANCE	32

/* --- Main scan function --- */

PG_FUNCTION_INFO_V1(svec_ann_scan);
Datum
svec_ann_scan(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Oid			tbl_oid = PG_GETARG_OID(0);
	Svec	   *query = PG_GETARG_SVEC_P(1);
	int			nprobe = PG_GETARG_INT32(2);
	int			lim = PG_GETARG_INT32(3);
	int			rerank_topk = PG_GETARG_INT32(4);
	int			cb_id = PG_GETARG_INT32(5);
	int			ivf_id = PG_GETARG_INT32(6);
	char	   *pq_col = text_to_cstring(PG_GETARG_TEXT_PP(7));
	char	   *sketch_tbl_name = text_to_cstring(PG_GETARG_TEXT_PP(8));
	int			sketch_topk = PG_GETARG_INT32(9);
	bool		is_residual = (ivf_id > 0);
	int			dim, M, dsub, nlist;
	int			effective_topk, n_cand;
	int			sketch_out = 0;
	int64		n_early_term = 0, n_subtable_evals = 0;
	int			i, j, m, c, p;
	float	   *dist_table = NULL;
	float	   *per_centroid_dt = NULL;
	int		   *centroid_to_probe = NULL;
	float	   *centroid_dists;
	int		   *probe_ids;
	TopKEntry  *heap;
	int			heap_size;
	AnnResult  *results;
	instr_time	t_start, t_ivf, t_dt, t_scan, t_sketch, t_rerank, t_out;

	/* Setup materialized SRF (tuplestore) */
	if (sorted_heap_ann_timing)
		INSTR_TIME_SET_CURRENT(t_start);

	InitMaterializedSRF(fcinfo, 0);

	/* Load codebooks (cached in TopMemoryContext) */
	pq_load_codebook(cb_id);
	if (ivf_id <= 0)
		ivf_id = cb_id;
	ivf_load_centroids(ivf_id);

	M = cached_pq.M;
	dsub = cached_pq.dsub;
	dim = cached_pq.total_dim;
	nlist = cached_ivf.nlist;

	if (query->dim != dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("query dimension %d doesn't match codebook "
						"(expected %d)", query->dim, dim)));
	if (lim < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("svec_ann_scan: lim must be at least 1")));

	if (nprobe > nlist)
		nprobe = nlist;
	if (nprobe < 1)
		nprobe = 1;

	effective_topk = rerank_topk > 0 ? Max(rerank_topk, lim) : lim;

	/* ---- Step 1: Compute PQ distance table (raw PQ only) ---- */
	if (!is_residual)
	{
		float  *norm_q = palloc(sizeof(float) * dim);

		memcpy(norm_q, query->x, sizeof(float) * dim);
		normalize_vector(norm_q, dim);

		dist_table = palloc(sizeof(float) * M * PQ_KSUB);
		for (m = 0; m < M; m++)
		{
			float  *qsub = norm_q + m * dsub;
			float  *cb_base = cached_pq.centroids + m * PQ_KSUB * dsub;

			for (c = 0; c < PQ_KSUB; c++)
			{
				float  *cent = cb_base + c * dsub;
				float	d = 0.0f;

				for (j = 0; j < dsub; j++)
				{
					float diff = qsub[j] - cent[j];

					d += diff * diff;
				}
				dist_table[m * PQ_KSUB + c] = d;
			}
		}
		pfree(norm_q);
	}
	/* Residual PQ distance tables computed after IVF probe (Step 2b) */

	if (sorted_heap_ann_timing)
		INSTR_TIME_SET_CURRENT(t_dt);

	/* ---- Step 2: IVF probe (raw query, not normalized) ---- */
	centroid_dists = palloc(sizeof(float) * nlist);
	for (i = 0; i < nlist; i++)
	{
		float  *cent = cached_ivf.centroids + i * dim;
		float	d = 0.0f;

		for (j = 0; j < dim; j++)
		{
			float diff = query->x[j] - cent[j];

			d += diff * diff;
		}
		centroid_dists[i] = d;
	}

	probe_ids = palloc(sizeof(int) * nprobe);
	for (p = 0; p < nprobe; p++)
	{
		float	best_d = FLT_MAX;
		int		best_i = 0;

		for (i = 0; i < nlist; i++)
		{
			if (centroid_dists[i] < best_d)
			{
				best_d = centroid_dists[i];
				best_i = i;
			}
		}
		probe_ids[p] = best_i;
		centroid_dists[best_i] = FLT_MAX;	/* mark used */
	}
	pfree(centroid_dists);

	/* ---- Step 2b: Per-centroid distance tables (residual PQ) ---- */
	if (is_residual)
	{
		per_centroid_dt = palloc(sizeof(float) * nprobe * M * PQ_KSUB);
		centroid_to_probe = palloc(sizeof(int) * nlist);
		memset(centroid_to_probe, 0xFF, sizeof(int) * nlist);	/* -1 */

		for (p = 0; p < nprobe; p++)
		{
			float  *cent = cached_ivf.centroids + probe_ids[p] * dim;
			float  *dt = per_centroid_dt + (size_t) p * M * PQ_KSUB;
			float  *q_residual = palloc(sizeof(float) * dim);

			/* query_residual = query - centroid */
			for (j = 0; j < dim; j++)
				q_residual[j] = query->x[j] - cent[j];

			/* Build distance table from query residual to PQ centroids */
			for (m = 0; m < M; m++)
			{
				float  *qsub = q_residual + m * dsub;
				float  *cb_base = cached_pq.centroids + m * PQ_KSUB * dsub;

				for (c = 0; c < PQ_KSUB; c++)
				{
					float  *pqc = cb_base + c * dsub;
					float	d = 0.0f;

					for (j = 0; j < dsub; j++)
					{
						float diff = qsub[j] - pqc[j];

						d += diff * diff;
					}
					dt[m * PQ_KSUB + c] = d;
				}
			}

			pfree(q_residual);
			centroid_to_probe[probe_ids[p]] = p;
		}
	}

	if (sorted_heap_ann_timing)
		INSTR_TIME_SET_CURRENT(t_ivf);

	/* ---- Step 3+4: Direct index scan + ADC ---- */
	{
		Relation		rel;
		Relation		pk_index;
		Snapshot		snapshot;
		TupleDesc		td;
		TupleTableSlot *slot;
		AttrNumber		pq_attno;
		AttrNumber		id_attno;
		Oid				id_typid;
		Oid				typoutput;
		bool			typisvarlena;
		ScanKeyData		skey[1];
		Oid				partid_typid;
		RegProcedure	eq_proc;

		rel = table_open(tbl_oid, AccessShareLock);
		td = RelationGetDescr(rel);
		snapshot = GetActiveSnapshot();
		slot = table_slot_create(rel, NULL);

		/* Find PK index */
		if (!rel->rd_indexvalid)
			RelationGetIndexList(rel);
		if (!OidIsValid(rel->rd_pkindex))
			ereport(ERROR,
					(errmsg("svec_ann_scan: table has no primary key")));
		pk_index = index_open(rel->rd_pkindex, AccessShareLock);

		/* Resolve attribute numbers */
		pq_attno = get_attnum(tbl_oid, pq_col);
		id_attno = get_attnum(tbl_oid, "id");
		id_typid = TupleDescAttr(td, id_attno - 1)->atttypid;
		getTypeOutputInfo(id_typid, &typoutput, &typisvarlena);

		/* Build equality operator for partition_id (first PK column) */
		{
			AttrNumber	pk_first_heap_attno;
			Oid			opclass, opfamily, eq_opr;

			pk_first_heap_attno = pk_index->rd_index->indkey.values[0];
			partid_typid = TupleDescAttr(td, pk_first_heap_attno - 1)->atttypid;
			opclass = GetDefaultOpClass(partid_typid, BTREE_AM_OID);
			opfamily = get_opclass_family(opclass);
			eq_opr = get_opfamily_member(opfamily, partid_typid,
										 partid_typid,
										 BTEqualStrategyNumber);
			eq_proc = get_opcode(eq_opr);
		}

		heap = palloc(sizeof(TopKEntry) * effective_topk);
		heap_size = 0;
		n_cand = 0;

		/* Phase 3: hoist index_beginscan outside nprobe loop */
		{
			IndexScanDesc iscan;

#if PG_VERSION_NUM < 180000
			iscan = index_beginscan(rel, pk_index, snapshot, 1, 0);
#else
			iscan = index_beginscan(rel, pk_index, snapshot, NULL, 1, 0);
#endif

			/* Scan each probe partition via PK index */
			for (p = 0; p < nprobe; p++)
			{
				float  *dt;
				int	   *cur_perm = cached_pq.perm;

				/* Select per-centroid distance table */
				if (is_residual)
					dt = per_centroid_dt + (size_t) p * M * PQ_KSUB;
				else
					dt = dist_table;

				ScanKeyInit(&skey[0],
							1,		/* first index column = partition_id */
							BTEqualStrategyNumber,
							eq_proc,
							Int16GetDatum(probe_ids[p]));
				index_rescan(iscan, skey, 1, NULL, 0);

				while (index_getnext_slot(iscan, ForwardScanDirection, slot))
				{
					bool	isnull;
					Datum	code_d;
					bytea  *code;
					uint8  *codes;
					int		code_len;
					float	dist;
					float	threshold;

					n_cand++;

					code_d = slot_getattr(slot, pq_attno, &isnull);
					if (isnull)
						continue;

					code = DatumGetByteaPP(code_d);
					codes = (uint8 *) VARDATA_ANY(code);
					code_len = VARSIZE_ANY_EXHDR(code);
					if (code_len != M)
						continue;

					/* Phase 1: ADC with early termination + reordering */
					threshold = (heap_size < effective_topk)
						? FLT_MAX : heap[0].dist;
					dist = 0.0f;
					for (m = 0; m < M; m++)
					{
						int		pm = cur_perm[m];

						dist += dt[pm * PQ_KSUB + codes[pm]];

						if ((m & (PQ_EARLY_TERM_STRIDE - 1))
							== (PQ_EARLY_TERM_STRIDE - 1)
							&& dist >= threshold)
						{
							n_early_term++;
							n_subtable_evals += (m + 1);
							goto next_row;
						}
					}
					n_subtable_evals += M;

					/* Phase 2: skip TID copy for rejected rows */
					if (heap_size >= effective_topk
						&& dist >= heap[0].dist)
						continue;

					{
						ItemPointerData row_tid;

						ItemPointerCopy(&slot->tts_tid, &row_tid);
						topk_insert(heap, &heap_size, effective_topk,
									dist, &row_tid, probe_ids[p]);
					}

			next_row:
					;	/* early-terminated row */
				}
			}

			index_endscan(iscan);
		}

		index_close(pk_index, AccessShareLock);

		if (sorted_heap_ann_timing)
		{
			INSTR_TIME_SET_CURRENT(t_scan);
			t_sketch = t_scan;	/* default: zero delta when sketch unused */
		}

		/* ---- Step 5: Copy top-K into results array ---- */
		results = palloc(sizeof(AnnResult) * heap_size);
		for (j = 0; j < heap_size; j++)
		{
			ItemPointerCopy(&heap[j].tid, &results[j].tid);
			results[j].id = NULL;
			results[j].distance = (float8) heap[j].dist;
			results[j].partition_id = heap[j].partition_id;
		}

		/* ---- Step 6a: Sketch rerank via sidecar table ---- */
		if (sketch_tbl_name[0] != '\0' && sketch_topk > 0
			&& rerank_topk > 0 && heap_size > 0)
		{
			Oid				sketch_tbl_oid;
			Relation		sketch_rel;
			Relation		sketch_pk;
			TupleTableSlot *sketch_slot;
			TupleDesc		sketch_td;
			AttrNumber		sketch_attno;
			IndexScanDesc	sk_iscan;
			ScanKeyData		sk_skey[2];
			RegProcedure	sk_eq_part, sk_eq_id;
			Datum		   *id_datums;
			int				sketch_effective;

			sketch_effective = Max(sketch_topk, lim);

			/* Resolve sidecar table */
			sketch_tbl_oid = DatumGetObjectId(
				DirectFunctionCall1(regclassin,
									CStringGetDatum(sketch_tbl_name)));
			sketch_rel = table_open(sketch_tbl_oid, AccessShareLock);
			sketch_td = RelationGetDescr(sketch_rel);
			sketch_slot = table_slot_create(sketch_rel, NULL);
			sketch_attno = get_attnum(sketch_tbl_oid, "sketch");

			if (sketch_attno == InvalidAttrNumber)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("svec_ann_scan: sidecar table \"%s\" "
								"has no \"sketch\" column",
								sketch_tbl_name)));

			/* Find sidecar PK and build equality operators */
			if (!sketch_rel->rd_indexvalid)
				RelationGetIndexList(sketch_rel);
			if (!OidIsValid(sketch_rel->rd_pkindex))
				ereport(ERROR,
						(errmsg("svec_ann_scan: sidecar table \"%s\" "
								"has no primary key", sketch_tbl_name)));
			sketch_pk = index_open(sketch_rel->rd_pkindex, AccessShareLock);

			/* PK column 1: partition_id */
			{
				AttrNumber	att = sketch_pk->rd_index->indkey.values[0];
				Oid			t = TupleDescAttr(sketch_td, att - 1)->atttypid;
				Oid			oc = GetDefaultOpClass(t, BTREE_AM_OID);
				Oid			of = get_opclass_family(oc);

				sk_eq_part = get_opcode(
					get_opfamily_member(of, t, t, BTEqualStrategyNumber));
			}

			/* PK column 2: id */
			{
				AttrNumber	att = sketch_pk->rd_index->indkey.values[1];
				Oid			t = TupleDescAttr(sketch_td, att - 1)->atttypid;
				Oid			oc = GetDefaultOpClass(t, BTREE_AM_OID);
				Oid			of = get_opclass_family(oc);

				sk_eq_id = get_opcode(
					get_opfamily_member(of, t, t, BTEqualStrategyNumber));
			}

			/* Phase A: fetch id from main table for all candidates */
			id_datums = palloc(sizeof(Datum) * heap_size);

			qsort(results, heap_size, sizeof(AnnResult),
				  cmp_ann_result_by_blk);

			for (j = 0; j < Min(RERANK_PREFETCH_DISTANCE, heap_size); j++)
				PrefetchBuffer(rel, MAIN_FORKNUM,
							   ItemPointerGetBlockNumber(&results[j].tid));

			for (j = 0; j < heap_size; j++)
			{
				bool	isnull;
				Datum	id_d;

				if (j + RERANK_PREFETCH_DISTANCE < heap_size)
					PrefetchBuffer(rel, MAIN_FORKNUM,
								   ItemPointerGetBlockNumber(
									   &results[j + RERANK_PREFETCH_DISTANCE].tid));

				if (!table_tuple_fetch_row_version(rel, &results[j].tid,
												   snapshot, slot))
				{
					results[j].distance = DBL_MAX;
					id_datums[j] = (Datum) 0;
					continue;
				}

				/* Read id (inline, no TOAST decompression) */
				id_d = slot_getattr(slot, id_attno, &isnull);
				if (isnull)
				{
					results[j].distance = DBL_MAX;
					id_datums[j] = (Datum) 0;
				}
				else
				{
					/*
					 * Copy the datum so it survives ExecClearTuple.  For
					 * pass-by-reference types (text, varchar, etc.) we must
					 * datumCopy; for pass-by-value (int4, int8) the Datum
					 * itself is the value.
					 */
					id_datums[j] = datumCopy(id_d,
											 TupleDescAttr(td, id_attno - 1)->attbyval,
											 TupleDescAttr(td, id_attno - 1)->attlen);
				}
				ExecClearTuple(slot);
			}

			/* Phase B: look up sidecar, compute sketch distance */
#if PG_VERSION_NUM < 180000
			sk_iscan = index_beginscan(sketch_rel, sketch_pk,
									   snapshot, 2, 0);
#else
			sk_iscan = index_beginscan(sketch_rel, sketch_pk,
									   snapshot, NULL, 2, 0);
#endif

			for (j = 0; j < heap_size; j++)
			{
				bool	isnull;
				Datum	sketch_d;
				Hsvec  *sk;

				if (results[j].distance == DBL_MAX)
					continue;

				ScanKeyInit(&sk_skey[0], 1, BTEqualStrategyNumber,
							sk_eq_part,
							Int16GetDatum(results[j].partition_id));
				ScanKeyInit(&sk_skey[1], 2, BTEqualStrategyNumber,
							sk_eq_id, id_datums[j]);

				index_rescan(sk_iscan, sk_skey, 2, NULL, 0);

				if (index_getnext_slot(sk_iscan, ForwardScanDirection,
									   sketch_slot))
				{
					sketch_d = slot_getattr(sketch_slot, sketch_attno,
											&isnull);
					if (!isnull)
					{
						sk = (Hsvec *) PG_DETOAST_DATUM(sketch_d);
						results[j].distance =
							cosine_distance_f32_f16(query->x,
													sk->x, sk->dim);
					}
					ExecClearTuple(sketch_slot);
				}
				else
					results[j].distance = DBL_MAX;
			}

			index_endscan(sk_iscan);

			/* Sort by sketch distance, keep top sketch_effective */
			qsort(results, heap_size, sizeof(AnnResult), cmp_ann_result);
			if (heap_size > sketch_effective)
				heap_size = sketch_effective;
			sketch_out = heap_size;

			pfree(id_datums);
			ExecDropSingleTupleTableSlot(sketch_slot);
			index_close(sketch_pk, AccessShareLock);
			table_close(sketch_rel, AccessShareLock);

			if (sorted_heap_ann_timing)
				INSTR_TIME_SET_CURRENT(t_sketch);
		}

		/* ---- Step 6b: C-level rerank + id extraction via table AM ---- */
		if (rerank_topk > 0 && heap_size > 0)
		{
			AttrNumber	emb_attno = get_attnum(tbl_oid, "embedding");

			/* Sort by block number for sequential I/O */
			qsort(results, heap_size, sizeof(AnnResult),
				  cmp_ann_result_by_blk);

			/* Prefetch initial batch of heap pages */
			for (j = 0; j < Min(RERANK_PREFETCH_DISTANCE, heap_size); j++)
				PrefetchBuffer(rel, MAIN_FORKNUM,
							   ItemPointerGetBlockNumber(&results[j].tid));

			/* Rerank: fetch embedding only, compute exact distance */
			for (j = 0; j < heap_size; j++)
			{
				bool	isnull;
				Datum	emb_d;
				Svec   *candidate;

				/* Prefetch ahead */
				if (j + RERANK_PREFETCH_DISTANCE < heap_size)
					PrefetchBuffer(rel, MAIN_FORKNUM,
								   ItemPointerGetBlockNumber(
									   &results[j + RERANK_PREFETCH_DISTANCE].tid));

				if (!table_tuple_fetch_row_version(rel, &results[j].tid,
												   snapshot, slot))
				{
					results[j].distance = DBL_MAX;
					continue;
				}

				emb_d = slot_getattr(slot, emb_attno, &isnull);
				if (!isnull)
				{
					candidate = DatumGetSvecP(emb_d);
					if (candidate->dim != query->dim)
						ereport(ERROR,
								(errcode(ERRCODE_DATA_EXCEPTION),
								 errmsg("svec_ann_scan rerank: embedding "
										"dimension %d doesn't match query "
										"(%d)", candidate->dim, query->dim)));
					results[j].distance =
						svec_cosine_distance_internal(query, candidate);
				}

				ExecClearTuple(slot);
			}

			/* Sort by exact distance, keep top-lim */
			qsort(results, heap_size, sizeof(AnnResult), cmp_ann_result);
			if (heap_size > lim)
				heap_size = lim;
		}
		else
		{
			/* No rerank: sort by ADC distance, keep top-lim */
			qsort(results, heap_size, sizeof(AnnResult), cmp_ann_result);
			if (heap_size > lim)
				heap_size = lim;
		}

		/* Fetch id for final results only */
		for (j = 0; j < heap_size; j++)
		{
			bool	isnull;
			Datum	id_d;

			if (!table_tuple_fetch_row_version(rel, &results[j].tid,
											   snapshot, slot))
			{
				results[j].id = pstrdup("");
				continue;
			}

			id_d = slot_getattr(slot, id_attno, &isnull);
			results[j].id = isnull ? pstrdup("")
				: OidOutputFunctionCall(typoutput, id_d);

			ExecClearTuple(slot);
		}

		ExecDropSingleTupleTableSlot(slot);
		table_close(rel, AccessShareLock);

		if (sorted_heap_ann_timing)
			INSTR_TIME_SET_CURRENT(t_rerank);
	}

	/* ---- Step 7: Write to tuplestore ---- */
	for (j = 0; j < heap_size; j++)
	{
		Datum	values[2];
		bool	nulls[2] = {false, false};

		values[0] = CStringGetTextDatum(results[j].id ? results[j].id : "");
		values[1] = Float8GetDatum(results[j].distance);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							values, nulls);
	}

	pfree(results);
	pfree(heap);
	if (dist_table)
		pfree(dist_table);
	if (per_centroid_dt)
		pfree(per_centroid_dt);
	if (centroid_to_probe)
		pfree(centroid_to_probe);
	pfree(probe_ids);

	if (sorted_heap_ann_timing)
	{
		INSTR_TIME_SET_CURRENT(t_out);
		elog(DEBUG1, "svec_ann_scan: dt=%.3fms ivf=%.3fms "
			 "scan=%.3fms sketch=%.3fms rerank=%.3fms "
			 "out=%.3fms total=%.3fms "
			 "(cand=%d topk=%d sketch_out=%d "
			 "early_term=%ld/%d avg_m=%.0f)",
			 INSTR_TIME_GET_MILLISEC(t_dt) - INSTR_TIME_GET_MILLISEC(t_start),
			 INSTR_TIME_GET_MILLISEC(t_ivf) - INSTR_TIME_GET_MILLISEC(t_dt),
			 INSTR_TIME_GET_MILLISEC(t_scan) - INSTR_TIME_GET_MILLISEC(t_ivf),
			 INSTR_TIME_GET_MILLISEC(t_sketch) - INSTR_TIME_GET_MILLISEC(t_scan),
			 INSTR_TIME_GET_MILLISEC(t_rerank) - INSTR_TIME_GET_MILLISEC(t_sketch),
			 INSTR_TIME_GET_MILLISEC(t_out) - INSTR_TIME_GET_MILLISEC(t_rerank),
			 INSTR_TIME_GET_MILLISEC(t_out) - INSTR_TIME_GET_MILLISEC(t_start),
			 n_cand, heap_size, sketch_out,
			 (long) n_early_term, n_cand,
			 n_cand > 0
			 ? (double) n_subtable_evals / n_cand : 0.0);
	}

	return (Datum) 0;
}
