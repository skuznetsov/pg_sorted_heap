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

#include "fmgr.h"
#include "funcapi.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#include <math.h>
#include <string.h>

#include "svec.h"
#include "pq.h"

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
} PQCodebookCache;

static PQCodebookCache cached_pq = {-1, 0, 0, 0, NULL};

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
		cached_pq.cb_id = -1;
	}

	/* Read codebook metadata first */
	snprintf(sql, sizeof(sql),
			 "SELECT m, dsub FROM _pq_codebook_meta WHERE cb_id = %d",
			 cb_id);

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
			 "FROM _pq_codebooks WHERE cb_id = %d "
			 "ORDER BY sub_id, cent_id",
			 cb_id);

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
}

/* ----------------------------------------------------------------
 *  K-means on a set of dsub-dimensional vectors
 * ---------------------------------------------------------------- */
static void
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
 *  Fisher-Yates shuffle for sample selection
 * ---------------------------------------------------------------- */
static void
shuffle_indices(int *arr, int n, uint64 seed)
{
	int		i;
	uint64	state = seed;

	for (i = n - 1; i > 0; i--)
	{
		/* Simple splitmix64-based random */
		uint64	z = (state += 0x9e3779b97f4a7c15ULL);
		int		j;

		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
		z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
		z ^= (z >> 31);
		j = (int) (z % ((uint64) i + 1));

		/* Swap */
		{
			int tmp = arr[i];
			arr[i] = arr[j];
			arr[j] = tmp;
		}
	}
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
	int			ret;
	int			total_rows;
	int			n_samples;
	int			dim, dsub;
	float	   *sample_data;	/* n_samples × dim */
	float	   *sub_data;		/* n_samples × dsub for one subvector */
	float	   *centroids;		/* 256 × dsub for one subvector */
	int		   *indices;
	int			cb_id;
	int			m, i, j;
	char		sql[256];
	MemoryContext func_ctx;
	MemoryContext tmp_ctx;
	MemoryContext old_ctx;

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

	/* Execute source query to get training data */
	SPI_connect();
	ret = SPI_execute(query_str, true, 0);

	if (ret != SPI_OK_SELECT || SPI_processed == 0)
	{
		SPI_finish();
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 errmsg("source query returned no rows")));
	}

	total_rows = (int) SPI_processed;

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

	n_samples = (total_rows < max_samples) ? total_rows : max_samples;

	ereport(NOTICE,
			(errmsg("PQ training: dim=%d, M=%d, dsub=%d, Ksub=%d, "
					"n_iter=%d, data=%d rows, samples=%d",
					dim, M, dsub, PQ_KSUB, n_iter, total_rows, n_samples)));

	/*
	 * Allocate indices and sample_data in func_ctx, NOT in SPI's procCxt.
	 * SPI_finish() deletes procCxt, so anything allocated there becomes
	 * a dangling pointer — the root cause of the previous
	 * "could not find block containing chunk" crash.
	 */
	old_ctx = MemoryContextSwitchTo(func_ctx);
	indices = palloc(sizeof(int) * total_rows);
	sample_data = palloc(sizeof(float) * n_samples * dim);
	MemoryContextSwitchTo(old_ctx);

	for (i = 0; i < total_rows; i++)
		indices[i] = i;
	shuffle_indices(indices, total_rows, 42);

	/*
	 * Copy sampled vectors into contiguous float array.
	 * Detoast each vector and pfree immediately to keep memory bounded.
	 */
	for (i = 0; i < n_samples; i++)
	{
		bool	isnull;
		Datum	d;
		Svec   *vec;

		d = SPI_getbinval(SPI_tuptable->vals[indices[i]],
						  SPI_tuptable->tupdesc, 1, &isnull);

		if (isnull)
		{
			SPI_finish();
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("NULL vector in training data at row %d",
							indices[i])));
		}

		vec = (Svec *) PG_DETOAST_DATUM(d);

		if (vec->dim != dim)
		{
			SPI_finish();
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("inconsistent vector dim at row %d: "
							"expected %d, got %d",
							indices[i], dim, vec->dim)));
		}

		memcpy(sample_data + i * dim, vec->x, sizeof(float) * dim);

		/* Free detoasted copy to prevent memory growth */
		if (DatumGetPointer(d) != (Pointer) vec)
			pfree(vec);
	}

	SPI_finish();
	pfree(indices);

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

	/* Create meta table if not exists */
	ret = SPI_execute(
		"CREATE TABLE IF NOT EXISTS _pq_codebook_meta ("
		"  cb_id int PRIMARY KEY,"
		"  m int NOT NULL,"
		"  dsub int NOT NULL,"
		"  total_dim int NOT NULL"
		")", false, 0);

	if (ret != SPI_OK_UTILITY)
	{
		SPI_finish();
		ereport(ERROR, (errmsg("failed to create _pq_codebook_meta")));
	}

	/* Create codebook table if not exists */
	ret = SPI_execute(
		"CREATE TABLE IF NOT EXISTS _pq_codebooks ("
		"  cb_id int NOT NULL,"
		"  sub_id int NOT NULL,"
		"  cent_id int NOT NULL,"
		"  centroid svec NOT NULL,"
		"  PRIMARY KEY (cb_id, sub_id, cent_id)"
		")", false, 0);

	if (ret != SPI_OK_UTILITY)
	{
		SPI_finish();
		ereport(ERROR, (errmsg("failed to create _pq_codebooks")));
	}

	/* Get next cb_id */
	ret = SPI_execute(
		"SELECT COALESCE(MAX(cb_id), 0) + 1 FROM _pq_codebook_meta",
		true, 1);

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
			 "INSERT INTO _pq_codebook_meta (cb_id, m, dsub, total_dim) "
			 "VALUES (%d, %d, %d, %d)",
			 cb_id, M, dsub, dim);
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
						 "INSERT INTO _pq_codebooks "
						 "(cb_id, sub_id, cent_id, centroid) VALUES ");

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
			appendStringInfo(&insert_buf, "]'::svec)");
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
	int			M, dsub;
	bytea	   *result;
	uint8	   *codes;
	int			m, j, c;

	pq_load_codebook(cb_id);

	M = cached_pq.M;
	dsub = cached_pq.dsub;

	if (vec->dim != cached_pq.total_dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("vector dimension %d doesn't match codebook "
						"(expected %d)", vec->dim, cached_pq.total_dim)));

	result = (bytea *) palloc(VARHDRSZ + M);
	SET_VARSIZE(result, VARHDRSZ + M);
	codes = (uint8 *) VARDATA(result);

	for (m = 0; m < M; m++)
	{
		float  *sub = vec->x + m * dsub;
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

	for (m = 0; m < M; m++)
	{
		float  *qsub = query->x + m * dsub;
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

	for (m = 0; m < M; m++)
	{
		float  *qsub = query->x + m * dsub;
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
