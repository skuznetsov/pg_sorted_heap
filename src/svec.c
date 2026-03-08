/*
 * svec.c
 *
 * Sorted Vector (svec): a lightweight float32 vector type for pg_sorted_heap.
 * Text I/O: [x1,x2,...,xn]  (identical to pgvector format)
 * Binary I/O: int16 dim + dim × float32
 * Cosine distance operator: <=>
 */
#include "postgres.h"

#include "common/shortest_dec.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/float.h"

#include <math.h>

#if defined(__aarch64__) && defined(__ARM_NEON)
#include <arm_neon.h>
#endif

#include "svec.h"

/* ----------------------------------------------------------------
 *  Text input: '[1.0,2.0,3.0]' → Svec
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(svec_in);
Datum
svec_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
	int32		typmod = PG_GETARG_INT32(2);
	float	   *values;
	int			dim = 0;
	int			capacity = 16;
	char	   *p;
	Svec	   *result;

	values = palloc(sizeof(float) * capacity);

	/* Skip leading whitespace */
	p = str;
	while (*p == ' ' || *p == '\t')
		p++;

	if (*p != '[')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("svec must start with \"[\"")));
	p++;

	/* Parse comma-separated floats */
	while (*p != '\0' && *p != ']')
	{
		char	   *end;
		float		val;

		/* Skip whitespace */
		while (*p == ' ' || *p == '\t')
			p++;

		if (*p == ']')
			break;

		val = strtof(p, &end);
		if (end == p)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type svec: \"%s\"", str)));
		p = end;

		if (dim >= capacity)
		{
			capacity *= 2;
			values = repalloc(values, sizeof(float) * capacity);
		}
		values[dim++] = val;

		/* Skip whitespace + comma */
		while (*p == ' ' || *p == '\t')
			p++;
		if (*p == ',')
			p++;
	}

	if (*p != ']')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("svec must end with \"]\"")));
	p++;

	/* Trailing garbage check */
	while (*p == ' ' || *p == '\t')
		p++;
	if (*p != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("unexpected characters after \"]\" in svec input")));

	if (dim < 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("svec must have at least 1 dimension")));

	if (dim > SVEC_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("svec cannot have more than %d dimensions", SVEC_MAX_DIM)));

	/* Check typmod */
	if (typmod != -1 && typmod != dim)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("expected %d dimensions, not %d", typmod, dim)));

	result = (Svec *) palloc0(SVEC_SIZE(dim));
	SET_VARSIZE(result, SVEC_SIZE(dim));
	result->dim = dim;
	result->unused = 0;
	memcpy(result->x, values, sizeof(float) * dim);

	pfree(values);

	PG_RETURN_SVEC_P(result);
}

/* ----------------------------------------------------------------
 *  Text output: Svec → '[1,2,3]'
 *
 *  Uses Ryu (float_to_shortest_decimal_bufn) for fast float-to-string
 *  conversion — same approach as pgvector.  Pre-allocates the output
 *  buffer to avoid StringInfo realloc overhead.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(svec_out);
Datum
svec_out(PG_FUNCTION_ARGS)
{
	Svec	   *vec = PG_GETARG_SVEC_P(0);
	int			dim = vec->dim;
	char	   *buf;
	char	   *ptr;
	int			i;

	/*
	 * FLOAT_SHORTEST_DECIMAL_LEN (16) covers the longest float output
	 * including NaN/Inf/sign/exponent plus NUL.  We need:
	 *   dim * (FLOAT_SHORTEST_DECIMAL_LEN - 1) for values
	 *   dim - 1 for commas
	 *   3 for '[', ']', '\0'
	 */
	buf = (char *) palloc(FLOAT_SHORTEST_DECIMAL_LEN * dim + 2);
	ptr = buf;

	*ptr++ = '[';

	for (i = 0; i < dim; i++)
	{
		if (i > 0)
			*ptr++ = ',';
		ptr += float_to_shortest_decimal_bufn(vec->x[i], ptr);
	}

	*ptr++ = ']';
	*ptr = '\0';

	PG_RETURN_CSTRING(buf);
}

/* ----------------------------------------------------------------
 *  Typmod input: svec(768) → typmod = 768
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(svec_typmod_in);
Datum
svec_typmod_in(PG_FUNCTION_ARGS)
{
	ArrayType  *ta = PG_GETARG_ARRAYTYPE_P(0);
	int			n;
	int32	   *dims;

	dims = ArrayGetIntegerTypmods(ta, &n);

	if (n != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("svec type modifier must have exactly one dimension")));

	if (dims[0] < 1 || dims[0] > SVEC_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("svec dimensions must be between 1 and %d", SVEC_MAX_DIM)));

	PG_RETURN_INT32(dims[0]);
}

/* ----------------------------------------------------------------
 *  Binary receive
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(svec_recv);
Datum
svec_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	int32		typmod = PG_GETARG_INT32(2);
	int16		dim;
	int16		unused;
	Svec	   *result;
	int			i;

	dim = pq_getmsgint(buf, sizeof(int16));
	unused = pq_getmsgint(buf, sizeof(int16));

	(void) unused;	/* suppress warning */

	if (dim < 1 || dim > SVEC_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("svec dimensions out of range: %d", dim)));

	if (typmod != -1 && typmod != dim)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("expected %d dimensions, not %d", typmod, dim)));

	result = (Svec *) palloc0(SVEC_SIZE(dim));
	SET_VARSIZE(result, SVEC_SIZE(dim));
	result->dim = dim;
	result->unused = 0;

	for (i = 0; i < dim; i++)
		result->x[i] = pq_getmsgfloat4(buf);

	PG_RETURN_SVEC_P(result);
}

/* ----------------------------------------------------------------
 *  Binary send
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(svec_send);
Datum
svec_send(PG_FUNCTION_ARGS)
{
	Svec	   *vec = PG_GETARG_SVEC_P(0);
	StringInfoData buf;
	int			i;

	pq_begintypsend(&buf);
	pq_sendint16(&buf, vec->dim);
	pq_sendint16(&buf, vec->unused);

	for (i = 0; i < vec->dim; i++)
		pq_sendfloat4(&buf, vec->x[i]);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/* ----------------------------------------------------------------
 *  Cosine distance: 1 - dot(a,b) / (|a| * |b|)
 *
 *  Internal helper (callable from C without fmgr overhead) +
 *  SQL-callable wrapper.
 * ---------------------------------------------------------------- */
float8
svec_cosine_distance_internal(const Svec *a, const Svec *b)
{
	int			dim = a->dim;
	double		dot;
	double		norm_a;
	double		norm_b;
	double		similarity;
	int			i;

#if defined(__aarch64__) && defined(__ARM_NEON)
	{
		float32x4_t vdot = vdupq_n_f32(0.0f);
		float32x4_t vna = vdupq_n_f32(0.0f);
		float32x4_t vnb = vdupq_n_f32(0.0f);

		for (i = 0; i + 3 < dim; i += 4)
		{
			float32x4_t va = vld1q_f32(&a->x[i]);
			float32x4_t vb = vld1q_f32(&b->x[i]);

			vdot = vfmaq_f32(vdot, va, vb);
			vna = vfmaq_f32(vna, va, va);
			vnb = vfmaq_f32(vnb, vb, vb);
		}

		dot = (double) vaddvq_f32(vdot);
		norm_a = (double) vaddvq_f32(vna);
		norm_b = (double) vaddvq_f32(vnb);

		/* Scalar tail for non-multiple-of-4 */
		for (; i < dim; i++)
		{
			double	ai = (double) a->x[i];
			double	bi = (double) b->x[i];

			dot += ai * bi;
			norm_a += ai * ai;
			norm_b += bi * bi;
		}
	}
#else
	dot = 0.0;
	norm_a = 0.0;
	norm_b = 0.0;

	for (i = 0; i < dim; i++)
	{
		double		ai = (double) a->x[i];
		double		bi = (double) b->x[i];

		dot += ai * bi;
		norm_a += ai * ai;
		norm_b += bi * bi;
	}
#endif

	/* Handle zero vectors */
	if (norm_a == 0.0 || norm_b == 0.0)
		return get_float8_nan();

	similarity = dot / (sqrt(norm_a) * sqrt(norm_b));

	/* Clamp to [-1, 1] for numerical safety */
	if (similarity > 1.0)
		similarity = 1.0;
	else if (similarity < -1.0)
		similarity = -1.0;

	return 1.0 - similarity;
}

PG_FUNCTION_INFO_V1(svec_cosine_distance);
Datum
svec_cosine_distance(PG_FUNCTION_ARGS)
{
	Svec	   *a = PG_GETARG_SVEC_P(0);
	Svec	   *b = PG_GETARG_SVEC_P(1);

	if (a->dim != b->dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("svec dimensions must match: %d vs %d", a->dim, b->dim)));

	PG_RETURN_FLOAT8(svec_cosine_distance_internal(a, b));
}
