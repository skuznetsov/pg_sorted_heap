/*
 * hsvec.c
 *
 * Half-precision Sorted Vector (hsvec): a float16 vector type for pg_sorted_heap.
 * 2 bytes per dimension (vs 4 for svec), max 32000 dimensions.
 * Text I/O: [x1,x2,...,xn]  (identical to svec format)
 * Binary I/O: int16 dim + dim × uint16 (IEEE 754 binary16)
 * Cosine distance operator: <=>
 * Casts: hsvec → svec (implicit, lossless), svec → hsvec (assignment, lossy)
 */
#include "postgres.h"

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/float.h"

#include <math.h>

#include "hsvec.h"
#include "svec.h"

/* ----------------------------------------------------------------
 *  Text input: '[1.0,2.0,3.0]' → Hsvec
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(hsvec_in);
Datum
hsvec_in(PG_FUNCTION_ARGS)
{
	char	   *str = PG_GETARG_CSTRING(0);
	int32		typmod = PG_GETARG_INT32(2);
	half	   *values;
	int			dim = 0;
	int			capacity = 16;
	char	   *p;
	Hsvec	   *result;

	values = palloc(sizeof(half) * capacity);

	/* Skip leading whitespace */
	p = str;
	while (*p == ' ' || *p == '\t')
		p++;

	if (*p != '[')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("hsvec must start with \"[\"")));
	p++;

	/* Parse comma-separated floats, convert to half */
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
					 errmsg("invalid input syntax for type hsvec: \"%s\"", str)));
		p = end;

		if (dim >= capacity)
		{
			capacity *= 2;
			values = repalloc(values, sizeof(half) * capacity);
		}
		values[dim++] = Float4ToHalf(val);

		/* Skip whitespace + comma */
		while (*p == ' ' || *p == '\t')
			p++;
		if (*p == ',')
			p++;
	}

	if (*p != ']')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("hsvec must end with \"]\"")));
	p++;

	/* Trailing garbage check */
	while (*p == ' ' || *p == '\t')
		p++;
	if (*p != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("unexpected characters after \"]\" in hsvec input")));

	if (dim < 1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("hsvec must have at least 1 dimension")));

	if (dim > HSVEC_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("hsvec cannot have more than %d dimensions", HSVEC_MAX_DIM)));

	/* Check typmod */
	if (typmod != -1 && typmod != dim)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("expected %d dimensions, not %d", typmod, dim)));

	result = (Hsvec *) palloc0(HSVEC_SIZE(dim));
	SET_VARSIZE(result, HSVEC_SIZE(dim));
	result->dim = dim;
	result->unused = 0;
	memcpy(result->x, values, sizeof(half) * dim);

	pfree(values);

	PG_RETURN_HSVEC_P(result);
}

/* ----------------------------------------------------------------
 *  Text output: Hsvec → '[1,2,3]'
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(hsvec_out);
Datum
hsvec_out(PG_FUNCTION_ARGS)
{
	Hsvec	   *vec = PG_GETARG_HSVEC_P(0);
	int			dim = vec->dim;
	StringInfoData buf;
	int			i;

	initStringInfo(&buf);
	appendStringInfoChar(&buf, '[');

	for (i = 0; i < dim; i++)
	{
		if (i > 0)
			appendStringInfoChar(&buf, ',');
		appendStringInfo(&buf, "%g", (double) HalfToFloat4(vec->x[i]));
	}

	appendStringInfoChar(&buf, ']');

	PG_RETURN_CSTRING(buf.data);
}

/* ----------------------------------------------------------------
 *  Typmod input: hsvec(768) → typmod = 768
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(hsvec_typmod_in);
Datum
hsvec_typmod_in(PG_FUNCTION_ARGS)
{
	ArrayType  *ta = PG_GETARG_ARRAYTYPE_P(0);
	int			n;
	int32	   *dims;

	dims = ArrayGetIntegerTypmods(ta, &n);

	if (n != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("hsvec type modifier must have exactly one dimension")));

	if (dims[0] < 1 || dims[0] > HSVEC_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("hsvec dimensions must be between 1 and %d", HSVEC_MAX_DIM)));

	PG_RETURN_INT32(dims[0]);
}

/* ----------------------------------------------------------------
 *  Binary receive: int16 dim + dim × uint16 (IEEE 754 binary16)
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(hsvec_recv);
Datum
hsvec_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	int32		typmod = PG_GETARG_INT32(2);
	int16		dim;
	int16		unused;
	Hsvec	   *result;
	int			i;

	dim = pq_getmsgint(buf, sizeof(int16));
	unused = pq_getmsgint(buf, sizeof(int16));

	(void) unused;				/* suppress warning */

	if (dim < 1 || dim > HSVEC_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("hsvec dimensions out of range: %d", dim)));

	if (typmod != -1 && typmod != dim)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("expected %d dimensions, not %d", typmod, dim)));

	result = (Hsvec *) palloc0(HSVEC_SIZE(dim));
	SET_VARSIZE(result, HSVEC_SIZE(dim));
	result->dim = dim;
	result->unused = 0;

	for (i = 0; i < dim; i++)
	{
		uint16		raw = pq_getmsgint(buf, sizeof(uint16));

		memcpy(&result->x[i], &raw, sizeof(half));
	}

	PG_RETURN_HSVEC_P(result);
}

/* ----------------------------------------------------------------
 *  Binary send: int16 dim + dim × uint16 (IEEE 754 binary16)
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(hsvec_send);
Datum
hsvec_send(PG_FUNCTION_ARGS)
{
	Hsvec	   *vec = PG_GETARG_HSVEC_P(0);
	StringInfoData buf;
	int			i;

	pq_begintypsend(&buf);
	pq_sendint16(&buf, vec->dim);
	pq_sendint16(&buf, vec->unused);

	for (i = 0; i < vec->dim; i++)
	{
		uint16		raw;

		memcpy(&raw, &vec->x[i], sizeof(half));
		pq_sendint16(&buf, raw);
	}

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/* ----------------------------------------------------------------
 *  Cosine distance: 1 - dot(a,b) / (|a| * |b|)
 *  Promotes each half to double for accumulation precision.
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(hsvec_cosine_distance);
Datum
hsvec_cosine_distance(PG_FUNCTION_ARGS)
{
	Hsvec	   *a = PG_GETARG_HSVEC_P(0);
	Hsvec	   *b = PG_GETARG_HSVEC_P(1);
	int			dim;
	double		dot = 0.0;
	double		norm_a = 0.0;
	double		norm_b = 0.0;
	double		similarity;
	int			i;

	if (a->dim != b->dim)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("hsvec dimensions must match: %d vs %d", a->dim, b->dim)));

	dim = a->dim;

	for (i = 0; i < dim; i++)
	{
		double		ai = (double) HalfToFloat4(a->x[i]);
		double		bi = (double) HalfToFloat4(b->x[i]);

		dot += ai * bi;
		norm_a += ai * ai;
		norm_b += bi * bi;
	}

	/* Handle zero vectors */
	if (norm_a == 0.0 || norm_b == 0.0)
		PG_RETURN_FLOAT8(get_float8_nan());

	similarity = dot / (sqrt(norm_a) * sqrt(norm_b));

	/* Clamp to [-1, 1] for numerical safety */
	if (similarity > 1.0)
		similarity = 1.0;
	else if (similarity < -1.0)
		similarity = -1.0;

	PG_RETURN_FLOAT8(1.0 - similarity);
}

/* ----------------------------------------------------------------
 *  Cast: hsvec → svec (lossless, float16 → float32)
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(hsvec_to_svec);
Datum
hsvec_to_svec(PG_FUNCTION_ARGS)
{
	Hsvec	   *h = PG_GETARG_HSVEC_P(0);
	int			dim = h->dim;
	Svec	   *s;
	int			i;

	if (dim > SVEC_MAX_DIM)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("hsvec with %d dimensions exceeds svec maximum of %d",
						dim, SVEC_MAX_DIM)));

	s = (Svec *) palloc0(SVEC_SIZE(dim));
	SET_VARSIZE(s, SVEC_SIZE(dim));
	s->dim = dim;
	s->unused = 0;

	for (i = 0; i < dim; i++)
		s->x[i] = HalfToFloat4(h->x[i]);

	PG_RETURN_SVEC_P(s);
}

/* ----------------------------------------------------------------
 *  Cast: svec → hsvec (lossy, float32 → float16)
 * ---------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(svec_to_hsvec);
Datum
svec_to_hsvec(PG_FUNCTION_ARGS)
{
	Svec	   *s = PG_GETARG_SVEC_P(0);
	int			dim = s->dim;
	Hsvec	   *h;
	int			i;

	/* svec max dim (16000) always fits in hsvec max dim (32000) */
	h = (Hsvec *) palloc0(HSVEC_SIZE(dim));
	SET_VARSIZE(h, HSVEC_SIZE(dim));
	h->dim = dim;
	h->unused = 0;

	for (i = 0; i < dim; i++)
		h->x[i] = Float4ToHalf(s->x[i]);

	PG_RETURN_HSVEC_P(h);
}
