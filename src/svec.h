/*
 * svec.h
 *
 * Sorted Vector type: a lightweight float32 vector for pg_sorted_heap.
 * Binary-compatible layout with pgvector's "vector" type.
 * Text I/O format: [x1,x2,...,xn]
 */
#ifndef SVEC_H
#define SVEC_H

#include "postgres.h"
#include "fmgr.h"

typedef struct Svec
{
	int32		vl_len_;		/* varlena header (do not touch directly) */
	int16		dim;			/* number of dimensions */
	int16		unused;			/* reserved, must be 0 */
	float		x[FLEXIBLE_ARRAY_MEMBER];	/* float32 components */
} Svec;

#define SVEC_SIZE(dim)		(offsetof(Svec, x) + sizeof(float) * (dim))
#define SVEC_MAX_DIM		16000

#define DatumGetSvecP(d)		((Svec *) PG_DETOAST_DATUM(d))
#define PG_GETARG_SVEC_P(n)	DatumGetSvecP(PG_GETARG_DATUM(n))
#define PG_RETURN_SVEC_P(x)	PG_RETURN_POINTER(x)

/* I/O functions */
extern Datum svec_in(PG_FUNCTION_ARGS);
extern Datum svec_out(PG_FUNCTION_ARGS);
extern Datum svec_typmod_in(PG_FUNCTION_ARGS);
extern Datum svec_recv(PG_FUNCTION_ARGS);
extern Datum svec_send(PG_FUNCTION_ARGS);

/* Distance */
extern Datum svec_cosine_distance(PG_FUNCTION_ARGS);
extern float8 svec_cosine_distance_internal(const Svec *a, const Svec *b);

#endif							/* SVEC_H */
