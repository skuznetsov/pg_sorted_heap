/*
 * hsvec.h
 *
 * Half-precision Sorted Vector type: a float16 vector for pg_sorted_heap.
 * 2 bytes per dimension (vs 4 for svec), max 32000 dimensions.
 * Text I/O format: [x1,x2,...,xn]
 */
#ifndef HSVEC_H
#define HSVEC_H

#include "postgres.h"
#include "fmgr.h"

#include <string.h>
#include <math.h>

/* ----------------------------------------------------------------
 *  Half-float type definition
 *
 *  Uses native _Float16 on ARM (Apple Silicon, etc.) when available.
 *  Falls back to uint16 with software IEEE 754 conversion on x86.
 * ---------------------------------------------------------------- */
#if defined(__FLT16_MAX__)
#define HSVEC_NATIVE_FP16	1
typedef _Float16 half;
#else
#define HSVEC_NATIVE_FP16	0
typedef uint16 half;
#endif

/* ---- Conversion: half ↔ float ---- */

static inline float
HalfToFloat4(half h)
{
#if HSVEC_NATIVE_FP16
	return (float) h;
#else
	uint16		bits = h;
	uint32		sign = (bits >> 15) & 1;
	uint32		exp = (bits >> 10) & 0x1F;
	uint32		mant = bits & 0x3FF;
	uint32		f;
	float		result;

	if (exp == 0)
	{
		if (mant == 0)
		{
			/* +/- zero */
			f = sign << 31;
		}
		else
		{
			/* Denormalized: convert to normalized float32 */
			float		val = ldexpf((float) mant, -24);

			return sign ? -val : val;
		}
	}
	else if (exp == 31)
	{
		/* Infinity or NaN */
		f = (sign << 31) | 0x7F800000 | (mant << 13);
	}
	else
	{
		/* Normalized: rebias exponent from 15 to 127 */
		f = (sign << 31) | ((exp + 112) << 23) | (mant << 13);
	}

	memcpy(&result, &f, 4);
	return result;
#endif
}

static inline half
Float4ToHalf(float f)
{
#if HSVEC_NATIVE_FP16
	return (_Float16) f;
#else
	uint32		bits;
	uint16		sign;
	int32		exp;
	uint32		mant;

	memcpy(&bits, &f, 4);

	sign = (bits >> 16) & 0x8000;
	exp = ((bits >> 23) & 0xFF) - 127 + 15;
	mant = bits & 0x7FFFFF;

	if (exp <= 0)
	{
		if (exp < -10)
			return sign;		/* Underflow → zero */
		/* Denormalized */
		mant = (mant | 0x800000) >> (1 - exp);
		/* Round to nearest even */
		if ((mant & 0x1FFF) > 0x1000 ||
			((mant & 0x1FFF) == 0x1000 && (mant & 0x2000)))
			mant += 0x2000;
		return sign | (uint16) (mant >> 13);
	}
	else if (exp == 0xFF - 127 + 15)
	{
		if (mant)
			return sign | 0x7E00;	/* NaN */
		return sign | 0x7C00;	/* Infinity */
	}
	else if (exp > 30)
	{
		return sign | 0x7C00;	/* Overflow → Infinity */
	}

	/* Round to nearest even */
	if ((mant & 0x1FFF) > 0x1000 ||
		((mant & 0x1FFF) == 0x1000 && (mant & 0x2000)))
	{
		mant += 0x2000;
		if (mant & 0x800000)
		{
			mant = 0;
			exp++;
			if (exp > 30)
				return sign | 0x7C00;	/* Overflow after rounding */
		}
	}

	return sign | (uint16) (exp << 10) | (uint16) (mant >> 13);
#endif
}

/* ---- Hsvec struct ---- */

typedef struct Hsvec
{
	int32		vl_len_;		/* varlena header (do not touch directly) */
	int16		dim;			/* number of dimensions */
	int16		unused;			/* reserved, must be 0 */
	half		x[FLEXIBLE_ARRAY_MEMBER];	/* float16 components */
} Hsvec;

#define HSVEC_SIZE(dim)		(offsetof(Hsvec, x) + sizeof(half) * (dim))
#define HSVEC_MAX_DIM		32000

#define PG_GETARG_HSVEC_P(n)	((Hsvec *) PG_DETOAST_DATUM(PG_GETARG_DATUM(n)))
#define PG_RETURN_HSVEC_P(x)	PG_RETURN_POINTER(x)

/* I/O functions */
extern Datum hsvec_in(PG_FUNCTION_ARGS);
extern Datum hsvec_out(PG_FUNCTION_ARGS);
extern Datum hsvec_typmod_in(PG_FUNCTION_ARGS);
extern Datum hsvec_recv(PG_FUNCTION_ARGS);
extern Datum hsvec_send(PG_FUNCTION_ARGS);

/* Distance */
extern Datum hsvec_cosine_distance(PG_FUNCTION_ARGS);

/* Casts */
extern Datum hsvec_to_svec(PG_FUNCTION_ARGS);
extern Datum svec_to_hsvec(PG_FUNCTION_ARGS);

#endif							/* HSVEC_H */
