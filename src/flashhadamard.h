/*
 * flashhadamard.h — FlashHadamard experimental packed retrieval scan
 *
 * Storage: packed 4-bit nibble codes (transposed) + SQ8 rerank codes
 * Scan: packed ADC shortlist → SQ8 rerank → top-k
 */

#ifndef FLASHHADAMARD_H
#define FLASHHADAMARD_H

#include "postgres.h"

/* Block Hadamard rotation parameters (seed-derived) */
typedef struct FHParams
{
    int     dim;
    int     group_size;     /* 16 for FlashHadamard-16 */
    int     seed;
    int     n_groups;
    float  *group_scales;   /* [n_groups] RMS scales */
    float  *centers;        /* [16] Lloyd-Max centers for 4-bit */
    int    *perm;           /* [dim] random permutation */
    float  *signs;          /* [dim] random ±1 signs */
} FHParams;

/* Per-vector packed storage */
typedef struct FHCodes
{
    int      n_rows;
    int      dim;
    int      n_bytes;       /* dim/2 (packed nibbles) */
    uint8   *packed_t;      /* [n_bytes × n_rows] transposed packed codes */
    uint8   *sq8_codes;     /* [n_rows × dim] SQ8 rerank codes */
    float   *sq8_mins;      /* [dim] per-column min for SQ8 */
    float   *sq8_scales;    /* [dim] per-column scale for SQ8 */
    float   *norms;         /* [n_rows] L2 norms (NULL if unit vectors) */
} FHCodes;

/* FWHT on a single vector in-place */
extern void fh_fwht_inplace(float *data, int len);

/* Block Hadamard rotation of a single vector */
extern void fh_rotate_vec(const float *in, float *out, const FHParams *params);

/* Build byte tables for packed ADC scoring */
extern void fh_build_byte_tables(const float *coeffs, const float *centers,
                                  int dim, float *byte_tables);

/* Packed ADC score all rows (transposed layout, multi-threaded) */
extern void fh_packed_score_t(const uint8 *packed_t, const float *byte_tables,
                               const float *norms, int n_rows, int n_bytes,
                               float *out_scores);

/* Two-stage search: packed shortlist → SQ8 rerank */
extern int fh_search(const FHCodes *codes, const FHParams *params,
                      const float *query, int k, int shortlist_m,
                      int32 *out_ids, float *out_scores);

#endif /* FLASHHADAMARD_H */
