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

/* Top-k heap insert */
extern void fh_topk_insert(float score, int32 row_id, float *top_scores,
                            int32 *top_ids, int k, int *filled,
                            int *min_pos, float *min_score);

/* Fused packed score + top-k with parallel row sharding */
extern void fh_packed_score_topk_t(const uint8 *packed_t, const float *byte_tables,
                                    const float *norms, int n_rows, int n_bytes,
                                    int topk, int32 *top_ids, float *top_scores,
                                    int *filled);

/* Score multiple non-contiguous row ranges in parallel, merge into global top-k.
 * ranges: array of {row_start, row_count} pairs.
 * n_ranges: number of ranges.
 * n_rows_total: total rows in packed_t (stride for column access). */
extern void fh_packed_score_ranges_topk(const uint8 *packed_t, const float *byte_tables,
                                         const float *norms, int n_rows_total, int n_bytes,
                                         const int *ranges, int n_ranges,
                                         int topk, int32 *top_ids, float *top_scores,
                                         int *filled);

/* Two-stage search: packed shortlist → SQ8 rerank */
extern int fh_search(const FHCodes *codes, const FHParams *params,
                      const float *query, int k, int shortlist_m,
                      int32 *out_ids, float *out_scores);

#endif /* FLASHHADAMARD_H */
