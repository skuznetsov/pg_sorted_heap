/*
 * flashhadamard.c — FlashHadamard experimental packed retrieval scan
 *
 * Implements the two-stage search:
 *   Stage 1: Block-Hadamard rotation → packed 4-bit ADC shortlist
 *   Stage 2: SQ8 rerank on shortlist candidates
 *
 * This is the engine-facing C implementation of the research harness
 * validated in scripts/bench_turboquant_retrieval.py.
 */

#include "flashhadamard.h"

#include <math.h>
#include <string.h>
#include <float.h>

/* ================================================================
 * FWHT: Fast Walsh-Hadamard Transform (in-place, normalized)
 * ================================================================ */

void
fh_fwht_inplace(float *data, int len)
{
    int h, i;

    if (len <= 1)
        return;

    for (h = 1; h < len; h <<= 1)
    {
        for (i = 0; i < len; i += 2 * h)
        {
            int j;
            for (j = i; j < i + h; j++)
            {
                float a = data[j];
                float b = data[j + h];
                data[j]     = a + b;
                data[j + h] = a - b;
            }
        }
    }

    /* Normalize */
    {
        float scale = 1.0f / sqrtf((float)len);
        for (i = 0; i < len; i++)
            data[i] *= scale;
    }
}

/* ================================================================
 * Block Hadamard rotation of a single vector
 * ================================================================ */

void
fh_rotate_vec(const float *in, float *out, const FHParams *params)
{
    int     dim = params->dim;
    int     i, offset, block_len;
    float  *temp;

    /* Apply permutation + signs */
    temp = (float *) palloc(sizeof(float) * dim);
    for (i = 0; i < dim; i++)
        temp[i] = in[params->perm[i]] * params->signs[i];

    /* Block FWHT: decompose dim into powers of 2 */
    offset = 0;
    {
        int remaining = dim;
        while (remaining > 0)
        {
            block_len = 1;
            while (block_len * 2 <= remaining)
                block_len *= 2;

            memcpy(out + offset, temp + offset, sizeof(float) * block_len);
            fh_fwht_inplace(out + offset, block_len);

            offset += block_len;
            remaining -= block_len;
        }
    }

    pfree(temp);
}

/* ================================================================
 * Build 256-entry byte tables for packed nibble ADC
 * ================================================================ */

void
fh_build_byte_tables(const float *coeffs, const float *centers,
                      int dim, float *byte_tables)
{
    int     n_bytes = (dim + 1) / 2;
    int     byte_idx;

    for (byte_idx = 0; byte_idx < n_bytes; byte_idx++)
    {
        int     lo_dim = byte_idx * 2;
        int     hi_dim = lo_dim + 1;
        float   lo_coeff = coeffs[lo_dim];
        float   hi_coeff = (hi_dim < dim) ? coeffs[hi_dim] : 0.0f;
        int     code;

        for (code = 0; code < 256; code++)
        {
            int lo_nibble = code & 0x0F;
            int hi_nibble = code >> 4;
            byte_tables[byte_idx * 256 + code] =
                lo_coeff * centers[lo_nibble] + hi_coeff * centers[hi_nibble];
        }
    }
}

/* ================================================================
 * Packed ADC scoring (transposed layout, single-threaded)
 * ================================================================ */

void
fh_packed_score_t(const uint8 *packed_t, const float *byte_tables,
                   const float *norms, int n_rows, int n_bytes,
                   float *out_scores)
{
    int     row, byte_idx;

    /* Zero scores */
    memset(out_scores, 0, sizeof(float) * n_rows);

    /* 2-byte fused scoring loop */
    byte_idx = 0;
    for (; byte_idx + 1 < n_bytes; byte_idx += 2)
    {
        const uint8 *codes0 = packed_t + byte_idx * n_rows;
        const uint8 *codes1 = packed_t + (byte_idx + 1) * n_rows;
        const float *table0 = byte_tables + byte_idx * 256;
        const float *table1 = byte_tables + (byte_idx + 1) * 256;

        for (row = 0; row + 3 < n_rows; row += 4)
        {
            out_scores[row + 0] += table0[codes0[row + 0]] + table1[codes1[row + 0]];
            out_scores[row + 1] += table0[codes0[row + 1]] + table1[codes1[row + 1]];
            out_scores[row + 2] += table0[codes0[row + 2]] + table1[codes1[row + 2]];
            out_scores[row + 3] += table0[codes0[row + 3]] + table1[codes1[row + 3]];
        }
        for (; row < n_rows; row++)
            out_scores[row] += table0[codes0[row]] + table1[codes1[row]];
    }
    if (byte_idx < n_bytes)
    {
        const uint8 *codes = packed_t + byte_idx * n_rows;
        const float *table = byte_tables + byte_idx * 256;
        for (row = 0; row < n_rows; row++)
            out_scores[row] += table[codes[row]];
    }

    /* Apply norms */
    if (norms != NULL)
    {
        for (row = 0; row < n_rows; row++)
            out_scores[row] *= norms[row];
    }
}

/* ================================================================
 * Top-k selection via linear scan (simple for small k)
 * ================================================================ */

static void
topk_insert(float score, int32 row_id, float *top_scores, int32 *top_ids,
            int k, int *filled, int *min_pos, float *min_score)
{
    if (*filled < k)
    {
        int pos = *filled;
        top_scores[pos] = score;
        top_ids[pos] = row_id;
        (*filled)++;
        if (*filled == 1 || score < *min_score)
        {
            *min_score = score;
            *min_pos = pos;
        }
        return;
    }
    if (score <= *min_score)
        return;

    top_scores[*min_pos] = score;
    top_ids[*min_pos] = row_id;

    /* Recompute min */
    {
        int idx;
        *min_pos = 0;
        *min_score = top_scores[0];
        for (idx = 1; idx < k; idx++)
        {
            if (top_scores[idx] < *min_score)
            {
                *min_score = top_scores[idx];
                *min_pos = idx;
            }
        }
    }
}

/* ================================================================
 * Two-stage search: packed shortlist → SQ8 rerank
 * ================================================================ */

int
fh_search(const FHCodes *codes, const FHParams *params,
           const float *query, int k, int shortlist_m,
           int32 *out_ids, float *out_scores)
{
    int     dim = params->dim;
    int     n_rows = codes->n_rows;
    int     n_bytes = codes->n_bytes;
    float  *rotated;
    float  *coeffs;
    float  *byte_tables;
    float  *adc_scores;
    int32  *shortlist_ids;
    float  *shortlist_scores;
    int     filled, min_pos;
    float   min_score;
    int     i, j;
    float   inv_sqrt_dim = 1.0f / sqrtf((float)dim);

    /* Step 1: Rotate query */
    rotated = (float *) palloc(sizeof(float) * dim);
    fh_rotate_vec(query, rotated, params);

    /* Step 2: Build coefficients (fold group scales) */
    coeffs = (float *) palloc(sizeof(float) * dim);
    {
        float *expanded = (float *) palloc(sizeof(float) * dim);
        int g;
        for (g = 0; g < params->n_groups; g++)
        {
            int start = g * params->group_size;
            int end = Min(start + params->group_size, dim);
            for (j = start; j < end; j++)
                expanded[j] = params->group_scales[g];
        }
        for (i = 0; i < dim; i++)
            coeffs[i] = rotated[i] * expanded[i] * inv_sqrt_dim;
        pfree(expanded);
    }

    /* Step 3: Build byte tables + packed ADC score */
    byte_tables = (float *) palloc(sizeof(float) * n_bytes * 256);
    fh_build_byte_tables(coeffs, params->centers, dim, byte_tables);

    adc_scores = (float *) palloc(sizeof(float) * n_rows);
    fh_packed_score_t(codes->packed_t, byte_tables, codes->norms,
                       n_rows, n_bytes, adc_scores);

    /* Step 4: Top-M shortlist from ADC scores */
    shortlist_ids = (int32 *) palloc(sizeof(int32) * shortlist_m);
    shortlist_scores = (float *) palloc(sizeof(float) * shortlist_m);
    filled = 0; min_pos = 0; min_score = -FLT_MAX;

    for (i = 0; i < n_rows; i++)
        topk_insert(adc_scores[i], i, shortlist_scores, shortlist_ids,
                     shortlist_m, &filled, &min_pos, &min_score);

    /* Step 5: SQ8 rerank on shortlist */
    if (codes->sq8_codes != NULL && shortlist_m > k)
    {
        float *rerank_scores = (float *) palloc(sizeof(float) * filled);
        float *q_norm = (float *) palloc(sizeof(float) * dim);

        /* Normalize query for rerank */
        {
            float norm = 0.0f;
            for (i = 0; i < dim; i++)
                norm += query[i] * query[i];
            norm = sqrtf(norm);
            if (norm < 1e-12f) norm = 1e-12f;
            for (i = 0; i < dim; i++)
                q_norm[i] = query[i] / norm;
        }

        /* SQ8 decode + dot product for each shortlist candidate */
        for (i = 0; i < filled; i++)
        {
            int row = shortlist_ids[i];
            const uint8 *row_codes = codes->sq8_codes + row * dim;
            float dot = 0.0f;

            for (j = 0; j < dim; j++)
            {
                float decoded = (float)row_codes[j] * codes->sq8_scales[j]
                                + codes->sq8_mins[j];
                dot += decoded * q_norm[j];
            }
            rerank_scores[i] = dot;
        }

        /* Top-k from reranked shortlist */
        {
            int rk_filled = 0, rk_min_pos = 0;
            float rk_min_score = -FLT_MAX;

            for (i = 0; i < filled; i++)
                topk_insert(rerank_scores[i], shortlist_ids[i],
                             out_scores, out_ids, k,
                             &rk_filled, &rk_min_pos, &rk_min_score);

            pfree(rerank_scores);
            pfree(q_norm);
            pfree(adc_scores);
            pfree(byte_tables);
            pfree(coeffs);
            pfree(rotated);

            /* Sort output by descending score */
            for (i = 0; i < rk_filled; i++)
                for (j = i + 1; j < rk_filled; j++)
                    if (out_scores[j] > out_scores[i])
                    {
                        float ts = out_scores[i]; out_scores[i] = out_scores[j]; out_scores[j] = ts;
                        int32 ti = out_ids[i]; out_ids[i] = out_ids[j]; out_ids[j] = ti;
                    }

            return rk_filled;
        }
    }

    /* No SQ8 rerank: return ADC shortlist directly as top-k */
    {
        int out_k = Min(filled, k);
        /* Sort by descending score */
        for (i = 0; i < filled; i++)
            for (j = i + 1; j < filled; j++)
                if (shortlist_scores[j] > shortlist_scores[i])
                {
                    float ts = shortlist_scores[i]; shortlist_scores[i] = shortlist_scores[j]; shortlist_scores[j] = ts;
                    int32 ti = shortlist_ids[i]; shortlist_ids[i] = shortlist_ids[j]; shortlist_ids[j] = ti;
                }

        memcpy(out_ids, shortlist_ids, sizeof(int32) * out_k);
        memcpy(out_scores, shortlist_scores, sizeof(float) * out_k);

        pfree(shortlist_ids);
        pfree(shortlist_scores);
        pfree(adc_scores);
        pfree(byte_tables);
        pfree(coeffs);
        pfree(rotated);

        return out_k;
    }
}
