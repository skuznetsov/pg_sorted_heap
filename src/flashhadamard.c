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
#include "flashhadamard_store.h"
#include "svec.h"
#include "hsvec.h"

#include "fmgr.h"
#include "funcapi.h"
#include "executor/spi.h"
#include "access/table.h"
#include "access/tableam.h"
#include "executor/tuptable.h"
#include "utils/builtins.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/rel.h"
#include "catalog/pg_type.h">

#include <math.h>
#include <string.h>
#include <float.h>
#include <stdlib.h>

/* Gaussian Lloyd-Max centers for 4-bit (precomputed, matching Python harness) */
static const float fh_lloyd_max_16[16] = {
    -2.1519927f, -1.5341205f, -1.1503494f, -0.8326452f,
    -0.5485528f, -0.2822760f, -0.0248825f,  0.2279585f,
     0.4809854f,  0.7405728f,  1.0137205f,  1.3106381f,
     1.6481531f,  2.0637655f,  2.6476993f,  3.7169876f
};

/* Gaussian Lloyd-Max boundaries for 4-bit (for digitize) */
static const float fh_lloyd_max_bounds_17[17] = {
    -INFINITY,
    -1.8430566f, -1.3422349f, -0.9914973f, -0.6905990f,
    -0.4154144f, -0.1535793f,  0.1015380f,  0.3544720f,
     0.6107791f,  0.8771467f,  1.1621793f,  1.4793956f,
     1.8559593f,  2.3557324f,  3.1823435f,
     INFINITY
};

/* Digitize: map continuous value to Lloyd-Max level index [0..15] */
static int
fh_digitize(float val)
{
    int i;
    for (i = 1; i < 17; i++)
        if (val < fh_lloyd_max_bounds_17[i])
            return i - 1;
    return 15;
}

/* Splitmix64-style hash for seed-derived random permutation/signs */
static uint64
fh_splitmix64(uint64 *state)
{
    uint64 z = (*state += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

/* Generate seed-derived permutation and signs (matches numpy default_rng) */
void
fh_generate_perm_signs(int dim, int seed, int *perm, float *signs)
{
    /* Simple Fisher-Yates shuffle using splitmix64 */
    uint64 state = (uint64)seed;
    int i, j;

    for (i = 0; i < dim; i++)
        perm[i] = i;
    for (i = dim - 1; i > 0; i--)
    {
        j = (int)(fh_splitmix64(&state) % (uint64)(i + 1));
        { int tmp = perm[i]; perm[i] = perm[j]; perm[j] = tmp; }
    }
    for (i = 0; i < dim; i++)
        signs[i] = (fh_splitmix64(&state) & 1) ? 1.0f : -1.0f;
}

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
 * Fused packed score + top-k with parallel row sharding
 * ================================================================ */

#ifndef _WIN32
#include <pthread.h>

#define FH_MAX_THREADS 16
#define FH_DEFAULT_THREADS 8
#define FH_MIN_ROWS_PER_THREAD 4096

typedef struct FHScoreTask
{
    const uint8 *packed_t;
    const float *byte_tables;
    const float *norms;
    int          n_rows;      /* total rows (for column pointer math) */
    int          n_bytes;
    int          row_start;
    int          row_end;
    int          topk;
    int32       *top_ids;     /* per-thread top-k output */
    float       *top_scores;
} FHScoreTask;

static void *
fh_score_worker(void *arg)
{
    FHScoreTask *t = (FHScoreTask *)arg;
    int     rows = t->row_end - t->row_start;
    float  *scores;
    int     row, byte_idx;
    int     filled = 0, min_pos = 0;
    float   min_score = -FLT_MAX;

    scores = (float *)malloc(sizeof(float) * rows);
    if (!scores)
    {
        /* Alloc failure: mark all outputs as invalid */
        int idx;
        for (idx = 0; idx < t->topk; idx++)
        {
            t->top_ids[idx] = -1;
            t->top_scores[idx] = -FLT_MAX;
        }
        return NULL;
    }
    memset(scores, 0, sizeof(float) * rows);

    /* 2-byte fused scoring for this row range */
    byte_idx = 0;
    for (; byte_idx + 1 < t->n_bytes; byte_idx += 2)
    {
        const uint8 *codes0 = t->packed_t + (Size)byte_idx * t->n_rows + t->row_start;
        const uint8 *codes1 = t->packed_t + (Size)(byte_idx + 1) * t->n_rows + t->row_start;
        const float *table0 = t->byte_tables + byte_idx * 256;
        const float *table1 = t->byte_tables + (byte_idx + 1) * 256;

        for (row = 0; row + 3 < rows; row += 4)
        {
            scores[row + 0] += table0[codes0[row + 0]] + table1[codes1[row + 0]];
            scores[row + 1] += table0[codes0[row + 1]] + table1[codes1[row + 1]];
            scores[row + 2] += table0[codes0[row + 2]] + table1[codes1[row + 2]];
            scores[row + 3] += table0[codes0[row + 3]] + table1[codes1[row + 3]];
        }
        for (; row < rows; row++)
            scores[row] += table0[codes0[row]] + table1[codes1[row]];
    }
    if (byte_idx < t->n_bytes)
    {
        const uint8 *codes = t->packed_t + (Size)byte_idx * t->n_rows + t->row_start;
        const float *table = t->byte_tables + byte_idx * 256;
        for (row = 0; row < rows; row++)
            scores[row] += table[codes[row]];
    }

    /* Per-thread top-k with norms */
    for (row = 0; row < rows; row++)
    {
        float s = t->norms ? scores[row] * t->norms[t->row_start + row] : scores[row];
        fh_topk_insert(s, t->row_start + row, t->top_scores, t->top_ids,
                        t->topk, &filled, &min_pos, &min_score);
    }

    /* Mark unfilled slots */
    {
        int i;
        for (i = filled; i < t->topk; i++)
        {
            t->top_ids[i] = -1;
            t->top_scores[i] = -FLT_MAX;
        }
    }

    free(scores);
    return NULL;
}
#endif /* !_WIN32 */

void
fh_packed_score_topk_t(const uint8 *packed_t, const float *byte_tables,
                        const float *norms, int n_rows, int n_bytes,
                        int topk, int32 *top_ids, float *top_scores,
                        int *filled)
{
#ifndef _WIN32
    int     n_threads = FH_DEFAULT_THREADS;
    int     i, j;

    /* Adjust thread count */
    if (n_rows < FH_MIN_ROWS_PER_THREAD * 2)
        n_threads = 1;
    if (n_threads > FH_MAX_THREADS)
        n_threads = FH_MAX_THREADS;
    if (n_threads > n_rows / FH_MIN_ROWS_PER_THREAD)
        n_threads = Max(1, n_rows / FH_MIN_ROWS_PER_THREAD);

    if (n_threads > 1)
    {
        pthread_t       threads[FH_MAX_THREADS];
        FHScoreTask     tasks[FH_MAX_THREADS];
        int32          *all_ids;
        float          *all_scores;
        int             chunk = (n_rows + n_threads - 1) / n_threads;
        int             launched = 0;
        int             min_pos = 0;
        float           min_score = -FLT_MAX;

        all_ids = palloc(sizeof(int32) * n_threads * topk);
        all_scores = palloc(sizeof(float) * n_threads * topk);

        for (i = 0; i < n_threads; i++)
        {
            int start = i * chunk;
            if (start >= n_rows) break;
            int end = Min(start + chunk, n_rows);

            tasks[i].packed_t = packed_t;
            tasks[i].byte_tables = byte_tables;
            tasks[i].norms = norms;
            tasks[i].n_rows = n_rows;
            tasks[i].n_bytes = n_bytes;
            tasks[i].row_start = start;
            tasks[i].row_end = end;
            tasks[i].topk = topk;
            tasks[i].top_ids = all_ids + i * topk;
            tasks[i].top_scores = all_scores + i * topk;

            if (pthread_create(&threads[i], NULL, fh_score_worker, &tasks[i]) == 0)
                launched++;
            else
            {
                /* Thread creation failed: mark outputs invalid, don't join */
                int idx;
                for (idx = 0; idx < topk; idx++)
                {
                    tasks[i].top_ids[idx] = -1;
                    tasks[i].top_scores[idx] = -FLT_MAX;
                }
                break;  /* stop launching more threads */
            }
        }

        for (i = 0; i < launched; i++)
            pthread_join(threads[i], NULL);

        /* If partial launch: score remaining rows single-thread */
        if (launched > 0 && launched < n_threads)
        {
            int remaining_start = launched * chunk;
            if (remaining_start < n_rows)
            {
                /* Score the unassigned tail in-line */
                FHScoreTask tail;
                tail.packed_t = packed_t;
                tail.byte_tables = byte_tables;
                tail.norms = norms;
                tail.n_rows = n_rows;
                tail.n_bytes = n_bytes;
                tail.row_start = remaining_start;
                tail.row_end = n_rows;
                tail.topk = topk;
                tail.top_ids = all_ids + launched * topk;
                tail.top_scores = all_scores + launched * topk;
                fh_score_worker(&tail);
                launched++;  /* count the inline "thread" for merge */
            }
        }

        if (launched == 0)
        {
            pfree(all_ids);
            pfree(all_scores);
            /* Fall through to single-thread path below */
        }
        else
        {
            /* Merge all per-thread top-k into global top-k */
            *filled = 0;
            for (i = 0; i < launched * topk; i++)
            {
                if (all_ids[i] < 0) continue;
                fh_topk_insert(all_scores[i], all_ids[i],
                                top_scores, top_ids, topk,
                                filled, &min_pos, &min_score);
            }
            pfree(all_ids);
            pfree(all_scores);
            return;
        }
    }
#endif /* !_WIN32 */

    /* Single-thread fallback */
    {
        int     row, byte_idx;
        float  *scores;
        int     min_pos = 0;
        float   min_score = -FLT_MAX;

        scores = palloc(sizeof(float) * n_rows);
        memset(scores, 0, sizeof(float) * n_rows);

        byte_idx = 0;
        for (; byte_idx + 1 < n_bytes; byte_idx += 2)
        {
            const uint8 *codes0 = packed_t + (Size)byte_idx * n_rows;
            const uint8 *codes1 = packed_t + (Size)(byte_idx + 1) * n_rows;
            const float *table0 = byte_tables + byte_idx * 256;
            const float *table1 = byte_tables + (byte_idx + 1) * 256;

            for (row = 0; row + 3 < n_rows; row += 4)
            {
                scores[row + 0] += table0[codes0[row + 0]] + table1[codes1[row + 0]];
                scores[row + 1] += table0[codes0[row + 1]] + table1[codes1[row + 1]];
                scores[row + 2] += table0[codes0[row + 2]] + table1[codes1[row + 2]];
                scores[row + 3] += table0[codes0[row + 3]] + table1[codes1[row + 3]];
            }
            for (; row < n_rows; row++)
                scores[row] += table0[codes0[row]] + table1[codes1[row]];
        }
        if (byte_idx < n_bytes)
        {
            const uint8 *codes = packed_t + (Size)byte_idx * n_rows;
            const float *table = byte_tables + byte_idx * 256;
            for (row = 0; row < n_rows; row++)
                scores[row] += table[codes[row]];
        }

        *filled = 0;
        for (row = 0; row < n_rows; row++)
        {
            float s = norms ? scores[row] * norms[row] : scores[row];
            fh_topk_insert(s, row, top_scores, top_ids, topk,
                            filled, &min_pos, &min_score);
        }
        pfree(scores);
    }
}

/* ================================================================
 * Top-k selection via linear scan (simple for small k)
 * ================================================================ */

void
fh_topk_insert(float score, int32 row_id, float *top_scores, int32 *top_ids,
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
        fh_topk_insert(adc_scores[i], i, shortlist_scores, shortlist_ids,
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
                fh_topk_insert(rerank_scores[i], shortlist_ids[i],
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


/* ================================================================
 * PG FUNCTION: flashhadamard_build
 *
 * flashhadamard_build(
 *   source_tbl  regclass,
 *   embed_col   text,     -- column name with vector/halfvec embeddings
 *   sidecar_tbl text,     -- name for the sidecar table to create
 *   seed        int4 DEFAULT 42,
 *   group_size  int4 DEFAULT 16
 * ) RETURNS int4  -- number of vectors encoded
 *
 * Creates a sidecar table with:
 *   row_id      int4 PRIMARY KEY
 *   packed_t    bytea   (transposed packed 4-bit codes)
 *   sq8_codes   bytea   (SQ8 rerank codes)
 *   -- metadata stored as first row with row_id = -1
 * ================================================================ */

PG_FUNCTION_INFO_V1(flashhadamard_build);
Datum
flashhadamard_build(PG_FUNCTION_ARGS)
{
    Oid         source_oid = PG_GETARG_OID(0);
    char       *embed_col  = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char       *sidecar    = text_to_cstring(PG_GETARG_TEXT_PP(2));
    int         seed       = PG_GETARG_INT32(3);
    int         group_size = PG_GETARG_INT32(4);
    char       *source_name;
    StringInfoData sql;
    int         ret, n_rows, dim, n_bytes, n_groups;
    int         i, j, d;
    float      *all_vecs = NULL;     /* [n_rows × dim] */
    float      *norms = NULL;        /* [n_rows] */
    float      *rotated = NULL;      /* [n_rows × dim] */
    float      *group_scales = NULL; /* [n_groups] */
    uint8      *packed_codes = NULL; /* [n_rows × n_bytes] row-major */
    uint8      *packed_t = NULL;     /* [n_bytes × n_rows] transposed */
    uint8      *sq8_codes = NULL;    /* [n_rows × dim] */
    float      *sq8_mins = NULL;     /* [dim] */
    float      *sq8_scales = NULL;   /* [dim] */
    FHParams    params;

    source_name = get_rel_name(source_oid);
    if (!source_name)
        ereport(ERROR, (errmsg("flashhadamard_build: source table not found")));

    /* Streaming 3-pass build: no all_vecs buffer needed.
     * Pass 1: count + dim + running group RMS stats
     * Pass 2: rotate + quantize + pack + SQ8 encode (row by row)
     *         Output: packed_codes[n_rows × n_bytes], sq8_codes[n_rows × dim], norms[n_rows]
     * Step 3: Transpose packed_codes → packed_t
     */
    {
        Relation        rel;
        TupleDesc       td;
        TableScanDesc   scan;
        TupleTableSlot *slot;
        AttrNumber      embed_attno;
        Oid             embed_typid;
        bool            is_halfvec = false;
        Snapshot        snapshot;
        int             row_idx;
        float          *vec_buf;    /* single-row temp buffer [dim] */
        float          *rot_buf;    /* single-row rotated buffer [dim] */
        float          *expanded;   /* [dim] expanded group scales */
        double         *group_sum_sq; /* running RMS accumulator */
        int            *group_count;

        rel = table_open(source_oid, AccessShareLock);
        td = RelationGetDescr(rel);
        embed_attno = get_attnum(source_oid, embed_col);
        if (embed_attno == InvalidAttrNumber)
        {
            table_close(rel, AccessShareLock);
            ereport(ERROR, (errmsg("flashhadamard_build: column \"%s\" not found", embed_col)));
        }
        embed_typid = TupleDescAttr(td, embed_attno - 1)->atttypid;
        {
            char *typname = format_type_be(embed_typid);
            if (typname && (strstr(typname, "half") || strstr(typname, "hsvec")))
                is_halfvec = true;
            if (typname) pfree(typname);
        }

        snapshot = GetTransactionSnapshot();
        slot = table_slot_create(rel, NULL);

        /* --- Pass 1: count + dim + group RMS stats --- */
        scan = table_beginscan(rel, snapshot, 0, NULL);
        dim = 0;
        n_rows = 0;
        while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
        {
            if (dim == 0)
            {
                bool isnull;
                Datum d_val = slot_getattr(slot, embed_attno, &isnull);
                if (!isnull)
                {
                    Svec *sv = (Svec *) PG_DETOAST_DATUM(d_val);
                    dim = sv->dim;
                    if (sv != (Svec *) DatumGetPointer(d_val))
                        pfree(sv);
                }
            }
            n_rows++;
            ExecClearTuple(slot);
        }
        table_endscan(scan);

        if (n_rows == 0 || dim == 0)
        {
            ExecDropSingleTupleTableSlot(slot);
            table_close(rel, AccessShareLock);
            PG_RETURN_INT32(0);
        }

        n_bytes = (dim + 1) / 2;
        n_groups = (dim + group_size - 1) / group_size;

        /* Build rotation params */
        params.dim = dim;
        params.group_size = group_size;
        params.seed = seed;
        params.n_groups = n_groups;
        params.perm = palloc(sizeof(int) * dim);
        params.signs = palloc(sizeof(float) * dim);
        params.centers = (float *)fh_lloyd_max_16;
        fh_generate_perm_signs(dim, seed, params.perm, params.signs);

        /* Compute group RMS: stream vectors, rotate, accumulate stats */
        vec_buf = palloc(sizeof(float) * dim);
        rot_buf = palloc(sizeof(float) * dim);
        group_sum_sq = palloc0(sizeof(double) * n_groups);
        group_count = palloc0(sizeof(int) * n_groups);

        {
            float sqrt_dim = sqrtf((float)dim);
            int g;

            scan = table_beginscan(rel, snapshot, 0, NULL);
            while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
            {
                bool isnull;
                Datum d_val = slot_getattr(slot, embed_attno, &isnull);
                float norm = 0.0f;

                if (isnull) { ExecClearTuple(slot); continue; }

                /* Extract + normalize into vec_buf */
                if (is_halfvec)
                {
                    Hsvec *hv = (Hsvec *) PG_DETOAST_DATUM(d_val);
                    for (d = 0; d < Min(hv->dim, dim); d++)
                    { vec_buf[d] = HalfToFloat4(hv->x[d]); norm += vec_buf[d]*vec_buf[d]; }
                    for (d = Min(hv->dim, dim); d < dim; d++) vec_buf[d] = 0;
                    if (hv != (Hsvec *) DatumGetPointer(d_val)) pfree(hv);
                }
                else
                {
                    Svec *sv = (Svec *) PG_DETOAST_DATUM(d_val);
                    memcpy(vec_buf, sv->x, sizeof(float) * Min(sv->dim, dim));
                    for (d = Min(sv->dim, dim); d < dim; d++) vec_buf[d] = 0;
                    for (d = 0; d < Min(sv->dim, dim); d++) norm += vec_buf[d]*vec_buf[d];
                    if (sv != (Svec *) DatumGetPointer(d_val)) pfree(sv);
                }
                norm = sqrtf(norm);
                if (norm < 1e-12f) norm = 1e-12f;
                for (d = 0; d < dim; d++) vec_buf[d] /= norm;

                /* Rotate + accumulate group stats */
                fh_rotate_vec(vec_buf, rot_buf, &params);
                for (d = 0; d < dim; d++) rot_buf[d] *= sqrt_dim;

                for (g = 0; g < n_groups; g++)
                {
                    int gstart = g * group_size;
                    int gend = Min(gstart + group_size, dim);
                    for (d = gstart; d < gend; d++)
                    {
                        group_sum_sq[g] += (double)rot_buf[d] * rot_buf[d];
                        group_count[g]++;
                    }
                }
                ExecClearTuple(slot);
            }
            table_endscan(scan);

            /* Compute final group scales */
            group_scales = palloc(sizeof(float) * n_groups);
            for (g = 0; g < n_groups; g++)
            {
                group_scales[g] = (float)sqrt(group_sum_sq[g] / Max(group_count[g], 1));
                if (group_scales[g] < 1e-4f) group_scales[g] = 1e-4f;
            }
            params.group_scales = group_scales;
            pfree(group_sum_sq);
            pfree(group_count);
        }

        /* Expand group scales for encoding */
        expanded = palloc(sizeof(float) * dim);
        {
            int g;
            for (g = 0; g < n_groups; g++)
            {
                int gstart = g * group_size;
                int gend = Min(gstart + group_size, dim);
                for (d = gstart; d < gend; d++)
                    expanded[d] = group_scales[g];
            }
        }

        /* SQ8 min/max: need a pass to find per-column extremes.
         * We'll initialize and update during the encoding pass. */
        sq8_mins = palloc(sizeof(float) * dim);
        sq8_scales = palloc(sizeof(float) * dim);
        for (d = 0; d < dim; d++) { sq8_mins[d] = FLT_MAX; sq8_scales[d] = -FLT_MAX; }
        /* sq8_scales temporarily holds col_max */

        /* Allocate output buffers (no all_vecs needed!) */
        packed_codes = MemoryContextAllocHuge(CurrentMemoryContext, (Size)n_rows * n_bytes);
        norms = palloc(sizeof(float) * n_rows);

        /* --- Pass 2: rotate + quantize + pack + SQ8 min/max --- */
        /* Also store normalized vectors for SQ8 in a separate buffer */
        /* We need all normalized vectors for SQ8 encoding (two-pass per column).
         * But to avoid 1.2GB buffer, do SQ8 in a third pass reading packed sidecar.
         * Actually: SQ8 needs original normalized vectors, not rotated.
         * Alternative: store SQ8 codes computed from normalized vectors during this pass.
         * For SQ8, we need global min/max first → requires one more pass.
         * Compromise: compute min/max during this pass, then do SQ8 in a third pass. */

        {
            float sqrt_dim = sqrtf((float)dim);

            scan = table_beginscan(rel, snapshot, 0, NULL);
            row_idx = 0;
            while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
            {
                bool isnull;
                Datum d_val = slot_getattr(slot, embed_attno, &isnull);
                float norm = 0.0f;

                if (isnull)
                {
                    memset(packed_codes + (Size)row_idx * n_bytes, 0, n_bytes);
                    norms[row_idx] = 1e-12f;
                    row_idx++;
                    ExecClearTuple(slot);
                    continue;
                }

                /* Extract + normalize */
                if (is_halfvec)
                {
                    Hsvec *hv = (Hsvec *) PG_DETOAST_DATUM(d_val);
                    for (d = 0; d < Min(hv->dim, dim); d++)
                    { vec_buf[d] = HalfToFloat4(hv->x[d]); norm += vec_buf[d]*vec_buf[d]; }
                    for (d = Min(hv->dim, dim); d < dim; d++) vec_buf[d] = 0;
                    if (hv != (Hsvec *) DatumGetPointer(d_val)) pfree(hv);
                }
                else
                {
                    Svec *sv = (Svec *) PG_DETOAST_DATUM(d_val);
                    memcpy(vec_buf, sv->x, sizeof(float) * Min(sv->dim, dim));
                    for (d = Min(sv->dim, dim); d < dim; d++) vec_buf[d] = 0;
                    for (d = 0; d < Min(sv->dim, dim); d++) norm += vec_buf[d]*vec_buf[d];
                    if (sv != (Svec *) DatumGetPointer(d_val)) pfree(sv);
                }
                norm = sqrtf(norm);
                if (norm < 1e-12f) norm = 1e-12f;
                for (d = 0; d < dim; d++) vec_buf[d] /= norm;
                norms[row_idx] = norm;

                /* Update SQ8 min/max (on normalized vectors) */
                for (d = 0; d < dim; d++)
                {
                    if (vec_buf[d] < sq8_mins[d]) sq8_mins[d] = vec_buf[d];
                    if (vec_buf[d] > sq8_scales[d]) sq8_scales[d] = vec_buf[d]; /* temp: col_max */
                }

                /* Rotate + equalize + quantize + pack */
                fh_rotate_vec(vec_buf, rot_buf, &params);
                for (d = 0; d < dim; d++) rot_buf[d] *= sqrt_dim;
                {
                    uint8 *row_packed = packed_codes + (Size)row_idx * n_bytes;
                    memset(row_packed, 0, n_bytes);
                    for (d = 0; d < dim; d++)
                    {
                        float eq = rot_buf[d] / expanded[d];
                        int code = fh_digitize(eq);
                        int byte_idx = d / 2;
                        if (d % 2 == 0)
                            row_packed[byte_idx] |= (uint8)(code & 0x0F);
                        else
                            row_packed[byte_idx] |= (uint8)((code & 0x0F) << 4);
                    }
                }

                row_idx++;
                ExecClearTuple(slot);
            }
            n_rows = row_idx;
            table_endscan(scan);
        }

        /* Finalize SQ8 scales */
        for (d = 0; d < dim; d++)
        {
            float range = sq8_scales[d] - sq8_mins[d]; /* col_max - col_min */
            if (range < 1e-8f) range = 1e-8f;
            sq8_scales[d] = range / 255.0f;
        }

        /* --- Pass 3: SQ8 encode (needs global min/max from pass 2) --- */
        sq8_codes = MemoryContextAllocHuge(CurrentMemoryContext, (Size)n_rows * dim);
        {
            scan = table_beginscan(rel, snapshot, 0, NULL);
            row_idx = 0;
            while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
            {
                bool isnull;
                Datum d_val = slot_getattr(slot, embed_attno, &isnull);
                float norm;

                if (isnull)
                {
                    memset(sq8_codes + (Size)row_idx * dim, 0, dim);
                    row_idx++;
                    ExecClearTuple(slot);
                    continue;
                }

                /* Extract + normalize (same as pass 2) */
                norm = 0.0f;
                if (is_halfvec)
                {
                    Hsvec *hv = (Hsvec *) PG_DETOAST_DATUM(d_val);
                    for (d = 0; d < Min(hv->dim, dim); d++)
                    { vec_buf[d] = HalfToFloat4(hv->x[d]); norm += vec_buf[d]*vec_buf[d]; }
                    for (d = Min(hv->dim, dim); d < dim; d++) vec_buf[d] = 0;
                    if (hv != (Hsvec *) DatumGetPointer(d_val)) pfree(hv);
                }
                else
                {
                    Svec *sv = (Svec *) PG_DETOAST_DATUM(d_val);
                    memcpy(vec_buf, sv->x, sizeof(float) * Min(sv->dim, dim));
                    for (d = Min(sv->dim, dim); d < dim; d++) vec_buf[d] = 0;
                    for (d = 0; d < Min(sv->dim, dim); d++) norm += vec_buf[d]*vec_buf[d];
                    if (sv != (Svec *) DatumGetPointer(d_val)) pfree(sv);
                }
                norm = sqrtf(norm);
                if (norm < 1e-12f) norm = 1e-12f;
                for (d = 0; d < dim; d++) vec_buf[d] /= norm;

                /* SQ8 encode */
                {
                    uint8 *row_sq8 = sq8_codes + (Size)row_idx * dim;
                    for (d = 0; d < dim; d++)
                    {
                        int code = (int)roundf((vec_buf[d] - sq8_mins[d]) / sq8_scales[d]);
                        if (code < 0) code = 0;
                        if (code > 255) code = 255;
                        row_sq8[d] = (uint8)code;
                    }
                }

                row_idx++;
                ExecClearTuple(slot);
            }
            table_endscan(scan);
        }

        /* Transpose packed codes */
        packed_t = MemoryContextAllocHuge(CurrentMemoryContext, (Size)n_bytes * n_rows);
        for (i = 0; i < n_rows; i++)
            for (j = 0; j < n_bytes; j++)
                packed_t[j * n_rows + i] = packed_codes[i * n_bytes + j];

        /* Free row-major packed codes (no longer needed after transpose) */
        pfree(packed_codes);
        packed_codes = NULL;

        pfree(vec_buf);
        pfree(rot_buf);
        pfree(expanded);
        ExecDropSingleTupleTableSlot(slot);
        table_close(rel, AccessShareLock);
    }

    /* Step 8: Create chunked sidecar table (bounded memory per INSERT) */
    {
#define FH_CHUNK_ROWS 4096
        int     n_chunks = (n_rows + FH_CHUNK_ROWS - 1) / FH_CHUNK_ROWS;
        int     seg;

        elog(NOTICE, "fh_build: creating sidecar '%s' (%d rows, %d dim, %d chunks)",
             sidecar, n_rows, dim, n_chunks);

        SPI_connect();
        initStringInfo(&sql);

        /* DDL */
        appendStringInfo(&sql, "DROP TABLE IF EXISTS %s", quote_identifier(sidecar));
        SPI_execute(sql.data, false, 0);

        resetStringInfo(&sql);
        appendStringInfo(&sql,
            "CREATE TABLE %s ("
            "  segment_id int4 NOT NULL,"      /* -1 = metadata, 0..N = data chunks */
            "  row_start int4,"
            "  row_count int4,"
            "  packed_t bytea,"
            "  sq8_codes bytea,"
            "  meta_group_scales bytea,"
            "  meta_sq8_mins bytea,"
            "  meta_sq8_scales bytea,"
            "  meta_norms bytea,"
            "  meta_dim int4,"
            "  meta_seed int4,"
            "  meta_group_size int4,"
            "  meta_n_rows int4,"
            "  meta_n_chunks int4"
            ")", quote_identifier(sidecar));
        SPI_execute(sql.data, false, 0);

        /* Insert metadata row (segment_id = -1, small params only) */
        {
            bytea *gs_bytes = (bytea *)palloc(VARHDRSZ + sizeof(float) * n_groups);
            bytea *mins_bytes = (bytea *)palloc(VARHDRSZ + sizeof(float) * dim);
            bytea *scales_bytes = (bytea *)palloc(VARHDRSZ + sizeof(float) * dim);

            SET_VARSIZE(gs_bytes, VARHDRSZ + sizeof(float) * n_groups);
            memcpy(VARDATA(gs_bytes), group_scales, sizeof(float) * n_groups);
            SET_VARSIZE(mins_bytes, VARHDRSZ + sizeof(float) * dim);
            memcpy(VARDATA(mins_bytes), sq8_mins, sizeof(float) * dim);
            SET_VARSIZE(scales_bytes, VARHDRSZ + sizeof(float) * dim);
            memcpy(VARDATA(scales_bytes), sq8_scales, sizeof(float) * dim);

            {
                Datum vals[9];
                bool nls[9];
                Oid types[9] = {INT4OID, INT4OID, INT4OID, BYTEAOID, BYTEAOID, BYTEAOID, INT4OID, INT4OID, INT4OID};
                memset(nls, false, sizeof(nls));
                vals[0] = Int32GetDatum(-1);        /* segment_id */
                vals[1] = Int32GetDatum(0);         /* row_start (unused) */
                vals[2] = Int32GetDatum(0);         /* row_count (unused) */
                vals[3] = PointerGetDatum(gs_bytes);
                vals[4] = PointerGetDatum(mins_bytes);
                vals[5] = PointerGetDatum(scales_bytes);
                vals[6] = Int32GetDatum(dim);
                vals[7] = Int32GetDatum(seed);
                vals[8] = Int32GetDatum(group_size);

                resetStringInfo(&sql);
                appendStringInfo(&sql,
                    "INSERT INTO %s (segment_id, row_start, row_count, "
                    "meta_group_scales, meta_sq8_mins, meta_sq8_scales, "
                    "meta_dim, meta_seed, meta_group_size) "
                    "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                    quote_identifier(sidecar));
                ret = SPI_execute_with_args(sql.data, 9, types, vals, NULL, false, 0);
                if (ret != SPI_OK_INSERT)
                    ereport(WARNING, (errmsg("fh_build: metadata insert failed")));
            }
        }

        /* Insert data chunks */
        for (seg = 0; seg < n_chunks; seg++)
        {
            int     rstart = seg * FH_CHUNK_ROWS;
            int     rcount = Min(FH_CHUNK_ROWS, n_rows - rstart);
            Size    pt_size = (Size)n_bytes * rcount;
            Size    sq_size = (Size)dim * rcount;
            Size    norm_size = sizeof(float) * rcount;
            bytea  *pt_bytes, *sq_bytes, *norm_bytes;

            /* Build transposed packed_t for this chunk */
            pt_bytes = (bytea *)palloc(VARHDRSZ + pt_size);
            SET_VARSIZE(pt_bytes, VARHDRSZ + pt_size);
            {
                /* packed_t is already fully transposed: packed_t[byte_idx * n_rows + row] */
                /* Extract chunk: for each byte_idx, copy rows [rstart..rstart+rcount) */
                uint8 *dst = (uint8 *)VARDATA(pt_bytes);
                for (j = 0; j < n_bytes; j++)
                    memcpy(dst + j * rcount,
                           packed_t + (Size)j * n_rows + rstart,
                           rcount);
            }

            sq_bytes = (bytea *)palloc(VARHDRSZ + sq_size);
            SET_VARSIZE(sq_bytes, VARHDRSZ + sq_size);
            memcpy(VARDATA(sq_bytes), sq8_codes + (Size)rstart * dim, sq_size);

            norm_bytes = (bytea *)palloc(VARHDRSZ + norm_size);
            SET_VARSIZE(norm_bytes, VARHDRSZ + norm_size);
            memcpy(VARDATA(norm_bytes), norms + rstart, norm_size);

            {
                Datum vals[6];
                bool nls[6];
                Oid types[6] = {INT4OID, INT4OID, INT4OID, BYTEAOID, BYTEAOID, BYTEAOID};
                memset(nls, false, sizeof(nls));
                vals[0] = Int32GetDatum(seg);
                vals[1] = Int32GetDatum(rstart);
                vals[2] = Int32GetDatum(rcount);
                vals[3] = PointerGetDatum(pt_bytes);
                vals[4] = PointerGetDatum(sq_bytes);
                vals[5] = PointerGetDatum(norm_bytes);

                resetStringInfo(&sql);
                appendStringInfo(&sql,
                    "INSERT INTO %s (segment_id, row_start, row_count, "
                    "packed_t, sq8_codes, meta_norms) "
                    "VALUES ($1,$2,$3,$4,$5,$6)",
                    quote_identifier(sidecar));
                ret = SPI_execute_with_args(sql.data, 6, types, vals, NULL, false, 0);
                if (ret != SPI_OK_INSERT)
                    ereport(WARNING, (errmsg("fh_build: chunk %d insert failed", seg)));
            }

            pfree(pt_bytes);
            pfree(sq_bytes);
            pfree(norm_bytes);

            if ((seg + 1) % 5 == 0 || seg == n_chunks - 1)
                elog(NOTICE, "fh_build: inserted chunk %d/%d", seg + 1, n_chunks);
        }

        SPI_finish();
    }

    PG_RETURN_INT32(n_rows);
}


/* ================================================================
 * PG FUNCTION: flashhadamard_scan
 *
 * flashhadamard_scan(
 *   sidecar_tbl text,
 *   query       vector,
 *   k           int4 DEFAULT 10,
 *   shortlist_m int4 DEFAULT 12
 * ) RETURNS TABLE(row_id int4, score float8)
 * ================================================================ */

PG_FUNCTION_INFO_V1(flashhadamard_scan);
Datum
flashhadamard_scan(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    char       *sidecar = text_to_cstring(PG_GETARG_TEXT_PP(0));
    Datum       query_datum = PG_GETARG_DATUM(1);
    int         k = PG_GETARG_INT32(2);
    int         shortlist_m = PG_GETARG_INT32(3);

    StringInfoData sql;
    int         ret;
    int         dim, seed, group_size_val, n_bytes, n_groups;
    FHParams    params;
    float      *query_vec;
    float      *rotated_q;
    float      *coeffs;
    float      *byte_tables;
    int32      *global_top_ids;
    float      *global_top_scores;
    int         global_filled = 0, global_min_pos = 0;
    float       global_min_score = -FLT_MAX;
    int         n_chunks, seg;
    int         i, j;
    TupleDesc   tupdesc;
    Tuplestorestate *tupstore;

    InitMaterializedSRF(fcinfo, 0);
    tupstore = rsinfo->setResult;
    tupdesc = rsinfo->setDesc;

    SPI_connect();

    /* Load metadata (segment_id = -1) */
    initStringInfo(&sql);
    appendStringInfo(&sql,
        "SELECT meta_group_scales, meta_sq8_mins, meta_sq8_scales, "
        "meta_dim, meta_seed, meta_group_size "
        "FROM %s WHERE segment_id = -1",
        quote_identifier(sidecar));
    ret = SPI_execute(sql.data, true, 1);
    if (ret != SPI_OK_SELECT || SPI_processed != 1)
    {
        SPI_finish();
        ereport(ERROR, (errmsg("flashhadamard_scan: metadata not found")));
    }

    {
        HeapTuple tup = SPI_tuptable->vals[0];
        TupleDesc tdesc = SPI_tuptable->tupdesc;
        bool isnull;
        bytea *gs_bytes, *mins_bytes, *scales_bytes;

        dim = DatumGetInt32(SPI_getbinval(tup, tdesc, 4, &isnull));
        seed = DatumGetInt32(SPI_getbinval(tup, tdesc, 5, &isnull));
        group_size_val = DatumGetInt32(SPI_getbinval(tup, tdesc, 6, &isnull));
        n_bytes = (dim + 1) / 2;
        n_groups = (dim + group_size_val - 1) / group_size_val;

        gs_bytes = DatumGetByteaP(SPI_getbinval(tup, tdesc, 1, &isnull));
        mins_bytes = DatumGetByteaP(SPI_getbinval(tup, tdesc, 2, &isnull));
        scales_bytes = DatumGetByteaP(SPI_getbinval(tup, tdesc, 3, &isnull));

        params.dim = dim;
        params.group_size = group_size_val;
        params.seed = seed;
        params.n_groups = n_groups;
        params.perm = palloc(sizeof(int) * dim);
        params.signs = palloc(sizeof(float) * dim);
        params.centers = (float *)fh_lloyd_max_16;
        params.group_scales = palloc(sizeof(float) * n_groups);
        memcpy(params.group_scales, VARDATA_ANY(gs_bytes), sizeof(float) * n_groups);
        fh_generate_perm_signs(dim, seed, params.perm, params.signs);

        /* SQ8 params for rerank */
        /* Store in params struct extension or separate vars */
    }

    /* Extract + rotate query (once) */
    {
        bytea *raw = DatumGetByteaP(query_datum);
        int query_dim = *(uint16 *)(VARDATA(raw));
        float *qdata = (float *)(VARDATA(raw) + 2 * sizeof(uint16));
        float inv_sqrt_dim = 1.0f / sqrtf((float)dim);

        if (query_dim != dim)
            ereport(ERROR, (errmsg("flashhadamard_scan: query dim %d != %d", query_dim, dim)));

        query_vec = palloc(sizeof(float) * dim);
        memcpy(query_vec, qdata, sizeof(float) * dim);

        /* Rotate query + build coefficients + byte tables */
        rotated_q = palloc(sizeof(float) * dim);
        fh_rotate_vec(query_vec, rotated_q, &params);

        coeffs = palloc(sizeof(float) * dim);
        {
            float *expanded = palloc(sizeof(float) * dim);
            int g;
            for (g = 0; g < n_groups; g++)
            {
                int gstart = g * group_size_val;
                int gend = Min(gstart + group_size_val, dim);
                int dd;
                for (dd = gstart; dd < gend; dd++)
                    expanded[dd] = params.group_scales[g];
            }
            for (i = 0; i < dim; i++)
                coeffs[i] = rotated_q[i] * expanded[i] * inv_sqrt_dim;
            pfree(expanded);
        }

        byte_tables = palloc(sizeof(float) * n_bytes * 256);
        fh_build_byte_tables(coeffs, params.centers, dim, byte_tables);
    }

    /* Global top-k heap (shortlist_m candidates across all chunks) */
    global_top_ids = palloc(sizeof(int32) * shortlist_m);
    global_top_scores = palloc(sizeof(float) * shortlist_m);

    /* Count chunks */
    resetStringInfo(&sql);
    appendStringInfo(&sql,
        "SELECT count(*) FROM %s WHERE segment_id >= 0",
        quote_identifier(sidecar));
    ret = SPI_execute(sql.data, true, 1);
    n_chunks = (ret == SPI_OK_SELECT && SPI_processed > 0)
        ? DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &(bool){false}))
        : 0;

    /* Process each chunk: score + merge into global top-k */
    for (seg = 0; seg < n_chunks; seg++)
    {
        bytea *pt_bytes, *norm_bytes;
        int rstart, rcount;
        float *chunk_scores;
        bool isnull;

        resetStringInfo(&sql);
        appendStringInfo(&sql,
            "SELECT row_start, row_count, packed_t, meta_norms "
            "FROM %s WHERE segment_id = %d",
            quote_identifier(sidecar), seg);
        ret = SPI_execute(sql.data, true, 1);
        if (ret != SPI_OK_SELECT || SPI_processed != 1)
            continue;

        rstart = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
        rcount = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull));
        pt_bytes = DatumGetByteaP(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3, &isnull));
        norm_bytes = DatumGetByteaP(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4, &isnull));

        /* Score this chunk */
        chunk_scores = palloc(sizeof(float) * rcount);
        fh_packed_score_t((uint8 *)VARDATA_ANY(pt_bytes), byte_tables,
                           (float *)VARDATA_ANY(norm_bytes),
                           rcount, n_bytes, chunk_scores);

        /* Merge into global top-k */
        for (i = 0; i < rcount; i++)
            fh_topk_insert(chunk_scores[i], rstart + i,
                         global_top_scores, global_top_ids, shortlist_m,
                         &global_filled, &global_min_pos, &global_min_score);

        pfree(chunk_scores);
    }

    /* SQ8 rerank if shortlist_m > k */
    if (shortlist_m > k && global_filled > k)
    {
        /* Load SQ8 params from metadata */
        bytea *mins_bytes, *scales_bytes;
        float *sq8_mins_local, *sq8_scales_local;
        float *q_norm;
        float *rerank_scores;
        int32 *final_ids;
        float *final_scores;
        int final_filled = 0, final_min_pos = 0;
        float final_min_score = -FLT_MAX;

        resetStringInfo(&sql);
        appendStringInfo(&sql,
            "SELECT meta_sq8_mins, meta_sq8_scales FROM %s WHERE segment_id = -1",
            quote_identifier(sidecar));
        ret = SPI_execute(sql.data, true, 1);
        {
            bool isnull;
            mins_bytes = DatumGetByteaP(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
            scales_bytes = DatumGetByteaP(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull));
        }
        sq8_mins_local = (float *)VARDATA_ANY(mins_bytes);
        sq8_scales_local = (float *)VARDATA_ANY(scales_bytes);

        /* Normalize query for rerank */
        q_norm = palloc(sizeof(float) * dim);
        {
            float norm = 0.0f;
            for (i = 0; i < dim; i++) norm += query_vec[i] * query_vec[i];
            norm = sqrtf(norm);
            if (norm < 1e-12f) norm = 1e-12f;
            for (i = 0; i < dim; i++) q_norm[i] = query_vec[i] / norm;
        }

        /* For each shortlist candidate, load its SQ8 codes and rerank */
        rerank_scores = palloc(sizeof(float) * global_filled);
        final_ids = palloc(sizeof(int32) * k);
        final_scores = palloc(sizeof(float) * k);

        for (i = 0; i < global_filled; i++)
        {
            int row = global_top_ids[i];
            /* Find which chunk this row belongs to */
            int chunk_id = row / FH_CHUNK_ROWS;
            int chunk_offset = row % FH_CHUNK_ROWS;
            bytea *sq_bytes;
            uint8 *row_sq8;
            float dot = 0.0f;

            resetStringInfo(&sql);
            appendStringInfo(&sql,
                "SELECT sq8_codes FROM %s WHERE segment_id = %d",
                quote_identifier(sidecar), chunk_id);
            ret = SPI_execute(sql.data, true, 1);
            if (ret != SPI_OK_SELECT || SPI_processed != 1)
            {
                rerank_scores[i] = -FLT_MAX;
                continue;
            }
            {
                bool isnull;
                sq_bytes = DatumGetByteaP(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
            }
            row_sq8 = (uint8 *)VARDATA_ANY(sq_bytes) + (Size)chunk_offset * dim;

            for (j = 0; j < dim; j++)
            {
                float decoded = (float)row_sq8[j] * sq8_scales_local[j] + sq8_mins_local[j];
                dot += decoded * q_norm[j];
            }
            rerank_scores[i] = dot;
        }

        /* Final top-k from reranked scores */
        for (i = 0; i < global_filled; i++)
            fh_topk_insert(rerank_scores[i], global_top_ids[i],
                         final_scores, final_ids, k,
                         &final_filled, &final_min_pos, &final_min_score);

        /* Sort final results */
        for (i = 0; i < final_filled; i++)
            for (j = i + 1; j < final_filled; j++)
                if (final_scores[j] > final_scores[i])
                {
                    float ts = final_scores[i]; final_scores[i] = final_scores[j]; final_scores[j] = ts;
                    int32 ti = final_ids[i]; final_ids[i] = final_ids[j]; final_ids[j] = ti;
                }

        SPI_finish();

        for (i = 0; i < final_filled; i++)
        {
            Datum vals[2];
            bool nulls[2] = {false, false};
            vals[0] = Int32GetDatum(final_ids[i]);
            vals[1] = Float8GetDatum((float8)final_scores[i]);
            tuplestore_putvalues(tupstore, tupdesc, vals, nulls);
        }
    }
    else
    {
        /* No rerank: sort and emit shortlist directly */
        for (i = 0; i < global_filled; i++)
            for (j = i + 1; j < global_filled; j++)
                if (global_top_scores[j] > global_top_scores[i])
                {
                    float ts = global_top_scores[i]; global_top_scores[i] = global_top_scores[j]; global_top_scores[j] = ts;
                    int32 ti = global_top_ids[i]; global_top_ids[i] = global_top_ids[j]; global_top_ids[j] = ti;
                }

        SPI_finish();

        {
            int out_k = Min(global_filled, k);
            for (i = 0; i < out_k; i++)
            {
                Datum vals[2];
                bool nulls[2] = {false, false};
                vals[0] = Int32GetDatum(global_top_ids[i]);
                vals[1] = Float8GetDatum((float8)global_top_scores[i]);
                tuplestore_putvalues(tupstore, tupdesc, vals, nulls);
            }
        }
    }

    PG_RETURN_NULL();
}


/* ================================================================
 * PG FUNCTION: flashhadamard_store_build
 *
 * Builds file-based segment store (AM-lite, no TOAST in hot path).
 * Uses streaming 3-pass build + fh_store_write.
 * ================================================================ */

PG_FUNCTION_INFO_V1(flashhadamard_store_build);
Datum
flashhadamard_store_build(PG_FUNCTION_ARGS)
{
    Oid         source_oid = PG_GETARG_OID(0);
    char       *embed_col  = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char       *store_path = text_to_cstring(PG_GETARG_TEXT_PP(2));
    int         seed       = PG_GETARG_INT32(3);
    int         group_size = PG_GETARG_INT32(4);
    char       *source_name;
    int         ret, n_rows = 0, dim = 0, n_bytes, n_groups;
    int         i, j, d;
    float      *group_scales = NULL;
    float      *sq8_mins = NULL, *sq8_scales = NULL;
    float      *norms = NULL;
    uint8      *packed_codes = NULL, *packed_t = NULL, *sq8_codes = NULL;
    float      *centroids = NULL;
    int         n_seg = 0;
    FHParams    params;

    source_name = get_rel_name(source_oid);
    if (!source_name)
        ereport(ERROR, (errmsg("flashhadamard_store_build: table not found")));

    /* Streaming 3-pass encode (same as flashhadamard_build) */
    {
        Relation        rel;
        TupleDesc       td;
        TableScanDesc   scan;
        TupleTableSlot *slot;
        AttrNumber      embed_attno;
        bool            is_halfvec = false;
        Snapshot        snapshot;
        int             row_idx;
        float          *vec_buf, *rot_buf, *expanded;
        double         *group_sum_sq;
        int            *group_count_arr;
        Oid             embed_typid;

        rel = table_open(source_oid, AccessShareLock);
        td = RelationGetDescr(rel);
        embed_attno = get_attnum(source_oid, embed_col);
        if (embed_attno == InvalidAttrNumber)
        { table_close(rel, AccessShareLock); ereport(ERROR, (errmsg("column \"%s\" not found", embed_col))); }

        embed_typid = TupleDescAttr(td, embed_attno - 1)->atttypid;
        { char *tn = format_type_be(embed_typid); if (tn && (strstr(tn,"half")||strstr(tn,"hsvec"))) is_halfvec = true; if (tn) pfree(tn); }

        snapshot = GetTransactionSnapshot();
        slot = table_slot_create(rel, NULL);

        /* Pass 1: count + dim + group RMS */
        scan = table_beginscan(rel, snapshot, 0, NULL);
        while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
        {
            if (dim == 0)
            { bool isn; Datum dv = slot_getattr(slot, embed_attno, &isn);
              if (!isn) { Svec *sv = (Svec*)PG_DETOAST_DATUM(dv); dim = sv->dim; if (sv!=(Svec*)DatumGetPointer(dv)) pfree(sv); } }
            n_rows++;
            ExecClearTuple(slot);
        }
        table_endscan(scan);
        if (n_rows == 0 || dim == 0) { ExecDropSingleTupleTableSlot(slot); table_close(rel, AccessShareLock); PG_RETURN_INT32(0); }

        n_bytes = (dim + 1) / 2;
        n_groups = (dim + group_size - 1) / group_size;
        params.dim = dim; params.group_size = group_size; params.seed = seed;
        params.n_groups = n_groups;
        params.perm = palloc(sizeof(int)*dim); params.signs = palloc(sizeof(float)*dim);
        params.centers = (float*)fh_lloyd_max_16;
        fh_generate_perm_signs(dim, seed, params.perm, params.signs);

        vec_buf = palloc(sizeof(float)*dim); rot_buf = palloc(sizeof(float)*dim);
        group_sum_sq = palloc0(sizeof(double)*n_groups);
        group_count_arr = palloc0(sizeof(int)*n_groups);

        /* Pass 1b: accumulate group RMS stats */
        { float sqrt_dim = sqrtf((float)dim); int g;
          scan = table_beginscan(rel, snapshot, 0, NULL);
          while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
          { bool isn; Datum dv = slot_getattr(slot, embed_attno, &isn); float norm = 0;
            if (isn) { ExecClearTuple(slot); continue; }
            if (is_halfvec) { Hsvec *hv=(Hsvec*)PG_DETOAST_DATUM(dv); for(d=0;d<Min(hv->dim,dim);d++){vec_buf[d]=HalfToFloat4(hv->x[d]);norm+=vec_buf[d]*vec_buf[d];} for(d=Min(hv->dim,dim);d<dim;d++)vec_buf[d]=0; if(hv!=(Hsvec*)DatumGetPointer(dv))pfree(hv); }
            else { Svec *sv=(Svec*)PG_DETOAST_DATUM(dv); memcpy(vec_buf,sv->x,sizeof(float)*Min(sv->dim,dim)); for(d=Min(sv->dim,dim);d<dim;d++)vec_buf[d]=0; for(d=0;d<Min(sv->dim,dim);d++)norm+=vec_buf[d]*vec_buf[d]; if(sv!=(Svec*)DatumGetPointer(dv))pfree(sv); }
            norm=sqrtf(norm); if(norm<1e-12f)norm=1e-12f; for(d=0;d<dim;d++)vec_buf[d]/=norm;
            fh_rotate_vec(vec_buf, rot_buf, &params); for(d=0;d<dim;d++)rot_buf[d]*=sqrt_dim;
            for(g=0;g<n_groups;g++){int gs=g*group_size,ge=Min(gs+group_size,dim); for(d=gs;d<ge;d++){group_sum_sq[g]+=(double)rot_buf[d]*rot_buf[d];group_count_arr[g]++;}}
            ExecClearTuple(slot);
          }
          table_endscan(scan);
          group_scales = palloc(sizeof(float)*n_groups);
          for(g=0;g<n_groups;g++){group_scales[g]=(float)sqrt(group_sum_sq[g]/Max(group_count_arr[g],1)); if(group_scales[g]<1e-4f)group_scales[g]=1e-4f;}
          params.group_scales = group_scales;
          pfree(group_sum_sq); pfree(group_count_arr);
        }

        expanded = palloc(sizeof(float)*dim);
        { int g; for(g=0;g<n_groups;g++){int gs=g*group_size,ge=Min(gs+group_size,dim); for(d=gs;d<ge;d++)expanded[d]=group_scales[g];} }

        /* Allocate output buffers */
        packed_codes = MemoryContextAllocHuge(CurrentMemoryContext, (Size)n_rows*n_bytes);
        norms = palloc(sizeof(float)*n_rows);
        sq8_mins = palloc(sizeof(float)*dim); sq8_scales = palloc(sizeof(float)*dim);
        for(d=0;d<dim;d++){sq8_mins[d]=FLT_MAX; sq8_scales[d]=-FLT_MAX;}

        /* Segment centroid accumulators */
        {
            int *seg_counts;
            n_seg = (n_rows + FH_SEGMENT_SIZE - 1) / FH_SEGMENT_SIZE;
            centroids = palloc0(sizeof(float) * (Size)n_seg * dim);
            seg_counts = palloc0(sizeof(int) * n_seg);

        /* Pass 2: rotate + quantize + pack + SQ8 min/max + centroid accumulation */
        elog(NOTICE, "fh_store_build: pass 2 encoding %d vectors (%d segments)", n_rows, n_seg);
        { float sqrt_dim = sqrtf((float)dim);
          scan = table_beginscan(rel, snapshot, 0, NULL);
          row_idx = 0;
          while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
          { bool isn; Datum dv = slot_getattr(slot, embed_attno, &isn); float norm = 0;
            if (isn) { memset(packed_codes+(Size)row_idx*n_bytes,0,n_bytes); norms[row_idx]=1e-12f; row_idx++; ExecClearTuple(slot); continue; }
            if (is_halfvec) { Hsvec *hv=(Hsvec*)PG_DETOAST_DATUM(dv); for(d=0;d<Min(hv->dim,dim);d++){vec_buf[d]=HalfToFloat4(hv->x[d]);norm+=vec_buf[d]*vec_buf[d];} for(d=Min(hv->dim,dim);d<dim;d++)vec_buf[d]=0; if(hv!=(Hsvec*)DatumGetPointer(dv))pfree(hv); }
            else { Svec *sv=(Svec*)PG_DETOAST_DATUM(dv); memcpy(vec_buf,sv->x,sizeof(float)*Min(sv->dim,dim)); for(d=Min(sv->dim,dim);d<dim;d++)vec_buf[d]=0; for(d=0;d<Min(sv->dim,dim);d++)norm+=vec_buf[d]*vec_buf[d]; if(sv!=(Svec*)DatumGetPointer(dv))pfree(sv); }
            norm=sqrtf(norm); if(norm<1e-12f)norm=1e-12f; for(d=0;d<dim;d++)vec_buf[d]/=norm; norms[row_idx]=norm;
            for(d=0;d<dim;d++){if(vec_buf[d]<sq8_mins[d])sq8_mins[d]=vec_buf[d]; if(vec_buf[d]>sq8_scales[d])sq8_scales[d]=vec_buf[d];}
            fh_rotate_vec(vec_buf, rot_buf, &params); for(d=0;d<dim;d++)rot_buf[d]*=sqrt_dim;
            /* Accumulate centroid in equalized rotated space (rot/expanded, same as quantizer input) */
            { int seg_id = row_idx / FH_SEGMENT_SIZE;
              float *cen = centroids + (Size)seg_id * dim;
              for(d=0;d<dim;d++) cen[d] += rot_buf[d] / expanded[d];
              seg_counts[seg_id]++; }
            { uint8 *rp = packed_codes+(Size)row_idx*n_bytes; memset(rp,0,n_bytes);
              for(d=0;d<dim;d++){float eq=rot_buf[d]/expanded[d]; int code=fh_digitize(eq); int bi=d/2;
              if(d%2==0)rp[bi]|=(uint8)(code&0x0F); else rp[bi]|=(uint8)((code&0x0F)<<4);} }
            row_idx++; ExecClearTuple(slot);
          }
          n_rows = row_idx;
          table_endscan(scan);
        }

        /* Finalize centroids: divide by count, L2 normalize */
        { int s;
          n_seg = (n_rows + FH_SEGMENT_SIZE - 1) / FH_SEGMENT_SIZE;
          for (s = 0; s < n_seg; s++)
          { float *cen = centroids + (Size)s * dim;
            float cnorm = 0;
            int cnt = Max(seg_counts[s], 1);
            for(d=0;d<dim;d++) { cen[d] /= cnt; cnorm += cen[d]*cen[d]; }
            cnorm = sqrtf(cnorm);
            if (cnorm > 1e-12f) for(d=0;d<dim;d++) cen[d] /= cnorm;
          }
          pfree(seg_counts);
        }
        } /* end segment centroid accumulators block */
        for(d=0;d<dim;d++){float r=sq8_scales[d]-sq8_mins[d]; if(r<1e-8f)r=1e-8f; sq8_scales[d]=r/255.0f;}

        /* Pass 3: SQ8 encode */
        sq8_codes = MemoryContextAllocHuge(CurrentMemoryContext, (Size)n_rows*dim);
        elog(NOTICE, "fh_store_build: pass 3 SQ8 encoding");
        { scan = table_beginscan(rel, snapshot, 0, NULL);
          row_idx = 0;
          while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
          { bool isn; Datum dv = slot_getattr(slot, embed_attno, &isn); float norm;
            if (isn) { memset(sq8_codes+(Size)row_idx*dim,0,dim); row_idx++; ExecClearTuple(slot); continue; }
            norm=0;
            if (is_halfvec) { Hsvec *hv=(Hsvec*)PG_DETOAST_DATUM(dv); for(d=0;d<Min(hv->dim,dim);d++){vec_buf[d]=HalfToFloat4(hv->x[d]);norm+=vec_buf[d]*vec_buf[d];} for(d=Min(hv->dim,dim);d<dim;d++)vec_buf[d]=0; if(hv!=(Hsvec*)DatumGetPointer(dv))pfree(hv); }
            else { Svec *sv=(Svec*)PG_DETOAST_DATUM(dv); memcpy(vec_buf,sv->x,sizeof(float)*Min(sv->dim,dim)); for(d=Min(sv->dim,dim);d<dim;d++)vec_buf[d]=0; for(d=0;d<Min(sv->dim,dim);d++)norm+=vec_buf[d]*vec_buf[d]; if(sv!=(Svec*)DatumGetPointer(dv))pfree(sv); }
            norm=sqrtf(norm); if(norm<1e-12f)norm=1e-12f; for(d=0;d<dim;d++)vec_buf[d]/=norm;
            { uint8 *rsq = sq8_codes+(Size)row_idx*dim;
              for(d=0;d<dim;d++){int c=(int)roundf((vec_buf[d]-sq8_mins[d])/sq8_scales[d]); if(c<0)c=0; if(c>255)c=255; rsq[d]=(uint8)c;} }
            row_idx++; ExecClearTuple(slot);
          }
          table_endscan(scan);
        }

        /* Transpose packed codes */
        packed_t = MemoryContextAllocHuge(CurrentMemoryContext, (Size)n_bytes*n_rows);
        for(i=0;i<n_rows;i++) for(j=0;j<n_bytes;j++) packed_t[j*n_rows+i] = packed_codes[i*n_bytes+j];
        pfree(packed_codes); packed_codes = NULL;

        pfree(vec_buf); pfree(rot_buf); pfree(expanded);
        ExecDropSingleTupleTableSlot(slot);
        table_close(rel, AccessShareLock);
    }

    /* Write to file store */
    elog(NOTICE, "fh_store_build: writing store '%s' (%d rows, %d dim)", store_path, n_rows, dim);
    {
        FHMetaPageDataV2 meta;
        memset(&meta, 0, sizeof(meta));
        meta.magic = FH_STORE_MAGIC;
        meta.version = FH_STORE_VERSION;
        meta.dim = dim;
        meta.n_rows = n_rows;
        meta.n_bytes = n_bytes;
        meta.group_size = group_size;
        meta.n_groups = n_groups;
        meta.seed = seed;
        memcpy(meta.centers, fh_lloyd_max_16, sizeof(float) * FH_MAX_CENTERS);
        memcpy(meta.group_scales, group_scales, sizeof(float) * Min(n_groups, FH_MAX_GROUPS));

        meta.n_segments = n_seg;
        meta.segment_size = FH_SEGMENT_SIZE;

        ret = fh_store_write(store_path, &meta, sq8_mins, sq8_scales,
                              packed_t, (Size)n_bytes * n_rows,
                              sq8_codes, (Size)n_rows * dim,
                              norms, n_rows,
                              centroids, n_seg);
        if (ret != 0)
            ereport(ERROR, (errmsg("fh_store_build: write failed")));
    }

    elog(NOTICE, "fh_store_build: done");
    PG_RETURN_INT32(n_rows);
}


/* ================================================================
 * PG FUNCTION: flashhadamard_store_scan
 *
 * Scans file-based segment store directly (no SPI/TOAST).
 * ================================================================ */

PG_FUNCTION_INFO_V1(flashhadamard_store_scan);
Datum
flashhadamard_store_scan(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    char       *store_path = text_to_cstring(PG_GETARG_TEXT_PP(0));
    Datum       query_datum = PG_GETARG_DATUM(1);
    int         k = PG_GETARG_INT32(2);
    int         shortlist_m = PG_GETARG_INT32(3);
    int         seed = PG_GETARG_INT32(4);
    int         group_size = PG_GETARG_INT32(5);

    float      *query_vec;
    int         dim;
    FHParams    params;
    int32      *out_ids;
    float      *out_scores;
    int         n_results, i;
    TupleDesc   tupdesc;
    Tuplestorestate *tupstore;

    InitMaterializedSRF(fcinfo, 0);
    tupstore = rsinfo->setResult;
    tupdesc = rsinfo->setDesc;

    /* Extract query vector */
    {
        bytea *raw = DatumGetByteaP(query_datum);
        int query_dim = *(uint16 *)(VARDATA(raw));
        float *qdata = (float *)(VARDATA(raw) + 2 * sizeof(uint16));
        dim = query_dim;
        query_vec = palloc(sizeof(float) * dim);
        memcpy(query_vec, qdata, sizeof(float) * dim);
    }

    /* Build params from seed + cached meta (with dim validation) */
    {
        FHStoreCache *cache = fh_store_cache_get(store_path);
        int n_groups;
        int store_dim;
        if (!cache)
            ereport(ERROR, (errmsg("flashhadamard_store_scan: cannot open '%s'", store_path)));

        store_dim = cache->meta.dim;
        if (dim != store_dim)
            ereport(ERROR, (errmsg("flashhadamard_store_scan: query dim %d != store dim %d", dim, store_dim)));

        n_groups = (dim + group_size - 1) / group_size;
        params.dim = dim;
        params.group_size = group_size;
        params.seed = seed;
        params.n_groups = n_groups;
        params.perm = palloc(sizeof(int) * dim);
        params.signs = palloc(sizeof(float) * dim);
        params.centers = (float *)fh_lloyd_max_16;
        params.group_scales = palloc(sizeof(float) * n_groups);
        fh_generate_perm_signs(dim, seed, params.perm, params.signs);
        memcpy(params.group_scales, cache->meta.group_scales,
               sizeof(float) * Min(n_groups, FH_MAX_GROUPS));
    }

    out_ids = palloc(sizeof(int32) * k);
    out_scores = palloc(sizeof(float) * k);
    n_results = fh_store_scan(store_path, &params, query_vec, k, shortlist_m,
                               out_ids, out_scores);

    for (i = 0; i < n_results; i++)
    {
        Datum vals[2];
        bool nulls[2] = {false, false};
        vals[0] = Int32GetDatum(out_ids[i]);
        vals[1] = Float8GetDatum((float8)out_scores[i]);
        tuplestore_putvalues(tupstore, tupdesc, vals, nulls);
    }

    PG_RETURN_NULL();
}
