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
static void
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

    /* Step 1: Read vectors via direct table AM scan (streaming, no SPI materialization) */
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
        int             pass1_count = 0;

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

        /* Pass 1: count rows + get dim */
        snapshot = GetTransactionSnapshot();
        slot = table_slot_create(rel, NULL);
        scan = table_beginscan(rel, snapshot, 0, NULL);
        dim = 0;
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
            pass1_count++;
            ExecClearTuple(slot);
        }
        table_endscan(scan);

        n_rows = pass1_count;
        if (n_rows == 0 || dim == 0)
        {
            ExecDropSingleTupleTableSlot(slot);
            table_close(rel, AccessShareLock);
            PG_RETURN_INT32(0);
        }

        n_bytes = (dim + 1) / 2;
        n_groups = (dim + group_size - 1) / group_size;

        /* Allocate buffers */
        all_vecs = (float *) MemoryContextAllocHuge(CurrentMemoryContext,
                                                      sizeof(float) * (Size)n_rows * dim);
        norms = (float *) palloc(sizeof(float) * n_rows);

        /* Pass 2: read vectors into buffer */
        scan = table_beginscan(rel, snapshot, 0, NULL);
        row_idx = 0;
        while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
        {
            bool isnull;
            Datum d_val = slot_getattr(slot, embed_attno, &isnull);
            float *v = all_vecs + (Size)row_idx * dim;
            float norm = 0.0f;

            if (isnull)
            {
                memset(v, 0, sizeof(float) * dim);
                norms[row_idx] = 1e-12f;
                row_idx++;
                ExecClearTuple(slot);
                continue;
            }

            if (is_halfvec)
            {
                Hsvec *hv = (Hsvec *) PG_DETOAST_DATUM(d_val);
                int vdim = Min(hv->dim, dim);
                for (d = 0; d < vdim; d++)
                {
                    v[d] = HalfToFloat4(hv->x[d]);
                    norm += v[d] * v[d];
                }
                for (d = vdim; d < dim; d++)
                    v[d] = 0.0f;
                if (hv != (Hsvec *) DatumGetPointer(d_val))
                    pfree(hv);
            }
            else
            {
                Svec *sv = (Svec *) PG_DETOAST_DATUM(d_val);
                int vdim = Min(sv->dim, dim);
                memcpy(v, sv->x, sizeof(float) * vdim);
                for (d = vdim; d < dim; d++)
                    v[d] = 0.0f;
                for (d = 0; d < vdim; d++)
                    norm += v[d] * v[d];
                if (sv != (Svec *) DatumGetPointer(d_val))
                    pfree(sv);
            }

            norms[row_idx] = sqrtf(norm);
            if (norms[row_idx] < 1e-12f)
                norms[row_idx] = 1e-12f;
            for (d = 0; d < dim; d++)
                v[d] /= norms[row_idx];

            row_idx++;
            ExecClearTuple(slot);
        }
        n_rows = row_idx;
        table_endscan(scan);
        ExecDropSingleTupleTableSlot(slot);
        table_close(rel, AccessShareLock);
    }

    /* Step 2: Build rotation params */
    params.dim = dim;
    params.group_size = group_size;
    params.seed = seed;
    params.n_groups = n_groups;
    params.perm = palloc(sizeof(int) * dim);
    params.signs = palloc(sizeof(float) * dim);
    params.centers = (float *)fh_lloyd_max_16; /* const, don't free */
    fh_generate_perm_signs(dim, seed, params.perm, params.signs);

    /* Step 3: Rotate all vectors */
    rotated = MemoryContextAllocHuge(CurrentMemoryContext, sizeof(float) * (Size)n_rows * dim);
    {
        float inv_sqrt_dim = 1.0f / sqrtf((float)dim);
        float sqrt_dim = sqrtf((float)dim);

        for (i = 0; i < n_rows; i++)
        {
            fh_rotate_vec(all_vecs + i * dim, rotated + i * dim, &params);
            /* Scale by sqrt(dim) as in Python harness */
            for (d = 0; d < dim; d++)
                rotated[i * dim + d] *= sqrt_dim;
        }
    }

    /* Step 4: Group RMS scales */
    group_scales = palloc0(sizeof(float) * n_groups);
    {
        for (j = 0; j < n_groups; j++)
        {
            int start = j * group_size;
            int end = Min(start + group_size, dim);
            double sum_sq = 0.0;
            int count = 0;

            for (i = 0; i < n_rows; i++)
                for (d = start; d < end; d++)
                {
                    float val = rotated[i * dim + d];
                    sum_sq += (double)val * val;
                    count++;
                }
            group_scales[j] = (float)sqrt(sum_sq / Max(count, 1));
            if (group_scales[j] < 1e-4f) group_scales[j] = 1e-4f;
        }
    }
    params.group_scales = group_scales;

    /* Step 5: Equalize + quantize → packed codes */
    packed_codes = MemoryContextAllocHuge(CurrentMemoryContext, (Size)n_rows * n_bytes);
    {
        float *expanded = palloc(sizeof(float) * dim);
        int g;
        for (g = 0; g < n_groups; g++)
        {
            int start = g * group_size;
            int end = Min(start + group_size, dim);
            for (d = start; d < end; d++)
                expanded[d] = group_scales[g];
        }

        for (i = 0; i < n_rows; i++)
        {
            uint8 *row_packed = packed_codes + i * n_bytes;
            memset(row_packed, 0, n_bytes);

            for (d = 0; d < dim; d++)
            {
                float eq = rotated[i * dim + d] / expanded[d];
                int code = fh_digitize(eq);
                int byte_idx = d / 2;

                if (d % 2 == 0)
                    row_packed[byte_idx] |= (uint8)(code & 0x0F);
                else
                    row_packed[byte_idx] |= (uint8)((code & 0x0F) << 4);
            }
        }
        pfree(expanded);
    }

    /* Step 6: Transpose packed codes */
    packed_t = MemoryContextAllocHuge(CurrentMemoryContext, (Size)n_bytes * n_rows);
    for (i = 0; i < n_rows; i++)
        for (j = 0; j < n_bytes; j++)
            packed_t[j * n_rows + i] = packed_codes[i * n_bytes + j];

    /* Step 7: SQ8 encode (per-column min/max on original normalized vectors) */
    sq8_codes = MemoryContextAllocHuge(CurrentMemoryContext, (Size)n_rows * dim);
    sq8_mins = palloc(sizeof(float) * dim);
    sq8_scales = palloc(sizeof(float) * dim);
    {
        for (d = 0; d < dim; d++)
        {
            float col_min = FLT_MAX, col_max = -FLT_MAX;
            for (i = 0; i < n_rows; i++)
            {
                float v = all_vecs[i * dim + d];
                if (v < col_min) col_min = v;
                if (v > col_max) col_max = v;
            }
            float range = col_max - col_min;
            if (range < 1e-8f) range = 1e-8f;
            sq8_mins[d] = col_min;
            sq8_scales[d] = range / 255.0f;

            for (i = 0; i < n_rows; i++)
            {
                float v = all_vecs[i * dim + d];
                int code = (int)roundf((v - col_min) / sq8_scales[d]);
                if (code < 0) code = 0;
                if (code > 255) code = 255;
                sq8_codes[i * dim + d] = (uint8)code;
            }
        }
    }

    /* Step 8: Create sidecar table and store everything (SPI for DDL+INSERT) */
    elog(NOTICE, "fh_build: creating sidecar '%s' (%d rows, %d dim)", sidecar, n_rows, dim);
    SPI_connect();
    initStringInfo(&sql);
    appendStringInfo(&sql, "DROP TABLE IF EXISTS %s", quote_identifier(sidecar));
    SPI_execute(sql.data, false, 0);

    resetStringInfo(&sql);
    appendStringInfo(&sql,
        "CREATE TABLE %s ("
        "  row_id int4 NOT NULL,"
        "  packed_t bytea,"
        "  sq8_codes bytea,"
        "  meta_group_scales bytea,"
        "  meta_sq8_mins bytea,"
        "  meta_sq8_scales bytea,"
        "  meta_norms bytea,"
        "  meta_dim int4,"
        "  meta_seed int4,"
        "  meta_group_size int4,"
        "  meta_n_rows int4"
        ")", quote_identifier(sidecar));
    SPI_execute(sql.data, false, 0);

    elog(NOTICE, "fh_build: table created, inserting metadata (packed_t=%d, sq8=%d, norms=%d bytes)",
         n_bytes * n_rows, n_rows * dim, (int)(sizeof(float) * n_rows));

    /* Insert metadata row (row_id = -1) */
    {
        bytea *gs_bytes = (bytea *)palloc(VARHDRSZ + sizeof(float) * n_groups);
        bytea *mins_bytes = (bytea *)palloc(VARHDRSZ + sizeof(float) * dim);
        bytea *scales_bytes = (bytea *)palloc(VARHDRSZ + sizeof(float) * dim);
        bytea *norms_bytes = (bytea *)palloc(VARHDRSZ + sizeof(float) * n_rows);
        bytea *pt_bytes = (bytea *)MemoryContextAllocHuge(CurrentMemoryContext, VARHDRSZ + (Size)n_bytes * n_rows);
        bytea *sq_bytes = (bytea *)MemoryContextAllocHuge(CurrentMemoryContext, VARHDRSZ + (Size)n_rows * dim);

        SET_VARSIZE(gs_bytes, VARHDRSZ + sizeof(float) * n_groups);
        memcpy(VARDATA(gs_bytes), group_scales, sizeof(float) * n_groups);
        SET_VARSIZE(mins_bytes, VARHDRSZ + sizeof(float) * dim);
        memcpy(VARDATA(mins_bytes), sq8_mins, sizeof(float) * dim);
        SET_VARSIZE(scales_bytes, VARHDRSZ + sizeof(float) * dim);
        memcpy(VARDATA(scales_bytes), sq8_scales, sizeof(float) * dim);
        SET_VARSIZE(norms_bytes, VARHDRSZ + sizeof(float) * n_rows);
        memcpy(VARDATA(norms_bytes), norms, sizeof(float) * n_rows);
        SET_VARSIZE(pt_bytes, VARHDRSZ + n_bytes * n_rows);
        memcpy(VARDATA(pt_bytes), packed_t, n_bytes * n_rows);
        SET_VARSIZE(sq_bytes, VARHDRSZ + n_rows * dim);
        memcpy(VARDATA(sq_bytes), sq8_codes, n_rows * dim);

        {
            Datum values[11];
            bool nulls[11];
            Oid argtypes[11] = {INT4OID, BYTEAOID, BYTEAOID, BYTEAOID, BYTEAOID, BYTEAOID, BYTEAOID, INT4OID, INT4OID, INT4OID, INT4OID};

            memset(nulls, false, sizeof(nulls));
            values[0] = Int32GetDatum(-1);
            values[1] = PointerGetDatum(pt_bytes);
            values[2] = PointerGetDatum(sq_bytes);
            values[3] = PointerGetDatum(gs_bytes);
            values[4] = PointerGetDatum(mins_bytes);
            values[5] = PointerGetDatum(scales_bytes);
            values[6] = PointerGetDatum(norms_bytes);
            values[7] = Int32GetDatum(dim);
            values[8] = Int32GetDatum(seed);
            values[9] = Int32GetDatum(group_size);
            values[10] = Int32GetDatum(n_rows);

            resetStringInfo(&sql);
            appendStringInfo(&sql,
                "INSERT INTO %s (row_id, packed_t, sq8_codes, meta_group_scales, "
                "meta_sq8_mins, meta_sq8_scales, meta_norms, meta_dim, meta_seed, "
                "meta_group_size, meta_n_rows) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                quote_identifier(sidecar));

            ret = SPI_execute_with_args(sql.data, 11, argtypes, values, NULL, false, 0);
            if (ret != SPI_OK_INSERT)
                ereport(WARNING, (errmsg("flashhadamard_build: metadata insert failed")));
        }
    }

    SPI_finish();

    /* Memory allocated in SPI context is already freed by SPI_finish.
     * Do NOT pfree here — just return. */
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
    /* query is vector type — extract float array */
    Datum       query_datum = PG_GETARG_DATUM(1);
    int         k = PG_GETARG_INT32(2);
    int         shortlist_m = PG_GETARG_INT32(3);

    StringInfoData sql;
    int         ret;
    int         dim, seed, group_size_val, n_rows, n_bytes, n_groups;
    FHParams    params;
    FHCodes     codes;
    float      *query_vec;
    int32      *out_ids;
    float      *out_scores;
    int         n_results, i;
    TupleDesc   tupdesc;
    Tuplestorestate *tupstore;

    /* Setup materialized SRF */
    InitMaterializedSRF(fcinfo, 0);
    tupstore = rsinfo->setResult;
    tupdesc = rsinfo->setDesc;

    SPI_connect();

    /* Load metadata from sidecar (row_id = -1) */
    initStringInfo(&sql);
    appendStringInfo(&sql,
        "SELECT packed_t, sq8_codes, meta_group_scales, meta_sq8_mins, "
        "meta_sq8_scales, meta_norms, meta_dim, meta_seed, meta_group_size, "
        "meta_n_rows FROM %s WHERE row_id = -1",
        quote_identifier(sidecar));
    ret = SPI_execute(sql.data, true, 1);

    if (ret != SPI_OK_SELECT || SPI_processed != 1)
    {
        SPI_finish();
        ereport(ERROR, (errmsg("flashhadamard_scan: sidecar metadata not found")));
    }

    /* Extract metadata */
    {
        HeapTuple tup = SPI_tuptable->vals[0];
        TupleDesc tdesc = SPI_tuptable->tupdesc;
        bool isnull;
        bytea *pt_bytes, *sq_bytes, *gs_bytes, *mins_bytes, *scales_bytes, *norms_bytes;

        dim = DatumGetInt32(SPI_getbinval(tup, tdesc, 7, &isnull));
        seed = DatumGetInt32(SPI_getbinval(tup, tdesc, 8, &isnull));
        group_size_val = DatumGetInt32(SPI_getbinval(tup, tdesc, 9, &isnull));
        n_rows = DatumGetInt32(SPI_getbinval(tup, tdesc, 10, &isnull));
        n_bytes = (dim + 1) / 2;
        n_groups = (dim + group_size_val - 1) / group_size_val;

        pt_bytes = DatumGetByteaP(SPI_getbinval(tup, tdesc, 1, &isnull));
        sq_bytes = DatumGetByteaP(SPI_getbinval(tup, tdesc, 2, &isnull));
        gs_bytes = DatumGetByteaP(SPI_getbinval(tup, tdesc, 3, &isnull));
        mins_bytes = DatumGetByteaP(SPI_getbinval(tup, tdesc, 4, &isnull));
        scales_bytes = DatumGetByteaP(SPI_getbinval(tup, tdesc, 5, &isnull));
        norms_bytes = DatumGetByteaP(SPI_getbinval(tup, tdesc, 6, &isnull));

        /* Build params */
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

        /* Build codes */
        codes.n_rows = n_rows;
        codes.dim = dim;
        codes.n_bytes = n_bytes;
        codes.packed_t = MemoryContextAllocHuge(CurrentMemoryContext, (Size)n_bytes * n_rows);
        memcpy(codes.packed_t, VARDATA_ANY(pt_bytes), (Size)n_bytes * n_rows);
        codes.sq8_codes = MemoryContextAllocHuge(CurrentMemoryContext, (Size)n_rows * dim);
        memcpy(codes.sq8_codes, VARDATA_ANY(sq_bytes), (Size)n_rows * dim);
        codes.sq8_mins = palloc(sizeof(float) * dim);
        memcpy(codes.sq8_mins, VARDATA_ANY(mins_bytes), sizeof(float) * dim);
        codes.sq8_scales = palloc(sizeof(float) * dim);
        memcpy(codes.sq8_scales, VARDATA_ANY(scales_bytes), sizeof(float) * dim);
        codes.norms = palloc(sizeof(float) * n_rows);
        memcpy(codes.norms, VARDATA_ANY(norms_bytes), sizeof(float) * n_rows);
    }

    SPI_finish();

    /* Extract query vector from pgvector's vector type */
    {
        /* pgvector stores vector as: varlena header + uint16 dim + uint16 unused + float[] */
        bytea *raw = DatumGetByteaP(query_datum);
        int query_dim = *(uint16 *)(VARDATA(raw));
        float *qdata = (float *)(VARDATA(raw) + 2 * sizeof(uint16));

        if (query_dim != dim)
            ereport(ERROR, (errmsg("flashhadamard_scan: query dim %d != sidecar dim %d",
                                    query_dim, dim)));
        query_vec = palloc(sizeof(float) * dim);
        memcpy(query_vec, qdata, sizeof(float) * dim);
    }

    /* Run search */
    out_ids = palloc(sizeof(int32) * k);
    out_scores = palloc(sizeof(float) * k);
    n_results = fh_search(&codes, &params, query_vec, k, shortlist_m,
                           out_ids, out_scores);

    /* Emit results */
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
