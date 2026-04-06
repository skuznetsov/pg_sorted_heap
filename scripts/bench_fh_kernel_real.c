/*
 * bench_fh_kernel_real.c — Real-data kernel validation
 *
 * Loads an actual FH store + query vectors from PG, runs both float and
 * int16 kernels, compares: top-k overlap, score correlation, clipping rate.
 *
 * Build:
 *   cc -O2 -march=native -o bench_fh_kernel_real scripts/bench_fh_kernel_real.c -lm
 *
 * Run:
 *   ./bench_fh_kernel_real <store_path> <query_file> [topk]
 *
 * Query file: binary file with header (int32 n_queries, int32 dim),
 * then n_queries × dim float32 vectors. Generate with:
 *   psql -d fh_test -c "COPY (SELECT embedding::text FROM gutenberg_local
 *     ORDER BY id DESC LIMIT 50) TO '/tmp/fh_queries.csv'"
 * Or use the embedded PG loader below.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <float.h>
#include <math.h>
#include <time.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#ifdef __ARM_NEON
#include <arm_neon.h>
#define HAS_NEON 1
#else
#define HAS_NEON 0
#endif

/* ================================================================
 * Store format (must match flashhadamard_store.h)
 * ================================================================ */

#define FH_STORE_MAGIC  0x46484D31
#define FH_MAX_CENTERS  16
#define FH_MAX_GROUPS   512

typedef struct {
    uint32_t magic;
    uint16_t page_type;
    uint16_t reserved;
    int32_t  offset;
    int32_t  length;
} FHFilePageHeader;

typedef struct {
    uint32_t magic;
    int32_t  version, dim, n_rows, n_bytes, group_size, n_groups, seed;
    int64_t  off_sq8_params, off_packed, off_sq8, off_norms, off_centroids;
    int32_t  n_segments, segment_size;
    int64_t  off_end;
    float    centers[FH_MAX_CENTERS];
    float    group_scales[FH_MAX_GROUPS];
} FHMetaV2;

/* ================================================================
 * Splitmix64 + perm/signs (must match C engine)
 * ================================================================ */

static uint64_t splitmix64(uint64_t *state)
{
    uint64_t z = (*state += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

static void gen_perm_signs(int dim, int seed, int *perm, float *signs)
{
    uint64_t state = (uint64_t)seed;
    int i;
    for (i = 0; i < dim; i++) perm[i] = i;
    for (i = dim - 1; i > 0; i--)
    {
        int j = (int)(splitmix64(&state) % (uint64_t)(i + 1));
        int tmp = perm[i]; perm[i] = perm[j]; perm[j] = tmp;
    }
    for (i = 0; i < dim; i++)
        signs[i] = (splitmix64(&state) & 1) ? 1.0f : -1.0f;
}

/* FWHT in-place (normalized) */
static void fwht_inplace(float *data, int len)
{
    int h, i;
    float norm;
    for (h = 1; h < len; h <<= 1)
        for (i = 0; i < len; i += 2 * h)
        {
            int j;
            for (j = i; j < i + h; j++)
            {
                float x = data[j], y = data[j + h];
                data[j] = x + y;
                data[j + h] = x - y;
            }
        }
    norm = 1.0f / sqrtf((float)len);
    for (i = 0; i < len; i++) data[i] *= norm;
}

/* Rotate one vector */
static void rotate_vec(const float *in, float *out, int dim,
                       const int *perm, const float *signs)
{
    float *buf = (float *)malloc(sizeof(float) * dim);
    int i, offset, b;
    for (i = 0; i < dim; i++) buf[i] = in[perm[i]] * signs[i];
    /* Block FWHT */
    offset = 0;
    b = dim;
    while (b > 0)
    {
        int block = 1;
        while (block * 2 <= b) block *= 2;
        fwht_inplace(buf + offset, block);
        offset += block;
        b -= block;
    }
    memcpy(out, buf, sizeof(float) * dim);
    free(buf);
}

/* ================================================================
 * Float LUT kernel
 * ================================================================ */

static void build_float_tables(const float *coeffs, const float *centers,
                                int dim, float *tables)
{
    int n_bytes = (dim + 1) / 2;
    int bi, lo, hi;
    for (bi = 0; bi < n_bytes; bi++)
    {
        int d0 = 2 * bi, d1 = 2 * bi + 1;
        float c0 = coeffs[d0];
        float c1 = (d1 < dim) ? coeffs[d1] : 0.0f;
        for (lo = 0; lo < 16; lo++)
            for (hi = 0; hi < 16; hi++)
                tables[bi * 256 + lo + hi * 16] = centers[lo] * c0 + centers[hi] * c1;
    }
}

static void score_float(const uint8_t *packed_t, const float *tables,
                         const float *norms, int n_rows, int n_bytes,
                         float *scores)
{
    int byte_idx, row;
    memset(scores, 0, sizeof(float) * n_rows);
    for (byte_idx = 0; byte_idx + 1 < n_bytes; byte_idx += 2)
    {
        const uint8_t *c0 = packed_t + (size_t)byte_idx * n_rows;
        const uint8_t *c1 = packed_t + (size_t)(byte_idx + 1) * n_rows;
        const float *t0 = tables + byte_idx * 256;
        const float *t1 = tables + (byte_idx + 1) * 256;
        for (row = 0; row + 3 < n_rows; row += 4)
        {
            scores[row+0] += t0[c0[row+0]] + t1[c1[row+0]];
            scores[row+1] += t0[c0[row+1]] + t1[c1[row+1]];
            scores[row+2] += t0[c0[row+2]] + t1[c1[row+2]];
            scores[row+3] += t0[c0[row+3]] + t1[c1[row+3]];
        }
        for (; row < n_rows; row++)
            scores[row] += t0[c0[row]] + t1[c1[row]];
    }
    if (byte_idx < n_bytes)
    {
        const uint8_t *c = packed_t + (size_t)byte_idx * n_rows;
        const float *t = tables + byte_idx * 256;
        for (row = 0; row < n_rows; row++) scores[row] += t[c[row]];
    }
    /* Apply norms */
    if (norms)
        for (row = 0; row < n_rows; row++) scores[row] *= norms[row];
}

/* ================================================================
 * Int16 LUT kernel
 * ================================================================ */

static void build_int16_tables(const float *coeffs, const float *centers,
                                int dim, float int_scale,
                                int16_t *tables, int *clip_count)
{
    int n_bytes = (dim + 1) / 2;
    int bi, lo, hi;
    *clip_count = 0;
    for (bi = 0; bi < n_bytes; bi++)
    {
        int d0 = 2 * bi, d1 = 2 * bi + 1;
        float c0 = coeffs[d0];
        float c1 = (d1 < dim) ? coeffs[d1] : 0.0f;
        for (lo = 0; lo < 16; lo++)
            for (hi = 0; hi < 16; hi++)
            {
                float v = centers[lo] * c0 + centers[hi] * c1;
                int32_t q = (int32_t)roundf(v * int_scale);
                if (q > 32767 || q < -32768) (*clip_count)++;
                if (q > 32767) q = 32767;
                if (q < -32768) q = -32768;
                tables[bi * 256 + lo + hi * 16] = (int16_t)q;
            }
    }
}

#if HAS_NEON
static void score_int16_neon(const uint8_t *packed_t, const int16_t *tables,
                              int n_rows, int n_bytes,
                              int32_t *scores)
{
    int byte_idx, row;
    memset(scores, 0, sizeof(int32_t) * n_rows);
    for (byte_idx = 0; byte_idx + 1 < n_bytes; byte_idx += 2)
    {
        const uint8_t *c0 = packed_t + (size_t)byte_idx * n_rows;
        const uint8_t *c1 = packed_t + (size_t)(byte_idx + 1) * n_rows;
        const int16_t *t0 = tables + byte_idx * 256;
        const int16_t *t1 = tables + (byte_idx + 1) * 256;

        for (row = 0; row + 7 < n_rows; row += 8)
        {
            int16_t v0[8], v1[8];
            v0[0] = t0[c0[row+0]]; v1[0] = t1[c1[row+0]];
            v0[1] = t0[c0[row+1]]; v1[1] = t1[c1[row+1]];
            v0[2] = t0[c0[row+2]]; v1[2] = t1[c1[row+2]];
            v0[3] = t0[c0[row+3]]; v1[3] = t1[c1[row+3]];
            v0[4] = t0[c0[row+4]]; v1[4] = t1[c1[row+4]];
            v0[5] = t0[c0[row+5]]; v1[5] = t1[c1[row+5]];
            v0[6] = t0[c0[row+6]]; v1[6] = t1[c1[row+6]];
            v0[7] = t0[c0[row+7]]; v1[7] = t1[c1[row+7]];

            int16x8_t sum16 = vaddq_s16(vld1q_s16(v0), vld1q_s16(v1));
            int32x4_t s_lo = vld1q_s32(scores + row);
            int32x4_t s_hi = vld1q_s32(scores + row + 4);
            s_lo = vaddw_s16(s_lo, vget_low_s16(sum16));
            s_hi = vaddw_s16(s_hi, vget_high_s16(sum16));
            vst1q_s32(scores + row, s_lo);
            vst1q_s32(scores + row + 4, s_hi);
        }
        for (; row < n_rows; row++)
            scores[row] += (int32_t)t0[c0[row]] + (int32_t)t1[c1[row]];
    }
    if (byte_idx < n_bytes)
    {
        const uint8_t *c = packed_t + (size_t)byte_idx * n_rows;
        const int16_t *t = tables + byte_idx * 256;
        for (row = 0; row < n_rows; row++)
            scores[row] += (int32_t)t[c[row]];
    }
}
#endif

/* ================================================================
 * Top-k selection
 * ================================================================ */

static void topk_float(const float *scores, int n, int k,
                        int *ids, float *vals)
{
    int i, filled = 0, min_pos = 0;
    float min_val = -FLT_MAX;
    for (i = 0; i < n; i++)
    {
        if (filled < k)
        {
            ids[filled] = i;
            vals[filled] = scores[i];
            if (scores[i] < min_val || filled == 0) { min_val = scores[i]; min_pos = filled; }
            filled++;
        }
        else if (scores[i] > min_val)
        {
            ids[min_pos] = i;
            vals[min_pos] = scores[i];
            int j; min_val = FLT_MAX;
            for (j = 0; j < k; j++) if (vals[j] < min_val) { min_val = vals[j]; min_pos = j; }
        }
    }
}

static void topk_int32(const int32_t *scores, const float *norms, int n, int k,
                        int *ids, int32_t *vals)
{
    int i, filled = 0, min_pos = 0;
    float min_fval = -FLT_MAX;
    /* For ranking with norms: use float(int_score) * norm as comparison key */
    float *fvals = (float *)malloc(sizeof(float) * k);
    for (i = 0; i < n; i++)
    {
        float fv = norms ? (float)scores[i] * norms[i] : (float)scores[i];
        if (filled < k)
        {
            ids[filled] = i;
            vals[filled] = scores[i];
            fvals[filled] = fv;
            if (fv < min_fval || filled == 0) { min_fval = fv; min_pos = filled; }
            filled++;
        }
        else if (fv > min_fval)
        {
            ids[min_pos] = i;
            vals[min_pos] = scores[i];
            fvals[min_pos] = fv;
            int j; min_fval = FLT_MAX;
            for (j = 0; j < k; j++) if (fvals[j] < min_fval) { min_fval = fvals[j]; min_pos = j; }
        }
    }
    free(fvals);
}

/* ================================================================
 * Timing
 * ================================================================ */

static double now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

/* Lloyd-Max centers */
static const float CENTERS[16] = {
    -2.1519927f, -1.5341205f, -1.1503494f, -0.8326452f,
    -0.5485528f, -0.2822760f, -0.0248825f,  0.2279585f,
     0.4809854f,  0.7405728f,  1.0137205f,  1.3106381f,
     1.6481531f,  2.0637655f,  2.6476993f,  3.7169876f,
};

/* ================================================================
 * Main
 * ================================================================ */

int main(int argc, char **argv)
{
    if (argc < 3)
    {
        fprintf(stderr, "Usage: %s <store_path> <queries.bin> [topk=12]\n", argv[0]);
        fprintf(stderr, "\nGenerate queries.bin:\n");
        fprintf(stderr, "  python3 -c \"\n");
        fprintf(stderr, "import psycopg2, numpy as np, struct\n");
        fprintf(stderr, "conn = psycopg2.connect('dbname=fh_test')\n");
        fprintf(stderr, "cur = conn.cursor()\n");
        fprintf(stderr, "cur.execute('SELECT embedding FROM gutenberg_local ORDER BY id DESC LIMIT 50')\n");
        fprintf(stderr, "vecs = [np.array([float(x) for x in r[0].strip(\\\"[]\\\").split(',')], dtype=np.float32) for (r,) in cur.fetchall()]\n");
        fprintf(stderr, "with open('/tmp/fh_queries.bin','wb') as f:\n");
        fprintf(stderr, "  f.write(struct.pack('<ii', len(vecs), len(vecs[0])))\n");
        fprintf(stderr, "  for v in vecs: f.write(v.tobytes())\n");
        fprintf(stderr, "\"\n");
        return 1;
    }

    const char *store_path = argv[1];
    const char *query_path = argv[2];
    int topk = argc > 3 ? atoi(argv[3]) : 12;

    /* Open store */
    int fd = open(store_path, O_RDONLY);
    if (fd < 0) { perror("open store"); return 1; }
    struct stat st;
    fstat(fd, &st);
    void *mapped = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mapped == MAP_FAILED) { perror("mmap"); return 1; }

    FHMetaV2 *meta = (FHMetaV2 *)((char *)mapped + sizeof(FHFilePageHeader));
    int dim = meta->dim, n_rows = meta->n_rows, n_bytes = meta->n_bytes;
    int group_size = meta->group_size, seed = meta->seed;
    int n_groups = meta->n_groups;

    uint8_t *packed_t = (uint8_t *)mapped + meta->off_packed;
    float *norms = (float *)((char *)mapped + meta->off_norms);

    printf("Store: %s\n", store_path);
    printf("  dim=%d n_rows=%d n_bytes=%d seed=%d group_size=%d\n",
           dim, n_rows, n_bytes, seed, group_size);

    /* Load queries */
    FILE *qf = fopen(query_path, "rb");
    if (!qf) { perror("open queries"); return 1; }
    int32_t n_queries, qdim;
    fread(&n_queries, 4, 1, qf);
    fread(&qdim, 4, 1, qf);
    if (qdim != dim) { fprintf(stderr, "dim mismatch: %d vs %d\n", qdim, dim); return 1; }
    float *queries = (float *)malloc(sizeof(float) * n_queries * dim);
    fread(queries, sizeof(float) * dim, n_queries, qf);
    fclose(qf);
    printf("Queries: %d × %dD\n\n", n_queries, dim);

    /* Build perm/signs */
    int *perm = (int *)malloc(sizeof(int) * dim);
    float *signs = (float *)malloc(sizeof(float) * dim);
    gen_perm_signs(dim, seed, perm, signs);

    /* Expanded group scales */
    float *expanded = (float *)malloc(sizeof(float) * dim);
    { int g;
      for (g = 0; g < n_groups; g++)
      {
          int gs = g * group_size, ge = gs + group_size;
          if (ge > dim) ge = dim;
          int d;
          for (d = gs; d < ge; d++) expanded[d] = meta->group_scales[g];
      }
    }

    /* Allocate */
    float *float_tables = (float *)malloc(sizeof(float) * n_bytes * 256);
    float *scores_f = (float *)malloc(sizeof(float) * n_rows);
    int *topk_f_ids = (int *)malloc(sizeof(int) * topk);
    float *topk_f_vals = (float *)malloc(sizeof(float) * topk);

#if HAS_NEON
    int16_t *int_tables = (int16_t *)malloc(sizeof(int16_t) * n_bytes * 256);
    int32_t *scores_i = (int32_t *)malloc(sizeof(int32_t) * n_rows);
    int *topk_i_ids = (int *)malloc(sizeof(int) * topk);
    int32_t *topk_i_vals = (int32_t *)malloc(sizeof(int32_t) * topk);
#endif

    /* Per-query validation */
    float inv_sqrt_dim = 1.0f / sqrtf((float)dim);
    int total_overlap = 0, total_pairs = 0;
    int total_clips = 0, total_lut_entries = 0;
    double total_float_ms = 0, total_int_ms = 0;
    int total_hit1 = 0;
    float int_scale = 2048.0f;  /* int16 quantization scale */

    int q;
    for (q = 0; q < n_queries; q++)
    {
        float *query = queries + q * dim;

        /* Normalize query */
        float qnorm = 0;
        int d;
        for (d = 0; d < dim; d++) qnorm += query[d] * query[d];
        qnorm = sqrtf(qnorm);
        if (qnorm < 1e-12f) qnorm = 1e-12f;
        float *qunit = (float *)malloc(sizeof(float) * dim);
        for (d = 0; d < dim; d++) qunit[d] = query[d] / qnorm;

        /* Rotate query */
        float *qrot = (float *)malloc(sizeof(float) * dim);
        rotate_vec(qunit, qrot, dim, perm, signs);

        /* Build coefficients (rotated_q * group_scale * inv_sqrt_dim) */
        float *coeffs = (float *)malloc(sizeof(float) * dim);
        for (d = 0; d < dim; d++)
            coeffs[d] = qrot[d] * expanded[d] * inv_sqrt_dim;

        /* Float kernel */
        build_float_tables(coeffs, CENTERS, dim, float_tables);
        double t0 = now_ms();
        score_float(packed_t, float_tables, norms, n_rows, n_bytes, scores_f);
        double float_ms = now_ms() - t0;
        total_float_ms += float_ms;
        topk_float(scores_f, n_rows, topk, topk_f_ids, topk_f_vals);

#if HAS_NEON
        /* Int16 kernel */
        int clip_count = 0;
        build_int16_tables(coeffs, CENTERS, dim, int_scale, int_tables, &clip_count);
        total_clips += clip_count;
        total_lut_entries += n_bytes * 256;

        t0 = now_ms();
        score_int16_neon(packed_t, int_tables, n_rows, n_bytes, scores_i);
        double int_ms = now_ms() - t0;
        total_int_ms += int_ms;

        /* Apply norms and find top-k for int scores */
        topk_int32(scores_i, norms, n_rows, topk, topk_i_ids, topk_i_vals);

        /* Overlap: how many of int16 top-k are in float top-k? */
        int overlap = 0;
        int i, j;
        for (i = 0; i < topk; i++)
            for (j = 0; j < topk; j++)
                if (topk_i_ids[i] == topk_f_ids[j]) { overlap++; break; }
        total_overlap += overlap;
        total_pairs += topk;

        /* Hit@1: does int16 top-1 match float top-1? */
        /* Find best in each */
        int best_f = 0, best_i = 0;
        for (i = 1; i < topk; i++) { if (topk_f_vals[i] > topk_f_vals[best_f]) best_f = i; }
        float best_i_fval = -FLT_MAX;
        for (i = 0; i < topk; i++) {
            float fv = norms ? (float)topk_i_vals[i] * norms[topk_i_ids[i]] : (float)topk_i_vals[i];
            if (fv > best_i_fval) { best_i_fval = fv; best_i = i; }
        }
        if (topk_f_ids[best_f] == topk_i_ids[best_i]) total_hit1++;

        if (q < 3)
            printf("  q%d: float=%.1fms int16=%.1fms overlap=%d/%d clips=%d\n",
                   q, float_ms, int_ms, overlap, topk, clip_count);
#endif

        free(qunit);
        free(qrot);
        free(coeffs);
    }

    printf("\n=== Results (%d queries, topk=%d) ===\n", n_queries, topk);
    printf("Float kernel:  avg %.1f ms/query  total %.0f ms\n",
           total_float_ms / n_queries, total_float_ms);
#if HAS_NEON
    printf("Int16 kernel:  avg %.1f ms/query  total %.0f ms\n",
           total_int_ms / n_queries, total_int_ms);
    printf("Speedup:       %.1f%%\n",
           100.0 * (1.0 - total_int_ms / total_float_ms));
    printf("\nQuality:\n");
    printf("  top-%d overlap: %d/%d = %.1f%%\n",
           topk, total_overlap, total_pairs, 100.0 * total_overlap / total_pairs);
    printf("  hit@1:         %d/%d = %.1f%%\n",
           total_hit1, n_queries, 100.0 * total_hit1 / n_queries);
    printf("  clipping:      %d/%d entries = %.4f%%\n",
           total_clips, total_lut_entries,
           100.0 * total_clips / total_lut_entries);
    printf("  int_scale:     %.0f\n", int_scale);
#endif

    munmap(mapped, st.st_size);
    close(fd);
    free(queries);
    free(perm); free(signs); free(expanded);
    free(float_tables); free(scores_f);
    free(topk_f_ids); free(topk_f_vals);
#if HAS_NEON
    free(int_tables); free(scores_i);
    free(topk_i_ids); free(topk_i_vals);
#endif

    return 0;
}
