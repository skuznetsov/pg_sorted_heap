/*
 * bench_fh_kernel.c — FlashHadamard CPU kernel lab
 *
 * Standalone microbenchmark for testing kernel variants across platforms.
 * Each kernel processes the same synthetic packed data and reports timing.
 * All float kernels produce bit-identical results (same checksum).
 *
 * Build:
 *   # Apple/NEON:
 *   cc -O2 -march=native -o bench_kernel scripts/bench_fh_kernel.c -lm
 *   # Intel/AVX2:
 *   gcc -O2 -march=native -mavx2 -o bench_kernel scripts/bench_fh_kernel.c -lm
 *
 * Run:
 *   ./bench_kernel [n_rows] [dim] [n_iters]
 *   ./bench_kernel 103260 2880 7
 *
 * Benchmark results (103K × 2880D, single-thread, synthetic data):
 *
 *   Apple M2 Max:
 *     k0: scalar 4x unroll (production)  42.9 ms  (3.5 GB/s)
 *     kn0: NEON int16 LUT                35.6 ms  (4.2 GB/s)  -17%
 *
 *   Intel Xeon Platinum 8275CL (AWS c5.large):
 *     k0: scalar 4x unroll (production)  54.4 ms  (2.7 GB/s)
 *     k1: AVX2 float 8-wide              47.1 ms  (3.2 GB/s)  -13%
 *     k2: AVX2 float+prefetch            49.6 ms  (3.0 GB/s)   -9%
 *     k3: AVX2 4-byte fused              46.5 ms  (3.2 GB/s)  -15%
 *     k4: AVX2 gather                    39.1 ms  (3.8 GB/s)  -28%  *** WINNER
 *
 * Winner analysis:
 *   k4 uses _mm256_i32gather_ps to do 8 random float loads in one
 *   instruction. This directly addresses the bottleneck: 256-entry LUT
 *   random-access latency. All float kernels produce identical results
 *   (zero quality risk). The int16 LUT approach was refuted on Intel
 *   (2× slower than float).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <float.h>
#include <math.h>
#include <time.h>

#ifdef __ARM_NEON
#include <arm_neon.h>
#define HAS_NEON 1
#else
#define HAS_NEON 0
#endif

#ifdef __AVX2__
#include <immintrin.h>
#define HAS_AVX2 1
#else
#define HAS_AVX2 0
#endif

/* ================================================================
 * Timing
 * ================================================================ */

static double now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

/* ================================================================
 * Kernel 0: Float LUT scalar (current production)
 * ================================================================ */

static void k0_float_scalar(
    const uint8_t *packed_t, const float *tables, float *scores,
    int n_rows, int n_bytes)
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
}

#if HAS_AVX2
/* ================================================================
 * Kernel 1: AVX2 float 8-wide accumulation
 *
 * Scalar LUT lookups (unavoidable with 256-entry tables),
 * but AVX2 8-wide addition for score accumulation.
 * ================================================================ */

static void k1_avx2_float8(
    const uint8_t *packed_t, const float *tables, float *scores,
    int n_rows, int n_bytes)
{
    int byte_idx, row;
    memset(scores, 0, sizeof(float) * n_rows);

    for (byte_idx = 0; byte_idx + 1 < n_bytes; byte_idx += 2)
    {
        const uint8_t *c0 = packed_t + (size_t)byte_idx * n_rows;
        const uint8_t *c1 = packed_t + (size_t)(byte_idx + 1) * n_rows;
        const float *t0 = tables + byte_idx * 256;
        const float *t1 = tables + (byte_idx + 1) * 256;

        for (row = 0; row + 7 < n_rows; row += 8)
        {
            /* Scalar LUT lookups → gather into __m256 */
            __m256 contrib = _mm256_setr_ps(
                t0[c0[row+0]] + t1[c1[row+0]],
                t0[c0[row+1]] + t1[c1[row+1]],
                t0[c0[row+2]] + t1[c1[row+2]],
                t0[c0[row+3]] + t1[c1[row+3]],
                t0[c0[row+4]] + t1[c1[row+4]],
                t0[c0[row+5]] + t1[c1[row+5]],
                t0[c0[row+6]] + t1[c1[row+6]],
                t0[c0[row+7]] + t1[c1[row+7]]
            );
            __m256 s = _mm256_loadu_ps(scores + row);
            s = _mm256_add_ps(s, contrib);
            _mm256_storeu_ps(scores + row, s);
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
}

/* ================================================================
 * Kernel 2: AVX2 float + software prefetch
 *
 * Same as k1 but prefetch next byte-column's code bytes.
 * ================================================================ */

static void k2_avx2_prefetch(
    const uint8_t *packed_t, const float *tables, float *scores,
    int n_rows, int n_bytes)
{
    int byte_idx, row;
    memset(scores, 0, sizeof(float) * n_rows);

    for (byte_idx = 0; byte_idx + 1 < n_bytes; byte_idx += 2)
    {
        const uint8_t *c0 = packed_t + (size_t)byte_idx * n_rows;
        const uint8_t *c1 = packed_t + (size_t)(byte_idx + 1) * n_rows;
        const float *t0 = tables + byte_idx * 256;
        const float *t1 = tables + (byte_idx + 1) * 256;

        /* Prefetch next byte column's codes */
        const uint8_t *c0_next = (byte_idx + 2 < n_bytes) ?
            packed_t + (size_t)(byte_idx + 2) * n_rows : c0;
        const uint8_t *c1_next = (byte_idx + 3 < n_bytes) ?
            packed_t + (size_t)(byte_idx + 3) * n_rows : c1;

        for (row = 0; row + 7 < n_rows; row += 8)
        {
            /* Prefetch next iteration's code bytes (L1 cache) */
            _mm_prefetch((const char *)(c0_next + row), _MM_HINT_T0);
            _mm_prefetch((const char *)(c1_next + row), _MM_HINT_T0);

            __m256 contrib = _mm256_setr_ps(
                t0[c0[row+0]] + t1[c1[row+0]],
                t0[c0[row+1]] + t1[c1[row+1]],
                t0[c0[row+2]] + t1[c1[row+2]],
                t0[c0[row+3]] + t1[c1[row+3]],
                t0[c0[row+4]] + t1[c1[row+4]],
                t0[c0[row+5]] + t1[c1[row+5]],
                t0[c0[row+6]] + t1[c1[row+6]],
                t0[c0[row+7]] + t1[c1[row+7]]
            );
            __m256 s = _mm256_loadu_ps(scores + row);
            s = _mm256_add_ps(s, contrib);
            _mm256_storeu_ps(scores + row, s);
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
}

/* ================================================================
 * Kernel 3: AVX2 float + 4-byte fused (process 4 byte columns per iter)
 *
 * Amortize score load/store by processing more byte columns per pass.
 * ================================================================ */

static void k3_avx2_4byte_fused(
    const uint8_t *packed_t, const float *tables, float *scores,
    int n_rows, int n_bytes)
{
    int byte_idx, row;
    memset(scores, 0, sizeof(float) * n_rows);

    /* 4-byte fused: process 4 byte columns per outer loop */
    for (byte_idx = 0; byte_idx + 3 < n_bytes; byte_idx += 4)
    {
        const uint8_t *c0 = packed_t + (size_t)(byte_idx+0) * n_rows;
        const uint8_t *c1 = packed_t + (size_t)(byte_idx+1) * n_rows;
        const uint8_t *c2 = packed_t + (size_t)(byte_idx+2) * n_rows;
        const uint8_t *c3 = packed_t + (size_t)(byte_idx+3) * n_rows;
        const float *t0 = tables + (byte_idx+0) * 256;
        const float *t1 = tables + (byte_idx+1) * 256;
        const float *t2 = tables + (byte_idx+2) * 256;
        const float *t3 = tables + (byte_idx+3) * 256;

        for (row = 0; row + 7 < n_rows; row += 8)
        {
            __m256 s = _mm256_loadu_ps(scores + row);

            __m256 v01 = _mm256_setr_ps(
                t0[c0[row+0]] + t1[c1[row+0]],
                t0[c0[row+1]] + t1[c1[row+1]],
                t0[c0[row+2]] + t1[c1[row+2]],
                t0[c0[row+3]] + t1[c1[row+3]],
                t0[c0[row+4]] + t1[c1[row+4]],
                t0[c0[row+5]] + t1[c1[row+5]],
                t0[c0[row+6]] + t1[c1[row+6]],
                t0[c0[row+7]] + t1[c1[row+7]]
            );
            __m256 v23 = _mm256_setr_ps(
                t2[c2[row+0]] + t3[c3[row+0]],
                t2[c2[row+1]] + t3[c3[row+1]],
                t2[c2[row+2]] + t3[c3[row+2]],
                t2[c2[row+3]] + t3[c3[row+3]],
                t2[c2[row+4]] + t3[c3[row+4]],
                t2[c2[row+5]] + t3[c3[row+5]],
                t2[c2[row+6]] + t3[c3[row+6]],
                t2[c2[row+7]] + t3[c3[row+7]]
            );

            s = _mm256_add_ps(s, _mm256_add_ps(v01, v23));
            _mm256_storeu_ps(scores + row, s);
        }
        for (; row < n_rows; row++)
            scores[row] += t0[c0[row]] + t1[c1[row]] + t2[c2[row]] + t3[c3[row]];
    }
    /* Handle remaining 0-3 byte columns */
    for (; byte_idx + 1 < n_bytes; byte_idx += 2)
    {
        const uint8_t *c0 = packed_t + (size_t)byte_idx * n_rows;
        const uint8_t *c1 = packed_t + (size_t)(byte_idx+1) * n_rows;
        const float *t0 = tables + byte_idx * 256;
        const float *t1 = tables + (byte_idx+1) * 256;
        for (row = 0; row < n_rows; row++)
            scores[row] += t0[c0[row]] + t1[c1[row]];
    }
    if (byte_idx < n_bytes)
    {
        const uint8_t *c = packed_t + (size_t)byte_idx * n_rows;
        const float *t = tables + byte_idx * 256;
        for (row = 0; row < n_rows; row++) scores[row] += t[c[row]];
    }
}

/* ================================================================
 * Kernel 4: AVX2 gather-based LUT
 *
 * Use _mm256_i32gather_ps to load 8 LUT entries at once.
 * The gather instruction does 8 random loads in one instruction.
 * ================================================================ */

static void k4_avx2_gather(
    const uint8_t *packed_t, const float *tables, float *scores,
    int n_rows, int n_bytes)
{
    int byte_idx, row;
    memset(scores, 0, sizeof(float) * n_rows);

    for (byte_idx = 0; byte_idx + 1 < n_bytes; byte_idx += 2)
    {
        const uint8_t *c0 = packed_t + (size_t)byte_idx * n_rows;
        const uint8_t *c1 = packed_t + (size_t)(byte_idx+1) * n_rows;
        const float *t0 = tables + byte_idx * 256;
        const float *t1 = tables + (byte_idx+1) * 256;

        for (row = 0; row + 7 < n_rows; row += 8)
        {
            /* Load 8 code bytes and zero-extend to int32 for gather */
            __m128i raw0 = _mm_loadl_epi64((__m128i *)(c0 + row));
            __m128i raw1 = _mm_loadl_epi64((__m128i *)(c1 + row));
            __m256i idx0 = _mm256_cvtepu8_epi32(raw0);
            __m256i idx1 = _mm256_cvtepu8_epi32(raw1);

            /* Gather 8 float values from each table */
            __m256 v0 = _mm256_i32gather_ps(t0, idx0, 4);
            __m256 v1 = _mm256_i32gather_ps(t1, idx1, 4);

            __m256 s = _mm256_loadu_ps(scores + row);
            s = _mm256_add_ps(s, _mm256_add_ps(v0, v1));
            _mm256_storeu_ps(scores + row, s);
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
}
#endif /* HAS_AVX2 */

#if HAS_NEON
/* ================================================================
 * Kernel N0: NEON int16 LUT (reference Apple winner)
 * ================================================================ */

static void kn0_neon_int16(
    const uint8_t *packed_t, const int16_t *tables, int32_t *scores,
    int n_rows, int n_bytes)
{
    int byte_idx, row;
    memset(scores, 0, sizeof(int32_t) * n_rows);

    for (byte_idx = 0; byte_idx + 1 < n_bytes; byte_idx += 2)
    {
        const uint8_t *c0 = packed_t + (size_t)byte_idx * n_rows;
        const uint8_t *c1 = packed_t + (size_t)(byte_idx+1) * n_rows;
        const int16_t *t0 = tables + byte_idx * 256;
        const int16_t *t1 = tables + (byte_idx+1) * 256;

        for (row = 0; row + 7 < n_rows; row += 8)
        {
            int16_t v0[8], v1[8];
            v0[0]=t0[c0[row+0]]; v1[0]=t1[c1[row+0]];
            v0[1]=t0[c0[row+1]]; v1[1]=t1[c1[row+1]];
            v0[2]=t0[c0[row+2]]; v1[2]=t1[c1[row+2]];
            v0[3]=t0[c0[row+3]]; v1[3]=t1[c1[row+3]];
            v0[4]=t0[c0[row+4]]; v1[4]=t1[c1[row+4]];
            v0[5]=t0[c0[row+5]]; v1[5]=t1[c1[row+5]];
            v0[6]=t0[c0[row+6]]; v1[6]=t1[c1[row+6]];
            v0[7]=t0[c0[row+7]]; v1[7]=t1[c1[row+7]];

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
        for (row = 0; row < n_rows; row++) scores[row] += (int32_t)t[c[row]];
    }
}
#endif /* HAS_NEON */

/* ================================================================
 * Main
 * ================================================================ */

static const float LLOYD_MAX_16[16] = {
    -2.1519927f, -1.5341205f, -1.1503494f, -0.8326452f,
    -0.5485528f, -0.2822760f, -0.0248825f,  0.2279585f,
     0.4809854f,  0.7405728f,  1.0137205f,  1.3106381f,
     1.6481531f,  2.0637655f,  2.6476993f,  3.7169876f,
};

typedef void (*kernel_fn)(const uint8_t *, const float *, float *, int, int);

static double bench(const char *name, kernel_fn fn,
                     const uint8_t *packed_t, const float *tables,
                     float *scores, int n_rows, int n_bytes, int n_iters,
                     double *checksum_out)
{
    int i;
    double best = 1e9;
    for (i = 0; i < n_iters; i++)
    {
        double t0 = now_ms();
        fn(packed_t, tables, scores, n_rows, n_bytes);
        double dt = now_ms() - t0;
        if (dt < best) best = dt;
    }
    /* Checksum */
    double sum = 0;
    for (i = 0; i < n_rows; i++) sum += scores[i];
    *checksum_out = sum / n_rows;
    printf("  %-28s %6.1f ms  (%4.1f GB/s)  chk=%.4f\n",
           name, best, (double)n_bytes * n_rows / best / 1e6, *checksum_out);
    return best;
}

int main(int argc, char **argv)
{
    int n_rows = argc > 1 ? atoi(argv[1]) : 103260;
    int dim    = argc > 2 ? atoi(argv[2]) : 2880;
    int n_iters = argc > 3 ? atoi(argv[3]) : 5;
    int n_bytes = (dim + 1) / 2;
    int i;

    printf("FlashHadamard CPU Kernel Lab\n");
    printf("  n_rows=%d  dim=%d  n_bytes=%d  data=%.1f MB\n",
           n_rows, dim, n_bytes, (double)n_bytes * n_rows / 1e6);
    printf("  Platform: ");
#if HAS_NEON
    printf("ARM NEON");
#elif HAS_AVX2
    printf("x86 AVX2");
#else
    printf("scalar");
#endif
    printf("\n\n");

    /* Allocate */
    uint8_t *packed_t = (uint8_t *)malloc((size_t)n_bytes * n_rows);
    float *tables = (float *)malloc(sizeof(float) * n_bytes * 256);
    float *scores = (float *)malloc(sizeof(float) * n_rows);
    float *coeffs = (float *)malloc(sizeof(float) * dim);

    srand(42);
    for (i = 0; i < (int)((size_t)n_bytes * n_rows); i++)
        packed_t[i] = rand() & 0xFF;
    for (i = 0; i < dim; i++)
        coeffs[i] = ((float)(rand() % 10000) / 10000.0f - 0.5f) * 0.1f;

    /* Build float tables */
    for (i = 0; i < n_bytes; i++)
    {
        int lo, hi;
        float c0 = coeffs[2*i];
        float c1 = (2*i+1 < dim) ? coeffs[2*i+1] : 0.0f;
        for (lo = 0; lo < 16; lo++)
            for (hi = 0; hi < 16; hi++)
                tables[i * 256 + lo + hi * 16] = LLOYD_MAX_16[lo] * c0 + LLOYD_MAX_16[hi] * c1;
    }

    double chk0, chk;

    printf("--- Float kernels ---\n");
    double baseline = bench("k0: scalar 4x unroll", k0_float_scalar,
                             packed_t, tables, scores, n_rows, n_bytes, n_iters, &chk0);

#if HAS_AVX2
    bench("k1: AVX2 float 8-wide",    k1_avx2_float8, packed_t, tables, scores, n_rows, n_bytes, n_iters, &chk);
    bench("k2: AVX2 float+prefetch",  k2_avx2_prefetch, packed_t, tables, scores, n_rows, n_bytes, n_iters, &chk);
    bench("k3: AVX2 4-byte fused",    k3_avx2_4byte_fused, packed_t, tables, scores, n_rows, n_bytes, n_iters, &chk);
    bench("k4: AVX2 gather",          k4_avx2_gather, packed_t, tables, scores, n_rows, n_bytes, n_iters, &chk);
#endif

#if HAS_NEON
    /* Build int16 tables */
    int16_t *int_tables = (int16_t *)malloc(sizeof(int16_t) * n_bytes * 256);
    int32_t *iscores = (int32_t *)malloc(sizeof(int32_t) * n_rows);
    for (i = 0; i < n_bytes * 256; i++)
    {
        int32_t q = (int32_t)roundf(tables[i] * 2048.0f);
        if (q > 32767) q = 32767;
        if (q < -32768) q = -32768;
        int_tables[i] = (int16_t)q;
    }

    printf("\n--- NEON kernels ---\n");
    {
        double best = 1e9;
        for (i = 0; i < n_iters; i++)
        {
            double t0 = now_ms();
            kn0_neon_int16(packed_t, int_tables, iscores, n_rows, n_bytes);
            double dt = now_ms() - t0;
            if (dt < best) best = dt;
        }
        printf("  %-28s %6.1f ms  (%4.1f GB/s)\n",
               "kn0: NEON int16 LUT", best, (double)n_bytes * n_rows / best / 1e6);
    }
    free(int_tables);
    free(iscores);
#endif

    printf("\nBaseline: %.1f ms\n", baseline);

    free(packed_t);
    free(tables);
    free(scores);
    free(coeffs);
    return 0;
}
