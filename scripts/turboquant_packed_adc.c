#include <stddef.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#if !defined(_WIN32)
#include <pthread.h>
#endif

#if defined(__GNUC__) || defined(__clang__)
#define TQ_RESTRICT __restrict__
#else
#define TQ_RESTRICT
#endif

#if defined(_WIN32)
#define TQ_EXPORT __declspec(dllexport)
#else
#define TQ_EXPORT
#endif

static double g_tq_blockhadamard_packed4_build_ms = 0.0;
static double g_tq_blockhadamard_packed4_score_ms = 0.0;
static uint64_t g_tq_blockhadamard_packed4_calls = 0;

static double tq_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1000000.0;
}

TQ_EXPORT void turboquant_blockhadamard_packed4_profile_reset(void) {
    g_tq_blockhadamard_packed4_build_ms = 0.0;
    g_tq_blockhadamard_packed4_score_ms = 0.0;
    g_tq_blockhadamard_packed4_calls = 0;
}

TQ_EXPORT void turboquant_blockhadamard_packed4_profile_get(
    double *build_ms,
    double *score_ms,
    uint64_t *calls
) {
    if (build_ms != NULL) {
        *build_ms = g_tq_blockhadamard_packed4_build_ms;
    }
    if (score_ms != NULL) {
        *score_ms = g_tq_blockhadamard_packed4_score_ms;
    }
    if (calls != NULL) {
        *calls = g_tq_blockhadamard_packed4_calls;
    }
}

TQ_EXPORT void turboquant_packed_adc_scores_f32(
    const uint8_t *TQ_RESTRICT packed_codes,
    const float *TQ_RESTRICT byte_tables,
    const float *TQ_RESTRICT norms,
    size_t n_rows,
    size_t n_bytes,
    float *TQ_RESTRICT out_scores
) {
    for (size_t row = 0; row < n_rows; row++) {
        const uint8_t *codes = packed_codes + row * n_bytes;
        float acc0 = 0.0f;
        float acc1 = 0.0f;
        float acc2 = 0.0f;
        float acc3 = 0.0f;
        size_t byte_idx = 0;

        for (; byte_idx + 3 < n_bytes; byte_idx += 4) {
            acc0 += byte_tables[(byte_idx + 0) * 256u + codes[byte_idx + 0]];
            acc1 += byte_tables[(byte_idx + 1) * 256u + codes[byte_idx + 1]];
            acc2 += byte_tables[(byte_idx + 2) * 256u + codes[byte_idx + 2]];
            acc3 += byte_tables[(byte_idx + 3) * 256u + codes[byte_idx + 3]];
        }
        for (; byte_idx < n_bytes; byte_idx++) {
            acc0 += byte_tables[byte_idx * 256u + codes[byte_idx]];
        }

        float score = (acc0 + acc1) + (acc2 + acc3);
        if (norms != NULL) {
            score *= norms[row];
        }
        out_scores[row] = score;
    }
}

TQ_EXPORT void turboquant_packed_adc_scores_t_f32(
    const uint8_t *TQ_RESTRICT packed_codes_t,
    const float *TQ_RESTRICT byte_tables,
    const float *TQ_RESTRICT norms,
    size_t n_rows,
    size_t n_bytes,
    float *TQ_RESTRICT out_scores
) {
    for (size_t row = 0; row < n_rows; row++) {
        out_scores[row] = 0.0f;
    }

    for (size_t byte_idx = 0; byte_idx < n_bytes; byte_idx++) {
        const uint8_t *codes = packed_codes_t + byte_idx * n_rows;
        const float *table = byte_tables + byte_idx * 256u;
        size_t row = 0;
        for (; row + 3 < n_rows; row += 4) {
            out_scores[row + 0] += table[codes[row + 0]];
            out_scores[row + 1] += table[codes[row + 1]];
            out_scores[row + 2] += table[codes[row + 2]];
            out_scores[row + 3] += table[codes[row + 3]];
        }
        for (; row < n_rows; row++) {
            out_scores[row] += table[codes[row]];
        }
    }

    if (norms != NULL) {
        for (size_t row = 0; row < n_rows; row++) {
            out_scores[row] *= norms[row];
        }
    }
}

static void tq_blockhadamard_packed4_score_range(
    const uint8_t *TQ_RESTRICT packed_codes_t,
    const float *TQ_RESTRICT coeffs,
    const float *TQ_RESTRICT centers,
    const float *TQ_RESTRICT norms,
    size_t n_rows,
    size_t dim,
    size_t row_start,
    size_t row_end,
    float *TQ_RESTRICT out_scores
) {
    const size_t n_bytes = (dim + 1u) / 2u;
    for (size_t row = row_start; row < row_end; row++) {
        out_scores[row] = 0.0f;
    }

    for (size_t byte_idx = 0; byte_idx < n_bytes; byte_idx++) {
        const size_t lo_dim = byte_idx * 2u;
        const size_t hi_dim = lo_dim + 1u;
        float lo_scaled[16];
        float hi_scaled[16];
        const float lo_coeff = coeffs[lo_dim];
        const float hi_coeff = hi_dim < dim ? coeffs[hi_dim] : 0.0f;
        for (size_t level = 0; level < 16; level++) {
            lo_scaled[level] = lo_coeff * centers[level];
            hi_scaled[level] = hi_coeff * centers[level];
        }

        const uint8_t *codes = packed_codes_t + byte_idx * n_rows + row_start;
        size_t row = row_start;
        for (; row + 3 < row_end; row += 4) {
            const uint8_t code0 = codes[0];
            const uint8_t code1 = codes[1];
            const uint8_t code2 = codes[2];
            const uint8_t code3 = codes[3];
            out_scores[row + 0] += lo_scaled[code0 & 0x0Fu] + hi_scaled[code0 >> 4];
            out_scores[row + 1] += lo_scaled[code1 & 0x0Fu] + hi_scaled[code1 >> 4];
            out_scores[row + 2] += lo_scaled[code2 & 0x0Fu] + hi_scaled[code2 >> 4];
            out_scores[row + 3] += lo_scaled[code3 & 0x0Fu] + hi_scaled[code3 >> 4];
            codes += 4;
        }
        for (; row < row_end; row++) {
            const uint8_t code = *codes++;
            out_scores[row] += lo_scaled[code & 0x0Fu] + hi_scaled[code >> 4];
        }
    }

    if (norms != NULL) {
        for (size_t row = row_start; row < row_end; row++) {
            out_scores[row] *= norms[row];
        }
    }
}

static int tq_blockhadamard_packed4_build_byte_tables(
    const float *TQ_RESTRICT coeffs,
    const float *TQ_RESTRICT centers,
    size_t dim,
    float *TQ_RESTRICT byte_tables
) {
    const size_t n_bytes = (dim + 1u) / 2u;
    for (size_t byte_idx = 0; byte_idx < n_bytes; byte_idx++) {
        const size_t lo_dim = byte_idx * 2u;
        const size_t hi_dim = lo_dim + 1u;
        float lo_scaled[16];
        float hi_scaled[16];
        const float lo_coeff = coeffs[lo_dim];
        const float hi_coeff = hi_dim < dim ? coeffs[hi_dim] : 0.0f;
        float *table = byte_tables + byte_idx * 256u;
        for (size_t level = 0; level < 16; level++) {
            lo_scaled[level] = lo_coeff * centers[level];
            hi_scaled[level] = hi_coeff * centers[level];
        }
        for (size_t code = 0; code < 256u; code++) {
            table[code] = lo_scaled[code & 0x0Fu] + hi_scaled[code >> 4];
        }
    }
    return 1;
}

TQ_EXPORT void turboquant_blockhadamard_packed4_scores_t_f32(
    const uint8_t *TQ_RESTRICT packed_codes_t,
    const float *TQ_RESTRICT coeffs,
    const float *TQ_RESTRICT centers,
    const float *TQ_RESTRICT norms,
    size_t n_rows,
    size_t dim,
    float *TQ_RESTRICT out_scores
) {
    const size_t n_bytes = (dim + 1u) / 2u;
    float *byte_tables = (float *)malloc(n_bytes * 256u * sizeof(float));
    if (byte_tables != NULL) {
        const double build_t0 = tq_now_ms();
        tq_blockhadamard_packed4_build_byte_tables(coeffs, centers, dim, byte_tables);
        const double score_t0 = tq_now_ms();
        turboquant_packed_adc_scores_t_f32(
            packed_codes_t,
            byte_tables,
            norms,
            n_rows,
            n_bytes,
            out_scores
        );
        g_tq_blockhadamard_packed4_build_ms += score_t0 - build_t0;
        g_tq_blockhadamard_packed4_score_ms += tq_now_ms() - score_t0;
        g_tq_blockhadamard_packed4_calls += 1;
        free(byte_tables);
        return;
    }
    const double score_t0 = tq_now_ms();
    tq_blockhadamard_packed4_score_range(
        packed_codes_t,
        coeffs,
        centers,
        norms,
        n_rows,
        dim,
        0,
        n_rows,
        out_scores
    );
    g_tq_blockhadamard_packed4_score_ms += tq_now_ms() - score_t0;
    g_tq_blockhadamard_packed4_calls += 1;
}

#if !defined(_WIN32)
typedef struct {
    const uint8_t *packed_codes_t;
    const float *byte_tables;
    const float *norms;
    size_t n_rows;
    size_t n_bytes;
    size_t row_start;
    size_t row_end;
    float *out_scores;
} tq_packed_adc_task_t;

typedef struct {
    const uint8_t *packed_codes_t;
    const float *coeffs;
    const float *centers;
    const float *norms;
    size_t n_rows;
    size_t dim;
    size_t row_start;
    size_t row_end;
    float *out_scores;
} tq_blockhadamard_packed4_task_t;

static void *tq_packed_adc_worker(void *arg) {
    tq_packed_adc_task_t *task = (tq_packed_adc_task_t *)arg;
    for (size_t row = task->row_start; row < task->row_end; row++) {
        task->out_scores[row] = 0.0f;
    }

    for (size_t byte_idx = 0; byte_idx < task->n_bytes; byte_idx++) {
        const uint8_t *codes = task->packed_codes_t + byte_idx * task->n_rows + task->row_start;
        const float *table = task->byte_tables + byte_idx * 256u;
        size_t row = task->row_start;
        for (; row + 3 < task->row_end; row += 4) {
            task->out_scores[row + 0] += table[codes[0]];
            task->out_scores[row + 1] += table[codes[1]];
            task->out_scores[row + 2] += table[codes[2]];
            task->out_scores[row + 3] += table[codes[3]];
            codes += 4;
        }
        for (; row < task->row_end; row++) {
            task->out_scores[row] += table[*codes++];
        }
    }

    if (task->norms != NULL) {
        for (size_t row = task->row_start; row < task->row_end; row++) {
            task->out_scores[row] *= task->norms[row];
        }
    }
    return NULL;
}

static void *tq_blockhadamard_packed4_worker(void *arg) {
    tq_blockhadamard_packed4_task_t *task = (tq_blockhadamard_packed4_task_t *)arg;
    tq_blockhadamard_packed4_score_range(
        task->packed_codes_t,
        task->coeffs,
        task->centers,
        task->norms,
        task->n_rows,
        task->dim,
        task->row_start,
        task->row_end,
        task->out_scores
    );
    return NULL;
}
#endif

TQ_EXPORT void turboquant_packed_adc_scores_t_mt_f32(
    const uint8_t *TQ_RESTRICT packed_codes_t,
    const float *TQ_RESTRICT byte_tables,
    const float *TQ_RESTRICT norms,
    size_t n_rows,
    size_t n_bytes,
    size_t n_threads,
    float *TQ_RESTRICT out_scores
) {
#if defined(_WIN32)
    (void)n_threads;
    turboquant_packed_adc_scores_t_f32(packed_codes_t, byte_tables, norms, n_rows, n_bytes, out_scores);
#else
    if (n_threads <= 1 || n_rows < 16384 || n_bytes < 256) {
        turboquant_packed_adc_scores_t_f32(packed_codes_t, byte_tables, norms, n_rows, n_bytes, out_scores);
        return;
    }
    if (n_threads > n_rows) {
        n_threads = n_rows;
    }

    pthread_t threads[64];
    tq_packed_adc_task_t tasks[64];
    if (n_threads > 64) {
        n_threads = 64;
    }
    size_t chunk = (n_rows + n_threads - 1) / n_threads;
    size_t launched = 0;
    for (size_t idx = 0; idx < n_threads; idx++) {
        size_t start = idx * chunk;
        if (start >= n_rows) {
            break;
        }
        size_t end = start + chunk;
        if (end > n_rows) {
            end = n_rows;
        }
        tasks[idx].packed_codes_t = packed_codes_t;
        tasks[idx].byte_tables = byte_tables;
        tasks[idx].norms = norms;
        tasks[idx].n_rows = n_rows;
        tasks[idx].n_bytes = n_bytes;
        tasks[idx].row_start = start;
        tasks[idx].row_end = end;
        tasks[idx].out_scores = out_scores;
        pthread_create(&threads[idx], NULL, tq_packed_adc_worker, &tasks[idx]);
        launched++;
    }
    for (size_t idx = 0; idx < launched; idx++) {
        pthread_join(threads[idx], NULL);
    }
#endif
}

TQ_EXPORT void turboquant_blockhadamard_packed4_scores_t_mt_f32(
    const uint8_t *TQ_RESTRICT packed_codes_t,
    const float *TQ_RESTRICT coeffs,
    const float *TQ_RESTRICT centers,
    const float *TQ_RESTRICT norms,
    size_t n_rows,
    size_t dim,
    size_t n_threads,
    float *TQ_RESTRICT out_scores
) {
#if defined(_WIN32)
    (void)n_threads;
    turboquant_blockhadamard_packed4_scores_t_f32(
        packed_codes_t,
        coeffs,
        centers,
        norms,
        n_rows,
        dim,
        out_scores
    );
#else
    const size_t n_bytes = (dim + 1u) / 2u;
    float *byte_tables = (float *)malloc(n_bytes * 256u * sizeof(float));
    if (byte_tables != NULL) {
        const double build_t0 = tq_now_ms();
        tq_blockhadamard_packed4_build_byte_tables(coeffs, centers, dim, byte_tables);
        const double score_t0 = tq_now_ms();
        turboquant_packed_adc_scores_t_mt_f32(
            packed_codes_t,
            byte_tables,
            norms,
            n_rows,
            n_bytes,
            n_threads,
            out_scores
        );
        g_tq_blockhadamard_packed4_build_ms += score_t0 - build_t0;
        g_tq_blockhadamard_packed4_score_ms += tq_now_ms() - score_t0;
        g_tq_blockhadamard_packed4_calls += 1;
        free(byte_tables);
        return;
    }
    if (n_threads <= 1 || n_rows < 16384 || n_bytes < 256) {
        turboquant_blockhadamard_packed4_scores_t_f32(
            packed_codes_t,
            coeffs,
            centers,
            norms,
            n_rows,
            dim,
            out_scores
        );
        return;
    }
    if (n_threads > n_rows) {
        n_threads = n_rows;
    }

    pthread_t threads[64];
    tq_blockhadamard_packed4_task_t tasks[64];
    if (n_threads > 64) {
        n_threads = 64;
    }
    size_t chunk = (n_rows + n_threads - 1) / n_threads;
    size_t launched = 0;
    for (size_t idx = 0; idx < n_threads; idx++) {
        size_t start = idx * chunk;
        if (start >= n_rows) {
            break;
        }
        size_t end = start + chunk;
        if (end > n_rows) {
            end = n_rows;
        }
        tasks[idx].packed_codes_t = packed_codes_t;
        tasks[idx].coeffs = coeffs;
        tasks[idx].centers = centers;
        tasks[idx].norms = norms;
        tasks[idx].n_rows = n_rows;
        tasks[idx].dim = dim;
        tasks[idx].row_start = start;
        tasks[idx].row_end = end;
        tasks[idx].out_scores = out_scores;
        pthread_create(&threads[idx], NULL, tq_blockhadamard_packed4_worker, &tasks[idx]);
        launched++;
    }
    for (size_t idx = 0; idx < launched; idx++) {
        pthread_join(threads[idx], NULL);
    }
#endif
}
