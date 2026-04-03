#include <stddef.h>
#include <stdlib.h>
#include <stdint.h>
#include <math.h>
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
static double g_tq_blockhadamard_packed4_topk_build_ms = 0.0;
static double g_tq_blockhadamard_packed4_topk_score_ms = 0.0;
static double g_tq_blockhadamard_packed4_topk_merge_ms = 0.0;
static uint64_t g_tq_blockhadamard_packed4_topk_calls = 0;

static double tq_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1000000.0;
}

TQ_EXPORT void turboquant_blockhadamard_packed4_profile_reset(void) {
    g_tq_blockhadamard_packed4_build_ms = 0.0;
    g_tq_blockhadamard_packed4_score_ms = 0.0;
    g_tq_blockhadamard_packed4_calls = 0;
    g_tq_blockhadamard_packed4_topk_build_ms = 0.0;
    g_tq_blockhadamard_packed4_topk_score_ms = 0.0;
    g_tq_blockhadamard_packed4_topk_merge_ms = 0.0;
    g_tq_blockhadamard_packed4_topk_calls = 0;
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

TQ_EXPORT void turboquant_blockhadamard_packed4_topk_profile_get(
    double *build_ms,
    double *score_ms,
    double *merge_ms,
    uint64_t *calls
) {
    if (build_ms != NULL) {
        *build_ms = g_tq_blockhadamard_packed4_topk_build_ms;
    }
    if (score_ms != NULL) {
        *score_ms = g_tq_blockhadamard_packed4_topk_score_ms;
    }
    if (merge_ms != NULL) {
        *merge_ms = g_tq_blockhadamard_packed4_topk_merge_ms;
    }
    if (calls != NULL) {
        *calls = g_tq_blockhadamard_packed4_topk_calls;
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

    size_t byte_idx = 0;
    for (; byte_idx + 1 < n_bytes; byte_idx += 2) {
        const uint8_t *codes0 = packed_codes_t + (byte_idx + 0) * n_rows;
        const uint8_t *codes1 = packed_codes_t + (byte_idx + 1) * n_rows;
        const float *table0 = byte_tables + (byte_idx + 0) * 256u;
        const float *table1 = byte_tables + (byte_idx + 1) * 256u;
        size_t row = 0;
        for (; row + 3 < n_rows; row += 4) {
            out_scores[row + 0] += table0[codes0[row + 0]] + table1[codes1[row + 0]];
            out_scores[row + 1] += table0[codes0[row + 1]] + table1[codes1[row + 1]];
            out_scores[row + 2] += table0[codes0[row + 2]] + table1[codes1[row + 2]];
            out_scores[row + 3] += table0[codes0[row + 3]] + table1[codes1[row + 3]];
        }
        for (; row < n_rows; row++) {
            out_scores[row] += table0[codes0[row]] + table1[codes1[row]];
        }
    }
    if (byte_idx < n_bytes) {
        const uint8_t *codes = packed_codes_t + byte_idx * n_rows;
        const float *table = byte_tables + byte_idx * 256u;
        for (size_t row = 0; row < n_rows; row++) {
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

typedef struct {
    const uint8_t *packed_codes_t;
    const float *byte_tables;
    const float *norms;
    size_t n_rows;
    size_t n_bytes;
    size_t row_start;
    size_t row_end;
    size_t k;
    int32_t *top_ids;
    float *top_scores;
} tq_packed_adc_topk_task_t;

static void *tq_packed_adc_worker(void *arg) {
    tq_packed_adc_task_t *task = (tq_packed_adc_task_t *)arg;
    for (size_t row = task->row_start; row < task->row_end; row++) {
        task->out_scores[row] = 0.0f;
    }

    size_t byte_idx = 0;
    for (; byte_idx + 1 < task->n_bytes; byte_idx += 2) {
        const uint8_t *codes0 =
            task->packed_codes_t + (byte_idx + 0) * task->n_rows + task->row_start;
        const uint8_t *codes1 =
            task->packed_codes_t + (byte_idx + 1) * task->n_rows + task->row_start;
        const float *table0 = task->byte_tables + (byte_idx + 0) * 256u;
        const float *table1 = task->byte_tables + (byte_idx + 1) * 256u;
        size_t row = task->row_start;
        for (; row + 3 < task->row_end; row += 4) {
            task->out_scores[row + 0] += table0[codes0[0]] + table1[codes1[0]];
            task->out_scores[row + 1] += table0[codes0[1]] + table1[codes1[1]];
            task->out_scores[row + 2] += table0[codes0[2]] + table1[codes1[2]];
            task->out_scores[row + 3] += table0[codes0[3]] + table1[codes1[3]];
            codes0 += 4;
            codes1 += 4;
        }
        for (; row < task->row_end; row++) {
            task->out_scores[row] += table0[*codes0++] + table1[*codes1++];
        }
    }
    if (byte_idx < task->n_bytes) {
        const uint8_t *codes = task->packed_codes_t + byte_idx * task->n_rows + task->row_start;
        const float *table = task->byte_tables + byte_idx * 256u;
        for (size_t row = task->row_start; row < task->row_end; row++) {
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

static void tq_topk_insert(
    float score,
    int32_t row_id,
    float *top_scores,
    int32_t *top_ids,
    size_t k,
    size_t *filled,
    size_t *min_pos,
    float *min_score
) {
    if (*filled < k) {
        const size_t pos = *filled;
        top_scores[pos] = score;
        top_ids[pos] = row_id;
        (*filled)++;
        if (*filled == 1 || score < *min_score) {
            *min_score = score;
            *min_pos = pos;
        }
        return;
    }
    if (score <= *min_score) {
        return;
    }
    top_scores[*min_pos] = score;
    top_ids[*min_pos] = row_id;
    size_t new_min_pos = 0;
    float new_min_score = top_scores[0];
    for (size_t idx = 1; idx < k; idx++) {
        if (top_scores[idx] < new_min_score) {
            new_min_score = top_scores[idx];
            new_min_pos = idx;
        }
    }
    *min_pos = new_min_pos;
    *min_score = new_min_score;
}

static void *tq_packed_adc_topk_worker(void *arg) {
    tq_packed_adc_topk_task_t *task = (tq_packed_adc_topk_task_t *)arg;
    const size_t rows = task->row_end - task->row_start;
    float *scores = (float *)malloc(rows * sizeof(float));
    if (scores == NULL) {
        for (size_t idx = 0; idx < task->k; idx++) {
            task->top_ids[idx] = -1;
            task->top_scores[idx] = -INFINITY;
        }
        return NULL;
    }
    for (size_t row = 0; row < rows; row++) {
        scores[row] = 0.0f;
    }

    size_t byte_idx = 0;
    for (; byte_idx + 1 < task->n_bytes; byte_idx += 2) {
        const uint8_t *codes0 =
            task->packed_codes_t + (byte_idx + 0) * task->n_rows + task->row_start;
        const uint8_t *codes1 =
            task->packed_codes_t + (byte_idx + 1) * task->n_rows + task->row_start;
        const float *table0 = task->byte_tables + (byte_idx + 0) * 256u;
        const float *table1 = task->byte_tables + (byte_idx + 1) * 256u;
        size_t row = 0;
        for (; row + 3 < rows; row += 4) {
            scores[row + 0] += table0[codes0[row + 0]] + table1[codes1[row + 0]];
            scores[row + 1] += table0[codes0[row + 1]] + table1[codes1[row + 1]];
            scores[row + 2] += table0[codes0[row + 2]] + table1[codes1[row + 2]];
            scores[row + 3] += table0[codes0[row + 3]] + table1[codes1[row + 3]];
        }
        for (; row < rows; row++) {
            scores[row] += table0[codes0[row]] + table1[codes1[row]];
        }
    }
    if (byte_idx < task->n_bytes) {
        const uint8_t *codes = task->packed_codes_t + byte_idx * task->n_rows + task->row_start;
        const float *table = task->byte_tables + byte_idx * 256u;
        for (size_t row = 0; row < rows; row++) {
            scores[row] += table[codes[row]];
        }
    }

    size_t filled = 0;
    size_t min_pos = 0;
    float min_score = -INFINITY;
    for (size_t row = 0; row < rows; row++) {
        float score = scores[row];
        if (task->norms != NULL) {
            score *= task->norms[task->row_start + row];
        }
        tq_topk_insert(
            score,
            (int32_t)(task->row_start + row),
            task->top_scores,
            task->top_ids,
            task->k,
            &filled,
            &min_pos,
            &min_score
        );
    }
    for (size_t idx = filled; idx < task->k; idx++) {
        task->top_ids[idx] = -1;
        task->top_scores[idx] = -INFINITY;
    }
    free(scores);
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

TQ_EXPORT void turboquant_blockhadamard_packed4_topk_t_mt_f32(
    const uint8_t *TQ_RESTRICT packed_codes_t,
    const float *TQ_RESTRICT coeffs,
    const float *TQ_RESTRICT centers,
    const float *TQ_RESTRICT norms,
    size_t n_rows,
    size_t dim,
    size_t n_threads,
    size_t k,
    int32_t *TQ_RESTRICT out_ids,
    float *TQ_RESTRICT out_scores
) {
#if defined(_WIN32)
    (void)n_threads;
    (void)k;
    (void)out_ids;
    (void)out_scores;
#else
    const size_t n_bytes = (dim + 1u) / 2u;
    const double build_t0 = tq_now_ms();
    float *byte_tables = (float *)malloc(n_bytes * 256u * sizeof(float));
    if (byte_tables == NULL) {
        return;
    }
    tq_blockhadamard_packed4_build_byte_tables(coeffs, centers, dim, byte_tables);
    const double score_t0 = tq_now_ms();
    if (n_threads <= 1 || n_rows < 16384 || n_bytes < 256) {
        n_threads = 1;
    }
    if (n_threads > n_rows) {
        n_threads = n_rows;
    }
    if (n_threads > 64) {
        n_threads = 64;
    }

    pthread_t threads[64];
    tq_packed_adc_topk_task_t tasks[64];
    size_t chunk = (n_rows + n_threads - 1) / n_threads;
    int32_t *all_ids = (int32_t *)malloc(n_threads * k * sizeof(int32_t));
    float *all_scores = (float *)malloc(n_threads * k * sizeof(float));
    if (all_ids == NULL || all_scores == NULL) {
        free(byte_tables);
        free(all_ids);
        free(all_scores);
        return;
    }
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
        tasks[idx].k = k;
        tasks[idx].top_ids = all_ids + idx * k;
        tasks[idx].top_scores = all_scores + idx * k;
        pthread_create(&threads[idx], NULL, tq_packed_adc_topk_worker, &tasks[idx]);
        launched++;
    }
    for (size_t idx = 0; idx < launched; idx++) {
        pthread_join(threads[idx], NULL);
    }

    const double merge_t0 = tq_now_ms();
    size_t filled = 0;
    size_t min_pos = 0;
    float min_score = -INFINITY;
    for (size_t idx = 0; idx < launched * k; idx++) {
        if (all_ids[idx] < 0) {
            continue;
        }
        tq_topk_insert(
            all_scores[idx],
            all_ids[idx],
            out_scores,
            out_ids,
            k,
            &filled,
            &min_pos,
            &min_score
        );
    }
    for (size_t idx = filled; idx < k; idx++) {
        out_ids[idx] = -1;
        out_scores[idx] = -INFINITY;
    }
    for (size_t i = 0; i < filled; i++) {
        for (size_t j = i + 1; j < filled; j++) {
            if (out_scores[j] > out_scores[i]) {
                float ts = out_scores[i];
                out_scores[i] = out_scores[j];
                out_scores[j] = ts;
                int32_t ti = out_ids[i];
                out_ids[i] = out_ids[j];
                out_ids[j] = ti;
            }
        }
    }
    g_tq_blockhadamard_packed4_topk_build_ms += score_t0 - build_t0;
    g_tq_blockhadamard_packed4_topk_score_ms += merge_t0 - score_t0;
    g_tq_blockhadamard_packed4_topk_merge_ms += tq_now_ms() - merge_t0;
    g_tq_blockhadamard_packed4_topk_calls += 1;
    free(byte_tables);
    free(all_ids);
    free(all_scores);
#endif
}
