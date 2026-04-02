#include <stddef.h>
#include <stdint.h>
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
