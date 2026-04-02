#include <stddef.h>
#include <stdint.h>

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
