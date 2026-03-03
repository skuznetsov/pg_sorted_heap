/*
 * pq.h
 *
 * Product Quantization (PQ) for approximate nearest neighbor search.
 *
 * Stores compact PQ codes (M bytes per vector) that enable fast
 * Asymmetric Distance Computation (ADC) without detoasting full vectors.
 *
 * Usage:
 *   1. svec_pq_train(query, M, Ksub, n_iter) → trains codebook from data
 *   2. svec_pq_encode(svec, codebook_id) → bytea PQ code
 *   3. svec_pq_distance(svec_query, bytea_code, codebook_id) → float8 ADC distance
 */
#ifndef PQ_H
#define PQ_H

#include "postgres.h"
#include "fmgr.h"

/* Maximum subvectors (also max PQ code length in bytes) */
#define PQ_MAX_M		1024

/* Centroids per subvector (fixed at 256 for 1-byte codes) */
#define PQ_KSUB			256

/* Maximum iterations for k-means training */
#define PQ_MAX_ITER		50

/* SQL-callable functions */
extern Datum svec_pq_train(PG_FUNCTION_ARGS);
extern Datum svec_pq_encode(PG_FUNCTION_ARGS);
extern Datum svec_pq_distance(PG_FUNCTION_ARGS);
extern Datum svec_pq_distance_table(PG_FUNCTION_ARGS);
extern Datum svec_pq_adc_lookup(PG_FUNCTION_ARGS);

#endif							/* PQ_H */
