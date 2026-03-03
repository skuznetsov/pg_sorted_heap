#!/usr/bin/env python3
"""Generate test vectors as CSV for COPY INTO PostgreSQL."""
import numpy as np
import sys

N = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
DIMS = int(sys.argv[2]) if len(sys.argv) > 2 else 128
N_CLUSTERS = 50
SPREAD = 0.15
SEED = 42

rng = np.random.RandomState(SEED)

# Cluster centers on unit sphere
centers = rng.randn(N_CLUSTERS, DIMS).astype(np.float32)
centers /= np.linalg.norm(centers, axis=1, keepdims=True)

# Assign vectors to clusters
labels = rng.randint(0, N_CLUSTERS, N)
vectors = centers[labels] + rng.randn(N, DIMS).astype(np.float32) * SPREAD
norms = np.linalg.norm(vectors, axis=1, keepdims=True)
norms[norms == 0] = 1
vectors /= norms

# Output as CSV: one line per vector, pgvector format [x1,x2,...,xn]
for v in vectors:
    vals = ','.join(f'{x:.6f}' for x in v)
    print(f'[{vals}]')
