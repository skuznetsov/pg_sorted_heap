/* pg_sorted_heap -- 0.10.0 to 0.11.0 upgrade
 *
 * No SQL-level changes. C-level additions:
 * - Hybrid L0 HNSW: full svec vectors in L0 sidecar (auto-detected)
 * - SQ8 scalar quantization for L0 cache (4x memory savings)
 * - sorted_heap.hnsw_cache_sq8 GUC (default on)
 * - --l0-dim / --full-vectors flags in build_hnsw_graph.py
 */
