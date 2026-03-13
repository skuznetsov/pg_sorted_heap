-- Upgrade 0.9.14 → 0.9.15
-- Adds rerank1_topk parameter to svec_hnsw_scan for two-stage dense rerank
-- via an optional {prefix}_r1 sidecar table (nid int4 PK, rerank_vec hsvec).
--
-- Must DROP the 6-arg function and recreate as 7-arg; PostgreSQL does not
-- allow adding parameters via CREATE OR REPLACE.

DROP FUNCTION IF EXISTS @extschema@.svec_hnsw_scan(regclass, @extschema@.svec, text, int4, int4, int4);

CREATE FUNCTION @extschema@.svec_hnsw_scan(
    tbl           regclass,
    query         @extschema@.svec,
    prefix        text,
    ef_search     int4 DEFAULT 64,
    lim           int4 DEFAULT 10,
    rerank_topk   int4 DEFAULT 0,
    rerank1_topk  int4 DEFAULT 0
)
RETURNS TABLE(id text, distance float8)
AS '$libdir/pg_sorted_heap', 'svec_hnsw_scan'
LANGUAGE C STRICT STABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.svec_hnsw_scan(regclass, @extschema@.svec, text, int4, int4, int4, int4)
IS 'Hierarchical HNSW search via sidecar PG tables. prefix names the table family: {prefix}_meta, {prefix}_l0..lN, and optionally {prefix}_r1 (nid int4 PK, rerank_vec hsvec). Pipeline: greedy ef=1 descent through upper levels → ef_search beam at L0 (hsvec(384) sketch) → optional dense rerank via {prefix}_r1 keeping top rerank1_topk → exact svec cosine rerank via main table keeping top lim. rerank_topk controls how many L0 candidates reach exact rerank: 0 (default) means no truncation (all ef_search candidates are reranked); 0 < rerank_topk < ef_search truncates before exact rerank, reducing TOAST I/O at the cost of recall; rerank_topk >= ef_search has no effect. rerank1_topk (default 0): set >0 to enable the dense r1 stage (requires {prefix}_r1 sidecar with hsvec prefix embeddings); absent _r1 is silently skipped. r1 is a cold-TOAST knob only — on warm buffer pools the btree lookup overhead exceeds TOAST savings; leave rerank1_topk=0 unless TOAST I/O is the measured bottleneck. Enable sorted_heap.hnsw_cache_l0 for session-local L0+upper cache (~100 MB, built on first call, evicted on relcache invalidation). Recommended operating points (103K × 2880-dim svec, warm pool, hsvec(384) sketch): balanced: ef_search=96 rerank_topk=48 → 0.98 ms p50, 96.8% recall@10; quality: ef_search=96 rerank_topk=0 → 1.83 ms p50, 98.4% recall@10; latency: ef_search=64 rerank_topk=32 → 0.85 ms p50, 92.8% recall@10. Timing breakdown in DEBUG1 when sorted_heap.ann_timing=on.';
