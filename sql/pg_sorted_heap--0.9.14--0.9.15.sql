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
IS 'Hierarchical HNSW search via sidecar PG tables. prefix names the table family: {prefix}_meta, {prefix}_l0..lN, and optionally {prefix}_r1 (nid int4 PK, rerank_vec hsvec). Pipeline: greedy ef=1 descent through upper levels → ef_search beam at L0 (hsvec(384) sketch) → optional dense rerank via {prefix}_r1 keeping top rerank1_topk → optional exact svec cosine rerank via main table keeping top lim. Set rerank1_topk>0 to enable the dense r1 stage (requires _r1 sidecar populated with hsvec(768) prefix embeddings). Timing breakdown in DEBUG1 when sorted_heap.ann_timing=on.';
