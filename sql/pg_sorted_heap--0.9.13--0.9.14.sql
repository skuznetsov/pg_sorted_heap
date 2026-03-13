-- Adds svec_hnsw_scan: hierarchical HNSW search via sidecar tables.

CREATE FUNCTION @extschema@.svec_hnsw_scan(
    tbl           regclass,
    query         @extschema@.svec,
    prefix        text,
    ef_search     int4 DEFAULT 64,
    lim           int4 DEFAULT 10,
    rerank_topk   int4 DEFAULT 0
)
RETURNS TABLE(id text, distance float8)
AS '$libdir/pg_sorted_heap', 'svec_hnsw_scan'
LANGUAGE C STRICT STABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.svec_hnsw_scan(regclass, @extschema@.svec, text, int4, int4, int4)
IS 'Hierarchical HNSW search via sidecar PG tables built by build_hnsw_graph.py. prefix names the table family: {prefix}_meta (entry_nid, max_level), {prefix}_l0 (nid, sketch, neighbors, src_id, src_tid), {prefix}_l1..lN (nid, sketch, neighbors). Greedy ef=1 descent through upper levels, ef_search beam at L0, then exact cosine rerank via main table. Timing breakdown in DEBUG1 when sorted_heap.ann_timing=on.';
