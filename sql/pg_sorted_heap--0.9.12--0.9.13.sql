-- pg_sorted_heap upgrade: 0.9.12 → 0.9.13
-- Adds entry_table parameter to svec_graph_scan for multi-entry NSW search.

-- Drop old 6-arg signature (C reads 7 args now — stale wrapper is UB)
DROP FUNCTION IF EXISTS @extschema@.svec_graph_scan(regclass, @extschema@.svec, text, int4, int4, int4);

CREATE FUNCTION @extschema@.svec_graph_scan(
    tbl           regclass,
    query         @extschema@.svec,
    graph_table   text,
    ef_search     int4 DEFAULT 64,
    lim           int4 DEFAULT 10,
    rerank_topk   int4 DEFAULT 0,
    entry_table   text DEFAULT ''
)
RETURNS TABLE(id text, distance float8)
AS '$libdir/pg_sorted_heap', 'svec_graph_scan'
LANGUAGE C STRICT STABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.svec_graph_scan(regclass, @extschema@.svec, text, int4, int4, int4, text)
IS 'NSW graph search via btree-backed sidecar. rerank_topk>0 pre-filters candidates by sketch distance before exact rerank. entry_table (entry_nid int4, centroid hsvec) enables multi-start NSW from closest centroids (empty=use nid=0). Graph table needs: nid int4 PK, sketch hsvec, neighbors int4[], src_id text, src_tid tid.';
