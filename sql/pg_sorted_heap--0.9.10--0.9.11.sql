-- pg_sorted_heap upgrade: 0.9.10 → 0.9.11
-- Adds svec_graph_scan: NSW graph search via btree-backed sidecar table.

CREATE FUNCTION @extschema@.svec_graph_scan(
    tbl           regclass,
    query         @extschema@.svec,
    graph_table   text,
    ef_search     int4 DEFAULT 64,
    lim           int4 DEFAULT 10
)
RETURNS TABLE(id text, distance float8)
AS '$libdir/pg_sorted_heap', 'svec_graph_scan'
LANGUAGE C STRICT STABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.svec_graph_scan(regclass, @extschema@.svec, text, int4, int4)
IS 'NSW graph search via btree-backed sidecar. Navigate graph using hsvec sketch distances, exact rerank via main table embedding. Graph table needs: nid int4 PK, sketch hsvec, neighbors int4[], src_id text, src_tid tid.';
