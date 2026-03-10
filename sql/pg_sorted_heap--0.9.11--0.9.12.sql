-- pg_sorted_heap upgrade: 0.9.11 → 0.9.12
-- Adds rerank_topk parameter to svec_graph_scan for sketch pre-filtering.

-- Drop old 5-arg signature (C reads 6 args now — stale wrapper is UB)
DROP FUNCTION IF EXISTS @extschema@.svec_graph_scan(regclass, @extschema@.svec, text, int4, int4);

CREATE FUNCTION @extschema@.svec_graph_scan(
    tbl           regclass,
    query         @extschema@.svec,
    graph_table   text,
    ef_search     int4 DEFAULT 64,
    lim           int4 DEFAULT 10,
    rerank_topk   int4 DEFAULT 0
)
RETURNS TABLE(id text, distance float8)
AS '$libdir/pg_sorted_heap', 'svec_graph_scan'
LANGUAGE C STRICT STABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.svec_graph_scan(regclass, @extschema@.svec, text, int4, int4, int4)
IS 'NSW graph search via btree-backed sidecar. rerank_topk>0 pre-filters candidates by sketch distance before exact rerank (0=use all ef_search candidates). Graph table needs: nid int4 PK, sketch hsvec, neighbors int4[], src_id text, src_tid tid.';
