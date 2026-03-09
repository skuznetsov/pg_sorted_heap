-- pg_sorted_heap upgrade: 0.9.9 → 0.9.10
-- Adds three-stage rerank with sidecar sketch table support to svec_ann_scan.

-- Drop old 8-arg signature to avoid stale wrapper over C symbol
DROP FUNCTION IF EXISTS @extschema@.svec_ann_scan(regclass, @extschema@.svec, int4, int4, int4, int4, int4, text);

-- Recreate with sketch_table + sketch_topk parameters
CREATE FUNCTION @extschema@.svec_ann_scan(
    tbl           regclass,
    query         @extschema@.svec,
    nprobe        int4 DEFAULT 10,
    lim           int4 DEFAULT 10,
    rerank_topk   int4 DEFAULT 0,
    cb_id         int4 DEFAULT 1,
    ivf_cb_id     int4 DEFAULT 0,
    pq_column     text DEFAULT 'pq_code',
    sketch_table  text DEFAULT '',
    sketch_topk   int4 DEFAULT 0
)
RETURNS TABLE(id text, distance float8)
AS '$libdir/pg_sorted_heap', 'svec_ann_scan'
LANGUAGE C STRICT STABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.svec_ann_scan(regclass, @extschema@.svec, int4, int4, int4, int4, int4, text, text, int4)
IS 'C-level IVF-PQ ANN scan with optional three-stage rerank. sketch_table names a sidecar table with (partition_id, id, sketch hsvec) for prefix-truncation sketch rerank (MRL embeddings).';
