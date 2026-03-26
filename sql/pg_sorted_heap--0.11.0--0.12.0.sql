-- sorted_hnsw Index Access Method
-- v0.12.0: Index AM for HNSW vector search

-- AM handler function
CREATE FUNCTION @extschema@.sorted_hnsw_handler(internal)
RETURNS index_am_handler
AS '$libdir/pg_sorted_heap', 'sorted_hnsw_handler'
LANGUAGE C STRICT;

-- Register the access method
CREATE ACCESS METHOD sorted_hnsw TYPE INDEX
HANDLER @extschema@.sorted_hnsw_handler;

COMMENT ON ACCESS METHOD sorted_hnsw
IS 'HNSW index for approximate nearest neighbor search on svec and hsvec columns. '
   'Stores SQ8-quantized vectors inline in index tuples for sub-millisecond search. '
   'Usage: CREATE INDEX ON tbl USING sorted_hnsw (col svec_cosine_ops|hsvec_cosine_ops) WITH (m=16, ef_construction=200); '
   'Query: SELECT * FROM tbl ORDER BY col <=> query_vec LIMIT 10; '
   'GUC: sorted_hnsw.ef_search (default 96) controls search beam width.';

-- Operator classes for cosine distance ordering.
CREATE OPERATOR CLASS svec_cosine_ops
DEFAULT FOR TYPE @extschema@.svec USING sorted_hnsw AS
    OPERATOR 1 @extschema@.<=> (@extschema@.svec, @extschema@.svec) FOR ORDER BY float_ops,
    FUNCTION 1 @extschema@.svec_cosine_distance(@extschema@.svec, @extschema@.svec);

CREATE OPERATOR CLASS hsvec_cosine_ops
DEFAULT FOR TYPE @extschema@.hsvec USING sorted_hnsw AS
    OPERATOR 1 @extschema@.<=> (@extschema@.hsvec, @extschema@.hsvec) FOR ORDER BY float_ops,
    FUNCTION 1 @extschema@.hsvec_cosine_distance(@extschema@.hsvec, @extschema@.hsvec);

-- Narrow GraphRAG helper for expanding known source entity IDs on sorted_heap.
CREATE FUNCTION @extschema@.sorted_heap_expand_ids(
  rel regclass,
  seed_ids int4[],
  relation_filter int4 DEFAULT NULL,
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  entity_id int4,
  relation_id int2,
  target_id int4,
  embedding @extschema@.svec,
  payload text
)
AS '$libdir/pg_sorted_heap', 'sorted_heap_expand_ids'
LANGUAGE C STABLE;

CREATE FUNCTION @extschema@.sorted_heap_expand_rerank(
  rel regclass,
  seed_ids int4[],
  query @extschema@.svec,
  top_k int4,
  relation_filter int4 DEFAULT NULL,
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS '$libdir/pg_sorted_heap', 'sorted_heap_expand_rerank'
LANGUAGE C STABLE;

CREATE FUNCTION @extschema@.sorted_heap_expand_twohop_rerank(
  rel regclass,
  seed_ids int4[],
  query @extschema@.svec,
  top_k int4,
  hop1_relation_filter int4 DEFAULT NULL,
  hop2_relation_filter int4 DEFAULT NULL,
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS '$libdir/pg_sorted_heap', 'sorted_heap_expand_twohop_rerank'
LANGUAGE C STABLE;

CREATE FUNCTION @extschema@.sorted_heap_expand_twohop_path_rerank(
  rel regclass,
  seed_ids int4[],
  query @extschema@.svec,
  top_k int4,
  hop1_relation_filter int4 DEFAULT NULL,
  hop2_relation_filter int4 DEFAULT NULL,
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS '$libdir/pg_sorted_heap', 'sorted_heap_expand_twohop_path_rerank'
LANGUAGE C STABLE;

CREATE FUNCTION @extschema@.sorted_heap_graph_rag_scan(
  rel regclass,
  query @extschema@.svec,
  ann_k int4,
  top_k int4,
  relation_filter int4 DEFAULT NULL,
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS '$libdir/pg_sorted_heap', 'sorted_heap_graph_rag_scan'
LANGUAGE C STABLE;

CREATE FUNCTION @extschema@.sorted_heap_graph_rag_twohop_scan(
  rel regclass,
  query @extschema@.svec,
  ann_k int4,
  top_k int4,
  hop1_relation_filter int4 DEFAULT NULL,
  hop2_relation_filter int4 DEFAULT NULL,
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS '$libdir/pg_sorted_heap', 'sorted_heap_graph_rag_twohop_scan'
LANGUAGE C STABLE;

CREATE FUNCTION @extschema@.sorted_heap_graph_rag_twohop_path_scan(
  rel regclass,
  query @extschema@.svec,
  ann_k int4,
  top_k int4,
  hop1_relation_filter int4 DEFAULT NULL,
  hop2_relation_filter int4 DEFAULT NULL,
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS '$libdir/pg_sorted_heap', 'sorted_heap_graph_rag_twohop_path_scan'
LANGUAGE C STABLE;
