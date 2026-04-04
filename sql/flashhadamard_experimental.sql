-- FlashHadamard experimental packed retrieval functions.
-- NOT part of the extension upgrade path — load explicitly for experiments.
--
-- Usage:
--   \i sql/flashhadamard_experimental.sql
--   SELECT flashhadamard_build('gutenberg_gptoss'::regclass, 'embedding', 'fh_gutenberg', 42, 16);
--   SELECT * FROM flashhadamard_scan('fh_gutenberg', (SELECT embedding::vector FROM gutenberg_gptoss LIMIT 1), 10, 12);

CREATE OR REPLACE FUNCTION flashhadamard_build(
    source_tbl  regclass,
    embed_col   text,
    sidecar_tbl text,
    seed        int4 DEFAULT 42,
    group_size  int4 DEFAULT 16
) RETURNS int4
AS '$libdir/pg_sorted_heap', 'flashhadamard_build'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION flashhadamard_build(regclass, text, text, int4, int4) IS
'Build FlashHadamard sidecar table with packed 4-bit codes + SQ8 rerank codes. '
'Experimental — not part of the stable sorted_heap/HNSW path.';

CREATE OR REPLACE FUNCTION flashhadamard_scan(
    sidecar_tbl text,
    query       vector,
    k           int4 DEFAULT 10,
    shortlist_m int4 DEFAULT 12
) RETURNS TABLE(row_id int4, score float8)
AS '$libdir/pg_sorted_heap', 'flashhadamard_scan'
LANGUAGE C STABLE;

COMMENT ON FUNCTION flashhadamard_scan(text, vector, int4, int4) IS
'Two-stage FlashHadamard search: packed ADC shortlist → SQ8 rerank → top-k. '
'Experimental — not part of the stable sorted_heap/HNSW path.';
