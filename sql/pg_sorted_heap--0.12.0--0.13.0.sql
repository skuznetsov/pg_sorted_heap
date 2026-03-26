CREATE FUNCTION @extschema@.sorted_heap_graph_rag(
  rel regclass,
  query @extschema@.svec,
  relation_path int4[],
  ann_k int4 DEFAULT 64,
  top_k int4 DEFAULT 10,
  score_mode text DEFAULT 'path',
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS $$
DECLARE
  path_len int4;
  hop int4;
  seed_ids int4[];
  mode text;
  seed_sql text;
BEGIN
  IF ann_k < 1 THEN
    RAISE EXCEPTION 'sorted_heap_graph_rag: ann_k must be >= 1';
  END IF;
  IF top_k < 1 THEN
    RAISE EXCEPTION 'sorted_heap_graph_rag: top_k must be >= 1';
  END IF;
  IF limit_rows < 0 THEN
    RAISE EXCEPTION 'sorted_heap_graph_rag: limit_rows must be >= 0';
  END IF;
  IF relation_path IS NULL OR array_ndims(relation_path) <> 1 THEN
    RAISE EXCEPTION 'sorted_heap_graph_rag: relation_path must be a one-dimensional int4[]';
  END IF;

  path_len := array_length(relation_path, 1);
  IF path_len IS NULL OR path_len < 1 OR path_len > 2 THEN
    RAISE EXCEPTION 'sorted_heap_graph_rag: relation_path must have length 1 or 2';
  END IF;

  FOREACH hop IN ARRAY relation_path LOOP
    IF hop < -32768 OR hop > 32767 THEN
      RAISE EXCEPTION 'sorted_heap_graph_rag: relation_path element % is outside int2 range', hop;
    END IF;
  END LOOP;

  mode := coalesce(nullif(lower(btrim(score_mode)), ''), 'path');

  IF path_len = 1 THEN
    IF mode NOT IN ('endpoint', 'path') THEN
      RAISE EXCEPTION 'sorted_heap_graph_rag: score_mode % is invalid for 1-hop paths (expected endpoint or path)', mode;
    END IF;

    seed_sql := format(
      'SELECT array_agg(entity_id::int4)
       FROM (
         SELECT DISTINCT entity_id
         FROM (
           SELECT entity_id
           FROM %s
           ORDER BY embedding <=> $1
           LIMIT $2
         ) ann
       ) seeds',
      rel);
    EXECUTE seed_sql USING query, ann_k INTO seed_ids;

    IF seed_ids IS NULL OR array_length(seed_ids, 1) IS NULL THEN
      RETURN;
    END IF;

    RETURN QUERY
    SELECT g.entity_id, g.relation_id, g.target_id, g.payload, g.distance
    FROM @extschema@.sorted_heap_expand_rerank(
      rel,
      seed_ids,
      query,
      top_k,
      relation_path[1],
      limit_rows
    ) AS g;
    RETURN;
  END IF;

  IF mode = 'path' THEN
    RETURN QUERY
    SELECT g.entity_id, g.relation_id, g.target_id, g.payload, g.distance
    FROM @extschema@.sorted_heap_graph_rag_twohop_path_scan(
      rel,
      query,
      ann_k,
      top_k,
      relation_path[1],
      relation_path[2],
      limit_rows
    ) AS g;
    RETURN;
  END IF;

  IF mode = 'endpoint' THEN
    RETURN QUERY
    SELECT g.entity_id, g.relation_id, g.target_id, g.payload, g.distance
    FROM @extschema@.sorted_heap_graph_rag_twohop_scan(
      rel,
      query,
      ann_k,
      top_k,
      relation_path[1],
      relation_path[2],
      limit_rows
    ) AS g;
    RETURN;
  END IF;

  RAISE EXCEPTION 'sorted_heap_graph_rag: unsupported score_mode % (expected endpoint or path)', mode;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION @extschema@.sorted_heap_graph_rag(regclass, @extschema@.svec, int4[], int4, int4, text, int4)
IS 'Unified fact-shaped GraphRAG entry point. relation_path length 1 performs ANN seed on entity_id plus one-hop rerank. relation_path length 2 performs ANN seed plus two-hop endpoint or path-aware rerank depending on score_mode.';
