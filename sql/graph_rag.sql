CREATE EXTENSION pg_sorted_heap;

SET client_min_messages = warning;

CREATE TABLE facts_sh (
  entity_id   int4 NOT NULL,
  relation_id int2 NOT NULL,
  target_id   int4 NOT NULL,
  embedding   svec(4) NOT NULL,
  payload     text NOT NULL,
  PRIMARY KEY (entity_id, relation_id, target_id)
) USING sorted_heap;

INSERT INTO facts_sh VALUES
  (1, 1, 2, '[1,0,0,0]'::svec, 'a'),
  (1, 2, 3, '[0.9,0.1,0,0]'::svec, 'b'),
  (2, 1, 4, '[0,1,0,0]'::svec, 'c'),
  (3, 1, 1, '[0,0,1,0]'::svec, 'd'),
  (3, 2, 5, '[0,0,0.9,0.1]'::svec, 'e'),
  (4, 1, 6, '[0,0,0,1]'::svec, 'f');

COPY (
  SELECT 'ok'
  FROM (SELECT sorted_heap_compact('facts_sh'::regclass)) s
) TO STDOUT;
ANALYZE facts_sh;

COPY (
  SELECT entity_id, relation_id, target_id, payload
  FROM sorted_heap_expand_ids('facts_sh'::regclass, ARRAY[1,3], NULL, 0)
  ORDER BY entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  SELECT entity_id, relation_id, target_id, payload
  FROM sorted_heap_expand_ids('facts_sh'::regclass, ARRAY[1,3], 1, 0)
  ORDER BY entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  SELECT entity_id, relation_id, target_id, payload
  FROM sorted_heap_expand_ids('facts_sh'::regclass, ARRAY[1,3], NULL, 2)
  ORDER BY entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
  FROM sorted_heap_expand_rerank('facts_sh'::regclass, ARRAY[1,3], '[1,0,0,0]'::svec, 2, NULL, 0)
  ORDER BY distance, entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
  FROM sorted_heap_graph_rag_scan('facts_sh'::regclass, '[1,0,0,0]'::svec, 2, 2, NULL, 0)
  ORDER BY distance, entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  SELECT entity_id, relation_id, target_id, payload
  FROM sorted_heap_expand_ids('facts_sh'::regclass, ARRAY[1,3], 2, 0)
  ORDER BY entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
  FROM sorted_heap_expand_rerank('facts_sh'::regclass, ARRAY[1,3], '[1,0,0,0]'::svec, 2, 2, 0)
  ORDER BY distance, entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
  FROM sorted_heap_expand_twohop_rerank('facts_sh'::regclass, ARRAY[1,3], '[1,0,0,0]'::svec, 2, 1, 1, 0)
  ORDER BY distance, entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
  FROM sorted_heap_expand_twohop_path_rerank('facts_sh'::regclass, ARRAY[1,3], '[1,0,0,0]'::svec, 2, 1, 2, 0)
  ORDER BY distance, entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
  FROM sorted_heap_graph_rag_scan('facts_sh'::regclass, '[1,0,0,0]'::svec, 2, 2, 2, 0)
  ORDER BY distance, entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
  FROM sorted_heap_graph_rag_twohop_scan('facts_sh'::regclass, '[0,0,1,0]'::svec, 2, 2, 1, 2, 0)
  ORDER BY distance, entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
  FROM sorted_heap_graph_rag_twohop_path_scan('facts_sh'::regclass, '[0,0,1,0]'::svec, 2, 2, 1, 2, 0)
  ORDER BY distance, entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
  FROM sorted_heap_graph_rag(
    'facts_sh'::regclass,
    '[1,0,0,0]'::svec,
    relation_path := ARRAY[2],
    ann_k := 2,
    top_k := 2,
    score_mode := 'endpoint',
    limit_rows := 0
  )
  ORDER BY distance, entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
  FROM sorted_heap_graph_rag(
    'facts_sh'::regclass,
    '[0,0,1,0]'::svec,
    relation_path := ARRAY[1,2],
    ann_k := 2,
    top_k := 2,
    score_mode := 'endpoint',
    limit_rows := 0
  )
  ORDER BY distance, entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
  FROM sorted_heap_graph_rag(
    'facts_sh'::regclass,
    '[0,0,1,0]'::svec,
    relation_path := ARRAY[1,2],
    ann_k := 2,
    top_k := 2,
    score_mode := 'path',
    limit_rows := 0
  )
  ORDER BY distance, entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload
    FROM sorted_heap_expand_ids('facts_sh'::regclass, ARRAY[1,3], NULL, 0)
  ),
  sql_baseline AS (
    SELECT entity_id, relation_id, target_id, payload
    FROM facts_sh
    WHERE entity_id = ANY (ARRAY[1,3]::int4[])
  )
  SELECT count(*) AS diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM sql_baseline)
    UNION ALL
    (SELECT * FROM sql_baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_expand_rerank('facts_sh'::regclass, ARRAY[1,3], '[1,0,0,0]'::svec, 2, NULL, 0)
  ),
  sql_baseline AS (
    SELECT entity_id, relation_id, target_id, payload,
           round((embedding <=> '[1,0,0,0]'::svec)::numeric, 6) AS distance
    FROM facts_sh
    WHERE entity_id = ANY (ARRAY[1,3]::int4[])
    ORDER BY embedding <=> '[1,0,0,0]'::svec, entity_id, relation_id, target_id
    LIMIT 2
  )
  SELECT count(*) AS rerank_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM sql_baseline)
    UNION ALL
    (SELECT * FROM sql_baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_expand_twohop_rerank('facts_sh'::regclass, ARRAY[1,3], '[1,0,0,0]'::svec, 2, 1, 1, 0)
  ),
  hop1 AS MATERIALIZED (
    SELECT DISTINCT target_id
    FROM facts_sh
    WHERE entity_id = ANY (ARRAY[1,3]::int4[])
      AND relation_id = 1
  ),
  sql_baseline AS (
    SELECT entity_id, relation_id, target_id, payload,
           round((embedding <=> '[1,0,0,0]'::svec)::numeric, 6) AS distance
    FROM facts_sh
    WHERE entity_id = ANY (ARRAY(SELECT target_id FROM hop1))
      AND relation_id = 1
    ORDER BY embedding <=> '[1,0,0,0]'::svec, entity_id, relation_id, target_id
    LIMIT 2
  )
  SELECT count(*) AS twohop_rerank_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM sql_baseline)
    UNION ALL
    (SELECT * FROM sql_baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_expand_twohop_path_rerank('facts_sh'::regclass, ARRAY[1,3], '[1,0,0,0]'::svec, 2, 1, 2, 0)
  ),
  hop1 AS MATERIALIZED (
    SELECT DISTINCT ON (target_id)
           target_id AS parent_id,
           (embedding <=> '[1,0,0,0]'::svec) AS hop1_distance
    FROM facts_sh
    WHERE entity_id = ANY (ARRAY[1,3]::int4[])
      AND relation_id = 1
    ORDER BY target_id, embedding <=> '[1,0,0,0]'::svec, entity_id
  ),
  sql_baseline AS (
    SELECT city.entity_id, city.relation_id, city.target_id, city.payload,
           round(((city.embedding <=> '[1,0,0,0]'::svec) + hop1.hop1_distance)::numeric, 6) AS distance
    FROM facts_sh city
    JOIN hop1 ON hop1.parent_id = city.entity_id
    WHERE city.relation_id = 2
    ORDER BY ((city.embedding <=> '[1,0,0,0]'::svec) + hop1.hop1_distance),
             city.entity_id, city.relation_id, city.target_id
    LIMIT 2
  )
  SELECT count(*) AS twohop_path_rerank_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM sql_baseline)
    UNION ALL
    (SELECT * FROM sql_baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag_scan('facts_sh'::regclass, '[1,0,0,0]'::svec, 2, 2, NULL, 0)
  ),
  ann AS MATERIALIZED (
    SELECT target_id
    FROM facts_sh
    ORDER BY embedding <=> '[1,0,0,0]'::svec
    LIMIT 2
  ),
  seeds AS MATERIALIZED (
    SELECT DISTINCT target_id FROM ann
  ),
  expanded AS MATERIALIZED (
    SELECT *
    FROM facts_sh
    WHERE entity_id = ANY (ARRAY(SELECT target_id FROM seeds))
  ),
  sql_baseline AS (
    SELECT entity_id, relation_id, target_id, payload,
           round((embedding <=> '[1,0,0,0]'::svec)::numeric, 6) AS distance
    FROM expanded
    ORDER BY embedding <=> '[1,0,0,0]'::svec, entity_id, relation_id, target_id
    LIMIT 2
  )
  SELECT count(*) AS graph_rag_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM sql_baseline)
    UNION ALL
    (SELECT * FROM sql_baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag(
      'facts_sh'::regclass,
      '[1,0,0,0]'::svec,
      relation_path := ARRAY[2],
      ann_k := 2,
      top_k := 2,
      score_mode := 'endpoint',
      limit_rows := 0
    )
  ),
  ann AS MATERIALIZED (
    SELECT DISTINCT entity_id
    FROM (
      SELECT entity_id
      FROM facts_sh
      ORDER BY embedding <=> '[1,0,0,0]'::svec
      LIMIT 2
    ) ann
  ),
  sql_baseline AS (
    SELECT entity_id, relation_id, target_id, payload,
           round((embedding <=> '[1,0,0,0]'::svec)::numeric, 6) AS distance
    FROM facts_sh
    WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM ann))
      AND relation_id = 2
    ORDER BY embedding <=> '[1,0,0,0]'::svec, entity_id, relation_id, target_id
    LIMIT 2
  )
  SELECT count(*) AS graph_rag_unified_onehop_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM sql_baseline)
    UNION ALL
    (SELECT * FROM sql_baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag_twohop_scan('facts_sh'::regclass, '[0,0,1,0]'::svec, 2, 2, 1, 2, 0)
  ),
  ann AS MATERIALIZED (
    SELECT entity_id
    FROM facts_sh
    ORDER BY embedding <=> '[0,0,1,0]'::svec
    LIMIT 2
  ),
  seeds AS MATERIALIZED (
    SELECT DISTINCT entity_id FROM ann
  ),
  hop1 AS MATERIALIZED (
    SELECT DISTINCT target_id
    FROM facts_sh
    WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
      AND relation_id = 1
  ),
  sql_baseline AS (
    SELECT entity_id, relation_id, target_id, payload,
           round((embedding <=> '[0,0,1,0]'::svec)::numeric, 6) AS distance
    FROM facts_sh
    WHERE entity_id = ANY (ARRAY(SELECT target_id FROM hop1))
      AND relation_id = 2
    ORDER BY embedding <=> '[0,0,1,0]'::svec, entity_id, relation_id, target_id
    LIMIT 2
  )
  SELECT count(*) AS graph_rag_twohop_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM sql_baseline)
    UNION ALL
    (SELECT * FROM sql_baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag(
      'facts_sh'::regclass,
      '[0,0,1,0]'::svec,
      relation_path := ARRAY[1,2],
      ann_k := 2,
      top_k := 2,
      score_mode := 'endpoint',
      limit_rows := 0
    )
  ),
  baseline AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag_twohop_scan('facts_sh'::regclass, '[0,0,1,0]'::svec, 2, 2, 1, 2, 0)
  )
  SELECT count(*) AS graph_rag_unified_twohop_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM baseline)
    UNION ALL
    (SELECT * FROM baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag_twohop_path_scan('facts_sh'::regclass, '[0,0,1,0]'::svec, 2, 2, 1, 2, 0)
  ),
  ann AS MATERIALIZED (
    SELECT entity_id
    FROM facts_sh
    ORDER BY embedding <=> '[0,0,1,0]'::svec
    LIMIT 2
  ),
  seeds AS MATERIALIZED (
    SELECT DISTINCT entity_id FROM ann
  ),
  hop1 AS MATERIALIZED (
    SELECT DISTINCT ON (target_id)
           target_id AS parent_id,
           (embedding <=> '[0,0,1,0]'::svec) AS hop1_distance
    FROM facts_sh
    WHERE entity_id = ANY (ARRAY(SELECT entity_id FROM seeds))
      AND relation_id = 1
    ORDER BY target_id, embedding <=> '[0,0,1,0]'::svec, entity_id
  ),
  sql_baseline AS (
    SELECT city.entity_id, city.relation_id, city.target_id, city.payload,
           round(((city.embedding <=> '[0,0,1,0]'::svec) + hop1.hop1_distance)::numeric, 6) AS distance
    FROM facts_sh city
    JOIN hop1 ON hop1.parent_id = city.entity_id
    WHERE city.relation_id = 2
    ORDER BY ((city.embedding <=> '[0,0,1,0]'::svec) + hop1.hop1_distance),
             city.entity_id, city.relation_id, city.target_id
    LIMIT 2
  )
  SELECT count(*) AS graph_rag_twohop_path_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM sql_baseline)
    UNION ALL
    (SELECT * FROM sql_baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag(
      'facts_sh'::regclass,
      '[0,0,1,0]'::svec,
      relation_path := ARRAY[1,2],
      ann_k := 2,
      top_k := 2,
      score_mode := 'path',
      limit_rows := 0
    )
  ),
  baseline AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag_twohop_path_scan('facts_sh'::regclass, '[0,0,1,0]'::svec, 2, 2, 1, 2, 0)
  )
  SELECT count(*) AS graph_rag_unified_twohop_path_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM baseline)
    UNION ALL
    (SELECT * FROM baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload
    FROM sorted_heap_expand_ids('facts_sh'::regclass, ARRAY[1,3], 2, 0)
  ),
  sql_baseline AS (
    SELECT entity_id, relation_id, target_id, payload
    FROM facts_sh
    WHERE entity_id = ANY (ARRAY[1,3]::int4[])
      AND relation_id = 2
  )
  SELECT count(*) AS filtered_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM sql_baseline)
    UNION ALL
    (SELECT * FROM sql_baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_expand_rerank('facts_sh'::regclass, ARRAY[1,3], '[1,0,0,0]'::svec, 2, 2, 0)
  ),
  sql_baseline AS (
    SELECT entity_id, relation_id, target_id, payload,
           round((embedding <=> '[1,0,0,0]'::svec)::numeric, 6) AS distance
    FROM facts_sh
    WHERE entity_id = ANY (ARRAY[1,3]::int4[])
      AND relation_id = 2
    ORDER BY embedding <=> '[1,0,0,0]'::svec, entity_id, relation_id, target_id
    LIMIT 2
  )
  SELECT count(*) AS filtered_rerank_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM sql_baseline)
    UNION ALL
    (SELECT * FROM sql_baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag_scan('facts_sh'::regclass, '[1,0,0,0]'::svec, 2, 2, 2, 0)
  ),
  ann AS MATERIALIZED (
    SELECT target_id
    FROM facts_sh
    ORDER BY embedding <=> '[1,0,0,0]'::svec
    LIMIT 2
  ),
  seeds AS MATERIALIZED (
    SELECT DISTINCT target_id FROM ann
  ),
  expanded AS MATERIALIZED (
    SELECT *
    FROM facts_sh
    WHERE entity_id = ANY (ARRAY(SELECT target_id FROM seeds))
      AND relation_id = 2
  ),
  sql_baseline AS (
    SELECT entity_id, relation_id, target_id, payload,
           round((embedding <=> '[1,0,0,0]'::svec)::numeric, 6) AS distance
    FROM expanded
    ORDER BY embedding <=> '[1,0,0,0]'::svec, entity_id, relation_id, target_id
    LIMIT 2
  )
  SELECT count(*) AS filtered_graph_rag_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM sql_baseline)
    UNION ALL
    (SELECT * FROM sql_baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  SELECT 'ok'
  FROM (SELECT sorted_heap_graph_rag_reset_stats()) s
) TO STDOUT;

COPY (
  SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
  FROM sorted_heap_expand_rerank('facts_sh'::regclass, ARRAY[1,3], '[1,0,0,0]'::svec, 2, 2, 0)
  ORDER BY distance, entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  SELECT calls, api, seed_count, expanded_rows, reranked_rows, returned_rows,
         (ann_ms >= 0.0)::int,
         (expand_ms >= 0.0)::int,
         (rerank_ms >= 0.0)::int,
         (total_ms >= 0.0)::int
  FROM sorted_heap_graph_rag_stats()
) TO STDOUT;

COPY (
  SELECT 'ok'
  FROM (SELECT sorted_heap_graph_rag_reset_stats()) s
) TO STDOUT;

COPY (
  SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
  FROM sorted_heap_graph_rag(
    'facts_sh'::regclass,
    '[0,0,1,0]'::svec,
    relation_path := ARRAY[1,2],
    ann_k := 2,
    top_k := 2,
    score_mode := 'path',
    limit_rows := 0
  )
  ORDER BY distance, entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  SELECT calls, api, seed_count, expanded_rows, reranked_rows, returned_rows,
         (ann_ms >= 0.0)::int,
         (expand_ms >= 0.0)::int,
         (rerank_ms >= 0.0)::int,
         (total_ms >= 0.0)::int
  FROM sorted_heap_graph_rag_stats()
) TO STDOUT;

CREATE TABLE facts_chain_sh (
  entity_id   int4 NOT NULL,
  relation_id int2 NOT NULL,
  target_id   int4 NOT NULL,
  embedding   svec(4) NOT NULL,
  payload     text NOT NULL,
  PRIMARY KEY (entity_id, relation_id, target_id)
) USING sorted_heap;

INSERT INTO facts_chain_sh VALUES
  (8, 1, 18, '[-1,0,0,0]'::svec, 'm1'),
  (18, 2, 28, '[-0.8,-0.2,0,0]'::svec, 'm2'),
  (28, 3, 38, '[-0.6,-0.4,0,0]'::svec, 'm3'),
  (9, 1, 19, '[0,1,0,0]'::svec, 'n1'),
  (19, 2, 29, '[0,0.8,0.2,0]'::svec, 'n2'),
  (29, 3, 39, '[0,0.6,0.4,0]'::svec, 'n3');

COPY (
  SELECT 'ok'
  FROM (SELECT sorted_heap_compact('facts_chain_sh'::regclass)) s
) TO STDOUT;

ANALYZE facts_chain_sh;

COPY (
  SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
  FROM sorted_heap_expand_multihop_rerank(
    'facts_chain_sh'::regclass,
    ARRAY[8],
    '[-1,0,0,0]'::svec,
    2,
    ARRAY[1,2,3],
    0
  )
  ORDER BY distance, entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
  FROM sorted_heap_expand_multihop_path_rerank(
    'facts_chain_sh'::regclass,
    ARRAY[8],
    '[-1,0,0,0]'::svec,
    2,
    ARRAY[1,2,3],
    0
  )
  ORDER BY distance, entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
  FROM sorted_heap_graph_rag(
    'facts_chain_sh'::regclass,
    '[-1,0,0,0]'::svec,
    relation_path := ARRAY[1,2,3],
    ann_k := 1,
    top_k := 2,
    score_mode := 'endpoint',
    limit_rows := 0
  )
  ORDER BY distance, entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
  FROM sorted_heap_graph_rag(
    'facts_chain_sh'::regclass,
    '[-1,0,0,0]'::svec,
    relation_path := ARRAY[1,2,3],
    ann_k := 1,
    top_k := 2,
    score_mode := 'path',
    limit_rows := 0
  )
  ORDER BY distance, entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_expand_multihop_rerank(
      'facts_chain_sh'::regclass,
      ARRAY[8],
      '[-1,0,0,0]'::svec,
      2,
      ARRAY[1,2,3],
      0
    )
  ),
  hop1 AS MATERIALIZED (
    SELECT DISTINCT target_id
    FROM facts_chain_sh
    WHERE entity_id = ANY (ARRAY[8]::int4[])
      AND relation_id = 1
  ),
  hop2 AS MATERIALIZED (
    SELECT DISTINCT target_id
    FROM facts_chain_sh
    WHERE entity_id = ANY (ARRAY(SELECT target_id FROM hop1))
      AND relation_id = 2
  ),
  sql_baseline AS (
    SELECT entity_id, relation_id, target_id, payload,
           round((embedding <=> '[-1,0,0,0]'::svec)::numeric, 6) AS distance
    FROM facts_chain_sh
    WHERE entity_id = ANY (ARRAY(SELECT target_id FROM hop2))
      AND relation_id = 3
    ORDER BY embedding <=> '[-1,0,0,0]'::svec, entity_id, relation_id, target_id
    LIMIT 2
  )
  SELECT count(*) AS multihop_rerank_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM sql_baseline)
    UNION ALL
    (SELECT * FROM sql_baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_expand_multihop_path_rerank(
      'facts_chain_sh'::regclass,
      ARRAY[8],
      '[-1,0,0,0]'::svec,
      2,
      ARRAY[1,2,3],
      0
    )
  ),
  hop1 AS MATERIALIZED (
    SELECT DISTINCT ON (target_id)
           target_id AS node_1,
           (embedding <=> '[-1,0,0,0]'::svec) AS d1
    FROM facts_chain_sh
    WHERE entity_id = ANY (ARRAY[8]::int4[])
      AND relation_id = 1
    ORDER BY target_id, embedding <=> '[-1,0,0,0]'::svec, entity_id
  ),
  hop2 AS MATERIALIZED (
    SELECT DISTINCT ON (t.target_id)
           t.target_id AS node_2,
           (hop1.d1 + (t.embedding <=> '[-1,0,0,0]'::svec)) AS d2
    FROM facts_chain_sh t
    JOIN hop1 ON hop1.node_1 = t.entity_id
    WHERE t.relation_id = 2
    ORDER BY t.target_id, (hop1.d1 + (t.embedding <=> '[-1,0,0,0]'::svec)), t.entity_id
  ),
  sql_baseline AS (
    SELECT t.entity_id, t.relation_id, t.target_id, t.payload,
           round((hop2.d2 + (t.embedding <=> '[-1,0,0,0]'::svec))::numeric, 6) AS distance
    FROM facts_chain_sh t
    JOIN hop2 ON hop2.node_2 = t.entity_id
    WHERE t.relation_id = 3
    ORDER BY (hop2.d2 + (t.embedding <=> '[-1,0,0,0]'::svec)),
             t.entity_id, t.relation_id, t.target_id
    LIMIT 2
  )
  SELECT count(*) AS multihop_path_rerank_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM sql_baseline)
    UNION ALL
    (SELECT * FROM sql_baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

CREATE TABLE facts_chain_seg_a (
  entity_id   int4 NOT NULL,
  relation_id int2 NOT NULL,
  target_id   int4 NOT NULL,
  embedding   svec(4) NOT NULL,
  payload     text NOT NULL,
  PRIMARY KEY (entity_id, relation_id, target_id)
) USING sorted_heap;

CREATE TABLE facts_chain_seg_b (
  entity_id   int4 NOT NULL,
  relation_id int2 NOT NULL,
  target_id   int4 NOT NULL,
  embedding   svec(4) NOT NULL,
  payload     text NOT NULL,
  PRIMARY KEY (entity_id, relation_id, target_id)
) USING sorted_heap;

INSERT INTO facts_chain_seg_a VALUES
  (8, 1, 18, '[-1,0,0,0]'::svec, 'm1'),
  (18, 2, 28, '[-0.8,-0.2,0,0]'::svec, 'm2'),
  (28, 3, 38, '[-0.6,-0.4,0,0]'::svec, 'm3');

INSERT INTO facts_chain_seg_b VALUES
  (9, 1, 19, '[0,1,0,0]'::svec, 'n1'),
  (19, 2, 29, '[0,0.8,0.2,0]'::svec, 'n2'),
  (29, 3, 39, '[0,0.6,0.4,0]'::svec, 'n3');

COPY (
  SELECT 'ok'
  FROM (SELECT sorted_heap_compact('facts_chain_seg_a'::regclass)) s
) TO STDOUT;

COPY (
  SELECT 'ok'
  FROM (SELECT sorted_heap_compact('facts_chain_seg_b'::regclass)) s
) TO STDOUT;

ANALYZE facts_chain_seg_a;
ANALYZE facts_chain_seg_b;

COPY (
  SELECT 'ok'
  FROM (
    SELECT sorted_heap_graph_segment_register('chain_route', 'facts_chain_seg_a'::regclass, 1, 8, 'hot')
  ) s
) TO STDOUT;

COPY (
  SELECT 'ok'
  FROM (
    SELECT sorted_heap_graph_segment_register('chain_route', 'facts_chain_seg_b'::regclass, 9, 16, 'sealed')
  ) s
) TO STDOUT;

COPY (
  SELECT 'ok'
  FROM (
    SELECT sorted_heap_graph_segment_register('chain_grouped', 'facts_chain_seg_a'::regclass, 1, 16, 'hot')
  ) s
) TO STDOUT;

COPY (
  SELECT 'ok'
  FROM (
    SELECT sorted_heap_graph_segment_register('chain_grouped', 'facts_chain_seg_b'::regclass, 1, 16, 'sealed')
  ) s
) TO STDOUT;

COPY (
  SELECT route_name, rel::text, route_min, route_max, coalesce(segment_group, '')
  FROM sorted_heap_graph_segment_config('chain_route')
) TO STDOUT;

COPY (
  SELECT rel::text, route_min, route_max, coalesce(segment_group, '')
  FROM sorted_heap_graph_segment_resolve('chain_route', 8, 0)
) TO STDOUT;

COPY (
  SELECT rel::text, route_min, route_max, coalesce(segment_group, '')
  FROM sorted_heap_graph_segment_resolve('chain_grouped', 8, 0, ARRAY['hot'])
) TO STDOUT;

COPY (
  SELECT rel::text, route_min, route_max, coalesce(segment_group, '')
  FROM sorted_heap_graph_segment_resolve('chain_grouped', 8, 1, ARRAY['sealed','hot'])
) TO STDOUT;

COPY (
  SELECT 'ok'
  FROM (
    SELECT sorted_heap_graph_route_policy_register('chain_grouped', 'prefer_hot', ARRAY['hot','sealed'])
  ) s
) TO STDOUT;

COPY (
  SELECT 'ok'
  FROM (
    SELECT sorted_heap_graph_route_policy_register('chain_grouped', 'prefer_sealed', ARRAY['sealed','hot'])
  ) s
) TO STDOUT;

COPY (
  SELECT route_name, policy_name, array_to_string(segment_groups, ',')
  FROM sorted_heap_graph_route_policy_config('chain_grouped')
) TO STDOUT;

COPY (
  SELECT array_to_string(sorted_heap_graph_route_policy_groups('chain_grouped', 'prefer_sealed'), ',')
) TO STDOUT;

COPY (
  SELECT source_rel::text, entity_id, relation_id, target_id, payload,
         round(distance::numeric, 6) AS distance
  FROM sorted_heap_graph_rag_segmented(
    ARRAY['facts_chain_seg_a'::regclass, 'facts_chain_seg_b'::regclass],
    '[-1,0,0,0]'::svec,
    relation_path := ARRAY[1,2,3],
    ann_k := 1,
    top_k := 2,
    score_mode := 'path',
    limit_rows := 0
  )
  ORDER BY distance, entity_id, relation_id, target_id, source_rel::text
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag_segmented(
      ARRAY['facts_chain_seg_a'::regclass, 'facts_chain_seg_b'::regclass],
      '[-1,0,0,0]'::svec,
      relation_path := ARRAY[1,2,3],
      ann_k := 1,
      top_k := 2,
      score_mode := 'endpoint',
      limit_rows := 0
    )
  ),
  baseline AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM (
      SELECT entity_id, relation_id, target_id, payload, distance
      FROM sorted_heap_graph_rag(
        'facts_chain_seg_a'::regclass,
        '[-1,0,0,0]'::svec,
        relation_path := ARRAY[1,2,3],
        ann_k := 1,
        top_k := 2,
        score_mode := 'endpoint',
        limit_rows := 0
      )
      UNION ALL
      SELECT entity_id, relation_id, target_id, payload, distance
      FROM sorted_heap_graph_rag(
        'facts_chain_seg_b'::regclass,
        '[-1,0,0,0]'::svec,
        relation_path := ARRAY[1,2,3],
        ann_k := 1,
        top_k := 2,
        score_mode := 'endpoint',
        limit_rows := 0
      )
    ) merged
    ORDER BY distance, entity_id, relation_id, target_id
    LIMIT 2
  )
  SELECT count(*) AS segmented_multihop_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM baseline)
    UNION ALL
    (SELECT * FROM baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag_segmented(
      ARRAY['facts_chain_seg_a'::regclass, 'facts_chain_seg_b'::regclass],
      '[-1,0,0,0]'::svec,
      relation_path := ARRAY[1,2,3],
      ann_k := 1,
      top_k := 2,
      score_mode := 'path',
      limit_rows := 0
    )
  ),
  baseline AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM (
      SELECT entity_id, relation_id, target_id, payload, distance
      FROM sorted_heap_graph_rag(
        'facts_chain_seg_a'::regclass,
        '[-1,0,0,0]'::svec,
        relation_path := ARRAY[1,2,3],
        ann_k := 1,
        top_k := 2,
        score_mode := 'path',
        limit_rows := 0
      )
      UNION ALL
      SELECT entity_id, relation_id, target_id, payload, distance
      FROM sorted_heap_graph_rag(
        'facts_chain_seg_b'::regclass,
        '[-1,0,0,0]'::svec,
        relation_path := ARRAY[1,2,3],
        ann_k := 1,
        top_k := 2,
        score_mode := 'path',
        limit_rows := 0
      )
    ) merged
    ORDER BY distance, entity_id, relation_id, target_id
    LIMIT 2
  )
  SELECT count(*) AS segmented_multihop_path_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM baseline)
    UNION ALL
    (SELECT * FROM baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  SELECT source_rel::text, entity_id, relation_id, target_id, payload,
         round(distance::numeric, 6) AS distance
  FROM sorted_heap_graph_rag_routed(
    'chain_route',
    8,
    '[-1,0,0,0]'::svec,
    relation_path := ARRAY[1,2,3],
    ann_k := 1,
    top_k := 2,
    score_mode := 'path',
    limit_rows := 0
  )
  ORDER BY distance, entity_id, relation_id, target_id, source_rel::text
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag_routed(
      'chain_route',
      8,
      '[-1,0,0,0]'::svec,
      relation_path := ARRAY[1,2,3],
      ann_k := 1,
      top_k := 2,
      score_mode := 'path',
      limit_rows := 0
    )
  ),
  baseline AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag(
      'facts_chain_seg_a'::regclass,
      '[-1,0,0,0]'::svec,
      relation_path := ARRAY[1,2,3],
      ann_k := 1,
      top_k := 2,
      score_mode := 'path',
      limit_rows := 0
    )
  )
  SELECT count(*) AS routed_multihop_path_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM baseline)
    UNION ALL
    (SELECT * FROM baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag_routed(
      'chain_grouped',
      8,
      '[-1,0,0,0]'::svec,
      relation_path := ARRAY[1,2,3],
      ann_k := 1,
      top_k := 2,
      score_mode := 'path',
      limit_rows := 0,
      segment_groups := ARRAY['hot']
    )
  ),
  baseline AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag(
      'facts_chain_seg_a'::regclass,
      '[-1,0,0,0]'::svec,
      relation_path := ARRAY[1,2,3],
      ann_k := 1,
      top_k := 2,
      score_mode := 'path',
      limit_rows := 0
    )
  )
  SELECT count(*) AS routed_group_multihop_path_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM baseline)
    UNION ALL
    (SELECT * FROM baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag_routed(
      'chain_grouped',
      8,
      '[-1,0,0,0]'::svec,
      relation_path := ARRAY[1,2,3],
      ann_k := 1,
      top_k := 2,
      score_mode := 'path',
      limit_rows := 0,
      fanout_limit := 1,
      segment_groups := ARRAY['sealed','hot']
    )
  ),
  baseline AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag(
      'facts_chain_seg_b'::regclass,
      '[-1,0,0,0]'::svec,
      relation_path := ARRAY[1,2,3],
      ann_k := 1,
      top_k := 2,
      score_mode := 'path',
      limit_rows := 0
    )
  )
  SELECT count(*) AS routed_group_order_multihop_path_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM baseline)
    UNION ALL
    (SELECT * FROM baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag_routed_policy(
      'chain_grouped',
      8,
      'prefer_sealed',
      '[-1,0,0,0]'::svec,
      relation_path := ARRAY[1,2,3],
      ann_k := 1,
      top_k := 2,
      score_mode := 'path',
      limit_rows := 0,
      fanout_limit := 1
    )
  ),
  baseline AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag(
      'facts_chain_seg_b'::regclass,
      '[-1,0,0,0]'::svec,
      relation_path := ARRAY[1,2,3],
      ann_k := 1,
      top_k := 2,
      score_mode := 'path',
      limit_rows := 0
    )
  )
  SELECT count(*) AS routed_policy_multihop_path_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM baseline)
    UNION ALL
    (SELECT * FROM baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  SELECT 'ok'
  FROM (
    SELECT sorted_heap_graph_exact_register('chain_exact', 'left', 'facts_chain_seg_a'::regclass, 100, 'hot')
  ) s
) TO STDOUT;

COPY (
  SELECT 'ok'
  FROM (
    SELECT sorted_heap_graph_exact_register('chain_exact', 'both', 'facts_chain_seg_a'::regclass, 100, 'hot')
  ) s
) TO STDOUT;

COPY (
  SELECT 'ok'
  FROM (
    SELECT sorted_heap_graph_exact_register('chain_exact', 'both', 'facts_chain_seg_b'::regclass, 50, 'sealed')
  ) s
) TO STDOUT;

COPY (
  SELECT route_name, route_key, rel::text, priority, coalesce(segment_group, '')
  FROM sorted_heap_graph_exact_config('chain_exact')
) TO STDOUT;

COPY (
  SELECT rel::text, priority, coalesce(segment_group, '')
  FROM sorted_heap_graph_exact_resolve('chain_exact', 'left', 0)
) TO STDOUT;

COPY (
  SELECT rel::text, priority, coalesce(segment_group, '')
  FROM sorted_heap_graph_exact_resolve('chain_exact', 'both', 0)
) TO STDOUT;

COPY (
  SELECT rel::text, priority, coalesce(segment_group, '')
  FROM sorted_heap_graph_exact_resolve('chain_exact', 'both', 0, ARRAY['sealed'])
) TO STDOUT;

COPY (
  SELECT rel::text, priority, coalesce(segment_group, '')
  FROM sorted_heap_graph_exact_resolve('chain_exact', 'both', 1, ARRAY['sealed','hot'])
) TO STDOUT;

COPY (
  SELECT 'ok'
  FROM (
    SELECT sorted_heap_graph_route_policy_register('chain_exact', 'prefer_hot', ARRAY['hot','sealed'])
  ) s
) TO STDOUT;

COPY (
  SELECT 'ok'
  FROM (
    SELECT sorted_heap_graph_route_policy_register('chain_exact', 'prefer_sealed', ARRAY['sealed','hot'])
  ) s
) TO STDOUT;

COPY (
  SELECT route_name, policy_name, array_to_string(segment_groups, ',')
  FROM sorted_heap_graph_route_policy_config('chain_exact')
) TO STDOUT;

COPY (
  SELECT array_to_string(sorted_heap_graph_route_policy_groups('chain_exact', 'prefer_sealed'), ',')
) TO STDOUT;

COPY (
  SELECT source_rel::text, entity_id, relation_id, target_id, payload,
         round(distance::numeric, 6) AS distance
  FROM sorted_heap_graph_rag_routed_exact(
    'chain_exact',
    'left',
    '[-1,0,0,0]'::svec,
    relation_path := ARRAY[1,2,3],
    ann_k := 1,
    top_k := 2,
    score_mode := 'path',
    limit_rows := 0
  )
  ORDER BY distance, entity_id, relation_id, target_id, source_rel::text
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag_routed_exact(
      'chain_exact',
      'left',
      '[-1,0,0,0]'::svec,
      relation_path := ARRAY[1,2,3],
      ann_k := 1,
      top_k := 2,
      score_mode := 'path',
      limit_rows := 0
    )
  ),
  baseline AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag(
      'facts_chain_seg_a'::regclass,
      '[-1,0,0,0]'::svec,
      relation_path := ARRAY[1,2,3],
      ann_k := 1,
      top_k := 2,
      score_mode := 'path',
      limit_rows := 0
    )
  )
  SELECT count(*) AS routed_exact_multihop_path_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM baseline)
    UNION ALL
    (SELECT * FROM baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag_routed_exact(
      'chain_exact',
      'both',
      '[-1,0,0,0]'::svec,
      relation_path := ARRAY[1,2,3],
      ann_k := 1,
      top_k := 2,
      score_mode := 'path',
      limit_rows := 0
    )
  ),
  baseline AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag_segmented(
      ARRAY['facts_chain_seg_a'::regclass, 'facts_chain_seg_b'::regclass],
      '[-1,0,0,0]'::svec,
      relation_path := ARRAY[1,2,3],
      ann_k := 1,
      top_k := 2,
      score_mode := 'path',
      limit_rows := 0
    )
  )
  SELECT count(*) AS routed_exact_segmented_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM baseline)
    UNION ALL
    (SELECT * FROM baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag_routed_exact(
      'chain_exact',
      'both',
      '[-1,0,0,0]'::svec,
      relation_path := ARRAY[1,2,3],
      ann_k := 1,
      top_k := 2,
      score_mode := 'path',
      limit_rows := 0,
      segment_groups := ARRAY['sealed']
    )
  ),
  baseline AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag(
      'facts_chain_seg_b'::regclass,
      '[-1,0,0,0]'::svec,
      relation_path := ARRAY[1,2,3],
      ann_k := 1,
      top_k := 2,
      score_mode := 'path',
      limit_rows := 0
    )
  )
  SELECT count(*) AS routed_exact_group_multihop_path_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM baseline)
    UNION ALL
    (SELECT * FROM baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag_routed_exact(
      'chain_exact',
      'both',
      '[-1,0,0,0]'::svec,
      relation_path := ARRAY[1,2,3],
      ann_k := 1,
      top_k := 2,
      score_mode := 'path',
      limit_rows := 0,
      fanout_limit := 1,
      segment_groups := ARRAY['sealed','hot']
    )
  ),
  baseline AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag(
      'facts_chain_seg_b'::regclass,
      '[-1,0,0,0]'::svec,
      relation_path := ARRAY[1,2,3],
      ann_k := 1,
      top_k := 2,
      score_mode := 'path',
      limit_rows := 0
    )
  )
  SELECT count(*) AS routed_exact_group_order_multihop_path_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM baseline)
    UNION ALL
    (SELECT * FROM baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  WITH helper AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag_routed_exact_policy(
      'chain_exact',
      'both',
      'prefer_sealed',
      '[-1,0,0,0]'::svec,
      relation_path := ARRAY[1,2,3],
      ann_k := 1,
      top_k := 2,
      score_mode := 'path',
      limit_rows := 0,
      fanout_limit := 1
    )
  ),
  baseline AS (
    SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
    FROM sorted_heap_graph_rag(
      'facts_chain_seg_b'::regclass,
      '[-1,0,0,0]'::svec,
      relation_path := ARRAY[1,2,3],
      ann_k := 1,
      top_k := 2,
      score_mode := 'path',
      limit_rows := 0
    )
  )
  SELECT count(*) AS routed_exact_policy_multihop_path_diff_rows
  FROM (
    (SELECT * FROM helper EXCEPT ALL SELECT * FROM baseline)
    UNION ALL
    (SELECT * FROM baseline EXCEPT ALL SELECT * FROM helper)
  ) diff
) TO STDOUT;

COPY (
  SELECT sorted_heap_graph_route_policy_unregister('chain_exact')
) TO STDOUT;

COPY (
  SELECT sorted_heap_graph_route_policy_unregister('chain_grouped')
) TO STDOUT;

COPY (
  SELECT sorted_heap_graph_exact_unregister('chain_exact')
) TO STDOUT;

COPY (
  SELECT sorted_heap_graph_segment_unregister('chain_grouped')
) TO STDOUT;

COPY (
  SELECT sorted_heap_graph_segment_unregister('chain_route')
) TO STDOUT;

DROP TABLE facts_chain_seg_a;
DROP TABLE facts_chain_seg_b;

DROP TABLE facts_chain_sh;

CREATE TABLE facts_alias (
  src_id    int4 NOT NULL,
  edge_type int2 NOT NULL,
  dst_id    int4 NOT NULL,
  vec       svec(4) NOT NULL,
  body      text NOT NULL,
  PRIMARY KEY (src_id, edge_type, dst_id)
) USING sorted_heap;

INSERT INTO facts_alias VALUES
  (1, 1, 2, '[1,0,0,0]'::svec, 'a'),
  (1, 2, 3, '[0.9,0.1,0,0]'::svec, 'b'),
  (2, 1, 4, '[0,1,0,0]'::svec, 'c'),
  (3, 1, 1, '[0,0,1,0]'::svec, 'd'),
  (3, 2, 5, '[0,0,0.9,0.1]'::svec, 'e'),
  (4, 1, 6, '[0,0,0,1]'::svec, 'f');

COPY (
  SELECT 'ok'
  FROM (SELECT sorted_heap_compact('facts_alias'::regclass)) s
) TO STDOUT;

COPY (
  SELECT 'ok'
  FROM (
    SELECT sorted_heap_graph_register(
      'facts_alias'::regclass,
      entity_column := 'src_id',
      relation_column := 'edge_type',
      target_column := 'dst_id',
      embedding_column := 'vec',
      payload_column := 'body'
    )
  ) s
) TO STDOUT;

COPY (
  SELECT entity_column::text, relation_column::text, target_column::text,
         embedding_column::text, payload_column::text, is_registered
  FROM sorted_heap_graph_config('facts_alias'::regclass)
) TO STDOUT;

COPY (
  SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
  FROM sorted_heap_expand_rerank('facts_alias'::regclass, ARRAY[1,3], '[1,0,0,0]'::svec, 2, 2, 0)
  ORDER BY distance, entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
  FROM sorted_heap_graph_rag(
    'facts_alias'::regclass,
    '[1,0,0,0]'::svec,
    relation_path := ARRAY[2],
    ann_k := 2,
    top_k := 2,
    score_mode := 'endpoint',
    limit_rows := 0
  )
  ORDER BY distance, entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  SELECT entity_id, relation_id, target_id, payload, round(distance::numeric, 6) AS distance
  FROM sorted_heap_graph_rag_twohop_path_scan('facts_alias'::regclass, '[0,0,1,0]'::svec, 2, 2, 1, 2, 0)
  ORDER BY distance, entity_id, relation_id, target_id
) TO STDOUT;

COPY (
  SELECT sorted_heap_graph_unregister('facts_alias'::regclass)
) TO STDOUT;

COPY (
  SELECT entity_column::text, relation_column::text, target_column::text,
         embedding_column::text, payload_column::text, is_registered
  FROM sorted_heap_graph_config('facts_alias'::regclass)
) TO STDOUT;

DROP TABLE facts_alias;
DROP TABLE facts_sh;
DROP EXTENSION pg_sorted_heap;
