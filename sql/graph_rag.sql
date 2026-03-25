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

SELECT sorted_heap_compact('facts_sh'::regclass);
ANALYZE facts_sh;

SELECT entity_id, relation_id, target_id, payload
FROM sorted_heap_expand_ids('facts_sh'::regclass, ARRAY[1,3], NULL, 0)
ORDER BY entity_id, relation_id, target_id;

SELECT entity_id, relation_id, target_id, payload
FROM sorted_heap_expand_ids('facts_sh'::regclass, ARRAY[1,3], 1, 0)
ORDER BY entity_id, relation_id, target_id;

SELECT entity_id, relation_id, target_id, payload
FROM sorted_heap_expand_ids('facts_sh'::regclass, ARRAY[1,3], NULL, 2)
ORDER BY entity_id, relation_id, target_id;

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
) diff;

DROP TABLE facts_sh;
DROP EXTENSION pg_sorted_heap;
