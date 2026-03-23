CREATE EXTENSION pg_sorted_heap;

-- SH-HNSW: sorted_hnsw Index AM tests
SET client_min_messages = warning;

-- Same-session interaction: run a sorted_heap custom scan before HNSW
CREATE TABLE sh_combo (id int PRIMARY KEY, note text) USING sorted_heap;
INSERT INTO sh_combo
SELECT i, repeat('x', 10)
FROM generate_series(1, 5000) AS i;
SELECT sorted_heap_compact('sh_combo'::regclass);
ANALYZE sh_combo;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF)
SELECT note FROM sh_combo WHERE id = 42;
RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE sh_combo;

CREATE TABLE hnsw_test (id serial PRIMARY KEY, v svec(4));
INSERT INTO hnsw_test (v)
SELECT format('[%s,%s,%s,%s]',
  round((sin(id * 1.0))::numeric, 4),
  round((cos(id * 1.0))::numeric, 4),
  round((sin(id * 2.0))::numeric, 4),
  round((cos(id * 2.0))::numeric, 4))::svec
FROM generate_series(1, 100) AS id;
CREATE INDEX hnsw_test_idx ON hnsw_test USING sorted_hnsw (v) WITH (m = 16, ef_construction = 64);
SET client_min_messages = notice;

SELECT pg_relation_size('hnsw_test_idx') > 0 AS hnsw_index_exists;

SET enable_seqscan = off;
SET sorted_hnsw.ef_search = 32;

SELECT count(*) AS hnsw_result_count FROM (
  SELECT id FROM hnsw_test ORDER BY v <=> '[0.8,0.6,0.9,0.1]'::svec LIMIT 5
) x;

-- Self-query
SELECT round(min(v <=> (SELECT v FROM hnsw_test WHERE id = 1))::numeric, 6) AS self_dist FROM (
  SELECT v FROM hnsw_test ORDER BY v <=> (SELECT v FROM hnsw_test WHERE id = 1) LIMIT 5
) x;

-- DELETE + VACUUM
DELETE FROM hnsw_test WHERE id BETWEEN 1 AND 5;
VACUUM hnsw_test;
SELECT count(*) AS after_delete FROM (
  SELECT id FROM hnsw_test ORDER BY v <=> '[0.8,0.6,0.9,0.1]'::svec LIMIT 5
) x;

-- INSERT after build
INSERT INTO hnsw_test (v) VALUES ('[0.5,0.5,0.5,0.5]');
SELECT count(*) AS total_after_insert FROM hnsw_test;

-- Query after DML
SELECT count(*) AS query_after_dml FROM (
  SELECT id FROM hnsw_test ORDER BY v <=> '[0.8,0.6,0.9,0.1]'::svec LIMIT 5
) x;

-- Empty index bootstrap: first INSERT must materialize the first node
SET client_min_messages = warning;
CREATE TABLE hnsw_empty (id serial PRIMARY KEY, v svec(4));
CREATE INDEX hnsw_empty_idx ON hnsw_empty USING sorted_hnsw (v) WITH (m = 16, ef_construction = 64);
INSERT INTO hnsw_empty (v) VALUES ('[1,0,0,0]');
SELECT count(*) AS empty_after_insert FROM hnsw_empty;
SELECT count(*) AS empty_query_count FROM (
  SELECT id FROM hnsw_empty ORDER BY v <=> '[1,0,0,0]'::svec LIMIT 1
) x;

RESET enable_seqscan;
RESET sorted_hnsw.ef_search;
DROP TABLE hnsw_empty;
DROP TABLE hnsw_test;
DROP EXTENSION pg_sorted_heap;
