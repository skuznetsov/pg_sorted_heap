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
SET sorted_heap.hnsw_ef_patience = 4;
SELECT round(min(v <=> (SELECT v FROM hnsw_test WHERE id = 1))::numeric, 6) AS self_dist_patience FROM (
  SELECT v FROM hnsw_test ORDER BY v <=> (SELECT v FROM hnsw_test WHERE id = 1) LIMIT 5
) x;
RESET sorted_heap.hnsw_ef_patience;

-- Planner guard: automatic index path is only valid for LIMIT <= ef_search
RESET enable_seqscan;
EXPLAIN (COSTS OFF)
SELECT id FROM hnsw_test ORDER BY v <=> '[0.8,0.6,0.9,0.1]'::svec;
EXPLAIN (COSTS OFF)
SELECT id FROM hnsw_test ORDER BY v <=> '[0.8,0.6,0.9,0.1]'::svec LIMIT 40;
EXPLAIN (COSTS OFF)
SELECT id FROM hnsw_test WHERE id > 10 ORDER BY v <=> '[0.8,0.6,0.9,0.1]'::svec LIMIT 5;
SET enable_seqscan = off;

-- Dead heap tuples before VACUUM: top-up must still fill LIMIT K
CREATE TABLE hnsw_dead (id int PRIMARY KEY, v svec(4));
INSERT INTO hnsw_dead
SELECT i,
       CASE
         WHEN i <= 20
           THEN format('[1,%s,0,0]', round((i / 1000.0)::numeric, 3))
         ELSE format('[0,1,%s,0]', round(((i - 20) / 1000.0)::numeric, 3))
       END::svec
FROM generate_series(1, 40) AS i;
SET client_min_messages = warning;
CREATE INDEX hnsw_dead_idx ON hnsw_dead USING sorted_hnsw (v) WITH (m = 16, ef_construction = 64);
SET client_min_messages = notice;
SET sorted_hnsw.ef_search = 8;
DELETE FROM hnsw_dead WHERE id BETWEEN 1 AND 10;
SELECT count(*) AS dead_topup_count FROM (
  SELECT id FROM hnsw_dead ORDER BY v <=> '[1,0,0,0]'::svec LIMIT 5
) x;
DROP TABLE hnsw_dead;
SET sorted_hnsw.ef_search = 32;

-- Dead-heavy churn after VACUUM: stale graph entry points must not make a
-- live table look empty. This models app-level reindexing that deletes old
-- chunks and inserts fresh chunks without rebuilding the ANN index.
CREATE TABLE hnsw_dead_churn (id serial PRIMARY KEY, v svec(4));
INSERT INTO hnsw_dead_churn (v)
SELECT format('[1,%s,0,0]', round((i / 1000.0)::numeric, 3))::svec
FROM generate_series(1, 200) AS i;
SET client_min_messages = warning;
CREATE INDEX hnsw_dead_churn_idx ON hnsw_dead_churn USING sorted_hnsw (v) WITH (m = 2, ef_construction = 8);
SET client_min_messages = notice;
DELETE FROM hnsw_dead_churn;
VACUUM hnsw_dead_churn;
INSERT INTO hnsw_dead_churn (v)
SELECT format('[1,%s,0,0]', round((i / 1000.0)::numeric, 3))::svec
FROM generate_series(1, 5) AS i;
SET sorted_hnsw.ef_search = 4;
COPY (
  SELECT count(*) = 4 AS dead_churn_fallback_ok FROM (
    SELECT id FROM hnsw_dead_churn ORDER BY v <=> '[1,0,0,0]'::svec LIMIT 4
  ) x
) TO STDOUT;
DROP TABLE hnsw_dead_churn;
SET sorted_hnsw.ef_search = 32;

-- DELETE + VACUUM
DELETE FROM hnsw_test WHERE id BETWEEN 1 AND 5;
VACUUM hnsw_test;
SELECT count(*) AS after_delete FROM (
  SELECT id FROM hnsw_test ORDER BY v <=> '[0.8,0.6,0.9,0.1]'::svec LIMIT 5
) x;

-- INSERT after build
INSERT INTO hnsw_test (v) VALUES ('[0.8,0.6,0.9,0.1]');
SELECT count(*) AS total_after_insert FROM hnsw_test;

-- Same-session scan cache must see the new exact vector immediately
SELECT round(min(v <=> '[0.8,0.6,0.9,0.1]'::svec)::numeric, 6) AS exact_after_insert FROM (
  SELECT v FROM hnsw_test ORDER BY v <=> '[0.8,0.6,0.9,0.1]'::svec LIMIT 1
) x;

-- Repeated same-session inserts must keep cached neighbor pointers valid
INSERT INTO hnsw_test (v)
SELECT format('[0.8,0.6,0.9,%s]', round((g / 100.0)::numeric, 2))::svec
FROM generate_series(11, 26) AS g;
SELECT round(min(v <=> '[0.8,0.6,0.9,0.1]'::svec)::numeric, 6) AS exact_after_many_inserts FROM (
  SELECT v FROM hnsw_test ORDER BY v <=> '[0.8,0.6,0.9,0.1]'::svec LIMIT 1
) x;

-- REINDEX must invalidate cached graph state via relfilenode/cache_gen
SET client_min_messages = warning;
REINDEX INDEX hnsw_test_idx;
SET client_min_messages = notice;
COPY (
  SELECT (round(min(v <=> '[0.8,0.6,0.9,0.1]'::svec)::numeric, 6) = 0::numeric) AS exact_after_reindex_ok FROM (
    SELECT v FROM hnsw_test ORDER BY v <=> '[0.8,0.6,0.9,0.1]'::svec LIMIT 1
  ) x
) TO STDOUT;

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

-- Low-memory SQ8 build path
SET sorted_hnsw.build_sq8 = on;
CREATE TABLE hnsw_buildsq8 (id serial PRIMARY KEY, v svec(4));
INSERT INTO hnsw_buildsq8 (v)
SELECT format('[%s,%s,%s,%s]',
  round((sin(id * 1.0))::numeric, 4),
  round((cos(id * 1.0))::numeric, 4),
  round((sin(id * 2.0))::numeric, 4),
  round((cos(id * 2.0))::numeric, 4))::svec
FROM generate_series(1, 80) AS id;
CREATE INDEX hnsw_buildsq8_idx ON hnsw_buildsq8 USING sorted_hnsw (v) WITH (m = 16, ef_construction = 64);
COPY (
  SELECT count(*) AS buildsq8_result_count FROM (
    SELECT id FROM hnsw_buildsq8 ORDER BY v <=> '[0.8,0.6,0.9,0.1]'::svec LIMIT 5
  ) x
) TO STDOUT;
COPY (
  SELECT round(min(v <=> (SELECT v FROM hnsw_buildsq8 WHERE id = 1))::numeric, 6) AS buildsq8_self_dist FROM (
    SELECT v FROM hnsw_buildsq8 ORDER BY v <=> (SELECT v FROM hnsw_buildsq8 WHERE id = 1) LIMIT 5
  ) x
) TO STDOUT;
DROP TABLE hnsw_buildsq8;
RESET sorted_hnsw.build_sq8;

-- Native hsvec path: no upcasted storage contract
CREATE TABLE hnsw_half (id serial PRIMARY KEY, v hsvec(4));
INSERT INTO hnsw_half (v)
SELECT format('[%s,%s,%s,%s]',
  round((sin(id * 1.0))::numeric, 4),
  round((cos(id * 1.0))::numeric, 4),
  round((sin(id * 2.0))::numeric, 4),
  round((cos(id * 2.0))::numeric, 4))::hsvec
FROM generate_series(1, 80) AS id;
CREATE INDEX hnsw_half_idx ON hnsw_half USING sorted_hnsw (v hsvec_cosine_ops) WITH (m = 16, ef_construction = 64);
SELECT count(*) AS hsvec_result_count FROM (
  SELECT id FROM hnsw_half ORDER BY v <=> '[0.8,0.6,0.9,0.1]'::hsvec LIMIT 5
) x;
SELECT round(min(v <=> (SELECT v FROM hnsw_half WHERE id = 1))::numeric, 6) AS hsvec_self_dist FROM (
  SELECT v FROM hnsw_half ORDER BY v <=> (SELECT v FROM hnsw_half WHERE id = 1) LIMIT 5
) x;
EXPLAIN (COSTS OFF)
SELECT id FROM hnsw_half ORDER BY v <=> '[0.8,0.6,0.9,0.1]'::hsvec LIMIT 5;

RESET enable_seqscan;
RESET sorted_hnsw.ef_search;
DROP TABLE hnsw_half;
DROP TABLE hnsw_empty;
DROP TABLE hnsw_test;
DROP EXTENSION pg_sorted_heap;
