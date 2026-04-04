-- FlashHadamard regression tests
-- Run: psql -d fh_test -f scripts/test_flashhadamard.sql

\set ON_ERROR_STOP off
\pset tuples_only on

-- Setup
\echo '=== FlashHadamard Regression Tests ==='

-- Test 1: Build + scan on tiny data (correctness)
\echo 'TEST 1: tiny build + scan'
DROP TABLE IF EXISTS fh_reg_tiny CASCADE;
CREATE TABLE fh_reg_tiny (id serial PRIMARY KEY, embedding vector(8));
INSERT INTO fh_reg_tiny (embedding) VALUES
  ('[1,0,0,0,0,0,0,0]'),('[0,1,0,0,0,0,0,0]'),('[0,0,1,0,0,0,0,0]'),
  ('[0.5,0.5,0,0,0,0,0,0]'),('[0,0,0.5,0.5,0,0,0,0]');

SELECT flashhadamard_store_build('fh_reg_tiny'::regclass, 'embedding', '/tmp/fh_reg_tiny.store', 42, 4);
SELECT CASE WHEN count(*) = 3 THEN 'PASS' ELSE 'FAIL: expected 3 rows' END AS test1
FROM flashhadamard_store_scan('/tmp/fh_reg_tiny.store', '[1,0,0,0,0,0,0,0]'::vector(8), 3, 5, 42, 4);
DROP TABLE fh_reg_tiny;

-- Test 2: Dim mismatch → clean error (not crash)
\echo 'TEST 2: dim mismatch error'
SELECT CASE WHEN true THEN 'running...' END;
DO $$
BEGIN
  PERFORM flashhadamard_store_scan('/tmp/fh_reg_tiny.store', '[1,0,0]'::vector(3), 3, 5, 42, 4);
  RAISE NOTICE 'FAIL: should have errored on dim mismatch';
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'PASS: dim mismatch caught: %', SQLERRM;
END;
$$;

-- Test 3: Store not found → clean error
\echo 'TEST 3: store not found error'
DO $$
BEGIN
  PERFORM flashhadamard_store_scan('/tmp/nonexistent_fh.store', '[1,0,0,0,0,0,0,0]'::vector(8), 3, 5, 42, 4);
  RAISE NOTICE 'FAIL: should have errored on missing store';
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'PASS: missing store caught: %', SQLERRM;
END;
$$;

-- Test 4: Top-1 score correctness (exact match should be ~1.0)
\echo 'TEST 4: top-1 score correctness'
SELECT CASE
  WHEN score > 0.99 AND score < 1.01 THEN 'PASS: top score ≈ 1.0'
  ELSE 'FAIL: top score = ' || score::text
END AS test4
FROM flashhadamard_store_scan('/tmp/fh_reg_tiny.store', '[1,0,0,0,0,0,0,0]'::vector(8), 1, 5, 42, 4)
LIMIT 1;

-- Test 5: Build on medium data (1000 × 64D)
\echo 'TEST 5: medium build + scan'
DROP TABLE IF EXISTS fh_reg_med CASCADE;
CREATE TABLE fh_reg_med (id serial PRIMARY KEY, embedding vector(64));
INSERT INTO fh_reg_med (embedding)
  SELECT ('[' || string_agg(v::text, ',') || ']')::vector
  FROM generate_series(1, 1000) r,
       LATERAL (SELECT random()::float4 v FROM generate_series(1, 64)) d
  GROUP BY r;

SELECT flashhadamard_store_build('fh_reg_med'::regclass, 'embedding', '/tmp/fh_reg_med.store', 42, 16);
SELECT CASE WHEN count(*) = 10 THEN 'PASS' ELSE 'FAIL: expected 10 rows' END AS test5
FROM flashhadamard_store_scan('/tmp/fh_reg_med.store',
  (SELECT embedding FROM fh_reg_med WHERE id = 1), 10, 12, 42, 16);
DROP TABLE fh_reg_med;

\echo '=== Tests Complete ==='
