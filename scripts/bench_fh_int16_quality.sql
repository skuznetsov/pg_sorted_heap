-- FlashHadamard int16 kernel quality validation
--
-- Compares float vs int16 final SQ8-reranked results.
-- Run 4 times with different env configs:
--   FH_THREADS=1                (1t float baseline)
--   FH_THREADS=1 FH_INT16=1    (1t int16)
--   FH_THREADS=8                (8t float baseline)
--   FH_THREADS=8 FH_INT16=1    (8t int16)
--
-- Each run saves results to a persistent table.
-- After all 4 runs, compare quality.
--
-- Usage:
--   # Step 1: Run each config (restart PG between each)
--   FH_THREADS=1 pg_ctl restart ...
--   psql -d fh_test -v config=1t_float -f scripts/bench_fh_int16_quality.sql
--
--   FH_THREADS=1 FH_INT16=1 pg_ctl restart ...
--   psql -d fh_test -v config=1t_int16 -f scripts/bench_fh_int16_quality.sql
--
--   # etc.
--
--   # Step 2: Compare (any session, tables persist)
--   psql -d fh_test -f scripts/bench_fh_int16_compare.sql

\set ON_ERROR_STOP on
\pset tuples_only on
\timing on

-- Config name from -v config=...
\echo '=== Config: ' :config ' ==='

-- 50 held-out queries
CREATE TEMP TABLE _q AS SELECT id, embedding FROM gutenberg_local ORDER BY id DESC LIMIT 50;

-- Warmup (triggers mmap cache)
SELECT count(*) FROM flashhadamard_store_scan('/tmp/fh_gutenberg.store',
  (SELECT embedding FROM _q LIMIT 1), 10, 12, 42, 4, 100);

-- Full SQ8-reranked results (shortlist_m=12 → top k=10)
DROP TABLE IF EXISTS fh_quality_ :config;
CREATE TABLE fh_quality_ :config AS
  SELECT q.id AS qid, r.row_id, r.score
  FROM _q q,
  LATERAL flashhadamard_store_scan('/tmp/fh_gutenberg.store', q.embedding, 10, 12, 42, 4, 100) r;

SELECT :config || ' rows: ' || count(*) FROM fh_quality_ :config;

-- Latency: 10 queries × 3 rounds
\echo '--- Latency ---'
SELECT sum(c) FROM (SELECT (SELECT count(*) FROM flashhadamard_store_scan('/tmp/fh_gutenberg.store', q.embedding, 10, 12, 42, 4, 100)) c FROM (SELECT embedding FROM _q LIMIT 10) q) t;
SELECT sum(c) FROM (SELECT (SELECT count(*) FROM flashhadamard_store_scan('/tmp/fh_gutenberg.store', q.embedding, 10, 12, 42, 4, 100)) c FROM (SELECT embedding FROM _q LIMIT 10) q) t;
SELECT sum(c) FROM (SELECT (SELECT count(*) FROM flashhadamard_store_scan('/tmp/fh_gutenberg.store', q.embedding, 10, 12, 42, 4, 100)) c FROM (SELECT embedding FROM _q LIMIT 10) q) t;

\echo '=== Done ==='
