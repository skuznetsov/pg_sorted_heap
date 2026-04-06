-- FlashHadamard int16 kernel quality comparison
--
-- Run after bench_fh_int16_quality.sql has been executed for all 4 configs.
-- Compares final SQ8-reranked quality: hit@1, top-10 overlap, max score diff.
--
-- Usage:
--   psql -d fh_test -f scripts/bench_fh_int16_compare.sql

\pset tuples_only off
\pset format aligned

\echo '=========================================='
\echo '  FlashHadamard Int16 Quality Comparison  '
\echo '  (SQ8-reranked final results, 50 queries)'
\echo '=========================================='

-- Verify tables exist
SELECT 'Tables found:' AS info,
  (SELECT count(*) FROM fh_quality_1t_float) AS "1t_float",
  (SELECT count(*) FROM fh_quality_1t_int16) AS "1t_int16",
  (SELECT count(*) FROM fh_quality_8t_float) AS "8t_float",
  (SELECT count(*) FROM fh_quality_8t_int16) AS "8t_int16";

\echo ''
\echo '--- hit@1 (vs 8t_float baseline) ---'

SELECT '1t_float' AS config,
  count(*) || '/50' AS hit1
FROM (SELECT DISTINCT ON (qid) qid, row_id FROM fh_quality_8t_float ORDER BY qid, score DESC) b
JOIN (SELECT DISTINCT ON (qid) qid, row_id FROM fh_quality_1t_float ORDER BY qid, score DESC) t
  ON b.qid = t.qid AND b.row_id = t.row_id

UNION ALL
SELECT '1t_int16',
  count(*) || '/50'
FROM (SELECT DISTINCT ON (qid) qid, row_id FROM fh_quality_8t_float ORDER BY qid, score DESC) b
JOIN (SELECT DISTINCT ON (qid) qid, row_id FROM fh_quality_1t_int16 ORDER BY qid, score DESC) t
  ON b.qid = t.qid AND b.row_id = t.row_id

UNION ALL
SELECT '8t_int16',
  count(*) || '/50'
FROM (SELECT DISTINCT ON (qid) qid, row_id FROM fh_quality_8t_float ORDER BY qid, score DESC) b
JOIN (SELECT DISTINCT ON (qid) qid, row_id FROM fh_quality_8t_int16 ORDER BY qid, score DESC) t
  ON b.qid = t.qid AND b.row_id = t.row_id;

\echo ''
\echo '--- top-10 overlap (vs 8t_float baseline) ---'

SELECT '1t_float' AS config,
  round(100.0 * count(*) / 500, 1) || '%' AS overlap
FROM fh_quality_1t_float a JOIN fh_quality_8t_float b ON a.qid = b.qid AND a.row_id = b.row_id

UNION ALL
SELECT '1t_int16',
  round(100.0 * count(*) / 500, 1) || '%'
FROM fh_quality_1t_int16 a JOIN fh_quality_8t_float b ON a.qid = b.qid AND a.row_id = b.row_id

UNION ALL
SELECT '8t_int16',
  round(100.0 * count(*) / 500, 1) || '%'
FROM fh_quality_8t_int16 a JOIN fh_quality_8t_float b ON a.qid = b.qid AND a.row_id = b.row_id;

\echo ''
\echo '--- max reranked score diff (vs 8t_float, overlapping results only) ---'

SELECT '1t_int16' AS config,
  round(max(abs(a.score - b.score))::numeric, 8) AS max_diff
FROM fh_quality_1t_int16 a JOIN fh_quality_8t_float b ON a.qid = b.qid AND a.row_id = b.row_id

UNION ALL
SELECT '8t_int16',
  round(max(abs(a.score - b.score))::numeric, 8)
FROM fh_quality_8t_int16 a JOIN fh_quality_8t_float b ON a.qid = b.qid AND a.row_id = b.row_id;

\echo ''
\echo '=========================================='
