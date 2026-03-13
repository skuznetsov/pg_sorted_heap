-- S2 characterization: sorted_prefix_pages behavior under write patterns
-- Axes: fillfactor (100/50/10) x workload (append/recycle/update)
-- Run: PGHOST=localhost PGPORT=5432 psql -d contrib_regression -f scripts/bench_s2_prefix.sql
--
-- Note: can't use DO block because VACUUM can't execute inside PL/pgSQL.
-- Each fillfactor x workload is an explicit SQL sequence.
--
-- Results (2026-03-13, PG18, 2000 rows x 240B tuples):
--
--  ff  | workload | start_pfx | min_pfx | merge_pfx | pages | survive%
-- -----+----------+-----------+---------+-----------+-------+---------
--  100 | append   |        59 |      59 |        65 |    59 |   100.0
--  100 | recycle  |        59 |       0 |        65 |    59 |     0.0
--  100 | update   |        51 |      50 |        51 |    51 |    98.0
--   50 | append   |       118 |     118 |       130 |   118 |   100.0
--   50 | recycle  |       118 |       0 |       130 |   118 |     0.0
--   50 | update   |       101 |       0 |       101 |   101 |     0.0
--   10 | append   |       667 |     667 |       734 |   667 |   100.0
--   10 | recycle  |       667 |       0 |         3 |   667 |     0.0
--   10 | update   |       570 |       0 |       570 |   570 |     0.0
--
--  Note: ff=10 recycle merge_pfx=3 — after DELETE+VACUUM+merge the heavily
--  sparse table has many empty pages; only the first few entries are monotonic.
--
-- Contract:
--   - append-only: prefix survives 100% at all fillfactors
--   - ff=100 + updates: ~98% survival (non-HOT, new tuples go to tail)
--   - ff<100 + updates: prefix collapses (HOT keeps tuple on prefix page)
--   - recycle inserts: always collapse (low PK on recycled early pages)
--   - merge always restores prefix correctly
--   - S2 is designed for append-mostly / post-compact tables
--   - S2+ (PK-comparing tuple_update) deferred until product need arises

\timing on
\pset footer off

CREATE EXTENSION IF NOT EXISTS pg_sorted_heap;

-- Results accumulator
DROP TABLE IF EXISTS _s2_results;
CREATE TABLE _s2_results (
    ff        int,
    workload  text,
    phase     text,
    prefix    int,
    total_pg  int,
    note      text
);

-- Helper: extract prefix from zonemap stats
CREATE OR REPLACE FUNCTION _s2_prefix(tbl regclass) RETURNS int AS $$
    SELECT coalesce(
        (regexp_match(sorted_heap_zonemap_stats(tbl), 'sorted_prefix=(\d+)'))[1]::int,
        -1)
$$ LANGUAGE sql;

-- Helper: total data pages = relpages - 1 (meta) - overflow_pages
-- Note: relpages is refreshed by compact/merge/VACUUM but may lag after plain INSERT.
CREATE OR REPLACE FUNCTION _s2_pages(tbl regclass) RETURNS int AS $$
    DECLARE n int; ovfl int;
    BEGIN
        EXECUTE format('ANALYZE %s', tbl);
        SELECT relpages INTO n FROM pg_class WHERE oid = tbl;
        ovfl := coalesce(
            (regexp_match(sorted_heap_zonemap_stats(tbl), 'overflow_pages=(\d+)'))[1]::int, 0);
        RETURN greatest(n - 1 - ovfl, 0);
    END
$$ LANGUAGE plpgsql;

-- Helper: record a measurement
CREATE OR REPLACE FUNCTION _s2_record(p_ff int, p_wl text, p_phase text, p_tbl regclass, p_note text DEFAULT NULL)
RETURNS void AS $$
    INSERT INTO _s2_results VALUES (p_ff, p_wl, p_phase, _s2_prefix(p_tbl), _s2_pages(p_tbl), p_note)
$$ LANGUAGE sql;

-- ================================================================
-- FF=100
-- ================================================================
\echo '--- ff=100 setup ---'
DROP TABLE IF EXISTS s2_ff100 CASCADE;
CREATE TABLE s2_ff100 (id int PRIMARY KEY, pad text) USING sorted_heap WITH (fillfactor=100);
INSERT INTO s2_ff100 SELECT g, repeat('x', 200) FROM generate_series(1, 2000) g;
SELECT sorted_heap_compact('s2_ff100'::regclass);
SELECT _s2_record(100, 'setup', 'post_compact', 's2_ff100', 'ff=100 rows=2000');

-- Append workload
\echo '--- ff=100 append ---'
INSERT INTO s2_ff100 SELECT g, repeat('a', 200) FROM generate_series(2001, 2200) g;
SELECT _s2_record(100, 'append', 'after_200_inserts', 's2_ff100');
SELECT sorted_heap_merge('s2_ff100'::regclass);
SELECT _s2_record(100, 'append', 'post_merge', 's2_ff100');
SELECT sorted_heap_compact('s2_ff100'::regclass);

-- Recycle workload
\echo '--- ff=100 recycle ---'
DELETE FROM s2_ff100 WHERE id <= 500;
VACUUM s2_ff100;
SELECT _s2_record(100, 'recycle', 'after_delete_vacuum', 's2_ff100', 'deleted ids 1..500');
INSERT INTO s2_ff100 VALUES (-100, repeat('r', 200)), (-200, repeat('r', 200)),
    (-300, repeat('r', 200)), (-400, repeat('r', 200)), (-500, repeat('r', 200)),
    (-600, repeat('r', 200)), (-700, repeat('r', 200)), (-800, repeat('r', 200)),
    (-900, repeat('r', 200)), (-1000, repeat('r', 200));
SELECT _s2_record(100, 'recycle', 'after_10_low_inserts', 's2_ff100');
SELECT sorted_heap_merge('s2_ff100'::regclass);
SELECT _s2_record(100, 'recycle', 'post_merge', 's2_ff100');
SELECT sorted_heap_compact('s2_ff100'::regclass);

-- Update workload
\echo '--- ff=100 update ---'
SELECT _s2_record(100, 'update', 'pre_updates', 's2_ff100', 'ff=100 HOT=unlikely (pages full)');
UPDATE s2_ff100 SET pad = repeat('u', 200) WHERE id IN (
    SELECT id FROM s2_ff100 ORDER BY id LIMIT 20);
SELECT _s2_record(100, 'update', 'after_20_updates', 's2_ff100');
SELECT sorted_heap_merge('s2_ff100'::regclass);
SELECT _s2_record(100, 'update', 'post_merge', 's2_ff100');
DROP TABLE s2_ff100 CASCADE;

-- ================================================================
-- FF=50
-- ================================================================
\echo '--- ff=50 setup ---'
DROP TABLE IF EXISTS s2_ff50 CASCADE;
CREATE TABLE s2_ff50 (id int PRIMARY KEY, pad text) USING sorted_heap WITH (fillfactor=50);
INSERT INTO s2_ff50 SELECT g, repeat('x', 200) FROM generate_series(1, 2000) g;
SELECT sorted_heap_compact('s2_ff50'::regclass);
SELECT _s2_record(50, 'setup', 'post_compact', 's2_ff50', 'ff=50 rows=2000');

-- Append workload
\echo '--- ff=50 append ---'
INSERT INTO s2_ff50 SELECT g, repeat('a', 200) FROM generate_series(2001, 2200) g;
SELECT _s2_record(50, 'append', 'after_200_inserts', 's2_ff50');
SELECT sorted_heap_merge('s2_ff50'::regclass);
SELECT _s2_record(50, 'append', 'post_merge', 's2_ff50');
SELECT sorted_heap_compact('s2_ff50'::regclass);

-- Recycle workload
\echo '--- ff=50 recycle ---'
DELETE FROM s2_ff50 WHERE id <= 500;
VACUUM s2_ff50;
SELECT _s2_record(50, 'recycle', 'after_delete_vacuum', 's2_ff50', 'deleted ids 1..500');
INSERT INTO s2_ff50 VALUES (-100, repeat('r', 200)), (-200, repeat('r', 200)),
    (-300, repeat('r', 200)), (-400, repeat('r', 200)), (-500, repeat('r', 200)),
    (-600, repeat('r', 200)), (-700, repeat('r', 200)), (-800, repeat('r', 200)),
    (-900, repeat('r', 200)), (-1000, repeat('r', 200));
SELECT _s2_record(50, 'recycle', 'after_10_low_inserts', 's2_ff50');
SELECT sorted_heap_merge('s2_ff50'::regclass);
SELECT _s2_record(50, 'recycle', 'post_merge', 's2_ff50');
SELECT sorted_heap_compact('s2_ff50'::regclass);

-- Update workload
\echo '--- ff=50 update ---'
SELECT _s2_record(50, 'update', 'pre_updates', 's2_ff50', 'ff=50 HOT=likely (50% free space)');
UPDATE s2_ff50 SET pad = repeat('u', 200) WHERE id IN (
    SELECT id FROM s2_ff50 ORDER BY id LIMIT 20);
SELECT _s2_record(50, 'update', 'after_20_updates', 's2_ff50');
SELECT sorted_heap_merge('s2_ff50'::regclass);
SELECT _s2_record(50, 'update', 'post_merge', 's2_ff50');
DROP TABLE s2_ff50 CASCADE;

-- ================================================================
-- FF=10
-- ================================================================
\echo '--- ff=10 setup ---'
DROP TABLE IF EXISTS s2_ff10 CASCADE;
CREATE TABLE s2_ff10 (id int PRIMARY KEY, pad text) USING sorted_heap WITH (fillfactor=10);
INSERT INTO s2_ff10 SELECT g, repeat('x', 200) FROM generate_series(1, 2000) g;
SELECT sorted_heap_compact('s2_ff10'::regclass);
SELECT _s2_record(10, 'setup', 'post_compact', 's2_ff10', 'ff=10 rows=2000');

-- Append workload
\echo '--- ff=10 append ---'
INSERT INTO s2_ff10 SELECT g, repeat('a', 200) FROM generate_series(2001, 2200) g;
SELECT _s2_record(10, 'append', 'after_200_inserts', 's2_ff10');
SELECT sorted_heap_merge('s2_ff10'::regclass);
SELECT _s2_record(10, 'append', 'post_merge', 's2_ff10');
SELECT sorted_heap_compact('s2_ff10'::regclass);

-- Recycle workload
\echo '--- ff=10 recycle ---'
DELETE FROM s2_ff10 WHERE id <= 500;
VACUUM s2_ff10;
SELECT _s2_record(10, 'recycle', 'after_delete_vacuum', 's2_ff10', 'deleted ids 1..500');
INSERT INTO s2_ff10 VALUES (-100, repeat('r', 200)), (-200, repeat('r', 200)),
    (-300, repeat('r', 200)), (-400, repeat('r', 200)), (-500, repeat('r', 200)),
    (-600, repeat('r', 200)), (-700, repeat('r', 200)), (-800, repeat('r', 200)),
    (-900, repeat('r', 200)), (-1000, repeat('r', 200));
SELECT _s2_record(10, 'recycle', 'after_10_low_inserts', 's2_ff10');
SELECT sorted_heap_merge('s2_ff10'::regclass);
SELECT _s2_record(10, 'recycle', 'post_merge', 's2_ff10');
SELECT sorted_heap_compact('s2_ff10'::regclass);

-- Update workload
\echo '--- ff=10 update ---'
SELECT _s2_record(10, 'update', 'pre_updates', 's2_ff10', 'ff=10 HOT=likely (90% free space)');
UPDATE s2_ff10 SET pad = repeat('u', 200) WHERE id IN (
    SELECT id FROM s2_ff10 ORDER BY id LIMIT 20);
SELECT _s2_record(10, 'update', 'after_20_updates', 's2_ff10');
SELECT sorted_heap_merge('s2_ff10'::regclass);
SELECT _s2_record(10, 'update', 'post_merge', 's2_ff10');
DROP TABLE s2_ff10 CASCADE;

-- ================================================================
-- RESULTS
-- ================================================================

\pset format wrapped
\pset columns 120

\echo ''
\echo '=== S2 CHARACTERIZATION: sorted_prefix_pages ==='
\echo ''

SELECT
    ff,
    workload,
    phase,
    prefix,
    total_pg AS pages,
    CASE
        WHEN total_pg > 0 THEN round(100.0 * prefix / total_pg, 1)
        ELSE 0
    END AS prefix_pct,
    coalesce(note, '') AS note
FROM _s2_results
ORDER BY ff DESC, workload,
    CASE phase
        WHEN 'post_compact' THEN 1
        WHEN 'pre_updates' THEN 1
        WHEN 'after_delete_vacuum' THEN 2
        WHEN 'after_200_inserts' THEN 2
        WHEN 'after_10_low_inserts' THEN 3
        WHEN 'after_20_updates' THEN 3
        WHEN 'post_merge' THEN 4
    END;

\echo ''
\echo '=== SUMMARY: prefix survival by workload ==='
\echo ''

SELECT
    r.ff,
    r.workload,
    coalesce(
        max(CASE WHEN r.phase IN ('post_compact','pre_updates') THEN r.prefix END),
        s.prefix
    ) AS start_pfx,
    min(CASE WHEN r.phase NOT IN ('post_compact','pre_updates','post_merge') THEN r.prefix END) AS min_pfx,
    max(CASE WHEN r.phase = 'post_merge' THEN r.prefix END) AS merge_pfx,
    coalesce(
        max(CASE WHEN r.phase IN ('post_compact','pre_updates') THEN r.total_pg END),
        s.total_pg
    ) AS pages,
    CASE
        WHEN coalesce(
            max(CASE WHEN r.phase IN ('post_compact','pre_updates') THEN r.prefix END),
            s.prefix) > 0
        THEN round(100.0 *
            min(CASE WHEN r.phase NOT IN ('post_compact','pre_updates','post_merge') THEN r.prefix END)::numeric /
            coalesce(
                max(CASE WHEN r.phase IN ('post_compact','pre_updates') THEN r.prefix END),
                s.prefix), 1)
        ELSE 0
    END AS survive_pct
FROM _s2_results r
JOIN _s2_results s ON s.ff = r.ff AND s.workload = 'setup'
WHERE r.workload != 'setup'
GROUP BY r.ff, r.workload, s.prefix, s.total_pg
ORDER BY r.ff DESC, r.workload;

-- Cleanup
DROP TABLE _s2_results;
DROP FUNCTION _s2_prefix(regclass);
DROP FUNCTION _s2_pages(regclass);
DROP FUNCTION _s2_record(int, text, text, regclass, text);
