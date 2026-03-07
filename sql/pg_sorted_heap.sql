CREATE EXTENSION pg_sorted_heap;
SELECT public.version();
SELECT public.pg_sorted_heap_observability() AS observability_bootstrap;
SELECT (public.pg_sorted_heap_observability() ~ 'pg_sorted_heap=0.9.8') AS observability_probe;

-- ====================================================================
-- Functional regression tests: multi-type index, JOIN UNNEST rescan,
-- delete+vacuum consistency, locator edge cases, directed placement
-- ====================================================================

-- Test int2 and int4 index support (only int8 tested above)
CREATE TABLE pg_sorted_heap_int2_smoke(id smallint) USING clustered_heap;
CREATE INDEX pg_sorted_heap_int2_smoke_idx
	ON pg_sorted_heap_int2_smoke USING clustered_pk_index (id);
INSERT INTO pg_sorted_heap_int2_smoke(id) SELECT generate_series(1,20)::smallint;
SELECT count(*) AS int2_row_count FROM pg_sorted_heap_int2_smoke;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS int2_filter_eq FROM pg_sorted_heap_int2_smoke WHERE id = 10::smallint;
SELECT count(*) AS int2_filter_range FROM pg_sorted_heap_int2_smoke WHERE id BETWEEN 5::smallint AND 15::smallint;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE pg_sorted_heap_int2_smoke;

CREATE TABLE pg_sorted_heap_int4_smoke(id integer) USING clustered_heap;
CREATE INDEX pg_sorted_heap_int4_smoke_idx
	ON pg_sorted_heap_int4_smoke USING clustered_pk_index (id);
INSERT INTO pg_sorted_heap_int4_smoke(id) SELECT generate_series(1,20);
SELECT count(*) AS int4_row_count FROM pg_sorted_heap_int4_smoke;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS int4_filter_eq FROM pg_sorted_heap_int4_smoke WHERE id = 10;
SELECT count(*) AS int4_filter_range FROM pg_sorted_heap_int4_smoke WHERE id BETWEEN 5 AND 15;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE pg_sorted_heap_int4_smoke;

-- Test locator edge cases: zero, large values, boundary
SELECT locator_major(locator_pack(0, 0)) AS loc_zero_major,
       locator_minor(locator_pack(0, 0)) AS loc_zero_minor;
SELECT locator_cmp(locator_pack(0, 0), locator_pack(0, 0)) AS loc_cmp_equal_zero;
SELECT locator_cmp(locator_pack(0, 0), locator_pack(0, 1)) AS loc_cmp_zero_vs_one;
SELECT locator_to_hex(locator_pack(9223372036854775807, 9223372036854775807)) AS loc_max_hex;
SELECT locator_major(locator_pack(9223372036854775807, 0)) AS loc_max_major;
SELECT locator_minor(locator_pack(0, 9223372036854775807)) AS loc_max_minor;
SELECT locator_to_hex(locator_next_minor(locator_pack(0, 0), 1)) AS loc_next_from_zero;
SELECT locator_to_hex(locator_advance_major(locator_pack(0, 5), 1)) AS loc_advance_from_zero;

-- Test JOIN UNNEST correctness
CREATE TABLE pg_sorted_heap_join_unnest_base(id bigint) USING clustered_heap;
CREATE INDEX pg_sorted_heap_join_unnest_base_idx
	ON pg_sorted_heap_join_unnest_base USING clustered_pk_index (id);
INSERT INTO pg_sorted_heap_join_unnest_base(id) SELECT generate_series(1,100);
-- Probe with array of keys via JOIN (exercises rescan on inner side)
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_hashjoin = off;
SET enable_mergejoin = off;
SELECT count(*) AS join_unnest_hit_count
FROM pg_sorted_heap_join_unnest_base b
JOIN (SELECT unnest(ARRAY[5,10,15,20,25,30,50,75,99,100]) AS id) k ON b.id = k.id;
-- Probe with keys not in table (should return 0 matches)
SELECT count(*) AS join_unnest_miss_count
FROM pg_sorted_heap_join_unnest_base b
JOIN (SELECT unnest(ARRAY[101,200,300]) AS id) k ON b.id = k.id;
-- Mixed hit/miss
SELECT count(*) AS join_unnest_mixed_count
FROM pg_sorted_heap_join_unnest_base b
JOIN (SELECT unnest(ARRAY[1,50,100,101,200]) AS id) k ON b.id = k.id;
RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_hashjoin;
RESET enable_mergejoin;
DROP TABLE pg_sorted_heap_join_unnest_base;

-- Test delete + vacuum + re-query consistency
CREATE TABLE pg_sorted_heap_vacuum_consistency(id bigint) USING clustered_heap;
CREATE INDEX pg_sorted_heap_vacuum_consistency_idx
	ON pg_sorted_heap_vacuum_consistency USING clustered_pk_index (id);
INSERT INTO pg_sorted_heap_vacuum_consistency(id) SELECT generate_series(1,50);
-- Delete a range and verify count
DELETE FROM pg_sorted_heap_vacuum_consistency WHERE id BETWEEN 10 AND 30;
SELECT count(*) AS vacuum_pre_vacuum_count FROM pg_sorted_heap_vacuum_consistency;
-- Vacuum and re-verify
VACUUM pg_sorted_heap_vacuum_consistency;
SELECT count(*) AS vacuum_post_vacuum_count FROM pg_sorted_heap_vacuum_consistency;
-- Verify remaining rows are correct
SELECT array_agg(id ORDER BY id) AS vacuum_remaining_ids
FROM pg_sorted_heap_vacuum_consistency
WHERE id <= 15;
-- Re-verify data integrity after vacuum
SELECT count(*) AS vacuum_post_gc_count FROM pg_sorted_heap_vacuum_consistency;
DROP TABLE pg_sorted_heap_vacuum_consistency;

-- Test segment split boundary: insert exactly at capacity edge
CREATE TABLE pg_sorted_heap_split_edge(id bigint) USING clustered_heap;
CREATE INDEX pg_sorted_heap_split_edge_idx
	ON pg_sorted_heap_split_edge USING clustered_pk_index (id);
-- Insert exactly split_threshold rows
INSERT INTO pg_sorted_heap_split_edge(id) SELECT generate_series(1,16);
SELECT count(*) AS split_edge_at_capacity FROM pg_sorted_heap_split_edge;
-- Insert more rows
INSERT INTO pg_sorted_heap_split_edge(id) SELECT generate_series(17,20);
SELECT count(*) AS split_edge_total_rows FROM pg_sorted_heap_split_edge;
DROP TABLE pg_sorted_heap_split_edge;

-- Test empty table operations
CREATE TABLE pg_sorted_heap_empty(id bigint) USING clustered_heap;
CREATE INDEX pg_sorted_heap_empty_idx
	ON pg_sorted_heap_empty USING clustered_pk_index (id);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT count(*) AS empty_count FROM pg_sorted_heap_empty;
SELECT count(*) AS empty_filter_count FROM pg_sorted_heap_empty WHERE id = 1;
RESET enable_seqscan;
RESET enable_bitmapscan;
DROP TABLE pg_sorted_heap_empty;

-- ================================================================
-- Directed placement: verify rows with same key land on same block
-- ================================================================
CREATE TABLE pg_sorted_heap_directed(id int) USING clustered_heap;
CREATE INDEX pg_sorted_heap_directed_idx
    ON pg_sorted_heap_directed USING clustered_pk_index (id);

-- Insert 200 rows: 20 distinct keys, 10 rows each.
-- With directed placement, all 10 rows for the same key should land
-- on the same block (or very few blocks).
INSERT INTO pg_sorted_heap_directed(id)
SELECT key_id
FROM generate_series(1, 20) AS key_id,
     generate_series(1, 10) AS rep;

-- For each key, count distinct blocks.  Perfect clustering = 1 block per key.
-- Allow up to 2 (page could fill up for large tuples).
SELECT
    CASE WHEN every(blk_count <= 2)
         THEN 'directed_placement_ok'
         ELSE 'directed_placement_FAIL'
    END AS directed_placement_result
FROM (
    SELECT id, count(DISTINCT (ctid::text::point)[0]::int) AS blk_count
    FROM pg_sorted_heap_directed
    GROUP BY id
) sub;

-- Verify monotonic block ordering: keys inserted in order should have
-- non-decreasing minimum block numbers.
SELECT
    CASE WHEN bool_and(min_blk >= lag_blk OR lag_blk IS NULL)
         THEN 'block_order_ok'
         ELSE 'block_order_FAIL'
    END AS block_order_result
FROM (
    SELECT id,
           min((ctid::text::point)[0]::int) AS min_blk,
           lag(min((ctid::text::point)[0]::int)) OVER (ORDER BY id) AS lag_blk
    FROM pg_sorted_heap_directed
    GROUP BY id
) sub;

DROP TABLE pg_sorted_heap_directed;

-- ================================================================
-- COPY path directed placement (multi_insert override)
-- ================================================================
CREATE TABLE pg_sorted_heap_copy_dp(id int, payload text) USING clustered_heap;
CREATE INDEX pg_sorted_heap_copy_dp_idx
    ON pg_sorted_heap_copy_dp USING clustered_pk_index (id);

-- Use INSERT ... SELECT which goes through multi_insert for large batches
-- 30 keys x 30 rows each = 900 rows, ~500 byte payload -> multi-block
INSERT INTO pg_sorted_heap_copy_dp(id, payload)
SELECT ((g % 30) + 1), repeat('x', 500)
FROM generate_series(1, 900) g;

-- With multi_insert directed placement, same-key rows should cluster
SELECT
    CASE WHEN avg(blk_count) <= 4.0
         THEN 'copy_directed_ok'
         ELSE 'copy_directed_FAIL'
    END AS copy_directed_result
FROM (
    SELECT id, count(DISTINCT (ctid::text::point)[0]::int) AS blk_count
    FROM pg_sorted_heap_copy_dp
    GROUP BY id
) sub;

DROP TABLE pg_sorted_heap_copy_dp;

-- ================================================================
-- UPDATE + DELETE on directed-placement table
-- ================================================================
CREATE TABLE pg_sorted_heap_upd(id int, val int, payload text) USING clustered_heap;
CREATE INDEX pg_sorted_heap_upd_idx
    ON pg_sorted_heap_upd USING clustered_pk_index (id);

-- Insert 5 keys, 10 rows each
INSERT INTO pg_sorted_heap_upd(id, val, payload)
SELECT (g % 5) + 1, g, repeat('y', 200)
FROM generate_series(1, 50) g;

-- Verify initial count
SELECT count(*) AS before_count FROM pg_sorted_heap_upd;

-- UPDATE: change payload (same key = HOT candidate)
UPDATE pg_sorted_heap_upd SET val = val + 1000 WHERE id = 3;

-- UPDATE: change the clustering key itself
UPDATE pg_sorted_heap_upd SET id = 99 WHERE id = 5;

-- Verify counts by key after updates
SELECT id, count(*) AS cnt FROM pg_sorted_heap_upd
WHERE id IN (3, 5, 99) GROUP BY id ORDER BY id;

-- DELETE a whole key group
DELETE FROM pg_sorted_heap_upd WHERE id = 2;

-- Verify final count
SELECT count(*) AS after_count FROM pg_sorted_heap_upd;

-- Re-INSERT into the table (zone map should still work for new inserts)
INSERT INTO pg_sorted_heap_upd(id, val, payload)
SELECT 2, g, repeat('z', 200)
FROM generate_series(1, 5) g;

SELECT count(*) AS final_count FROM pg_sorted_heap_upd;

DROP TABLE pg_sorted_heap_upd;

-- ================================================================
-- NULL clustering key handling
-- ================================================================
CREATE TABLE pg_sorted_heap_null(id int, payload text) USING clustered_heap;
CREATE INDEX pg_sorted_heap_null_idx
    ON pg_sorted_heap_null USING clustered_pk_index (id);

-- Insert rows with NULL key: directed placement skips NULL keys (safe fallback),
-- but the index AM rejects NULLs with an error.  These INSERTs are expected to fail.
INSERT INTO pg_sorted_heap_null(id, payload) VALUES (NULL, 'null_row_1');
INSERT INTO pg_sorted_heap_null(id, payload) VALUES (NULL, 'null_row_2');
INSERT INTO pg_sorted_heap_null(id, payload) VALUES (1, 'normal_row');

SELECT count(*) AS null_test_count FROM pg_sorted_heap_null;

-- Verify we can read back all rows including NULLs
SELECT id IS NULL AS is_null, count(*) AS cnt
FROM pg_sorted_heap_null GROUP BY (id IS NULL) ORDER BY is_null;

DROP TABLE pg_sorted_heap_null;

-- ================================================================
-- Directed placement with many distinct keys (fast path exercise)
-- ================================================================
CREATE TABLE pg_sorted_heap_many(id int, payload text) USING clustered_heap;
CREATE INDEX pg_sorted_heap_many_idx
    ON pg_sorted_heap_many USING clustered_pk_index (id);

-- 200 distinct keys x 5 rows each = 1000 rows, triggers fast path (>64 keys)
INSERT INTO pg_sorted_heap_many(id, payload)
SELECT ((g % 200) + 1), repeat('m', 100)
FROM generate_series(1, 1000) g;

SELECT
    CASE WHEN count(*) = 1000
         THEN 'many_keys_count_ok'
         ELSE 'many_keys_count_FAIL'
    END AS many_keys_result
FROM pg_sorted_heap_many;

-- Even with fast path, zone map should provide some clustering benefit
-- (not as tight as group path, but better than random heap)
SELECT
    CASE WHEN avg(blk_count) <= 10.0
         THEN 'many_keys_scatter_ok'
         ELSE 'many_keys_scatter_FAIL'
    END AS many_keys_scatter_result
FROM (
    SELECT id, count(DISTINCT (ctid::text::point)[0]::int) AS blk_count
    FROM pg_sorted_heap_many
    GROUP BY id
) sub;

DROP TABLE pg_sorted_heap_many;

-- ================================================================
-- VACUUM on directed-placement table (delete + vacuum + re-insert)
-- ================================================================
CREATE TABLE pg_sorted_heap_vac_dp(id int, payload text) USING clustered_heap;
CREATE INDEX pg_sorted_heap_vac_dp_idx
    ON pg_sorted_heap_vac_dp USING clustered_pk_index (id);

-- 10 keys x 50 rows = 500 rows
INSERT INTO pg_sorted_heap_vac_dp(id, payload)
SELECT ((g % 10) + 1), repeat('v', 200)
FROM generate_series(1, 500) g;

SELECT count(*) AS vac_before FROM pg_sorted_heap_vac_dp;

-- Delete 60% of rows
DELETE FROM pg_sorted_heap_vac_dp WHERE id <= 6;

VACUUM pg_sorted_heap_vac_dp;

-- Verify remaining rows survived vacuum
SELECT count(*) AS vac_after FROM pg_sorted_heap_vac_dp;

-- Re-insert: directed placement should still cluster new rows
INSERT INTO pg_sorted_heap_vac_dp(id, payload)
SELECT ((g % 6) + 1), repeat('w', 200)
FROM generate_series(1, 300) g;

-- Verify data integrity and count
SELECT
    CASE WHEN count(*) = 500
         THEN 'vac_reinsert_ok'
         ELSE 'vac_reinsert_FAIL'
    END AS vac_reinsert_result
FROM pg_sorted_heap_vac_dp;

DROP TABLE pg_sorted_heap_vac_dp;

-- ================================================================
-- TRUNCATE invalidates zone map (re-insert should not crash)
-- ================================================================
CREATE TABLE pg_sorted_heap_trunc(id int, payload text) USING clustered_heap;
CREATE INDEX pg_sorted_heap_trunc_idx
    ON pg_sorted_heap_trunc USING clustered_pk_index (id);

INSERT INTO pg_sorted_heap_trunc(id, payload)
SELECT g, repeat('t', 100)
FROM generate_series(1, 100) g;

SELECT count(*) AS before_trunc FROM pg_sorted_heap_trunc;

TRUNCATE pg_sorted_heap_trunc;

SELECT count(*) AS after_trunc FROM pg_sorted_heap_trunc;

-- Re-insert after truncate: zone map was invalidated, should rebuild cleanly
INSERT INTO pg_sorted_heap_trunc(id, payload)
SELECT g, repeat('u', 100)
FROM generate_series(1, 50) g;

SELECT
    CASE WHEN count(*) = 50
         THEN 'trunc_reinsert_ok'
         ELSE 'trunc_reinsert_FAIL'
    END AS trunc_reinsert_result
FROM pg_sorted_heap_trunc;

DROP TABLE pg_sorted_heap_trunc;

-- ================================================================
-- JOIN with btree on directed-placement table (production pattern)
-- ================================================================
-- This is the key scenario: standard btree serves JOINs efficiently
-- because directed placement physically clusters rows by key.
CREATE TABLE pg_sorted_heap_join(id int, payload text) USING clustered_heap;
CREATE INDEX pg_sorted_heap_join_pkidx
    ON pg_sorted_heap_join USING clustered_pk_index (id);
CREATE INDEX pg_sorted_heap_join_btree
    ON pg_sorted_heap_join USING btree (id);

-- 1000 keys x 100 rows = 100K rows, interleaved insert
INSERT INTO pg_sorted_heap_join(id, payload)
SELECT ((g % 1000) + 1), repeat('j', 100)
FROM generate_series(1, 100000) g;

ANALYZE pg_sorted_heap_join;

-- Verify the btree index is used (not clustered_pk_index)
-- Nested loop + index scan should touch very few blocks per key
SELECT
    CASE WHEN count(*) = 20000
         THEN 'join_btree_count_ok'
         ELSE 'join_btree_count_FAIL'
    END AS join_btree_result
FROM pg_sorted_heap_join d
JOIN (SELECT unnest(ARRAY(SELECT generate_series(1, 200))) AS id) keys
ON d.id = keys.id;

-- Verify clustering held through the full insert:
-- each key's rows should be on <=5 blocks
SELECT
    CASE WHEN avg(blk_count) <= 5.0
         THEN 'join_btree_scatter_ok'
         ELSE 'join_btree_scatter_FAIL'
    END AS join_btree_scatter_result
FROM (
    SELECT id, count(DISTINCT (ctid::text::point)[0]::int) AS blk_count
    FROM pg_sorted_heap_join
    GROUP BY id
) sub;

DROP TABLE pg_sorted_heap_join;

-- ================================================================
-- sorted_heap Table AM: Phase 1 tests
-- ================================================================

-- Test SH-1: Create table with sorted_heap AM
CREATE TABLE sh_basic(id bigint, val text) USING sorted_heap;

-- Test SH-2: Single INSERT + SELECT
INSERT INTO sh_basic(id, val) VALUES (1, 'hello');
SELECT count(*) AS sh_single_count FROM sh_basic;

-- Test SH-3: Bulk INSERT
INSERT INTO sh_basic(id, val)
SELECT g, 'row_' || g FROM generate_series(2, 100) g;
SELECT count(*) AS sh_multi_count FROM sh_basic;

-- Test SH-4: Data roundtrip (correct values returned)
SELECT id, val FROM sh_basic WHERE id = 50;

-- Test SH-5: DELETE
DELETE FROM sh_basic WHERE id BETWEEN 20 AND 30;
SELECT count(*) AS sh_after_delete FROM sh_basic;

-- Test SH-6: UPDATE
UPDATE sh_basic SET val = 'updated' WHERE id = 1;
SELECT val AS sh_updated_val FROM sh_basic WHERE id = 1;

-- Test SH-7: VACUUM
VACUUM sh_basic;
SELECT count(*) AS sh_after_vacuum FROM sh_basic;

-- Test SH-8: Index creation and index scan
CREATE INDEX sh_basic_idx ON sh_basic USING btree (id);
SET enable_seqscan = off;
SELECT count(*) AS sh_idx_count FROM sh_basic WHERE id = 50;
RESET enable_seqscan;

-- Test SH-9: TRUNCATE + re-insert
TRUNCATE sh_basic;
SELECT count(*) AS sh_after_trunc FROM sh_basic;
INSERT INTO sh_basic(id, val) VALUES (1, 'after_truncate');
SELECT count(*) AS sh_reinsert FROM sh_basic;

DROP TABLE sh_basic;

-- Test SH-10: Empty table
CREATE TABLE sh_empty(id bigint) USING sorted_heap;
SELECT count(*) AS sh_empty FROM sh_empty;
DROP TABLE sh_empty;

-- Test SH-11: Bulk multi-insert path (large batch)
CREATE TABLE sh_bulk(id int, payload text) USING sorted_heap;
INSERT INTO sh_bulk(id, payload)
SELECT g, repeat('x', 200) FROM generate_series(1, 1000) g;
SELECT count(*) AS sh_bulk_count FROM sh_bulk;
SELECT count(*) AS sh_bulk_range
FROM sh_bulk WHERE id BETWEEN 500 AND 510;
DROP TABLE sh_bulk;

-- Test SH-12: ANALYZE
CREATE TABLE sh_analyze(id bigint, val text) USING sorted_heap;
INSERT INTO sh_analyze(id, val)
SELECT g, repeat('a', 100) FROM generate_series(1, 500) g;
ANALYZE sh_analyze;
SELECT count(*) AS sh_post_analyze FROM sh_analyze;
DROP TABLE sh_analyze;

-- Test SH-13: NULL values
CREATE TABLE sh_null(id bigint, val text) USING sorted_heap;
INSERT INTO sh_null(id, val) VALUES (NULL, 'null_id');
INSERT INTO sh_null(id, val) VALUES (1, NULL);
INSERT INTO sh_null(id, val) VALUES (NULL, NULL);
SELECT count(*) AS sh_null_count FROM sh_null;
DROP TABLE sh_null;

-- Test SH-14: Co-existence with clustered_heap
CREATE TABLE ch_coexist(id int, payload text) USING clustered_heap;
CREATE INDEX ch_coexist_pkidx ON ch_coexist USING clustered_pk_index (id);
CREATE INDEX ch_coexist_idx ON ch_coexist USING btree (id);
CREATE TABLE sh_coexist(id int, payload text) USING sorted_heap;
INSERT INTO ch_coexist(id, payload) SELECT g, 'ch_' || g FROM generate_series(1, 10) g;
INSERT INTO sh_coexist(id, payload) SELECT g, 'sh_' || g FROM generate_series(1, 10) g;
SELECT
    (SELECT count(*) FROM ch_coexist) AS ch_count,
    (SELECT count(*) FROM sh_coexist) AS sh_count;
DROP TABLE ch_coexist;
DROP TABLE sh_coexist;

-- Test SH-15: COPY path (exercises multi_insert)
CREATE TABLE sh_copy(id int, val text) USING sorted_heap;
COPY sh_copy FROM stdin;
1	alpha
2	beta
3	gamma
4	delta
5	epsilon
\.
SELECT count(*) AS sh_copy_count FROM sh_copy;
DROP TABLE sh_copy;

-- ================================================================
-- sorted_heap Table AM: Phase 2 tests (PK-sorted COPY)
-- ================================================================

-- Test SH2-1: COPY with int PK — verify physical sort order
CREATE TABLE sh2_pk_int(id int PRIMARY KEY, val text) USING sorted_heap;
-- Generate data in reverse order, COPY into sorted_heap
CREATE TEMP TABLE sh2_src1 AS
    SELECT id, 'v' || id AS val FROM generate_series(1, 500) id ORDER BY id DESC;
COPY sh2_src1 TO '/tmp/sh2_pk_int.csv' CSV;
COPY sh2_pk_int FROM '/tmp/sh2_pk_int.csv' CSV;
-- Verify zero inversions in physical order vs PK order
SELECT
    CASE WHEN count(*) = 0
         THEN 'pk_int_sorted_ok'
         ELSE 'pk_int_sorted_FAIL'
    END AS sh2_pk_int_result
FROM (
    SELECT id < lag(id) OVER (ORDER BY ctid) AS inv
    FROM sh2_pk_int
) sub
WHERE inv;
SELECT count(*) AS sh2_pk_int_count FROM sh2_pk_int;
DROP TABLE sh2_pk_int;
DROP TABLE sh2_src1;

-- Test SH2-2: COPY with composite PK (text, int)
CREATE TABLE sh2_composite(cat text, id int, val text, PRIMARY KEY(cat, id)) USING sorted_heap;
CREATE TEMP TABLE sh2_src2 AS
    SELECT chr(65 + (g % 5)) AS cat, g AS id, 'v' || g AS val
    FROM generate_series(1, 200) g
    ORDER BY random();
COPY sh2_src2 TO '/tmp/sh2_composite.csv' CSV;
COPY sh2_composite FROM '/tmp/sh2_composite.csv' CSV;
-- Verify sort: cat ASC, then id ASC within cat
SELECT
    CASE WHEN count(*) = 0
         THEN 'composite_sorted_ok'
         ELSE 'composite_sorted_FAIL'
    END AS sh2_composite_result
FROM (
    SELECT (cat < lag(cat) OVER (ORDER BY ctid))
        OR (cat = lag(cat) OVER (ORDER BY ctid)
            AND id < lag(id) OVER (ORDER BY ctid)) AS inv
    FROM sh2_composite
) sub
WHERE inv;
SELECT count(*) AS sh2_composite_count FROM sh2_composite;
DROP TABLE sh2_composite;
DROP TABLE sh2_src2;

-- Test SH2-3: COPY without PK (no crash, works as heap)
CREATE TABLE sh2_nopk(id int, val text) USING sorted_heap;
CREATE TEMP TABLE sh2_src3 AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 100) g ORDER BY random();
COPY sh2_src3 TO '/tmp/sh2_nopk.csv' CSV;
COPY sh2_nopk FROM '/tmp/sh2_nopk.csv' CSV;
SELECT count(*) AS sh2_nopk_count FROM sh2_nopk;
DROP TABLE sh2_nopk;
DROP TABLE sh2_src3;

-- Test SH2-4: PK created after table — relcache callback triggers re-detection
CREATE TABLE sh2_latepk(id int, val text) USING sorted_heap;
-- COPY without PK (unsorted)
CREATE TEMP TABLE sh2_src4 AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 50) g ORDER BY random();
COPY sh2_src4 TO '/tmp/sh2_latepk.csv' CSV;
COPY sh2_latepk FROM '/tmp/sh2_latepk.csv' CSV;
-- Now add PK
ALTER TABLE sh2_latepk ADD PRIMARY KEY (id);
-- COPY more data — this batch should be sorted
TRUNCATE sh2_src4;
INSERT INTO sh2_src4 SELECT g, 'w' || g FROM generate_series(51, 100) g ORDER BY random();
COPY sh2_src4 TO '/tmp/sh2_latepk2.csv' CSV;
COPY sh2_latepk FROM '/tmp/sh2_latepk2.csv' CSV;
-- Verify second batch is sorted (filter by id > 50 to check only new rows)
SELECT
    CASE WHEN count(*) = 0
         THEN 'latepk_sorted_ok'
         ELSE 'latepk_sorted_FAIL'
    END AS sh2_latepk_result
FROM (
    SELECT id < lag(id) OVER (ORDER BY ctid) AS inv
    FROM sh2_latepk
    WHERE id > 50
) sub
WHERE inv;
SELECT count(*) AS sh2_latepk_count FROM sh2_latepk;
DROP TABLE sh2_latepk;
DROP TABLE sh2_src4;

-- Test SH2-5: COPY with text PK (collation-aware sort)
CREATE TABLE sh2_textpk(name text PRIMARY KEY, val int) USING sorted_heap;
CREATE TEMP TABLE sh2_src5 AS
    SELECT 'item_' || lpad(g::text, 4, '0') AS name, g AS val
    FROM generate_series(1, 200) g
    ORDER BY random();
COPY sh2_src5 TO '/tmp/sh2_textpk.csv' CSV;
COPY sh2_textpk FROM '/tmp/sh2_textpk.csv' CSV;
SELECT
    CASE WHEN count(*) = 0
         THEN 'textpk_sorted_ok'
         ELSE 'textpk_sorted_FAIL'
    END AS sh2_textpk_result
FROM (
    SELECT name < lag(name) OVER (ORDER BY ctid) AS inv
    FROM sh2_textpk
) sub
WHERE inv;
SELECT count(*) AS sh2_textpk_count FROM sh2_textpk;
DROP TABLE sh2_textpk;
DROP TABLE sh2_src5;

-- Test SH2-6: COPY with NULLs in non-PK columns (no crash)
CREATE TABLE sh2_nulls(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh2_src6(id int, val text);
INSERT INTO sh2_src6 VALUES (5, NULL), (3, 'three'), (1, NULL), (4, 'four'), (2, NULL);
COPY sh2_src6 TO '/tmp/sh2_nulls.csv' CSV;
COPY sh2_nulls FROM '/tmp/sh2_nulls.csv' CSV;
-- Still sorted by PK
SELECT
    CASE WHEN count(*) = 0
         THEN 'nulls_sorted_ok'
         ELSE 'nulls_sorted_FAIL'
    END AS sh2_nulls_result
FROM (
    SELECT id < lag(id) OVER (ORDER BY ctid) AS inv
    FROM sh2_nulls
) sub
WHERE inv;
SELECT count(*) AS sh2_nulls_count FROM sh2_nulls;
DROP TABLE sh2_nulls;
DROP TABLE sh2_src6;

-- Test SH2-7: INSERT...SELECT (tuple_insert path) — works, no crash
CREATE TABLE sh2_insert_sel(id int PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh2_insert_sel SELECT g, 'v' || g FROM generate_series(1, 100) g;
SELECT count(*) AS sh2_insert_sel_count FROM sh2_insert_sel;
DROP TABLE sh2_insert_sel;

-- Test SH2-8: Single inserts still work after Phase 2 changes
CREATE TABLE sh2_singles(id int PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh2_singles VALUES (5, 'e');
INSERT INTO sh2_singles VALUES (3, 'c');
INSERT INTO sh2_singles VALUES (1, 'a');
INSERT INTO sh2_singles VALUES (4, 'd');
INSERT INTO sh2_singles VALUES (2, 'b');
SELECT count(*) AS sh2_singles_count FROM sh2_singles;
-- Verify data roundtrip
SELECT id, val FROM sh2_singles ORDER BY id;
DROP TABLE sh2_singles;

-- Test SH2-9: COPY + VACUUM + more COPY (PK cache survives)
CREATE TABLE sh2_vac(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh2_src9 AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 200) g ORDER BY random();
COPY sh2_src9 TO '/tmp/sh2_vac.csv' CSV;
COPY sh2_vac FROM '/tmp/sh2_vac.csv' CSV;
DELETE FROM sh2_vac WHERE id <= 100;
VACUUM sh2_vac;
-- Second COPY after vacuum
TRUNCATE sh2_src9;
INSERT INTO sh2_src9 SELECT g, 'w' || g FROM generate_series(201, 400) g ORDER BY random();
COPY sh2_src9 TO '/tmp/sh2_vac2.csv' CSV;
COPY sh2_vac FROM '/tmp/sh2_vac2.csv' CSV;
SELECT
    CASE WHEN count(*) = 0
         THEN 'vac_sorted_ok'
         ELSE 'vac_sorted_FAIL'
    END AS sh2_vac_result
FROM (
    SELECT id < lag(id) OVER (ORDER BY ctid) AS inv
    FROM sh2_vac
    WHERE id > 200
) sub
WHERE inv;
SELECT count(*) AS sh2_vac_count FROM sh2_vac;
DROP TABLE sh2_vac;
DROP TABLE sh2_src9;

-- ================================================================
-- sorted_heap Table AM: Phase 3 tests (Zone Maps)
-- ================================================================

-- Test SH3-1: COPY with int PK — zone map created
CREATE TABLE sh3_zonemap(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh3_src1 AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 500) g ORDER BY random();
COPY sh3_src1 TO '/tmp/sh3_zonemap.csv' CSV;
COPY sh3_zonemap FROM '/tmp/sh3_zonemap.csv' CSV;
-- Verify zone map was populated
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh3_zonemap'::regclass) LIKE 'version=% nentries=% pk_typid=23%'
         THEN 'zonemap_created_ok'
         ELSE 'zonemap_created_FAIL'
    END AS sh3_zonemap_created;
SELECT count(*) AS sh3_zonemap_count FROM sh3_zonemap;
DROP TABLE sh3_zonemap;
DROP TABLE sh3_src1;

-- Test SH3-2: Text PK — zone map not used (graceful degradation)
CREATE TABLE sh3_textpk(name text PRIMARY KEY, val int) USING sorted_heap;
CREATE TEMP TABLE sh3_src2 AS
    SELECT 'item_' || lpad(g::text, 4, '0') AS name, g AS val
    FROM generate_series(1, 100) g ORDER BY random();
COPY sh3_src2 TO '/tmp/sh3_textpk.csv' CSV;
COPY sh3_textpk FROM '/tmp/sh3_textpk.csv' CSV;
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh3_textpk'::regclass) LIKE '%nentries=0%'
         THEN 'zonemap_textpk_skip_ok'
         ELSE 'zonemap_textpk_skip_FAIL'
    END AS sh3_textpk_result;
SELECT count(*) AS sh3_textpk_count FROM sh3_textpk;
DROP TABLE sh3_textpk;
DROP TABLE sh3_src2;

-- Test SH3-3: No PK — zone map not used, data accessible
CREATE TABLE sh3_nopk(id int, val text) USING sorted_heap;
CREATE TEMP TABLE sh3_src3 AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 100) g ORDER BY random();
COPY sh3_src3 TO '/tmp/sh3_nopk.csv' CSV;
COPY sh3_nopk FROM '/tmp/sh3_nopk.csv' CSV;
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh3_nopk'::regclass) LIKE '%nentries=0%'
         THEN 'zonemap_nopk_skip_ok'
         ELSE 'zonemap_nopk_skip_FAIL'
    END AS sh3_nopk_result;
SELECT count(*) AS sh3_nopk_count FROM sh3_nopk;
DROP TABLE sh3_nopk;
DROP TABLE sh3_src3;

-- Test SH3-4: TRUNCATE resets zone map
CREATE TABLE sh3_trunc(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh3_src4 AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 100) g ORDER BY random();
COPY sh3_src4 TO '/tmp/sh3_trunc.csv' CSV;
COPY sh3_trunc FROM '/tmp/sh3_trunc.csv' CSV;
-- Verify zone map has entries
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh3_trunc'::regclass) LIKE '%nentries=0%'
         THEN 'pre_trunc_FAIL'
         ELSE 'pre_trunc_has_entries'
    END AS sh3_trunc_before;
TRUNCATE sh3_trunc;
-- Verify zone map is reset after truncate
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh3_trunc'::regclass) LIKE '%nentries=0%'
         THEN 'zonemap_trunc_ok'
         ELSE 'zonemap_trunc_FAIL'
    END AS sh3_trunc_after;
DROP TABLE sh3_trunc;
DROP TABLE sh3_src4;

-- Test SH3-5: Zone map entries have correct min/max ranges
CREATE TABLE sh3_ranges(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh3_src5 AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 100) g ORDER BY random();
COPY sh3_src5 TO '/tmp/sh3_ranges.csv' CSV;
COPY sh3_ranges FROM '/tmp/sh3_ranges.csv' CSV;
-- Zone map should contain entries; first entry's min should be >= 1
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh3_ranges'::regclass) LIKE 'version=% nentries=% pk_typid=23%flags=0 [1:%'
         THEN 'zonemap_ranges_ok'
         ELSE 'zonemap_ranges_FAIL'
    END AS sh3_ranges_result;
-- All data accessible
SELECT count(*) AS sh3_ranges_count FROM sh3_ranges;
DROP TABLE sh3_ranges;
DROP TABLE sh3_src5;

-- Test SH3-6: COPY + DELETE + VACUUM — zone map survives, data accessible
CREATE TABLE sh3_vacuum(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh3_src6 AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 500) g ORDER BY random();
COPY sh3_src6 TO '/tmp/sh3_vacuum.csv' CSV;
COPY sh3_vacuum FROM '/tmp/sh3_vacuum.csv' CSV;
DELETE FROM sh3_vacuum WHERE id BETWEEN 100 AND 200;
VACUUM sh3_vacuum;
-- Zone map still has entries (conservative — may be wider than actual data)
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh3_vacuum'::regclass) LIKE 'version=% nentries=%'
         THEN 'zonemap_vacuum_ok'
         ELSE 'zonemap_vacuum_FAIL'
    END AS sh3_vacuum_result;
SELECT count(*) AS sh3_vacuum_count FROM sh3_vacuum;
-- Verify data accessible after vacuum
SELECT count(*) AS sh3_vacuum_range FROM sh3_vacuum WHERE id BETWEEN 50 AND 150;
DROP TABLE sh3_vacuum;
DROP TABLE sh3_src6;

-- Test SH3-7: Existing Phase 2 sort still works with zone map
CREATE TABLE sh3_sort(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh3_src7 AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 500) g ORDER BY id DESC;
COPY sh3_src7 TO '/tmp/sh3_sort.csv' CSV;
COPY sh3_sort FROM '/tmp/sh3_sort.csv' CSV;
-- Physical sort order still correct
SELECT
    CASE WHEN count(*) = 0
         THEN 'zonemap_sort_ok'
         ELSE 'zonemap_sort_FAIL'
    END AS sh3_sort_result
FROM (
    SELECT id < lag(id) OVER (ORDER BY ctid) AS inv
    FROM sh3_sort
) sub
WHERE inv;
-- Zone map populated
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh3_sort'::regclass) LIKE 'version=% nentries=% pk_typid=23%'
         THEN 'zonemap_sort_stats_ok'
         ELSE 'zonemap_sort_stats_FAIL'
    END AS sh3_sort_stats;
DROP TABLE sh3_sort;
DROP TABLE sh3_src7;

-- ================================================================
-- sorted_heap Table AM: Phase 4 tests (Compaction)
-- ================================================================

-- Test SH4-1: Multiple COPY batches → compact → global sort
CREATE TABLE sh4_compact(id int PRIMARY KEY, val text) USING sorted_heap;
-- Batch 1: ids 201-400 (will be after batch 2 in PK order)
CREATE TEMP TABLE sh4_src1 AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(201, 400) g ORDER BY random();
COPY sh4_src1 TO '/tmp/sh4_batch1.csv' CSV;
COPY sh4_compact FROM '/tmp/sh4_batch1.csv' CSV;
-- Batch 2: ids 1-200 (physically after batch 1 but lower PK)
CREATE TEMP TABLE sh4_src2 AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 200) g ORDER BY random();
COPY sh4_src2 TO '/tmp/sh4_batch2.csv' CSV;
COPY sh4_compact FROM '/tmp/sh4_batch2.csv' CSV;

SELECT count(*) AS sh4_pre_compact_count FROM sh4_compact;

-- Compact: rewrites in global PK order
SELECT sorted_heap_compact('sh4_compact'::regclass);

-- Verify global sort — zero inversions
SELECT
    CASE WHEN count(*) = 0
         THEN 'compact_sorted_ok'
         ELSE 'compact_sorted_FAIL'
    END AS sh4_compact_result
FROM (
    SELECT id < lag(id) OVER (ORDER BY ctid) AS inv
    FROM sh4_compact
) sub
WHERE inv;

SELECT count(*) AS sh4_post_compact_count FROM sh4_compact;

DROP TABLE sh4_compact;
DROP TABLE sh4_src1;
DROP TABLE sh4_src2;

-- Test SH4-2: Zone map accuracy after compaction
CREATE TABLE sh4_zonemap(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh4_zm_src AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 500) g ORDER BY random();
COPY sh4_zm_src TO '/tmp/sh4_zonemap.csv' CSV;
COPY sh4_zonemap FROM '/tmp/sh4_zonemap.csv' CSV;
SELECT sorted_heap_compact('sh4_zonemap'::regclass);
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh4_zonemap'::regclass) LIKE 'version=% nentries=% pk_typid=23%'
         THEN 'compact_zonemap_ok'
         ELSE 'compact_zonemap_FAIL'
    END AS sh4_zonemap_result;
SELECT count(*) AS sh4_zonemap_count FROM sh4_zonemap;
DROP TABLE sh4_zonemap;
DROP TABLE sh4_zm_src;

-- Test SH4-3: Compact table without PK — should error
CREATE TABLE sh4_nopk(id int, val text) USING sorted_heap;
INSERT INTO sh4_nopk SELECT g, 'v' || g FROM generate_series(1, 10) g;
SELECT sorted_heap_compact('sh4_nopk'::regclass);
DROP TABLE sh4_nopk;

-- Test SH4-4: Compact after DELETE + VACUUM
CREATE TABLE sh4_vacuum(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh4_vac_src AS
    SELECT g AS id, 'v' || g AS val FROM generate_series(1, 500) g ORDER BY random();
COPY sh4_vac_src TO '/tmp/sh4_vacuum.csv' CSV;
COPY sh4_vacuum FROM '/tmp/sh4_vacuum.csv' CSV;
DELETE FROM sh4_vacuum WHERE id BETWEEN 100 AND 300;
VACUUM sh4_vacuum;
SELECT sorted_heap_compact('sh4_vacuum'::regclass);
SELECT
    CASE WHEN count(*) = 0
         THEN 'compact_vacuum_sorted_ok'
         ELSE 'compact_vacuum_sorted_FAIL'
    END AS sh4_vacuum_result
FROM (
    SELECT id < lag(id) OVER (ORDER BY ctid) AS inv
    FROM sh4_vacuum
) sub
WHERE inv;
SELECT count(*) AS sh4_vacuum_count FROM sh4_vacuum;
DROP TABLE sh4_vacuum;
DROP TABLE sh4_vac_src;

-- Test SH4-5: Standalone zonemap rebuild
CREATE TABLE sh4_rebuild(id int PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh4_rebuild SELECT g, 'v' || g FROM generate_series(1, 100) g;
SELECT sorted_heap_rebuild_zonemap('sh4_rebuild'::regclass);
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh4_rebuild'::regclass) LIKE 'version=% nentries=% pk_typid=23%'
         THEN 'rebuild_zonemap_ok'
         ELSE 'rebuild_zonemap_FAIL'
    END AS sh4_rebuild_result;
DROP TABLE sh4_rebuild;

-- Test SH4-6: Compact with bigint PK
CREATE TABLE sh4_bigint(id bigint PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh4_big_src AS
    SELECT g::bigint AS id, 'v' || g AS val FROM generate_series(1, 300) g ORDER BY random();
COPY sh4_big_src TO '/tmp/sh4_bigint.csv' CSV;
COPY sh4_bigint FROM '/tmp/sh4_bigint.csv' CSV;
CREATE TEMP TABLE sh4_big_src2 AS
    SELECT (g + 300)::bigint AS id, 'w' || g AS val FROM generate_series(1, 300) g ORDER BY random();
COPY sh4_big_src2 TO '/tmp/sh4_bigint2.csv' CSV;
COPY sh4_bigint FROM '/tmp/sh4_bigint2.csv' CSV;
SELECT sorted_heap_compact('sh4_bigint'::regclass);
SELECT
    CASE WHEN count(*) = 0
         THEN 'compact_bigint_sorted_ok'
         ELSE 'compact_bigint_sorted_FAIL'
    END AS sh4_bigint_result
FROM (
    SELECT id < lag(id) OVER (ORDER BY ctid) AS inv
    FROM sh4_bigint
) sub
WHERE inv;
SELECT count(*) AS sh4_bigint_count FROM sh4_bigint;
DROP TABLE sh4_bigint;
DROP TABLE sh4_big_src;
DROP TABLE sh4_big_src2;

-- ================================================================
-- sorted_heap Table AM: Phase 5 tests (Scan Pruning via Zone Maps)
-- ================================================================

-- Helper: check if EXPLAIN plan contains a pattern
CREATE FUNCTION sh5_plan_contains(query text, pattern text) RETURNS boolean AS $$
DECLARE
    r record;
BEGIN
    FOR r IN EXECUTE 'EXPLAIN (COSTS OFF) ' || query
    LOOP
        IF r."QUERY PLAN" LIKE '%' || pattern || '%' THEN
            RETURN true;
        END IF;
    END LOOP;
    RETURN false;
END;
$$ LANGUAGE plpgsql;

-- Setup: load data via COPY, enough for many pages
CREATE TABLE sh5_scan(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh5_src AS
    SELECT g AS id, repeat('x', 100) AS val FROM generate_series(1, 2000) g ORDER BY random();
COPY sh5_src TO '/tmp/sh5_scan.csv' CSV;
COPY sh5_scan FROM '/tmp/sh5_scan.csv' CSV;

-- Disable index scans to test seq scan vs custom scan
SET enable_indexscan = off;
SET enable_bitmapscan = off;

-- Test SH5-1: Before compact — no pruning (zone map not valid)
SELECT sh5_plan_contains(
    'SELECT * FROM sh5_scan WHERE id BETWEEN 100 AND 200',
    'SortedHeapScan') AS sh5_before_compact;

-- Compact + analyze
SELECT sorted_heap_compact('sh5_scan'::regclass);
ANALYZE sh5_scan;

-- Test SH5-2: After compact — custom scan used for range query
SELECT sh5_plan_contains(
    'SELECT * FROM sh5_scan WHERE id BETWEEN 100 AND 200',
    'SortedHeapScan') AS sh5_after_compact;

-- Test SH5-3: Range query — correct results
SELECT count(*) AS sh5_range_count FROM sh5_scan WHERE id BETWEEN 100 AND 200;

-- Test SH5-4: Point query — correct result
SELECT count(*) AS sh5_point_count FROM sh5_scan WHERE id = 500;

-- Test SH5-5: Full scan (no WHERE) — all rows
SELECT count(*) AS sh5_full_count FROM sh5_scan;

-- Test SH5-6: INSERT invalidates zone map, falls back to seq scan
INSERT INTO sh5_scan VALUES (2001, 'extra');
SELECT sh5_plan_contains(
    'SELECT * FROM sh5_scan WHERE id BETWEEN 100 AND 200',
    'SortedHeapScan') AS sh5_after_insert;
SELECT count(*) AS sh5_after_insert_range FROM sh5_scan WHERE id BETWEEN 100 AND 200;

-- Test SH5-7: Re-compact restores pruning
SELECT sorted_heap_compact('sh5_scan'::regclass);
ANALYZE sh5_scan;
SELECT sh5_plan_contains(
    'SELECT * FROM sh5_scan WHERE id BETWEEN 100 AND 200',
    'SortedHeapScan') AS sh5_recompact;
SELECT count(*) AS sh5_recompact_range FROM sh5_scan WHERE id BETWEEN 100 AND 200;

RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE sh5_scan;
DROP TABLE sh5_src;
DROP FUNCTION sh5_plan_contains(text, text);

-- ================================================================
-- sorted_heap Table AM: Phase 6 tests (Production Hardening)
-- ================================================================

-- Helper: check if EXPLAIN plan contains a pattern
CREATE FUNCTION sh6_plan_contains(query text, pattern text) RETURNS boolean AS $$
DECLARE
    r record;
BEGIN
    FOR r IN EXECUTE 'EXPLAIN (COSTS OFF) ' || query
    LOOP
        IF r."QUERY PLAN" LIKE '%' || pattern || '%' THEN
            RETURN true;
        END IF;
    END LOOP;
    RETURN false;
END;
$$ LANGUAGE plpgsql;

-- Setup
CREATE TABLE sh6_guc(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh6_src AS
    SELECT g AS id, repeat('x', 80) AS val FROM generate_series(1, 1000) g ORDER BY random();
COPY sh6_src TO '/tmp/sh6_guc.csv' CSV;
COPY sh6_guc FROM '/tmp/sh6_guc.csv' CSV;
SELECT sorted_heap_compact('sh6_guc'::regclass);
ANALYZE sh6_guc;
SET enable_indexscan = off;
SET enable_bitmapscan = off;

-- Test SH6-1: GUC off → no SortedHeapScan
SET sorted_heap.enable_scan_pruning = off;
SELECT sh6_plan_contains(
    'SELECT * FROM sh6_guc WHERE id BETWEEN 100 AND 200',
    'SortedHeapScan') AS sh6_guc_off;

-- Test SH6-2: GUC on → SortedHeapScan
SET sorted_heap.enable_scan_pruning = on;
SELECT sh6_plan_contains(
    'SELECT * FROM sh6_guc WHERE id BETWEEN 100 AND 200',
    'SortedHeapScan') AS sh6_guc_on;

RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE sh6_guc;
DROP TABLE sh6_src;

-- Test SH6-3: TIMESTAMP PK + compact + range query
CREATE TABLE sh6_ts(ts timestamp PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh6_ts
    SELECT '2024-01-01'::timestamp + (g || ' seconds')::interval, 'v' || g
    FROM generate_series(1, 1000) g;
SELECT sorted_heap_compact('sh6_ts'::regclass);
ANALYZE sh6_ts;
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh6_ts'::regclass) LIKE 'version=% nentries=%'
         THEN 'ts_zonemap_ok'
         ELSE 'ts_zonemap_FAIL'
    END AS sh6_ts_result;
SELECT count(*) AS sh6_ts_range
FROM sh6_ts
WHERE ts BETWEEN '2024-01-01 00:05:00'::timestamp AND '2024-01-01 00:10:00'::timestamp;
DROP TABLE sh6_ts;

-- Test SH6-4: DATE PK + compact + point query
CREATE TABLE sh6_date(d date PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh6_date
    SELECT '2024-01-01'::date + g, 'v' || g
    FROM generate_series(1, 500) g;
SELECT sorted_heap_compact('sh6_date'::regclass);
ANALYZE sh6_date;
SELECT count(*) AS sh6_date_point
FROM sh6_date WHERE d = '2024-03-01'::date;
DROP TABLE sh6_date;

-- Test SH6-5: INSERT within zone map range → pruning still works
CREATE TABLE sh6_insert(id int PRIMARY KEY, val text) USING sorted_heap;
CREATE TEMP TABLE sh6_ins_src AS
    SELECT g AS id, repeat('x', 80) AS val FROM generate_series(1, 500) g ORDER BY random();
COPY sh6_ins_src TO '/tmp/sh6_insert.csv' CSV;
COPY sh6_insert FROM '/tmp/sh6_insert.csv' CSV;
SELECT sorted_heap_compact('sh6_insert'::regclass);
ANALYZE sh6_insert;

SET enable_indexscan = off;
SET enable_bitmapscan = off;

-- Insert into covered page — pruning should survive
INSERT INTO sh6_insert VALUES (250, 'new') ON CONFLICT (id) DO UPDATE SET val = 'new';
SELECT sh6_plan_contains(
    'SELECT * FROM sh6_insert WHERE id = 100',
    'SortedHeapScan') AS sh6_insert_covered;

RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE sh6_insert;
DROP TABLE sh6_ins_src;

-- Test SH6-6: INSERT outside zone map range → pruning disabled
-- (Use a fresh table, insert beyond existing pages)
CREATE TABLE sh6_outside(id int PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh6_outside SELECT g, repeat('x', 80) FROM generate_series(1, 100) g;
SELECT sorted_heap_compact('sh6_outside'::regclass);
ANALYZE sh6_outside;

SET enable_indexscan = off;
SET enable_bitmapscan = off;

-- Insert row that will go to a new page (beyond zone map coverage)
INSERT INTO sh6_outside SELECT g, repeat('x', 80) FROM generate_series(101, 1000) g;
SELECT sh6_plan_contains(
    'SELECT * FROM sh6_outside WHERE id = 50',
    'SortedHeapScan') AS sh6_insert_outside;

-- Test SH6-7: Re-compact restores pruning
SELECT sorted_heap_compact('sh6_outside'::regclass);
ANALYZE sh6_outside;
SELECT sh6_plan_contains(
    'SELECT * FROM sh6_outside WHERE id = 50',
    'SortedHeapScan') AS sh6_recompact;

RESET enable_indexscan;
RESET enable_bitmapscan;
DROP TABLE sh6_outside;

-- Test SH6-9: EXPLAIN ANALYZE shows counters (textual check)
CREATE TABLE sh6_explain(id int PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh6_explain SELECT g, repeat('x', 80) FROM generate_series(1, 500) g;
SELECT sorted_heap_compact('sh6_explain'::regclass);
ANALYZE sh6_explain;

SET enable_indexscan = off;
SET enable_bitmapscan = off;

CREATE FUNCTION sh6_explain_has_counters() RETURNS boolean AS $$
DECLARE
    r record;
BEGIN
    FOR r IN EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT * FROM sh6_explain WHERE id BETWEEN 100 AND 200'
    LOOP
        IF r."QUERY PLAN" LIKE '%Scanned Blocks%' THEN
            RETURN true;
        END IF;
    END LOOP;
    RETURN false;
END;
$$ LANGUAGE plpgsql;
SELECT sh6_explain_has_counters() AS sh6_explain_counters;

RESET enable_indexscan;
RESET enable_bitmapscan;
DROP FUNCTION sh6_explain_has_counters();
DROP TABLE sh6_explain;

-- Test SH6-10: sorted_heap_scan_stats() with reset
SELECT sorted_heap_reset_stats();
-- Create a small table, compact it, run a pruned query to generate stats
CREATE TABLE sh6_stats(id int PRIMARY KEY, v text) USING sorted_heap;
INSERT INTO sh6_stats SELECT i, 'x' FROM generate_series(1, 1000) i;
SELECT sorted_heap_compact('sh6_stats'::regclass);
ANALYZE sh6_stats;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM sh6_stats WHERE id = 50;
RESET enable_indexscan;
RESET enable_bitmapscan;
SELECT
    CASE WHEN (sorted_heap_scan_stats()).total_scans >= 1
         THEN 'scan_stats_ok'
         ELSE 'scan_stats_FAIL'
    END AS sh6_stats_result;
DROP TABLE sh6_stats;

-- ====================================================================
-- SH7: Online compact (sorted_heap_compact_online)
-- ====================================================================

-- Test SH7-1: Basic online compact produces correct row count
CREATE TABLE sh7_basic(id int PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh7_basic SELECT g, repeat('x', 80) FROM generate_series(1, 500) g;
-- Scramble order: delete and re-insert some rows
DELETE FROM sh7_basic WHERE id % 3 = 0;
INSERT INTO sh7_basic SELECT g, repeat('y', 80) FROM generate_series(501, 700) g;
SELECT count(*) AS sh7_before_count FROM sh7_basic;
CALL sorted_heap_compact_online('sh7_basic'::regclass);
SELECT count(*) AS sh7_after_count FROM sh7_basic;

-- Test SH7-2: Data is sorted after online compact
SELECT (bool_and(id >= lag_id)) AS sh7_sorted
FROM (
    SELECT id, lag(id, 1, 0) OVER (ORDER BY ctid) AS lag_id
    FROM sh7_basic
) sub;

-- Test SH7-3: Zone map populated after online compact
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh7_basic'::regclass) LIKE 'version=% nentries=% pk_typid=23%flags=valid%'
         THEN 'zonemap_online_ok'
         ELSE 'zonemap_online_FAIL: ' || sorted_heap_zonemap_stats('sh7_basic'::regclass)
    END AS sh7_zonemap_result;

-- Test SH7-4: Scan pruning works after online compact
ANALYZE sh7_basic;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT sh6_plan_contains(
    'SELECT * FROM sh7_basic WHERE id = 50',
    'SortedHeapScan') AS sh7_pruning_works;
RESET enable_indexscan;
RESET enable_bitmapscan;

-- Test SH7-5: Online compact produces same result as regular compact
CREATE TABLE sh7_compare(id int PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh7_compare SELECT g, repeat('z', 40) FROM generate_series(1, 300) g;
DELETE FROM sh7_compare WHERE id % 5 = 0;
INSERT INTO sh7_compare SELECT g, repeat('w', 40) FROM generate_series(301, 400) g;
CALL sorted_heap_compact_online('sh7_compare'::regclass);
SELECT count(*) AS sh7_compare_online_count FROM sh7_compare;
-- Re-compact with regular compact to verify equivalence
SELECT sorted_heap_compact('sh7_compare'::regclass);
SELECT count(*) AS sh7_compare_regular_count FROM sh7_compare;

DROP TABLE sh7_basic;
DROP TABLE sh7_compare;

-- ============================================================
-- SH8: Multi-column zone map for composite PK
-- ============================================================

-- Test SH8-1: Composite PK (int, int) — zone map has pk_typid2
CREATE TABLE sh8_intint(a int, b int, val text, PRIMARY KEY(a, b)) USING sorted_heap;
INSERT INTO sh8_intint
  SELECT (g / 100) + 1, g % 100, repeat('x', 40)
  FROM generate_series(1, 500) g;
SELECT sorted_heap_compact('sh8_intint'::regclass);
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh8_intint'::regclass)
              LIKE 'version=6%pk_typid=23 pk_typid2=23%flags=valid%'
         THEN 'sh8_intint_zonemap_ok'
         ELSE 'sh8_intint_zonemap_FAIL: ' || sorted_heap_zonemap_stats('sh8_intint'::regclass)
    END AS sh8_1_result;

-- Test SH8-2: Query on both columns — correct results
SELECT count(*) AS sh8_both_count FROM sh8_intint WHERE a = 3 AND b >= 50;
SELECT count(*) AS sh8_a_only FROM sh8_intint WHERE a BETWEEN 2 AND 4;
SELECT count(*) AS sh8_total FROM sh8_intint;

-- Test SH8-3: Pruning on both columns via EXPLAIN
ANALYZE sh8_intint;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
-- Query on col1 only should prune
SELECT sh6_plan_contains(
    'SELECT * FROM sh8_intint WHERE a = 3',
    'SortedHeapScan') AS sh8_col1_pruning;
-- Query on col2 only — no pruning when b=50 exists in all blocks (nblocks=total)
SELECT sh6_plan_contains(
    'SELECT * FROM sh8_intint WHERE b = 50',
    'SortedHeapScan') AS sh8_col2_pruning;
RESET enable_indexscan;
RESET enable_bitmapscan;

-- Test SH8-4: Composite PK (int, timestamp) — col2 tracked
CREATE TABLE sh8_ts(user_id int, ts timestamp, val text,
    PRIMARY KEY(user_id, ts)) USING sorted_heap;
INSERT INTO sh8_ts
  SELECT (g / 100) + 1,
         '2024-01-01'::timestamp + (g || ' hours')::interval,
         repeat('y', 30)
  FROM generate_series(1, 400) g;
SELECT sorted_heap_compact('sh8_ts'::regclass);
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh8_ts'::regclass)
              LIKE 'version=6%pk_typid=23 pk_typid2=1114%flags=valid%'
         THEN 'sh8_ts_zonemap_ok'
         ELSE 'sh8_ts_zonemap_FAIL: ' || sorted_heap_zonemap_stats('sh8_ts'::regclass)
    END AS sh8_4_result;

-- Test SH8-5: Non-trackable second column — graceful degradation
CREATE TABLE sh8_text(a int, b text, val text, PRIMARY KEY(a, b)) USING sorted_heap;
INSERT INTO sh8_text
  SELECT g, 'key' || g, repeat('z', 40)
  FROM generate_series(1, 200) g;
SELECT sorted_heap_compact('sh8_text'::regclass);
-- pk_typid2 should be 0 (InvalidOid) — text not supported
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh8_text'::regclass)
              LIKE 'version=6%pk_typid=23 pk_typid2=0%flags=valid%'
         THEN 'sh8_text_degradation_ok'
         ELSE 'sh8_text_degradation_FAIL: ' || sorted_heap_zonemap_stats('sh8_text'::regclass)
    END AS sh8_5_result;
-- Queries still work (col1 pruning only)
SELECT count(*) AS sh8_text_count FROM sh8_text WHERE a = 100;

-- Test SH8-6: Single-column PK — unchanged behavior
CREATE TABLE sh8_single(id int PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh8_single SELECT g, repeat('w', 40) FROM generate_series(1, 300) g;
SELECT sorted_heap_compact('sh8_single'::regclass);
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh8_single'::regclass)
              LIKE 'version=6%pk_typid=23 pk_typid2=0%flags=valid%'
         THEN 'sh8_single_ok'
         ELSE 'sh8_single_FAIL: ' || sorted_heap_zonemap_stats('sh8_single'::regclass)
    END AS sh8_6_result;
SELECT count(*) AS sh8_single_count FROM sh8_single WHERE id BETWEEN 100 AND 200;

DROP TABLE sh8_intint;
DROP TABLE sh8_ts;
DROP TABLE sh8_text;
DROP TABLE sh8_single;

-- ============================================================
-- SH9: Parallel Custom Scan
-- ============================================================

-- Create a table large enough for parallel to kick in
CREATE TABLE sh9_par(id int PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh9_par
  SELECT g, repeat('p', 80)
  FROM generate_series(1, 50000) g;
SELECT sorted_heap_compact('sh9_par'::regclass);

-- Test SH9-1: Serial baseline — correct result count
SELECT count(*) AS sh9_serial_count FROM sh9_par WHERE id BETWEEN 1 AND 50000;

-- Test SH9-2: Force parallel, verify correct results
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SET max_parallel_workers_per_gather = 2;
SET parallel_tuple_cost = 0;
SET parallel_setup_cost = 0;
SET min_parallel_table_scan_size = 0;

SELECT count(*) AS sh9_parallel_count FROM sh9_par WHERE id BETWEEN 1 AND 50000;

-- Test SH9-3: EXPLAIN shows Gather + parallel-aware SortedHeapScan
SELECT
    CASE WHEN sh6_plan_contains(
        'SELECT * FROM sh9_par WHERE id BETWEEN 10000 AND 40000',
        'Gather')
         THEN 'sh9_gather_ok'
         ELSE 'sh9_gather_FAIL'
    END AS sh9_2_result;

SELECT
    CASE WHEN sh6_plan_contains(
        'SELECT * FROM sh9_par WHERE id BETWEEN 10000 AND 40000',
        'SortedHeapScan')
         THEN 'sh9_scan_ok'
         ELSE 'sh9_scan_FAIL'
    END AS sh9_3_result;

-- Test SH9-4: Default parallel GUCs, narrow range stays serial (no Gather)
-- Keep seqscan/indexscan/bitmapscan off to force SortedHeapScan
-- but reset parallel GUCs to defaults
RESET max_parallel_workers_per_gather;
RESET parallel_tuple_cost;
RESET parallel_setup_cost;
RESET min_parallel_table_scan_size;

SELECT
    CASE WHEN sh6_plan_contains(
        'SELECT * FROM sh9_par WHERE id = 100',
        'SortedHeapScan')
         THEN 'sh9_serial_scan_ok'
         ELSE 'sh9_serial_scan_FAIL'
    END AS sh9_4a_result;

-- Narrow range should NOT have Gather with default parallel settings
SELECT
    CASE WHEN NOT sh6_plan_contains(
        'SELECT * FROM sh9_par WHERE id = 100',
        'Gather')
         THEN 'sh9_no_gather_ok'
         ELSE 'sh9_no_gather_FAIL'
    END AS sh9_4b_result;

RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;

DROP TABLE sh9_par;

-- ============================================================
-- SH10: Zone map overflow boundary correctness
-- ============================================================
-- Table with >250 data pages exercises v5 overflow zone map entries.
-- Verify that INSERT after compact doesn't corrupt scan results
-- for pages in the overflow range (entries 250+).

CREATE TABLE sh10_ovfl(id int PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh10_ovfl
  SELECT g, repeat('x', 80)
  FROM generate_series(1, 50000) g;
SELECT sorted_heap_compact('sh10_ovfl'::regclass);

-- SH10-1: Serial custom scan returns correct count after compact
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SET max_parallel_workers_per_gather = 0;

SELECT count(*) AS sh10_post_compact
  FROM sh10_ovfl WHERE id BETWEEN 1 AND 50000;

-- SH10-2: INSERT into overflow-range page, then re-count
-- Zone map becomes stale, pruning disabled — count must still be correct
INSERT INTO sh10_ovfl VALUES (50001, 'overflow_insert');

SELECT count(*) AS sh10_post_insert
  FROM sh10_ovfl WHERE id BETWEEN 1 AND 50001;

-- SH10-3: Re-compact, verify overflow entries restored
SELECT sorted_heap_compact('sh10_ovfl'::regclass);

SELECT count(*) AS sh10_post_recompact
  FROM sh10_ovfl WHERE id BETWEEN 1 AND 50001;

RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
RESET max_parallel_workers_per_gather;

DROP TABLE sh10_ovfl;

-- ============================================================
-- SH11: Incremental merge compaction (sorted_heap_merge)
-- ============================================================

-- SH11-1: Create 50K-row table, compact to establish sorted prefix
CREATE TABLE sh11_merge(id int PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh11_merge
  SELECT g, 'row-' || g
  FROM generate_series(1, 50000) g;
SELECT sorted_heap_compact('sh11_merge'::regclass);

-- SH11-2: INSERT rows with keys that overlap existing range.
-- Keys -5000..-1 sort before 1..50000, creating zone map overlap
-- at the boundary between compacted prefix and new tail pages.
INSERT INTO sh11_merge
  SELECT g, 'new-' || g
  FROM generate_series(-5000, -1) g;

-- SH11-3: Merge (prefix sequential scan + tail tuplesort)
SELECT sorted_heap_merge('sh11_merge'::regclass);

-- SH11-4: Verify correct count (55000) after merge
SELECT count(*) AS sh11_count_after_merge FROM sh11_merge;

-- SH11-5: Verify zone map valid (flags=valid means ZONEMAP_VALID)
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh11_merge'::regclass) LIKE '%flags=valid%'
         THEN 'sh11_zonemap_valid_ok'
         ELSE 'sh11_zonemap_valid_FAIL: ' ||
              sorted_heap_zonemap_stats('sh11_merge'::regclass)
    END AS sh11_5_result;

-- SH11-6: Verify scan pruning works after merge
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SET max_parallel_workers_per_gather = 0;

SELECT count(*) AS sh11_pruned_count
  FROM sh11_merge WHERE id BETWEEN 100 AND 200;

-- Check that EXPLAIN shows zone map pruning
SELECT
    CASE WHEN sh6_plan_contains(
        'SELECT * FROM sh11_merge WHERE id BETWEEN 100 AND 200',
        'Zone Map')
         THEN 'sh11_zonemap_pruning_ok'
         ELSE 'sh11_zonemap_pruning_FAIL'
    END AS sh11_6_result;

RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
RESET max_parallel_workers_per_gather;

-- SH11-7: Re-compact, verify same count (merge didn't lose data)
SELECT sorted_heap_compact('sh11_merge'::regclass);
SELECT count(*) AS sh11_count_after_recompact FROM sh11_merge;

-- SH11-8: Merge on already-sorted table → early exit or minimal work
SELECT sorted_heap_merge('sh11_merge'::regclass);
SELECT count(*) AS sh11_count_after_merge2 FROM sh11_merge;

-- SH11-9: Merge on never-compacted table → full tuplesort fallback
CREATE TABLE sh11_unsorted(id int PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh11_unsorted
  SELECT g, 'row-' || g
  FROM generate_series(1, 1000) g;
SELECT sorted_heap_merge('sh11_unsorted'::regclass);
SELECT count(*) AS sh11_unsorted_count FROM sh11_unsorted;
DROP TABLE sh11_unsorted;

-- SH11-10: Merge on empty table → no-op
CREATE TABLE sh11_empty(id int PRIMARY KEY) USING sorted_heap;
SELECT sorted_heap_merge('sh11_empty'::regclass);
DROP TABLE sh11_empty;

DROP TABLE sh11_merge;

-- ============================================================
-- SH12: Online merge compaction (sorted_heap_merge_online)
-- ============================================================

-- SH12-1: Create table, compact, add overlapping rows, online merge
CREATE TABLE sh12_merge(id int PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh12_merge
  SELECT g, 'row-' || g
  FROM generate_series(1, 50000) g;
SELECT sorted_heap_compact('sh12_merge'::regclass);

INSERT INTO sh12_merge
  SELECT g, 'new-' || g
  FROM generate_series(-5000, -1) g;

SELECT count(*) AS sh12_before FROM sh12_merge;

CALL sorted_heap_merge_online('sh12_merge'::regclass);

-- SH12-2: Verify correct count after online merge
SELECT count(*) AS sh12_after FROM sh12_merge;

-- SH12-3: Verify data is physically sorted (ctid order = PK order)
SELECT (bool_and(id >= lag_id)) AS sh12_sorted
FROM (
    SELECT id, lag(id, 1, -999999) OVER (ORDER BY ctid) AS lag_id
    FROM sh12_merge
) sub;

-- SH12-4: Verify zone map valid (flags=valid)
SELECT
    CASE WHEN sorted_heap_zonemap_stats('sh12_merge'::regclass) LIKE '%flags=valid%'
         THEN 'sh12_zonemap_ok'
         ELSE 'sh12_zonemap_FAIL: ' ||
              sorted_heap_zonemap_stats('sh12_merge'::regclass)
    END AS sh12_4_result;

-- SH12-5: Verify scan pruning works after online merge
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SET max_parallel_workers_per_gather = 0;

SELECT
    CASE WHEN sh6_plan_contains(
        'SELECT * FROM sh12_merge WHERE id BETWEEN 100 AND 200',
        'Zone Map')
         THEN 'sh12_pruning_ok'
         ELSE 'sh12_pruning_FAIL'
    END AS sh12_5_result;

RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
RESET max_parallel_workers_per_gather;

-- SH12-6: Online merge on already-sorted table → early exit
SELECT sorted_heap_compact('sh12_merge'::regclass);
CALL sorted_heap_merge_online('sh12_merge'::regclass);
SELECT count(*) AS sh12_already_sorted_count FROM sh12_merge;

-- SH12-7: Online merge on empty table → no-op
CREATE TABLE sh12_empty(id int PRIMARY KEY) USING sorted_heap;
CALL sorted_heap_merge_online('sh12_empty'::regclass);
DROP TABLE sh12_empty;

-- SH12-8: Online merge on never-compacted table → full tuplesort fallback
CREATE TABLE sh12_unsorted(id int PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh12_unsorted
  SELECT g, 'row-' || g
  FROM generate_series(1, 1000) g;
CALL sorted_heap_merge_online('sh12_unsorted'::regclass);
SELECT count(*) AS sh12_unsorted_count FROM sh12_unsorted;
DROP TABLE sh12_unsorted;

DROP TABLE sh12_merge;

-- ============================================================
-- SH13: Zone map support for UUID and text types
-- ============================================================

-- SH13-1: UUID PK — zone map populated after compact
CREATE TABLE sh13_uuid(
    id uuid PRIMARY KEY,
    val int
) USING sorted_heap;

-- Insert 10K rows with UUIDs generated from integers
INSERT INTO sh13_uuid
  SELECT (lpad(to_hex(g), 8, '0') || '-0000-0000-0000-000000000000')::uuid, g
  FROM generate_series(1, 10000) g;

SELECT sorted_heap_compact('sh13_uuid'::regclass);
SELECT CASE WHEN sorted_heap_zonemap_stats('sh13_uuid'::regclass)
                 LIKE '%flags=valid%'
         THEN 'sh13_uuid_zm_valid'
         ELSE 'sh13_uuid_zm_FAIL'
    END AS sh13_1_result;

-- SH13-2: UUID scan pruning
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SET max_parallel_workers_per_gather = 0;

SELECT CASE
    WHEN sh6_plan_contains(
        'SELECT * FROM sh13_uuid WHERE id = ''00000064-0000-0000-0000-000000000000''::uuid',
        'Zone Map')
         THEN 'sh13_uuid_point_pruning_ok'
         ELSE 'sh13_uuid_point_pruning_FAIL'
    END AS sh13_2a_result;

SELECT CASE
    WHEN sh6_plan_contains(
        'SELECT * FROM sh13_uuid WHERE id >= ''00000001-0000-0000-0000-000000000000''::uuid AND id <= ''00000064-0000-0000-0000-000000000000''::uuid',
        'Zone Map')
         THEN 'sh13_uuid_range_pruning_ok'
         ELSE 'sh13_uuid_range_pruning_FAIL'
    END AS sh13_2b_result;

RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
RESET max_parallel_workers_per_gather;

-- SH13-3: TEXT PK with C collation — zone map works
CREATE TABLE sh13_text(
    id text COLLATE "C" PRIMARY KEY,
    val int
) USING sorted_heap;

INSERT INTO sh13_text
  SELECT lpad(g::text, 10, '0'), g
  FROM generate_series(1, 10000) g;

SELECT sorted_heap_compact('sh13_text'::regclass);
SELECT CASE WHEN sorted_heap_zonemap_stats('sh13_text'::regclass)
                 LIKE '%flags=valid%'
         THEN 'sh13_text_zm_valid'
         ELSE 'sh13_text_zm_FAIL'
    END AS sh13_3_result;

-- SH13-4: TEXT scan pruning with C collation
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SET max_parallel_workers_per_gather = 0;

SELECT CASE
    WHEN sh6_plan_contains(
        'SELECT * FROM sh13_text WHERE id = ''0000000100''',
        'Zone Map')
         THEN 'sh13_text_point_pruning_ok'
         ELSE 'sh13_text_point_pruning_FAIL'
    END AS sh13_4a_result;

SELECT CASE
    WHEN sh6_plan_contains(
        'SELECT * FROM sh13_text WHERE id >= ''0000000001'' AND id <= ''0000000100''',
        'Zone Map')
         THEN 'sh13_text_range_pruning_ok'
         ELSE 'sh13_text_range_pruning_FAIL'
    END AS sh13_4b_result;

RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;
RESET max_parallel_workers_per_gather;

-- SH13-5: Online compact on UUID PK → error
DO $$
BEGIN
    CALL sorted_heap_compact_online('sh13_uuid'::regclass);
    RAISE NOTICE 'sh13_online_compact_uuid_FAIL';
EXCEPTION WHEN feature_not_supported THEN
    RAISE NOTICE 'sh13_online_compact_uuid_blocked_ok';
END;
$$;

-- SH13-6: Online merge on UUID PK → error
DO $$
BEGIN
    CALL sorted_heap_merge_online('sh13_uuid'::regclass);
    RAISE NOTICE 'sh13_online_merge_uuid_FAIL';
EXCEPTION WHEN feature_not_supported THEN
    RAISE NOTICE 'sh13_online_merge_uuid_blocked_ok';
END;
$$;

-- SH13-7: VARCHAR PK with C collation — zone map works
CREATE TABLE sh13_varchar(
    id varchar(20) COLLATE "C" PRIMARY KEY,
    val int
) USING sorted_heap;

INSERT INTO sh13_varchar
  SELECT lpad(g::text, 10, '0'), g
  FROM generate_series(1, 1000) g;

SELECT sorted_heap_compact('sh13_varchar'::regclass);
SELECT CASE WHEN sorted_heap_zonemap_stats('sh13_varchar'::regclass)
                 LIKE '%flags=valid%'
         THEN 'sh13_varchar_zm_valid'
         ELSE 'sh13_varchar_zm_FAIL'
    END AS sh13_7_result;

DROP TABLE sh13_varchar;
DROP TABLE sh13_text;
DROP TABLE sh13_uuid;

-- ================================================================
-- SH14: Unlimited Zone Map Capacity (v6 linked overflow)
-- ================================================================

-- SH14-1: Create wide-row table exceeding old 32 overflow page limit
-- Each row ~430 bytes → ~18 rows/page → 170K rows ≈ 9,444 pages
-- Old limit was 8,410 pages (250 + 32*255). This exceeds it.
CREATE TABLE sh14_big(
    id int PRIMARY KEY,
    padding text
) USING sorted_heap;

INSERT INTO sh14_big
  SELECT g, repeat('x', 400)
  FROM generate_series(1, 170000) g;

SELECT sorted_heap_compact('sh14_big'::regclass);

-- SH14-2: Zone map stats should show full meta page + overflow with chain
SELECT CASE WHEN sorted_heap_zonemap_stats('sh14_big'::regclass)
                 LIKE '%nentries=250%flags=valid%total_overflow_pages=%'
         THEN 'sh14_overflow_chain_ok'
         ELSE 'sh14_overflow_chain_FAIL: ' ||
              sorted_heap_zonemap_stats('sh14_big'::regclass)
    END AS sh14_2_result;

-- SH14-3: Scan pruning on high page numbers (data in overflow range)
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_bitmapscan = off;

SELECT sh6_plan_contains(
    'SELECT id FROM sh14_big WHERE id = 169000',
    'SortedHeapScan') AS sh14_3_plan;

SELECT id FROM sh14_big WHERE id = 169000;

-- SH14-4: Range query in overflow zone
SELECT count(*) FROM sh14_big WHERE id BETWEEN 168000 AND 170000;

RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;

DROP TABLE sh14_big;

-- ================================================================
-- SH15: VACUUM Zone Map Rebuild
-- ================================================================

-- SH15-1: Create table, compact → zone map valid (flags=valid)
CREATE TABLE sh15_vac(
    id int PRIMARY KEY,
    val int
) USING sorted_heap;

INSERT INTO sh15_vac SELECT g, g FROM generate_series(1, 5000) g;
SELECT sorted_heap_compact('sh15_vac'::regclass);

SELECT CASE WHEN sorted_heap_zonemap_stats('sh15_vac'::regclass)
                 LIKE '%flags=valid%'
         THEN 'sh15_zm_valid_after_compact'
         ELSE 'sh15_zm_FAIL'
    END AS sh15_1_result;

-- SH15-2: Single-row INSERTs to overflow onto new page → invalidate zone map
-- After compact, zm_nentries covers all pages. We need INSERTs that spill
-- to a new page (zmidx >= zm_nentries).
DO $$
BEGIN
    FOR i IN 5001..5500 LOOP
        INSERT INTO sh15_vac VALUES (i, i);
    END LOOP;
END;
$$;

SELECT CASE WHEN sorted_heap_zonemap_stats('sh15_vac'::regclass)
                 NOT LIKE '%flags=valid%'
         THEN 'sh15_zm_invalidated_ok'
         ELSE 'sh15_zm_still_valid_FAIL'
    END AS sh15_2_result;

-- SH15-3: VACUUM should rebuild zone map (zone map becomes valid again)
VACUUM sh15_vac;

SELECT CASE WHEN sorted_heap_zonemap_stats('sh15_vac'::regclass)
                 LIKE '%flags=valid%'
         THEN 'sh15_vacuum_rebuilt_zm_ok'
         ELSE 'sh15_vacuum_rebuild_FAIL: ' ||
              sorted_heap_zonemap_stats('sh15_vac'::regclass)
    END AS sh15_3_result;

-- SH15-4: Scan pruning works after vacuum rebuild
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_bitmapscan = off;

SELECT sh6_plan_contains(
    'SELECT id FROM sh15_vac WHERE id = 100',
    'SortedHeapScan') AS sh15_4_plan;

SELECT id FROM sh15_vac WHERE id = 100;

RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_bitmapscan;

-- SH15-5: GUC off → VACUUM does NOT rebuild zone map
-- First invalidate again
DO $$
BEGIN
    FOR i IN 5501..6000 LOOP
        INSERT INTO sh15_vac VALUES (i, i);
    END LOOP;
END;
$$;

SET sorted_heap.vacuum_rebuild_zonemap = off;
VACUUM sh15_vac;

SELECT CASE WHEN sorted_heap_zonemap_stats('sh15_vac'::regclass)
                 NOT LIKE '%flags=valid%'
         THEN 'sh15_guc_off_no_rebuild_ok'
         ELSE 'sh15_guc_off_FAIL'
    END AS sh15_5_result;

-- SH15-6: Reset GUC, VACUUM → rebuilt
RESET sorted_heap.vacuum_rebuild_zonemap;
VACUUM sh15_vac;

SELECT CASE WHEN sorted_heap_zonemap_stats('sh15_vac'::regclass)
                 LIKE '%flags=valid%'
         THEN 'sh15_guc_on_rebuild_ok'
         ELSE 'sh15_guc_on_FAIL: ' ||
              sorted_heap_zonemap_stats('sh15_vac'::regclass)
    END AS sh15_6_result;

DROP TABLE sh15_vac;

-- ============================================================
-- SH16: Secondary index preservation across all rewrite paths
-- ============================================================

-- SH16-1: Create table with secondary indexes
CREATE TABLE sh16_idx(
    id int PRIMARY KEY,
    category int,
    name text
) USING sorted_heap;

CREATE INDEX sh16_idx_category ON sh16_idx(category);
CREATE INDEX sh16_idx_name ON sh16_idx(name);

INSERT INTO sh16_idx
  SELECT g, g % 100, 'name_' || lpad(g::text, 6, '0')
  FROM generate_series(1, 10000) g;

-- SH16-2: Verify secondary indexes work pre-compact
SET enable_seqscan = off;
SELECT count(*) AS sh16_pre_compact_cat
  FROM sh16_idx WHERE category = 42;
SELECT id AS sh16_pre_compact_name
  FROM sh16_idx WHERE name = 'name_005000';
RESET enable_seqscan;

-- SH16-3: Compact → secondary indexes rebuilt by cluster_rel
SELECT sorted_heap_compact('sh16_idx'::regclass);

SET enable_seqscan = off;
SELECT count(*) AS sh16_post_compact_cat
  FROM sh16_idx WHERE category = 42;
SELECT id AS sh16_post_compact_name
  FROM sh16_idx WHERE name = 'name_005000';
RESET enable_seqscan;

-- SH16-4: Merge → finish_heap_swap updates secondary indexes
INSERT INTO sh16_idx
  SELECT g, g % 100, 'name_' || lpad(g::text, 6, '0')
  FROM generate_series(10001, 15000) g;

SELECT sorted_heap_merge('sh16_idx'::regclass);

SET enable_seqscan = off;
SELECT count(*) AS sh16_post_merge_cat
  FROM sh16_idx WHERE category = 42;
SELECT id AS sh16_post_merge_name
  FROM sh16_idx WHERE name = 'name_012000';
RESET enable_seqscan;

-- SH16-5: Online compact → finish_heap_swap updates secondary indexes
INSERT INTO sh16_idx
  SELECT g, g % 100, 'name_' || lpad(g::text, 6, '0')
  FROM generate_series(15001, 20000) g;

CALL sorted_heap_compact_online('sh16_idx'::regclass);

SET enable_seqscan = off;
SELECT count(*) AS sh16_post_online_compact_cat
  FROM sh16_idx WHERE category = 42;
SELECT id AS sh16_post_online_compact_name
  FROM sh16_idx WHERE name = 'name_018000';
RESET enable_seqscan;

-- SH16-6: Online merge → finish_heap_swap updates secondary indexes
INSERT INTO sh16_idx
  SELECT g, g % 100, 'name_' || lpad(g::text, 6, '0')
  FROM generate_series(20001, 25000) g;

CALL sorted_heap_merge_online('sh16_idx'::regclass);

SET enable_seqscan = off;
SELECT count(*) AS sh16_post_online_merge_cat
  FROM sh16_idx WHERE category = 42;
SELECT id AS sh16_post_online_merge_name
  FROM sh16_idx WHERE name = 'name_022000';
RESET enable_seqscan;

-- SH16-7: DML with secondary indexes
UPDATE sh16_idx SET category = 999 WHERE id = 1;
DELETE FROM sh16_idx WHERE id = 2;

SET enable_seqscan = off;
SELECT count(*) AS sh16_dml_cat999
  FROM sh16_idx WHERE category = 999;
SELECT count(*) AS sh16_dml_total
  FROM sh16_idx;
RESET enable_seqscan;

DROP TABLE sh16_idx;

-- SH16-8: UNIQUE constraint on secondary index
CREATE TABLE sh16_uniq(
    id int PRIMARY KEY,
    email text,
    code int
) USING sorted_heap;

CREATE UNIQUE INDEX sh16_uniq_email ON sh16_uniq(email);
CREATE UNIQUE INDEX sh16_uniq_code  ON sh16_uniq(code);

INSERT INTO sh16_uniq
  SELECT g, 'user' || g || '@test.com', g * 10
  FROM generate_series(1, 5000) g;

-- Duplicate email must fail
\set ON_ERROR_STOP off
INSERT INTO sh16_uniq VALUES (9999, 'user1@test.com', 99990);
\set ON_ERROR_STOP on

-- Duplicate code must fail
\set ON_ERROR_STOP off
INSERT INTO sh16_uniq VALUES (9998, 'unique@test.com', 10);
\set ON_ERROR_STOP on

-- SH16-9: UNIQUE constraint survives compact
SELECT sorted_heap_compact('sh16_uniq'::regclass);

SET enable_seqscan = off;
SELECT id AS sh16_uniq_lookup
  FROM sh16_uniq WHERE email = 'user2500@test.com';
RESET enable_seqscan;

\set ON_ERROR_STOP off
INSERT INTO sh16_uniq VALUES (9997, 'user1@test.com', 99970);
\set ON_ERROR_STOP on

-- SH16-10: UNIQUE constraint survives merge
INSERT INTO sh16_uniq
  SELECT g, 'user' || g || '@test.com', g * 10
  FROM generate_series(5001, 8000) g;

SELECT sorted_heap_merge('sh16_uniq'::regclass);

\set ON_ERROR_STOP off
INSERT INTO sh16_uniq VALUES (9996, 'user6000@test.com', 99960);
\set ON_ERROR_STOP on

-- SH16-11: UNIQUE constraint survives online compact
INSERT INTO sh16_uniq
  SELECT g, 'user' || g || '@test.com', g * 10
  FROM generate_series(8001, 10000) g;

CALL sorted_heap_compact_online('sh16_uniq'::regclass);

\set ON_ERROR_STOP off
INSERT INTO sh16_uniq VALUES (9995, 'user9000@test.com', 99950);
\set ON_ERROR_STOP on

-- SH16-12: UNIQUE constraint survives online merge
INSERT INTO sh16_uniq
  SELECT g, 'user' || g || '@test.com', g * 10
  FROM generate_series(10001, 12000) g;

CALL sorted_heap_merge_online('sh16_uniq'::regclass);

\set ON_ERROR_STOP off
INSERT INTO sh16_uniq VALUES (9994, 'user11000@test.com', 99940);
\set ON_ERROR_STOP on

-- Final count: only the original rows, no duplicates snuck in
SELECT count(*) AS sh16_uniq_final FROM sh16_uniq;

DROP TABLE sh16_uniq;

-- ================================================================
-- SH17: Runtime Parameter Resolution (Prepared Statements)
-- Tests that sorted_heap can generate generic plans with Param nodes.
-- ================================================================

CREATE TABLE sh17(id int PRIMARY KEY, val text) USING sorted_heap;
INSERT INTO sh17 SELECT g, 'row-'||g FROM generate_series(1,10000) g;
SELECT sorted_heap_compact('sh17'::regclass);
ANALYZE sh17;

SET enable_indexscan = off;
SET enable_bitmapscan = off;

-- SH17-1: Point query with $1 under forced generic plan
PREPARE sh17_point(int) AS SELECT count(*) FROM sh17 WHERE id = $1;
SET plan_cache_mode = force_generic_plan;
EXECUTE sh17_point(500);
EXPLAIN (COSTS OFF) EXECUTE sh17_point(500);

-- SH17-2: Range query with $1, $2
PREPARE sh17_range(int,int) AS
  SELECT count(*) FROM sh17 WHERE id BETWEEN $1 AND $2;
EXECUTE sh17_range(100, 200);
EXPLAIN (COSTS OFF) EXECUTE sh17_range(100, 200);

-- SH17-3: Mixed Const + Param
PREPARE sh17_mixed(int) AS
  SELECT count(*) FROM sh17 WHERE id > 50 AND id < $1;
EXECUTE sh17_mixed(200);

-- SH17-4: Correctness cross-check (non-prepared must match)
SELECT count(*) AS sh17_point_check FROM sh17 WHERE id = 500;
SELECT count(*) AS sh17_range_check FROM sh17 WHERE id BETWEEN 100 AND 200;
SELECT count(*) AS sh17_mixed_check FROM sh17 WHERE id > 50 AND id < 200;

RESET plan_cache_mode;
RESET enable_indexscan;
RESET enable_bitmapscan;
DEALLOCATE ALL;
DROP TABLE sh17;

DROP FUNCTION sh6_plan_contains(text, text);

-- ================================================================
-- SH18: hsvec (half-precision float16 vector) type
-- Tests I/O, typmod, cosine distance, casts, precision, edge cases.
-- ================================================================

-- SH18-1: Text I/O roundtrip
SELECT '[1.5,2.0,3.0]'::hsvec;
SELECT '[0,0,0,0]'::hsvec;
SELECT '[-1.0, 0.5, 1.0]'::hsvec;

-- SH18-2: Typmod parsing
SELECT '[1,2,3]'::hsvec(3);

-- SH18-3: Dimension limits — 1 dim ok, 32001 rejected
SELECT '[42]'::hsvec AS dim_1;
\set ON_ERROR_STOP off
SELECT ('[' || array_to_string(array_fill(1.0::float, ARRAY[32001]), ',') || ']')::hsvec;
\set ON_ERROR_STOP on

-- SH18-4: Parse errors
\set ON_ERROR_STOP off
SELECT '1,2,3'::hsvec;
SELECT '[1,2,3'::hsvec;
SELECT '[]'::hsvec;
\set ON_ERROR_STOP on

-- SH18-5: Cosine distance — identical vectors = 0
SELECT ('[1,0,0]'::hsvec <=> '[1,0,0]'::hsvec) AS dist_identical;

-- SH18-6: Cosine distance — orthogonal vectors = 1
SELECT ('[1,0,0]'::hsvec <=> '[0,1,0]'::hsvec) AS dist_orthogonal;

-- SH18-7: Cosine distance — opposite vectors = 2
SELECT ('[1,0,0]'::hsvec <=> '[-1,0,0]'::hsvec) AS dist_opposite;

-- SH18-8: Cosine distance — zero vector = NaN
SELECT ('[1,0,0]'::hsvec <=> '[0,0,0]'::hsvec) AS dist_zero;

-- SH18-9: Cosine distance — dimension mismatch
\set ON_ERROR_STOP off
SELECT '[1,2]'::hsvec <=> '[1,2,3]'::hsvec;
\set ON_ERROR_STOP on

-- SH18-10: Cast hsvec → svec (implicit, lossless)
SELECT pg_typeof('[1,2,3]'::hsvec::svec) AS cast_type;
SELECT '[1,2,3]'::hsvec::svec;

-- SH18-11: Cast svec → hsvec (assignment, lossy)
SELECT pg_typeof('[1,2,3]'::svec::hsvec) AS cast_type;
SELECT '[1,2,3]'::svec::hsvec;

-- SH18-12: Precision — fp16 roundtrip preserves typical values
SELECT '[0.5,1.0,2.0,0.25,0.125]'::hsvec;

-- SH18-13: hsvec cosine matches svec cosine within fp16 precision
SELECT
    abs(('[0.3,0.7,0.1]'::hsvec <=> '[0.9,0.2,0.5]'::hsvec)
      - ('[0.3,0.7,0.1]'::svec  <=> '[0.9,0.2,0.5]'::svec)) < 0.01 AS dist_close;

-- SH18-14: hsvec in table column
CREATE TABLE sh18_vec(id serial, v hsvec(3));
INSERT INTO sh18_vec(v) VALUES ('[1,0,0]'), ('[0,1,0]'), ('[0,0,1]');
SELECT id, v FROM sh18_vec ORDER BY id;
SELECT id, v <=> '[1,0,0]'::hsvec AS dist FROM sh18_vec ORDER BY dist;
DROP TABLE sh18_vec;

-- SH18-15: Implicit cast allows hsvec in svec functions
SELECT svec_cosine_distance('[1,0,0]'::hsvec, '[0,1,0]'::hsvec) AS implicit_cast_dist;

DROP EXTENSION pg_sorted_heap;
