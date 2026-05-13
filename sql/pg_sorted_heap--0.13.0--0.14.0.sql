CREATE FUNCTION @extschema@.sorted_heap_zonemap_may_match_int8(target regclass, lower_bound bigint, upper_bound bigint)
RETURNS boolean
AS '$libdir/pg_sorted_heap', 'sorted_heap_zonemap_may_match_int8'
LANGUAGE C STRICT;

COMMENT ON FUNCTION @extschema@.sorted_heap_zonemap_may_match_int8(regclass, bigint, bigint)
IS 'Metadata-only first-key zone-map overlap probe. Returns false only when valid zone-map metadata proves an int8 range cannot match; returns true on stale or unsupported metadata so callers fail open to normal heap execution.';

CREATE TABLE @extschema@.sorted_heap_append_run_registry (
  run_id bigserial PRIMARY KEY,
  relid regclass NOT NULL,
  relfilenode oid NOT NULL,
  block_start bigint NOT NULL,
  block_end bigint NOT NULL,
  row_count bigint NOT NULL CHECK (row_count > 0),
  rows_per_block numeric NOT NULL CHECK (rows_per_block > 0),
  first_row_json jsonb NOT NULL,
  last_row_json jsonb NOT NULL,
  key_columns name[],
  first_key bigint[],
  last_key bigint[],
  ordering_mode text NOT NULL CHECK (ordering_mode IN ('full-order')),
  order_by text NOT NULL,
  source_sql_hash text NOT NULL,
  valid boolean NOT NULL DEFAULT true,
  created_xid text NOT NULL DEFAULT pg_current_xact_id()::text,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (key_columns IS NULL AND first_key IS NULL AND last_key IS NULL)
    OR (
      cardinality(key_columns) BETWEEN 1 AND 2
      AND cardinality(first_key) = cardinality(key_columns)
      AND cardinality(last_key) = cardinality(key_columns)
    )
  )
);

COMMENT ON TABLE @extschema@.sorted_heap_append_run_registry
IS 'Observational append-run witness registry for trusted ordered bulk loads. Rows are not consumed by merge/compaction yet and become stale when relfilenode changes.';

CREATE FUNCTION @extschema@.sorted_heap_bulk_load_ordered(
  target regclass,
  source_sql text,
  order_by text,
  analyze_after boolean DEFAULT false,
  key_columns name[] DEFAULT NULL
)
RETURNS bigint
AS $$
DECLARE
  target_relkind "char";
  target_am name;
  target_relfilenode oid;
  inserted_rows bigint;
  block_start bigint;
  block_end bigint;
  rows_per_block numeric;
  first_row_json jsonb;
  last_row_json jsonb;
  key_column name;
  first_key bigint[];
  last_key bigint[];
BEGIN
  IF source_sql IS NULL OR btrim(source_sql) = '' THEN
    RAISE EXCEPTION 'source_sql must not be empty';
  END IF;
  IF order_by IS NULL OR btrim(order_by) = '' THEN
    RAISE EXCEPTION 'order_by must not be empty';
  END IF;
  IF position(';' in source_sql) > 0 OR position(';' in order_by) > 0 THEN
    RAISE EXCEPTION 'source_sql and order_by must be single SQL fragments without semicolons';
  END IF;
  IF key_columns IS NOT NULL AND cardinality(key_columns) NOT BETWEEN 1 AND 2 THEN
    RAISE EXCEPTION 'key_columns must contain one or two column names';
  END IF;

  SELECT c.relkind, am.amname, pg_relation_filenode(c.oid::regclass)
  INTO target_relkind, target_am, target_relfilenode
  FROM pg_class c
  JOIN pg_am am ON am.oid = c.relam
  WHERE c.oid = target;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'target relation % does not exist', target;
  END IF;
  IF target_relkind <> 'r' THEN
    RAISE EXCEPTION 'target relation % must be a concrete table', target;
  END IF;
  IF target_am NOT IN ('sorted_heap', 'clustered_heap') THEN
    RAISE EXCEPTION 'target relation % must use sorted_heap or clustered_heap, found %',
      target, target_am;
  END IF;

  EXECUTE format(
    'WITH sorted_heap_bulk_src AS MATERIALIZED (
       SELECT *
       FROM (%s) AS sorted_heap_bulk_input
     ),
     inserted AS (
       INSERT INTO %s
       SELECT * FROM sorted_heap_bulk_src
       ORDER BY %s
       RETURNING ctid
     ),
     bounds AS (
       SELECT ordered_rows[1] AS first_row_json,
              ordered_rows[array_length(ordered_rows, 1)] AS last_row_json
       FROM (
         SELECT array_agg(to_jsonb(sorted_heap_bulk_src) ORDER BY %s) AS ordered_rows
         FROM sorted_heap_bulk_src
       ) ordered
     )
     SELECT count(*)::bigint,
            min((ctid::text::point)[0]::bigint),
            max((ctid::text::point)[0]::bigint),
            CASE
              WHEN count(*) > 0 THEN
                count(*)::numeric / (max((ctid::text::point)[0]::bigint) - min((ctid::text::point)[0]::bigint) + 1)
              ELSE NULL
            END,
            (SELECT first_row_json FROM bounds),
            (SELECT last_row_json FROM bounds)
     FROM inserted',
    source_sql,
    target,
    order_by,
    order_by
  ) INTO inserted_rows, block_start, block_end, rows_per_block, first_row_json, last_row_json;

  IF inserted_rows > 0 THEN
    IF key_columns IS NOT NULL THEN
      first_key := ARRAY[]::bigint[];
      last_key := ARRAY[]::bigint[];

      FOREACH key_column IN ARRAY key_columns LOOP
        IF NOT (first_row_json ? key_column) OR NOT (last_row_json ? key_column) THEN
          RAISE EXCEPTION 'key column % is absent from source row JSON; alias source_sql columns to match key_columns', key_column;
        END IF;

        first_key := array_append(first_key, (first_row_json ->> key_column)::bigint);
        last_key := array_append(last_key, (last_row_json ->> key_column)::bigint);
      END LOOP;
    END IF;

    INSERT INTO @extschema@.sorted_heap_append_run_registry (
      relid,
      relfilenode,
      block_start,
      block_end,
      row_count,
      rows_per_block,
      first_row_json,
      last_row_json,
      key_columns,
      first_key,
      last_key,
      ordering_mode,
      order_by,
      source_sql_hash
    )
    VALUES (
      target,
      target_relfilenode,
      block_start,
      block_end,
      inserted_rows,
      rows_per_block,
      first_row_json,
      last_row_json,
      key_columns,
      first_key,
      last_key,
      'full-order',
      order_by,
      md5(source_sql)
    );
  END IF;

  IF analyze_after THEN
    EXECUTE format('ANALYZE %s', target);
  END IF;

  RETURN inserted_rows;
END
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION @extschema@.sorted_heap_bulk_load_ordered(regclass, text, text, boolean, name[])
IS 'Explicit trusted-operator bulk load helper: inserts rows from source_sql into a sorted_heap/clustered_heap table after ordering by order_by and records an observational append-run witness.';

CREATE FUNCTION @extschema@.sorted_heap_append_run_status(target regclass DEFAULT NULL)
RETURNS TABLE (
  run_id bigint,
  relid regclass,
  relfilenode oid,
  current_relfilenode oid,
  block_start bigint,
  block_end bigint,
  row_count bigint,
  rows_per_block numeric,
  first_row_json jsonb,
  last_row_json jsonb,
  key_columns name[],
  first_key bigint[],
  last_key bigint[],
  ordering_mode text,
  order_by text,
  source_sql_hash text,
  valid boolean,
  is_current boolean,
  created_at timestamptz
)
LANGUAGE SQL STABLE AS
$$
  SELECT
    r.run_id,
    r.relid,
    r.relfilenode,
    f.current_relfilenode,
    r.block_start,
    r.block_end,
    r.row_count,
    r.rows_per_block,
    r.first_row_json,
    r.last_row_json,
    r.key_columns,
    r.first_key,
    r.last_key,
    r.ordering_mode,
    r.order_by,
    r.source_sql_hash,
    r.valid,
    r.valid AND f.current_relfilenode IS NOT NULL AND r.relfilenode = f.current_relfilenode AS is_current,
    r.created_at
  FROM @extschema@.sorted_heap_append_run_registry r
  LEFT JOIN pg_class c ON c.oid = r.relid::oid
  LEFT JOIN LATERAL (
    SELECT pg_relation_filenode(c.oid::regclass) AS current_relfilenode
    WHERE c.oid IS NOT NULL
  ) f ON true
  WHERE target IS NULL OR r.relid = target
  ORDER BY r.run_id
$$;

COMMENT ON FUNCTION @extschema@.sorted_heap_append_run_status(regclass)
IS 'Lists ordered bulk-load append-run witnesses and marks whether each witness still matches the current relation filenode. Stale rows must not be used as merge input.';

CREATE FUNCTION @extschema@.sorted_heap_append_run_invalidate(target regclass DEFAULT NULL)
RETURNS bigint
LANGUAGE plpgsql AS
$$
DECLARE
  affected bigint;
BEGIN
  UPDATE @extschema@.sorted_heap_append_run_registry r
  SET valid = false
  WHERE r.valid
    AND (target IS NULL OR r.relid = target);
  GET DIAGNOSTICS affected = ROW_COUNT;
  RETURN affected;
END
$$;

COMMENT ON FUNCTION @extschema@.sorted_heap_append_run_invalidate(regclass)
IS 'Marks ordered bulk-load append-run witnesses invalid. This is a fail-closed operational helper; it does not rewrite table data.';

CREATE FUNCTION @extschema@.sorted_heap_append_run_cleanup(target regclass DEFAULT NULL)
RETURNS bigint
LANGUAGE plpgsql AS
$$
DECLARE
  affected bigint;
BEGIN
  DELETE FROM @extschema@.sorted_heap_append_run_registry r
  WHERE (target IS NULL OR r.relid = target)
    AND (
      NOT r.valid
      OR NOT EXISTS (
        SELECT 1
        FROM pg_class c
        WHERE c.oid = r.relid::oid
      )
      OR EXISTS (
        SELECT 1
        FROM pg_class c
        WHERE c.oid = r.relid::oid
          AND pg_relation_filenode(c.oid::regclass) IS DISTINCT FROM r.relfilenode
      )
    );
  GET DIAGNOSTICS affected = ROW_COUNT;
  RETURN affected;
END
$$;

COMMENT ON FUNCTION @extschema@.sorted_heap_append_run_cleanup(regclass)
IS 'Deletes invalid, stale, or relation-orphaned ordered bulk-load append-run witnesses. Current releases do not consume witnesses for merge.';

CREATE FUNCTION @extschema@.sorted_heap_append_run_plan(target regclass)
RETURNS TABLE (
  relid regclass,
  run_count bigint,
  current_run_count bigint,
  stale_run_count bigint,
  total_rows bigint,
  current_rows bigint,
  block_start bigint,
  block_end bigint,
  avg_rows_per_block numeric,
  can_merge boolean,
  reason text
)
LANGUAGE SQL STABLE AS
$$
  WITH s AS (
    SELECT *
    FROM @extschema@.sorted_heap_append_run_status(target)
  )
  SELECT
    target AS relid,
    count(*)::bigint AS run_count,
    count(*) FILTER (WHERE is_current)::bigint AS current_run_count,
    count(*) FILTER (WHERE NOT is_current)::bigint AS stale_run_count,
    COALESCE(sum(row_count), 0)::bigint AS total_rows,
    COALESCE(sum(row_count) FILTER (WHERE is_current), 0)::bigint AS current_rows,
    min(block_start) FILTER (WHERE is_current) AS block_start,
    max(block_end) FILTER (WHERE is_current) AS block_end,
    avg(rows_per_block) FILTER (WHERE is_current) AS avg_rows_per_block,
    false AS can_merge,
    CASE
      WHEN count(*) = 0 THEN 'no_append_run_witnesses'
      WHEN count(*) FILTER (WHERE NOT is_current) > 0 THEN 'stale_or_invalid_witnesses_present'
      ELSE 'observational_only_not_merge_input'
    END AS reason
  FROM s
$$;

COMMENT ON FUNCTION @extschema@.sorted_heap_append_run_plan(regclass)
IS 'Dry-run summary for ordered bulk-load append-run witnesses. can_merge is intentionally false until crash-safe proof-bearing merge consumption is implemented.';
