CREATE FUNCTION @extschema@.sorted_heap_partition_status(parent regclass)
RETURNS TABLE (
  parent_relid regclass,
  leaf_relid regclass,
  leaf_name text,
  relkind "char",
  am_name name,
  is_sorted_heap boolean,
  has_primary_key boolean,
  zone_map_valid boolean,
  zone_map_sorted boolean,
  sorted_prefix_pages integer,
  zone_map_entries integer,
  overflow_pages integer,
  relation_size_bytes bigint
)
AS $$
DECLARE
  parent_kind "char";
  parent_am name;
  rec record;
  stats text;
  flags text;
  m text[];
BEGIN
  SELECT c.relkind, a.amname
  INTO parent_kind, parent_am
  FROM pg_catalog.pg_class c
  LEFT JOIN pg_catalog.pg_am a ON a.oid = c.relam
  WHERE c.oid = parent;

  IF parent_kind IS NULL THEN
    RAISE EXCEPTION 'relation % does not exist', parent;
  END IF;

  IF parent_kind <> 'p' AND NOT (parent_kind = 'r' AND parent_am = 'sorted_heap') THEN
    RAISE EXCEPTION '% is neither a partitioned table nor a sorted_heap table', parent;
  END IF;

  FOR rec IN EXECUTE
    CASE
      WHEN parent_kind = 'p' THEN
        'SELECT p.relid::regclass AS leaf_relid,
                p.relid::text AS leaf_name,
                c.relkind,
                a.amname,
                EXISTS (
                  SELECT 1
                  FROM pg_catalog.pg_index i
                  WHERE i.indrelid = p.relid
                    AND i.indisprimary
                ) AS has_primary_key,
                CASE
                  WHEN c.relkind IN (''r'', ''t'', ''m'') THEN
                    pg_catalog.pg_total_relation_size(p.relid)
                  ELSE NULL::bigint
                END AS relation_size_bytes
         FROM pg_catalog.pg_partition_tree($1) AS p
         JOIN pg_catalog.pg_class c ON c.oid = p.relid
         LEFT JOIN pg_catalog.pg_am a ON a.oid = c.relam
         WHERE p.isleaf
         ORDER BY p.relid::text'
      ELSE
        'SELECT c.oid::regclass AS leaf_relid,
                c.oid::text AS leaf_name,
                c.relkind,
                a.amname,
                EXISTS (
                  SELECT 1
                  FROM pg_catalog.pg_index i
                  WHERE i.indrelid = c.oid
                    AND i.indisprimary
                ) AS has_primary_key,
                pg_catalog.pg_total_relation_size(c.oid) AS relation_size_bytes
         FROM pg_catalog.pg_class c
         LEFT JOIN pg_catalog.pg_am a ON a.oid = c.relam
         WHERE c.oid = $1'
    END
    USING parent
  LOOP
    parent_relid := parent;
    leaf_relid := rec.leaf_relid;
    leaf_name := rec.leaf_name;
    relkind := rec.relkind;
    am_name := rec.amname;
    is_sorted_heap := COALESCE(rec.amname = 'sorted_heap', false);
    has_primary_key := rec.has_primary_key;
    zone_map_valid := NULL;
    zone_map_sorted := NULL;
    sorted_prefix_pages := NULL;
    zone_map_entries := NULL;
    overflow_pages := NULL;
    relation_size_bytes := rec.relation_size_bytes;

    IF is_sorted_heap THEN
      stats := @extschema@.sorted_heap_zonemap_stats(rec.leaf_relid);

      m := pg_catalog.regexp_match(stats, 'flags=([^ ]+)');
      flags := CASE WHEN m IS NULL THEN NULL ELSE m[1] END;
      zone_map_valid := flags = 'valid' OR flags = 'valid,sorted';
      zone_map_sorted := flags = 'sorted' OR flags = 'valid,sorted';

      m := pg_catalog.regexp_match(stats, 'nentries=([0-9]+)');
      zone_map_entries := CASE WHEN m IS NULL THEN NULL ELSE m[1]::integer END;

      m := pg_catalog.regexp_match(stats, 'overflow_pages=([0-9]+)');
      overflow_pages := CASE WHEN m IS NULL THEN NULL ELSE m[1]::integer END;

      m := pg_catalog.regexp_match(stats, 'sorted_prefix=([0-9]+)');
      sorted_prefix_pages := CASE WHEN m IS NULL THEN NULL ELSE m[1]::integer END;
    END IF;

    RETURN NEXT;
  END LOOP;
END;
$$ LANGUAGE plpgsql STABLE;

CREATE FUNCTION @extschema@.sorted_heap_partition_index_status(parent regclass)
RETURNS TABLE (
  parent_relid regclass,
  leaf_relid regclass,
  leaf_name text,
  index_relid regclass,
  index_name text,
  index_am name,
  is_valid boolean,
  is_ready boolean,
  is_live boolean,
  is_primary boolean,
  is_unique boolean,
  is_sorted_hnsw boolean,
  is_btree boolean
)
AS $$
  SELECT
    s.parent_relid,
    s.leaf_relid,
    s.leaf_name,
    i.indexrelid::regclass AS index_relid,
    i.indexrelid::text AS index_name,
    am.amname AS index_am,
    i.indisvalid AS is_valid,
    i.indisready AS is_ready,
    i.indislive AS is_live,
    i.indisprimary AS is_primary,
    i.indisunique AS is_unique,
    COALESCE(am.amname = 'sorted_hnsw', false) AS is_sorted_hnsw,
    COALESCE(am.amname = 'btree', false) AS is_btree
  FROM @extschema@.sorted_heap_partition_status(parent) AS s
  LEFT JOIN pg_catalog.pg_index i ON i.indrelid = s.leaf_relid
  LEFT JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
  LEFT JOIN pg_catalog.pg_am am ON am.oid = ic.relam
  ORDER BY s.leaf_name, i.indexrelid::text NULLS LAST;
$$ LANGUAGE sql STABLE;

CREATE FUNCTION @extschema@.sorted_heap_scan_stats_by_relation()
RETURNS TABLE (
  relid regclass,
  relname text,
  total_scans bigint,
  blocks_scanned bigint,
  blocks_pruned bigint,
  source text
)
AS '$libdir/pg_sorted_heap', 'sorted_heap_scan_stats_by_relation'
LANGUAGE C STRICT;

CREATE FUNCTION @extschema@.sorted_heap_partition_scan_stats(parent regclass)
RETURNS TABLE (
  parent_relid regclass,
  leaf_relid regclass,
  leaf_name text,
  total_scans bigint,
  blocks_scanned bigint,
  blocks_pruned bigint,
  source text
)
AS $$
  SELECT
    s.parent_relid,
    s.leaf_relid,
    s.leaf_name,
    COALESCE(r.total_scans, 0)::bigint AS total_scans,
    COALESCE(r.blocks_scanned, 0)::bigint AS blocks_scanned,
    COALESCE(r.blocks_pruned, 0)::bigint AS blocks_pruned,
    'local'::text AS source
  FROM @extschema@.sorted_heap_partition_status(parent) AS s
  LEFT JOIN @extschema@.sorted_heap_scan_stats_by_relation() AS r
    ON r.relid = s.leaf_relid
  WHERE s.is_sorted_heap IS TRUE
  ORDER BY s.leaf_name;
$$ LANGUAGE sql STABLE;

CREATE FUNCTION @extschema@.sorted_heap_restore_plan(parent regclass DEFAULT NULL)
RETURNS TABLE (
  parent_relid regclass,
  leaf_relid regclass,
  leaf_name text,
  zone_map_valid boolean,
  zone_map_sorted boolean,
  sorted_prefix_pages integer,
  sorted_hnsw_indexes bigint,
  needs_compact boolean,
  post_restore_hnsw_rebuild_recommended boolean,
  recommended_action text
)
AS $$
  WITH roots AS (
    SELECT c.oid::regclass AS root_relid
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_am a ON a.oid = c.relam
    WHERE $1 IS NULL
      AND c.relkind = 'r'
      AND a.amname = 'sorted_heap'
    UNION ALL
    SELECT $1
    WHERE $1 IS NOT NULL
  ),
  leaves AS (
    SELECT DISTINCT ON (s.leaf_relid)
      s.parent_relid,
      s.leaf_relid,
      s.leaf_name,
      s.zone_map_valid,
      s.zone_map_sorted,
      s.sorted_prefix_pages
    FROM roots r
    CROSS JOIN LATERAL @extschema@.sorted_heap_partition_status(r.root_relid) AS s
    WHERE s.is_sorted_heap IS TRUE
    ORDER BY s.leaf_relid, s.leaf_name
  ),
  hnsw AS (
    SELECT i.indrelid::regclass AS leaf_relid,
           count(*)::bigint AS sorted_hnsw_indexes
    FROM pg_catalog.pg_index i
    JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
    JOIN pg_catalog.pg_am am ON am.oid = ic.relam
    WHERE am.amname = 'sorted_hnsw'
    GROUP BY i.indrelid
  )
  SELECT
    l.parent_relid,
    l.leaf_relid,
    l.leaf_name,
    l.zone_map_valid,
    l.zone_map_sorted,
    l.sorted_prefix_pages,
    COALESCE(h.sorted_hnsw_indexes, 0)::bigint AS sorted_hnsw_indexes,
    (l.zone_map_valid IS NOT TRUE) AS needs_compact,
    (COALESCE(h.sorted_hnsw_indexes, 0) > 0) AS post_restore_hnsw_rebuild_recommended,
    CASE
      WHEN l.zone_map_valid IS NOT TRUE AND COALESCE(h.sorted_hnsw_indexes, 0) > 0
        THEN 'compact_or_merge_and_rebuild_sorted_hnsw'
      WHEN l.zone_map_valid IS NOT TRUE
        THEN 'compact_or_merge'
      WHEN COALESCE(h.sorted_hnsw_indexes, 0) > 0
        THEN 'rebuild_sorted_hnsw_after_pg_restore'
      ELSE 'none'
    END AS recommended_action
  FROM leaves l
  LEFT JOIN hnsw h ON h.leaf_relid = l.leaf_relid
  ORDER BY l.leaf_name;
$$ LANGUAGE sql STABLE;

CREATE FUNCTION @extschema@.sorted_heap_partition_maintenance(
  parent regclass,
  operation text,
  fail_on_unsupported boolean DEFAULT true
) RETURNS TABLE (
  parent_relid regclass,
  leaf_relid regclass,
  leaf_name text,
  operation_name text,
  status text,
  message text,
  elapsed_ms double precision
)
AS $$
DECLARE
  rec record;
  t0 timestamptz;
BEGIN
  IF operation NOT IN ('compact', 'merge', 'rebuild_zonemap') THEN
    RAISE EXCEPTION 'unsupported sorted_heap partition maintenance operation: %', operation;
  END IF;

  IF fail_on_unsupported THEN
    SELECT s.*
    INTO rec
    FROM @extschema@.sorted_heap_partition_status(parent) AS s
    WHERE s.is_sorted_heap IS NOT TRUE
       OR s.has_primary_key IS NOT TRUE
    LIMIT 1;

    IF FOUND THEN
      IF rec.is_sorted_heap IS NOT TRUE THEN
        RAISE EXCEPTION 'unsupported partition leaf %: access method is %',
          rec.leaf_relid,
          COALESCE(rec.am_name::text, rec.relkind::text);
      ELSE
        RAISE EXCEPTION 'unsupported sorted_heap partition leaf %: primary key is required',
          rec.leaf_relid;
      END IF;
    END IF;
  END IF;

  FOR rec IN
    SELECT s.*
    FROM @extschema@.sorted_heap_partition_status(parent) AS s
    ORDER BY s.leaf_name
  LOOP
    parent_relid := rec.parent_relid;
    leaf_relid := rec.leaf_relid;
    leaf_name := rec.leaf_name;
    operation_name := operation;
    elapsed_ms := 0;

    IF rec.is_sorted_heap IS NOT TRUE THEN
      status := 'skipped';
      message := 'unsupported access method: ' || COALESCE(rec.am_name::text, rec.relkind::text);
      RETURN NEXT;
      CONTINUE;
    END IF;

    IF rec.has_primary_key IS NOT TRUE THEN
      status := 'skipped';
      message := 'primary key is required';
      RETURN NEXT;
      CONTINUE;
    END IF;

    t0 := pg_catalog.clock_timestamp();
    IF operation = 'compact' THEN
      PERFORM @extschema@.sorted_heap_compact(rec.leaf_relid);
    ELSIF operation = 'merge' THEN
      PERFORM @extschema@.sorted_heap_merge(rec.leaf_relid);
    ELSE
      PERFORM @extschema@.sorted_heap_rebuild_zonemap(rec.leaf_relid);
    END IF;

    elapsed_ms := 1000.0 * EXTRACT(EPOCH FROM (pg_catalog.clock_timestamp() - t0));
    status := 'ok';
    message := NULL;
    RETURN NEXT;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION @extschema@.sorted_heap_compact_partitions(
  parent regclass,
  fail_on_unsupported boolean DEFAULT true
) RETURNS TABLE (
  parent_relid regclass,
  leaf_relid regclass,
  leaf_name text,
  operation_name text,
  status text,
  message text,
  elapsed_ms double precision
)
AS $$
  SELECT *
  FROM @extschema@.sorted_heap_partition_maintenance($1, 'compact', $2);
$$ LANGUAGE SQL;

CREATE FUNCTION @extschema@.sorted_heap_merge_partitions(
  parent regclass,
  fail_on_unsupported boolean DEFAULT true
) RETURNS TABLE (
  parent_relid regclass,
  leaf_relid regclass,
  leaf_name text,
  operation_name text,
  status text,
  message text,
  elapsed_ms double precision
)
AS $$
  SELECT *
  FROM @extschema@.sorted_heap_partition_maintenance($1, 'merge', $2);
$$ LANGUAGE SQL;

CREATE FUNCTION @extschema@.sorted_heap_rebuild_zonemap_partitions(
  parent regclass,
  fail_on_unsupported boolean DEFAULT true
) RETURNS TABLE (
  parent_relid regclass,
  leaf_relid regclass,
  leaf_name text,
  operation_name text,
  status text,
  message text,
  elapsed_ms double precision
)
AS $$
  SELECT *
  FROM @extschema@.sorted_heap_partition_maintenance($1, 'rebuild_zonemap', $2);
$$ LANGUAGE SQL;

CREATE FUNCTION @extschema@.sorted_heap_partition_maintenance_plan(
  parent regclass,
  operation text DEFAULT 'compact'
) RETURNS TABLE (
  parent_relid regclass,
  leaf_relid regclass,
  leaf_name text,
  operation_name text,
  status text,
  message text,
  lock_mode text,
  tablespace_oid oid,
  tablespace_name name,
  tablespace_location text,
  relation_size_bytes bigint,
  estimated_temp_bytes bigint
)
AS $$
DECLARE
  rec record;
BEGIN
  IF operation NOT IN ('compact', 'merge', 'rebuild_zonemap') THEN
    RAISE EXCEPTION 'unsupported sorted_heap partition maintenance operation: %', operation;
  END IF;

  FOR rec IN
    SELECT s.*
    FROM @extschema@.sorted_heap_partition_status(parent) AS s
    ORDER BY s.leaf_name
  LOOP
    parent_relid := rec.parent_relid;
    leaf_relid := rec.leaf_relid;
    leaf_name := rec.leaf_name;
    operation_name := operation;
    relation_size_bytes := rec.relation_size_bytes;
    lock_mode := 'AccessExclusiveLock';
    SELECT ts.oid,
           ts.spcname,
           pg_catalog.pg_tablespace_location(ts.oid)
    INTO tablespace_oid,
         tablespace_name,
         tablespace_location
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_database d
      ON d.datname = pg_catalog.current_database()
    JOIN pg_catalog.pg_tablespace ts
      ON ts.oid = COALESCE(NULLIF(c.reltablespace, 0), d.dattablespace)
    WHERE c.oid = rec.leaf_relid;

    IF rec.is_sorted_heap IS NOT TRUE THEN
      status := 'blocked';
      message := 'unsupported access method: ' || COALESCE(rec.am_name::text, rec.relkind::text);
      estimated_temp_bytes := NULL;
      RETURN NEXT;
      CONTINUE;
    END IF;

    IF rec.has_primary_key IS NOT TRUE THEN
      status := 'blocked';
      message := 'primary key is required';
      estimated_temp_bytes := NULL;
      RETURN NEXT;
      CONTINUE;
    END IF;

    status := 'would_run';
    message := NULL;
    estimated_temp_bytes := CASE
      WHEN operation IN ('compact', 'merge') THEN rec.relation_size_bytes
      ELSE 0
    END;
    RETURN NEXT;
  END LOOP;
END;
$$ LANGUAGE plpgsql STABLE;

CREATE FUNCTION @extschema@.sorted_hnsw_partition_search(
  parent regclass,
  vector_column name,
  query text,
  top_k integer,
  local_k integer DEFAULT NULL,
  leaf_relids regclass[] DEFAULT NULL,
  fail_on_unsupported boolean DEFAULT true
) RETURNS TABLE (
  leaf_relid regclass,
  leaf_name text,
  distance double precision,
  row_data jsonb
)
AS $$
DECLARE
  rec record;
  local_limit integer;
  ef integer;
  union_sql text := '';
  sep text := '';
  vector_type text;
  vector_typname name;
  vector_attnum smallint;
  index_name text;
  missing_leaf regclass;
BEGIN
  IF top_k IS NULL OR top_k <= 0 THEN
    RAISE EXCEPTION 'top_k must be positive';
  END IF;

  local_limit := COALESCE(local_k, top_k);
  IF local_limit < top_k THEN
    RAISE EXCEPTION 'local_k must be >= top_k for global partition merge';
  END IF;

  ef := current_setting('sorted_hnsw.ef_search')::integer;
  IF local_limit > ef THEN
    RAISE EXCEPTION 'local_k (%) must be <= sorted_hnsw.ef_search (%)', local_limit, ef
      USING HINT = 'Increase sorted_hnsw.ef_search or lower local_k.';
  END IF;

  IF leaf_relids IS NOT NULL THEN
    SELECT wanted.leaf_relid
    INTO missing_leaf
    FROM pg_catalog.unnest(leaf_relids) AS wanted(leaf_relid)
    LEFT JOIN @extschema@.sorted_heap_partition_status(parent) AS s
      ON s.leaf_relid = wanted.leaf_relid
    WHERE s.leaf_relid IS NULL
    LIMIT 1;

    IF FOUND THEN
      RAISE EXCEPTION 'selected leaf % is not a leaf of partition parent %',
        missing_leaf, parent;
    END IF;
  END IF;

  IF fail_on_unsupported THEN
    SELECT s.*
    INTO rec
    FROM @extschema@.sorted_heap_partition_status(parent) AS s
    WHERE (leaf_relids IS NULL OR s.leaf_relid = ANY(leaf_relids))
      AND (s.is_sorted_heap IS NOT TRUE OR s.has_primary_key IS NOT TRUE)
    LIMIT 1;

    IF FOUND THEN
      IF rec.is_sorted_heap IS NOT TRUE THEN
        RAISE EXCEPTION 'unsupported partition leaf %: access method is %',
          rec.leaf_relid,
          COALESCE(rec.am_name::text, rec.relkind::text);
      ELSE
        RAISE EXCEPTION 'unsupported sorted_heap partition leaf %: primary key is required',
          rec.leaf_relid;
      END IF;
    END IF;
  END IF;

  FOR rec IN
    SELECT s.*
    FROM @extschema@.sorted_heap_partition_status(parent) AS s
    WHERE (leaf_relids IS NULL OR s.leaf_relid = ANY(leaf_relids))
      AND s.is_sorted_heap IS TRUE
      AND s.has_primary_key IS TRUE
    ORDER BY s.leaf_name
  LOOP
    SELECT a.atttypid::regtype::text, t.typname, a.attnum
    INTO vector_type, vector_typname, vector_attnum
    FROM pg_catalog.pg_attribute a
    JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
    WHERE a.attrelid = rec.leaf_relid
      AND a.attname = vector_column
      AND NOT a.attisdropped;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'vector column % does not exist on partition leaf %',
        vector_column, rec.leaf_relid;
    END IF;

    IF vector_typname NOT IN ('svec', 'hsvec') THEN
      RAISE EXCEPTION 'vector column %.% must be svec or hsvec, got %',
        rec.leaf_relid, vector_column, vector_type;
    END IF;

    SELECT idx.relname
    INTO index_name
    FROM pg_catalog.pg_index i
    JOIN pg_catalog.pg_class idx ON idx.oid = i.indexrelid
    JOIN pg_catalog.pg_am am ON am.oid = idx.relam
    WHERE i.indrelid = rec.leaf_relid
      AND am.amname = 'sorted_hnsw'
      AND i.indisvalid
      AND i.indisready
      AND i.indnkeyatts = 1
      AND i.indkey[0] = vector_attnum
      AND i.indpred IS NULL
      AND i.indexprs IS NULL
    LIMIT 1;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'partition leaf % must have a valid sorted_hnsw index on column %',
        rec.leaf_relid, vector_column
        USING HINT = 'Create a leaf-local sorted_hnsw index before calling sorted_hnsw_partition_search.';
    END IF;

    union_sql := union_sql || sep || pg_catalog.format(
      '(SELECT %L::regclass AS leaf_relid,
               %L::text AS leaf_name,
               (t.%I <=> $1::%s)::double precision AS distance,
               pg_catalog.to_jsonb(t) AS row_data
        FROM %s AS t
        ORDER BY t.%I <=> $1::%s
        LIMIT $2)',
      rec.leaf_relid::text,
      rec.leaf_name,
      vector_column,
      vector_type,
      rec.leaf_relid,
      vector_column,
      vector_type);
    sep := ' UNION ALL ';
  END LOOP;

  IF union_sql = '' THEN
    RETURN;
  END IF;

  RETURN QUERY EXECUTE
    'SELECT leaf_relid, leaf_name, distance, row_data
     FROM (' || union_sql || ') AS candidates
     ORDER BY distance
     LIMIT $3'
    USING query, local_limit, top_k;
END;
$$ LANGUAGE plpgsql STABLE;

CREATE FUNCTION @extschema@.sorted_hnsw_partition_search_status(
  parent regclass,
  vector_column name,
  query text,
  top_k integer,
  local_k integer DEFAULT NULL,
  leaf_relids regclass[] DEFAULT NULL,
  fail_on_unsupported boolean DEFAULT true
) RETURNS TABLE (
  requested_top_k integer,
  effective_local_k integer,
  selected_leaves integer,
  returned_rows integer,
  underfilled boolean,
  fallback text
)
AS $$
DECLARE
  local_limit integer;
  selected_count integer;
  returned_count integer;
BEGIN
  IF top_k IS NULL OR top_k <= 0 THEN
    RAISE EXCEPTION 'top_k must be positive';
  END IF;

  local_limit := COALESCE(local_k, top_k);

  SELECT count(*)::integer
  INTO returned_count
  FROM @extschema@.sorted_hnsw_partition_search(
    parent, vector_column, query, top_k, local_limit, leaf_relids,
    fail_on_unsupported);

  SELECT count(*)::integer
  INTO selected_count
  FROM @extschema@.sorted_heap_partition_status(parent) AS s
  WHERE (leaf_relids IS NULL OR s.leaf_relid = ANY(leaf_relids))
    AND s.is_sorted_heap IS TRUE
    AND s.has_primary_key IS TRUE;

  RETURN QUERY
  SELECT top_k,
         local_limit,
         selected_count,
         returned_count,
         returned_count < top_k,
         CASE WHEN returned_count < top_k
              THEN 'underfilled_no_fallback'
              ELSE 'none'
         END;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION @extschema@.sorted_hnsw_partition_search_status(regclass, name, text, integer, integer, regclass[], boolean)
IS 'Diagnostic status for sorted_hnsw_partition_search(...). Reports requested/effective budgets, selected leaves, returned row count, underfill state, and fallback marker without changing the row-returning search API.';

CREATE TABLE @extschema@.sorted_heap_graph_registry (
  relid regclass PRIMARY KEY,
  entity_column name NOT NULL,
  relation_column name NOT NULL,
  target_column name NOT NULL,
  embedding_column name NOT NULL,
  payload_column name NOT NULL
);

SELECT pg_catalog.pg_extension_config_dump('@extschema@.sorted_heap_graph_registry', '');

CREATE FUNCTION @extschema@.sorted_heap_graph_register(
  rel regclass,
  entity_column name DEFAULT 'entity_id',
  relation_column name DEFAULT 'relation_id',
  target_column name DEFAULT 'target_id',
  embedding_column name DEFAULT 'embedding',
  payload_column name DEFAULT 'payload'
) RETURNS void
AS $$
  INSERT INTO @extschema@.sorted_heap_graph_registry (
    relid, entity_column, relation_column, target_column, embedding_column, payload_column
  )
  VALUES ($1, $2, $3, $4, $5, $6)
  ON CONFLICT (relid) DO UPDATE SET
    entity_column = EXCLUDED.entity_column,
    relation_column = EXCLUDED.relation_column,
    target_column = EXCLUDED.target_column,
    embedding_column = EXCLUDED.embedding_column,
    payload_column = EXCLUDED.payload_column;
$$ LANGUAGE SQL;

CREATE FUNCTION @extschema@.sorted_heap_graph_unregister(rel regclass)
RETURNS boolean
AS $$
DECLARE
  deleted_rows int4;
BEGIN
  DELETE FROM @extschema@.sorted_heap_graph_registry WHERE relid = rel;
  GET DIAGNOSTICS deleted_rows = ROW_COUNT;
  RETURN deleted_rows > 0;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION @extschema@.sorted_heap_graph_config(rel regclass)
RETURNS TABLE (
  entity_column name,
  relation_column name,
  target_column name,
  embedding_column name,
  payload_column name,
  is_registered boolean
)
AS $$
  SELECT
    COALESCE(r.entity_column, 'entity_id'::name) AS entity_column,
    COALESCE(r.relation_column, 'relation_id'::name) AS relation_column,
    COALESCE(r.target_column, 'target_id'::name) AS target_column,
    COALESCE(r.embedding_column, 'embedding'::name) AS embedding_column,
    COALESCE(r.payload_column, 'payload'::name) AS payload_column,
    (r.relid IS NOT NULL) AS is_registered
  FROM (SELECT $1::regclass AS relid) args
  LEFT JOIN @extschema@.sorted_heap_graph_registry r ON r.relid = args.relid;
$$ LANGUAGE SQL STABLE;

CREATE TABLE @extschema@.sorted_heap_graph_segment_meta_registry (
  relid regclass PRIMARY KEY,
  segment_group text,
  relation_family text,
  segment_labels text[]
);

SELECT pg_catalog.pg_extension_config_dump('@extschema@.sorted_heap_graph_segment_meta_registry', '');

CREATE FUNCTION @extschema@.sorted_heap_graph_segment_meta_register(
  rel regclass,
  segment_group text DEFAULT NULL,
  relation_family text DEFAULT NULL,
  segment_labels text[] DEFAULT NULL
) RETURNS void
AS $$
BEGIN
  IF segment_labels IS NOT NULL
     AND (array_ndims(segment_labels) <> 1
          OR array_length(segment_labels, 1) IS NULL
          OR array_length(segment_labels, 1) < 1) THEN
    RAISE EXCEPTION 'sorted_heap_graph_segment_meta_register: segment_labels must be a non-empty one-dimensional text[]';
  END IF;

  INSERT INTO @extschema@.sorted_heap_graph_segment_meta_registry (
    relid, segment_group, relation_family, segment_labels
  )
  VALUES ($1, $2, $3, $4)
  ON CONFLICT (relid) DO UPDATE SET
    segment_group = EXCLUDED.segment_group,
    relation_family = EXCLUDED.relation_family,
    segment_labels = EXCLUDED.segment_labels;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION @extschema@.sorted_heap_graph_segment_meta_unregister(
  rel regclass DEFAULT NULL
) RETURNS int4
AS $$
DECLARE
  deleted_rows int4;
BEGIN
  DELETE FROM @extschema@.sorted_heap_graph_segment_meta_registry
  WHERE sorted_heap_graph_segment_meta_registry.relid = COALESCE(sorted_heap_graph_segment_meta_unregister.rel, sorted_heap_graph_segment_meta_registry.relid);
  GET DIAGNOSTICS deleted_rows = ROW_COUNT;
  RETURN deleted_rows;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION @extschema@.sorted_heap_graph_segment_meta_config(
  rel regclass DEFAULT NULL
) RETURNS TABLE (
  rel regclass,
  segment_group text,
  relation_family text,
  segment_labels text[]
)
AS $$
  SELECT m.relid, m.segment_group, m.relation_family, m.segment_labels
  FROM @extschema@.sorted_heap_graph_segment_meta_registry m
  WHERE ($1 IS NULL OR m.relid = $1)
  ORDER BY m.relid;
$$ LANGUAGE SQL STABLE;

CREATE TABLE @extschema@.sorted_heap_graph_segment_registry (
  route_name text NOT NULL,
  relid regclass NOT NULL,
  route_min int8 NOT NULL,
  route_max int8 NOT NULL,
  segment_group text,
  relation_family text,
  PRIMARY KEY (route_name, relid),
  CHECK (route_max >= route_min)
);

SELECT pg_catalog.pg_extension_config_dump('@extschema@.sorted_heap_graph_segment_registry', '');

CREATE FUNCTION @extschema@.sorted_heap_graph_segment_register(
  route_name text,
  rel regclass,
  route_min int8,
  route_max int8,
  segment_group text DEFAULT NULL,
  relation_family text DEFAULT NULL
) RETURNS void
AS $$
  INSERT INTO @extschema@.sorted_heap_graph_segment_registry (
    route_name, relid, route_min, route_max, segment_group, relation_family
  )
  VALUES ($1, $2, $3, $4, $5, $6)
  ON CONFLICT (route_name, relid) DO UPDATE SET
    route_min = EXCLUDED.route_min,
    route_max = EXCLUDED.route_max,
    segment_group = EXCLUDED.segment_group,
    relation_family = EXCLUDED.relation_family;
$$ LANGUAGE SQL;

CREATE FUNCTION @extschema@.sorted_heap_graph_segment_unregister(
  route_name text,
  rel regclass DEFAULT NULL
) RETURNS int4
AS $$
DECLARE
  deleted_rows int4;
BEGIN
  IF rel IS NULL THEN
    DELETE FROM @extschema@.sorted_heap_graph_segment_registry
    WHERE sorted_heap_graph_segment_registry.route_name = sorted_heap_graph_segment_unregister.route_name;
  ELSE
    DELETE FROM @extschema@.sorted_heap_graph_segment_registry
    WHERE sorted_heap_graph_segment_registry.route_name = sorted_heap_graph_segment_unregister.route_name
      AND relid = rel;
  END IF;
  GET DIAGNOSTICS deleted_rows = ROW_COUNT;
  RETURN deleted_rows;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION @extschema@.sorted_heap_graph_segment_config(
  route_name text DEFAULT NULL,
  segment_groups text[] DEFAULT NULL,
  relation_family text DEFAULT NULL,
  segment_labels text[] DEFAULT NULL
)
RETURNS TABLE (
  route_name text,
  rel regclass,
  route_min int8,
  route_max int8,
  segment_group text,
  relation_family text,
  segment_labels text[]
)
AS $$
  SELECT x.route_name, x.relid, x.route_min, x.route_max, x.segment_group, x.relation_family, x.segment_labels
  FROM (
    SELECT s.route_name,
           s.relid,
           s.route_min,
           s.route_max,
           COALESCE(s.segment_group, m.segment_group) AS segment_group,
           COALESCE(s.relation_family, m.relation_family) AS relation_family,
           m.segment_labels AS segment_labels
    FROM @extschema@.sorted_heap_graph_segment_registry s
    LEFT JOIN @extschema@.sorted_heap_graph_segment_meta_registry m ON m.relid = s.relid
  ) x
  WHERE ($1 IS NULL OR x.route_name = $1)
    AND ($2 IS NULL OR x.segment_group = ANY($2))
    AND ($3 IS NULL OR x.relation_family = $3)
    AND ($4 IS NULL OR COALESCE(x.segment_labels, ARRAY[]::text[]) @> $4)
  ORDER BY x.route_name,
           CASE WHEN $2 IS NULL THEN 0 ELSE array_position($2, x.segment_group) END,
           x.segment_group,
           x.relation_family,
           x.route_min,
           x.route_max,
           x.relid;
$$ LANGUAGE SQL STABLE;

CREATE FUNCTION @extschema@.sorted_heap_graph_segment_catalog(
  route_name text DEFAULT NULL,
  segment_groups text[] DEFAULT NULL,
  relation_family text DEFAULT NULL,
  segment_labels text[] DEFAULT NULL
)
RETURNS TABLE (
  route_name text,
  rel regclass,
  route_min int8,
  route_max int8,
  route_segment_group text,
  route_relation_family text,
  shared_segment_group text,
  shared_relation_family text,
  effective_segment_group text,
  effective_relation_family text,
  shared_segment_labels text[],
  effective_segment_labels text[],
  segment_group_source text,
  relation_family_source text,
  segment_labels_source text
)
AS $$
  SELECT x.route_name,
         x.relid,
         x.route_min,
         x.route_max,
         x.route_segment_group,
         x.route_relation_family,
         x.shared_segment_group,
         x.shared_relation_family,
         x.effective_segment_group,
         x.effective_relation_family,
         x.shared_segment_labels,
         x.effective_segment_labels,
         x.segment_group_source,
         x.relation_family_source,
         x.segment_labels_source
  FROM (
    SELECT s.route_name,
           s.relid,
           s.route_min,
           s.route_max,
           s.segment_group AS route_segment_group,
           s.relation_family AS route_relation_family,
           m.segment_group AS shared_segment_group,
           m.relation_family AS shared_relation_family,
           COALESCE(s.segment_group, m.segment_group) AS effective_segment_group,
           COALESCE(s.relation_family, m.relation_family) AS effective_relation_family,
           m.segment_labels AS shared_segment_labels,
           m.segment_labels AS effective_segment_labels,
           CASE
             WHEN s.segment_group IS NOT NULL THEN 'route'
             WHEN m.segment_group IS NOT NULL THEN 'shared'
             ELSE 'unset'
           END AS segment_group_source,
           CASE
             WHEN s.relation_family IS NOT NULL THEN 'route'
             WHEN m.relation_family IS NOT NULL THEN 'shared'
             ELSE 'unset'
           END AS relation_family_source,
           CASE
             WHEN m.segment_labels IS NOT NULL THEN 'shared'
             ELSE 'unset'
           END AS segment_labels_source
    FROM @extschema@.sorted_heap_graph_segment_registry s
    LEFT JOIN @extschema@.sorted_heap_graph_segment_meta_registry m ON m.relid = s.relid
  ) x
  WHERE ($1 IS NULL OR x.route_name = $1)
    AND ($2 IS NULL OR x.effective_segment_group = ANY($2))
    AND ($3 IS NULL OR x.effective_relation_family = $3)
    AND ($4 IS NULL OR COALESCE(x.effective_segment_labels, ARRAY[]::text[]) @> $4)
  ORDER BY x.route_name,
           CASE WHEN $2 IS NULL THEN 0 ELSE array_position($2, x.effective_segment_group) END,
           x.effective_segment_group,
           x.effective_relation_family,
           x.route_min,
           x.route_max,
           x.relid;
$$ LANGUAGE SQL STABLE;

CREATE FUNCTION @extschema@.sorted_heap_graph_segment_resolve(
  route_name text,
  route_value int8,
  fanout_limit int4 DEFAULT 0,
  segment_groups text[] DEFAULT NULL,
  relation_family text DEFAULT NULL,
  segment_labels text[] DEFAULT NULL
) RETURNS TABLE (
  rel regclass,
  route_min int8,
  route_max int8,
  segment_group text,
  relation_family text,
  segment_labels text[]
)
AS $$
  SELECT chosen.relid, chosen.route_min, chosen.route_max, chosen.segment_group, chosen.relation_family, chosen.segment_labels
  FROM (
    SELECT s.relid,
           s.route_min,
           s.route_max,
           COALESCE(s.segment_group, m.segment_group) AS segment_group,
           COALESCE(s.relation_family, m.relation_family) AS relation_family,
           m.segment_labels AS segment_labels
    FROM @extschema@.sorted_heap_graph_segment_registry s
    LEFT JOIN @extschema@.sorted_heap_graph_segment_meta_registry m ON m.relid = s.relid
    WHERE s.route_name = $1
      AND $2 BETWEEN s.route_min AND s.route_max
      AND ($4 IS NULL OR COALESCE(s.segment_group, m.segment_group) = ANY($4))
      AND ($5 IS NULL OR COALESCE(s.relation_family, m.relation_family) = $5)
      AND ($6 IS NULL OR COALESCE(m.segment_labels, ARRAY[]::text[]) @> $6)
    ORDER BY CASE WHEN $4 IS NULL THEN 0 ELSE array_position($4, COALESCE(s.segment_group, m.segment_group)) END,
             (s.route_max - s.route_min),
             s.route_min,
             s.route_max,
             COALESCE(s.segment_group, m.segment_group),
             COALESCE(s.relation_family, m.relation_family),
             s.relid
    LIMIT CASE WHEN $3 IS NULL OR $3 <= 0 THEN NULL ELSE $3 END
  ) chosen
  ORDER BY CASE WHEN $4 IS NULL THEN 0 ELSE array_position($4, chosen.segment_group) END,
           chosen.route_min,
           chosen.route_max,
           chosen.segment_group,
           chosen.relation_family,
           chosen.relid;
$$ LANGUAGE SQL STABLE;

CREATE TABLE @extschema@.sorted_heap_graph_exact_registry (
  route_name text NOT NULL,
  route_key text NOT NULL,
  relid regclass NOT NULL,
  priority int4 NOT NULL DEFAULT 0,
  segment_group text,
  relation_family text,
  PRIMARY KEY (route_name, route_key, relid)
);

SELECT pg_catalog.pg_extension_config_dump('@extschema@.sorted_heap_graph_exact_registry', '');

CREATE FUNCTION @extschema@.sorted_heap_graph_exact_register(
  route_name text,
  route_key text,
  rel regclass,
  priority int4 DEFAULT 0,
  segment_group text DEFAULT NULL,
  relation_family text DEFAULT NULL
) RETURNS void
AS $$
  INSERT INTO @extschema@.sorted_heap_graph_exact_registry (
    route_name, route_key, relid, priority, segment_group, relation_family
  )
  VALUES ($1, $2, $3, $4, $5, $6)
  ON CONFLICT (route_name, route_key, relid) DO UPDATE SET
    priority = EXCLUDED.priority,
    segment_group = EXCLUDED.segment_group,
    relation_family = EXCLUDED.relation_family;
$$ LANGUAGE SQL;

CREATE FUNCTION @extschema@.sorted_heap_graph_exact_unregister(
  route_name text,
  route_key text DEFAULT NULL,
  rel regclass DEFAULT NULL
) RETURNS int4
AS $$
DECLARE
  deleted_rows int4;
BEGIN
  DELETE FROM @extschema@.sorted_heap_graph_exact_registry
  WHERE sorted_heap_graph_exact_registry.route_name = sorted_heap_graph_exact_unregister.route_name
    AND (sorted_heap_graph_exact_unregister.route_key IS NULL
         OR sorted_heap_graph_exact_registry.route_key = sorted_heap_graph_exact_unregister.route_key)
    AND (sorted_heap_graph_exact_unregister.rel IS NULL
         OR sorted_heap_graph_exact_registry.relid = sorted_heap_graph_exact_unregister.rel);
  GET DIAGNOSTICS deleted_rows = ROW_COUNT;
  RETURN deleted_rows;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION @extschema@.sorted_heap_graph_exact_config(
  route_name text DEFAULT NULL,
  route_key text DEFAULT NULL,
  segment_groups text[] DEFAULT NULL,
  relation_family text DEFAULT NULL,
  segment_labels text[] DEFAULT NULL
) RETURNS TABLE (
  route_name text,
  route_key text,
  rel regclass,
  priority int4,
  segment_group text,
  relation_family text,
  segment_labels text[]
)
AS $$
  SELECT x.route_name, x.route_key, x.relid, x.priority, x.segment_group, x.relation_family, x.segment_labels
  FROM (
    SELECT s.route_name,
           s.route_key,
           s.relid,
           s.priority,
           COALESCE(s.segment_group, m.segment_group) AS segment_group,
           COALESCE(s.relation_family, m.relation_family) AS relation_family,
           m.segment_labels AS segment_labels
    FROM @extschema@.sorted_heap_graph_exact_registry s
    LEFT JOIN @extschema@.sorted_heap_graph_segment_meta_registry m ON m.relid = s.relid
  ) x
  WHERE ($1 IS NULL OR x.route_name = $1)
    AND ($2 IS NULL OR x.route_key = $2)
    AND ($3 IS NULL OR x.segment_group = ANY($3))
    AND ($4 IS NULL OR x.relation_family = $4)
    AND ($5 IS NULL OR COALESCE(x.segment_labels, ARRAY[]::text[]) @> $5)
  ORDER BY x.route_name,
           x.route_key,
           CASE WHEN $3 IS NULL THEN 0 ELSE array_position($3, x.segment_group) END,
           x.priority DESC,
           x.segment_group,
           x.relation_family,
           x.relid;
$$ LANGUAGE SQL STABLE;

CREATE FUNCTION @extschema@.sorted_heap_graph_exact_catalog(
  route_name text DEFAULT NULL,
  route_key text DEFAULT NULL,
  segment_groups text[] DEFAULT NULL,
  relation_family text DEFAULT NULL,
  segment_labels text[] DEFAULT NULL
)
RETURNS TABLE (
  route_name text,
  route_key text,
  rel regclass,
  priority int4,
  route_segment_group text,
  route_relation_family text,
  shared_segment_group text,
  shared_relation_family text,
  effective_segment_group text,
  effective_relation_family text,
  shared_segment_labels text[],
  effective_segment_labels text[],
  segment_group_source text,
  relation_family_source text,
  segment_labels_source text
)
AS $$
  SELECT x.route_name,
         x.route_key,
         x.relid,
         x.priority,
         x.route_segment_group,
         x.route_relation_family,
         x.shared_segment_group,
         x.shared_relation_family,
         x.effective_segment_group,
         x.effective_relation_family,
         x.shared_segment_labels,
         x.effective_segment_labels,
         x.segment_group_source,
         x.relation_family_source,
         x.segment_labels_source
  FROM (
    SELECT s.route_name,
           s.route_key,
           s.relid,
           s.priority,
           s.segment_group AS route_segment_group,
           s.relation_family AS route_relation_family,
           m.segment_group AS shared_segment_group,
           m.relation_family AS shared_relation_family,
           COALESCE(s.segment_group, m.segment_group) AS effective_segment_group,
           COALESCE(s.relation_family, m.relation_family) AS effective_relation_family,
           m.segment_labels AS shared_segment_labels,
           m.segment_labels AS effective_segment_labels,
           CASE
             WHEN s.segment_group IS NOT NULL THEN 'route'
             WHEN m.segment_group IS NOT NULL THEN 'shared'
             ELSE 'unset'
           END AS segment_group_source,
           CASE
             WHEN s.relation_family IS NOT NULL THEN 'route'
             WHEN m.relation_family IS NOT NULL THEN 'shared'
             ELSE 'unset'
           END AS relation_family_source,
           CASE
             WHEN m.segment_labels IS NOT NULL THEN 'shared'
             ELSE 'unset'
           END AS segment_labels_source
    FROM @extschema@.sorted_heap_graph_exact_registry s
    LEFT JOIN @extschema@.sorted_heap_graph_segment_meta_registry m ON m.relid = s.relid
  ) x
  WHERE ($1 IS NULL OR x.route_name = $1)
    AND ($2 IS NULL OR x.route_key = $2)
    AND ($3 IS NULL OR x.effective_segment_group = ANY($3))
    AND ($4 IS NULL OR x.effective_relation_family = $4)
    AND ($5 IS NULL OR COALESCE(x.effective_segment_labels, ARRAY[]::text[]) @> $5)
  ORDER BY x.route_name,
           x.route_key,
           CASE WHEN $3 IS NULL THEN 0 ELSE array_position($3, x.effective_segment_group) END,
           x.priority DESC,
           x.effective_segment_group,
           x.effective_relation_family,
           x.relid;
$$ LANGUAGE SQL STABLE;

CREATE FUNCTION @extschema@.sorted_heap_graph_exact_resolve(
  route_name text,
  route_key text,
  fanout_limit int4 DEFAULT 0,
  segment_groups text[] DEFAULT NULL,
  relation_family text DEFAULT NULL,
  segment_labels text[] DEFAULT NULL
) RETURNS TABLE (
  rel regclass,
  priority int4,
  segment_group text,
  relation_family text,
  segment_labels text[]
)
AS $$
  SELECT chosen.relid, chosen.priority, chosen.segment_group, chosen.relation_family, chosen.segment_labels
  FROM (
    SELECT s.relid,
           s.priority,
           COALESCE(s.segment_group, m.segment_group) AS segment_group,
           COALESCE(s.relation_family, m.relation_family) AS relation_family,
           m.segment_labels AS segment_labels
    FROM @extschema@.sorted_heap_graph_exact_registry s
    LEFT JOIN @extschema@.sorted_heap_graph_segment_meta_registry m ON m.relid = s.relid
    WHERE s.route_name = $1
      AND s.route_key = $2
      AND ($4 IS NULL OR COALESCE(s.segment_group, m.segment_group) = ANY($4))
      AND ($5 IS NULL OR COALESCE(s.relation_family, m.relation_family) = $5)
      AND ($6 IS NULL OR COALESCE(m.segment_labels, ARRAY[]::text[]) @> $6)
    ORDER BY CASE WHEN $4 IS NULL THEN 0 ELSE array_position($4, COALESCE(s.segment_group, m.segment_group)) END,
             s.priority DESC,
             COALESCE(s.segment_group, m.segment_group),
             COALESCE(s.relation_family, m.relation_family),
             s.relid
    LIMIT CASE WHEN $3 IS NULL OR $3 <= 0 THEN NULL ELSE $3 END
  ) chosen
  ORDER BY CASE WHEN $4 IS NULL THEN 0 ELSE array_position($4, chosen.segment_group) END,
           chosen.priority DESC,
           chosen.segment_group,
           chosen.relation_family,
           chosen.relid;
$$ LANGUAGE SQL STABLE;

CREATE TABLE @extschema@.sorted_heap_graph_route_policy_registry (
  route_name text NOT NULL,
  policy_name text NOT NULL,
  segment_groups text[] NOT NULL,
  PRIMARY KEY (route_name, policy_name)
);

SELECT pg_catalog.pg_extension_config_dump('@extschema@.sorted_heap_graph_route_policy_registry', '');

CREATE FUNCTION @extschema@.sorted_heap_graph_route_policy_register(
  route_name text,
  policy_name text,
  segment_groups text[]
) RETURNS void
AS $$
BEGIN
  IF segment_groups IS NULL OR array_ndims(segment_groups) <> 1 OR array_length(segment_groups, 1) IS NULL OR array_length(segment_groups, 1) < 1 THEN
    RAISE EXCEPTION 'sorted_heap_graph_route_policy_register: segment_groups must be a non-empty one-dimensional text[]';
  END IF;

  INSERT INTO @extschema@.sorted_heap_graph_route_policy_registry (
    route_name, policy_name, segment_groups
  )
  VALUES ($1, $2, $3)
  ON CONFLICT ON CONSTRAINT sorted_heap_graph_route_policy_registry_pkey DO UPDATE SET
    segment_groups = EXCLUDED.segment_groups;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION @extschema@.sorted_heap_graph_route_policy_unregister(
  route_name text,
  policy_name text DEFAULT NULL
) RETURNS int4
AS $$
DECLARE
  deleted_rows int4;
BEGIN
  DELETE FROM @extschema@.sorted_heap_graph_route_policy_registry
  WHERE sorted_heap_graph_route_policy_registry.route_name = sorted_heap_graph_route_policy_unregister.route_name
    AND (sorted_heap_graph_route_policy_unregister.policy_name IS NULL
         OR sorted_heap_graph_route_policy_registry.policy_name = sorted_heap_graph_route_policy_unregister.policy_name);
  GET DIAGNOSTICS deleted_rows = ROW_COUNT;
  RETURN deleted_rows;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION @extschema@.sorted_heap_graph_route_policy_config(
  route_name text DEFAULT NULL,
  policy_name text DEFAULT NULL
) RETURNS TABLE (
  route_name text,
  policy_name text,
  segment_groups text[]
)
AS $$
  SELECT p.route_name, p.policy_name, p.segment_groups
  FROM @extschema@.sorted_heap_graph_route_policy_registry p
  WHERE ($1 IS NULL OR p.route_name = $1)
    AND ($2 IS NULL OR p.policy_name = $2)
  ORDER BY p.route_name, p.policy_name;
$$ LANGUAGE SQL STABLE;

CREATE FUNCTION @extschema@.sorted_heap_graph_route_policy_groups(
  route_name text,
  policy_name text
) RETURNS text[]
AS $$
DECLARE
  groups text[];
BEGIN
  SELECT p.segment_groups
  INTO groups
  FROM @extschema@.sorted_heap_graph_route_policy_registry p
  WHERE p.route_name = sorted_heap_graph_route_policy_groups.route_name
    AND p.policy_name = sorted_heap_graph_route_policy_groups.policy_name;

  IF groups IS NULL THEN
    RAISE EXCEPTION 'sorted_heap_graph_route_policy_groups: no policy % registered under route %',
      policy_name, route_name;
  END IF;

  RETURN groups;
END;
$$ LANGUAGE plpgsql STABLE;

CREATE TABLE @extschema@.sorted_heap_graph_route_profile_registry (
  route_name text NOT NULL,
  profile_name text NOT NULL,
  policy_name text,
  segment_groups text[],
  relation_family text,
  fanout_limit int4 NOT NULL DEFAULT 0,
  segment_labels text[],
  PRIMARY KEY (route_name, profile_name)
);

SELECT pg_catalog.pg_extension_config_dump('@extschema@.sorted_heap_graph_route_profile_registry', '');

CREATE FUNCTION @extschema@.sorted_heap_graph_route_profile_register(
  route_name text,
  profile_name text,
  policy_name text DEFAULT NULL,
  segment_groups text[] DEFAULT NULL,
  relation_family text DEFAULT NULL,
  fanout_limit int4 DEFAULT 0,
  segment_labels text[] DEFAULT NULL
) RETURNS void
AS $$
BEGIN
  IF fanout_limit < 0 THEN
    RAISE EXCEPTION 'sorted_heap_graph_route_profile_register: fanout_limit must be >= 0';
  END IF;

  IF policy_name IS NOT NULL AND segment_groups IS NOT NULL THEN
    RAISE EXCEPTION 'sorted_heap_graph_route_profile_register: policy_name and segment_groups cannot both be non-NULL';
  END IF;

  IF segment_groups IS NOT NULL
     AND (array_ndims(segment_groups) <> 1
          OR array_length(segment_groups, 1) IS NULL
          OR array_length(segment_groups, 1) < 1) THEN
    RAISE EXCEPTION 'sorted_heap_graph_route_profile_register: segment_groups must be a non-empty one-dimensional text[]';
  END IF;

  IF segment_labels IS NOT NULL
     AND (array_ndims(segment_labels) <> 1
          OR array_length(segment_labels, 1) IS NULL
          OR array_length(segment_labels, 1) < 1) THEN
    RAISE EXCEPTION 'sorted_heap_graph_route_profile_register: segment_labels must be a non-empty one-dimensional text[]';
  END IF;

  INSERT INTO @extschema@.sorted_heap_graph_route_profile_registry (
    route_name, profile_name, policy_name, segment_groups, relation_family, fanout_limit, segment_labels
  )
  VALUES ($1, $2, $3, $4, $5, $6, $7)
  ON CONFLICT ON CONSTRAINT sorted_heap_graph_route_profile_registry_pkey DO UPDATE SET
    policy_name = EXCLUDED.policy_name,
    segment_groups = EXCLUDED.segment_groups,
    relation_family = EXCLUDED.relation_family,
    fanout_limit = EXCLUDED.fanout_limit,
    segment_labels = EXCLUDED.segment_labels;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION @extschema@.sorted_heap_graph_route_profile_unregister(
  route_name text,
  profile_name text DEFAULT NULL
) RETURNS int4
AS $$
DECLARE
  deleted_rows int4;
BEGIN
  DELETE FROM @extschema@.sorted_heap_graph_route_profile_registry
  WHERE sorted_heap_graph_route_profile_registry.route_name = sorted_heap_graph_route_profile_unregister.route_name
    AND (sorted_heap_graph_route_profile_unregister.profile_name IS NULL
         OR sorted_heap_graph_route_profile_registry.profile_name = sorted_heap_graph_route_profile_unregister.profile_name);
  GET DIAGNOSTICS deleted_rows = ROW_COUNT;
  RETURN deleted_rows;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION @extschema@.sorted_heap_graph_route_profile_config(
  route_name text DEFAULT NULL,
  profile_name text DEFAULT NULL
) RETURNS TABLE (
  route_name text,
  profile_name text,
  policy_name text,
  segment_groups text[],
  relation_family text,
  fanout_limit int4,
  segment_labels text[]
)
AS $$
  SELECT p.route_name, p.profile_name, p.policy_name, p.segment_groups, p.relation_family, p.fanout_limit, p.segment_labels
  FROM @extschema@.sorted_heap_graph_route_profile_registry p
  WHERE ($1 IS NULL OR p.route_name = $1)
    AND ($2 IS NULL OR p.profile_name = $2)
  ORDER BY p.route_name, p.profile_name;
$$ LANGUAGE SQL STABLE;

CREATE FUNCTION @extschema@.sorted_heap_graph_route_profile_resolve(
  route_name text,
  profile_name text
) RETURNS TABLE (
  policy_name text,
  segment_groups text[],
  relation_family text,
  fanout_limit int4,
  segment_labels text[]
)
AS $$
DECLARE
  resolved_policy_name text;
  resolved_segment_groups text[];
  resolved_relation_family text;
  resolved_fanout_limit int4;
  resolved_segment_labels text[];
BEGIN
  SELECT p.policy_name, p.segment_groups, p.relation_family, p.fanout_limit, p.segment_labels
  INTO resolved_policy_name, resolved_segment_groups, resolved_relation_family, resolved_fanout_limit, resolved_segment_labels
  FROM @extschema@.sorted_heap_graph_route_profile_registry p
  WHERE p.route_name = sorted_heap_graph_route_profile_resolve.route_name
    AND p.profile_name = sorted_heap_graph_route_profile_resolve.profile_name;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'sorted_heap_graph_route_profile_resolve: no profile % registered under route %',
      profile_name, route_name;
  END IF;

  RETURN QUERY
  SELECT resolved_policy_name, resolved_segment_groups, resolved_relation_family, resolved_fanout_limit, resolved_segment_labels;
END;
$$ LANGUAGE plpgsql STABLE;

CREATE TABLE @extschema@.sorted_heap_graph_route_default_registry (
  route_name text PRIMARY KEY,
  profile_name text NOT NULL,
  FOREIGN KEY (route_name, profile_name)
    REFERENCES @extschema@.sorted_heap_graph_route_profile_registry(route_name, profile_name)
    ON DELETE CASCADE
);

SELECT pg_catalog.pg_extension_config_dump('@extschema@.sorted_heap_graph_route_default_registry', '');

CREATE FUNCTION @extschema@.sorted_heap_graph_route_default_register(
  route_name text,
  profile_name text
) RETURNS void
AS $$
BEGIN
  PERFORM 1
  FROM @extschema@.sorted_heap_graph_route_profile_registry p
  WHERE p.route_name = sorted_heap_graph_route_default_register.route_name
    AND p.profile_name = sorted_heap_graph_route_default_register.profile_name;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'sorted_heap_graph_route_default_register: no profile % registered under route %',
      profile_name, route_name;
  END IF;

  INSERT INTO @extschema@.sorted_heap_graph_route_default_registry (
    route_name, profile_name
  )
  VALUES ($1, $2)
  ON CONFLICT ON CONSTRAINT sorted_heap_graph_route_default_registry_pkey DO UPDATE SET
    profile_name = EXCLUDED.profile_name;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION @extschema@.sorted_heap_graph_route_default_unregister(
  route_name text DEFAULT NULL
) RETURNS int4
AS $$
DECLARE
  deleted_rows int4;
BEGIN
  DELETE FROM @extschema@.sorted_heap_graph_route_default_registry
  WHERE sorted_heap_graph_route_default_registry.route_name = COALESCE(sorted_heap_graph_route_default_unregister.route_name, sorted_heap_graph_route_default_registry.route_name);
  GET DIAGNOSTICS deleted_rows = ROW_COUNT;
  RETURN deleted_rows;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION @extschema@.sorted_heap_graph_route_default_config(
  route_name text DEFAULT NULL
) RETURNS TABLE (
  route_name text,
  profile_name text
)
AS $$
  SELECT d.route_name, d.profile_name
  FROM @extschema@.sorted_heap_graph_route_default_registry d
  WHERE ($1 IS NULL OR d.route_name = $1)
  ORDER BY d.route_name;
$$ LANGUAGE SQL STABLE;

CREATE FUNCTION @extschema@.sorted_heap_graph_route_default_resolve(
  route_name text
) RETURNS text
AS $$
DECLARE
  resolved_profile_name text;
BEGIN
  SELECT d.profile_name
  INTO resolved_profile_name
  FROM @extschema@.sorted_heap_graph_route_default_registry d
  WHERE d.route_name = sorted_heap_graph_route_default_resolve.route_name;

  IF resolved_profile_name IS NULL THEN
    RAISE EXCEPTION 'sorted_heap_graph_route_default_resolve: no default profile registered under route %',
      route_name;
  END IF;

  RETURN resolved_profile_name;
END;
$$ LANGUAGE plpgsql STABLE;

CREATE FUNCTION @extschema@.sorted_heap_graph_route_profile_catalog(
  route_name text DEFAULT NULL,
  profile_name text DEFAULT NULL
) RETURNS TABLE (
  route_name text,
  profile_name text,
  policy_name text,
  inline_segment_groups text[],
  policy_segment_groups text[],
  effective_segment_groups text[],
  segment_groups_source text,
  relation_family text,
  fanout_limit int4,
  segment_labels text[],
  is_default boolean
)
AS $$
  SELECT p.route_name,
         p.profile_name,
         p.policy_name,
         p.segment_groups AS inline_segment_groups,
         pol.segment_groups AS policy_segment_groups,
         COALESCE(p.segment_groups, pol.segment_groups) AS effective_segment_groups,
         CASE
           WHEN p.segment_groups IS NOT NULL THEN 'inline'
           WHEN pol.segment_groups IS NOT NULL THEN 'policy'
           ELSE 'unset'
         END AS segment_groups_source,
         p.relation_family,
         p.fanout_limit,
         p.segment_labels,
         (d.route_name IS NOT NULL) AS is_default
  FROM @extschema@.sorted_heap_graph_route_profile_registry p
  LEFT JOIN @extschema@.sorted_heap_graph_route_policy_registry pol
    ON pol.route_name = p.route_name
   AND pol.policy_name = p.policy_name
  LEFT JOIN @extschema@.sorted_heap_graph_route_default_registry d
    ON d.route_name = p.route_name
   AND d.profile_name = p.profile_name
  WHERE ($1 IS NULL OR p.route_name = $1)
    AND ($2 IS NULL OR p.profile_name = $2)
  ORDER BY p.route_name,
           (d.route_name IS NOT NULL) DESC,
           p.profile_name;
$$ LANGUAGE SQL STABLE;

CREATE FUNCTION @extschema@.sorted_heap_graph_route_catalog(
  route_name text DEFAULT NULL
) RETURNS TABLE (
  route_name text,
  range_shard_count int8,
  exact_binding_count int8,
  policy_count int8,
  profile_count int8,
  default_profile_name text,
  default_effective_segment_groups text[],
  default_segment_groups_source text,
  default_relation_family text,
  default_fanout_limit int4,
  default_segment_labels text[]
)
AS $$
  WITH routes AS (
    SELECT s.route_name
    FROM @extschema@.sorted_heap_graph_segment_registry s
    UNION
    SELECT e.route_name
    FROM @extschema@.sorted_heap_graph_exact_registry e
    UNION
    SELECT p.route_name
    FROM @extschema@.sorted_heap_graph_route_policy_registry p
    UNION
    SELECT p.route_name
    FROM @extschema@.sorted_heap_graph_route_profile_registry p
    UNION
    SELECT d.route_name
    FROM @extschema@.sorted_heap_graph_route_default_registry d
  ),
  range_counts AS (
    SELECT s.route_name, count(*)::int8 AS range_shard_count
    FROM @extschema@.sorted_heap_graph_segment_registry s
    GROUP BY s.route_name
  ),
  exact_counts AS (
    SELECT e.route_name, count(*)::int8 AS exact_binding_count
    FROM @extschema@.sorted_heap_graph_exact_registry e
    GROUP BY e.route_name
  ),
  policy_counts AS (
    SELECT p.route_name, count(*)::int8 AS policy_count
    FROM @extschema@.sorted_heap_graph_route_policy_registry p
    GROUP BY p.route_name
  ),
  profile_counts AS (
    SELECT p.route_name, count(*)::int8 AS profile_count
    FROM @extschema@.sorted_heap_graph_route_profile_registry p
    GROUP BY p.route_name
  ),
  defaults AS (
    SELECT c.route_name,
           c.profile_name AS default_profile_name,
           c.effective_segment_groups AS default_effective_segment_groups,
           c.segment_groups_source AS default_segment_groups_source,
           c.relation_family AS default_relation_family,
           c.fanout_limit AS default_fanout_limit,
           c.segment_labels AS default_segment_labels
    FROM @extschema@.sorted_heap_graph_route_profile_catalog() c
    WHERE c.is_default
  )
  SELECT r.route_name,
         COALESCE(rc.range_shard_count, 0) AS range_shard_count,
         COALESCE(ec.exact_binding_count, 0) AS exact_binding_count,
         COALESCE(pc.policy_count, 0) AS policy_count,
         COALESCE(pr.profile_count, 0) AS profile_count,
         d.default_profile_name,
         d.default_effective_segment_groups,
         d.default_segment_groups_source,
         d.default_relation_family,
         d.default_fanout_limit,
         d.default_segment_labels
  FROM routes r
  LEFT JOIN range_counts rc ON rc.route_name = r.route_name
  LEFT JOIN exact_counts ec ON ec.route_name = r.route_name
  LEFT JOIN policy_counts pc ON pc.route_name = r.route_name
  LEFT JOIN profile_counts pr ON pr.route_name = r.route_name
  LEFT JOIN defaults d ON d.route_name = r.route_name
  WHERE ($1 IS NULL OR r.route_name = $1)
  ORDER BY r.route_name;
$$ LANGUAGE SQL STABLE;

CREATE FUNCTION @extschema@.sorted_heap_graph_rag_stats()
RETURNS TABLE (
  calls bigint,
  api text,
  seed_count bigint,
  expanded_rows bigint,
  reranked_rows bigint,
  returned_rows bigint,
  ann_ms float8,
  expand_ms float8,
  rerank_ms float8,
  total_ms float8
)
AS '$libdir/pg_sorted_heap', 'sorted_heap_graph_rag_stats'
LANGUAGE C STABLE;

CREATE FUNCTION @extschema@.sorted_heap_graph_rag_reset_stats()
RETURNS void
AS '$libdir/pg_sorted_heap', 'sorted_heap_graph_rag_reset_stats'
LANGUAGE C VOLATILE;

CREATE FUNCTION @extschema@.sorted_heap_expand_multihop_rerank(
  rel regclass,
  seed_ids int4[],
  query @extschema@.svec,
  top_k int4,
  relation_path int4[],
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS '$libdir/pg_sorted_heap', 'sorted_heap_expand_multihop_rerank'
LANGUAGE C STABLE;

CREATE FUNCTION @extschema@.sorted_heap_expand_multihop_path_rerank(
  rel regclass,
  seed_ids int4[],
  query @extschema@.svec,
  top_k int4,
  relation_path int4[],
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS '$libdir/pg_sorted_heap', 'sorted_heap_expand_multihop_path_rerank'
LANGUAGE C STABLE;

CREATE FUNCTION @extschema@.sorted_heap_graph_rag_multihop_scan(
  rel regclass,
  query @extschema@.svec,
  ann_k int4,
  top_k int4,
  relation_path int4[],
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS '$libdir/pg_sorted_heap', 'sorted_heap_graph_rag_multihop_scan'
LANGUAGE C STABLE;

CREATE FUNCTION @extschema@.sorted_heap_graph_rag_multihop_path_scan(
  rel regclass,
  query @extschema@.svec,
  ann_k int4,
  top_k int4,
  relation_path int4[],
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS '$libdir/pg_sorted_heap', 'sorted_heap_graph_rag_multihop_path_scan'
LANGUAGE C STABLE;

CREATE FUNCTION @extschema@.sorted_heap_graph_rag(
  rel regclass,
  query @extschema@.svec,
  relation_path int4[],
  ann_k int4 DEFAULT 64,
  top_k int4 DEFAULT 10,
  score_mode text DEFAULT 'path',
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS $$
DECLARE
  path_len int4;
  hop int4;
  seed_ids int4[];
  mode text;
  seed_sql text;
  cfg record;
BEGIN
  IF ann_k < 1 THEN
    RAISE EXCEPTION 'sorted_heap_graph_rag: ann_k must be >= 1';
  END IF;
  IF top_k < 1 THEN
    RAISE EXCEPTION 'sorted_heap_graph_rag: top_k must be >= 1';
  END IF;
  IF limit_rows < 0 THEN
    RAISE EXCEPTION 'sorted_heap_graph_rag: limit_rows must be >= 0';
  END IF;
  IF relation_path IS NULL OR array_ndims(relation_path) <> 1 THEN
    RAISE EXCEPTION 'sorted_heap_graph_rag: relation_path must be a one-dimensional int4[]';
  END IF;

  path_len := array_length(relation_path, 1);
  IF path_len IS NULL OR path_len < 1 THEN
    RAISE EXCEPTION 'sorted_heap_graph_rag: relation_path must have length >= 1';
  END IF;

  FOREACH hop IN ARRAY relation_path LOOP
    IF hop < -32768 OR hop > 32767 THEN
      RAISE EXCEPTION 'sorted_heap_graph_rag: relation_path element % is outside int2 range', hop;
    END IF;
  END LOOP;

  mode := coalesce(nullif(lower(btrim(score_mode)), ''), 'path');
  SELECT * INTO cfg
  FROM @extschema@.sorted_heap_graph_config(rel);

  IF path_len = 1 THEN
    IF mode NOT IN ('endpoint', 'path') THEN
      RAISE EXCEPTION 'sorted_heap_graph_rag: score_mode % is invalid for 1-hop paths (expected endpoint or path)', mode;
    END IF;

    seed_sql := format(
      'SELECT array_agg(%1$I::int4)
       FROM (
         SELECT DISTINCT %1$I
         FROM (
           SELECT %1$I
           FROM %2$s
           ORDER BY %3$I <=> $1
           LIMIT $2
         ) ann
       ) seeds',
      cfg.entity_column,
      rel,
      cfg.embedding_column);
    EXECUTE seed_sql USING query, ann_k INTO seed_ids;

    IF seed_ids IS NULL OR array_length(seed_ids, 1) IS NULL THEN
      RETURN;
    END IF;

    RETURN QUERY
    SELECT g.entity_id, g.relation_id, g.target_id, g.payload, g.distance
    FROM @extschema@.sorted_heap_expand_rerank(
      rel,
      seed_ids,
      query,
      top_k,
      relation_path[1],
      limit_rows
    ) AS g;
    RETURN;
  END IF;

  IF mode = 'path' THEN
    IF path_len > 2 THEN
      RETURN QUERY
      SELECT g.entity_id, g.relation_id, g.target_id, g.payload, g.distance
      FROM @extschema@.sorted_heap_graph_rag_multihop_path_scan(
        rel,
        query,
        ann_k,
        top_k,
        relation_path,
        limit_rows
      ) AS g;
      RETURN;
    END IF;
    RETURN QUERY
    SELECT g.entity_id, g.relation_id, g.target_id, g.payload, g.distance
    FROM @extschema@.sorted_heap_graph_rag_twohop_path_scan(
      rel,
      query,
      ann_k,
      top_k,
      relation_path[1],
      relation_path[2],
      limit_rows
    ) AS g;
    RETURN;
  END IF;

  IF mode = 'endpoint' THEN
    IF path_len > 2 THEN
      RETURN QUERY
      SELECT g.entity_id, g.relation_id, g.target_id, g.payload, g.distance
      FROM @extschema@.sorted_heap_graph_rag_multihop_scan(
        rel,
        query,
        ann_k,
        top_k,
        relation_path,
        limit_rows
      ) AS g;
      RETURN;
    END IF;
    RETURN QUERY
    SELECT g.entity_id, g.relation_id, g.target_id, g.payload, g.distance
    FROM @extschema@.sorted_heap_graph_rag_twohop_scan(
      rel,
      query,
      ann_k,
      top_k,
      relation_path[1],
      relation_path[2],
      limit_rows
    ) AS g;
    RETURN;
  END IF;

  RAISE EXCEPTION 'sorted_heap_graph_rag: unsupported score_mode % (expected endpoint or path)', mode;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION @extschema@.sorted_heap_graph_rag(regclass, @extschema@.svec, int4[], int4, int4, text, int4)
IS 'Unified fact-shaped GraphRAG entry point. relation_path length 1 performs ANN seed on entity_id plus one-hop rerank, where score_mode path is intentionally equivalent to endpoint. Longer relation_path values perform multi-hop endpoint or path-aware rerank depending on score_mode. limit_rows is an optional work cap for expansion/rerank stages; 0 means unlimited.';

CREATE FUNCTION @extschema@.sorted_heap_graph_rag_segmented(
  rels regclass[],
  query @extschema@.svec,
  relation_path int4[],
  ann_k int4 DEFAULT 64,
  top_k int4 DEFAULT 10,
  score_mode text DEFAULT 'path',
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  source_rel regclass,
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS $$
DECLARE
  shard regclass;
  union_sql text := '';
  shard_count int4 := 0;
BEGIN
  IF ann_k < 1 THEN
    RAISE EXCEPTION 'sorted_heap_graph_rag_segmented: ann_k must be >= 1';
  END IF;
  IF top_k < 1 THEN
    RAISE EXCEPTION 'sorted_heap_graph_rag_segmented: top_k must be >= 1';
  END IF;
  IF limit_rows < 0 THEN
    RAISE EXCEPTION 'sorted_heap_graph_rag_segmented: limit_rows must be >= 0';
  END IF;
  IF rels IS NULL OR array_ndims(rels) <> 1 THEN
    RAISE EXCEPTION 'sorted_heap_graph_rag_segmented: rels must be a one-dimensional regclass[]';
  END IF;

  FOREACH shard IN ARRAY rels LOOP
    IF shard IS NULL THEN
      RAISE EXCEPTION 'sorted_heap_graph_rag_segmented: rels must not contain NULLs';
    END IF;

    shard_count := shard_count + 1;
    IF shard_count > 1 THEN
      union_sql := union_sql || E'\nUNION ALL\n';
    END IF;

    union_sql := union_sql || format(
      'SELECT %1$s::oid::regclass AS source_rel,
              g.entity_id,
              g.relation_id,
              g.target_id,
              g.payload,
              g.distance
       FROM @extschema@.sorted_heap_graph_rag(
         %1$s::oid::regclass,
         $1,
         $2,
         $3,
         $4,
         $5,
         $6
       ) AS g',
      shard::oid
    );
  END LOOP;

  IF shard_count < 1 THEN
    RAISE EXCEPTION 'sorted_heap_graph_rag_segmented: rels must have length >= 1';
  END IF;

  RETURN QUERY EXECUTE format(
    'SELECT source_rel, entity_id, relation_id, target_id, payload, distance
     FROM (%s) AS merged
     ORDER BY distance, entity_id, relation_id, target_id, source_rel
     LIMIT $4',
    union_sql
  )
  USING query, relation_path, ann_k, top_k, score_mode, limit_rows;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION @extschema@.sorted_heap_graph_rag_segmented(regclass[], @extschema@.svec, int4[], int4, int4, text, int4)
IS 'Beta segmented GraphRAG wrapper. Executes sorted_heap_graph_rag(...) across a candidate shard list, merges shard-local rows globally, and returns the top-k result set plus source_rel.';

CREATE FUNCTION @extschema@.sorted_heap_graph_rag_routed(
  route_name text,
  route_value int8,
  query @extschema@.svec,
  relation_path int4[],
  ann_k int4 DEFAULT 64,
  top_k int4 DEFAULT 10,
  score_mode text DEFAULT 'path',
  limit_rows int4 DEFAULT 0,
  fanout_limit int4 DEFAULT 0,
  segment_groups text[] DEFAULT NULL,
  relation_family text DEFAULT NULL,
  segment_labels text[] DEFAULT NULL
) RETURNS TABLE (
  source_rel regclass,
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS $$
DECLARE
  rels regclass[];
BEGIN
  IF fanout_limit < 0 THEN
    RAISE EXCEPTION 'sorted_heap_graph_rag_routed: fanout_limit must be >= 0';
  END IF;

  SELECT array_agg(rel ORDER BY route_min, route_max, rel)
  INTO rels
  FROM @extschema@.sorted_heap_graph_segment_resolve(route_name, route_value, fanout_limit, segment_groups, relation_family, segment_labels);

  IF rels IS NULL OR array_length(rels, 1) IS NULL THEN
    RETURN;
  END IF;

  RETURN QUERY
  SELECT g.source_rel, g.entity_id, g.relation_id, g.target_id, g.payload, g.distance
  FROM @extschema@.sorted_heap_graph_rag_segmented(
    rels,
    query,
    relation_path,
    ann_k,
    top_k,
    score_mode,
    limit_rows
  ) AS g;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION @extschema@.sorted_heap_graph_rag_routed(text, int8, @extschema@.svec, int4[], int4, int4, text, int4, int4, text[], text, text[])
IS 'Beta routed GraphRAG wrapper. Resolves candidate shards from sorted_heap_graph_segment_registry using a route value plus optional segment-group, relation-family, and shared segment-label filtering, then delegates to sorted_heap_graph_rag_segmented(...).';

CREATE FUNCTION @extschema@.sorted_heap_graph_rag_routed_exact(
  route_name text,
  route_key text,
  query @extschema@.svec,
  relation_path int4[],
  ann_k int4 DEFAULT 64,
  top_k int4 DEFAULT 10,
  score_mode text DEFAULT 'path',
  limit_rows int4 DEFAULT 0,
  fanout_limit int4 DEFAULT 0,
  segment_groups text[] DEFAULT NULL,
  relation_family text DEFAULT NULL,
  segment_labels text[] DEFAULT NULL
) RETURNS TABLE (
  source_rel regclass,
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS $$
DECLARE
  rels regclass[];
BEGIN
  IF fanout_limit < 0 THEN
    RAISE EXCEPTION 'sorted_heap_graph_rag_routed_exact: fanout_limit must be >= 0';
  END IF;

  SELECT array_agg(rel ORDER BY priority DESC, rel)
  INTO rels
  FROM @extschema@.sorted_heap_graph_exact_resolve(route_name, route_key, fanout_limit, segment_groups, relation_family, segment_labels);

  IF rels IS NULL OR array_length(rels, 1) IS NULL THEN
    RETURN;
  END IF;

  RETURN QUERY
  SELECT g.source_rel, g.entity_id, g.relation_id, g.target_id, g.payload, g.distance
  FROM @extschema@.sorted_heap_graph_rag_segmented(
    rels,
    query,
    relation_path,
    ann_k,
    top_k,
    score_mode,
    limit_rows
  ) AS g;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION @extschema@.sorted_heap_graph_rag_routed_exact(text, text, @extschema@.svec, int4[], int4, int4, text, int4, int4, text[], text, text[])
IS 'Beta exact-key routed GraphRAG wrapper. Resolves candidate shards from sorted_heap_graph_exact_registry using an exact route key plus optional segment-group, relation-family, and shared segment-label filtering, then delegates to sorted_heap_graph_rag_segmented(...).';

CREATE FUNCTION @extschema@.sorted_heap_graph_rag_routed_policy(
  route_name text,
  route_value int8,
  policy_name text,
  query @extschema@.svec,
  relation_path int4[],
  ann_k int4 DEFAULT 64,
  top_k int4 DEFAULT 10,
  score_mode text DEFAULT 'path',
  limit_rows int4 DEFAULT 0,
  fanout_limit int4 DEFAULT 0,
  relation_family text DEFAULT NULL,
  segment_labels text[] DEFAULT NULL
) RETURNS TABLE (
  source_rel regclass,
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS $$
DECLARE
  groups text[];
BEGIN
  groups := @extschema@.sorted_heap_graph_route_policy_groups(route_name, policy_name);

  RETURN QUERY
  SELECT g.source_rel, g.entity_id, g.relation_id, g.target_id, g.payload, g.distance
  FROM @extschema@.sorted_heap_graph_rag_routed(
    route_name,
    route_value,
    query,
    relation_path,
    ann_k,
    top_k,
    score_mode,
    limit_rows,
    fanout_limit,
    groups,
    relation_family,
    segment_labels
  ) AS g;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION @extschema@.sorted_heap_graph_rag_routed_policy(text, int8, text, @extschema@.svec, int4[], int4, int4, text, int4, int4, text, text[])
IS 'Beta routed GraphRAG wrapper. Resolves a named segment-group policy from sorted_heap_graph_route_policy_registry, then delegates to sorted_heap_graph_rag_routed(...) with optional relation-family and shared segment-label filtering.';

CREATE FUNCTION @extschema@.sorted_heap_graph_rag_routed_exact_policy(
  route_name text,
  route_key text,
  policy_name text,
  query @extschema@.svec,
  relation_path int4[],
  ann_k int4 DEFAULT 64,
  top_k int4 DEFAULT 10,
  score_mode text DEFAULT 'path',
  limit_rows int4 DEFAULT 0,
  fanout_limit int4 DEFAULT 0,
  relation_family text DEFAULT NULL,
  segment_labels text[] DEFAULT NULL
) RETURNS TABLE (
  source_rel regclass,
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS $$
DECLARE
  groups text[];
BEGIN
  groups := @extschema@.sorted_heap_graph_route_policy_groups(route_name, policy_name);

  RETURN QUERY
  SELECT g.source_rel, g.entity_id, g.relation_id, g.target_id, g.payload, g.distance
  FROM @extschema@.sorted_heap_graph_rag_routed_exact(
    route_name,
    route_key,
    query,
    relation_path,
    ann_k,
    top_k,
    score_mode,
    limit_rows,
    fanout_limit,
    groups,
    relation_family,
    segment_labels
  ) AS g;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION @extschema@.sorted_heap_graph_rag_routed_exact_policy(text, text, text, @extschema@.svec, int4[], int4, int4, text, int4, int4, text, text[])
IS 'Beta exact-key routed GraphRAG wrapper. Resolves a named segment-group policy from sorted_heap_graph_route_policy_registry, then delegates to sorted_heap_graph_rag_routed_exact(...) with optional relation-family and shared segment-label filtering.';

CREATE FUNCTION @extschema@.sorted_heap_graph_rag_routed_profile(
  route_name text,
  route_value int8,
  profile_name text,
  query @extschema@.svec,
  relation_path int4[],
  ann_k int4 DEFAULT 64,
  top_k int4 DEFAULT 10,
  score_mode text DEFAULT 'path',
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  source_rel regclass,
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS $$
DECLARE
  resolved_policy_name text;
  resolved_segment_groups text[];
  resolved_relation_family text;
  resolved_fanout_limit int4;
  resolved_segment_labels text[];
BEGIN
  SELECT p.policy_name, p.segment_groups, p.relation_family, p.fanout_limit, p.segment_labels
  INTO resolved_policy_name, resolved_segment_groups, resolved_relation_family, resolved_fanout_limit, resolved_segment_labels
  FROM @extschema@.sorted_heap_graph_route_profile_resolve(route_name, profile_name) AS p;

  IF resolved_segment_groups IS NOT NULL THEN
    RETURN QUERY
    SELECT g.source_rel, g.entity_id, g.relation_id, g.target_id, g.payload, g.distance
    FROM @extschema@.sorted_heap_graph_rag_routed(
      route_name,
      route_value,
      query,
      relation_path,
      ann_k,
      top_k,
      score_mode,
      limit_rows,
      resolved_fanout_limit,
      resolved_segment_groups,
      resolved_relation_family,
      resolved_segment_labels
    ) AS g;
  ELSIF resolved_policy_name IS NULL THEN
    RETURN QUERY
    SELECT g.source_rel, g.entity_id, g.relation_id, g.target_id, g.payload, g.distance
    FROM @extschema@.sorted_heap_graph_rag_routed(
      route_name,
      route_value,
      query,
      relation_path,
      ann_k,
      top_k,
      score_mode,
      limit_rows,
      resolved_fanout_limit,
      NULL,
      resolved_relation_family,
      resolved_segment_labels
    ) AS g;
  ELSE
    RETURN QUERY
    SELECT g.source_rel, g.entity_id, g.relation_id, g.target_id, g.payload, g.distance
    FROM @extschema@.sorted_heap_graph_rag_routed_policy(
      route_name,
      route_value,
      resolved_policy_name,
      query,
      relation_path,
      ann_k,
      top_k,
      score_mode,
      limit_rows,
      resolved_fanout_limit,
      resolved_relation_family,
      resolved_segment_labels
    ) AS g;
  END IF;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION @extschema@.sorted_heap_graph_rag_routed_profile(text, int8, text, @extschema@.svec, int4[], int4, int4, text, int4)
IS 'Beta routed GraphRAG wrapper. Resolves a named route profile containing policy_name or inline segment_groups, relation_family, fanout_limit, and optional shared segment-label filtering, then delegates to the existing routed GraphRAG path.';

CREATE FUNCTION @extschema@.sorted_heap_graph_rag_routed_exact_profile(
  route_name text,
  route_key text,
  profile_name text,
  query @extschema@.svec,
  relation_path int4[],
  ann_k int4 DEFAULT 64,
  top_k int4 DEFAULT 10,
  score_mode text DEFAULT 'path',
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  source_rel regclass,
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS $$
DECLARE
  resolved_policy_name text;
  resolved_segment_groups text[];
  resolved_relation_family text;
  resolved_fanout_limit int4;
  resolved_segment_labels text[];
BEGIN
  SELECT p.policy_name, p.segment_groups, p.relation_family, p.fanout_limit, p.segment_labels
  INTO resolved_policy_name, resolved_segment_groups, resolved_relation_family, resolved_fanout_limit, resolved_segment_labels
  FROM @extschema@.sorted_heap_graph_route_profile_resolve(route_name, profile_name) AS p;

  IF resolved_segment_groups IS NOT NULL THEN
    RETURN QUERY
    SELECT g.source_rel, g.entity_id, g.relation_id, g.target_id, g.payload, g.distance
    FROM @extschema@.sorted_heap_graph_rag_routed_exact(
      route_name,
      route_key,
      query,
      relation_path,
      ann_k,
      top_k,
      score_mode,
      limit_rows,
      resolved_fanout_limit,
      resolved_segment_groups,
      resolved_relation_family,
      resolved_segment_labels
    ) AS g;
  ELSIF resolved_policy_name IS NULL THEN
    RETURN QUERY
    SELECT g.source_rel, g.entity_id, g.relation_id, g.target_id, g.payload, g.distance
    FROM @extschema@.sorted_heap_graph_rag_routed_exact(
      route_name,
      route_key,
      query,
      relation_path,
      ann_k,
      top_k,
      score_mode,
      limit_rows,
      resolved_fanout_limit,
      NULL,
      resolved_relation_family,
      resolved_segment_labels
    ) AS g;
  ELSE
    RETURN QUERY
    SELECT g.source_rel, g.entity_id, g.relation_id, g.target_id, g.payload, g.distance
    FROM @extschema@.sorted_heap_graph_rag_routed_exact_policy(
      route_name,
      route_key,
      resolved_policy_name,
      query,
      relation_path,
      ann_k,
      top_k,
      score_mode,
      limit_rows,
      resolved_fanout_limit,
      resolved_relation_family,
      resolved_segment_labels
    ) AS g;
  END IF;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION @extschema@.sorted_heap_graph_rag_routed_exact_profile(text, text, text, @extschema@.svec, int4[], int4, int4, text, int4)
IS 'Beta exact-key routed GraphRAG wrapper. Resolves a named route profile containing policy_name or inline segment_groups, relation_family, fanout_limit, and optional shared segment-label filtering, then delegates to the existing exact-key routed GraphRAG path.';

CREATE FUNCTION @extschema@.sorted_heap_graph_rag_routed_default(
  route_name text,
  route_value int8,
  query @extschema@.svec,
  relation_path int4[],
  ann_k int4 DEFAULT 64,
  top_k int4 DEFAULT 10,
  score_mode text DEFAULT 'path',
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  source_rel regclass,
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS $$
DECLARE
  resolved_profile_name text;
BEGIN
  resolved_profile_name := @extschema@.sorted_heap_graph_route_default_resolve(route_name);

  RETURN QUERY
  SELECT g.source_rel, g.entity_id, g.relation_id, g.target_id, g.payload, g.distance
  FROM @extschema@.sorted_heap_graph_rag_routed_profile(
    route_name,
    route_value,
    resolved_profile_name,
    query,
    relation_path,
    ann_k,
    top_k,
    score_mode,
    limit_rows
  ) AS g;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION @extschema@.sorted_heap_graph_rag_routed_default(text, int8, @extschema@.svec, int4[], int4, int4, text, int4)
IS 'Beta routed GraphRAG wrapper. Resolves the default route profile for a route and delegates to sorted_heap_graph_rag_routed_profile(...).';

CREATE FUNCTION @extschema@.sorted_heap_graph_rag_routed_exact_default(
  route_name text,
  route_key text,
  query @extschema@.svec,
  relation_path int4[],
  ann_k int4 DEFAULT 64,
  top_k int4 DEFAULT 10,
  score_mode text DEFAULT 'path',
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  source_rel regclass,
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS $$
DECLARE
  resolved_profile_name text;
BEGIN
  resolved_profile_name := @extschema@.sorted_heap_graph_route_default_resolve(route_name);

  RETURN QUERY
  SELECT g.source_rel, g.entity_id, g.relation_id, g.target_id, g.payload, g.distance
  FROM @extschema@.sorted_heap_graph_rag_routed_exact_profile(
    route_name,
    route_key,
    resolved_profile_name,
    query,
    relation_path,
    ann_k,
    top_k,
    score_mode,
    limit_rows
  ) AS g;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION @extschema@.sorted_heap_graph_rag_routed_exact_default(text, text, @extschema@.svec, int4[], int4, int4, text, int4)
IS 'Beta exact-key routed GraphRAG wrapper. Resolves the default route profile for a route and delegates to sorted_heap_graph_rag_routed_exact_profile(...).';

-- Unified router wrapper

CREATE FUNCTION @extschema@.sorted_heap_graph_route(
  route_name text,
  query @extschema@.svec,
  relation_path int4[],
  route_key text DEFAULT NULL,
  route_value int8 DEFAULT NULL,
  profile_name text DEFAULT NULL,
  policy_name text DEFAULT NULL,
  ann_k int4 DEFAULT 64,
  top_k int4 DEFAULT 10,
  score_mode text DEFAULT 'path',
  limit_rows int4 DEFAULT 0,
  fanout_limit int4 DEFAULT 0,
  segment_groups text[] DEFAULT NULL,
  relation_family text DEFAULT NULL,
  segment_labels text[] DEFAULT NULL
) RETURNS TABLE (
  source_rel regclass,
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS $$
DECLARE
  use_exact boolean;
  has_overrides boolean;
  default_profile text;
BEGIN
  IF route_key IS NOT NULL AND route_value IS NOT NULL THEN
    RAISE EXCEPTION 'sorted_heap_graph_route: provide route_key or route_value, not both';
  END IF;
  IF route_key IS NULL AND route_value IS NULL THEN
    RAISE EXCEPTION 'sorted_heap_graph_route: must provide route_key or route_value';
  END IF;
  use_exact := route_key IS NOT NULL;

  IF profile_name IS NOT NULL AND policy_name IS NOT NULL THEN
    RAISE EXCEPTION 'sorted_heap_graph_route: provide profile_name or policy_name, not both';
  END IF;

  IF profile_name IS NOT NULL THEN
    IF segment_groups IS NOT NULL OR relation_family IS NOT NULL
       OR segment_labels IS NOT NULL OR fanout_limit != 0
    THEN
      RAISE EXCEPTION 'sorted_heap_graph_route: profile_name cannot be combined with segment_groups, relation_family, segment_labels, or fanout_limit';
    END IF;
    IF use_exact THEN
      RETURN QUERY SELECT g.* FROM @extschema@.sorted_heap_graph_rag_routed_exact_profile(
        route_name, route_key, profile_name, query, relation_path,
        ann_k, top_k, score_mode, limit_rows) AS g;
    ELSE
      RETURN QUERY SELECT g.* FROM @extschema@.sorted_heap_graph_rag_routed_profile(
        route_name, route_value, profile_name, query, relation_path,
        ann_k, top_k, score_mode, limit_rows) AS g;
    END IF;
    RETURN;
  END IF;

  IF policy_name IS NOT NULL THEN
    IF segment_groups IS NOT NULL THEN
      RAISE EXCEPTION 'sorted_heap_graph_route: policy_name cannot be combined with segment_groups';
    END IF;
    IF use_exact THEN
      RETURN QUERY SELECT g.* FROM @extschema@.sorted_heap_graph_rag_routed_exact_policy(
        route_name, route_key, policy_name, query, relation_path,
        ann_k, top_k, score_mode, limit_rows,
        fanout_limit, relation_family, segment_labels) AS g;
    ELSE
      RETURN QUERY SELECT g.* FROM @extschema@.sorted_heap_graph_rag_routed_policy(
        route_name, route_value, policy_name, query, relation_path,
        ann_k, top_k, score_mode, limit_rows,
        fanout_limit, relation_family, segment_labels) AS g;
    END IF;
    RETURN;
  END IF;

  has_overrides := segment_groups IS NOT NULL
    OR relation_family IS NOT NULL
    OR segment_labels IS NOT NULL
    OR fanout_limit != 0;

  IF has_overrides THEN
    IF use_exact THEN
      RETURN QUERY SELECT g.* FROM @extschema@.sorted_heap_graph_rag_routed_exact(
        route_name, route_key, query, relation_path,
        ann_k, top_k, score_mode, limit_rows,
        fanout_limit, segment_groups, relation_family, segment_labels) AS g;
    ELSE
      RETURN QUERY SELECT g.* FROM @extschema@.sorted_heap_graph_rag_routed(
        route_name, route_value, query, relation_path,
        ann_k, top_k, score_mode, limit_rows,
        fanout_limit, segment_groups, relation_family, segment_labels) AS g;
    END IF;
    RETURN;
  END IF;

  SELECT d.profile_name INTO default_profile
  FROM @extschema@.sorted_heap_graph_route_default_registry d
  WHERE d.route_name = sorted_heap_graph_route.route_name;

  IF default_profile IS NOT NULL THEN
    IF use_exact THEN
      RETURN QUERY SELECT g.* FROM @extschema@.sorted_heap_graph_rag_routed_exact_profile(
        route_name, route_key, default_profile, query, relation_path,
        ann_k, top_k, score_mode, limit_rows) AS g;
    ELSE
      RETURN QUERY SELECT g.* FROM @extschema@.sorted_heap_graph_rag_routed_profile(
        route_name, route_value, default_profile, query, relation_path,
        ann_k, top_k, score_mode, limit_rows) AS g;
    END IF;
    RETURN;
  END IF;

  IF use_exact THEN
    RETURN QUERY SELECT g.* FROM @extschema@.sorted_heap_graph_rag_routed_exact(
      route_name, route_key, query, relation_path,
      ann_k, top_k, score_mode, limit_rows) AS g;
  ELSE
    RETURN QUERY SELECT g.* FROM @extschema@.sorted_heap_graph_rag_routed(
      route_name, route_value, query, relation_path,
      ann_k, top_k, score_mode, limit_rows) AS g;
  END IF;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION @extschema@.sorted_heap_graph_route(text, @extschema@.svec, int4[], text, int8, text, text, int4, int4, text, int4, int4, text[], text, text[])
IS 'Unified routed GraphRAG entry point. Dispatches to the appropriate routed path based on provided parameters. Resolution order: (1) route_key XOR route_value, (2) profile_name XOR policy_name, (3) profile path, (4) policy path, (5) explicit overrides, (6) default profile, (7) base dispatcher.';

CREATE FUNCTION @extschema@.sorted_heap_graph_route_plan(
  route_name text,
  route_key text DEFAULT NULL,
  route_value int8 DEFAULT NULL,
  profile_name text DEFAULT NULL,
  policy_name text DEFAULT NULL,
  fanout_limit int4 DEFAULT 0,
  segment_groups text[] DEFAULT NULL,
  relation_family text DEFAULT NULL,
  segment_labels text[] DEFAULT NULL
) RETURNS TABLE (
  route_kind text,
  resolution_path text,
  used_profile_name text,
  used_policy_name text,
  used_default boolean,
  effective_fanout_limit int4,
  effective_segment_groups text[],
  effective_relation_family text,
  effective_segment_labels text[],
  candidate_shards regclass[]
)
AS $$
DECLARE
  use_exact boolean;
  has_overrides boolean;
  default_profile text;
  rels regclass[];
  r_path text;
  r_profile text;
  r_policy text;
  r_default boolean := false;
  r_fanout int4 := fanout_limit;
  r_groups text[];
  r_family text;
  r_labels text[];
  p_policy text;
  p_groups text[];
  p_family text;
  p_fanout int4;
  p_labels text[];
BEGIN
  IF route_key IS NOT NULL AND route_value IS NOT NULL THEN
    RAISE EXCEPTION 'sorted_heap_graph_route_plan: provide route_key or route_value, not both';
  END IF;
  IF route_key IS NULL AND route_value IS NULL THEN
    RAISE EXCEPTION 'sorted_heap_graph_route_plan: must provide route_key or route_value';
  END IF;
  use_exact := route_key IS NOT NULL;

  IF profile_name IS NOT NULL AND policy_name IS NOT NULL THEN
    RAISE EXCEPTION 'sorted_heap_graph_route_plan: provide profile_name or policy_name, not both';
  END IF;

  IF profile_name IS NOT NULL THEN
    IF segment_groups IS NOT NULL OR relation_family IS NOT NULL
       OR segment_labels IS NOT NULL OR fanout_limit != 0
    THEN
      RAISE EXCEPTION 'sorted_heap_graph_route_plan: profile_name cannot be combined with segment_groups, relation_family, segment_labels, or fanout_limit';
    END IF;
  END IF;

  IF policy_name IS NOT NULL AND segment_groups IS NOT NULL THEN
    RAISE EXCEPTION 'sorted_heap_graph_route_plan: policy_name cannot be combined with segment_groups';
  END IF;

  IF profile_name IS NOT NULL THEN
    r_path := 'profile';
    r_profile := profile_name;
    SELECT p.policy_name, p.segment_groups, p.relation_family, p.fanout_limit, p.segment_labels
    INTO p_policy, p_groups, p_family, p_fanout, p_labels
    FROM @extschema@.sorted_heap_graph_route_profile_resolve(route_name, profile_name) AS p;
    r_policy := p_policy;
    r_fanout := p_fanout;
    r_groups := p_groups;
    r_family := p_family;
    r_labels := p_labels;
  ELSIF policy_name IS NOT NULL THEN
    r_path := 'policy';
    r_policy := policy_name;
    r_groups := @extschema@.sorted_heap_graph_route_policy_groups(route_name, policy_name);
    r_family := relation_family;
    r_labels := segment_labels;
  ELSIF segment_groups IS NOT NULL OR relation_family IS NOT NULL
        OR segment_labels IS NOT NULL OR fanout_limit != 0
  THEN
    r_path := 'explicit';
    r_groups := segment_groups;
    r_family := relation_family;
    r_labels := segment_labels;
  ELSE
    SELECT d.profile_name INTO default_profile
    FROM @extschema@.sorted_heap_graph_route_default_registry d
    WHERE d.route_name = sorted_heap_graph_route_plan.route_name;

    IF default_profile IS NOT NULL THEN
      r_path := 'default';
      r_default := true;
      r_profile := default_profile;
      SELECT p.policy_name, p.segment_groups, p.relation_family, p.fanout_limit, p.segment_labels
      INTO p_policy, p_groups, p_family, p_fanout, p_labels
      FROM @extschema@.sorted_heap_graph_route_profile_resolve(route_name, default_profile) AS p;
      r_policy := p_policy;
      r_fanout := p_fanout;
      r_groups := p_groups;
      r_family := p_family;
      r_labels := p_labels;
    ELSE
      r_path := 'base';
    END IF;
  END IF;

  IF use_exact THEN
    SELECT array_agg(rel ORDER BY priority DESC, rel)
    INTO rels
    FROM @extschema@.sorted_heap_graph_exact_resolve(
      route_name, route_key, COALESCE(r_fanout, 0),
      r_groups, r_family, r_labels);
  ELSE
    SELECT array_agg(rel ORDER BY route_min, route_max, rel)
    INTO rels
    FROM @extschema@.sorted_heap_graph_segment_resolve(
      route_name, route_value, COALESCE(r_fanout, 0),
      r_groups, r_family, r_labels);
  END IF;

  RETURN QUERY SELECT
    CASE WHEN use_exact THEN 'exact'::text ELSE 'range'::text END,
    r_path,
    r_profile,
    r_policy,
    r_default,
    COALESCE(r_fanout, 0),
    r_groups,
    r_family,
    r_labels,
    rels;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION @extschema@.sorted_heap_graph_route_plan(text, text, int8, text, text, int4, text[], text, text[])
IS 'Explain which routing path sorted_heap_graph_route would take for the given parameters. Returns the resolution path, resolved profile/policy/default, effective filter knobs, and the candidate shard list. Uses the same resolution order as sorted_heap_graph_route.';
