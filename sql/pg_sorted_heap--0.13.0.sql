-- pg_sorted_heap extension SQL

\echo Use "CREATE EXTENSION pg_sorted_heap" to load this file.

CREATE DOMAIN @extschema@.clustered_locator AS bytea
	CHECK (octet_length(VALUE) = 16);

CREATE FUNCTION @extschema@.version()
RETURNS text
AS '$libdir/pg_sorted_heap', 'pg_sorted_heap_version'
LANGUAGE C STRICT;

CREATE FUNCTION @extschema@.observability()
RETURNS text
AS '$libdir/pg_sorted_heap', 'pg_sorted_heap_observability'
LANGUAGE C STRICT;

CREATE FUNCTION @extschema@.pg_sorted_heap_observability()
RETURNS text
AS '$libdir/pg_sorted_heap', 'pg_sorted_heap_observability'
LANGUAGE C STRICT;

CREATE FUNCTION @extschema@.tableam_handler(internal)
RETURNS table_am_handler
AS '$libdir/pg_sorted_heap', 'pg_sorted_heap_tableam_handler'
LANGUAGE C STRICT;

CREATE FUNCTION @extschema@.pk_index_handler(internal)
RETURNS index_am_handler
AS '$libdir/pg_sorted_heap', 'pg_sorted_heap_pkidx_handler'
LANGUAGE C STRICT;

CREATE FUNCTION @extschema@.locator_pack(bigint, bigint)
RETURNS @extschema@.clustered_locator
AS '$libdir/pg_sorted_heap', 'pg_sorted_heap_locator_pack'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION @extschema@.locator_pack_int8(bigint)
RETURNS @extschema@.clustered_locator
AS '$libdir/pg_sorted_heap', 'pg_sorted_heap_locator_pack_int8'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION @extschema@.locator_major(@extschema@.clustered_locator)
RETURNS bigint
AS '$libdir/pg_sorted_heap', 'pg_sorted_heap_locator_major'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION @extschema@.locator_minor(@extschema@.clustered_locator)
RETURNS bigint
AS '$libdir/pg_sorted_heap', 'pg_sorted_heap_locator_minor'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION @extschema@.locator_to_hex(@extschema@.clustered_locator)
RETURNS text
AS '$libdir/pg_sorted_heap', 'pg_sorted_heap_locator_to_hex'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION @extschema@.locator_cmp(@extschema@.clustered_locator, @extschema@.clustered_locator)
RETURNS int
AS '$libdir/pg_sorted_heap', 'pg_sorted_heap_locator_cmp'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION @extschema@.locator_lt(@extschema@.clustered_locator, @extschema@.clustered_locator)
RETURNS boolean
LANGUAGE SQL STRICT IMMUTABLE AS
$$ SELECT @extschema@.locator_cmp($1, $2) < 0 $$;

CREATE FUNCTION @extschema@.locator_le(@extschema@.clustered_locator, @extschema@.clustered_locator)
RETURNS boolean
LANGUAGE SQL STRICT IMMUTABLE AS
$$ SELECT @extschema@.locator_cmp($1, $2) <= 0 $$;

CREATE FUNCTION @extschema@.locator_eq(@extschema@.clustered_locator, @extschema@.clustered_locator)
RETURNS boolean
LANGUAGE SQL STRICT IMMUTABLE AS
$$ SELECT @extschema@.locator_cmp($1, $2) = 0 $$;

CREATE FUNCTION @extschema@.locator_ge(@extschema@.clustered_locator, @extschema@.clustered_locator)
RETURNS boolean
LANGUAGE SQL STRICT IMMUTABLE AS
$$ SELECT @extschema@.locator_cmp($1, $2) >= 0 $$;

CREATE FUNCTION @extschema@.locator_gt(@extschema@.clustered_locator, @extschema@.clustered_locator)
RETURNS boolean
LANGUAGE SQL STRICT IMMUTABLE AS
$$ SELECT @extschema@.locator_cmp($1, $2) > 0 $$;

CREATE FUNCTION @extschema@.locator_ne(@extschema@.clustered_locator, @extschema@.clustered_locator)
RETURNS boolean
LANGUAGE SQL STRICT IMMUTABLE AS
$$ SELECT @extschema@.locator_cmp($1, $2) <> 0 $$;

CREATE OPERATOR @extschema@.< (
	LEFTARG = @extschema@.clustered_locator,
	RIGHTARG = @extschema@.clustered_locator,
	PROCEDURE = @extschema@.locator_lt
);

CREATE OPERATOR @extschema@.<= (
	LEFTARG = @extschema@.clustered_locator,
	RIGHTARG = @extschema@.clustered_locator,
	PROCEDURE = @extschema@.locator_le
);

CREATE OPERATOR @extschema@.>= (
	LEFTARG = @extschema@.clustered_locator,
	RIGHTARG = @extschema@.clustered_locator,
	PROCEDURE = @extschema@.locator_ge
);

CREATE OPERATOR @extschema@.> (
	LEFTARG = @extschema@.clustered_locator,
	RIGHTARG = @extschema@.clustered_locator,
	PROCEDURE = @extschema@.locator_gt
);

CREATE OPERATOR @extschema@.= (
	LEFTARG = @extschema@.clustered_locator,
	RIGHTARG = @extschema@.clustered_locator,
	PROCEDURE = @extschema@.locator_eq,
	NEGATOR = OPERATOR(@extschema@.<>)
);

CREATE OPERATOR @extschema@.<> (
	LEFTARG = @extschema@.clustered_locator,
	RIGHTARG = @extschema@.clustered_locator,
	PROCEDURE = @extschema@.locator_ne
);

CREATE OPERATOR CLASS @extschema@.clustered_locator_ops
DEFAULT FOR TYPE @extschema@.clustered_locator USING btree AS
	OPERATOR        1  <  (@extschema@.clustered_locator, @extschema@.clustered_locator),
	OPERATOR        2  <= (@extschema@.clustered_locator, @extschema@.clustered_locator),
	OPERATOR        3  =  (@extschema@.clustered_locator, @extschema@.clustered_locator),
	OPERATOR        4  >= (@extschema@.clustered_locator, @extschema@.clustered_locator),
	OPERATOR        5  >  (@extschema@.clustered_locator, @extschema@.clustered_locator),
	FUNCTION        1  @extschema@.locator_cmp(@extschema@.clustered_locator, @extschema@.clustered_locator);

CREATE FUNCTION @extschema@.locator_advance_major(@extschema@.clustered_locator, bigint)
RETURNS @extschema@.clustered_locator
AS '$libdir/pg_sorted_heap', 'pg_sorted_heap_locator_advance_major'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION @extschema@.locator_next_minor(@extschema@.clustered_locator, bigint)
RETURNS @extschema@.clustered_locator
AS '$libdir/pg_sorted_heap', 'pg_sorted_heap_locator_next_minor'
LANGUAGE C STRICT IMMUTABLE;

CREATE ACCESS METHOD clustered_heap TYPE TABLE HANDLER @extschema@.tableam_handler;
CREATE ACCESS METHOD clustered_pk_index TYPE INDEX HANDLER @extschema@.pk_index_handler;

CREATE OPERATOR FAMILY @extschema@.clustered_pk_int_ops
USING clustered_pk_index;

CREATE OPERATOR CLASS @extschema@.clustered_pk_int2_ops
DEFAULT FOR TYPE int2 USING clustered_pk_index
FAMILY @extschema@.clustered_pk_int_ops AS
	OPERATOR        1  <  (int2, int2),
	OPERATOR        2  <= (int2, int2),
	OPERATOR        3  =  (int2, int2),
	OPERATOR        4  >= (int2, int2),
	OPERATOR        5  >  (int2, int2),
	FUNCTION        1  btint2cmp(int2, int2);

CREATE OPERATOR CLASS @extschema@.clustered_pk_int4_ops
DEFAULT FOR TYPE int4 USING clustered_pk_index
FAMILY @extschema@.clustered_pk_int_ops AS
	OPERATOR        1  <  (int4, int4),
	OPERATOR        2  <= (int4, int4),
	OPERATOR        3  =  (int4, int4),
	OPERATOR        4  >= (int4, int4),
	OPERATOR        5  >  (int4, int4),
	FUNCTION        1  btint4cmp(int4, int4);

CREATE OPERATOR CLASS @extschema@.clustered_pk_int8_ops
DEFAULT FOR TYPE int8 USING clustered_pk_index
FAMILY @extschema@.clustered_pk_int_ops AS
	OPERATOR        1  <  (int8, int8),
	OPERATOR        2  <= (int8, int8),
	OPERATOR        3  =  (int8, int8),
	OPERATOR        4  >= (int8, int8),
	OPERATOR        5  >  (int8, int8),
	FUNCTION        1  btint8cmp(int8, int8);

ALTER OPERATOR FAMILY @extschema@.clustered_pk_int_ops
USING clustered_pk_index ADD
	OPERATOR        1  <  (int2, int4),
	OPERATOR        2  <= (int2, int4),
	OPERATOR        3  =  (int2, int4),
	OPERATOR        4  >= (int2, int4),
	OPERATOR        5  >  (int2, int4),
	FUNCTION        1  (int2, int4) btint24cmp(int2, int4),
	OPERATOR        1  <  (int4, int2),
	OPERATOR        2  <= (int4, int2),
	OPERATOR        3  =  (int4, int2),
	OPERATOR        4  >= (int4, int2),
	OPERATOR        5  >  (int4, int2),
	FUNCTION        1  (int4, int2) btint42cmp(int4, int2),
	OPERATOR        1  <  (int2, int8),
	OPERATOR        2  <= (int2, int8),
	OPERATOR        3  =  (int2, int8),
	OPERATOR        4  >= (int2, int8),
	OPERATOR        5  >  (int2, int8),
	FUNCTION        1  (int2, int8) btint28cmp(int2, int8),
	OPERATOR        1  <  (int8, int2),
	OPERATOR        2  <= (int8, int2),
	OPERATOR        3  =  (int8, int2),
	OPERATOR        4  >= (int8, int2),
	OPERATOR        5  >  (int8, int2),
	FUNCTION        1  (int8, int2) btint82cmp(int8, int2),
	OPERATOR        1  <  (int4, int8),
	OPERATOR        2  <= (int4, int8),
	OPERATOR        3  =  (int4, int8),
	OPERATOR        4  >= (int4, int8),
	OPERATOR        5  >  (int4, int8),
	FUNCTION        1  (int4, int8) btint48cmp(int4, int8),
	OPERATOR        1  <  (int8, int4),
	OPERATOR        2  <= (int8, int4),
	OPERATOR        3  =  (int8, int4),
	OPERATOR        4  >= (int8, int4),
	OPERATOR        5  >  (int8, int4),
	FUNCTION        1  (int8, int4) btint84cmp(int8, int4);


COMMENT ON ACCESS METHOD clustered_heap IS 'Clustered table access method with directed placement via zone map.';
COMMENT ON ACCESS METHOD clustered_pk_index IS 'Clustered index AM for key discovery (scan callbacks disabled; use btree for queries).';

CREATE FUNCTION @extschema@.sorted_heap_handler(internal)
RETURNS table_am_handler
AS '$libdir/pg_sorted_heap', 'sorted_heap_tableam_handler'
LANGUAGE C STRICT;

CREATE ACCESS METHOD sorted_heap TYPE TABLE
	HANDLER @extschema@.sorted_heap_handler;

COMMENT ON ACCESS METHOD sorted_heap IS 'Sorted heap table access method with LSM-style tiered storage.';

CREATE FUNCTION @extschema@.sorted_heap_zonemap_stats(regclass)
RETURNS text
AS '$libdir/pg_sorted_heap', 'sorted_heap_zonemap_stats'
LANGUAGE C STRICT;

CREATE FUNCTION @extschema@.sorted_heap_compact(regclass)
RETURNS void
AS '$libdir/pg_sorted_heap', 'sorted_heap_compact'
LANGUAGE C STRICT;

CREATE FUNCTION @extschema@.sorted_heap_rebuild_zonemap(regclass)
RETURNS void
AS '$libdir/pg_sorted_heap', 'sorted_heap_rebuild_zonemap_sql'
LANGUAGE C STRICT;

CREATE FUNCTION @extschema@.sorted_heap_scan_stats(
  OUT total_scans bigint,
  OUT blocks_scanned bigint,
  OUT blocks_pruned bigint,
  OUT source text
) RETURNS record
AS '$libdir/pg_sorted_heap', 'sorted_heap_scan_stats'
LANGUAGE C STRICT;

CREATE FUNCTION @extschema@.sorted_heap_reset_stats()
RETURNS void
AS '$libdir/pg_sorted_heap', 'sorted_heap_reset_stats'
LANGUAGE C STRICT;

CREATE FUNCTION @extschema@.sorted_heap_compact_trigger()
RETURNS trigger
AS '$libdir/pg_sorted_heap', 'sorted_heap_compact_trigger'
LANGUAGE C;

CREATE PROCEDURE @extschema@.sorted_heap_compact_online(regclass)
AS '$libdir/pg_sorted_heap', 'sorted_heap_compact_online'
LANGUAGE C;

CREATE FUNCTION @extschema@.sorted_heap_merge(regclass)
RETURNS void
AS '$libdir/pg_sorted_heap', 'sorted_heap_merge'
LANGUAGE C STRICT;

CREATE PROCEDURE @extschema@.sorted_heap_merge_online(regclass)
AS '$libdir/pg_sorted_heap', 'sorted_heap_merge_online'
LANGUAGE C;

COMMENT ON EXTENSION pg_sorted_heap IS 'Physically clustered storage via directed placement in table AM.';

-- ================================================================
-- svec: sorted vector type (float32, variable-length)
-- ================================================================

CREATE FUNCTION @extschema@.svec_in(cstring, oid, int4)
RETURNS @extschema@.svec
AS '$libdir/pg_sorted_heap', 'svec_in'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION @extschema@.svec_out(@extschema@.svec)
RETURNS cstring
AS '$libdir/pg_sorted_heap', 'svec_out'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION @extschema@.svec_typmod_in(cstring[])
RETURNS int4
AS '$libdir/pg_sorted_heap', 'svec_typmod_in'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION @extschema@.svec_recv(internal, oid, int4)
RETURNS @extschema@.svec
AS '$libdir/pg_sorted_heap', 'svec_recv'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION @extschema@.svec_send(@extschema@.svec)
RETURNS bytea
AS '$libdir/pg_sorted_heap', 'svec_send'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE @extschema@.svec (
	INPUT = @extschema@.svec_in,
	OUTPUT = @extschema@.svec_out,
	TYPMOD_IN = @extschema@.svec_typmod_in,
	RECEIVE = @extschema@.svec_recv,
	SEND = @extschema@.svec_send,
	STORAGE = external,
	INTERNALLENGTH = VARIABLE,
	ALIGNMENT = int4
);

COMMENT ON TYPE @extschema@.svec IS 'Sorted vector: float32 array for ANN hashing and cosine distance.';

-- Cosine distance operator: <=>
CREATE FUNCTION @extschema@.svec_cosine_distance(@extschema@.svec, @extschema@.svec)
RETURNS float8
AS '$libdir/pg_sorted_heap', 'svec_cosine_distance'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR @extschema@.<=> (
	LEFTARG = @extschema@.svec,
	RIGHTARG = @extschema@.svec,
	FUNCTION = @extschema@.svec_cosine_distance,
	COMMUTATOR = OPERATOR(@extschema@.<=>)
);

COMMENT ON FUNCTION @extschema@.svec_cosine_distance(@extschema@.svec, @extschema@.svec)
IS 'Cosine distance: 1 - cos(a, b). Range [0, 2].';

-- ================================================================
-- hsvec: half-precision sorted vector type (float16, variable-length)
-- 2 bytes per dimension (vs 4 for svec), max 32000 dimensions.
-- ================================================================

CREATE FUNCTION @extschema@.hsvec_in(cstring, oid, int4)
RETURNS @extschema@.hsvec
AS '$libdir/pg_sorted_heap', 'hsvec_in'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION @extschema@.hsvec_out(@extschema@.hsvec)
RETURNS cstring
AS '$libdir/pg_sorted_heap', 'hsvec_out'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION @extschema@.hsvec_typmod_in(cstring[])
RETURNS int4
AS '$libdir/pg_sorted_heap', 'hsvec_typmod_in'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION @extschema@.hsvec_recv(internal, oid, int4)
RETURNS @extschema@.hsvec
AS '$libdir/pg_sorted_heap', 'hsvec_recv'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION @extschema@.hsvec_send(@extschema@.hsvec)
RETURNS bytea
AS '$libdir/pg_sorted_heap', 'hsvec_send'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE @extschema@.hsvec (
	INPUT = @extschema@.hsvec_in,
	OUTPUT = @extschema@.hsvec_out,
	TYPMOD_IN = @extschema@.hsvec_typmod_in,
	RECEIVE = @extschema@.hsvec_recv,
	SEND = @extschema@.hsvec_send,
	STORAGE = external,
	INTERNALLENGTH = VARIABLE,
	ALIGNMENT = int4
);

COMMENT ON TYPE @extschema@.hsvec IS 'Half-precision vector: float16 array. 2 bytes/dim, max 32000 dims.';

-- Cosine distance operator: <=> (hsvec, hsvec) → float8
CREATE FUNCTION @extschema@.hsvec_cosine_distance(@extschema@.hsvec, @extschema@.hsvec)
RETURNS float8
AS '$libdir/pg_sorted_heap', 'hsvec_cosine_distance'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR @extschema@.<=> (
	LEFTARG = @extschema@.hsvec,
	RIGHTARG = @extschema@.hsvec,
	FUNCTION = @extschema@.hsvec_cosine_distance,
	COMMUTATOR = OPERATOR(@extschema@.<=>)
);

COMMENT ON FUNCTION @extschema@.hsvec_cosine_distance(@extschema@.hsvec, @extschema@.hsvec)
IS 'Cosine distance for half-precision vectors: 1 - cos(a, b). Range [0, 2].';

-- ---- Casts ----

-- hsvec → svec (implicit: allows hsvec in any function expecting svec)
CREATE FUNCTION @extschema@.hsvec_to_svec(@extschema@.hsvec)
RETURNS @extschema@.svec
AS '$libdir/pg_sorted_heap', 'hsvec_to_svec'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE CAST (@extschema@.hsvec AS @extschema@.svec)
	WITH FUNCTION @extschema@.hsvec_to_svec(@extschema@.hsvec)
	AS IMPLICIT;

-- svec → hsvec (assignment: explicit or in INSERT/UPDATE context, lossy)
CREATE FUNCTION @extschema@.svec_to_hsvec(@extschema@.svec)
RETURNS @extschema@.hsvec
AS '$libdir/pg_sorted_heap', 'svec_to_hsvec'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE CAST (@extschema@.svec AS @extschema@.hsvec)
	WITH FUNCTION @extschema@.svec_to_hsvec(@extschema@.svec)
	AS ASSIGNMENT;

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

CREATE TABLE @extschema@.sorted_heap_graph_segment_registry (
  route_name text NOT NULL,
  relid regclass NOT NULL,
  route_min int8 NOT NULL,
  route_max int8 NOT NULL,
  segment_group text,
  PRIMARY KEY (route_name, relid),
  CHECK (route_max >= route_min)
);

SELECT pg_catalog.pg_extension_config_dump('@extschema@.sorted_heap_graph_segment_registry', '');

CREATE FUNCTION @extschema@.sorted_heap_graph_segment_register(
  route_name text,
  rel regclass,
  route_min int8,
  route_max int8,
  segment_group text DEFAULT NULL
) RETURNS void
AS $$
  INSERT INTO @extschema@.sorted_heap_graph_segment_registry (
    route_name, relid, route_min, route_max, segment_group
  )
  VALUES ($1, $2, $3, $4, $5)
  ON CONFLICT (route_name, relid) DO UPDATE SET
    route_min = EXCLUDED.route_min,
    route_max = EXCLUDED.route_max,
    segment_group = EXCLUDED.segment_group;
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
  segment_groups text[] DEFAULT NULL
)
RETURNS TABLE (
  route_name text,
  rel regclass,
  route_min int8,
  route_max int8,
  segment_group text
)
AS $$
  SELECT s.route_name, s.relid, s.route_min, s.route_max, s.segment_group
  FROM @extschema@.sorted_heap_graph_segment_registry s
  WHERE ($1 IS NULL OR s.route_name = $1)
    AND ($2 IS NULL OR s.segment_group = ANY($2))
  ORDER BY s.route_name, s.segment_group, s.route_min, s.route_max, s.relid;
$$ LANGUAGE SQL STABLE;

CREATE FUNCTION @extschema@.sorted_heap_graph_segment_resolve(
  route_name text,
  route_value int8,
  fanout_limit int4 DEFAULT 0,
  segment_groups text[] DEFAULT NULL
) RETURNS TABLE (
  rel regclass,
  route_min int8,
  route_max int8,
  segment_group text
)
AS $$
  SELECT chosen.relid, chosen.route_min, chosen.route_max, chosen.segment_group
  FROM (
    SELECT s.relid, s.route_min, s.route_max, s.segment_group
    FROM @extschema@.sorted_heap_graph_segment_registry s
    WHERE s.route_name = $1
      AND $2 BETWEEN s.route_min AND s.route_max
      AND ($4 IS NULL OR s.segment_group = ANY($4))
    ORDER BY (s.route_max - s.route_min), s.route_min, s.route_max, s.segment_group, s.relid
    LIMIT CASE WHEN $3 IS NULL OR $3 <= 0 THEN NULL ELSE $3 END
  ) chosen
  ORDER BY chosen.route_min, chosen.route_max, chosen.segment_group, chosen.relid;
$$ LANGUAGE SQL STABLE;

CREATE TABLE @extschema@.sorted_heap_graph_exact_registry (
  route_name text NOT NULL,
  route_key text NOT NULL,
  relid regclass NOT NULL,
  priority int4 NOT NULL DEFAULT 0,
  segment_group text,
  PRIMARY KEY (route_name, route_key, relid)
);

SELECT pg_catalog.pg_extension_config_dump('@extschema@.sorted_heap_graph_exact_registry', '');

CREATE FUNCTION @extschema@.sorted_heap_graph_exact_register(
  route_name text,
  route_key text,
  rel regclass,
  priority int4 DEFAULT 0,
  segment_group text DEFAULT NULL
) RETURNS void
AS $$
  INSERT INTO @extschema@.sorted_heap_graph_exact_registry (
    route_name, route_key, relid, priority, segment_group
  )
  VALUES ($1, $2, $3, $4, $5)
  ON CONFLICT (route_name, route_key, relid) DO UPDATE SET
    priority = EXCLUDED.priority,
    segment_group = EXCLUDED.segment_group;
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
  segment_groups text[] DEFAULT NULL
) RETURNS TABLE (
  route_name text,
  route_key text,
  rel regclass,
  priority int4,
  segment_group text
)
AS $$
  SELECT s.route_name, s.route_key, s.relid, s.priority, s.segment_group
  FROM @extschema@.sorted_heap_graph_exact_registry s
  WHERE ($1 IS NULL OR s.route_name = $1)
    AND ($2 IS NULL OR s.route_key = $2)
    AND ($3 IS NULL OR s.segment_group = ANY($3))
  ORDER BY s.route_name, s.route_key, s.priority DESC, s.segment_group, s.relid;
$$ LANGUAGE SQL STABLE;

CREATE FUNCTION @extschema@.sorted_heap_graph_exact_resolve(
  route_name text,
  route_key text,
  fanout_limit int4 DEFAULT 0,
  segment_groups text[] DEFAULT NULL
) RETURNS TABLE (
  rel regclass,
  priority int4,
  segment_group text
)
AS $$
  SELECT chosen.relid, chosen.priority, chosen.segment_group
  FROM (
    SELECT s.relid, s.priority, s.segment_group
    FROM @extschema@.sorted_heap_graph_exact_registry s
    WHERE s.route_name = $1
      AND s.route_key = $2
      AND ($4 IS NULL OR s.segment_group = ANY($4))
    ORDER BY s.priority DESC, s.segment_group, s.relid
    LIMIT CASE WHEN $3 IS NULL OR $3 <= 0 THEN NULL ELSE $3 END
  ) chosen
  ORDER BY chosen.priority DESC, chosen.segment_group, chosen.relid;
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

CREATE FUNCTION @extschema@.sorted_heap_expand_ids(
  rel regclass,
  seed_ids int4[],
  relation_filter int4 DEFAULT NULL,
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  entity_id int4,
  relation_id int2,
  target_id int4,
  embedding @extschema@.svec,
  payload text
)
AS '$libdir/pg_sorted_heap', 'sorted_heap_expand_ids'
LANGUAGE C STABLE;

CREATE FUNCTION @extschema@.sorted_heap_expand_rerank(
  rel regclass,
  seed_ids int4[],
  query @extschema@.svec,
  top_k int4,
  relation_filter int4 DEFAULT NULL,
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS '$libdir/pg_sorted_heap', 'sorted_heap_expand_rerank'
LANGUAGE C STABLE;

CREATE FUNCTION @extschema@.sorted_heap_expand_twohop_rerank(
  rel regclass,
  seed_ids int4[],
  query @extschema@.svec,
  top_k int4,
  hop1_relation_filter int4 DEFAULT NULL,
  hop2_relation_filter int4 DEFAULT NULL,
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS '$libdir/pg_sorted_heap', 'sorted_heap_expand_twohop_rerank'
LANGUAGE C STABLE;

CREATE FUNCTION @extschema@.sorted_heap_expand_twohop_path_rerank(
  rel regclass,
  seed_ids int4[],
  query @extschema@.svec,
  top_k int4,
  hop1_relation_filter int4 DEFAULT NULL,
  hop2_relation_filter int4 DEFAULT NULL,
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS '$libdir/pg_sorted_heap', 'sorted_heap_expand_twohop_path_rerank'
LANGUAGE C STABLE;

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

CREATE FUNCTION @extschema@.sorted_heap_graph_rag_scan(
  rel regclass,
  query @extschema@.svec,
  ann_k int4,
  top_k int4,
  relation_filter int4 DEFAULT NULL,
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS '$libdir/pg_sorted_heap', 'sorted_heap_graph_rag_scan'
LANGUAGE C STABLE;

CREATE FUNCTION @extschema@.sorted_heap_graph_rag_twohop_scan(
  rel regclass,
  query @extschema@.svec,
  ann_k int4,
  top_k int4,
  hop1_relation_filter int4 DEFAULT NULL,
  hop2_relation_filter int4 DEFAULT NULL,
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS '$libdir/pg_sorted_heap', 'sorted_heap_graph_rag_twohop_scan'
LANGUAGE C STABLE;

CREATE FUNCTION @extschema@.sorted_heap_graph_rag_twohop_path_scan(
  rel regclass,
  query @extschema@.svec,
  ann_k int4,
  top_k int4,
  hop1_relation_filter int4 DEFAULT NULL,
  hop2_relation_filter int4 DEFAULT NULL,
  limit_rows int4 DEFAULT 0
) RETURNS TABLE (
  entity_id int4,
  relation_id int2,
  target_id int4,
  payload text,
  distance float8
)
AS '$libdir/pg_sorted_heap', 'sorted_heap_graph_rag_twohop_path_scan'
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
IS 'Unified fact-shaped GraphRAG entry point. relation_path length 1 performs ANN seed on entity_id plus one-hop rerank. Longer relation_path values perform multi-hop endpoint or path-aware rerank depending on score_mode.';

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
  segment_groups text[] DEFAULT NULL
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
  FROM @extschema@.sorted_heap_graph_segment_resolve(route_name, route_value, fanout_limit, segment_groups);

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

COMMENT ON FUNCTION @extschema@.sorted_heap_graph_rag_routed(text, int8, @extschema@.svec, int4[], int4, int4, text, int4, int4, text[])
IS 'Beta routed GraphRAG wrapper. Resolves candidate shards from sorted_heap_graph_segment_registry using a route value plus optional segment-group filtering, then delegates to sorted_heap_graph_rag_segmented(...).';

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
  segment_groups text[] DEFAULT NULL
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
  FROM @extschema@.sorted_heap_graph_exact_resolve(route_name, route_key, fanout_limit, segment_groups);

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

COMMENT ON FUNCTION @extschema@.sorted_heap_graph_rag_routed_exact(text, text, @extschema@.svec, int4[], int4, int4, text, int4, int4, text[])
IS 'Beta exact-key routed GraphRAG wrapper. Resolves candidate shards from sorted_heap_graph_exact_registry using an exact route key plus optional segment-group filtering, then delegates to sorted_heap_graph_rag_segmented(...).';

-- ----------------------------------------------------------------
-- SimHash: 12-bit locality-sensitive hash for svec columns
-- ----------------------------------------------------------------

CREATE FUNCTION @extschema@.sorted_vector_hash(@extschema@.svec, int4 DEFAULT 42)
RETURNS int2
AS '$libdir/pg_sorted_heap', 'sorted_vector_hash'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION @extschema@.sorted_vector_hash(float4[], int4 DEFAULT 42)
RETURNS int2
AS '$libdir/pg_sorted_heap', 'sorted_vector_hash_arr'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.sorted_vector_hash(@extschema@.svec, int4)
IS '12-bit SimHash (LSH for cosine similarity). Deterministic from seed.';

CREATE FUNCTION @extschema@.sorted_vector_vq(@extschema@.svec, n_centroids int4, seed int4 DEFAULT 42)
RETURNS int2
AS '$libdir/pg_sorted_heap', 'sorted_vector_vq'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION @extschema@.sorted_vector_vq(float4[], n_centroids int4, seed int4 DEFAULT 42)
RETURNS int2
AS '$libdir/pg_sorted_heap', 'sorted_vector_vq_arr'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.sorted_vector_vq(@extschema@.svec, int4, int4)
IS 'Random Codebook VQ hash: assign to nearest of n_centroids random unit vectors. Deterministic from seed.';

-- ----------------------------------------------------------------
-- Residual VQ (RVQ): two-level hash for uniform bucket distribution
-- ----------------------------------------------------------------

CREATE FUNCTION @extschema@.sorted_vector_rvq(@extschema@.svec, n_coarse int4, n_fine int4, seed int4 DEFAULT 42)
RETURNS int2
AS '$libdir/pg_sorted_heap', 'sorted_vector_rvq'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION @extschema@.sorted_vector_rvq(float4[], n_coarse int4, n_fine int4, seed int4 DEFAULT 42)
RETURNS int2
AS '$libdir/pg_sorted_heap', 'sorted_vector_rvq_arr'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.sorted_vector_rvq(@extschema@.svec, int4, int4, int4)
IS 'Residual VQ hash: two-level (coarse → residual → fine). hash = coarse×n_fine + fine. Uniform buckets in high-D.';

-- ----------------------------------------------------------------
-- Random Projection VQ (RPVQ): project to low-D, then VQ
-- Best for high-D vectors (>256D) where plain VQ degenerates
-- ----------------------------------------------------------------

CREATE FUNCTION @extschema@.sorted_vector_rpvq(@extschema@.svec, n_proj int4, n_centroids int4, seed int4 DEFAULT 42)
RETURNS int2
AS '$libdir/pg_sorted_heap', 'sorted_vector_rpvq'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION @extschema@.sorted_vector_rpvq(float4[], n_proj int4, n_centroids int4, seed int4 DEFAULT 42)
RETURNS int2
AS '$libdir/pg_sorted_heap', 'sorted_vector_rpvq_arr'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.sorted_vector_rpvq(@extschema@.svec, int4, int4, int4)
IS 'Random Projection VQ: project to n_proj dims then VQ. Best for high-D where plain VQ degenerates.';

-- ----------------------------------------------------------------
-- Centered VQ (CVQ): subtract reference vector (dataset mean), then VQ
-- Solves high-D degeneration where random centroids are orthogonal to data
-- ----------------------------------------------------------------

CREATE FUNCTION @extschema@.sorted_vector_cvq(@extschema@.svec, ref @extschema@.svec, n_centroids int4, seed int4 DEFAULT 42)
RETURNS int2
AS '$libdir/pg_sorted_heap', 'sorted_vector_cvq'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION @extschema@.sorted_vector_cvq(float4[], ref float4[], n_centroids int4, seed int4 DEFAULT 42)
RETURNS int2
AS '$libdir/pg_sorted_heap', 'sorted_vector_cvq_arr'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.sorted_vector_cvq(@extschema@.svec, @extschema@.svec, int4, int4)
IS 'Centered VQ: subtract reference (mean) vector then VQ. Best for tightly clustered high-D data.';

CREATE FUNCTION @extschema@.sorted_vector_cvq_probe(@extschema@.svec, ref @extschema@.svec, n_centroids int4, n_probes int4, seed int4 DEFAULT 42)
RETURNS int2[]
AS '$libdir/pg_sorted_heap', 'sorted_vector_cvq_probe'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.sorted_vector_cvq_probe(@extschema@.svec, @extschema@.svec, int4, int4, int4)
IS 'Multi-probe CVQ: returns top-K nearest CVQ bucket IDs as int2[]. Use with ANY() for multi-bucket scan.';

-- ================================================================
-- Product Quantization (PQ) functions
-- ================================================================

-- Train PQ codebook from a query returning svec vectors.
-- M = number of subvectors, n_iter = k-means iterations,
-- max_samples = max training samples (randomly sampled).
-- Returns codebook ID.
CREATE FUNCTION @extschema@.svec_pq_train(
    source_query text,
    m int4,
    n_iter int4 DEFAULT 10,
    max_samples int4 DEFAULT 10000
)
RETURNS int4
AS '$libdir/pg_sorted_heap', 'svec_pq_train'
LANGUAGE C STRICT;

COMMENT ON FUNCTION @extschema@.svec_pq_train(text, int4, int4, int4)
IS 'Train Product Quantization codebook. M subvectors, 256 centroids each. Returns codebook ID.';

-- Encode a vector to M-byte PQ code using trained codebook.
CREATE FUNCTION @extschema@.svec_pq_encode(@extschema@.svec, cb_id int4 DEFAULT 1)
RETURNS bytea
AS '$libdir/pg_sorted_heap', 'svec_pq_encode'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.svec_pq_encode(@extschema@.svec, int4)
IS 'Encode vector to PQ code (M bytes) using trained codebook.';

-- Asymmetric Distance Computation: estimate squared L2 distance
-- between a query vector and a PQ-encoded vector.
CREATE FUNCTION @extschema@.svec_pq_distance(@extschema@.svec, code bytea, cb_id int4)
RETURNS float8
AS '$libdir/pg_sorted_heap', 'svec_pq_distance'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.svec_pq_distance(@extschema@.svec, bytea, int4)
IS 'ADC distance: estimate squared L2 between query vector and PQ code. Fast (no TOAST reads).';

-- Split ADC for batch queries: precompute table once, lookup per row (power-user API)
CREATE FUNCTION @extschema@.svec_pq_distance_table(@extschema@.svec, cb_id int4 DEFAULT 1)
RETURNS bytea
AS '$libdir/pg_sorted_heap', 'svec_pq_distance_table'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.svec_pq_distance_table(@extschema@.svec, int4)
IS 'Precompute ADC distance table (M×256 floats) for a query vector. Call once per query.';

CREATE FUNCTION @extschema@.svec_pq_adc_lookup(dist_table bytea, code bytea)
RETURNS float8
AS '$libdir/pg_sorted_heap', 'svec_pq_adc_lookup'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.svec_pq_adc_lookup(bytea, bytea)
IS 'ADC lookup: sum precomputed distances using PQ code. O(M) per call — sub-microsecond.';

-- Combined ADC: auto-caches distance table per scan, then O(M) lookup per row.
-- Eliminates the CTE pattern needed with distance_table + adc_lookup.
CREATE FUNCTION @extschema@.svec_pq_adc(@extschema@.svec, code bytea, cb_id int4 DEFAULT 1)
RETURNS float8
AS '$libdir/pg_sorted_heap', 'svec_pq_adc'
LANGUAGE C STRICT STABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.svec_pq_adc(@extschema@.svec, bytea, int4)
IS 'ADC distance with auto-cached distance table. Use in ORDER BY — no CTE needed.';

-- ================================================================
-- IVF (Inverted File Index) functions for IVF-PQ approximate nearest neighbor search.
-- sorted_heap physical clustering by PK prefix acts as the inverted file.
-- ================================================================

-- Train IVF centroids via k-means on full vectors.
-- Returns codebook ID.
CREATE FUNCTION @extschema@.svec_ivf_train(
    source_query text,
    nlist int4,
    n_iter int4 DEFAULT 10,
    max_samples int4 DEFAULT 10000
)
RETURNS int4
AS '$libdir/pg_sorted_heap', 'svec_ivf_train'
LANGUAGE C STRICT;

COMMENT ON FUNCTION @extschema@.svec_ivf_train(text, int4, int4, int4)
IS 'Train IVF centroids via k-means. nlist partitions. Returns codebook ID.';

-- Assign vector to nearest IVF centroid. For use in GENERATED columns.
CREATE FUNCTION @extschema@.svec_ivf_assign(@extschema@.svec, cb_id int4 DEFAULT 1)
RETURNS int2
AS '$libdir/pg_sorted_heap', 'svec_ivf_assign'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.svec_ivf_assign(@extschema@.svec, int4)
IS 'Assign vector to nearest IVF centroid. Returns centroid_id (int2). IMMUTABLE for GENERATED columns.';

-- Find nprobe nearest IVF centroids for a query vector.
CREATE FUNCTION @extschema@.svec_ivf_probe(@extschema@.svec, nprobe int4, cb_id int4 DEFAULT 1)
RETURNS int2[]
AS '$libdir/pg_sorted_heap', 'svec_ivf_probe'
LANGUAGE C STRICT STABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.svec_ivf_probe(@extschema@.svec, int4, int4)
IS 'Find nprobe nearest IVF centroids for query. Returns int2[] of centroid_ids for WHERE partition_id = ANY(...).';

-- ================================================================
-- Convenience: combined IVF + PQ training in one call.
-- ================================================================
CREATE FUNCTION @extschema@.svec_ann_train(
    source_query text,
    nlist int4,
    m int4,
    n_iter int4 DEFAULT 10,
    max_samples int4 DEFAULT 10000
)
RETURNS TABLE(ivf_cb_id int4, pq_cb_id int4)
AS $$
    SELECT @extschema@.svec_ivf_train(source_query, nlist, n_iter, max_samples) AS ivf_cb_id,
           @extschema@.svec_pq_train(source_query, m, n_iter, max_samples) AS pq_cb_id;
$$ LANGUAGE SQL;

COMMENT ON FUNCTION @extschema@.svec_ann_train(text, int4, int4, int4, int4)
IS 'Train both IVF centroids and PQ codebook in one call. Returns (ivf_cb_id, pq_cb_id).';

-- ================================================================
-- ANN search: IVF probe → PQ scan → optional exact rerank
--
-- Usage (PQ-only, fastest):
--   SELECT * FROM svec_ann_search('my_table', query_vec, 10, 10);
--
-- With exact reranking (higher recall):
--   SELECT * FROM svec_ann_search('my_table', query_vec, 10, 10, rerank_topk := 30);
--
-- Table must have columns: id text, partition_id int2, pq_code bytea, embedding (castable to svec)
-- ================================================================
CREATE FUNCTION @extschema@.svec_ann_search(
    tbl       regclass,
    query     @extschema@.svec,
    nprobe    int4 DEFAULT 10,
    lim       int4 DEFAULT 10,
    rerank_topk int4 DEFAULT 0,
    cb_id     int4 DEFAULT 1
)
RETURNS TABLE(id text, distance float8)
AS $$
DECLARE
    _sql text;
BEGIN
    IF rerank_topk > 0 THEN
        -- Two-stage: PQ coarse → exact cosine rerank
        _sql := format(
            'SELECT c.id, c.embedding::@extschema@.svec <=> $1 AS distance
             FROM (
                 SELECT i.id, i.embedding
                 FROM %s i
                 WHERE i.partition_id = ANY(@extschema@.svec_ivf_probe($1, $2, $5))
                 ORDER BY @extschema@.svec_pq_adc($1, i.pq_code, $5)
                 LIMIT $4
             ) c
             ORDER BY c.embedding::@extschema@.svec <=> $1
             LIMIT $3',
            tbl);
        RETURN QUERY EXECUTE _sql USING query, nprobe, lim, rerank_topk, cb_id;
    ELSE
        -- PQ-only (fastest)
        _sql := format(
            'SELECT i.id, @extschema@.svec_pq_adc($1, i.pq_code, $4)::float8 AS distance
             FROM %s i
             WHERE i.partition_id = ANY(@extschema@.svec_ivf_probe($1, $2, $4))
             ORDER BY @extschema@.svec_pq_adc($1, i.pq_code, $4)
             LIMIT $3',
            tbl);
        RETURN QUERY EXECUTE _sql USING query, nprobe, lim, cb_id;
    END IF;
END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.svec_ann_search(regclass, @extschema@.svec, int4, int4, int4, int4)
IS 'IVF-PQ approximate nearest neighbor search. Set rerank_topk > 0 for exact cosine reranking.';

-- ================================================================
-- C-level ANN scan: IVF probe + PQ ADC + rerank in single C call.
-- Eliminates per-row SQL function call overhead for maximum speed.
-- ================================================================
CREATE FUNCTION @extschema@.svec_ann_scan(
    tbl           regclass,
    query         @extschema@.svec,
    nprobe        int4 DEFAULT 10,
    lim           int4 DEFAULT 10,
    rerank_topk   int4 DEFAULT 0,
    cb_id         int4 DEFAULT 1,
    ivf_cb_id     int4 DEFAULT 0,
    pq_column     text DEFAULT 'pq_code',
    sketch_table  text DEFAULT '',
    sketch_topk   int4 DEFAULT 0
)
RETURNS TABLE(id text, distance float8)
AS '$libdir/pg_sorted_heap', 'svec_ann_scan'
LANGUAGE C STRICT STABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.svec_ann_scan(regclass, @extschema@.svec, int4, int4, int4, int4, int4, text, text, int4)
IS 'C-level IVF-PQ ANN scan with optional three-stage rerank. sketch_table names a sidecar table with (partition_id, id, sketch hsvec) for prefix-truncation sketch rerank (MRL embeddings).';

-- ================================================================
-- NSW graph scan: greedy best-first search on btree-backed graph.
-- Graph table: (nid int4 PK, sketch hsvec, neighbors int4[],
--               src_id text, src_tid tid).
-- For index-only scan performance, create:
--   CREATE UNIQUE INDEX ON graph_tbl (nid) INCLUDE (sketch, neighbors);
-- and VACUUM the graph table.
-- ================================================================
CREATE FUNCTION @extschema@.svec_graph_scan(
    tbl           regclass,
    query         @extschema@.svec,
    graph_table   text,
    ef_search     int4 DEFAULT 64,
    lim           int4 DEFAULT 10,
    rerank_topk   int4 DEFAULT 0,
    entry_table   text DEFAULT ''
)
RETURNS TABLE(id text, distance float8)
AS '$libdir/pg_sorted_heap', 'svec_graph_scan'
LANGUAGE C STRICT STABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.svec_graph_scan(regclass, @extschema@.svec, text, int4, int4, int4, text)
IS 'NSW graph search via btree-backed sidecar. rerank_topk>0 pre-filters candidates by sketch distance before exact rerank. entry_table (entry_nid int4, centroid hsvec) enables multi-start NSW from closest centroids (empty=use nid=0). Graph table needs: nid int4 PK, sketch hsvec, neighbors int4[], src_id text, src_tid tid.';

-- ================================================================
-- Residual PQ: train/encode/ADC on residuals (vec - IVF centroid).
-- Standard FAISS approach — removes inter-centroid variance so PQ
-- focuses on fine intra-cluster distinctions, improving recall.
-- ================================================================

-- Train residual PQ codebook: k-means on (vec - nearest centroid).
CREATE FUNCTION @extschema@.svec_pq_train_residual(
    source_query text,
    m int4,
    ivf_cb_id int4 DEFAULT 1,
    n_iter int4 DEFAULT 10,
    max_samples int4 DEFAULT 10000
)
RETURNS int4
AS '$libdir/pg_sorted_heap', 'svec_pq_train_residual'
LANGUAGE C STRICT;

COMMENT ON FUNCTION @extschema@.svec_pq_train_residual(text, int4, int4, int4, int4)
IS 'Train PQ codebook on IVF residuals (vec - centroid). Returns codebook ID. Use with svec_pq_encode_residual.';

-- Encode vector residual (vec - centroid[partition_id]) to PQ code.
CREATE FUNCTION @extschema@.svec_pq_encode_residual(
    @extschema@.svec,
    partition_id int2,
    pq_cb_id int4,
    ivf_cb_id int4 DEFAULT 1
)
RETURNS bytea
AS '$libdir/pg_sorted_heap', 'svec_pq_encode_residual'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.svec_pq_encode_residual(@extschema@.svec, int2, int4, int4)
IS 'PQ-encode residual (vec - IVF centroid). For GENERATED columns in IVF-PQ tables.';

-- Per-centroid ADC distance table for residual PQ.
-- Must be called once per (query, centroid_id) pair.
CREATE FUNCTION @extschema@.svec_pq_distance_table_residual(
    @extschema@.svec,
    centroid_id int2,
    pq_cb_id int4,
    ivf_cb_id int4 DEFAULT 1
)
RETURNS bytea
AS '$libdir/pg_sorted_heap', 'svec_pq_distance_table_residual'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.svec_pq_distance_table_residual(@extschema@.svec, int2, int4, int4)
IS 'Precompute ADC distance table for residual PQ. One table per probed centroid. Use with svec_pq_adc_lookup.';

-- ================================================================
-- Hierarchical HNSW search via PG sidecar tables.
-- Tables built by build_hnsw_graph.py:
--   {prefix}_meta   (entry_nid int4, max_level int2)
--   {prefix}_l0     (nid int4 PK, sketch hsvec, neighbors int4[],
--                   src_id text, src_tid tid)
--   {prefix}_l1..lN (nid int4 PK, sketch hsvec, neighbors int4[])
-- ================================================================
CREATE FUNCTION @extschema@.svec_hnsw_scan(
    tbl           regclass,
    query         @extschema@.svec,
    prefix        text,
    ef_search     int4 DEFAULT 64,
    lim           int4 DEFAULT 10,
    rerank_topk   int4 DEFAULT 0,
    rerank1_topk  int4 DEFAULT 0
)
RETURNS TABLE(id text, distance float8)
AS '$libdir/pg_sorted_heap', 'svec_hnsw_scan'
LANGUAGE C STRICT STABLE PARALLEL SAFE;

COMMENT ON FUNCTION @extschema@.svec_hnsw_scan(regclass, @extschema@.svec, text, int4, int4, int4, int4)
IS 'Hierarchical HNSW search via sidecar PG tables. prefix names the table family: {prefix}_meta, {prefix}_l0..lN, and optionally {prefix}_r1 (nid int4 PK, rerank_vec hsvec). Pipeline: greedy ef=1 descent through upper levels → ef_search beam at L0 (hsvec(384) sketch) → optional dense rerank via {prefix}_r1 keeping top rerank1_topk → exact svec cosine rerank via main table keeping top lim. rerank_topk controls how many L0 candidates reach exact rerank: 0 (default) means no truncation (all ef_search candidates are reranked); 0 < rerank_topk < ef_search truncates before exact rerank, reducing TOAST I/O at the cost of recall; rerank_topk >= ef_search has no effect. rerank1_topk (default 0): set >0 to enable the dense r1 stage (requires {prefix}_r1 sidecar with hsvec prefix embeddings); absent _r1 is silently skipped. r1 is a cold-TOAST knob only — on warm buffer pools the btree lookup overhead exceeds TOAST savings; leave rerank1_topk=0 unless TOAST I/O is the measured bottleneck. Enable sorted_heap.hnsw_cache_l0 for session-local L0+upper cache (~100 MB, built on first call, evicted on relcache invalidation). Recommended operating points (103K × 2880-dim svec, warm pool, hsvec(384) sketch): balanced: ef_search=96 rerank_topk=48 → 0.98 ms p50, 96.8% recall@10; quality: ef_search=96 rerank_topk=0 → 1.83 ms p50, 98.4% recall@10; latency: ef_search=64 rerank_topk=32 → 0.85 ms p50, 92.8% recall@10. Timing breakdown in DEBUG1 when sorted_heap.ann_timing=on.';
-- sorted_hnsw Index Access Method
-- v0.12.0: Index AM for HNSW vector search

-- AM handler function
CREATE FUNCTION @extschema@.sorted_hnsw_handler(internal)
RETURNS index_am_handler
AS '$libdir/pg_sorted_heap', 'sorted_hnsw_handler'
LANGUAGE C STRICT;

-- Register the access method
CREATE ACCESS METHOD sorted_hnsw TYPE INDEX
HANDLER @extschema@.sorted_hnsw_handler;

COMMENT ON ACCESS METHOD sorted_hnsw
IS 'HNSW index for approximate nearest neighbor search on svec and hsvec columns. '
   'Stores SQ8-quantized vectors inline in index tuples for sub-millisecond search. '
   'Usage: CREATE INDEX ON tbl USING sorted_hnsw (col svec_cosine_ops|hsvec_cosine_ops) WITH (m=16, ef_construction=200); '
   'Query: SELECT * FROM tbl ORDER BY col <=> query_vec LIMIT 10; '
   'GUC: sorted_hnsw.ef_search (default 96) controls search beam width.';

-- Operator classes for cosine distance ordering.
CREATE OPERATOR CLASS svec_cosine_ops
DEFAULT FOR TYPE @extschema@.svec USING sorted_hnsw AS
    OPERATOR 1 @extschema@.<=> (@extschema@.svec, @extschema@.svec) FOR ORDER BY float_ops,
    FUNCTION 1 @extschema@.svec_cosine_distance(@extschema@.svec, @extschema@.svec);

CREATE OPERATOR CLASS hsvec_cosine_ops
DEFAULT FOR TYPE @extschema@.hsvec USING sorted_hnsw AS
    OPERATOR 1 @extschema@.<=> (@extschema@.hsvec, @extschema@.hsvec) FOR ORDER BY float_ops,
    FUNCTION 1 @extschema@.hsvec_cosine_distance(@extschema@.hsvec, @extschema@.hsvec);
