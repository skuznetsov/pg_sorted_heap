-- pg_sorted_heap upgrade: 0.9.7 → 0.9.8
-- Adds hsvec (half-precision float16 vector) type

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
