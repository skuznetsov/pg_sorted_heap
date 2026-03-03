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
