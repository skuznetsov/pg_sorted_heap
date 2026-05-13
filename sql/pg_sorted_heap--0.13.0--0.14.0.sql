CREATE FUNCTION @extschema@.sorted_heap_zonemap_may_match_int8(target regclass, lower_bound bigint, upper_bound bigint)
RETURNS boolean
AS '$libdir/pg_sorted_heap', 'sorted_heap_zonemap_may_match_int8'
LANGUAGE C STRICT;

COMMENT ON FUNCTION @extschema@.sorted_heap_zonemap_may_match_int8(regclass, bigint, bigint)
IS 'Metadata-only first-key zone-map overlap probe. Returns false only when valid zone-map metadata proves an int8 range cannot match; returns true on stale or unsupported metadata so callers fail open to normal heap execution.';
