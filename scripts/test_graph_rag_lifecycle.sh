#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# GraphRAG lifecycle test for pg_sorted_heap
# ============================================================
#
# Verifies the GraphRAG fact-graph surface across:
# - extension upgrade 0.12.0 -> 0.13.0
# - alias-schema registration
# - pg_dump / pg_restore of the registry-backed alias mapping
# - pg_dump / pg_restore of segmented/routed GraphRAG registries
#
# Usage: ./scripts/test_graph_rag_lifecycle.sh [tmp_root] [port]

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
PORT="${2:-65498}"

if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root must be absolute: $TMP_ROOT" >&2
  exit 2
fi
if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [ "$PORT" -le 1024 ] || [ "$PORT" -ge 65535 ]; then
  echo "port must be 1025..65534" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if command -v pg_config >/dev/null 2>&1; then
  PG_BINDIR="$(pg_config --bindir)"
else
  PG_BINDIR="/opt/homebrew/Cellar/postgresql@18/18.1_1/bin"
fi

make -C "$ROOT_DIR" install >/dev/null 2>/dev/null || true

pass=0
fail=0
total=0

check() {
  local name="$1" expected="$2" actual="$3"
  total=$((total + 1))
  if [ "$expected" = "$actual" ]; then
    echo "  PASS: $name"
    pass=$((pass + 1))
  else
    echo "  FAIL: $name (expected=$expected actual=$actual)"
    fail=$((fail + 1))
  fi
}

TMP_DIR=""
cleanup() {
  if [ -n "$TMP_DIR" ] && [ -d "$TMP_DIR/data" ]; then
    "$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -m fast stop >/dev/null 2>&1 || true
  fi
  if [ -n "$TMP_DIR" ]; then
    rm -rf "$TMP_DIR"
  fi
}
trap cleanup EXIT

TMP_DIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_graphrag_lifecycle.XXXXXX")"
"$PG_BINDIR/initdb" -D "$TMP_DIR/data" -A trust --no-locale >/dev/null 2>&1

cat >> "$TMP_DIR/data/postgresql.conf" <<'PGCONF'
log_min_messages = warning
shared_preload_libraries = ''
PGCONF

"$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" -l "$TMP_DIR/postmaster.log" \
  -o "-k $TMP_DIR -p $PORT" start >/dev/null

PSQL() {
  "$PG_BINDIR/psql" -h "$TMP_DIR" -p "$PORT" "$@" -v ON_ERROR_STOP=1 -qtAX
}

DB="graph_rag_lifecycle"
"$PG_BINDIR/createdb" -h "$TMP_DIR" -p "$PORT" "$DB"

echo "=== GraphRAG lifecycle test ==="

signature_sql() {
  local rel="$1" query="$2" relation_path="$3" ann_k="$4" top_k="$5" score_mode="$6"
  cat <<SQL
SELECT COALESCE(
  string_agg(
    format('%s:%s:%s:%s', entity_id, relation_id, target_id, payload),
    '|' ORDER BY distance, entity_id, relation_id, target_id
  ),
  ''
)
FROM sorted_heap_graph_rag(
  '${rel}'::regclass,
  '${query}'::svec,
  relation_path := ${relation_path},
  ann_k := ${ann_k},
  top_k := ${top_k},
  score_mode := '${score_mode}',
  limit_rows := 0
);
SQL
}

path_signature_sql() {
  local rel="$1" query="$2" ann_k="$3" top_k="$4" hop1="$5" hop2="$6"
  cat <<SQL
SELECT COALESCE(
  string_agg(
    format('%s:%s:%s:%s', entity_id, relation_id, target_id, payload),
    '|' ORDER BY distance, entity_id, relation_id, target_id
  ),
  ''
)
FROM sorted_heap_graph_rag_twohop_path_scan(
  '${rel}'::regclass,
  '${query}'::svec,
  ${ann_k},
  ${top_k},
  ${hop1},
  ${hop2},
  0
);
SQL
}

routed_default_signature_sql() {
  local route_name="$1" route_value="$2" query="$3" relation_path="$4" ann_k="$5" top_k="$6" score_mode="$7"
  cat <<SQL
SELECT COALESCE(
  string_agg(
    format('%s:%s:%s:%s:%s', source_rel::text, entity_id, relation_id, target_id, payload),
    '|' ORDER BY distance, entity_id, relation_id, target_id, source_rel::text
  ),
  ''
)
FROM sorted_heap_graph_rag_routed_default(
  '${route_name}',
  ${route_value},
  '${query}'::svec,
  relation_path := ${relation_path},
  ann_k := ${ann_k},
  top_k := ${top_k},
  score_mode := '${score_mode}',
  limit_rows := 0
);
SQL
}

routed_exact_default_signature_sql() {
  local route_name="$1" route_key="$2" query="$3" relation_path="$4" ann_k="$5" top_k="$6" score_mode="$7"
  cat <<SQL
SELECT COALESCE(
  string_agg(
    format('%s:%s:%s:%s:%s', source_rel::text, entity_id, relation_id, target_id, payload),
    '|' ORDER BY distance, entity_id, relation_id, target_id, source_rel::text
  ),
  ''
)
FROM sorted_heap_graph_rag_routed_exact_default(
  '${route_name}',
  '${route_key}',
  '${query}'::svec,
  relation_path := ${relation_path},
  ann_k := ${ann_k},
  top_k := ${top_k},
  score_mode := '${score_mode}',
  limit_rows := 0
);
SQL
}

unified_route_range_signature_sql() {
  local route_name="$1" route_value="$2" query="$3" relation_path="$4" ann_k="$5" top_k="$6" score_mode="$7"
  cat <<SQL
SELECT COALESCE(
  string_agg(
    format('%s:%s:%s:%s:%s', source_rel::text, entity_id, relation_id, target_id, payload),
    '|' ORDER BY distance, entity_id, relation_id, target_id, source_rel::text
  ),
  ''
)
FROM sorted_heap_graph_route(
  '${route_name}',
  '${query}'::svec,
  relation_path := ${relation_path},
  route_value := ${route_value},
  ann_k := ${ann_k},
  top_k := ${top_k},
  score_mode := '${score_mode}',
  limit_rows := 0
);
SQL
}

unified_route_exact_signature_sql() {
  local route_name="$1" route_key="$2" query="$3" relation_path="$4" ann_k="$5" top_k="$6" score_mode="$7"
  cat <<SQL
SELECT COALESCE(
  string_agg(
    format('%s:%s:%s:%s:%s', source_rel::text, entity_id, relation_id, target_id, payload),
    '|' ORDER BY distance, entity_id, relation_id, target_id, source_rel::text
  ),
  ''
)
FROM sorted_heap_graph_route(
  '${route_name}',
  '${query}'::svec,
  relation_path := ${relation_path},
  route_key := '${route_key}',
  ann_k := ${ann_k},
  top_k := ${top_k},
  score_mode := '${score_mode}',
  limit_rows := 0
);
SQL
}

route_plan_sql() {
  local route_name="$1"
  local key_param="$2"  # 'route_key := ...' or 'route_value := ...'
  cat <<SQL
SELECT route_kind || ':' || resolution_path || ':' || COALESCE(used_profile_name, '') || ':' || used_default::text || ':' || COALESCE(array_to_string(candidate_shards::text[], ','), '')
FROM sorted_heap_graph_route_plan('${route_name}', ${key_param});
SQL
}

config_sql() {
  local rel="$1"
  cat <<SQL
SELECT format(
  '%s|%s|%s|%s|%s|%s',
  entity_column,
  relation_column,
  target_column,
  embedding_column,
  payload_column,
  is_registered
)
FROM sorted_heap_graph_config('${rel}'::regclass);
SQL
}

index_exists_sql() {
  local idx="$1"
  cat <<SQL
SELECT CASE WHEN count(*) = 1 THEN 't' ELSE 'f' END
FROM pg_class c
JOIN pg_am am ON am.oid = c.relam
WHERE c.relname = '${idx}'
  AND am.amname = 'sorted_hnsw';
SQL
}

# --- Phase 1: install 0.12 and verify pre-upgrade beta wrapper ---
PSQL "$DB" -c "CREATE EXTENSION pg_sorted_heap VERSION '0.12.0'"

extver=$(PSQL "$DB" -c "SELECT extversion FROM pg_extension WHERE extname = 'pg_sorted_heap'")
check "version_before_upgrade" "0.12.0" "$extver"

PSQL "$DB" <<'SQL'
CREATE TABLE facts_v12 (
  entity_id   int4 NOT NULL,
  relation_id int2 NOT NULL,
  target_id   int4 NOT NULL,
  embedding   svec(4) NOT NULL,
  payload     text NOT NULL,
  PRIMARY KEY (entity_id, relation_id, target_id)
) USING sorted_heap;

INSERT INTO facts_v12 VALUES
  (1, 1, 2, '[1,0,0,0]'::svec, 'a'),
  (1, 2, 3, '[0.9,0.1,0,0]'::svec, 'b'),
  (2, 1, 4, '[0,1,0,0]'::svec, 'c'),
  (3, 1, 1, '[0,0,1,0]'::svec, 'd'),
  (3, 2, 5, '[0,0,0.9,0.1]'::svec, 'e'),
  (4, 1, 6, '[0,0,0,1]'::svec, 'f');

SELECT sorted_heap_compact('facts_v12'::regclass);
ANALYZE facts_v12;
SQL

pre_upgrade_count=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_rag_twohop_path_scan('facts_v12'::regclass, '[0,0,1,0]'::svec, 2, 2, 1, 2, 0)")
check "pre_upgrade_wrapper_rows" "1" "$pre_upgrade_count"

pre_upgrade_path_sig=$(PSQL "$DB" -c "$(path_signature_sql facts_v12 '[0,0,1,0]' 2 2 1 2)")
check "pre_upgrade_path_signature_nonempty" "t" "$([ -n "$pre_upgrade_path_sig" ] && echo t || echo f)"

# --- Phase 2: upgrade to 0.13 and verify unified surface ---
PSQL "$DB" -c "ALTER EXTENSION pg_sorted_heap UPDATE TO '0.13.0'"

extver=$(PSQL "$DB" -c "SELECT extversion FROM pg_extension WHERE extname = 'pg_sorted_heap'")
check "version_after_upgrade" "0.13.0" "$extver"

canonical_cfg=$(PSQL "$DB" -c "$(config_sql facts_v12)")
check "canonical_config_defaults" "entity_id|relation_id|target_id|embedding|payload|f" "$canonical_cfg"

post_upgrade_path_sig=$(PSQL "$DB" -c "$(path_signature_sql facts_v12 '[0,0,1,0]' 2 2 1 2)")
check "post_upgrade_path_signature" "$pre_upgrade_path_sig" "$post_upgrade_path_sig"

post_upgrade_unified_sig=$(PSQL "$DB" -c "$(signature_sql facts_v12 '[0,0,1,0]' 'ARRAY[1,2]' 2 2 path)")
check "unified_path_signature" "$pre_upgrade_path_sig" "$post_upgrade_unified_sig"

# --- Phase 3: alias registration + dump/restore ---
PSQL "$DB" <<'SQL'
CREATE TABLE facts_alias (
  src_id    int4 NOT NULL,
  edge_type int2 NOT NULL,
  dst_id    int4 NOT NULL,
  vec       svec(4) NOT NULL,
  body      text NOT NULL,
  PRIMARY KEY (src_id, edge_type, dst_id)
) USING sorted_heap;

INSERT INTO facts_alias VALUES
  (1, 1, 2, '[1,0,0,0]'::svec, 'a'),
  (1, 2, 3, '[0.9,0.1,0,0]'::svec, 'b'),
  (2, 1, 4, '[0,1,0,0]'::svec, 'c'),
  (3, 1, 1, '[0,0,1,0]'::svec, 'd'),
  (3, 2, 5, '[0,0,0.9,0.1]'::svec, 'e'),
  (4, 1, 6, '[0,0,0,1]'::svec, 'f');

SELECT sorted_heap_compact('facts_alias'::regclass);
CREATE INDEX facts_alias_vec_idx ON facts_alias USING sorted_hnsw (vec svec_cosine_ops);
ANALYZE facts_alias;

SELECT sorted_heap_graph_register(
  'facts_alias'::regclass,
  entity_column := 'src_id',
  relation_column := 'edge_type',
  target_column := 'dst_id',
  embedding_column := 'vec',
  payload_column := 'body'
);
SQL

registry_rows=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_registry")
check "registry_rows_before_dump" "1" "$registry_rows"

alias_cfg=$(PSQL "$DB" -c "$(config_sql facts_alias)")
check "alias_config_registered" "src_id|edge_type|dst_id|vec|body|t" "$alias_cfg"

alias_index=$(PSQL "$DB" -c "$(index_exists_sql facts_alias_vec_idx)")
check "alias_sorted_hnsw_index_before_dump" "t" "$alias_index"

alias_unified_sig=$(PSQL "$DB" -c "$(signature_sql facts_alias '[0,0,1,0]' 'ARRAY[1,2]' 2 2 path)")
check "alias_unified_signature_nonempty" "t" "$([ -n "$alias_unified_sig" ] && echo t || echo f)"

alias_path_sig=$(PSQL "$DB" -c "$(path_signature_sql facts_alias '[0,0,1,0]' 2 2 1 2)")
check "alias_path_signature" "$alias_unified_sig" "$alias_path_sig"

PSQL "$DB" <<'SQL'
CREATE TABLE facts_seg_a (
  entity_id   int4 NOT NULL,
  relation_id int2 NOT NULL,
  target_id   int4 NOT NULL,
  embedding   svec(4) NOT NULL,
  payload     text NOT NULL,
  PRIMARY KEY (entity_id, relation_id, target_id)
) USING sorted_heap;

CREATE TABLE facts_seg_b (
  entity_id   int4 NOT NULL,
  relation_id int2 NOT NULL,
  target_id   int4 NOT NULL,
  embedding   svec(4) NOT NULL,
  payload     text NOT NULL,
  PRIMARY KEY (entity_id, relation_id, target_id)
) USING sorted_heap;

INSERT INTO facts_seg_a VALUES
  (8, 1, 18, '[-1,0,0,0]'::svec, 'm1'),
  (18, 2, 28, '[-0.8,-0.2,0,0]'::svec, 'm2'),
  (28, 3, 38, '[-0.6,-0.4,0,0]'::svec, 'm3');

INSERT INTO facts_seg_b VALUES
  (9, 1, 19, '[0,1,0,0]'::svec, 'n1'),
  (19, 2, 29, '[0,0.8,0.2,0]'::svec, 'n2'),
  (29, 3, 39, '[0,0.6,0.4,0]'::svec, 'n3');

SELECT sorted_heap_compact('facts_seg_a'::regclass);
SELECT sorted_heap_compact('facts_seg_b'::regclass);
ANALYZE facts_seg_a;
ANALYZE facts_seg_b;

SELECT sorted_heap_graph_segment_meta_register('facts_seg_a'::regclass, 'hot', 'left', ARRAY['hot','mutable']);
SELECT sorted_heap_graph_segment_meta_register('facts_seg_b'::regclass, 'sealed', 'right', ARRAY['sealed','archive']);

SELECT sorted_heap_graph_segment_register('lifecycle_route', 'facts_seg_a'::regclass, 1, 8);
SELECT sorted_heap_graph_segment_register('lifecycle_route', 'facts_seg_b'::regclass, 9, 16);

SELECT sorted_heap_graph_exact_register('lifecycle_exact', 'both', 'facts_seg_a'::regclass, 100);
SELECT sorted_heap_graph_exact_register('lifecycle_exact', 'both', 'facts_seg_b'::regclass, 50);

SELECT sorted_heap_graph_route_policy_register('lifecycle_route', 'prefer_sealed', ARRAY['sealed','hot']);
SELECT sorted_heap_graph_route_policy_register('lifecycle_exact', 'prefer_sealed', ARRAY['sealed','hot']);

SELECT sorted_heap_graph_route_profile_register('lifecycle_route', 'sealed_right', 'prefer_sealed', NULL, 'right', 1, ARRAY['archive']);
SELECT sorted_heap_graph_route_profile_register('lifecycle_exact', 'sealed_right', 'prefer_sealed', NULL, 'right', 1, ARRAY['archive']);

SELECT sorted_heap_graph_route_default_register('lifecycle_route', 'sealed_right');
SELECT sorted_heap_graph_route_default_register('lifecycle_exact', 'sealed_right');
SQL

segment_meta_rows=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_segment_meta_config()")
check "segment_meta_rows_before_dump" "2" "$segment_meta_rows"

segment_meta_labels=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_segment_meta_config() WHERE array_to_string(segment_labels, ',') IN ('hot,mutable', 'sealed,archive')")
check "segment_meta_labels_before_dump" "2" "$segment_meta_labels"

segment_rows=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_segment_config('lifecycle_route')")
check "segment_rows_before_dump" "2" "$segment_rows"

segment_shared_sources=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_segment_catalog('lifecycle_route') WHERE segment_group_source = 'shared' AND relation_family_source = 'shared'")
check "segment_catalog_shared_before_dump" "2" "$segment_shared_sources"

segment_label_sources=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_segment_catalog('lifecycle_route') WHERE segment_labels_source = 'shared' AND array_to_string(effective_segment_labels, ',') IN ('hot,mutable', 'sealed,archive')")
check "segment_catalog_labels_before_dump" "2" "$segment_label_sources"

exact_rows=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_exact_config('lifecycle_exact')")
check "exact_rows_before_dump" "2" "$exact_rows"

exact_shared_sources=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_exact_catalog('lifecycle_exact', 'both') WHERE segment_group_source = 'shared' AND relation_family_source = 'shared'")
check "exact_catalog_shared_before_dump" "2" "$exact_shared_sources"

exact_label_sources=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_exact_catalog('lifecycle_exact', 'both') WHERE segment_labels_source = 'shared' AND array_to_string(effective_segment_labels, ',') IN ('hot,mutable', 'sealed,archive')")
check "exact_catalog_labels_before_dump" "2" "$exact_label_sources"

policy_rows=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_route_policy_config() WHERE route_name IN ('lifecycle_route', 'lifecycle_exact')")
check "route_policy_rows_before_dump" "2" "$policy_rows"

profile_rows=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_route_profile_config() WHERE route_name IN ('lifecycle_route', 'lifecycle_exact')")
check "route_profile_rows_before_dump" "2" "$profile_rows"

default_rows=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_route_default_config() WHERE route_name IN ('lifecycle_route', 'lifecycle_exact')")
check "route_default_rows_before_dump" "2" "$default_rows"

default_label_routes=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_route_catalog() WHERE route_name IN ('lifecycle_route', 'lifecycle_exact') AND array_to_string(default_segment_labels, ',') = 'archive'")
check "route_default_labels_before_dump" "2" "$default_label_routes"

lifecycle_routed_sig=$(PSQL "$DB" -c "$(routed_default_signature_sql lifecycle_route 10 '[0,1,0,0]' 'ARRAY[1,2]' 2 2 path)")
check "routed_default_signature_nonempty" "t" "$([ -n "$lifecycle_routed_sig" ] && echo t || echo f)"

lifecycle_exact_routed_sig=$(PSQL "$DB" -c "$(routed_exact_default_signature_sql lifecycle_exact both '[0,1,0,0]' 'ARRAY[1,2]' 2 2 path)")
check "routed_exact_default_signature" "$lifecycle_routed_sig" "$lifecycle_exact_routed_sig"

# Unified router: sorted_heap_graph_route (range path, uses default profile)
unified_range_sig=$(PSQL "$DB" -c "$(unified_route_range_signature_sql lifecycle_route 10 '[0,1,0,0]' 'ARRAY[1,2]' 2 2 path)")
check "unified_route_range_nonempty" "t" "$([ -n "$unified_range_sig" ] && echo t || echo f)"
check "unified_route_range_matches_routed_default" "$lifecycle_routed_sig" "$unified_range_sig"

# Unified router: sorted_heap_graph_route (exact-key path, uses default profile)
unified_exact_sig=$(PSQL "$DB" -c "$(unified_route_exact_signature_sql lifecycle_exact both '[0,1,0,0]' 'ARRAY[1,2]' 2 2 path)")
check "unified_route_exact_nonempty" "t" "$([ -n "$unified_exact_sig" ] && echo t || echo f)"
check "unified_route_exact_matches_routed_default" "$lifecycle_exact_routed_sig" "$unified_exact_sig"

# Route plan: verify resolution paths
range_plan=$(PSQL "$DB" -c "$(route_plan_sql lifecycle_route "route_value := 10")")
check "route_plan_range_nonempty" "t" "$([ -n "$range_plan" ] && echo t || echo f)"
check "route_plan_range_default" "t" "$(echo "$range_plan" | grep -c 'default' | awk '{print ($1>0)?"t":"f"}')"

exact_plan=$(PSQL "$DB" -c "$(route_plan_sql lifecycle_exact "route_key := 'both'")")
check "route_plan_exact_nonempty" "t" "$([ -n "$exact_plan" ] && echo t || echo f)"
check "route_plan_exact_default" "t" "$(echo "$exact_plan" | grep -c 'default' | awk '{print ($1>0)?"t":"f"}')"

"$PG_BINDIR/pg_dump" -h "$TMP_DIR" -p "$PORT" -Fc "$DB" -f "$TMP_DIR/graph_rag.fc" 2>/dev/null
"$PG_BINDIR/dropdb" -h "$TMP_DIR" -p "$PORT" "$DB"
"$PG_BINDIR/createdb" -h "$TMP_DIR" -p "$PORT" "$DB"
"$PG_BINDIR/pg_restore" -h "$TMP_DIR" -p "$PORT" -d "$DB" "$TMP_DIR/graph_rag.fc" 2>/dev/null

restored_extver=$(PSQL "$DB" -c "SELECT extversion FROM pg_extension WHERE extname = 'pg_sorted_heap'")
check "version_after_restore" "0.13.0" "$restored_extver"

restored_registry_rows=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_registry")
check "registry_rows_after_restore" "1" "$restored_registry_rows"

restored_alias_cfg=$(PSQL "$DB" -c "$(config_sql facts_alias)")
check "alias_config_after_restore" "$alias_cfg" "$restored_alias_cfg"

restored_alias_index=$(PSQL "$DB" -c "$(index_exists_sql facts_alias_vec_idx)")
check "alias_sorted_hnsw_index_after_restore" "t" "$restored_alias_index"

restored_alias_unified_sig=$(PSQL "$DB" -c "$(signature_sql facts_alias '[0,0,1,0]' 'ARRAY[1,2]' 2 2 path)")
check "alias_unified_signature_after_restore" "$alias_unified_sig" "$restored_alias_unified_sig"

restored_alias_path_sig=$(PSQL "$DB" -c "$(path_signature_sql facts_alias '[0,0,1,0]' 2 2 1 2)")
check "alias_path_signature_after_restore" "$alias_path_sig" "$restored_alias_path_sig"

restored_segment_meta_rows=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_segment_meta_config()")
check "segment_meta_rows_after_restore" "2" "$restored_segment_meta_rows"

restored_segment_meta_labels=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_segment_meta_config() WHERE array_to_string(segment_labels, ',') IN ('hot,mutable', 'sealed,archive')")
check "segment_meta_labels_after_restore" "2" "$restored_segment_meta_labels"

restored_segment_rows=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_segment_config('lifecycle_route')")
check "segment_rows_after_restore" "2" "$restored_segment_rows"

restored_segment_shared_sources=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_segment_catalog('lifecycle_route') WHERE segment_group_source = 'shared' AND relation_family_source = 'shared'")
check "segment_catalog_shared_after_restore" "2" "$restored_segment_shared_sources"

restored_segment_label_sources=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_segment_catalog('lifecycle_route') WHERE segment_labels_source = 'shared' AND array_to_string(effective_segment_labels, ',') IN ('hot,mutable', 'sealed,archive')")
check "segment_catalog_labels_after_restore" "2" "$restored_segment_label_sources"

restored_exact_rows=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_exact_config('lifecycle_exact')")
check "exact_rows_after_restore" "2" "$restored_exact_rows"

restored_exact_shared_sources=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_exact_catalog('lifecycle_exact', 'both') WHERE segment_group_source = 'shared' AND relation_family_source = 'shared'")
check "exact_catalog_shared_after_restore" "2" "$restored_exact_shared_sources"

restored_exact_label_sources=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_exact_catalog('lifecycle_exact', 'both') WHERE segment_labels_source = 'shared' AND array_to_string(effective_segment_labels, ',') IN ('hot,mutable', 'sealed,archive')")
check "exact_catalog_labels_after_restore" "2" "$restored_exact_label_sources"

restored_policy_rows=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_route_policy_config() WHERE route_name IN ('lifecycle_route', 'lifecycle_exact')")
check "route_policy_rows_after_restore" "2" "$restored_policy_rows"

restored_profile_rows=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_route_profile_config() WHERE route_name IN ('lifecycle_route', 'lifecycle_exact')")
check "route_profile_rows_after_restore" "2" "$restored_profile_rows"

restored_default_rows=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_route_default_config() WHERE route_name IN ('lifecycle_route', 'lifecycle_exact')")
check "route_default_rows_after_restore" "2" "$restored_default_rows"

restored_default_label_routes=$(PSQL "$DB" -c "SELECT count(*) FROM sorted_heap_graph_route_catalog() WHERE route_name IN ('lifecycle_route', 'lifecycle_exact') AND array_to_string(default_segment_labels, ',') = 'archive'")
check "route_default_labels_after_restore" "2" "$restored_default_label_routes"

restored_lifecycle_routed_sig=$(PSQL "$DB" -c "$(routed_default_signature_sql lifecycle_route 10 '[0,1,0,0]' 'ARRAY[1,2]' 2 2 path)")
check "routed_default_signature_after_restore" "$lifecycle_routed_sig" "$restored_lifecycle_routed_sig"

restored_lifecycle_exact_routed_sig=$(PSQL "$DB" -c "$(routed_exact_default_signature_sql lifecycle_exact both '[0,1,0,0]' 'ARRAY[1,2]' 2 2 path)")
check "routed_exact_default_signature_after_restore" "$lifecycle_exact_routed_sig" "$restored_lifecycle_exact_routed_sig"

# Unified router after restore
restored_unified_range_sig=$(PSQL "$DB" -c "$(unified_route_range_signature_sql lifecycle_route 10 '[0,1,0,0]' 'ARRAY[1,2]' 2 2 path)")
check "unified_route_range_after_restore" "$unified_range_sig" "$restored_unified_range_sig"

restored_unified_exact_sig=$(PSQL "$DB" -c "$(unified_route_exact_signature_sql lifecycle_exact both '[0,1,0,0]' 'ARRAY[1,2]' 2 2 path)")
check "unified_route_exact_after_restore" "$unified_exact_sig" "$restored_unified_exact_sig"

# Route plan after restore
restored_range_plan=$(PSQL "$DB" -c "$(route_plan_sql lifecycle_route "route_value := 10")")
check "route_plan_range_after_restore" "$range_plan" "$restored_range_plan"

restored_exact_plan=$(PSQL "$DB" -c "$(route_plan_sql lifecycle_exact "route_key := 'both'")")
check "route_plan_exact_after_restore" "$exact_plan" "$restored_exact_plan"

PSQL "$DB" -c "SELECT sorted_heap_compact('facts_alias'::regclass)" >/dev/null
post_restore_compact_sig=$(PSQL "$DB" -c "$(signature_sql facts_alias '[0,0,1,0]' 'ARRAY[1,2]' 2 2 path)")
check "alias_signature_after_restore_compact" "$alias_unified_sig" "$post_restore_compact_sig"

server_running=$("$PG_BINDIR/pg_ctl" -D "$TMP_DIR/data" status 2>&1 | grep -c 'server is running' || true)
check "no_crashes" "1" "$server_running"

echo ""
echo "graph_rag_lifecycle_test status=$([ "$fail" -eq 0 ] && echo ok || echo FAILED) pass=$pass fail=$fail total=$total"
[ "$fail" -eq 0 ] || exit 1
