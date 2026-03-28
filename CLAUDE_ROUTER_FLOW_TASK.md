# Claude Router Flow Task Brief

This file is the current bounded handoff plan for Claude on the next
GraphRAG productization branch.

The previous branch added the unified routed dispatcher:

- `sorted_heap_graph_route(...)`
- `sorted_heap_graph_route_plan(...)`

That branch is done. The next job is not "more routing primitives." The next
job is to turn the current routed control plane into the first app-facing
operator flow.

## Current verified state

- Narrow stable GraphRAG surface:
  - `sorted_heap_graph_rag(...)`
  - graph config helpers
  - stats/reset helpers
- Segmented/routed GraphRAG remains beta.
- Unified routed dispatcher exists and is regression-covered:
  - `sorted_heap_graph_route(...)`
  - `sorted_heap_graph_route_plan(...)`
- Resolution order is now explicit and tested:
  1. `route_key XOR route_value`
  2. `profile_name XOR policy_name`
  3. explicit profile
  4. explicit policy
  5. explicit call-site routing overrides
  6. route default profile
  7. base exact/range routed wrapper

Recently verified signals:

- `make installcheck REGRESS='graph_rag'`
  - passed after the unified dispatcher landed
- `make installcheck REGRESS='pg_sorted_heap sorted_hnsw graph_rag'`
  - passed after the unified dispatcher landed

## Do not waste time here

These are not the next branch:

1. Another low-level routing wrapper
   - not needed right now
   - the current SQL surface is already broad enough

2. New routing registries / control-plane redesign
   - out of scope
   - reuse the current exact/range + policy/profile/default + metadata model

3. More benchmark work
   - the large-scale segmented/routed benchmark branch is already well
     characterized
   - do not spend time on more routing-performance curves here

## Primary mission

### Mission: define and prove the first app-facing router flow

Target audience:

- application developers
- operators wiring a tenant / knowledge-base / shard-aware GraphRAG path

What "app-facing flow" means here:

- one canonical bootstrap/setup sequence
- one canonical plan/inspect sequence
- one canonical query sequence
- one clear answer to: "what should I call in an app?"

This is not primarily an engine branch. It is a productization / operator-flow
branch built on top of the APIs that already exist.

## Scope

Prefer this shape:

1. docs + examples
2. regression proof of the documented flow
3. only if a real friction point remains, propose the smallest additional
   helper needed

Default allowed files:

- `README.md`
- `docs/api.md`
- `docs/graphrag-segmentation-plan.md`
- `CHANGELOG.md`
- `sql/graph_rag.sql`
- `expected/graph_rag.out`

Allowed only if truly required by a concrete friction point:

- `sql/pg_sorted_heap--0.13.0.sql`
- `sql/pg_sorted_heap--0.12.0--0.13.0.sql`

Out of scope:

- new C code
- benchmark harness changes
- shared-cache work
- registry redesign
- new profile/policy/catalog layers
- another "unified" low-level wrapper unless a real documented gap blocks the
  canonical app flow

## Required approach

### Phase 1: identify the canonical app path

Use the existing surface and answer these questions explicitly:

1. For a tenant / KB style workload, what is the recommended setup path?
2. For a range-routed workload, what is the recommended setup path?
3. What should an application call at query time?
4. When should an operator call `sorted_heap_graph_route_plan(...)`?
5. Which of the older beta wrappers should be treated as implementation
   building blocks rather than the preferred app entry points?

Expected output from this phase:

- one concise "recommended flow" for exact-key routing
- optionally one concise "recommended flow" for range routing
- one short list of "use this" vs "these remain lower-level building blocks"

### Phase 2: prove the flow in regression

Add or refine a regression block that demonstrates the app-facing flow
end-to-end:

1. register shard metadata
2. register exact/range routes
3. register a profile/default if the flow recommends one
4. inspect with `sorted_heap_graph_route_plan(...)`
5. query with `sorted_heap_graph_route(...)`
6. confirm equivalence against the underlying routed path when appropriate

What matters:

- the docs and regression should describe the same canonical flow
- the flow should be easy to copy into an application/service
- resolution order should remain visible rather than magical

### Phase 3: only if needed, propose one tiny helper

Only do this if Phase 1-2 exposes a real usability gap that cannot be solved
cleanly by docs + examples + existing wrappers.

If that happens:

- stop and name the exact friction
- propose one smallest helper
- do not widen into another routing sub-framework

## Acceptance criteria

Claude should not claim success without all of these:

1. The branch stays centered on app-facing router flow, not new routing
   infrastructure.
2. `git diff --check` passes.
3. At minimum:
   - `make installcheck REGRESS='graph_rag'`
4. If any SQL surface changes are made:
   - `make installcheck REGRESS='pg_sorted_heap sorted_hnsw graph_rag'`
5. Final report includes:
   - changed files
   - the recommended app-facing flow
   - exact commands run
   - exact pass/fail signals
   - residual risks

## Nice-to-have, not required

- one short "operator recipe" section in the docs
- one short note clarifying which routed wrappers are lower-level building
  blocks versus the preferred app entry point

## Deliverable format for Claude

Ask Claude to return:

1. a short summary
2. changed files
3. the final recommended flow
4. verification commands + outcomes
5. residual risks

## Review standard

When Claude comes back, review against this checklist:

- Did he keep the branch app-facing, or drift back into low-level routing API
  work?
- Did he prove the documented flow in regression?
- Did he avoid inventing a new control plane?
- If he added a new helper, was it truly necessary?
- Did he leave temp artifacts behind?

If any answer is "no", reject or trim the branch before merging.
