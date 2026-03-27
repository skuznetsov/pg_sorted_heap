# Claude Opus GraphRAG Task Brief

This file is the current bounded handoff plan for Claude Opus on
`pg_sorted_heap` GraphRAG work.

The goal is not "find something interesting." The goal is to spend Claude
tokens on the next branch that is both real and reviewable.

## Current verified state

- Stable `0.13` GraphRAG surface is the narrow fact-shaped API centered on
  `sorted_heap_graph_rag(...)`.
- Segmented/routed GraphRAG is still beta.
- Dump/restore lifecycle for GraphRAG control-plane state is now covered,
  including shared/default `segment_labels`.
- GraphRAG docs now explicitly state:
  - `limit_rows` is a work cap, not a final result-count override
  - one-hop `score_mode := 'path'` is intentionally equivalent to
    `score_mode := 'endpoint'`

Recently verified local signals:

- `make test-graphrag-lifecycle`
  - `status=ok pass=48 fail=0 total=48`
- `make test-graphrag-release`
  - SQL regressions passed
  - lifecycle `48/48`
  - crash `22/22`
  - concurrent `40/40`
- `make installcheck REGRESS='graph_rag'`
  - passed after the one-hop/path docs + regression clarification

## Do not waste time here

These two branches were rechecked and are not the right next target:

1. "Routing wrappers have no regression coverage"
   - False.
   - `sql/graph_rag.sql` already exercises:
     - raw routed
     - raw exact-key routed
     - policy-backed routed
     - profile-backed routed
     - default-backed routed

2. "`path_filters` leak in multihop ERROR paths"
   - Not accepted as a real release blocker.
   - The code does not manually `pfree` on every `ERROR` path, but that alone
     is not evidence of a persistent leak in PostgreSQL because statement
     memory contexts are unwound on `ereport(ERROR)`.
   - If you want to challenge this, you need a concrete memory-context
     reproducer, not just grep.

## Primary mission

### Mission: harden the chunked local L0 cache path under integration stress

This is the best use of Claude right now.

Reason:

- `sorted_hnsw` had a real allocator/OOB history recently.
- The chunked local-cache change fixed a real large-scale allocator frontier.
- The current remaining concern is not feature surface but robustness under
  repeated query/build/concurrent-operation paths.

The audit direction that is actually worth following is:

- "chunked cache complexity needs stronger integration testing"

## Scope

Focus only on this branch:

- `src/sorted_hnsw.c`
- `src/hnsw_build.c`
- existing related test harnesses:
  - `scripts/test_graph_rag_concurrent.sh`
  - `scripts/test_crash_recovery.sh`
  - `scripts/test_concurrent_online_ops.sh`
  - `scripts/test_graph_rag_crash_recovery.sh`
- `Makefile`
- release-facing docs only if a new permanent test target is added

Out of scope:

- new routing metadata dimensions
- new routed/profile/default APIs
- benchmark-only tuning work
- docs-only cleanup not tied to this mission
- speculative memory "fixes" without a reproducer

## Required approach

### Phase 1: rediscovery

1. Read the current chunked-cache and related build/load paths in:
   - `src/sorted_hnsw.c`
   - `src/hnsw_build.c`
2. Identify exactly:
   - which local structures are page-backed/chunked now
   - which paths allocate them
   - which operations reuse them
   - which operations rebuild or invalidate them
3. Write down the concrete invariants before changing anything.

Minimum expected output from this phase:

- one paragraph or bullet list of invariants
- one or two concrete failure modes worth testing

### Phase 2: add a deterministic integration harness

Goal:

- stress the chunked local-cache path in a way that would have caught:
  - giant contiguous allocation regressions
  - owner-context misuse
  - reuse/reload breakage across repeated query paths

Preferred shape:

- add or extend one shell harness under `scripts/`
- keep it deterministic
- use an ephemeral cluster
- use enough rows to exercise the real path, but not so many that the test
  becomes flaky or too slow

Suggested scenarios:

1. Build a `sorted_hnsw` index on a non-trivial table.
2. Run repeated KNN queries in one backend to warm/use the local path.
3. Run GraphRAG queries that hit the same ANN/index path repeatedly.
4. Run online compact and online merge with concurrent KNN + GraphRAG workers.
5. Reconnect in a fresh session and repeat the KNN/GraphRAG checks.

What matters:

- no crash
- no memory allocation errors
- no corrupted result signatures
- no invalid index after online ops

### Phase 3: convert it into a release-quality check if it proves stable

If the harness is reliable:

- add a `make` target
- wire it into the release workflow only if runtime is still reasonable

If it is useful but too heavy:

- keep it as a standalone harness
- do not force it into the release bundle
- document why

## Acceptance criteria

Claude should not claim success without all of these:

1. Code or harness change is bounded to this mission.
2. `git diff --check` passes.
3. Existing relevant regressions still pass:
   - at minimum:
     - `make installcheck REGRESS='sorted_hnsw graph_rag'`
4. The new or extended harness passes locally.
5. Final report includes:
   - exact commands run
   - exact pass/fail signals
   - changed files
   - any residual risks

## Nice-to-have, not required

If the main mission is solved cleanly and cheaply, one secondary cleanup is
acceptable:

- add a short operator/developer note explaining what the new harness is
  protecting against

Do not start a second engineering branch if the first one is not fully
closed.

## Deliverable format for Claude

Ask Claude to return:

1. a short summary
2. changed files
3. verification commands + outcomes
4. remaining risks

## Review standard

When Claude comes back, review against this checklist:

- Did he actually test the chunked-cache path, or only touch docs?
- Did he add a reproducer/harness instead of speculative fixes?
- Did he keep the branch bounded?
- Did he preserve existing GraphRAG/routed behavior?
- Did he leave temp artifacts behind?

If any answer is "no", reject or trim the branch before merging.
