# pg_sorted_heap TODO

## Closed: 0.14 stabilization gate

Risk tier: SAFE for local docs/tests/bench smokes; escalate before any
CAUTION-tier code rewrite, dependency change, or remote operation.

Definition of Done:
- `git diff --check`
- `make installcheck REGRESS='pg_sorted_heap sorted_hnsw graph_rag'`
- PostgreSQL 16 `installcheck` for the same regression set
- `make test-release` or a documented failure/blocked gate with root cause
- `pg_upgrade` 16->18 and 17->18
- release archive contract and installcheck from unpacked archive
- small benchmark smokes for witness bulk load and partition/vector paths
- `docs/release-0.14.0.md` reflects the actual verified 0.14 surface

Checklist:
- [x] Reconfirm packaging/version/upgrade metadata for 0.14.
- [x] Run core regression and release gates.
- [x] Run focused performance sanity smokes for new 0.14 paths.
- [x] Update release notes with current features, caveats, and verified gates.
- [x] Record remaining blockers or mark this checklist closed.

Status: closed on 2026-05-14. No release blockers were found in the local
stabilization pass. Follow-up compatibility gates rechecked PostgreSQL 16
`installcheck`, `pg_upgrade` 16->18, the release archive contract, and
installcheck from an unpacked `git archive` on PostgreSQL 18. The only local
caveat is that `cfmem` side memory could not start because the Homebrew
`llama.cpp` dylib path is stale; this does not affect the PostgreSQL extension
gates.

Use the focused docs instead of maintaining a second stale project-status copy:

- `README.md` - landing-page overview and release positioning.
- `docs/index.md` - documentation map.
- `docs/api-stable.md` - compact application-facing SQL surface.
- `docs/api.md` - complete SQL reference.
- `docs/stability-matrix.md` - stable, beta, experimental, and legacy/manual boundaries.
- `docs/functional-gap-specs.md` - current functional gap specifications.
- `docs/spec-*.md` - focused design specs for larger follow-up tracks.

When a new implementation task starts, add only the active checklist here and
move durable results into the relevant doc once the work is complete.
