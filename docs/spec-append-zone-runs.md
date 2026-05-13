---
layout: default
title: Append-Zone Runs
nav_order: 20
---

# Spec: Append-Zone Run Metadata

Status: design guardrail
Risk tier: CAUTION
Primary goal: capture the safe contract for future append-run metadata without
using page-level zone maps as a stronger proof than they provide.

## Problem

The tempting optimization is to treat newly inserted sorted batches as
"append-zone runs" and later merge them cheaply with existing sorted storage.
That can reduce online compaction pressure and improve insert locality.

The current durable zone map is not enough to make that safe. It stores
page-level min/max ranges. That proves page range pruning, but it does not
prove tuple order inside a page and it does not prove a whole appended region is
internally sorted.

Using page min/max metadata as sorted-run input would risk silently producing
incorrect physical order during merge.

## Required Contract

A future append-run registry must be proof-bearing:

- record relation filenode or a rewrite-safe storage identity;
- record inclusive block range for the run;
- record first and last lexicographic key;
- record key arity and supported key type OIDs;
- record whether row order inside every page in the run was certified;
- invalidate on rewrite, truncate, unlogged reset, restore, partition attach,
  and any operation that can move tuples outside the certified path;
- fail closed to regular compact/merge when proof is missing or stale.

Certification can come from a controlled insert path that sorts the batch and
keeps every emitted page in nondecreasing key order. It cannot be inferred from
the current zone-map min/max fields alone.

## Non-Goals

- Do not merge from current zone-map entries as if they were sorted runs.
- Do not use this metadata to bypass MVCC visibility.
- Do not make cross-run ordering claims after arbitrary UPDATE/DELETE churn
  unless replay/invalidation rules re-certify the affected range.

## Future Acceptance Tests

- Append two sorted batches, certify two runs, merge, and verify physical key
  order against `ORDER BY`.
- Append an unsorted batch through a non-certified path and verify the registry
  is absent or invalid.
- Run compact, truncate, restore, and partition attach/detach and verify stale
  run metadata is not used.
- Force a mixed-page edge case and verify the implementation falls back instead
  of treating page min/max as tuple-order proof.

## Decision

For `0.14`, do not implement durable append-zone merge acceleration from the
existing zone map. Keep sorted multi-insert and zone-map pruning as separate
features, and require a new proof-bearing run registry before using appended
regions as merge inputs.
