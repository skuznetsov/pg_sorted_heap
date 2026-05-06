---
layout: default
title: Online Lossy-PK Maintenance
nav_order: 18
---

# Spec: Online Compact/Merge for Lossy PK Types

Status: design-ready, implementation pending
Risk tier: CAUTION
Primary goal: define the requirements for supporting online compact/merge on
UUID, text, and varchar primary keys without replay-key collisions.

## Problem

Offline `sorted_heap_compact(...)` and `sorted_heap_merge(...)` work for UUID,
`text COLLATE "C"`, and `varchar COLLATE "C"` primary keys. Zone maps store a
lossy order-preserving `int64` key for those types:

- UUID: first 8 bytes;
- text/varchar: first 8 bytes, zero padded.

That representation is conservative enough for pruning because collisions only
reduce pruning precision. It is not safe for online replay.

Online compact/merge currently use:

- an `int8` log-table key;
- a backend hash table keyed by `int64` primary-key representation;
- replay logic that maps the logged key back to a tuple in the replacement
  relation.

If two distinct UUID/text/varchar primary keys collapse to the same `int64`,
online replay can delete or update the wrong row. The current implementation
therefore rejects those primary-key types, and SH13 regression covers
fail-closed online compact/merge behavior for UUID, text, and varchar.

## Current Contract

Supported online compact/merge PK types:

- int2;
- int4;
- int8;
- timestamp;
- timestamptz;
- date.

Unsupported online compact/merge PK types:

- uuid;
- text;
- varchar.

For unsupported types, use offline `sorted_heap_compact(...)` or
`sorted_heap_merge(...)`.

## Non-Goals

- Do not reuse the lossy zone-map key for replay identity.
- Do not rely on hash equality without a full-key equality check.
- Do not support non-C collation text/varchar online maintenance until the
  ordering and equality contract is explicit.
- Do not change zone-map encoding as part of this feature; pruning and replay
  identity are separate concerns.

## Required Design

### Lossless replay key

The online log must store enough data to uniquely identify the original primary
key:

- fixed-width pass-by-value keys can stay compact;
- UUID should store the full 16-byte value;
- text/varchar should store the full byte sequence plus enough collation/type
  metadata to compare correctly;
- composite keys need all key attributes, not just the first column.

### Hash table semantics

The replay hash table may use a hash digest for lookup speed, but equality must
compare the full key. A digest collision must degrade to a small collision
chain, not to wrong-row replay.

### Log schema v2

The next online log table should be shape-explicit. The log table is temporary
to one online operation, so there is no long-lived on-disk compatibility
problem, but the shape should still be self-describing enough for debugging.

Recommended v2 shape:

```sql
CREATE UNLOGGED TABLE _sh_compact_log_<relid> (
    id bigserial,
    action char(1) NOT NULL,
    pk_typid oid NOT NULL,
    pk_collation oid NOT NULL,
    pk_key bytea NOT NULL
);
```

`pk_key` is a lossless canonical byte representation for one primary-key
attribute:

- `int2/int4/int8`, `timestamp`, `timestamptz`, `date`: binary canonical
  representation, preserving the current supported semantics.
- `uuid`: raw 16 UUID bytes.
- `text/varchar COLLATE "C"`: detoasted bytes without lossy truncation.
- non-C text/varchar: still fail-closed until collation ordering/equality is
  explicitly implemented and tested.

The log schema must be safe across transaction boundaries while the online
operation is running. It does not need to survive the operation after cleanup.

The old `pk_val int8` log shape should not be kept as the internal default.
Keeping two replay paths increases risk without a user-visible compatibility
benefit, because online log tables are created and dropped inside one
operation.

### Key codec

Extract replay identity into a small internal abstraction:

```text
SortedHeapReplayKey {
  Oid typid;
  Oid collation;
  Size len;
  uint8 bytes[len];
}
```

Required operations:

- encode a tuple key from trigger OLD/NEW rows;
- encode a tuple key from copy-phase rows;
- reconstruct a scan Datum for replay `I`/`U` lookup when the type is supported;
- compare full key equality for hash-map collision chains;
- hash bytes for lookup speed, never for identity.

This abstraction must be separate from the zone-map key encoder. Zone maps may
remain lossy for pruning; replay keys must be lossless.

### Replay lookup

Replay must find rows in the replacement relation by full PK equality. For
text/varchar, comparison must follow the same semantics used by the primary-key
btree opclass/collation, not by the zone-map byte prefix.

Replay has two lookup directions:

- `D`/old half of PK-changing `U`: remove the copied row from the replacement
  relation via the copy-phase full-key -> TID map.
- `I`/new half of PK-changing `U`: scan the original relation's primary-key
  index with a reconstructed Datum, then insert the current row version into
  the replacement relation.

The copy-phase map must not be keyed by `int64`. Use hash digest -> collision
chain, and compare `(typid, collation, len, bytes)` before treating an entry as
the target row.

### Crash and cleanup

The existing online compact/merge crash model must remain true:

- original table remains authoritative until final swap;
- interrupted operation must not leave a half-applied replacement relation;
- orphaned temp/log artifacts must be safe to remove or ignore;
- retry must produce correct data.

## Acceptance Tests

### L1. UUID prefix collision

Create two UUID primary keys with the same first 8 bytes and different trailing
bytes. Run online compact while concurrently updating/deleting one of them.

Expected:

- both rows remain distinguishable;
- replay affects only the intended row;
- final table matches an exact heap oracle.

### L2. Text prefix collision

Create two `text COLLATE "C"` primary keys sharing the first 8 bytes. Run
online merge while changing one row.

Expected:

- replay uses full text equality, not the first-8-byte zone-map key;
- no duplicate or missing primary keys after swap.

### L3. Composite key replay

Create a composite PK where the first column is identical and the second column
differs. Mutate rows during online compact.

Expected:

- replay identity uses the full composite key;
- final table matches exact pre/post DML semantics.

### L4. Non-C collation stays blocked

Create a text PK with non-C collation.

Expected:

- online compact/merge remains fail-closed unless the future design explicitly
  supports that collation.

### L5. Crash during replay

Crash during the replay loop after several UUID/text updates.

Expected:

- original relation remains usable after recovery;
- retry completes;
- final table passes primary-key uniqueness and row-content oracle checks.

## Implementation Direction

Preferred order:

1. Keep current fail-closed checks.
2. Replace the internal online log with v2 `pk_key bytea` even for already
   supported scalar PKs, while preserving existing int/date/timestamp behavior.
3. Extract replay-key encoding into a small internal abstraction.
4. Add lossless key equality and hashing for single-column UUID.
5. Add UUID collision regression with concurrent DML and online compact.
6. Add UUID online merge regression.
7. Extend to `text/varchar COLLATE "C"` with deliberate first-8-byte
   collisions.
8. Extend to composite keys only after single-column lossless replay is proven.
9. Add crash/concurrent tests before broadening the supported type list.

Do not enable UUID/text by merely removing the current fail-closed guard. The
guard should move only after the v2 log path and collision tests are in place.

## Pre-Mortem

Likely breakages:

- DML trigger logs a lossy or toasted pointer instead of durable bytes.
- Replay hash map uses digest equality and silently corrupts colliding keys.
- `I`/`U` replay reconstructs a Datum with the wrong type or collation.
- Text support accidentally claims non-C collation safety.
- Crash cleanup misses the new log-table shape.

Fastest detection signals:

- UUID/text first-8-byte collision tests under concurrent online compact/merge.
- `make test-concurrent` for existing int/date/timestamp regressions.
- crash-recovery smoke with replay-in-progress once UUID path is enabled.

## Quadrumvirate Notes

Cassandra:

- Likely failure mode: replacing `int64` with a hash digest but forgetting
  full-key equality.
- Likely failure mode: supporting UUID and accidentally implying text/varchar
  collation safety.

Daedalus:

- Reframe from "make zone-map key lossless" to "separate pruning key from
  replay identity". Pruning can stay lossy; replay identity cannot.

Maieutic:

- Assumption: online replay only needs the first PK column. Counterexample:
  composite PK updates and tenant-local identifiers.
- Assumption: a hash table key is identity. False unless equality compares the
  full logical key.

Adversary:

- Collision tests must use deliberately colliding UUID/text prefixes.
- Crash tests must cover replay-in-progress, not only copy-in-progress.
- Non-C collation must stay blocked until explicitly designed.
