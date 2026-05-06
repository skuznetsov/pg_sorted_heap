---
layout: default
title: Online Lossy-PK Maintenance
nav_order: 18
---

# Spec: Online Compact/Merge for Lossy PK Types

Status: proposed
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
therefore rejects those primary-key types, and SH13 regression covers the
fail-closed behavior.

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

## Required Future Design

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

### Log schema

A future log table should be versioned or shape-explicit. Candidate approaches:

- typed columns per PK attribute for supported scalar cases;
- binary serialized key plus typid/collation metadata;
- composite Datum serialization with a strict memory-context and detoast
  policy.

The log schema must be safe across transaction boundaries while the online
operation is running. It does not need to survive the operation after cleanup.

### Replay lookup

Replay must find rows in the replacement relation by full PK equality. For
text/varchar, comparison must follow the same semantics used by the primary-key
btree opclass/collation, not by the zone-map byte prefix.

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
2. Extract replay-key encoding into a small internal abstraction.
3. Add lossless key equality and hashing for single-column UUID.
4. Extend to text/varchar with C collation.
5. Extend to composite keys only after single-column lossless replay is proven.
6. Add crash/concurrent tests before broadening the supported type list.

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
