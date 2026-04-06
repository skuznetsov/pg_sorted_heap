# FlashHadamard Tech Debt

## Ranges scorer anomaly: `n_ranges == shortlist_m`

**Status:** Parked (not a product blocker — pruning is parked at 103K)

**Bug:** `fh_packed_score_ranges_topk` produces anomalous recall when
`n_ranges == shortlist_m` (the topk parameter). Specifically:

- shortlist_m=12, nprobe=12 → recall@10 drops to ~3% (expected ~75%)
- shortlist_m=20, nprobe=12 → recall@10 is normal (~80%)
- shortlist_m=12, nprobe=11 or nprobe=13 → normal

The bug also appears at `n_ranges == FH_MAX_THREADS` (16) with shortlist_m=12.

**Location:** `src/flashhadamard.c:525` (`fh_packed_score_ranges_topk`)

**Reproduce:**
```sql
-- Requires: gutenberg_local (103K × 2880D), k-means store
\i sql/flashhadamard_experimental.sql

CREATE TEMP TABLE _q AS SELECT id, embedding FROM gutenberg_local ORDER BY id DESC LIMIT 50;

-- Ground truth
CREATE TEMP TABLE _gt AS SELECT q.id AS qid, r.row_id FROM _q q,
  LATERAL flashhadamard_store_scan('/tmp/fh_gutenberg_kmeans.store',
    q.embedding, 10, 12, 42, 4, 100) r;

-- Bug: nprobe=12 with shortlist_m=12
CREATE TEMP TABLE _p12 AS SELECT q.id AS qid, r.row_id FROM _q q,
  LATERAL flashhadamard_store_scan('/tmp/fh_gutenberg_kmeans.store',
    q.embedding, 10, 12, 42, 4, 12) r;

-- Expected ~75%, actual ~3%
SELECT round(100.0 * count(*) / 500, 1) || '%' AS recall
FROM _p12 p JOIN _gt g ON g.qid = p.qid AND g.row_id = p.row_id;
```

**Notes:**
- Single-query results look correct; aggregate recall over 50 queries drops
- Parity gate (nprobe >= n_segments → exhaustive) is unaffected
- Exhaustive scorer (`fh_packed_score_topk_t`) has no such bug
- The ranges scorer also loses inline-fallback results when n_ranges > FH_MAX_THREADS
  due to `*filled = 0` reset before thread merge (line ~653)

**Impact:** Only affects pruned search (not exhaustive). Pruning is parked at 103K.
Needs fixing before any future pruning work at 500K+.

## Experimental execution model: pthread in PG backend

The parallel scorer uses `pthread_create` inside a PG backend process.
This works but is not integrated with PG's signal handling, cancel
protocol, or error recovery. Acceptable for experimental use; not
production-hardened.

## Intel/AVX2 int16 kernel: refuted

The NEON int16 LUT approach (256-entry int16 tables + widening accumulation)
is 2× slower on Intel Xeon Platinum 8223CL (AWS c5.large) than the float
LUT path. The ~9% end-to-end win is **Apple Silicon NEON-specific**.
A different kernel design (e.g., AVX-512 VPERM, gather-based scoring)
would be needed for Intel; the current int16 LUT approach does not transfer.
