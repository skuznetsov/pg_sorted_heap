# FlashHadamard Retrospective

## Purpose

This note records the high-signal retrospective for the FlashHadamard work so
the conclusions do not get buried in chat logs or intermediate benchmark notes.
It is intentionally narrower than `flashhadamard-note.md`: this file captures
what was done well, what was done poorly, what was actually proven, what remains
unproven, and what the next real paradigm shift would be.

## What Went Well

1. **We pivoted away from the wrong bottleneck.**
   Early engine results were dominated by `SPI + TOAST + row/bytea` overhead.
   We did not keep polishing SQL-side plumbing once the pattern was visible.
   The move to raw file storage, `mmap`, backend-local cache, and then parallel
   scan was the correct regime change.

2. **We built an escalation ladder instead of one giant rewrite.**
   The sequence was productive:
   `SPI+TOAST -> raw file -> mmap -> backend cache -> fused top-k -> parallel`.
   Each step had a measurable latency effect and clarified where the next real
   bottleneck lived.

3. **Negative results were documented instead of rationalized away.**
   The project now has explicit refutations for:
   - IVF at `103K`
   - selective memory routing
   - RP-sort segment construction
   - FH-space k-means segmentation
   - several earlier quantization branches

4. **Parity gates eventually improved the quality of reasoning.**
   Once we required `nprobe = n_segments` to route to the canonical exhaustive
   scorer, pruning results stopped being confounded by scorer divergence.

5. **The work ended in a real serving path, not just a research artifact.**
   The current `103K x 2880D` exhaustive parallel engine path is fast enough to
   be operationally meaningful, not merely an academic prototype.

## What Could Have Been Better

1. **Benchmark discipline should have become canonical earlier.**
   Too many numbers were floating around across different setups:
   helper vs engine, warm vs cold, prefetched query vs table fetch, different
   scorer variants, different stores. The project became much cleaner once one
   canonical operating point was enforced.

2. **Algorithm quality, engine overhead, and harness overhead were conflated for
   too long.**
   "Beats helper" initially blurred:
   - Python/ctypes harness overhead
   - raw kernel throughput
   - PostgreSQL integration overhead

3. **Pruning needed parity gates earlier.**
   Several pruning measurements were not trustworthy until the scorer path was
   unified enough to ensure that `nprobe = n_segments` meant "same answer by
   construction".

4. **Some branches were explored longer than their evidence deserved.**
   Once it became clear that a coarse signal lived in the wrong space, or that a
   path was dominated by execution model mismatch, more sweeps did not add much.

5. **`pthread inside backend` delivered speed before architecture.**
   This was acceptable as an experimental branch, but it means the current speed
   result is stronger as a systems/perf proof than as a fully productized
   PostgreSQL execution model.

## What We Actually Proved

### Verified

1. **FlashHadamard is a real retrieval/serving family.**
   It is not just a paper-style transform trick. It supports a practical
   retrieval path on a real `103K x 2880D` workload.

2. **Exhaustive parallel search is the correct operating point at `103K`.**
   At this scale, exhaustive packed scan beats the more elaborate alternatives
   we tested. IVF is not justified here, and pruning does not yet earn its
   complexity.

3. **The main early engine bottleneck was storage/execution regime, not the
   scoring math alone.**
   The decisive improvement came from eliminating `SPI + TOAST + bytea` from the
   hot path.

4. **The engine path can beat the Python helper benchmark path.**
   This claim is justified when worded narrowly: the in-engine path outperformed
   the Python/ctypes helper benchmark path under the measured setup.

5. **Current pruning at `103K` is not a product win.**
   Content-sort remained the best pruning baseline (`93.5% recall@10` at
   `nprobe=4`), while FH-space RP-sort and FH-space k-means were both refuted.

### Refuted

1. **"Pruning is the next obvious speed win at `103K`."**
   Not with current segment designs. The complexity is not justified relative to
   the already-strong exhaustive path.

2. **"FH-space clustering automatically yields better segment pruning."**
   It did not. Both RP-sort and FH-space k-means underperformed the much simpler
   content-sort baseline.

3. **"The remaining engine gap was just one missing local micro-optimization."**
   The large gains came from execution-regime changes, not tiny scalar tweaks.

## What We Did Not Prove

1. **We did not prove that FlashHadamard is universally better than SQ8.**
   It is competitive and strong on this workload, but not established as a
   universal replacement across quality, portability, and hardware friendliness.

2. **We did not prove that FlashHadamard is universally better than TurboQuant.**
   The work diverged into a practical retrieval family, but not into a full
   head-to-head proof against the original Google implementation or all of its
   intended use cases.

3. **We did not prove that the current in-engine path beats a raw C kernel in a
   perfectly controlled apples-to-apples environment.**
   The measured claim should stay narrower:
   "beats the Python helper benchmark path".

4. **We did not prove that pruning is bad in general.**
   We proved that pruning is not currently a winning product path at `103K`
   under the segment constructions tested.

5. **We did not prove that the current kernel family is near-optimal on CPU.**
   We proved that the current scalar/fused regime is strong, not that it is the
   final hardware-native form.

## Where We Were Better Than Alternatives

1. **Versus float32 exact storage**
   Clear win on storage and still strong retrieval quality.

2. **Versus SQL-side FH storage**
   Clear win. The raw store + `mmap` + cache + parallel regime is qualitatively
   better than `SPI + TOAST + row/bytea`.

3. **Versus IVF at `103K`**
   Clear win. Exhaustive packed scan remained the stronger answer at this scale.

## Where We Were Not Better, Or Failed To Prove It

1. **Versus SQ8 as a universal serving baseline**
   Not proven. SQ8 still has a cleaner hardware-native narrative and may remain
   stronger on some quality-sensitive or SIMD-friendly paths.

2. **Versus better segment-pruning schemes**
   Not proven. Our tested pruning schemes did not beat the simple content-sort
   baseline and did not justify productization at `103K`.

3. **Versus a pure raw-C kernel benchmark**
   Not proven. The current win is over the helper benchmark path, not yet over
   a stripped-down C-only apples-to-apples benchmark with identical execution
   substrate.

## The Next Paradigm Shift (Partially Validated)

The first major paradigm shift was:

> Stop storing and scanning FlashHadamard as SQL blobs.

That unlocked the move from hundreds of milliseconds down to single-digit
milliseconds.

The second paradigm shift is underway:

> Stop computing FlashHadamard as a scalar float-LUT kernel.

**NEON int16 kernel (`FH_INT16=1`):** Replaces 256-entry float32 LUTs with
int16 LUTs + NEON `vaddw_s16` widening accumulation. Integrated behind runtime
env flag. Partially validated on 50 Gutenberg queries:

- ~9% end-to-end speedup (1t: 40.4→36.8ms, 8t: 7.7→7.0ms)
- hit@1 preserved (50/50)
- 1t int16 is quality-identical to float (100% top-10 overlap)
- 8t int16 has 77.6% top-10 overlap (shortlist boundary differences)
- Reranked scores for overlapping results: zero diff

Status: **experimental, not default**. One dataset, one query set. The next
validation step is Intel/AVX to determine if the win is NEON-specific or
generalizes.

This remains more promising than:
- more pruning work at `103K`
- more planner or storage polishing
- more local scalar cleanup

## Recommended Positioning

For the current dataset scale (`103K x 2880D`):

1. **Production-like operating point**
   - exhaustive parallel FlashHadamard engine path

2. **Reference path**
   - Python helper benchmark path

3. **Research branches parked**
   - segment pruning at `103K`
   - RP-sort
   - FH-space k-means

4. **Experimental kernel option**
   - NEON int16 kernel (`FH_INT16=1`): ~9% e2e win, partially validated
   - not default; recommended for testing on Apple/NEON hardware

5. **Next real research frontier**
   - Intel/AVX validation of the same fixed-point kernel idea
   - broader dataset/query validation for NEON path

## One-Paragraph Summary

The project succeeded because it stopped treating FlashHadamard as a local
algorithm tweak and started treating it as a serving-system problem. The big
win came from changing execution regime, not from polishing scalar code in
place. That produced a real exhaustive parallel engine path at `103K`, while
also refuting several attractive but weaker ideas such as IVF-at-this-scale and
FH-space pruning. The next breakthrough will likely not come from more storage
or planner work, but from a hardware-native kernel redesign.
