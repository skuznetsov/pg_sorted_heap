# GraphRAG on `sorted_heap`

This note evaluates a narrow question:

> Can current `sorted_heap` + current vector search already support a useful
> GraphRAG-style retrieval workflow, or do we need a new storage/model layer?

The conclusion so far is:

- **1-hop fact retrieval by source entity already fits `sorted_heap` well.**
- **Naive SQL join-based multi-hop expansion does not expose much advantage.**
- **`ANY(array_of_seed_ids)` expansion does trigger `SortedHeapScan`, but on
  warm and medium-scale local benchmarks it still loses to heap+btree on
  end-to-end latency despite reading fewer blocks.**
- Narrow C helpers for expansion and fused top-K rerank now exist as:
  - `sorted_heap_expand_ids(...)`
  - `sorted_heap_expand_rerank(...)`
- A one-call convenience wrapper now exists as:
  - `sorted_heap_graph_rag_scan(...)`
- Those helpers materially improve the `sorted_heap` path on the synthetic
  GraphRAG benchmark, though pure heap+btree expansion is still faster on
  this synthetic workload.
- Therefore the next promising primitive was correctly **a narrow C helper**,
  not a new graph storage engine and not a giant monolithic
  `graph_rag_scan()` API.

## Existing anchors

The repository already has the main building blocks:

1. **Zone-map pruning on `sorted_heap`**
   - planner hook + `SortedHeapScan` custom scan
   - supports base-relation restriction on the leading PK columns

2. **Planner-integrated ANN via `sorted_hnsw`**
   - exact ordered results
   - works on both heap tables and `sorted_heap` tables

3. **Legacy graph traversal precedent**
   - `svec_graph_scan()` in `pq.c`
   - this is for ANN sidecar graph navigation, not fact graphs
   - still useful as evidence that the extension can host graph-like traversal
     logic in C

## What was benchmarked

Synthetic fact graph schema:

```sql
CREATE TABLE facts_heap (
    entity_id   int4 NOT NULL,
    relation_id int2 NOT NULL,
    target_id   int4 NOT NULL,
    embedding   svec(32) NOT NULL,
    payload     text NOT NULL,
    PRIMARY KEY (entity_id, relation_id, target_id)
);

CREATE TABLE facts_sh (
    entity_id   int4 NOT NULL,
    relation_id int2 NOT NULL,
    target_id   int4 NOT NULL,
    embedding   svec(32) NOT NULL,
    payload     text NOT NULL,
    PRIMARY KEY (entity_id, relation_id, target_id)
) USING sorted_heap;
```

Both tables also receive the same ANN index:

```sql
CREATE INDEX ... USING sorted_hnsw (embedding) WITH (m = 16, ef_construction = 64);
```

Benchmark harness:

- [`scripts/bench_graph_rag.py`](/Users/sergey/Projects/C/clustered_pg/scripts/bench_graph_rag.py)
- local ephemeral PostgreSQL 18 temp cluster
- deterministic synthetic fact graph
- compares:
  - `hop1_entity`
  - `hop1_entity_relation`
  - `hop2_join`
  - `hop2_in`
  - `seed_expand_join`
  - `seed_expand_in`
  - `seed_expand_rerank_join`
  - `seed_expand_rerank_in`
  - `seed_expand_fn`
  - `seed_expand_rerank_fn`
  - `seed_expand_rerank_topk_fn`
  - `seed_graph_rag_scan_fn`

The key comparison is between:

- **join-shaped expansion**
- **`ANY(array(seed_ids))` expansion**

The second shape is the one that allows `sorted_heap` to expose its pruning
logic directly on `entity_id`.

## Local findings

### Small smoke run

On a tiny graph (`300` entities, `4` edges/entity):

- `facts_sh` reduced buffer hits strongly for:
  - `hop1_entity`
  - `hop1_entity_relation`
  - `hop2_in`
  - `seed_expand_in`
- but end-to-end latency stayed close to heap because the whole dataset was
  fully warm and tiny

Most importantly:

- **join-shaped expansion largely erased the `sorted_heap` advantage**
- **`ANY(array(...))` expansion preserved `SortedHeapScan`**

### Medium warm run

On `20K` entities, `8` edges/entity (`160K` rows total), warm local cache:

- `hop1_entity`
  - heap: `Index Scan`
  - sorted_heap: `Custom Scan:SortedHeapScan`
  - sorted_heap reads fewer blocks and is roughly latency-parity

- `seed_expand_join`
  - bad shape for both
  - sorted_heap is not meaningfully better

- `seed_expand_in`
  - sorted_heap does use `SortedHeapScan`
  - buffer footprint drops
  - but **heap+btree still wins on total latency**

This means:

> current SQL shape can make `sorted_heap` read less, but executor/custom-scan
> overhead can still dominate the total time on warm-medium datasets

### Medium run with lower shared buffers

On `20K` entities, `16` edges/entity (`320K` rows total), `shared_buffers=64MB`:

- `hop1_entity`
  - sorted_heap stayed strong: fewer hits, same-or-better latency

- `seed_expand_join`
  - both paths were much worse
  - heap and sorted_heap were similar, with read noise dominating

- `seed_expand_in`
  - heap: lower latency
  - sorted_heap: fewer touched blocks / lower expansion footprint
  - but **still slower end-to-end**

This is the most important current result:

> On a graph larger than a warm toy dataset, `sorted_heap` already shows the
> expected locality/pruning behavior for seed expansion, but the current
> SQL + `CustomScan` path is not enough to turn that into a consistent latency
> win over heap+btree.

## Design implications

### What not to build first

1. **Not a new graph storage engine**
   - current evidence does not justify that jump
   - 1-hop retrieval is already good on current storage

2. **Not a giant monolithic `svec_graph_rag_scan()`**
   - it would have to combine:
     - ANN seed retrieval
     - graph expansion
     - rerank
   - this is a large surface area
   - it also risks duplicating planner/index logic from `sorted_hnsw`

### What to build next

The next narrow primitive should be something like:

```sql
sorted_heap_expand_ids(
    rel regclass,
    seed_ids int4[],
    relation_filter int2 DEFAULT NULL,
    limit_rows int4 DEFAULT 0
)
```

Why this shape:

- ANN seed retrieval can stay in SQL:
  - `SELECT target_id FROM facts ORDER BY embedding <=> $query LIMIT K`
- expansion becomes a dedicated low-overhead C primitive
- it avoids:
  - repeated executor/planner setup
  - generic `CustomScan` overhead for this narrow use case
- it keeps the product boundary small:
  - “expand these known entity IDs quickly”

That primitive can later be composed into:

1. SQL-only GraphRAG
2. a higher-level helper
3. maybe a monolithic API if the narrow primitive proves valuable

## Helper result

The narrow helpers now exist:

```sql
sorted_heap_expand_ids(
    rel regclass,
    seed_ids int4[],
    relation_filter int4 DEFAULT NULL,
    limit_rows int4 DEFAULT 0
)
RETURNS TABLE (
    entity_id int4,
    relation_id int2,
    target_id int4,
    embedding svec,
    payload text
)
```

and:

```sql
sorted_heap_expand_rerank(
    rel regclass,
    seed_ids int4[],
    query svec,
    top_k int4,
    relation_filter int4 DEFAULT NULL,
    limit_rows int4 DEFAULT 0
)
RETURNS TABLE (
    entity_id int4,
    relation_id int2,
    target_id int4,
    payload text,
    distance float8
)
```

and:

```sql
sorted_heap_graph_rag_scan(
    rel regclass,
    query svec,
    ann_k int4,
    top_k int4,
    relation_filter int4 DEFAULT NULL,
    limit_rows int4 DEFAULT 0
)
RETURNS TABLE (
    entity_id int4,
    relation_id int2,
    target_id int4,
    payload text,
    distance float8
)
```

Their current contract is intentionally narrow:

- relation must be a `sorted_heap` table
- relation must expose the columns:
  - `entity_id int4`
  - `relation_id int2`
  - `target_id int4`
  - `embedding svec`
  - `payload text`
- the function reuses the zone-map range builder directly
- it emits fact rows for known source entity IDs

On the medium-pressure benchmark (`20K` entities, `16` edges/entity,
`320K` rows, `shared_buffers=64MB`, fresh backend, `runs=3`), the helpers
produced:

- `facts_heap seed_expand_in`: `0.123 ms`
- `facts_sh seed_expand_in`: `0.285 ms`
- `facts_sh seed_expand_fn`: `0.165 ms`
- `facts_sh seed_expand_rerank_in`: `0.369 ms`
- `facts_sh seed_expand_rerank_fn`: `0.234 ms`
- `facts_sh seed_expand_rerank_topk_fn`: `0.139 ms`
- `facts_sh seed_graph_rag_scan_fn`: `0.144 ms`

Interpretation:

- `sorted_heap_expand_ids()` converts the observed block-pruning/locality
  advantage into a **real latency win over the current SQL + CustomScan path**
- `sorted_heap_expand_rerank()` removes most of the remaining rerank overhead
  and is now materially faster than the current `sorted_heap` SQL rerank path
  (`0.139 ms` vs `0.369 ms`)
- `sorted_heap_graph_rag_scan()` is only slightly slower than the direct fused
  helper composition (`0.144 ms` vs `0.139 ms`), so the convenience API does
  not erase the win
- pure heap+btree expansion is still faster on this synthetic workload
  (`0.123 ms` vs `0.165 ms`)

Relation-filtered probes narrow that gap further:

- `facts_heap seed_expand_rel_in`: `0.074 ms`
- `facts_sh seed_expand_rel_in`: `0.151 ms`
- `facts_sh seed_expand_rel_fn`: `0.108 ms`
- `facts_heap seed_expand_rerank_rel_in`: `0.087 ms`
- `facts_sh seed_expand_rerank_rel_in`: `0.167 ms`
- `facts_sh seed_expand_rerank_rel_topk_fn`: `0.104 ms`
- `facts_sh seed_graph_rag_rel_scan_fn`: `0.120 ms`

So the relation-filtered GraphRAG path is materially better than the current
SQL + `CustomScan` form, but it still does not clearly beat heap+btree on this
synthetic corpus. The filtered helper path is nevertheless close enough that a
real fact graph, wider payloads, or colder cache state may flip the comparison.

Payload-width sensitivity does matter, but not monotonically.

The benchmark harness now supports `--payload-bytes` to widen synthetic fact
rows and test the claim that locality should matter more once facts stop being
tiny strings. On the same medium-pressure setup (`20K` entities, degree `16`,
`320K` rows, `shared_buffers=64MB`, fresh backend):

- with `payload_bytes=1024`
  - `facts_heap seed_expand_in`: `0.188 ms`
  - `facts_sh seed_expand_in`: `0.185 ms`
  - `facts_heap seed_expand_rerank_rel_in`: `0.120 ms`
  - `facts_sh seed_expand_rerank_rel_topk_fn`: `0.100 ms`
  - `facts_sh seed_graph_rag_rel_scan_fn`: `0.125 ms`

- with `payload_bytes=2048`
  - `facts_heap seed_expand_in`: `0.113 ms`
  - `facts_sh seed_expand_in`: `0.208 ms`
  - `facts_heap seed_expand_rerank_rel_in`: `0.090 ms`
  - `facts_sh seed_expand_rerank_rel_topk_fn`: `0.122 ms`
  - `facts_sh seed_graph_rag_rel_scan_fn`: `0.127 ms`

Interpretation:

- a wider inline payload can make `sorted_heap` competitive or slightly better
  on seed expansion
- but the effect is not monotonic, so "wider payload always helps sorted_heap"
  is false on this synthetic generator
- this synthetic text filler is still a weak proxy for real fact payloads
  because compression/TOAST behavior can change the balance again

So the next falsifier should be a real-dataset GraphRAG harness or a more
realistic payload model, not another synthetic-only extrapolation.

## Real-text Gutenberg graph

A better falsifier now exists in:

- [`scripts/bench_graph_rag_gutenberg.py`](/Users/sergey/Projects/C/clustered_pg/scripts/bench_graph_rag_gutenberg.py)

This harness uses real Gutenberg paragraphs instead of synthetic payload text.
It builds a small text graph:

- relation `1`: `book -> paragraph` (`contains`)
- relation `2`: `paragraph -> next_paragraph` (`next`)

Embeddings are still deterministic lexical hash vectors, not external model
embeddings. That means this harness is good for measuring graph-expansion
latency on real text payloads and a real graph topology, but it is not a
semantic-quality benchmark.

Two useful runs on `shared_buffers=64MB`, fresh backend:

`64 books x 128 paragraphs/book` (`14,549` rows):

- `facts_heap seed_expand_rerank_rel_in`: `0.071 ms`
- `facts_sh seed_expand_rerank_rel_in`: `0.088 ms`
- `facts_sh seed_expand_rerank_rel_topk_fn`: `0.061 ms`
- `facts_sh seed_graph_rag_rel_scan_fn`: `0.084 ms`

`128 books x 256 paragraphs/book` (`58,954` rows):

- `facts_heap seed_expand_rel_in`: `0.073 ms`
- `facts_sh seed_expand_rel_in`: `0.078 ms`
- `facts_sh seed_expand_rel_fn`: `0.069 ms`
- `facts_heap seed_expand_rerank_rel_in`: `0.079 ms`
- `facts_sh seed_expand_rerank_rel_in`: `0.101 ms`
- `facts_sh seed_expand_rerank_rel_topk_fn`: `0.063 ms`
- `facts_sh seed_graph_rag_rel_scan_fn`: `0.089 ms`

This is the first non-synthetic result that materially weakens the earlier
"heap+btree simply wins" story:

- the plain `sorted_heap` SQL path is still worse than heap+btree
- but the fused filtered helper on the real-text Gutenberg graph is already
  at parity or slightly better than heap+btree on the rerank path
- the one-call wrapper is close enough that its overhead is visible but not
  disqualifying

So the narrow-helper direction survives the real-text falsifier better than the
short-payload synthetic benchmark suggested.

## pgvector parity on the real-text graph

The Gutenberg harness also now supports a comparable `pgvector` path on the
same graph:

- ANN seeds come from a `facts_pgv` table with `vector(dim)` + HNSW
- graph expansion and exact rerank still happen in PostgreSQL over the fact
  rows, which is the relevant GraphRAG shape

This is important because a pure ANN benchmark would miss the real product
question: how expensive is "ANN seed + graph expansion + exact rerank" as one
workflow?

On fresh-backend runs with `shared_buffers=64MB`:

`64 books x 128 paragraphs/book` (`14,549` rows):

- heap rerank baseline: `0.064 ms`
- `sorted_heap_expand_rerank(... relation=2)`: `0.060 ms`
- `sorted_heap_graph_rag_scan(... relation=2)`: `0.075 ms`
- `pgvector ANN -> heap expansion -> exact rerank`: `0.180 ms`

`128 books x 256 paragraphs/book` (`58,954` rows):

- heap rerank baseline: `0.085 ms`
- `sorted_heap_expand_rerank(... relation=2)`: `0.071 ms`
- `sorted_heap_graph_rag_scan(... relation=2)`: `0.087 ms`
- `pgvector ANN -> heap expansion -> exact rerank`: `0.295 ms`

The buffer footprint matches the latency story:

- `sorted_heap` helper path stays around hundreds of shared-buffer hits
- the `pgvector` path needs several thousands of shared-buffer hits before the
  same exact rerank step

This does **not** mean `pgvector` is bad at pure ANN. It means that for this
GraphRAG workload shape, once the seed stage is followed by relational graph
expansion and exact rerank, the narrow `sorted_heap` helper path is materially
better aligned with the whole workflow than an external ANN seed on a separate
table.

## zvec parity on the real-text graph

The same Gutenberg harness now also supports a comparable `zvec` path:

- ANN seeds come from a temporary `zvec` HNSW collection built from the same
  fact rows
- graph expansion and exact rerank still happen in PostgreSQL over `facts_heap`

This produced a mixed but useful result.

On the medium real-text slice (`64 books x 128 paragraphs/book`, `14,549` rows,
fresh backend, `shared_buffers=64MB`):

- heap rerank baseline: `0.068 ms`
- `sorted_heap_expand_rerank(... relation=2)`: `0.066 ms`
- `sorted_heap_graph_rag_scan(... relation=2)`: `0.082 ms`
- `zvec ANN -> heap expansion -> exact rerank`: `0.322 ms`

So on the medium slice, the `zvec` path is stable but materially slower than
the fused `sorted_heap` helper. The SQL-side buffer footprint is not the
bottleneck there; the external ANN seed stage dominates the total latency.

On the larger real-text slice (`128 books x 256 paragraphs/book`, `58,954`
rows), the result is currently **not publishable as a clean latency row**:

- the `sorted_heap` helper path remains stable:
  - `sorted_heap_expand_rerank(... relation=2)`: `0.070 ms`
  - `sorted_heap_graph_rag_scan(... relation=2)`: `0.084 ms`
- the `zvec` path fails during ANN seed retrieval at `ann_k=32`

The failure is not coming from PostgreSQL or from the GraphRAG SQL wrapper.
A pure `zvec`-only reproduction on the same `58,954`-row lexical-hash corpus
shows the same failure mode:

- for one probe query, `topk=8` and `topk=10` return valid document IDs
- `topk>=16` returns empty `doc.id` values after:
  - `Failed to find target chunk for index 58379`

The Gutenberg GraphRAG harness now turns that into an explicit benchmark error:

- `RuntimeError: zvec returned unmapped doc ids (...)`

So the objective conclusion today is narrower than for `pgvector`:

- `zvec` does not currently provide a robust large-slice GraphRAG parity row on
  this real-text workflow at `ann_k=32`
- on the medium slice where it does run, it is materially slower than the
  fused `sorted_heap` helper path
- on the larger slice, the current blocker is `zvec` ANN seed instability, not
  PostgreSQL expansion/rerank overhead

## Qdrant parity on the real-text graph

The Gutenberg harness now also supports a comparable `Qdrant` path:

- ANN seeds come from a local Qdrant HNSW collection built from the same fact
  rows
- graph expansion and exact rerank still happen in PostgreSQL over `facts_heap`

Unlike `zvec`, this path stayed stable on both the medium and larger real-text
slices. The result is simpler:

`64 books x 128 paragraphs/book` (`14,549` rows):

- heap rerank baseline: `0.074 ms`
- `sorted_heap_expand_rerank(... relation=2)`: `0.062 ms`
- `sorted_heap_graph_rag_scan(... relation=2)`: `0.083 ms`
- `Qdrant ANN -> heap expansion -> exact rerank`: `1.535 ms`

`128 books x 256 paragraphs/book` (`58,954` rows):

- heap rerank baseline: `0.081 ms`
- `sorted_heap_expand_rerank(... relation=2)`: `0.083 ms`
- `sorted_heap_graph_rag_scan(... relation=2)`: `0.085 ms`
- `Qdrant ANN -> heap expansion -> exact rerank`: `1.769 ms`

So on this GraphRAG workflow shape:

- Qdrant is robust on the real-text benchmark
- but its external ANN seed stage dominates end-to-end latency
- the fused `sorted_heap` helper remains roughly an order of magnitude faster
  on the rerank path

That again does **not** mean Qdrant is a bad vector engine in isolation. It
means that when the workflow is "external ANN seed + relational graph
expansion + exact rerank inside PostgreSQL", the narrow in-engine helper path
is much better aligned with the total job than a remote vector service.

## Robustness rerun

The same real-text Gutenberg harness was then rerun with a larger query set
(`query_count=64`, `runs=3`) to check whether the earlier `16`-query results
were just small-sample noise.

The ranking stayed the same on both slices:

- medium slice (`64 x 128`):
  - `sorted_heap_expand_rerank(... relation=2)`: `0.062 ms`
  - `sorted_heap_graph_rag_scan(... relation=2)`: `0.081 ms`
  - `pgvector ANN -> heap expansion -> exact rerank`: `0.219 ms`
  - `zvec ANN -> heap expansion -> exact rerank`: `0.342 ms`
  - `Qdrant ANN -> heap expansion -> exact rerank`: `1.567 ms`

- larger slice (`128 x 256`):
  - `sorted_heap_expand_rerank(... relation=2)`: `0.067 ms`
  - `sorted_heap_graph_rag_scan(... relation=2)`: `0.088 ms`
  - `pgvector ANN -> heap expansion -> exact rerank`: `0.309 ms`
  - `Qdrant ANN -> heap expansion -> exact rerank`: `1.911 ms`
  - `zvec` remains excluded from this large-slice rerun because the
    previously observed `ann_k=32` instability is still the blocker

So the current GraphRAG conclusion is no longer resting on one short probe set.
At least on this real-text Gutenberg workflow, the fused `sorted_heap` helper
still has the best end-to-end latency profile after the query set is expanded.

## Higher-dimension rerun

The same medium Gutenberg slice (`64 books x 128 paragraphs/book`) was then
rerun at higher lexical-hash embedding dimensions to test whether the earlier
result depended too heavily on the cheap `32D` setting.

At `128D` (`query_count=64`, `runs=3`):

- heap rerank baseline: `0.107 ms`
- `sorted_heap_expand_rerank(... relation=2)`: `0.090 ms`
- `sorted_heap_graph_rag_scan(... relation=2)`: `0.097 ms`
- `pgvector ANN -> heap expansion -> exact rerank`: `0.386 ms`
- `zvec ANN -> heap expansion -> exact rerank`: `0.518 ms`
- `Qdrant ANN -> heap expansion -> exact rerank`: `1.732 ms`

At `384D` on the same slice:

- heap rerank baseline: `0.185 ms`
- `sorted_heap_expand_rerank(... relation=2)`: `0.186 ms`
- `sorted_heap_graph_rag_scan(... relation=2)`: `0.203 ms`
- `pgvector ANN -> heap expansion -> exact rerank`: `0.815 ms`
- `zvec ANN -> heap expansion -> exact rerank`: `1.101 ms`
- `Qdrant ANN -> heap expansion -> exact rerank`: `2.275 ms`

This changes the interpretation in one important way:

- the `sorted_heap` helper remains clearly best-aligned with the full
  GraphRAG workflow versus the external ANN paths
- but the win over the pure heap rerank baseline is **dimension-sensitive**
- by `384D`, exact rerank cost dominates enough that the fused helper is only
  at parity with heap+btree rather than clearly ahead

So the current evidence supports a narrower claim than "sorted_heap always wins
GraphRAG":

> the fused `sorted_heap` helper is the best end-to-end path among the tested
> in-PG and external ANN competitors on this workflow shape, but its advantage
> over heap+btree narrows substantially as exact rerank dimension grows

One more tuning falsifier was useful here:

- dropping `ann_k` from `32` to `24` on the `384D` medium slice does reduce
  latency
- but it is **not** a free operating-point improvement
- a direct result-set comparison for `sorted_heap_graph_rag_scan(...)` on the
  `64`-query probe set showed mismatches on `62/64` queries versus `ann_k=32`

So the current faster-than-`ann_k=32` settings should be treated as a
quality/latency tradeoff, not as a no-regression default recommendation.

One important measurement caveat was also discovered and fixed during this
work:

- direct filtered `ORDER BY embedding <=> $query LIMIT K` on a base table with
  a `sorted_hnsw` index is **not** a valid GraphRAG baseline for current
  Phase 1 semantics
- the automatic `sorted_hnsw` path is now explicitly costed out when extra
  base-relation quals are present
- GraphRAG rerank baselines must therefore materialize the expanded set first,
  then rerank it

This is enough to falsify the pessimistic branch:

> the next useful GraphRAG step is not necessarily a new storage engine; a
> carefully scoped C primitive can already recover a substantial part of the
> lost latency

## Recommended roadmap

### Phase 0 — completed

- Build local prototype benchmark
- Falsify naive SQL assumptions

### Phase 1 — current

`sorted_heap_expand_ids()` is implemented and regression-covered.

### Phase 2 — current

`sorted_heap_expand_rerank()` is implemented and regression-covered.

Current success criterion that was met:

- beats the current `sorted_heap` SQL `seed_expand_in` / `seed_expand_rerank_in`
  patterns at medium scale

Current gap that remains:

- pure heap+btree expansion is still faster on this synthetic benchmark

### Phase 3 — next

Add GraphRAG composition query:

- ANN seed in SQL via `sorted_hnsw`
- expansion via `sorted_heap_expand_ids()`
- rerank via `sorted_heap_expand_rerank()` or SQL over materialized expansion

### Phase 4

`sorted_heap_graph_rag_scan()` is now implemented as the narrow one-call
composition wrapper.

### Phase 5

Only if the current wrapper still shows too much overhead:

- consider a fused helper for:
  - ANN seed IDs
  - expansion
  - rerank

## Current verdict

`sorted_heap` already has a plausible GraphRAG foundation, and the new helper
proves that a narrow C primitive can materially improve the GraphRAG path.

What is now true:

- SQL-only GraphRAG composition was not enough
- `sorted_heap_expand_ids()` is enough to recover a large part of that gap
- `sorted_heap_expand_rerank()` recovers most of the rerank overhead on the
  current `sorted_heap` path
- `sorted_heap_graph_rag_scan()` makes the composition available as a single
  SQL call without giving back much latency
- the narrow-helper direction is a justified building block
- on the real-text GraphRAG shape, `pgvector` parity is already materially
  worse end-to-end than the fused `sorted_heap` helper path
- `zvec` is stable on the medium slice but currently not robust on the larger
  real-text slice at `ann_k=32`
- `Qdrant` is robust on both real-text slices but materially slower than the
  fused `sorted_heap` helper on the same workflow

What is not yet true:

- `sorted_heap` is not yet clearly better than heap+btree on pure expansion
  latency for this synthetic workload
- even the relation-filtered GraphRAG path still trails heap+btree slightly on
  this synthetic benchmark

The correct next step is therefore:

> **tune or extend the narrow expansion primitive before considering a bigger
> graph-specific subsystem**

That remains the smallest change that can still convert the observed
block-pruning advantage into an end-to-end query win.
