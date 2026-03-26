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
sorted_heap_expand_twohop_rerank(
    rel regclass,
    seed_ids int4[],
    query svec,
    top_k int4,
    hop1_relation_filter int4 DEFAULT NULL,
    hop2_relation_filter int4 DEFAULT NULL,
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

That instability is now isolated more sharply by the repo-owned reproducer:

- [`scripts/repro_zvec_gutenberg_threshold.py`](/Users/sergey/Projects/C/clustered_pg/scripts/repro_zvec_gutenberg_threshold.py)

Current threshold signature on the lexical-hash Gutenberg corpus:

- `topk=16`, `dim=32`
- `64x256`, `80x256`, `96x256`, `112x256` slices are stable
  - `28,661`, `36,064`, `43,684`, `51,166` rows
- `128x256` fails
  - `58,954` rows
  - first bad probe: `query #10`
  - returned ids are empty strings after
    `Failed to find target chunk for index 58379`

So the current failure signature is not just "large-ish GraphRAG benchmark".
It looks more like a size-thresholded `zvec` retrieval bug on this corpus
shape.

That theory is now falsified by a second repo-owned reproducer on a plain
synthetic FP32 corpus:

- [`scripts/repro_zvec_synthetic_threshold.py`](/Users/sergey/Projects/C/clustered_pg/scripts/repro_zvec_synthetic_threshold.py)

Current synthetic signature:

- `dim=32`, `ef_search=64`
- `topk=7` already reproduces the issue
- a compact failing case exists at `4,950` rows
  - nearby controls:
    - `4,900` rows: ok
    - `4,950` rows: bad
    - `5,000` rows: bad
  - `topk<=6` is clean on the `4,950`-row case
- failures are non-monotonic by row count
  - bad: `16,000`, `20,000`, `28,000`, `30,000`, `45,000`, `60,000`
  - ok: `24,000`, `29,000`, `75,000` (`100` probe queries still clean at `75k`)
- another local non-monotonic pocket exists around `7k-8k`
  - `7,000`: ok
  - `7,500`: bad
  - `7,800`: ok
  - `7,900`: bad
- representative stderr lines:
  - `Failed to find target chunk for index 4945`
  - `Failed to find target chunk for index 14999`
  - `Failed to find target chunk for index 29999`
  - `Failed to find target chunk for index 59999`

So the stronger objective conclusion is:

- the failure is not Gutenberg-specific
- it is not a simple monotonic "too many rows" threshold either
- the current evidence points to a broader `zvec` retrieval defect around
  forward-store / chunk lookup, not to PostgreSQL GraphRAG expansion logic

For an upstream-ready summary of the current evidence, see:

- [`docs/zvec-empty-id-bug.md`](/Users/sergey/Projects/C/clustered_pg/docs/zvec-empty-id-bug.md)

Two more diagnostic observations make that conclusion sharper:

- when the synthetic bug triggers, the ANN scores still come back while
  `doc.id` is empty for the whole result set
  - `4,950 rows`, `topk=6`: valid ids
  - `4,950 rows`, `topk=7`: same score bands, but every `doc.id` is `''`
- on a larger synthetic case (`16,000` rows), exact cosine inspection shows
  the best-score bucket spans `1000, 2000, ..., 16000`, and `zvec` already
  returns empty ids at `topk=5`

That does not prove the internal root cause, but it strongly suggests the ANN
ranking stage is still producing plausible scores while the forward-store
document lookup stage is failing. A reasonable working hypothesis is that some
tied-score / candidate-materialization paths touch unresolved high indexes and
poison metadata resolution for the whole returned batch.

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

## Two-hop Gutenberg composition

The next adversarial question was whether the current helper story survives a
real **two-hop** workflow, not just the earlier "ANN seeds -> one filtered
expansion -> rerank" shape.

The initial Gutenberg falsifier first used a composed path from the existing
narrow primitives:

1. ANN seeds from the fact table
2. first hop via `sorted_heap_expand_ids(..., relation=2)`
3. second hop via `sorted_heap_expand_rerank(..., relation=2)`

That composition benchmark was intentionally a harsher test than the earlier
one-hop helper story, because it asked whether the current primitives were
already enough to make multi-hop GraphRAG plausible before inventing a
dedicated two-hop helper.

The answer was "yes, barely enough". That justified one narrow extra helper,
not a new storage engine:

```sql
sorted_heap_expand_twohop_rerank(...)
```

This fused helper keeps the same contract shape as the earlier rerank helper,
but removes the intermediate SQL/materialization boundary between hop1 and the
second-hop rerank.

On the medium real-text slice (`64 books x 128 paragraphs/book`, `14,549` rows,
`32D`, `query_count=64`, `runs=3`, fresh backend, `shared_buffers=64MB`):

- heap baseline, `seed_expand2_rerank_rel_in`: `0.102 ms`
- plain `sorted_heap` SQL, `seed_expand2_rerank_rel_in`: `0.136 ms`
- helper-composed `sorted_heap`, `seed_expand2_rerank_rel_topk_fn`: `0.105 ms`
- fused `sorted_heap_expand_twohop_rerank(...)`: `0.081 ms`

So on the medium slice, the dedicated helper now does what the composed path
only hinted at:

- it beats heap+btree on latency
- it materially beats the composed two-hop helper path
- it also cuts shared-buffer hits strongly (`421` vs `1298` for the heap
  baseline, and `421` vs `662` for the composed helper)

On the larger real-text slice (`128 books x 256 paragraphs/book`, `58,954`
rows, same settings except the larger corpus):

- heap baseline, `seed_expand2_rerank_rel_in`: `0.114 ms`
- plain `sorted_heap` SQL, `seed_expand2_rerank_rel_in`: `0.153 ms`
- helper-composed `sorted_heap`, `seed_expand2_rerank_rel_topk_fn`: `0.111 ms`
- fused `sorted_heap_expand_twohop_rerank(...)`: `0.092 ms`

So the larger slice confirms the same shape: the dedicated two-hop helper is
not a tiny micro-win on one probe set; it keeps the lead over both heap+btree
and the composed helper.

The same medium two-hop slice was also benchmarked against the external ANN
seed paths:

- `pgvector ANN -> heap 2-hop expansion -> exact rerank`: `0.253 ms`
- `zvec ANN -> heap 2-hop expansion -> exact rerank`: `0.374 ms`
- `Qdrant ANN -> heap 2-hop expansion -> exact rerank`: `1.789 ms`

So the product-level conclusion stays consistent in the two-hop case as well:
the narrow in-engine `sorted_heap` helper remains the fastest end-to-end
GraphRAG path among the tested competitors on this real-text slice.

At higher exact-rerank dimension, the advantage narrows again rather than
disappearing:

`64 books x 128 paragraphs/book`, `384D`, `query_count=64`, `runs=3`:

- heap baseline, `seed_expand2_rerank_rel_in`: `0.225 ms`
- plain `sorted_heap` SQL, `seed_expand2_rerank_rel_in`: `0.266 ms`
- helper-composed `sorted_heap`, `seed_expand2_rerank_rel_topk_fn`: `0.258 ms`
- fused `sorted_heap_expand_twohop_rerank(...)`: `0.236 ms`

Interpretation:

- the dedicated helper makes two-hop GraphRAG clearly viable on the real-text
  Gutenberg path
- the latency win is still not universal; at higher dimensions it narrows
  toward parity with heap+btree
- but the locality signal remains stronger than latency alone suggests
  (`1264` shared hits for the fused helper vs `3155` for the heap baseline on
  the `384D` medium run)

So the correct next inference is narrower than "we need a graph storage
engine" and also narrower than "we need a broad graph query layer":

> a dedicated but still narrow two-hop helper is justified; anything broader
> should now be treated as product/API design, not as a prerequisite for making
> two-hop GraphRAG fast enough to matter.

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

### Phase 4 — current

`sorted_heap_graph_rag_scan()` is now implemented as the narrow one-call
composition wrapper.

### Phase 5 — current

`sorted_heap_expand_twohop_rerank()` is now implemented as the narrow fused
two-hop helper.

Current success criterion that was met:

- beats the previous composed two-hop helper on the real-text Gutenberg graph
- beats heap+btree on the medium and larger `32D` two-hop slices

Current gap that remains:

- at `384D`, the fused two-hop helper narrows to near-parity with heap+btree
  rather than keeping a clear lead

### Phase 6 — next

Only if the current two-hop and one-call wrappers still leave meaningful
headroom:

- consider a broader wrapper for:
  - ANN seed IDs
  - two-hop expansion
  - rerank
- or tune candidate count / rerank workload rather than broadening the API

## Cogniformerus-style multihop facts

The real missing falsifier was not another paragraph graph slice. It was a
benchmark that matches the current `cogniformerus` multihop question shape:

- fact `1`: `person -> parent`
- fact `2`: `parent -> city`
- query: `Where does the parent of Person_i live?`

That now exists in:

- [`scripts/bench_graph_rag_multihop.py`](/Users/sergey/Projects/C/clustered_pg/scripts/bench_graph_rag_multihop.py)
- [`scripts/sweep_graph_rag_multihop.py`](/Users/sergey/Projects/C/clustered_pg/scripts/sweep_graph_rag_multihop.py)

The benchmark builds a deterministic fact graph and measures:

- latency
- `hit@1`
- `hit@k`

for the expected final `city` fact after two-hop expansion and rerank.

### Important contract discovery

This benchmark immediately exposed a semantic limitation in the current
convenience wrapper:

- `sorted_heap_graph_rag_scan()` seeds expansion from ANN `target_id`
- that is a good fit for the Gutenberg `paragraph -> next_paragraph` graph
- it is **not** the right seed contract for the fact benchmark above
- the fact benchmark needs ANN seeds based on `entity_id`, then:
  - hop 1 on relation `1`
  - hop 2 on relation `2`

So the current one-call wrapper is still too specialized for this workload
shape. The lower-level helper family is fine; the wrapper contract is the
narrow part.

That gap is now closed by:

- `sorted_heap_graph_rag_twohop_scan(...)`

This wrapper keeps the fact-shaped contract narrow:

- ANN seed on `entity_id`
- hop 1 relation filter
- hop 2 relation filter
- final rerank delegated to `sorted_heap_expand_twohop_rerank(...)`

### Early failure that mattered

At `32D`, the fact benchmark initially produced very poor answer retrieval.
That was a benchmark-quality failure, not a helper failure:

- the first draft seeded on `target_id`, which was the wrong graph contract
- after fixing that, the deterministic query embedding was still too weak
  at low dimension to make the question reliably retrievable

So the publishable multihop results start at `384D`, where the question shape
becomes stable enough that latency numbers mean something.

### Tuned 384D result

On `5K` multihop chains (`10K` rows total), `64` queries, `3` runs,
`shared_buffers=64MB`, fresh backend, with:

- `ann_k=64`
- `sorted_hnsw.ef_search=64`
- `ef_construction=200`

the current frontier is:

- heap composed two-hop SQL
  - `0.515 ms`
  - `hit@1 = 71.9%`
  - `hit@k = 85.9%`
- `sorted_heap` composed two-hop helper
  - `0.471 ms`
  - `hit@1 = 70.3%`
  - `hit@k = 82.8%`
- `sorted_heap_expand_twohop_rerank()`
  - `0.442 ms`
  - `hit@1 = 70.3%`
  - `hit@k = 82.8%`
- `sorted_heap_graph_rag_twohop_scan()`
  - `0.417 ms`
  - `hit@1 = 71.9%`
  - `hit@k = 84.4%`
- pgvector
  - `1.397 ms`
  - `hit@1 = 70.3%`
  - `hit@k = 87.5%`
- zvec
  - `1.076 ms`
  - `hit@1 = 76.6%`
  - `hit@k = 96.9%`
- Qdrant
  - `2.921 ms`
  - `hit@1 = 76.6%`
  - `hit@k = 96.9%`

Interpretation:

- the fused two-hop helper is now the **fastest PostgreSQL path** on this
  fact-shaped workload
- the new fact-shaped one-call wrapper stays effectively at parity with the
  fused helper, so this time the convenience API does **not** erase the win
- it remains materially faster than pgvector on the same workflow
- it is **not** the quality leader at this operating point
- zvec and Qdrant still win on answer retrieval quality here, but at much
  higher latency

### Seed frontier after the wrapper fix

The next honest question was not API shape but ANN seed quality. That is now
measured directly by:

- [`scripts/sweep_graph_rag_multihop.py`](/Users/sergey/Projects/C/clustered_pg/scripts/sweep_graph_rag_multihop.py)

This harness keeps the corpus fixed per `ef_construction` and sweeps:

- `m`
- `ann_k`
- `sorted_hnsw.ef_search`
- `ef_construction`

without paying a full temp-cluster and schema rewrite for every single probe
point.

On the same `5K` chains / `10K` rows / `384D` / `64` queries / fresh-backend
benchmark, the stable wrapper frontier is now:

- `ef_construction=64`, `ann_k=64`, `ef_search=64`
  - `0.386 ms`
  - `hit@1 = 70.3%`
  - `hit@k = 82.8%`
- `ef_construction=200`, `ann_k=64`, `ef_search=64`
  - `0.393 ms`
  - `hit@1 = 71.9%`
  - `hit@k = 84.4%`
- `ef_construction=400`, `ann_k=64`, `ef_search=64`
  - `0.421 ms`
  - `hit@1 = 70.3%`
  - `hit@k = 85.9%`
- `ef_construction=200`, `ann_k=64`, `ef_search=128`
  - `0.651 ms`
  - `hit@1 = 73.4%`
  - `hit@k = 95.3%`
- `ef_construction=400`, `ann_k=64`, `ef_search=128`
  - `0.663 ms`
  - `hit@1 = 75.0%`
  - `hit@k = 95.3%`

For a higher-quality but much slower seed tier:

- `ann_k=96`, `ef_search=64` lands around `2.2-2.4 ms`
  with `hit@k = 96.9%`

That leads to a narrower, more honest recommendation:

- if latency is the hard constraint, keep the fast tier near
  `ef_construction=200`, `ann_k=64`, `ef_search=64`
- if answer quality matters more, the best balanced point we measured is
  `ef_construction=200`, `ann_k=64`, `ef_search=128`
- `ef_construction=400` does improve `hit@1` slightly at the same `95.3%`
  `hit@k`, but it does not improve `hit@k` over `200`, so it should not be
  the default recommendation without a separate build-cost justification

That build-cost justification now exists too on this exact `10K x 384D`
multihop benchmark:

- `ef_construction=64`: `43.716 s` to build both ANN indexes
- `ef_construction=200`: `80.046 s`
- `ef_construction=400`: `91.352 s`

So the current recommendation is:

- default to `ef_construction=200`
- treat `ef_construction=400` as a niche `hit@1` knob, not the new default

### `m` frontier on the same multihop benchmark

The next useful falsifier was whether graph degree buys more than another
`ef_construction` increase.

Keeping:

- `ef_construction=200`
- `ann_k=64`
- `64` queries
- `3` runs
- fresh backend

the `m` sweep came out as:

- `m=16`, `ef_search=64`
  - `0.405 ms`
  - `hit@1 = 71.9%`
  - `hit@k = 87.5%`
- `m=24`, `ef_search=64`
  - `0.466 ms`
  - `hit@1 = 75.0%`
  - `hit@k = 93.8%`
- `m=32`, `ef_search=64`
  - `0.491 ms`
  - `hit@1 = 78.1%`
  - `hit@k = 93.8%`
- `m=16`, `ef_search=128`
  - `0.672 ms`
  - `hit@1 = 73.4%`
  - `hit@k = 95.3%`
- `m=24`, `ef_search=128`
  - `0.738 ms`
  - `hit@1 = 75.0%`
  - `hit@k = 96.9%`
- `m=32`, `ef_search=128`
  - `0.771 ms`
  - `hit@1 = 76.6%`
  - `hit@k = 96.9%`

The one-off build-cost probe for the same `10K x 384D` graph was:

- `m=16`, `ef_construction=200`: `79.425 s`
- `m=24`, `ef_construction=200`: `86.562 s`
- `m=32`, `ef_construction=200`: `75.404 s`

That last `m=32` build number should be treated cautiously; it was a single
one-off probe and is likely noisy enough that only the query-time frontier is
trustworthy here.

The stable conclusion is still clear:

- `m=24` is the best current quality-per-latency tradeoff we measured
- `m=32` buys a little more `hit@1`, but no additional `hit@k`
- so for fact-shaped multihop GraphRAG, the best current balanced point is:
  - `m=24`
  - `ef_construction=200`
  - `ann_k=64`
  - `sorted_hnsw.ef_search=128`

One more ann_k falsifier matters here too:

- increasing `ann_k` above `64` at this `m=24 / ef_construction=200 / ef_search=128`
  point did **not** help
- `ann_k=80/96/128` all increased latency and reduced `hit@k`
- so `ann_k=64` remains the current sweet spot, not just a legacy default

### Full parity rerun at the balanced point

Re-running the full multihop parity benchmark on that exact setting:

- `m=24`
- `ef_construction=200`
- `ann_k=64`
- `sorted_hnsw.ef_search=128`
- `64` queries
- `3` runs
- `384D`

produced:

- heap two-hop SQL
  - `0.762 ms`
  - `hit@1 = 75.0%`
  - `hit@k = 96.9%`
- `sorted_heap_expand_twohop_rerank()`
  - `0.726 ms`
  - `hit@1 = 75.0%`
  - `hit@k = 96.9%`
- `sorted_heap_graph_rag_twohop_scan()`
  - `0.727 ms`
  - `hit@1 = 75.0%`
  - `hit@k = 96.9%`
- pgvector
  - `1.244 ms`
  - `hit@1 = 70.3%`
  - `hit@k = 85.9%`
- zvec
  - `0.927 ms`
  - `hit@1 = 76.6%`
  - `hit@k = 96.9%`
- Qdrant
  - `2.417 ms`
  - `hit@1 = 76.6%`
  - `hit@k = 96.9%`

That is a materially stronger result than the earlier `m=16` baseline:

- the fused `sorted_heap` path now matches `zvec` and `Qdrant` on `hit@k`
- it stays faster than both external paths
- it also beats pgvector on both latency and answer quality on this workload
- `zvec` and `Qdrant` still keep a small `hit@1` edge, so the answer-quality
  story is now about `hit@1`, not `hit@k`

### Full parity rerun at the higher-quality point

The next question was whether that remaining `hit@1` gap could be closed
without giving back the latency lead. Re-running the same full parity benchmark
at:

- `m=32`
- `ef_construction=200`
- `ann_k=64`
- `sorted_hnsw.ef_search=128`

produced:

- heap two-hop SQL
  - `0.810 ms`
  - `hit@1 = 76.6%`
  - `hit@k = 96.9%`
- `sorted_heap_expand_twohop_rerank()`
  - `0.774 ms`
  - `hit@1 = 76.6%`
  - `hit@k = 96.9%`
- `sorted_heap_graph_rag_twohop_scan()`
  - `0.786 ms`
  - `hit@1 = 76.6%`
  - `hit@k = 96.9%`
- pgvector
  - `1.220 ms`
  - `hit@1 = 70.3%`
  - `hit@k = 84.4%`
- zvec
  - `0.874 ms`
  - `hit@1 = 76.6%`
  - `hit@k = 96.9%`
- Qdrant
  - `2.487 ms`
  - `hit@1 = 76.6%`
  - `hit@k = 96.9%`

So the current picture is now more precise:

- `m=24` is still the better quality-per-latency recommendation
- `m=32` is the point where `sorted_heap` reaches full observed parity with
  `zvec` and Qdrant on both `hit@1` and `hit@k`
- even at that higher-quality point, the `sorted_heap` helper remains faster
  than both external paths
- pgvector remains behind on both latency and answer quality on this workload

### AWS ARM64 parity rerun (`5K` chains)

The next environment-variance adversary check was to rerun the same
`5K`-chain / `10K`-row / `384D` fact benchmark on an AWS ARM64 host
(`4 vCPU`, `8 GiB RAM`) using the repo-owned wrapper:

- [`scripts/bench_graph_rag_multihop_aws.sh`](/Users/sergey/Projects/C/clustered_pg/scripts/bench_graph_rag_multihop_aws.sh)

At the previously recommended local balanced point:

- `m=24`
- `ef_construction=200`
- `ann_k=64`
- `sorted_hnsw.ef_search=128`
- `64` queries
- `3` runs
- fresh backend

the AWS rerun produced:

- heap two-hop SQL
  - `1.087 ms`
  - `hit@1 = 75.0%`
  - `hit@k = 96.9%`
- `sorted_heap_expand_twohop_rerank()`
  - `0.947 ms`
  - `hit@1 = 76.6%`
  - `hit@k = 98.4%`
- `sorted_heap_graph_rag_twohop_scan()`
  - `1.004 ms`
  - `hit@1 = 76.6%`
  - `hit@k = 98.4%`
- pgvector
  - `1.296 ms`
  - `hit@1 = 70.3%`
  - `hit@k = 85.9%`
- zvec
  - `1.646 ms`
  - `hit@1 = 76.6%`
  - `hit@k = 96.9%`
- Qdrant
  - `3.396 ms`
  - `hit@1 = 76.6%`
  - `hit@k = 96.9%`

That is stronger than the local balanced point in one important way:

- on this AWS rerun, `sorted_heap` does not just match `zvec` and Qdrant on
  `hit@k`; it exceeds them (`98.4%` vs `96.9%`) while staying faster than both

But the second half of the adversary check matters too. Re-running the same
AWS benchmark at the local higher-quality point:

- `m=32`
- `ef_construction=200`
- `ann_k=64`
- `sorted_hnsw.ef_search=128`

produced:

- `sorted_heap_graph_rag_twohop_scan()`
  - `1.066 ms`
  - `hit@1 = 76.6%`
  - `hit@k = 96.9%`

So the local `m=32` parity story does **not** carry over unchanged to this
AWS ARM64 environment. The portable conclusion is therefore narrower:

- `m=24 / ef_construction=200 / ann_k=64 / ef_search=128` is the current
  best verified cross-environment point
- local and AWS frontiers are directionally consistent, but not numerically
  identical
- this is exactly why the AWS rerun is worth keeping as a separate falsifier,
  not merging blindly into the local tuning story

### Larger local scale check (`10K` chains)

The next adversary check was whether the `5K`-chain tuning carried forward to a
larger local fact graph without retuning.

On `10K` chains (`20K` rows total), `64` queries, `384D`, fresh backend:

- `m=24`, `ef_construction=200`, `ann_k=64`, `ef_search=128`
  - `sorted_heap_graph_rag_twohop_scan()` -> `0.885 ms`
  - `hit@1 = 71.9%`
  - `hit@k = 92.2%`
- `m=32`, `ef_construction=200`, `ann_k=64`, `ef_search=128`
  - `sorted_heap_graph_rag_twohop_scan()` -> `0.972 ms`
  - `hit@1 = 73.4%`
  - `hit@k = 93.8%`

So the `5K`-chain operating point does **not** generalize unchanged.

The next narrow falsifier was whether this larger-graph drop was just a search
beam issue. Sweeping `ef_search` upward at `m=32` gave:

- `ef_search=192`
  - `1.310 ms`
  - `hit@1 = 76.6%`
  - `hit@k = 95.3%`
- `ef_search=256`
  - `1.734 ms`
  - `hit@1 = 78.1%`
  - `hit@k = 95.3%`

That is a useful but incomplete recovery:

- higher `ef_search` does recover part of the quality loss
- it does **not** recover the earlier `96.9% hit@k` local point
- so the larger-graph gap is not purely a beam-width problem

The next falsifier after that was stronger graph construction. On the same
`10K`-chain graph, keeping `m=32`, `ann_k=64`, and comparing
`ef_construction=200` vs `400` gave:

- at `ef_search=128`
  - `ef_construction=200` -> `0.976 ms`, `hit@1 = 75.0%`, `hit@k = 93.8%`
  - `ef_construction=400` -> `1.094 ms`, `hit@1 = 75.0%`, `hit@k = 93.8%`
- at `ef_search=192`
  - `ef_construction=200` -> `1.357 ms`, `hit@1 = 76.6%`, `hit@k = 95.3%`
  - `ef_construction=400` -> `1.381 ms`, `hit@1 = 76.6%`, `hit@k = 95.3%`

So this larger-graph gap is not fixed by a simple `ef_construction=400` bump
either.

The current best explanation is therefore narrower:

- the verified `5K`-chain local frontier is real
- the same operating points do not carry forward unchanged to `10K` chains
- and the obvious local rescue knobs (`ef_search`, `ef_construction`) only
  recover part of the drop

That is enough to stop local knob-turning for this pass. The next useful step
would be a different class of experiment, not more of the same sweep.

The next adversary check after that was whether this larger-graph caveat was
just a local-machine artifact. Re-running the `10K`-chain benchmark on the
same AWS ARM64 host (`4 vCPU`, `8 GiB RAM`) showed that it is not.

At the same balanced portable point:

- `m=24`
- `ef_construction=200`
- `ann_k=64`
- `sorted_hnsw.ef_search=128`

the AWS rerun produced:

- heap two-hop SQL
  - `1.389 ms`
  - `hit@1 = 71.9%`
  - `hit@k = 92.2%`
- `sorted_heap_expand_twohop_rerank()`
  - `1.190 ms`
  - `hit@1 = 71.9%`
  - `hit@k = 92.2%`
- `sorted_heap_graph_rag_twohop_scan()`
  - `1.248 ms`
  - `hit@1 = 71.9%`
  - `hit@k = 92.2%`

That essentially matches the larger local result. So the `10K`-chain drop is
cross-environment robust, not just a local Apple/M-series artifact.

The one meaningful local rescue point transferred cleanly to AWS too.
Re-running the `10K`-chain benchmark at:

- `m=32`
- `ef_construction=200`
- `ann_k=64`
- `sorted_hnsw.ef_search=192`

produced:

- heap two-hop SQL
  - `1.896 ms`
  - `hit@1 = 76.6%`
  - `hit@k = 95.3%`
- `sorted_heap_expand_twohop_rerank()`
  - `1.617 ms`
  - `hit@1 = 76.6%`
  - `hit@k = 95.3%`
- `sorted_heap_graph_rag_twohop_scan()`
  - `1.687 ms`
  - `hit@1 = 76.6%`
  - `hit@k = 95.3%`

So the larger-scale picture is now materially stronger:

- the `10K`-chain quality drop is cross-environment robust
- the best current larger-graph recovery point is also cross-environment
  robust: `m=32 / ef_search=192`
- but even that recovery point does **not** restore the earlier `5K`-chain
  `98.4% hit@k` AWS frontier
- so the remaining gap is unlikely to be solved by another trivial
  `ef_search` or `m` tweak alone

### Exact-seed upper-bound diagnostic

The next root-cause check was to remove ANN approximation from the seed stage
entirely. The multihop harness now supports an `--exact-seed-diagnostics`
mode, which replaces ANN seed retrieval with exact brute-force top-K seeds on
`facts_heap`, then reuses the same graph expansion/rerank path.

This matters because it separates two very different explanations:

- "the remaining gap is caused by approximate ANN seeds"
- "the remaining gap is already in the benchmark/query/task shape"

On the `5K`-chain balanced local point:

- `m=24`
- `ef_construction=200`
- `ann_k=64`
- `sorted_hnsw.ef_search=128`

the exact-seed diagnostic did **not** improve quality:

- ANN-seeded `sorted_heap_expand_twohop_rerank()`
  - `0.702 ms`
  - `hit@1 = 75.0%`
  - `hit@k = 96.9%`
- exact-seeded `sorted_heap_expand_twohop_rerank()`
  - `0.811 ms`
  - `hit@1 = 75.0%`
  - `hit@k = 96.9%`

And the seed-stage diagnostic showed no hidden ANN loss there either:

- ANN seeds
  - `seed_person_pct = 98.4%`
  - `expanded_city_pct = 98.4%`
  - `avg_person_rank = 1.00`
  - `city_rank_p95 = 6`
  - `city_rank_max = 17`
- exact seeds
  - `seed_person_pct = 98.4%`
  - `expanded_city_pct = 98.4%`
  - `avg_person_rank = 1.00`
  - `city_rank_p95 = 6`
  - `city_rank_max = 17`

So even at `5K`, the final `96.9% hit@k` is already below seed coverage.
But the rerank distribution is still concentrated: the correct city stays
within the top 6 for 95% of reachable queries, and the miss comes from a
small number of sharper outliers.

On the `10K`-chain balanced local point:

- `m=24`
- `ef_construction=200`
- `ann_k=64`
- `sorted_hnsw.ef_search=128`

the exact-seed diagnostic again did **not** improve quality:

- ANN-seeded `sorted_heap_expand_twohop_rerank()`
  - `0.839 ms`
  - `hit@1 = 71.9%`
  - `hit@k = 92.2%`
- exact-seeded `sorted_heap_expand_twohop_rerank()`
  - `0.947 ms`
  - `hit@1 = 71.9%`
  - `hit@k = 92.2%`

The seed-stage diagnostic was even more revealing on `10K`:

- ANN seeds
  - `seed_person_pct = 96.9%`
  - `expanded_city_pct = 96.9%`
  - `avg_person_rank = 1.00`
  - `city_rank_p95 = 3`
  - `city_rank_max = 20`
- exact seeds
  - `seed_person_pct = 96.9%`
  - `expanded_city_pct = 96.9%`
  - `avg_person_rank = 1.00`
  - `city_rank_p95 = 3`
  - `city_rank_max = 19`

So the larger-graph gap is **not** coming from missing the correct seed fact.
At `10K`, seed coverage stays at `96.9%`, but final `hit@k` drops to `92.2%`.
And it is not a broad rerank collapse either: for 95% of reachable queries the
correct city still ranks in the top 3, but a few outliers fall as far as
rank `19-20`, which is enough to miss `top_k = 10`.

This is a strong falsifier:

- on this synthetic fact benchmark, the current `5K` and `10K` frontiers are
  **not** ANN-approximation limited at the tested operating points
- ANN and exact seeds have identical seed coverage on both scales
- the remaining gap is mostly an outlier-ranking problem, not a broad seed or
  rerank failure
- exact seeds cost extra latency but do not recover answer quality
- so the next meaningful gain is unlikely to come from more seed-ANN tuning
  alone

The remaining gap now looks more like a property of the task construction,
query embedding, or graph benchmark semantics than of `sorted_hnsw`
approximation itself. More specifically: the dominant remaining loss now looks
downstream of seed retrieval, not inside it, and it is concentrated in a small
set of bad cases rather than a general degradation across the query set.

So the honest story on this fact benchmark is a latency/quality frontier:

- `sorted_heap_expand_twohop_rerank()` leads on latency

### Path-aware rerank diagnostic

The next falsifier was to keep the same ANN seeds and the same two-hop
expansion, but change only the final scorer. The current multihop helper
reranks on the hop-2 city fact embedding alone. A path-aware SQL baseline was
added to the harness that scores each candidate as:

- `path_distance = (hop1_embedding <=> query) + (hop2_embedding <=> query)`

That simple change materially improved answer quality on the same balanced
points:

- `5K` chains, `m=24`, `ef_construction=200`, `ann_k=64`,
  `sorted_hnsw.ef_search=128`
  - city-only `sorted_heap_graph_rag_twohop_scan()`
    - `0.762 ms`
    - `hit@1 = 75.0%`
    - `hit@k = 96.9%`
  - path-aware SQL rerank on `facts_sh`
    - `0.957 ms`
    - `hit@1 = 98.4%`
    - `hit@k = 98.4%`

- `10K` chains, same knobs
  - city-only `sorted_heap_graph_rag_twohop_scan()`
    - `0.937 ms`
    - `hit@1 = 71.9%`
    - `hit@k = 92.2%`
  - path-aware SQL rerank on `facts_sh`
    - `1.179 ms`
    - `hit@1 = 95.3%`
    - `hit@k = 96.9%`

This is the strongest current architectural signal on the fact-shaped
benchmark:

- the remaining quality gap is not well explained by seed recall
- it is also not well explained by broad rerank collapse
- a simple path-aware scorer recovers most of the lost quality with only a
  modest latency increase

That branch is now implemented locally too:

- `sorted_heap_expand_twohop_path_rerank(...)`
- `sorted_heap_graph_rag_twohop_path_scan(...)`

And the fused helper beats the SQL path-aware baseline on the same balanced
points:

- `5K` chains
  - SQL path-aware baseline: `0.847 ms`, `hit@1 = 98.4%`, `hit@k = 98.4%`
  - fused helper: `0.726 ms`, `hit@1 = 98.4%`, `hit@k = 98.4%`
  - one-call wrapper: `0.739 ms`, `hit@1 = 98.4%`, `hit@k = 98.4%`

- `10K` chains
  - SQL path-aware baseline: `0.942 ms`, `hit@1 = 95.3%`, `hit@k = 96.9%`
  - fused helper: `0.823 ms`, `hit@1 = 95.3%`, `hit@k = 96.9%`
  - one-call wrapper: `0.834 ms`, `hit@1 = 95.3%`, `hit@k = 96.9%`

So for multihop fact retrieval, the next serious question is no longer
whether path-aware rerank helps. It does. The next question is whether this
new helper/wrapper transfers cleanly to AWS and then to a real
`cogniformerus`-like corpus.
- at the balanced `m=24` point, `sorted_heap` matches `zvec` / `Qdrant` on
  `hit@k` and trails only slightly on `hit@1`
- at the higher-quality `m=32` point, `sorted_heap` reaches parity with
  `zvec` / `Qdrant` on both `hit@1` and `hit@k`
- pgvector is slower and weaker on answer quality than the tuned
  `sorted_heap` helper on this workload

This also falsifies one tempting but wrong simplification:

> once the helper is fast, the remaining GraphRAG problem is solved

Not quite. On fact-shaped multihop queries, seed ANN quality and graph build
quality still matter enough that `ann_k`, `ef_search`, and graph build quality
remain first-class tuning knobs. But the old hop-2-only rerank contract was a
separate, larger problem, and the new path-aware helper fixes most of it on
the current local benchmark.

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
- `sorted_heap_expand_twohop_rerank()` turns the earlier two-hop composition
  evidence into a real latency win on the real-text Gutenberg slices we tested
- on the cogniformerus-style `person -> parent -> city` benchmark, the fused
  two-hop helper is the fastest PostgreSQL path we tested
- `sorted_heap_graph_rag_twohop_scan()` closes the current fact-shaped wrapper
  gap without materially giving back latency
- `sorted_heap_expand_twohop_path_rerank()` upgrades the fact-shaped rerank
  contract to use hop-1 and hop-2 evidence together
- `sorted_heap_graph_rag_twohop_path_scan()` makes that path-aware contract
  available as a single-call primitive
- the narrow-helper direction is a justified building block
- the current helper model already composes into a competitive two-hop
  real-text GraphRAG path on Gutenberg without requiring a new graph API
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
- two-hop helper composition is not yet a universal latency win; at higher
  rerank dimensions it narrows to parity with heap+btree rather than staying
  clearly ahead
- the current one-call wrapper still bakes in a `target_id` seed contract,
  which is wrong for the current fact-shaped multihop workload
- on the cogniformerus-style fact benchmark, the tuned helper is now a
  latency leader and can reach full observed parity with the strongest
  external paths we tested when moved to the `m=32 / ef_search=128` point

The correct next step is therefore:

> **tune the current narrow helper family before considering a bigger
> graph-specific subsystem**

That remains the smallest change that can still convert the observed
block-pruning advantage into an end-to-end query win.
