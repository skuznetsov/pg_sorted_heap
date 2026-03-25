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
- Therefore the next promising primitive is **a narrow C helper for expanding
  known seed IDs**, not a new graph storage engine and not a giant
  monolithic `graph_rag_scan()` API.

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

## Recommended roadmap

### Phase 0 — completed

- Build local prototype benchmark
- Falsify naive SQL assumptions

### Phase 1 — next

Implement **`sorted_heap_expand_ids()`**:

- input: array of source entity IDs
- optional relation filter
- direct zone-map/range expansion in C
- return rows or TIDs

Success criterion:

- beats heap+btree on the `seed_expand_in` pattern at medium scale

### Phase 2

Add GraphRAG composition query:

- ANN seed in SQL via `sorted_hnsw`
- expansion via `sorted_heap_expand_ids()`
- rerank in SQL or narrow helper

### Phase 3

Only if Phase 2 still shows SQL overhead:

- consider a fused helper for:
  - ANN seed IDs
  - expansion
  - rerank

## Current verdict

`sorted_heap` already has a plausible GraphRAG foundation, but the present
SQL-only prototype does **not** yet prove a compelling latency win for
multi-hop/seeded expansion.

The correct next step is therefore:

> **build a narrow expansion primitive, not a new graph engine**

That is the smallest change that can still convert the observed block-pruning
advantage into an end-to-end query win.
