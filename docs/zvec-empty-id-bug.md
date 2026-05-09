# zvec empty-id retrieval bug

This note is an upstream-ready issue draft for a `zvec` retrieval defect that
showed up during GraphRAG parity work in `pg_sorted_heap`.

The short version:

- ANN scores still come back
- returned `doc.id` values become empty strings
- the failure reproduces on both:
  - a real-text Gutenberg GraphRAG corpus
  - a plain synthetic FP32 corpus

So this does **not** look like a PostgreSQL expansion/rerank bug.

Environment used for the verified local repro:

- `zvec 0.2.0`
- Python package at:
  - `/opt/homebrew/Caskroom/miniconda/base/lib/python3.12/site-packages/zvec`
- native extension:
  - `/opt/homebrew/Caskroom/miniconda/base/lib/python3.12/site-packages/_zvec.cpython-312-darwin.so`

## Minimal synthetic reproducer

Repo-owned script:

- [`scripts/repro_zvec_synthetic_threshold.py`](../scripts/repro_zvec_synthetic_threshold.py)

Command:

```bash
python3 scripts/repro_zvec_synthetic_threshold.py --rows 4900,4950,5000 --query-count 5
```

Current verified output shape:

```text
SYNTH_THRESH|rows=4900|status=ok|first_bad_query=None|sample=None
SYNTH_THRESH|rows=4950|status=bad|first_bad_query=1|sample=['', '', '', '', '', '', '']
SYNTH_THRESH|rows=5000|status=bad|first_bad_query=1|sample=['', '', '', '', '', '', '']
```

stderr also reports:

```text
Failed to find target chunk for index 4945
```

Current minimal signature:

- `dim=32`
- `ef_search=64`
- `topk=7`
- `rows=4950`

Neighbor controls:

- `rows=4900`, same params: `ok`
- `rows=4950`, `topk<=6`: `ok`
- `rows=4950`, `topk>=7`: `bad`

The compact repro also survives simple runtime knob changes:

- `memory_limit_mb=8192`: `bad`
- `memory_limit_mb=1024`: `bad`
- `memory_limit_mb=256`: `bad`
- `query_threads=1`, `optimize_threads=1`: `bad`
- `query_threads=2`, `optimize_threads=2`: `bad`
- `query_threads=4`, `optimize_threads=4`: `bad`

So the current minimal case does not look like a trivial thread-count or
memory-budget artifact.

It also survives broad HNSW parameter changes on the same compact case:

- `ef_search=16`, `ef_construction=16`, `m=8`: `bad`
- `ef_search=16`, `ef_construction=64`, `m=16`: `bad`
- `ef_search=32`, `ef_construction=64`, `m=16`: `bad`
- `ef_search=64`, `ef_construction=64`, `m=16`: `bad`
- `ef_search=128`, `ef_construction=64`, `m=16`: `bad`
- `ef_search=64`, `ef_construction=128`, `m=16`: `bad`
- `ef_search=64`, `ef_construction=64`, `m=8`: `bad`
- `ef_search=64`, `ef_construction=64`, `m=32`: `bad`

So the compact failing case does not look like a fragile HNSW tuning artifact
either.

## Stronger diagnostics

On the compact synthetic case:

- `rows=4950`, `topk=6`
  - valid ids come back
- `rows=4950`, `topk=7`
  - scores still come back
  - every `doc.id` becomes `''`

That means the failure is not "query returns nothing". Ranking still appears to
produce plausible scores, but document metadata resolution fails.

Representative observation:

```text
CASE 4950 6 [('1600', 0.12408530712127686), ..., ('2946', 0.14136314392089844)]
CASE 4950 7 [('', 0.12408530712127686), ..., ('', 0.14136314392089844)]
```

The bug is also non-monotonic by row count. Verified examples:

- bad: `4950`, `5000`, `7500`, `7900`, `16000`, `28000`, `30000`, `45000`, `60000`
- ok: `4900`, `7000`, `7800`, `24000`, `75000`

So this is not a simple "after N rows everything breaks" threshold.

## Real-text corroboration

Repo-owned script:

- [`scripts/repro_zvec_gutenberg_threshold.py`](../scripts/repro_zvec_gutenberg_threshold.py)

Current verified Gutenberg signature:

- `dim=32`
- `topk=16`
- `ef_search=64`
- `64x256`, `80x256`, `96x256`, `112x256` slices: `ok`
- `128x256` (`58,954` rows): `bad`

Observed failure:

```text
Failed to find target chunk for index 58379
```

Returned ids are empty strings / unmapped ids for the first bad probe.

## Additional context

One larger synthetic case gives another useful hint:

- `rows=16000`
- exact cosine inspection shows the best-score bucket spans
  `1000, 2000, ..., 16000`
- `zvec` already returns empty ids at `topk=5`

This does not prove the internal root cause, but it suggests the failure may
depend on candidate-materialization / metadata-fetch paths rather than on the
ANN score computation itself.

The shipped native binary also contains the exact failing message plus nearby
storage component paths:

```text
/Users/cuiys/workspace/zvec/src/db/index/storage/mmap_forward_store.cc
/Users/cuiys/workspace/zvec/src/db/index/storage/bufferpool_forward_store.cc
Failed to find target chunk for index %d
Encountered empty chunk at index %d
```

That does not prove which codepath is at fault, but it makes the working
component hypothesis narrower: the failure is plausibly in forward-store chunk
lookup or chunk materialization rather than in HNSW distance ranking itself.

Debug file logging on the compact synthetic case adds one more strong clue.
With `log_level=DEBUG`, `query_threads=1`, and the same `4950 / topk=7` repro,
`zvec` reports:

```text
Opened IPC with 4950 rows, 2 cols, 2 chunks, is_fixed_batch_size[1] fixed_batch_size[2432]
...
Failed to find target chunk for index 4945
...
Record batch: _zvec_uid_: [
  null,
  null,
  null,
  null,
  null,
  null,
  null
]
```

So the strongest current reading is:

- the forward store opens successfully
- the query reaches the recall/output stage
- metadata resolution for `_zvec_uid_` fails and the returned batch carries
  null ids even though scores are still present

## Why this matters

For the `pg_sorted_heap` GraphRAG benchmark harness, this bug currently blocks a
clean large-slice `zvec` parity row. PostgreSQL-side expansion+rereank remains
stable; the unstable stage is the `zvec` ANN seed retrieval itself.

## Status

Verified locally with repo-owned reproducers. No claim yet about the exact
internal root cause inside `zvec`.
