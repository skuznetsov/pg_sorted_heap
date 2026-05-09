# Research and experimental benchmark targets.
#
# These targets are intentionally kept outside the main Makefile path so the
# default release/test surface stays focused on stable extension workflows.

TURBOQUANT_DATASET ?= glove-100
TURBOQUANT_SAMPLE_SIZE ?= 4000
TURBOQUANT_QUERY_COUNT ?= 50
TURBOQUANT_K ?= 10
TURBOQUANT_SEED ?= 42
TURBOQUANT_PQ_M ?= 0
TURBOQUANT_PQ_BITS ?= 8
TURBOQUANT_PQ_MAX_TRAIN ?= 20000
TURBOQUANT_TURBO_BITS ?= 4
TURBOQUANT_METRIC ?= cosine
TURBOQUANT_PG_DSN ?=
TURBOQUANT_BASE_SQL ?=
TURBOQUANT_QUERY_SQL ?=
TURBOQUANT_SHARED_SQL ?=
TURBOQUANT_FOLDS ?= 5
TURBOQUANT_METHODS ?=
TURBOQUANT_GUTENBERG_METHODS ?= turboquant_mse,turboquant_blockhadamard,turboquant_blockhadamard_twopass,turboquant_block32_dither
TURBOQUANT_GUTENBERG_SCREEN_METHODS ?= turboquant_blockhadamard_packed4,turboquant_blockhadamard_packed4_topk
TURBOQUANT_LOCAL_CUBE_PORT ?= 30432
TURBOQUANT_LOCAL_CUBE_DB ?= cogniformerus
TURBOQUANT_ARGS ?=
FH_PG_DSN ?=
FH_STORE_PATH ?= /tmp/fh_bench.store
FH_THREADS ?= 8

build-turboquant-packed-helper:
	@mkdir -p ./build && \
	if [ "$$(uname -s)" = "Darwin" ]; then \
		$${CC:-cc} -O3 -std=c99 -dynamiclib -o ./build/turboquant_packed_adc.dylib ./scripts/turboquant_packed_adc.c; \
	else \
		$${CC:-cc} -O3 -std=c99 -shared -fPIC -o ./build/turboquant_packed_adc.so ./scripts/turboquant_packed_adc.c; \
	fi

bench-turboquant:
	@PYTHON_BIN="$$(./scripts/find_vector_python.sh)" && \
	"$$PYTHON_BIN" ./scripts/bench_turboquant_retrieval.py \
	  --dataset $(TURBOQUANT_DATASET) \
	  --sample-size $(TURBOQUANT_SAMPLE_SIZE) \
	  --query-count $(TURBOQUANT_QUERY_COUNT) \
	  --k $(TURBOQUANT_K) \
	  --seed $(TURBOQUANT_SEED) \
	  --pq-m $(TURBOQUANT_PQ_M) \
	  --pq-bits $(TURBOQUANT_PQ_BITS) \
	  --pq-max-train $(TURBOQUANT_PQ_MAX_TRAIN) \
	  --turbo-bits $(TURBOQUANT_TURBO_BITS) \
	  $(if $(TURBOQUANT_METHODS),--methods '$(TURBOQUANT_METHODS)') \
	  $(TURBOQUANT_ARGS)

bench-turboquant-sql:
	@if [ -z "$$TURBOQUANT_PG_DSN" ] || [ -z "$(TURBOQUANT_BASE_SQL)" ] || [ -z "$(TURBOQUANT_QUERY_SQL)" ]; then \
		echo "bench-turboquant-sql requires exported TURBOQUANT_PG_DSN, TURBOQUANT_BASE_SQL, and TURBOQUANT_QUERY_SQL" >&2; \
		exit 2; \
	fi
	@PYTHON_BIN="$$(./scripts/find_vector_python.sh)" && \
	"$$PYTHON_BIN" ./scripts/bench_turboquant_retrieval.py \
	  --base-sql "$(TURBOQUANT_BASE_SQL)" \
	  --query-sql "$(TURBOQUANT_QUERY_SQL)" \
	  --metric $(TURBOQUANT_METRIC) \
	  --k $(TURBOQUANT_K) \
	  --seed $(TURBOQUANT_SEED) \
	  --pq-m $(TURBOQUANT_PQ_M) \
	  --pq-bits $(TURBOQUANT_PQ_BITS) \
	  --pq-max-train $(TURBOQUANT_PQ_MAX_TRAIN) \
	  --turbo-bits $(TURBOQUANT_TURBO_BITS) \
	  $(if $(TURBOQUANT_METHODS),--methods '$(TURBOQUANT_METHODS)') \
	  $(TURBOQUANT_ARGS)

bench-turboquant-sql-holdout:
	@if [ -z "$$TURBOQUANT_PG_DSN" ] || [ -z "$(TURBOQUANT_SHARED_SQL)" ]; then \
		echo "bench-turboquant-sql-holdout requires exported TURBOQUANT_PG_DSN and TURBOQUANT_SHARED_SQL" >&2; \
		exit 2; \
	fi
	@PYTHON_BIN="$$(./scripts/find_vector_python.sh)" && \
	"$$PYTHON_BIN" ./scripts/bench_turboquant_retrieval.py \
	  --shared-sql "$(TURBOQUANT_SHARED_SQL)" \
	  --metric $(TURBOQUANT_METRIC) \
	  --query-count $(TURBOQUANT_QUERY_COUNT) \
	  --folds $(TURBOQUANT_FOLDS) \
	  --k $(TURBOQUANT_K) \
	  --seed $(TURBOQUANT_SEED) \
	  --pq-m $(TURBOQUANT_PQ_M) \
	  --pq-bits $(TURBOQUANT_PQ_BITS) \
	  --pq-max-train $(TURBOQUANT_PQ_MAX_TRAIN) \
	  --turbo-bits $(TURBOQUANT_TURBO_BITS) \
	  $(if $(TURBOQUANT_METHODS),--methods '$(TURBOQUANT_METHODS)') \
	  $(TURBOQUANT_ARGS)

define resolve_turboquant_local_cube_dsn
	if [ -z "$$TURBOQUANT_PG_DSN" ]; then \
		echo "bench-turboquant-gutenberg requires exported TURBOQUANT_PG_DSN" >&2; \
		exit 2; \
	fi; \
	PYTHON_BIN="$$(./scripts/find_vector_python.sh)"
endef

bench-turboquant-gutenberg: bench-turboquant-gutenberg-vetted

bench-turboquant-gutenberg-screen:
	@$(resolve_turboquant_local_cube_dsn) && \
	"$$PYTHON_BIN" ./scripts/bench_turboquant_packed_screen.py \
	  --base-sql "SELECT embedding::text FROM public.gutenberg_gptoss_sh ORDER BY id" \
	  --query-sql "SELECT q.qvec::text FROM public.bench_gptoss_queries q JOIN public.bench_hnsw_gt gt USING (qid) ORDER BY q.qid" \
	  --metric cosine \
	  --k $(TURBOQUANT_K) \
	  --turbo-bits $(TURBOQUANT_TURBO_BITS) \
	  --seed $(TURBOQUANT_SEED) \
	  --methods '$(TURBOQUANT_GUTENBERG_SCREEN_METHODS)' \
	  --parity-against turboquant_blockhadamard_packed4 \
	  $(TURBOQUANT_ARGS)

bench-turboquant-gutenberg-vetted:
	@$(resolve_turboquant_local_cube_dsn) && \
	"$$PYTHON_BIN" ./scripts/bench_turboquant_retrieval.py \
	  --base-sql "SELECT embedding::text FROM public.gutenberg_gptoss_sh ORDER BY id" \
	  --query-sql "SELECT q.qvec::text FROM public.bench_gptoss_queries q JOIN public.bench_hnsw_gt gt USING (qid) ORDER BY q.qid" \
	  --metric cosine \
	  --k $(TURBOQUANT_K) \
	  --seed $(TURBOQUANT_SEED) \
	  --pq-m $(TURBOQUANT_PQ_M) \
	  --pq-bits $(TURBOQUANT_PQ_BITS) \
	  --pq-max-train $(TURBOQUANT_PQ_MAX_TRAIN) \
	  --turbo-bits $(TURBOQUANT_TURBO_BITS) \
	  --skip-fp16 --skip-sq8 --skip-pq \
	  --methods '$(TURBOQUANT_GUTENBERG_METHODS)' \
	  $(TURBOQUANT_ARGS)

bench-turboquant-gutenberg-full:
	@$(resolve_turboquant_local_cube_dsn) && \
	"$$PYTHON_BIN" ./scripts/bench_turboquant_retrieval.py \
	  --base-sql "SELECT embedding::text FROM public.gutenberg_gptoss_sh ORDER BY id" \
	  --query-sql "SELECT qvec::text FROM public.bench_gptoss_queries ORDER BY qid" \
	  --metric cosine \
	  --k $(TURBOQUANT_K) \
	  --seed $(TURBOQUANT_SEED) \
	  --pq-m $(TURBOQUANT_PQ_M) \
	  --pq-bits $(TURBOQUANT_PQ_BITS) \
	  --pq-max-train $(TURBOQUANT_PQ_MAX_TRAIN) \
	  --turbo-bits $(TURBOQUANT_TURBO_BITS) \
	  --skip-fp16 --skip-sq8 --skip-pq \
	  --methods '$(TURBOQUANT_GUTENBERG_METHODS)' \
	  $(TURBOQUANT_ARGS)

test-flashhadamard:
	psql -f ./scripts/test_flashhadamard.sql

bench-flashhadamard:
	@echo "FlashHadamard benchmark (FH_THREADS=$(FH_THREADS))"
	@psql -c "\i sql/flashhadamard_experimental.sql" 2>/dev/null || true
	@echo "Building store..."
	@psql -c "\\timing on" \
		-c "SELECT flashhadamard_store_build('gutenberg_local'::regclass, 'embedding', '$(FH_STORE_PATH)', 42, 16)" \
		2>&1 | grep -E "Time:|done"
	@echo "Benchmark (10 queries, warm)..."
	@psql -c "CREATE TEMP TABLE _fhbq AS SELECT embedding FROM gutenberg_local ORDER BY id LIMIT 10" \
		-c "SELECT count(*) FROM flashhadamard_store_scan('$(FH_STORE_PATH)', (SELECT embedding FROM _fhbq LIMIT 1), 10, 12, 42, 16)" \
		-c "\\timing on" \
		-c "SELECT sum(c) FROM (SELECT (SELECT count(*) FROM flashhadamard_store_scan('$(FH_STORE_PATH)', q.embedding, 10, 12, 42, 16)) c FROM _fhbq q) t" \
		2>&1 | grep -E "Time:|sum"
	@echo "Done. Set FH_THREADS=N before PG start to control parallelism."

research-help:
	@echo "pg_sorted_heap research targets:"
	@echo "  make build-turboquant-packed-helper"
	@echo "  make bench-turboquant TURBOQUANT_DATASET=<glove-100|nytimes-256> TURBOQUANT_SAMPLE_SIZE=<n> TURBOQUANT_QUERY_COUNT=<n> TURBOQUANT_K=<k> TURBOQUANT_PQ_M=<0|m> TURBOQUANT_PQ_BITS=<bits> TURBOQUANT_PQ_MAX_TRAIN=<n> TURBOQUANT_TURBO_BITS=<bits> TURBOQUANT_ARGS='<extra args>'"
	@echo "  export TURBOQUANT_PG_DSN='<dsn>'; make bench-turboquant-sql TURBOQUANT_BASE_SQL='<sql>' TURBOQUANT_QUERY_SQL='<sql>' TURBOQUANT_METRIC=<cosine|ip> TURBOQUANT_K=<k> TURBOQUANT_PQ_M=<0|m> TURBOQUANT_PQ_BITS=<bits> TURBOQUANT_PQ_MAX_TRAIN=<n> TURBOQUANT_TURBO_BITS=<bits> TURBOQUANT_ARGS='<extra args>'"
	@echo "  export TURBOQUANT_PG_DSN='<dsn>'; make bench-turboquant-sql-holdout TURBOQUANT_SHARED_SQL='<sql>' TURBOQUANT_METRIC=<cosine|ip> TURBOQUANT_QUERY_COUNT=<n> TURBOQUANT_FOLDS=<n> TURBOQUANT_K=<k> TURBOQUANT_PQ_M=<0|m> TURBOQUANT_PQ_BITS=<bits> TURBOQUANT_PQ_MAX_TRAIN=<n> TURBOQUANT_TURBO_BITS=<bits> TURBOQUANT_ARGS='<extra args>'"
	@echo "  export TURBOQUANT_PG_DSN='<dsn>'; make bench-turboquant-gutenberg-vetted TURBOQUANT_GUTENBERG_METHODS='<csv methods>' TURBOQUANT_TURBO_BITS=<bits> TURBOQUANT_K=<k> TURBOQUANT_ARGS='<extra args>'"
	@echo "  export TURBOQUANT_PG_DSN='<dsn>'; make bench-turboquant-gutenberg-screen TURBOQUANT_GUTENBERG_SCREEN_METHODS='<csv packed methods>' TURBOQUANT_TURBO_BITS=<bits> TURBOQUANT_K=<k> TURBOQUANT_ARGS='<extra args>'"
	@echo "  export TURBOQUANT_PG_DSN='<dsn>'; make bench-turboquant-gutenberg-full TURBOQUANT_GUTENBERG_METHODS='<csv methods>' TURBOQUANT_TURBO_BITS=<bits> TURBOQUANT_K=<k> TURBOQUANT_ARGS='<extra args>'"
	@echo "  make test-flashhadamard"
	@echo "  make bench-flashhadamard FH_THREADS=<n> FH_STORE_PATH=<path>"
	@echo ""
	@echo "defaults:"
	@echo "  TURBOQUANT_DATASET=$(TURBOQUANT_DATASET) TURBOQUANT_SAMPLE_SIZE=$(TURBOQUANT_SAMPLE_SIZE) TURBOQUANT_QUERY_COUNT=$(TURBOQUANT_QUERY_COUNT) TURBOQUANT_K=$(TURBOQUANT_K) TURBOQUANT_SEED=$(TURBOQUANT_SEED) TURBOQUANT_PQ_M=$(TURBOQUANT_PQ_M) TURBOQUANT_PQ_BITS=$(TURBOQUANT_PQ_BITS) TURBOQUANT_PQ_MAX_TRAIN=$(TURBOQUANT_PQ_MAX_TRAIN) TURBOQUANT_TURBO_BITS=$(TURBOQUANT_TURBO_BITS) TURBOQUANT_METRIC=$(TURBOQUANT_METRIC) TURBOQUANT_FOLDS=$(TURBOQUANT_FOLDS) TURBOQUANT_METHODS=$(TURBOQUANT_METHODS) TURBOQUANT_GUTENBERG_METHODS=$(TURBOQUANT_GUTENBERG_METHODS) TURBOQUANT_LOCAL_CUBE_PORT=$(TURBOQUANT_LOCAL_CUBE_PORT) TURBOQUANT_LOCAL_CUBE_DB=$(TURBOQUANT_LOCAL_CUBE_DB) FH_THREADS=$(FH_THREADS) FH_STORE_PATH=$(FH_STORE_PATH)"
