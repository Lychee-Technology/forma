# Federated Benchmark Autoresearch Pilot

This directory holds the first benchmark-driven autoresearch pilot for federated query performance.

The goal is narrower than the existing test-oriented autoresearch loop under `tools/autoresearch/testing`.
This pilot is designed to answer a single question: can the benchmark produce stable enough evidence to judge candidate performance changes for federated query hotspots?

## Scope

- benchmark-driven performance evidence only
- no coverage-based keep/discard logic
- no controller-owned commit loop yet
- manual review of benchmark diffs remains the final decision step

## Targets

- `postgres_duckdb_query`
  - focus: deep pagination, SQL shape, pushdown, and count/sort pressure in `internal/postgres_duckdb_query.go`
- `federated_query_execution`
  - focus: live harness execution, tier composition, fallback, and routing provenance in `internal/e2e_harness/federated/query.go`
- `entity_query_service`
  - focus: application-facing query normalization, sort binding, and pagination metadata in `internal/entity_query_service.go`

The current live benchmark now exercises `baseline-page-1` through a real `EntityManager.Query` service path, so service-layer query work is benchmark-backed for that workload while deeper routing and concurrency tuning still primarily lives under the harness execution target.

## Gates

Each target supports three deterministic local gates:

- `fast`: smallest live subset for quick iteration
- `medium`: wider local review subset
- `heavy`: widest manual subset still intended for benchmark-backed keep/discard review

Each gate run starts from a clean target-and-gate artifact directory before writing new baseline, candidate, or diff outputs.
That keeps repeated local runs deterministic and prevents stale files from earlier captures from contaminating later review.

Artifacts are now written under gate-scoped directories:

- `reports/baseline/<target>/<gate>/`
- `reports/candidates/<target>/<gate>/`
- `reports/diff/<target>/<gate>/`
- `reports/runs/<target>/<gate>/`

## Layout

- `common.sh`: shared path and target helpers
- `program-perf.md`: benchmark-specific autoresearch rules
- `prompts/`: single-candidate performance prompts
- `targets/`: performance target briefs
- `scripts/autoloop.sh`: supervised benchmark autoresearch controller
- `scripts/opencode_autoresearch.sh`: OpenCode prompt launcher for one performance candidate
- `scripts/benchmark_baseline.sh`: capture baseline benchmark artifacts
- `scripts/benchmark_candidate.sh`: capture candidate benchmark artifacts
- `scripts/benchmark_gate.sh`: compare baseline and candidate artifacts and emit a review summary
- `results.tsv`: controller-written run ledger
- `issues.tsv`: backlog of harness, environment, bug, and perf-opportunity findings
- `reports/`: generated benchmark artifacts and diffs

## Recommended Flow

If you want the live benchmark to reuse external infrastructure instead of starting testcontainers, export these vars before running the scripts:

```bash
export FEDERATED_BENCHMARK_PG_HOST=10.0.0.16
export FEDERATED_BENCHMARK_PG_PORT=5432
export FEDERATED_BENCHMARK_PG_USER=postgres
export FEDERATED_BENCHMARK_PG_PASSWORD=postgres
export FEDERATED_BENCHMARK_PG_DB=postgres
export FEDERATED_BENCHMARK_PG_SSLMODE=disable

export FEDERATED_BENCHMARK_S3_ENDPOINT=http://10.0.0.36:9000
export FEDERATED_BENCHMARK_S3_ACCESS_KEY=rustfsaccess
export FEDERATED_BENCHMARK_S3_SECRET_KEY=rustfssecret
export FEDERATED_BENCHMARK_S3_BUCKET=test-bucket
export FEDERATED_BENCHMARK_S3_PREFIX=test-project
export FEDERATED_BENCHMARK_S3_REGION=us-east-1
```

1. Capture a baseline:

```bash
./tools/autoresearch/benchmark/federated/scripts/benchmark_baseline.sh postgres_duckdb_query fast
```

2. Apply a candidate code change.

3. Capture candidate evidence:

```bash
./tools/autoresearch/benchmark/federated/scripts/benchmark_candidate.sh postgres_duckdb_query fast
```

4. Compare baseline and candidate:

```bash
./tools/autoresearch/benchmark/federated/scripts/benchmark_gate.sh postgres_duckdb_query fast
```

Swap `postgres_duckdb_query` for `federated_query_execution` or `entity_query_service`, and `fast` for `medium` or `heavy`, when you need a different target or benchmark subset.

## Current Decision Model

This pilot does not auto-commit or auto-discard changes.

Use the generated diff artifacts for manual review with these default rules:

- any correctness regression: discard
- infrastructure failure: rerun before judging
- no target-workload latency improvement: discard
- clear protected-workload regression: discard

## Supervised Autoresearch Loop

This directory now includes a supervised single-candidate autoresearch controller modeled after `tools/autoresearch/testing`, but adapted for benchmark-backed performance work.

Run a dry-run first:

```bash
./tools/autoresearch/benchmark/federated/scripts/autoloop.sh \
  --model github-copilot/gpt-5-mini \
  --target postgres_duckdb_query \
  --gate fast \
  --baseline \
  --iterations 3 \
  --dry-run
```

Normal run:

```bash
./tools/autoresearch/benchmark/federated/scripts/autoloop.sh \
  --model github-copilot/gpt-5-mini \
  --target postgres_duckdb_query \
  --gate fast \
  --baseline \
  --iterations 3
```

Current controller behavior:

- creates a dedicated research worktree and branch
- asks OpenCode for exactly one performance candidate per iteration
- runs benchmark candidate + compare after each candidate
- archives the patch, benchmark artifacts, and recommendation under `reports/runs/`
- records the outcome in `results.tsv`
- records benchmark, harness, and environment findings in `issues.tsv`
- discards the worktree changes after archiving so the loop stays supervised and reviewable

## Relationship To `tools/autoresearch/testing`

This pilot intentionally lives outside `tools/autoresearch/testing` because its evidence model, artifact shape, and decision rules are benchmark-oriented rather than test-oriented.

In practice:

- `tools/autoresearch/testing/targets/*.md` describe BDD-style regression targets and test scenarios
- `tools/autoresearch/benchmark/federated/targets/*.md` describe performance hotspots, protected workloads, expected win areas, and benchmark gate subsets
- a benchmark target brief should justify decisions with workload-level evidence, not only with behavioral test coverage

Current target coverage:

- `postgres_duckdb_query`: benchmark-backed deep-pagination and DuckDB SQL-path tuning
- `federated_query_execution`: benchmark-backed mixed execution, fallback, and routing provenance tuning
- `entity_query_service`: benchmark-backed service-layer query normalization and pagination work for the `baseline-page-1` workload

If the pilot proves reliable, a later follow-up can add a benchmark-specific controller loop or connect this path to the broader autoresearch framework.
