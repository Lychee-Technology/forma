# Federated Query Benchmark Reviewer Guide

Last updated: 2026-04-17  
Repository: `forma`

## Review Order

Review the benchmark work in this order:

1. `#40` benchmark foundation
2. `#41` workload execution follow-up

This keeps the review incremental: first the benchmark model and data pipeline, then the first executable workload path.

## PR #40 Focus

PR: `#40 add federated query benchmark foundation`

Primary areas to review:

- docs and issue breakdown
- benchmark CLI scaffold
- TPC-E-inspired schema fixtures
- deterministic generator
- tier split and tier load helpers
- harness support for richer benchmark records

Key files:

- `docs/cn/federated-query-benchmark-hld.md`
- `docs/federated-query/federated-query-benchmark-hld-en.md`
- `docs/federated-query/federated-query-benchmark-implementation-plan.md`
- `cmd/benchmark/main.go`
- `internal/e2e_harness/federated/benchmark/config.go`
- `internal/e2e_harness/federated/benchmark/schema.go`
- `internal/e2e_harness/federated/benchmark/generator.go`
- `internal/e2e_harness/federated/benchmark/tier.go`
- `internal/e2e_harness/federated/harness.go`
- `internal/e2e_harness/federated/s3_operations.go`

Questions to ask while reviewing:

- does the benchmark shape match current federated query terminology?
- are schema fixtures and generator defaults reasonable for phase 1?
- is tier preparation reusable enough for later workload and reporting work?
- are the harness changes scoped and justified?

## PR #41 Focus

PR: `#41 add federated benchmark workload execution`

Primary areas to review:

- workload execution path through the federated harness
- config overrides for small executable benchmark runs
- schema-scoped tier loading correctness
- deterministic hot/EAV seeding behavior
- deep pagination assertions at benchmark-run level

Key files:

- `internal/e2e_harness/federated/benchmark/runner.go`
- `internal/e2e_harness/federated/benchmark/workload.go`
- `internal/e2e_harness/federated/benchmark/dataset.go`
- `internal/e2e_harness/federated/benchmark/tier.go`
- `internal/e2e_harness/federated/seeding.go`
- `cmd/benchmark/main.go`
- `internal/e2e_harness/federated/benchmark_workload_execution_test.go`

Questions to ask while reviewing:

- does `RunWithHarness` have the right responsibilities for phase 1?
- are generator overrides sufficient for smoke and e2e benchmark execution?
- does schema-scoped tier loading prevent accidental cross-schema contamination?
- are the current correctness assertions useful without overfitting to the temporary harness query model?

## Known Gaps

These are intentionally left for follow-up work:

- richer filter-aware workload execution against a more expressive query model
- benchmark reporting/export beyond the current execution result structure
- stronger correctness assertions over result contents rather than only page-level invariants
- CI and operational benchmark guidance
