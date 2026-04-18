# Federated Query Benchmark Implementation Plan

Last updated: 2026-04-17  
Repository: `forma`

## 1. Scope

This plan implements a TPC-E-inspired benchmark for hybrid cold + hot queries in Forma. The benchmark will extend the existing federated E2E harness rather than introducing a fully separate runtime or test framework.

The implementation is organized into four phases:

1. benchmark scaffolding and schema definitions
2. deterministic dataset generation and tier preparation
3. workload execution, correctness checks, and reporting
4. baseline runs and operational integration

## 2. Phase Breakdown

### Phase 1: Scaffolding and Schema Definitions

Deliver a minimal runnable benchmark skeleton.

Tasks:

- create a benchmark package or command entrypoint
- define benchmark config types
- define workload interfaces and result types
- add TPC-E-inspired schema fixtures for `trade`, `customer`, and `security`
- document CLI and config expectations

Exit criteria:

- benchmark command can initialize and parse config
- schema registration path is wired into the existing harness
- workload matrix is declared in code, even if some cases are stubbed

### Phase 2: Data Generation and Tier Preparation

Implement deterministic dataset generation with tier-aware loading.

Tasks:

- extend the generator to support `uniform`, `zipf`, `temporal`, `partition-skew`, and `hotspot-overlap`
- add calibration helpers for selectivity targets
- define tier mix profiles such as `60/30/10`, `40/20/40`, and `85/10/5`
- write base and delta parquet files through the existing harness
- insert hot rows into Postgres using existing seed helpers
- support overlap rows, update rows, delete rows, and restore rows

Exit criteria:

- same seed always produces the same dataset shape
- tiered dataset preparation works for at least `small` scale
- overlap and delete scenarios are represented in generated data

### Phase 3: Workloads, Correctness, and Reporting

Implement the workload matrix and result capture.

Tasks:

- add baseline pagination workloads
- add deep-pagination workloads including page `100,000`
- add cold-only, hot-only, and mixed-tier workloads
- add dedup, delete-shadowing, and last-write-wins validation cases
- capture latency, row count, tier mix, and execution metadata
- export console, JSON, and Markdown reports

Exit criteria:

- benchmark can run a chosen workload subset end to end
- each workload returns both performance metrics and correctness verdicts
- report files are generated in a stable format suitable for artifact retention

### Phase 4: Baselines and Operational Integration

Turn the benchmark into a repeatable engineering tool.

Tasks:

- run baseline results for `small` and `medium`
- document recommended run commands and expected runtimes
- define which workloads are safe for CI and which should be manual or nightly
- add operator guidance for interpreting results
- record known limitations and future optimization hooks such as keyset pagination

Exit criteria:

- at least one documented baseline exists for `small` and `medium`
- the repository contains a clear runbook for benchmark use
- the benchmark is ready for iterative optimization work

## 3. Proposed Code Layout

Suggested initial layout:

```text
internal/e2e_harness/federated/benchmark/
  config.go
  schema.go
  generator.go
  dataset.go
  workload.go
  runner.go
  report.go

cmd/benchmark/
  main.go

docs/federated-query/
  federated-query-benchmark-hld-en.md
  federated-query-benchmark-implementation-plan.md

docs/cn/
  federated-query-benchmark-hld.md
```

This layout keeps benchmark logic close to the existing harness while allowing a dedicated CLI.

## 4. Workload Matrix

The initial workload set should include:

- baseline pagination
- high-selectivity hot filter + pagination
- low-selectivity hot filter + pagination
- EAV filter + pagination
- mixed hot + EAV filter + pagination
- deep page jumps at 1, 100, 1,000, and 100,000
- hot-only time window queries
- cold-only historical queries
- mixed cold + hot window queries
- overlap and soft-delete correctness cases

Each workload should declare:

- name
- target schema
- supported distributions
- scale eligibility
- page size
- offset or page number
- expected correctness assertions

## 5. Verification Strategy

Verification has three layers:

### 5.1 Generator Verification

- validate row count
- validate distribution shape
- validate overlap ratio
- validate delete ratio
- validate reproducibility by seed

### 5.2 Query Correctness Verification

- compare merged results against expected deduplicated row sets
- verify stable ordering across repeated runs
- verify delete shadowing behavior
- verify attribute-level merge correctness for overlapping rows

### 5.3 Performance Verification

- record p50, p95, p99, and max
- record QPS for concurrent runs
- compare deep-pagination workloads across offsets
- compare distribution-specific performance regressions

## 6. Rollout Strategy

Recommended rollout order:

1. land documents and issue backlog
2. land scaffolding and schema fixtures
3. land generator and tier-prep support
4. land workload runner and result export
5. capture baselines and publish guidance

This keeps the implementation incremental and reviewable.

## 7. Deferred Work

The following items are explicitly deferred beyond phase 1:

- keyset-pagination execution path as a production benchmark alternative
- optimizer-driven dynamic workload shaping
- distributed benchmark agents
- automated benchmark trend dashboards
- official CI gating on large-scale benchmark thresholds

## 8. Success Criteria

The implementation is successful when:

- engineers can run the benchmark locally and in controlled environments
- benchmark datasets are reproducible
- deep-pagination behavior is measurable and comparable
- hybrid cold + hot correctness is automatically validated
- reports are useful enough to guide performance optimization work
