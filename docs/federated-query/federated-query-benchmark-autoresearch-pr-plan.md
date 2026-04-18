# Federated Query Benchmark Autoresearch PR Plan

Last updated: 2026-04-18  
Repository: `forma`

## 1. Objective

The next benchmark phase is not about widening TPC-E coverage. It is about turning the existing TPC-E-inspired benchmark into a stable optimization judge for federated-query tuning and future autoresearch loops.

The benchmark is ready for autoresearch-guided optimization only when it can do all of the following reliably:

- execute live benchmark workloads end to end without relying on ad hoc test-only entrypoints
- reject correctness regressions before reporting latency wins
- emit stable, workload-level evidence that can be compared to a saved baseline
- support fast and repeatable benchmark subsets suitable for automated keep/discard decisions

## 2. Delivery Principles

- correctness before speed: any failed correctness assertion blocks optimization claims
- `trade` remains the primary benchmark schema until the execution path is trustworthy
- benchmark artifacts must be machine-comparable, not just human-readable
- small reproducible runs come before larger and noisier runs
- autoresearch should consume benchmark evidence only after the benchmark becomes a trustworthy gate

## 3. PR Sequence

### PR 1: Executable Benchmark Runtime

Goal: promote the benchmark from scaffolded CLI validation to a supported live-execution path.

Scope:

- add an executable benchmark mode backed by the live federated harness
- preserve existing `smoke` and `plan` modes for cheap validation
- make benchmark run outputs identify whether execution was validation-only or live
- cover the live path with focused tests and operator commands

Exit criteria:

- `cmd/benchmark` can execute supported workloads against the harness
- local execution does not require reaching into test-only helper code by hand
- the benchmark has a single documented runtime story for validation and live execution

### PR 2: Result Semantics and Correctness Hardening

Goal: make benchmark results safe to use as optimization evidence.

Scope:

- fix `total_records` and deep-page result semantics
- strengthen assertions for dedup, ordering stability, delete shadowing, and last-write-wins
- add explicit workload failure signals for correctness regressions
- tighten benchmark result structures so that downstream automation can trust them

Exit criteria:

- workload results distinguish empty pages from broken query behavior
- repeated runs with the same seed produce stable correctness verdicts
- benchmark failures clearly separate correctness failures from infrastructure failures

### PR 3: Filter Fidelity and Schema-Scoped Workloads

Goal: make the executed workloads match the benchmark model instead of only the current happy path.

Scope:

- complete hot, EAV, and mixed filter execution fidelity
- make schema targeting explicit in execution instead of declarative-only metadata
- keep `trade` as the primary executed schema while retaining fixture sanity coverage for `customer` and `security`
- expand workload assertions around schema-scoped and filter-scoped behavior

Exit criteria:

- hot and EAV workloads execute with consistent semantics across tiers
- benchmark execution honors workload schema intent
- mixed-tier and deep-page workloads are representative enough for tuning work

### PR 4: Metrics, Artifact Schema, and Baseline Diff

Goal: make benchmark artifacts directly usable for automated comparison.

Scope:

- extend summary metrics beyond latency percentiles and QPS
- capture workload-level metadata, environment data, and benchmark identifiers
- add machine-readable diff support against a saved baseline
- keep output formats stable across runs with the same seed and workload set

Exit criteria:

- benchmark output can answer which workloads improved, regressed, or stayed flat
- saved baselines are structurally stable and safe for automation
- artifact fields are sufficient for later autoresearch decision rules

### PR 5: Baseline Capture and CI Policy

Goal: codify which benchmark subsets are safe for local automation, CI, and manual tuning review.

Scope:

- capture initial `small` and `medium` benchmark baselines
- define fast, medium, and heavy benchmark subsets
- document acceptable runtime envelopes and environment expectations
- align CI commands with the benchmark's live and validation modes

Exit criteria:

- the repository has documented baseline artifacts and comparison guidance
- CI-safe subsets are clearly separated from heavier manual runs
- benchmark commands used by later autoresearch loops are stable and documented

### PR 6: Autoresearch Perf Loop Scaffold

Goal: add a performance-oriented autoresearch loop that consumes benchmark evidence instead of only test coverage.

Scope:

- introduce a performance autoresearch mode alongside the existing testing mode
- define decision artifact fields for latency, regressions, and correctness gates
- add controller support for benchmark baseline, candidate run, and keep/discard evaluation
- keep the same commit-per-keep operational model

Exit criteria:

- a single performance iteration can run, compare benchmark evidence, and emit a decision artifact
- keep/discard logic can block candidates that regress correctness or protected workloads
- the new flow does not weaken the current test-oriented autoresearch safeguards

### PR 7: Autoresearch Perf Targets and Gates

Goal: make the performance loop actionable for real tuning targets.

Scope:

- add performance target briefs for the first federated-query hotspots
- define fast, medium, and heavy benchmark gates for those targets
- make target guidance explicit about protected workloads and known risk areas
- document how performance targets differ from BDD testing targets

Exit criteria:

- target authors can run the perf loop against a known module and workload subset
- gate scripts are deterministic enough for iterative local use
- target briefs point at benchmark evidence rather than vague speed goals

### PR 8: Optimization Wave 1, Deep Pagination

Goal: use the benchmark to improve the highest-risk pagination path first.

Scope:

- investigate deep-page cost in merge-before-pagination flows
- optimize query planning or execution where page depth dominates latency
- validate improvements with the benchmark fast and medium gates
- record observed wins and remaining bottlenecks in the artifact trail

Exit criteria:

- target deep-page workloads improve without correctness regressions
- the benchmark can prove the win at workload level rather than anecdotal logs

### PR 9: Optimization Wave 2, Filter Pushdown and EAV

Goal: improve selective filter performance after the pagination path is measurable and trustworthy.

Scope:

- investigate hot filter, EAV filter, and mixed filter routing costs
- optimize pushdown and tier merge behavior where possible
- use benchmark deltas to guard against regressions in non-target workloads

Exit criteria:

- targeted filter workloads improve under benchmark comparison
- deep-page and baseline workloads remain within guardrail thresholds

### PR 10: Optimization Wave 3, Routing and Concurrency

Goal: improve strategy selection and tail behavior under repeated runs.

Scope:

- investigate routing heuristics between Postgres and DuckDB paths
- add or refine fixed-concurrency benchmark execution
- tune workload groups where tail latency or routing instability dominates
- protect correctness and prior wins with the same benchmark guardrails

Exit criteria:

- routing-sensitive workloads show measurable improvement or clearer guardrails
- concurrency-aware benchmark runs remain reproducible enough for repeated comparison

## 4. Ready-For-Autoresearch Gate

The repository should not treat the benchmark as a performance autoresearch gate until PRs 1 through 5 are complete.

At that point, the minimum bar is:

- live benchmark execution exists as a supported command path
- correctness assertions are strong enough to reject bad optimizations
- workload-level baselines and diffs are machine-readable
- the benchmark has CI-safe and local-safe execution subsets

PRs 6 and 7 then turn that benchmark into an optimization controller input. PRs 8 through 10 are the first optimization campaigns that should rely on it.
