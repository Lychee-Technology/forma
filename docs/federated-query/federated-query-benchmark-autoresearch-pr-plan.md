# Federated Query Benchmark Autoresearch PR Plan

Last updated: 2026-04-20  
Repository: `forma`

## 1. Objective

The next benchmark phase is not about widening TPC-E coverage. It is about turning the existing TPC-E-inspired benchmark into a stable optimization judge for federated-query tuning and future autoresearch loops.

The benchmark is ready for autoresearch-guided optimization only when it can do all of the following reliably:

- execute live benchmark workloads end to end without relying on ad hoc test-only entrypoints
- reject correctness regressions before reporting latency wins
- emit stable, workload-level evidence that can be compared to a saved baseline
- support fast and repeatable benchmark subsets suitable for automated keep/discard decisions

## 1.1 Current State

The repository has already landed the first benchmark foundation wave:

- a supported CLI with `smoke`, `plan`, and `live` benchmark modes
- live benchmark execution backed by the federated harness
- stronger correctness and failure semantics in benchmark results
- workload-level summaries, machine-readable diff support, and baseline capture artifacts
- repeated-run stability summaries and grouped oracle provenance in reports
- documented baseline presets, CI-safe benchmark subsets, and a benchmark readiness gate

This means the benchmark readiness phase is complete. The next phase is benchmark-driven automation and optimization work, rather than more readiness hardening.

## 2. Delivery Principles

- correctness before speed: any failed correctness assertion blocks optimization claims
- `trade` remains the primary benchmark schema until the execution path is trustworthy
- benchmark artifacts must be machine-comparable, not just human-readable
- small reproducible runs come before larger and noisier runs
- autoresearch should consume benchmark evidence only after the benchmark becomes a trustworthy gate

## 3. PR Sequence

### PR 0: Backlog Alignment

Goal: align the benchmark roadmap with the shipped runtime and the remaining open gaps.

Scope:

- audit the current benchmark issue set against the shipped CLI, runtime, workload matrix, reports, and docs
- distinguish closed foundation work from still-open readiness work
- update the umbrella roadmap so later optimization issues point at current prerequisites

Exit criteria:

- the umbrella issue reflects the current repository state instead of the original phase-1 split
- remaining readiness issues are ordered and non-duplicative
- newer follow-up issues clearly refine rather than repeat earlier closed work

### PR 1: Workload Matrix Completion

Goal: finish the remaining representative workload coverage needed before optimization waves begin.

Scope:

- add the remaining low-selectivity, mixed-filter, and tier-targeted window cases to the workload matrix
- ensure the new workload set executes through the live path with explicit expected-result semantics
- cover the expanded matrix with benchmark runner and harness-backed tests

Exit criteria:

- the benchmark covers the agreed pre-optimization workload matrix
- newly added workloads are executable rather than declarative-only
- later deep-page, filter, and routing issues have the intended benchmark targets available

### PR 2: Selective Oracle Generalization

Goal: remove workload-name-specific truth-pass behavior from selective benchmark correctness checks.

Scope:

- extract selective workload truth-pass logic into a reusable oracle mechanism
- define when `loaded-state` is sufficient and when `truth-pass` is required
- keep oracle provenance visible in benchmark results

Exit criteria:

- adding a new selective workload does not require bespoke oracle wiring in the runner
- selective workload correctness remains explainable and reproducible
- oracle provenance stays explicit in reports and summaries

### PR 3: Stability And Oracle Provenance Hardening

Goal: make repeated-run benchmark trust signals explicit before the benchmark is treated as an optimization judge.

Scope:

- add same-seed repeated-run stability checks for supported live workloads
- expose oracle provenance clearly in reports and operator guidance
- document how reviewers should interpret loaded-state and truth-pass benchmark verdicts

Exit criteria:

- repeated runs with the same seed produce stable correctness verdicts for the supported live subset
- benchmark reports show enough oracle provenance for selective workloads
- CI and review guidance explain how to interpret these signals

### PR 4: Baseline Capture and CI Policy

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

### PR 5: Benchmark Readiness Gate

Goal: define the explicit bar the benchmark must clear before optimization and autoresearch decisions depend on it.

Scope:

- define the readiness checklist for benchmark-driven optimization work
- identify protected workloads and known methodology limitations
- document the exit criteria for enabling benchmark-driven keep/discard automation

Exit criteria:

- the repository has a documented readiness gate with explicit pass criteria
- methodology limitations and protected workloads are visible to reviewers and automation
- later optimization and autoresearch issues can cite a stable benchmark gate

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

The repository should not treat the benchmark as a performance autoresearch gate until PRs 0 through 5 are complete.

At that point, the minimum bar is:

- the benchmark backlog is aligned with shipped behavior and remaining dependencies
- the benchmark covers the intended pre-optimization workload matrix
- selective workload correctness no longer depends on workload-name-specific oracle branching
- repeated-run stability and oracle provenance are visible and documented
- workload-level baselines and diffs are machine-readable
- the benchmark has CI-safe and local-safe execution subsets

PRs 6 and 7 then turn that benchmark into an optimization controller input. PRs 8 through 10 are the first optimization campaigns that should rely on it.
