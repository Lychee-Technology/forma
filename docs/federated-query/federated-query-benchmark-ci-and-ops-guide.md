# Federated Query Benchmark CI and Operator Guide

Last updated: 2026-04-20  
Repository: `forma`

## Purpose

This guide defines which benchmark subsets are safe for local validation, PR-time CI, and heavier manual review. It also documents how to read the generated artifacts and what remains deferred from full performance-gating workflows.

## Current Execution Model

- `go run ./cmd/benchmark run ...` supports both validation and live execution paths
- `-mode smoke` validates config, fixtures, and workload resolution only
- `-mode plan` validates config and emits the planned workload shape only
- `-mode live` creates the federated harness, prepares tiered benchmark data, and executes supported workloads end to end
- live benchmark summaries distinguish correctness failures from infrastructure failures, and workload-level expected-result checks are part of the executable path
- benchmark summaries now expose workload oracle provenance so selective workloads can be distinguished between `loaded-state` and `truth-pass` expected-result modes
- benchmark summaries now expose repeated-run stability status so reviewers can quickly tell whether same-seed live runs stayed stable
- harness-backed tests such as `go test ./internal/e2e_harness/federated/... -run TestBenchmarkWorkloadExecution_RunWithHarness` remain the focused executable coverage path for repository tests

This means CI can continue to treat smoke mode as the cheap artifact regression layer, while local or manual runs can use live mode for executable benchmark evidence. It is still not an official throughput gate.

## Oracle Modes

The benchmark reports a workload `oracle_mode` because correctness verdicts depend on how expected results were derived.

### Why this exists

The benchmark compares actual federated query output against an expected result set. For some workloads, the expected result can be reconstructed directly from the benchmark's loaded tier state. For other workloads, especially selective hot or EAV-heavy cases, the safest expected-result path is to confirm visibility through targeted federated truth passes.

Without this distinction, two correctness failures could look identical even though they were derived from different benchmark methods.

### `loaded-state`

- expected rows are reconstructed from the benchmark's loaded tier state
- this is the normal mode for workloads whose visibility semantics are already well modeled by the benchmark runner
- it is cheaper and should remain the default where it is trustworthy

### `truth-pass`

- expected rows are confirmed through the live federated path for targeted candidate rows
- this is used for workloads where final visible filter semantics are better validated through execution than through synthetic reconstruction alone
- it is intentionally narrower and more expensive than `loaded-state`

### Operational guidance

- do not treat a switch between `loaded-state` and `truth-pass` as a pure performance change; it also changes how correctness is judged
- when a selective workload fails under `truth-pass`, assume the benchmark has already validated the expected answer through the executable path
- when comparing benchmark summaries, use `oracle_mode` alongside `correctness_failures` so reviewers understand whether failures came from loaded-state reconstruction or truth-pass-backed verification

## PreferHot

The benchmark also carries `prefer_hot` as workload intent metadata.

At this stage, `prefer_hot` means the workload is intended to emphasize hot-tier freshness or hot-biased access patterns. For most workloads it is still intent metadata only. The current exception is tier-mix workloads where the benchmark may apply a hot-preferred Postgres-only override to make that execution choice explicit.

Operationally, that means:

- use `prefer_hot` to group or explain workloads in reviews and benchmark summaries
- do not interpret `prefer_hot=true` as proof that cold tiers were excluded from execution unless the run also records a hot-preferred execution override in plan notes
- if a later change turns `prefer_hot` into a real routing or planning flag, treat that as a benchmark methodology change rather than a simple benchmark expansion

## Workload Groups

### Smoke

Use for PR validation and fast local checks.

- workloads: `baseline-page-1`, `hot-selective-page`
- scale: `small`
- mode: `smoke`
- goal: verify fixture loading, workload registration, CLI/report generation, and benchmark config stability
- expected runtime: usually under 1 minute on CI runners

Command:

```bash
make benchmark-smoke
```

### Regular Regression

Use for pre-merge review, local baseline capture, and scheduled CI.

- preset: `small-live`
- workloads: baseline pagination, schema-scoped lookups, selective filters, and tier-window coverage
- scale: `small`
- mode: `live`
- goal: verify workload set shape, artifacts, and supported federated execution paths without using the heaviest deep-page cases
- expected runtime: 1 to 5 minutes depending on runner size and whether harness-backed tests are included

Commands:

```bash
make benchmark-regression
go test -v ./internal/e2e_harness/federated/... -run TestBenchmarkWorkloadExecution_RunWithHarness -timeout=10m
```

### Heavy Run

Use for manual performance review or nightly jobs only.

- preset: `heavy-plan`
- workloads: full workload set including `deep-page-1000` and `deep-page-100000`
- scale: `large`
- mode: `plan`
- goal: compare deep-pagination planning shape and prepare for later real benchmark gating
- expected runtime: several minutes for planning, significantly longer once large executable runs are wired into the CLI

Command:

```bash
make benchmark-heavy
```

## Recommended Commands

### Local Smoke

```bash
go run ./cmd/benchmark baseline \
  -preset ci-smoke \
  -output-dir .artifacts/benchmark
```

### Local Regression Live Subset

```bash
go run ./cmd/benchmark baseline \
  -preset small-live \
  -output-dir .artifacts/benchmark
```

### Harness-Backed Execution Check

```bash
go test -v ./internal/e2e_harness/federated/... \
  -run TestBenchmarkWorkloadExecution_RunWithHarness \
  -timeout=10m
```

### Medium Live Baseline

```bash
go run ./cmd/benchmark baseline \
  -preset medium-live \
  -output-dir .artifacts/benchmark
```

## CI Guidance

Use these rules in CI and automation for the current benchmark model.

- run `make benchmark-smoke` on pull requests
- keep PR-time benchmark automation on the `ci-smoke` preset only
- do not run `deep-page-100000` in PR-time CI
- use scheduled jobs or manual review environments for `make benchmark-regression`
- reserve `benchmark-heavy` for manual or nightly jobs
- treat harness-backed execution tests as smoke coverage, not as stable performance thresholds

The repository CI workflow now includes a dedicated `benchmark-smoke` job for the scaffolded benchmark command and the existing federated e2e smoke tests remain the executable coverage path.

## Environment Requirements

- Go toolchain matching the repository CI version
- writable workspace for `.artifacts/benchmark` output
- for CLI smoke/plan runs: no external services beyond normal test prerequisites
- for CLI `-mode live` and harness-backed execution: the same environment required by federated e2e tests, including Docker-backed services when applicable

Operational expectations:

- `small` scale is safe for laptops and CI runners
- `medium` scale is suitable for controlled review runs
- `large` scale should be reserved for manual capacity-aware environments

## How To Read Results

Check these fields first in `benchmark-summary.json` or the Markdown summary:

- `execution_count`
- `p50`, `p95`, `p99`
- `max`
- `avg`
- `qps`
- per-assertion pass/fail counts
- workload `oracle_mode`
- top-level `stability`
- top-level `oracle_provenance`

Interpretation guidance:

- assertion failures are correctness regressions, even if latency improves
- rising `p95` and `p99` with a flat `p50` usually means tail instability rather than broad slowdown
- isolated `max` spikes often point to skew, tier imbalance, or unstable deep-page behavior
- a drop in `qps` without matching row-count changes is a regression candidate worth manual review
- planning-only output should stay structurally stable across runs for the same seed and workload selection
- supported live workloads should also keep repeated-run `FailureKind`, `total_records`, and page `row_ids` stable for the same seed and workload selection
- if `stability.enabled=true`, review `unstable_workloads`, `failure_kind_failures`, `total_record_failures`, and `page_row_id_failures` before trusting latency changes
- use `oracle_provenance` to see which workloads were judged via `loaded-state` versus `truth-pass` without reading every workload row individually

## Common Failure Modes

- unknown workload name: the workload list passed to `-workloads` does not match `DefaultWorkloads`
- invalid mode, scale, or distribution: config validation failed before execution
- missing baseline artifacts: the output directory was not writable or `-baseline-dir` was omitted
- harness-backed benchmark test failure: fixture registration, seeded dataset loading, or federated query execution regressed

## Deferred Work

These items remain intentionally out of scope for the current operating model:

- official CI performance gating on large-scale thresholds
- benchmark trend dashboards and longitudinal regression tracking
- keyset pagination comparison workloads
- distributed benchmark agents

Until those arrive, use this guide as a safe execution policy rather than as a production-grade benchmark SLO gate.
