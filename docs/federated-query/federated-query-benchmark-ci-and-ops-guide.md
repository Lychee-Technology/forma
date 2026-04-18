# Federated Query Benchmark CI and Operator Guide

Last updated: 2026-04-18  
Repository: `forma`

## Purpose

This guide defines which benchmark subsets are safe for local validation, PR-time CI, and heavier manual review. It also documents how to read the generated artifacts and what is still deferred in phase 1.

## Current Execution Model

- `go run ./cmd/benchmark run ...` is currently a scaffolded validation path
- `-mode smoke` validates config, fixtures, and workload resolution only
- `-mode plan` validates config and emits the planned workload shape only
- live federated-query execution currently happens through the harness-backed test path such as `go test ./internal/e2e_harness/federated/... -run TestBenchmarkWorkloadExecution_RunWithHarness`

This means phase-1 benchmark CI should be treated as configuration, fixture, and artifact regression coverage first. It is not yet an official throughput gate.

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

- workloads: `baseline-page-1`, `hot-selective-page`, `eav-selective-page`, `mixed-tier-window`
- scale: `small` or `medium`
- mode: `plan` for CLI-only checks, harness-backed test for executable query coverage
- goal: verify workload set shape, artifacts, and supported federated execution paths without using the heaviest deep-page cases
- expected runtime: 1 to 5 minutes depending on runner size and whether harness-backed tests are included

Commands:

```bash
make benchmark-regression
go test -v ./internal/e2e_harness/federated/... -run TestBenchmarkWorkloadExecution_RunWithHarness -timeout=10m
```

### Heavy Run

Use for manual performance review or nightly jobs only.

- workloads: full workload set including `deep-page-1000` and `deep-page-100000`
- scale: `medium` or `large`
- mode: `plan` today, harness-backed execution only when the environment can tolerate larger seeded datasets
- goal: compare deep-pagination planning shape and prepare for later real benchmark gating
- expected runtime: several minutes for planning, significantly longer once large executable runs are wired into the CLI

Command:

```bash
make benchmark-heavy
```

## Recommended Commands

### Local Smoke

```bash
go run ./cmd/benchmark run \
  -mode smoke \
  -scale small \
  -distribution uniform \
  -workloads baseline-page-1,hot-selective-page \
  -baseline-dir .artifacts/benchmark/smoke
```

### Local Regression Planning

```bash
go run ./cmd/benchmark run \
  -mode plan \
  -scale medium \
  -distribution zipf \
  -iterations 5 \
  -workloads baseline-page-1,hot-selective-page,eav-selective-page,mixed-tier-window \
  -baseline-dir .artifacts/benchmark/regression-medium
```

### Harness-Backed Execution Check

```bash
go test -v ./internal/e2e_harness/federated/... \
  -run TestBenchmarkWorkloadExecution_RunWithHarness \
  -timeout=10m
```

## CI Guidance

Use these rules in CI until the benchmark CLI is wired to live execution.

- run `make benchmark-smoke` on pull requests
- keep CI benchmark runs on `small` scale only
- do not run `deep-page-100000` in PR-time CI
- use scheduled jobs for `benchmark-regression`
- reserve `benchmark-heavy` for manual or nightly jobs
- treat harness-backed execution tests as smoke coverage, not as stable performance thresholds

The repository CI workflow now includes a dedicated `benchmark-smoke` job for the scaffolded benchmark command and the existing federated e2e smoke tests remain the executable coverage path.

## Environment Requirements

- Go toolchain matching the repository CI version
- writable workspace for `.artifacts/benchmark` output
- for CLI smoke/plan runs: no external services beyond normal test prerequisites
- for harness-backed execution: the same environment required by federated e2e tests, including Docker-backed services when applicable

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

Interpretation guidance:

- assertion failures are correctness regressions, even if latency improves
- rising `p95` and `p99` with a flat `p50` usually means tail instability rather than broad slowdown
- isolated `max` spikes often point to skew, tier imbalance, or unstable deep-page behavior
- a drop in `qps` without matching row-count changes is a regression candidate worth manual review
- planning-only output should stay structurally stable across runs for the same seed and workload selection

## Common Failure Modes

- unknown workload name: the workload list passed to `-workloads` does not match `DefaultWorkloads`
- invalid mode, scale, or distribution: config validation failed before execution
- missing baseline artifacts: the output directory was not writable or `-baseline-dir` was omitted
- harness-backed benchmark test failure: fixture registration, seeded dataset loading, or federated query execution regressed

## Deferred Work

These items remain intentionally out of scope for the current phase-1 operating model:

- official CI performance gating on large-scale thresholds
- benchmark trend dashboards and longitudinal regression tracking
- CLI wiring for live harness-backed execution instead of validation-only `run`
- keyset pagination comparison workloads
- distributed benchmark agents

Until those arrive, use this guide as a safe execution policy rather than as a production-grade benchmark SLO gate.
