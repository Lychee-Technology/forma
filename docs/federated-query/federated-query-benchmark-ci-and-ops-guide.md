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

### Heavy Plan Run

- preset: `heavy-plan`
- mode: plan (validation only — it does not generate data or execute queries)
- scale: `large`
- expected runtime: minutes
- command: `make benchmark-heavy`

### Heavy Live Run

- preset: `heavy-live`
- mode: live, full workload matrix
- scale: `large` (10M trades / 1M customers / 100k securities)
- distribution: `hotspot-overlap`; tier profile: `balanced-60-30-10`
- truth-pass oracles are spot-check sampled (`truth_pass_sample_cap=10000`),
  per workload (a workload at or under the cap stays plain `truth-pass`);
  a sampled pass asserts reconstruction ≡ engine truth on the sample only —
  see the baseline runbook before comparing against uncapped baselines
- resource bounds: DuckDB memory 8192MB (preset), `-duckdb-memory-mb` to
  override; `-run-timeout` aborts a stuck run
- expected runtime and peak RSS/disk: pending first calibrated capture (see
  the calibration ladder in the baseline runbook); plan for multiple hours
  on an idle machine
- policy: manual, on-demand only. Never in CI, no scheduled job. Run on an
  idle machine — loaded machines inflate truth-pass oracle cost by 5-10x
- command: `make benchmark-heavy-live`

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

### Concurrency Evidence Sweep (#104)

```bash
make benchmark-concurrency
```

Runs the full small-live preset at `Concurrency=1,2,4,8` and aggregates the
four runs into `.artifacts/benchmark/concurrency/concurrency-report.md`
(overall and per-workload p50/p95/p99/QPS per level, plus the
`concurrent-run-*` stability assertion pass rates from PR #94). Each level is
a complete live run, operator-initiated only, never CI. Since #156 batched the
uncapped truth-pass oracle into one paginated visibility sweep per workload
(replacing ~24k per-candidate federated queries), per-level runtime is
materially lower than the earlier truth-pass-dominated ~35-minute figure — the
dominant cost is now tier loading plus measured-workload execution; re-measure
on your host. Concurrent baselines write to `-c{N}`
suffixed directories and form their own trend comparability group, so they
never pollute the sequential (`C=1`) baseline window.

Individual pieces compose too:

```bash
go run ./cmd/benchmark baseline -preset small-live -concurrency 4 \
  -output-dir .artifacts/benchmark/concurrency
go run ./cmd/benchmark concurrency-report \
  -input-dir .artifacts/benchmark/concurrency -md-out report.md
```

DuckDB resources are connection-level configuration (the query template no
longer hardcodes PRAGMAs); override per run with `-duckdb-threads` /
`-duckdb-memory-mb` on either `baseline` or `run`.

## CI Guidance

Use these rules in CI and automation for the current benchmark model.

- run `make benchmark-smoke` on pull requests
- keep PR-time benchmark automation on the `ci-smoke` preset only
- do not run `deep-page-100000` in PR-time CI
- use scheduled jobs or manual review environments for `make benchmark-regression`
- reserve `benchmark-heavy` and `benchmark-heavy-live` for manual capacity-aware runs
- treat harness-backed execution tests as smoke coverage, not as stable performance thresholds

The repository CI workflow now includes a dedicated `benchmark-smoke` job for the scaffolded benchmark command and the existing federated e2e smoke tests remain the executable coverage path.

## Benchmark Readiness Gate

Treat the benchmark as ready to inform optimization review only when all of the following are true:

- the live benchmark path is available through `go run ./cmd/benchmark run -mode live ...` and the documented baseline presets
- correctness failures and infrastructure failures are reported separately in benchmark summaries
- same-seed repeated-run stability is visible through the top-level `stability` summary
- oracle methodology is visible through workload `oracle_mode` and top-level `oracle_provenance`
- workload-level summaries and diff artifacts are available for baseline comparison
- CI-safe and manual-only workload subsets remain separated by policy

If any of those conditions stops being true, treat the benchmark as evidence for debugging only rather than as an optimization gate.

### Protected Workloads

Use these workloads as the default protected set when evaluating benchmark-driven query changes:

- `baseline-page-1`
- `hot-selective-page`
- `hot-low-selectivity-page`
- `eav-selective-page`
- `mixed-hot-eav-page`
- `mixed-tier-window`
- `hot-only-window`
- `cold-only-window`
- `deep-page-1000`

Operationally:

- treat correctness regressions in any protected workload as a hard stop
- treat repeated-run instability in any protected workload as a methodology problem that must be resolved before trusting latency deltas
- reserve `deep-page-100000` for heavier manual review rather than routine protection in CI-safe flows

### Known Methodology Limits

- `truth-pass` workloads use the live federated path to validate expected results, so oracle mode changes are methodology changes rather than pure performance changes
- `prefer_hot` is still primarily provenance metadata outside the documented tier-mix execution override case
- live benchmark runs are intended for repeatable correctness and relative performance review, not for production-grade throughput SLOs

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

## Longitudinal Trend Analysis

The `benchmark trend` subcommand scans stored `benchmark-summary.json` artifacts and surfaces regressions, baseline drift, and methodology changes across runs.

### Quick Start

```bash
go run ./cmd/benchmark trend -history-dir .artifacts/benchmark/history
```

### Recommended History Layout

```
.artifacts/benchmark/history/<channel>/<preset>/<distribution>/<timestamp>-<shortsha>/
  benchmark-result.json
  benchmark-result.md
  benchmark-summary.json
```

Runs should carry provenance metadata (`-channel`, `-git-sha`, `-git-ref`) so the trend engine can group and order them correctly.

### PR CI Behavior

- Correctness regressions in any protected workload return a non-zero exit code.
- Latency regressions (`p95`) are surfaced in the trend report but do not block PR CI.
- Use `-protected-workloads` to target the standard set or a custom subset.

### Nightly / Manual Review

- Accumulate history via scheduled `make benchmark-regression` runs with provenance flags.
- Run `benchmark trend` against the accumulated directory before release review.
- Check `baseline_drift` signals to distinguish new feature impact from pre-existing platform drift.

### Protected Workload Defaults

```
baseline-page-1
hot-selective-page
hot-low-selectivity-page
eav-selective-page
mixed-hot-eav-page
mixed-tier-window
hot-only-window
cold-only-window
deep-page-1000
```

## Deferred Work

These items remain intentionally out of scope for the current operating model:

- official CI performance gating on large-scale thresholds
- keyset pagination comparison workloads
- distributed benchmark agents
- full production-grade hosted dashboards
