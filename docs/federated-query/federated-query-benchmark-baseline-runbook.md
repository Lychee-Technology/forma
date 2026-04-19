# Federated Query Benchmark Baseline Runbook

Last updated: 2026-04-18  
Repository: `forma`

## Purpose

This runbook defines a repeatable baseline capture flow for the federated query benchmark at `small` and `medium` scale.

## Recommended Baseline Scales

- `small`: local development and PR validation
- `medium`: controlled baseline comparison and regression review

## Recommended Commands

### Small Baseline

```bash
go run ./cmd/benchmark run \
  -mode smoke \
  -scale small \
  -distribution uniform \
  -iterations 5 \
  -baseline-dir .artifacts/benchmark/small-uniform
```

### Medium Baseline

```bash
go run ./cmd/benchmark run \
  -mode plan \
  -scale medium \
  -distribution zipf \
  -iterations 10 \
  -baseline-dir .artifacts/benchmark/medium-zipf
```

## Output Files

Each baseline directory should contain:

- `benchmark-result.json`
- `benchmark-result.md`
- `benchmark-summary.json`

## How To Compare Runs

Compare these fields first:

- `execution_count`
- `p50`
- `p95`
- `p99`
- `max`
- `avg`
- `qps`
- assertion pass/fail counts

## Oracle Mode

`oracle_mode` describes how the benchmark derived the expected result set for a workload before comparing it with the actual federated query output.

This is important because the benchmark is not only measuring latency. It is also making a correctness judgment. That judgment depends on what the benchmark believes the correct answer should be, so reviewers need to know where that expected answer came from.

### `loaded-state`

Use this interpretation when `oracle_mode` is `loaded-state`.

- the benchmark reconstructs the expected visible rows from the benchmark's loaded tier state
- base and delta rows come from the generated benchmark records after tier splitting
- hot rows come from the loaded Postgres state snapshot used by the live harness path
- delete shadowing, last-write-wins, page slicing, and time-window filtering are then applied to that reconstructed state

This is the default oracle mode and is appropriate when the benchmark can explain the visible result set directly from the loaded benchmark data model.

Typical examples:

- `baseline-page-1`
- schema-scoped workloads such as `customer-region-page` and `security-symbol-page`
- window workloads such as `mixed-tier-window`, `hot-only-window`, and `cold-only-window`
- deep page workloads

### `truth-pass`

Use this interpretation when `oracle_mode` is `truth-pass`.

- the benchmark still starts from the loaded tier state
- but for candidate rows that match the workload shape, it validates expected visibility by running a targeted federated query through the live harness path
- the resulting expected set is therefore backed by executable federated semantics rather than only by synthetic reconstruction

This mode is used when reproducing the final visible filter semantics directly from benchmark records is not yet trustworthy enough. In practice this is most useful for selective workloads that depend on hot-state visibility rules, EAV-backed filters, or combinations of both.

Typical examples:

- `hot-selective-page`
- `hot-low-selectivity-page`
- `eav-selective-page`
- `mixed-hot-eav-page`

### How To Interpret It

- if `oracle_mode` is `loaded-state`, a correctness failure usually means the query result diverged from the benchmark's reconstructed loaded state
- if `oracle_mode` is `truth-pass`, a correctness failure means the benchmark's final expected answer was confirmed through the live federated path itself
- `truth-pass` is usually more expensive than `loaded-state`, so do not assume all workloads should use it by default
- if a workload changes oracle mode between baselines, treat that as a benchmark methodology change, not a pure performance change

## Interpretation Guidance

- use `small` to catch obvious correctness or latency regressions quickly
- use `medium` to compare behavior across distributions and page-depth workloads
- treat assertion failures as correctness regressions even if latency improves
- treat large `max` growth separately from percentile movement; it is often a sign of tier skew or unstable deep pagination
- read `correctness_failures` and `infra_failures` separately in benchmark summaries; only the latter indicates an execution-environment problem
- for repeated executions with the same seed, expect `FailureKind`, `total_records`, and page `row_ids` to remain stable for supported workloads
- read workload `oracle_mode` when interpreting selective filter workloads; `truth-pass` means expected results were validated through the live federated path rather than only loaded-state reconstruction

## Current Limitations

- baseline presets currently favor `smoke` and `plan` modes for artifact stability over live execution cost
- use `go run ./cmd/benchmark run -mode live ...` when you need executable benchmark evidence instead of planning-only artifacts
- live benchmark correctness checks compare query results against the benchmark's loaded tier state rather than only the pre-split generated dataset
- selective hot/EAV workloads may use a truth-pass-backed oracle mode to align expected results with the executable federated filter semantics
- baseline capture is designed for artifact stability first, not for production-like throughput measurement
- CI integration and operator workflow guidance are documented in `docs/federated-query/federated-query-benchmark-ci-and-ops-guide.md`
