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
