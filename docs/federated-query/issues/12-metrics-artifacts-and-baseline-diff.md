## Summary

Expand benchmark metrics, artifact schema, and baseline diff support.

## Why

The benchmark is only useful for iterative tuning if its outputs can be compared automatically at workload level. Today the repository has report generation, but it still lacks the richer metrics and structured comparisons needed for automated performance decisions.

## Scope

- expand benchmark metrics beyond latency percentiles and QPS
- capture workload-level metadata, environment metadata, and benchmark identifiers in artifacts
- add a machine-readable diff flow against a saved baseline
- keep JSON, Markdown, and console outputs stable enough for automation

## Acceptance Criteria

- artifacts can identify workload-level improvements and regressions across runs
- the benchmark records enough metadata to support later autoresearch decision rules
- baseline comparison output is machine-readable and stable across repeated runs with the same seed and workload set

## References

- `internal/e2e_harness/federated/benchmark/report.go`
- `docs/federated-query/issues/06-reporting-and-baseline-capture.md`
- `docs/federated-query/federated-query-benchmark-baseline-runbook.md`
