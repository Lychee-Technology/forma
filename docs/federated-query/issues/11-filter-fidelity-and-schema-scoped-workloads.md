## Summary

Complete filter fidelity and schema-scoped workload execution for the federated benchmark.

## Why

The benchmark model already describes hot, EAV, mixed-tier, and schema-scoped workloads, but the executed path still leans heavily on the current `trade` happy path. That gap makes it hard to trust performance results for filter-heavy tuning work.

## Scope

- align executed workloads with declared hot, EAV, and mixed filter semantics
- make workload schema targeting explicit in execution instead of declarative-only metadata
- keep `trade` as the primary executed schema while preserving sanity coverage for `customer` and `security`
- add assertions that prove schema-scoped and filter-scoped behavior is working as intended

## Acceptance Criteria

- supported hot and EAV workloads execute with consistent semantics across base, delta, and hot tiers
- benchmark execution honors workload schema intent instead of relying on implicit harness state
- filter-heavy benchmark cases are representative enough to guide later pushdown optimization work

## References

- `internal/e2e_harness/federated/benchmark/workload.go`
- `internal/e2e_harness/federated/benchmark/runner.go`
- `internal/e2e_harness/federated/query.go`
- `docs/federated-query/federated-query-benchmark-hld-en.md`
