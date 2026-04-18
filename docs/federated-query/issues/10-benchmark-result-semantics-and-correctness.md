## Summary

Harden benchmark result semantics and correctness assertions.

## Why

Optimization work is only trustworthy if the benchmark can reject incorrect behavior. The current result model and assertions are useful, but they are still too shallow for automated keep or discard decisions.

## Scope

- fix benchmark result semantics for total records, deep-page behavior, and workload failure reporting
- strengthen assertions for deduplication, stable ordering, delete shadowing, and last-write-wins behavior
- separate correctness failures from infrastructure or environment failures
- make the result model explicit enough for downstream automation and artifact diffing

## Acceptance Criteria

- workload results distinguish empty pages from broken execution behavior
- repeated benchmark runs with the same seed produce stable correctness verdicts
- correctness regressions fail the benchmark in a way that later automation can consume directly

## References

- `internal/e2e_harness/federated/benchmark/runner.go`
- `internal/e2e_harness/federated/query.go`
- `docs/federated-query/issues/05-workload-matrix-and-deep-pagination.md`
