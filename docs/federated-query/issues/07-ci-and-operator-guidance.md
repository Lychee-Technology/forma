## Summary

Document repeatable benchmark execution for local runs, CI, and manual performance review.

## Why

Some workloads should be safe for routine regression use, while others should be reserved for manual or nightly execution. The repository needs a clear operating model.

## Scope

- document local benchmark commands
- classify workloads into smoke, regular regression, and heavy-run groups
- describe expected runtime and environment needs
- explain how to interpret benchmark results and common failure modes
- document deferred follow-up areas such as keyset pagination comparisons

## Acceptance Criteria

- repository docs describe how to run benchmark subsets safely
- CI-suitable and non-CI workloads are clearly separated
- operators have guidance for reading reports and spotting regressions
- known limitations and deferred work are documented

## References

- `docs/federated-query/federated-query-benchmark-hld-en.md`
- `docs/federated-query/federated-query-benchmark-implementation-plan.md`
