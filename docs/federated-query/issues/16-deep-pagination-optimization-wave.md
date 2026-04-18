## Summary

Optimize the federated deep-pagination path using benchmark evidence.

## Why

Deep pagination remains one of the highest-risk federated-query paths because it amplifies merge, sort, and count costs as offsets grow. It is the best first optimization wave once the benchmark becomes a trustworthy judge.

## Scope

- investigate deep-page latency in the merge-before-pagination path
- optimize planning or execution where large offsets dominate latency
- validate improvements with benchmark fast and medium gates
- record wins, regressions, and remaining bottlenecks through benchmark artifacts

## Acceptance Criteria

- target deep-page workloads improve under benchmark comparison without correctness regressions
- benchmark artifacts make the improvement visible at workload level, not just in anecdotal logs
- remaining bottlenecks are documented for later optimization waves

## References

- `docs/federated-query/issues/05-workload-matrix-and-deep-pagination.md`
- `internal/e2e_harness/federated/query.go`
- `internal/postgres_duckdb_query.go`
