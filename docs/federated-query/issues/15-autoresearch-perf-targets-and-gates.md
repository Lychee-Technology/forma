## Summary

Add autoresearch performance targets and benchmark gate scripts for federated-query tuning.

## Why

Once the performance loop exists, it still needs target briefs and deterministic benchmark gate scripts before engineers can use it productively. Those targets should point at real hotspots and define which workloads are protected or expected to improve.

## Scope

- add initial performance target briefs for federated-query hotspots such as query planning, federated execution, and service-layer routing
- define fast, medium, and heavy benchmark gates for those targets
- document protected workloads, expected win areas, and known risk areas in each target brief
- explain how benchmark-driven perf targets differ from the existing BDD testing targets

## Implementation Notes

- initial shipped benchmark-backed target briefs cover `internal/postgres_duckdb_query.go` and `internal/e2e_harness/federated/query.go`
- the benchmark now also exercises `baseline-page-1` through `internal/entity_query_service.go`, making service-layer query work benchmark-backed for the primary pagination path
- baseline, candidate, and diff scripts clear the target-and-gate artifact directory before each run so repeated local gate use does not mix stale outputs with fresh evidence

## Acceptance Criteria

- target authors can run a performance autoresearch iteration against a known module and benchmark subset
- gate scripts are deterministic enough for repeated local use
- target briefs tie decisions to benchmark evidence instead of generic speed claims

## References

- `tools/autoresearch/testing/targets/postgres_duckdb_query.md`
- `tools/autoresearch/testing/targets/entity_query_service.md`
- `internal/e2e_harness/federated/query.go`
- `internal/postgres_duckdb_query.go`
