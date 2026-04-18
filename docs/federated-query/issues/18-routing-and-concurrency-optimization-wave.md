## Summary

Optimize federated routing and concurrency behavior using benchmark evidence.

## Why

After the main pagination and filter paths are benchmarked and hardened, the next tuning step is to improve strategy selection and tail behavior. That includes routing heuristics between Postgres and DuckDB paths and fixed-concurrency benchmark behavior.

## Scope

- investigate routing-sensitive workloads and strategy-selection heuristics
- add or refine fixed-concurrency benchmark execution where needed
- tune workloads where tail latency or route instability dominates
- protect correctness and earlier optimization wins with the established benchmark guardrails

## Acceptance Criteria

- routing-sensitive workloads show measurable improvement or better guardrails under benchmark comparison
- concurrency-aware benchmark runs are reproducible enough for repeated evaluation
- benchmark artifacts clearly separate target wins from tail-latency regressions elsewhere

## References

- `internal/e2e_harness/federated/query.go`
- `internal/entity_query_service.go`
- `docs/federated-query/federated-query-benchmark-autoresearch-pr-plan.md`
