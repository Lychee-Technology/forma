## Summary

Optimize federated filter pushdown and EAV behavior using benchmark evidence.

## Why

After deep pagination is measurable and guarded, the next major opportunity is selective filtering across hot fields, EAV fields, and mixed predicates. This is where benchmark fidelity work should start paying off directly.

## Scope

- investigate hot-filter, EAV-filter, and mixed-filter workload costs
- optimize pushdown and tier-merge behavior where possible
- validate improvements with workload-level benchmark diffs and protected workload guardrails
- record trade-offs between target wins and non-target workload movement

## Acceptance Criteria

- targeted filter workloads improve under benchmark comparison
- deep-page and baseline workloads stay within agreed guardrails
- benchmark artifacts clearly show where the win came from and what risks remain

## References

- `internal/e2e_harness/federated/query.go`
- `internal/postgres_duckdb_query.go`
- `docs/federated-query/federated-query-benchmark-hld-en.md`
