## Summary

Implement the benchmark workload matrix, including deep pagination and large page jumps.

## Why

Deep pagination is one of the highest-risk areas in hybrid cold + hot querying, especially when merged results must be ordered and sliced after combining sources.

## Scope

- add baseline pagination workloads
- add high- and low-selectivity filter workloads
- add EAV-only and hybrid hot + EAV workloads
- add hot-only, cold-only, and mixed-tier workloads
- add deep page jumps for pages `1`, `100`, `1,000`, and `100,000`
- add correctness assertions for ordering, dedup, and delete shadowing

## Acceptance Criteria

- workload definitions are declarative and runnable through the benchmark runner
- page `100,000` benchmark cases are included
- workload outputs capture both latency and correctness results
- repeated runs produce stable ordering and duplicate-free page slices

## References

- `docs/cn/federated-query-benchmark-hld.md`
- `docs/federated-query/federated-query-benchmark-hld-en.md`
