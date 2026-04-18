## Summary

Create the umbrella tracking issue for the TPC-E-inspired federated query benchmark for Forma.

## Why

The repository already has federated correctness and performance coverage, but it does not yet have a benchmark focused on hybrid cold + hot query behavior, multiple data distributions, and deep pagination scenarios such as page `100,000`.

## Scope

- define the benchmark scope and phase boundaries
- reference the HLD and implementation plan
- track the execution issues required for phase 1
- establish acceptance criteria for the first usable benchmark release

## Acceptance Criteria

- HLD and implementation plan documents are linked
- phase 1 deliverables are listed explicitly
- execution issues are linked from the umbrella issue
- the issue can be used as the single coordination entrypoint for the benchmark effort

## Execution Issues

- [ ] #34 Add benchmark scaffolding and TPC-E-inspired schema fixtures
- [ ] #35 Implement deterministic benchmark generator and distribution models
- [ ] #36 Add tier preparation and overlap/delete dataset support
- [ ] #37 Implement workload matrix and deep pagination benchmark cases
- [ ] #38 Add benchmark reporting and baseline capture support
- [ ] #39 Document CI and operator guidance for benchmark execution

## References

- `docs/cn/federated-query-benchmark-hld.md`
- `docs/federated-query/federated-query-benchmark-hld-en.md`
- `docs/federated-query/federated-query-benchmark-implementation-plan.md`
