## Summary

Track the follow-up work required to turn the federated query benchmark into a reliable foundation for autoresearch-guided performance tuning.

## Why

The current benchmark foundation is useful for validation and early workload execution, but it is not yet strong enough to act as an optimization judge. The repository needs a single coordination issue for the work that makes benchmark evidence trustworthy and automatable.

## Scope

- define the follow-up benchmark phases after the closed phase-1 issue set
- link the PR-sliced execution plan for benchmark and autoresearch work
- track the execution issues needed before optimization waves begin
- establish the readiness bar for using benchmark artifacts in autoresearch keep or discard decisions

## Acceptance Criteria

- the roadmap links the benchmark readiness work and optimization waves
- execution issues are listed explicitly and grouped in delivery order
- the issue can be used as the single coordination entrypoint for benchmark-to-autoresearch work

## Execution Issues

- [ ] #52 Add executable benchmark runtime backed by the live federated harness
- [ ] #48 Harden benchmark result semantics and correctness assertions
- [ ] #50 Complete filter fidelity and schema-scoped benchmark workloads
- [ ] #47 Expand benchmark metrics, artifact schema, and baseline diff support
- [ ] #45 Capture benchmark baselines and codify CI execution policy
- [ ] #49 Scaffold a performance-oriented autoresearch loop around benchmark evidence
- [ ] #53 Add autoresearch performance targets and benchmark gate scripts
- [ ] #46 Optimize the federated deep-pagination path using benchmark evidence
- [ ] #51 Optimize federated filter pushdown and EAV behavior using benchmark evidence
- [ ] #54 Optimize federated routing and concurrency behavior using benchmark evidence

## References

- `docs/federated-query/federated-query-benchmark-autoresearch-pr-plan.md`
- `docs/federated-query/federated-query-benchmark-autoresearch-issues.md`
- `docs/federated-query/federated-query-benchmark-hld-en.md`
