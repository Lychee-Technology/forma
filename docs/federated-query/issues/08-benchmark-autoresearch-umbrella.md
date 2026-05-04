## Summary

Track the follow-up work required to turn the federated query benchmark into a reliable foundation for autoresearch-guided performance tuning.

## Why

The current benchmark foundation is useful for validation and early workload execution, but it is not yet strong enough to act as an optimization judge. The repository needs a single coordination issue for the work that makes benchmark evidence trustworthy and automatable.

## Scope

- track the remaining benchmark readiness work after the shipped runtime and phase-1 hardening work
- link the PR-sliced execution plan for benchmark and autoresearch work
- distinguish closed benchmark foundations from still-open follow-up gaps
- establish the readiness bar for using benchmark artifacts in autoresearch keep or discard decisions

## Acceptance Criteria

- the roadmap links the benchmark readiness work and optimization waves
- execution issues are listed explicitly and grouped in delivery order
- the issue can be used as the single coordination entrypoint for benchmark-to-autoresearch work

## Shipped Foundations

- [x] #52 Add executable benchmark runtime backed by the live federated harness
- [x] #48 Harden benchmark result semantics and correctness assertions
- [x] #50 Complete filter fidelity and schema-scoped benchmark workloads
- [x] #47 Expand benchmark metrics, artifact schema, and baseline diff support

## Remaining Readiness Work

- [x] #70 Align benchmark follow-up backlog with shipped runtime and remaining gaps
- [x] #64 Expand the benchmark workload matrix with low-selectivity, mixed-filter, and tier-targeted window cases
- [x] #63 Generalize selective-workload benchmark oracles beyond workload-specific truth passes
- [x] #65 Harden benchmark stability checks and expose oracle provenance in reports
- [x] #45 Capture benchmark baselines and codify CI execution policy
- [x] #69 Define a benchmark readiness gate before optimization and autoresearch decisions

## Benchmark-Driven Automation

- [x] #49 Scaffold a performance-oriented autoresearch loop around benchmark evidence
- [x] #53 Add autoresearch performance targets and benchmark gate scripts

## Optimization Waves

- [x] #46 Optimize the federated deep-pagination path using benchmark evidence
- [x] #51 Optimize federated filter pushdown and EAV behavior using benchmark evidence
- [x] #54 Optimize federated routing and concurrency behavior using benchmark evidence

## Post-Optimization Bottleneck Harvest

- [x] #101 Harvest remaining bottlenecks from the 3 completed optimization waves
- See `docs/federated-query/federated-query-post-optimization-bottleneck-catalog.md` for the full bottleneck catalog

## References

- `docs/federated-query/federated-query-benchmark-autoresearch-pr-plan.md`
- `docs/federated-query/federated-query-benchmark-autoresearch-issues.md`
- `docs/federated-query/federated-query-benchmark-hld-en.md`
