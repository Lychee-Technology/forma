# Forma Federated Benchmark Autoresearch

## Goal

Improve federated query performance for one benchmark target at a time.
Optimize for benchmark-backed latency wins, not broad code churn.
The active target brief and gate define the workload subset that matters for the current iteration.

## Scope

You may edit:
- the active target production file
- narrowly supporting production code directly required by the target optimization

You must not edit:
- benchmark methodology
- workload definitions
- CI workflows
- dependency files
- documentation outside `tools/autoresearch/benchmark/federated`
- unrelated production areas

## Working Style

- Work on exactly one target brief at a time.
- Prefer the smallest correct performance change.
- Keep each patch reviewable and easy to attribute.
- Prefer removing repeated work over adding new abstraction.
- Prefer SQL-shape and plan improvements before Go-side micro-optimizations.
- For deep-pagination targets, prefer changes that reduce rows flowing into window functions, total-count work, and final sorts.
- Preserve correctness first.
- Treat protected workloads as regressions guards, not second-class metrics.

## Keep Criteria

A candidate is worth recommending only when all are true:
- benchmark candidate completes with `correctness_failures = 0`
- benchmark candidate completes with `infra_failures = 0`
- at least one target workload shows a clear latency win in `avg` or `p95`
- protected workloads do not show a clear regression in `avg` or `p95`
- the patch stays tightly scoped to the target performance problem

## Default Thresholds

Use these controller defaults unless the target brief says otherwise:
- clear target improvement: `avg` or `p95` improves by at least 3%
- clear protected regression: `avg` or `p95` regresses by at least 5%

These thresholds are a screening rule for the supervised pilot, not a permanent policy.

## Discard Criteria

- any correctness regression
- any infrastructure failure after the benchmark run completes
- no meaningful target-workload improvement
- clear regression on protected workloads
- broad or hard-to-explain code churn

## Evidence

Primary evidence comes from:
- baseline `benchmark-summary.json`
- candidate `benchmark-summary.json`
- machine-readable diff report
- workload-level deltas for target and protected workloads

Read the active brief for gate-specific workload sets before deciding whether a candidate is worth keeping.

When the target brief is about deep pagination, reason explicitly about whether the patch reduces:
- rows entering the final pagination stage
- work done by `ROW_NUMBER()`, `COUNT(*) OVER()`, or final global sort steps
- row width carried through those stages

## Experiment Loop

1. Read this file.
2. Read the active target brief.
3. Read the active target source file and nearby helper code.
4. Produce one small performance candidate.
5. Let the controller run benchmark candidate and compare steps.
6. Review the benchmark-backed recommendation.
7. Archive the patch and evidence.

## Never Do

- Do not modify git state.
- Do not broaden into benchmark methodology work.
- Do not keep a patch only because it is novel.
- Do not trade correctness or protected-workload behavior for a narrow win.
- Do not introduce speculative refactors without benchmark motivation.
- Do not spend an iteration on call-site or buffer micro-optimizations when the target brief asks for SQL/template improvements.
- Do not spend an iteration on SQL rewrites that are unlikely to reduce deep-page window/count/sort work.
