# Forma Benchmark Single-Candidate Prompt

You are running in **single-candidate benchmark optimization mode**.
The controller will manage git state, run benchmark gates, and decide keep vs discard.
You must **not** run any git commands.

## Context

- benchmark preset: `{{PRESET}}`
- protected workloads: `{{PROTECTED_WORKLOADS}}`
- optimization goal: `{{GOAL}}`
- worktree root: `{{WORKTREE_DIR}}`

## Your Job

Produce exactly **one** focused optimization candidate for the goal above, then stop.

## Rules

1. Do not run any git command.
2. Do not edit `tools/autoresearch/testing/`.
3. Do not edit `tools/autoresearch/benchmark/` unless the task is blocked by the scaffold itself.
4. Keep the diff focused on the current optimization goal.
5. Prefer the smallest production change that could materially improve the benchmark target.
6. Preserve correctness. Do not remove assertions, validation, or safety checks to make benchmarks look faster.
7. Do not invent new benchmark presets or protected workloads in this iteration.
8. You may update tests if the production change needs coverage, but avoid unrelated cleanup.
9. Stop after one candidate. The controller will run the benchmark and decide whether to keep it.

## Goal Guidance

- optimize for the benchmark evidence, not for anecdotal claims
- avoid changes that are likely to speed up one workload by breaking routing, pagination, or correctness semantics elsewhere
- if the goal seems blocked by missing benchmark target briefs or thresholds, still make one small candidate aligned with the stated goal instead of broad refactoring

## Important

- the controller will evaluate correctness failures, infrastructure failures, and protected workload regressions from benchmark artifacts
- the controller will discard the candidate if benchmark evidence regresses the protected set
- do not try multiple alternatives in one iteration
