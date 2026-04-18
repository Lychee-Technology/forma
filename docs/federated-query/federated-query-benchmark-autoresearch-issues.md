# Federated Query Benchmark Autoresearch Issue Breakdown

Last updated: 2026-04-18  
Repository: `forma`

## Issue Set

This backlog extends the closed phase-1 benchmark work and reorganizes the next phase around one goal: make the benchmark trustworthy enough for autoresearch-guided performance tuning.

Planned issues:

1. `#55` umbrella issue for the benchmark-to-autoresearch follow-up plan
2. `#52` executable benchmark runtime backed by the live federated harness
3. `#48` result semantics and correctness hardening
4. `#50` filter fidelity and schema-scoped workload execution
5. `#47` metrics expansion, artifact schema, and baseline diff support
6. `#45` baseline capture and CI execution policy
7. `#49` autoresearch perf loop scaffolding
8. `#53` autoresearch perf targets and gate scripts
9. `#46` deep-pagination optimization wave
10. `#51` filter pushdown and EAV optimization wave
11. `#54` routing and concurrency optimization wave

## PR Mapping

| PR | Primary Goal |
|---|---|
| PR 1 | `#52` Promote the benchmark to a supported live-execution runtime |
| PR 2 | `#48` Make benchmark outputs trustworthy for optimization decisions |
| PR 3 | `#50` Align executed workloads with the benchmark data model |
| PR 4 | `#47` Produce machine-comparable metrics and baseline diffs |
| PR 5 | `#45` Codify baseline capture and CI-safe benchmark subsets |
| PR 6 | `#49` Add a benchmark-driven autoresearch performance loop |
| PR 7 | `#53` Add performance target briefs and benchmark gate scripts |
| PR 8 | `#46` Improve deep-pagination performance using the benchmark |
| PR 9 | `#51` Improve filter pushdown and EAV behavior using the benchmark |
| PR 10 | `#54` Improve routing heuristics and concurrency behavior |

## Notes

- PRs 1 through 5 are readiness work and should land before benchmark-driven autoresearch optimization begins.
- PRs 6 and 7 integrate the benchmark with autoresearch without changing the current BDD testing loop.
- PRs 8 through 10 should be evaluated with the benchmark guardrails created earlier in the sequence.
