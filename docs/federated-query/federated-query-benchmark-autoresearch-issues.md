# Federated Query Benchmark Autoresearch Issue Breakdown

Last updated: 2026-04-20  
Repository: `forma`

## Issue Set

This backlog extends the closed phase-1 benchmark work and reorganizes the remaining work around one goal: make the benchmark trustworthy enough for autoresearch-guided performance tuning.

Planned issues:

1. `#55` umbrella issue for the benchmark-to-autoresearch follow-up plan
2. `#49` autoresearch perf loop scaffolding
3. `#53` autoresearch perf targets and gate scripts
4. `#46` deep-pagination optimization wave
5. `#51` filter pushdown and EAV optimization wave
6. `#54` routing and concurrency optimization wave

Already closed foundation issues:

- `#70` backlog alignment for benchmark follow-up work
- `#64` remaining workload-matrix expansion before optimization waves
- `#63` selective workload oracle generalization
- `#65` stability checks and oracle provenance hardening
- `#45` baseline capture and CI execution policy
- `#69` benchmark readiness gate
- `#52` executable benchmark runtime backed by the live federated harness
- `#48` result semantics and correctness hardening
- `#50` filter fidelity and schema-scoped workload execution
- `#47` metrics expansion, artifact schema, and baseline diff support

## PR Mapping

| PR | Primary Goal |
|---|---|
| PR 0 | `#70` Align the roadmap with shipped benchmark foundations and remaining gaps |
| PR 1 | `#64` Complete the pre-optimization workload matrix |
| PR 2 | `#63` Generalize selective workload oracle handling |
| PR 3 | `#65` Harden repeated-run stability checks and oracle provenance |
| PR 4 | `#45` Codify baseline capture and CI-safe benchmark subsets |
| PR 5 | `#69` Define the readiness gate before benchmark-driven optimization decisions |
| PR 6 | `#49` Add a benchmark-driven autoresearch performance loop |
| PR 7 | `#53` Add performance target briefs and benchmark gate scripts |
| PR 8 | `#46` Improve deep-pagination performance using the benchmark |
| PR 9 | `#51` Improve filter pushdown and EAV behavior using the benchmark |
| PR 10 | `#54` Improve routing heuristics and concurrency behavior |

## Notes

- PRs 0 through 5 are readiness work and should land before benchmark-driven autoresearch optimization begins.
- PRs 6 and 7 integrate the benchmark with autoresearch without changing the current BDD testing loop.
- PRs 8 through 10 should be evaluated with the benchmark guardrails created earlier in the sequence.
