# Federated Query Benchmark Issue Breakdown

Last updated: 2026-04-17  
Repository: `forma`

## Issue Set

This backlog decomposes the federated query benchmark into a small set of deliverable GitHub issues.

1. `#33` Umbrella issue for benchmark scope, deliverables, and acceptance criteria
2. `#34` Benchmark scaffolding and TPC-E-inspired schema fixtures
3. `#35` Deterministic generator and distribution models
4. `#36` Tier preparation and overlap/delete dataset support
5. `#37` Workload matrix including deep pagination and large page jumps
6. `#38` Reporting, metrics export, and baseline capture guidance
7. `#39` CI and operational documentation for repeatable benchmark execution

## Mapping

| Area | Primary Issue Goal |
|---|---|
| Project framing | `#33` Align scope, phases, and acceptance criteria |
| Scaffolding | `#34` Create config, runner, schema fixtures, and CLI shell |
| Data generation | `#35` Add reproducible scale and distribution-aware datasets |
| Tier loading | `#36` Prepare base, delta, and hot data with overlap semantics |
| Workloads | `#37` Add pagination, mixed filters, deep pages, and correctness checks |
| Reporting | `#38` Export machine-readable and human-readable benchmark results |
| Operations | `#39` Define how the benchmark runs in CI, nightly, and local workflows |

## Notes

- Issue titles should stay imperative and implementation-oriented.
- Phase 1 should prefer medium-scale reproducibility over large-scale sophistication.
- Deep pagination should be tracked as a first-class benchmark area, not as a side case.
