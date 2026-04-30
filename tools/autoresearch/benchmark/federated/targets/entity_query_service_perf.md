# Performance Target: `entity_query_service`

## Objective

Use the live federated benchmark to judge whether candidate changes improve the primary service-layer query path without introducing correctness regressions or obscuring the distinction between service-path wins and lower-level execution-path wins.

## Why This Target

`internal/entity_query_service.go` is the application-facing query entry point for pagination normalization, schema lookup, sort binding, projection shaping, and result pagination metadata.

The benchmark now exercises `baseline-page-1` through a real `EntityManager.Query` path backed by the benchmark harness data, which makes the service layer a first-class benchmark target for routing and pagination follow-up work.

## In Scope

- `internal/entity_query_service.go`
- `internal/entity_manager_query.go`
- request normalization, page sizing, sort binding, and result pagination metadata that materially affect the benchmarked service path
- service-path behavior that changes whether `baseline-page-1` remains a stable benchmark-backed entrypoint into federated query work

## Out Of Scope

- benchmark methodology changes
- workload definition changes
- direct DuckDB SQL template rewrites
- broad entity manager refactors unrelated to query-path cost or correctness
- cross-schema search work that the live benchmark does not currently execute

## Benchmark Subset

Gate definitions:

- `fast`
  - target workloads: `baseline-page-1`
  - protected workloads: `customer-region-page`, `security-symbol-page`
  - scale: `small`
  - iterations: `1`
- `medium`
  - target workloads: `baseline-page-1`
  - protected workloads: `customer-region-page`, `security-symbol-page`
  - scale: `small`
  - iterations: `2`
- `heavy`
  - target workloads: `baseline-page-1`
  - protected workloads: `customer-region-page`, `security-symbol-page`, `mixed-tier-window`
  - scale: `medium`
  - iterations: `2`

Default target workloads:

- `baseline-page-1`

Default protected workloads:

- `customer-region-page`
- `security-symbol-page`

## Benchmark Config

- mode: `live`
- distribution: `hotspot-overlap`
- tier profile: `balanced`
- seed: `42`

## Success Criteria

- `baseline-page-1` shows a clear latency improvement in `avg` or `p95`
- `correctness_failures = 0`
- `infra_failures = 0`
- protected workloads do not show a clear regression
- benchmark artifacts still make it obvious that the target workload ran through the service path

## Expected Win Areas

- reduce unnecessary service-layer work before the repository query runs
- avoid repeated schema or sort-resolution work that materially affects the benchmarked query path
- preserve or improve pagination metadata correctness while reducing request-path overhead

## Known Risk Areas

- changing normalization behavior in a way that alters user-visible pagination semantics
- improving the service path for trade pagination while regressing schema-scoped workloads
- attributing lower-level execution wins to the service layer without keeping the benchmark evidence interpretable

## Discard Criteria

- any correctness regression
- repeated infrastructure failure after rerun
- no meaningful improvement on `baseline-page-1`
- obvious regression on protected workloads

## Evidence To Review

- baseline `benchmark-summary.json`
- candidate `benchmark-summary.json`
- machine-readable diff report
- gate-specific artifact directories under `reports/{baseline,candidates,diff}/entity_query_service/<gate>/`
- workload-level deltas for:
  - `baseline-page-1`
  - `customer-region-page`
  - `security-symbol-page`

## Gate Commands

```bash
./tools/autoresearch/benchmark/federated/scripts/benchmark_baseline.sh entity_query_service fast
./tools/autoresearch/benchmark/federated/scripts/benchmark_candidate.sh entity_query_service fast
./tools/autoresearch/benchmark/federated/scripts/benchmark_gate.sh entity_query_service fast
```

Use `medium` or `heavy` in place of `fast` when you need the wider benchmark subset.

## Candidate Ideas

- trim redundant request normalization or metadata backfill work on the common query path
- reduce repeated schema-cache lookups or sort-binding work when it materially affects the service-path benchmark
- simplify service-level pagination handling only when the returned page metadata stays exact
