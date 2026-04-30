# Performance Target: `federated_query_execution`

## Objective

Use the federated benchmark to judge whether candidate changes improve mixed federated execution behavior in the live harness path without introducing correctness regressions or hiding routing behavior behind methodology changes.

## Why This Target

`internal/e2e_harness/federated/query.go` is the executable benchmark path for tier discovery, dirty-id collection, federated SQL assembly, fallback behavior, and hot-path routing overrides.

It is the most direct benchmark-backed target for federated execution and routing provenance in the current repository.

## In Scope

- `internal/e2e_harness/federated/query.go`
- tier composition and fallback behavior that materially changes benchmark execution cost
- hot/cold/mixed execution assembly in the benchmark live path
- routing provenance or plan-note changes that make benchmark evidence easier to interpret
- query-shape changes in the harness path when they directly affect mixed execution workloads

## Out Of Scope

- benchmark methodology changes
- workload definition changes
- service-layer pagination or schema validation behavior
- controller logic or git automation changes
- broad benchmark fixture refactors

## Benchmark Subset

Gate definitions:

- `fast`
  - target workloads: `mixed-tier-window`
  - protected workloads: `baseline-page-1`
  - scale: `small`
  - iterations: `1`
- `medium`
  - target workloads: `mixed-tier-window`, `hot-only-window`, `cold-only-window`
  - protected workloads: `baseline-page-1`
  - scale: `small`
  - iterations: `2`
- `heavy`
  - target workloads: `mixed-tier-window`, `hot-only-window`, `cold-only-window`
  - protected workloads: `baseline-page-1`, `customer-region-page`, `security-symbol-page`
  - scale: `medium`
  - iterations: `2`

Default target workloads:

- `mixed-tier-window`
- `hot-only-window`
- `cold-only-window`

Default protected workloads:

- `baseline-page-1`
- `customer-region-page`
- `security-symbol-page`

## Benchmark Config

- mode: `live`
- distribution: `hotspot-overlap`
- tier profile: `balanced`
- seed: `42`

## Success Criteria

- at least one target workload shows a clear latency improvement in `avg` or `p95`
- `correctness_failures = 0`
- `infra_failures = 0`
- protected workloads do not show a clear regression
- benchmark evidence still explains whether the run used mixed execution, fallback, or explicit hot override behavior

## Discard Criteria

- any correctness regression
- repeated infrastructure failure after rerun
- no meaningful improvement on target execution workloads
- obvious regression on protected workloads
- changes that make routing behavior less interpretable without proving a target-workload win

## Expected Win Areas

- avoid unnecessary fallback or duplicate tier checks in the live benchmark path
- reduce unnecessary work when assembling mixed base/delta/hot execution queries
- make hot-only versus mixed execution routing more explicit when it helps compare benchmark evidence
- tighten count/select query construction only when it changes actual execution work for mixed workloads

## Known Risk Areas

- changing fallback behavior in a way that hides tier-file failures rather than fixing execution cost
- conflating `prefer_hot` provenance with a stronger routing override than the benchmark intends
- speeding up one tier-mix path while regressing schema-scoped workloads that share the same harness path
- shifting execution semantics in a way that turns an evidence change into a methodology change

## Evidence To Review

- baseline `benchmark-summary.json`
- candidate `benchmark-summary.json`
- machine-readable diff report
- gate-specific artifact directories under `reports/{baseline,candidates,diff}/federated_query_execution/<gate>/`
- workload-level deltas for:
  - `mixed-tier-window`
  - `hot-only-window`
  - `cold-only-window`
  - `baseline-page-1`
  - `customer-region-page`
  - `security-symbol-page`

## Candidate Ideas

- avoid building or scanning unused tier inputs when the benchmark already knows they are absent
- reduce duplicated count/select query work when mixed execution and fallback paths share large query fragments
- tighten routing and provenance notes so repeated runs can explain why a path was chosen
- simplify tier query assembly only when it keeps the benchmark's visible execution semantics intact

## Gate Commands

```bash
./tools/autoresearch/benchmark/federated/scripts/benchmark_baseline.sh federated_query_execution fast
./tools/autoresearch/benchmark/federated/scripts/benchmark_candidate.sh federated_query_execution fast
./tools/autoresearch/benchmark/federated/scripts/benchmark_gate.sh federated_query_execution fast
```

Use `medium` or `heavy` in place of `fast` when you need the wider benchmark subset.
