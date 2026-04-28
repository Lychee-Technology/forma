# Performance Target: `postgres_duckdb_query`

## Objective

Use the federated benchmark to judge whether candidate changes improve deep-pagination behavior in the federated DuckDB SQL path without introducing correctness regressions.

## Why This Target

`internal/postgres_duckdb_query.go` is the core bridge for federated query execution between Postgres state, DuckDB planning/execution, dirty-id handling, and result streaming.

It is both correctness-sensitive and performance-sensitive, and the benchmark already has workload coverage that exercises the deep-page path directly.

## In Scope

- `internal/postgres_duckdb_query.go`
- `internal/duckdb_template_renderer.go`
- `internal/advanced_query_template_duckdb.go`
- deep-page SQL shape, pushdown, and template hot spots
- dirty-id exclusion SQL that materially affects deep-page latency
- pagination/order/qualify/query-merge structure that can change DuckDB work

## Out Of Scope

- benchmark methodology changes
- workload definition changes
- selective filter oracle redesign
- `PreferHot` execution semantics outside existing benchmark scope
- Go-side row streaming or buffer micro-optimizations unless they are directly required by a SQL-path change

## Benchmark Subset

Gate definitions:

- `fast`
  - target workloads: `deep-page-1000`
  - protected workloads: `baseline-page-1`, `mixed-tier-window`
  - scale: `small`
  - iterations: `1`
- `medium`
  - target workloads: `deep-page-1000`, `deep-page-100000`
  - protected workloads: `baseline-page-1`, `mixed-tier-window`
  - scale: `small`
  - iterations: `2`
- `heavy`
  - target workloads: `deep-page-1000`, `deep-page-100000`
  - protected workloads: `baseline-page-1`, `mixed-tier-window`, `hot-only-window`, `cold-only-window`
  - scale: `medium`
  - iterations: `2`

Default target workloads:

- `deep-page-1000`
- `deep-page-100000`

Default protected workloads:

- `baseline-page-1`
- `mixed-tier-window`

## Benchmark Config

- mode: `live`
- scale: `small`
- distribution: `hotspot-overlap`
- tier profile: `balanced`
- iterations: `2`
- seed: `42`

## Success Criteria

- at least one target workload shows a clear latency improvement in `avg` or `p95`
- `correctness_failures = 0`
- `infra_failures = 0`
- protected workloads do not show a clear regression

## Expected Win Areas

- reduce work entering the final deep-page pagination stage
- reduce row width carried through deep-page sort and window steps
- reduce unnecessary work in dirty-id exclusion, dedup, or count paths when it materially changes deep-page cost
- simplify SQL shape only when it changes actual planner or execution work

## Known Risk Areas

- changing total-count behavior or page membership semantics
- moving dedup or delete filtering across stages in a way that changes visible rows
- improving deep-page latency by regressing tier-mix workloads that still share the SQL path
- switching to a narrower intermediate shape that later re-expands rows incorrectly

## Discard Criteria

- any correctness regression
- repeated infrastructure failure after rerun
- no meaningful improvement on either deep-page workload
- obvious regression on protected workloads

## Evidence To Review

- baseline `benchmark-summary.json`
- candidate `benchmark-summary.json`
- machine-readable diff report
- gate-specific artifact directories under `reports/{baseline,candidates,diff}/postgres_duckdb_query/<gate>/`
- workload-level deltas for:
  - `deep-page-1000`
  - `deep-page-100000`
  - `baseline-page-1`
  - `mixed-tier-window`

## Gate Commands

```bash
./tools/autoresearch/benchmark/federated/scripts/benchmark_baseline.sh postgres_duckdb_query fast
./tools/autoresearch/benchmark/federated/scripts/benchmark_candidate.sh postgres_duckdb_query fast
./tools/autoresearch/benchmark/federated/scripts/benchmark_gate.sh postgres_duckdb_query fast
```

Use `medium` or `heavy` in place of `fast` when you need the wider benchmark subset.

## Candidate Ideas

- reduce the number of rows reaching `COUNT(*) OVER()`, `ROW_NUMBER()`, and final `ORDER BY created_at DESC LIMIT/OFFSET`
- narrow the row shape before deep-page ordering and only widen rows after page membership is known when correctness still holds
- push deduplication, deletion filtering, or source pruning earlier only when it clearly reduces the final window/sort work
- improve SQL pushdown/selectivity so hot/cold sources emit fewer rows before the final pagination stage
- simplify the federated SQL template so DuckDB does less work on rows that will not survive deep pagination

## Deep-Page Focus

When choosing candidates, prioritize changes that materially reduce work in the final deep-page stage:

- rows entering `unified`
- rows surviving into `ROW_NUMBER() OVER (PARTITION BY row_id ORDER BY ver_ts DESC)`
- rows carried through `COUNT(*) OVER()` and `CEIL(COUNT(*) OVER() / page_size)`
- rows participating in final `ORDER BY created_at DESC LIMIT/OFFSET`

Prefer candidates that change deep-page SQL shape over low-leverage rewrites.

Examples of higher-value directions:

- compute page membership from a narrower intermediate relation before projecting wide columns
- separate total-count work from page-row ordering work if the template can preserve exact semantics
- prune deleted or superseded rows before expensive global ordering/window steps

Examples of lower-value directions unless benchmark evidence strongly suggests otherwise:

- cosmetic SQL rewrites that leave row counts and sort/window work unchanged
- anti-join rewrites that do not materially change the amount of data reaching the final deep-page stage
- Go-side execution or allocation tweaks that do not change SQL shape
