# Federated Query Post-Optimization Bottleneck Catalog

Last updated: 2026-05-03
Repository: `forma`

## Purpose

This catalog harvests remaining performance ceilings, tail-latency regressions, and workload-specific bottlenecks discovered by the three completed benchmark-driven optimization waves. Each entry links to benchmark evidence, identifies the suspected cause, and classifies the ceiling as tunable or architectural.

## 1. Deep Pagination Wave

**Wave:** `#46`, PR `#77` — optimize federated deep-pagination tradeTime pagination
**What was done:** skip federated select when offset exceeds visible rows; trim sort-only queries to tradeTime-only projections

### 1.1 Deep offset remains an architectural ceiling

| Field | Value |
|-------|-------|
| **Workload** | `deep-page-100000` |
| **Observed ceiling** | 100k-offset still requires scanning through all preceding rows before the slice window. Even after short-circuit empty-page selects and trimmed projections, the fundamental cost is linear in offset. |
| **Metric evidence** | PR #77 medium gate: `deep-page-100000 avg_latency_delta=-111.3ms` (win from empty-page short-circuit), but absolute p95/p99 at offset 100k remain orders of magnitude above page-1 latencies |
| **Suspected cause** | LIMIT/OFFSET requires the database to materialize and discard all rows up to the offset before returning the window. This is a fundamental property of offset-based pagination. |
| **Evidence reference** | PR #77, `internal/postgres_duckdb_query.go`, `internal/e2e_harness/federated/query.go` |
| **Tunable vs architectural** | Architectural (offset semantics) |
| **Recommended follow-up** | Keyset pagination. PR #102 already implements keyset through the DuckDB template with `keyset-page-1`, `keyset-page-1000`, `keyset-page-100000` workloads. Remaining work: validate keyset at large-scale live execution, add keyset vs offset benchmark comparison workflows. |
| **Linked issue** | `#97` (closed — already implemented), `#100` (open — large-scale validation) |

### 1.2 baseline-page-1 latency regression from projection negotiation

| Field | Value |
|-------|-------|
| **Workload** | `baseline-page-1` |
| **Observed ceiling** | baseline-page-1 regressed in medium gate despite being a protected workload: +32.2ms in PR #77 medium gate, +92.1ms in PR #78 medium gate. Fast gate was clean (-32.2ms in PR #77, -44.0ms in PR #78). |
| **Metric evidence** | PR #77 medium: `baseline-page-1 avg_latency_delta=+32.2ms`; PR #78 medium: `baseline-page-1 avg_latency_delta=+92.1ms`. No correctness failures, all row IDs match baseline. |
| **Suspected cause** | Projection negotiation overhead: the projection trimming and conditional EAV join determination add resolution cost that amortizes poorly on small first-page queries. The medium gate uses a different distribution (zipf) which may amplify the overhead. |
| **Evidence reference** | PRs #77, #78 benchmark diffs; `internal/postgres_duckdb_query.go:206` (`needsEAVJoin`); `internal/federated_routing.go` (routing decision per request) |
| **Tunable vs architectural** | Tunable — projection decision caching, precomputed attribute-to-column maps, or per-schema warmup could reduce this overhead |
| **Recommended follow-up** | Profile baseline-page-1 across distributions to isolate whether the regression is projection negotiation, routing decision overhead, or distribution-specific. Cache projection decisions per-schema to avoid per-request resolution. |
| **Linked issue** | `#104` (to be created) |

### 1.3 Deep-page projection trimming introduces trade-off

| Field | Value |
|-------|-------|
| **Workload** | `deep-page-1000` (sort-only path) vs filter workloads (full-projection path) |
| **Observed ceiling** | The narrow projection path for sort-only deep pages saves scan cost but bifurcates the code path — every new attribute added to the query shape must be classified as sort-only vs filter vs window to determine which projection to use. |
| **Suspected cause** | `buildFederatedQueryCountSQLDynamic` and `buildParquetTierQuery` now have conditional projection logic. The classification itself is straightforward but adds maintenance surface. |
| **Evidence reference** | PR #77, `internal/e2e_harness/federated/query.go` (projection selection helpers), `TestProjectionSelectionHelpers` |
| **Tunable vs architectural** | Architectural — this is a code-maintenance ceiling rather than a latency ceiling. Unifying projections where safe would reduce it. |
| **Recommended follow-up** | Document the projection classification rules. When schema attribute layout stabilizes, consider whether a unified default projection is fast enough to eliminate the bifurcation. |
| **Linked issue** | None (documentation note only) |

## 2. Filter Pushdown and EAV Wave

**Wave:** `#51`, PRs `#78`, `#86`, `#87`; sub-issues `#79`-`#85`
**What was done:** targeted EAV pivot with attr_id pushdown; dynamic DuckDB template; conditional EAV join; tier-specific pushdown metrics; service-path routing for all filter workloads

### 2.1 Cold-tier EAV extraction cost is architectural

| Field | Value |
|-------|-------|
| **Workload** | `eav-selective-page`, `eav-low-selectivity-page`, `mixed-hot-eav-page` |
| **Observed ceiling** | EAV-only attributes (no column_binding) stored in `attributes_json` inside Parquet require DuckDB `json_extract_string` at filter time. Direct column reads are ~10-100x faster than JSON extraction on the same data volume. |
| **Metric evidence** | PR #87 noted "cold-tier EAV filter semantics already corrected in production/service benchmark path via `BuildBenchmarkS3Projection` with `json_extract_string` for EAV-only attributes." The fix confirmed correctness but did not eliminate the JSON extraction cost. |
| **Suspected cause** | Production Parquet files store hot-column attributes as flat columns (via CDC flush), but EAV-only attributes live in a JSON blob. This is by design for schema flexibility, but it means cold-tier EAV filtering will always involve JSON extraction. |
| **Evidence reference** | PR #87, `internal/duckdb_schema_projection.go:404-430` (benchmark EAV JSON extraction), `internal/e2e_harness/federated/benchmark/generator.go` (Parquet generation) |
| **Tunable vs architectural** | Mostly architectural. Could be mitigated by: (1) CDC-side EAV column flattening into Parquet for attribute sets known at compile time, (2) DuckDB JSON index extensions, (3) hot-tier-biased routing for EAV-heavy filter queries. |
| **Recommended follow-up** | Add tier-specific EAV extraction cost measurement to pushdown workloads. Investigate CDC-side flattening for common EAV attributes. |
| **Linked issue** | None (medium priority — document and revisit after large-scale baseline) |

### 2.2 Pushdown efficiency baseline unpublished

| Field | Value |
|-------|-------|
| **Workload** | `tier-pushdown-hot`, `tier-pushdown-eav`, `tier-pushdown-mixed`, `tier-pushdown-cold-only` |
| **Observed ceiling** | The pushdown measurement infrastructure exists (PR #87) and the workloads have automated assertions (`pushdown-active`, `pushdown-efficiency-reasonable`), but no published baseline shows actual pushdown efficiency ratios. Without a published baseline, it is impossible to evaluate whether pushdown is working as expected or detect regressions. |
| **Metric evidence** | PR #87 body does not include benchmark deltas or pushdown efficiency numbers. The test suite validates that assertions pass but does not publish the values. |
| **Suspected cause** | These workloads were introduced as a measurement tool rather than a gating mechanism. A baseline run and publication step was deferred. |
| **Evidence reference** | PR #87, `internal/e2e_harness/federated/benchmark/runner.go` (`extractPerTierMetrics`, `validatePushdownAssertions`) |
| **Tunable vs architectural** | Methodology debt — tunable by running and publishing a baseline |
| **Recommended follow-up** | Run `small-live` and `medium-live` baselines with pushdown workloads, publish the efficiency ratios, and add pushdown efficiency to the protected workload guardrail set. |
| **Linked issue** | None (can be done as part of `#100` large-scale baseline work) |

### 2.3 Service path vs harness path unification — resolved

The filter wave's status assessment (`#51` body) identified 8 gaps. All 8 were addressed by sub-issues `#79`-`#85`:
- `#79`: Service-path filter workloads — closed by PR #86
- `#80`: EAV-only low-selectivity workload — closed by PR #86
- `#81`: Dynamic schema mapping in template — closed by PR #86
- `#82`: Tier-specific pushdown benchmarks — closed by PR #87
- `#83`: Cold-tier EAV filter semantics — closed by PR #87
- `#84`: Conditional EAV scan — closed by PR #86 via `needsEAVJoin`
- `#85`: Dirty-ID fallback removal — closed by PR #86

No remaining bottlenecks from the original gap list.

## 3. Routing and Concurrency Wave

**Wave:** `#54`, PR `#94`; sub-issues `#88`-`#93`
**What was done:** query-shape-aware routing; fixed-concurrency benchmark execution; concurrency stability assertions; route evidence in benchmark artifacts; routing heuristic tuning

### 3.1 Concurrency tail latency plateau

| Field | Value |
|-------|-------|
| **Workload** | Routing-sensitive workloads under `Concurrency > 1` |
| **Observed ceiling** | Fixed-concurrency execution infrastructure (PR #94) was added and concurrency stability checks were implemented, but the PR body does not report benchmark deltas for concurrent workloads. Without evidence of tail latency improvement under concurrency, the assumption is that p95/p99 remain dominated by thread-pool contention (DuckDB threads=4, Postgres connection pool) or shared resource contention. |
| **Metric evidence** | No published concurrent benchmark deltas in PR #94. The infrastructure captures per-worker results and route evidence, but the actual latency numbers under concurrency are unreported. |
| **Suspected cause** | DuckDB `PRAGMA threads=4` is fixed during comparisons (PR #94 intentionally kept threads constant), but the thread count may be suboptimal for concurrent query mixing. Postgres pool size is configured per deployment but not tuned for benchmark concurrency. Shared state (DuckDB instance, S3 connection) may introduce contention. |
| **Evidence reference** | PR #94, `internal/federated_routing.go`, `internal/e2e_harness/federated/benchmark/runner.go:211-245` (concurrent execution), `internal/advanced_query_template_duckdb.go:15` (threads=4), `internal/bootstrap/postgres.go:33` (`MaxConns`) |
| **Tunable vs architectural** | Tunable — thread/pool sizing, per-worker DuckDB isolation, request-batching strategies |
| **Recommended follow-up** | Run small-live benchmarks with `Concurrency=2,4,8` and report p50/p95/p99 deltas. Profile DuckDB thread contention under concurrent mixed-tier queries. Test whether per-worker DuckDB connections reduce tail latency. |
| **Linked issue** | `#104` (to be created, shared with 1.2 if scope allows) |

### 3.2 Routing heuristic biases not yet validated at large scale

| Field | Value |
|-------|-------|
| **Workload** | `routing-hot-selective`, `routing-full-scan`, `routing-mixed-filter` |
| **Observed ceiling** | The new routing heuristics (deep-page → DuckDB, small-page → PG, cold-only → DuckDB) were validated with unit tests and small-live benchmarks but not at realistic data volumes. The `offset >= 10000 → DuckDB` threshold and `limit < 1000 → PG` threshold were chosen based on reasoning, not on cross-scale benchmark evidence. |
| **Metric evidence** | PR #94 includes routing heuristic tests (`TestEvaluateRoutingPolicy_QueryShapeAware` with 10 scenarios) but no cross-scale benchmark evidence. Route-engine tracking in artifacts exists but baseline route distributions are not published. |
| **Suspected cause** | Thresholds were set based on architecture reasoning rather than empirical tuning. At medium or large scales, the optimal threshold may shift. |
| **Evidence reference** | PR #94, `internal/federated_routing.go:102-110` (deep-page and small-page thresholds), `internal/e2e_harness/federated/benchmark/workload.go` (routing workloads) |
| **Tunable vs architectural** | Tunable — cross-scale tuning of thresholds |
| **Recommended follow-up** | Validate routing thresholds at medium and (when available) large scales. Compare route distributions vs latency for the same workload under different routing strategies. |
| **Linked issue** | `#100` (large-scale live execution — co-track) |

### 3.3 Route instability under concurrency

| Field | Value |
|-------|-------|
| **Workload** | Tier-mix workloads under `Concurrency > 1` |
| **Observed ceiling** | `concurrent-run-route-engine-stable` assertion (PR #94) checks whether all workers in a concurrent batch chose the same engine, but the assertion's pass/fail results are not published. If workers diverge (some PG, some DuckDB), results may not be comparable. |
| **Metric evidence** | The assertion infrastructure exists (`assertions.go:1586`) but no published evidence of route stability or instability under concurrency. |
| **Suspected cause** | Routing is deterministic for a given query shape, so divergence should only occur if workers see different dataset states or if race conditions affect routing signals. |
| **Evidence reference** | PR #94, `internal/e2e_harness/federated/benchmark/runner.go:1586-1594` |
| **Tunable vs architectural** | Methodology — resolved if assertion passes; if it fails, indicates a correctness or state-isolation problem |
| **Recommended follow-up** | Run concurrent benchmarks with route-stability assertion enabled and report results. |
| **Linked issue** | None (can be checked during `#104` benchmark runs) |

## 4. Cross-Cutting Ceilings

These ceilings span multiple waves or fall outside any single wave's scope.

### 4.1 Large-scale live benchmark evidence is missing

| Field | Value |
|-------|-------|
| **Workload** | All |
| **Observed ceiling** | All published benchmark evidence is from `small-live` and `medium-live` presets. The `large` scale is `plan`-only (see `benchmark-heavy` preset). Deep-page-100000 at scale 100k rows is not the same as deep-page-100000 at scale 1M rows. Without large-scale live runs, all latency evidence is at reduced data volume. |
| **Metric evidence** | `docs/federated-query/federated-query-benchmark-ci-and-ops-guide.md:99-113` — `heavy-plan` is plan-only; `large` scale live execution is explicitly deferred |
| **Suspected cause** | Large-scale execution requires more resources (memory, time) than current CI or local environments provide. The Docker-dependent harness and the DuckDB embedded engine both have resource ceilings. |
| **Evidence reference** | CI/ops guide, `cmd/benchmark/main.go`, `#100` |
| **Tunable vs architectural** | Methodology — requires environment scaling, not code changes |
| **Recommended follow-up** | Already tracked as `#100`. This catalog does not create a new issue. |
| **Linked issue** | `#100` (open) |

### 4.2 No production resilience patterns tested in benchmark

| Field | Value |
|-------|-------|
| **Workload** | All (potential failure modes untested) |
| **Observed ceiling** | The benchmark tests correctness and latency under normal operation. It does not test: Postgres connection failures, S3 unavailability, DuckDB OOM, stale dirty-ID sets, or partial degraded modes. The `consistency_mode` field (PR #103, `#98`) adds the plumbing but no benchmark workloads exercise `eventual` mode or degraded-mode fallbacks. |
| **Metric evidence** | `docs/federated-query/design.md` §9 references resilience patterns; `docs/federated-query/federated-query-benchmark-hld-en.md` does not include failure-mode workloads |
| **Suspected cause** | Resilience testing requires fault injection that the current harness does not support. |
| **Evidence reference** | `docs/federated-query/design.md` §9, PR #103, `#98` |
| **Tunable vs architectural** | Methodology (requires harness fault injection) / architectural (degraded modes are structural) |
| **Recommended follow-up** | Already tracked as `#98` and `#99`. This catalog does not create a new issue. |
| **Linked issue** | `#98` (open — circuit breaker and degraded modes), `#99` (open — longitudinal regression tracking) |

### 4.3 DuckDB thread configuration not workload-aware

| Field | Value |
|-------|-------|
| **Workload** | All DuckDB-path workloads |
| **Observed ceiling** | `PRAGMA threads=4` is hardcoded in the DuckDB template and in the CDC flusher. Under concurrent benchmark execution, 4 threads may be suboptimal — too many for single-concurrency workloads (context switching overhead) and too few for high-concurrency mixed workloads (underutilization). |
| **Metric evidence** | `internal/advanced_query_template_duckdb.go:15` (`PRAGMA threads=4`), `internal/duckdb_conn.go:213` (config-driven threads), `internal/cdc/config.go:119` (default threads=4). The config supports `MaxParallelism` but the template ignores it. |
| **Suspected cause** | The template has a hardcoded pragma that overrides the connection-level thread setting. The connection-level `MaxParallelism` is set but the template re-declares `threads=4`. |
| **Evidence reference** | `internal/advanced_query_template_duckdb.go:15`, `internal/duckdb_conn.go:205-215` |
| **Tunable vs architectural** | Tunable — replace hardcoded pragma with config-driven or workload-adaptive thread count |
| **Recommended follow-up** | Make template thread count configurable or auto-tuned. Measure QPS vs thread count under concurrent workloads. |
| **Linked issue** | None (low effort — can be addressed alongside other routing/concurrency tuning) |

## 5. Benchmark Evidence Gaps

The local `.artifacts/benchmark/` tree is empty. All benchmark evidence referenced in this catalog comes from:

- PR descriptions and commit messages (PRs #77, #78, #86, #87, #94)
- Issue bodies (`#46`, `#51`, `#54`)
- The benchmark result schema in code (`report.go`, `runner.go`)
- Test assertions and suite results

No committed baseline artifacts (`benchmark-result.json`, `benchmark-summary.json`, diff reports) exist in the repository for the three optimization waves. The evidence is textual and embedded in PR descriptions rather than machine-readable.

## 6. Priority-Ranked Bottleneck Summary

| Priority | Bottleneck | Section | Nature | Linked Issue |
|----------|-----------|---------|--------|-------------|
| P0 | Deep offset architectural ceiling → keyset validation at scale | 1.1 | Architectural | #97 (closed), #100 |
| P0 | baseline-page-1 latency regression from projection negotiation | 1.2 | Tunable | #104 (new) |
| P1 | Concurrency tail latency plateau | 3.1 | Tunable | #104 (new) |
| P1 | Cold-tier EAV JSON extraction cost | 2.1 | Mostly architectural | None |
| P1 | DuckDB threads not workload-aware | 4.3 | Tunable | None |
| P1 | Routing threshold cross-scale validation | 3.2 | Tunable | #100 (co-track) |
| P2 | Pushdown efficiency baseline unpublished | 2.2 | Methodology | #100 (co-track) |
| P2 | Production resilience patterns untested | 4.2 | Methodology/Arch | #98, #99 |
| P2 | Large-scale live evidence missing | 4.1 | Methodology | #100 |
| P2 | Route instability under concurrency | 3.3 | Methodology | #104 (verify) |
| Note | Deep-page projection bifurcation | 1.3 | Code maintenance | None |

## References

- PR #77: Deep pagination optimization (offset short-circuit, trimmed projections)
- PR #78: Targeted EAV pivot with attr_id pushdown
- PR #86: Dynamic DuckDB template, service-path fidelity, conditional EAV join
- PR #87: Tier-specific pushdown efficiency benchmarks
- PR #94: Routing heuristics and fixed-concurrency benchmark execution
- PR #102: Keyset pagination implementation
- PR #103: Consistency mode request field
- `docs/federated-query/federated-query-benchmark-baseline-runbook.md`
- `docs/federated-query/federated-query-benchmark-ci-and-ops-guide.md`
- `docs/federated-query/federated-query-benchmark-autoresearch-pr-plan.md`
- `internal/e2e_harness/federated/benchmark/report.go` (artifact schema)
- `internal/e2e_harness/federated/benchmark/runner.go` (execution and assertion model)
