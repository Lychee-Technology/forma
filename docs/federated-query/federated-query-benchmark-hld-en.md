# Federated Query Benchmark High-Level Design

Last updated: 2026-04-17  
Repository: `forma`

## 1. Background

Forma already has federated querying, cold/hot tiering, CDC flush, compaction, basic pagination, and baseline performance tests. What it does not yet have is a dedicated benchmark that focuses on the hard parts of hybrid querying across cold and hot data.

The current performance suite is useful for regression coverage, but it does not systematically answer the following questions:

- How does the system behave under domain-shaped hybrid query workloads?
- How do different data distributions affect routing, predicate pushdown, and merge cost?
- How badly does deep pagination degrade as page numbers grow?
- What is the correctness and performance cost of overlap, soft deletes, and last-write-wins merging across tiers?

This design defines a TPC-E-inspired benchmark adapted to Forma's storage model and execution paths.

## 2. Goals

- Provide a repeatable, extensible, and comparable benchmark for Forma hybrid queries.
- Use a financial trading domain inspired by TPC-E, centered on trade, customer, and security entities.
- Cover both hot-column access and long-tail EAV filters.
- Exercise pagination, sorting, filtering, deep page jumps, and cross-tier merge behavior under multiple data distributions.
- Produce a stable set of metrics for optimization work, including latency percentiles, throughput, tier hit ratios, dedup rate, and pushdown efficiency.

## 3. Non-Goals

- Full TPC-E compliance is out of scope. This benchmark will not implement all 33 tables or the full transaction mix.
- This is not an official TPC benchmark publication framework and does not target auditability requirements.
- It does not replace the existing `internal/e2e_harness/federated` test suite. It extends it with benchmark-oriented workload coverage.
- Phase 1 does not target distributed execution or multi-host load generation.

## 4. Design Principles

- Reuse the existing E2E harness wherever possible.
- Preserve TPC-E-style business semantics while fitting Forma's `entity_main + eav_data + change_log + parquet` architecture.
- Ensure reproducibility through deterministic data generation with explicit seeds.
- Cover both correctness and performance. The benchmark is not just a load test.
- Treat deep pagination as a first-class problem, and explicitly model `LIMIT/OFFSET` behavior alongside future keyset alternatives.

## 5. Logical Architecture

The benchmark is split into six logical layers:

1. Schema Mapping Layer
2. Data Generation Layer
3. Tier Preparation Layer
4. Workload Definition Layer
5. Benchmark Runner Layer
6. Metrics and Report Layer

### 5.1 Schema Mapping Layer

This layer maps TPC-E-inspired entities into Forma schemas.

The initial benchmark will define three core schemas:

- `trade`
- `customer`
- `security`

Each schema must include:

- hot fields suitable for indexed filtering and sorting
- EAV fields that force dynamic attribute access
- fields that participate in overlap and merge cases

Recommended `trade` schema:

- Hot: `symbol`, `trade_type`, `quantity`, `price`, `trade_time`, `customer_id`
- EAV: `exchange`, `commission`, `is_cash`, `broker_id`, `order_channel`

This shape allows the benchmark to cover:

- pure hot pushdown
- pure EAV filtering
- hybrid hot + EAV predicates
- time-ordered pagination

### 5.2 Data Generation Layer

This layer generates benchmark datasets with configurable scale, distribution, and overlap patterns.

The benchmark must support these data distributions:

- `uniform`: baseline distribution
- `zipf`: hot customers, hot symbols, and hot regions
- `temporal`: higher density in recent time windows
- `partition-skew`: heavily imbalanced region or sector partitions
- `hotspot-overlap`: a controlled fraction of row IDs present in both cold and hot tiers

The generator output must guarantee:

- multiple versions of the same `row_id` can appear across tiers
- timestamps increase in a way that allows clear last-write-wins validation
- some records carry soft-delete markers
- selectivity can be calibrated for high-, medium-, and low-selectivity filters

### 5.3 Tier Preparation Layer

This layer assigns generated rows into three tiers:

- Cold/Base: historical parquet, larger files, low churn
- Warm/Delta: recently flushed parquet, smaller files, newer data
- Hot: unflushed Postgres data in `entity_main`, `eav_data`, and `change_log`

Default tier mix:

- Base: 60%
- Delta: 30%
- Hot: 10%

The benchmark must also support alternative mixes such as:

- high-hotness: 40/20/40
- long-tail history: 85/10/5
- high-overlap: 5%-10% key overlap between Base and Hot

Tier preparation is responsible for:

- writing cold and warm data to base and delta parquet
- inserting hot rows into Postgres
- marking overlap, delete, update, and restore scenarios

### 5.4 Workload Definition Layer

The initial workload set is organized into five categories.

#### A. Baseline Pagination

- unfiltered pagination ordered by `trade_time DESC`
- high-selectivity filter + pagination
- low-selectivity filter + pagination
- EAV filter + pagination
- mixed hot + EAV predicates + pagination

#### B. Deep Pagination

- `page = 1`
- `page = 100`
- `page = 1,000`
- `page = 100,000`

The benchmark must keep page size fixed, for example `20` or `50`, and explicitly record the offset size.

#### C. Tier Hit Modes

- hot-only hits
- cold-only hits
- cold + hot hits
- cold + warm + hot hits

#### D. Dedup and Override Cases

- same key in cold and hot with different versions
- hot-tier delete hiding a cold-tier row
- cold data partially overridden by hot-tier EAV updates

#### E. Routing and Strategy Comparisons

- `PreferHot = true`
- `RoutingStrategyHybrid`
- small result sets routed to Postgres
- large result sets routed to DuckDB

### 5.5 Benchmark Runner Layer

The runner manages the full benchmark lifecycle:

1. register schemas
2. generate data
3. prepare tiers
4. warm up the system
5. execute workloads repeatedly
6. aggregate latency and correctness metrics
7. export structured reports

The runner must support:

- serial execution
- fixed-concurrency execution
- running a subset of workloads
- explicit scale and distribution selection
- explicit random seed control

Recommended scales:

- `small`: 100K rows
- `medium`: 1M rows
- `large`: 10M rows

### 5.6 Metrics and Report Layer

The benchmark must emit:

- `p50`, `p95`, `p99`, `max`, `avg`
- `qps`
- `result_count`
- `hot_hit_ratio`
- `cold_hit_ratio`
- `dedup_count`
- `delete_filtered_count`
- `pushdown_efficiency`
- `memory_peak_mb`

Output formats:

- console summary
- JSON result file
- Markdown report

This allows the same benchmark to serve local tuning, CI artifact generation, and regression analysis.

## 6. Data Model

### 6.1 TPC-E-Inspired Entities

The benchmark does not attempt a full TPC-E schema port. Instead, it borrows the domain shape:

- `trade`: high write frequency and strong time locality
- `customer`: medium update frequency and selective filtering value
- `security`: low update frequency and cold-reference behavior

### 6.2 Hot and EAV Requirements

Each schema should include at least:

- 2 sortable hot fields
- 2 high-selectivity hot filter fields
- 1 low-selectivity hot filter field
- 2 EAV fields
- 1 field likely to participate in cross-tier override cases

## 7. Pagination Design

Deep pagination is a primary focus of this benchmark.

### 7.1 Baseline Path

Phase 1 uses the current pagination semantics:

- `LIMIT page_size OFFSET offset`

This gives a direct measurement of the current cost profile on merged results.

### 7.2 Deep Pagination Concerns

The benchmark must measure:

- how DuckDB read and sort cost changes as offset grows
- the cost of merge-before-pagination when both hot and cold paths contribute rows
- whether small-result heuristics misroute some workloads
- whether total-count computation becomes the bottleneck on very large page jumps

### 7.3 Large Page Jump Cases

The benchmark should include at least these fixed cases:

- `page_size=20, page=1`
- `page_size=20, page=100`
- `page_size=20, page=1,000`
- `page_size=20, page=100,000`

Equivalent keyset-style cases should be preserved as future comparison points, even if the keyset execution path is not implemented in phase 1.

## 8. Correctness Requirements

The benchmark must validate semantics, not just latency.

It must check:

- correct merged row counts across cold, warm, and hot tiers
- correct winner selection for the latest version of the same `row_id`
- correct suppression of cold rows by hot soft deletes
- correct EAV attribute winner selection during cross-tier merges
- stable, ordered, and duplicate-free pagination results

## 9. Observability Requirements

Each workload execution should capture:

- query name
- distribution
- scale
- page size
- offset or page number
- returned rows
- source tier mix
- duration
- error status

When execution-plan details are available, the benchmark should also capture:

- whether DuckDB was used
- whether `PreferHot` was enabled
- per-source row counts
- merge duration

## 10. Integration with Existing Code

The benchmark should build on the existing components instead of creating a separate test system:

- `internal/e2e_harness/federated/fixtures.go`
- `internal/e2e_harness/federated/seeding.go`
- `internal/e2e_harness/federated/query.go`
- `internal/e2e_harness/federated/performance_test.go`
- `internal/federated_pagination.go`
- `internal/federated_merge.go`
- `internal/federated_routing.go`

## 11. Risks and Tradeoffs

### 11.1 Risks

- The current harness query model is relatively simple and may need extension for richer hybrid predicates.
- Large-scale dataset preparation may dominate benchmark runtime.
- Deep pagination may be expensive at large scale if it requires full merge before slicing.
- Matching exact selectivity targets across different distributions requires calibration support in the generator.

### 11.2 Tradeoffs

- Phase 1 prioritizes a reproducible and useful benchmark over optimizer-level sophistication.
- Phase 1 reuses the current federated query stack rather than introducing a separate benchmark-only query DSL.
- Phase 1 should treat `medium` as the main regression scale and `large` as a controlled performance experiment scale.

## 12. Milestones

Recommended milestones:

1. define schemas, distributions, and workload matrix
2. implement data generation, tier loading, and benchmark runner
3. add deep-pagination, overlap, correctness checks, and reporting
4. establish baseline results and wire them into CI or a repeatable performance review flow

## 13. Deliverables

Phase 1 deliverables:

- benchmark HLD document
- benchmark implementation plan
- benchmark issue backlog
- benchmark scaffolding code
- at least one sample result set for `small` and `medium`
- a dedicated deep-pagination report
