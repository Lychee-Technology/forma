# Telemetry: metric catalogue and emitter registration

`internal/telemetry` is the metrics hook the module emits through. Every
`Emit*` helper routes one catalogued metric to a process-wide emitter, which
is a no-op until an entrypoint registers a backend. Before #423 no shipped
binary registered one, so every counter was inert on every deployment; this
page records the contract that ended that and how an operator turns a backend
on.

## Metric catalogue (the label contract)

`internal/telemetry/catalogue.go` declares one `Descriptor` per metric:

| Field    | Meaning |
|----------|---------|
| `Name`   | The wire name, verbatim on every backend. Dashboards key on it, so it never carries a namespace prefix. |
| `Kind`   | `counter`, `gauge` or `histogram`. Fixes the Go type of the emitted value (below). |
| `Unit`   | `count`, `ratio` or `milliseconds`. Backends map it onto their own unit vocabulary. |
| `Labels` | The exact, ordered set of label keys every emission carries. |
| `Help`   | Human text a backend may publish. |

The value contract per kind:

| Kind        | Go value  | Semantics |
|-------------|-----------|-----------|
| `counter`   | `int64`   | A monotonic increment (usually `1`). |
| `gauge`     | `float64` | The last observed value. |
| `histogram` | `int64`   | One observation, in the descriptor's unit. |

An emission whose name is not catalogued, whose label keys differ from the
descriptor, or whose value has the wrong Go type is **dropped** by every
backend: logged at Warn, counted under `forma_telemetry_dropped_total{reason}`
by the Prometheus exporter, never served under a shape no dashboard expects,
never a panic. `TestEveryEmitHelperIsInTheCatalogue` scans `telemetry.go` for
every exported `Emit*` helper and fails unless each one emits a catalogued
name with exactly its labels and value type, so the drop path is a safety net,
not a workflow.

**Adding a metric** therefore means: add a `Descriptor`, add the `Emit*`
helper that references it, and add the helper to `helperCalls` in
`catalogue_test.go`. No backend code changes; the mapping follows from the
kind.

Catalogued today:

| Name | Kind | Labels | Emitted by |
|------|------|--------|------------|
| `fed_query_latency_histogram` | histogram (ms) | `stage` | federated query engine |
| `fed_query_row_count` | counter | `source` | federated query engine |
| `fed_query_pushdown_efficiency` | gauge (ratio) | `schema_id` | federated query engine |
| `compaction_manifest_contract_violation_total` | counter | `schema_id` | compactor |
| `compaction_dirty_ratio` | gauge (ratio) | `schema_id` | compactor |
| `compaction_rewrite_pending_total` | counter | `schema_id` | compactor |
| `compaction_rewrite_applied_total` | counter | `schema_id` | compactor |
| `parquet_checksum_mismatch_total` | counter | `schema_id` | compactor (#347) |
| `entity_report_only_validation_violation_total` | counter | `schema_id`, `schema_name`, `kind` | entity writes (#317) |

## Backends

Two providers exist. Each is an adapter from the emitter signature onto one
backend, built from the catalogue at construction.

**`prometheus`** (`internal/telemetry/promexport`) builds one
`CounterVec` / `GaugeVec` / `HistogramVec` per descriptor on a private
registry, alongside the standard Go runtime and process collectors, and serves
it as a pull-based scrape endpoint. Histogram buckets are millisecond-scaled
(1 ms to 30 s). Only `cmd/server` can host the endpoint.

**`emf`** (`internal/telemetry/emfexport`) writes one CloudWatch Embedded
Metric Format line per emission to stdout. CloudWatch Logs extracts EMF from
any log group it ingests — a Lambda function's, an ECS task's under the
`awslogs` driver — into real CloudWatch metrics with no collector, sidecar,
extension or extra dependency. Labels become the metric's single dimension
set, in catalogue order; the kind maps onto `Count`, `None` (ratios) or
`Milliseconds`. A namespace is mandatory.

OpenTelemetry is not a provider. Nothing in-tree needs it, and the OTel
Prometheus exporter is itself built on the Prometheus client this module now
carries. The catalogue makes an `otelexport` a mechanical addition if a
deployment ever needs OTLP.

## Turning it on

Off by default: with no `METRICS_*` variable set, the emitter stays the no-op
it has always been and nothing changes for an existing deployment.
`bootstrap.MetricsConfigFromEnv` overlays these onto `forma.MetricsConfig`:

| Variable | Default | Meaning |
|----------|---------|---------|
| `METRICS_ENABLED` | `false` | Register an emitter at startup. |
| `METRICS_PROVIDER` | `prometheus` on `cmd/server`, `emf` on `cmd/lambda` | Which backend. |
| `METRICS_PATH` | `/metrics` | Scrape path (Prometheus only). |
| `METRICS_NAMESPACE` | `dataplane` | CloudWatch namespace (EMF only). Not applied to Prometheus names. |

`cmd/server` mounts the scrape handler on `METRICS_PATH` when the provider is
`prometheus`, and writes EMF to stdout when it is `emf`. `cmd/lambda`
supports `emf` only — a function has nothing a Prometheus server could scrape
— so `METRICS_ENABLED=true` alone is enough there, and naming `prometheus` is
a cold-start error. A misspelt provider is a startup error on both: a typo
must not run silently inert, which is the condition #423 exists to end.

The library layer follows the same rule: `forma.DefaultConfig` ships
`Metrics.Enabled = false`. The field was never read before #423, so the flip
changed nothing for anyone.

## What stays log-only

- The #317 milestone log line ("report-only schema validation violations
  reached a milestone") is unchanged. It is the default-visible signal for a
  deployment that does not scrape, and `docs/error-handling.md` already
  describes the counter as emitter-dependent.
- `cmd/tools` (the `compactor` subcommand emits the `compaction_*` and
  `parquet_checksum_mismatch_total` counters) registers no emitter yet. A
  short-lived CLI cannot be scraped; the EMF provider is the natural fit for a
  cron-driven run whose stdout lands in CloudWatch, and wiring it is #594.
