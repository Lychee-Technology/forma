# Telemetry: the metric contract and how to receive it

Forma is a library first. It emits nine metrics and chooses no backend for
them: nothing in the module links Prometheus, OpenTelemetry, CloudWatch or
any other telemetry SDK. An application that embeds Forma hands it a
`forma.MetricEmitter` and adapts each `forma.Metric` onto whatever it already
uses. Without one, Forma emits nothing (#423).

## Receiving metrics from a library instance

```go
type MetricEmitter interface {
    EmitMetric(ctx context.Context, m Metric)
}

type Metric struct {
    Name   string            // wire name, e.g. "fed_query_latency_histogram"
    Kind   MetricKind        // counter | gauge | histogram
    Unit   MetricUnit        // count | ratio | milliseconds
    Labels map[string]string // exactly the catalogued keys; a fresh map per emission
    Value  float64           // counter: increment; gauge: last value; histogram: one observation
}
```

Set one per Forma instance on `Config.Metrics.Emitter` (or with
`forma.WithMetricEmitter`):

```go
cfg := forma.DefaultConfig(registry)
cfg.Metrics.Emitter = myEmitter // adapts forma.Metric onto your backend
manager, err := factory.NewEntityManagerWithConfigContext(ctx, cfg, pool)
```

Every metric that manager and its federated engine emit reaches
`myEmitter`; a second manager built with another emitter reports only to
that one; a manager built without one is silent. There is no process-global
registration. `forma.MetricEmitterFunc` adapts a plain function, and
`example_metrics_test.go` shows a complete adapter.

`EmitMetric` runs synchronously on the goroutine doing the write, query or
compaction pass, so an emitter must be cheap, non-blocking and safe for
concurrent use. It is also the safety boundary: a panic inside `EmitMetric`
is recovered and logged, and an emission that does not match its catalogue
descriptor (a Forma bug) is dropped and logged. Telemetry never fails the
operation that emitted.

A `compaction.Compactor` is built by hand rather than by the factory; it
takes its sink through the exported `Metrics` field
(`telemetry.NewSink(emitter)`). The shipped `cmd/tools compactor` does not
set it yet; see #594.

## The catalogue (the label contract)

`forma.MetricCatalogue()` returns one `MetricDescriptor` per metric — name,
kind, unit, the exact ordered label keys, help text. An emitter that
pre-registers instruments (a Prometheus vector needs its label names up
front; an OTel instrument needs its kind and unit) builds them from it.
Every label is bounded-cardinality by construction: a schema id or name, or
a fixed enumeration.

| Name | Kind | Unit | Labels | Emitted by |
|------|------|------|--------|------------|
| `fed_query_latency_histogram` | histogram | milliseconds | `stage` (`translation`, `execution`, `streaming`) | federated query engine |
| `fed_query_row_count` | counter | count | `source` (`pg`, `duckdb`) | federated query engine |
| `fed_query_pushdown_efficiency` | gauge | ratio | `schema_id` | federated query engine |
| `compaction_manifest_contract_violation_total` | counter | count | `schema_id` | compactor |
| `compaction_dirty_ratio` | gauge | ratio | `schema_id` | compactor |
| `compaction_rewrite_pending_total` | counter | count | `schema_id` | compactor |
| `parquet_checksum_mismatch_total` | counter | count | `schema_id` | compactor (#347) |
| `compaction_rewrite_applied_total` | counter | count | `schema_id` | compactor (#188) |
| `entity_report_only_validation_violation_total` | counter | count | `schema_id`, `schema_name`, `kind` (`required`, `constraint`) | entity writes (#317) |

Names are wire names: dashboards key on them verbatim, so they are never
renamed or prefixed. **Adding a metric** means adding its descriptor to
`metrics.go`, the `Emit*` helper on `internal/telemetry.Sink` that emits it,
and a row in `helperCalls` in `internal/telemetry/telemetry_test.go`; the
contract test there refuses a helper whose emission does not match its
descriptor, and a helper with no row.

## The demo binaries

`cmd/server` and `cmd/lambda` are reference entrypoints, so they get the
simplest thing that makes every metric visible without a dependency: an
opt-in stdout emitter.

| Variable | Default | Meaning |
|----------|---------|---------|
| `METRICS_STDOUT` | unset (off) | `true`/`1` writes every emitted metric as one JSON line on stdout. |

Each line has a stable shape:

```json
{"type":"forma_metric","ts":"2026-09-19T06:00:00Z","name":"entity_report_only_validation_violation_total","kind":"counter","unit":"count","value":1,"labels":{"kind":"constraint","schema_id":"12","schema_name":"lead"}}
```

On Lambda the lines land in the function's log group like any other stdout.
There is no `/metrics` endpoint, no registry, no provider selection and no
startup failure path: a boolean cannot be misconfigured, and unset means
nothing new is written. An operator who wants a real backend behind one of
these binaries writes an adapter against `forma.MetricEmitter` in a fork or
wrapper; Forma does not carry one.

## `MetricsConfig` legacy fields

`forma.MetricsConfig.Emitter` is the only field Forma reads. Every other
field (`Enabled`, `Provider`, `Endpoint`, `CollectionInterval`, the
`Enable*` switches, `Namespace`, `Labels`, `MaxSamples`) predates #423, was
never read, still is not, and keeps its default (`Enabled: true`,
`Provider: "prometheus"`, …) so existing configs round-trip unchanged. In particular an emitter is not gated on `Enabled`:
`WithMetrics(MetricsConfig{Emitter: e})` would zero `Enabled` and silently
reproduce the "configured but inert" condition #423 ended.

## What stays log-only

The #317 milestone log line ("report-only schema validation violations
reached a milestone") is unchanged: it is the default-visible signal for a
deployment with no emitter, and `docs/error-handling.md` describes the
counter as emitter-dependent.
