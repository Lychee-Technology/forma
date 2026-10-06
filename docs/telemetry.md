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
(`telemetry.NewSink(emitter)`). The shipped `cmd/tools compactor` sets it
from `METRICS_STDOUT`; see [The demo binaries](#the-demo-binaries).

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
| `duckdb_circuit_breaker_transition_total` | counter | count | `state` (`open`, `half_open`, `closed`) | federated query engine (#634) |
| `compaction_manifest_contract_violation_total` | counter | count | `schema_id` | compactor |
| `compaction_dirty_ratio` | gauge | ratio | `schema_id` | compactor |
| `compaction_rewrite_pending_total` | counter | count | `schema_id` | compactor |
| `parquet_checksum_mismatch_total` | counter | count | `schema_id` | compactor (#347) |
| `compaction_rewrite_applied_total` | counter | count | `schema_id` | compactor (#188) |
| `entity_report_only_validation_violation_total` | counter | count | `schema_id`, `schema_name`, `kind` (`required`, `constraint`) | entity writes (#317) |

What the federated series measure, per successful DuckDB pass (they are not
gated on the caller asking for an execution plan):

- `fed_query_latency_histogram`: `translation` is SQL rendering, `execution`
  is the `duck.Query` call, `streaming` is the row iteration and handler loop.
  The three are disjoint intervals; the execution plan's `duckdb_fetch` is
  `execution + streaming`.
- `fed_query_row_count`: `pg` is the size of the dirty set fetched from
  Postgres for the anti-join; `duckdb` is the row count of the merged DuckDB
  scan. There is no `s3` series, and no series reports what the Postgres
  scans inside DuckDB read: Forma does not observe them (see
  [Retired metrics](#retired-metrics)).

All five samples are emitted together, after the pass has succeeded. A pass
that fails, whether at SQL rendering, at the DuckDB `Query` call or while
streaming rows, emits nothing, so no counter or histogram ever carries a
failed attempt. A query answered by the corrupt-parquet retry (#251) is
therefore counted once, from the pass that produced the returned page, the
same pass the execution plan describes.

`duckdb_circuit_breaker_transition_total` is not one of those five. It
counts the DuckDB circuit breaker's state changes, labelled by the state
entered: `open` when failures reach the threshold or a half-open probe fails,
`half_open` when a request is admitted as the probe after the open period,
and `closed` when a DuckDB pass succeeds. It moves once per transition, not
per request, and a request the open breaker rejects adds nothing. Each
transition also writes one line through the engine's logger: Warn "duckdb
circuit breaker opened" with the state it left and the error of the pass
that opened it, and Info for "half-open: probe admitted" and "closed".
`docs/federated-query/design.md` §7.1 lists exactly which events count.

The breaker series is a counter rather than a state gauge, so a trip that
opens and closes between two scrapes is still counted:
`increase(duckdb_circuit_breaker_transition_total{state="open"}[5m]) > 0`
alerts on any trip. Like `compaction_dirty_ratio`, it is not a heartbeat.
Transitions happen only when federated queries reach the breaker, so an
`open` with no later `half_open` or `closed` means no query has arrived
since, not that the breaker is still rejecting.

Names are wire names: dashboards key on them verbatim, so they are never
renamed or prefixed. **Adding a metric** means adding its descriptor to
`metrics.go`, the `Emit*` helper on `internal/telemetry.Sink` that emits it,
and a row in `helperCalls` in `internal/telemetry/telemetry_test.go`; the
contract test there refuses a helper whose emission does not match its
descriptor, and a helper with no row.

### Retired metrics

A metric that cannot keep the meaning its name promises is retired, not
redefined: its descriptor, its `Emit*` helper and its `helperCalls` row are
removed, and its name goes into `retiredMetricNames` in `metrics_test.go`,
which fails if the name is ever catalogued again. A retired name is never
reused for another quantity. An emitter that pre-registers from
`forma.MetricCatalogue()` simply stops seeing it; a dashboard keyed on it
stops receiving samples.

| Name | Retired by | Why |
|------|------------|-----|
| `fed_query_pushdown_efficiency` (gauge, ratio, `schema_id`) | #596 | It was the dirty-set size over the final matching row count, a stand-in for a Postgres-rows-scanned ratio Forma does not observe: the hot leg runs inside DuckDB, which exposes the scans' row counts only through per-query profiling. Its numerator is still emitted as `fed_query_row_count{source="pg"}`. |

## The demo binaries

`cmd/server`, `cmd/lambda` and the `compactor` subcommand of `cmd/tools` are
reference entrypoints, so they get the simplest thing that makes every metric
visible without a dependency: an opt-in stdout emitter.

| Variable | Default | Meaning |
|----------|---------|---------|
| `METRICS_STDOUT` | unset (off) | `true`/`1` writes every emitted metric as one JSON line on stdout. |

Each line has a stable shape:

```json
{"type":"forma_metric","ts":"2026-09-19T06:00:00Z","name":"entity_report_only_validation_violation_total","kind":"counter","unit":"count","value":1,"labels":{"kind":"constraint","schema_id":"12","schema_name":"lead"}}
```

On Lambda the lines land in the function's log group like any other stdout.

`cmd/tools compactor` is the only producer of the five compactor metrics, so
it reads the same variable (#594): a cron- or job-driven run whose output
reaches a log group reports them with no collector. Every pass that loads the
schema's manifest emits at least `compaction_dirty_ratio`. A pass that finds
no manifest emits nothing, and neither does one that fails before the
manifest is loaded, so the gauge is not a heartbeat: a run that printed none
either had no manifest to read (exit code 0) or failed before reading it
(non-zero).

The subcommand's logs go to stderr, so on a successful run stdout carries the
metric lines and nothing else. A failed run also prints its
`compactor: <error>` text there, after any metric the pass emitted before it
failed, and that text can span several lines; a consumer keeps the lines that
are `"type":"forma_metric"` objects and skips the rest. No other `cmd/tools`
subcommand has a metric to emit, and the emitter belongs to the one
`Compactor` the subcommand builds, so a subcommand that prints a document on
stdout (`inline-schema`) never shares it with a metric line.

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
