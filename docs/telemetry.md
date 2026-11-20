# Telemetry: the metric contract and how to receive it

Forma is a library first. It emits ten metrics and chooses no backend for
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
| `fed_query_latency_histogram` | histogram | milliseconds | `stage` (`translation`, `execution`, `streaming`, `snapshot`) | federated query engine |
| `fed_query_row_count` | counter | count | `source` (`pg`, `duckdb`, `hot`) | federated query engine |
| `duckdb_circuit_breaker_transition_total` | counter | count | `state` (`open`, `half_open`, `closed`) | federated query engine (#634) |
| `federated_degraded_fallback_total` | counter | count | `cause` (`breaker_rejected`, `duckdb_unavailable`, `corrupt_parquet`, `scan_guard_violation`, `read_failed`, `postgres_read_failed`, `other`), `outcome` (`served`, `failed`) | federated query engine (#714) |
| `compaction_dirty_ratio` | gauge | ratio | `schema_id` | compactor |
| `compaction_rewrite_pending_total` | counter | count | `schema_id` | compactor |
| `parquet_checksum_mismatch_total` | counter | count | `schema_id` | compactor (#347) |
| `compaction_rewrite_applied_total` | counter | count | `schema_id` | compactor (#188) |
| `entity_report_only_validation_violation_total` | counter | count | `schema_id`, `schema_name`, `kind` (`required`, `constraint`) | entity writes (#317) |

What the federated series measure, per successful federated pass (they are
not gated on the caller asking for an execution plan). A pass reads
PostgreSQL over pgx in one snapshot, runs one DuckDB statement over the
parquet tiers, and merges the two in Go (design.md §6.1). A pass that waits
for one of the engine's `duckdb.maxConnections` pass slots (design.md §6.1,
*Connection use*) waits before its snapshot, and no stage reports that wait.

- `fed_query_latency_histogram`: `translation` is SQL rendering, `execution`
  is the DuckDB query call, `streaming` is the parquet leg's row iteration
  plus the loop that hands the merged page to the row handler, so a slow
  handler shows there. The three are disjoint intervals; the execution plan's
  `duckdb_fetch` is `execution + streaming`. `snapshot` is the time the pass
  spent on its PostgreSQL reads: opening the snapshot, the dirty set, the hot
  keys, and after the merge the page's hot rows. The DuckDB statement runs
  while the snapshot is open, between the hot keys and the page read; its
  time is in `execution` and `streaming`, not in `snapshot`. When a deep
  page's window carries no count, the pass recounts the parquet leg with a
  second DuckDB statement (#181); its translation, execution and streaming
  add to the page statement's, and so to the plan's `duckdb_fetch`.
- `fed_query_row_count`: `pg` is the size of the dirty set the pass read
  from Postgres and staged for the anti-join. `hot` is the number of hot
  keys the page drew from: the rows of the dirty window that match the
  request's condition (and, on a keyset page, lie after its cursor). Beside
  `pg` it says how much of the hot tier the predicate kept. `duckdb` counts
  the rows of the parquet leg's window, which on an offset page holds up to
  `hot` + limit rows. There is no `s3` series.

A pass over a schema with no parquet to read (#720) runs no DuckDB statement
and emits only `snapshot`, `pg` and `hot`. A pass whose request leaves out the
hot tier reads no hot keys, and its `hot` is 0.

All of a pass's samples are emitted together, after the pass has succeeded.
A pass that fails, whether at a snapshot read, at SQL rendering, at the
DuckDB query call, or while streaming rows (the row handler included), emits
nothing, so no counter or histogram ever carries a failed attempt. A query answered by the
publication-race retry (#720) is therefore counted once, from the pass that produced the returned page, the
same pass the execution plan describes.

`duckdb_circuit_breaker_transition_total` is not one of a pass's samples. It
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

`federated_degraded_fallback_total` is not one of a pass's samples either.
It counts the PostgreSQL-only fallbacks a query falls back to, on a
deployment with `duckdb.allowPartialDegradedMode`
(`DUCKDB_ALLOW_PARTIAL_DEGRADED_MODE`), when its federated pass fails with
a degradable error (design.md §7.2): one sample of 1 per attempt, emitted once
the fallback's PostgreSQL read has returned. `outcome` is `served` when that
read answered the request and `failed` when it failed, so a request is never
counted under both. `cause` is the class of the failure the fallback absorbed,
never its text, taken from the engine's sentinels and error types:

- `breaker_rejected`: the open DuckDB circuit breaker turned the request away
  before any DuckDB work.
- `duckdb_unavailable`: the engine has no DuckDB client. Since #715 a
  deployment with DuckDB enabled fails startup instead of running without
  one, so only an engine assembled without a client reaches this.
- `corrupt_parquet`: the read listed parquet objects confirmed corrupt, by
  this pass or by an earlier one whose verdict is still cached (#722).
- `scan_guard_violation`: the guarded per-file scan named objects that break
  the export schema invariant (#351).
- `read_failed`: any other parquet-side failure, including a manifest read
  that fails before DuckDB runs and a DuckDB statement or recount that fails.
- `postgres_read_failed`: a read in the pass's PostgreSQL snapshot failed,
  such as the dirty-ID fetch.
- `other`: a degradable failure that carries none of these classifications.

When one error carries two classifications, the more specific one wins: a
breaker rejection is also a `duckdb_unavailable` error, and a corrupt object
or a scan-guard violation is also a `read_failed` one. `cause` describes the
original failure even when the fallback fails: a fallback that meets the same
PostgreSQL outage as the pass counts as
`{cause="postgres_read_failed", outcome="failed"}`.

This counter covers what the other federated series cannot. The `fed_query_*`
series go flat during an outage, which reads the same as no traffic, and a
failure before DuckDB is consulted, such as a manifest read during an S3
outage, never counts toward the breaker. The breaker series counts
transitions, so a request the open breaker rejects adds nothing there and one
`breaker_rejected` attempt here. The two series never count the same event.
The counter does not follow the fallback's log line (requirements.md §10.2):
that line is written before the PostgreSQL read, at Debug for a breaker
rejection, while the counter is emitted after the read whatever the log
level. A request routed to PostgreSQL alone without a failure, a failure on a
deployment with the fallback off, and a failure that is never degraded (an
expired deadline or a manifest inconsistency, for example) add nothing.

`increase(federated_degraded_fallback_total{outcome="served"}[5m]) > 0`
alerts on degraded answers. A rising `outcome="failed"` means the fallback
itself is failing, usually because PostgreSQL is down too.

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
| `compaction_manifest_contract_violation_total` (counter, count, `schema_id`) | #890 | It counted V1 compaction passes whose `SaveManifest` succeeded without advancing the manifest's metadata. The V2 compactor publishes each generation with an exact compare-and-set on the schema's publication record, which either commits the generation or fails the pass, so the event it counted cannot occur and nothing emits it. |
| `fed_query_pushdown_efficiency` (gauge, ratio, `schema_id`) | #596 | It was the dirty-set size over the final matching row count, a stand-in for a Postgres-rows-scanned ratio Forma did not observe: the hot leg then ran inside DuckDB, which exposes the scans' row counts only through per-query profiling. Its numerator is still emitted as `fed_query_row_count{source="pg"}`. |

## The demo binaries

`cmd/server`, `cmd/lambda` and the `compactor` subcommand of `cmd/tools` are
reference entrypoints, so they get the simplest thing that makes every metric
visible without a dependency: an opt-in stdout emitter.

| Variable | Default | Meaning |
|----------|---------|---------|
| `METRICS_STDOUT` | unset (off) | `true` (or `1`/`t`, in any case) writes every emitted metric as one JSON line on stdout. A set value that is not a boolean fails startup (#642). |

Each line has a stable shape:

```json
{"type":"forma_metric","ts":"2026-09-19T06:00:00Z","name":"entity_report_only_validation_violation_total","kind":"counter","unit":"count","value":1,"labels":{"kind":"constraint","schema_id":"12","schema_name":"lead"}}
```

On Lambda the lines land in the function's log group like any other stdout.

`cmd/tools compactor` is the only producer of the four compactor metrics, so
it reads the same variable (#594): a cron- or job-driven run whose output
reaches a log group reports them with no collector. Every pass that loads the
schema's manifest emits at least `compaction_dirty_ratio`. A pass that finds
no manifest emits nothing, and neither does one that fails before the
manifest is loaded, so the gauge is not a heartbeat: a run that printed none
either had no manifest to read (exit code 0) or failed before reading it
(non-zero).

`compaction_rewrite_applied_total` counts committed dirty-ratio rewrites
only, passes that merged every base and delta file into a new base object,
and `compaction_rewrite_pending_total` counts passes whose dirty ratio called
for such a rewrite while the compactor had no merge wiring. A delta
promotion, which takes precedence when the delta tier reaches the target base
size, is counted by neither, committed or not. Since #890 a promotion also
merges into a new base object, because a V2 key names its tier, but the two
series keep the meaning their names promise; a pass reports a promotion
through its `promotion_applied` outcome and its `compaction published` log
line.

The subcommand's logs go to stderr, and so does a failed run's
`compactor: <error>` text (#643), so stdout carries the metric lines and
nothing else: on a failed run, just the metrics the pass emitted before it
failed. No other `cmd/tools`
subcommand has a metric to emit, and the emitter belongs to the one
`Compactor` the subcommand builds, so a subcommand that prints a document on
stdout (`inline-schema`) never shares it with a metric line.

There is no `/metrics` endpoint, no registry and no provider selection, and
unset means nothing new is written. The one way to misconfigure the variable is
a value that is not a boolean, such as `yes` or `on`. That value used to read
as false, so the operator who turned metrics on got none; since #642 it fails
the server's startup, the Lambda cold start or the compactor run, naming the
variable and the value. An operator who wants a real backend behind one of
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
