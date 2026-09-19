package forma

import "context"

// MetricKind is the semantic of a Metric's Value, and therefore the instrument
// an emitter maps it onto.
type MetricKind string

const (
	// MetricKindCounter: Value is a monotonic increment (usually 1). Map onto
	// a counter's Add.
	MetricKindCounter MetricKind = "counter"
	// MetricKindGauge: Value is the last observed value. Map onto a gauge's Set.
	MetricKindGauge MetricKind = "gauge"
	// MetricKindHistogram: Value is one observation in the metric's Unit. Map
	// onto a histogram's Observe (or a summary, a timer, ...).
	MetricKindHistogram MetricKind = "histogram"
)

// MetricUnit names the unit a Metric's Value is measured in.
type MetricUnit string

const (
	MetricUnitCount        MetricUnit = "count"
	MetricUnitRatio        MetricUnit = "ratio"
	MetricUnitMilliseconds MetricUnit = "milliseconds"
)

// Metric is one telemetry event Forma hands to the MetricEmitter an embedder
// configured (#423). Every field is Forma-owned: an emitter adapts it onto
// whatever backend the application uses (OpenTelemetry, Prometheus,
// CloudWatch, StatsD, a log line, nothing) and Forma never chooses one.
//
// Name, Kind, Unit and the set of Labels keys are fixed per metric and listed
// by MetricCatalogue; an emission never carries a key its descriptor does not
// declare. Labels is a fresh map per emission, so an emitter may retain or
// mutate it. Value is a float64 for every kind because that is what every
// backend accepts; counter and histogram values are integral in practice.
type Metric struct {
	Name   string
	Kind   MetricKind
	Unit   MetricUnit
	Labels map[string]string
	Value  float64
}

// MetricEmitter receives every Metric a Forma instance emits. Set one on
// Config.Metrics.Emitter (or with WithMetricEmitter); the default, nil, emits
// nothing. Forma calls EmitMetric synchronously on the goroutine doing the
// work, so an implementation must be cheap and non-blocking, and must be safe
// for concurrent use. A panic inside EmitMetric is recovered and logged; it
// never fails the write, query or compaction pass that emitted.
type MetricEmitter interface {
	EmitMetric(ctx context.Context, m Metric)
}

// MetricEmitterFunc adapts a function to MetricEmitter.
type MetricEmitterFunc func(ctx context.Context, m Metric)

// EmitMetric calls f.
func (f MetricEmitterFunc) EmitMetric(ctx context.Context, m Metric) { f(ctx, m) }

// MetricDescriptor is the stable contract for one metric: what MetricCatalogue
// promises about every Metric emitted under Name.
//
// Labels is the exact, ordered set of label keys every emission carries.
// Every label is bounded-cardinality by construction — a schema id or name,
// a fixed enumeration such as a stage or a violation kind — so an emitter may
// pre-register one instrument per descriptor and one series per label
// combination without a cardinality guard of its own. Order matters to
// backends with positional label vectors or dimension sets.
type MetricDescriptor struct {
	Name   string
	Kind   MetricKind
	Unit   MetricUnit
	Labels []string
	Help   string
}

// metricCatalogue lists every metric Forma emits, in a stable order. Names
// are wire names: dashboards key on them verbatim, so they are never renamed
// and never prefixed. Adding a metric means adding its descriptor here and
// the helper in internal/telemetry that emits it; the telemetry package's
// contract test refuses a helper whose emission does not match its
// descriptor.
var metricCatalogue = []MetricDescriptor{
	{
		Name:   "fed_query_latency_histogram",
		Kind:   MetricKindHistogram,
		Unit:   MetricUnitMilliseconds,
		Labels: []string{"stage"},
		Help:   "Federated query latency per stage, in milliseconds: translation is SQL rendering, execution is the DuckDB query call, streaming is the row iteration and handler loop.",
	},
	{
		Name:   "fed_query_row_count",
		Kind:   MetricKindCounter,
		Unit:   MetricUnitCount,
		Labels: []string{"source"},
		Help:   "Rows handled by one successful federated query pass, per source: pg is the size of the dirty set fetched from the Postgres change_log for the anti-join (the hot rows that override their S3 copies), duckdb is the row count returned by the merged DuckDB scan.",
	},
	{
		Name:   "fed_query_pushdown_efficiency",
		Kind:   MetricKindGauge,
		Unit:   MetricUnitRatio,
		Labels: []string{"schema_id"},
		Help:   "Hot-tier dirty-set size over final matching rows for the last federated query, per schema. A proxy for Postgres pushdown cost: Forma does not observe the postgres_scan row count, so the anti-join dirty set (the upper bound of hot rows the scan can return) stands in for it. High means the hot tier is large relative to what the query returns.",
	},
	{
		Name:   "compaction_manifest_contract_violation_total",
		Kind:   MetricKindCounter,
		Unit:   MetricUnitCount,
		Labels: []string{"schema_id"},
		Help:   "Compaction passes where SaveManifest succeeded without advancing manifest metadata.",
	},
	{
		Name:   "compaction_dirty_ratio",
		Kind:   MetricKindGauge,
		Unit:   MetricUnitRatio,
		Labels: []string{"schema_id"},
		Help:   "Dirty ratio evaluated by the last compaction pass for the schema.",
	},
	{
		Name:   "compaction_rewrite_pending_total",
		Kind:   MetricKindCounter,
		Unit:   MetricUnitCount,
		Labels: []string{"schema_id"},
		Help:   "Compaction passes that needed a dirty-ratio rewrite but could not apply one yet.",
	},
	{
		Name:   "parquet_checksum_mismatch_total",
		Kind:   MetricKindCounter,
		Unit:   MetricUnitCount,
		Labels: []string{"schema_id"},
		Help:   "Parquet objects whose bytes no longer hash to the checksum their manifest entry carries (#347).",
	},
	{
		Name:   "compaction_rewrite_applied_total",
		Kind:   MetricKindCounter,
		Unit:   MetricUnitCount,
		Labels: []string{"schema_id"},
		Help:   "Committed dirty-ratio rewrites (#188).",
	},
	{
		Name:   "entity_report_only_validation_violation_total",
		Kind:   MetricKindCounter,
		Unit:   MetricUnitCount,
		Labels: []string{"schema_id", "schema_name", "kind"},
		Help:   "Writes that violated their entity JSON schema and were accepted because strict update validation is off (#317).",
	},
}

// MetricCatalogue returns a copy of every metric descriptor, in declaration
// order. An emitter that pre-registers instruments (a Prometheus vector needs
// its label names up front, for example) builds them from this.
func MetricCatalogue() []MetricDescriptor {
	out := make([]MetricDescriptor, len(metricCatalogue))
	for i, d := range metricCatalogue {
		out[i] = d
		out[i].Labels = append([]string(nil), d.Labels...)
	}
	return out
}

// LookupMetric returns the descriptor for a wire name.
func LookupMetric(name string) (MetricDescriptor, bool) {
	for _, d := range metricCatalogue {
		if d.Name == name {
			d.Labels = append([]string(nil), d.Labels...)
			return d, true
		}
	}
	return MetricDescriptor{}, false
}

// LabelKeysMatch reports whether labels carries exactly the descriptor's keys.
func (d MetricDescriptor) LabelKeysMatch(labels map[string]string) bool {
	if len(labels) != len(d.Labels) {
		return false
	}
	for _, key := range d.Labels {
		if _, ok := labels[key]; !ok {
			return false
		}
	}
	return true
}
