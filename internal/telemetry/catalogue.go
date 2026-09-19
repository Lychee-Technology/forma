package telemetry

import "fmt"

// Kind is the metric type a Descriptor maps onto. It fixes the Go type of the
// value an Emit* helper passes through the hook, so an adapter can convert any
// emission onto its backend from the descriptor alone (#423):
//
//   - KindCounter:   int64, a monotonic increment (usually 1).
//   - KindGauge:     float64, the last observed value.
//   - KindHistogram: int64, one observation in the descriptor's Unit.
type Kind string

const (
	KindCounter   Kind = "counter"
	KindGauge     Kind = "gauge"
	KindHistogram Kind = "histogram"
)

// Coerce converts the value an Emit* helper passed to the number an adapter
// records, refusing any Go type the kind does not prescribe. A refusal is a
// programming error in the helper, not runtime data; TestEveryEmitHelperIsInTheCatalogue
// catches it before it ships.
func (k Kind) Coerce(value any) (float64, error) {
	switch k {
	case KindCounter, KindHistogram:
		v, ok := value.(int64)
		if !ok {
			return 0, fmt.Errorf("telemetry kind %s expects int64, got %T", k, value)
		}
		return float64(v), nil
	case KindGauge:
		v, ok := value.(float64)
		if !ok {
			return 0, fmt.Errorf("telemetry kind %s expects float64, got %T", k, value)
		}
		return v, nil
	default:
		return 0, fmt.Errorf("telemetry kind %q is unknown", string(k))
	}
}

// Unit names the measurement unit of a histogram or gauge.
type Unit string

const (
	UnitCount        Unit = "count"
	UnitRatio        Unit = "ratio"
	UnitMilliseconds Unit = "milliseconds"
)

// Descriptor is one catalogued metric: the wire name the Emit* helper emits,
// its Kind, Unit, the exact set of label keys every emission carries, and a
// help string for backends that publish one. Labels are ordered; adapters that
// need a stable dimension order (EMF, Prometheus label vectors) use this order.
type Descriptor struct {
	Name   string
	Kind   Kind
	Unit   Unit
	Labels []string
	Help   string
}

var (
	fedQueryLatency = Descriptor{
		Name:   "fed_query_latency_histogram",
		Kind:   KindHistogram,
		Unit:   UnitMilliseconds,
		Labels: []string{"stage"},
		Help:   "Federated query latency per stage (translation, execution, streaming), in milliseconds.",
	}
	fedQueryRowCount = Descriptor{
		Name:   "fed_query_row_count",
		Kind:   KindCounter,
		Unit:   UnitCount,
		Labels: []string{"source"},
		Help:   "Rows contributed to federated query results per source (pg, s3, duckdb).",
	}
	fedQueryPushdownEfficiency = Descriptor{
		Name:   "fed_query_pushdown_efficiency",
		Kind:   KindGauge,
		Unit:   UnitRatio,
		Labels: []string{"schema_id"},
		Help:   "Ratio of Postgres-scanned rows to final result rows for the last federated query; high means poor pushdown.",
	}
	compactionManifestContractViolation = Descriptor{
		Name:   "compaction_manifest_contract_violation_total",
		Kind:   KindCounter,
		Unit:   UnitCount,
		Labels: []string{"schema_id"},
		Help:   "Compaction passes where SaveManifest succeeded without advancing manifest metadata.",
	}
	compactionDirtyRatio = Descriptor{
		Name:   "compaction_dirty_ratio",
		Kind:   KindGauge,
		Unit:   UnitRatio,
		Labels: []string{"schema_id"},
		Help:   "Dirty ratio evaluated by the last compaction pass for the schema.",
	}
	compactionRewritePending = Descriptor{
		Name:   "compaction_rewrite_pending_total",
		Kind:   KindCounter,
		Unit:   UnitCount,
		Labels: []string{"schema_id"},
		Help:   "Compaction passes that needed a dirty-ratio rewrite but could not apply one yet.",
	}
	parquetChecksumMismatch = Descriptor{
		Name:   "parquet_checksum_mismatch_total",
		Kind:   KindCounter,
		Unit:   UnitCount,
		Labels: []string{"schema_id"},
		Help:   "Parquet objects whose bytes no longer hash to the checksum their manifest entry carries (#347).",
	}
	compactionRewriteApplied = Descriptor{
		Name:   "compaction_rewrite_applied_total",
		Kind:   KindCounter,
		Unit:   UnitCount,
		Labels: []string{"schema_id"},
		Help:   "Committed dirty-ratio rewrites (#188).",
	}
	reportOnlyValidationViolation = Descriptor{
		Name:   "entity_report_only_validation_violation_total",
		Kind:   KindCounter,
		Unit:   UnitCount,
		Labels: []string{"schema_id", "schema_name", "kind"},
		Help:   "Writes that violated their entity JSON schema and were accepted because strict update validation is off (#317).",
	}
)

// catalogue lists every metric the Emit* helpers can emit, in a stable order.
// Adding an Emit* helper means adding its descriptor here; the contract test
// fails otherwise.
var catalogue = []Descriptor{
	fedQueryLatency,
	fedQueryRowCount,
	fedQueryPushdownEfficiency,
	compactionManifestContractViolation,
	compactionDirtyRatio,
	compactionRewritePending,
	parquetChecksumMismatch,
	compactionRewriteApplied,
	reportOnlyValidationViolation,
}

// Catalogue returns a copy of every descriptor, in declaration order.
func Catalogue() []Descriptor {
	out := make([]Descriptor, len(catalogue))
	copy(out, catalogue)
	return out
}

// Lookup returns the descriptor for a wire name.
func Lookup(name string) (Descriptor, bool) {
	for _, d := range catalogue {
		if d.Name == name {
			return d, true
		}
	}
	return Descriptor{}, false
}

// LabelValues arranges an emission's labels in the descriptor's order and
// refuses a label set that is not exactly the descriptor's, so an adapter can
// feed a positional label vector or dimension set without guessing.
func (d Descriptor) LabelValues(labels map[string]string) ([]string, error) {
	if len(labels) != len(d.Labels) {
		return nil, fmt.Errorf("metric %s expects labels %v, got %d labels", d.Name, d.Labels, len(labels))
	}
	values := make([]string, len(d.Labels))
	for i, key := range d.Labels {
		v, ok := labels[key]
		if !ok {
			return nil, fmt.Errorf("metric %s expects label %q, which is missing", d.Name, key)
		}
		values[i] = v
	}
	return values, nil
}
