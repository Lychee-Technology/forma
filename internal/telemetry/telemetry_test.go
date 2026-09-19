package telemetry

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// recorder is the test emitter: it keeps every metric it is handed.
type recorder struct{ got []forma.Metric }

func (r *recorder) EmitMetric(_ context.Context, m forma.Metric) { r.got = append(r.got, m) }

// helperCall is one row of the contract table: how to invoke an Emit* helper
// and exactly what must come out of it.
type helperCall struct {
	helper string
	call   func(context.Context, *Sink)
	name   string
	labels map[string]string
	value  float64
}

var helperCalls = []helperCall{
	{"EmitLatency", func(ctx context.Context, s *Sink) { s.EmitLatency(ctx, "execution", 42) },
		"fed_query_latency_histogram", map[string]string{"stage": "execution"}, 42},
	{"EmitRowCount", func(ctx context.Context, s *Sink) { s.EmitRowCount(ctx, "duckdb", 7) },
		"fed_query_row_count", map[string]string{"source": "duckdb"}, 7},
	{"EmitPushdownEfficiency", func(ctx context.Context, s *Sink) { s.EmitPushdownEfficiency(ctx, 3, 0.25) },
		"fed_query_pushdown_efficiency", map[string]string{"schema_id": "3"}, 0.25},
	{"EmitCompactionManifestContractViolation", func(ctx context.Context, s *Sink) { s.EmitCompactionManifestContractViolation(ctx, 4) },
		"compaction_manifest_contract_violation_total", map[string]string{"schema_id": "4"}, 1},
	{"EmitCompactionDirtyRatio", func(ctx context.Context, s *Sink) { s.EmitCompactionDirtyRatio(ctx, 5, 0.5) },
		"compaction_dirty_ratio", map[string]string{"schema_id": "5"}, 0.5},
	{"EmitCompactionRewritePending", func(ctx context.Context, s *Sink) { s.EmitCompactionRewritePending(ctx, 6) },
		"compaction_rewrite_pending_total", map[string]string{"schema_id": "6"}, 1},
	{"EmitParquetChecksumMismatch", func(ctx context.Context, s *Sink) { s.EmitParquetChecksumMismatch(ctx, 7) },
		"parquet_checksum_mismatch_total", map[string]string{"schema_id": "7"}, 1},
	{"EmitCompactionRewriteApplied", func(ctx context.Context, s *Sink) { s.EmitCompactionRewriteApplied(ctx, 8) },
		"compaction_rewrite_applied_total", map[string]string{"schema_id": "8"}, 1},
	{"EmitReportOnlyValidationViolation", func(ctx context.Context, s *Sink) { s.EmitReportOnlyValidationViolation(ctx, 100, "lead", "required") },
		"entity_report_only_validation_violation_total", map[string]string{"schema_id": "100", "schema_name": "lead", "kind": "required"}, 1},
}

// TestEveryEmitHelperMatchesTheCatalogue is the metric contract: each helper
// emits exactly its catalogued name, kind, unit and label keys, with the
// value the caller passed, so an embedder mapping forma.MetricCatalogue onto
// a backend sees precisely what the descriptor promised.
func TestEveryEmitHelperMatchesTheCatalogue(t *testing.T) {
	covered := map[string]bool{}
	for _, tc := range helperCalls {
		t.Run(tc.helper, func(t *testing.T) {
			rec := &recorder{}
			tc.call(context.Background(), NewSink(rec))
			require.Len(t, rec.got, 1, "helper must emit exactly once")
			got := rec.got[0]
			desc, ok := forma.LookupMetric(tc.name)
			require.True(t, ok, "%s is not in forma.MetricCatalogue", tc.name)
			require.Equal(t, desc.Name, got.Name)
			require.Equal(t, desc.Kind, got.Kind)
			require.Equal(t, desc.Unit, got.Unit)
			require.Equal(t, tc.labels, got.Labels)
			require.True(t, desc.LabelKeysMatch(got.Labels), "labels %v do not match descriptor %v", got.Labels, desc.Labels)
			require.Equal(t, tc.value, got.Value)
			covered[tc.name] = true
		})
	}
	for _, d := range forma.MetricCatalogue() {
		require.True(t, covered[d.Name], "catalogued metric %s has no Emit* helper in the contract table", d.Name)
	}
}

// TestContractTableCoversEveryEmitHelper keeps the table above honest: adding
// an Emit* method to Sink without a row here fails, so a new metric cannot
// ship without its catalogue check.
func TestContractTableCoversEveryEmitHelper(t *testing.T) {
	inTable := map[string]bool{}
	for _, tc := range helperCalls {
		inTable[tc.helper] = true
	}
	sinkType := reflect.TypeOf((*Sink)(nil))
	for i := 0; i < sinkType.NumMethod(); i++ {
		name := sinkType.Method(i).Name
		if strings.HasPrefix(name, "Emit") {
			require.True(t, inTable[name], "Sink.%s has no row in helperCalls", name)
		}
	}
}

// TestNilSinkIsANoOp: the unconfigured default — a nil emitter, and therefore
// a nil *Sink — emits nothing and never dereferences.
func TestNilSinkIsANoOp(t *testing.T) {
	require.Nil(t, NewSink(nil))
	var s *Sink
	for _, tc := range helperCalls {
		tc.call(context.Background(), s)
	}
	(&Sink{}).EmitLatency(context.Background(), "execution", 1)
}

// TestOffContractEmissionIsDroppedNotPanicked: an emission that is not in
// the catalogue, or whose label keys differ from its descriptor, never
// reaches the emitter and never panics — a Forma bug must not become an
// application crash.
func TestOffContractEmissionIsDroppedNotPanicked(t *testing.T) {
	rec := &recorder{}
	s := NewSink(rec)
	s.emit(context.Background(), "not_a_metric", map[string]string{}, 1)
	s.emit(context.Background(), "fed_query_row_count", map[string]string{"stage": "x"}, 1)
	s.emit(context.Background(), "fed_query_row_count", map[string]string{"source": "pg", "extra": "y"}, 1)
	s.emit(context.Background(), "fed_query_row_count", nil, 1)
	require.Empty(t, rec.got)
}

// TestPanickingEmitterIsContained: the embedder's emitter throwing must not
// fail the Forma operation that emitted.
func TestPanickingEmitterIsContained(t *testing.T) {
	s := NewSink(forma.MetricEmitterFunc(func(context.Context, forma.Metric) { panic("emitter bug") }))
	require.NotPanics(t, func() { s.EmitRowCount(context.Background(), "pg", 1) })
}

// TestEmitReportOnlyValidationViolation pins the #317 counter's wire shape:
// the name a scraping deployment keys its dashboard on, and the three labels
// the rollout question ("is it safe to flip VALIDATE_UPDATES_STRICT for this
// schema yet") is asked over.
func TestEmitReportOnlyValidationViolation(t *testing.T) {
	rec := &recorder{}
	NewSink(rec).EmitReportOnlyValidationViolation(context.Background(), 100, "lead", "required")

	require.Len(t, rec.got, 1)
	require.Equal(t, forma.Metric{
		Name: "entity_report_only_validation_violation_total",
		Kind: forma.MetricKindCounter,
		Unit: forma.MetricUnitCount,
		Labels: map[string]string{
			"schema_id":   "100",
			"schema_name": "lead",
			"kind":        "required",
		},
		Value: 1,
	}, rec.got[0])
}
