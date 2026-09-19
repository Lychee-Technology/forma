package federated

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/stretchr/testify/require"
)

// metricRecorder keeps every metric the engine it was injected into emits.
type metricRecorder struct{ events []forma.Metric }

func (r *metricRecorder) EmitMetric(_ context.Context, m forma.Metric) {
	r.events = append(r.events, m)
}

func (r *metricRecorder) names() map[string]int {
	out := map[string]int{}
	for _, m := range r.events {
		out[m.Name+"/"+labelString(m.Labels)]++
	}
	return out
}

func labelString(labels map[string]string) string {
	for _, k := range []string{"stage", "source", "schema_id"} {
		if v, ok := labels[k]; ok {
			return k + "=" + v
		}
	}
	return ""
}

func newMetricsTestEngine(t *testing.T, opts ...EngineOption) (*DBFederatedQueryEngine, *fakeDuckDBExecutor) {
	t.Helper()
	duck := &fakeDuckDBExecutor{}
	e := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDirtyIDFetcher{}, duck, nil,
		hybridDuckConfig(), coldPlanCacheMetadata(t), "host=x",
		append([]EngineOption{
			WithPlanCache(queryplan.NewCache(64)),
			WithParquetSource(&fakeParquetSource{paths: []string{coldPlanCachePath}}),
		}, opts...)...)
	e.schemaValidator.markValidated(coldPlanCachePath, coldPlanCacheFooter(false), nil)
	return e, duck
}

// TestEngineEmitsQueryMetricsToTheInjectedEmitter pins the federated half of
// #423: one query through an engine built with WithMetricEmitter delivers the
// translation/execution/streaming latencies, the per-source row counts and
// the pushdown ratio to that emitter, each on its catalogued contract.
func TestEngineEmitsQueryMetricsToTheInjectedEmitter(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	rec := &metricRecorder{}
	e, duck := newMetricsTestEngine(t, WithMetricEmitter(rec))
	runColdPlanCacheQuery(t, e, duck)

	require.Equal(t, map[string]int{
		"fed_query_latency_histogram/stage=translation": 1,
		"fed_query_latency_histogram/stage=execution":   1,
		"fed_query_latency_histogram/stage=streaming":   1,
		"fed_query_row_count/source=pg":                 1,
		"fed_query_row_count/source=duckdb":             1,
		"fed_query_pushdown_efficiency/schema_id=0":     1,
	}, rec.names())
	for _, m := range rec.events {
		desc, ok := forma.LookupMetric(m.Name)
		require.True(t, ok, "%s is not catalogued", m.Name)
		require.Equal(t, desc.Kind, m.Kind)
		require.Equal(t, desc.Unit, m.Unit)
		require.True(t, desc.LabelKeysMatch(m.Labels))
	}
}

// TestEngineWithoutEmitterEmitsNothing: the default engine has no sink and the
// query path runs unchanged; an engine in the same process that does have one
// receives only its own emissions.
func TestEngineWithoutEmitterEmitsNothing(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	rec := &metricRecorder{}
	silent, silentDuck := newMetricsTestEngine(t)
	loud, loudDuck := newMetricsTestEngine(t, WithMetricEmitter(rec))

	runColdPlanCacheQuery(t, silent, silentDuck)
	require.Empty(t, rec.events, "an engine built without an emitter must not reach another engine's emitter")

	runColdPlanCacheQuery(t, loud, loudDuck)
	require.Len(t, rec.events, 6)
}

// TestEngineSurvivesAPanickingEmitter: the embedder's emitter throwing never
// fails the query.
func TestEngineSurvivesAPanickingEmitter(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	boom := forma.MetricEmitterFunc(func(context.Context, forma.Metric) { panic("emitter bug") })
	e, duck := newMetricsTestEngine(t, WithMetricEmitter(boom))
	require.NotPanics(t, func() { runColdPlanCacheQuery(t, e, duck) })
}
