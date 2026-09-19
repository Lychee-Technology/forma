package federated

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
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

// value returns the single emission under name+label, failing on zero or
// several so a test never asserts against a stale or ambiguous sample.
func (r *metricRecorder) value(t *testing.T, name, label string) float64 {
	t.Helper()
	var found []float64
	for _, m := range r.events {
		if m.Name == name && labelString(m.Labels) == label {
			found = append(found, m.Value)
		}
	}
	require.Len(t, found, 1, "%s/%s", name, label)
	return found[0]
}

func labelString(labels map[string]string) string {
	for _, k := range []string{"stage", "source", "schema_id"} {
		if v, ok := labels[k]; ok {
			return k + "=" + v
		}
	}
	return ""
}

// everyFedQuerySeries is the label set one successful DuckDB pass must
// produce, keyed on the queried schema.
var everyFedQuerySeries = map[string]int{
	"fed_query_latency_histogram/stage=translation":                                  1,
	"fed_query_latency_histogram/stage=execution":                                    1,
	"fed_query_latency_histogram/stage=streaming":                                    1,
	"fed_query_row_count/source=pg":                                                  1,
	"fed_query_row_count/source=duckdb":                                              1,
	fmt.Sprintf("fed_query_pushdown_efficiency/schema_id=%d", coldPlanCacheSchemaID): 1,
}

func newMetricsTestEngine(t *testing.T, opts ...EngineOption) (*DBFederatedQueryEngine, *fakeDuckDBExecutor) {
	t.Helper()
	duck := &fakeDuckDBExecutor{}
	return buildMetricsTestEngine(t, &fakeDirtyIDFetcher{}, duck, opts...), duck
}

// buildMetricsTestEngine assembles the metric-test engine over caller-owned
// dirty-id and DuckDB fakes; duck may be wrapped (see clockedExecutor) as
// long as the wrapped fake is the one handed here for row scripting.
func buildMetricsTestEngine(t *testing.T, dirty *fakeDirtyIDFetcher, duck DuckDBQueryExecutor, opts ...EngineOption) *DBFederatedQueryEngine {
	t.Helper()
	e := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, dirty, duck, nil,
		hybridDuckConfig(), coldPlanCacheMetadata(t), "host=x",
		append([]EngineOption{
			WithPlanCache(queryplan.NewCache(64)),
			WithParquetSource(&fakeParquetSource{paths: []string{coldPlanCachePath}}),
		}, opts...)...)
	e.schemaValidator.markValidated(coldPlanCachePath, coldPlanCacheFooter(false), nil)
	return e
}

func requireCataloguedEmissions(t *testing.T, rec *metricRecorder) {
	t.Helper()
	for _, m := range rec.events {
		desc, ok := forma.LookupMetric(m.Name)
		require.True(t, ok, "%s is not catalogued", m.Name)
		require.Equal(t, desc.Kind, m.Kind)
		require.Equal(t, desc.Unit, m.Unit)
		require.True(t, desc.LabelKeysMatch(m.Labels))
	}
}

// TestEngineEmitsQueryMetricsToTheInjectedEmitter pins the federated half of
// #423: one query through an engine built with WithMetricEmitter delivers the
// translation/execution/streaming latencies, the per-source row counts and
// the pushdown ratio to that emitter, each on its catalogued contract and
// labelled with the queried schema, not a placeholder.
func TestEngineEmitsQueryMetricsToTheInjectedEmitter(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	rec := &metricRecorder{}
	e, duck := newMetricsTestEngine(t, WithMetricEmitter(rec))
	runColdPlanCacheQuery(t, e, duck)

	require.Equal(t, everyFedQuerySeries, rec.names())
	requireCataloguedEmissions(t, rec)
}

// TestEngineEmitsQueryMetricsWithoutAnExecutionPlan: the API default is
// IncludeExecutionPlan=false (and Go callers may pass nil options), and the
// metric stream must be identical to the plan-requested one. Before PR #595
// review item 1, four of the six series were only emitted when a caller
// opted into the diagnostic plan payload.
func TestEngineEmitsQueryMetricsWithoutAnExecutionPlan(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	for name, opts := range map[string]*model.FederatedQueryOptions{
		"plan not requested": {IncludeExecutionPlan: false},
		"nil options":        nil,
	} {
		t.Run(name, func(t *testing.T) {
			rec := &metricRecorder{}
			e, duck := newMetricsTestEngine(t, WithMetricEmitter(rec))
			runColdPlanCacheQueryWith(t, e, duck, "", opts)

			require.Equal(t, everyFedQuerySeries, rec.names())
			requireCataloguedEmissions(t, rec)
			if opts != nil {
				require.Nil(t, opts.ExecutionPlan, "metrics must not force a plan the caller did not ask for")
			}
		})
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

// fakeClock is the engine's timing source in the stage tests: the wrapped
// executor and row iterator advance it by fixed costs, so the asserted stage
// values are exact instead of sleep-and-hope.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }
func (c *fakeClock) engineOption() EngineOption {
	return func(e *DBFederatedQueryEngine) { e.now = c.now }
}

// clockedExecutor charges queryCost to the clock for the main scan's Query
// call and rowCost for its first Next, leaving the schema probes untouched.
type clockedExecutor struct {
	inner              *fakeDuckDBExecutor
	clock              *fakeClock
	queryCost, rowCost time.Duration
}

func (x *clockedExecutor) Query(ctx context.Context, sql string, args ...any) (duckDBRowsIterator, error) {
	before := x.inner.calls
	rows, err := x.inner.Query(ctx, sql, args...)
	if err != nil || x.inner.calls == before {
		return rows, err
	}
	x.clock.advance(x.queryCost)
	return &clockedRows{duckDBRowsIterator: rows, clock: x.clock, cost: x.rowCost}, nil
}

type clockedRows struct {
	duckDBRowsIterator
	clock   *fakeClock
	cost    time.Duration
	charged bool
}

func (r *clockedRows) Next() bool {
	if !r.charged {
		r.charged = true
		r.clock.advance(r.cost)
	}
	return r.duckDBRowsIterator.Next()
}

// TestEngineLatencyStagesAreDisjoint pins PR #595 review item 3: execution
// is the duck.Query call alone and streaming is the row loop alone. Under
// the previous measure execution absorbed the whole fetch and streaming was
// the rounding remainder, so the histogram could not say which was slow.
// The execution plan's duckdb_fetch stays the sum, so plan readers are
// unaffected by the split.
func TestEngineLatencyStagesAreDisjoint(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	rec := &metricRecorder{}
	duck := &fakeDuckDBExecutor{}
	e := buildMetricsTestEngine(t, &fakeDirtyIDFetcher{},
		&clockedExecutor{inner: duck, clock: clock, queryCost: 40 * time.Millisecond, rowCost: 25 * time.Millisecond},
		WithMetricEmitter(rec), clock.engineOption())

	opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true,
		ExecutionPlan: &model.ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}}}
	runColdPlanCacheQueryWith(t, e, duck, "", opts)

	require.Equal(t, 40.0, rec.value(t, "fed_query_latency_histogram", "stage=execution"))
	require.Equal(t, 25.0, rec.value(t, "fed_query_latency_histogram", "stage=streaming"))
	require.Equal(t, int64(65), opts.ExecutionPlan.Timings["duckdb_fetch"])
	require.Equal(t, int64(65), opts.ExecutionPlan.Timings["total"])
	last := opts.ExecutionPlan.Sources[len(opts.ExecutionPlan.Sources)-1]
	require.Equal(t, "duckdb", last.Engine)
	require.Equal(t, int64(65), last.DurationMs)
	require.Equal(t, int64(1), last.ActualRows)
}

// TestEnginePushdownEfficiencyIsTheDirtySetOverTheResult pins PR #595 review
// items 2 and 4 at the engine seam: the gauge carries the queried schema and
// its value is the anti-join dirty-set size (3 here) over the query's total
// match count (1, the window count the single fake row reports), no longer
// read back out of execution-plan sources.
func TestEnginePushdownEfficiencyIsTheDirtySetOverTheResult(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	rec := &metricRecorder{}
	duck := &fakeDuckDBExecutor{}
	dirty := &fakeDirtyIDFetcher{ids: []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}}
	e := buildMetricsTestEngine(t, dirty, duck, WithMetricEmitter(rec))

	opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true,
		ExecutionPlan: &model.ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}}}
	runColdPlanCacheQueryWith(t, e, duck, "", opts)

	label := fmt.Sprintf("schema_id=%d", coldPlanCacheSchemaID)
	require.Equal(t, 3.0, rec.value(t, "fed_query_pushdown_efficiency", label))
	require.Equal(t, 3.0, rec.value(t, "fed_query_row_count", "source=pg"))
	require.Contains(t, opts.ExecutionPlan.Notes, "pushdown_efficiency=3.000 (dirty_rows=3 final_rows=1)")
}

// TestPushdownEfficiencyDenominatorFallbacks: the total match count is the
// denominator; the streamed page stands in when the template reported none,
// and 1 when nothing matched, so an empty result is never a division by zero.
func TestPushdownEfficiencyDenominatorFallbacks(t *testing.T) {
	for name, tc := range map[string]struct {
		outcome   duckDBScanOutcome
		ratio     float64
		finalRows int64
	}{
		"total reported":       {duckDBScanOutcome{dirtyRows: 6, rowCount: 2, totalRecords: 3}, 2, 3},
		"page only":            {duckDBScanOutcome{dirtyRows: 6, rowCount: 2}, 3, 2},
		"empty result":         {duckDBScanOutcome{dirtyRows: 6}, 6, 1},
		"empty dirty set":      {duckDBScanOutcome{rowCount: 4, totalRecords: 4}, 0, 4},
		"nothing at all":       {duckDBScanOutcome{}, 0, 1},
		"negative total guard": {duckDBScanOutcome{dirtyRows: 1, rowCount: 5, totalRecords: -1}, 0.2, 5},
	} {
		t.Run(name, func(t *testing.T) {
			ratio, finalRows := pushdownEfficiency(tc.outcome)
			require.InDelta(t, tc.ratio, ratio, 1e-9)
			require.Equal(t, tc.finalRows, finalRows)
		})
	}
}
