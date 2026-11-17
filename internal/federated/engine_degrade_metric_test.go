package federated

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// The degraded fallback's counter, federated_degraded_fallback_total (#714).

const degradedFallbackMetric = "federated_degraded_fallback_total"

// degradedFallbackRecorder keeps every degraded-fallback sample and the
// breaker transitions an engine emits; other metrics are only counted.
type degradedFallbackRecorder struct {
	mu          sync.Mutex
	samples     []forma.Metric
	transitions []string
	other       int
}

func (r *degradedFallbackRecorder) EmitMetric(_ context.Context, m forma.Metric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch m.Name {
	case degradedFallbackMetric:
		r.samples = append(r.samples, m)
	case breakerTransitionMetric:
		r.transitions = append(r.transitions, m.Labels["state"])
	default:
		r.other++
	}
}

// take returns and clears the degraded-fallback samples recorded so far.
func (r *degradedFallbackRecorder) take() []forma.Metric {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.samples
	r.samples = nil
	return out
}

func recordDegradedFallbacks(engine *DBFederatedQueryEngine) *degradedFallbackRecorder {
	rec := &degradedFallbackRecorder{}
	WithMetricEmitter(rec)(engine)
	return rec
}

// requireOneDegradedSample asserts that exactly one sample was emitted, that
// it is a counter increment of 1 on the catalogued contract, and that it
// carries the expected labels, both drawn from the fixed enumerations.
func requireOneDegradedSample(t *testing.T, samples []forma.Metric, cause, outcome string) {
	t.Helper()
	require.Len(t, samples, 1, "one sample per fallback attempt; got %+v", samples)
	got := samples[0]
	desc, ok := forma.LookupMetric(degradedFallbackMetric)
	require.True(t, ok)
	require.Equal(t, desc.Kind, got.Kind)
	require.Equal(t, desc.Unit, got.Unit)
	require.Equal(t, float64(1), got.Value)
	require.Equal(t, map[string]string{"cause": cause, "outcome": outcome}, got.Labels)
	require.Contains(t, degradeCauses, got.Labels["cause"])
	require.Contains(t, degradeOutcomes, got.Labels["outcome"])
}

// TestDegradedFallbackMetricPerDegradePoint runs every point at which a
// DuckDB-routed request can degrade, including the failures before DuckDB is
// consulted that never reach the breaker. Each served fallback emits one
// sample of value 1 whose cause names the class of the absorbed failure, and
// no label carries the failure's text or an object key.
func TestDegradedFallbackMetricPerDegradePoint(t *testing.T) {
	for _, tc := range degradedPlanCases() {
		t.Run(tc.name, func(t *testing.T) {
			restore := initTestDescriptors()
			defer restore()

			engine, fq := tc.setup(t)
			rec := recordDegradedFallbacks(engine)
			opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: true}
			_, err := engine.Query(context.Background(), degradedLogTables(), fq, opts)
			require.NoError(t, err, "degraded mode must absorb the failure")

			samples := rec.take()
			requireOneDegradedSample(t, samples, tc.metricCause, degradeOutcomeServed)
			for _, v := range samples[0].Labels {
				require.NotContains(t, v, tc.cause, "a label must never carry the failure's text")
			}
		})
	}
}

// TestDegradedFallbackMetricOutcomeFollowsTheFallbackRead holds the absorbed
// cause fixed and varies only the PostgreSQL-only read. A served fallback and
// a failed one each emit exactly one sample, under the original cause, never
// both outcomes. The cause stays the absorbed failure's even when the
// fallback's own error is a PostgreSQL one, and the fallback line still
// records the absorbed failure when the request fails.
func TestDegradedFallbackMetricOutcomeFollowsTheFallbackRead(t *testing.T) {
	causes := []struct {
		name     string
		duckErr  error
		leg      *fakeHotLeg
		absorbed string
		class    string
	}{
		{name: "duckdb read failure", duckErr: fmt.Errorf("forced duck failure"),
			absorbed: "forced duck failure", class: degradeCauseReadFailed},
		{name: "dirty-ID fetch failure", leg: &fakeHotLeg{changeLogErr: fmt.Errorf("forced dirty-ID failure")},
			absorbed: "forced dirty-ID failure", class: degradeCausePostgresReadFailed},
	}
	for _, c := range causes {
		for _, fallbackFails := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fallback fails=%v", c.name, fallbackFails), func(t *testing.T) {
				restore := initTestDescriptors()
				defer restore()

				pg := &fakePostgresFederatedSource{}
				want := degradeOutcomeServed
				if fallbackFails {
					pg.err = fmt.Errorf("forced postgres failure")
					want = degradeOutcomeFailed
				}
				leg := c.leg
				if leg != nil {
					leg = &fakeHotLeg{changeLogErr: leg.changeLogErr}
				}
				engine := NewDBFederatedQueryEngine(pg, &fakeDuckDBExecutor{err: c.duckErr}, nil,
					hybridDuckConfig(), testMetadataCacheSchema7(t), withTestParquetPath(), withFakeHotLeg(leg))
				rec := recordDegradedFallbacks(engine)
				logs := withObservedLogs(engine)

				opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: true}
				_, err := engine.Query(context.Background(), degradedLogTables(), coldTierQuery(), opts)
				if fallbackFails {
					require.ErrorContains(t, err, "degraded postgres-only fallback")
					require.ErrorIs(t, err, ErrPostgresReadFailed)
				} else {
					require.NoError(t, err)
				}

				requireOneDegradedSample(t, rec.take(), c.class, want)
				entries := logs.FilterMessage(degradedFallbackMsg).All()
				require.Len(t, entries, 1)
				require.Contains(t, entries[0].ContextMap()["error"], c.absorbed)
			})
		}
	}
}

// TestNoDegradedFallbackMetricWithoutAFallback: only an actual fallback
// attempt counts. A healthy federated pass, a request routed to PostgreSQL
// alone, a degradable failure the caller did not opt to absorb, and a
// non-degradable failure under degraded mode all emit no sample.
func TestNoDegradedFallbackMetricWithoutAFallback(t *testing.T) {
	degraded := func() *model.FederatedQueryOptions {
		return &model.FederatedQueryOptions{AllowPartialDegradedMode: true}
	}
	cases := []struct {
		name    string
		run     func(t *testing.T, rec *degradedFallbackRecorder) error
		wantErr bool
	}{
		{name: "healthy federated pass", run: func(t *testing.T, rec *degradedFallbackRecorder) error {
			engine, duck := newMetricsTestEngine(t, WithMetricEmitter(rec))
			duck.rows = &singleDuckDBRow{rowID: uuid.New()}
			fq := &model.FederatedAttributeQuery{
				AttributeQuery: model.AttributeQuery{SchemaID: coldPlanCacheSchemaID, Limit: 2000,
					Condition: &forma.KvCondition{Attr: "score", Value: "gt:50"}},
				PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierCold},
			}
			_, err := engine.Query(context.Background(), degradedLogTables(), fq, degraded())
			require.Positive(t, rec.other, "the healthy pass's fed_query_* samples reach the emitter")
			return err
		}},
		{name: "postgres-only routing", run: func(t *testing.T, rec *degradedFallbackRecorder) error {
			engine := newSentinelTestEngine(t, &fakeDuckDBExecutor{err: fmt.Errorf("must not run")}, nil)
			WithMetricEmitter(rec)(engine)
			fq := &model.FederatedAttributeQuery{
				AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
				PreferredTiers: []model.DataTier{model.DataTierHot},
			}
			_, err := engine.Query(context.Background(), degradedLogTables(), fq, degraded())
			return err
		}},
		{name: "degradable failure without degraded mode", wantErr: true, run: func(t *testing.T, rec *degradedFallbackRecorder) error {
			engine := newSentinelTestEngine(t, &fakeDuckDBExecutor{err: fmt.Errorf("forced duck failure")}, nil)
			WithMetricEmitter(rec)(engine)
			_, err := engine.Query(context.Background(), degradedLogTables(), coldTierQuery(), nil)
			return err
		}},
		{name: "non-degradable failure under degraded mode", wantErr: true, run: func(t *testing.T, rec *degradedFallbackRecorder) error {
			src := &fakeParquetSource{pathsErr: fmt.Errorf("stamped for another schema: %w", forma.ErrManifestSchemaMismatch)}
			engine := newParquetSourceTestEngine(t, &fakeDuckDBExecutor{}, src)
			WithMetricEmitter(rec)(engine)
			_, err := engine.Query(context.Background(), degradedLogTables(), coldTierQuery(), degraded())
			require.ErrorIs(t, err, forma.ErrManifestSchemaMismatch)
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			restore := initTestDescriptors()
			defer restore()

			rec := &degradedFallbackRecorder{}
			err := tc.run(t, rec)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Empty(t, rec.take())
		})
	}
}

// TestDegradedFallbackMetricAcrossBreakerRejections places the counter beside
// the breaker's transition counter (#634). The failure that trips the breaker
// is one read_failed attempt and one "open" transition. Each request the open
// breaker then rejects is one more breaker_rejected attempt, and no further
// transition is emitted for it.
func TestDegradedFallbackMetricAcrossBreakerRejections(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &scriptedDuckDBExecutor{script: []func() (duckDBRowsIterator, error){
		func() (duckDBRowsIterator, error) { return nil, fmt.Errorf("forced execution failure") },
	}}
	engine := newBreakerTestEngine(t, duck, NewCircuitBreaker(1, time.Minute, time.Minute))
	rec := recordDegradedFallbacks(engine)
	fq, tables := breakerTestQuery()
	opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: true}

	_, err := engine.Query(context.Background(), tables, fq, opts)
	require.NoError(t, err)
	requireOneDegradedSample(t, rec.take(), degradeCauseReadFailed, degradeOutcomeServed)

	const rejected = 3
	for i := 0; i < rejected; i++ {
		_, err := engine.Query(context.Background(), tables, fq, opts)
		require.NoError(t, err)
		requireOneDegradedSample(t, rec.take(), degradeCauseBreakerRejected, degradeOutcomeServed)
	}
	require.Equal(t, []string{"open"}, rec.transitions, "rejections must not fabricate transitions")
	require.Equal(t, 1, duck.calls, "the rejected requests must not reach DuckDB")
}

// TestDegradedFallbackMetricIgnoresTheLogLevel: a breaker rejection's
// fallback line is Debug, which the production logger drops. The counter
// does not follow the log, so the rejection is still counted.
func TestDegradedFallbackMetricIgnoresTheLogLevel(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	breaker := NewCircuitBreaker(1, time.Minute, time.Minute)
	breaker.RecordFailure()
	engine := newSentinelTestEngine(t, &fakeDuckDBExecutor{}, breaker)
	core, logs := observer.New(zapcore.InfoLevel)
	WithLogger(zap.New(core))(engine)
	rec := recordDegradedFallbacks(engine)

	opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: true}
	_, err := engine.Query(context.Background(), degradedLogTables(), coldTierQuery(), opts)
	require.NoError(t, err)
	require.Zero(t, logs.FilterMessage(degradedFallbackMsg).Len(), "precondition: the line is below Info")
	requireOneDegradedSample(t, rec.take(), degradeCauseBreakerRejected, degradeOutcomeServed)
}

// TestDegradedFallbackMetricCountsAnUnclassifiedCause: a degradable failure
// that carries no sentinel is still counted, as "other", so every attempt
// lands in exactly one class.
func TestDegradedFallbackMetricCountsAnUnclassifiedCause(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	engine := newSentinelTestEngine(t, &fakeDuckDBExecutor{}, nil)
	rec := recordDegradedFallbacks(engine)
	opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: true}
	_, err := engine.degradeToPostgresOnly(context.Background(), degradedLogTables(), coldTierQuery(),
		opts, markExecutionPlan(opts), errors.New("unclassified failure"))
	require.NoError(t, err)
	requireOneDegradedSample(t, rec.take(), degradeCauseOther, degradeOutcomeServed)
}

// TestClassifyDegradeCause pins the precedence through wrapped errors: the
// specific class wins over the generic sentinel the same error also carries,
// and a class is decided by sentinels and types, never by message text.
func TestClassifyDegradeCause(t *testing.T) {
	readFailed := fmt.Errorf("duck.Query: %w: boom", ErrFederatedReadFailed)
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"breaker rejection over duckdb unavailable",
			fmt.Errorf("duckdb federated query: %w", fmt.Errorf("%w: %w", errBreakerRejected, ErrDuckDBUnavailable)),
			degradeCauseBreakerRejected},
		{"missing client", fmt.Errorf("duckdb client not available: %w", ErrDuckDBUnavailable),
			degradeCauseDuckDBUnavailable},
		{"corrupt object over read failed",
			fmt.Errorf("page pass: %w", &CorruptParquetError{SchemaID: 7, Paths: []string{"s3://b/x.parquet"}, cause: readFailed}),
			degradeCauseCorruptParquet},
		{"scan-guard violation over read failed",
			fmt.Errorf("page pass: %w", &ParquetGuardViolationError{SchemaID: 7, Paths: []string{"s3://b/x.parquet"}, cause: readFailed}),
			degradeCauseScanGuardViolation},
		{"read failed", fmt.Errorf("manifest parquet source: %w: s3 down", ErrFederatedReadFailed),
			degradeCauseReadFailed},
		{"postgres read failed", fmt.Errorf("read the dirty set: %w: refused", ErrPostgresReadFailed),
			degradeCausePostgresReadFailed},
		{"message text is not a class", errors.New("federated read failed: duckdb circuit breaker open"),
			degradeCauseOther},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, classifyDegradeCause(tc.err))
		})
	}
}

// TestDegradedFallbackLabelSetIsBounded: the counter's label values come from
// two fixed, duplicate-free enumerations, so its series are bounded by their
// product, and the descriptor declares exactly the two keys.
func TestDegradedFallbackLabelSetIsBounded(t *testing.T) {
	desc, ok := forma.LookupMetric(degradedFallbackMetric)
	require.True(t, ok)
	require.Equal(t, []string{"cause", "outcome"}, desc.Labels)
	require.Equal(t, forma.MetricKindCounter, desc.Kind)

	for _, set := range [][]string{degradeCauses, degradeOutcomes} {
		seen := map[string]bool{}
		for _, v := range set {
			require.False(t, seen[v], "duplicate label value %q", v)
			require.Equal(t, strings.ToLower(v), v)
			seen[v] = true
		}
	}
	require.Len(t, degradeCauses, 7)
	require.Len(t, degradeOutcomes, 2)
}
