package federated

import (
	"context"
	"fmt"
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

const breakerTransitionMetric = "duckdb_circuit_breaker_transition_total"

// breakerReportRecorder keeps the breaker transition samples an engine emits
// and counts every emission or log line that ran while the breaker's mutex
// was held. TryLock never blocks, so a report made under the lock is counted
// rather than deadlocking the test.
type breakerReportRecorder struct {
	breaker *CircuitBreaker

	mu          sync.Mutex
	states      []string
	underLock   int
	otherMetric int
}

func (r *breakerReportRecorder) EmitMetric(_ context.Context, m forma.Metric) {
	r.checkLockFree()
	r.mu.Lock()
	defer r.mu.Unlock()
	if m.Name != breakerTransitionMetric {
		r.otherMetric++
		return
	}
	r.states = append(r.states, m.Labels["state"])
}

func (r *breakerReportRecorder) checkLockFree() {
	if r.breaker.mu.TryLock() {
		r.breaker.mu.Unlock()
		return
	}
	r.mu.Lock()
	r.underLock++
	r.mu.Unlock()
}

// take returns the transition states emitted since the last take.
func (r *breakerReportRecorder) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.states
	r.states = nil
	return out
}

// newBreakerReportEngine wires an engine whose metric emitter and logger
// both record, and whose log hook checks the breaker's mutex like the
// emitter does.
func newBreakerReportEngine(t *testing.T, duck DuckDBQueryExecutor, breaker *CircuitBreaker, rec *breakerReportRecorder) (*DBFederatedQueryEngine, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	logger := zap.New(core, zap.Hooks(func(zapcore.Entry) error {
		rec.checkLockFree()
		return nil
	}))
	engine := newBreakerTestEngine(t, duck, breaker)
	WithMetricEmitter(rec)(engine)
	WithLogger(logger)(engine)
	return engine, logs
}

// TestBreakerTransitionsReportedThroughQuery walks a breaker through a full
// trip-and-recovery cycle via the production Query path (#634): every step
// asserts exactly which transition samples and log lines it produced, and
// that none of them ran under the breaker's mutex.
func TestBreakerTransitionsReportedThroughQuery(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	failure := func() (duckDBRowsIterator, error) { return nil, fmt.Errorf("forced execution failure") }
	success := func() (duckDBRowsIterator, error) { return &singleDuckDBRow{rowID: uuid.New()}, nil }
	duck := &scriptedDuckDBExecutor{script: []func() (duckDBRowsIterator, error){
		failure, failure, // trip a threshold-2 breaker
		failure,          // the first probe fails
		success, success, // the second probe closes; closed traffic follows
	}}
	breaker := NewCircuitBreaker(2, time.Minute, time.Minute)
	rec := &breakerReportRecorder{breaker: breaker}
	engine, logs := newBreakerReportEngine(t, duck, breaker, rec)
	fq, tables := breakerTestQuery()

	type logLine struct {
		level zapcore.Level
		msg   string
		from  string
	}
	steps := []struct {
		name    string
		before  func(*CircuitBreaker)
		wantErr string
		states  []string
		lines   []logLine
	}{
		{name: "failure below threshold", wantErr: "forced execution failure"},
		{name: "failure trips", wantErr: "forced execution failure", states: []string{"open"},
			lines: []logLine{{zapcore.WarnLevel, "duckdb circuit breaker opened", "closed"}}},
		{name: "open breaker rejects", wantErr: "circuit breaker open"},
		{name: "probe admitted and fails", before: elapseOpenPeriod, wantErr: "forced execution failure",
			states: []string{"half_open", "open"},
			lines: []logLine{
				{zapcore.InfoLevel, "duckdb circuit breaker half-open: probe admitted", ""},
				{zapcore.WarnLevel, "duckdb circuit breaker opened", "half_open"},
			}},
		{name: "re-opened breaker rejects", wantErr: "circuit breaker open"},
		{name: "probe admitted and succeeds", before: elapseOpenPeriod, states: []string{"half_open", "closed"},
			lines: []logLine{
				{zapcore.InfoLevel, "duckdb circuit breaker half-open: probe admitted", ""},
				{zapcore.InfoLevel, "duckdb circuit breaker closed", "half_open"},
			}},
		{name: "closed traffic reports nothing"},
	}
	for _, step := range steps {
		if step.before != nil {
			step.before(breaker)
		}
		_, err := engine.Query(context.Background(), tables, fq, nil)
		if step.wantErr == "" {
			require.NoError(t, err, step.name)
		} else {
			require.ErrorContains(t, err, step.wantErr, step.name)
		}
		require.Equal(t, step.states, rec.take(), "%s: transition samples", step.name)

		entries := logs.TakeAll()
		require.Len(t, entries, len(step.lines), "%s: log lines %v", step.name, entries)
		for i, want := range step.lines {
			got := entries[i]
			require.Equal(t, want.level, got.Level, "%s: line %d", step.name, i)
			require.Equal(t, want.msg, got.Message, "%s: line %d", step.name, i)
			fields := got.ContextMap()
			if want.from != "" {
				require.Equal(t, want.from, fields["from"], "%s: line %d from", step.name, i)
			}
			if want.level == zapcore.WarnLevel {
				require.Contains(t, fields["error"], "forced execution failure",
					"%s: the opened line carries the failing pass's error", step.name)
			}
		}
	}
	require.Zero(t, rec.underLock, "a transition was emitted or logged while the breaker's mutex was held")
	require.Positive(t, rec.otherMetric, "the successful passes' fed_query_* series reach the same emitter")
}

// TestBreakerCorruptRetryReportsHalfOpenOnce pins the #251 interaction: the
// probe pass hits confirmed corruption, hands its slot back through
// ReleaseProbe, and the retry re-reserves it inside the same request. The
// breaker was already reported half-open, so the request reports half_open
// once, then closed.
func TestBreakerCorruptRetryReportsHalfOpenOnce(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &retryFakeDuck{passes: []retryPass{
		{midStreamFail: true, drainFails: []string{retryCorruptPath1}},
	}}
	breaker := NewCircuitBreaker(1, time.Minute, time.Minute)
	rec := &breakerReportRecorder{breaker: breaker}
	e := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDirtyIDFetcher{}, duck, breaker,
		hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x",
		WithParquetSource(&fakeParquetSource{paths: []string{retryKeptPathA, retryCorruptPath1}}),
		WithMetricEmitter(rec))

	require.Equal(t, BreakerTransition{BreakerClosed, BreakerOpen}, breaker.RecordFailure())
	elapseOpenPeriod(breaker)

	_, err := e.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), nil)
	require.NoError(t, err)
	require.Len(t, duck.mainSQL, 2, "the failed probe pass plus exactly one admitted retry")
	require.Equal(t, []string{"half_open", "closed"}, rec.take(),
		"release-and-retry must not report half_open twice")
	require.Zero(t, rec.underLock)
}
