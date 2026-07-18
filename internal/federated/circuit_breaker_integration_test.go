package federated

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"text/template"
	"time"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

// #185 breaker scenario 5: both failure classes — query execution errors and
// row-stream errors — must increment the breaker through the production
// StreamDuckDBFederatedQuery path, and a clean stream must reset it. The
// engine-internal seams (fakeDuckDBExecutor / iterators) are the injection
// surface; a deterministic mid-stream infrastructure failure cannot be
// staged from real containers.

// failingScanRows yields rows whose Scan always fails, driving the
// streamDuckDBRows error path.
type failingScanRows struct{}

func (f *failingScanRows) Next() bool             { return true }
func (f *failingScanRows) Scan(dest ...any) error { return fmt.Errorf("forced scan failure") }
func (f *failingScanRows) Err() error             { return nil }
func (f *failingScanRows) Close() error           { return nil }

// scriptedDuckDBExecutor plays one prepared outcome per Query call and
// repeats the last one when the script runs out.
type scriptedDuckDBExecutor struct {
	calls  int
	script []func() (duckDBRowsIterator, error)
}

func (s *scriptedDuckDBExecutor) Query(ctx context.Context, sql string, args ...any) (duckDBRowsIterator, error) {
	idx := s.calls
	s.calls++
	if idx >= len(s.script) {
		idx = len(s.script) - 1
	}
	return s.script[idx]()
}

func newBreakerTestEngine(t *testing.T, duck DuckDBQueryExecutor, breaker *CircuitBreaker) *DBFederatedQueryEngine {
	t.Helper()
	engine := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDirtyIDFetcher{}, duck, breaker,
		forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}},
		testMetadataCacheSchema7(t), "")
	engine.buildDuckSQL = func(tpl *template.Template, params any, q *model.FederatedAttributeQuery, dirtyIDs []uuid.UUID, dual *sqlgen.DualClauses) (string, []any, error) {
		return "SELECT fake", nil, nil
	}
	return engine
}

func breakerTestQuery() (*model.FederatedAttributeQuery, model.StorageTables) {
	return &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 2000},
		PreferredTiers: []model.DataTier{model.DataTierWarm, model.DataTierCold},
	}, model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"}
}

func TestBreakerRecordsExecutionFailures(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	breaker := NewCircuitBreaker(2, time.Minute, time.Minute)
	duck := &fakeDuckDBExecutor{err: fmt.Errorf("forced execution failure")}
	engine := newBreakerTestEngine(t, duck, breaker)
	fq, tables := breakerTestQuery()

	for i := 0; i < 2; i++ {
		require.False(t, breaker.IsOpen(), "breaker open before threshold at failure %d", i+1)
		_, err := engine.Query(context.Background(), tables, fq, nil)
		require.ErrorContains(t, err, "execute duckdb query")
	}
	require.True(t, breaker.IsOpen(), "two execution failures must open a threshold-2 breaker")

	// The open breaker rejects before DuckDB: the executor is not reached.
	calls := duck.calls
	_, err := engine.Query(context.Background(), tables, fq, nil)
	require.ErrorContains(t, err, "circuit breaker open")
	require.Equal(t, calls, duck.calls, "open breaker must not reach the executor")
}

func TestBreakerRecordsStreamFailures(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	breaker := NewCircuitBreaker(2, time.Minute, time.Minute)
	duck := &fakeDuckDBExecutor{rows: &failingScanRows{}}
	engine := newBreakerTestEngine(t, duck, breaker)
	fq, tables := breakerTestQuery()

	for i := 0; i < 2; i++ {
		require.False(t, breaker.IsOpen(), "breaker open before threshold at failure %d", i+1)
		_, err := engine.Query(context.Background(), tables, fq, nil)
		require.ErrorContains(t, err, "scan duckdb row")
	}
	require.True(t, breaker.IsOpen(), "two row-stream failures must open a threshold-2 breaker")
}

func TestBreakerSuccessResetsFailureHistory(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	breaker := NewCircuitBreaker(2, time.Minute, time.Minute)
	duck := &scriptedDuckDBExecutor{script: []func() (duckDBRowsIterator, error){
		func() (duckDBRowsIterator, error) { return nil, fmt.Errorf("forced execution failure") },
		func() (duckDBRowsIterator, error) { return &singleDuckDBRow{rowID: uuid.New()}, nil },
		func() (duckDBRowsIterator, error) { return nil, fmt.Errorf("forced execution failure") },
	}}
	engine := newBreakerTestEngine(t, duck, breaker)
	fq, tables := breakerTestQuery()

	_, err := engine.Query(context.Background(), tables, fq, nil)
	require.Error(t, err)

	_, err = engine.Query(context.Background(), tables, fq, nil)
	require.NoError(t, err, "clean stream must succeed and reset the breaker")

	// One failure after the reset stays below the threshold of 2: the
	// success cleared the prior failure from the history.
	_, err = engine.Query(context.Background(), tables, fq, nil)
	require.Error(t, err)
	require.False(t, breaker.IsOpen(), "success must have cleared the failure history")
}

// blockingDuckDBExecutor blocks each Query until released, letting a test
// hold the half-open probe in flight while other callers race the breaker.
type blockingDuckDBExecutor struct {
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (b *blockingDuckDBExecutor) Query(ctx context.Context, sql string, args ...any) (duckDBRowsIterator, error) {
	b.calls.Add(1)
	select {
	case b.entered <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-b.release:
		return &singleDuckDBRow{rowID: uuid.New()}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// #246: through the real Query path, the half-open probe executes DuckDB
// while a concurrent caller is rejected without reaching the executor —
// mirroring the open-breaker short-circuit call-count assertion above.
func TestBreakerHalfOpenSingleProbeThroughQueryPath(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	breaker := NewCircuitBreaker(1, time.Minute, 50*time.Millisecond)
	duck := &blockingDuckDBExecutor{entered: make(chan struct{}, 1), release: make(chan struct{})}
	engine := newBreakerTestEngine(t, duck, breaker)
	fq, tables := breakerTestQuery()

	breaker.RecordFailure() // threshold 1: breaker opens
	time.Sleep(100 * time.Millisecond)

	// A cancellable context plus the ctx-aware executor guarantee the probe
	// goroutine terminates even when an assertion below fails fatally.
	ctx, cancel := context.WithCancel(context.Background())
	var probeErr error
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		_, probeErr = engine.Query(ctx, tables, fq, nil)
	}()
	t.Cleanup(func() {
		cancel()
		<-probeDone
	})

	select {
	case <-duck.entered: // probe admitted and inside the executor
	case <-probeDone:
		t.Fatalf("probe exited before reaching the executor: %v", probeErr)
	}

	// A concurrent caller is rejected without reaching the executor.
	_, err := engine.Query(ctx, tables, fq, nil)
	require.ErrorContains(t, err, "circuit breaker open")
	require.Equal(t, int32(1), duck.calls.Load(), "non-probe caller must not reach the executor")

	close(duck.release)
	<-probeDone
	require.NoError(t, probeErr, "probe must succeed and close the breaker")

	// Probe success closed the breaker: the next caller flows through.
	_, err = engine.Query(ctx, tables, fq, nil)
	require.NoError(t, err, "breaker must be closed after probe success")
	require.Equal(t, int32(2), duck.calls.Load(), "post-probe caller must reach the executor")
}
