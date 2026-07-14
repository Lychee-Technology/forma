package federated

import (
	"context"
	"fmt"
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
