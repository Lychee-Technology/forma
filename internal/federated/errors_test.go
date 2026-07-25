package federated

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// erroringRowsIterator yields no rows and fails at iteration end, modeling a
// mid-stream fault (e.g. truncated parquet discovered after planning).
type erroringRowsIterator struct{ err error }

func (r *erroringRowsIterator) Next() bool             { return false }
func (r *erroringRowsIterator) Scan(dest ...any) error { return nil }
func (r *erroringRowsIterator) Err() error             { return r.err }
func (r *erroringRowsIterator) Close() error           { return nil }

// coldTierQuery returns a query shape that routes through the DuckDB
// federated path (non-hot-only tiers; hot-only would short-circuit to
// Postgres and never reach the classified error sites).
func coldTierQuery() *model.FederatedAttributeQuery {
	return &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 2000},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierCold},
	}
}

// newSentinelTestEngine wires a parquet source that resolves one path. These
// tests classify failures at the DuckDB execute/stream stages, so the query
// has to get that far: since #299 an empty path set fails at resolution with
// ErrNoParquetPaths and never reaches the sites under test.
func newSentinelTestEngine(t *testing.T, duck DuckDBQueryExecutor, breaker *CircuitBreaker) *DBFederatedQueryEngine {
	t.Helper()
	return NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDirtyIDFetcher{}, duck, breaker,
		forma.DuckDBConfig{Enabled: true, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}},
		testMetadataCacheSchema7(t), "host=x", withTestParquetPath())
}

// TestSentinel_ExecuteFailureIsFederatedReadFailed pins the #187 branch
// classification: a DuckDB execution failure (corrupt/truncated/wrong-schema
// parquet, storage rejections) carries ErrFederatedReadFailed, asserted by
// errors.Is — never by driver message text.
func TestSentinel_ExecuteFailureIsFederatedReadFailed(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &fakeDuckDBExecutor{err: fmt.Errorf("IO Error: some driver text")}
	engine := newSentinelTestEngine(t, duck, nil)

	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), nil)

	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.NotErrorIs(t, err, ErrParquetSetInconsistent)
	require.NotErrorIs(t, err, ErrDuckDBUnavailable)
	require.ErrorContains(t, err, "some driver text", "raw driver error must stay in the chain for logs")
}

// TestSentinel_StreamFailureIsFederatedReadFailed pins the mid-stream fault
// branch (iterate) to the same read-failure classification.
func TestSentinel_StreamFailureIsFederatedReadFailed(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &fakeDuckDBExecutor{rows: &erroringRowsIterator{err: fmt.Errorf("stream broke mid-flight")}}
	engine := newSentinelTestEngine(t, duck, nil)

	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), nil)

	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.ErrorContains(t, err, "iterate duckdb rows")
}

// TestSentinel_ClientUnavailable pins the nil-client branch to
// ErrDuckDBUnavailable (distinct from read failures: nothing was read).
func TestSentinel_ClientUnavailable(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	engine := newSentinelTestEngine(t, nil, nil)

	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), nil)

	require.ErrorIs(t, err, ErrDuckDBUnavailable)
	require.NotErrorIs(t, err, ErrFederatedReadFailed)
}

// TestSentinel_BreakerOpenIsDuckDBUnavailable pins the breaker-open rejection
// to ErrDuckDBUnavailable: the query never reached DuckDB.
func TestSentinel_BreakerOpenIsDuckDBUnavailable(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	breaker := NewCircuitBreaker(1, time.Minute, time.Minute)
	breaker.RecordFailure()
	require.True(t, breaker.IsOpen())

	duck := &fakeDuckDBExecutor{rows: &singleDuckDBRow{rowID: uuid.New()}}
	engine := newSentinelTestEngine(t, duck, breaker)

	_, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), nil)

	require.ErrorIs(t, err, ErrDuckDBUnavailable)
	require.Equal(t, 0, duck.calls, "breaker-open must reject before DuckDB executes")
}

// TestParquetSetInconsistentError_Unwrap pins the typed carrier: errors.Is
// reaches the sentinel and errors.As surfaces schema + missing keys, so
// operators (and #187 e2e) can read the offending state from the error.
func TestParquetSetInconsistentError_Unwrap(t *testing.T) {
	inner := &ParquetSetInconsistentError{SchemaID: 22, MissingKeys: []string{"a.parquet", "b.parquet"}}
	err := fmt.Errorf("execute duckdb query: %w", inner)

	require.ErrorIs(t, err, ErrParquetSetInconsistent)
	var typed *ParquetSetInconsistentError
	require.True(t, errors.As(err, &typed))
	require.Equal(t, int16(22), typed.SchemaID)
	require.Equal(t, []string{"a.parquet", "b.parquet"}, typed.MissingKeys)
	require.ErrorContains(t, err, "schema 22")
	require.ErrorContains(t, err, "a.parquet")
}

// TestParquetSetInconsistentAliasIsIdentical pins that the #301 promotion is an
// alias, not a copy: a value built through the federated name is matched by the
// root sentinel, so internal/httpapi's classification sees the errors the engine
// actually produces.
func TestParquetSetInconsistentAliasIsIdentical(t *testing.T) {
	err := fmt.Errorf("execute duckdb query: %w",
		&ParquetSetInconsistentError{SchemaID: 9, MissingKeys: []string{"k.parquet"}})

	require.ErrorIs(t, err, forma.ErrParquetSetInconsistent)
	require.ErrorIs(t, err, ErrParquetSetInconsistent)

	var typed *forma.ParquetSetInconsistentError
	require.True(t, errors.As(err, &typed))
	require.Equal(t, int16(9), typed.SchemaID)
}
