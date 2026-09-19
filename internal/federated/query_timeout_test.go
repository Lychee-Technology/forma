package federated

import (
	"context"
	"errors"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

// budgetBlockedDuckDBExecutor answers schema probes like the plain fake but holds
// every main scan until its context is done, standing in for a DuckDB pass
// that would run past the configured budget.
type budgetBlockedDuckDBExecutor struct {
	fakeDuckDBExecutor
	sawDeadline bool
}

func (b *budgetBlockedDuckDBExecutor) Query(ctx context.Context, sql string, args ...any) (duckDBRowsIterator, error) {
	if strings.HasPrefix(sql, "DESCRIBE ") {
		return b.fakeDuckDBExecutor.Query(ctx, sql, args...)
	}
	if rows, ok := answerParquetDrainSQL(sql); ok {
		return rows, nil
	}
	b.calls++
	_, b.sawDeadline = ctx.Deadline()
	<-ctx.Done()
	return nil, ctx.Err()
}

func newTimeoutTestEngine(t *testing.T, duck DuckDBQueryExecutor, timeout time.Duration) *DBFederatedQueryEngine {
	t.Helper()
	engine := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDirtyIDFetcher{}, duck,
		NewCircuitBreaker(100, time.Minute, time.Minute),
		forma.DuckDBConfig{Enabled: true, QueryTimeout: timeout, Routing: forma.RoutingPolicy{Strategy: forma.RoutingStrategyHybrid}},
		testMetadataCacheSchema7(t), "", withTestParquetPath())
	engine.buildDuckSQL = func(tpl *template.Template, params any, q *model.FederatedAttributeQuery, dirtyIDs []uuid.UUID, dual *sqlgen.DualClauses) (string, []any, error) {
		return "SELECT fake", nil, nil
	}
	return engine
}

// TestQueryTimeoutCancelsTheDuckDBPass pins #465: DuckDBConfig.QueryTimeout
// is a real deadline on the context the executor runs under, a pass that
// outlives it is cancelled, and the caller sees context.DeadlineExceeded
// even under AllowPartialDegradedMode, because a spent budget is not a
// condition to answer around from Postgres.
func TestQueryTimeoutCancelsTheDuckDBPass(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &budgetBlockedDuckDBExecutor{}
	engine := newTimeoutTestEngine(t, duck, 50*time.Millisecond)
	fq, tables := breakerTestQuery()

	start := time.Now()
	_, err := engine.Query(context.Background(), tables, fq, &model.FederatedQueryOptions{AllowPartialDegradedMode: true})
	elapsed := time.Since(start)

	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded), "want DeadlineExceeded, got %v", err)
	require.True(t, duck.sawDeadline, "executor context carried no deadline")
	require.Less(t, elapsed, 5*time.Second, "pass was not cancelled by the budget")
	require.Equal(t, 1, duck.calls, "a timed-out pass must not be retried")
}

// TestZeroQueryTimeoutLeavesTheCallerContextAlone pins the unset contract: a
// zero QueryTimeout adds no deadline, so a caller-supplied cancellation is
// the only bound.
func TestZeroQueryTimeoutLeavesTheCallerContextAlone(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &budgetBlockedDuckDBExecutor{}
	engine := newTimeoutTestEngine(t, duck, 0)
	fq, tables := breakerTestQuery()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	_, err := engine.Query(ctx, tables, fq, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled), "want Canceled, got %v", err)
	require.False(t, duck.sawDeadline, "a zero QueryTimeout must not add a deadline")
}
