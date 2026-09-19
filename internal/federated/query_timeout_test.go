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

// deepPageDuckDBExecutor answers the first main scan with no rows — which
// on a deep page makes Query recount (#181) — and holds the recount until
// its context is done, recording the deadline every main scan ran under.
type deepPageDuckDBExecutor struct {
	fakeDuckDBExecutor
	deadlines []time.Time
}

func (d *deepPageDuckDBExecutor) Query(ctx context.Context, sql string, args ...any) (duckDBRowsIterator, error) {
	if strings.HasPrefix(sql, "DESCRIBE ") {
		return d.fakeDuckDBExecutor.Query(ctx, sql, args...)
	}
	if rows, ok := answerParquetDrainSQL(sql); ok {
		return rows, nil
	}
	d.calls++
	deadline, _ := ctx.Deadline()
	d.deadlines = append(d.deadlines, deadline)
	if d.calls == 1 {
		return &verifyFakeRows{}, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestQueryTimeoutIsOneBudgetForTheWholeRequest pins the #465 review
// finding: a request that takes several DuckDB passes — here a deep page
// whose empty scan triggers the recount — spends one QueryTimeout in total,
// not one per pass. The recount runs under the very deadline the page pass
// ran under, so a configuration validated against a single DuckDB budget
// (bootstrap.LargestRequestBudget) really does cover the request.
func TestQueryTimeoutIsOneBudgetForTheWholeRequest(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &deepPageDuckDBExecutor{}
	engine := newTimeoutTestEngine(t, duck, 50*time.Millisecond)
	fq, tables := breakerTestQuery()
	fq.Offset = 10

	start := time.Now()
	_, err := engine.Query(context.Background(), tables, fq, nil)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded), "want DeadlineExceeded, got %v", err)
	require.Equal(t, 2, duck.calls, "the empty deep page must be recounted")
	require.False(t, duck.deadlines[0].IsZero(), "the page pass carried no deadline")
	require.True(t, duck.deadlines[1].Equal(duck.deadlines[0]),
		"the recount ran under a later deadline (%v) than the page pass (%v): each pass was given its own budget",
		duck.deadlines[1], duck.deadlines[0])
	require.Less(t, elapsed, 5*time.Second, "the recount was not cancelled by the budget")
}
