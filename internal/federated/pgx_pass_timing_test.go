package federated

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// The stage timings of a pgx pass on the engine's clock (#856 review R2-F1,
// R2-F2).

// clockedStagedQuerier charges costs[i] to the clock for the pass's i-th
// staged statement, and fails the statements failAt names.
type clockedStagedQuerier struct {
	DuckDBQueryExecutor
	clock  *fakeClock
	costs  []time.Duration
	failAt map[int]bool
	calls  int
}

var errStagedStatementFailed = errors.New("staged statement failed")

func (q *clockedStagedQuerier) queryStaged(ctx context.Context, dirty []uuid.UUID, sqlStr string, args ...any) (duckDBRowsIterator, error) {
	i := q.calls
	q.calls++
	if i < len(q.costs) {
		q.clock.advance(q.costs[i])
	}
	if q.failAt[i] {
		return nil, errStagedStatementFailed
	}
	return q.DuckDBQueryExecutor.(stagedQuerier).queryStaged(ctx, dirty, sqlStr, args...)
}

func newExecutionPlanOpts() *model.FederatedQueryOptions {
	return &model.FederatedQueryOptions{IncludeExecutionPlan: true,
		ExecutionPlan: &model.ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}}}
}

// The row handler's time is streaming, as on the attach path: a handler that
// takes 30ms a row over a 4-row page shows as 120ms of streaming, in the
// histogram and in the plan's duckdb_fetch, and not as snapshot time.
func TestPgxPassStreamingIncludesTheRowHandler(t *testing.T) {
	f := newPgxFixture(t, tieRunRows())
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	rec := &metricRecorder{}
	duck := &clockedStagedQuerier{
		DuckDBQueryExecutor: NewDuckDBClientQueryExecutor(&DuckDBClient{DB: f.db}),
		clock:               clock,
		costs:               []time.Duration{40 * time.Millisecond},
	}
	e := f.engineOver(t, duck, []string{f.dir + "/base.parquet"}, WithMetricEmitter(rec), clock.engineOption())
	opts := newExecutionPlanOpts()
	rows := 0
	_, err := e.StreamDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 4, 0, nil, opts,
		func(context.Context, *model.PersistentRecord) error {
			rows++
			clock.advance(30 * time.Millisecond)
			return nil
		})
	require.NoError(t, err)
	require.Equal(t, 4, rows)
	require.Equal(t, 40.0, rec.value(t, "fed_query_latency_histogram", "stage=execution"))
	require.Equal(t, 120.0, rec.value(t, "fed_query_latency_histogram", "stage=streaming"))
	require.Equal(t, 0.0, rec.value(t, "fed_query_latency_histogram", "stage=snapshot"))
	require.Equal(t, int64(160), opts.ExecutionPlan.Timings["duckdb_fetch"])
	require.Equal(t, int64(160), opts.ExecutionPlan.Timings["total"])
	for _, src := range opts.ExecutionPlan.Sources {
		if src.Engine == "duckdb" {
			require.Equal(t, int64(160), src.DurationMs)
		}
	}
}

// A failed parquet statement records duckdb_fetch from its own start: the
// page's statement, and the #181 recount after a page that carried no count.
func TestPgxPassFailedStatementMeasuresItsOwnAttempt(t *testing.T) {
	cases := map[string]struct {
		offset int
		costs  []time.Duration
		failAt int
	}{
		"page statement": {offset: 0, costs: []time.Duration{40 * time.Millisecond}, failAt: 0},
		"recount":        {offset: 20, costs: []time.Duration{1000 * time.Millisecond, 40 * time.Millisecond}, failAt: 1},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			f := newPgxFixture(t, tieRunRows())
			clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
			duck := &clockedStagedQuerier{
				DuckDBQueryExecutor: NewDuckDBClientQueryExecutor(&DuckDBClient{DB: f.db}),
				clock:               clock,
				costs:               c.costs,
				failAt:              map[int]bool{c.failAt: true},
			}
			e := f.engineOver(t, duck, []string{f.dir + "/base.parquet"}, clock.engineOption())
			opts := newExecutionPlanOpts()
			_, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 4, c.offset, nil, opts)
			require.ErrorIs(t, err, errStagedStatementFailed)
			require.Equal(t, c.failAt+1, duck.calls)
			require.Equal(t, int64(40), opts.ExecutionPlan.Timings["duckdb_fetch"])
		})
	}
}
