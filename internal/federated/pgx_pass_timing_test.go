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
// R2-F2, R3-F1).

// clockedStagedQuerier charges costs[i] to the clock for the pass's i-th
// staged statement and streamCosts[i] to its row iteration, and fails the
// statements failAt names.
type clockedStagedQuerier struct {
	DuckDBQueryExecutor
	clock       *fakeClock
	costs       []time.Duration
	streamCosts []time.Duration
	failAt      map[int]bool
	calls       int
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
	rows, err := q.DuckDBQueryExecutor.(stagedQuerier).queryStaged(ctx, dirty, sqlStr, args...)
	if err != nil || i >= len(q.streamCosts) {
		return rows, err
	}
	return &clockedRows{duckDBRowsIterator: rows, clock: q.clock, cost: q.streamCosts[i]}, nil
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

// A deep page whose window carried no count runs the #181 recount, a second
// DuckDB statement of the same pass. Its execution and streaming add to the
// page statement's, in the histogram and in the plan's duckdb_fetch and
// DuckDB source, so a slow recount shows (#856 review R3-F1).
func TestPgxPassReportsTheRecountsStages(t *testing.T) {
	f := newPgxFixture(t, tieRunRows())
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	rec := &metricRecorder{}
	duck := &clockedStagedQuerier{
		DuckDBQueryExecutor: NewDuckDBClientQueryExecutor(&DuckDBClient{DB: f.db}),
		clock:               clock,
		costs:               []time.Duration{40 * time.Millisecond, 300 * time.Millisecond},
		streamCosts:         []time.Duration{7 * time.Millisecond, 50 * time.Millisecond},
	}
	e := f.engineOver(t, duck, []string{f.dir + "/base.parquet"}, WithMetricEmitter(rec), clock.engineOption())
	opts := newExecutionPlanOpts()
	records, total, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 4, 20, nil, opts)
	require.NoError(t, err)
	require.Empty(t, records)
	require.Equal(t, int64(12), total, "the recount's count")
	require.Equal(t, 2, duck.calls, "the page statement and the recount")
	require.Equal(t, 340.0, rec.value(t, "fed_query_latency_histogram", "stage=execution"))
	require.Equal(t, 57.0, rec.value(t, "fed_query_latency_histogram", "stage=streaming"))
	require.Equal(t, 0.0, rec.value(t, "fed_query_latency_histogram", "stage=translation"))
	require.Equal(t, 0.0, rec.value(t, "fed_query_latency_histogram", "stage=snapshot"))
	require.Equal(t, 0.0, rec.value(t, "fed_query_row_count", "source=duckdb"), "the page's parquet rows, not the recount's")
	require.Equal(t, int64(397), opts.ExecutionPlan.Timings["duckdb_fetch"])
	require.Equal(t, int64(397), opts.ExecutionPlan.Timings["total"])
	duckSources := 0
	for _, src := range opts.ExecutionPlan.Sources {
		if src.Engine == "duckdb" {
			duckSources++
			require.Equal(t, int64(397), src.DurationMs)
		}
	}
	require.Equal(t, 1, duckSources, "the plan names the page's statement once")
}

// addStages sums every stage, translation included, which the engine's
// clock cannot charge to the statement builder.
func TestParquetLegAddStagesSumsEveryStage(t *testing.T) {
	leg := parquetLeg{translateMs: 1, executeMs: 2, streamMs: 3, count: 9, countKnown: true}
	leg.addStages(parquetLeg{translateMs: 10, executeMs: 20, streamMs: 30, count: 4})
	require.Equal(t, parquetLeg{translateMs: 11, executeMs: 22, streamMs: 33, count: 9, countKnown: true}, leg)
}
