package federated

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// What a pgx pass reports.

// The execution plan records the hot leg as a PostgreSQL source whose
// predicate PostgreSQL evaluated, and the dirty set as read in the snapshot;
// every fed_query_* series of the pass is emitted once, the snapshot stage
// and the hot-match count included.
func TestPgxPassReportsTheHotLegAsAPostgresSource(t *testing.T) {
	f := newPgxFixture(t, tieRunRows())
	rec := &metricRecorder{}
	e := f.engine(t, []string{f.dir + "/base.parquet"}, WithMetricEmitter(rec))
	opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true}
	records, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 4, 0, nil, opts)
	require.NoError(t, err)
	require.Len(t, records, 4)

	plan := opts.ExecutionPlan
	require.Len(t, plan.Sources, 3)
	dirty, duck, hot := plan.Sources[0], plan.Sources[1], plan.Sources[2]
	require.Equal(t, "postgres", dirty.Engine)
	require.NotContains(t, dirty.SQL, "flushed_at >=", "nothing widens the dirty set")
	require.Equal(t, int64(len(f.leg.dirty)), dirty.RowEstimate)
	require.Equal(t, "duckdb", duck.Engine)
	require.Contains(t, duck.SQL, "temp.main.forma_dirty_ids")
	require.Equal(t, int64(4), duck.ActualRows, "the parquet leg answers the first page's window, ranks [0, 4)")
	require.Equal(t, "postgres", hot.Engine)
	require.Equal(t, hotLegPgxReason, hot.Reason)
	require.True(t, hot.PredicatePushdown)
	require.Equal(t, int64(6), hot.RowEstimate)
	require.Equal(t, int64(2), hot.ActualRows, "rows 9 and 12 of the first page are hot")
	require.Contains(t, plan.Timings, "pg_snapshot")

	require.Equal(t, map[string]int{
		"fed_query_latency_histogram/stage=snapshot":    1,
		"fed_query_latency_histogram/stage=translation": 1,
		"fed_query_latency_histogram/stage=execution":   1,
		"fed_query_latency_histogram/stage=streaming":   1,
		"fed_query_row_count/source=pg":                 1,
		"fed_query_row_count/source=duckdb":             1,
		"fed_query_row_count/source=hot":                1,
	}, rec.names())
	require.Equal(t, float64(6), rec.value(t, "fed_query_row_count", "source=hot"))
	require.Equal(t, float64(len(f.leg.dirty)), rec.value(t, "fed_query_row_count", "source=pg"))
}
