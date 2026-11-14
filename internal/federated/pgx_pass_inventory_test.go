package federated

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// The pgx pass's path set: the frozen inventory, the #720 empty set, and
// what the pass reports.

// A publication after the snapshot through a source-authored glob (design.md
// §6.1.1, Frozen inventory): the glob was listed at resolution, so an object
// a writer publishes between the snapshot and the scan is outside the scan.
// The clean row the writer updated reads at its snapshot version, and the row
// it created is not in the answer.
func TestPgxPassScansTheInventoryListedAtResolution(t *testing.T) {
	f := newPgxFixture(t, tieRunRows())
	e := f.engine(t, []string{filepath.Join(f.dir, "*.parquet")})
	f.leg.afterChangeLog = func() {
		f.writeParquet(t, "late.parquet", []pgxRow{
			{i: 1, createdAt: 0, age: ageOf(99), parquet: true},
			{i: 99, createdAt: 900, age: ageOf(40), parquet: true},
		})
	}
	records, total, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 20, 0, nil, nil)
	require.NoError(t, err)
	require.Equal(t, int64(12), total)
	require.NotContains(t, recordIDs(records), parquetOnlyRowID(99), "an object published after the listing is outside the scan")
	for _, r := range records {
		if r.RowID == parquetOnlyRowID(1) {
			require.Equal(t, map[string]int32{"integer_01": 31}, r.Int32Items, "row 1 reads at its snapshot version")
		}
	}
}

// A listed object deleted before the scan fails the read, and no manifest
// listed it, so the failure claims no manifest inconsistency.
func TestPgxPassListedObjectDeletedBeforeTheScanIsNotAnInconsistency(t *testing.T) {
	f := newPgxFixture(t, tieRunRows())
	src := &fakeParquetSource{paths: []string{filepath.Join(f.dir, "*.parquet")}, missing: []string{"base.parquet"}}
	e := f.engine(t, nil, WithParquetSource(src))
	f.leg.afterChangeLog = func() { require.NoError(t, os.Remove(filepath.Join(f.dir, "base.parquet"))) }
	_, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 5, 0, nil, nil)
	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.NotErrorIs(t, err, ErrParquetSetInconsistent)
	require.Zero(t, src.missingCalls, "objects a glob listed are not probed as manifest entries")
}

// A failed listing fails the pass as a degradable read failure, rather than
// scanning the glob outside the snapshot.
func TestPgxPassFailedListingFailsThePass(t *testing.T) {
	e := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDirtyIDFetcher{}, &fakeDuckDBExecutor{}, nil,
		pgxDuckConfig(), parquetOnlyMetadata(t), "", WithParquetSource(&fakeParquetSource{paths: []string{"s3://b/7/delta/*.parquet"}}))
	e.hotLeg = &fakeHotLeg{t: t}
	_, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 5, 0, nil, nil)
	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.True(t, degradableFederatedError(err))
	require.Zero(t, e.hotLeg.(*fakeHotLeg).begins, "no snapshot is opened for a set that cannot be listed")
}

func hotOnlyRows() []pgxRow {
	return []pgxRow{
		{i: 1, createdAt: 10, age: ageOf(30), hot: true},
		{i: 2, createdAt: 30, age: ageOf(30), hot: true},
		{i: 3, createdAt: 20, age: ageOf(30), hot: true},
	}
}

// A schema with no parquet (#720) runs no DuckDB statement: when the
// snapshot records no parquet history, the hot leg is the whole answer.
func TestPgxPassWithoutParquetAnswersFromTheHotLeg(t *testing.T) {
	f := newPgxFixture(t, hotOnlyRows())
	rec := &metricRecorder{}
	e := f.engine(t, []string{filepath.Join(f.dir, "*.parquet")}, WithMetricEmitter(rec))
	opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true}
	records, total, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 2, 0, nil, opts)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{parquetOnlyRowID(2), parquetOnlyRowID(3)}, recordIDs(records))
	require.Equal(t, int64(3), total)
	require.True(t, f.leg.lastChangeLog.ParquetHistory, "the snapshot answers the history check")
	for _, src := range opts.ExecutionPlan.Sources {
		require.NotEqual(t, "duckdb", src.Engine, "no DuckDB statement ran")
	}
	require.Contains(t, opts.ExecutionPlan.Notes, NoteEmptyParquetScan)
	require.Equal(t, map[string]int{
		"fed_query_latency_histogram/stage=snapshot": 1,
		"fed_query_row_count/source=pg":              1,
		"fed_query_row_count/source=hot":             1,
	}, rec.names())
}

// With parquet history and still no object, the parquet tiers cannot be
// read: a degradable read failure, as a glob scan matching nothing is today.
func TestPgxPassWithHistoryButNoObjectFails(t *testing.T) {
	f := newPgxFixture(t, hotOnlyRows())
	f.leg.history = true
	e := f.engine(t, []string{filepath.Join(f.dir, "*.parquet")})
	_, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 2, 0, nil, nil)
	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.True(t, degradableFederatedError(err))
}

// A publication that lands before the history check is found by the
// re-resolution, and the retried pass scans it.
func TestPgxPassRetriesAPublicationRacingTheEmptySet(t *testing.T) {
	rows := append(hotOnlyRows(), pgxRow{i: 4, createdAt: 40, age: ageOf(30), parquet: true})
	f := newPgxFixture(t, rows)
	require.NoError(t, os.Remove(filepath.Join(f.dir, "base.parquet")))
	f.leg.history = true
	published := false
	f.leg.afterChangeLog = func() {
		if !published {
			published = true
			f.writeParquet(t, "first.parquet", []pgxRow{rows[3]})
		}
	}
	e := f.engine(t, []string{filepath.Join(f.dir, "*.parquet")})
	records, total, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 5, 0, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{parquetOnlyRowID(4), parquetOnlyRowID(2), parquetOnlyRowID(3), parquetOnlyRowID(1)}, recordIDs(records))
	require.Equal(t, int64(4), total)
	require.Equal(t, 2, f.leg.begins, "the raced pass is retried once")
}

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
	require.Contains(t, dirty.SQL, "flushed_at >= $2", "the hot form reads the widened set")
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
	for _, src := range plan.Sources {
		require.NotEqual(t, hotLegPredicateReason, src.Reason, "nothing evaluates the hot leg in DuckDB")
	}

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
