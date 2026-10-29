package federated

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

// fallbackGlob is the shape of the level-3 glob a source authors for a
// schema with no manifest entries (manifest.S3FallbackGlob).
const fallbackGlob = "s3://b/7/*.parquet"

const emptyScanMarker = "WHERE false) AS cold_scan"

var emptyScanTables = model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"}

func allTierQuery() *model.FederatedAttributeQuery {
	return &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 2000},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
	}
}

func newEmptyScanTestEngine(t *testing.T, duck *fakeDuckDBExecutor, dirty *fakeDirtyIDFetcher, paths []string, opts ...EngineOption) *DBFederatedQueryEngine {
	t.Helper()
	opts = append(opts, WithParquetSource(&fakeParquetSource{paths: paths}))
	return NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, dirty, duck,
		NewCircuitBreaker(1, time.Minute, time.Minute),
		hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x", opts...)
}

// TestNeverFlushedSchemaScansNoParquet pins #720: the source's fallback glob
// matches no object and every row is served hot, so the read renders the empty
// scan source instead of a read_parquet that DuckDB would refuse. The
// breaker opens on one failure, so the later queries prove none was recorded.
func TestNeverFlushedSchemaScansNoParquet(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &fakeDuckDBExecutor{globFiles: []string{}}
	dirty := &fakeDirtyIDFetcher{}
	engine := newEmptyScanTestEngine(t, duck, dirty, []string{fallbackGlob})

	for i := 0; i < 3; i++ {
		duck.rows = &singleDuckDBRow{rowID: uuid.New()}
		opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true, ExecutionPlan: &model.ExecutionPlan{Timings: map[string]int64{}}}
		page, err := engine.Query(context.Background(), emptyScanTables, allTierQuery(), opts)
		require.NoError(t, err, "query %d", i)
		require.Len(t, page.Records, 1, "the hot leg's row is the answer")
		require.Contains(t, opts.ExecutionPlan.Notes, NoteEmptyParquetScan)
	}
	require.Contains(t, duck.lastSQL, emptyScanMarker)
	require.NotContains(t, duck.lastSQL, "read_parquet(", "a glob that matches nothing must not reach read_parquet")
	require.NotContains(t, duck.lastSQL, fallbackGlob)
	require.Equal(t, 3, dirty.parquetOnlyCalls)
	require.Less(t, dirty.parquetOnlyCutoffMs, sqlgen.FlushGraceCutoffDisabled,
		"an all-tier query asks about rows flushed before its grace cutoff, the ones its hot leg does not serve")
}

// TestNeverFlushedHotExcludedQueryUsesTheStrictBarrier: a hot-excluded query
// keeps flushed_at = 0 as its barrier, so any flushed row means parquet
// should hold data for it.
func TestNeverFlushedHotExcludedQueryUsesTheStrictBarrier(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &fakeDuckDBExecutor{globFiles: []string{}, rows: &fakeStringRows{}}
	dirty := &fakeDirtyIDFetcher{}
	engine := newEmptyScanTestEngine(t, duck, dirty, []string{fallbackGlob})

	q := allTierQuery()
	q.PreferredTiers = []model.DataTier{model.DataTierWarm, model.DataTierCold}
	_, err := engine.Query(context.Background(), emptyScanTables, q, nil)
	require.NoError(t, err)
	require.Contains(t, duck.lastSQL, emptyScanMarker)
	require.Equal(t, sqlgen.FlushGraceCutoffDisabled, dirty.parquetOnlyCutoffMs)
}

// TestEmptyFallbackGlobKeepsTheScanWhenItCannotProveNoParquet covers the
// cases where an empty scan would hide data or rests on no evidence. Each
// keeps today's read_parquet over the resolved set.
func TestEmptyFallbackGlobKeepsTheScanWhenItCannotProveNoParquet(t *testing.T) {
	cases := []struct {
		name        string
		paths       []string
		globFiles   []string
		parquetOnly bool
		wantCheck   bool
	}{
		{
			// A lost manifest over flushed rows, or over rows a cdc-init
			// export left without a change_log entry: the hot leg does not
			// serve them, so the read must still fail on the empty glob.
			name: "rows only parquet serves", paths: []string{fallbackGlob}, globFiles: []string{},
			parquetOnly: true, wantCheck: true,
		},
		{
			name: "glob listing failed", paths: []string{fallbackGlob}, globFiles: nil,
		},
		{
			name: "glob matches an object", paths: []string{fallbackGlob}, globFiles: []string{"s3://b/7/a.parquet"},
		},
		{
			name: "manifest-listed objects", paths: []string{testParquetPath},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			restore := initTestDescriptors()
			defer restore()

			duck := &fakeDuckDBExecutor{globFiles: tc.globFiles, rows: &singleDuckDBRow{rowID: uuid.New()}}
			dirty := &fakeDirtyIDFetcher{parquetOnly: tc.parquetOnly}
			engine := newEmptyScanTestEngine(t, duck, dirty, tc.paths)

			_, err := engine.Query(context.Background(), emptyScanTables, allTierQuery(), nil)
			require.NoError(t, err, "the fake scan succeeds; only the rendered SQL is under test")
			require.Contains(t, duck.lastSQL, "read_parquet(")
			require.NotContains(t, duck.lastSQL, emptyScanMarker)
			require.Equal(t, tc.wantCheck, dirty.parquetOnlyCalls > 0)
		})
	}
}

// TestHintGlobMatchingNothingIsNotTreatedAsNeverFlushed: a caller hint names
// objects no manifest vouches for, so an empty hint glob keeps failing as the
// scan reports it.
func TestHintGlobMatchingNothingIsNotTreatedAsNeverFlushed(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &fakeDuckDBExecutor{globFiles: []string{}, rows: &singleDuckDBRow{rowID: uuid.New()}}
	dirty := &fakeDirtyIDFetcher{}
	engine := newEmptyScanTestEngine(t, duck, dirty, []string{testParquetPath})

	q := allTierQuery()
	q.DuckDBHints = &model.DuckDBRenderHints{S3ParquetPathTemplate: "s3://b/hinted/{{.SchemaID}}/*.parquet"}
	_, err := engine.Query(context.Background(), emptyScanTables, q, nil)
	require.NoError(t, err)
	require.Contains(t, duck.lastSQL, "s3://b/hinted/7/*.parquet")
	require.Zero(t, dirty.parquetOnlyCalls)
}

// TestParquetOnlyCheckFailureFailsTheRead: without the PostgreSQL check the
// engine cannot tell a never-flushed schema from a lost manifest, so it
// neither scans nor answers. The failure is the Postgres side's.
func TestParquetOnlyCheckFailureFailsTheRead(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &fakeDuckDBExecutor{globFiles: []string{}}
	dirty := &fakeDirtyIDFetcher{parquetOnlyErr: errors.New("connection reset")}
	engine := newEmptyScanTestEngine(t, duck, dirty, []string{fallbackGlob})

	_, err := engine.Query(context.Background(), emptyScanTables, allTierQuery(), nil)
	require.ErrorIs(t, err, ErrPostgresReadFailed)
	require.Zero(t, duck.calls, "no main scan runs")
}

// TestEmptyScanPlanIsNotReusedAfterTheFirstFlush: the compiled empty-scan
// skeleton is keyed apart, so once the glob matches an object the next query
// compiles a real read_parquet.
func TestEmptyScanPlanIsNotReusedAfterTheFirstFlush(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &fakeDuckDBExecutor{globFiles: []string{}, rows: &singleDuckDBRow{rowID: uuid.New()}}
	engine := newEmptyScanTestEngine(t, duck, &fakeDirtyIDFetcher{}, []string{fallbackGlob},
		WithPlanCache(queryplan.NewCache(16)))

	_, err := engine.Query(context.Background(), emptyScanTables, allTierQuery(), nil)
	require.NoError(t, err)
	require.Contains(t, duck.lastSQL, emptyScanMarker)

	duck.globFiles = []string{"s3://b/7/delta.parquet"}
	duck.rows = &singleDuckDBRow{rowID: uuid.New()}
	_, err = engine.Query(context.Background(), emptyScanTables, allTierQuery(), nil)
	require.NoError(t, err)
	require.NotContains(t, duck.lastSQL, emptyScanMarker)
	require.Contains(t, duck.lastSQL, "read_parquet('"+fallbackGlob+"'")
}
