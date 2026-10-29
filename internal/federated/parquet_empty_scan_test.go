package federated

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
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
	return newEmptyScanTestEngineWithConfig(t, hybridDuckConfig(), duck, dirty, paths, opts...)
}

func newEmptyScanTestEngineWithConfig(t *testing.T, cfg forma.DuckDBConfig, duck *fakeDuckDBExecutor, dirty *fakeDirtyIDFetcher, paths []string, opts ...EngineOption) *DBFederatedQueryEngine {
	t.Helper()
	opts = append(opts, WithParquetSource(&fakeParquetSource{paths: paths}))
	return NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, dirty, duck,
		NewCircuitBreaker(1, time.Minute, time.Minute),
		cfg, testMetadataCacheSchema7(t), "host=x", opts...)
}

// renderedBarrierCutoffs returns every flushed_at cutoff literal the dirty
// barrier renders in sql: one in dirty_ids and one in pg_source.
func renderedBarrierCutoffs(t *testing.T, sql string) []int64 {
	t.Helper()
	var cutoffs []int64
	for _, m := range regexp.MustCompile(`flushed_at >= (\d+)`).FindAllStringSubmatch(sql, -1) {
		n, err := strconv.ParseInt(m[1], 10, 64)
		require.NoError(t, err)
		cutoffs = append(cutoffs, n)
	}
	require.Len(t, cutoffs, 2, "an all-tier query renders the barrier in dirty_ids and pg_source")
	return cutoffs
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
		"an all-tier query asks at its path-resolution anchor, not with the strict barrier")
}

// TestEmptyScanAsksAtThePathResolutionAnchor: a flush marked before the
// anchor published a manifest the resolution would have found, so a lost one
// must keep failing the read even inside the #252 grace margin. The check
// therefore asks at the anchor, while the hot leg keeps the margin.
func TestEmptyScanAsksAtThePathResolutionAnchor(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	cfg := hybridDuckConfig()
	cfg.FlushVisibilityGraceMs = 60_000
	duck := &fakeDuckDBExecutor{globFiles: []string{}, rows: &singleDuckDBRow{rowID: uuid.New()}}
	dirty := &fakeDirtyIDFetcher{}
	engine := newEmptyScanTestEngineWithConfig(t, cfg, duck, dirty, []string{fallbackGlob})

	before := time.Now().UnixMilli()
	_, err := engine.Query(context.Background(), emptyScanTables, allTierQuery(), nil)
	after := time.Now().UnixMilli()
	require.NoError(t, err)
	require.Contains(t, duck.lastSQL, emptyScanMarker)
	require.GreaterOrEqual(t, dirty.parquetOnlyCutoffMs, before, "the check must not reach back by the grace margin")
	require.LessOrEqual(t, dirty.parquetOnlyCutoffMs, after)
	for _, cutoff := range renderedBarrierCutoffs(t, duck.lastSQL) {
		require.Equal(t, dirty.parquetOnlyCutoffMs-60_000, cutoff, "the hot leg keeps the configured margin")
	}
}

// TestEmptyScanKeepsARacingFirstFlushHotWithWideningDisabled: the empty scan
// source cannot see a delta a first flush publishes after the check, so the
// rows it marks must stay in the hot leg. With the #252 widening disabled the
// strict barrier would drop them from both legs; the empty scan renders the
// anchor instead.
func TestEmptyScanKeepsARacingFirstFlushHotWithWideningDisabled(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	cfg := hybridDuckConfig()
	cfg.FlushVisibilityGraceMs = -1
	duck := &fakeDuckDBExecutor{globFiles: []string{}, rows: &singleDuckDBRow{rowID: uuid.New()}}
	dirty := &fakeDirtyIDFetcher{}
	engine := newEmptyScanTestEngineWithConfig(t, cfg, duck, dirty, []string{fallbackGlob})

	before := time.Now().UnixMilli()
	_, err := engine.Query(context.Background(), emptyScanTables, allTierQuery(), nil)
	after := time.Now().UnixMilli()
	require.NoError(t, err)
	require.Contains(t, duck.lastSQL, emptyScanMarker)
	for _, cutoff := range renderedBarrierCutoffs(t, duck.lastSQL) {
		require.GreaterOrEqual(t, cutoff, before)
		require.LessOrEqual(t, cutoff, after, "the hot leg must keep rows marked after the anchor")
		require.Equal(t, dirty.parquetOnlyCutoffMs, cutoff, "the hot leg serves at the instant the check asked about")
	}

	// A path set that does scan parquet keeps the operator's strict barrier.
	duck.globFiles = []string{"s3://b/7/delta.parquet"}
	duck.rows = &singleDuckDBRow{rowID: uuid.New()}
	_, err = engine.Query(context.Background(), emptyScanTables, allTierQuery(), nil)
	require.NoError(t, err)
	require.NotContains(t, duck.lastSQL, emptyScanMarker)
	for _, cutoff := range renderedBarrierCutoffs(t, duck.lastSQL) {
		require.Equal(t, sqlgen.FlushGraceCutoffDisabled, cutoff)
	}
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
