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
	return newEmptyScanTestEngineWithSource(t, hybridDuckConfig(), duck, dirty, &fakeParquetSource{paths: paths}, opts...)
}

// newEmptyScanTestEngineWithSource builds the engine over src, so a test can
// change what the source resolves mid-query. The breaker opens on one
// failure.
func newEmptyScanTestEngineWithSource(t *testing.T, cfg forma.DuckDBConfig, duck *fakeDuckDBExecutor, dirty *fakeDirtyIDFetcher, src ParquetSource, opts ...EngineOption) *DBFederatedQueryEngine {
	t.Helper()
	opts = append(opts, WithParquetSource(src))
	return NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, dirty, duck,
		NewCircuitBreaker(1, time.Minute, time.Minute),
		cfg, testMetadataCacheSchema7(t), "host=x", opts...)
}

// rowPerScan answers every main scan with one row: Close rewinds it, so a
// retried pass reads a row again.
type rowPerScan struct{ singleDuckDBRow }

func (r *rowPerScan) Close() error {
	r.scanned = false
	return nil
}

func planOpts() *model.FederatedQueryOptions {
	return &model.FederatedQueryOptions{IncludeExecutionPlan: true, ExecutionPlan: &model.ExecutionPlan{Timings: map[string]int64{}}}
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
		opts := planOpts()
		page, err := engine.Query(context.Background(), emptyScanTables, allTierQuery(), opts)
		require.NoError(t, err, "query %d", i)
		require.Len(t, page.Records, 1, "the hot leg's row is the answer")
		require.Contains(t, opts.ExecutionPlan.Notes, NoteEmptyParquetScan)
	}
	require.Contains(t, duck.lastSQL, emptyScanMarker)
	require.NotContains(t, duck.lastSQL, "read_parquet(", "a glob that matches nothing must not reach read_parquet")
	require.NotContains(t, duck.lastSQL, fallbackGlob)
	require.Equal(t, 3, dirty.historyCalls)
}

// TestEmptyScanKeepsTheConfiguredBarrier: the empty scan's completeness
// does not rest on the dirty barrier, so its hot leg renders the operator's
// barrier like any other read. With the #252 widening disabled that is the
// strict one.
func TestEmptyScanKeepsTheConfiguredBarrier(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	cfg := hybridDuckConfig()
	cfg.FlushVisibilityGraceMs = -1
	duck := &fakeDuckDBExecutor{globFiles: []string{}, rows: &singleDuckDBRow{rowID: uuid.New()}}
	engine := newEmptyScanTestEngineWithSource(t, cfg, duck, &fakeDirtyIDFetcher{}, &fakeParquetSource{paths: []string{fallbackGlob}})

	_, err := engine.Query(context.Background(), emptyScanTables, allTierQuery(), nil)
	require.NoError(t, err)
	require.Contains(t, duck.lastSQL, emptyScanMarker)
	for _, cutoff := range renderedBarrierCutoffs(t, duck.lastSQL) {
		require.Equal(t, sqlgen.FlushGraceCutoffDisabled, cutoff)
	}
}

// TestEmptyScanRetriesWhenAPublicationRacesIt (#720 review): a flush or a
// cdc-init handoff that publishes after the check can move rows out of the
// hot leg, and the fixed empty scan cannot read what it published. The
// recheck after the pass sees the publication, so the page is discarded and
// the retry scans the new object. No clock is involved, and the breaker
// records no failure.
func TestEmptyScanRetriesWhenAPublicationRacesIt(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &fakeDuckDBExecutor{globFiles: []string{}, rows: &rowPerScan{singleDuckDBRow{rowID: uuid.New()}}}
	dirty := &fakeDirtyIDFetcher{}
	dirty.afterHistory = func() { duck.globFiles = []string{"s3://b/7/delta.parquet"} }
	engine := newEmptyScanTestEngine(t, duck, dirty, []string{fallbackGlob})

	opts := planOpts()
	page, err := engine.Query(context.Background(), emptyScanTables, allTierQuery(), opts)
	require.NoError(t, err)
	require.Len(t, page.Records, 1)
	require.Equal(t, 2, duck.calls, "the raced pass and its retry")
	require.Contains(t, duck.lastSQL, "read_parquet('"+fallbackGlob+"'", "the retry scans the published object")
	require.NotContains(t, opts.ExecutionPlan.Notes, NoteEmptyParquetScan, "the plan describes the retry only")
	require.Equal(t, 1, dirty.historyCalls)

	_, err = engine.Query(context.Background(), emptyScanTables, allTierQuery(), nil)
	require.NoError(t, err, "the breaker opens on one failure, so the race recorded none")
}

// TestRetryRewindPreservesPrePassPlan pins #348 item 2 on the retry that
// remains (#720): rewind is a truncate-to-mark, and the strongest wrong
// implementation — rewinding to an empty mark (executionPlanMark{}) —
// destroys what the CALLER recorded before the first pass (e.g. the keyset
// coordinator's "keyset pagination" note). Routing alone cannot catch that:
// rewind never touches Routing.
func TestRetryRewindPreservesPrePassPlan(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &fakeDuckDBExecutor{globFiles: []string{}, rows: &rowPerScan{singleDuckDBRow{rowID: uuid.New()}}}
	dirty := &fakeDirtyIDFetcher{}
	dirty.afterHistory = func() { duck.globFiles = []string{"s3://b/7/delta.parquet"} }
	engine := newEmptyScanTestEngine(t, duck, dirty, []string{fallbackGlob})

	opts := &model.FederatedQueryOptions{
		IncludeExecutionPlan: true,
		ExecutionPlan: &model.ExecutionPlan{
			Timings: map[string]int64{"caller_pre_pass": 7},
			Notes:   []string{"caller pre-pass note"},
			Sources: []model.DataSourcePlan{{
				Tier: model.DataTierHot, Engine: "postgres", Reason: "caller pre-pass source",
			}},
		},
	}
	_, err := engine.Query(context.Background(), emptyScanTables, allTierQuery(), opts)
	require.NoError(t, err)
	require.Equal(t, 2, duck.calls, "the rewind under test is only reached on the retry path")

	pre := 0
	for _, src := range opts.ExecutionPlan.Sources {
		if src.Reason == "caller pre-pass source" {
			pre++
		}
	}
	require.Equal(t, 1, pre, "a caller-recorded pre-pass source must survive the rewind")
	require.Contains(t, opts.ExecutionPlan.Notes, "caller pre-pass note",
		"a caller-recorded pre-pass note must survive the rewind")
	require.Equal(t, int64(7), opts.ExecutionPlan.Timings["caller_pre_pass"],
		"a caller-recorded pre-pass timing must survive the snapshot-restore")
}

// TestEmptyScanRacedTwiceFailsTheRead: a retry that races a publication
// again fails the read as a degradable ErrFederatedReadFailed rather than
// looping or answering short, and still records no breaker failure.
func TestEmptyScanRacedTwiceFailsTheRead(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &fakeDuckDBExecutor{globFiles: []string{}, rows: &rowPerScan{singleDuckDBRow{rowID: uuid.New()}}}
	src := &recheckFailingSource{fakeParquetSource: fakeParquetSource{paths: []string{fallbackGlob}}}
	engine := newEmptyScanTestEngineWithSource(t, hybridDuckConfig(), duck, &fakeDirtyIDFetcher{}, src)

	_, _, err := engine.ExecuteDuckDBFederatedQuery(context.Background(), emptyScanTables, allTierQuery(), 10, 0, nil, nil)
	var raced *parquetPublishedDuringReadError
	require.ErrorAs(t, err, &raced)
	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.Equal(t, 2, duck.calls, "one retry, no loop")
	require.False(t, engine.breaker.IsOpen(), "a race is not a DuckDB failure")
}

// recheckFailingSource resolves the fallback glob on a pass's first
// resolution and fails every recheck, as a store would that a writer keeps
// publishing to.
type recheckFailingSource struct {
	fakeParquetSource
}

func (s *recheckFailingSource) Paths(ctx context.Context, schemaID int16) ([]string, map[string]map[string]string, error) {
	s.pathsCalls++
	if s.pathsCalls%2 == 0 {
		return nil, nil, errors.New("manifest changed under the read")
	}
	return s.paths, nil, nil
}

// TestParquetHistoryFromAPublicationTheResolutionMissedRetries: a flush
// that publishes between path resolution and the check leaves a flushed
// entry the check finds. That is not a loss, so the source is resolved again
// and the read retries over the new manifest instead of failing on the
// stale glob.
func TestParquetHistoryFromAPublicationTheResolutionMissedRetries(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &fakeDuckDBExecutor{globFiles: []string{}, rows: &rowPerScan{singleDuckDBRow{rowID: uuid.New()}}}
	src := &fakeParquetSource{paths: []string{fallbackGlob}}
	dirty := &fakeDirtyIDFetcher{history: true}
	dirty.afterHistory = func() { src.paths = []string{testParquetPath} }
	engine := newEmptyScanTestEngineWithSource(t, hybridDuckConfig(), duck, dirty, src)

	page, err := engine.Query(context.Background(), emptyScanTables, allTierQuery(), nil)
	require.NoError(t, err)
	require.Len(t, page.Records, 1)
	require.Equal(t, 1, duck.calls, "the stale glob is never scanned")
	require.Contains(t, duck.lastSQL, testParquetPath)
	require.Equal(t, 1, dirty.historyCalls, "the retry resolves a manifest and asks nothing")
}

// TestNeverFlushedHotExcludedQueryScansNoParquet: a hot-excluded query on a
// never-flushed schema asks for flushed data only, and there is none, so it
// takes the empty scan too and answers an empty page.
func TestNeverFlushedHotExcludedQueryScansNoParquet(t *testing.T) {
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
	require.Equal(t, 1, dirty.historyCalls)
}

// TestEmptyFallbackGlobKeepsTheScanWhenItCannotProveNoParquet covers the
// cases where an empty scan would hide data or rests on no evidence. Each
// keeps today's read_parquet over the resolved set.
func TestEmptyFallbackGlobKeepsTheScanWhenItCannotProveNoParquet(t *testing.T) {
	cases := []struct {
		name      string
		paths     []string
		globFiles []string
		history   bool
		wantCheck bool
	}{
		{
			// A lost manifest over flushed rows, or over rows a cdc-init
			// export left without a change_log entry: PostgreSQL records a
			// publication, so the read must still fail on the empty glob.
			name: "parquet history", paths: []string{fallbackGlob}, globFiles: []string{},
			history: true, wantCheck: true,
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
			dirty := &fakeDirtyIDFetcher{history: tc.history}
			engine := newEmptyScanTestEngine(t, duck, dirty, tc.paths)

			_, err := engine.Query(context.Background(), emptyScanTables, allTierQuery(), nil)
			require.NoError(t, err, "the fake scan succeeds; only the rendered SQL is under test")
			require.Contains(t, duck.lastSQL, "read_parquet(")
			require.NotContains(t, duck.lastSQL, emptyScanMarker)
			require.Equal(t, tc.wantCheck, dirty.historyCalls > 0)
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
	require.Zero(t, dirty.historyCalls)
}

// TestParquetOnlyCheckFailureFailsTheRead: without the PostgreSQL check the
// engine cannot tell a never-flushed schema from a lost manifest, so it
// neither scans nor answers. The failure is the Postgres side's.
func TestParquetOnlyCheckFailureFailsTheRead(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &fakeDuckDBExecutor{globFiles: []string{}}
	dirty := &fakeDirtyIDFetcher{historyErr: errors.New("connection reset")}
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
