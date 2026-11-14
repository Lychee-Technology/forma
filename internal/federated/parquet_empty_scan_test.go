package federated

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// The #720 empty path set seen through the engine: a schema whose
// source-authored glob lists no object. pgx_pass_inventory_test.go covers the
// pass itself (the hot leg answers, parquet history fails the read, a racing
// publication is retried); these tests cover the request around it.

// fallbackGlob is the shape of the level-3 glob a source authors for a
// schema with no manifest entries (manifest.S3FallbackGlob).
const fallbackGlob = "s3://b/7/*.parquet"

var emptyScanTables = model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"}

func allTierQuery() *model.FederatedAttributeQuery {
	return &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 2000},
		PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold},
	}
}

func newEmptyScanTestEngine(t *testing.T, duck *fakeDuckDBExecutor, leg *fakeHotLeg, paths []string, opts ...EngineOption) *DBFederatedQueryEngine {
	t.Helper()
	return newEmptyScanTestEngineWithSource(t, hybridDuckConfig(), duck, leg, &fakeParquetSource{paths: paths}, opts...)
}

// newEmptyScanTestEngineWithSource builds the engine over src, so a test can
// change what the source resolves mid-query. The breaker opens on one
// failure.
func newEmptyScanTestEngineWithSource(t *testing.T, cfg forma.DuckDBConfig, duck *fakeDuckDBExecutor, leg *fakeHotLeg, src ParquetSource, opts ...EngineOption) *DBFederatedQueryEngine {
	t.Helper()
	opts = append(opts, WithParquetSource(src), withFakeHotLeg(leg))
	return NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, duck,
		NewCircuitBreaker(1, time.Minute, time.Minute),
		cfg, testMetadataCacheSchema7(t), opts...)
}

// rowPerScan answers every main scan with one row: Close rewinds it, so a
// retried pass reads a row again.
type rowPerScan struct{ singleDuckDBRow }

func (r *rowPerScan) Close() error {
	r.scanned = false
	return nil
}

// racingPublicationFixture is a never-flushed schema whose first pass a
// publication races: the glob lists no object at resolution, the snapshot
// records no parquet history, and the object lands before the pass ends, so
// the pass is discarded and the retry scans it.
func racingPublicationFixture(t *testing.T) (*pgxFixture, *DBFederatedQueryEngine) {
	t.Helper()
	rows := append(hotOnlyRows(), pgxRow{i: 4, createdAt: 40, age: ageOf(30), parquet: true})
	f := newPgxFixture(t, rows)
	require.NoError(t, os.Remove(filepath.Join(f.dir, "base.parquet")))
	published := false
	f.leg.afterChangeLog = func() {
		if !published {
			published = true
			f.writeParquet(t, "first.parquet", []pgxRow{rows[3]})
		}
	}
	breaker := NewCircuitBreaker(1, time.Minute, time.Minute)
	e := f.engine(t, []string{filepath.Join(f.dir, "*.parquet")})
	e.breaker = breaker
	return f, e
}

// TestEmptyScanRetriesWhenAPublicationRacesIt (#720 review): a flush or a
// cdc-init handoff that publishes after the check can move rows out of the
// hot leg, and the pass that found no object cannot read what it published.
// The recheck after the pass sees the publication, so the page is discarded
// and the retry scans the new object. The plan describes the retry only, and
// the breaker records no failure.
func TestEmptyScanRetriesWhenAPublicationRacesIt(t *testing.T) {
	f, e := racingPublicationFixture(t)

	opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true,
		ExecutionPlan: &model.ExecutionPlan{Timings: map[string]int64{}}}
	page, err := e.Query(context.Background(), tieRunTables, pgxRoutedQuery(0, 5), opts)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{parquetOnlyRowID(4), parquetOnlyRowID(2), parquetOnlyRowID(3), parquetOnlyRowID(1)}, recordIDs(page.Records))
	require.Equal(t, 2, f.leg.begins, "the raced pass and its retry")
	require.NotContains(t, opts.ExecutionPlan.Notes, NoteEmptyParquetScan, "the plan describes the retry only")
	require.Equal(t, 1, countPlanSources(opts.ExecutionPlan, "duckdb"),
		"the plan must describe the pass that produced the page, not both passes")
	require.Equal(t, 2, countPlanSources(opts.ExecutionPlan, "postgres"),
		"a retried request must not double-count the dirty set or the hot leg")
	require.False(t, e.breaker.IsOpen(), "the breaker opens on one failure, so the race recorded none")
}

// TestRetryRewindPreservesPrePassPlan pins #348 item 2 on the retry that
// remains (#720): rewind is a truncate-to-mark, and the strongest wrong
// implementation — rewinding to an empty mark (executionPlanMark{}) —
// destroys what the CALLER recorded before the first pass (e.g. the keyset
// coordinator's "keyset pagination" note). Routing alone cannot catch that:
// rewind never touches Routing.
func TestRetryRewindPreservesPrePassPlan(t *testing.T) {
	f, e := racingPublicationFixture(t)

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
	_, err := e.Query(context.Background(), tieRunTables, pgxRoutedQuery(0, 5), opts)
	require.NoError(t, err)
	require.Equal(t, 2, f.leg.begins, "the rewind under test is only reached on the retry path")

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

// countPlanSources counts the plan's sources read by engine.
func countPlanSources(plan *model.ExecutionPlan, engine string) int {
	n := 0
	for _, src := range plan.Sources {
		if src.Engine == engine {
			n++
		}
	}
	return n
}

// TestEmptyScanRacedTwiceFailsTheRead: a retry that races a publication
// again fails the read as a degradable ErrFederatedReadFailed rather than
// looping or answering short, and still records no breaker failure.
func TestEmptyScanRacedTwiceFailsTheRead(t *testing.T) {
	f := newPgxFixture(t, hotOnlyRows())
	f.leg.history = true
	src := &recheckFailingSource{fakeParquetSource: fakeParquetSource{paths: []string{filepath.Join(f.dir, "*.parquet")}}}
	e := f.engine(t, nil, WithParquetSource(src))
	e.breaker = NewCircuitBreaker(1, time.Minute, time.Minute)

	_, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 10, 0, nil, nil)
	var raced *parquetPublishedDuringReadError
	require.ErrorAs(t, err, &raced)
	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.Equal(t, 2, f.leg.begins, "one retry, no loop")
	require.False(t, e.breaker.IsOpen(), "a race is not a DuckDB failure")
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

// TestNeverFlushedHotExcludedQueryScansNoParquet: a hot-excluded query on a
// never-flushed schema asks for flushed data only, and there is none, so it
// runs no DuckDB statement and answers an empty page.
func TestNeverFlushedHotExcludedQueryScansNoParquet(t *testing.T) {
	f := newPgxFixture(t, hotOnlyRows())
	duck := &fakeDuckDBExecutor{globFiles: []string{}}
	e := f.engineOver(t, stagedFake{duck}, []string{fallbackGlob})

	q := pgxQuery()
	q.PreferredTiers = []model.DataTier{model.DataTierWarm, model.DataTierCold}
	records, total, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, q, 10, 0, nil, nil)
	require.NoError(t, err)
	require.Empty(t, records)
	require.Zero(t, total)
	require.Zero(t, duck.calls, "no parquet statement runs")
	require.Equal(t, 1, f.leg.historyChecks)
	require.Nil(t, f.leg.lastHotKeys, "a hot-excluded pass has no hot leg")
}

// TestEmptyFallbackGlobKeepsTheScanWhenItCannotProveNoParquet covers the
// path sets that are not empty: each keeps the scan, and none asks
// PostgreSQL for parquet history. (A failed listing fails the pass:
// TestPgxPassFailedListingFailsThePass; history over an empty set fails the
// read: TestPgxPassWithHistoryButNoObjectFails.)
func TestEmptyFallbackGlobKeepsTheScanWhenItCannotProveNoParquet(t *testing.T) {
	cases := []struct {
		name      string
		paths     []string
		globFiles []string
	}{
		{name: "glob matches an object", paths: []string{fallbackGlob}, globFiles: []string{"s3://b/7/a.parquet"}},
		{name: "manifest-listed objects", paths: []string{testParquetPath}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			restore := initTestDescriptors()
			defer restore()

			duck := &fakeDuckDBExecutor{globFiles: tc.globFiles, rows: &singleDuckDBRow{rowID: uuid.New()}}
			leg := &fakeHotLeg{}
			engine := newEmptyScanTestEngine(t, duck, leg, tc.paths)

			_, err := engine.Query(context.Background(), emptyScanTables, allTierQuery(), nil)
			require.NoError(t, err, "the fake scan succeeds; only the rendered SQL is under test")
			require.Contains(t, duck.lastSQL, "read_parquet(")
			require.Zero(t, leg.historyChecks)
		})
	}
}

// TestHintGlobMatchingNothingIsNotTreatedAsNeverFlushed: a caller hint names
// objects no manifest vouches for, so an empty hint glob is not frozen into
// an empty set: it reaches the scan, which reports it, and PostgreSQL is not
// asked for parquet history.
func TestHintGlobMatchingNothingIsNotTreatedAsNeverFlushed(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &fakeDuckDBExecutor{globFiles: []string{}, rows: &singleDuckDBRow{rowID: uuid.New()}}
	leg := &fakeHotLeg{}
	engine := newEmptyScanTestEngine(t, duck, leg, []string{testParquetPath})

	q := allTierQuery()
	q.DuckDBHints = &model.DuckDBRenderHints{S3ParquetPathTemplate: "s3://b/hinted/{{.SchemaID}}/*.parquet"}
	_, err := engine.Query(context.Background(), emptyScanTables, q, nil)
	require.NoError(t, err)
	require.Contains(t, duck.lastSQL, "s3://b/hinted/7/*.parquet")
	require.Zero(t, leg.historyChecks)
}
