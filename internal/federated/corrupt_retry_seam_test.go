package federated

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/stretchr/testify/require"
)

// The retry seam needs a four-object set: two readable objects so the
// remainder can still answer, and two separately-failing objects so a SECOND
// retryable failure can be staged without reusing the first one.
const (
	retryKeptPathA    = "s3://b/7/keep-a.parquet"
	retryKeptPathB    = "s3://b/7/keep-b.parquet"
	retryCorruptPath1 = "s3://b/7/rot1.parquet"
	retryCorruptPath2 = "s3://b/7/rot2.parquet"
)

// retryPass describes what the duck does on one main-scan attempt.
type retryPass struct {
	// midStreamFail opens the scan, hands the caller one row, then fails
	// while iterating — the #285-sensitive branch, and the only shape that
	// can prove the retry does not double-deliver rows.
	midStreamFail bool
	// drainFails lists path substrings whose per-object verification drain
	// fails during THIS pass, so "corruption appearing mid-flight" is
	// expressible.
	drainFails []string
}

// retryFakeDuck routes three kinds of SQL: schema probes (delegated to the
// standard engine fake), per-object verification drains, and main scans. It
// scripts behavior per pass so a whole retry sequence is one table.
type retryFakeDuck struct {
	fakeDuckDBExecutor
	passes []retryPass

	mainSQL []string // one entry per main-scan attempt: the retry's odometer
	drains  int
	// drainsAtClose records how many verification drains had run when the
	// failed scan's iterator was first closed. #285 requires zero: the
	// engine pool holds a single connection, so the rows must be released
	// BEFORE verification issues its own queries.
	drainsAtClose int
	closeSeen     bool
}

func (d *retryFakeDuck) pass() retryPass {
	i := len(d.mainSQL) - 1
	if i < 0 || i >= len(d.passes) {
		return retryPass{}
	}
	return d.passes[i]
}

func (d *retryFakeDuck) noteClose() {
	if d.closeSeen {
		return
	}
	d.closeSeen = true
	d.drainsAtClose = d.drains
}

func (d *retryFakeDuck) Query(ctx context.Context, sql string, args ...any) (duckDBRowsIterator, error) {
	if strings.HasPrefix(sql, "DESCRIBE ") || strings.HasPrefix(sql, "SELECT file FROM glob(") {
		return d.fakeDuckDBExecutor.Query(ctx, sql, args...)
	}
	if strings.HasPrefix(sql, "SELECT * FROM read_parquet(") {
		d.drains++
		for _, token := range d.pass().drainFails {
			if strings.Contains(sql, token) {
				return nil, fmt.Errorf("IO Error: cannot open %s", token)
			}
		}
		return &verifyFakeRows{rowsLeft: 1}, nil
	}
	d.mainSQL = append(d.mainSQL, sql)
	if d.pass().midStreamFail {
		return &retryMidStreamRows{duck: d}, nil
	}
	return &singleDuckDBRow{rowID: uuid.New()}, nil
}

// retryMidStreamRows yields one row and then reports a read failure, the
// shape DuckDB produces when a corrupt page is reached after rows have
// already been handed to the caller.
type retryMidStreamRows struct {
	singleDuckDBRow
	duck *retryFakeDuck
}

func (r *retryMidStreamRows) Err() error   { return fmt.Errorf("parquet page decode failed") }
func (r *retryMidStreamRows) Close() error { r.duck.noteClose(); return nil }

func newRetryEngine(t *testing.T, duck DuckDBQueryExecutor, paths []string) *DBFederatedQueryEngine {
	t.Helper()
	return newParquetSourceTestEngine(t, duck, &fakeParquetSource{paths: paths})
}

func countSources(plan *model.ExecutionPlan, reason string) int {
	n := 0
	for _, src := range plan.Sources {
		if src.Reason == reason {
			n++
		}
	}
	return n
}

// TestCorruptRetryAnswersFromReadableRemainder is the only test that exercises
// Task 6's behavioural payload end to end: a mid-stream read failure over a
// source-authored set is verified, attributed to one object, and answered by a
// SECOND scan of the remainder. Delete the retry from
// ExecuteDuckDBFederatedQuery and the request fails outright; share the record
// slice across passes and the page carries the failed pass's row as well.
func TestCorruptRetryAnswersFromReadableRemainder(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &retryFakeDuck{passes: []retryPass{
		{midStreamFail: true, drainFails: []string{retryCorruptPath1}},
	}}
	e := newRetryEngine(t, duck, []string{retryKeptPathA, retryCorruptPath1})

	opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true}
	page, err := e.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), opts)
	require.NoError(t, err, "the readable remainder must still answer the query")

	require.Len(t, duck.mainSQL, 2, "exactly two main scans: the failed pass and one retry")
	require.Contains(t, duck.mainSQL[0], retryCorruptPath1, "the first pass scans the full set")
	require.NotContains(t, duck.mainSQL[1], retryCorruptPath1,
		"the retry must scan the remainder — the confirmed-corrupt object is excluded at resolution")
	require.Contains(t, duck.mainSQL[1], retryKeptPathA)

	require.Len(t, page.Records, 1,
		"each pass buffers into a fresh slice: the row the failed pass already delivered must not survive into the page")

	// #285: the failed scan's connection is released before verification runs.
	require.True(t, duck.closeSeen, "the failed pass's iterator must be closed")
	require.Zero(t, duck.drainsAtClose,
		"rows.Close() must precede the verification drains — the engine pool holds a single connection")
}

// TestCorruptRetryPlanDescribesOnlyTheRetryPass pins the diagnostic surface:
// both passes share one *FederatedQueryOptions and the plan recorder only ever
// appends, so without a rewind the caller receives two identically-labelled
// DuckDB scans (the failed one reporting ActualRows=0) and a double-counted
// hot-tier estimate — with the explanatory Notes stripped at the HTTP boundary.
func TestCorruptRetryPlanDescribesOnlyTheRetryPass(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &retryFakeDuck{passes: []retryPass{
		{midStreamFail: true, drainFails: []string{retryCorruptPath1}},
	}}
	e := newRetryEngine(t, duck, []string{retryKeptPathA, retryCorruptPath1})

	opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true}
	_, err := e.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), opts)
	require.NoError(t, err)
	require.NotNil(t, opts.ExecutionPlan)

	require.Equal(t, 1, countSources(opts.ExecutionPlan, "duckdb template rendered"),
		"the plan must describe the pass that produced the page, not both passes")
	require.Equal(t, 1, countSources(opts.ExecutionPlan, "dirty id set fetched"),
		"a retried request must not double-count the hot-tier row estimate")
	for _, src := range opts.ExecutionPlan.Sources {
		require.NotContains(t, src.SQL, retryCorruptPath1,
			"no surviving plan source may advertise a scan of the excluded object")
	}
	// The rewind must not swallow what the caller recorded before the first
	// pass: routing is decided in Query, above the retry.
	require.True(t, opts.ExecutionPlan.Routing.UseDuckDB,
		"the routing decision predates the failed pass and must survive the rewind")
	requireExclusionNote(t, opts.ExecutionPlan.Notes, retryCorruptPath1)
}

// TestCorruptRetryStopsAfterOneRetry is the boundedness leg. Corruption is
// staged to appear on the retry too, so the second pass ALSO returns a
// retryable error — the one input that tells a single `if` apart from a loop.
// Exactly two main scans may run, and the failure must surface wrapped.
func TestCorruptRetryStopsAfterOneRetry(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &retryFakeDuck{passes: []retryPass{
		{midStreamFail: true, drainFails: []string{retryCorruptPath1}},
		{midStreamFail: true, drainFails: []string{retryCorruptPath2}},
		// A third pass would succeed — so a loop would answer the query and
		// this test would see three scans and no error.
	}}
	e := newRetryEngine(t, duck,
		[]string{retryKeptPathA, retryKeptPathB, retryCorruptPath1, retryCorruptPath2})

	_, err := e.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), nil)

	require.Error(t, err, "a second retryable failure must surface, not spawn another retry")
	require.Contains(t, err.Error(), "retry after excluding corrupt parquet",
		"the surfaced error must name the exclusion that was already attempted")
	require.Contains(t, err.Error(), retryCorruptPath1,
		"the wrap must carry the first pass's attribution")
	require.Len(t, duck.mainSQL, 2, "exactly one retry: two main scans, never three")
}

// TestCorruptRetryPageCarriesPartialMarker pins the #348 engine seam of the
// public partial contract: the page produced by the post-exclusion retry must
// carry the excluded object set — and it must do so WITHOUT
// IncludeExecutionPlan, because the public marker exists precisely for
// callers that never asked for a plan.
func TestCorruptRetryPageCarriesPartialMarker(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &retryFakeDuck{passes: []retryPass{
		{midStreamFail: true, drainFails: []string{retryCorruptPath1}},
	}}
	e := newRetryEngine(t, duck, []string{retryKeptPathA, retryCorruptPath1})

	page, err := e.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), &model.FederatedQueryOptions{})
	require.NoError(t, err)
	require.NotNil(t, page.Partial,
		"a page answered from the readable remainder must be marked partial")
	require.Equal(t, []string{retryCorruptPath1}, page.Partial.ExcludedObjects)
}

// TestCleanQueryPageHasNoPartialMarker is the negative leg: a scan over the
// full resolved set must not be marked partial.
func TestCleanQueryPageHasNoPartialMarker(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &retryFakeDuck{passes: []retryPass{{}}}
	e := newRetryEngine(t, duck, []string{retryKeptPathA, retryKeptPathB})

	page, err := e.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"},
		coldTierQuery(), &model.FederatedQueryOptions{})
	require.NoError(t, err)
	require.Nil(t, page.Partial, "a full-set scan must not carry a partial marker")
}

// TestCorruptRetryTimingsDescribeOnlyTheRetryPass pins #348 item 1: Timings
// IS projected through toExecutionPlan (unlike Notes), so stale keys from the
// failed pass are publicly visible. The plan cache keys on the resolved path
// set (#255), which makes the conflicting pair deterministic: request 1 warms
// the cache for the full set, request 2's failed pass HITs that entry, and
// its retry compiles the never-seen remainder set — a MISS. Without a
// Timings rewind one request publicly reports both stamps.
func TestCorruptRetryTimingsDescribeOnlyTheRetryPass(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &retryFakeDuck{passes: []retryPass{
		{}, // request 1: clean full-set pass — warms the plan cache
		{midStreamFail: true, drainFails: []string{retryCorruptPath1}}, // request 2, pass 1
		{}, // request 2, retry over the remainder
	}}
	e := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDirtyIDFetcher{}, duck, nil,
		hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x",
		WithParquetSource(&fakeParquetSource{paths: []string{retryKeptPathA, retryCorruptPath1}}),
		WithPlanCache(queryplan.NewCache(64)))

	tables := model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"}
	_, err := e.Query(context.Background(), tables, coldTierQuery(),
		&model.FederatedQueryOptions{IncludeExecutionPlan: true})
	require.NoError(t, err, "request 1 must succeed and warm the plan cache")

	opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true}
	_, err = e.Query(context.Background(), tables, coldTierQuery(), opts)
	require.NoError(t, err)
	require.Len(t, duck.mainSQL, 3, "one clean scan, one failed scan, one retry")

	_, hitStamp := opts.ExecutionPlan.Timings["plan_cache_hit"]
	_, missStamp := opts.ExecutionPlan.Timings["plan_cache_miss"]
	require.False(t, hitStamp, "the failed pass's cache-hit stamp must not survive the rewind")
	require.True(t, missStamp, "the retry compiled a fresh plan for the remainder set")
}
