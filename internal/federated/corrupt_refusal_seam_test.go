package federated

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// The refusal seam resolves a set of readable objects plus objects that fail
// verification, so a confirmation has the readable remainder it requires
// (confirmCorruptPaths) and a test can tell the corrupt object from the rest
// by substring.
const (
	retryKeptPathA    = "s3://b/7/keep-a.parquet"
	retryKeptPathB    = "s3://b/7/keep-b.parquet"
	retryCorruptPath1 = "s3://b/7/rot1.parquet"
)

// retryPass describes what the duck does on one main-scan attempt.
type retryPass struct {
	// midStreamFail opens the scan, hands the caller one row, then fails
	// while iterating — the #285-sensitive branch, and the only shape that
	// can prove the retry does not double-deliver rows.
	midStreamFail bool
	// drainFails lists path substrings whose per-object verification drain
	// fails during THIS pass.
	drainFails []string
}

// retryFakeDuck routes three kinds of SQL: schema probes (delegated to the
// standard engine fake), per-object verification drains, and main scans. It
// scripts behavior per pass so a whole request is one table.
type retryFakeDuck struct {
	fakeDuckDBExecutor
	passes []retryPass

	mainSQL []string // one entry per main-scan attempt
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
	if isGuardedParquetDrainSQL(sql) {
		// #351 guarded drains stay off both odometers: this fake scripts main
		// scans and the bare #251 verification drains (drainsAtClose is the
		// #285 ordering proof and counts exactly those). Without this branch a
		// guarded drain would fall through to the main-scan arm and silently
		// advance the pass index.
		return &verifyFakeRows{rowsLeft: 1}, nil
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

var corruptTables = model.StorageTables{EntityMain: "main", EAVData: "eav", ChangeLog: "change_log"}

// requireCorruptRefusal asserts err is the #722 refusal naming exactly path,
// and that it stays degradable: AllowPartialDegradedMode must be able to
// answer it from PostgreSQL.
func requireCorruptRefusal(t *testing.T, err error, path string) {
	t.Helper()
	var corrupt *CorruptParquetError
	require.ErrorAs(t, err, &corrupt)
	require.Equal(t, []string{path}, corrupt.Paths)
	require.Equal(t, int16(7), corrupt.SchemaID)
	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.True(t, degradableFederatedError(err), "a corrupt-object refusal must stay degradable")
}

// TestConfirmedCorruptionFailsThePassWithoutRetry pins #722 at the scan:
// a mid-stream read failure over a source-authored set is verified and
// attributed to one object, and the read fails instead of scanning the
// readable remainder. A remainder retry would answer a row whose newest
// version (an update or a tombstone) lives only in the corrupt object with
// the older version a readable object holds.
func TestConfirmedCorruptionFailsThePassWithoutRetry(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &retryFakeDuck{passes: []retryPass{
		{midStreamFail: true, drainFails: []string{retryCorruptPath1}},
		{}, // a retry would succeed over the remainder, and this test would see no error
	}}
	e := newRetryEngine(t, duck, []string{retryKeptPathA, retryCorruptPath1})

	page, err := e.Query(context.Background(), corruptTables, coldTierQuery(), &model.FederatedQueryOptions{})
	require.Nil(t, page, "the row the failed pass already read must not be returned")
	requireCorruptRefusal(t, err, retryCorruptPath1)
	require.Len(t, duck.mainSQL, 1, "confirmed corruption earns no retry over the remainder")

	// #285: the failed scan's connection is released before verification runs.
	require.True(t, duck.closeSeen, "the failed pass's iterator must be closed")
	require.Zero(t, duck.drainsAtClose,
		"rows.Close() must precede the verification drains — the engine pool holds a single connection")
}

// TestCachedCorruptObjectRefusesWithoutScan pins the cached half of #722:
// while a confirmed-corrupt object is cached, a set that lists it fails at
// path resolution. No main scan, verification drain or schema probe runs, so
// the retention window does not repeat the failure-path cost per query.
func TestCachedCorruptObjectRefusesWithoutScan(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &retryFakeDuck{passes: []retryPass{{}}}
	e := newRetryEngine(t, duck, []string{retryKeptPathA, retryCorruptPath1})
	e.corruptPaths.Add([]string{retryCorruptPath1})

	_, err := e.Query(context.Background(), corruptTables, coldTierQuery(), nil)
	requireCorruptRefusal(t, err, retryCorruptPath1)
	require.Empty(t, duck.mainSQL, "a cached verdict must refuse before the scan")
	require.Zero(t, duck.drains, "a cached verdict must not re-verify the set")
	require.Zero(t, duck.describeCalls, "a cached verdict must refuse before the pre-read probes")
}

// TestCachedCorruptRefusalIsBreakerNeutral: the refusal happens before DuckDB
// is consulted, so it hands the admission back instead of recording a
// failure. Otherwise one corrupt object would open the engine-wide breaker
// and turn every schema's federated reads away for the open period.
func TestCachedCorruptRefusalIsBreakerNeutral(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &retryFakeDuck{passes: []retryPass{{}}}
	breaker := NewCircuitBreaker(1, time.Minute, time.Minute)
	e := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, &fakeDirtyIDFetcher{}, duck, breaker,
		hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x",
		WithParquetSource(&fakeParquetSource{paths: []string{retryKeptPathA, retryCorruptPath1}}))
	e.corruptPaths.Add([]string{retryCorruptPath1})

	for i := 0; i < 3; i++ {
		_, err := e.Query(context.Background(), corruptTables, coldTierQuery(), nil)
		requireCorruptRefusal(t, err, retryCorruptPath1)
	}
	require.False(t, breaker.IsOpen(), "threshold 1: a single recorded failure would have opened it")
}

// TestResolveParquetPathsCachedCorruptClassifiesMissingFirst pins
// missing-before-corrupt on the cached refusal (#722 review): a listed object
// absent from storage, the cached one or any other, is manifest inconsistency
// and must not be absorbed by the degradable corruption refusal. The probe
// covers the whole listed set, and a probe that cannot run proves no loss.
func TestResolveParquetPathsCachedCorruptClassifiesMissingFirst(t *testing.T) {
	listed := []string{"s3://b/a.parquet", "s3://b/bad.parquet"}
	for name, missing := range map[string][]string{
		"another listed object deleted": {"a.parquet"},
		"the cached object deleted":     {"bad.parquet"},
	} {
		t.Run(name, func(t *testing.T) {
			src := &fakeParquetSource{paths: listed, missing: missing}
			e := newExclusionTestEngine(src)
			e.corruptPaths.Add([]string{"s3://b/bad.parquet"})

			_, _, _, err := e.resolveParquetPaths(context.Background(), exclusionTestQuery())
			var inconsistent *ParquetSetInconsistentError
			require.ErrorAs(t, err, &inconsistent)
			require.Equal(t, missing, inconsistent.MissingKeys)
			require.NotErrorIs(t, err, ErrFederatedReadFailed)
			require.False(t, degradableFederatedError(err), "a lost object must not degrade behind a cached verdict")
			require.Equal(t, listed, src.lastScanned, "the probe covers the whole listed set")
		})
	}

	t.Run("probe error keeps the refusal", func(t *testing.T) {
		e := newExclusionTestEngine(&fakeParquetSource{paths: listed, missingErr: fmt.Errorf("connection refused")})
		e.corruptPaths.Add([]string{"s3://b/bad.parquet"})

		_, _, _, err := e.resolveParquetPaths(context.Background(), exclusionTestQuery())
		var corrupt *CorruptParquetError
		require.ErrorAs(t, err, &corrupt)
		require.True(t, degradableFederatedError(err))
	})
}

// TestCachedCorruptRefusalSurfacesAnotherMissingObject pins the #722 review
// finding at the engine: while A's verdict is cached, a different listed
// object B deleted from storage is manifest inconsistency. It fails with
// degraded mode off and on, and it records a breaker failure, as the same
// loss found by a failed scan would.
func TestCachedCorruptRefusalSurfacesAnotherMissingObject(t *testing.T) {
	for _, degraded := range []bool{false, true} {
		t.Run(fmt.Sprintf("degraded=%v", degraded), func(t *testing.T) {
			restore := initTestDescriptors()
			defer restore()

			duck := &retryFakeDuck{passes: []retryPass{{}}}
			pg := &fakePostgresFederatedSource{}
			breaker := NewCircuitBreaker(1, time.Minute, time.Minute)
			src := &fakeParquetSource{
				paths:   []string{retryKeptPathA, retryCorruptPath1},
				missing: []string{"7/keep-a.parquet"},
			}
			e := NewDBFederatedQueryEngine(pg, &fakeDirtyIDFetcher{}, duck, breaker,
				hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x", WithParquetSource(src))
			e.corruptPaths.Add([]string{retryCorruptPath1})

			opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: degraded}
			_, err := e.Query(context.Background(), corruptTables, coldTierQuery(), opts)
			var inconsistent *ParquetSetInconsistentError
			require.ErrorAs(t, err, &inconsistent)
			require.Equal(t, []string{"7/keep-a.parquet"}, inconsistent.MissingKeys)
			var corrupt *CorruptParquetError
			require.False(t, errors.As(err, &corrupt), "the cached verdict must not mask the loss")
			require.Zero(t, pg.queryCalls, "inconsistency is non-degradable")
			require.Empty(t, duck.mainSQL, "the cached verdict still refuses before the scan")
			require.True(t, breaker.IsOpen(), "threshold 1: the loss records a breaker failure")
		})
	}
}

// budgetBlockedParquetSource lists its paths but holds every missing-object
// probe until the request's context is done, standing in for existence
// checks that run past the query budget.
type budgetBlockedParquetSource struct {
	fakeParquetSource
}

func (s *budgetBlockedParquetSource) MissingIn(ctx context.Context, scanned []string) ([]string, error) {
	s.missingCalls++
	<-ctx.Done()
	return nil, fmt.Errorf("probe %d parquet objects: %w", len(scanned), ctx.Err())
}

// TestMissingObjectProbePreservesTheExpiredBudget pins the #722 review
// finding: when the missing-object probe runs into the request's deadline,
// the failure still reports context.DeadlineExceeded and does not degrade
// (#465). The probe proves no loss, so the classification is unchanged. That
// holds for a cached refusal, whose cause chain is otherwise only the
// sentinel, and for a scan that failed for another reason before the probe
// spent the rest of the budget. The Postgres fake ignores its context, so a
// fallback taken on the spent budget would answer instead of failing.
func TestMissingObjectProbePreservesTheExpiredBudget(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, degraded := range []bool{false, true} {
			t.Run(fmt.Sprintf("cached=%v/degraded=%v", cached, degraded), func(t *testing.T) {
				restore := initTestDescriptors()
				defer restore()

				duck := &retryFakeDuck{passes: []retryPass{{midStreamFail: true}}}
				pg := &fakePostgresFederatedSource{}
				src := &budgetBlockedParquetSource{fakeParquetSource{paths: []string{retryKeptPathA, retryCorruptPath1}}}
				cfg := hybridDuckConfig()
				cfg.QueryTimeout = 50 * time.Millisecond
				e := NewDBFederatedQueryEngine(pg, &fakeDirtyIDFetcher{}, duck, nil,
					cfg, testMetadataCacheSchema7(t), "host=x", WithParquetSource(src))
				if cached {
					e.corruptPaths.Add([]string{retryCorruptPath1})
				}

				opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: degraded}
				_, err := e.Query(context.Background(), corruptTables, coldTierQuery(), opts)
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Equal(t, 1, src.missingCalls, "the probe is what ran into the budget")
				require.Zero(t, pg.queryCalls, "a spent budget must not take the Postgres fallback")
				if cached {
					requireCachedRefusalKept(t, err)
					require.Empty(t, duck.mainSQL, "the cached verdict still refuses before the scan")
				}
			})
		}
	}
}

// requireCachedRefusalKept asserts the deadline joined the cached refusal
// rather than replacing it: the corrupt object is still named.
func requireCachedRefusalKept(t *testing.T, err error) {
	t.Helper()
	var corrupt *CorruptParquetError
	require.ErrorAs(t, err, &corrupt)
	require.Equal(t, []string{retryCorruptPath1}, corrupt.Paths)
	require.ErrorIs(t, err, ErrFederatedReadFailed)
}

// TestCorruptRefusalDegradesToPostgresOnly pins the #722 answer under
// AllowPartialDegradedMode, for a fresh confirmation and for a cached
// verdict: the page comes from PostgreSQL alone, which holds every live row
// in its current version, and carries the #468 coverage marker for the cold
// tier the request asked for.
func TestCorruptRefusalDegradesToPostgresOnly(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cached=%v", cached), func(t *testing.T) {
			restore := initTestDescriptors()
			defer restore()

			duck := &retryFakeDuck{passes: []retryPass{
				{midStreamFail: true, drainFails: []string{retryCorruptPath1}},
			}}
			pg := &fakePostgresFederatedSource{}
			e := NewDBFederatedQueryEngine(pg, &fakeDirtyIDFetcher{}, duck, nil,
				hybridDuckConfig(), testMetadataCacheSchema7(t), "host=x",
				WithParquetSource(&fakeParquetSource{paths: []string{retryKeptPathA, retryCorruptPath1}}))
			if cached {
				e.corruptPaths.Add([]string{retryCorruptPath1})
			}

			opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: true}
			page, err := e.Query(context.Background(), corruptTables, coldTierQuery(), opts)
			require.NoError(t, err, "degraded mode must absorb the refusal")
			require.Equal(t, 1, pg.queryCalls, "the answer is the Postgres-only read")
			require.NotNil(t, page.Partial, "a postgres-only answer to a hot+cold request is marked hot-tier-only (#468)")
			require.Equal(t, []model.DataTier{model.DataTierCold}, page.Partial.UnconsultedTiers)
			require.Same(t, page.Partial, opts.PartialScan, "the out-parameter must agree with the page")
		})
	}
}

// TestCleanQueryPageHasNoPartialMarker is the negative leg: a scan over the
// full resolved set is not partial, and the pass clears a marker that a
// reused options value still carries from an earlier answer.
func TestCleanQueryPageHasNoPartialMarker(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &retryFakeDuck{passes: []retryPass{{}}}
	e := newRetryEngine(t, duck, []string{retryKeptPathA, retryKeptPathB})

	opts := &model.FederatedQueryOptions{
		PartialScan: &model.PartialScan{UnconsultedTiers: []model.DataTier{model.DataTierCold}},
	}
	page, err := e.Query(context.Background(), corruptTables, coldTierQuery(), opts)
	require.NoError(t, err)
	require.Nil(t, page.Partial, "a full-set scan must not carry a partial marker")
	require.Nil(t, opts.PartialScan, "the out-parameter must describe THIS call, not the previous one")
}

// TestPostgresOnlyAnswerClearsReusedPartialMarker pins the PR #412 review
// observation: the hot-only gate never runs a DuckDB pass, so without the
// Query entry reset a caller reusing one options value across queries would
// read the PREVIOUS call's marker after a postgres-only answer.
func TestPostgresOnlyAnswerClearsReusedPartialMarker(t *testing.T) {
	restore := initTestDescriptors()
	defer restore()

	duck := &retryFakeDuck{passes: []retryPass{{}}}
	e := newRetryEngine(t, duck, []string{retryKeptPathA, retryKeptPathB})

	opts := &model.FederatedQueryOptions{
		PartialScan: &model.PartialScan{UnconsultedTiers: []model.DataTier{model.DataTierCold}},
	}
	hotOnly := coldTierQuery()
	hotOnly.PreferredTiers = []model.DataTier{model.DataTierHot}
	page, err := e.Query(context.Background(), corruptTables, hotOnly, opts)
	require.NoError(t, err)
	require.Nil(t, page.Partial, "a hot-only page never carries the marker")
	require.Nil(t, opts.PartialScan, "the out-parameter must describe THIS call, not the previous one")
}
