//go:build e2e

package production

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestConcurrentFlushSnapshot pins issue #182: rows mutated concurrently with
// a CDC flush must stay dirty and queries must always see the latest version.
//
// The pipeline has two mutation windows, and both are exercised:
//
//   - selection->export ("before export", issue scenario 1's literal pause
//     point): UpdateBeforeExport drives the mutation through
//     cdc.CDCConfig.BeforeExportHook. The export CTE's changed_at <= snapshot
//     filter (internal/cdc/duckdb_exporter.go) must keep the mutated row out
//     of the parquet entirely, and the identical mark-flushed guard
//     (internal/cdc/helpers.go) must keep it dirty.
//   - export->mark-flushed (the harder window: the parquet already carries
//     the stale value and only the mark-flushed guard saves the row):
//     the *AfterSnapshot scenarios hold the flush open with PausingS3 at
//     CopyObject. The visibility probes assert the dirty hot version shadows
//     the exported stale copy (anti-join), and after the retry flush drains
//     the row, LWW on ver_ts keeps suppressing it.
//
// The unit twin of the mark-flushed guard is
// TestMarkFlushedIDsAtSnapshot_SkipsUpdatedRows.
func TestConcurrentFlushSnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	t.Run("UpdateBeforeExport", func(t *testing.T) {
		t.Parallel()
		testUpdateBeforeExport(ctx, t)
	})
	t.Run("UpdateAfterSnapshot", func(t *testing.T) {
		t.Parallel()
		testUpdateAfterSnapshot(ctx, t)
	})
	t.Run("DeleteAfterSnapshot", func(t *testing.T) {
		t.Parallel()
		testDeleteAfterSnapshot(ctx, t)
	})
	t.Run("InsertAfterSnapshot", func(t *testing.T) {
		t.Parallel()
		testInsertAfterSnapshot(ctx, t)
	})
}

const pausedFlushTimeout = 60 * time.Second

type flushOutcome struct {
	report *FlushReport
	err    error
}

// runPausedFlush starts one real flush that pauses at its first CopyObject,
// invokes mutate on the test goroutine while the flush is suspended, then
// resumes the flush and returns its outcome. The seeds stay small and no
// chunking config is set, so the single batch issues exactly one CopyObject
// and the pause fires deterministically once.
func runPausedFlush(ctx context.Context, t *testing.T, env *Env, mutate func()) (*FlushReport, error) {
	t.Helper()
	pauser := NewPausingS3(env.Cluster.S3, S3OpCopy)
	ctx, cancel := context.WithTimeout(ctx, pausedFlushTimeout)

	// The flush goroutine must not touch t; it reports through done (buffered
	// so it never leaks even when the test dies before reading it).
	done := make(chan flushOutcome, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		report, err := env.RunFlushWith(ctx, FlushOverrides{S3: pauser})
		done <- flushOutcome{report: report, err: err}
	}()
	// Registered after NewEnv's cleanups, so this runs first: any early
	// t.Fatal (including inside mutate) releases the paused flush and waits
	// for it to exit while the Env's resources are still alive.
	t.Cleanup(func() {
		pauser.Resume()
		cancel()
		<-finished
	})

	select {
	case <-pauser.Reached():
	case out := <-done:
		t.Fatalf("flush finished before reaching the CopyObject pause: err=%v", out.err)
	}
	mutate()
	pauser.Resume()
	out := <-done
	if out.err != nil {
		return out.report, fmt.Errorf("flush under CopyObject pause: %w", out.err)
	}
	return out.report, nil
}

// runCleanRetry re-runs the flush without any pause and asserts it drains
// every dirty row — the issue's retry-convergence criterion.
func runCleanRetry(ctx context.Context, t *testing.T, env *Env) *FlushReport {
	t.Helper()
	retry, err := env.RunFlushWith(ctx, FlushOverrides{})
	if err != nil {
		t.Fatalf("clean retry flush: %v", err)
	}
	if retry.UnflushedAfter != 0 {
		t.Errorf("retry must drain all dirty rows, unflushed = %d", retry.UnflushedAfter)
	}
	return retry
}

// assertPausedFlushSplit pins the snapshot guard at row level: after the
// paused flush, exactly the mutated row (which WAS in the selected batch)
// stays dirty while its siblings carry a flushed_at marker, and the report's
// counters agree. This is the observable face of "marked fewer rows than
// requested" — the run still succeeds.
func assertPausedFlushSplit(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, report *FlushReport, wantFlushed int, wantDirtyID string) (finals []string) {
	t.Helper()
	flushed, dirty := fetchChangeLogRowIDs(ctx, t, env, wide)
	if len(flushed) != wantFlushed || len(dirty) != 1 || !dirty[wantDirtyID] {
		t.Fatalf("guard must keep exactly the concurrent row dirty: flushed=%v dirty=%v want dirty={%s}",
			flushed, dirty, wantDirtyID)
	}
	if report.UnflushedBefore != 4 || report.UnflushedAfter != 1 {
		t.Errorf("unflushed before/after = %d/%d, want 4/1", report.UnflushedBefore, report.UnflushedAfter)
	}
	finals, _ = splitKeys(report.NewObjects)
	if len(finals) != 1 {
		t.Fatalf("paused flush must promote exactly one final delta, got %v", finals)
	}
	assertManifestDeltaPaths(t, report.Manifests, wide, finals)
	return finals
}

// assertFederatedRowCount is assertRowCount plus a routing check: the flushed
// sibling rows are only served by the parquet side (the hot scan reads
// flushed_at = 0 rows), so the count is meaningful evidence that the cold
// delta was consulted only if the query actually routed to DuckDB.
func assertFederatedRowCount(ctx context.Context, t *testing.T, env *Env, name string, q Query, want int) {
	t.Helper()
	res := env.AssertQueryMatches(ctx, q)
	if res == nil {
		return
	}
	if len(res.Records) != want {
		t.Fatalf("%s returned %d rows, want %d", name, len(res.Records), want)
	}
	if !res.Plan.Routing.UseDuckDB {
		t.Errorf("%s did not route to duckdb: %+v", name, res.Plan.Routing)
	}
}

// assertUpdateRetryConverges runs the clean retry after a deferred concurrent
// update and asserts the issue's convergence criterion: the victim drains as
// exactly one v2 record (payload and ver_ts, not just ID membership) into one
// new delta, the manifest tracks both deltas, and the oracle-checked probes
// see exactly one visible version with v1 unreachable.
func assertUpdateRetryConverges(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, update *Event, priorFinals []string) {
	t.Helper()
	retry := runCleanRetry(ctx, t, env)
	retryFinals, _ := splitKeys(retry.NewObjects)
	if len(retryFinals) != 1 {
		t.Fatalf("retry must promote exactly one more final delta, got %v", retryFinals)
	}
	assertLiveParquetRow(ctx, t, env, retryFinals[0], update)
	assertManifestDeltaPaths(t, retry.Manifests, wide, append(append([]string(nil), priorFinals...), retryFinals...))
	assertFederatedRowCount(ctx, t, env, "converged rows", Query{Schema: wide, Limit: 10}, 4)
	assertRowCount(ctx, t, env, "converged hot value",
		Query{Schema: wide, Filters: []Filter{{Attr: "title", Value: "hot-00"}}, Limit: 10}, 1)
	assertZeroRows(ctx, t, env, "converged stale value",
		Query{Schema: wide, Filters: []Filter{{Attr: "title", Value: "open-00"}}, Limit: 10})
}

// testUpdateBeforeExport is issue #182 scenario 1 at its literal pause point,
// plus scenario 4 asserted against the actual snapshot value: the mutation
// lands after dirty-ID selection and before the DuckDB export, driven through
// cdc.CDCConfig.BeforeExportHook riding the FlushOverrides{Config} injection.
// Both halves of the dual guard must hold: the export CTE's
// changed_at <= snapshot filter keeps the row out of the parquet entirely,
// and the identical mark-flushed guard keeps it dirty.
func testUpdateBeforeExport(ctx context.Context, t *testing.T) {
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	creates := buildOpenCreates(wide, 4)
	mustApplyEvents(ctx, t, env, "seed creates", creates...)
	victim := creates[0]
	assertRowCount(ctx, t, env, "pre-flush rows", Query{Schema: wide, Limit: 10, PreferHot: true}, 4)
	assertRowCount(ctx, t, env, "pre-flush victim by v1",
		Query{Schema: wide, Filters: []Filter{{Attr: "title", Value: "open-00"}}, Limit: 10, PreferHot: true}, 1)

	// RunFlushWith is synchronous, so the hook runs inline on this goroutine,
	// deterministically inside the selection->export window; it reports
	// failures through the flush error rather than t.*.
	var update *Event
	var hookSnapshot int64
	var hookBatch int
	cfg := env.CDC
	cfg.BeforeExportHook = func(hctx context.Context, _ int16, ids []uuid.UUID, snapshot int64) error {
		hookSnapshot = snapshot
		hookBatch = len(ids)
		update = UpdateEvent(wide, victim.RowID, map[string]any{
			"title": "hot-00",
			"count": float64(900000),
		})
		return env.ApplyEvents(hctx, update)
	}
	report, err := env.RunFlushWith(ctx, FlushOverrides{Config: &cfg})
	if err != nil {
		t.Fatalf("flush with before-export mutation: %v", err)
	}

	// Positive control: the hook fired on the full selected batch, and the
	// mutation's changed_at sits strictly past the very snapshot the guards
	// compare against (issue scenario 4, asserted directly).
	if hookBatch != 4 {
		t.Fatalf("before-export hook saw %d batch ids, want 4", hookBatch)
	}
	if victim.ChangedAt > hookSnapshot {
		t.Fatalf("victim v1 changed_at %d must be <= snapshot %d", victim.ChangedAt, hookSnapshot)
	}
	if update.ChangedAt <= hookSnapshot {
		t.Fatalf("concurrent update changed_at %d must be > snapshot %d", update.ChangedAt, hookSnapshot)
	}

	// Both guard halves: the mutated row stays dirty and never entered the
	// parquet — the delta holds exactly the three flushed siblings.
	flushed, dirty := fetchChangeLogRowIDs(ctx, t, env, wide)
	if len(flushed) != 3 || len(dirty) != 1 || !dirty[victim.RowID.String()] {
		t.Fatalf("guard must keep exactly the mutated row dirty: flushed=%v dirty=%v", flushed, dirty)
	}
	if report.UnflushedBefore != 4 || report.UnflushedAfter != 1 {
		t.Errorf("unflushed before/after = %d/%d, want 4/1", report.UnflushedBefore, report.UnflushedAfter)
	}
	finals, _ := splitKeys(report.NewObjects)
	if len(finals) != 1 {
		t.Fatalf("flush must promote exactly one final delta, got %v", finals)
	}
	assertSameRowIDs(t, "pre-export parquet vs flushed siblings",
		fetchParquetRowIDs(ctx, t, env, finals), flushed)
	assertManifestDeltaPaths(t, report.Manifests, wide, finals)

	// Visibility: no cold copy of the victim exists; the dirty hot row serves
	// v2 and v1 is gone everywhere.
	assertFederatedRowCount(ctx, t, env, "post-flush rows", Query{Schema: wide, Limit: 10}, 4)
	assertRowCount(ctx, t, env, "hot value reachable",
		Query{Schema: wide, Filters: []Filter{{Attr: "title", Value: "hot-00"}}, Limit: 10}, 1)
	assertZeroRows(ctx, t, env, "v1 value unreachable",
		Query{Schema: wide, Filters: []Filter{{Attr: "title", Value: "open-00"}}, Limit: 10})

	assertUpdateRetryConverges(ctx, t, env, wide, update, finals)
}

// testUpdateAfterSnapshot is issue #182 scenarios 1 and 4: a row updated
// between snapshot capture and mark-flushed keeps flushed_at = 0, the
// federated read serves the hot (new) value, and the exported stale value is
// unreachable — first via the dirty anti-join, after retry via LWW. The delta
// parquet written by the paused flush contains the victim's stale value and
// the manifest tracks that file; both are expected (reads never consult the
// manifest, and the dirty row_id evicts the stale copy) — do not "fix" this.
func testUpdateAfterSnapshot(ctx context.Context, t *testing.T) {
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	creates := buildOpenCreates(wide, 4)
	mustApplyEvents(ctx, t, env, "seed creates", creates...)
	victim := creates[0]
	// Positive controls before anything flushes (PreferHot: no parquet yet).
	assertRowCount(ctx, t, env, "pre-flush rows", Query{Schema: wide, Limit: 10, PreferHot: true}, 4)
	assertRowCount(ctx, t, env, "pre-flush victim by v1",
		Query{Schema: wide, Filters: []Filter{{Attr: "title", Value: "open-00"}}, Limit: 10, PreferHot: true}, 1)

	var update *Event
	report, err := runPausedFlush(ctx, t, env, func() {
		update = UpdateEvent(wide, victim.RowID, map[string]any{
			"title": "hot-00",
			"count": float64(900000),
		})
		mustApplyEvents(ctx, t, env, "update during pause", update)
		// Guards the degenerate equal-ver_ts tie (#210): the mutation must sit
		// strictly after the exported version at ms resolution.
		assertStrictlyNewer(t, []*Event{victim}, []*Event{update})
	})
	if err != nil {
		t.Fatal(err)
	}

	finals := assertPausedFlushSplit(ctx, t, env, wide, report, 3, victim.RowID.String())
	// The exported parquet physically contains all 4 selected rows, and the
	// victim's record is exactly one stale v1 version (payload and ver_ts).
	wantExported := map[string]bool{}
	for _, c := range creates {
		wantExported[c.RowID.String()] = true
	}
	assertSameRowIDs(t, "paused-flush parquet vs selected batch",
		fetchParquetRowIDs(ctx, t, env, finals), wantExported)
	assertLiveParquetRow(ctx, t, env, finals[0], victim)

	// Visibility: the dirty hot version shadows the exported copy. The oracle
	// expects the victim to carry v2; the stale v1 must be unreachable even by
	// direct filter (anti-join evicts it before the filter runs).
	assertFederatedRowCount(ctx, t, env, "post-flush rows", Query{Schema: wide, Limit: 10}, 4)
	assertRowCount(ctx, t, env, "hot value reachable",
		Query{Schema: wide, Filters: []Filter{{Attr: "title", Value: "hot-00"}}, Limit: 10}, 1)
	assertZeroRows(ctx, t, env, "exported stale value unreachable",
		Query{Schema: wide, Filters: []Filter{{Attr: "title", Value: "open-00"}}, Limit: 10})

	// Retry convergence: the victim drains into a second delta; the two cold
	// copies now resolve by LWW on ver_ts — still exactly one visible version.
	assertUpdateRetryConverges(ctx, t, env, wide, update, finals)
}

// testDeleteAfterSnapshot is issue #182 scenarios 2 and 4: a row soft-deleted
// between snapshot capture and mark-flushed keeps its tombstone dirty, the
// deletion is immediately visible (the hot tombstone shadows the live value
// the paused flush exported), and after retry the flushed tombstone keeps
// suppressing both cold copies via LWW + deleted_ts.
func testDeleteAfterSnapshot(ctx context.Context, t *testing.T) {
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	creates := buildOpenCreates(wide, 4)
	mustApplyEvents(ctx, t, env, "seed creates", creates...)
	victim := creates[0]
	assertRowCount(ctx, t, env, "pre-flush rows", Query{Schema: wide, Limit: 10, PreferHot: true}, 4)
	// Same-filter positive control for the zero-row probes below: the victim
	// is reachable by this value until the concurrent delete lands.
	assertRowCount(ctx, t, env, "pre-flush victim by v1",
		Query{Schema: wide, Filters: []Filter{{Attr: "title", Value: "open-00"}}, Limit: 10, PreferHot: true}, 1)

	var del *Event
	report, err := runPausedFlush(ctx, t, env, func() {
		del = DeleteEvent(wide, victim.RowID)
		mustApplyEvents(ctx, t, env, "delete during pause", del)
		assertStrictlyNewer(t, []*Event{victim}, []*Event{del})
	})
	if err != nil {
		t.Fatal(err)
	}

	finals := assertPausedFlushSplit(ctx, t, env, wide, report, 3, victim.RowID.String())
	// The paused flush exported the victim as exactly one LIVE v1 record —
	// the delete landed after the export, so no tombstone is cold yet.
	assertLiveParquetRow(ctx, t, env, finals[0], victim)

	// Deletion visible now: 3 rows; the exported live copy must not resurrect
	// the victim (dirty tombstone anti-joins it out before the filter).
	assertFederatedRowCount(ctx, t, env, "post-flush rows", Query{Schema: wide, Limit: 10}, 3)
	assertRowCount(ctx, t, env, "survivor reachable",
		Query{Schema: wide, Filters: []Filter{{Attr: "title", Value: "open-01"}}, Limit: 10}, 1)
	assertZeroRows(ctx, t, env, "deleted row unreachable by exported value",
		Query{Schema: wide, Filters: []Filter{{Attr: "title", Value: "open-00"}}, Limit: 10})

	// Retry flushes the tombstone as exactly one all-NULL record carrying the
	// delete's timestamps; LWW + deleted_ts must keep both cold copies of the
	// victim invisible.
	retry := runCleanRetry(ctx, t, env)
	retryFinals, _ := splitKeys(retry.NewObjects)
	if len(retryFinals) != 1 {
		t.Fatalf("retry must promote exactly one more final delta, got %v", retryFinals)
	}
	assertTombstoneParquet(ctx, t, env, retryFinals[0], del)
	assertFederatedRowCount(ctx, t, env, "converged rows", Query{Schema: wide, Limit: 10}, 3)
	assertZeroRows(ctx, t, env, "deleted row stays unreachable",
		Query{Schema: wide, Filters: []Filter{{Attr: "title", Value: "open-00"}}, Limit: 10})
}

// testInsertAfterSnapshot is issue #182 scenario 3: a row created while the
// flush is paused stays dirty because it was never in the selected batch
// (distinct mechanism from the changed_at guard — mark-flushed only touches
// batch row IDs), never enters the paused flush's parquet, and is visible
// through the hot tier immediately.
func testInsertAfterSnapshot(ctx context.Context, t *testing.T) {
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	creates := buildOpenCreates(wide, 4)
	mustApplyEvents(ctx, t, env, "seed creates", creates...)
	assertRowCount(ctx, t, env, "pre-flush rows", Query{Schema: wide, Limit: 10, PreferHot: true}, 4)

	inserted := CreateEvent(wide, map[string]any{
		"title": "new-99",
		"count": float64(990000),
	})
	report, err := runPausedFlush(ctx, t, env, func() {
		mustApplyEvents(ctx, t, env, "insert during pause", inserted)
	})
	if err != nil {
		t.Fatal(err)
	}

	// All 4 selected rows flush; only the concurrent insert stays dirty, and
	// the exported parquet contains exactly the flushed set (not the insert).
	flushed, dirty := fetchChangeLogRowIDs(ctx, t, env, wide)
	if len(flushed) != 4 || len(dirty) != 1 || !dirty[inserted.RowID.String()] {
		t.Fatalf("only the concurrent insert may stay dirty: flushed=%v dirty=%v", flushed, dirty)
	}
	if report.UnflushedBefore != 4 || report.UnflushedAfter != 1 {
		t.Errorf("unflushed before/after = %d/%d, want 4/1", report.UnflushedBefore, report.UnflushedAfter)
	}
	finals, _ := splitKeys(report.NewObjects)
	if len(finals) != 1 {
		t.Fatalf("paused flush must promote exactly one final delta, got %v", finals)
	}
	assertSameRowIDs(t, "parquet vs flushed set",
		fetchParquetRowIDs(ctx, t, env, finals), flushed)
	assertManifestDeltaPaths(t, report.Manifests, wide, finals)

	// The new row is visible through the hot tier right away.
	assertFederatedRowCount(ctx, t, env, "post-flush rows", Query{Schema: wide, Limit: 10}, 5)
	assertRowCount(ctx, t, env, "inserted row reachable",
		Query{Schema: wide, Filters: []Filter{{Attr: "title", Value: "new-99"}}, Limit: 10}, 1)

	runCleanRetry(ctx, t, env)
	assertFederatedRowCount(ctx, t, env, "converged rows", Query{Schema: wide, Limit: 10}, 5)
	assertRowCount(ctx, t, env, "converged inserted row",
		Query{Schema: wide, Filters: []Filter{{Attr: "title", Value: "new-99"}}, Limit: 10}, 1)
}
