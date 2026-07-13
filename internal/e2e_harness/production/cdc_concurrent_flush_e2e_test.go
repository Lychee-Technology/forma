//go:build e2e

package production

import (
	"context"
	"testing"
	"time"
)

// TestConcurrentFlushSnapshot pins issue #182: rows mutated concurrently with
// a CDC flush must stay dirty and queries must always see the latest version.
//
// Mechanism: PausingS3 holds the flush open at CopyObject — after dirty-ID
// selection, snapshot capture, and the DuckDB export, before
// MarkFlushedIDsAtSnapshot — so each scenario mutates inside the real race
// window. The changed_at <= snapshot guard (internal/cdc/helpers.go) must
// then skip the mutated row while its untouched siblings flush; the unit
// twin of that guard is TestMarkFlushedIDsAtSnapshot_SkipsUpdatedRows.
// Because the pause sits after the export, the delta parquet still carries
// the pre-mutation value — the visibility probes assert the dirty hot
// version shadows it (anti-join), and after the retry flush drains the row,
// LWW on ver_ts keeps suppressing it. The earlier select->export window
// needs no e2e: the export CTE applies the same changed_at <= snapshot
// filter (internal/cdc/duckdb_exporter.go), so a row mutated there never
// enters the parquet at all.
func TestConcurrentFlushSnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
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
	return out.report, out.err
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

	report, err := runPausedFlush(ctx, t, env, func() {
		update := UpdateEvent(wide, victim.RowID, map[string]any{
			"title": "hot-00",
			"count": float64(900000),
		})
		mustApplyEvents(ctx, t, env, "update during pause", update)
		// Guards the degenerate equal-ver_ts tie (#210): the mutation must sit
		// strictly after the exported version at ms resolution.
		assertStrictlyNewer(t, []*Event{victim}, []*Event{update})
	})
	if err != nil {
		t.Fatalf("paused flush: %v", err)
	}

	finals := assertPausedFlushSplit(ctx, t, env, wide, report, 3, victim.RowID.String())
	// The exported parquet physically contains all 4 selected rows — the
	// victim with its stale v1 value.
	wantExported := map[string]bool{}
	for _, c := range creates {
		wantExported[c.RowID.String()] = true
	}
	assertSameRowIDs(t, "paused-flush parquet vs selected batch",
		fetchParquetRowIDs(ctx, t, env, finals), wantExported)

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
	retry := runCleanRetry(ctx, t, env)
	retryFinals, _ := splitKeys(retry.NewObjects)
	if len(retryFinals) != 1 {
		t.Fatalf("retry must promote exactly one more final delta, got %v", retryFinals)
	}
	assertSameRowIDs(t, "retry parquet vs victim",
		fetchParquetRowIDs(ctx, t, env, retryFinals), map[string]bool{victim.RowID.String(): true})
	assertManifestDeltaPaths(t, retry.Manifests, wide, append(finals, retryFinals...))
	assertFederatedRowCount(ctx, t, env, "converged rows", Query{Schema: wide, Limit: 10}, 4)
	assertRowCount(ctx, t, env, "converged hot value",
		Query{Schema: wide, Filters: []Filter{{Attr: "title", Value: "hot-00"}}, Limit: 10}, 1)
	assertZeroRows(ctx, t, env, "converged stale value",
		Query{Schema: wide, Filters: []Filter{{Attr: "title", Value: "open-00"}}, Limit: 10})
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

	report, err := runPausedFlush(ctx, t, env, func() {
		del := DeleteEvent(wide, victim.RowID)
		mustApplyEvents(ctx, t, env, "delete during pause", del)
		assertStrictlyNewer(t, []*Event{victim}, []*Event{del})
	})
	if err != nil {
		t.Fatalf("paused flush: %v", err)
	}

	assertPausedFlushSplit(ctx, t, env, wide, report, 3, victim.RowID.String())

	// Deletion visible now: 3 rows; the exported live copy must not resurrect
	// the victim (dirty tombstone anti-joins it out before the filter).
	assertFederatedRowCount(ctx, t, env, "post-flush rows", Query{Schema: wide, Limit: 10}, 3)
	assertRowCount(ctx, t, env, "survivor reachable",
		Query{Schema: wide, Filters: []Filter{{Attr: "title", Value: "open-01"}}, Limit: 10}, 1)
	assertZeroRows(ctx, t, env, "deleted row unreachable by exported value",
		Query{Schema: wide, Filters: []Filter{{Attr: "title", Value: "open-00"}}, Limit: 10})

	// Retry flushes the tombstone; LWW + deleted_ts must keep both cold copies
	// of the victim invisible.
	runCleanRetry(ctx, t, env)
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
		t.Fatalf("paused flush: %v", err)
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
