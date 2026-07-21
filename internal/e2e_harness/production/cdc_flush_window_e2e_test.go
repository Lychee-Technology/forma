//go:build e2e

package production

import (
	"context"
	"fmt"
	"testing"
)

// TestFlushManifestWindowVisibility pins issue #252: a federated query landing
// between a flush's mark-flushed and its manifest-append must still see the
// batch's rows. Under the pre-#252 ordering (copy -> mark -> append) the batch
// was transiently invisible in every tier: the rows had left the hot tier
// (flushed_at != 0 excludes them from dirty_ids and pg_source) while the delta
// was not yet listed in the manifest the reader resolves paths from.
//
// The probe needs a NON-EMPTY manifest: a never-flushed schema falls back to
// the legacy glob (QuerySource.Fallback), which sees the copied final object
// regardless of manifest state — the window only exists for manifest-driven
// reads. So generation 1 flushes cleanly first, then generation 2's flush is
// held open at its manifest PutObject while the query probes the window.
func TestFlushManifestWindowVisibility(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	// The #252 read-side grace would also serve the window (see
	// cdc_flush_grace_e2e_test.go); disable it so this test pins the
	// write-side ordering fix in isolation.
	env.DuckCfg.FlushVisibilityGraceMs = -1
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	gen1 := buildOpenCreates(wide, 4)
	mustApplyEvents(ctx, t, env, "seed generation 1", gen1...)
	first, err := env.RunFlushWith(ctx, FlushOverrides{})
	if err != nil {
		t.Fatalf("first clean flush: %v", err)
	}
	firstFinals, _ := splitKeys(first.NewObjects)
	if len(firstFinals) != 1 {
		t.Fatalf("first flush must promote exactly one final delta, got %v", firstFinals)
	}
	assertManifestDeltaPaths(t, first.Manifests, wide, firstFinals)
	assertFederatedRowCount(ctx, t, env, "post-first-flush rows",
		Query{Schema: wide, Limit: 10}, 4)

	gen2 := make([]*Event, 0, 4)
	for i := 0; i < 4; i++ {
		gen2 = append(gen2, CreateEvent(wide, map[string]any{
			"title": fmt.Sprintf("fresh-%02d", i),
			"count": float64(300000 + i),
		}))
	}
	mustApplyEvents(ctx, t, env, "seed generation 2", gen2...)
	assertFederatedRowCount(ctx, t, env, "pre-pause rows",
		Query{Schema: wide, Limit: 10}, 8)

	// Hold the flush open at its manifest PutObject. The reader must keep
	// seeing all 8 rows through the whole pipeline, including this window.
	pauser := NewPausingS3OnKey(env.Cluster.S3, S3OpPut, "manifest/")
	report, err := runFlushPausedOn(ctx, t, env, pauser, func() {
		// The probe bypasses the oracle helper on purpose: the red signal is
		// a crisp row count, not an artifact-dumping diff. It must route to
		// DuckDB — the PG-only path reads the main table directly and has no
		// window to expose.
		res, qerr := env.Query(ctx, Query{Schema: wide, Limit: 10})
		if qerr != nil {
			t.Fatalf("mid-window query: %v", qerr)
		}
		if !res.Plan.Routing.UseDuckDB {
			t.Errorf("mid-window query did not route to duckdb: %+v", res.Plan.Routing)
		}
		if got := len(res.Records); got != 8 {
			t.Errorf("mid-flush window: query saw %d rows, want 8 — the in-flight batch is visible in neither tier (#252)", got)
		}
	})
	if err != nil {
		t.Fatalf("flush under manifest-append pause: %v", err)
	}
	if report.UnflushedAfter != 0 {
		t.Errorf("flush must drain all dirty rows, unflushed = %d", report.UnflushedAfter)
	}
	secondFinals, _ := splitKeys(report.NewObjects)
	if len(secondFinals) != 1 {
		t.Fatalf("paused flush must promote exactly one final delta, got %v", secondFinals)
	}
	assertManifestDeltaPaths(t, report.Manifests, wide,
		append(append([]string(nil), firstFinals...), secondFinals...))

	// Converged: both deltas listed, nothing hot, oracle agrees on all 8.
	assertFederatedRowCount(ctx, t, env, "converged rows",
		Query{Schema: wide, Limit: 10}, 8)
	assertRowCount(ctx, t, env, "generation 2 reachable",
		Query{Schema: wide, Filters: []Filter{{Attr: "title", Value: "fresh-00"}}, Limit: 10}, 1)
}

// runFlushPausedOn starts one real flush with the given pauser installed as
// its S3 client, invokes during on the test goroutine while the flush is
// suspended at the pauser's first matching call, then resumes the flush and
// returns its outcome. The generic core of runPausedFlush (which pins the
// CopyObject pause point for #182); #252 pauses at the manifest PutObject.
func runFlushPausedOn(ctx context.Context, t *testing.T, env *Env, pauser *PausingS3, during func()) (*FlushReport, error) {
	t.Helper()
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
	// t.Fatal (including inside during) releases the paused flush and waits
	// for it to exit while the Env's resources are still alive.
	t.Cleanup(func() {
		pauser.Resume()
		cancel()
		<-finished
	})

	select {
	case <-pauser.Reached():
	case out := <-done:
		t.Fatalf("flush finished before reaching the pause: err=%v", out.err)
	}
	during()
	pauser.Resume()
	out := <-done
	if out.err != nil {
		return out.report, fmt.Errorf("flush under pause: %w", out.err)
	}
	return out.report, nil
}
