//go:build e2e

package production

import (
	"context"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/compaction"
)

// TestCompactionRewriteInFlightReaderRetention is #461's regression under
// Protocol V2 (#890): a federated pass whose snapshot read the record at the
// pre-compaction generation must keep working after the compaction commits
// the next one. The pass is held right after its record read
// (pausingPublicationReader) while a real rewrite publishes; on release it
// fetches the replaced generation's manifest and scans exactly the objects it
// lists, which the compactor never deletes (their retirement is #891's). The
// read answers as the oracle does, before and after.
//
// #461's impact had two halves: the failed read, and the breaker-open event
// it fed, which disables the DuckDB route for every query. The Env therefore
// runs a threshold-1 breaker, so a single RecordFailure anywhere in the
// scenario opens it, and the test asserts breaker neutrality explicitly
// (#499) rather than inferring it from the held query's routing.
func TestCompactionRewriteInFlightReaderRetention(t *testing.T) {
	cluster := SharedCluster(t)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide
	transitions := &breakerTransitions{}
	env := NewEnv(t, cluster, WithBreaker(1, time.Minute), WithMetricEmitter(transitions))
	// Installed before any event: seeding assembles the engine.
	pauser := newPausingPublicationReader()
	pauser.install(env)

	creates := seedCompactionBase(ctx, t, env, wide, 5)
	update := UpdateEvent(wide, creates[0].RowID, map[string]any{"title": "inflight-v2"})
	if err := env.ApplyEvents(ctx, update); err != nil {
		t.Fatalf("apply update: %v", err)
	}
	mustFlush(ctx, t, env) // dirty ratio 1/5 = 20% > 5%: rewrite-eligible

	preSwap := loadSchemaManifest(ctx, t, env, wide)
	preSwapKeys := make([]string, 0, len(preSwap.Files))
	for _, f := range preSwap.Files {
		preSwapKeys = append(preSwapKeys, f.Path)
	}
	if len(preSwapKeys) < 2 {
		t.Fatalf("pre-swap generation lists %d files, want at least base+delta", len(preSwapKeys))
	}
	want := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})

	// The in-flight reader: its pass reads the record at the pre-swap
	// generation, then waits.
	type outcome struct {
		res *QueryResult
		err error
	}
	qctx, cancel := context.WithTimeout(ctx, pausedFlushTimeout)
	done := make(chan outcome, 1)
	finished := make(chan struct{})
	pauser.armed.Store(true)
	go func() {
		defer close(finished)
		res, err := env.Query(qctx, Query{Schema: wide, Limit: 100})
		done <- outcome{res: res, err: err}
	}()
	t.Cleanup(func() {
		pauser.Release()
		cancel()
		<-finished
	})
	select {
	case <-pauser.Reached():
	case out := <-done:
		t.Fatalf("query finished before pausing after its record read: err=%v", out.err)
	}

	// The compactor publishes the next generation while the pass holds the
	// pre-swap one.
	result, err := env.RunCompaction(ctx, wide)
	if err != nil {
		t.Fatalf("compaction: %v", err)
	}
	if result.Outcome != compaction.RewriteApplied {
		t.Fatalf("outcome = %s (dirty ratio %.2f), want %s", result.Outcome, result.DirtyRatio, compaction.RewriteApplied)
	}
	if n := compactionPublishesSince(preSwap, result); n != 1 {
		t.Fatalf("compaction committed %d publishes since the pass's generation, want one", n)
	}

	// Writer-side acceptance: every pre-swap source object survives the
	// publish.
	after := map[string]bool{}
	for _, k := range schemaParquetKeys(ctx, t, env, wide) {
		after[k] = true
	}
	for _, k := range preSwapKeys {
		if !after[k] {
			t.Errorf("pre-swap source %s was deleted at publish; the in-flight pass of its generation would lose it", k)
		}
	}

	// Reader-side acceptance: the held pass scans the replaced generation and
	// answers what it answered before the compaction.
	pauser.Release()
	out := <-done
	if out.err != nil {
		t.Fatalf("in-flight query across the compaction: %v", out.err)
	}
	if !out.res.Plan.Routing.UseDuckDB {
		t.Fatalf("in-flight query did not take the federated pass: %+v", out.res.Plan.Routing)
	}
	if got, wantN := len(out.res.Records), len(want.Records); got != wantN {
		t.Errorf("in-flight query read %d rows, want the %d it read before the compaction", got, wantN)
	}

	// Breaker neutrality (#499): the held read recorded no failure. With
	// threshold 1 a single RecordFailure would have opened the breaker and
	// emitted an "open" transition; a success on a closed breaker emits
	// nothing, so the expected transition list is empty.
	breaker := env.Breaker()
	if breaker == nil {
		t.Fatal("env has no circuit breaker; breaker neutrality would pass vacuously")
	}
	if breaker.IsOpen() {
		t.Fatal("circuit breaker is open after the in-flight query; it fed the breaker a failure")
	}
	transitions.expect(t, "in-flight query")

	// The next pass reads the compact generation and routes to DuckDB.
	post := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	if post == nil || post.Plan == nil || !post.Plan.Routing.UseDuckDB {
		t.Fatalf("post-swap query did not route to duckdb")
	}
	transitions.expect(t, "post-swap query")
}
