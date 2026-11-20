//go:build e2e

package production

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/compaction"
	fedengine "github.com/lychee-technology/forma/internal/federated"
)

// TestCompactionRewriteInFlightReaderRetention is #461's regression under
// Protocol V2 (#890): a federated pass whose snapshot read the record at the
// pre-compaction generation must keep working after the compaction commits
// the next one. The pass is held right after its record read
// (pausingPublicationReader) while a real rewrite publishes; on release it
// resolves the replaced generation and scans exactly the objects it lists,
// which the compactor never deletes, and which a garbage collection run
// while the pass is held retires without deleting (#891). The held read
// answers the oracle's rows and values, and the next read answers them from
// the compact generation.
//
// No query runs between the flush that publishes the pre-compaction
// generation and the held pass, so the engine's manifest cache does not hold
// that generation when the pass resolves it after the publish; the test also
// fetches its manifest from the store afresh once the compaction committed.
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
	preSwapKeys := sortedGenerationKeys(preSwap)
	if len(preSwapKeys) < 2 {
		t.Fatalf("pre-swap generation lists %d files, want at least base+delta", len(preSwapKeys))
	}

	q := Query{Schema: wide, Limit: 100}
	held := holdQueryAfterRecordRead(ctx, t, env, pauser, q)

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
	requireReplacedGenerationRetained(ctx, t, env, wide, preSwap)

	// Garbage collection runs while the pass holds the replaced generation:
	// it retires the generation and deletes nothing it lists.
	gc := runReconcile(ctx, t, env, wide, reconcileOptions(true))
	if !slices.Contains(gc.Collected.Retired, preSwap.Generation) {
		t.Fatalf("the collection retired %v, want the held generation %d among them", gc.Collected.Retired, preSwap.Generation)
	}
	if len(gc.Collected.Objects)+len(gc.Collected.Manifests) != 0 {
		t.Fatalf("the collection deleted %v and %v while a pass held their generation", gc.Collected.Objects, gc.Collected.Manifests)
	}
	requireReplacedGenerationRetained(ctx, t, env, wide, preSwap)

	// Reader-side acceptance: the held pass resolves and scans the replaced
	// generation and answers the oracle's rows and values.
	res := held.release(t)
	requireResolvedGeneration(t, pauser.Resolved(), preSwap)
	if !res.Plan.Routing.UseDuckDB {
		t.Fatalf("in-flight query did not take the federated pass: %+v", res.Plan.Routing)
	}
	env.AssertResultMatches(ctx, q, res)

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
	post := env.AssertQueryMatches(ctx, q)
	if post == nil || post.Plan == nil || !post.Plan.Routing.UseDuckDB {
		t.Fatalf("post-swap query did not route to duckdb")
	}
	if gens := pauser.Resolved(); len(gens) < 2 || gens[len(gens)-1].Generation != result.Generation {
		t.Fatalf("post-swap query resolved %+v, want the compact generation %d last", gens, result.Generation)
	}
	transitions.expect(t, "post-swap query")
}

// heldQuery is a query whose pass waits right after its record read.
type heldQuery struct {
	pauser *pausingPublicationReader
	done   chan heldOutcome
}

type heldOutcome struct {
	res *QueryResult
	err error
}

// holdQueryAfterRecordRead starts q and returns once its pass has read the
// record and is held there.
func holdQueryAfterRecordRead(ctx context.Context, t *testing.T, env *Env, pauser *pausingPublicationReader, q Query) *heldQuery {
	t.Helper()
	qctx, cancel := context.WithTimeout(ctx, pausedFlushTimeout)
	h := &heldQuery{pauser: pauser, done: make(chan heldOutcome, 1)}
	finished := make(chan struct{})
	pauser.armed.Store(true)
	go func() {
		defer close(finished)
		res, err := env.Query(qctx, q)
		h.done <- heldOutcome{res: res, err: err}
	}()
	t.Cleanup(func() {
		pauser.Release()
		cancel()
		<-finished
	})
	select {
	case <-pauser.Reached():
	case out := <-h.done:
		t.Fatalf("query finished before pausing after its record read: err=%v", out.err)
	}
	return h
}

// release lets the held pass go on and answers its result.
func (h *heldQuery) release(t *testing.T) *QueryResult {
	t.Helper()
	h.pauser.Release()
	out := <-h.done
	if out.err != nil {
		t.Fatalf("in-flight query across the compaction: %v", out.err)
	}
	return out.res
}

// requireReplacedGenerationRetained checks, after the compaction committed,
// that every object the replaced generation lists is still in the bucket and
// that its manifest is still fetchable from the store, verified, listing
// them: what the held pass resolves when it goes on.
func requireReplacedGenerationRetained(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, replaced *Manifest) {
	t.Helper()
	want := sortedGenerationKeys(replaced)
	after := map[string]bool{}
	for _, k := range schemaParquetKeys(ctx, t, env, schema) {
		after[k] = true
	}
	for _, k := range want {
		if !after[k] {
			t.Errorf("pre-swap source %s was deleted at publish; the in-flight pass of its generation would lose it", k)
		}
	}
	keys, err := env.fetchGenerationKeys(ctx, replaced)
	if err != nil {
		t.Fatalf("the replaced generation's manifest is no longer fetchable after the compaction: %v", err)
	}
	slices.Sort(keys)
	if !slices.Equal(keys, want) {
		t.Fatalf("the replaced generation's manifest lists %v after the compaction, want %v", keys, want)
	}
}

// requireResolvedGeneration checks that the held pass, the only armed one
// so far, resolved the replaced generation and was handed exactly its
// objects to scan.
func requireResolvedGeneration(t *testing.T, resolved []fedengine.PublishedGeneration, replaced *Manifest) {
	t.Helper()
	if len(resolved) == 0 {
		t.Fatal("the held pass resolved no generation")
	}
	want := sortedGenerationKeys(replaced)
	for _, gen := range resolved {
		if gen.Epoch != replaced.Epoch || gen.Generation != replaced.Generation {
			t.Fatalf("the held pass resolved epoch %d generation %d, want the replaced epoch %d generation %d",
				gen.Epoch, gen.Generation, replaced.Epoch, replaced.Generation)
		}
		keys := slices.Clone(gen.Keys)
		slices.Sort(keys)
		if !slices.Equal(keys, want) {
			t.Fatalf("the held pass scans %v, want the replaced generation's %v", keys, want)
		}
	}
}

// sortedGenerationKeys answers the object keys m lists, sorted.
func sortedGenerationKeys(m *Manifest) []string {
	keys := make([]string, 0, len(m.Files))
	for _, f := range m.Files {
		keys = append(keys, f.Path)
	}
	slices.Sort(keys)
	return keys
}
