//go:build e2e

package production

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
)

// raceCounters tracks mutator progress. Written only by the mutator
// goroutine; the main goroutine reads them atomically to prove mutations
// overlapped RunInit (snapshot before the call vs after it returns).
type raceCounters struct {
	created atomic.Int64
	updated atomic.Int64
	deleted atomic.Int64
}

// TestInitUnderConcurrentMutation (#176 scenario 5): cdc-init pages with
// OFFSET over live data (internal/cdc/init.go selectEntityMainBatch — no
// snapshot), so concurrent writes can skip or duplicate rows in the base
// tier. The system-level contract asserted here: whatever the base tier
// captured, unflushed change_log entries shadow it (hot) and the follow-up
// flush lands the rest in delta, so the final federated state is exact.
// The started barrier plus counter snapshots around RunInit prove at least
// one create and one update genuinely overlapped the init pagination
// (review round 1: without them the scheduler could serialize the test).
//
// Since #290 cdc-init holds the per-schema advisory lock for each schema's
// export + manifest publish. The mutator here writes entity rows directly
// and takes no advisory lock, so the race window and every assertion below
// are unchanged. What DID change: a concurrent init/flusher/reconcile on
// the same schema now surfaces as ErrSchemaLockContended instead of racing
// (unit-covered in internal/cdc/init_lock_test.go).
func TestInitUnderConcurrentMutation(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	env.CDC.BatchSize = 5 // 60 seed rows -> >=12 OFFSET pages of race window

	seed := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 60})
	if err := env.ApplyEvents(ctx, seed...); err != nil {
		t.Fatalf("apply seed creates: %v", err)
	}

	stop := make(chan struct{})
	started := make(chan struct{})
	counters := &raceCounters{}
	errCh := make(chan error, 1)
	go runInitRaceMutator(ctx, env, wide, seed, stop, started, counters, errCh)

	// Barrier: the mutator has completed one full round before init starts.
	select {
	case <-started:
	case err := <-errCh:
		t.Fatalf("mutator failed before init started: %v", err)
	}
	createdBefore := counters.created.Load()
	updatedBefore := counters.updated.Load()

	report, initErr := env.RunInit(ctx, wide)
	createdDuring := counters.created.Load() - createdBefore
	updatedDuring := counters.updated.Load() - updatedBefore
	close(stop)
	if err := <-errCh; err != nil {
		t.Fatalf("concurrent mutator failed: %v", err)
	}
	if initErr != nil {
		t.Fatalf("run init under concurrent mutation: %v", initErr)
	}
	if report.RowsExported == 0 {
		t.Fatal("init exported no rows")
	}
	if createdDuring == 0 || updatedDuring == 0 {
		t.Fatalf("no mutation overlapped init (creates=%d updates=%d during); the race window did not open", createdDuring, updatedDuring)
	}
	created, deleted := counters.created.Load(), counters.deleted.Load()
	t.Logf("init exported %d rows in %d files; mutator created %d (%d during init), updated %d during init, deleted %d",
		report.RowsExported, report.FilesCreated, created, createdDuring, updatedDuring, deleted)

	// Handoff: flush everything the mutator produced (plus any rows init's
	// pagination missed — their change_log entries are still unflushed).
	// One flush pass drains at most BatchSize rows (no drain loop in the
	// flusher), so restore the default first; race-free because the mutator
	// has been joined via errCh above.
	env.CDC.BatchSize = 10000
	flush, err := env.RunFlush(ctx)
	if err != nil {
		t.Fatalf("post-init flush: %v", err)
	}
	if flush.UnflushedAfter != 0 {
		t.Fatalf("flush left %d unflushed rows", flush.UnflushedAfter)
	}

	// Exact end state: 60 seed + created - deleted, every value the latest.
	expected := 60 + int(created) - int(deleted)
	result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: expected + 50})
	if result != nil && len(result.Records) != expected {
		t.Fatalf("federated result has %d rows, want %d (no skips, no duplicates)", len(result.Records), expected)
	}
	env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "count", Op: "gte", Value: "400000"}},
		Limit:   expected + 50,
	})
}

// runInitRaceMutator applies a continuous create/update/delete stream until
// stop closes. It is the SOLE ApplyEvents/GenerateScript caller while it
// runs (Env event bookkeeping is unsynchronized). started closes after the
// first fully applied round; the final error (or nil) is sent on errCh.
// Update targets (seed[0:40]) and delete targets (seed[40:50]) are disjoint
// — Update on a deleted row returns ErrNotFound by pinned contract (#175).
func runInitRaceMutator(ctx context.Context, env *Env, wide SchemaRef, seed []*Event, stop, started chan struct{}, c *raceCounters, errCh chan<- error) {
	startedClosed := false
	for i := 0; ; i++ {
		select {
		case <-stop:
			errCh <- nil
			return
		default:
		}
		var batch []*Event
		// New creates are capped so the query limit stays safe.
		addCreate := c.created.Load() < 200
		if addCreate {
			batch = append(batch, env.GenerateScript(ScriptSpec{Schema: wide, Creates: 1})...)
		}
		batch = append(batch, UpdateEvent(wide, seed[i%40].RowID, map[string]any{
			"title": fmt.Sprintf("race-%03d", i),
			"count": float64(400000 + i),
		}))
		// Ten deletes from seed[40:50]: shifts OFFSET pages leftward, the
		// classic skip trigger.
		addDelete := c.deleted.Load() < 10
		if addDelete {
			batch = append(batch, DeleteEvent(wide, seed[40+int(c.deleted.Load())].RowID))
		}
		if err := env.ApplyEvents(ctx, batch...); err != nil {
			errCh <- fmt.Errorf("mutator round %d: %w", i, err)
			return
		}
		if addCreate {
			c.created.Add(1)
		}
		c.updated.Add(1)
		if addDelete {
			c.deleted.Add(1)
		}
		if !startedClosed {
			close(started)
			startedClosed = true
		}
	}
}
