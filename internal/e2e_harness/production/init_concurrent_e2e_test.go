//go:build e2e

package production

import (
	"context"
	"fmt"
	"testing"
)

// TestInitUnderConcurrentMutation (#176 scenario 5): cdc-init pages with
// OFFSET over live data (internal/cdc/init.go selectEntityMainBatch — no
// snapshot), so concurrent writes can skip or duplicate rows in the base
// tier. The system-level contract asserted here: whatever the base tier
// captured, unflushed change_log entries shadow it (hot) and the follow-up
// flush lands the rest in delta, so the final federated state is exact.
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
	type mutations struct {
		created int
		deleted int
		err     error
	}
	done := make(chan mutations, 1)
	go func() {
		var m mutations
		defer func() { done <- m }()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			var batch []*Event
			// One new create per round (created rows are hot until the
			// post-init flush) — capped so the query limit below stays safe.
			if m.created < 200 {
				news := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 1})
				batch = append(batch, news...)
				m.created++
			}
			// Rolling updates over seed[0:40] (disjoint from delete range).
			batch = append(batch, UpdateEvent(wide, seed[i%40].RowID, map[string]any{
				"title": fmt.Sprintf("race-%03d", i),
				"count": float64(400000 + i),
			}))
			// Ten deletes from seed[40:50]: shifts OFFSET pages leftward,
			// the classic skip trigger.
			if m.deleted < 10 {
				batch = append(batch, DeleteEvent(wide, seed[40+m.deleted].RowID))
				m.deleted++
			}
			if err := env.ApplyEvents(ctx, batch...); err != nil {
				m.err = fmt.Errorf("mutator round %d: %w", i, err)
				return
			}
		}
	}()

	report, initErr := env.RunInit(ctx, wide)
	close(stop)
	m := <-done
	if m.err != nil {
		t.Fatalf("concurrent mutator failed: %v", m.err)
	}
	if initErr != nil {
		t.Fatalf("run init under concurrent mutation: %v", initErr)
	}
	if report.RowsExported == 0 {
		t.Fatal("init exported no rows")
	}
	t.Logf("init exported %d rows in %d files; mutator created %d, deleted %d",
		report.RowsExported, report.FilesCreated, m.created, m.deleted)

	// The small BatchSize above only served to widen the init OFFSET race
	// window; it also caps rows-per-flush, so restore the harness default
	// (env.go) before the handoff flush so a single RunOnce drains the whole
	// hot tier in one snapshot, exactly as the sibling handoff tests do. The
	// mutator has already stopped (m received above), so this is race-free.
	env.CDC.BatchSize = 10000

	// Handoff: flush everything the mutator produced (plus any rows init's
	// pagination missed — their change_log entries are still unflushed).
	flush, err := env.RunFlush(ctx)
	if err != nil {
		t.Fatalf("post-init flush: %v", err)
	}
	if flush.UnflushedAfter != 0 {
		t.Fatalf("flush left %d unflushed rows", flush.UnflushedAfter)
	}

	// Exact end state: 60 seed + created - deleted, every value the latest.
	expected := 60 + m.created - m.deleted
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
