//go:build e2e

package production

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/compaction"
)

// TestCompactionManifestAtomicity covers #188 scenario 5 with a REAL ETag
// conflict (the spike confirmed rustfs rejects a stale If-Match with 412):
// the compactor's manifest save is held open by PausingS3 while a concurrent
// real flush appends a new delta and advances the manifest; on resume the
// stale conditional put must fail, and the retry must recompute the merge
// from the fresh manifest. No entries may be lost — the concurrently flushed
// rows end up folded into the final base — and the version advances
// monotonically past the concurrent writer's.
func TestCompactionManifestAtomicity(t *testing.T) {
	cluster := SharedCluster(t)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide
	env := NewEnv(t, cluster)

	creates := seedCompactionBase(ctx, t, env, wide, 5)
	update := UpdateEvent(wide, creates[0].RowID, map[string]any{"title": "atomicity-v2"})
	if err := env.ApplyEvents(ctx, update); err != nil {
		t.Fatalf("apply update: %v", err)
	}
	mustFlush(ctx, t, env) // dirty ratio 1/5 = 20% > 5%: rewrite-eligible

	everListed := map[string]bool{}
	recordListedKeys(ctx, t, env, wide, everListed)

	// Pause the compactor exactly at its manifest save; the rewrite's parquet
	// staging traffic (DuckDB httpfs + CopyObject) does not match.
	pauser := NewPausingS3OnKey(cluster.S3, S3OpPut, "manifest/")
	t.Cleanup(pauser.Resume)

	type outcome struct {
		result compaction.CompactionResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := env.RunCompactionWith(ctx, wide, CompactionOverrides{S3: pauser})
		done <- outcome{result, err}
	}()

	select {
	case <-pauser.Reached():
	case o := <-done:
		t.Fatalf("compaction finished without touching the manifest save (outcome=%s err=%v)", o.result.Outcome, o.err)
	case <-time.After(2 * time.Minute):
		t.Fatal("compaction never reached the manifest save")
	}

	// Concurrent writer: a real flush through the UNDECORATED client appends
	// a new delta entry and bumps the manifest ETag under the paused save.
	concurrent := UpdateEvent(wide, creates[1].RowID, map[string]any{"title": "atomicity-concurrent-v2"})
	if err := env.ApplyEvents(ctx, concurrent); err != nil {
		t.Fatalf("apply concurrent update: %v", err)
	}
	mustFlush(ctx, t, env)
	recordListedKeys(ctx, t, env, wide, everListed) // + the concurrent delta
	midVersion := loadSchemaManifest(ctx, t, env, wide).Version

	pauser.Resume()
	o := <-done
	if o.err != nil {
		t.Fatalf("compaction with concurrent manifest writer: %v", o.err)
	}
	if o.result.Outcome != compaction.RewriteApplied {
		t.Fatalf("outcome = %s (dirty ratio %.2f), want %s after conflict retry", o.result.Outcome, o.result.DirtyRatio, compaction.RewriteApplied)
	}
	// Monotonicity past the concurrent writer proves the stale first attempt
	// did not clobber it (If-Match held) and the commit happened after.
	if o.result.Version <= midVersion {
		t.Errorf("final manifest version = %d, want > %d (the concurrent flush's save)", o.result.Version, midVersion)
	}

	// No lost entries: the retry recomputed from the fresh manifest, so the
	// concurrently flushed delta is folded into the final base — its payload
	// must be the winning version physically present in the new base file.
	winners := map[uuid.UUID]*Event{
		creates[0].RowID: update,
		creates[1].RowID: concurrent,
		creates[2].RowID: creates[2],
		creates[3].RowID: creates[3],
		creates[4].RowID: creates[4],
	}
	assertRewrittenBase(ctx, t, env, o.result.NewBaseKey, winners, nil)

	m := loadSchemaManifest(ctx, t, env, wide)
	if got := countTier(m, "delta"); got != 0 {
		t.Errorf("delta entries after conflict retry = %d, want 0 (concurrent delta folded)", got)
	}
	assertNoDuplicateManifestEntries(t, m)
	// The failed first attempt's staged base was never listed, so it must not
	// linger (the confirmed-412 cleanup); the retry's retired sources are in
	// everListed and may (#461: must be able to) survive unlisted.
	assertManifestMatchesInventory(ctx, t, env, wide, everListed)

	// Full result equivalence: engine vs oracle, which knows both updates.
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	env.AssertQueryMatches(ctx, Query{Schema: wide, Sorts: []Sort{{Attr: "count"}}, Limit: 100})
}
