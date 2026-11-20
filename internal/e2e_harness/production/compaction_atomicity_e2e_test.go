//go:build e2e

package production

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/compaction"
)

// TestCompactionManifestAtomicity covers #188 scenario 5 under Protocol V2
// (#890): the compactor is held at the write-once put of its staged manifest
// by PausingS3 while a concurrent real flush publishes a new delta and marks
// its rows. On resume the compactor's compare-and-set against the generation
// it read misses, and since a flush only appended files it rebases: it stages
// a new manifest from the flush's generation under a fresh token, listing its
// merged base in place of the sources and keeping the flush's delta. Nothing
// is lost, nothing is merged twice, and the compaction commits exactly one
// generation, past the flush's.
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
	input := loadSchemaManifest(ctx, t, env, wide)

	everListed := map[string]bool{}
	recordListedKeys(ctx, t, env, wide, everListed)

	// Pause the compactor exactly at its manifest staging, after its merge
	// output is staged and before its publication transaction.
	pauser := NewPausingS3OnKey(cluster.S3, S3OpPut, "/_manifests/")
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
		t.Fatalf("compaction finished without staging its manifest (outcome=%s err=%v)", o.result.Outcome, o.err)
	case <-time.After(2 * time.Minute):
		t.Fatal("compaction never reached its manifest staging")
	}

	// Concurrent writer: a real flush through the UNDECORATED client
	// publishes a generation on top of the one the compactor read.
	concurrent := UpdateEvent(wide, creates[1].RowID, map[string]any{"title": "atomicity-concurrent-v2"})
	if err := env.ApplyEvents(ctx, concurrent); err != nil {
		t.Fatalf("apply concurrent update: %v", err)
	}
	mustFlush(ctx, t, env)
	recordListedKeys(ctx, t, env, wide, everListed) // + the concurrent delta
	mid := loadSchemaManifest(ctx, t, env, wide)

	pauser.Resume()
	o := <-done
	if o.err != nil {
		t.Fatalf("compaction with a concurrent flush: %v", o.err)
	}
	if o.result.Outcome != compaction.RewriteApplied {
		t.Fatalf("outcome = %s (dirty ratio %.2f), want %s after the rebase", o.result.Outcome, o.result.DirtyRatio, compaction.RewriteApplied)
	}
	if o.result.Rebases != 1 || o.result.Restarts != 0 {
		t.Errorf("rebases = %d, restarts = %d; want one rebase over the flush and no second merge", o.result.Rebases, o.result.Restarts)
	}
	if o.result.InputGeneration != input.Generation {
		t.Errorf("input generation = %d, want %d, the generation the merge read", o.result.InputGeneration, input.Generation)
	}
	if n := compactionPublishesSince(mid, o.result); n != 1 {
		t.Errorf("compaction committed %d publishes past the concurrent flush's, want exactly one", n)
	}

	// The merge holds what the input generation held; the concurrent
	// update stays in the flush's delta, which the compact generation lists.
	winners := map[uuid.UUID]*Event{
		creates[0].RowID: update,
		creates[1].RowID: creates[1],
		creates[2].RowID: creates[2],
		creates[3].RowID: creates[3],
		creates[4].RowID: creates[4],
	}
	assertRewrittenBase(ctx, t, env, o.result.NewBaseKey, winners, nil)

	m := loadSchemaManifest(ctx, t, env, wide)
	if got := countTier(m, "delta"); got != 1 {
		t.Errorf("delta entries after the rebase = %d, want the concurrent flush's 1", got)
	}
	if got := countTier(m, "base"); got != 1 {
		t.Errorf("base entries after the rebase = %d, want the merged base alone", got)
	}
	assertNoDuplicateManifestEntries(t, m)
	// The rebase reused its staged merge output, so no never-listed parquet
	// lingers; the merged sources are in everListed and stay for the readers
	// of the generations they were listed in.
	assertManifestMatchesInventory(ctx, t, env, wide, everListed)

	// Full result equivalence: engine vs oracle, which knows both updates.
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	env.AssertQueryMatches(ctx, Query{Schema: wide, Sorts: []Sort{{Attr: "count"}}, Limit: 100})
}
