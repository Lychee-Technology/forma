//go:build e2e

package production

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma/internal/compaction"
)

// compactionEquivalenceQueries returns the query set every compaction
// scenario snapshots before and after the pass: an unsorted page (row-ID-set
// and value equality) and a sorted page (order stability on the unique-value
// count attribute).
func compactionEquivalenceQueries(schema SchemaRef) []Query {
	return []Query{
		{Schema: schema, Limit: 100},
		{Schema: schema, Sorts: []Sort{{Attr: "count"}}, Limit: 100},
	}
}

// assertCompactionEquivalence snapshots the caller-supplied query set, runs
// one compaction pass via RunCompactionWith, re-snapshots, and asserts the
// literal before/after identity plus oracle equality on both sides (#188
// success criterion: bit-for-bit identical federated results). The query set
// must fit the schema's attributes (compactionEquivalenceQueries for
// e2e_wide, buildEvolutionEquivalenceQueries for the #257 evolution fixture).
// Returns the result.
func assertCompactionEquivalence(
	ctx context.Context,
	t *testing.T,
	env *Env,
	schema SchemaRef,
	queries []Query,
	ov CompactionOverrides,
	label string,
) compaction.CompactionResult {
	t.Helper()

	before := make([]*resultSnapshot, len(queries))
	for i, q := range queries {
		env.AssertQueryMatches(ctx, q) // oracle equality pre-compaction
		before[i] = snapshotQueryResult(ctx, t, env, q)
	}

	result, err := env.RunCompactionWith(ctx, schema, ov)
	if err != nil {
		t.Fatalf("%s: compaction: %v", label, err)
	}

	for i, q := range queries {
		after := snapshotQueryResult(ctx, t, env, q)
		assertResultsIdentical(t, label, before[i], after)
		env.AssertQueryMatches(ctx, q) // oracle equality post-compaction
	}
	return result
}

// TestCompactionNoopPaths covers #188 scenarios 3 and 4: a schema with no
// deltas and a schema whose dirty ratio is under the threshold must both come
// back Noop with every surface untouched — manifest bytes/ETag/version, S3
// inventory, change_log — and identical query results.
func TestCompactionNoopPaths(t *testing.T) {
	cluster := SharedCluster(t)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	t.Run("NoDeltas", func(t *testing.T) {
		env := NewEnv(t, cluster)
		seedCompactionBase(ctx, t, env, wide, 5)

		// Positive control: the base tier really serves the query via DuckDB,
		// so the equivalence below is a parquet-read assertion, not hot-only.
		healthy := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
		if healthy == nil || healthy.Plan == nil || !healthy.Plan.Routing.UseDuckDB {
			t.Fatalf("precondition: base-only query did not route to duckdb")
		}

		state := captureState(t, ctx, env, wide)
		result := assertCompactionEquivalence(ctx, t, env, wide,
			compactionEquivalenceQueries(wide), CompactionOverrides{}, "noop-no-deltas")
		if result.Outcome != compaction.Noop {
			t.Errorf("outcome = %s, want %s", result.Outcome, compaction.Noop)
		}
		if result.DirtyRatio != 0 {
			t.Errorf("dirty ratio = %f, want 0 with no deltas", result.DirtyRatio)
		}
		assertStateUnchanged(t, "noop-no-deltas", state, captureState(t, ctx, env, wide))
	})

	t.Run("LowDirtyRatio", func(t *testing.T) {
		env := NewEnv(t, cluster)
		creates := seedCompactionBase(ctx, t, env, wide, 40)

		// One updated row against 40 base rows: ratio 2.5%, under the default
		// 5% trigger (strictly-greater comparison), and KB-scale delta bytes
		// stay under the default 256 MB promotion threshold.
		update := UpdateEvent(wide, creates[0].RowID, map[string]any{"title": "low-dirty-ratio-v2"})
		if err := env.ApplyEvents(ctx, update); err != nil {
			t.Fatalf("apply update: %v", err)
		}
		mustFlush(ctx, t, env)
		assertDeltaSizesPopulated(t, loadSchemaManifest(ctx, t, env, wide))

		state := captureState(t, ctx, env, wide)
		result := assertCompactionEquivalence(ctx, t, env, wide,
			compactionEquivalenceQueries(wide), CompactionOverrides{}, "low-dirty-ratio")
		if result.Outcome != compaction.Noop {
			t.Errorf("outcome = %s, want %s (ratio below threshold must skip)", result.Outcome, compaction.Noop)
		}
		// Positive control: a ratio was computed and consciously rejected —
		// not the vacuous zero of an empty delta tier.
		if result.DirtyRatio <= 0 || result.DirtyRatio > 0.05 {
			t.Errorf("dirty ratio = %f, want in (0, 0.05]", result.DirtyRatio)
		}
		assertStateUnchanged(t, "low-dirty-ratio", state, captureState(t, ctx, env, wide))

		m := loadSchemaManifest(ctx, t, env, wide)
		if got := countTier(m, "delta"); got != 1 {
			t.Errorf("delta entries = %d, want 1 (skip must not promote)", got)
		}
	})
}

// TestCompactionPromotionEquivalence covers #188 scenario 1: with real
// SizeBytes metadata and a lowered byte threshold, promotion relabels every
// delta entry as base without touching any parquet object, and federated
// results are identical before and after. Hot rows are present so the pass
// provably leaves the hot tier alone; the seed's 2/5 dirty ratio also
// exceeds the rewrite trigger, pinning promotion precedence within one pass.
func TestCompactionPromotionEquivalence(t *testing.T) {
	cluster := SharedCluster(t)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide
	env := NewEnv(t, cluster)

	creates := seedCompactionBase(ctx, t, env, wide, 5)
	updates := []*Event{
		UpdateEvent(wide, creates[0].RowID, map[string]any{"title": "promotion-v2-a"}),
		UpdateEvent(wide, creates[1].RowID, map[string]any{"title": "promotion-v2-b"}),
	}
	if err := env.ApplyEvents(ctx, updates...); err != nil {
		t.Fatalf("apply updates: %v", err)
	}
	mustFlush(ctx, t, env)
	hot := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 3})
	if err := env.ApplyEvents(ctx, hot...); err != nil {
		t.Fatalf("apply hot events: %v", err)
	}

	mBefore := loadSchemaManifest(ctx, t, env, wide)
	assertDeltaSizesPopulated(t, mBefore)
	baseBefore, deltaBefore := countTier(mBefore, "base"), countTier(mBefore, "delta")
	invBefore := snapshotS3Inventory(t, ctx, env)

	result := assertCompactionEquivalence(ctx, t, env, wide,
		compactionEquivalenceQueries(wide), CompactionOverrides{TargetBaseSizeBytes: 1}, "promotion")
	if result.Outcome != compaction.PromotionApplied {
		t.Fatalf("outcome = %s (dirty ratio %.2f), want %s", result.Outcome, result.DirtyRatio, compaction.PromotionApplied)
	}
	if result.Version != mBefore.Version+1 {
		t.Errorf("manifest version = %d, want %d (exactly one save)", result.Version, mBefore.Version+1)
	}

	mAfter := loadSchemaManifest(ctx, t, env, wide)
	if got := countTier(mAfter, "delta"); got != 0 {
		t.Errorf("delta entries after promotion = %d, want 0", got)
	}
	if got := countTier(mAfter, "base"); got != baseBefore+deltaBefore {
		t.Errorf("base entries after promotion = %d, want %d", got, baseBefore+deltaBefore)
	}
	assertNoDuplicateManifestEntries(t, mAfter)

	// Promotion is a manifest-only relabel: zero parquet objects created,
	// deleted, or modified.
	assertParquetInventoryUnchanged(t, "promotion", invBefore, snapshotS3Inventory(t, ctx, env))

	// The relabeled entries still serve reads: the post-compaction query must
	// route through DuckDB over the manifest-listed objects.
	post := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	if post == nil || post.Plan == nil || !post.Plan.Routing.UseDuckDB {
		t.Fatalf("post-promotion query did not route to duckdb")
	}
}
