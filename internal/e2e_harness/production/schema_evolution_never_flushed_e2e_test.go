//go:build e2e

package production

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
)

// queryTierRestricted runs a parquet-only (warm+cold) read through the engine
// alone and asserts its total. The oracle is deliberately tier-blind — it
// folds the event log into the full logical set with no notion of what has
// been flushed — so a tier-restricted read taken while unflushed hot rows
// exist cannot be compared against it (that divergence would be a fixture
// artifact, not a product defect). These assertions therefore pin the
// engine's physical answer directly, the same way assertTierMatrixS3Only
// does for #184.
func queryTierRestricted(ctx context.Context, t *testing.T, env *Env, q Query, want int64, why string) *QueryResult {
	t.Helper()
	res, err := env.Query(ctx, q)
	if err != nil {
		t.Fatalf("tier-restricted query (%s): %v", why, err)
	}
	if res.Total != want {
		t.Fatalf("tier-restricted total (%s) = %d, want %d", why, res.Total, want)
	}
	return res
}

// TestSchemaEvolutionNeverFlushedColumn covers #255: v2 adds `score` BEFORE
// its first flush, so the base parquet (v1 generation) is the ENTIRE cold
// set and physically lacks the column — union_by_name (#189) cannot invent
// it. Contract: cold-inclusive reads succeed with score as a typed NULL on
// every parquet-tier row (exact SQL NULL semantics for filters and sorts),
// hot rows serve real values, and the compiled-plan cache does not keep
// projecting NULL after the first flush lands the column (the #255
// plan-cache poisoning hazard).
func TestSchemaEvolutionNeverFlushedColumn(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	v1 := writeSimpleSchemaDir(t, coreProps, coreAttrs)
	v2 := writeSimpleSchemaDir(t, scoreIntProps, scoreIntAttrs)
	env := NewEnv(t, cluster, WithSchemaDir(v1))
	simple := DefaultSchemaFixtures()[0]

	// 5 base rows under v1 (ordinals 0-4), exported as base; evolve to v2;
	// 3 hot rows under v2 (ordinals 5-7, score 50/60/70) left UNFLUSHED.
	seedGeneration(ctx, t, env, simple, 5, buildEvolutionProfile(buildLabeledExtras(nil)))
	baseKey := runInitBase(ctx, t, env, simple)
	if err := env.EvolveSchema(ctx, v2); err != nil {
		t.Fatalf("evolve schema to v2: %v", err)
	}
	seedGeneration(ctx, t, env, simple, 3, buildEvolutionProfile(buildLabeledExtras(func(ordinal int) map[string]any {
		return map[string]any{"score": float64(ordinal * 10)}
	})))

	// Shape precondition: the whole cold set lacks score.
	forbidParquetCols(t, "base (v1)", describeParquetCols(ctx, t, env, baseKey), "score")

	// Full scan across all tiers: 8 rows (5 base + 3 hot), no binder error.
	full := env.AssertQueryMatches(ctx, Query{Schema: simple, Limit: 20})
	assertUsesDuckDB(t, full)
	if full != nil && full.Total != 8 {
		t.Fatalf("full scan total = %d, want 8 (5 v1 base + 3 hot)", full.Total)
	}

	// Filter on the never-flushed attribute: hot rows only (score 50/60/70).
	filtered := env.AssertQueryMatches(ctx, Query{
		Schema:  simple,
		Filters: []Filter{{Attr: "score", Op: "gte", Value: "50"}},
		Limit:   20,
	})
	if filtered != nil && filtered.Total != 3 {
		t.Fatalf("score >= 50 total = %d, want 3 (hot rows only; NULL excludes every base row)", filtered.Total)
	}

	// Sort on it: base rows (score NULL) land NULLS LAST.
	env.AssertQueryMatches(ctx, Query{
		Schema: simple,
		Sorts:  []Sort{{Attr: "score"}},
		Limit:  20,
	})

	// Hot-excluded (cold-only) read — the issue's title case: must succeed,
	// serving the 5 base rows with score wholly absent/NULL. This is the
	// positive query that makes the zero-row assertion below trustworthy.
	coldTiers := []model.DataTier{model.DataTierWarm, model.DataTierCold}
	coldOnly := queryTierRestricted(ctx, t, env, Query{
		Schema: simple, PreferredTiers: coldTiers, Limit: 20,
	}, 5, "base rows; unflushed hot invisible")
	assertUsesDuckDB(t, coldOnly)

	// Plan-transition (cache-poisoning) regression: compile & cache the
	// cold-only filter shape while score is cold-absent...
	coldFilter := Query{
		Schema: simple, PreferredTiers: coldTiers,
		Filters: []Filter{{Attr: "score", Op: "gte", Value: "50"}},
		Limit:   20,
	}
	queryTierRestricted(ctx, t, env, coldFilter, 0, "pre-flush cold-only score filter")

	// ...then land the FIRST flush carrying score and re-run the same shape.
	// A poisoned plan (cached NULL projection) would keep the flushed rows
	// invisible and return 0. Post-flush nothing is hot, so the parquet-only
	// read is the whole logical set again and the oracle is authoritative.
	mustFlush(ctx, t, env)
	postFlush := env.AssertQueryMatches(ctx, coldFilter)
	assertUsesDuckDB(t, postFlush)
	if postFlush != nil && postFlush.Total != 3 {
		t.Fatalf("post-flush cold-only score filter total = %d, want 3 — plan-cache poisoning (#255): the skeleton compiled while score was cold-absent is still projecting NULL", postFlush.Total)
	}
}
