//go:build e2e

package production

import (
	"context"
	"fmt"
	"testing"
)

// TestFilterLWW (#178): filter predicates must be applied AFTER last-write-
// wins deduplication. A newer version that does not match the filter — or a
// tombstone carrying no attributes at all — must never expose an older
// version whose stale values still match. The production template guarantees
// this via the s3_source row_id semijoin + the post-dedup filter in the
// visible CTE (internal/sqlgen/advanced_query_template_duckdb.go, fixed in
// #173/PR #191); this suite pins that contract end to end and is the
// regression gate for the #195 single-scan pushdown rewrite.
func TestFilterLWW(t *testing.T) {
	cluster := SharedCluster(t)
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	scenarios := []struct {
		name string
		run  func(ctx context.Context, t *testing.T, env *Env, schema SchemaRef)
	}{
		{"stale_filter_delta_generations", testStaleFilterDeltaGenerations},
		{"stale_filter_base_vs_delta", testStaleFilterBaseVsDelta},
		{"tombstone_suppresses_stale_filter", testTombstoneSuppressesStaleFilter},
		{"hot_shadow_stale_filter", testHotShadowStaleFilter},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			sc.run(context.Background(), t, NewEnv(t, cluster), wide)
		})
	}
}

// assertStrictlyNewer fails fast when any new version's changed_at is not
// strictly after its predecessor's at millisecond resolution — otherwise an
// LWW probe degrades into the undefined equal-ver_ts tie tracked by #210.
func assertStrictlyNewer(t *testing.T, olds, news []*Event) {
	t.Helper()
	for i := range olds {
		if news[i].ChangedAt <= olds[i].ChangedAt {
			t.Fatalf("row %d: newer changed_at %d not strictly after older %d",
				i, news[i].ChangedAt, olds[i].ChangedAt)
		}
	}
}

// assertZeroRows runs an oracle-checked query that must return no rows: the
// predicate matches only a superseded version, so any row in the result is a
// filter-before-LWW resurrection.
func assertZeroRows(ctx context.Context, t *testing.T, env *Env, name string, q Query) {
	t.Helper()
	if res := env.AssertQueryMatches(ctx, q); res != nil && len(res.Records) != 0 {
		t.Errorf("%s: stale probe returned %d rows, want 0", name, len(res.Records))
	}
}

// testStaleFilterDeltaGenerations is issue #178 scenario 1 in the
// delta-generation layout: v1 (matching) in delta #1, v2 (non-matching) in
// delta #2, no hot rows. The semijoin admits every row_id via its v1 match;
// LWW must pick v2 and the visible filter must then reject it.
func testStaleFilterDeltaGenerations(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := make([]*Event, 0, 5)
	for i := 0; i < 5; i++ {
		creates = append(creates, CreateEvent(wide, map[string]any{
			"title": fmt.Sprintf("open-%02d", i),
			"count": float64(100000 + i),
		}))
	}
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}
	mustFlush(ctx, t, env) // delta #1 = v1 @ create-time ver_ts

	// Load-bearing positive: v1 is served from delta #1 by its own value
	// (guards every zero-row probe below against a broken read path).
	res := env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "title", Value: "open-03"}},
		Limit:   10,
	})
	if res != nil && len(res.Records) != 1 {
		t.Fatalf("v1 positive control returned %d rows, want 1", len(res.Records))
	}

	v2 := make([]*Event, 0, len(creates))
	for i, c := range creates {
		v2 = append(v2, UpdateEvent(wide, c.RowID, map[string]any{
			"title": fmt.Sprintf("closed-%02d", i),
			"count": float64(200000 + i),
		}))
	}
	if err := env.ApplyEvents(ctx, v2...); err != nil {
		t.Fatalf("apply v2 updates: %v", err)
	}
	assertStrictlyNewer(t, creates, v2)
	flush2 := mustFlush(ctx, t, env) // delta #2 = v2 @ update-time ver_ts
	if got := countTier(flush2.Manifests[wide.ID], "delta"); got != 2 {
		t.Fatalf("manifest holds %d delta files, want 2 (v1 and v2 generations)", got)
	}

	// Positive: the winning v2 generation is reachable by its value.
	env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "title", Value: "closed-03"}},
		Limit:   10,
	})

	// Adversarial: predicates matching ONLY the superseded v1 generation.
	assertZeroRows(ctx, t, env, "equals_stale_title", Query{
		Schema: wide, Filters: []Filter{{Attr: "title", Value: "open-03"}}, Limit: 10})
	assertZeroRows(ctx, t, env, "starts_with_stale_title", Query{
		Schema: wide, Filters: []Filter{{Attr: "title", Op: "starts_with", Value: "open-"}}, Limit: 10})
	assertZeroRows(ctx, t, env, "range_stale_count", Query{
		Schema: wide, Filters: []Filter{{Attr: "count", Op: "lt", Value: "200000"}}, Limit: 10})
}

// testStaleFilterBaseVsDelta is issue #178 scenario 1 in the base-vs-delta
// layout: v1 (matching) in base via init + onboarding cleanup, v2
// (non-matching) in a delta flushed afterwards. Deterministic under #210:
// base ver_ts = create time < delta ver_ts = update time.
func testStaleFilterBaseVsDelta(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := make([]*Event, 0, 5)
	for i := 0; i < 5; i++ {
		creates = append(creates, CreateEvent(wide, map[string]any{
			"title": fmt.Sprintf("open-%02d", i),
			"count": float64(100000 + i),
		}))
	}
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}
	report, err := env.RunInit(ctx, wide) // base = v1 @ create-time ver_ts
	if err != nil {
		t.Fatalf("run init: %v", err)
	}
	if report.RowsExported != 5 {
		t.Fatalf("init exported %d rows, want 5", report.RowsExported)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID)

	// Load-bearing positive: base serves v1 by its own value.
	res := env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "title", Value: "open-02"}},
		Limit:   10,
	})
	if res != nil && len(res.Records) != 1 {
		t.Fatalf("v1 positive control returned %d rows, want 1", len(res.Records))
	}

	v2 := make([]*Event, 0, len(creates))
	for i, c := range creates {
		v2 = append(v2, UpdateEvent(wide, c.RowID, map[string]any{
			"title": fmt.Sprintf("closed-%02d", i),
			"count": float64(200000 + i),
		}))
	}
	if err := env.ApplyEvents(ctx, v2...); err != nil {
		t.Fatalf("apply v2 updates: %v", err)
	}
	assertStrictlyNewer(t, creates, v2)
	mustFlush(ctx, t, env) // delta = v2 @ update-time ver_ts; rows now clean

	// Positive: v2 reachable.
	env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "title", Value: "closed-02"}},
		Limit:   10,
	})

	// Adversarial: v1-only predicates across base/delta tiers.
	assertZeroRows(ctx, t, env, "equals_stale_title_base", Query{
		Schema: wide, Filters: []Filter{{Attr: "title", Value: "open-02"}}, Limit: 10})
	assertZeroRows(ctx, t, env, "starts_with_stale_title_base", Query{
		Schema: wide, Filters: []Filter{{Attr: "title", Op: "starts_with", Value: "open-"}}, Limit: 10})
	assertZeroRows(ctx, t, env, "range_stale_count_base", Query{
		Schema: wide, Filters: []Filter{{Attr: "count", Op: "lt", Value: "200000"}}, Limit: 10})
}
