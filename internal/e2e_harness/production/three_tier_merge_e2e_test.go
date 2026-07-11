//go:build e2e

package production

import (
	"context"
	"fmt"
	"testing"

	"github.com/lychee-technology/forma/internal/manifest"
)

// TestThreeTierMerge (#177 scenarios 4-6): merge-on-read correctness when
// base, delta, and hot tiers hold rows simultaneously — union of disjoint
// rows, LWW on triple overlap, dirty-set anti-join shadowing, and LWW keyed
// strictly on changed_at rather than tier layout. Every query is checked
// against the independent oracle (totals, row IDs, ordering, all attribute
// values).
func TestThreeTierMerge(t *testing.T) {
	cluster := SharedCluster(t)
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	scenarios := []struct {
		name string
		run  func(ctx context.Context, t *testing.T, env *Env, schema SchemaRef)
	}{
		{"union", testThreeTierUnion},
		{"triple_overlap_hot_wins", testTripleOverlapHotWins},
		{"dirty_hot_shadows_cold", testDirtyHotShadowsCold},
		{"changed_at_beats_tier_layout", testChangedAtBeatsTierLayout},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			sc.run(context.Background(), t, NewEnv(t, cluster), wide)
		})
	}
}

// testThreeTierUnion is #177 scenario 4: old rows in base (init + onboarding
// cleanup), mid rows in delta (flush), new rows hot (unflushed). All 24
// disjoint rows must be present exactly once, with a stable total order and
// pagination across tier boundaries.
func testThreeTierUnion(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	old := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 10})
	if err := env.ApplyEvents(ctx, old...); err != nil {
		t.Fatalf("apply old creates: %v", err)
	}
	report, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("run init: %v", err)
	}
	if report.RowsExported != 10 {
		t.Fatalf("init exported %d rows, want 10", report.RowsExported)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID)

	mid := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 8})
	if err := env.ApplyEvents(ctx, mid...); err != nil {
		t.Fatalf("apply mid creates: %v", err)
	}
	mustFlush(ctx, t, env)

	fresh := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 6})
	if err := env.ApplyEvents(ctx, fresh...); err != nil {
		t.Fatalf("apply hot creates: %v", err)
	}

	// Tier precondition: exactly the 6 hot rows are dirty.
	var dirty int64
	if err := env.Pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM change_log WHERE flushed_at = 0").Scan(&dirty); err != nil {
		t.Fatalf("count dirty rows: %v", err)
	}
	if dirty != 6 {
		t.Fatalf("dirty set has %d rows, want 6 (hot batch only)", dirty)
	}

	result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	if result != nil {
		if len(result.Records) != 24 {
			t.Fatalf("union result has %d rows, want 24 (10 base + 8 delta + 6 hot)", len(result.Records))
		}
		if !result.Plan.Routing.UseDuckDB {
			t.Errorf("union query did not route to duckdb: %+v", result.Plan.Routing)
		}
	}
	// Total order across tiers (count is unique per generated ordinal), and a
	// paginated window that straddles tier boundaries.
	env.AssertQueryMatches(ctx, Query{
		Schema: wide,
		Sorts:  []Sort{{Attr: "count"}},
		Limit:  100,
	})
	env.AssertQueryMatches(ctx, Query{
		Schema: wide,
		Sorts:  []Sort{{Attr: "count"}},
		Limit:  7,
		Offset: 5,
	})
}

// testTripleOverlapHotWins is #177 scenario 5: the same six row_ids exist in
// base (v1, via init), delta (v2, via flush), and hot (v3, unflushed). LWW
// must fold each to the hot v3 — the oracle's attribute comparison also
// proves v1-only attributes survive the partial updates.
func testTripleOverlapHotWins(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 6})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}
	report, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("run init: %v", err)
	}
	if report.RowsExported != 6 {
		t.Fatalf("init exported %d rows, want 6", report.RowsExported)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID)

	v2 := make([]*Event, 0, len(creates))
	for i, c := range creates {
		v2 = append(v2, UpdateEvent(wide, c.RowID, map[string]any{
			"title": fmt.Sprintf("delta-%02d", i),
			"count": float64(200000 + i),
		}))
	}
	if err := env.ApplyEvents(ctx, v2...); err != nil {
		t.Fatalf("apply v2 updates: %v", err)
	}
	mustFlush(ctx, t, env)

	v3 := make([]*Event, 0, len(creates))
	for i, c := range creates {
		v3 = append(v3, UpdateEvent(wide, c.RowID, map[string]any{
			"title": fmt.Sprintf("hot-%02d", i),
			"count": float64(300000 + i),
		}))
	}
	if err := env.ApplyEvents(ctx, v3...); err != nil {
		t.Fatalf("apply v3 updates: %v", err)
	}

	// Oracle folds create+v2+v3 per row: exactly 6 rows, all carrying v3
	// titles/counts — a duplicate, a v1/v2 resurrection, or a stale
	// attribute all fail the diff.
	result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	if result != nil && len(result.Records) != 6 {
		t.Fatalf("triple-overlap result has %d rows, want 6", len(result.Records))
	}
	// Filter keyed on a v3-only value must find its row through the dirty set.
	env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "title", Value: "hot-03"}},
		Limit:   10,
	})
	// Ordering over post-LWW values.
	env.AssertQueryMatches(ctx, Query{
		Schema: wide,
		Sorts:  []Sort{{Attr: "count", Desc: true}},
		Limit:  10,
	})
}

// testDirtyHotShadowsCold is #177 scenario 6 and the anti-join success
// criterion: a row lives in cold base parquet AND has an unflushed hot
// version. The hot version must shadow the cold one — including under a
// filter that only the stale cold version matches (the anti-join, not the
// filter, is what evicts it).
func testDirtyHotShadowsCold(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 5})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID)

	// Load-bearing positive: base serves all 5 rows before anything is dirty
	// (guards every zero-row probe below against a broken read path).
	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})

	oldTitle := creates[0].Attrs["title"].(string)
	update := UpdateEvent(wide, creates[0].RowID, map[string]any{
		"title": "shadow-00",
		"count": float64(500000),
	})
	if err := env.ApplyEvents(ctx, update); err != nil {
		t.Fatalf("apply shadow update: %v", err)
	}

	// 5 rows total: 4 pure base + 1 hot-shadowed; attribute diff proves the
	// shadowed row carries v2 values.
	result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})
	if result != nil && len(result.Records) != 5 {
		t.Fatalf("dirty-shadow result has %d rows, want 5", len(result.Records))
	}
	// The hot version is reachable by its new value...
	env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "title", Value: "shadow-00"}},
		Limit:   10,
	})
	// ...and the cold version is unreachable even by its old value: the row_id
	// is in the dirty set, so the anti-join must discard the base row before
	// the filter could match it (oracle expects zero rows).
	env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "title", Value: oldTitle}},
		Limit:   10,
	})
}

// testChangedAtBeatsTierLayout supersedes the ConflictingCreatedAndUpdatedAt
// stub. The init export contract makes created_at and updated_at orderings
// genuinely conflict across tiers: cdc-init stamps base rows with
// ltbase_created_at as the version timestamp (internal/cdc/init_exporter.go),
// so the base copy written LAST carries the OLDEST ver_ts while the freshest
// version lives in a delta written earlier. LWW must pick the strictly newest
// changed_at (the update time) — not created_at, tier tags, or file write
// order. The equal-ver_ts tie between CONFLICTING parquet versions (base
// state newer than the last flush) is undefined by the ranking and
// deliberately not exercised here; the init ver_ts contract is tracked by
// #210.
func testChangedAtBeatsTierLayout(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 5})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}
	mustFlush(ctx, t, env) // delta #1 = v1 @ create-time ver_ts

	v2 := make([]*Event, 0, len(creates))
	for i, c := range creates {
		v2 = append(v2, UpdateEvent(wide, c.RowID, map[string]any{
			"title": fmt.Sprintf("newer-%02d", i),
			"count": float64(400000 + i),
		}))
	}
	if err := env.ApplyEvents(ctx, v2...); err != nil {
		t.Fatalf("apply v2 updates: %v", err)
	}
	// The LWW discriminator below is v2's changed_at: assert it is strictly
	// later than the create's at millisecond resolution, so the test fails
	// fast instead of degrading into an undefined equal-ver_ts tie.
	for i := range creates {
		if v2[i].ChangedAt <= creates[i].ChangedAt {
			t.Fatalf("row %d: v2 changed_at %d not strictly after create changed_at %d",
				i, v2[i].ChangedAt, creates[i].ChangedAt)
		}
	}
	flush2 := mustFlush(ctx, t, env) // delta #2 = v2 @ update-time ver_ts
	if got := countTier(flush2.Manifests[wide.ID], "delta"); got != 2 {
		t.Fatalf("manifest holds %d delta files, want 2 (v1 and v2 generations)", got)
	}

	report, err := env.RunInit(ctx, wide) // base = v2 attrs @ create-time ver_ts
	if err != nil {
		t.Fatalf("run init: %v", err)
	}
	if report.RowsExported != 5 {
		t.Fatalf("init exported %d rows, want 5", report.RowsExported)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID) // no hot rows

	// Physical precondition: three copies of every row — v1 and v2 in the two
	// deltas plus the base duplicate of v2 — so dedup genuinely has to choose.
	assertBaseRows(ctx, t, env, report.Manifest, 5)
	var deltaRows int64
	for _, f := range manifest.FilterByTier(report.Manifest, "delta") {
		deltaRows += parquetRowCount(ctx, t, env, f.Path)
	}
	if deltaRows != 10 {
		t.Fatalf("delta tier holds %d physical rows, want 10 (v1 + v2 generations)", deltaRows)
	}

	// Oracle expects 5 rows carrying v2 values. A ranking keyed on created_at
	// sees all three copies tie and can surface v1; only changed_at picks the
	// delta-#2 version deterministically — any v1 leak fails the diff.
	result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})
	if result != nil && len(result.Records) != 5 {
		t.Fatalf("changed_at-conflict result has %d rows, want 5", len(result.Records))
	}
	env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "title", Value: "newer-03"}},
		Limit:   10,
	})
	// Positive range probe over v2 counts: all 5 rows qualify only if the v2
	// versions won. (Stale-value filter matrices on clean rows are #178.)
	env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "count", Op: "gte", Value: "400000"}},
		Limit:   10,
	})
}
