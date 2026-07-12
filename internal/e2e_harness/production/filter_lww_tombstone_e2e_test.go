//go:build e2e

package production

import (
	"context"
	"testing"
)

// testTombstoneSuppressesStaleFilter is issue #178 scenario 2: the newest
// version is a tombstone with no attributes at all, so the filter can only
// ever match the superseded live version. The deleted row must not appear —
// first while the tombstone is hot (dirty-set anti-join), then after it is
// flushed to delta parquet (LWW + deleted_ts suppression in visible).
func testTombstoneSuppressesStaleFilter(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := buildOpenCreates(wide, 4)
	mustApplyEvents(ctx, t, env, "apply creates", creates...)
	mustFlush(ctx, t, env) // delta #1 = live v1 for all four rows

	// Load-bearing positive: the victim row is served by its value pre-delete.
	assertRowCount(ctx, t, env, "pre-delete positive control", Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "title", Value: "open-00"}},
		Limit:   10,
	}, 1)

	dels := []*Event{
		DeleteEvent(wide, creates[0].RowID),
		DeleteEvent(wide, creates[1].RowID),
	}
	mustApplyEvents(ctx, t, env, "apply deletes", dels...)
	assertStrictlyNewer(t, creates[:2], dels)

	// Hot-tombstone arm: the dirty set evicts the delta v1; the tombstone
	// itself has no entity_main row, so nothing hot can match either.
	assertZeroRows(ctx, t, env, "hot_tombstone_equals", Query{
		Schema: wide, Filters: []Filter{{Attr: "title", Value: "open-00"}}, Limit: 10})

	flush2 := mustFlush(ctx, t, env) // delta #2 = tombstones (NULL attrs)
	if got := countTier(flush2.Manifests[wide.ID], "delta"); got != 2 {
		t.Fatalf("manifest holds %d delta files, want 2 (live + tombstone generations)", got)
	}

	// Flushed-tombstone arm: semijoin admits the row via its live v1 match;
	// the tombstone wins LWW and visible drops it on deleted_ts.
	assertZeroRows(ctx, t, env, "flushed_tombstone_equals_0", Query{
		Schema: wide, Filters: []Filter{{Attr: "title", Value: "open-00"}}, Limit: 10})
	assertZeroRows(ctx, t, env, "flushed_tombstone_equals_1", Query{
		Schema: wide, Filters: []Filter{{Attr: "title", Value: "open-01"}}, Limit: 10})
	// Range matching only the two deleted rows' v1 counts (100000, 100001).
	assertZeroRows(ctx, t, env, "flushed_tombstone_range", Query{
		Schema: wide, Filters: []Filter{{Attr: "count", Op: "lte", Value: "100001"}}, Limit: 10})

	// Survivors stay reachable — the suppression is per-row, not per-file.
	assertRowCount(ctx, t, env, "survivor probe", Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "title", Value: "open-02"}},
		Limit:   10,
	}, 1)
	assertRowCount(ctx, t, env, "post-delete total", Query{Schema: wide, Limit: 10}, 2)
}

// testHotShadowStaleFilter is issue #178 scenario 3: the newer version is a
// dirty (unflushed) hot row. The anti-join must evict the warm delta v1
// before the filter could match it, and the non-matching hot version must
// not reintroduce the row. Complements testDirtyHotShadowsCold (dirty over
// base, single equality probe) with a warm-delta layout, range/prefix
// operators, and a hot-tombstone arm.
func testHotShadowStaleFilter(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := buildOpenCreates(wide, 5)
	mustApplyEvents(ctx, t, env, "apply creates", creates...)
	mustFlush(ctx, t, env) // delta = v1 for all five rows (warm tier)

	// Load-bearing positive: warm delta serves v1 by its value.
	assertRowCount(ctx, t, env, "warm positive control", Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "title", Value: "open-00"}},
		Limit:   10,
	}, 1)

	upd := UpdateEvent(wide, creates[0].RowID, map[string]any{
		"title": "closed-00",
		"count": float64(200000),
	})
	mustApplyEvents(ctx, t, env, "apply shadow update", upd)
	assertStrictlyNewer(t, creates[:1], []*Event{upd})

	// The dirty hot v2 does not match; the warm v1 must not resurface.
	assertZeroRows(ctx, t, env, "hot_shadow_equals", Query{
		Schema: wide, Filters: []Filter{{Attr: "title", Value: "open-00"}}, Limit: 10})
	// Prefix over all v1 titles: only the four unshadowed rows qualify.
	assertRowCount(ctx, t, env, "prefix probe", Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "title", Op: "starts_with", Value: "open-"}},
		Limit:   10,
	}, 4)
	// The hot v2 is reachable by its own value through the dirty path.
	assertRowCount(ctx, t, env, "hot-reachability probe", Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "count", Op: "gte", Value: "200000"}},
		Limit:   10,
	}, 1)

	// Hot-tombstone shadow over a warm v1.
	del := DeleteEvent(wide, creates[1].RowID)
	mustApplyEvents(ctx, t, env, "apply shadow delete", del)
	assertZeroRows(ctx, t, env, "hot_tombstone_shadow_equals", Query{
		Schema: wide, Filters: []Filter{{Attr: "title", Value: "open-01"}}, Limit: 10})
	assertRowCount(ctx, t, env, "post-shadow total", Query{Schema: wide, Limit: 10}, 4)
}
