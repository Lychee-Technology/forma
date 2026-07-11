//go:build e2e

package production

import (
	"context"
	"fmt"
	"testing"
)

// testTombstoneSuppressesStaleFilter is issue #178 scenario 2: the newest
// version is a tombstone with no attributes at all, so the filter can only
// ever match the superseded live version. The deleted row must not appear —
// first while the tombstone is hot (dirty-set anti-join), then after it is
// flushed to delta parquet (LWW + deleted_ts suppression in visible).
func testTombstoneSuppressesStaleFilter(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := make([]*Event, 0, 4)
	for i := 0; i < 4; i++ {
		creates = append(creates, CreateEvent(wide, map[string]any{
			"title": fmt.Sprintf("open-%02d", i),
			"count": float64(100000 + i),
		}))
	}
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}
	mustFlush(ctx, t, env) // delta #1 = live v1 for all four rows

	// Load-bearing positive: the victim row is served by its value pre-delete.
	res := env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "title", Value: "open-00"}},
		Limit:   10,
	})
	if res != nil && len(res.Records) != 1 {
		t.Fatalf("pre-delete positive control returned %d rows, want 1", len(res.Records))
	}

	dels := []*Event{
		DeleteEvent(wide, creates[0].RowID),
		DeleteEvent(wide, creates[1].RowID),
	}
	if err := env.ApplyEvents(ctx, dels...); err != nil {
		t.Fatalf("apply deletes: %v", err)
	}
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
	res = env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "title", Value: "open-02"}},
		Limit:   10,
	})
	if res != nil && len(res.Records) != 1 {
		t.Fatalf("survivor probe returned %d rows, want 1", len(res.Records))
	}
	res = env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})
	if res != nil && len(res.Records) != 2 {
		t.Fatalf("post-delete total is %d rows, want 2", len(res.Records))
	}
}
