//go:build e2e

package production

import (
	"context"
	"testing"
)

// TestThreeTierSoftDelete (#177 scenario 7): rows deleted in one tier while a
// live version exists in another must be invisible, whichever mechanism is in
// play — the dirty-set anti-join for an unflushed (hot) tombstone over a live
// base row, and parquet LWW for a flushed (delta) tombstone over a live base
// row. Live rows in every tier stay visible.
func TestThreeTierSoftDelete(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 6})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID)

	// Load-bearing positive: all 6 rows served from base before any delete.
	pre := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	if pre != nil && len(pre.Records) != 6 {
		t.Fatalf("pre-delete result has %d rows, want 6", len(pre.Records))
	}

	// creates[1]: tombstone flushed to delta; live v1 stays in base parquet.
	if err := env.ApplyEvents(ctx, DeleteEvent(wide, creates[1].RowID)); err != nil {
		t.Fatalf("apply delta-tier delete: %v", err)
	}
	mustFlush(ctx, t, env)

	// creates[0]: tombstone stays hot (unflushed); live v1 stays in base.
	if err := env.ApplyEvents(ctx, DeleteEvent(wide, creates[0].RowID)); err != nil {
		t.Fatalf("apply hot-tier delete: %v", err)
	}

	// Two live hot rows so the visible set spans base AND hot.
	fresh := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 2})
	if err := env.ApplyEvents(ctx, fresh...); err != nil {
		t.Fatalf("apply hot creates: %v", err)
	}

	// 6 visible: 4 base survivors + 2 hot; both deleted rows invisible.
	result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	if result != nil && len(result.Records) != 6 {
		t.Fatalf("mixed-tier result has %d rows, want 6 (2 of 8 deleted)", len(result.Records))
	}

	// Live control first (guards the zero-row probes), then each tombstone.
	env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "title", Value: creates[2].Attrs["title"].(string)}},
		Limit:   10,
	})
	// Hot tombstone vs live base version: anti-join must hide it (oracle: 0 rows).
	env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "title", Value: creates[0].Attrs["title"].(string)}},
		Limit:   10,
	})
	// Delta tombstone vs live base version: parquet LWW must hide it (oracle: 0 rows).
	env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "title", Value: creates[1].Attrs["title"].(string)}},
		Limit:   10,
	})
	// Ordering with deletions interleaved across tiers.
	env.AssertQueryMatches(ctx, Query{
		Schema: wide,
		Sorts:  []Sort{{Attr: "count", Desc: true}},
		Limit:  100,
	})
}
