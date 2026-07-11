//go:build e2e

package production

import (
	"context"
	"testing"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// TestKeysetLWW (#212): the keyset cursor must be applied AFTER last-write-
// wins deduplication. Pre-fix, the DuckDB template injected the cursor into
// the ranked CTE — before ROW_NUMBER picked rn = 1 — so a superseded version
// that satisfied the cursor could win its partition and resurrect. This is
// the keyset twin of the #173/#178 filter-before-LWW bug and, with #178's
// suite, the regression gate for the #195 single-scan rewrite.
func TestKeysetLWW(t *testing.T) {
	cluster := SharedCluster(t)
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	scenarios := []struct {
		name string
		run  func(ctx context.Context, t *testing.T, env *Env, schema SchemaRef)
	}{
		{"attribute_cursor_resurrection", testKeysetAttributeCursorResurrection},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			sc.run(context.Background(), t, NewEnv(t, cluster), wide)
		})
	}
}

// pageRowIDs renders a page's row IDs for failure messages.
func pageRowIDs(page *QueryResult) []string {
	ids := make([]string, 0, len(page.Records))
	for _, r := range page.Records {
		ids = append(ids, r.RowID.String())
	}
	return ids
}

// testKeysetAttributeCursorResurrection is the issue #212 reproduction (the
// #178 Task 6 decision-gate probe): row A is superseded from count=900 to
// count=100 across two flushed delta generations; a cursor after count=500
// must return exactly C(600) — A's live version sorts before the cursor, and
// its stale 900 version must not resurrect.
func testKeysetAttributeCursorResurrection(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	a := CreateEvent(wide, map[string]any{"title": "ks-a", "count": float64(900)})
	b := CreateEvent(wide, map[string]any{"title": "ks-b", "count": float64(400)})
	c := CreateEvent(wide, map[string]any{"title": "ks-c", "count": float64(600)})
	if err := env.ApplyEvents(ctx, a, b, c); err != nil {
		t.Fatalf("apply creates: %v", err)
	}
	mustFlush(ctx, t, env) // delta #1: a=900, b=400, c=600

	upd := UpdateEvent(wide, a.RowID, map[string]any{"count": float64(100)})
	if err := env.ApplyEvents(ctx, upd); err != nil {
		t.Fatalf("apply update: %v", err)
	}
	assertStrictlyNewer(t, []*Event{a}, []*Event{upd})
	mustFlush(ctx, t, env) // delta #2: a=100; all rows clean

	// Positive control (oracle-checked): ascending count = a(100), b(400), c(600).
	res := env.AssertQueryMatches(ctx, Query{
		Schema: wide, Sorts: []Sort{{Attr: "count"}}, Limit: 10})
	if res != nil && len(res.Records) != 3 {
		t.Fatalf("sorted control returned %d rows, want 3", len(res.Records))
	}

	// Cursor after count=500: only C(600) qualifies among LWW winners.
	page, err := env.Query(ctx, Query{
		Schema: wide,
		Sorts:  []Sort{{Attr: "count"}},
		Keyset: &model.KeysetCursor{
			Columns: []model.KeysetColumn{{Attribute: "count", Direction: forma.SortOrderAsc}},
			Values:  []any{float64(500)},
			Mode:    model.KeysetCursorModeAfter,
		},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("keyset query: %v", err)
	}
	if len(page.Records) != 1 || page.Records[0].RowID != c.RowID {
		t.Fatalf("keyset page = %v, want exactly [%s]: a stale pre-dedup version leaked through the cursor",
			pageRowIDs(page), c.RowID)
	}
}
