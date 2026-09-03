//go:build e2e

package production

import (
	"context"
	"fmt"
	"testing"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// keysetZeroRowID is the canonical zero UUID, used as the row_id arg for a
// keyset cursor pinned to a synthetic boundary value: the row_id disjunct only
// fires on an exact tie with the preceding key, which a synthetic boundary
// (a value no seeded row carries) never hits, so the arm is inert. Every cursor
// must still END on row_id — the engine now rejects cursors lacking that
// trailing tiebreak (validateKeysetCursor, #183).
const keysetZeroRowID = "00000000-0000-0000-0000-000000000000"

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
		{"created_at_cursor_resurrection", testKeysetCreatedAtCursorResurrection},
		{"cursor_with_mixed_filters", testKeysetCursorWithMixedFilters},
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
			Columns: []model.KeysetColumn{
				{Attribute: "count", Direction: forma.SortOrderAsc},
				{Attribute: "row_id", Direction: forma.SortOrderAsc}, // required trailing tiebreak (#183)
			},
			Values: []any{float64(500), keysetZeroRowID}, // 500 matches no row ⇒ row_id arm inert
			Mode:   model.KeysetCursorModeAfter,
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

// testKeysetCreatedAtCursorResurrection pins the #212 finding — a created_at
// cursor must not resurrect a superseded version — on top of the #460
// contract that created_at is now version-INVARIANT on every tier: the S3
// projection reads the exported ltbase_created_at, so an update no longer
// moves the winner's created_at forward. A row therefore keeps its cursor
// position across an update and across the flush boundary; what the cursor
// must still never do is admit the row twice, once per version.
func testKeysetCreatedAtCursorResurrection(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	a := CreateEvent(wide, map[string]any{"title": "ca-a", "count": float64(900)})
	if err := env.ApplyEvents(ctx, a); err != nil {
		t.Fatalf("apply create a: %v", err)
	}
	mustFlush(ctx, t, env) // delta #1: A@t1

	// t1 < t2 < t3 < t4 must hold STRICTLY, and every leg here is CROSS-ROW,
	// which #274's per-row monotonicity cannot order: b and c are different rows
	// (applied one per ApplyEvents call precisely so each lands on its own
	// millisecond — one batch would let two independent clock reads tie), and
	// upd is compared against c. The cursor below is pinned to c.ChangedAt with
	// B expected to qualify strictly below it, so a tie does not merely weaken
	// the probe, it changes what it asserts (#276).
	waitClockPast(t, a)
	b := CreateEvent(wide, map[string]any{"title": "ca-b", "count": float64(400)})
	if err := env.ApplyEvents(ctx, b); err != nil {
		t.Fatalf("apply create b: %v", err)
	}
	waitClockPast(t, b)
	c := CreateEvent(wide, map[string]any{"title": "ca-c", "count": float64(600)})
	if err := env.ApplyEvents(ctx, c); err != nil {
		t.Fatalf("apply create c: %v", err)
	}
	waitClockPast(t, c)
	upd := UpdateEvent(wide, a.RowID, map[string]any{"count": float64(100)})
	if err := env.ApplyEvents(ctx, upd); err != nil {
		t.Fatalf("apply update: %v", err)
	}
	// t1 < t2 < t3 < t4 strictly, or the probe degrades into a ver_ts tie (#210).
	assertStrictlyNewer(t, []*Event{a, b, c}, []*Event{b, c, upd})
	mustFlush(ctx, t, env) // delta #2: B@t2, C@t3, A_v2@t4; all rows clean

	// Positive control (oracle-checked): all three rows visible.
	res := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 10})
	if res != nil && len(res.Records) != 3 {
		t.Fatalf("control returned %d rows, want 3", len(res.Records))
	}

	// created_at DESC, cursor after C(t3) → created_at < t3. Two LWW winners
	// qualify: B(t2) and A, whose created_at stayed t1 through the update
	// (#460 — pre-fix A's winner carried the update-time changed_at t4 and
	// fell outside the cursor entirely). Each must appear EXACTLY once:
	// pre-#212 the superseded A@t1 version passed the cursor pre-dedup, won
	// rn = 1, and resurrected A as a second row.
	page, err := env.Query(ctx, Query{
		Schema: wide,
		Keyset: &model.KeysetCursor{
			Columns: []model.KeysetColumn{
				{Attribute: "created_at", Direction: forma.SortOrderDesc},
				{Attribute: "row_id", Direction: forma.SortOrderAsc}, // required trailing tiebreak (#183)
			},
			// Record-derived boundary at C: the row_id arm (created_at = t3 AND
			// row_id > c.RowID) excludes C itself while B(t2) qualifies via the
			// leading created_at < t3 disjunct.
			Values: []any{c.ChangedAt, c.RowID.String()},
			Mode:   model.KeysetCursorModeAfter,
		},
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("created_at keyset query: %v", err)
	}
	// created_at DESC, row_id ASC: B(t2) precedes A(t1).
	if len(page.Records) != 2 || page.Records[0].RowID != b.RowID || page.Records[1].RowID != a.RowID {
		t.Fatalf("created_at keyset page = %v, want exactly [%s %s]: either a stale pre-dedup version leaked through the cursor, or a winner's created_at drifted off its creation time (#460)",
			pageRowIDs(page), b.RowID, a.RowID)
	}
	if got := page.Records[1].CreatedAt; got != a.ChangedAt {
		t.Fatalf("A created_at = %d, want its creation stamp %d: the federated route must not report the LWW version stamp (#460)",
			got, a.ChangedAt)
	}
}

// testKeysetCursorWithMixedFilters drives a mixed MAIN+EAV composite filter
// (DuckArgs + PgMainArgs) together with a keyset cursor through the real
// engine: one statement, one placeholder style, args bound in appearance
// order (#161/#212). Phase 2 supersedes a row so the cursor must also drop
// its live version without resurrecting the old one.
func testKeysetCursorWithMixedFilters(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	rows := make([]*Event, 0, 5)
	for i := 0; i < 5; i++ {
		rows = append(rows, CreateEvent(wide, map[string]any{
			"title": fmt.Sprintf("grp-%02d", i),  // MAIN text_01 → PgMain pushdown
			"note":  fmt.Sprintf("keep-%02d", i), // EAV text → DuckDB clause
			"count": float64(100 * (i + 1)),      // 100..500
		}))
	}
	if err := env.ApplyEvents(ctx, rows...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}
	mustFlush(ctx, t, env) // single generation, all rows clean

	filters := []Filter{
		{Attr: "title", Op: "starts_with", Value: "grp-"},
		{Attr: "note", Op: "contains", Value: "keep"},
	}

	// Positive control (oracle-checked): the composite matches all five rows.
	res := env.AssertQueryMatches(ctx, Query{Schema: wide, Filters: filters, Limit: 10})
	if res != nil && len(res.Records) != 5 {
		t.Fatalf("filter control returned %d rows, want 5", len(res.Records))
	}

	keysetQuery := Query{
		Schema:  wide,
		Filters: filters,
		Sorts:   []Sort{{Attr: "count"}},
		Keyset: &model.KeysetCursor{
			Columns: []model.KeysetColumn{
				{Attribute: "count", Direction: forma.SortOrderAsc},
				{Attribute: "row_id", Direction: forma.SortOrderAsc}, // required trailing tiebreak (#183)
			},
			Values: []any{float64(250), keysetZeroRowID}, // 250 matches no row ⇒ row_id arm inert
			Mode:   model.KeysetCursorModeAfter,
		},
		Limit: 10,
	}

	assertKeysetPage(ctx, t, env, keysetQuery, rows[2], rows[3], rows[4]) // 300, 400, 500

	// Supersede 300 → 50: the winner now sorts before the cursor and must
	// vanish; its 300 version must not resurrect.
	upd := UpdateEvent(wide, rows[2].RowID, map[string]any{"count": float64(50)})
	if err := env.ApplyEvents(ctx, upd); err != nil {
		t.Fatalf("apply update: %v", err)
	}
	assertStrictlyNewer(t, []*Event{rows[2]}, []*Event{upd})
	mustFlush(ctx, t, env)

	// Winner still matches the filters (flush snapshots the full row).
	res = env.AssertQueryMatches(ctx, Query{Schema: wide, Filters: filters, Limit: 10})
	if res != nil && len(res.Records) != 5 {
		t.Fatalf("post-update filter control returned %d rows, want 5", len(res.Records))
	}

	assertKeysetPage(ctx, t, env, keysetQuery, rows[3], rows[4]) // 400, 500
}

// assertKeysetPage runs a keyset query (no oracle support for cursors) and
// requires the page to equal exactly the given events, in order.
func assertKeysetPage(ctx context.Context, t *testing.T, env *Env, q Query, want ...*Event) {
	t.Helper()
	page, err := env.Query(ctx, q)
	if err != nil {
		t.Fatalf("keyset query: %v", err)
	}
	ok := len(page.Records) == len(want)
	if ok {
		for i := range want {
			if page.Records[i].RowID != want[i].RowID {
				ok = false
				break
			}
		}
	}
	if !ok {
		wantIDs := make([]string, 0, len(want))
		for _, w := range want {
			wantIDs = append(wantIDs, w.RowID.String())
		}
		t.Fatalf("keyset page = %v, want exactly %v", pageRowIDs(page), wantIDs)
	}
}
