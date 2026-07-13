//go:build e2e

package production

import (
	"context"
	"fmt"
	"testing"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// TestPaginationUnderInsert (#183, epic #172 Phase 3) contrasts the two
// pagination strategies under a concurrent front-insert. OFFSET windows
// re-base on every request, so a row inserted before the current position
// duplicates a boundary row and shifts the tail — deterministic only thanks
// to Task 1's total order, but never a no-dup/no-skip guarantee. Keyset
// cursors carry that guarantee: a row inserted into already-visited territory
// is neither duplicated nor able to displace an unread row. The third
// scenario pins the one place the keyset guarantee lapses — a cursor whose
// last column is not row_id silently skips a tie group.
func TestPaginationUnderInsert(t *testing.T) {
	cluster := SharedCluster(t)
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	scenarios := []struct {
		name string
		run  func(ctx context.Context, t *testing.T, env *Env, schema SchemaRef)
	}{
		{"offset_insert_characterization", testOffsetInsertCharacterization},
		{"keyset_insert_no_dup_no_skip", testKeysetInsertNoDupNoSkip},
		{"keyset_tie_omission_probe", testKeysetTieOmissionProbe},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			sc.run(context.Background(), t, NewEnv(t, cluster), wide)
		})
	}
}

const (
	// paginationFarFutureMs is a created_at sentinel comfortably past any real
	// row timestamp (~1.75e12 ms in 2026) yet float64-exact (< 2^53), so an
	// "after" cursor at [created_at DESC] carrying it admits every row. It is
	// the idiomatic open first page for a descending keyset scan, which the
	// engine otherwise only positions from a prior page's last record.
	paginationFarFutureMs = int64(4_000_000_000_000) // ~year 2096
	// paginationMaxRowID is the largest canonical UUID string; paired with the
	// created_at sentinel it never matches (the created_at disjunct already
	// admits everything), so its only job is to be a valid row_id arg.
	paginationMaxRowID = "ffffffff-ffff-ffff-ffff-ffffffffffff"
)

// mustQuery runs a raw (non-oracle) query and fails fast on error.
func mustQuery(ctx context.Context, t *testing.T, env *Env, q Query) *QueryResult {
	t.Helper()
	res, err := env.Query(ctx, q)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	return res
}

// nextKeysetCursor builds an "after" cursor from a page's last record for the
// given system columns, mirroring the engine's own extractCursorFromRecord
// value types (created_at → int64 CreatedAt, row_id → RowID.String()).
func nextKeysetCursor(cols []model.KeysetColumn, rec *model.PersistentRecord) *model.KeysetCursor {
	vals := make([]any, len(cols))
	for i, c := range cols {
		switch c.Attribute {
		case "created_at":
			vals[i] = rec.CreatedAt
		case "row_id":
			vals[i] = rec.RowID.String()
		}
	}
	return &model.KeysetCursor{Columns: cols, Values: vals, Mode: model.KeysetCursorModeAfter}
}

// assertOffsetPage pins one OFFSET window to an exact ordered row-id set and
// its reported Total.
func assertOffsetPage(t *testing.T, page *QueryResult, want []*Event, wantTotal int64) {
	t.Helper()
	got := pageRowIDs(page)
	wantIDs := make([]string, len(want))
	for i, w := range want {
		wantIDs[i] = w.RowID.String()
	}
	if !equalStrings(got, wantIDs) {
		t.Fatalf("offset window = %v, want exactly %v (fixture non-determinism or engine re-base drift)", got, wantIDs)
	}
	if page.Total != wantTotal {
		t.Fatalf("offset window total = %d, want %d", page.Total, wantTotal)
	}
}

// testOffsetInsertCharacterization is an honest characterization of OFFSET
// pagination under a front-insert, NOT a bug pin. OFFSET windows re-base on
// every request by design: inserting a row that sorts before the current
// position duplicates the page-1/page-2 boundary row and shifts the tail, and
// the freshly inserted row — sorting into an already-served window — is never
// seen. This is inherent to OFFSET; it is deterministic here only because
// Task 1 appended row_id ASC to give equal keys a total order (counts are
// unique so no ties even arise). The no-dup/no-skip guarantee the issue asks
// for is a keyset property, pinned by keyset_insert_no_dup_no_skip.
func testOffsetInsertCharacterization(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	const n = 30
	creates := make([]*Event, 0, n)
	for i := 0; i < n; i++ {
		creates = append(creates, CreateEvent(wide, map[string]any{
			"title": fmt.Sprintf("off-%02d", i),
			"count": float64(100 + 10*i), // 100..390, unique ⇒ count ASC = creation order
		}))
	}
	mustApplyEvents(ctx, t, env, "offset-characterization creates", creates...)
	mustFlush(ctx, t, env) // rows cold; the shifter below is the only hot row

	base := Query{Schema: wide, Sorts: []Sort{{Attr: "count"}}, Limit: 10}

	// Page 1 read BEFORE the insert: the untouched [r0..r9] window over 30 rows.
	p1 := mustQuery(ctx, t, env, base) // Offset 0
	assertOffsetPage(t, p1, creates[0:10], 30)

	// The shifter sorts before every original and stays hot (no flush): the
	// federated pg_source picks it up, re-basing every later OFFSET window by
	// one position over the order [shifter, r0..r29].
	shifter := CreateEvent(wide, map[string]any{"title": "shifter", "count": float64(50)})
	mustApplyEvents(ctx, t, env, "front-insert shifter", shifter)

	q := base
	q.Offset = 10
	p2 := mustQuery(ctx, t, env, q)
	q.Offset = 20
	p3 := mustQuery(ctx, t, env, q)
	q.Offset = 30
	p4 := mustQuery(ctx, t, env, q)

	assertOffsetPage(t, p2, creates[9:19], 31)  // r9..r18 — r9 duplicated at the boundary
	assertOffsetPage(t, p3, creates[19:29], 31) // r19..r28
	assertOffsetPage(t, p4, creates[29:30], 31) // [r29]

	// The new row never surfaces; every original appears at least once.
	pages := [][]string{pageRowIDs(p1), pageRowIDs(p2), pageRowIDs(p3), pageRowIDs(p4)}
	newID := shifter.RowID.String()
	seen := make(map[string]bool, n)
	for _, page := range pages {
		for _, id := range page {
			if id == newID {
				t.Fatalf("front-inserted row %s surfaced in an OFFSET window; under a front-insert it never should", newID)
			}
			seen[id] = true
		}
	}
	for _, c := range creates {
		if !seen[c.RowID.String()] {
			t.Fatalf("original row %s missing from OFFSET windows 1-4", c.RowID.String())
		}
	}
}

// testKeysetInsertNoDupNoSkip pins the no-dup/no-skip guarantee the issue asks
// for: keyset pagination over [created_at DESC, row_id DESC] survives a
// front-insert cleanly. After page 1 a new row with the newest created_at is
// inserted; it sorts into already-visited front territory, so the cursor
// (positioned after page 1's last record) excludes it and pages 2-3 return the
// remaining originals. Every original appears exactly once; the new row appears
// zero times — "current or future page" is vacuously satisfied and it is never
// duplicated. A duplicate or omission here would be a keyset engine bug.
func testKeysetInsertNoDupNoSkip(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	const n = 30
	creates := make([]*Event, 0, n)
	for i := 0; i < n; i++ {
		creates = append(creates, CreateEvent(wide, map[string]any{
			"title": fmt.Sprintf("ks-%02d", i),
			"count": float64(1000 + i),
		}))
	}
	mustApplyEvents(ctx, t, env, "keyset no-dup creates", creates...)
	mustFlush(ctx, t, env)

	cols := []model.KeysetColumn{
		{Attribute: "created_at", Direction: forma.SortOrderDesc},
		{Attribute: "row_id", Direction: forma.SortOrderDesc},
	}
	cursor := &model.KeysetCursor{
		Columns: cols,
		Values:  []any{paginationFarFutureMs, paginationMaxRowID}, // open first page
		Mode:    model.KeysetCursorModeAfter,
	}

	seen := make(map[string]int, n+1)
	var newID string
	for p := 0; p < 3; p++ {
		page := mustQuery(ctx, t, env, Query{Schema: wide, Keyset: cursor, Limit: 10})
		if len(page.Records) != 10 {
			t.Fatalf("keyset page %d returned %d rows, want 10", p+1, len(page.Records))
		}
		for _, id := range pageRowIDs(page) {
			seen[id]++
		}
		if p == 0 {
			// Newest created_at ⇒ sorts to the front, into already-visited
			// territory a correct keyset never re-serves.
			newEv := CreateEvent(wide, map[string]any{"title": "ks-inserted", "count": float64(9999)})
			mustApplyEvents(ctx, t, env, "front-insert new row", newEv)
			newID = newEv.RowID.String()
		}
		cursor = nextKeysetCursor(cols, page.Records[len(page.Records)-1])
	}

	for _, c := range creates {
		if id := c.RowID.String(); seen[id] != 1 {
			t.Fatalf("original row %s appeared %d times across keyset pages, want exactly 1 (keyset dup/skip under front insert — engine bug)", id, seen[id])
		}
	}
	if seen[newID] != 0 {
		t.Fatalf("front-inserted row %s appeared %d times across keyset pages, want 0 (it sorted into visited territory)", newID, seen[newID])
	}

	// Positive control: the new row is real and visible outside the cursor window.
	assertRowCount(ctx, t, env, "post-insert full requery", Query{Schema: wide, Limit: 40}, 31)
}

// testKeysetTieOmissionProbe pins the lossy edge of keyset pagination: a cursor
// whose last column is NOT row_id silently skips a tie group. Five rows are
// collapsed onto one created_at (cold, identical timestamps); after reading the
// first keyset page over [created_at DESC], the continuation predicate is
// created_at < T — but every remaining tied row also has created_at = T, so all
// three unread rows are excluded and the next page is empty. This is PINNED as
// the current behavior, not asserted as correct: the keyset contract requires
// callers to end cursors with row_id (internal/federated/keyset.go:98-113
// documents the row comparison but nothing enforces the tiebreak). Tracked by
// follow-up #NNN.
func testKeysetTieOmissionProbe(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	const n = 5
	events := make([]*Event, 0, n)
	for i := 0; i < n; i++ {
		events = append(events, CreateEvent(wide, map[string]any{
			"title": fmt.Sprintf("tie-%02d", i),
			"count": float64(10 + i),
		}))
	}
	mustApplyEvents(ctx, t, env, "tie-omission creates", events...)

	// Collapse all five unflushed slots onto one created_at so the cursor tie
	// group is exact; mirror into the events for the oracle (folds by ChangedAt).
	tied := events[0].ChangedAt
	env.ExecSQL(ctx, "UPDATE change_log SET changed_at = $1 WHERE schema_id = $2 AND flushed_at = 0", tied, wide.ID)
	for _, ev := range events {
		ev.ChangedAt = tied
	}

	mustFlush(ctx, t, env)
	env.ExecSQL(ctx, "DELETE FROM change_log") // cold-only; all five share created_at = tied

	// Positive control: the read path returns all five tied rows, so a later
	// empty page cannot be a false green from a broken read path.
	assertRowCount(ctx, t, env, "tie control", Query{Schema: wide, Limit: 10}, n)

	cols := []model.KeysetColumn{{Attribute: "created_at", Direction: forma.SortOrderDesc}}
	page1 := mustQuery(ctx, t, env, Query{
		Schema: wide,
		Keyset: &model.KeysetCursor{Columns: cols, Values: []any{paginationFarFutureMs}, Mode: model.KeysetCursorModeAfter},
		Limit:  2,
	})
	if len(page1.Records) != 2 {
		t.Fatalf("tie page 1 returned %d rows, want 2", len(page1.Records))
	}

	// Continue from page 1's last created_at WITHOUT a row_id tiebreak: the next
	// predicate is created_at < T, which excludes all three remaining tied rows.
	// PIN exactly that lossy behavior as an empty page (follow-up #NNN).
	probe := Query{Schema: wide, Keyset: nextKeysetCursor(cols, page1.Records[len(page1.Records)-1]), Limit: 2}
	assertKeysetPage(ctx, t, env, probe) // no want events ⇒ the omission is the empty page
}
