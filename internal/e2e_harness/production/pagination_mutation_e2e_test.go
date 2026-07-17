//go:build e2e

package production

import (
	"context"
	"fmt"
	"strings"
	"testing"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// TestPaginationUnderInsert (#183, epic #172 Phase 3) contrasts the two
// pagination strategies under a concurrent insert. OFFSET windows re-base on
// every request, so a row inserted before the current position duplicates a
// boundary row and shifts the tail — deterministic only thanks to Task 1's
// total order, but never a no-dup/no-skip guarantee. Keyset cursors carry that
// guarantee: with an ascending scan a row inserted at the tail surfaces on a
// future page exactly once (keyset_insert_into_future_page); with a descending
// scan the same row sorts into already-visited territory and cannot be re-served
// without snapshot isolation (keyset_insert_front_desc_unreachable). The last
// scenario pins the enforced tiebreak contract: a cursor whose final column is
// not row_id is now REJECTED rather than allowed to silently skip a tie group.
func TestPaginationUnderInsert(t *testing.T) {
	cluster := SharedCluster(t)
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	scenarios := []struct {
		name string
		run  func(ctx context.Context, t *testing.T, env *Env, schema SchemaRef)
	}{
		{"offset_insert_characterization", testOffsetInsertCharacterization},
		{"keyset_insert_into_future_page", testKeysetInsertIntoFuturePage},
		{"keyset_insert_front_desc_unreachable", testKeysetInsertNoDupNoSkip},
		{"keyset_incomplete_cursor_rejected", testKeysetIncompleteCursorRejected},
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
// given system columns, using the value types the keyset predicate binds
// (created_at → int64 CreatedAt, row_id → RowID.String()).
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

// testKeysetInsertNoDupNoSkip (scenario keyset_insert_front_desc_unreachable) is
// the DESC half of the #183 liveness contract, and the deliberate spec tension:
// keyset pagination over [created_at DESC, row_id DESC] survives a front-insert
// cleanly, but the inserted row is UNREACHABLE. After page 1 a new row with the
// newest created_at is inserted; under DESC it sorts to the front — into
// already-visited territory the cursor (positioned after page 1's last record)
// can never re-serve. So every original appears exactly once and the new row
// appears zero times: no dup, no skip of a pre-existing row, but "row appears in
// a current or future page" is NOT satisfiable without snapshot isolation. The
// zero-count assertion is kept as the characterization of that gap. The liveness
// half of #183 — an inserted row that DOES surface on a later page — is pinned
// by testKeysetInsertIntoFuturePage, where an ascending scan sends the newest
// row into an unvisited window. A duplicate or a skipped original here would be
// a keyset engine bug.
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

// testKeysetInsertIntoFuturePage is the liveness half of #183: an ascending
// keyset scan over [created_at ASC, row_id ASC] paginates 30 pre-seeded rows;
// after page 1 a new row with the NEWEST created_at is inserted. Under ASC it
// sorts past every already-served window, into unvisited tail territory, so it
// MUST surface on a later page — exactly once, alongside all 30 originals,
// with no duplicate anywhere. This is the guarantee the front-insert DESC
// scenario cannot demonstrate (there the row sorts into visited territory).
func testKeysetInsertIntoFuturePage(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	const n = 30
	creates := make([]*Event, 0, n)
	for i := 0; i < n; i++ {
		creates = append(creates, CreateEvent(wide, map[string]any{
			"title": fmt.Sprintf("fut-%02d", i),
			"count": float64(2000 + i),
		}))
	}
	mustApplyEvents(ctx, t, env, "future-page creates", creates...)
	mustFlush(ctx, t, env) // originals cold; the tail insert below stays hot

	cols := []model.KeysetColumn{
		{Attribute: "created_at", Direction: forma.SortOrderAsc},
		{Attribute: "row_id", Direction: forma.SortOrderAsc},
	}
	// Open first page: a boundary below every real row (created_at 0, zero
	// row_id) so the ascending "after" predicate admits the whole set.
	cursor := &model.KeysetCursor{
		Columns: cols,
		Values:  []any{int64(0), keysetZeroRowID},
		Mode:    model.KeysetCursorModeAfter,
	}

	seen := make(map[string]int, n+1)
	var newID string
	// 30 originals + 1 insert over pages of 10 ⇒ the inserted tail row lands on
	// page 4; cap iterations well above that and stop on the first empty page.
	for p := 0; p < 8; p++ {
		page := mustQuery(ctx, t, env, Query{Schema: wide, Keyset: cursor, Limit: 10})
		if len(page.Records) == 0 {
			break
		}
		for _, id := range pageRowIDs(page) {
			seen[id]++
		}
		if p == 0 {
			// Newest created_at ⇒ sorts to the tail, into an unvisited window.
			newEv := CreateEvent(wide, map[string]any{"title": "fut-inserted", "count": float64(9999)})
			mustApplyEvents(ctx, t, env, "tail-insert new row", newEv)
			newID = newEv.RowID.String()
		}
		cursor = nextKeysetCursor(cols, page.Records[len(page.Records)-1])
	}

	for _, c := range creates {
		if id := c.RowID.String(); seen[id] != 1 {
			t.Fatalf("original row %s appeared %d times across keyset pages, want exactly 1 (ascending keyset dup/skip — engine bug)", id, seen[id])
		}
	}
	if seen[newID] != 1 {
		t.Fatalf("tail-inserted row %s appeared %d times, want exactly 1 (ascending insert must surface on a future page)", newID, seen[newID])
	}

	// Positive control: the full set is exactly the 30 originals plus the insert.
	assertRowCount(ctx, t, env, "post-insert full requery", Query{Schema: wide, Limit: 40}, n+1)
}

// testKeysetIncompleteCursorRejected pins the enforced tiebreak contract (#183):
// a cursor whose final column is NOT row_id would silently skip every row tied
// at the page boundary, so the engine now REJECTS it rather than serving a lossy
// page. Pre-fix this was pinned as a lossy empty continuation page; enforcement
// supersedes that follow-up. The rejection is structural (independent of the
// data), so the positive control drives the identical query with a trailing
// row_id appended and requires it to succeed — proving only the missing tiebreak
// is refused, not the underlying read.
func testKeysetIncompleteCursorRejected(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	const n = 5
	events := make([]*Event, 0, n)
	for i := 0; i < n; i++ {
		events = append(events, CreateEvent(wide, map[string]any{
			"title": fmt.Sprintf("rej-%02d", i),
			"count": float64(10 + i),
		}))
	}
	mustApplyEvents(ctx, t, env, "incomplete-cursor creates", events...)
	mustFlush(ctx, t, env)
	env.ExecSQL(ctx, "DELETE FROM change_log") // cold-only

	// Positive control: the read path returns all five rows.
	assertRowCount(ctx, t, env, "reject control", Query{Schema: wide, Limit: 10}, n)

	// A created_at-only cursor lacks the trailing row_id tiebreak ⇒ rejected.
	_, err := env.Query(ctx, Query{
		Schema: wide,
		Keyset: &model.KeysetCursor{
			Columns: []model.KeysetColumn{{Attribute: "created_at", Direction: forma.SortOrderDesc}},
			Values:  []any{paginationFarFutureMs},
			Mode:    model.KeysetCursorModeAfter,
		},
		Limit: 2,
	})
	if err == nil {
		t.Fatal("expected the created_at-only keyset cursor to be rejected, got nil error")
	}
	// Substring match (the validator returns a plain wrapped error, not a
	// sentinel): the message must name the offending column and the tiebreak.
	msg := err.Error()
	if !strings.Contains(msg, "created_at") || !strings.Contains(msg, "row_id") {
		t.Fatalf("rejection error must name the offending column and the row_id tiebreak, got: %v", err)
	}

	// Positive control: the same scan WITH a trailing row_id is accepted.
	page, err := env.Query(ctx, Query{
		Schema: wide,
		Keyset: &model.KeysetCursor{
			Columns: []model.KeysetColumn{
				{Attribute: "created_at", Direction: forma.SortOrderDesc},
				{Attribute: "row_id", Direction: forma.SortOrderAsc},
			},
			Values: []any{paginationFarFutureMs, paginationMaxRowID},
			Mode:   model.KeysetCursorModeAfter,
		},
		Limit: 2,
	})
	if err != nil {
		t.Fatalf("cursor with trailing row_id must be accepted, got: %v", err)
	}
	if len(page.Records) != 2 {
		t.Fatalf("accepted cursor page returned %d rows, want 2", len(page.Records))
	}
}
