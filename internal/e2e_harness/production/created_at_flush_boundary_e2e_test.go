//go:build e2e

package production

import (
	"context"
	"fmt"
	"testing"
)

// TestCreatedAtAcrossFlushBoundary (#460): the federated read path used to
// alias the LWW version stamp into the created_at slot for every
// warm/cold-winning row, while the hot leg reported the true creation time.
// The reported created_at was therefore wrong for parquet-winning rows, and
// the default sort key changed value the moment a row was flushed — so a
// LIMIT/OFFSET window straddling a flush could duplicate or skip rows.
//
// These scenarios drive the real write path, the real cdc-flush, and the real
// federated engine, so they exercise the exported parquet bytes rather than a
// synthetic fixture.
func TestCreatedAtAcrossFlushBoundary(t *testing.T) {
	cluster := SharedCluster(t)
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	scenarios := []struct {
		name string
		run  func(ctx context.Context, t *testing.T, env *Env, schema SchemaRef)
	}{
		{"created_updated_flushed_reports_creation_time", testFlushedRowReportsCreationTime},
		{"oltp_and_federated_routes_agree", testOLTPAndFederatedCreatedAtAgree},
		{"default_order_stable_across_flush", testDefaultOrderStableAcrossFlush},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			sc.run(context.Background(), t, NewEnv(t, cluster), wide)
		})
	}
}

// testFlushedRowReportsCreationTime is the first acceptance criterion: a
// created→updated→flushed row must report its creation time, not its update
// time, through the federated route.
func testFlushedRowReportsCreationTime(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	create := CreateEvent(wide, map[string]any{"title": "ca-flushed", "count": float64(1)})
	if err := env.ApplyEvents(ctx, create); err != nil {
		t.Fatalf("apply create: %v", err)
	}
	waitClockPast(t, create)
	update := UpdateEvent(wide, create.RowID, map[string]any{"count": float64(2)})
	if err := env.ApplyEvents(ctx, update); err != nil {
		t.Fatalf("apply update: %v", err)
	}
	// A same-millisecond update would make creation and version stamp equal,
	// which is exactly the case the defect is invisible in.
	assertStrictlyNewer(t, []*Event{create}, []*Event{update})
	mustFlush(ctx, t, env) // the row now lives only in delta parquet

	page, err := env.Query(ctx, Query{Schema: wide, Limit: 10})
	if err != nil {
		t.Fatalf("federated query: %v", err)
	}
	if len(page.Records) != 1 {
		t.Fatalf("query returned %d rows, want 1", len(page.Records))
	}
	rec := page.Records[0]
	if rec.CreatedAt != create.ChangedAt {
		t.Fatalf("created_at = %d, want the creation stamp %d (update stamp is %d): "+
			"the parquet leg must project ltbase_created_at, not the LWW version stamp (#460)",
			rec.CreatedAt, create.ChangedAt, update.ChangedAt)
	}
	if rec.UpdatedAt != update.ChangedAt {
		t.Fatalf("updated_at = %d, want the update stamp %d: #460 narrows changed_at to ver_ts, it does not move it",
			rec.UpdatedAt, update.ChangedAt)
	}
}

// testOLTPAndFederatedCreatedAtAgree pins the cross-route half of the
// acceptance criteria: the route is chosen by page size and tier hints, so
// the same row must report the same created_at whichever route serves it.
// The OLTP route reads entity_main.ltbase_created_at directly and was always
// right; the federated route disagreed for parquet-winning rows until #460.
func testOLTPAndFederatedCreatedAtAgree(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	create := CreateEvent(wide, map[string]any{"title": "ca-routes", "count": float64(1)})
	if err := env.ApplyEvents(ctx, create); err != nil {
		t.Fatalf("apply create: %v", err)
	}
	waitClockPast(t, create)
	update := UpdateEvent(wide, create.RowID, map[string]any{"count": float64(2)})
	if err := env.ApplyEvents(ctx, update); err != nil {
		t.Fatalf("apply update: %v", err)
	}
	assertStrictlyNewer(t, []*Event{create}, []*Event{update})
	mustFlush(ctx, t, env)

	hot, err := env.Query(ctx, Query{Schema: wide, PreferHot: true, Limit: 10})
	if err != nil {
		t.Fatalf("oltp query: %v", err)
	}
	fed, err := env.Query(ctx, Query{Schema: wide, Limit: 10})
	if err != nil {
		t.Fatalf("federated query: %v", err)
	}
	if hot.Plan.Routing.UseDuckDB {
		t.Fatalf("precondition: PreferHot query routed to duckdb: %+v", hot.Plan.Routing)
	}
	if !fed.Plan.Routing.UseDuckDB {
		t.Fatalf("precondition: default query did not route to duckdb: %+v", fed.Plan.Routing)
	}
	if len(hot.Records) != 1 || len(fed.Records) != 1 {
		t.Fatalf("route row counts = oltp %d, federated %d, want 1 each", len(hot.Records), len(fed.Records))
	}
	if hot.Records[0].CreatedAt != fed.Records[0].CreatedAt {
		t.Fatalf("created_at disagrees by route: oltp %d, federated %d (creation stamp %d, update stamp %d) — "+
			"the route is chosen by page size, so the two must never disagree (#460)",
			hot.Records[0].CreatedAt, fed.Records[0].CreatedAt, create.ChangedAt, update.ChangedAt)
	}
	if hot.Records[0].CreatedAt != create.ChangedAt {
		t.Fatalf("both routes report created_at = %d, want the creation stamp %d",
			hot.Records[0].CreatedAt, create.ChangedAt)
	}
}

// pageThrough walks the default order two rows at a time and returns the
// concatenated row ids.
func pageThrough(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, total, pageSize int) []string {
	t.Helper()
	var ids []string
	for offset := 0; offset < total; offset += pageSize {
		page, err := env.Query(ctx, Query{Schema: wide, Limit: pageSize, Offset: offset})
		if err != nil {
			t.Fatalf("federated query at offset %d: %v", offset, err)
		}
		ids = append(ids, pageRowIDs(page)...)
	}
	return ids
}

// testDefaultOrderStableAcrossFlush is the second acceptance criterion: a
// flush changes no row's creation time, so it must change no row's position
// in the federated default order (created_at DESC, row_id ASC).
//
// The probe stays on the federated route throughout and crosses the boundary
// in BOTH directions — cold → hot (the updates leave stale parquet copies
// that the dirty set discards) → cold again — because the defect lived in the
// disagreement between the two UNION ALL legs: whichever leg served a row
// decided which quantity landed in its sort key. The updates are applied in
// REVERSE creation order, so the version stamps rank the rows backwards and a
// leg reporting the version stamp reverses the page order outright.
//
// The OLTP route is deliberately not compared here: its default order is
// row_id ASC, not created_at DESC, so the two routes' DEFAULT orders differ
// for reasons #460 does not own. Cross-route agreement on the created_at
// VALUE is pinned by testOLTPAndFederatedCreatedAtAgree.
func testDefaultOrderStableAcrossFlush(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	const rows = 6

	creates := make([]*Event, 0, rows)
	for i := 0; i < rows; i++ {
		ev := CreateEvent(wide, map[string]any{
			"title": fmt.Sprintf("ca-order-%02d", i),
			"count": float64(i),
		})
		// One ApplyEvents call per row so each lands on its own millisecond:
		// a batch would let two rows share a creation stamp and turn the
		// order probe into a row_id tiebreak.
		if err := env.ApplyEvents(ctx, ev); err != nil {
			t.Fatalf("apply create %d: %v", i, err)
		}
		waitClockPast(t, ev)
		creates = append(creates, ev)
	}
	mustFlush(ctx, t, env) // every row cold; the federated route has a path set

	cold := pageThrough(ctx, t, env, wide, rows, 2)
	if len(cold) != rows {
		t.Fatalf("cold paging returned %d rows, want %d", len(cold), rows)
	}

	// Reverse-order updates: each row goes hot again over a now-stale parquet
	// copy, and the version stamps end up ranking the rows backwards.
	updates := make([]*Event, 0, rows)
	for i := rows - 1; i >= 0; i-- {
		ev := UpdateEvent(wide, creates[i].RowID, map[string]any{
			"count": float64(100 + i),
		})
		if err := env.ApplyEvents(ctx, ev); err != nil {
			t.Fatalf("apply update %d: %v", i, err)
		}
		waitClockPast(t, ev)
		updates = append(updates, ev)
	}

	hot := pageThrough(ctx, t, env, wide, rows, 2)
	assertSameOrder(t, "cold", cold, "hot-over-stale-parquet", hot, updates)

	mustFlush(ctx, t, env) // back to cold, now from the second delta

	reflushed := pageThrough(ctx, t, env, wide, rows, 2)
	assertSameOrder(t, "cold", cold, "reflushed", reflushed, updates)

	seen := make(map[string]int, rows)
	for _, id := range reflushed {
		seen[id]++
	}
	if len(seen) != rows {
		t.Fatalf("post-flush paging tiled %d distinct rows, want %d (%v): "+
			"an unstable sort key duplicates or skips rows across LIMIT/OFFSET windows (#460)",
			len(seen), rows, reflushed)
	}
}

// assertSameOrder fails when two paged row-id sequences differ, naming the
// tier transition that moved a row.
func assertSameOrder(t *testing.T, wantLabel string, want []string, gotLabel string, got []string, updates []*Event) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("%s paging returned %d rows, want %d (%s: %v)", gotLabel, len(got), len(want), wantLabel, want)
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("default order changed between %s and %s at position %d:\n  %s %v\n  %s  %v\n"+
				"neither an update nor a flush changes a row's creation time, so neither may change its sort key (#460); "+
				"newest update stamp is %d",
				wantLabel, gotLabel, i, wantLabel, want, gotLabel, got, updates[len(updates)-1].ChangedAt)
		}
	}
}
