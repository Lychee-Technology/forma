//go:build e2e

package production

import (
	"context"
	"fmt"
	"sort"
	"testing"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// TestSortStability (#183): sort order must be a *total* order, stable across
// page boundaries and repeated runs, on every routing path. Task 1 appended
// `row_id ASC` to the federated non-keyset ORDER BY so equal sort keys break
// ties deterministically; without it, duplicate-key tie groups reorder by
// physical scan order and split across OFFSET windows (rows duplicated or
// dropped) and reshuffle run to run. These scenarios pin the per-page oracle
// order, the page-union invariant (every created row appears on exactly one
// page), repeat-N determinism, and the internal multi-key AttributeOrders
// contract. Expected GREEN: Tasks 1-2 already landed.
func TestSortStability(t *testing.T) {
	cluster := SharedCluster(t)
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	scenarios := []struct {
		name string
		run  func(ctx context.Context, t *testing.T, env *Env, schema SchemaRef)
	}{
		{"duplicate_key_page_union", testDuplicateKeyPageUnion},
		{"repeat_n_stable_order", testRepeatNStableOrder},
		{"hot_oltp_page_union", testHotOLTPPageUnion},
		{"multi_key_mixed_directions", testMultiKeyMixedDirections},
		{"multi_key_public_api_desc", testMultiKeyPublicAPIDesc},
		{"multi_key_mixed_public_federated", testMultiKeyMixedPublicFederated},
		{"multi_key_mixed_public_hot", testMultiKeyMixedPublicHot},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			sc.run(context.Background(), t, NewEnv(t, cluster), wide)
		})
	}
}

// buildDuplicateQtyRows builds 21 creates whose pure-EAV "qty" repeats across
// {10,20,30} (7 rows each), forming three 7-way tie groups; "count" stays
// unique for row identity.
func buildDuplicateQtyRows(wide SchemaRef) []*Event {
	events := make([]*Event, 0, 21)
	for i := 0; i < 21; i++ {
		events = append(events, CreateEvent(wide, map[string]any{
			"title": fmt.Sprintf("dup-%02d", i),
			"count": float64(1000 + i),       // unique identity aid
			"qty":   float64(10 * (1 + i%3)), // 10/20/30 — 7-way ties per value
		}))
	}
	return events
}

// reverseOrderUpdates re-sends every create as an update in reverse creation
// order, keeping qty (the tie key) intact but refreshing the title. This makes
// the delta parquet's physical row order disagree with row_id order — an
// adversarial fixture that exposes scan-order-dependent tie breaking.
func reverseOrderUpdates(wide SchemaRef, creates []*Event) []*Event {
	updates := make([]*Event, 0, len(creates))
	for i := len(creates) - 1; i >= 0; i-- {
		ev := creates[i]
		updates = append(updates, UpdateEvent(wide, ev.RowID, map[string]any{
			"title": fmt.Sprintf("dup-v2-%02d", i),
			"qty":   ev.Attrs["qty"], // keep the tie groups intact
		}))
	}
	return updates
}

// buildMixedRankRows builds 12 creates with main-bound smallint "rank" drawn
// from {1,2} (6 each, alternating) and unique main-bound integer "count", for
// the two-key mixed-direction sort.
func buildMixedRankRows(wide SchemaRef) []*Event {
	events := make([]*Event, 0, 12)
	for i := 0; i < 12; i++ {
		events = append(events, CreateEvent(wide, map[string]any{
			"title": fmt.Sprintf("mk-%02d", i),
			"rank":  float64(1 + i%2), // 1,2,1,2,… → six 1s, six 2s
			"count": float64(500 + i), // unique per row
		}))
	}
	return events
}

// seedDuplicateQty applies the 21 duplicate-qty creates then re-applies them as
// reverse-creation-order updates, guarding the update generation with
// assertStrictlyNewer so no create/update pair collapses into a ver_ts tie
// (#210). Rows are left hot; the caller flushes if it wants the DuckDB path.
// Returns the create events (row identity + qty tie groups).
func seedDuplicateQty(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) []*Event {
	t.Helper()
	creates := buildDuplicateQtyRows(wide)
	mustApplyEvents(ctx, t, env, "duplicate-qty creates", creates...)
	updates := reverseOrderUpdates(wide, creates)
	mustApplyEvents(ctx, t, env, "reverse-order updates", updates...)
	// updates[k] targets creates[len-1-k]; align the slices for the guard.
	revCreates := make([]*Event, len(creates))
	for i := range creates {
		revCreates[i] = creates[len(creates)-1-i]
	}
	assertStrictlyNewer(t, revCreates, updates)
	return creates
}

// collectPages pages through base in OFFSET windows of pageSize across the
// first upTo rows, returning each page's row IDs and asserting no page exceeds
// pageSize. Plain (non-oracle) reads so cursor-based callers can reuse it;
// reused by a later pagination-stability task.
func collectPages(ctx context.Context, t *testing.T, env *Env, base Query, pageSize, upTo int) [][]string {
	t.Helper()
	pages := make([][]string, 0, (upTo+pageSize-1)/pageSize)
	for offset := 0; offset < upTo; offset += pageSize {
		q := base
		q.Limit = pageSize
		q.Offset = offset
		res, err := env.Query(ctx, q)
		if err != nil {
			t.Fatalf("collectPages offset=%d: %v", offset, err)
		}
		ids := pageRowIDs(res)
		if len(ids) > pageSize {
			t.Fatalf("collectPages offset=%d returned %d rows, exceeds page size %d",
				offset, len(ids), pageSize)
		}
		pages = append(pages, ids)
	}
	return pages
}

// assertPageUnion pins the page-union invariant: concatenated across all
// windows the pages must cover exactly the created row set — no row on two
// pages (a split tie group duplicating) and none dropped at a boundary.
func assertPageUnion(t *testing.T, pages [][]string, creates []*Event) {
	t.Helper()
	want := make(map[string]bool, len(creates))
	for _, c := range creates {
		want[c.RowID.String()] = true
	}
	seen := make(map[string]bool, len(creates))
	for _, page := range pages {
		for _, id := range page {
			if seen[id] {
				t.Fatalf("row %s appeared on more than one page: a split tie group duplicated a row", id)
			}
			seen[id] = true
			if !want[id] {
				t.Fatalf("row %s is not in the created set", id)
			}
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("page union covered %d distinct rows, want %d: a tie-group boundary dropped rows",
			len(seen), len(want))
	}
}

// assertRankThenCountDesc verifies, by direct record inspection, that rank
// ascends monotonically (so all rank-1 rows precede all rank-2 rows) and that
// count descends strictly within each rank group — belt-and-braces beyond the
// oracle's order diff. Typed-map keys verified against mainColumnValue
// (assert.go): smallint→Int16Items, integer→Int32Items.
func assertRankThenCountDesc(t *testing.T, records []*model.PersistentRecord) {
	t.Helper()
	var prevRank int16
	var prevCount int32
	started := false
	for i, rec := range records {
		rank, ok := rec.Int16Items["smallint_01"]
		if !ok {
			t.Fatalf("record %d missing rank (smallint_01)", i)
		}
		count, ok := rec.Int32Items["integer_01"]
		if !ok {
			t.Fatalf("record %d missing count (integer_01)", i)
		}
		if started {
			if rank < prevRank {
				t.Fatalf("record %d rank %d < previous %d: rank must ascend", i, rank, prevRank)
			}
			if rank == prevRank && count >= prevCount {
				t.Fatalf("record %d count %d not strictly below previous %d within rank %d",
					i, count, prevCount, rank)
			}
		}
		prevRank, prevCount, started = rank, count, true
	}
}

// testDuplicateKeyPageUnion pins per-page oracle order AND the page-union
// invariant on the DuckDB path: with 7-way qty ties and delta rows physically
// out of row_id order, only Task 1's row_id tiebreak keeps the six-row windows
// non-overlapping and complete.
func testDuplicateKeyPageUnion(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := seedDuplicateQty(ctx, t, env, wide)
	mustFlush(ctx, t, env)
	env.ExecSQL(ctx, "DELETE FROM change_log") // cold-only ⇒ DuckDB routing

	base := Query{Schema: wide, Sorts: []Sort{{Attr: "qty"}}}
	for _, off := range []int{0, 6, 12, 18} {
		q := base
		q.Limit = 6
		q.Offset = off
		res := env.AssertQueryMatches(ctx, q)
		if off == 0 && res != nil && !res.Plan.Routing.UseDuckDB {
			t.Fatalf("first page did not route to duckdb: %+v", res.Plan.Routing)
		}
	}

	pages := collectPages(ctx, t, env, base, 6, 21)
	assertPageUnion(t, pages, creates)
}

// testRepeatNStableOrder pins repeat-N determinism (the issue's "same order
// each time"): the full sorted page must come back byte-identical across ten
// runs on the DuckDB path.
func testRepeatNStableOrder(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	seedDuplicateQty(ctx, t, env, wide)
	mustFlush(ctx, t, env)
	env.ExecSQL(ctx, "DELETE FROM change_log") // cold-only ⇒ DuckDB routing

	q := Query{Schema: wide, Sorts: []Sort{{Attr: "qty"}}, Limit: 25}
	res := env.AssertQueryMatches(ctx, q)
	if res == nil {
		return
	}
	if !res.Plan.Routing.UseDuckDB {
		t.Fatalf("expected DuckDB routing, got OLTP: %+v", res.Plan.Routing)
	}
	first := pageRowIDs(res)
	if len(first) != 21 {
		t.Fatalf("sorted page returned %d rows, want 21", len(first))
	}
	for i := 0; i < 9; i++ {
		again, err := env.Query(ctx, q)
		if err != nil {
			t.Fatalf("repeat %d: %v", i, err)
		}
		if got := pageRowIDs(again); !equalStrings(got, first) {
			t.Fatalf("repeat %d order flapped:\n first=%v\n again=%v", i, first, got)
		}
	}
}

// testHotOLTPPageUnion is the OLTP twin of duplicate_key_page_union: unflushed
// rows under PreferHot route through the Postgres path (which already carried
// the row_id tiebreak), pinning cross-path parity for the page-union invariant.
func testHotOLTPPageUnion(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := seedDuplicateQty(ctx, t, env, wide)
	// No flush: rows stay hot; PreferHot forces the OLTP template.

	base := Query{Schema: wide, Sorts: []Sort{{Attr: "qty"}}, PreferHot: true}
	for _, off := range []int{0, 6, 12, 18} {
		q := base
		q.Limit = 6
		q.Offset = off
		res := env.AssertQueryMatches(ctx, q)
		if off == 0 && res != nil && res.Plan.Routing.UseDuckDB {
			t.Fatalf("hot page unexpectedly routed to duckdb: %+v", res.Plan.Routing)
		}
	}

	pages := collectPages(ctx, t, env, base, 6, 21)
	assertPageUnion(t, pages, creates)
}

// testMultiKeyMixedDirections pins the two-key sort (rank ASC, count DESC) on
// the DuckDB path against the oracle and by direct inspection.
//
// This scenario pins the per-key-direction contract at the internal
// AttributeOrders level the federated engine implements. Since #240 the same
// shape is publicly reachable through QueryRequest.Sort — see
// testMultiKeyMixedPublicFederated / testMultiKeyMixedPublicHot for the
// public-surface twins; this internal pin stays as the engine-level anchor.
func testMultiKeyMixedDirections(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := buildMixedRankRows(wide)
	mustApplyEvents(ctx, t, env, "mixed-rank creates", creates...)
	mustFlush(ctx, t, env)
	env.ExecSQL(ctx, "DELETE FROM change_log") // cold-only ⇒ DuckDB routing

	q := Query{Schema: wide, Sorts: []Sort{{Attr: "rank"}, {Attr: "count", Desc: true}}, Limit: 20}
	res := env.AssertQueryMatches(ctx, q)
	if res == nil {
		return
	}
	if !res.Plan.Routing.UseDuckDB {
		t.Fatalf("expected DuckDB routing, got OLTP: %+v", res.Plan.Routing)
	}
	if len(res.Records) != 12 {
		t.Fatalf("multi-key sort returned %d rows, want 12", len(res.Records))
	}
	assertRankThenCountDesc(t, res.Records)
}

// testMultiKeyPublicAPIDesc covers the publicly expressible half of #183's
// (status, created_at DESC) ask: a two-key sort through the real
// forma.EntityManager.Query surface. The legacy SortBy surface threads a single
// shared SortOrder across all keys, so this scenario pins the uniform-DESC slice
// of that legacy contract; mixed per-key directions travel through the
// structured Sort field (#240). Here (rank DESC, count DESC) stands in for the
// categorical-primary +
// distinct-secondary shape: rank repeats (six 1s, six 2s) so the count key is
// load-bearing, and count is unique so the pair is a total order. The rows are
// cold-only and the request opts into the federated path, so a correct ordered
// result can only come back through DuckDB.
//
// Note: e2e_wide has no "status"/"created_at" public attributes, so the
// scenario uses rank+count — both main-bound schema attributes — as the
// faithful categorical+distinct analog of #183's key pair.
func testMultiKeyPublicAPIDesc(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := buildMixedRankRows(wide)
	mustApplyEvents(ctx, t, env, "public-api multi-key creates", creates...)
	mustFlush(ctx, t, env)
	env.ExecSQL(ctx, "DELETE FROM change_log") // cold/warm-only ⇒ federated DuckDB path

	// Oracle: uniform DESC over (rank, count), row_id irrelevant (count unique).
	want := make([]*Event, len(creates))
	copy(want, creates)
	sort.SliceStable(want, func(i, j int) bool {
		ri, rj := want[i].Attrs["rank"].(float64), want[j].Attrs["rank"].(float64)
		if ri != rj {
			return ri > rj // rank DESC
		}
		return want[i].Attrs["count"].(float64) > want[j].Attrs["count"].(float64) // count DESC
	})

	res, err := env.EntityManager().Query(ctx, &forma.QueryRequest{
		SchemaName:   wide.Name,
		Page:         1,
		ItemsPerPage: 20,
		SortBy:       []string{"rank", "count"},
		SortOrder:    forma.SortOrderDesc,
		Federated: &forma.FederatedQueryRequest{
			Enabled:               true,
			PreferredTiers:        []string{"hot", "warm", "cold"},
			S3ParquetPathTemplate: env.ParquetGlob(),
			IncludeExecutionPlan:  true,
		},
	})
	if err != nil {
		t.Fatalf("public-api federated query: %v", err)
	}
	if len(res.Data) != len(want) {
		t.Fatalf("public-api multi-key sort returned %d rows, want %d", len(res.Data), len(want))
	}
	for i := range want {
		if res.Data[i].RowID != want[i].RowID {
			t.Fatalf("public-api row %d = %s, want %s: (rank DESC, count DESC) order drift through EntityManager.Query",
				i, res.Data[i].RowID, want[i].RowID)
		}
	}
}

// mixedRankOracle sorts the created events by (rank ASC, count DESC) — the
// exact #183 shape (categorical primary ascending, distinct secondary
// descending) that #240 makes publicly expressible. count is unique so the
// pair is a total order and row_id never has to break ties.
func mixedRankOracle(creates []*Event) []*Event {
	want := make([]*Event, len(creates))
	copy(want, creates)
	sort.SliceStable(want, func(i, j int) bool {
		ri, rj := want[i].Attrs["rank"].(float64), want[j].Attrs["rank"].(float64)
		if ri != rj {
			return ri < rj // rank ASC
		}
		return want[i].Attrs["count"].(float64) > want[j].Attrs["count"].(float64) // count DESC
	})
	return want
}

// assertPublicOrderMatches compares a public QueryResult's row order against
// the oracle event order, failing on the first divergence.
func assertPublicOrderMatches(t *testing.T, label string, res *forma.QueryResult, want []*Event) {
	t.Helper()
	if len(res.Data) != len(want) {
		t.Fatalf("%s returned %d rows, want %d", label, len(res.Data), len(want))
	}
	for i := range want {
		if res.Data[i].RowID != want[i].RowID {
			t.Fatalf("%s row %d = %s, want %s: (rank ASC, count DESC) order drift",
				label, i, res.Data[i].RowID, want[i].RowID)
		}
	}
}

// testMultiKeyMixedPublicFederated promotes the mixed-direction pin (#240) to
// the public surface on the DuckDB path: (rank ASC, count DESC) through the
// real forma.EntityManager.Query via the structured Sort field. Rows are
// cold-only, so a correctly ordered result can only come back through the
// federated per-key ORDER BY rendering.
func testMultiKeyMixedPublicFederated(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := buildMixedRankRows(wide)
	mustApplyEvents(ctx, t, env, "mixed-public federated creates", creates...)
	mustFlush(ctx, t, env)
	env.ExecSQL(ctx, "DELETE FROM change_log") // cold-only ⇒ DuckDB routing

	res, err := env.EntityManager().Query(ctx, &forma.QueryRequest{
		SchemaName:   wide.Name,
		Page:         1,
		ItemsPerPage: 20,
		Sort: []forma.OrderBy{
			{Attribute: "rank"}, // direction omitted → asc default
			{Attribute: "count", SortOrder: forma.SortOrderDesc},
		},
		Federated: &forma.FederatedQueryRequest{
			Enabled:               true,
			PreferredTiers:        []string{"hot", "warm", "cold"},
			S3ParquetPathTemplate: env.ParquetGlob(),
			IncludeExecutionPlan:  true,
		},
	})
	if err != nil {
		t.Fatalf("public mixed-direction federated query: %v", err)
	}
	if res.ExecutionPlan == nil || !res.ExecutionPlan.Routing.UsedDuckDB {
		t.Fatalf("expected DuckDB routing, got plan %+v", res.ExecutionPlan)
	}
	assertPublicOrderMatches(t, "public federated mixed sort", res, mixedRankOracle(creates))
}

// testMultiKeyMixedPublicHot is the OLTP twin: unflushed rows without a
// federated hint route through the Postgres optimized template, pinning that
// the PG dialect also renders per-key directions arriving from the public
// Sort field.
func testMultiKeyMixedPublicHot(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := buildMixedRankRows(wide)
	mustApplyEvents(ctx, t, env, "mixed-public hot creates", creates...)
	// No flush: rows stay hot ⇒ OLTP path.

	res, err := env.EntityManager().Query(ctx, &forma.QueryRequest{
		SchemaName:   wide.Name,
		Page:         1,
		ItemsPerPage: 20,
		Sort: []forma.OrderBy{
			{Attribute: "rank"},
			{Attribute: "count", SortOrder: forma.SortOrderDesc},
		},
	})
	if err != nil {
		t.Fatalf("public mixed-direction hot query: %v", err)
	}
	assertPublicOrderMatches(t, "public hot mixed sort", res, mixedRankOracle(creates))
}
