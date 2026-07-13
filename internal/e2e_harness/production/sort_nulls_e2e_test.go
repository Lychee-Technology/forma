//go:build e2e

package production

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// TestSortNulls (#183): NULL sort values must land in the same position (last,
// both directions) on every routing path. DuckDB defaults to NULLS LAST for
// ASC and DESC; PostgreSQL treats NULL as largest (ASC→last, DESC→FIRST), so
// pre-fix the PG-only/OLTP path diverges from the federated path and from the
// oracle on DESC sorts. Fixed by explicit NULLS LAST in the optimized-query
// template's ORDER BYs (both the ordered CTE — its LIMIT window depends on
// placement — and the final select).
func TestSortNulls(t *testing.T) {
	cluster := SharedCluster(t)
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	scenarios := []struct {
		name string
		run  func(ctx context.Context, t *testing.T, env *Env, schema SchemaRef)
	}{
		{"federated_nulls_last_asc_desc", testFederatedNullsLast},
		{"hot_oltp_nulls_last_desc", testHotOLTPNullsLastDesc},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			t.Parallel()
			sc.run(context.Background(), t, NewEnv(t, cluster), wide)
		})
	}
}

// buildSparseNoteRows returns creates where the first withNote rows carry the
// pure-EAV "note" attribute and the remainder omit it entirely (no eav_data
// row ⇒ NULL in the DuckDB pivot / PG sort subquery). count stays unique so
// only the note column exercises tie/NULL handling.
func buildSparseNoteRows(wide SchemaRef, withNote, withoutNote int) []*Event {
	events := make([]*Event, 0, withNote+withoutNote)
	for i := 0; i < withNote; i++ {
		events = append(events, CreateEvent(wide, map[string]any{
			"title": fmt.Sprintf("noted-%02d", i),
			"count": float64(100 + i),
			"note":  fmt.Sprintf("n-%02d", i),
		}))
	}
	for i := 0; i < withoutNote; i++ {
		events = append(events, CreateEvent(wide, map[string]any{
			"title": fmt.Sprintf("bare-%02d", i),
			"count": float64(200 + i),
		}))
	}
	return events
}

// testHotOLTPNullsLastDesc pins NULLS LAST on the PG-only path (PreferHot).
// Pre-fix this is a deterministic red: PG's default DESC places NULLs FIRST,
// the oracle (and the DuckDB path) place them LAST.
func testHotOLTPNullsLastDesc(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	events := buildSparseNoteRows(wide, 4, 4)
	mustApplyEvents(ctx, t, env, "sparse creates", events...)
	// No flush: rows stay hot; PreferHot forces the OLTP template.
	res := env.AssertQueryMatches(ctx, Query{
		Schema: wide, Sorts: []Sort{{Attr: "note", Desc: true}}, Limit: 20, PreferHot: true})
	if res == nil {
		t.Fatal("hot DESC sort on nullable attribute diverged from the oracle (NULLS FIRST vs NULLS LAST)")
	}
	if res.Plan.Routing.UseDuckDB {
		t.Fatal("expected PG-only routing under PreferHot")
	}
	assertNoteNullsLast(t, res, 4)
}

// testFederatedNullsLast pins NULLS LAST on the federated/DuckDB path for both
// directions, and that repeated runs return the same order (stability, not
// just placement).
func testFederatedNullsLast(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	events := buildSparseNoteRows(wide, 4, 4)
	mustApplyEvents(ctx, t, env, "sparse creates", events...)
	mustFlush(ctx, t, env)
	env.ExecSQL(ctx, "DELETE FROM change_log") // cold-only ⇒ DuckDB routing
	for _, desc := range []bool{false, true} {
		q := Query{Schema: wide, Sorts: []Sort{{Attr: "note", Desc: desc}}, Limit: 20}
		res := env.AssertQueryMatches(ctx, q)
		if res == nil {
			t.Fatalf("federated note sort desc=%v diverged from oracle", desc)
		}
		if !res.Plan.Routing.UseDuckDB {
			t.Fatalf("expected DuckDB routing, got OLTP (desc=%v)", desc)
		}
		assertNoteNullsLast(t, res, 4)
		first := pageRowIDs(res)
		for i := 0; i < 4; i++ { // repeat-N: same order every run
			again, err := env.Query(ctx, q)
			if err != nil {
				t.Fatalf("repeat %d: %v", i, err)
			}
			if got := pageRowIDs(again); !equalStrings(got, first) {
				t.Fatalf("repeat %d order flapped:\n first=%v\n again=%v", i, first, got)
			}
		}
	}
}

// assertNoteNullsLast pins that the nNoted rows carrying "note" lead the page
// and the nNoted NULL-note rows trail it, on either sort direction. Titles
// carry the shape (noted-* vs bare-*), so a title-prefix check is a faithful
// proxy for note-NULL placement without re-deriving EAV values.
func assertNoteNullsLast(t *testing.T, res *QueryResult, nNoted int) {
	t.Helper()
	if got := len(res.Records); got != nNoted*2 {
		t.Fatalf("want %d records, got %d", nNoted*2, got)
	}
	for i, rec := range res.Records {
		title := rec.TextItems["text_01"]
		wantPrefix := "noted-"
		if i >= nNoted {
			wantPrefix = "bare-"
		}
		if !strings.HasPrefix(title, wantPrefix) {
			t.Fatalf("record %d title %q: want prefix %q (note NULLs must sort last)",
				i, title, wantPrefix)
		}
	}
}

// equalStrings reports whether two string slices are element-wise equal.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
