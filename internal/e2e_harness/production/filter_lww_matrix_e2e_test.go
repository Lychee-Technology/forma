//go:build e2e

package production

import (
	"context"
	"fmt"
	"testing"
)

// buildMatrixV1Creates builds the five v1 rows whose every filterable
// attribute the v2 updates flip.
func buildMatrixV1Creates(wide SchemaRef) []*Event {
	creates := make([]*Event, 0, 5)
	for i := 0; i < 5; i++ {
		creates = append(creates, CreateEvent(wide, map[string]any{
			"title":  fmt.Sprintf("alpha-%02d", i),    // text, MAIN text_01
			"note":   fmt.Sprintf("old-note-%02d", i), // text, EAV
			"count":  float64(1000 + i),               // integer, MAIN integer_01
			"qty":    float64(10 + i),                 // integer, EAV
			"score":  1.5,                             // numeric, MAIN double_01
			"ratio":  2.25,                            // numeric, EAV
			"active": true,                            // bool, EAV
			"born":   "2000-01-01",                    // date, EAV
			"joined": "2010-06-15",                    // date, MAIN unix_ms
		}))
	}
	return creates
}

// buildMatrixV2Updates flips every filterable attribute of the v1 rows.
func buildMatrixV2Updates(wide SchemaRef, creates []*Event) []*Event {
	v2 := make([]*Event, 0, len(creates))
	for i, c := range creates {
		v2 = append(v2, UpdateEvent(wide, c.RowID, map[string]any{
			"title":  fmt.Sprintf("beta-%02d", i),
			"note":   fmt.Sprintf("new-note-%02d", i),
			"count":  float64(5000 + i),
			"qty":    float64(500 + i),
			"score":  9.5,
			"ratio":  8.75,
			"active": false,
			"born":   "2020-02-02",
			"joined": "2030-01-01",
		}))
	}
	return v2
}

// assertMatrixV1PositiveControls guards the zero-row matrix probes against
// false greens: at the v1-only stage, every probed storage class must return
// rows through the same predicate paths the matrix later expects to be empty.
func assertMatrixV1PositiveControls(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	t.Helper()
	controls := []struct {
		name   string
		filter Filter
		want   int
	}{
		{"MAIN text", Filter{Attr: "title", Value: "alpha-02"}, 1},
		{"EAV text", Filter{Attr: "note", Op: "contains", Value: "old-"}, 5},
		// Date controls (#200): all five v1 rows carry born=2000-01-01 and
		// joined=2010-06-15.
		{"EAV date", Filter{Attr: "born", Op: "lt", Value: "2010-01-01T00:00:00Z"}, 5},
		{"MAIN date", Filter{Attr: "joined", Op: "lt", Value: "2020-01-01T00:00:00Z"}, 5},
	}
	for _, c := range controls {
		assertRowCount(ctx, t, env, c.name+" positive control",
			Query{Schema: wide, Filters: []Filter{c.filter}, Limit: 10}, c.want)
	}
}

// testStaleFilterOperatorMatrix is issue #178 scenario 4: text, numeric,
// and bool operators over both MAIN-bound and EAV attributes, where every
// predicate matches only the superseded v1 generation. Also pins the
// multi-attribute AND semantics: a conjunction whose legs match different
// generations must yield zero rows — the winner is resolved first, then the
// whole conjunction applies to it.
//
// Date-operator probes over both storage classes (born in the EAV table,
// joined bound to a MAIN unix_ms column) are covered since #200 fixed
// federated date-predicate binding.
func testStaleFilterOperatorMatrix(ctx context.Context, t *testing.T, env *Env, wide SchemaRef) {
	creates := buildMatrixV1Creates(wide)
	mustApplyEvents(ctx, t, env, "apply creates", creates...)
	mustFlush(ctx, t, env) // delta #1 = v1

	// Load-bearing positives across storage classes.
	assertMatrixV1PositiveControls(ctx, t, env, wide)

	v2 := buildMatrixV2Updates(wide, creates)
	mustApplyEvents(ctx, t, env, "apply v2 updates", v2...)
	assertStrictlyNewer(t, creates, v2)
	mustFlush(ctx, t, env) // delta #2 = v2; rows clean

	// Positive: winner reachable.
	env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "title", Value: "beta-02"}},
		Limit:   10,
	})

	// Matrix: every probe matches only v1 → zero rows each.
	probes := []struct {
		name   string
		filter Filter
	}{
		{"main_text_equals", Filter{Attr: "title", Value: "alpha-02"}},
		{"main_text_starts_with", Filter{Attr: "title", Op: "starts_with", Value: "alpha"}},
		{"eav_text_contains", Filter{Attr: "note", Op: "contains", Value: "old-"}},
		{"main_int_lt", Filter{Attr: "count", Op: "lt", Value: "2000"}},
		{"eav_int_lt", Filter{Attr: "qty", Op: "lt", Value: "100"}},
		{"main_numeric_lt", Filter{Attr: "score", Op: "lt", Value: "2"}},
		{"eav_numeric_lt", Filter{Attr: "ratio", Op: "lt", Value: "3"}},
		{"eav_bool_equals", Filter{Attr: "active", Value: "1"}},
		{"eav_date_lt", Filter{Attr: "born", Op: "lt", Value: "2010-01-01T00:00:00Z"}},
		{"main_date_lt", Filter{Attr: "joined", Op: "lt", Value: "2020-01-01T00:00:00Z"}},
	}
	for _, p := range probes {
		assertZeroRows(ctx, t, env, p.name, Query{
			Schema: wide, Filters: []Filter{p.filter}, Limit: 10})
	}

	// Straddling AND: title matches v2, active matches v1 — no single
	// version satisfies both, so the LWW winner fails the conjunction.
	assertZeroRows(ctx, t, env, "straddling_and", Query{
		Schema: wide,
		Filters: []Filter{
			{Attr: "title", Value: "beta-02"},
			{Attr: "active", Value: "1"},
		},
		Limit: 10,
	})
	// Same conjunction anchored fully on the winner: exactly one row.
	assertRowCount(ctx, t, env, "winner AND probe", Query{
		Schema: wide,
		Filters: []Filter{
			{Attr: "title", Value: "beta-02"},
			{Attr: "active", Value: "0"},
		},
		Limit: 10,
	}, 1)
}
