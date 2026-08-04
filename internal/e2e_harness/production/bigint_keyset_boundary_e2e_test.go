//go:build e2e

package production

import (
	"context"
	"math"
	"testing"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// keysetBoundaryAmounts are the seeded `amount` (bigint_01) values in ascending
// order. They straddle 2^53 and the int64 ceiling: 2^53+1 and MaxInt64-1 are the
// two values a float64 cannot carry (they round to 2^53 and 2^63), so a
// continuation cursor pinned to either one is exactly where a float64-bound
// keyset predicate skips or duplicates a row. #205 made these legal column
// state, so the cursor has to be able to address them.
var keysetBoundaryAmounts = []int64{
	1<<53 - 1,         // 9007199254740991 — last float64-exact integer
	1 << 53,           // 9007199254740992
	1<<53 + 1,         // 9007199254740993 — NOT float64-representable
	1<<53 + 2,         // 9007199254740994
	math.MaxInt64 - 1, // 9223372036854775806 — NOT float64-representable
	math.MaxInt64,     // 9223372036854775807 — NOT float64-representable
}

// keysetWalkCase is one full continuation walk: a direction and a page size,
// plus the exact ascending/descending sequence the walk must observe and the
// exact `amount` values the continuation cursors must be pinned to. Pinning the
// cursor values (not just the visited sequence) is what makes the probe
// meaningful: the page arithmetic is chosen so that every walk positions at
// least one cursor on a value float64 cannot represent, and a change that
// silently re-pages away from those boundaries fails here instead of quietly
// degrading into a float-exact walk.
type keysetWalkCase struct {
	name        string
	desc        bool
	pageSize    int
	wantSeen    []int64
	wantCursors []int64
}

// TestBigintKeysetBoundaryNoDupNoSkip (#281, #205 M-2) walks keyset cursors over
// a bound bigint whose values straddle 2^53, building every continuation cursor
// from the previous page's last record as exact int64 — the raw-bind contract
// pinned by TestKeysetArgsPreserveInt64Above2p53 (cursor Values bind verbatim;
// BIGINT columns require int64). Each walk must see every row exactly once, in
// key order: with an inexact cursor value the `BIGINT col > DOUBLE param`
// comparison rounds the bound and either re-serves the boundary row (dup) or
// steps past its neighbour (skip).
//
// Keyset is a DuckDB-only feature (no Postgres keyset path exists), so every
// page asserts Plan.Routing.UseDuckDB. The oracle is deliberately not used: it
// normalizes numerics to float64 (oracle.go) and is structurally blind to a
// 2^53 miss — assertions here are exact int64 sequences.
//
// A float64-cursor variant is deliberately not asserted: model.KeysetCursor is
// in-process only (Query.Keyset's JSON tag is encode-only, no decode path
// exists), so the binder contract makes the caller responsible for the types.
func TestBigintKeysetBoundaryNoDupNoSkip(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	seeded := make([]*Event, 0, len(keysetBoundaryAmounts))
	for _, amount := range keysetBoundaryAmounts {
		seeded = append(seeded, CreateEvent(wide, map[string]any{"title": "ks-bigint", "amount": amount}))
	}
	mustApplyEvents(ctx, t, env, "keyset boundary creates", seeded...)

	// Export every row to base, then drop its change_log entry so the rows leave
	// the dirty set and are genuinely served from cold parquet (the RunInit-plus
	// DELETE idiom of TestNullAndBoundaryRoundTripAcrossTiers / #281 Task 3;
	// RunInit alone leaves the rows unflushed, i.e. still hot-served).
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx,
		"DELETE FROM change_log WHERE schema_id = $1 AND row_id = ANY($2)",
		wide.ID, rowIDs(seeded))

	// Positive control: the full set is readable before any cursor is involved.
	assertRowCount(ctx, t, env, "keyset boundary control",
		Query{Schema: wide, Limit: 100}, len(keysetBoundaryAmounts))

	asc := keysetBoundaryAmounts
	desc := reversedInt64s(asc)
	for _, tc := range []keysetWalkCase{
		// pages [0..2] [3..5] ⇒ the page-1 cursor sits on 2^53+1.
		{"asc_page3", false, 3, asc, []int64{asc[2], asc[5]}},
		// pages [0..4] [5] ⇒ the page-1 cursor sits on MaxInt64-1.
		{"asc_page5", false, 5, asc, []int64{asc[4], asc[5]}},
		// pages [0,1] [2,3] [4,5] ⇒ cursors on MaxInt64-1 and 2^53+1.
		{"desc_page2", true, 2, desc, []int64{desc[1], desc[3], desc[5]}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runKeysetBoundaryWalk(ctx, t, env, wide, tc)
		})
	}
}

// runKeysetBoundaryWalk drives one walk to exhaustion and checks the visited
// sequence and the continuation-cursor positions against the case.
func runKeysetBoundaryWalk(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, tc keysetWalkCase) {
	t.Helper()

	dir := forma.SortOrderAsc
	if tc.desc {
		dir = forma.SortOrderDesc
	}
	cols := []model.KeysetColumn{
		{Attribute: "amount", Direction: dir},
		// Required trailing tiebreak (#183). ASC on both directions matches the
		// non-keyset ORDER BY that serves page 1 (buildNonKeysetOrderBy appends
		// row_id ASC); amounts here are unique, so the arm never fires anyway.
		{Attribute: "row_id", Direction: forma.SortOrderAsc},
	}
	sorts := []Sort{{Attr: "amount", Desc: tc.desc}} // mirror keyset_lww_e2e_test.go

	var seen, cursorAt []int64
	var cursor *model.KeysetCursor
	maxPages := len(tc.wantSeen)/tc.pageSize + 3 // stop runaway loops on a dup
	for page := 0; page < maxPages; page++ {
		res := mustQuery(ctx, t, env, Query{
			Schema: wide, Sorts: sorts, Limit: tc.pageSize, Keyset: cursor,
		})
		if !res.Plan.Routing.UseDuckDB {
			t.Fatalf("page %d: keyset walk must route to DuckDB, got %+v", page, res.Plan.Routing)
		}
		if len(res.Records) == 0 {
			break
		}
		for _, r := range res.Records {
			seen = append(seen, r.Int64Items["bigint_01"])
		}
		// The continuation bound is the last record's exact int64 amount and its
		// row_id string — never round-tripped through float64 or JSON.
		last := res.Records[len(res.Records)-1]
		bound := last.Int64Items["bigint_01"]
		cursorAt = append(cursorAt, bound)
		cursor = &model.KeysetCursor{
			Columns: cols,
			Values:  []any{bound, last.RowID.String()},
			Mode:    model.KeysetCursorModeAfter,
		}
	}

	assertInt64Seq(t, "visited sequence (skip or dup at the 2^53 boundary)", seen, tc.wantSeen)
	assertInt64Seq(t, "continuation cursor positions", cursorAt, tc.wantCursors)
}

// assertInt64Seq requires two int64 sequences to be equal, reporting the first
// divergence together with both full sequences.
func assertInt64Seq(t *testing.T, label string, got, want []int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d values %v, want %d %v", label, len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: [%d] = %d, want %d (got %v, want %v)", label, i, got[i], want[i], got, want)
		}
	}
}

// reversedInt64s returns a reversed copy, leaving the input untouched.
func reversedInt64s(in []int64) []int64 {
	out := make([]int64, len(in))
	for i, v := range in {
		out[len(in)-1-i] = v
	}
	return out
}
