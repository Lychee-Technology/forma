//go:build e2e

package production

import (
	"context"
	"math"
	"testing"

	"github.com/google/uuid"
)

// boundaryProbe is one filter probe plus the exact set of seeded rows it must
// return. The expectation is a row-identity set, never an oracle comparison:
// the oracle normalizes numerics to float64 (oracle.go) and is structurally
// blind to a 2^53 miss.
type boundaryProbe struct {
	name   string
	filter Filter
	want   []*Event
}

// TestBigintFilterBoundaryBothDialects (#281) probes comparison-side exactness
// for a bound bigint above 2^53 on both binder paths: PreferHot (Postgres,
// ConvertPgMainValue int64) and federated (DuckDB, parseDuckDBRawParam → exact
// decimal string). Pre-#281 the bind rendered via %.15g of the float64
// ("9.00719925474099e+15" / "9.22337203685478e+18"), so the predicate could not
// address these values at all — equality probes returned nothing (the rendered
// literal is a value no row holds, and the MaxInt64-side literal overflows
// CAST(? AS BIGINT) outright) and hot/cold tiers disagreed on the same filter.
// #205 made those values legal column state, so the filter has to be able to
// address them.
func TestBigintFilterBoundaryBothDialects(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	atPow53 := CreateEvent(wide, map[string]any{"title": "b-2p53", "amount": int64(1) << 53})        // 9007199254740992
	abovePow53 := CreateEvent(wide, map[string]any{"title": "b-2p53p1", "amount": int64(1)<<53 + 1}) // 9007199254740993
	nearMax := CreateEvent(wide, map[string]any{"title": "b-maxm1", "amount": int64(math.MaxInt64) - 1})
	atMax := CreateEvent(wide, map[string]any{"title": "b-max", "amount": int64(math.MaxInt64)})
	seeded := []*Event{atPow53, abovePow53, nearMax, atMax}
	if err := env.ApplyEvents(ctx, seeded...); err != nil {
		t.Fatalf("apply boundary rows: %v", err)
	}

	probes := []boundaryProbe{
		{"equals_2p53_plus_1", Filter{Attr: "amount", Op: "equals", Value: "9007199254740993"}, []*Event{abovePow53}},
		{"equals_2p53_excludes_neighbor", Filter{Attr: "amount", Op: "equals", Value: "9007199254740992"}, []*Event{atPow53}},
		{"gt_2p53_boundary", Filter{Attr: "amount", Op: "gt", Value: "9007199254740992"}, []*Event{abovePow53, nearMax, atMax}},
		{"equals_maxint64_minus_1", Filter{Attr: "amount", Op: "equals", Value: "9223372036854775806"}, []*Event{nearMax}},
		{"lte_maxint64_minus_1_excludes_max", Filter{Attr: "amount", Op: "lte", Value: "9223372036854775806"}, []*Event{atPow53, abovePow53, nearMax}},
		// #357: the SAME value in the other two accepted spellings must address
		// the same row on both binders. Pre-#357 only the bare-digits spelling
		// bound exactly — these two rode ParseFloat's rounded float64, so
		// "…993.0" silently addressed 2^53 and matched the WRONG row.
		{"equals_2p53_plus_1_decimal_spelling", Filter{Attr: "amount", Op: "equals", Value: "9007199254740993.0"}, []*Event{abovePow53}},
		{"gte_2p53_plus_1_exponent_spelling", Filter{Attr: "amount", Op: "gte", Value: "9.007199254740993e15"}, []*Event{abovePow53, nearMax, atMax}},
	}

	// PG dialect: rows are unflushed, PreferHot short-circuits to postgres-only.
	runBoundaryProbes(ctx, t, env, "hot-pg", Query{Schema: wide, PreferHot: true, Limit: 100}, false, probes)

	// DuckDB dialect: export every row to base, then drop the change_log
	// entries so the rows leave the dirty set and are served from cold parquet
	// (the RunInit-plus-DELETE idiom of TestNullAndBoundaryRoundTripAcrossTiers;
	// RunInit alone leaves the rows unflushed, i.e. still hot).
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx,
		"DELETE FROM change_log WHERE schema_id = $1 AND row_id = ANY($2)",
		wide.ID, rowIDs(seeded))
	runBoundaryProbes(ctx, t, env, "cold-duck", Query{Schema: wide, Limit: 100}, true, probes)
}

// TestEAVBigintFilterBoundaryBothDialects (#357) is the execution-level twin of
// the "bigint EAV-only above 2^53" characterization row: it proves on real
// storage, on BOTH binders, that the exact bind #281 introduced does not
// silently change which rows an EAV-only bigint filter returns.
//
// The contract has two halves and only asserting both is meaningful:
//   - the value that was never stored (2^53+1) matches NOTHING on either leg —
//     an exact bind must not "find" a row by colliding with the same rounding
//     error the write path made;
//   - the value that WAS stored (2^53, the rounded form) still matches the row
//     on either leg — the exactness must not cost reachability.
//
// Tier parity is the point: hot Postgres and cold DuckDB must answer the same
// filter identically. AssertQueryMatches is deliberately unused — the oracle
// normalizes numerics to float64 and is structurally blind to this distinction.
func TestEAVBigintFilterBoundaryBothDialects(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	// `total` (attr 15) is the EAV-only bigint; `amount` (bound bigint_01) is
	// only row identity here, so assertExactBigintRowSet can name the rows.
	aboveCeiling := CreateEvent(wide, map[string]any{
		"title": "eav-2p53p1", "total": int64(1)<<53 + 1, "amount": int64(101),
	})
	belowCeiling := CreateEvent(wide, map[string]any{
		"title": "eav-small", "total": int64(7), "amount": int64(102),
	})
	seeded := []*Event{aboveCeiling, belowCeiling}
	mustApplyEvents(ctx, t, env, "eav bigint boundary creates", seeded...)

	probes := []boundaryProbe{
		// Never stored: the write rounded it away. Empty on both legs.
		{"eav_equals_2p53_plus_1_matches_nothing",
			Filter{Attr: "total", Op: "equals", Value: "9007199254740993"}, nil},
		// Stored (rounded) value stays addressable.
		{"eav_equals_2p53_matches_rounded_row",
			Filter{Attr: "total", Op: "equals", Value: "9007199254740992"}, []*Event{aboveCeiling}},
		// Below the ceiling nothing is lossy — sanity that the probe schema works.
		{"eav_equals_below_ceiling", Filter{Attr: "total", Op: "equals", Value: "7"}, []*Event{belowCeiling}},
		// #357 spellings: same value, decimal/exponent form, same answer.
		{"eav_equals_2p53_decimal_spelling",
			Filter{Attr: "total", Op: "equals", Value: "9007199254740992.0"}, []*Event{aboveCeiling}},
		{"eav_equals_2p53_plus_1_exponent_spelling_matches_nothing",
			Filter{Attr: "total", Op: "equals", Value: "9.007199254740993e15"}, nil},
	}

	// PG leg: rows are unflushed, PreferHot short-circuits to postgres-only.
	hotBase := Query{Schema: wide, PreferHot: true, Limit: 100}
	assertEAVCeilingStored(ctx, t, env, "hot-pg", hotBase, aboveCeiling)
	runBoundaryProbes(ctx, t, env, "eav/hot-pg", hotBase, false, probes)

	// DuckDB leg: export to base, then drop the change_log entries so the rows
	// leave the dirty set and are served from cold parquet.
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx,
		"DELETE FROM change_log WHERE schema_id = $1 AND row_id = ANY($2)",
		wide.ID, rowIDs(seeded))

	coldBase := Query{Schema: wide, Limit: 100}
	assertEAVCeilingStored(ctx, t, env, "cold-duck", coldBase, aboveCeiling)
	runBoundaryProbes(ctx, t, env, "eav/cold-duck", coldBase, true, probes)
}

// assertEAVCeilingStored pins WHY the 2^53+1 probe must come back empty: the
// tier really holds the rounded 2^53, so an empty result is the storage
// contract and not a broken binder. Without this the miss probe would pass
// just as happily against a filter that matches nothing at all.
func assertEAVCeilingStored(ctx context.Context, t *testing.T, env *Env, label string, base Query, ev *Event) {
	t.Helper()
	res := mustQuery(ctx, t, env, base)
	for _, rec := range res.Records {
		if rec.RowID == ev.RowID {
			// maxEAVInt is 2^53, the value the write path rounded 2^53+1 down
			// to: eav_data persists value_numeric only (transform clears the
			// exact int64 sidecar for unbound attributes), so the float64 hop
			// is lossy above the ceiling (#205).
			assertEAVNumeric(t, label+" eav total", rec, 15, maxEAVInt)
			return
		}
	}
	t.Fatalf("%s: seeded EAV boundary row %s absent from the unfiltered control query", label, ev.RowID)
}

// runBoundaryProbes executes every probe against base and asserts the routing
// dialect plus the exact row set. wantDuck pins which binder actually ran:
// without it a mis-routed leg would silently retest the other dialect.
func runBoundaryProbes(ctx context.Context, t *testing.T, env *Env, label string,
	base Query, wantDuck bool, probes []boundaryProbe) {
	t.Helper()
	for _, p := range probes {
		q := base
		q.Filters = []Filter{p.filter}
		res := mustQuery(ctx, t, env, q)
		if got := res.Plan.Routing.UseDuckDB; got != wantDuck {
			t.Errorf("%s/%s: UseDuckDB = %t, want %t (routing %+v)",
				label, p.name, got, wantDuck, res.Plan.Routing)
		}
		assertExactBigintRowSet(t, label+"/"+p.name, res, p.want)
	}
}

// assertExactBigintRowSet fails unless res.Records is exactly the given events
// (as an unordered set) and each carries the exact seeded amount. Both halves
// matter: an inexact bind can drop a row that should match and/or admit one
// that should not, and only an exact-set assertion sees both directions.
func assertExactBigintRowSet(t *testing.T, label string, res *QueryResult, want []*Event) {
	t.Helper()
	got := map[uuid.UUID]int64{}
	for _, r := range res.Records {
		got[r.RowID] = r.Int64Items["bigint_01"]
	}
	if len(got) != len(want) {
		t.Errorf("%s: %d rows, want %d (got %v)", label, len(got), len(want), got)
	}
	for _, ev := range want {
		amt, ok := got[ev.RowID]
		if !ok {
			t.Errorf("%s: missing row %s (%v)", label, ev.RowID, ev.Attrs["title"])
			continue
		}
		wantAmt, isInt := ev.Attrs["amount"].(int64)
		if !isInt {
			t.Fatalf("%s: seeded amount for %v is %T, want int64", label, ev.Attrs["title"], ev.Attrs["amount"])
		}
		if amt != wantAmt {
			t.Errorf("%s: row %s amount = %d, want %d", label, ev.RowID, amt, wantAmt)
		}
	}
}
