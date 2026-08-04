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
