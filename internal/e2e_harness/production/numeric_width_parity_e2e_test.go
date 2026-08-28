//go:build e2e

package production

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// This file is the #384 acceptance suite: for EAV-only attributes whose
// stored value_numeric falls outside (or beside) the declared width, hot
// Postgres and cold DuckDB must answer the same filter identically —
// projection by storage width (BIGINT/DOUBLE), operand casts by storage
// width, and one bool truth rule (value_numeric <> 0) on both routes.
//
// Out-of-range values are planted by direct eav_data UPDATEs: the write
// funnel rejects them since #384 (TestEAVIntegerWidthWriteRejection), so the
// planted rows stand in for history written before the rejection existed.

// widthProbe is one filter probe plus the exact row set it must return, on
// both dialects. Expectations are row-identity sets, never oracle
// comparisons (the oracle normalizes numerics to float64).
type widthProbe struct {
	name   string
	filter Filter
	want   []*Event
}

// runWidthProbes executes every probe and asserts the routing dialect plus
// the exact row-ID set. wantDuck pins which binder actually ran.
func runWidthProbes(ctx context.Context, t *testing.T, env *Env, label string,
	base Query, wantDuck bool, probes []widthProbe) {
	t.Helper()
	for _, p := range probes {
		q := base
		q.Filters = []Filter{p.filter}
		res := mustQuery(ctx, t, env, q)
		if got := res.Plan.Routing.UseDuckDB; got != wantDuck {
			t.Errorf("%s/%s: UseDuckDB = %t, want %t (routing %+v)",
				label, p.name, got, wantDuck, res.Plan.Routing)
		}
		assertWidthRowSet(t, label+"/"+p.name, res, p.want)
	}
}

// assertWidthRowSet fails unless res.Records is exactly the given events as an
// unordered row-ID set. Both directions matter: a width mismatch can drop a
// row that should match (NULL projection) and/or fail the query outright.
func assertWidthRowSet(t *testing.T, label string, res *QueryResult, want []*Event) {
	t.Helper()
	got := map[uuid.UUID]bool{}
	for _, r := range res.Records {
		got[r.RowID] = true
	}
	if len(got) != len(want) {
		t.Errorf("%s: %d rows, want %d (got %v)", label, len(got), len(want), got)
	}
	for _, ev := range want {
		if !got[ev.RowID] {
			t.Errorf("%s: missing row %s (%v)", label, ev.RowID, ev.Attrs["title"])
		}
	}
}

// serveFromCold exports every seeded row to base parquet and clears their
// change_log entries so the rows leave the dirty set and are served from the
// cold tier (the RunInit-plus-DELETE idiom of the boundary suites).
func serveFromCold(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, seeded []*Event) {
	t.Helper()
	if _, err := env.RunInit(ctx, schema); err != nil {
		t.Fatalf("run init: %v", err)
	}
	env.ExecSQL(ctx,
		"DELETE FROM change_log WHERE schema_id = $1 AND row_id = ANY($2)",
		schema.ID, rowIDs(seeded))
}

// TestEAVIntegerWidthParityBothDialects is the #384 headline: an EAV-only
// integer/smallint value past the declared width (planted directly, standing
// in for pre-rejection history) must be matched by the same filter on the hot
// Postgres route and every DuckDB tier. Pre-#384 the DuckDB legs projected it
// to NULL via TRY_CAST at declared width (no match) and the strict
// CAST(? AS INTEGER) operand raised a Conversion Error for the out-of-range
// literal, while hot Postgres compared the stored NUMERIC and matched.
func TestEAVIntegerWidthParityBothDialects(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	// `qty` (attr 14) is the EAV-only integer, `level` (attr 13) the EAV-only
	// smallint. In-range placeholders first; the over-range state is planted.
	rowOver := CreateEvent(wide, map[string]any{"title": "w-qty-over", "qty": 42})
	rowSmall := CreateEvent(wide, map[string]any{"title": "w-qty-small", "qty": 7})
	rowLevel := CreateEvent(wide, map[string]any{"title": "w-level-over", "level": 5})
	seeded := []*Event{rowOver, rowSmall, rowLevel}
	mustApplyEvents(ctx, t, env, "width parity creates", seeded...)

	env.ExecSQL(ctx,
		"UPDATE eav_data SET value_numeric = 4294967296 WHERE schema_id = $1 AND row_id = $2 AND attr_id = 14",
		wide.ID, rowOver.RowID)
	env.ExecSQL(ctx,
		"UPDATE eav_data SET value_numeric = 40000 WHERE schema_id = $1 AND row_id = $2 AND attr_id = 13",
		wide.ID, rowLevel.RowID)

	probes := []widthProbe{
		// 2^32: past INT32. Equality must address the stored value on both
		// dialects, and the DuckDB leg must not error on the operand.
		{"qty_equals_2p32", Filter{Attr: "qty", Op: "equals", Value: "4294967296"}, []*Event{rowOver}},
		// Range operator with an in-range literal still sees the row.
		{"qty_gt_maxint32", Filter{Attr: "qty", Op: "gt", Value: "2147483647"}, []*Event{rowOver}},
		// The placeholder value must be gone — guards that the UPDATE took.
		{"qty_equals_placeholder_gone", Filter{Attr: "qty", Op: "equals", Value: "42"}, nil},
		// In-range values keep working.
		{"qty_equals_in_range", Filter{Attr: "qty", Op: "equals", Value: "7"}, []*Event{rowSmall}},
		// smallint twin at 40000 (past INT16).
		{"level_equals_40000", Filter{Attr: "level", Op: "equals", Value: "40000"}, []*Event{rowLevel}},
		{"level_gt_maxint16", Filter{Attr: "level", Op: "gt", Value: "32767"}, []*Event{rowLevel}},
	}

	hotBase := Query{Schema: wide, PreferHot: true, Limit: 100}
	runWidthProbes(ctx, t, env, "int/hot-pg", hotBase, false, probes)

	serveFromCold(ctx, t, env, wide, seeded)
	coldBase := Query{Schema: wide, Limit: 100}
	runWidthProbes(ctx, t, env, "int/cold-duck", coldBase, true, probes)
}

// TestEAVIntegerWidthWriteRejection pins the write half of the #384 ruling:
// the production write path answers invalid input for numeric-family values
// that do not fit the declared integer type, instead of storing state the
// tiers used to disagree on.
func TestEAVIntegerWidthWriteRejection(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	rejected := []struct {
		name  string
		attrs map[string]any
	}{
		{"integer_past_2p31", map[string]any{"title": "rej-int", "qty": int64(4294967296)}},
		{"smallint_past_2p15", map[string]any{"title": "rej-small", "level": 40000}},
		{"integer_non_integral", map[string]any{"title": "rej-frac", "qty": 1.5}},
	}
	for _, tc := range rejected {
		err := env.ApplyEvents(ctx, CreateEvent(wide, tc.attrs))
		if err == nil {
			t.Errorf("%s: out-of-width write must be rejected", tc.name)
			continue
		}
		if !errors.Is(err, forma.ErrInvalidInput) {
			t.Errorf("%s: rejection must be user-facing invalid input, got %v", tc.name, err)
		}
	}

	// Control: boundary values stay writable.
	ok := CreateEvent(wide, map[string]any{"title": "ok-bounds", "qty": 2147483647, "level": -32768})
	mustApplyEvents(ctx, t, env, "in-range boundary write", ok)
}

// TestEAVBoolTruthinessParityBothDialects pins the #384 bool ruling: both
// routes compare the value_numeric <> 0 truthiness. Pre-#384 the OLTP route
// bound equality against exactly 1.0/0.0, so a stored 2 (planted; the write
// funnels store only 1/0, #404) matched equals:1 on the DuckDB tiers and not
// on the hot route.
func TestEAVBoolTruthinessParityBothDialects(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	// `active` is attr 8, EAV-only bool.
	rowTrue := CreateEvent(wide, map[string]any{"title": "b-true", "active": true})
	rowStored2 := CreateEvent(wide, map[string]any{"title": "b-two", "active": true})
	rowFalse := CreateEvent(wide, map[string]any{"title": "b-false", "active": false})
	seeded := []*Event{rowTrue, rowStored2, rowFalse}
	mustApplyEvents(ctx, t, env, "bool truthiness creates", seeded...)

	env.ExecSQL(ctx,
		"UPDATE eav_data SET value_numeric = 2 WHERE schema_id = $1 AND row_id = $2 AND attr_id = 8",
		wide.ID, rowStored2.RowID)

	probes := []widthProbe{
		{"active_equals_1", Filter{Attr: "active", Op: "equals", Value: "1"}, []*Event{rowTrue, rowStored2}},
		{"active_equals_0", Filter{Attr: "active", Op: "equals", Value: "0"}, []*Event{rowFalse}},
		{"active_not_equals_1", Filter{Attr: "active", Op: "not_equals", Value: "1"}, []*Event{rowFalse}},
	}

	hotBase := Query{Schema: wide, PreferHot: true, Limit: 100}
	runWidthProbes(ctx, t, env, "bool/hot-pg", hotBase, false, probes)

	serveFromCold(ctx, t, env, wide, seeded)
	coldBase := Query{Schema: wide, Limit: 100}
	runWidthProbes(ctx, t, env, "bool/cold-duck", coldBase, true, probes)
}

// TestEAVNumericOperandWidthParityBothDialects pins the #384 numeric ruling:
// operands cast to DOUBLE, the width every tier column actually carries.
// Pre-#384 the CAST(? AS DECIMAL(38,10)) operand silently truncated literals
// beyond 10 fractional digits (mismatching a stored DOUBLE the hot route
// matched) and overflowed outright past ~1e28 (one-sided query failure —
// the gt_1e28 probes could not even run on the DuckDB leg).
func TestEAVNumericOperandWidthParityBothDialects(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	// `ratio` is attr 16, EAV-only numeric. All values are float64-clean.
	rowFine := CreateEvent(wide, map[string]any{"title": "n-fine", "ratio": 0.12345678905})
	rowCoarse := CreateEvent(wide, map[string]any{"title": "n-coarse", "ratio": 0.1234567890})
	rowHuge := CreateEvent(wide, map[string]any{"title": "n-huge", "ratio": 1e30})
	seeded := []*Event{rowFine, rowCoarse, rowHuge}
	mustApplyEvents(ctx, t, env, "numeric width creates", seeded...)

	probes := []widthProbe{
		// 11 fractional digits: DECIMAL(38,10) truncated this operand.
		{"ratio_equals_11_digits", Filter{Attr: "ratio", Op: "equals", Value: "0.12345678905"}, []*Event{rowFine}},
		// Past DECIMAL(38,10)'s integer range: must answer, not error.
		{"ratio_gt_1e28", Filter{Attr: "ratio", Op: "gt", Value: "1e28"}, []*Event{rowHuge}},
		{"ratio_lt_1", Filter{Attr: "ratio", Op: "lt", Value: "1"}, []*Event{rowFine, rowCoarse}},
	}

	hotBase := Query{Schema: wide, PreferHot: true, Limit: 100}
	runWidthProbes(ctx, t, env, "num/hot-pg", hotBase, false, probes)

	serveFromCold(ctx, t, env, wide, seeded)
	coldBase := Query{Schema: wide, Limit: 100}
	runWidthProbes(ctx, t, env, "num/cold-duck", coldBase, true, probes)
}
