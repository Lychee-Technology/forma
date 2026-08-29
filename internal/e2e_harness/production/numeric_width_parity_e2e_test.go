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
// Postgres and every DuckDB tier must answer the same filter identically —
// projection and operand casts by storage width (DOUBLE: the write funnel
// narrows everything through float64), and one bool truth rule
// (value_numeric <> 0) with one operand parse rule on both routes.
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

// runWidthProbesAllTiers drives the same probe set through all three tiers
// via the real pipeline: hot (unflushed, PreferHot → Postgres), warm (after a
// real CDC flush the rows are served from delta parquet), and cold (after a
// real compaction the delta is merged into base). The warm stage is what pins
// that the CDC export — not just the init export — carries the storage-width
// contract.
func runWidthProbesAllTiers(ctx context.Context, t *testing.T, env *Env, schema SchemaRef,
	label string, probes []widthProbe) {
	t.Helper()
	hotBase := Query{Schema: schema, PreferHot: true, Limit: 100}
	runWidthProbes(ctx, t, env, label+"/hot-pg", hotBase, false, probes)

	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("%s: flush: %v", label, err)
	}
	duckBase := Query{Schema: schema, Limit: 100}
	runWidthProbes(ctx, t, env, label+"/warm-duck", duckBase, true, probes)

	if _, err := env.RunCompaction(ctx, schema); err != nil {
		t.Fatalf("%s: compaction: %v", label, err)
	}
	runWidthProbes(ctx, t, env, label+"/cold-duck", duckBase, true, probes)
}

// TestEAVIntegerWidthParityBothDialects is the #384 headline: an EAV-only
// integer/smallint value past the declared width (planted directly, standing
// in for pre-rejection history) must be matched by the same filter on the hot
// Postgres route and every DuckDB tier. Pre-#384 the DuckDB legs projected an
// over-range value to NULL via TRY_CAST at declared width (no match), rounded
// a non-integral value, and the strict CAST(? AS INTEGER) operand raised a
// Conversion Error for the out-of-range literal — while hot Postgres compared
// the stored NUMERIC and matched throughout.
func TestEAVIntegerWidthParityBothDialects(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	// `qty` (attr 14) is the EAV-only integer, `level` (attr 13) the EAV-only
	// smallint. In-range placeholders first; the illegal state is planted.
	rowOver := CreateEvent(wide, map[string]any{"title": "w-qty-over", "qty": 42})
	rowFrac := CreateEvent(wide, map[string]any{"title": "w-qty-frac", "qty": 43})
	rowP53 := CreateEvent(wide, map[string]any{"title": "w-qty-p53", "qty": 44})
	rowSmall := CreateEvent(wide, map[string]any{"title": "w-qty-small", "qty": 7})
	rowLevel := CreateEvent(wide, map[string]any{"title": "w-level-over", "level": 5})
	seeded := []*Event{rowOver, rowFrac, rowP53, rowSmall, rowLevel}
	mustApplyEvents(ctx, t, env, "width parity creates", seeded...)

	env.ExecSQL(ctx,
		"UPDATE eav_data SET value_numeric = 4294967296 WHERE schema_id = $1 AND row_id = $2 AND attr_id = 14",
		wide.ID, rowOver.RowID)
	env.ExecSQL(ctx,
		"UPDATE eav_data SET value_numeric = 1.5 WHERE schema_id = $1 AND row_id = $2 AND attr_id = 14",
		wide.ID, rowFrac.RowID)
	env.ExecSQL(ctx,
		"UPDATE eav_data SET value_numeric = 9007199254740992 WHERE schema_id = $1 AND row_id = $2 AND attr_id = 14",
		wide.ID, rowP53.RowID)
	env.ExecSQL(ctx,
		"UPDATE eav_data SET value_numeric = 40000 WHERE schema_id = $1 AND row_id = $2 AND attr_id = 13",
		wide.ID, rowLevel.RowID)

	probes := []widthProbe{
		// 2^32: past INT32. Equality must address the stored value on both
		// dialects, and the DuckDB leg must not error on the operand.
		{"qty_equals_2p32", Filter{Attr: "qty", Op: "equals", Value: "4294967296"}, []*Event{rowOver}},
		// Range operator with an in-range literal sees both planted rows
		// above INT32 (2^32 and 2^53).
		{"qty_gt_maxint32", Filter{Attr: "qty", Op: "gt", Value: "2147483647"}, []*Event{rowOver, rowP53}},
		// Non-integral history (P1a): DOUBLE projection keeps 1.5 as 1.5 on
		// every DuckDB tier — an integer-width cast rounded it to 2, matching
		// equals:2 only on the DuckDB legs while Postgres compared 1.5.
		{"qty_equals_frac", Filter{Attr: "qty", Op: "equals", Value: "1.5"}, []*Event{rowFrac}},
		{"qty_equals_2_not_frac", Filter{Attr: "qty", Op: "equals", Value: "2"}, nil},
		// 2^53 boundary (fourth-review P1): the stored value is the float64
		// image 2^53; the operand 2^53+1 narrows to that same image on BOTH
		// engines (PG via NarrowEAVNumericOperand, DuckDB via CAST AS
		// DOUBLE), so both match — pre-fix PG bound the exact int64 and
		// missed while the DuckDB tiers matched. A representable neighbor
		// (…990) still misses on both.
		{"qty_equals_2p53", Filter{Attr: "qty", Op: "equals", Value: "9007199254740992"}, []*Event{rowP53}},
		{"qty_equals_2p53_plus_1_same_image", Filter{Attr: "qty", Op: "equals", Value: "9007199254740993"}, []*Event{rowP53}},
		{"qty_equals_2p53_minus_2_empty", Filter{Attr: "qty", Op: "equals", Value: "9007199254740990"}, nil},
		// The placeholder values must be gone — guards that the UPDATEs took.
		{"qty_equals_placeholder_gone", Filter{Attr: "qty", Op: "equals", Value: "42"}, nil},
		{"qty_equals_frac_placeholder_gone", Filter{Attr: "qty", Op: "equals", Value: "43"}, nil},
		{"qty_equals_p53_placeholder_gone", Filter{Attr: "qty", Op: "equals", Value: "44"}, nil},
		// In-range values keep working.
		{"qty_equals_in_range", Filter{Attr: "qty", Op: "equals", Value: "7"}, []*Event{rowSmall}},
		// smallint twin at 40000 (past INT16).
		{"level_equals_40000", Filter{Attr: "level", Op: "equals", Value: "40000"}, []*Event{rowLevel}},
		{"level_gt_maxint16", Filter{Attr: "level", Op: "gt", Value: "32767"}, []*Event{rowLevel}},
		// Operand past BIGINT (P2a): compares as DOUBLE instead of erroring
		// only on the DuckDB route.
		{"qty_gt_1e30_empty", Filter{Attr: "qty", Op: "gt", Value: "1e30"}, nil},
	}

	runWidthProbesAllTiers(ctx, t, env, wide, "int", probes)
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
		// Operand spellings under the engine-shared parse rule (P2b): "true"
		// used to 400 only on the PG route, "2" used to error only on the
		// DuckDB route.
		{"active_equals_true", Filter{Attr: "active", Op: "equals", Value: "true"}, []*Event{rowTrue, rowStored2}},
		{"active_equals_2_truthy", Filter{Attr: "active", Op: "equals", Value: "2"}, []*Event{rowTrue, rowStored2}},
	}

	runWidthProbesAllTiers(ctx, t, env, wide, "bool", probes)
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
	rowP16 := CreateEvent(wide, map[string]any{"title": "n-p16", "ratio": 0.1234567890123456})
	seeded := []*Event{rowFine, rowCoarse, rowHuge, rowP16}
	mustApplyEvents(ctx, t, env, "numeric width creates", seeded...)

	probes := []widthProbe{
		// 11 fractional digits: DECIMAL(38,10) truncated this operand.
		{"ratio_equals_11_digits", Filter{Attr: "ratio", Op: "equals", Value: "0.12345678905"}, []*Event{rowFine}},
		// 16 significant digits (P1b): the %.15g operand render dropped the
		// last digit, so the DuckDB route bound an adjacent float64 and
		// equality missed the row only there.
		{"ratio_equals_16_digits", Filter{Attr: "ratio", Op: "equals", Value: "0.1234567890123456"}, []*Event{rowP16}},
		// Past DECIMAL(38,10)'s integer range: must answer, not error.
		{"ratio_gt_1e28", Filter{Attr: "ratio", Op: "gt", Value: "1e28"}, []*Event{rowHuge}},
		{"ratio_lt_1", Filter{Attr: "ratio", Op: "lt", Value: "1"}, []*Event{rowFine, rowCoarse, rowP16}},
	}

	runWidthProbesAllTiers(ctx, t, env, wide, "num", probes)
}
