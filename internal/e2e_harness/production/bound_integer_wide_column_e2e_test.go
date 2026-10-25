//go:build e2e

package production

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma/internal/transform"
)

// This file is the #627 acceptance. An integer bound to a column wider than
// its declared type can hold an image that type cannot: a bigint in a
// double_* column keeps its float64 image (#590), and an integer in a
// bigint_* column keeps an int64. The write funnel refuses such images, so
// they are planted here by direct SQL, standing in for history written
// before the funnel or by hand. The CDC export reads the column through
// TRY_CAST to the declared type, so the parquet copy holds NULL for an image
// past the type's range, NaN or ±Infinity, and a fraction rounded half to
// even. The DuckDB hot leg read the raw column instead: the outer select's
// strict cast failed every federated query that reached the unflushed row,
// and the pg_source pushdown and the `visible` filter compared the raw
// image, so a filter matched the row differently before and after its
// flush. Both hot projections read it the export's way now. The OLTP route
// is not exercised here: it refuses both images with a consistency error
// (#590 for the bigint, #708 for the integer;
// width_audit_bound_integer_e2e_test.go).

const wideIntegerProps = `{
    "name": {"type": "string"},
    "ratio": {"type": "integer"},
    "qty": {"type": "integer"}
  }`

const wideIntegerAttrs = `{
  "name": {"attributeID": 1, "valueType": "text", "column_binding": {"col_name": "text_01"}},
  "ratio": {"attributeID": 2, "valueType": "bigint", "column_binding": {"col_name": "double_01"}},
  "qty": {"attributeID": 3, "valueType": "integer", "column_binding": {"col_name": "bigint_01"}}
}
`

// The mixed generation adds an EAV-only attribute so the hot leg takes the
// EAV-joined projection instead of the no-EAV shortcut.
const wideIntegerMixedProps = `{
    "name": {"type": "string"},
    "ratio": {"type": "integer"},
    "qty": {"type": "integer"},
    "note": {"type": "string"}
  }`

const wideIntegerMixedAttrs = `{
  "name": {"attributeID": 1, "valueType": "text", "column_binding": {"col_name": "text_01"}},
  "ratio": {"attributeID": 2, "valueType": "bigint", "column_binding": {"col_name": "double_01"}},
  "qty": {"attributeID": 3, "valueType": "integer", "column_binding": {"col_name": "bigint_01"}},
  "note": {"attributeID": 4, "valueType": "text"}
}
`

// wideIntegerRow is one seeded row: the image planted in its column, and
// what every DuckDB tier answers for ratio and qty (nil: absent).
type wideIntegerRow struct {
	ev          *Event
	column      string // the column the image is planted in; "" plants nothing
	image       string // a PostgreSQL literal
	ratio, qty  any
	unsetRatio  bool // the row is written without a ratio
	description string
}

func wideIntegerRows() []*wideIntegerRow {
	return []*wideIntegerRow{
		{description: "anchor", ratio: float64(7), qty: float64(1)},
		{description: "whole past 2^53", column: "double_01", image: "9007199254740994", ratio: float64(9007199254740994), qty: float64(1)},
		{description: "fraction rounding down", column: "double_01", image: "1000.5", ratio: float64(1000), qty: float64(1)},
		{description: "fraction rounding up", column: "double_01", image: "1001.5", ratio: float64(1002), qty: float64(1)},
		{description: "2^63", column: "double_01", image: "9223372036854775808", qty: float64(1)},
		{description: "NaN", column: "double_01", image: "'NaN'", qty: float64(1)},
		{description: "Infinity", column: "double_01", image: "'Infinity'", qty: float64(1)},
		{description: "-Infinity", column: "double_01", image: "'-Infinity'", qty: float64(1)},
		{description: "integer past int32", column: "bigint_01", image: "4294967296", ratio: float64(7)},
		{description: "ratio unset", unsetRatio: true, qty: float64(1)},
	}
}

// wideIntegerProbes is the truth table every DuckDB tier must reach. Each
// probe sits where the raw image and the declared-type read disagree.
func wideIntegerProbes(rows []*wideIntegerRow) []widthProbe {
	pick := func(keep func(r *wideIntegerRow) bool) []*Event {
		var out []*Event
		for _, r := range rows {
			if keep(r) {
				out = append(out, r.ev)
			}
		}
		return out
	}
	ratioIs := func(v float64) func(*wideIntegerRow) bool {
		return func(r *wideIntegerRow) bool { got, ok := r.ratio.(float64); return ok && got == v }
	}
	ratioAbove := func(v float64) func(*wideIntegerRow) bool {
		return func(r *wideIntegerRow) bool { got, ok := r.ratio.(float64); return ok && got > v }
	}
	return []widthProbe{
		{"ratio_is_null", Filter{Attr: "ratio", Op: "is_null", Value: "true"}, pick(func(r *wideIntegerRow) bool { return r.ratio == nil })},
		{"ratio_is_not_null", Filter{Attr: "ratio", Op: "is_not_null", Value: "true"}, pick(func(r *wideIntegerRow) bool { return r.ratio != nil })},
		{"ratio_eq_1000", Filter{Attr: "ratio", Op: "equals", Value: "1000"}, pick(ratioIs(1000))},
		{"ratio_eq_1002", Filter{Attr: "ratio", Op: "equals", Value: "1002"}, pick(ratioIs(1002))},
		{"ratio_gt_1000", Filter{Attr: "ratio", Op: "gt", Value: "1000"}, pick(ratioAbove(1000))},
		{"qty_is_null", Filter{Attr: "qty", Op: "is_null", Value: "true"}, pick(func(r *wideIntegerRow) bool { return r.qty == nil })},
		{"qty_eq_1", Filter{Attr: "qty", Op: "equals", Value: "1"}, pick(func(r *wideIntegerRow) bool { return r.qty != nil })},
	}
}

// seedWideIntegerRows creates the rows through the API, flushes the anchor
// so the schema has a parquet leg to glob, and plants each image in an
// unflushed row.
func seedWideIntegerRows(ctx context.Context, t *testing.T, env *Env, withNote bool) []*wideIntegerRow {
	t.Helper()
	simple := DefaultSchemaFixtures()[0]
	rows := wideIntegerRows()
	for i, r := range rows {
		attrs := map[string]any{"name": r.description, "qty": 1}
		if !r.unsetRatio {
			attrs["ratio"] = 7
		}
		if withNote {
			attrs["note"] = "n-" + r.description
		}
		r.ev = CreateEvent(simple, attrs)
		mustApplyEvents(ctx, t, env, "create "+r.description, r.ev)
		if i == 0 {
			if _, err := env.RunFlush(ctx); err != nil {
				t.Fatalf("anchor flush: %v", err)
			}
		}
		if r.column != "" {
			env.ExecSQL(ctx, "UPDATE "+env.Tables.EntityMain+" SET "+r.column+" = "+r.image+
				" WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2", simple.ID, r.ev.RowID)
		}
	}
	return rows
}

// assertWideIntegerReads reads the whole row set through DuckDB and pins
// what each row answers for ratio and qty: the query must not fail, and a
// row's values are its parquet copy's on every tier.
func assertWideIntegerReads(ctx context.Context, t *testing.T, env *Env, label string, rows []*wideIntegerRow) {
	t.Helper()
	res := mustQuery(ctx, t, env, Query{Schema: DefaultSchemaFixtures()[0], Limit: 100})
	if !res.Plan.Routing.UseDuckDB {
		t.Fatalf("%s: unfiltered read did not route to duckdb: %+v", label, res.Plan.Routing)
	}
	transformer := transform.NewPersistentRecordTransformer(env.Registry)
	got := map[string]map[string]any{}
	for _, rec := range res.Records {
		attrs, err := transformer.FromPersistentRecord(ctx, rec)
		if err != nil {
			t.Fatalf("%s: decode row %s: %v", label, rec.RowID, err)
		}
		got[rec.RowID.String()] = jsonNormalizedAttrs(t, attrs)
	}
	for _, r := range rows {
		attrs, ok := got[r.ev.RowID.String()]
		if !ok {
			t.Errorf("%s: row %q missing from the unfiltered read", label, r.description)
			continue
		}
		if attrs["ratio"] != r.ratio || attrs["qty"] != r.qty {
			t.Errorf("%s: row %q reads ratio=%v qty=%v, want ratio=%v qty=%v",
				label, r.description, attrs["ratio"], attrs["qty"], r.ratio, r.qty)
		}
	}
}

// runWideIntegerAllTiers walks the planted rows through the unflushed
// DuckDB hot leg, delta parquet and base parquet, with the same expected
// answers on each.
func runWideIntegerAllTiers(ctx context.Context, t *testing.T, env *Env, withNote bool) {
	t.Helper()
	simple := DefaultSchemaFixtures()[0]
	rows := seedWideIntegerRows(ctx, t, env, withNote)
	duck := Query{Schema: simple, Limit: 100}

	runWidthProbes(ctx, t, env, "hot-duck", duck, true, wideIntegerProbes(rows))
	assertWideIntegerReads(ctx, t, env, "hot-duck", rows)

	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	runWidthProbes(ctx, t, env, "warm-duck", duck, true, wideIntegerProbes(rows))
	assertWideIntegerReads(ctx, t, env, "warm-duck", rows)

	if _, err := env.RunCompaction(ctx, simple); err != nil {
		t.Fatalf("compaction: %v", err)
	}
	runWidthProbes(ctx, t, env, "cold-duck", duck, true, wideIntegerProbes(rows))
	assertWideIntegerReads(ctx, t, env, "cold-duck", rows)
}

// TestBoundIntegerWideColumnImagesNoEAVAllTiers: every attribute is
// column-bound, so the DuckDB hot leg takes BuildPGSelectNoEAV.
func TestBoundIntegerWideColumnImagesNoEAVAllTiers(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(writeSimpleSchemaDir(t, wideIntegerProps, wideIntegerAttrs)))
	runWideIntegerAllTiers(ctx, t, env, false)
}

// TestBoundIntegerWideColumnImagesEAVJoinedAllTiers: the EAV-only note
// forces the EAV-joined hot projection, where the bound columns sit in a
// COALESCE with the EAV pivot.
func TestBoundIntegerWideColumnImagesEAVJoinedAllTiers(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(writeSimpleSchemaDir(t, wideIntegerMixedProps, wideIntegerMixedAttrs)))
	runWideIntegerAllTiers(ctx, t, env, true)
}
