//go:build e2e

package production

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

// This file is the #404 acceptance for column-bound bools (PR #564 review,
// findings 1 and 2): a schema whose attributes are ALL column-bound takes the
// no-EAV projection on the DuckDB hot leg, and the same rows are then read
// from delta parquet after a real CDC flush and from base parquet after a
// real compaction. Every route must reach the same verdict from the same
// storage image under the one read-side rule — `> 0.5` for bool_smallint,
// `= '1'` for bool_text — and an unset optional bool must stay NULL on every
// tier, never collapse to false at flush.
//
// Off-contract images (SMALLINT -1 / 2, VARCHAR 'true') are planted by
// direct entity_main UPDATEs: the write funnel rejects them, so they stand in
// for history written before the rejection existed. They are probed on every
// tier, the unflushed hot routes included: a filter on the bool itself is
// pushed down into entity_main on both the Postgres route and the DuckDB hot
// leg's postgres_scan, and since #565 that pushdown carries the read
// contract's verdict — `smallint_01 BETWEEN 1 AND 32767` for the truthy
// side of bool_smallint, `(text_02 = '1') = false` for the falsy side of
// bool_text — so the planted 2 answers `equals:true` and the planted 'true'
// answers `equals:false` the way the projection reads them, instead of the
// raw `= 1` / `= '0'` compare matching neither operand.

const boundBoolProps = `{
    "name": { "type": "string" },
    "flagInt": { "type": "boolean" },
    "flagText": { "type": "boolean" }
  }`

const boundBoolAttrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "flagInt": { "attributeID": 2, "valueType": "bool", "column_binding": { "col_name": "smallint_01", "encoding": "bool_smallint" } },
  "flagText": { "attributeID": 3, "valueType": "bool", "column_binding": { "col_name": "text_02", "encoding": "bool_text" } }
}
`

// boundBoolRows is the seeded row set; the off-contract members are created
// on-contract and then overwritten in entity_main.
type boundBoolRows struct {
	trueRow, falseRow, unset, negOne, two, textTrue *Event
}

func seedBoundBoolRows(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) boundBoolRows {
	t.Helper()
	rows := boundBoolRows{
		trueRow:  CreateEvent(schema, map[string]any{"name": "b-true", "flagInt": true, "flagText": true}),
		falseRow: CreateEvent(schema, map[string]any{"name": "b-false", "flagInt": false, "flagText": false}),
		unset:    CreateEvent(schema, map[string]any{"name": "b-unset"}),
		negOne:   CreateEvent(schema, map[string]any{"name": "b-neg-one", "flagInt": false}),
		two:      CreateEvent(schema, map[string]any{"name": "b-two", "flagInt": false}),
		textTrue: CreateEvent(schema, map[string]any{"name": "b-text-true", "flagText": false}),
	}
	mustApplyEvents(ctx, t, env, "bound bool creates",
		rows.trueRow, rows.falseRow, rows.unset, rows.negOne, rows.two, rows.textTrue)
	return rows
}

// plantOffContract overwrites the storage image behind the write funnel's
// back. The rows stay unflushed, so the next CDC flush exports the planted
// image and the warm/cold stages read it from parquet.
func plantOffContract(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, rows boundBoolRows) {
	t.Helper()
	env.ExecSQL(ctx, "UPDATE entity_main SET smallint_01 = -1 WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2",
		schema.ID, rows.negOne.RowID)
	env.ExecSQL(ctx, "UPDATE entity_main SET smallint_01 = 2 WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2",
		schema.ID, rows.two.RowID)
	env.ExecSQL(ctx, "UPDATE entity_main SET text_02 = 'true' WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2",
		schema.ID, rows.textTrue.RowID)
}

// boolSpellingProbes expands one attribute's truthy/falsy row sets over the
// operand spellings every route must read under the one parseBoolOperand
// rule (#503): the ParseBool words and the integer spellings, where any
// positive integer is truthy. Before #564 `equals:true` was a 400 on the
// PreferHot route alone (strconv.Atoi), so the spellings are pinned side by
// side rather than through `true`/`false` only.
func boolSpellingProbes(attr string, truthy, falsy []*Event) []widthProbe {
	return []widthProbe{
		{attr + "_true", Filter{Attr: attr, Op: "equals", Value: "true"}, truthy},
		{attr + "_2", Filter{Attr: attr, Op: "equals", Value: "2"}, truthy},
		{attr + "_false", Filter{Attr: attr, Op: "equals", Value: "false"}, falsy},
		{attr + "_0", Filter{Attr: attr, Op: "equals", Value: "0"}, falsy},
		// not_equals is the complement on a two-valued domain: the pushdown
		// binds the opposite range under the same clause text (#565), and
		// the unset row still matches neither side.
		{attr + "_ne_true", Filter{Attr: attr, Op: "not_equals", Value: "true"}, falsy},
		{attr + "_ne_false", Filter{Attr: attr, Op: "not_equals", Value: "false"}, truthy},
	}
}

// boolRejectionProbes: a spelling outside the shared rule is invalid input
// on every route, never an answer on one and a 400 on the other.
func boolRejectionProbes() []rejectionProbe {
	return []rejectionProbe{
		{"flagInt_banana", Filter{Attr: "flagInt", Op: "equals", Value: "banana"}},
		{"flagText_banana", Filter{Attr: "flagText", Op: "equals", Value: "banana"}},
	}
}

// onContractProbes is the truth table while every image is still 1/0 and
// "1"/"0": the same on every route, pushdown or not. The unset row matches
// neither side of either attribute.
func onContractProbes(rows boundBoolRows) []widthProbe {
	return append(
		boolSpellingProbes("flagInt", []*Event{rows.trueRow}, []*Event{rows.falseRow, rows.negOne, rows.two}),
		boolSpellingProbes("flagText", []*Event{rows.trueRow}, []*Event{rows.falseRow, rows.textTrue})...,
	)
}

// parquetProbes is the truth table once the off-contract images are planted,
// on every tier: the hot routes push the #565 range / `= '1'` compare into
// entity_main and the parquet legs carry the exported verdict, so 2 sits
// with true, -1 with false, and 'true' with false on all of them.
func parquetProbes(rows boundBoolRows) []widthProbe {
	return append(
		boolSpellingProbes("flagInt", []*Event{rows.trueRow, rows.two}, []*Event{rows.falseRow, rows.negOne}),
		boolSpellingProbes("flagText", []*Event{rows.trueRow}, []*Event{rows.falseRow, rows.textTrue})...,
	)
}

// boundBoolImage is what one record carries in the two bound columns.
type boundBoolImage struct {
	small    int16
	smallSet bool
	text     string
	textSet  bool
}

// assertBoundBoolImages reads the whole row set through DuckDB and pins the
// image each record carries. The DuckDB outer select re-derives the physical
// column from the verdict, so an off-contract row reads as the verdict's
// image: 2 → 1, -1 → 0, 'true' → "0" — the same answer the hot-leg
// truthiness, the export and the Go read funnel reach, where the raw
// projection plus CAST(attr AS BOOLEAN) used to say true for -1 and 'true'
// (finding 2). The unset row carries NO value in either column: the
// nullable export must not have persisted false (finding 1). Bound bools
// travel in Int16Items/TextItems, so this is checked on the raw record.
func assertBoundBoolImages(ctx context.Context, t *testing.T, env *Env, label string, schema SchemaRef, rows boundBoolRows) {
	t.Helper()
	res := mustQuery(ctx, t, env, Query{Schema: schema, Limit: 100})
	if !res.Plan.Routing.UseDuckDB {
		t.Fatalf("%s: unfiltered read did not route to duckdb: %+v", label, res.Plan.Routing)
	}
	images := map[uuid.UUID]boundBoolImage{}
	for _, rec := range res.Records {
		img := boundBoolImage{}
		img.small, img.smallSet = rec.Int16Items["smallint_01"]
		img.text, img.textSet = rec.TextItems["text_02"]
		images[rec.RowID] = img
	}
	set := func(small int16, text string) boundBoolImage {
		return boundBoolImage{small: small, smallSet: true, text: text, textSet: true}
	}
	for _, tc := range []struct {
		ev   *Event
		want boundBoolImage
	}{
		{rows.trueRow, set(1, "1")},
		{rows.falseRow, set(0, "0")},
		{rows.unset, boundBoolImage{}},
		{rows.negOne, boundBoolImage{small: 0, smallSet: true}},
		{rows.two, boundBoolImage{small: 1, smallSet: true}},
		{rows.textTrue, boundBoolImage{text: "0", textSet: true}},
	} {
		got, ok := images[tc.ev.RowID]
		if !ok {
			t.Fatalf("%s: row %s (%v) missing from the unfiltered read", label, tc.ev.RowID, tc.ev.Attrs["name"])
		}
		if got != tc.want {
			t.Errorf("%s: %v image = %+v, want %+v", label, tc.ev.Attrs["name"], got, tc.want)
		}
	}
}

// TestBoundBoolParityNoEAVAllTiers: name, flagInt (bool_smallint) and
// flagText (bool_text) are all column-bound, so the DuckDB hot leg takes
// BuildPGSelectNoEAV. Before PR #564 that projection emitted the raw column
// and left the outer CAST(attr AS BOOLEAN) to read it (SMALLINT -1 → true,
// VARCHAR 'true' → true), the export wrapped the truthiness in
// CASE ... ELSE FALSE so an unset bool became false once flushed, and the
// outer CAST(attr AS BOOLEAN) could not be scanned into the SMALLINT slot at
// all. All three are pinned here across the unflushed DuckDB hot leg, delta
// parquet and base parquet, and the planted images are additionally probed
// through both hot routes' pushdown (#565).
func TestBoundBoolParityNoEAVAllTiers(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster, WithSchemaDir(writeSimpleSchemaDir(t, boundBoolProps, boundBoolAttrs)))
	simple := DefaultSchemaFixtures()[0]

	// A flushed anchor row gives the schema its first parquet object and
	// manifest; without one the federated read has no S3 leg to glob and
	// fails before it reaches the hot leg under test. The anchor sets neither
	// bool, so it never matches a probe.
	mustApplyEvents(ctx, t, env, "anchor create", CreateEvent(simple, map[string]any{"name": "b-anchor"}))
	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("anchor flush: %v", err)
	}

	rows := seedBoundBoolRows(ctx, t, env, simple)
	hotPG := Query{Schema: simple, PreferHot: true, Limit: 100}
	duck := Query{Schema: simple, Limit: 100}

	// On-contract images: the Postgres route and the unflushed DuckDB hot leg
	// (no-EAV projection, bool pushdown into the Postgres scan) agree.
	runWidthProbes(ctx, t, env, "hot-pg", hotPG, false, onContractProbes(rows))
	runWidthProbes(ctx, t, env, "hot-duck", duck, true, onContractProbes(rows))
	runRejectionProbes(ctx, t, env, "hot-pg", hotPG, boolRejectionProbes())
	runRejectionProbes(ctx, t, env, "hot-duck", duck, boolRejectionProbes())

	// Off-contract images: the unfiltered read derives the truthiness image
	// through mainColBoolExpr, and a filter on the bool pushes the #565
	// BETWEEN range into entity_main on both hot routes, so the planted 2 is
	// returned by equals:true here exactly as it is once flushed.
	plantOffContract(ctx, t, env, simple, rows)
	runWidthProbes(ctx, t, env, "hot-pg-planted", hotPG, false, parquetProbes(rows))
	runWidthProbes(ctx, t, env, "hot-duck-planted", duck, true, parquetProbes(rows))
	assertBoundBoolImages(ctx, t, env, "hot-duck", simple, rows)

	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	runWidthProbes(ctx, t, env, "warm-duck", duck, true, parquetProbes(rows))
	runRejectionProbes(ctx, t, env, "warm-duck", duck, boolRejectionProbes())
	assertBoundBoolImages(ctx, t, env, "warm-duck", simple, rows)

	if _, err := env.RunCompaction(ctx, simple); err != nil {
		t.Fatalf("compaction: %v", err)
	}
	runWidthProbes(ctx, t, env, "cold-duck", duck, true, parquetProbes(rows))
	runRejectionProbes(ctx, t, env, "cold-duck", duck, boolRejectionProbes())
	assertBoundBoolImages(ctx, t, env, "cold-duck", simple, rows)
}
