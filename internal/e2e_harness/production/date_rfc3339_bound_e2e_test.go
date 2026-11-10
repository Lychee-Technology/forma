//go:build e2e

package production

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/transform"
)

// This file is the #555 acceptance for a date/datetime attribute bound to a
// text column with the rfc3339 encoding. schemameta.ValidateColumnBinding
// admits the pair (#459), the write path stores the RFC3339 string
// (transform.storeWithEncoding), the Postgres route parses it back and the
// CDC export normalises it to epoch-ms BIGINT (cdc.castDateMainValue, #219).
// The DuckDB reader did neither end: the hot leg projected the raw VARCHAR
// into a COALESCE with the pivot's BIGINT (a binder error on the EAV-joined
// projection, a VARCHAR in the UNION ALL on the no-EAV one), and the outer
// select cast the unified epoch-ms back to BIGINT under the text column's
// alias, so a DuckDB read on ANY tier handed transform.readWithEncoding
// "1704164645000" to time.Parse. Both ends are pinned here on the unflushed
// DuckDB hot leg, delta parquet and base parquet, for both hot projections:
// a schema whose attributes are all column-bound takes BuildPGSelectNoEAV,
// and one with an EAV-only attribute takes buildPGProjection.

const rfc3339Props = `{
    "name": { "type": "string" },
    "seenAt": { "type": "string", "format": "date-time" },
    "bornOn": { "type": "string", "format": "date" }
  }`

const rfc3339Attrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "seenAt": { "attributeID": 2, "valueType": "datetime", "column_binding": { "col_name": "text_02", "encoding": "rfc3339" } },
  "bornOn": { "attributeID": 3, "valueType": "date", "column_binding": { "col_name": "text_03", "encoding": "rfc3339" } }
}
`

// The mixed generation adds an EAV-only attribute so the hot leg takes the
// EAV-joined projection instead of the no-EAV shortcut.
const rfc3339MixedProps = `{
    "name": { "type": "string" },
    "seenAt": { "type": "string", "format": "date-time" },
    "bornOn": { "type": "string", "format": "date" },
    "note": { "type": "string" }
  }`

const rfc3339MixedAttrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "seenAt": { "attributeID": 2, "valueType": "datetime", "column_binding": { "col_name": "text_02", "encoding": "rfc3339" } },
  "bornOn": { "attributeID": 3, "valueType": "date", "column_binding": { "col_name": "text_03", "encoding": "rfc3339" } },
  "note": { "attributeID": 4, "valueType": "text" }
}
`

// Each instant is written as its canonical image (#842), so the image a
// read carries back is the string written.
const (
	rfc3339Early = "2024-01-02T03:04:05.250Z"
	rfc3339Late  = "2025-06-07T08:09:10.000Z"
	rfc3339Day   = "2024-01-02"
	rfc3339DayTS = "2024-01-02T00:00:00.000Z"
	// The ends of the encoding's contract (#582, #842): the first and last
	// millisecond the RFC 3339 four-digit year can name. Both must survive
	// every tier byte-identical and decode to the same instant.
	rfc3339Floor     = "0000-01-01T00:00:00.000Z"
	rfc3339FloorDay  = "0000-01-01"
	rfc3339Ceiling   = "9999-12-31T23:59:59.999Z"
	rfc3339CeilDay   = "9999-12-31"
	rfc3339CeilDayTS = "9999-12-31T00:00:00.000Z"
)

// rfc3339Rows is the seeded row set: two distinct instants, the floor and
// ceiling of the encoding's range, and one row that leaves both date
// attributes unset.
type rfc3339Rows struct {
	early, late, floor, ceiling, unset *Event
}

func seedRFC3339Rows(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, withNote bool) rfc3339Rows {
	t.Helper()
	attrs := func(name string, extra map[string]any) map[string]any {
		m := map[string]any{"name": name}
		for k, v := range extra {
			m[k] = v
		}
		if withNote {
			m["note"] = "n-" + name
		}
		return m
	}
	rows := rfc3339Rows{
		early:   CreateEvent(schema, attrs("d-early", map[string]any{"seenAt": rfc3339Early, "bornOn": rfc3339Day})),
		late:    CreateEvent(schema, attrs("d-late", map[string]any{"seenAt": rfc3339Late, "bornOn": "2025-06-07"})),
		floor:   CreateEvent(schema, attrs("d-floor", map[string]any{"seenAt": rfc3339Floor, "bornOn": rfc3339FloorDay})),
		ceiling: CreateEvent(schema, attrs("d-ceiling", map[string]any{"seenAt": rfc3339Ceiling, "bornOn": rfc3339CeilDay})),
		unset:   CreateEvent(schema, attrs("d-unset", nil)),
	}
	mustApplyEvents(ctx, t, env, "rfc3339 creates", rows.early, rows.late, rows.floor, rows.ceiling, rows.unset)
	return rows
}

// rfc3339Probes is the truth table every route must reach: the filter
// operand is parsed once (conditionexpr.ParseRFC3339OrUnixMs) and compared
// against the RFC3339 string on the Postgres routes and the epoch-ms verdict
// on the DuckDB legs. The unset row matches nothing.
func rfc3339Probes(rows rfc3339Rows) []widthProbe {
	return []widthProbe{
		{"seenAt_eq", Filter{Attr: "seenAt", Op: "equals", Value: rfc3339Early}, []*Event{rows.early}},
		{"seenAt_gt", Filter{Attr: "seenAt", Op: "gt", Value: rfc3339Early}, []*Event{rows.late, rows.ceiling}},
		{"seenAt_lt", Filter{Attr: "seenAt", Op: "lt", Value: rfc3339Late}, []*Event{rows.early, rows.floor}},
		{"seenAt_gte", Filter{Attr: "seenAt", Op: "gte", Value: rfc3339Early}, []*Event{rows.early, rows.late, rows.ceiling}},
		{"seenAt_floor_eq", Filter{Attr: "seenAt", Op: "equals", Value: rfc3339Floor}, []*Event{rows.floor}},
		{"seenAt_floor_lte", Filter{Attr: "seenAt", Op: "lte", Value: rfc3339Floor}, []*Event{rows.floor}},
		{"seenAt_ceiling_eq", Filter{Attr: "seenAt", Op: "equals", Value: rfc3339Ceiling}, []*Event{rows.ceiling}},
		{"seenAt_ceiling_gte", Filter{Attr: "seenAt", Op: "gte", Value: rfc3339Ceiling}, []*Event{rows.ceiling}},
		{"bornOn_eq", Filter{Attr: "bornOn", Op: "equals", Value: rfc3339DayTS}, []*Event{rows.early}},
		{"bornOn_gt", Filter{Attr: "bornOn", Op: "gt", Value: rfc3339DayTS}, []*Event{rows.late, rows.ceiling}},
		{"bornOn_floor_eq", Filter{Attr: "bornOn", Op: "equals", Value: rfc3339Floor}, []*Event{rows.floor}},
		{"bornOn_ceiling_eq", Filter{Attr: "bornOn", Op: "equals", Value: rfc3339CeilDayTS}, []*Event{rows.ceiling}},
	}
}

// assertRFC3339Sort pins that a sort on the datetime orders the set rows by
// instant on the given route; the unset row's NULL placement is engine
// policy and is not compared.
func assertRFC3339Sort(ctx context.Context, t *testing.T, env *Env, label string, base Query, wantDuck bool, rows rfc3339Rows) {
	t.Helper()
	q := base
	q.Sorts = []Sort{{Attr: "seenAt", Desc: true}}
	res := mustQuery(ctx, t, env, q)
	if got := res.Plan.Routing.UseDuckDB; got != wantDuck {
		t.Errorf("%s/sort: UseDuckDB = %t, want %t (routing %+v)", label, got, wantDuck, res.Plan.Routing)
	}
	pos := map[uuid.UUID]int{}
	for i, rec := range res.Records {
		pos[rec.RowID] = i
	}
	order := []*Event{rows.ceiling, rows.late, rows.early, rows.floor}
	for i := 1; i < len(order); i++ {
		prevPos, prevOK := pos[order[i-1].RowID]
		curPos, curOK := pos[order[i].RowID]
		if !prevOK || !curOK {
			t.Fatalf("%s/sort: set rows missing from the sorted read (%v %t, %v %t)", label, order[i-1].Attrs["name"], prevOK, order[i].Attrs["name"], curOK)
		}
		if prevPos > curPos {
			t.Errorf("%s/sort: seenAt DESC placed %v at %d after %v at %d", label, order[i-1].Attrs["name"], prevPos, order[i].Attrs["name"], curPos)
		}
	}
}

// rfc3339Image is what one record carries in the two bound text columns and
// what the read funnel decodes them to.
type rfc3339Image struct {
	seenAt, bornOn   string
	seenSet, bornSet bool
	seenMs, bornMs   int64
	seenDec, bornDec bool
}

// assertRFC3339Images reads the whole row set through DuckDB and pins both
// the physical image each record carries (the RFC3339 string the write path
// stores, so a DuckDB read is byte-identical to a Postgres read) and the
// epoch-ms the read funnel decodes it to. The unset row carries no value.
func assertRFC3339Images(ctx context.Context, t *testing.T, env *Env, label string, schema SchemaRef, rows rfc3339Rows) {
	t.Helper()
	res := mustQuery(ctx, t, env, Query{Schema: schema, Limit: 100})
	if !res.Plan.Routing.UseDuckDB {
		t.Fatalf("%s: unfiltered read did not route to duckdb: %+v", label, res.Plan.Routing)
	}
	transformer := transform.NewPersistentRecordTransformer(env.Registry)
	images := map[uuid.UUID]rfc3339Image{}
	for _, rec := range res.Records {
		img := rfc3339Image{}
		img.seenAt, img.seenSet = rec.TextItems["text_02"]
		img.bornOn, img.bornSet = rec.TextItems["text_03"]
		attrs, err := transformer.FromPersistentRecord(ctx, rec)
		if err != nil {
			t.Fatalf("%s: decode row %s: %v", label, rec.RowID, err)
		}
		norm := jsonNormalizedAttrs(t, attrs)
		img.seenMs, img.seenDec = decodedMillis(t, label, norm, "seenAt")
		img.bornMs, img.bornDec = decodedMillis(t, label, norm, "bornOn")
		images[rec.RowID] = img
	}
	for _, tc := range []struct {
		ev   *Event
		want rfc3339Image
	}{
		{rows.early, rfc3339ImageOf(t, rfc3339Early, rfc3339DayTS)},
		{rows.late, rfc3339ImageOf(t, rfc3339Late, "2025-06-07T00:00:00.000Z")},
		{rows.floor, rfc3339ImageOf(t, rfc3339Floor, rfc3339Floor)},
		{rows.ceiling, rfc3339ImageOf(t, rfc3339Ceiling, rfc3339CeilDayTS)},
		{rows.unset, rfc3339Image{}},
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

// rfc3339ImageOf is the expected image of a row whose two dates were written
// as the given RFC3339 instants: the strings themselves in the text columns
// and their epoch-ms out of the read funnel.
func rfc3339ImageOf(t *testing.T, seenAt, bornOn string) rfc3339Image {
	t.Helper()
	seen, err := time.Parse(time.RFC3339, seenAt)
	if err != nil {
		t.Fatalf("parse %q: %v", seenAt, err)
	}
	born, err := time.Parse(time.RFC3339, bornOn)
	if err != nil {
		t.Fatalf("parse %q: %v", bornOn, err)
	}
	return rfc3339Image{
		seenAt: seenAt, bornOn: bornOn, seenSet: true, bornSet: true,
		seenMs: seen.UnixMilli(), bornMs: born.UnixMilli(), seenDec: true, bornDec: true,
	}
}

// decodedMillis reads one decoded date attribute back to epoch-ms, or false
// when the attribute is absent.
func decodedMillis(t *testing.T, label string, attrs map[string]any, key string) (int64, bool) {
	t.Helper()
	raw, ok := attrs[key]
	if !ok || raw == nil {
		return 0, false
	}
	s, ok := raw.(string)
	if !ok {
		t.Fatalf("%s: decoded %s = %v (%T), want an RFC3339 string", label, key, raw, raw)
	}
	parsed, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("%s: decoded %s = %q: %v", label, key, s, err)
	}
	return parsed.UnixMilli(), true
}

// runRFC3339AllTiers seeds the rows under the Env's schema generation and
// walks them through the unflushed DuckDB hot leg, delta parquet and base
// parquet, checking the truth table, the sort and the stored image on each.
func runRFC3339AllTiers(ctx context.Context, t *testing.T, env *Env, withNote bool) {
	t.Helper()
	simple := DefaultSchemaFixtures()[0]

	// A flushed anchor row gives the schema its first parquet object and
	// manifest; without one the federated read has no S3 leg to glob and
	// fails before it reaches the hot leg under test. The anchor sets
	// neither date, so it never matches a probe.
	mustApplyEvents(ctx, t, env, "anchor create", CreateEvent(simple, map[string]any{"name": "d-anchor"}))
	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("anchor flush: %v", err)
	}

	rows := seedRFC3339Rows(ctx, t, env, simple, withNote)
	hotPG := Query{Schema: simple, PreferHot: true, Limit: 100}
	duck := Query{Schema: simple, Limit: 100}

	runWidthProbes(ctx, t, env, "hot-pg", hotPG, false, rfc3339Probes(rows))
	assertRFC3339Sort(ctx, t, env, "hot-pg", hotPG, false, rows)
	runWidthProbes(ctx, t, env, "hot-duck", duck, true, rfc3339Probes(rows))
	assertRFC3339Sort(ctx, t, env, "hot-duck", duck, true, rows)
	assertRFC3339Images(ctx, t, env, "hot-duck", simple, rows)

	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	runWidthProbes(ctx, t, env, "warm-duck", duck, true, rfc3339Probes(rows))
	assertRFC3339Sort(ctx, t, env, "warm-duck", duck, true, rows)
	assertRFC3339Images(ctx, t, env, "warm-duck", simple, rows)

	if _, err := env.RunCompaction(ctx, simple); err != nil {
		t.Fatalf("compaction: %v", err)
	}
	runWidthProbes(ctx, t, env, "cold-duck", duck, true, rfc3339Probes(rows))
	assertRFC3339Sort(ctx, t, env, "cold-duck", duck, true, rows)
	assertRFC3339Images(ctx, t, env, "cold-duck", simple, rows)
}

// TestBoundRFC3339DateParityNoEAVAllTiers: name, seenAt and bornOn are all
// column-bound, so the DuckDB hot leg takes BuildPGSelectNoEAV.
func TestBoundRFC3339DateParityNoEAVAllTiers(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster, WithSchemaDir(writeSimpleSchemaDir(t, rfc3339Props, rfc3339Attrs)))
	runRFC3339AllTiers(ctx, t, env, false)
}

// TestBoundRFC3339DateParityEAVJoinedAllTiers: the EAV-only note attribute
// forces the EAV-joined hot projection, where the bound dates sit in a
// COALESCE with the pivot's epoch-ms BIGINT.
func TestBoundRFC3339DateParityEAVJoinedAllTiers(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster, WithSchemaDir(writeSimpleSchemaDir(t, rfc3339MixedProps, rfc3339MixedAttrs)))
	runRFC3339AllTiers(ctx, t, env, true)
}
