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
// text column with the iso8601 encoding. schemameta.ValidateColumnBinding
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

const iso8601Props = `{
    "name": { "type": "string" },
    "seenAt": { "type": "string", "format": "date-time" },
    "bornOn": { "type": "string", "format": "date" }
  }`

const iso8601Attrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "seenAt": { "attributeID": 2, "valueType": "datetime", "column_binding": { "col_name": "text_02", "encoding": "iso8601" } },
  "bornOn": { "attributeID": 3, "valueType": "date", "column_binding": { "col_name": "text_03", "encoding": "iso8601" } }
}
`

// The mixed generation adds an EAV-only attribute so the hot leg takes the
// EAV-joined projection instead of the no-EAV shortcut.
const iso8601MixedProps = `{
    "name": { "type": "string" },
    "seenAt": { "type": "string", "format": "date-time" },
    "bornOn": { "type": "string", "format": "date" },
    "note": { "type": "string" }
  }`

const iso8601MixedAttrs = `{
  "name": { "attributeID": 1, "valueType": "text", "column_binding": { "col_name": "text_01" } },
  "seenAt": { "attributeID": 2, "valueType": "datetime", "column_binding": { "col_name": "text_02", "encoding": "iso8601" } },
  "bornOn": { "attributeID": 3, "valueType": "date", "column_binding": { "col_name": "text_03", "encoding": "iso8601" } },
  "note": { "attributeID": 4, "valueType": "text" }
}
`

const (
	iso8601Early = "2024-01-02T03:04:05Z"
	iso8601Late  = "2025-06-07T08:09:10Z"
	iso8601Day   = "2024-01-02"
	iso8601DayTS = "2024-01-02T00:00:00Z"
)

// iso8601Rows is the seeded row set: two distinct instants and one row that
// leaves both date attributes unset.
type iso8601Rows struct {
	early, late, unset *Event
}

func seedISO8601Rows(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, withNote bool) iso8601Rows {
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
	rows := iso8601Rows{
		early: CreateEvent(schema, attrs("d-early", map[string]any{"seenAt": iso8601Early, "bornOn": iso8601Day})),
		late:  CreateEvent(schema, attrs("d-late", map[string]any{"seenAt": iso8601Late, "bornOn": "2025-06-07"})),
		unset: CreateEvent(schema, attrs("d-unset", nil)),
	}
	mustApplyEvents(ctx, t, env, "iso8601 creates", rows.early, rows.late, rows.unset)
	return rows
}

// iso8601Probes is the truth table every route must reach: the filter
// operand is parsed once (conditionexpr.ParseRFC3339OrUnixMs) and compared
// against the RFC3339 string on the Postgres routes and the epoch-ms verdict
// on the DuckDB legs. The unset row matches nothing.
func iso8601Probes(rows iso8601Rows) []widthProbe {
	return []widthProbe{
		{"seenAt_eq", Filter{Attr: "seenAt", Op: "equals", Value: iso8601Early}, []*Event{rows.early}},
		{"seenAt_gt", Filter{Attr: "seenAt", Op: "gt", Value: iso8601Early}, []*Event{rows.late}},
		{"seenAt_lt", Filter{Attr: "seenAt", Op: "lt", Value: iso8601Late}, []*Event{rows.early}},
		{"seenAt_gte", Filter{Attr: "seenAt", Op: "gte", Value: iso8601Early}, []*Event{rows.early, rows.late}},
		{"bornOn_eq", Filter{Attr: "bornOn", Op: "equals", Value: iso8601DayTS}, []*Event{rows.early}},
		{"bornOn_gt", Filter{Attr: "bornOn", Op: "gt", Value: iso8601DayTS}, []*Event{rows.late}},
	}
}

// assertISO8601Sort pins that a sort on the datetime orders the set rows by
// instant on the given route; the unset row's NULL placement is engine
// policy and is not compared.
func assertISO8601Sort(ctx context.Context, t *testing.T, env *Env, label string, base Query, wantDuck bool, rows iso8601Rows) {
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
	latePos, lateOK := pos[rows.late.RowID]
	earlyPos, earlyOK := pos[rows.early.RowID]
	if !lateOK || !earlyOK {
		t.Fatalf("%s/sort: set rows missing from the sorted read (late %t, early %t)", label, lateOK, earlyOK)
	}
	if latePos > earlyPos {
		t.Errorf("%s/sort: seenAt DESC placed late at %d after early at %d", label, latePos, earlyPos)
	}
}

// iso8601Image is what one record carries in the two bound text columns and
// what the read funnel decodes them to.
type iso8601Image struct {
	seenAt, bornOn   string
	seenSet, bornSet bool
	seenMs, bornMs   int64
	seenDec, bornDec bool
}

// assertISO8601Images reads the whole row set through DuckDB and pins both
// the physical image each record carries (the RFC3339 string the write path
// stores, so a DuckDB read is byte-identical to a Postgres read) and the
// epoch-ms the read funnel decodes it to. The unset row carries no value.
func assertISO8601Images(ctx context.Context, t *testing.T, env *Env, label string, schema SchemaRef, rows iso8601Rows) {
	t.Helper()
	res := mustQuery(ctx, t, env, Query{Schema: schema, Limit: 100})
	if !res.Plan.Routing.UseDuckDB {
		t.Fatalf("%s: unfiltered read did not route to duckdb: %+v", label, res.Plan.Routing)
	}
	transformer := transform.NewPersistentRecordTransformer(env.Registry)
	images := map[uuid.UUID]iso8601Image{}
	for _, rec := range res.Records {
		img := iso8601Image{}
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
		want iso8601Image
	}{
		{rows.early, iso8601ImageOf(t, iso8601Early, iso8601DayTS)},
		{rows.late, iso8601ImageOf(t, iso8601Late, "2025-06-07T00:00:00Z")},
		{rows.unset, iso8601Image{}},
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

// iso8601ImageOf is the expected image of a row whose two dates were written
// as the given RFC3339 instants: the strings themselves in the text columns
// and their epoch-ms out of the read funnel.
func iso8601ImageOf(t *testing.T, seenAt, bornOn string) iso8601Image {
	t.Helper()
	seen, err := time.Parse(time.RFC3339, seenAt)
	if err != nil {
		t.Fatalf("parse %q: %v", seenAt, err)
	}
	born, err := time.Parse(time.RFC3339, bornOn)
	if err != nil {
		t.Fatalf("parse %q: %v", bornOn, err)
	}
	return iso8601Image{
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

// runISO8601AllTiers seeds the rows under the Env's schema generation and
// walks them through the unflushed DuckDB hot leg, delta parquet and base
// parquet, checking the truth table, the sort and the stored image on each.
func runISO8601AllTiers(ctx context.Context, t *testing.T, env *Env, withNote bool) {
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

	rows := seedISO8601Rows(ctx, t, env, simple, withNote)
	hotPG := Query{Schema: simple, PreferHot: true, Limit: 100}
	duck := Query{Schema: simple, Limit: 100}

	runWidthProbes(ctx, t, env, "hot-pg", hotPG, false, iso8601Probes(rows))
	assertISO8601Sort(ctx, t, env, "hot-pg", hotPG, false, rows)
	runWidthProbes(ctx, t, env, "hot-duck", duck, true, iso8601Probes(rows))
	assertISO8601Sort(ctx, t, env, "hot-duck", duck, true, rows)
	assertISO8601Images(ctx, t, env, "hot-duck", simple, rows)

	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	runWidthProbes(ctx, t, env, "warm-duck", duck, true, iso8601Probes(rows))
	assertISO8601Sort(ctx, t, env, "warm-duck", duck, true, rows)
	assertISO8601Images(ctx, t, env, "warm-duck", simple, rows)

	if _, err := env.RunCompaction(ctx, simple); err != nil {
		t.Fatalf("compaction: %v", err)
	}
	runWidthProbes(ctx, t, env, "cold-duck", duck, true, iso8601Probes(rows))
	assertISO8601Sort(ctx, t, env, "cold-duck", duck, true, rows)
	assertISO8601Images(ctx, t, env, "cold-duck", simple, rows)
}

// TestBoundISO8601DateParityNoEAVAllTiers: name, seenAt and bornOn are all
// column-bound, so the DuckDB hot leg takes BuildPGSelectNoEAV.
func TestBoundISO8601DateParityNoEAVAllTiers(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster, WithSchemaDir(writeSimpleSchemaDir(t, iso8601Props, iso8601Attrs)))
	runISO8601AllTiers(ctx, t, env, false)
}

// TestBoundISO8601DateParityEAVJoinedAllTiers: the EAV-only note attribute
// forces the EAV-joined hot projection, where the bound dates sit in a
// COALESCE with the pivot's epoch-ms BIGINT.
func TestBoundISO8601DateParityEAVJoinedAllTiers(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster, WithSchemaDir(writeSimpleSchemaDir(t, iso8601MixedProps, iso8601MixedAttrs)))
	runISO8601AllTiers(ctx, t, env, true)
}
