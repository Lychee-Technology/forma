//go:build e2e

package production

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/transform"
)

// This file is the #756 and #755 acceptance. A datetime bound to a text
// column with the rfc3339 encoding holds only canonical UTC images when
// written through the API, so the images below are planted by direct SQL,
// standing in for legacy or hand-written data. The DuckDB tiers read the
// column with a TIMESTAMP cast that dropped a UTC offset, so an offset image
// read as its wall clock in UTC there while the OLTP read applied the offset
// (#756). The pg-main pushdown compared the raw text against the canonical
// image, so an unflushed fraction, offset or space-separated image could be
// dropped by a comparison its parquet copy matched (#755). Both are pinned
// on the unflushed DuckDB hot leg, delta parquet and base parquet, through
// the production DuckDB reader and CDC exporter and their session settings.

// The planted images and the instant each names. offset, negOffset, upper
// and lower are RFC3339, so the OLTP read parses them; spaced is not, so a
// Get refuses it while every DuckDB tier reads it as UTC. upper and lower
// keep a four-digit year, but their offset carries the instant past it
// (10000-01-01T00:30:00Z, -0001-12-31T23:30:00Z), which the DuckDB outer
// select spelled with a year rfc3339.Parse refuses, failing the page.
const (
	rfc3339Canonical = "2024-01-02T03:04:05.000Z"
	rfc3339Offset    = "2024-01-02T11:04:06+08:00"
	rfc3339NegOffset = "2024-01-01T22:04:04-05:00"
	rfc3339Fraction  = "2024-01-02T03:04:05.5Z"
	rfc3339Spaced    = "2024-01-02 03:04:06"
	rfc3339Upper     = "9999-12-31T23:30:00-01:00"
	rfc3339Lower     = "0000-01-01T00:30:00+01:00"
)

// rfc3339OffsetRows is the seeded row set: an anchor flushed before the
// others, a row keeping its canonical image, one row per planted image, and
// a row with the datetime unset.
type rfc3339OffsetRows struct {
	anchor, canonical, offset, negOffset, fraction, spaced, upper, lower, unset *Event
}

func seedRFC3339OffsetRows(ctx context.Context, t *testing.T, env *Env, withNote bool) rfc3339OffsetRows {
	t.Helper()
	simple := DefaultSchemaFixtures()[0]
	create := func(name string, dated bool) *Event {
		attrs := map[string]any{"name": name}
		if dated {
			attrs["seenAt"] = rfc3339Canonical
		}
		if withNote {
			attrs["note"] = "n-" + name
		}
		ev := CreateEvent(simple, attrs)
		mustApplyEvents(ctx, t, env, "create "+name, ev)
		return ev
	}

	// The anchor gives the schema its first parquet object and manifest. It
	// sets no date.
	rows := rfc3339OffsetRows{anchor: create("off-anchor", false)}
	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("anchor flush: %v", err)
	}
	rows.canonical = create("off-canonical", true)
	rows.offset = create("off-offset", true)
	rows.negOffset = create("off-neg-offset", true)
	rows.fraction = create("off-fraction", true)
	rows.spaced = create("off-spaced", true)
	rows.upper = create("off-upper", true)
	rows.lower = create("off-lower", true)
	rows.unset = create("off-unset", false)

	plant := func(ev *Event, image string) {
		env.ExecSQL(ctx, "UPDATE "+env.Tables.EntityMain+" SET text_02 = $3"+
			" WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2", simple.ID, ev.RowID, image)
	}
	plant(rows.offset, rfc3339Offset)
	plant(rows.negOffset, rfc3339NegOffset)
	plant(rows.fraction, rfc3339Fraction)
	plant(rows.spaced, rfc3339Spaced)
	plant(rows.upper, rfc3339Upper)
	plant(rows.lower, rfc3339Lower)
	return rows
}

// rfc3339OffsetProbes orders the rows by instant: lower in year -1, then on
// 2024-01-02 negOffset 03:04:04Z, canonical 03:04:05Z, fraction
// 03:04:05.5Z, offset and spaced 03:04:06Z, then upper in year 10000.
// Every DuckDB tier must reach it; the unset row matches no comparison.
func rfc3339OffsetProbes(rows rfc3339OffsetRows) []widthProbe {
	const at4, at5, at6 = "2024-01-02T03:04:04Z", "2024-01-02T03:04:05Z", "2024-01-02T03:04:06Z"
	return []widthProbe{
		{"eq_offset_instant", Filter{Attr: "seenAt", Op: "equals", Value: at6}, []*Event{rows.offset, rows.spaced}},
		{"eq_offset_literal", Filter{Attr: "seenAt", Op: "equals", Value: rfc3339Offset}, []*Event{rows.offset, rows.spaced}},
		{"eq_neg_offset_instant", Filter{Attr: "seenAt", Op: "equals", Value: at4}, []*Event{rows.negOffset}},
		{"gt", Filter{Attr: "seenAt", Op: "gt", Value: at5}, []*Event{rows.fraction, rows.offset, rows.spaced, rows.upper}},
		{"gte", Filter{Attr: "seenAt", Op: "gte", Value: at5}, []*Event{rows.canonical, rows.fraction, rows.offset, rows.spaced, rows.upper}},
		{"lt", Filter{Attr: "seenAt", Op: "lt", Value: at5}, []*Event{rows.negOffset, rows.lower}},
		{"lte", Filter{Attr: "seenAt", Op: "lte", Value: at5}, []*Event{rows.canonical, rows.negOffset, rows.lower}},
		{"not_equals", Filter{Attr: "seenAt", Op: "not_equals", Value: at5}, []*Event{rows.fraction, rows.offset, rows.negOffset, rows.spaced, rows.upper, rows.lower}},
	}
}

// assertRFC3339OffsetReads pins #756's value: the instant the OLTP read
// (a Get) decodes from each RFC 3339 image equals the instant a DuckDB read
// of the whole set decodes. The DuckDB outer select renders the millisecond
// since #842, so the fraction row reads its instant too. The decoded time.Time is read
// as is, not through jsonNormalizedAttrs: time.MarshalJSON refuses the
// upper and lower instants' years (the API renders them as epoch ms, #591).
func assertRFC3339OffsetReads(ctx context.Context, t *testing.T, env *Env, label string, rows rfc3339OffsetRows) {
	t.Helper()
	simple := DefaultSchemaFixtures()[0]
	res := mustQuery(ctx, t, env, Query{Schema: simple, Limit: 100})
	if !res.Plan.Routing.UseDuckDB {
		t.Fatalf("%s: unfiltered read did not route to duckdb: %+v", label, res.Plan.Routing)
	}
	transformer := transform.NewPersistentRecordTransformer(env.Registry)
	duck := map[uuid.UUID]int64{}
	for _, rec := range res.Records {
		attrs, err := transformer.FromPersistentRecord(ctx, rec)
		if err != nil {
			t.Fatalf("%s: decode row %s: %v", label, rec.RowID, err)
		}
		switch v := attrs["seenAt"].(type) {
		case nil:
		case time.Time:
			duck[rec.RowID] = v.UnixMilli()
		default:
			t.Fatalf("%s: decode row %s: seenAt = %v (%T), want a time.Time", label, rec.RowID, v, v)
		}
	}
	for _, ev := range []*Event{rows.canonical, rows.offset, rows.negOffset, rows.fraction, rows.upper, rows.lower} {
		got, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: simple.Name, RowID: &ev.RowID})
		if err != nil {
			t.Fatalf("%s: get %v: %v", label, ev.Attrs["name"], err)
		}
		oltp, ok := got.Attributes["seenAt"].(time.Time)
		if !ok {
			t.Fatalf("%s: get %v: seenAt = %v (%T), want a time.Time", label, ev.Attrs["name"], got.Attributes["seenAt"], got.Attributes["seenAt"])
		}
		if ms, ok := duck[ev.RowID]; !ok || ms != oltp.UnixMilli() {
			t.Errorf("%s: %v reads %d on DuckDB (present %t), %d (%s) on OLTP",
				label, ev.Attrs["name"], ms, ok, oltp.UnixMilli(), oltp.UTC().Format(time.RFC3339Nano))
		}
	}
	if ms, want := duck[rows.spaced.RowID], time.Date(2024, 1, 2, 3, 4, 6, 0, time.UTC).UnixMilli(); ms != want {
		t.Errorf("%s: the offset-less image reads %d on DuckDB, want UTC %d", label, ms, want)
	}
}

// runRFC3339OffsetAllTiers walks the planted rows through the unflushed
// DuckDB hot leg, delta parquet and base parquet, with the same reads and
// probe answers on each.
func runRFC3339OffsetAllTiers(ctx context.Context, t *testing.T, env *Env, withNote bool) {
	t.Helper()
	simple := DefaultSchemaFixtures()[0]
	rows := seedRFC3339OffsetRows(ctx, t, env, withNote)
	duck := Query{Schema: simple, Limit: 100}
	check := func(label string) {
		assertRFC3339OffsetReads(ctx, t, env, label, rows)
		runWidthProbes(ctx, t, env, label, duck, true, rfc3339OffsetProbes(rows))
	}

	check("hot-duck")
	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	check("warm-duck")
	if _, err := env.RunCompaction(ctx, simple); err != nil {
		t.Fatalf("compaction: %v", err)
	}
	check("cold-duck")
}

// TestBoundRFC3339OffsetImageNoEAVAllTiers: every attribute is
// column-bound, so the DuckDB hot leg takes BuildPGSelectNoEAV.
func TestBoundRFC3339OffsetImageNoEAVAllTiers(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(writeSimpleSchemaDir(t, rfc3339Props, rfc3339Attrs)))
	runRFC3339OffsetAllTiers(ctx, t, env, false)
}

// TestBoundRFC3339OffsetImageEAVJoinedAllTiers: the EAV-only note forces
// the EAV-joined hot projection, where the bound datetime sits in a
// COALESCE with the EAV pivot.
func TestBoundRFC3339OffsetImageEAVJoinedAllTiers(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(writeSimpleSchemaDir(t, rfc3339MixedProps, rfc3339MixedAttrs)))
	runRFC3339OffsetAllTiers(ctx, t, env, true)
}
