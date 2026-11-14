//go:build e2e

package production

import (
	"context"
	"testing"
)

// This file is the #709 acceptance. A date/datetime bound to a text column
// with the rfc3339 encoding holds only canonical RFC3339 images when written
// through the API, so a string that is not a timestamp is planted here by
// direct SQL, standing in for legacy or hand-written data. Every parquet
// tier reads such a string as NULL: the CDC export reads the column through
// TRY_CAST (#555). The comparison probes pin that a comparison answers the
// same on both parquet tiers. While the rows are hot, the federated route
// filters and reads them on PostgreSQL through the OLTP reader (design.md
// §6.1, What a hot row reads as), so it must answer as the PostgreSQL
// route does, refusals included; a non-date image is outside the query
// contract (#763), so that answer need not be the parquet tiers'.

// rfc3339NonDateRows is the seeded row set: an anchor flushed before the
// others, a row holding canonical images, two rows whose images are replaced
// by strings that are not timestamps, and a row with both dates unset.
type rfc3339NonDateRows struct {
	anchor, canonical, garbled, empty, unset *Event
}

func seedRFC3339NonDateRows(ctx context.Context, t *testing.T, env *Env, withNote bool) rfc3339NonDateRows {
	t.Helper()
	simple := DefaultSchemaFixtures()[0]
	create := func(name string, extra map[string]any) *Event {
		attrs := map[string]any{"name": name}
		for k, v := range extra {
			attrs[k] = v
		}
		if withNote {
			attrs["note"] = "n-" + name
		}
		ev := CreateEvent(simple, attrs)
		mustApplyEvents(ctx, t, env, "create "+name, ev)
		return ev
	}
	dated := map[string]any{"seenAt": rfc3339Early, "bornOn": rfc3339Day}

	// The anchor gives the schema its first parquet object and manifest, so
	// the federated read has an S3 leg to glob. It sets neither date.
	rows := rfc3339NonDateRows{anchor: create("nd-anchor", nil)}
	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("anchor flush: %v", err)
	}
	rows.canonical = create("nd-canonical", dated)
	rows.garbled = create("nd-garbled", dated)
	rows.empty = create("nd-empty", dated)
	rows.unset = create("nd-unset", nil)

	plant := func(ev *Event, seenAt, bornOn string) {
		env.ExecSQL(ctx, "UPDATE "+env.Tables.EntityMain+" SET text_02 = $3, text_03 = $4"+
			" WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2", simple.ID, ev.RowID, seenAt, bornOn)
	}
	plant(rows.garbled, "not a date", "2024-13-45")
	plant(rows.empty, "", "")
	return rows
}

// rfc3339NonDateProbes is the truth table every parquet tier must reach: a
// non-timestamp string is absent, so it matches a null test and no
// comparison.
func rfc3339NonDateProbes(rows rfc3339NonDateRows) []widthProbe {
	absent := []*Event{rows.anchor, rows.garbled, rows.empty, rows.unset}
	present := []*Event{rows.canonical}
	return []widthProbe{
		{"seenAt_is_null", Filter{Attr: "seenAt", Op: "is_null", Value: "true"}, absent},
		{"seenAt_is_not_null", Filter{Attr: "seenAt", Op: "is_not_null", Value: "true"}, present},
		{"bornOn_is_null", Filter{Attr: "bornOn", Op: "is_null", Value: "true"}, absent},
		{"bornOn_is_not_null", Filter{Attr: "bornOn", Op: "is_not_null", Value: "true"}, present},
		// "not a date" sorts after every canonical image and "" before it,
		// so each passes one of these raw-column pre-filters.
		{"seenAt_gte_floor", Filter{Attr: "seenAt", Op: "gte", Value: rfc3339Floor}, present},
		{"seenAt_lte_ceiling", Filter{Attr: "seenAt", Op: "lte", Value: rfc3339Ceiling}, present},
		{"seenAt_not_equals", Filter{Attr: "seenAt", Op: "not_equals", Value: rfc3339Late}, present},
		{"bornOn_gte_floor", Filter{Attr: "bornOn", Op: "gte", Value: rfc3339Floor}, present},
	}
}

// runRFC3339NonDateAllTiers holds the unflushed rows' federated answers to
// the PostgreSQL route's, then walks them through delta parquet and base
// parquet, with the same expected answers on each.
func runRFC3339NonDateAllTiers(ctx context.Context, t *testing.T, env *Env, withNote bool) {
	t.Helper()
	simple := DefaultSchemaFixtures()[0]
	rows := seedRFC3339NonDateRows(ctx, t, env, withNote)
	duck := Query{Schema: simple, Limit: 100}

	requireProbesAgree(ctx, t, env, "hot", duck, rfc3339NonDateProbes(rows))

	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	runWidthProbes(ctx, t, env, "warm-duck", duck, true, rfc3339NonDateProbes(rows))

	if _, err := env.RunCompaction(ctx, simple); err != nil {
		t.Fatalf("compaction: %v", err)
	}
	runWidthProbes(ctx, t, env, "cold-duck", duck, true, rfc3339NonDateProbes(rows))
}

// TestBoundRFC3339NonDateStringNoEAVAllTiers: every attribute is
// column-bound, so the parquet tiers take the no-EAV projection.
func TestBoundRFC3339NonDateStringNoEAVAllTiers(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(writeSimpleSchemaDir(t, rfc3339Props, rfc3339Attrs)))
	runRFC3339NonDateAllTiers(ctx, t, env, false)
}

// TestBoundRFC3339NonDateStringEAVJoinedAllTiers: the EAV-only note forces
// the EAV-joined projection, where the bound dates sit in a COALESCE with
// the EAV pivot.
func TestBoundRFC3339NonDateStringEAVJoinedAllTiers(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(writeSimpleSchemaDir(t, rfc3339MixedProps, rfc3339MixedAttrs)))
	runRFC3339NonDateAllTiers(ctx, t, env, true)
}
