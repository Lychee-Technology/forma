//go:build e2e

package production

import (
	"context"
	"testing"
)

// This file is the #709 acceptance. A date/datetime bound to a text column
// with the iso8601 encoding holds only canonical RFC3339 images when written
// through the API, so a string that is not a timestamp is planted here by
// direct SQL, standing in for legacy or hand-written data. Every DuckDB tier
// reads such a string as NULL: the hot leg and the CDC export both read the
// column through TRY_CAST (#555). The pg-main pushdown's null test compared
// the raw column, which is not NULL, so is_null dropped the unflushed row
// from pg_source and matched it once its parquet copy served it. The
// comparison probes pin that a comparison answers the same on every tier
// too: the raw-column pre-filter may keep such a row, and `visible`, which
// reads NULL, drops it. The OLTP route is not exercised here.

// iso8601NonDateRows is the seeded row set: an anchor flushed before the
// others, a row holding canonical images, two rows whose images are replaced
// by strings that are not timestamps, and a row with both dates unset.
type iso8601NonDateRows struct {
	anchor, canonical, garbled, empty, unset *Event
}

func seedISO8601NonDateRows(ctx context.Context, t *testing.T, env *Env, withNote bool) iso8601NonDateRows {
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
	dated := map[string]any{"seenAt": iso8601Early, "bornOn": iso8601Day}

	// The anchor gives the schema its first parquet object and manifest, so
	// the federated read has an S3 leg to glob. It sets neither date.
	rows := iso8601NonDateRows{anchor: create("nd-anchor", nil)}
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

// iso8601NonDateProbes is the truth table every DuckDB tier must reach: a
// non-timestamp string is absent, so it matches a null test and no
// comparison.
func iso8601NonDateProbes(rows iso8601NonDateRows) []widthProbe {
	absent := []*Event{rows.anchor, rows.garbled, rows.empty, rows.unset}
	present := []*Event{rows.canonical}
	return []widthProbe{
		{"seenAt_is_null", Filter{Attr: "seenAt", Op: "is_null", Value: "true"}, absent},
		{"seenAt_is_not_null", Filter{Attr: "seenAt", Op: "is_not_null", Value: "true"}, present},
		{"bornOn_is_null", Filter{Attr: "bornOn", Op: "is_null", Value: "true"}, absent},
		{"bornOn_is_not_null", Filter{Attr: "bornOn", Op: "is_not_null", Value: "true"}, present},
		// "not a date" sorts after every canonical image and "" before it,
		// so each passes one of these raw-column pre-filters.
		{"seenAt_gte_floor", Filter{Attr: "seenAt", Op: "gte", Value: iso8601Floor}, present},
		{"seenAt_lte_ceiling", Filter{Attr: "seenAt", Op: "lte", Value: iso8601Ceiling}, present},
		{"seenAt_not_equals", Filter{Attr: "seenAt", Op: "not_equals", Value: iso8601Late}, present},
		{"bornOn_gte_floor", Filter{Attr: "bornOn", Op: "gte", Value: iso8601Floor}, present},
	}
}

// runISO8601NonDateAllTiers walks the planted rows through the unflushed
// DuckDB hot leg, delta parquet and base parquet, with the same expected
// answers on each.
func runISO8601NonDateAllTiers(ctx context.Context, t *testing.T, env *Env, withNote bool) {
	t.Helper()
	simple := DefaultSchemaFixtures()[0]
	rows := seedISO8601NonDateRows(ctx, t, env, withNote)
	duck := Query{Schema: simple, Limit: 100}

	runWidthProbes(ctx, t, env, "hot-duck", duck, true, iso8601NonDateProbes(rows))

	if _, err := env.RunFlush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	runWidthProbes(ctx, t, env, "warm-duck", duck, true, iso8601NonDateProbes(rows))

	if _, err := env.RunCompaction(ctx, simple); err != nil {
		t.Fatalf("compaction: %v", err)
	}
	runWidthProbes(ctx, t, env, "cold-duck", duck, true, iso8601NonDateProbes(rows))
}

// TestBoundISO8601NonDateStringNoEAVAllTiers: every attribute is
// column-bound, so the DuckDB hot leg takes BuildPGSelectNoEAV.
func TestBoundISO8601NonDateStringNoEAVAllTiers(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(writeSimpleSchemaDir(t, iso8601Props, iso8601Attrs)))
	runISO8601NonDateAllTiers(ctx, t, env, false)
}

// TestBoundISO8601NonDateStringEAVJoinedAllTiers: the EAV-only note forces
// the EAV-joined hot projection, where the bound dates sit in a COALESCE
// with the EAV pivot.
func TestBoundISO8601NonDateStringEAVJoinedAllTiers(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(writeSimpleSchemaDir(t, iso8601MixedProps, iso8601MixedAttrs)))
	runISO8601NonDateAllTiers(ctx, t, env, true)
}
