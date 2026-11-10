//go:build e2e

package production

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/iso8601"
	"github.com/lychee-technology/forma/internal/manifest"
)

// This file is the #763 regression for the canonical iso8601 storage
// contract (docs/iso8601-storage-contract.md). The PostgreSQL route compares
// an iso8601-bound column as text against the literal's canonical image and
// sorts it as text; the DuckDB tiers compare and sort the instant. The
// contract is that for every image Forma's writers store the two agree,
// because the canonical image's text order is its instant order. Here every
// image is written through the API, and every filter and sort must answer
// by instant on both routes and on every tier: the PostgreSQL route (which
// reads PostgreSQL whatever has been flushed) and the DuckDB route over the
// unflushed hot leg, delta parquet and base parquet. A non-canonical image
// is outside the contract and is not exercised here; see
// date_iso8601_offset_e2e_test.go for what DuckDB does with one.

// canonicalRow is one seeded row: the value written for each date attribute
// (empty leaves it unset) and the instant it names.
type canonicalRow struct {
	name, seenAt, bornOn string
	ev                   *Event
	seenMs, bornMs       int64
}

// canonicalRowSpecs place the instants so that each field of the image
// decides an order somewhere: the year's first digit and a 0999 → 1000
// carry, month and day carries (09 → 10), and hour, minute and second steps
// within one day. The offset row is #763's example, written through the API:
// it names 03:04:06Z and must be stored as that canonical image. bornOn runs
// in a different order from seenAt, so a sort on the wrong column shows.
var canonicalRowSpecs = []canonicalRow{
	{name: "c-floor", seenAt: "0000-01-01T00:00:00Z", bornOn: "2024-01-10"},
	{name: "c-0999", seenAt: "0999-12-31T23:59:59Z", bornOn: "9999-12-31"},
	{name: "c-1000", seenAt: "1000-01-01T00:00:00Z", bornOn: "0000-01-01"},
	{name: "c-sec5", seenAt: "2024-01-02T03:04:05Z", bornOn: "2024-09-30"},
	{name: "c-offset", seenAt: "2024-01-02T11:04:06+08:00", bornOn: "0999-12-31"},
	{name: "c-min5", seenAt: "2024-01-02T03:05:00Z", bornOn: "2024-10-01"},
	{name: "c-h09", seenAt: "2024-01-02T09:59:59Z", bornOn: "1000-01-01"},
	{name: "c-h10", seenAt: "2024-01-02T10:00:00Z", bornOn: "2024-01-09"},
	{name: "c-sep30", seenAt: "2024-09-30T23:59:59Z", bornOn: "2025-01-01"},
	{name: "c-oct01", seenAt: "2024-10-01T00:00:00Z", bornOn: "2024-12-31"},
	{name: "c-ceiling", seenAt: "9999-12-31T23:59:59Z", bornOn: "2024-01-02"},
	{name: "c-unset"},
}

// canonicalProbeLiterals are the filter literals per attribute: instants
// that equal a stored image, one between two (03:00:00Z), and the two other
// literal spellings a filter accepts, an offset and unix millis, each
// naming a stored instant.
var canonicalProbeLiterals = map[string][]string{
	"seenAt": {
		"1000-01-01T00:00:00Z",
		"2024-01-02T03:04:06Z",
		"2024-01-02T05:00:00+02:00",
		"2024-01-02T19:59:59+10:00",
		strconv.FormatInt(time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), 10),
	},
	"bornOn": {"0999-12-31T00:00:00Z", "2024-01-10T00:00:00Z", "2024-09-30T00:00:00Z"},
}

var canonicalProbeOps = []string{"equals", "not_equals", "gt", "gte", "lt", "lte"}

func mustInstantMillis(t *testing.T, literal string) int64 {
	t.Helper()
	if ms, err := strconv.ParseInt(literal, 10, 64); err == nil {
		return ms
	}
	if len(literal) == len("2006-01-02") {
		literal += "T00:00:00Z"
	}
	parsed, err := time.Parse(time.RFC3339, literal)
	if err != nil {
		t.Fatalf("parse %q: %v", literal, err)
	}
	return parsed.UnixMilli()
}

func seedCanonicalRows(ctx context.Context, t *testing.T, env *Env, withNote bool) []*canonicalRow {
	t.Helper()
	simple := DefaultSchemaFixtures()[0]
	rows := make([]*canonicalRow, 0, len(canonicalRowSpecs))
	events := make([]*Event, 0, len(canonicalRowSpecs))
	for _, spec := range canonicalRowSpecs {
		row := spec
		attrs := map[string]any{"name": row.name}
		if row.seenAt != "" {
			attrs["seenAt"] = row.seenAt
			row.seenMs = mustInstantMillis(t, row.seenAt)
		}
		if row.bornOn != "" {
			attrs["bornOn"] = row.bornOn
			row.bornMs = mustInstantMillis(t, row.bornOn)
		}
		if withNote {
			attrs["note"] = "n-" + row.name
		}
		row.ev = CreateEvent(simple, attrs)
		rows = append(rows, &row)
		events = append(events, row.ev)
	}
	mustApplyEvents(ctx, t, env, "canonical iso8601 creates", events...)
	return rows
}

// instantOf is a row's instant for attr, or false when the row leaves it
// unset.
func (r *canonicalRow) instantOf(attr string) (int64, bool) {
	if attr == "seenAt" {
		return r.seenMs, r.seenAt != ""
	}
	return r.bornMs, r.bornOn != ""
}

// canonicalProbes derives each probe's answer by comparing instants. A row
// that leaves the attribute unset matches no comparison, not_equals
// included.
func canonicalProbes(t *testing.T, rows []*canonicalRow) []widthProbe {
	t.Helper()
	compare := map[string]func(a, b int64) bool{
		"equals":     func(a, b int64) bool { return a == b },
		"not_equals": func(a, b int64) bool { return a != b },
		"gt":         func(a, b int64) bool { return a > b },
		"gte":        func(a, b int64) bool { return a >= b },
		"lt":         func(a, b int64) bool { return a < b },
		"lte":        func(a, b int64) bool { return a <= b },
	}
	var probes []widthProbe
	for _, attr := range []string{"seenAt", "bornOn"} {
		for _, literal := range canonicalProbeLiterals[attr] {
			operand := mustInstantMillis(t, literal)
			for _, op := range canonicalProbeOps {
				var want []*Event
				for _, row := range rows {
					if ms, set := row.instantOf(attr); set && compare[op](ms, operand) {
						want = append(want, row.ev)
					}
				}
				probes = append(probes, widthProbe{
					name:   fmt.Sprintf("%s_%s_%s", attr, op, literal),
					filter: Filter{Attr: attr, Op: op, Value: literal},
					want:   want,
				})
			}
		}
	}
	return probes
}

// assertCanonicalSorts pins the exact order of the rows that set the
// attribute under an ascending and a descending sort, which is their
// instant order. No two of them share an instant, so the order is total.
// The unset row's NULL placement is engine policy and is not compared.
func assertCanonicalSorts(ctx context.Context, t *testing.T, env *Env, label string, base Query, wantDuck bool, rows []*canonicalRow) {
	t.Helper()
	for _, attr := range []string{"seenAt", "bornOn"} {
		var set []*canonicalRow
		for _, row := range rows {
			if _, ok := row.instantOf(attr); ok {
				set = append(set, row)
			}
		}
		sort.Slice(set, func(i, j int) bool {
			a, _ := set[i].instantOf(attr)
			b, _ := set[j].instantOf(attr)
			return a < b
		})
		for _, desc := range []bool{false, true} {
			want := make([]string, 0, len(set))
			for i := range set {
				row := set[i]
				if desc {
					row = set[len(set)-1-i]
				}
				want = append(want, row.name)
			}
			q := base
			q.Sorts = []Sort{{Attr: attr, Desc: desc}}
			res := mustQuery(ctx, t, env, q)
			if got := res.Plan.Routing.UseDuckDB; got != wantDuck {
				t.Errorf("%s/sort %s desc=%t: UseDuckDB = %t, want %t", label, attr, desc, got, wantDuck)
			}
			names := map[uuid.UUID]string{}
			for _, row := range set {
				names[row.ev.RowID] = row.name
			}
			got := make([]string, 0, len(set))
			for _, rec := range res.Records {
				if name, ok := names[rec.RowID]; ok {
					got = append(got, name)
				}
			}
			if fmt.Sprint(got) != fmt.Sprint(want) {
				t.Errorf("%s/sort %s desc=%t:\n got %v\nwant %v", label, attr, desc, got, want)
			}
		}
	}
}

// assertCanonicalImages pins the storage half of the contract on the
// PostgreSQL route: each stored image is iso8601.Image of the instant
// written, whatever spelling the write used.
func assertCanonicalImages(ctx context.Context, t *testing.T, env *Env, rows []*canonicalRow) {
	t.Helper()
	res := mustQuery(ctx, t, env, Query{Schema: DefaultSchemaFixtures()[0], PreferHot: true, Limit: 100})
	byID := map[uuid.UUID]map[string]string{}
	for _, rec := range res.Records {
		byID[rec.RowID] = rec.TextItems
	}
	for _, row := range rows {
		items, ok := byID[row.ev.RowID]
		if !ok {
			t.Fatalf("stored images: %s missing from the PostgreSQL read", row.name)
		}
		for attr, column := range map[string]string{"seenAt": "text_02", "bornOn": "text_03"} {
			got, stored := items[column]
			ms, set := row.instantOf(attr)
			if !set {
				if stored {
					t.Errorf("stored images: %s.%s = %q, want unset", row.name, attr, got)
				}
				continue
			}
			want, rule := iso8601.Image(ms)
			if rule != "" {
				t.Fatalf("stored images: %s.%s: %s", row.name, attr, rule)
			}
			if got != want {
				t.Errorf("stored images: %s.%s = %q (stored %t), want the canonical %q", row.name, attr, got, stored, want)
			}
		}
	}
}

// runCanonicalContractAllTiers seeds the rows and checks every probe and
// sort on both routes before the flush (hot), after it (delta) and after a
// compaction rewrite has merged them into a new base file (base).
func runCanonicalContractAllTiers(ctx context.Context, t *testing.T, env *Env, withNote bool) {
	t.Helper()
	simple := DefaultSchemaFixtures()[0]

	// A flushed anchor row, promoted to base, gives the schema a base file
	// before the rows under test are written: the DuckDB route has an S3 leg
	// from the start, and the flushed rows later make the schema dirty enough
	// for a real rewrite. The anchor sets neither date.
	seedCanonicalAnchorBase(ctx, t, env, simple)

	rows := seedCanonicalRows(ctx, t, env, withNote)
	assertCanonicalImages(ctx, t, env, rows)
	probes := canonicalProbes(t, rows)
	pg := Query{Schema: simple, PreferHot: true, Limit: 100}
	duck := Query{Schema: simple, Limit: 100}
	check := func(tier string) {
		runWidthProbes(ctx, t, env, tier+"-pg", pg, false, probes)
		assertCanonicalSorts(ctx, t, env, tier+"-pg", pg, false, rows)
		runWidthProbes(ctx, t, env, tier+"-duck", duck, true, probes)
		assertCanonicalSorts(ctx, t, env, tier+"-duck", duck, true, rows)
	}

	check("hot")
	mustFlush(ctx, t, env)
	if got := countTier(loadSchemaManifest(ctx, t, env, simple), "delta"); got == 0 {
		t.Fatal("flush listed no delta entry; the warm stage would not read delta")
	}
	check("warm")
	rewriteCanonicalRowsToBase(ctx, t, env, simple, len(rows)+1)
	check("cold")
}

// seedCanonicalAnchorBase writes and flushes the anchor row and promotes its
// delta file to base with a one-byte threshold (the default 256 MB would
// leave this KB-scale tier alone).
func seedCanonicalAnchorBase(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) {
	t.Helper()
	mustApplyEvents(ctx, t, env, "anchor create", CreateEvent(schema, map[string]any{"name": "c-anchor"}))
	mustFlush(ctx, t, env)
	result, err := env.RunCompactionWith(ctx, schema, CompactionOverrides{TargetBaseSizeBytes: 1})
	if err != nil {
		t.Fatalf("anchor promotion: %v", err)
	}
	if result.Outcome != compaction.PromotionApplied {
		t.Fatalf("anchor promotion outcome = %s, want %s", result.Outcome, compaction.PromotionApplied)
	}
	m := loadSchemaManifest(ctx, t, env, schema)
	if base, delta := countTier(m, "base"), countTier(m, "delta"); base != 1 || delta != 0 {
		t.Fatalf("after anchor promotion: base = %d, delta = %d, want 1 and 0", base, delta)
	}
}

// rewriteCanonicalRowsToBase runs a default compaction pass, which must
// rewrite the anchor base and the rows' delta into one new base file
// (wantRows rows: the anchor and every seeded row), so the cold stage reads
// images the merge engine wrote, from the base tier only.
func rewriteCanonicalRowsToBase(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, wantRows int) {
	t.Helper()
	result, err := env.RunCompaction(ctx, schema)
	if err != nil {
		t.Fatalf("compaction: %v", err)
	}
	if result.Outcome != compaction.RewriteApplied {
		t.Fatalf("compaction outcome = %s (dirty ratio %.2f), want %s", result.Outcome, result.DirtyRatio, compaction.RewriteApplied)
	}
	if result.RowsOut != int64(wantRows) {
		t.Errorf("rewrite RowsOut = %d, want %d", result.RowsOut, wantRows)
	}
	m := loadSchemaManifest(ctx, t, env, schema)
	if base, delta := countTier(m, "base"), countTier(m, "delta"); base != 1 || delta != 0 {
		t.Fatalf("after rewrite: base = %d, delta = %d, want 1 and 0", base, delta)
	}
	if got := manifest.FilterByTier(m, "base")[0].Path; result.NewBaseKey == "" || got != result.NewBaseKey {
		t.Errorf("base entry %q is not the rewrite's new base %q", got, result.NewBaseKey)
	}
}

// TestCanonicalISO8601ContractNoEAVAllTiers: every attribute is
// column-bound, so the DuckDB hot leg takes BuildPGSelectNoEAV.
func TestCanonicalISO8601ContractNoEAVAllTiers(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(writeSimpleSchemaDir(t, iso8601Props, iso8601Attrs)))
	runCanonicalContractAllTiers(ctx, t, env, false)
}

// TestCanonicalISO8601ContractEAVJoinedAllTiers: the EAV-only note forces
// the EAV-joined hot projection, where the bound dates sit in a COALESCE
// with the EAV pivot.
func TestCanonicalISO8601ContractEAVJoinedAllTiers(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t), WithSchemaDir(writeSimpleSchemaDir(t, iso8601MixedProps, iso8601MixedAttrs)))
	runCanonicalContractAllTiers(ctx, t, env, true)
}
