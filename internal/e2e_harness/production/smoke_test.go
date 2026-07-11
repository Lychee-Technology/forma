//go:build e2e

package production

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// TestProductionSmoke is the harness's own proof (#173): a seeded script
// drives the real EntityManager, cdc.RunInit builds the cold tier,
// cdc.Runner flushes the warm tier, later writes stay hot, and every query
// is checked against the independent oracle.
//
// Tier layout built below (e2e_wide, 30 seeded creates):
//   - cold-only: creates[0:10] — exported to the base file by RunInit, then
//     their change_log entries are truncated via ExecSQL to model the
//     production onboarding contract (base bootstrap + log cleanup). Their
//     ONLY source is the base parquet.
//   - warm: creates[10:15] updated + creates[19:30] + lateCreates — all
//     flushed into delta parquet; the dirty set is empty for them.
//   - deleted: creates[15:17] — tombstones flushed to the delta.
//   - hot: creates[17:19] updated after the flush + hotCreates — unflushed,
//     served from Postgres via the dirty set.
func TestProductionSmoke(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 30})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}

	initReport, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("run init: %v", err)
	}
	if initReport.RowsExported != 30 || initReport.FilesCreated == 0 {
		t.Fatalf("init exported %d rows in %d files, want 30 rows in >=1 file", initReport.RowsExported, initReport.FilesCreated)
	}

	// Onboarding contract: the base export covers creates[0:10]; clearing
	// their change_log entries makes the base file their only source.
	coldIDs := rowIDs(creates[0:10])
	env.ExecSQL(ctx,
		"DELETE FROM change_log WHERE schema_id = $1 AND row_id = ANY($2)",
		wide.ID, coldIDs)

	// Mutations that will become the warm tier.
	var warmEvents []*Event
	for i := 10; i < 15; i++ {
		warmEvents = append(warmEvents, UpdateEvent(wide, creates[i].RowID, map[string]any{
			"title": fmt.Sprintf("updated-%02d", i),
			"count": float64(100000 + i),
			"note":  fmt.Sprintf("warm update %d", i),
		}))
	}
	warmEvents = append(warmEvents,
		DeleteEvent(wide, creates[15].RowID),
		DeleteEvent(wide, creates[16].RowID),
	)
	lateCreates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 5})
	warmEvents = append(warmEvents, lateCreates...)
	if err := env.ApplyEvents(ctx, warmEvents...); err != nil {
		t.Fatalf("apply warm mutations: %v", err)
	}

	// Dry-run immutability: parquet may be written, but flushed_at and the
	// manifest must not change.
	dry, err := env.RunFlushDry(ctx)
	if err != nil {
		t.Fatalf("dry-run flush: %v", err)
	}
	if dry.UnflushedBefore != dry.UnflushedAfter {
		t.Fatalf("dry-run changed unflushed count: %d -> %d", dry.UnflushedBefore, dry.UnflushedAfter)
	}
	if m := dry.Manifests[wide.ID]; m != nil && countTier(m, "delta") > 0 {
		t.Fatal("dry-run added delta entries to the manifest")
	}

	flush, err := env.RunFlush(ctx)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if flush.UnflushedAfter != 0 {
		t.Fatalf("flush left %d unflushed rows", flush.UnflushedAfter)
	}

	// Post-flush hot activity.
	var hotEvents []*Event
	for i := 17; i < 19; i++ {
		hotEvents = append(hotEvents, UpdateEvent(wide, creates[i].RowID, map[string]any{
			"title": fmt.Sprintf("hot-%02d", i),
			"count": float64(200000 + i),
		}))
	}
	hotCreates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 3})
	hotEvents = append(hotEvents, hotCreates...)
	if err := env.ApplyEvents(ctx, hotEvents...); err != nil {
		t.Fatalf("apply hot mutations: %v", err)
	}

	assertTierHits(ctx, t, env, wide, creates, lateCreates, hotCreates)
	runSmokeQueries(ctx, t, env, wide, creates)
}

// assertTierHits guards against the glob-mismatch "both sides empty" false
// green: base and delta parquet must each hold rows, and the federated
// result must contain rows whose only source is the cold tier, the warm
// tier, and the hot tier respectively.
func assertTierHits(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, creates, lateCreates, hotCreates []*Event) {
	t.Helper()

	manifests, err := env.loadManifests(ctx)
	if err != nil {
		t.Fatalf("load manifests: %v", err)
	}
	m := manifests[wide.ID]
	if m == nil {
		t.Fatal("no manifest for e2e_wide")
	}
	baseRows, deltaRows := int64(0), int64(0)
	for _, f := range m.Files {
		count := parquetRowCount(ctx, t, env, f.Path)
		switch f.Tier {
		case "base":
			baseRows += count
		case "delta":
			deltaRows += count
		}
	}
	if baseRows == 0 {
		t.Fatal("cold tier is empty: base parquet holds no rows")
	}
	if deltaRows == 0 {
		t.Fatal("warm tier is empty: delta parquet holds no rows")
	}

	result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	if result == nil {
		return
	}
	got := make(map[uuid.UUID]bool, len(result.Records))
	for _, rec := range result.Records {
		got[rec.RowID] = true
	}
	coldHits := countHits(got, creates[0:10])
	warmHits := countHits(got, creates[19:30]) + countHits(got, lateCreates)
	hotHits := countHits(got, hotCreates)
	if coldHits == 0 {
		t.Error("zero cold-tier hits: no base-only rows in the federated result")
	}
	if warmHits == 0 {
		t.Error("zero warm-tier hits: no delta-backed rows in the federated result")
	}
	if hotHits == 0 {
		t.Error("zero hot-tier hits: no unflushed rows in the federated result")
	}
	for _, deleted := range creates[15:17] {
		if got[deleted.RowID] {
			t.Errorf("deleted row %s is visible", deleted.RowID)
		}
	}
	t.Logf("tier hits: cold=%d warm=%d hot=%d (base rows=%d delta rows=%d)", coldHits, warmHits, hotHits, baseRows, deltaRows)
}

// runSmokeQueries is the oracle-checked query battery: unfiltered,
// equality, range, sorted, page 2, date filters (main-column-bound and
// EAV-only) across all three tiers, and PreferHot.
func runSmokeQueries(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, creates []*Event) {
	t.Helper()

	env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "title", Value: creates[3].Attrs["title"].(string)}},
		Limit:   10,
	})
	env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "count", Op: "gte", Value: "100"}, {Attr: "count", Op: "lt", Value: "100010"}},
		Limit:   100,
	})
	env.AssertQueryMatches(ctx, Query{
		Schema: wide,
		Sorts:  []Sort{{Attr: "count", Desc: true}},
		Limit:  10,
	})
	env.AssertQueryMatches(ctx, Query{
		Schema: wide,
		Sorts:  []Sort{{Attr: "count"}},
		Limit:  8,
		Offset: 8, // page 2
	})
	// Sort on the main-column-bound datetime attribute (#194).
	env.AssertQueryMatches(ctx, Query{
		Schema: wide,
		Sorts:  []Sort{{Attr: "touched"}},
		Limit:  10,
	})
	// Date predicates (#200): the DuckClause binds epoch-ms BIGINT params.
	// Values must be RFC3339 or epoch-ms — the engine's date parser rejects
	// bare "2006-01-02" literals (the oracle alone accepts them).
	// Range on the main-column-bound (unix_ms, bigint_02) date attribute.
	res := env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "joined", Op: "gte", Value: "1999-01-01T00:00:00Z"}},
		Limit:   100,
	})
	if res != nil && len(res.Records) == 0 {
		t.Error("joined date-range filter matched zero rows; want a non-empty subset")
	}
	// Equality + range on the EAV-only date attribute. creates[3] is a live
	// cold-tier row the smoke flow never mutates, so its born value must hit.
	bornEq := creates[3].Attrs["born"].(string) + "T00:00:00Z"
	res = env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "born", Value: bornEq}},
		Limit:   10,
	})
	if res != nil && len(res.Records) == 0 {
		t.Error("born equality filter matched zero rows; creates[3] is live in the cold tier")
	}
	env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "born", Op: "lt", Value: "2005-06-15T00:00:00Z"}},
		Limit:   100,
	})
	env.AssertQueryMatches(ctx, Query{Schema: wide, PreferHot: true, Limit: 100})
}

func rowIDs(events []*Event) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(events))
	for _, ev := range events {
		ids = append(ids, ev.RowID)
	}
	return ids
}

func countHits(got map[uuid.UUID]bool, events []*Event) int {
	n := 0
	for _, ev := range events {
		if got[ev.RowID] {
			n++
		}
	}
	return n
}

func parquetRowCount(ctx context.Context, t *testing.T, env *Env, key string) int64 {
	t.Helper()
	path := fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, strings.TrimPrefix(key, "/"))
	var count int64
	if err := env.Duck.DB.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", path)).Scan(&count); err != nil {
		t.Fatalf("count parquet rows in %s: %v", key, err)
	}
	return count
}
