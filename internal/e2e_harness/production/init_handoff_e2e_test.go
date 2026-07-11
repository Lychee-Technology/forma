//go:build e2e

package production

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/manifest"
)

// TestInitBaseOnlyParity (#176 scenario 1): a multi-batch cdc-init backfill
// produces a complete base tier, and after the onboarding contract clears
// change_log (base parquet becomes the rows' only source) the federated
// result matches the independent oracle exactly.
func TestInitBaseOnlyParity(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	// TargetFileSizeMB is 0 in the harness config, so BatchSize drives init
	// batching directly: 20 rows / batch 7 -> base files of 7+7+6 rows.
	env.CDC.BatchSize = 7

	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 20})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}

	report, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("run init: %v", err)
	}
	if report.RowsExported != 20 || report.FilesCreated != 3 {
		t.Fatalf("init exported %d rows in %d files, want 20 rows in 3 files", report.RowsExported, report.FilesCreated)
	}
	if got := filterParquetKeys(report.NewObjects); len(got) != 3 {
		t.Fatalf("init created parquet objects %v, want exactly 3", got)
	}
	if entries := manifest.FilterByTier(report.Manifest, "base"); len(entries) != 3 {
		t.Fatalf("manifest holds %d base entries, want 3", len(entries))
	}
	assertBaseRows(ctx, t, env, report.Manifest, 20)

	// Onboarding contract (mirrors production cdc-init handoff): clearing the
	// change_log makes the base parquet the ONLY source for these rows.
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID)

	result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	if result != nil && len(result.Records) != 20 {
		t.Fatalf("base-only federated result has %d rows, want 20", len(result.Records))
	}
	env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "title", Value: creates[5].Attrs["title"].(string)}},
		Limit:   10,
	})
	env.AssertQueryMatches(ctx, Query{
		Schema: wide,
		Sorts:  []Sort{{Attr: "count", Desc: true}},
		Limit:  10,
	})
}

// filterParquetKeys filters an object-key diff down to parquet files: the
// first manifest write also shows up in NewObjects.
func filterParquetKeys(keys []string) []string {
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		if strings.HasSuffix(k, ".parquet") && !strings.Contains(k, "/_tmp/") {
			out = append(out, k)
		}
	}
	return out
}

// assertBaseRows sums the physical row counts of every base-tier parquet in
// the manifest and compares against want.
func assertBaseRows(ctx context.Context, t *testing.T, env *Env, m *manifest.Manifest, want int64) {
	t.Helper()
	if m == nil {
		t.Fatal("nil manifest")
	}
	var got int64
	for _, f := range manifest.FilterByTier(m, "base") {
		got += parquetRowCount(ctx, t, env, f.Path)
	}
	if got != want {
		t.Fatalf("base tier holds %d physical rows, want %d", got, want)
	}
}

// TestInitThenIncrementalFlush (#176 scenario 2): backfill, clear the log
// (onboarding contract), then updates to backfilled rows plus new creates
// flush into delta. Federated count is exact (no duplicates, no gaps) and
// the delta version wins over the base version where the tiers overlap.
func TestInitThenIncrementalFlush(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 20})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}
	report, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("run init: %v", err)
	}
	if report.RowsExported != 20 {
		t.Fatalf("init exported %d rows, want 20", report.RowsExported)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID)

	// Incremental phase: update 5 backfilled rows (base holds v1, delta will
	// hold v2 -> genuine cross-tier overlap) and create 10 new rows.
	var incr []*Event
	for i := 0; i < 5; i++ {
		incr = append(incr, UpdateEvent(wide, creates[i].RowID, map[string]any{
			"title": fmt.Sprintf("handoff-%02d", i),
			"count": float64(300000 + i),
		}))
	}
	late := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 10})
	incr = append(incr, late...)
	if err := env.ApplyEvents(ctx, incr...); err != nil {
		t.Fatalf("apply incremental events: %v", err)
	}

	flush, err := env.RunFlush(ctx)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if flush.UnflushedAfter != 0 {
		t.Fatalf("flush left %d unflushed rows", flush.UnflushedAfter)
	}
	if len(manifest.FilterByTier(flush.Manifests[wide.ID], "delta")) == 0 {
		t.Fatal("no delta entries in manifest after incremental flush")
	}

	// Exact count: 20 backfilled + 10 incremental; the 5 updated rows must
	// not appear twice despite living in both base and delta.
	result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	if result != nil && len(result.Records) != 30 {
		t.Fatalf("federated result has %d rows, want 30 (20 base + 10 delta, overlap deduped)", len(result.Records))
	}
	// Latest version wins: the oracle expects exactly one row per updated
	// title, and its attribute values are the post-update ones.
	env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "title", Value: "handoff-03"}},
		Limit:   10,
	})
	env.AssertQueryMatches(ctx, Query{
		Schema:  wide,
		Filters: []Filter{{Attr: "count", Op: "gte", Value: "300000"}},
		Limit:   100,
	})
}

// TestInitFlushOverlapWithoutLogCleanup (#176 scenario 2 variant): nothing
// enforces the onboarding cleanup. If the operator skips it, the first flush
// re-exports every backfilled row into delta at the same version, and
// merge-on-read must still dedupe to an exact count.
func TestInitFlushOverlapWithoutLogCleanup(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 15})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}
	report, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("run init: %v", err)
	}
	if report.RowsExported != 15 {
		t.Fatalf("init exported %d rows, want 15", report.RowsExported)
	}

	// No change_log cleanup: the flush re-exports all 15 rows into delta.
	flush, err := env.RunFlush(ctx)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if flush.UnflushedAfter != 0 {
		t.Fatalf("flush left %d unflushed rows", flush.UnflushedAfter)
	}
	var deltaRows int64
	for _, f := range manifest.FilterByTier(flush.Manifests[wide.ID], "delta") {
		deltaRows += parquetRowCount(ctx, t, env, f.Path)
	}
	if deltaRows != 15 {
		t.Fatalf("delta tier holds %d rows, want 15 (full re-export of the backfilled set)", deltaRows)
	}
	assertBaseRows(ctx, t, env, flush.Manifests[wide.ID], 15)

	// Same 15 rows in base AND delta at identical versions: the federated
	// result must contain each exactly once.
	result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	if result != nil && len(result.Records) != 15 {
		t.Fatalf("federated result has %d rows, want 15 (base/delta overlap must dedupe)", len(result.Records))
	}
}
