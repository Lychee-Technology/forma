//go:build e2e

package production

import (
	"context"
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
	if got := parquetKeys(report.NewObjects); len(got) != 3 {
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

// parquetKeys filters an object-key diff down to parquet files: the first
// manifest write also shows up in NewObjects.
func parquetKeys(keys []string) []string {
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
