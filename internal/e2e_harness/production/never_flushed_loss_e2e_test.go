//go:build e2e

package production

import (
	"context"
	"errors"
	"testing"

	fedengine "github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/model"
)

// TestLostInitializedDataStillFails (#720 review): a cdc-init export leaves
// its rows in base parquet with no change_log entry once the onboarding
// handoff clears the log, so no flush history records them. When that
// export's manifest and objects are lost, the empty fallback glob must keep
// failing the read rather than answer from the later hot rows alone.
func TestLostInitializedDataStillFails(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1]

	if err := env.ApplyEvents(ctx, env.GenerateScript(ScriptSpec{Schema: wide, Creates: 3})...); err != nil {
		t.Fatalf("apply events: %v", err)
	}
	report, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("run init: %v", err)
	}
	if report.RowsExported != 3 || len(report.NewObjects) < 2 {
		t.Fatalf("precondition: init exported %d rows into %v, want 3 rows, a parquet object and the manifest",
			report.RowsExported, report.NewObjects)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID)
	for _, key := range report.NewObjects {
		deleteObject(ctx, t, env, key)
	}

	assertLostParquetFailsBothShapes(ctx, t, env, wide)
}

// assertLostParquetFailsBothShapes adds hot rows beside a schema whose
// parquet is lost, then requires both the all-tier and the hot-excluded
// query to fail rather than answer short.
func assertLostParquetFailsBothShapes(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) {
	t.Helper()
	if err := env.ApplyEvents(ctx, env.GenerateScript(ScriptSpec{Schema: schema, Creates: 2})...); err != nil {
		t.Fatalf("apply hot events: %v", err)
	}
	for _, tiers := range [][]model.DataTier{allTiers, parquetTiers} {
		res, err := env.Query(ctx, Query{Schema: schema, Limit: 20, PreferredTiers: tiers})
		if err == nil {
			t.Fatalf("tiers %v: query answered %d rows with the parquet data lost; want a read failure",
				tiers, len(res.Records))
		}
		if !errors.Is(err, fedengine.ErrFederatedReadFailed) {
			t.Fatalf("tiers %v: got %v, want ErrFederatedReadFailed", tiers, err)
		}
	}
}
