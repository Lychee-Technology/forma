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

// TestLostRecentFlushStillFailsInsideTheGraceMargin (#720 review): a flush
// marked inside the #252 grace margin published its manifest before the
// query resolved its paths, so losing that manifest is as much a loss as
// losing an old one. The read must fail at once, not only after the margin
// expires, and the hot-excluded shape must agree.
func TestLostRecentFlushStillFailsInsideTheGraceMargin(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	env.DuckCfg.FlushVisibilityGraceMs = 60_000
	wide := DefaultSchemaFixtures()[1]

	if err := env.ApplyEvents(ctx, env.GenerateScript(ScriptSpec{Schema: wide, Creates: 3})...); err != nil {
		t.Fatalf("apply events: %v", err)
	}
	flush := mustFlush(ctx, t, env)
	if len(flush.NewObjects) < 2 {
		t.Fatalf("precondition: first flush wrote %v, want a parquet object and the manifest", flush.NewObjects)
	}
	for _, key := range flush.NewObjects {
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

// TestFirstFlushAfterTheEmptyScanCheckStaysHot (#720 review): the empty scan
// source is fixed once the check clears the schema, so it cannot read the
// delta a first flush publishes afterwards. With the #252 widening disabled
// the strict barrier would then drop the flushed rows from the hot leg as
// well. The empty scan's hot leg serves at the check's anchor instead.
func TestFirstFlushAfterTheEmptyScanCheckStaysHot(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	env.DuckCfg.FlushVisibilityGraceMs = -1
	wide := DefaultSchemaFixtures()[1]

	racer := &flushAfterEmptyScanCheck{env: env}
	env.DirtyIDFetcherWrap = func(f fedengine.DirtyIDFetcher) fedengine.DirtyIDFetcher {
		racer.DirtyIDFetcher = f
		return racer
	}
	if err := env.ApplyEvents(ctx, env.GenerateScript(ScriptSpec{Schema: wide, Creates: 3})...); err != nil {
		t.Fatalf("apply events: %v", err)
	}

	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20, PreferredTiers: allTiers})
	if racer.flush == nil {
		t.Fatalf("the query never cleared the empty-scan check, so no flush raced it (flush error: %v)", racer.err)
	}
	if racer.flush.UnflushedAfter != 0 || len(racer.flush.NewObjects) == 0 {
		t.Fatalf("the racing flush wrote %v and left %d rows unflushed, want every row flushed",
			racer.flush.NewObjects, racer.flush.UnflushedAfter)
	}
}

// flushAfterEmptyScanCheck runs one real CDC flush right after the engine's
// first empty-scan check clears the schema, before DuckDB reads it.
type flushAfterEmptyScanCheck struct {
	fedengine.DirtyIDFetcher
	env   *Env
	flush *FlushReport
	err   error
}

func (r *flushAfterEmptyScanCheck) HasRowsOnlyInParquet(ctx context.Context, tables model.StorageTables, schemaID int16, cutoffMs int64) (bool, error) {
	found, err := r.DirtyIDFetcher.HasRowsOnlyInParquet(ctx, tables, schemaID, cutoffMs)
	if err != nil || found || r.flush != nil || r.err != nil {
		return found, err
	}
	r.flush, r.err = r.env.RunFlush(ctx)
	return found, err
}
