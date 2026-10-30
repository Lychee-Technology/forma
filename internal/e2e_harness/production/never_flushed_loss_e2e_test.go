//go:build e2e

package production

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

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

// TestLostFlushStampedAheadOfTheReaderStillFails (#720 review): flushed_at
// comes from the CDC host's clock, which can run ahead of the reader's. A
// completed flush stamped after the reader's "now" is still a prior
// publication, and losing it must fail the read.
func TestLostFlushStampedAheadOfTheReaderStillFails(t *testing.T) {
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
	env.ExecSQL(ctx, "UPDATE change_log SET flushed_at = $2 WHERE schema_id = $1 AND flushed_at <> 0",
		wide.ID, time.Now().UnixMilli()+30_000)
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

// The tests below run a writer between the #720 empty-scan check and the
// DuckDB read, the window where the empty scan source is already fixed. The
// writer publishes and then changes change_log, moving the rows out of the
// hot leg. The recheck after the pass sees the publication and retries, so
// the answer stays complete whatever the CDC host's clock says.

// TestFirstFlushAfterTheEmptyScanCheckStaysComplete: with the #252 widening
// disabled, the strict barrier drops the rows the flush marked from the hot
// leg of the raced pass.
func TestFirstFlushAfterTheEmptyScanCheckStaysComplete(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	env.DuckCfg.FlushVisibilityGraceMs = -1
	runFirstFlushRace(ctx, t, env, func(ctx context.Context, schema SchemaRef) error {
		return expectFlushedEverything(env.RunFlush(ctx))
	})
}

// TestFirstFlushOnASlowClockAfterTheEmptyScanCheckStaysComplete (#720
// review): a CDC host 30 seconds behind the reader stamps the marks below
// any cutoff the reader could pick, so no barrier keeps them hot.
func TestFirstFlushOnASlowClockAfterTheEmptyScanCheckStaysComplete(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	env.DuckCfg.FlushVisibilityGraceMs = 5_000
	runFirstFlushRace(ctx, t, env, func(ctx context.Context, schema SchemaRef) error {
		if err := expectFlushedEverything(env.RunFlush(ctx)); err != nil {
			return err
		}
		env.ExecSQL(ctx, "UPDATE change_log SET flushed_at = $2 WHERE schema_id = $1 AND flushed_at <> 0",
			schema.ID, time.Now().UnixMilli()-30_000)
		return nil
	})
}

// TestInitHandoffAfterTheEmptyScanCheckStaysComplete (#720 review): cdc-init
// publishes base parquet and the onboarding handoff then clears change_log,
// which leaves the hot leg with no entry to serve the rows by.
func TestInitHandoffAfterTheEmptyScanCheckStaysComplete(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	runFirstFlushRace(ctx, t, env, func(ctx context.Context, schema SchemaRef) error {
		report, err := env.RunInit(ctx, schema)
		if err != nil {
			return err
		}
		if report.RowsExported != 3 {
			return fmt.Errorf("init exported %d rows, want 3", report.RowsExported)
		}
		env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", schema.ID)
		return nil
	})
}

// runFirstFlushRace creates three rows on a never-flushed schema, runs
// publish once right after the first empty-scan check clears the schema,
// and requires the query that raced it to match the oracle.
func runFirstFlushRace(ctx context.Context, t *testing.T, env *Env, publish func(context.Context, SchemaRef) error) {
	t.Helper()
	wide := DefaultSchemaFixtures()[1]
	racer := &publishAfterEmptyScanCheck{publish: func(ctx context.Context) error { return publish(ctx, wide) }}
	env.DirtyIDFetcherWrap = func(f fedengine.DirtyIDFetcher) fedengine.DirtyIDFetcher {
		racer.DirtyIDFetcher = f
		return racer
	}
	if err := env.ApplyEvents(ctx, env.GenerateScript(ScriptSpec{Schema: wide, Creates: 3})...); err != nil {
		t.Fatalf("apply events: %v", err)
	}

	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20, PreferredTiers: allTiers})
	if !racer.ran {
		t.Fatal("the query never cleared the empty-scan check, so nothing raced it")
	}
	if racer.err != nil {
		t.Fatalf("the racing writer failed: %v", racer.err)
	}
}

func expectFlushedEverything(report *FlushReport, err error) error {
	if err != nil {
		return err
	}
	if report.UnflushedAfter != 0 || len(report.NewObjects) == 0 {
		return fmt.Errorf("the flush wrote %v and left %d rows unflushed, want every row flushed",
			report.NewObjects, report.UnflushedAfter)
	}
	return nil
}

// publishAfterEmptyScanCheck runs publish once, right after the engine's
// first empty-scan check finds no parquet history, before DuckDB reads.
type publishAfterEmptyScanCheck struct {
	fedengine.DirtyIDFetcher
	publish func(context.Context) error
	ran     bool
	err     error
}

func (r *publishAfterEmptyScanCheck) HasParquetHistory(ctx context.Context, tables model.StorageTables, schemaID int16) (bool, error) {
	found, err := r.DirtyIDFetcher.HasParquetHistory(ctx, tables, schemaID)
	if err != nil || found || r.ran {
		return found, err
	}
	r.ran = true
	r.err = r.publish(ctx)
	return found, nil
}
