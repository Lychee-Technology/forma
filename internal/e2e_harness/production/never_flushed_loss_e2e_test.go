//go:build e2e

package production

import (
	"context"
	"errors"
	"fmt"
	"testing"

	fedengine "github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/model"
)

// TestLostInitializedDataStillFails (#720 review): a cdc-init export leaves
// its rows in base parquet with no unflushed change_log entry once the
// onboarding handoff marks them. When that export's manifest and objects are
// lost, the published generation must keep failing the read as inconsistent
// rather than answer from the later hot rows alone.
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
	env.MarkOnboarded(ctx, wide)
	for _, key := range report.NewObjects {
		deleteObject(ctx, t, env, key)
	}

	assertLostParquetFailsBothShapes(ctx, t, env, wide)
}

// TestLostFlushedDataStillFails (#720 review): a flush published its
// generation and marked its rows in one transaction, so losing the
// generation's manifest and objects is loss. The read must fail at once, and
// the hot-excluded shape must agree.
func TestLostFlushedDataStillFails(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
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
// published generation is lost, then requires both the all-tier and the
// hot-excluded query to fail as a publication inconsistency rather than
// answer short.
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
		if !errors.Is(err, fedengine.ErrPublicationInconsistent) {
			t.Fatalf("tiers %v: got %v, want ErrPublicationInconsistent", tiers, err)
		}
		if errors.Is(err, fedengine.ErrFederatedReadFailed) {
			t.Fatalf("tiers %v: a lost generation is not a degradable read failure: %v", tiers, err)
		}
	}
}

// The tests below run a writer inside a pass over a never-published schema,
// after its snapshot read the schema's record as UNPUBLISHED (#720) and read
// the hot keys, before the pass answers. The writer publishes a generation
// and marks the rows' entries, moving them out of the hot set. The pass
// answers from its snapshot, which still holds the record unpublished and
// the rows hot, so the answer stays complete.

// TestFirstFlushAfterTheEmptyScanCheckStaysComplete: a first flush.
func TestFirstFlushAfterTheEmptyScanCheckStaysComplete(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	runFirstFlushRace(ctx, t, env, func(ctx context.Context, schema SchemaRef) error {
		return expectFlushedEverything(env.RunFlush(ctx))
	})
}

// TestInitHandoffAfterTheEmptyScanCheckStaysComplete (#720 review): cdc-init
// publishes base parquet and its handoff marks the exported entries, which
// leaves the hot leg with no entry to serve the rows by.
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
		env.MarkOnboarded(ctx, schema)
		return nil
	})
}

// runFirstFlushRace creates three rows on a never-published schema, runs
// publish once inside the first pass that reads hot keys, after its snapshot
// read the record unpublished, and requires the query that raced it to match the
// oracle.
func runFirstFlushRace(ctx context.Context, t *testing.T, env *Env, publish func(context.Context, SchemaRef) error) {
	t.Helper()
	wide := DefaultSchemaFixtures()[1]
	racer := &publishInsideThePass{publish: func(ctx context.Context) error { return publish(ctx, wide) }}
	env.HotLegReaderWrap = func(inner fedengine.HotLegReader) fedengine.HotLegReader {
		return &hookedHotLegReader{HotLegReader: inner, hook: racer.race}
	}
	if err := env.ApplyEvents(ctx, env.GenerateScript(ScriptSpec{Schema: wide, Creates: 3})...); err != nil {
		t.Fatalf("apply events: %v", err)
	}

	env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20, PreferredTiers: allTiers})
	if !racer.ran {
		t.Fatal("the query never read hot keys, so nothing raced it")
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

// publishInsideThePass runs publish once, the first time race is called.
type publishInsideThePass struct {
	publish func(context.Context) error
	ran     bool
	err     error
}

// race runs publish the first time it is called.
func (r *publishInsideThePass) race(ctx context.Context) {
	if r.ran {
		return
	}
	r.ran = true
	r.err = r.publish(ctx)
}
