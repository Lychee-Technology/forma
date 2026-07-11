//go:build e2e

package production

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestInitDeleteHandoff (#176 scenario 3): rows deleted before the backfill
// never reach the base tier (production deletes hard-delete entity_main and
// leave a change_log tombstone — there is no soft-deleted row for init to
// see); rows deleted after the backfill are tombstoned into delta and the
// base version must lose LWW. Both delete generations are invisible in the
// federated result while every surviving row stays visible.
func TestInitDeleteHandoff(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 20})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}

	// Pre-init deletes: creates[16:20]. Their create+delete change_log
	// entries merge into a single unflushed tombstone (slot-0 upsert).
	var preDel []*Event
	for _, ev := range creates[16:20] {
		preDel = append(preDel, DeleteEvent(wide, ev.RowID))
	}
	if err := env.ApplyEvents(ctx, preDel...); err != nil {
		t.Fatalf("apply pre-init deletes: %v", err)
	}

	report, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("run init: %v", err)
	}
	if report.RowsExported != 16 {
		t.Fatalf("init exported %d rows, want 16 (pre-init deletes must be excluded)", report.RowsExported)
	}
	assertBaseRows(ctx, t, env, report.Manifest, 16)

	// Post-init deletes: creates[13:16] are in the base tier; their
	// tombstones flush to delta and must shadow the base copies.
	var postDel []*Event
	for _, ev := range creates[13:16] {
		postDel = append(postDel, DeleteEvent(wide, ev.RowID))
	}
	if err := env.ApplyEvents(ctx, postDel...); err != nil {
		t.Fatalf("apply post-init deletes: %v", err)
	}

	flush, err := env.RunFlush(ctx)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if flush.UnflushedAfter != 0 {
		t.Fatalf("flush left %d unflushed rows", flush.UnflushedAfter)
	}

	result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	if result == nil {
		return
	}
	// Positive guard first: absence assertions below are meaningless if the
	// whole result is empty.
	if len(result.Records) != 13 {
		t.Fatalf("federated result has %d rows, want 13 (20 - 4 pre-init - 3 post-init deletes)", len(result.Records))
	}
	got := make(map[uuid.UUID]bool, len(result.Records))
	for _, rec := range result.Records {
		got[rec.RowID] = true
	}
	for _, ev := range creates[13:20] {
		if got[ev.RowID] {
			t.Errorf("deleted row %s is visible in the federated result", ev.RowID)
		}
	}
	for _, ev := range creates[0:13] {
		if !got[ev.RowID] {
			t.Errorf("surviving row %s is missing from the federated result", ev.RowID)
		}
	}
}

// TestInitSoftDeletedRowExcluded (#176 scenario 3, review round 1): a row
// already soft-deleted in entity_main when cdc-init runs must be excluded
// by init's `ltbase_deleted_at IS NULL` filter. The production API
// hard-deletes, so the soft-deleted storage state is injected: the row is
// deleted through the API first (real change_log tombstone, oracle in
// sync), then its main row re-materialized with ltbase_deleted_at set via
// the SQL escape hatch — a legal storage state the API cannot produce.
func TestInitSoftDeletedRowExcluded(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 6})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}
	soft := creates[5]
	if err := env.ApplyEvents(ctx, DeleteEvent(wide, soft.RowID)); err != nil {
		t.Fatalf("delete row: %v", err)
	}
	env.ExecSQL(ctx, `INSERT INTO entity_main
		(ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at, ltbase_deleted_at)
		VALUES ($1, $2, $3, $3, $3)`, wide.ID, soft.RowID, time.Now().UnixMilli())

	report, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("run init: %v", err)
	}
	if report.RowsExported != 5 {
		t.Fatalf("init exported %d rows, want 5 (soft-deleted row must be excluded)", report.RowsExported)
	}
	assertBaseRows(ctx, t, env, report.Manifest, 5)

	assertSoftDeletedInvisible(ctx, t, env, wide, soft.RowID)

	flush, err := env.RunFlush(ctx)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if flush.UnflushedAfter != 0 {
		t.Fatalf("flush left %d unflushed rows", flush.UnflushedAfter)
	}
	assertSoftDeletedInvisible(ctx, t, env, wide, soft.RowID)
}

// assertSoftDeletedInvisible checks the 5 surviving rows are all present
// (positive guard) and the soft-deleted row is not.
func assertSoftDeletedInvisible(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, softID uuid.UUID) {
	t.Helper()
	result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100})
	if result == nil {
		return
	}
	if len(result.Records) != 5 {
		t.Fatalf("federated result has %d rows, want 5", len(result.Records))
	}
	for _, rec := range result.Records {
		if rec.RowID == softID {
			t.Fatalf("soft-deleted row %s is visible in the federated result", softID)
		}
	}
}
