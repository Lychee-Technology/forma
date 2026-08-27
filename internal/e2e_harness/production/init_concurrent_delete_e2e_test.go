//go:build e2e

package production

import (
	"context"
	"sort"
	"testing"

	"github.com/google/uuid"
)

// TestInitConcurrentDelete pins issue #462: cdc-init's base backfill must not
// lose a live row when a concurrent delete lands below its batch cursor.
//
// The init batch loop paginates entity_main while ordinary CRUD keeps
// running (TrySchemaLock excludes only flusher/init/reconcile). With
// LIMIT/OFFSET pagination a row soft-deleted below the cursor shrinks the
// filtered set, shifts the window left, and silently drops exactly one live
// row from the wholesale-replaced base tier. The mutation is driven through
// cdc.CDCConfig.BeforeExportHook — the same selection->export seam #182 uses
// for the flusher — firing inside the second batch, i.e. after the cursor
// has moved past the victim's batch.
func TestInitConcurrentDelete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	creates := buildOpenCreates(wide, 6)
	mustApplyEvents(ctx, t, env, "seed creates", creates...)

	// Order rows by ltbase_row_id — the init batch ordering — so "lowest
	// row id" (the already-paginated-past victim) is deterministic even if
	// UUIDv7 assignment ever ties within a millisecond.
	sorted := append([]*Event(nil), creates...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].RowID.String() < sorted[j].RowID.String()
	})
	victim := sorted[0]

	// BatchSize 2 over 6 rows gives three batches. The hook soft-deletes the
	// victim while batch 2 is in flight: batch 1 (the victim's) is already
	// selected and exported, so the delete lands strictly below the cursor
	// before batch 3 is selected — the issue's reproduction verbatim.
	// RunInitWith is synchronous, so the hook runs inline and
	// deterministically; it reports failures through the init error.
	var hookBatches int
	cfg := env.CDC
	cfg.BatchSize = 2
	cfg.BeforeExportHook = func(hctx context.Context, _ int16, _ []uuid.UUID, _ int64) error {
		hookBatches++
		if hookBatches == 2 {
			return env.ApplyEvents(hctx, DeleteEvent(wide, victim.RowID))
		}
		return nil
	}
	report, err := env.RunInitWith(ctx, wide, InitOverrides{Config: &cfg})
	if err != nil {
		t.Fatalf("init with concurrent delete: %v", err)
	}

	// Positive control: the run really was batched and the delete really
	// fired mid-run (before the last batch selection).
	if hookBatches < 3 {
		t.Fatalf("init ran %d batches, want >= 3 (delete must land mid-run)", hookBatches)
	}

	// The acceptance criterion: every surviving row is present in the base
	// tier the manifest now points at. (The victim may legitimately appear
	// too — it was exported before the delete and LWW/tombstones hide it.)
	var basePaths []string
	for _, f := range report.Manifest.Files {
		if f.Tier == "base" {
			basePaths = append(basePaths, f.Path)
		}
	}
	if len(basePaths) == 0 {
		t.Fatalf("manifest has no base entries after init: %+v", report.Manifest.Files)
	}
	got := fetchParquetRowIDs(ctx, t, env, basePaths)
	for _, ev := range sorted[1:] {
		if !got[ev.RowID.String()] {
			t.Errorf("surviving row %s missing from the new base tier (silent cold-tier row loss)", ev.RowID)
		}
	}
}
