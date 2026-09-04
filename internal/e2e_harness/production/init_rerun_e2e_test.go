//go:build e2e

package production

import (
	"context"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/manifest"
)

// TestInitRerunIdempotency (#176 scenario 4, re-keyed by #416): rerunning
// cdc-init with no data changes exports the same batches to FRESH write-once
// base keys ({min}_{max}_{uuid}: same row ranges, new file ids), replaces the
// manifest's base tier with exactly that set (no duplicates), and leaves the
// first run's objects byte-for-byte untouched on S3 as unlisted orphans for
// manifest-reconcile --gc. The federated result is unchanged.
func TestInitRerunIdempotency(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	env.CDC.BatchSize = 7 // 20 rows -> 3 base files, so duplication is x3-visible

	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 20})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}

	first, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("first init: %v", err)
	}
	firstPaths := buildBasePaths(first.Manifest)
	if len(firstPaths) != 3 {
		t.Fatalf("first init produced %d base entries, want 3", len(firstPaths))
	}

	firstObjects := snapshotS3Inventory(t, ctx, env, buildSchemaKeyPrefix(env, wide))

	second, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("second init: %v", err)
	}
	// Write-once keys (#416): the rerun mints exactly one fresh base object per
	// batch and never reuses a first-run key. (_tmp keys are copy-staging
	// garbage, not exports, if one survives — swallowed-delete residue is
	// reclaimed by manifest-reconcile --gc, #226.)
	minted := make(map[string]bool)
	for _, k := range second.NewObjects {
		if strings.Contains(k, "/_tmp/") {
			continue
		}
		if _, dup := firstObjects[k]; dup {
			t.Errorf("rerun reported %s as new, but the first run already wrote it", k)
		}
		minted[k] = true
	}
	if len(minted) != 3 {
		t.Fatalf("rerun minted %d base objects, want 3 (one fresh key per batch): %v", len(minted), second.NewObjects)
	}
	// The superseded set is left alone: same stat and ETag for every
	// first-run object, so no listed-under-a-stamp bytes ever changed (the
	// TOCTOU the compactor's checksum gate false-positived on, #416).
	afterObjects := snapshotS3Inventory(t, ctx, env, buildSchemaKeyPrefix(env, wide))
	for k, before := range firstObjects {
		if after, ok := afterObjects[k]; !ok || after != before {
			t.Errorf("rerun touched first-run object %s: before %+v, after %+v", k, before, after)
		}
	}
	// No duplicate manifest entries: count RAW entries, not unique paths —
	// this is the assertion that goes red on append semantics (6 vs 3).
	rawEntries := manifest.FilterByTier(second.Manifest, "base")
	if len(rawEntries) != 3 {
		t.Fatalf("manifest holds %d base entries after rerun, want 3 (no duplicates)", len(rawEntries))
	}
	// The base tier is exactly the rerun's file set: every entry is a key the
	// rerun minted, and none of the first run's keys survives in the manifest.
	for _, f := range rawEntries {
		if !minted[f.Path] {
			t.Errorf("manifest base entry %s is not a key the rerun minted (stale first-run path or unexpected key)", f.Path)
		}
		if firstPaths[f.Path] {
			t.Errorf("rerun left first-run base path %s listed", f.Path)
		}
	}
	assertBaseRows(ctx, t, env, second.Manifest, 20)

	// Same federated results after the rerun (base-only source).
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID)
	assertFederatedRowCount(ctx, t, env, "post-rerun base tier",
		Query{Schema: wide, Limit: 100}, 20)
}

func buildBasePaths(m *manifest.Manifest) map[string]bool {
	paths := make(map[string]bool)
	for _, f := range manifest.FilterByTier(m, "base") {
		paths[f.Path] = true
	}
	return paths
}

// TestInitRerunAfterChangesReconcilesManifest (#176 scenario 4, review
// round 1): a rerun after inserts and deletes produces different
// deterministic batch ranges; the manifest's base tier must become exactly
// the new run's file set — no stale entries, no duplicates. The stale S3
// objects intentionally remain (object reconciliation is #203): the final
// parity check proves glob+LWW keeps queries exact anyway.
func TestInitRerunAfterChangesReconcilesManifest(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	env.CDC.BatchSize = 7 // 20 rows -> 3 files; 23 rows -> 4 files

	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 20})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}
	first, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("first init: %v", err)
	}
	firstPaths := buildBasePaths(first.Manifest)
	if len(firstPaths) != 3 {
		t.Fatalf("first init produced %d base entries, want 3", len(firstPaths))
	}

	// Change the dataset: 5 new creates, delete the two smallest row-ids so
	// every batch boundary shifts and at least one run-1 path goes stale.
	more := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 5})
	changes := append(append([]*Event{}, more...),
		DeleteEvent(wide, creates[0].RowID),
		DeleteEvent(wide, creates[1].RowID))
	if err := env.ApplyEvents(ctx, changes...); err != nil {
		t.Fatalf("apply changes: %v", err)
	}

	second, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("second init: %v", err)
	}
	if second.RowsExported != 23 {
		t.Fatalf("second init exported %d rows, want 23 (20 - 2 deleted + 5 new)", second.RowsExported)
	}
	entries := manifest.FilterByTier(second.Manifest, "base")
	if len(entries) != 4 {
		t.Fatalf("manifest holds %d base entries after changed rerun, want 4 (ceil(23/7)), got %+v", len(entries), entries)
	}
	secondPaths := buildBasePaths(second.Manifest)
	if len(secondPaths) != 4 {
		t.Fatalf("manifest base paths not unique after changed rerun: %d unique of 4", len(secondPaths))
	}
	stale := 0
	for p := range firstPaths {
		if !secondPaths[p] {
			stale++
		}
	}
	if stale == 0 {
		t.Fatal("scenario produced no stale run-1 path; deleting the two smallest row-ids must shift batch boundaries")
	}
	assertBaseRows(ctx, t, env, second.Manifest, 23)

	// Handoff flush (tombstones shadow the stale objects' copies), then
	// exact parity across base + stale objects + delta.
	env.CDC.BatchSize = 10000 // one flush pass drains at most BatchSize rows
	flush, err := env.RunFlush(ctx)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if flush.UnflushedAfter != 0 {
		t.Fatalf("flush left %d unflushed rows", flush.UnflushedAfter)
	}
	assertFederatedRowCount(ctx, t, env, "post-rerun parity",
		Query{Schema: wide, Limit: 100}, 23)
}
