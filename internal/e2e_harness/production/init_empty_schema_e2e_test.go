//go:build e2e

package production

import (
	"context"
	"testing"
)

// TestReInitRetiresEmptiedSchema (#519, #896): a schema that was
// initialized, then had every row deleted and the tombstones flushed, has
// zero live rows but still lists a base and a delta tier. Re-onboarding it
// with cdc-init publishes an empty init generation, which says the parquet
// tiers hold nothing: no base and no delta is listed, the federated result is
// empty, and no deleted row is resurrected. A dry run mutates nothing. Init
// deletes no object: the superseded base and delta objects stay on S3,
// unlisted, for V2 garbage collection (#891). Once rows exist again a plain
// init exports them.
func TestReInitRetiresEmptiedSchema(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1]

	creates := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 6})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply creates: %v", err)
	}
	first, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("first init: %v", err)
	}
	if first.RowsExported != 6 {
		t.Fatalf("first init exported %d rows, want 6", first.RowsExported)
	}
	oldBaseKeys := manifestKeys(TierEntries(first.Manifest, "base"))
	if len(oldBaseKeys) == 0 {
		t.Fatal("first init listed no base entries")
	}

	deletes := make([]*Event, 0, len(creates))
	for _, ev := range creates {
		deletes = append(deletes, DeleteEvent(wide, ev.RowID))
	}
	if err := env.ApplyEvents(ctx, deletes...); err != nil {
		t.Fatalf("apply deletes: %v", err)
	}
	flush, err := env.RunFlush(ctx)
	if err != nil {
		t.Fatalf("flush tombstones: %v", err)
	}
	if flush.UnflushedAfter != 0 {
		t.Fatalf("flush left %d unflushed rows", flush.UnflushedAfter)
	}
	deltaKeys := manifestKeys(TierEntries(flush.Manifests[wide.ID], "delta"))
	if len(deltaKeys) == 0 {
		t.Fatal("flush listed no delta entries for the tombstones")
	}

	before := captureState(t, ctx, env, wide)
	dry, err := env.RunInitWith(ctx, wide, InitOverrides{DryRun: true})
	if err != nil {
		t.Fatalf("dry-run re-init of the emptied schema: %v", err)
	}
	if dry.RowsExported != 0 || dry.FilesCreated != 0 {
		t.Errorf("dry-run planned %d rows / %d files, want none", dry.RowsExported, dry.FilesCreated)
	}
	assertStateUnchanged(t, "empty-schema dry-run", before, captureState(t, ctx, env, wide))

	report, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("re-init of the emptied schema: %v", err)
	}
	if report.RowsExported != 0 || report.FilesCreated != 0 {
		t.Errorf("re-init of the emptied schema exported %d rows / %d files, want none", report.RowsExported, report.FilesCreated)
	}
	if report.Manifest == nil {
		t.Fatal("the emptied schema has no published generation after its re-init")
	}
	if report.Manifest.Kind != "init" || len(report.Manifest.Files) != 0 {
		t.Fatalf("re-init published a %s generation listing %+v, want an empty init generation", report.Manifest.Kind, report.Manifest.Files)
	}
	// Init deletes nothing: the superseded objects are unlisted and left
	// for garbage collection (#891).
	assertS3KeysPresence(ctx, t, env, "after the empty re-init", deltaKeys, true)
	assertS3KeysPresence(ctx, t, env, "after the empty re-init", oldBaseKeys, true)

	if result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100}); result != nil && len(result.Records) != 0 {
		t.Errorf("federated result has %d rows over an emptied schema, want 0", len(result.Records))
	}

	// Rows arrive again: the next init exports them.
	more := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 3})
	if err := env.ApplyEvents(ctx, more...); err != nil {
		t.Fatalf("apply later creates: %v", err)
	}
	third, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("init after the empty re-init: %v", err)
	}
	if third.RowsExported != 3 {
		t.Errorf("init after the empty re-init exported %d rows, want 3", third.RowsExported)
	}
	assertBaseRows(ctx, t, env, third.Manifest, 3)
	if result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100}); result != nil && len(result.Records) != 3 {
		t.Errorf("federated result has %d rows after re-population, want 3", len(result.Records))
	}
}

// manifestKeys answers the bucket-relative keys of manifest entries.
func manifestKeys(entries []ManifestEntry) []string {
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, entry.Path)
	}
	return keys
}
