//go:build e2e

package production

import (
	"context"
	"errors"
	"testing"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifest"
)

// TestInitReplaceDeltaRetiresEmptiedSchema (#519): a schema that was
// initialized, then had every row deleted and the tombstones flushed, has
// zero live rows but still lists a base and a delta tier. A plain re-init
// refuses (#371); a dry-run with --replace-delta mutates nothing; a real
// --replace-delta run exports one zero-row base object and publishes it as
// the sole base entry with no delta listed (a manifest listing nothing
// would send reads to the glob fallback and resurrect the deleted rows),
// deletes the delta objects, leaves the superseded base objects on S3 as
// unlisted orphans for manifest-reconcile --gc, and the federated result is
// empty. Once rows exist again a plain init succeeds: the flag is
// idempotent for emptied schemas.
func TestInitReplaceDeltaRetiresEmptiedSchema(t *testing.T) {
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
	oldBaseKeys := manifestKeys(t, env, manifest.FilterByTier(first.Manifest, "base"))
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
	deltaKeys := manifestKeys(t, env, manifest.FilterByTier(flush.Manifests[wide.ID], "delta"))
	if len(deltaKeys) == 0 {
		t.Fatal("flush listed no delta entries for the tombstones")
	}

	// Zero live rows, delta listed: the plain re-init still refuses.
	if _, err := env.RunInit(ctx, wide); !errors.Is(err, cdc.ErrDeltaTierPresent) {
		t.Fatalf("plain init over emptied schema: err = %v, want cdc.ErrDeltaTierPresent", err)
	}

	before := captureState(t, ctx, env, wide)
	dry, err := env.RunInitWith(ctx, wide, InitOverrides{DryRun: true, ReplaceDelta: true})
	if err != nil {
		t.Fatalf("dry-run init with replace-delta: %v", err)
	}
	if dry.RowsExported != 0 || dry.FilesCreated != 1 {
		t.Errorf("dry-run planned %d rows / %d files, want 0 rows / 1 zero-row base", dry.RowsExported, dry.FilesCreated)
	}
	assertStateUnchanged(t, "empty-schema dry-run (replace-delta)", before, captureState(t, ctx, env, wide))

	report, err := env.RunInitWith(ctx, wide, InitOverrides{ReplaceDelta: true})
	if err != nil {
		t.Fatalf("init with replace-delta over emptied schema: %v", err)
	}
	if report.RowsExported != 0 || report.FilesCreated != 1 {
		t.Errorf("empty swap exported %d rows / %d files, want 0 rows / 1 zero-row base", report.RowsExported, report.FilesCreated)
	}
	if report.Manifest == nil {
		t.Fatal("manifest missing after the empty swap")
	}
	bases := manifest.FilterByTier(report.Manifest, "base")
	if len(bases) != 1 {
		t.Fatalf("manifest lists %d base entries after the empty swap, want exactly the zero-row base: %+v", len(bases), report.Manifest.Files)
	}
	if bases[0].RowCount != 0 || bases[0].RowIDMin != "" || bases[0].RowIDMax != "" {
		t.Errorf("zero-row base entry = %+v, want RowCount 0 and no row-id range", bases[0])
	}
	if n := countTier(report.Manifest, "delta"); n != 0 {
		t.Errorf("manifest still lists %d delta entries after the empty swap: %+v", n, report.Manifest.Files)
	}
	assertBaseRows(ctx, t, env, report.Manifest, 0)
	assertS3KeysPresence(ctx, t, env, "after empty swap", deltaKeys, false)
	// Init never deletes base objects (#416/#203): the superseded set is
	// unlisted and left for manifest-reconcile --gc.
	assertS3KeysPresence(ctx, t, env, "after empty swap", oldBaseKeys, true)

	if result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100}); result != nil && len(result.Records) != 0 {
		t.Errorf("federated result has %d rows over an emptied schema, want 0", len(result.Records))
	}

	// Rows arrive again: the plain init no longer refuses.
	more := env.GenerateScript(ScriptSpec{Schema: wide, Creates: 3})
	if err := env.ApplyEvents(ctx, more...); err != nil {
		t.Fatalf("apply later creates: %v", err)
	}
	third, err := env.RunInit(ctx, wide)
	if err != nil {
		t.Fatalf("plain init after the empty swap: %v", err)
	}
	if third.RowsExported != 3 {
		t.Errorf("init after the empty swap exported %d rows, want 3", third.RowsExported)
	}
	assertBaseRows(ctx, t, env, third.Manifest, 3)
	if result := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 100}); result != nil && len(result.Records) != 3 {
		t.Errorf("federated result has %d rows after re-population, want 3", len(result.Records))
	}
}

// manifestKeys resolves manifest entries to bucket-relative keys.
func manifestKeys(t *testing.T, env *Env, entries []manifest.FileEntry) []string {
	t.Helper()
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		key, ok := cdc.NormalizeObjectKey(env.Cluster.Bucket, entry.Path)
		if !ok {
			t.Fatalf("manifest entry %q is not addressable in bucket %s", entry.Path, env.Cluster.Bucket)
		}
		keys = append(keys, key)
	}
	return keys
}
