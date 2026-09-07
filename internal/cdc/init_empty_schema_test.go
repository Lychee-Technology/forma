package cdc

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/stretchr/testify/require"
)

// An empty entry set is never published, with or without the flag (#519): a
// manifest listing nothing would send reads to the glob fallback, so the
// stale base stays listed and nothing is saved. The zero-row schema case is
// handled by finishEmptySchema, which always publishes one base entry.
func TestUpdateSchemaManifest_EmptyEntriesNeverPublished(t *testing.T) {
	for _, flag := range []bool{false, true} {
		var events []string
		st := &recordingStore{events: &events}
		seedDeltaManifest(t, &st.memManifestStore, "delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet")
		runCtx := deltaRunContext(st, nil)
		runCtx.replaceDelta = flag
		state := &schemaInitState{schemaID: 1}

		require.NoError(t, updateSchemaManifest(context.Background(), runCtx, state))
		require.Empty(t, events, "replaceDelta=%v: an empty entry set must not be saved", flag)
		require.Empty(t, state.deltaPurge, "replaceDelta=%v: no swap, so no delta entry joins the purge set", flag)

		m, _, err := manifest.Load(context.Background(), st, "manifest/1.json")
		require.NoError(t, err)
		require.Len(t, m.Files, 2, "replaceDelta=%v: both tiers stay listed", flag)
		require.Equal(t, "base/1/old_range.parquet", m.Files[0].Path)
	}
}

// emptySchemaRunContext is the finishEmptySchema fixture: a manifest store
// and delete seam that record their events, an export seam that records the
// batch it was handed, a mock S3 client for the tmp->final copy, and the
// attribute cache the pre-flight would have resolved.
func emptySchemaRunContext(t *testing.T, st manifest.Store, listed []string, events *[]string) *initRunContext {
	t.Helper()
	runCtx := deltaRunContext(st, listed)
	runCtx.cfg.S3Prefix = "base"
	runCtx.s3Client = &objectOnlyS3Client{}
	runCtx.attrCaches = map[int16]forma.SchemaAttributeCache{1: testAttrCache()}
	runCtx.deleteObject = recordingDelete(events)
	runCtx.exportBaseFile = func(_ context.Context, state *schemaInitState, batch schemaBatchExport) error {
		require.Empty(t, batch.rowIDs, "the zero-row base exports no row ids")
		require.NotEmpty(t, state.attrCache, "the export needs the schema's attribute cache")
		require.Equal(t, "s3://bkt/"+batch.tmpKey, batch.s3TmpPath)
		*events = append(*events, "export "+batch.finalKey)
		return nil
	}
	return runCtx
}

// exportedKey returns the final key the export seam recorded.
func exportedKey(t *testing.T, events []string) string {
	t.Helper()
	require.NotEmpty(t, events)
	require.True(t, strings.HasPrefix(events[0], "export "), "first event %q, want the export", events[0])
	return strings.TrimPrefix(events[0], "export ")
}

// #519: a schema with zero live rows under --replace-delta exports one
// zero-row base object, publishes it as the sole base entry with the delta
// tier emptied, and deletes the inventoried delta objects only after the
// swap — the same export, save, delete order as a populated schema. The old
// base is unlisted (left for manifest-reconcile --gc), never deleted.
func TestFinishEmptySchema_ReplaceDeltaExportsZeroRowBaseThenSwapsThenPurges(t *testing.T) {
	listedKey := "delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet"
	unlistedKey := "delta/1/0b2f1b6e-0000-4c1d-8e57-000000000000.parquet"
	var events []string
	st := &recordingStore{events: &events}
	seedDeltaManifest(t, &st.memManifestStore, listedKey)
	runCtx := emptySchemaRunContext(t, st, []string{listedKey, unlistedKey}, &events)
	runCtx.replaceDelta = true

	inv, err := preflightDeltaTier(context.Background(), runCtx, 1)
	require.NoError(t, err)
	state, err := finishEmptySchema(context.Background(), runCtx, 1, inv)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, int64(0), state.rowsExported)
	require.Equal(t, 1, state.filesCreated)

	key := exportedKey(t, events)
	require.True(t, strings.HasPrefix(key, "base/1/base-"), "zero-row base key %q must take the merged-base shape", key)
	require.True(t, strings.HasSuffix(key, ".parquet"), key)
	require.Equal(t, []string{"export " + key, "save", "delete " + unlistedKey, "delete " + listedKey}, events)

	m, _, err := manifest.Load(context.Background(), st, "manifest/1.json")
	require.NoError(t, err)
	require.Len(t, m.Files, 1, "exactly the zero-row base is listed: %+v", m.Files)
	entry := m.Files[0]
	require.Equal(t, "base", entry.Tier)
	require.Equal(t, key, entry.Path)
	require.Equal(t, int64(0), entry.RowCount)
	require.Empty(t, entry.RowIDMin)
	require.Empty(t, entry.RowIDMax)
	require.Empty(t, manifest.FilterByTier(m, "delta"))
	require.Equal(t, int16(1), m.SchemaID)
}

// Without the flag the branch is the historical no-op: nothing exported,
// nothing saved, nothing deleted, the stale base still listed.
func TestFinishEmptySchema_WithoutFlagChangesNothing(t *testing.T) {
	var events []string
	st := &recordingStore{events: &events}
	seedDeltaManifest(t, &st.memManifestStore)
	runCtx := emptySchemaRunContext(t, st, []string{}, &events)

	state, err := finishEmptySchema(context.Background(), runCtx, 1, deltaInventory{})
	require.NoError(t, err)
	require.Nil(t, state)
	require.Empty(t, events)
	m, _, err := manifest.Load(context.Background(), st, "manifest/1.json")
	require.NoError(t, err)
	require.Len(t, m.Files, 1)
}

// Dry-run reports the zero-row base and the purge set like any purge and
// touches nothing: no export, no save, no delete.
func TestFinishEmptySchema_DryRunWritesAndDeletesNothing(t *testing.T) {
	listedKey := "delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet"
	var events []string
	st := &recordingStore{events: &events}
	seedDeltaManifest(t, &st.memManifestStore, listedKey)
	runCtx := emptySchemaRunContext(t, st, []string{listedKey}, &events)
	runCtx.replaceDelta, runCtx.dryRun = true, true

	inv, err := preflightDeltaTier(context.Background(), runCtx, 1)
	require.NoError(t, err)
	state, err := finishEmptySchema(context.Background(), runCtx, 1, inv)
	require.NoError(t, err)
	require.Empty(t, events, "dry-run neither exports, saves nor deletes")
	require.NotNil(t, state)
	require.Equal(t, 1, state.filesCreated, "dry-run reports the zero-row base it would write")
	require.Equal(t, int64(0), state.rowsExported)
	require.Equal(t, []string{listedKey}, state.deltaPurge)
}

// A failed save deletes nothing: the delete-after-swap ordering holds for
// the zero-row swap exactly as for a populated one.
func TestFinishEmptySchema_FailedSwapDeletesNothing(t *testing.T) {
	listedKey := "delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet"
	var events []string
	st := &recordingStore{events: &events, saveErr: errors.New("s3 write denied")}
	seedDeltaManifest(t, &st.memManifestStore, listedKey)
	runCtx := emptySchemaRunContext(t, st, []string{listedKey}, &events)
	runCtx.replaceDelta = true

	inv, err := preflightDeltaTier(context.Background(), runCtx, 1)
	require.NoError(t, err)
	_, err = finishEmptySchema(context.Background(), runCtx, 1, inv)
	require.Error(t, err)
	require.Contains(t, err.Error(), "schema 1")
	require.Equal(t, []string{"export " + exportedKey(t, events), "save"}, events)
}

// A failed export publishes nothing and deletes nothing: the old manifest
// still lists every tier.
func TestFinishEmptySchema_FailedExportPublishesNothing(t *testing.T) {
	listedKey := "delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet"
	var events []string
	st := &recordingStore{events: &events}
	seedDeltaManifest(t, &st.memManifestStore, listedKey)
	runCtx := emptySchemaRunContext(t, st, []string{listedKey}, &events)
	runCtx.replaceDelta = true
	runCtx.exportBaseFile = func(context.Context, *schemaInitState, schemaBatchExport) error {
		return errors.New("duckdb copy failed")
	}

	inv, err := preflightDeltaTier(context.Background(), runCtx, 1)
	require.NoError(t, err)
	_, err = finishEmptySchema(context.Background(), runCtx, 1, inv)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duckdb copy failed")
	require.Empty(t, events)
	m, _, err := manifest.Load(context.Background(), st, "manifest/1.json")
	require.NoError(t, err)
	require.Len(t, m.Files, 2)
}

// A schema that was never initialized (no manifest) and has no delta
// objects has nothing to retire: no base is exported and no manifest is
// minted, so the flag over an untouched schema leaves no trace.
func TestFinishEmptySchema_NothingListedNoDeltaDoesNothing(t *testing.T) {
	var events []string
	st := &recordingStore{events: &events}
	runCtx := emptySchemaRunContext(t, st, []string{}, &events)
	runCtx.replaceDelta = true

	inv, err := preflightDeltaTier(context.Background(), runCtx, 1)
	require.NoError(t, err)
	state, err := finishEmptySchema(context.Background(), runCtx, 1, inv)
	require.NoError(t, err)
	require.Nil(t, state)
	require.Empty(t, events)
	_, _, err = manifest.Load(context.Background(), st, "manifest/1.json")
	require.True(t, manifest.IsNotFound(err), "no manifest object minted, got %v", err)
}

// A never-initialized schema whose delta prefix holds an unlisted delta
// object still gets the swap: the zero-row base is published and the
// object purged, so the stale delta cannot be re-adopted by --repair.
func TestFinishEmptySchema_NothingListedButUnlistedDeltaStillRetires(t *testing.T) {
	unlistedKey := "delta/1/0b2f1b6e-0000-4c1d-8e57-000000000000.parquet"
	var events []string
	st := &recordingStore{events: &events}
	runCtx := emptySchemaRunContext(t, st, []string{unlistedKey}, &events)
	runCtx.replaceDelta = true

	inv, err := preflightDeltaTier(context.Background(), runCtx, 1)
	require.NoError(t, err)
	state, err := finishEmptySchema(context.Background(), runCtx, 1, inv)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, []string{"export " + exportedKey(t, events), "save", "delete " + unlistedKey}, events)
	m, _, err := manifest.Load(context.Background(), st, "manifest/1.json")
	require.NoError(t, err)
	require.Len(t, m.Files, 1)
	require.Equal(t, int64(0), m.Files[0].RowCount)
}

// Without a manifest template there is no manifest store (applyInitS3Wiring)
// and so no manifest to swap: the flag over an emptied schema is a no-op, as
// updateSchemaManifest is on the populated path, rather than a nil-store
// panic in the manifest load. The pre-flight only reaches this branch with
// an empty inventory (a non-empty one is refused without a store).
func TestFinishEmptySchema_NoManifestStoreIsNoOp(t *testing.T) {
	var events []string
	runCtx := emptySchemaRunContext(t, nil, []string{}, &events)
	runCtx.manifestStore = nil
	runCtx.manifestResolver = manifest.PathResolver{}
	runCtx.replaceDelta = true

	inv, err := preflightDeltaTier(context.Background(), runCtx, 1)
	require.NoError(t, err)
	require.True(t, inv.empty())
	state, err := finishEmptySchema(context.Background(), runCtx, 1, inv)
	require.NoError(t, err)
	require.Nil(t, state)
	require.Empty(t, events, "no store: nothing exported, saved or deleted")
}
