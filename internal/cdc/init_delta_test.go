package cdc

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const deltaTestTemplate = "manifest/{{.SchemaID}}.json"

// seedDeltaManifest stores a schema-1 manifest listing the given delta paths
// plus one old base entry.
func seedDeltaManifest(t *testing.T, st manifest.Store, deltaPaths ...string) {
	t.Helper()
	m := &manifest.Manifest{SchemaID: 1, Files: []manifest.FileEntry{
		{Tier: "base", Path: "base/1/old_range.parquet", RowCount: 1},
	}}
	for _, p := range deltaPaths {
		m.Files = append(m.Files, manifest.FileEntry{Tier: "delta", Path: p, RowCount: 5})
	}
	_, err := manifest.Save(context.Background(), st, "manifest/1.json", m, "")
	require.NoError(t, err)
}

func deltaRunContext(st manifest.Store, listed []string) *initRunContext {
	runCtx := &initRunContext{
		cfg:              CDCConfig{S3Bucket: "bkt"},
		manifestStore:    st,
		manifestResolver: manifest.PathResolver{PathTemplate: deltaTestTemplate},
		logger:           zap.NewNop(),
		deltaPrefix:      "delta",
	}
	if st == nil {
		runCtx.manifestStore = nil
	}
	if listed != nil {
		runCtx.listObjectKeys = func(context.Context, string) ([]string, error) { return listed, nil }
	}
	return runCtx
}

func TestPreflightDeltaTier_EmptyTierProceedsWithoutFlag(t *testing.T) {
	st := &memManifestStore{}
	seedDeltaManifest(t, st)
	runCtx := deltaRunContext(st, []string{"delta/1/base-0b2f1b6e-1111-4c1d-8e57-000000000001.parquet"})

	inv, err := preflightDeltaTier(context.Background(), runCtx, 1)
	require.NoError(t, err)
	require.True(t, inv.empty())
}

// The refusal (#371): a manifest-listed delta entry fails the schema with the
// named sentinel, in a real run and in dry-run alike, and names the flag.
func TestPreflightDeltaTier_RefusesListedDeltaWithoutFlag(t *testing.T) {
	st := &memManifestStore{}
	seedDeltaManifest(t, st, "delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet")
	for _, dry := range []bool{false, true} {
		runCtx := deltaRunContext(st, []string{})
		runCtx.dryRun = dry

		_, err := preflightDeltaTier(context.Background(), runCtx, 1)
		require.ErrorIs(t, err, ErrDeltaTierPresent, "dry=%t", dry)
		require.Contains(t, err.Error(), "--replace-delta")
		require.Contains(t, err.Error(), "1 manifest-listed and 0 unlisted")
	}
}

// An unlisted delta-shaped object under the delta prefix refuses too: the
// next manifest-reconcile --repair would re-list it under the new base.
// Base-shaped, nested (_tmp/), and non-UUID keys under the prefix are not
// delta and never enter the inventory.
func TestPreflightDeltaTier_RefusesUnlistedDeltaShapedObjectOnly(t *testing.T) {
	st := &memManifestStore{}
	seedDeltaManifest(t, st)
	listed := []string{
		"delta/1/0b2f1b6e-2222-4c1d-8e57-000000000002.parquet",
		"delta/1/00000000-0000-0000-0000-000000000000_ffffffff-ffff-ffff-ffff-ffffffffffff_0b2f1b6e-3333-4c1d-8e57-000000000003.parquet",
		"delta/1/base-0b2f1b6e-4444-4c1d-8e57-000000000004.parquet",
		"delta/1/_tmp/0b2f1b6e-5555-4c1d-8e57-000000000005.parquet",
		"delta/1/notes.txt",
	}
	runCtx := deltaRunContext(st, listed)

	inv, err := inventoryDeltaTier(context.Background(), runCtx, 1)
	require.NoError(t, err)
	require.Empty(t, inv.listed)
	require.Equal(t, []string{"delta/1/0b2f1b6e-2222-4c1d-8e57-000000000002.parquet"}, inv.unlisted)

	_, err = preflightDeltaTier(context.Background(), runCtx, 1)
	require.ErrorIs(t, err, ErrDeltaTierPresent)
	require.Contains(t, err.Error(), "0 manifest-listed and 1 unlisted")
}

// Without a delta prefix or an S3 client the listing is skipped and only the
// manifest is consulted; a listed entry still refuses.
func TestInventoryDeltaTier_SkipsListingWithoutPrefixOrClient(t *testing.T) {
	st := &memManifestStore{}
	seedDeltaManifest(t, st, "delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet")
	runCtx := deltaRunContext(st, nil)
	runCtx.deltaPrefix = ""

	inv, err := inventoryDeltaTier(context.Background(), runCtx, 1)
	require.NoError(t, err)
	require.Equal(t, []string{"delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet"}, inv.listed)
	require.Empty(t, inv.unlisted)
}

func TestInventoryDeltaTier_ListingFailurePropagates(t *testing.T) {
	st := &memManifestStore{}
	seedDeltaManifest(t, st)
	runCtx := deltaRunContext(st, nil)
	listErr := errors.New("access denied")
	runCtx.listObjectKeys = func(context.Context, string) ([]string, error) { return nil, listErr }

	_, err := preflightDeltaTier(context.Background(), runCtx, 1)
	require.ErrorIs(t, err, listErr)
	require.Contains(t, err.Error(), "list delta prefix delta/1/")
}

// With the flag the inventory becomes the purge set: listed and unlisted keys
// merged, de-duplicated (the listing sees the listed objects too), sorted.
func TestPreflightDeltaTier_FlagReturnsMergedPurgeSet(t *testing.T) {
	st := &memManifestStore{}
	listedKey := "delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet"
	unlistedKey := "delta/1/0b2f1b6e-0000-4c1d-8e57-000000000000.parquet"
	seedDeltaManifest(t, st, "s3://bkt/"+listedKey)
	runCtx := deltaRunContext(st, []string{listedKey, unlistedKey})
	runCtx.replaceDelta = true

	inv, err := preflightDeltaTier(context.Background(), runCtx, 1)
	require.NoError(t, err)
	require.Equal(t, []string{listedKey}, inv.listed, "s3:// URIs normalize to bucket-relative keys")
	require.Equal(t, []string{unlistedKey}, inv.unlisted)
	require.Equal(t, []string{unlistedKey, listedKey}, inv.purgeKeys())
}

// The purge is ordered after the manifest swap, so the flag without a
// manifest template has nothing to order it after and fails closed.
func TestPreflightDeltaTier_FlagWithoutManifestStoreErrors(t *testing.T) {
	runCtx := deltaRunContext(nil, []string{"delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet"})
	runCtx.replaceDelta = true

	_, err := preflightDeltaTier(context.Background(), runCtx, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "manifest template")
}

// A delta entry the run cannot address through its bucket (glob, foreign
// bucket) fails before any export, naming the entries.
func TestPreflightDeltaTier_FlagRefusesUnnormalizableEntries(t *testing.T) {
	st := &memManifestStore{}
	seedDeltaManifest(t, st, "delta/1/*.parquet", "s3://other-bucket/delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet")
	runCtx := deltaRunContext(st, []string{})
	runCtx.replaceDelta = true

	_, err := preflightDeltaTier(context.Background(), runCtx, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "delta/1/*.parquet")
	require.Contains(t, err.Error(), "s3://other-bucket/")
	require.NotErrorIs(t, err, ErrDeltaTierPresent, "with the flag the refusal is a different failure")
}

// The flag-path sibling of TestUpdateSchemaManifest_RerunReconcilesBaseTier:
// one save replaces the base tier AND empties the delta tier, and the
// dropped delta entries join the purge set.
func TestUpdateSchemaManifest_ReplaceDeltaEmptiesDeltaTier(t *testing.T) {
	st := &memManifestStore{}
	seedDeltaManifest(t, st, "delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet")
	runCtx := deltaRunContext(st, nil)
	runCtx.replaceDelta = true
	state := initStateWithOneEntry(1)

	require.NoError(t, updateSchemaManifest(context.Background(), runCtx, state))

	m, _, err := manifest.Load(context.Background(), st, "manifest/1.json")
	require.NoError(t, err)
	require.Len(t, m.Files, 1)
	require.Equal(t, "base", m.Files[0].Tier)
	require.Equal(t, "prefix/1/a_b.parquet", m.Files[0].Path)
	require.Equal(t, []string{"delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet"}, state.deltaPurge)
}

// deltaInjectingStore rejects the first save with a confirmed 412 and, like
// a flush that landed in between, adds a delta entry to the stored manifest.
type deltaInjectingStore struct {
	memManifestStore
	injected bool
}

func (s *deltaInjectingStore) Save(ctx context.Context, path string, data []byte, etag string) (string, error) {
	if s.injected {
		return s.memManifestStore.Save(ctx, path, data, etag)
	}
	s.injected = true
	m, _, err := manifest.Load(ctx, &s.memManifestStore, path)
	if err != nil {
		return "", err
	}
	m.Files = append(m.Files, manifest.FileEntry{Tier: "delta", Path: "delta/1/0b2f1b6e-9999-4c1d-8e57-000000000009.parquet"})
	if _, err := manifest.Save(ctx, &s.memManifestStore, path, m, ""); err != nil {
		return "", err
	}
	return "", &preconditionFailedErr{}
}

// A retried publish re-splices BOTH tiers from the reloaded manifest and
// captures the delta entry that appeared in between, so no delta survives
// the swap unpurged.
func TestUpdateSchemaManifest_ReplaceDeltaRetryCapturesLateDelta(t *testing.T) {
	st := &deltaInjectingStore{}
	seedDeltaManifest(t, &st.memManifestStore, "delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet")
	runCtx := deltaRunContext(st, nil)
	runCtx.replaceDelta = true
	state := initStateWithOneEntry(1)
	state.deltaPurge = []string{"delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet"}

	require.NoError(t, updateSchemaManifest(context.Background(), runCtx, state))

	m, _, err := manifest.Load(context.Background(), st, "manifest/1.json")
	require.NoError(t, err)
	require.Len(t, m.Files, 1)
	require.Equal(t, "prefix/1/a_b.parquet", m.Files[0].Path)
	require.Equal(t, []string{
		"delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet",
		"delta/1/0b2f1b6e-9999-4c1d-8e57-000000000009.parquet",
	}, state.deltaPurge)
}

// recordingStore appends "save" to events on every save.
type recordingStore struct {
	memManifestStore
	events  *[]string
	saveErr error
}

func (s *recordingStore) Save(ctx context.Context, path string, data []byte, etag string) (string, error) {
	*s.events = append(*s.events, "save")
	if s.saveErr != nil {
		return "", s.saveErr
	}
	return s.memManifestStore.Save(ctx, path, data, etag)
}

func recordingDelete(events *[]string) func(context.Context, string) error {
	return func(_ context.Context, key string) error {
		*events = append(*events, "delete "+key)
		return nil
	}
}

// Swap, then delete: every delete happens after the manifest save, and a
// failed save deletes nothing.
func TestPublishAndPurge_DeletesOnlyAfterTheSwapCommits(t *testing.T) {
	key := "delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet"

	var events []string
	st := &recordingStore{events: &events}
	seedDeltaManifest(t, &st.memManifestStore, key)
	runCtx := deltaRunContext(st, nil)
	runCtx.replaceDelta = true
	runCtx.deleteObject = recordingDelete(&events)
	state := initStateWithOneEntry(1)
	state.deltaPurge = []string{key}

	require.NoError(t, publishAndPurge(context.Background(), runCtx, state))
	require.Equal(t, []string{"save", "delete " + key}, events)

	events = nil
	failing := &recordingStore{events: &events, saveErr: errors.New("s3 write denied")}
	seedDeltaManifest(t, &failing.memManifestStore, key)
	runCtx = deltaRunContext(failing, nil)
	runCtx.replaceDelta = true
	runCtx.deleteObject = recordingDelete(&events)
	state = initStateWithOneEntry(1)
	state.deltaPurge = []string{key}

	require.Error(t, publishAndPurge(context.Background(), runCtx, state))
	require.Equal(t, []string{"save"}, events, "a failed swap must delete nothing")
}

func TestPurgeDeltaTier_DryRunDeletesNothing(t *testing.T) {
	deleted := 0
	runCtx := deltaRunContext(&memManifestStore{}, nil)
	runCtx.replaceDelta, runCtx.dryRun = true, true
	runCtx.deleteObject = func(context.Context, string) error { deleted++; return nil }
	state := &schemaInitState{schemaID: 1, deltaPurge: []string{"delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet"}}

	require.NoError(t, purgeDeltaTier(context.Background(), runCtx, state))
	require.Zero(t, deleted)
}

func TestPurgeDeltaTier_NoDeleteSeamErrors(t *testing.T) {
	runCtx := deltaRunContext(&memManifestStore{}, nil)
	runCtx.replaceDelta = true
	state := &schemaInitState{schemaID: 1, deltaPurge: []string{"delta/1/x.parquet"}}

	err := purgeDeltaTier(context.Background(), runCtx, state)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no S3 client")
}

// A failed delete is reported by key and does not stop the remaining
// deletes; the error says the manifest swap stays published.
func TestPurgeDeltaTier_DeleteFailureNamesKeyAndContinues(t *testing.T) {
	bad := "delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet"
	good := "delta/1/0b2f1b6e-2222-4c1d-8e57-000000000002.parquet"
	delErr := errors.New("access denied")
	var deleted []string
	runCtx := deltaRunContext(&memManifestStore{}, nil)
	runCtx.replaceDelta = true
	runCtx.deleteObject = func(_ context.Context, key string) error {
		if key == bad {
			return delErr
		}
		deleted = append(deleted, key)
		return nil
	}
	state := &schemaInitState{schemaID: 1, deltaPurge: []string{bad, good}}

	err := purgeDeltaTier(context.Background(), runCtx, state)
	require.ErrorIs(t, err, delErr)
	require.Contains(t, err.Error(), "1 of 2 objects not deleted")
	require.Contains(t, err.Error(), "undeleted: "+bad)
	require.Contains(t, err.Error(), "manifest swap stays published")
	require.Equal(t, []string{good}, deleted)
}

func TestNormalizeObjectKey(t *testing.T) {
	cases := []struct {
		path   string
		key    string
		wantOK bool
	}{
		{"delta/1/a.parquet", "delta/1/a.parquet", true},
		{"s3://bkt/delta/1/a.parquet", "delta/1/a.parquet", true},
		{"s3://other/delta/1/a.parquet", "", false},
		{"s3://bkt-2/delta/1/a.parquet", "", false},
		{"delta/1/*.parquet", "", false},
		{"delta/1/a?.parquet", "", false},
		{"delta/1/[ab].parquet", "", false},
	}
	for _, c := range cases {
		key, ok := NormalizeObjectKey("bkt", c.path)
		require.Equal(t, c.wantOK, ok, c.path)
		require.Equal(t, c.key, key, c.path)
	}
}

func TestIsDeltaShapedKey(t *testing.T) {
	prefix := "delta/1/"
	require.True(t, isDeltaShapedKey(prefix, "delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet"))
	require.False(t, isDeltaShapedKey(prefix, "delta/1/base-0b2f1b6e-1111-4c1d-8e57-000000000001.parquet"), "merged-base shape")
	require.False(t, isDeltaShapedKey(prefix, "delta/1/a_b_0b2f1b6e-1111-4c1d-8e57-000000000001.parquet"), "init-base shape")
	require.False(t, isDeltaShapedKey(prefix, "delta/1/_tmp/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet"), "nested tmp")
	require.False(t, isDeltaShapedKey(prefix, "delta/10/0b2f1b6e-1111-4c1d-8e57-000000000001.parquet"), "other schema")
	require.False(t, isDeltaShapedKey(prefix, "delta/1/0b2f1b6e-1111-4c1d-8e57-000000000001.csv"))
	require.False(t, isDeltaShapedKey(prefix, strings.TrimSuffix(prefix, "/")))
}
