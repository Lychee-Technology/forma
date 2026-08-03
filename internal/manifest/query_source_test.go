package manifest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// storeWithManifest seeds the shared in-memory memStore (manifest_test.go)
// with one schema manifest listing the given delta keys.
func storeWithManifest(t *testing.T, schemaID int16, keys ...string) *memStore {
	t.Helper()
	files := make([]FileEntry, 0, len(keys))
	for _, k := range keys {
		files = append(files, FileEntry{Tier: "delta", Path: k})
	}
	return storeWithFileEntries(t, schemaID, files...)
}

// storeWithFileEntries is the same fixture for tests that need to control
// per-entry fields (e.g. the #256 column stamps) rather than just the key.
func storeWithFileEntries(t *testing.T, schemaID int16, files ...FileEntry) *memStore {
	t.Helper()
	payload, err := json.Marshal(&Manifest{SchemaID: schemaID, Version: 1, Files: files})
	require.NoError(t, err)
	return &memStore{data: map[string][]byte{fmt.Sprintf("manifest/%d.json", schemaID): payload}}
}

func testQuerySource(store Store) *QuerySource {
	return &QuerySource{
		Store:    store,
		Resolver: PathResolver{PathTemplate: "manifest/{{.SchemaID}}.json"},
		Bucket:   "bkt",
	}
}

func TestQuerySource_PathsReturnsListedURIs(t *testing.T) {
	src := testQuerySource(storeWithManifest(t, 7, "p/7/a.parquet", "p/7/b.parquet"))

	paths, _, err := src.Paths(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, []string{"s3://bkt/p/7/a.parquet", "s3://bkt/p/7/b.parquet"}, paths)
}

func TestQuerySource_MissingManifestFallsBackToGlob(t *testing.T) {
	src := testQuerySource(&memStore{})
	src.Fallback = func(schemaID int16) string { return fmt.Sprintf("s3://bkt/p/%d/*.parquet", schemaID) }

	paths, _, err := src.Paths(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, []string{"s3://bkt/p/7/*.parquet"}, paths)
}

func TestQuerySource_MissingManifestWithoutFallbackIsEmpty(t *testing.T) {
	src := testQuerySource(&memStore{})

	paths, _, err := src.Paths(context.Background(), 7)
	require.NoError(t, err)
	require.Empty(t, paths)
}

func TestQuerySource_EmptyManifestFallsBackToGlob(t *testing.T) {
	src := testQuerySource(storeWithManifest(t, 7))
	src.Fallback = func(schemaID int16) string { return fmt.Sprintf("s3://bkt/p/%d/*.parquet", schemaID) }

	paths, _, err := src.Paths(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, []string{"s3://bkt/p/7/*.parquet"}, paths)
}

func TestQuerySource_PathsPassesURIEntriesUnchanged(t *testing.T) {
	// The manifest format accepts absolute s3:// entries (see
	// TestManifest_Parse); they must not be double-prefixed with the bucket.
	src := testQuerySource(storeWithManifest(t, 7, "p/7/a.parquet", "s3://otherbkt/base.parquet"))

	paths, _, err := src.Paths(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, []string{"s3://bkt/p/7/a.parquet", "s3://otherbkt/base.parquet"}, paths)
}

// TestPathsReturnsColumnStamps pins the #256 key contract: Paths returns each
// stamped entry's columns keyed by the exact URI it returned for that entry —
// relative keys get the bucket prefix, absolute s3:// entries pass through —
// so the validator can look stamps up without re-deriving URIs. Unstamped
// (legacy) entries contribute no key.
func TestPathsReturnsColumnStamps(t *testing.T) {
	relCols := map[string]string{
		"row_id": "UUID", "changed_at": "BIGINT", "deleted_at": "BIGINT", "attr_6": "BIGINT",
	}
	absCols := map[string]string{
		"row_id": "UUID", "changed_at": "BIGINT", "deleted_at": "BIGINT",
	}
	src := testQuerySource(storeWithFileEntries(t, 7,
		FileEntry{Tier: "delta", Path: "rel.parquet", Columns: relCols},
		FileEntry{Tier: "base", Path: "s3://other/abs.parquet", Columns: absCols},
		FileEntry{Tier: "delta", Path: "legacy.parquet"},
	))

	paths, stamps, err := src.Paths(context.Background(), 7)
	require.NoError(t, err)
	// Path order and content are unchanged by stamping.
	require.Equal(t, []string{
		"s3://bkt/rel.parquet", "s3://other/abs.parquet", "s3://bkt/legacy.parquet",
	}, paths)

	require.Equal(t, relCols, stamps["s3://bkt/rel.parquet"])
	require.Equal(t, absCols, stamps["s3://other/abs.parquet"])
	require.Len(t, stamps, 2, "the unstamped legacy entry must contribute no key")
	require.NotContains(t, stamps, "s3://bkt/legacy.parquet")
}

// TestPathsFallbackGlobHasNoStamps: the fallback glob names no manifest entry,
// so there is nothing to stamp — the validator expands and probes it as today.
func TestPathsFallbackGlobHasNoStamps(t *testing.T) {
	src := testQuerySource(storeWithManifest(t, 7))
	src.Fallback = func(schemaID int16) string { return fmt.Sprintf("s3://bkt/p/%d/*.parquet", schemaID) }

	paths, stamps, err := src.Paths(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, []string{"s3://bkt/p/7/*.parquet"}, paths)
	require.Nil(t, stamps)
}

func TestQuerySource_MissingInProbesScannedSet(t *testing.T) {
	src := testQuerySource(storeWithManifest(t, 7, "p/7/a.parquet", "p/7/gone.parquet"))
	src.Exists = func(ctx context.Context, key string) (bool, error) {
		return key == "p/7/a.parquet", nil
	}

	// Probing is over the scanned URIs, never a manifest reload: entries a
	// concurrent flush added to the manifest after the failed scan must not
	// influence classification.
	missing, err := src.MissingIn(context.Background(),
		[]string{"s3://bkt/p/7/a.parquet", "s3://bkt/p/7/gone.parquet"})
	require.NoError(t, err)
	require.Equal(t, []string{"p/7/gone.parquet"}, missing)
}

func TestQuerySource_MissingInSkipsGlobsAndForeignBuckets(t *testing.T) {
	probed := []string{}
	src := testQuerySource(&memStore{})
	src.Exists = func(ctx context.Context, key string) (bool, error) {
		probed = append(probed, key)
		return false, nil
	}

	// A glob (never-flushed fallback) and a foreign-bucket URI are both
	// unprovable with the configured probe — they must not fabricate
	// inconsistency.
	missing, err := src.MissingIn(context.Background(),
		[]string{"s3://bkt/p/7/*.parquet", "s3://otherbkt/base.parquet"})
	require.NoError(t, err)
	require.Empty(t, missing)
	require.Empty(t, probed)
}

func TestQuerySource_MissingInProbeErrorPropagates(t *testing.T) {
	src := testQuerySource(&memStore{})
	src.Exists = func(ctx context.Context, key string) (bool, error) {
		return false, fmt.Errorf("connection refused")
	}

	_, err := src.MissingIn(context.Background(), []string{"s3://bkt/p/7/a.parquet"})
	require.Error(t, err)
	require.ErrorContains(t, err, "probe parquet object p/7/a.parquet")
}

func TestQuerySource_MissingInWithoutProbeReportsNone(t *testing.T) {
	src := testQuerySource(&memStore{})

	missing, err := src.MissingIn(context.Background(), []string{"s3://bkt/p/7/a.parquet"})
	require.NoError(t, err)
	require.Empty(t, missing)
}
