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

	paths, err := src.Paths(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, []string{"s3://bkt/p/7/a.parquet", "s3://bkt/p/7/b.parquet"}, paths)
}

func TestQuerySource_MissingManifestFallsBackToGlob(t *testing.T) {
	src := testQuerySource(&memStore{})
	src.Fallback = func(schemaID int16) string { return fmt.Sprintf("s3://bkt/p/%d/*.parquet", schemaID) }

	paths, err := src.Paths(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, []string{"s3://bkt/p/7/*.parquet"}, paths)
}

func TestQuerySource_MissingManifestWithoutFallbackIsEmpty(t *testing.T) {
	src := testQuerySource(&memStore{})

	paths, err := src.Paths(context.Background(), 7)
	require.NoError(t, err)
	require.Empty(t, paths)
}

func TestQuerySource_EmptyManifestFallsBackToGlob(t *testing.T) {
	src := testQuerySource(storeWithManifest(t, 7))
	src.Fallback = func(schemaID int16) string { return fmt.Sprintf("s3://bkt/p/%d/*.parquet", schemaID) }

	paths, err := src.Paths(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, []string{"s3://bkt/p/7/*.parquet"}, paths)
}

func TestQuerySource_PathsPassesURIEntriesUnchanged(t *testing.T) {
	// The manifest format accepts absolute s3:// entries (see
	// TestManifest_Parse); they must not be double-prefixed with the bucket.
	src := testQuerySource(storeWithManifest(t, 7, "p/7/a.parquet", "s3://otherbkt/base.parquet"))

	paths, err := src.Paths(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, []string{"s3://bkt/p/7/a.parquet", "s3://otherbkt/base.parquet"}, paths)
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
