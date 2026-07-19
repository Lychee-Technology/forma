package reconcile

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifest"
)

// memManifestStore is an in-memory manifest.Store: not-found until first
// save, etag bumps per save (mirrors internal/cdc/init_test.go).
type memManifestStore struct {
	data  map[string][]byte
	etags map[string]string
	saves int
}

func newMemManifestStore() *memManifestStore {
	return &memManifestStore{data: map[string][]byte{}, etags: map[string]string{}}
}

func (s *memManifestStore) Load(_ context.Context, path string) ([]byte, string, error) {
	b, ok := s.data[path]
	if !ok {
		return nil, "", fmt.Errorf("NoSuchKey: manifest %s not found", path)
	}
	return b, s.etags[path], nil
}

func (s *memManifestStore) Save(_ context.Context, path string, data []byte, _ string) (string, error) {
	s.saves++
	s.data[path] = data
	etag := fmt.Sprintf("v%d", s.saves)
	s.etags[path] = etag
	return etag, nil
}

func TestResolverManifestStore_MissingManifestLoadsEmpty(t *testing.T) {
	store := &ResolverManifestStore{
		Store:    newMemManifestStore(),
		Resolver: manifest.PathResolver{PathTemplate: "manifest/{{.SchemaID}}.json"},
	}
	m, etag, err := store.Load(context.Background(), 7)
	require.NoError(t, err, "a schema without a manifest must reconcile as empty")
	require.Equal(t, int16(7), m.SchemaID)
	require.Empty(t, m.Files)
	require.Empty(t, etag)
}

func TestResolverManifestStore_SaveRoundTrip(t *testing.T) {
	store := &ResolverManifestStore{
		Store:    newMemManifestStore(),
		Resolver: manifest.PathResolver{PathTemplate: "manifest/{{.SchemaID}}.json"},
	}
	ctx := context.Background()

	m, etag, err := store.Load(ctx, 7)
	require.NoError(t, err)
	m.Files = append(m.Files, manifest.FileEntry{Tier: "delta", Path: "data/7/a.parquet"})
	_, err = store.Save(ctx, 7, m, etag)
	require.NoError(t, err)

	reloaded, etag2, err := store.Load(ctx, 7)
	require.NoError(t, err)
	require.NotEmpty(t, etag2)
	require.Len(t, reloaded.Files, 1)
	require.Equal(t, "data/7/a.parquet", reloaded.Files[0].Path)
	require.Greater(t, reloaded.Version, int64(0), "manifest.Save must bump the version")
}
