package compaction

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/stretchr/testify/require"
)

type recordingManifestStore struct {
	savedPath string
	savedData []byte
	savedETag string
	nextETag  string
	saveErr   error
}

func (s *recordingManifestStore) Load(ctx context.Context, path string) ([]byte, string, error) {
	return nil, "", errors.New("not implemented")
}

func (s *recordingManifestStore) Save(ctx context.Context, path string, data []byte, etag string) (string, error) {
	s.savedPath = path
	s.savedData = append([]byte(nil), data...)
	s.savedETag = etag
	if s.saveErr != nil {
		return "", s.saveErr
	}
	if s.nextETag == "" {
		return "etag-next", nil
	}
	return s.nextETag, nil
}

func TestManifestProvider_SaveManifest_AdvancesMetadataAndMutatesInput(t *testing.T) {
	store := &recordingManifestStore{nextETag: "etag-2"}
	provider := NewManifestProvider(cdc.ManifestConfig{
		Prefix:       "tenant-a",
		PathTemplate: "manifest/{{.SchemaID}}.json",
	}, store)

	initialVersion := int64(7)
	initialUpdatedAtMs := time.Now().Add(-time.Minute).UnixMilli()
	m := &manifest.Manifest{
		SchemaID:    42,
		Version:     initialVersion,
		UpdatedAtMs: initialUpdatedAtMs,
		Files: []manifest.FileEntry{
			{Tier: "base", Path: "base/file.parquet", RowCount: 10},
		},
	}

	newETag, err := provider.SaveManifest(context.Background(), 42, m, "etag-1")
	require.NoError(t, err)
	require.Equal(t, "etag-2", newETag)
	require.Equal(t, "tenant-a/manifest/42.json", store.savedPath)
	require.Equal(t, "etag-1", store.savedETag)

	// Contract: SaveManifest must update the same manifest pointer in-place.
	require.Equal(t, initialVersion+1, m.Version)
	require.Greater(t, m.UpdatedAtMs, initialUpdatedAtMs)

	persisted, err := manifest.Parse(store.savedData)
	require.NoError(t, err)
	require.Equal(t, m.SchemaID, persisted.SchemaID)
	require.Equal(t, m.Version, persisted.Version)
	require.Equal(t, m.UpdatedAtMs, persisted.UpdatedAtMs)
	require.Equal(t, m.Files, persisted.Files)
}
