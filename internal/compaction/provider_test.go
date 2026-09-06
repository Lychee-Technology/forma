package compaction

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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

// memManifestStore is a loadable in-memory manifest.Store: recordingManifestStore
// only records saves, so the #520 identity check needs one that can load.
type memManifestStore struct {
	data  map[string][]byte
	saves int
}

func (s *memManifestStore) Load(_ context.Context, path string) ([]byte, string, error) {
	b, ok := s.data[path]
	if !ok {
		return nil, "", fmt.Errorf("mem store %s: %w", path, manifest.ErrObjectNotFound)
	}
	return b, "etag", nil
}

func (s *memManifestStore) Save(_ context.Context, path string, data []byte, _ string) (string, error) {
	if s.data == nil {
		s.data = map[string][]byte{}
	}
	s.data[path] = append([]byte(nil), data...)
	s.saves++
	return fmt.Sprintf("etag-%d", s.saves), nil
}

// A fixed-file template collapses every schema onto one manifest. Once that
// manifest is stamped for schema 2, the compactor must not swap schema 1's
// tiers on it (#520) while schema 2 keeps loading it. The typed error is
// returned unwrapped so the compactor's "load manifest: %w" wrap is the only
// layer above it.
func TestManifestProvider_LoadManifest_RejectsForeignSchema(t *testing.T) {
	ctx := context.Background()
	store := &memManifestStore{}
	provider := NewManifestProvider(cdc.ManifestConfig{PathTemplate: "manifest/shared.json"}, store)
	_, err := manifest.Save(ctx, store, "manifest/shared.json", &manifest.Manifest{SchemaID: 2}, "")
	require.NoError(t, err)

	_, _, err = provider.LoadManifest(ctx, 1)
	require.ErrorIs(t, err, forma.ErrManifestSchemaMismatch)
	var mismatch *forma.ManifestSchemaMismatchError
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, int16(1), mismatch.RequestedSchemaID)
	require.Equal(t, int16(2), mismatch.ManifestSchemaID)
	require.Equal(t, "manifest/shared.json", mismatch.Path)

	m, _, err := provider.LoadManifest(ctx, 2)
	require.NoError(t, err, "the manifest still loads for the schema it is stamped for")
	require.Equal(t, int16(2), m.SchemaID)
}

// #522: a zero stamp proves nothing about ownership, and the compactor swaps
// a schema's tiers on the strength of the manifest's identity. A zero-stamped
// manifest that lists entries is refused for every schema, as the
// schema-mismatch species whose remedy is naming the owner on the object.
func TestManifestProvider_LoadManifest_RejectsUnstampedWithEntries(t *testing.T) {
	store := &memManifestStore{data: map[string][]byte{
		"manifest/1.json": []byte(`{"files":[{"tier":"delta","path":"data/1/a.parquet"}]}`),
	}}
	provider := NewManifestProvider(cdc.ManifestConfig{PathTemplate: "manifest/{{.SchemaID}}.json"}, store)

	_, _, err := provider.LoadManifest(context.Background(), 1)
	require.ErrorIs(t, err, forma.ErrManifestUnstamped)
	require.ErrorIs(t, err, forma.ErrManifestSchemaMismatch)
	var unstamped *forma.ManifestUnstampedError
	require.ErrorAs(t, err, &unstamped)
	require.Equal(t, int16(1), unstamped.RequestedSchemaID)
	require.Equal(t, "manifest/1.json", unstamped.Path)
	require.Equal(t, 1, unstamped.Entries)
}

// An empty zero-stamped manifest has nothing another schema could own: it
// loads stamped for the requested schema in memory, so the compactor's save
// persists the stamp.
func TestManifestProvider_LoadManifest_StampsEmptyUnstamped(t *testing.T) {
	store := &memManifestStore{data: map[string][]byte{
		"manifest/1.json": []byte(`{"files":[]}`),
	}}
	provider := NewManifestProvider(cdc.ManifestConfig{PathTemplate: "manifest/{{.SchemaID}}.json"}, store)

	m, _, err := provider.LoadManifest(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, int16(1), m.SchemaID)
	require.Empty(t, m.Files)
}

// End to end through the compactor: RunOnce over a foreign manifest fails
// before any tier analysis and saves nothing.
func TestCompactor_RunOnce_ForeignManifestFailsWithoutSave(t *testing.T) {
	ctx := context.Background()
	store := &memManifestStore{}
	_, err := manifest.Save(ctx, store, "manifest/1.json", &manifest.Manifest{
		SchemaID: 2,
		Files: []manifest.FileEntry{
			{Tier: "base", Path: "data/2/base.parquet", RowCount: 100, SizeBytes: 1 << 20},
			{Tier: "delta", Path: "data/2/delta.parquet", RowCount: 100, SizeBytes: 1 << 20},
		},
	}, "")
	require.NoError(t, err)
	savesBefore := store.saves

	c := &Compactor{
		Logger:   zap.NewNop(),
		Config:   cdc.CompactionConfig{SchemaID: 1}.WithDefaults(),
		Provider: NewManifestProvider(cdc.ManifestConfig{PathTemplate: "manifest/{{.SchemaID}}.json"}, store),
	}
	_, err = c.RunOnce(ctx)
	require.ErrorIs(t, err, forma.ErrManifestSchemaMismatch)
	require.Equal(t, savesBefore, store.saves, "a refused compaction must not save")
}
