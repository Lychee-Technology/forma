package cdc

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestUpdateManifest_NilStore(t *testing.T) {
	rowID := uuid.MustParse("018f05c0-0000-7000-8000-000000000001")
	err := updateManifest(
		context.Background(),
		nil,
		manifest.PathResolver{},
		1,
		"cdc/1/file.parquet",
		"delta",
		[]uuid.UUID{rowID},
		1700000000000,
		0,
		zap.NewNop(),
	)
	require.NoError(t, err)
}

func TestUpdateManifest_AppendsEntryWithRowBounds(t *testing.T) {
	ctx := context.Background()
	store := newInMemoryManifestStore()
	resolver := manifest.PathResolver{
		Prefix:       "cdc",
		PathTemplate: "manifest/{{.SchemaID}}.json",
	}

	idOldest := uuid.MustParse("018f05c0-0000-7000-8000-000000000001")
	idMiddle := uuid.MustParse("018f05c0-0001-7000-8000-000000000001")
	idNewest := uuid.MustParse("018f05c0-0002-7000-8000-000000000001")
	rowIDs := []uuid.UUID{idMiddle, idNewest, idOldest}
	createdAt := int64(1700000000000)

	err := updateManifest(
		ctx,
		store,
		resolver,
		7,
		"cdc/7/delta-file.parquet",
		"delta",
		rowIDs,
		createdAt,
		4096,
		zap.NewNop(),
	)
	require.NoError(t, err)

	manifestPath, err := resolver.Resolve(7)
	require.NoError(t, err)

	payload := store.data[manifestPath]
	require.NotEmpty(t, payload)

	m, err := manifest.Parse(payload)
	require.NoError(t, err)
	require.Equal(t, int16(7), m.SchemaID)
	require.Len(t, m.Files, 1)

	entry := m.Files[0]
	expectedMin, expectedMax := minMaxRowID(rowIDs)
	require.Equal(t, "delta", entry.Tier)
	require.Equal(t, "cdc/7/delta-file.parquet", entry.Path)
	require.Equal(t, int64(3), entry.RowCount)
	require.Equal(t, createdAt, entry.CreatedMin)
	require.Equal(t, createdAt, entry.CreatedMax)
	require.Equal(t, expectedMin.String(), entry.RowIDMin)
	require.Equal(t, expectedMax.String(), entry.RowIDMax)
	require.Equal(t, int64(4096), entry.SizeBytes)
}

func TestUpdateManifest_ResolverError(t *testing.T) {
	err := updateManifest(
		context.Background(),
		newInMemoryManifestStore(),
		manifest.PathResolver{PathTemplate: "{{"},
		1,
		"cdc/1/file.parquet",
		"delta",
		nil,
		1700000000000,
		0,
		zap.NewNop(),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "resolve manifest path")
}

func TestUpdateManifest_AppendError(t *testing.T) {
	store := newInMemoryManifestStore()
	store.loadErr = errors.New("boom")

	err := updateManifest(
		context.Background(),
		store,
		manifest.PathResolver{},
		1,
		"cdc/1/file.parquet",
		"delta",
		nil,
		1700000000000,
		0,
		zap.NewNop(),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "append to manifest")
}
