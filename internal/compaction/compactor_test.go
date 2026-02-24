package compaction

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// mockProvider implements FileProvider for testing
type mockProvider struct {
	manifest  *manifest.Manifest
	etag      string
	loadErr   error
	saveErr   error
	savedEtag string
}

func (m *mockProvider) LoadManifest(ctx context.Context, schemaID int16) (*manifest.Manifest, string, error) {
	if m.loadErr != nil {
		return nil, "", m.loadErr
	}
	return m.manifest, m.etag, nil
}

func (m *mockProvider) SaveManifest(ctx context.Context, schemaID int16, mani *manifest.Manifest, etag string) (string, error) {
	if m.saveErr != nil {
		return "", m.saveErr
	}
	if mani != nil {
		mani.UpdatedAtMs = time.Now().UnixMilli()
		if mani.Version == 0 {
			mani.Version = 1
		} else {
			mani.Version++
		}
	}
	m.manifest = mani
	m.savedEtag = "new-etag"
	return m.savedEtag, nil
}

func TestCompactor_RunOnce_RequiresSchemaID(t *testing.T) {
	logger := zap.NewNop()
	c := &Compactor{
		Logger:   logger,
		Config:   cdc.CompactionConfig{SchemaID: 0},
		Provider: &mockProvider{},
	}

	err := c.RunOnce(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "schema_id required")
}

func TestCompactor_RunOnce_RequiresProvider(t *testing.T) {
	logger := zap.NewNop()
	c := &Compactor{
		Logger: logger,
		Config: cdc.CompactionConfig{SchemaID: 1},
	}

	err := c.RunOnce(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "file provider is nil")
}

func TestCompactor_RunOnce_NoManifest(t *testing.T) {
	logger := zap.NewNop()
	c := &Compactor{
		Logger:   logger,
		Config:   cdc.CompactionConfig{SchemaID: 1},
		Provider: &mockProvider{manifest: nil},
	}

	err := c.RunOnce(context.Background())
	require.NoError(t, err) // no manifest = nothing to compact
}

func TestCompactor_RunOnce_NoFilesToCompact(t *testing.T) {
	logger := zap.NewNop()
	m := &manifest.Manifest{
		SchemaID:    1,
		Version:     1,
		UpdatedAtMs: time.Now().UnixMilli(),
		Files:       []manifest.FileEntry{},
	}
	provider := &mockProvider{manifest: m, etag: "etag-1"}

	c := &Compactor{
		Logger:   logger,
		Config:   cdc.CompactionConfig{SchemaID: 1},
		Provider: provider,
	}

	err := c.RunOnce(context.Background())
	require.NoError(t, err)
}

func TestCompactor_RunOnce_PromotsDeltas(t *testing.T) {
	logger := zap.NewNop()
	// Create manifest with enough delta files to trigger promotion
	// (totalDeltaMB >= targetBaseSizeMB)
	m := &manifest.Manifest{
		SchemaID:    1,
		Version:     1,
		UpdatedAtMs: time.Now().UnixMilli(),
		Files: []manifest.FileEntry{
			{Tier: "delta", Path: "delta1.parquet", SizeBytes: 128 * 1024 * 1024, RowCount: 1000},
			{Tier: "delta", Path: "delta2.parquet", SizeBytes: 128 * 1024 * 1024, RowCount: 1000},
		},
	}
	provider := &mockProvider{manifest: m, etag: "etag-1"}

	c := &Compactor{
		Logger: logger,
		Config: cdc.CompactionConfig{
			SchemaID:         1,
			TargetBaseSizeMB: 256, // 256 MB, and we have 256 MB total delta
		},
		Provider: provider,
	}

	err := c.RunOnce(context.Background())
	require.NoError(t, err)

	// Check that files were promoted to base tier
	for _, f := range provider.manifest.Files {
		require.Equal(t, "base", f.Tier)
	}
	require.Equal(t, "new-etag", provider.savedEtag)
	require.Equal(t, int64(2), provider.manifest.Version)
}

func TestCompactor_RunOnce_PromotesDeltasCaseInsensitiveTier(t *testing.T) {
	logger := zap.NewNop()
	// FilterByTier is case-insensitive, so compactor must also promote DELTA entries.
	m := &manifest.Manifest{
		SchemaID:    1,
		Version:     1,
		UpdatedAtMs: time.Now().UnixMilli(),
		Files: []manifest.FileEntry{
			{Tier: "DELTA", Path: "delta_upper.parquet", SizeBytes: 256 * 1024 * 1024, RowCount: 1000},
		},
	}
	provider := &mockProvider{manifest: m, etag: "etag-1"}

	c := &Compactor{
		Logger: logger,
		Config: cdc.CompactionConfig{
			SchemaID:         1,
			TargetBaseSizeMB: 256,
		},
		Provider: provider,
	}

	err := c.RunOnce(context.Background())
	require.NoError(t, err)

	require.Equal(t, "base", provider.manifest.Files[0].Tier)
	require.Equal(t, "new-etag", provider.savedEtag)
	require.Equal(t, int64(2), provider.manifest.Version)
}

func TestCompactor_RunOnce_NeedsRewriteWithoutPromotion_SkipsManifestUpdate(t *testing.T) {
	logger := zap.NewNop()
	// Force needsRewrite=true (delta/base rows = 100/1000 = 10% > 5%)
	// while keeping needsPromotion=false (10MB < 256MB).
	m := &manifest.Manifest{
		SchemaID:    1,
		Version:     1,
		UpdatedAtMs: time.Now().UnixMilli(),
		Files: []manifest.FileEntry{
			{Tier: "base", Path: "base.parquet", SizeBytes: 300 * 1024 * 1024, RowCount: 1000},
			{Tier: "delta", Path: "delta.parquet", SizeBytes: 10 * 1024 * 1024, RowCount: 100},
		},
	}
	provider := &mockProvider{manifest: m, etag: "etag-1"}

	c := &Compactor{
		Logger: logger,
		Config: cdc.CompactionConfig{
			SchemaID:         1,
			TargetBaseSizeMB: 256,
			DirtyRatioPct:    5,
		},
		Provider: provider,
	}

	err := c.RunOnce(context.Background())
	require.NoError(t, err)

	// Rewrite is not implemented yet, so manifest should not be persisted.
	require.Equal(t, "", provider.savedEtag)
	require.Equal(t, int64(1), provider.manifest.Version)

	// No rewrite/promotion happened in current implementation.
	require.Equal(t, "base", provider.manifest.Files[0].Tier)
	require.Equal(t, "delta", provider.manifest.Files[1].Tier)
}

func TestCompactor_RunOnce_LoadManifestError(t *testing.T) {
	logger := zap.NewNop()
	provider := &mockProvider{loadErr: errors.New("load failed")}

	c := &Compactor{
		Logger:   logger,
		Config:   cdc.CompactionConfig{SchemaID: 1},
		Provider: provider,
	}

	err := c.RunOnce(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "load manifest")
}

func TestCompactor_ComputeDirtyRatio(t *testing.T) {
	logger := zap.NewNop()
	c := &Compactor{Logger: logger}

	baseFiles := []manifest.FileEntry{
		{Tier: "base", RowCount: 1000},
	}
	deltaFiles := []manifest.FileEntry{
		{Tier: "delta", RowCount: 100},
	}

	ratio := c.computeDirtyRatio(baseFiles, deltaFiles)
	require.InDelta(t, 0.1, ratio, 0.001) // 100/1000 = 0.1
}

func TestCompactor_ComputeDirtyRatio_NoBase(t *testing.T) {
	logger := zap.NewNop()
	c := &Compactor{Logger: logger}

	baseFiles := []manifest.FileEntry{}
	deltaFiles := []manifest.FileEntry{
		{Tier: "delta", RowCount: 100},
	}

	ratio := c.computeDirtyRatio(baseFiles, deltaFiles)
	require.Equal(t, 0.0, ratio) // no base = ratio 0
}

func TestComputeBackoff(t *testing.T) {
	base := 100 * time.Millisecond
	max := 10 * time.Second

	// First attempt: ~100ms
	b0 := computeBackoff(0, base, max)
	require.GreaterOrEqual(t, b0, 50*time.Millisecond)
	require.LessOrEqual(t, b0, 150*time.Millisecond)

	// Third attempt: ~400ms
	b2 := computeBackoff(2, base, max)
	require.GreaterOrEqual(t, b2, 200*time.Millisecond)
	require.LessOrEqual(t, b2, 600*time.Millisecond)

	// 10th attempt should be capped at max
	b10 := computeBackoff(10, base, max)
	require.LessOrEqual(t, b10, 15*time.Second) // max + jitter
}

func TestIsRetryable(t *testing.T) {
	require.True(t, isRetryable(ErrConcurrentModification))
	require.True(t, isRetryable(errors.Join(ErrConcurrentModification, errors.New("other"))))
	require.False(t, isRetryable(errors.New("some other error")))
}
