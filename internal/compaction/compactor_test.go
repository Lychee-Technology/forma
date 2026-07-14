package compaction

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/lychee-technology/forma/internal/telemetry"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type telemetryEvent struct {
	name   string
	labels map[string]string
	value  any
}

// mockProvider implements FileProvider for testing
type mockProvider struct {
	manifest             *manifest.Manifest
	etag                 string
	loadErr              error
	saveErr              error
	savedEtag            string
	skipVersionAdvance   bool
	skipUpdatedAtAdvance bool
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
		if !m.skipUpdatedAtAdvance {
			nextUpdatedAtMs := time.Now().UnixMilli()
			if nextUpdatedAtMs <= mani.UpdatedAtMs {
				nextUpdatedAtMs = mani.UpdatedAtMs + 1
			}
			mani.UpdatedAtMs = nextUpdatedAtMs
		}
		if !m.skipVersionAdvance {
			if mani.Version == 0 {
				mani.Version = 1
			} else {
				mani.Version++
			}
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

	result, err := c.RunOnce(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "schema_id required")
	require.Empty(t, result.Outcome)
}

func TestCompactor_RunOnce_RequiresProvider(t *testing.T) {
	logger := zap.NewNop()
	c := &Compactor{
		Logger: logger,
		Config: cdc.CompactionConfig{SchemaID: 1},
	}

	result, err := c.RunOnce(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "file provider is nil")
	require.Empty(t, result.Outcome)
}

func TestCompactor_RunOnce_NoManifest(t *testing.T) {
	logger := zap.NewNop()
	c := &Compactor{
		Logger:   logger,
		Config:   cdc.CompactionConfig{SchemaID: 1},
		Provider: &mockProvider{manifest: nil},
	}

	result, err := c.RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, Noop, result.Outcome)
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

	result, err := c.RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, Noop, result.Outcome)
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

	result, err := c.RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, PromotionApplied, result.Outcome)

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

	result, err := c.RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, PromotionApplied, result.Outcome)
	require.Equal(t, "new-etag", provider.savedEtag)
	require.Equal(t, int64(2), provider.manifest.Version)
}

func TestCompactor_RunOnce_PromotesSubMBDeltasWithByteThreshold(t *testing.T) {
	logger := zap.NewNop()
	// A KB-scale delta tier truncates to 0 MB, so the MB threshold can never
	// fire on it; the byte-precise TargetBaseSizeBytes must (#188).
	m := &manifest.Manifest{
		SchemaID:    1,
		Version:     1,
		UpdatedAtMs: time.Now().UnixMilli(),
		Files: []manifest.FileEntry{
			{Tier: "delta", Path: "delta1.parquet", SizeBytes: 4 * 1024, RowCount: 10},
		},
	}
	provider := &mockProvider{manifest: m, etag: "etag-1"}

	c := &Compactor{
		Logger: logger,
		Config: cdc.CompactionConfig{
			SchemaID:            1,
			TargetBaseSizeBytes: 1024,
		},
		Provider: provider,
	}

	result, err := c.RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, PromotionApplied, result.Outcome)
	require.Equal(t, "base", provider.manifest.Files[0].Tier)
	require.Equal(t, int64(2), provider.manifest.Version)
}

func TestCompactionConfig_WithDefaults_DerivesTargetBaseSizeBytes(t *testing.T) {
	derived := cdc.CompactionConfig{TargetBaseSizeMB: 2}.WithDefaults()
	require.Equal(t, int64(2)<<20, derived.TargetBaseSizeBytes)

	explicit := cdc.CompactionConfig{TargetBaseSizeBytes: 512}.WithDefaults()
	require.Equal(t, int64(512), explicit.TargetBaseSizeBytes)
	require.Equal(t, cdc.DefaultTargetBaseSizeMB, explicit.TargetBaseSizeMB)
}

func TestCompactor_RunOnce_SaveManifestMissingVersionAdvance(t *testing.T) {
	logger := zap.NewNop()
	m := &manifest.Manifest{
		SchemaID:    1,
		Version:     1,
		UpdatedAtMs: time.Now().Add(-time.Minute).UnixMilli(),
		Files: []manifest.FileEntry{
			{Tier: "delta", Path: "delta.parquet", SizeBytes: 256 * 1024 * 1024, RowCount: 1000},
		},
	}
	provider := &mockProvider{
		manifest:           m,
		etag:               "etag-1",
		skipVersionAdvance: true,
	}

	c := &Compactor{
		Logger: logger,
		Config: cdc.CompactionConfig{
			SchemaID:         1,
			TargetBaseSizeMB: 256,
		},
		Provider: provider,
	}

	result, err := c.RunOnce(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, ErrManifestMetadataContractViolation)
	require.Contains(t, err.Error(), "version")
	require.Empty(t, result.Outcome)
}

func TestCompactor_RunOnce_SaveManifestMissingUpdatedAtAdvance(t *testing.T) {
	logger := zap.NewNop()
	m := &manifest.Manifest{
		SchemaID:    1,
		Version:     1,
		UpdatedAtMs: time.Now().Add(-time.Minute).UnixMilli(),
		Files: []manifest.FileEntry{
			{Tier: "delta", Path: "delta.parquet", SizeBytes: 256 * 1024 * 1024, RowCount: 1000},
		},
	}
	provider := &mockProvider{
		manifest:             m,
		etag:                 "etag-1",
		skipUpdatedAtAdvance: true,
	}

	c := &Compactor{
		Logger: logger,
		Config: cdc.CompactionConfig{
			SchemaID:         1,
			TargetBaseSizeMB: 256,
		},
		Provider: provider,
	}

	result, err := c.RunOnce(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, ErrManifestMetadataContractViolation)
	require.Contains(t, err.Error(), "updated_at_ms")
	require.Empty(t, result.Outcome)
}

func TestCompactor_RunOnce_SaveManifestContractViolationEmitsTelemetry(t *testing.T) {
	t.Cleanup(func() { telemetry.RegisterTelemetryEmitter(nil) })

	var gotName string
	var gotLabels map[string]string
	var gotValue any
	telemetry.RegisterTelemetryEmitter(func(ctx context.Context, name string, labels map[string]string, value any) {
		gotName = name
		gotLabels = labels
		gotValue = value
	})

	logger := zap.NewNop()
	m := &manifest.Manifest{
		SchemaID:    1,
		Version:     1,
		UpdatedAtMs: time.Now().Add(-time.Minute).UnixMilli(),
		Files: []manifest.FileEntry{
			{Tier: "delta", Path: "delta.parquet", SizeBytes: 256 * 1024 * 1024, RowCount: 1000},
		},
	}
	provider := &mockProvider{
		manifest:           m,
		etag:               "etag-1",
		skipVersionAdvance: true,
	}
	c := &Compactor{
		Logger: logger,
		Config: cdc.CompactionConfig{
			SchemaID:         1,
			TargetBaseSizeMB: 256,
		},
		Provider: provider,
	}

	_, err := c.RunOnce(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, ErrManifestMetadataContractViolation)
	require.Equal(t, "compaction_manifest_contract_violation_total", gotName)
	require.Equal(t, "1", gotLabels["schema_id"])
	require.Equal(t, int64(1), gotValue)
}

// A rewrite-eligible pass on a compactor WITHOUT merge wiring (nil Merger —
// e.g. a manifest-only invocation) must keep the pre-#188 stub contract:
// RewritePending, manifest untouched, pending telemetry.
func TestCompactor_RunOnce_RewriteWithoutMergeWiring_ReportsPending(t *testing.T) {
	t.Cleanup(func() { telemetry.RegisterTelemetryEmitter(nil) })

	events := make([]telemetryEvent, 0, 2)
	telemetry.RegisterTelemetryEmitter(func(ctx context.Context, name string, labels map[string]string, value any) {
		events = append(events, telemetryEvent{name: name, labels: labels, value: value})
	})

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

	result, err := c.RunOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, RewritePending, result.Outcome)

	require.Equal(t, "", provider.savedEtag)
	require.Equal(t, int64(1), provider.manifest.Version)

	require.Equal(t, "base", provider.manifest.Files[0].Tier)
	require.Equal(t, "delta", provider.manifest.Files[1].Tier)

	require.Len(t, events, 2)
	require.Equal(t, "compaction_dirty_ratio", events[0].name)
	require.Equal(t, "1", events[0].labels["schema_id"])
	require.Equal(t, 0.1, events[0].value)
	require.Equal(t, "compaction_rewrite_pending_total", events[1].name)
	require.Equal(t, "1", events[1].labels["schema_id"])
	require.Equal(t, int64(1), events[1].value)
}

func TestCompactor_RunOnce_LoadManifestError(t *testing.T) {
	logger := zap.NewNop()
	provider := &mockProvider{loadErr: errors.New("load failed")}

	c := &Compactor{
		Logger:   logger,
		Config:   cdc.CompactionConfig{SchemaID: 1},
		Provider: provider,
	}

	_, err := c.RunOnce(context.Background())
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
