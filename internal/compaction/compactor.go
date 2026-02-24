package compaction

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifest"
	"go.uber.org/zap"
)

// ErrConcurrentModification indicates manifest was modified by another process.
var ErrConcurrentModification = errors.New("manifest concurrently modified")

// FileProvider fetches manifest and lists actual files (optionally cross-check).
type FileProvider interface {
	LoadManifest(ctx context.Context, schemaID int16) (*manifest.Manifest, string, error)
	SaveManifest(ctx context.Context, schemaID int16, m *manifest.Manifest, etag string) (string, error)
}

type S3Copier interface {
	CopyObject(ctx context.Context, srcKey, dstKey string) error
	DeleteObject(ctx context.Context, key string) error
}

// Compactor performs Base/Delta maintenance per schema.
type Compactor struct {
	Logger     *zap.Logger
	Config     cdc.CompactionConfig
	Provider   FileProvider
	Copier     S3Copier
	Bucket     string
	DataPrefix string // root prefix for parquet files
	Resolver   manifest.PathResolver
}

// RunOnce executes compaction for a schema (or all if Config.SchemaID==0).
func (c *Compactor) RunOnce(ctx context.Context) error {
	if c.Provider == nil {
		return fmt.Errorf("file provider is nil")
	}
	schemas := []int16{c.Config.SchemaID}
	if c.Config.SchemaID == 0 {
		// For MVP, require explicit schema; all-schema sweep can be added later.
		return fmt.Errorf("schema_id required for compaction in MVP")
	}

	cfg := c.Config.WithDefaults()
	for _, sid := range schemas {
		if sid == 0 {
			continue
		}
		if err := c.compactSchemaWithRetry(ctx, sid, cfg); err != nil {
			return err
		}
	}
	return nil
}

// compactSchemaWithRetry wraps compactSchema with exponential backoff for transient errors.
func (c *Compactor) compactSchemaWithRetry(ctx context.Context, schemaID int16, cfg cdc.CompactionConfig) error {
	var lastErr error
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		err := c.compactSchema(ctx, schemaID, cfg)
		if err == nil {
			return nil
		}
		lastErr = err

		// Don't retry if context cancelled or non-retryable
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !isRetryable(err) {
			return err
		}

		if attempt < cfg.MaxRetries {
			backoff := computeBackoff(attempt, cfg.BaseBackoff, cfg.MaxBackoff)
			c.Logger.Warn("compaction failed, retrying",
				zap.Int16("schema_id", schemaID),
				zap.Int("attempt", attempt+1),
				zap.Duration("backoff", backoff),
				zap.Error(err))

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	return fmt.Errorf("compaction failed after %d retries: %w", cfg.MaxRetries, lastErr)
}

// computeBackoff returns exponential backoff with jitter.
func computeBackoff(attempt int, base, max time.Duration) time.Duration {
	// 2^attempt * base, capped at max
	backoff := min(base*(1<<attempt), max)
	// Add jitter: 0.5 to 1.5 of computed backoff
	jitter := 0.5 + rand.Float64()
	return time.Duration(float64(backoff) * jitter)
}

// isRetryable determines if an error warrants a retry.
func isRetryable(err error) bool {
	// Concurrent modification is retryable (optimistic concurrency conflict)
	if errors.Is(err, ErrConcurrentModification) {
		return true
	}
	// Could extend to check for transient S3 errors, network timeouts, etc.
	return false
}

func (c *Compactor) compactSchema(ctx context.Context, schemaID int16, cfg cdc.CompactionConfig) error {
	m, etag, err := c.Provider.LoadManifest(ctx, schemaID)
	if err != nil {
		return fmt.Errorf("load manifest: %w", err)
	}
	if m == nil {
		// No manifest yet means no data to compact
		c.Logger.Debug("no manifest for schema, skipping", zap.Int16("schema_id", schemaID))
		return nil
	}

	// Separate base and delta files
	baseFiles := manifest.FilterByTier(m, "base")
	deltaFiles := manifest.FilterByTier(m, "delta")

	sortFilesByRowID(baseFiles)
	sortFilesByRowID(deltaFiles)

	c.Logger.Debug("compaction analysis",
		zap.Int16("schema_id", schemaID),
		zap.Int("base_files", len(baseFiles)),
		zap.Int("delta_files", len(deltaFiles)))

	// Calculate total sizes
	var baseTotalBytes, deltaTotalBytes int64
	for _, f := range baseFiles {
		baseTotalBytes += f.SizeBytes
	}
	for _, f := range deltaFiles {
		deltaTotalBytes += f.SizeBytes
	}

	baseTotalMB := baseTotalBytes / (1024 * 1024)
	deltaTotalMB := deltaTotalBytes / (1024 * 1024)

	// Decision: promote deltas to base if delta total > target base size
	// or rewrite base if dirty ratio exceeded
	needsPromotion := deltaTotalMB >= int64(cfg.TargetBaseSizeMB)
	needsRewrite := len(baseFiles) > 0 && c.computeDirtyRatio(baseFiles, deltaFiles) > float64(cfg.DirtyRatioPct)/100.0

	if !needsPromotion && !needsRewrite {
		c.Logger.Debug("compaction not needed",
			zap.Int16("schema_id", schemaID),
			zap.Int64("base_mb", baseTotalMB),
			zap.Int64("delta_mb", deltaTotalMB))
		return nil
	}

	// For MVP: simple promotion - move delta files to base tier
	// Full rewrite (reading + merging parquet) deferred to later iteration
	if needsPromotion {
		c.Logger.Info("promoting deltas to base tier",
			zap.Int16("schema_id", schemaID),
			zap.Int64("delta_mb", deltaTotalMB))

		// Update file entries: change tier from delta to base
		for i := range m.Files {
			if m.Files[i].Tier == "delta" {
				m.Files[i].Tier = "base"
			}
		}
	}

	// Update manifest
	m.UpdatedAtMs = time.Now().UnixMilli()
	m.Version++

	newEtag, err := c.Provider.SaveManifest(ctx, schemaID, m, etag)
	if err != nil {
		// Check if this is a concurrent modification (etag mismatch)
		if newEtag == "" && etag != "" {
			return fmt.Errorf("%w: etag mismatch", ErrConcurrentModification)
		}
		return fmt.Errorf("save manifest: %w", err)
	}

	c.Logger.Info("compaction completed",
		zap.Int16("schema_id", schemaID),
		zap.Int64("version", m.Version),
		zap.String("etag", newEtag))

	return nil
}

// computeDirtyRatio estimates the ratio of updated/deleted rows to total base rows.
// For MVP, we use a simple heuristic: delta row count / base row count.
func (c *Compactor) computeDirtyRatio(baseFiles, deltaFiles []manifest.FileEntry) float64 {
	var baseRows, deltaRows int64
	for _, f := range baseFiles {
		baseRows += f.RowCount
	}
	for _, f := range deltaFiles {
		deltaRows += f.RowCount
	}
	if baseRows == 0 {
		return 0
	}
	return float64(deltaRows) / float64(baseRows)
}

// sortFilesByRowID sorts entries by RowIDMin lexicographically (UUID v7 is time-ordered).
func sortFilesByRowID(files []manifest.FileEntry) {
	sort.Slice(files, func(i, j int) bool {
		return files[i].RowIDMin < files[j].RowIDMin
	})
}
