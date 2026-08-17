package compaction

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/lychee-technology/forma/internal/telemetry"
	"go.uber.org/zap"
)

// ErrConcurrentModification indicates manifest was modified by another process.
var ErrConcurrentModification = errors.New("manifest concurrently modified")

// ErrManifestMetadataContractViolation indicates provider.SaveManifest succeeded
// but did not advance manifest metadata as required by FileProvider contract.
var ErrManifestMetadataContractViolation = errors.New("manifest metadata contract violated")

// CompactionOutcome describes the result of a compaction pass for a single schema.
type CompactionOutcome string

const (
	Noop             CompactionOutcome = "noop"
	PromotionApplied CompactionOutcome = "promotion_applied"
	// RewritePending is reported when the dirty ratio calls for a rewrite but
	// no Merger is wired, so the pass leaves everything untouched.
	RewritePending CompactionOutcome = "rewrite_pending"
	// RewriteApplied is reported when the dirty-ratio rewrite merged the full
	// base+delta set into a new base file and committed the manifest swap.
	RewriteApplied CompactionOutcome = "rewrite_applied"
)

// CompactionResult carries the outcome and metadata from a compaction pass.
type CompactionResult struct {
	Outcome    CompactionOutcome
	SchemaID   int16
	Version    int64
	DirtyRatio float64
	BaseMB     int64
	DeltaMB    int64

	// Rewrite metadata, populated only when Outcome is RewriteApplied.
	FilesMerged int    // source files folded into the new base
	RowsIn      int64  // rows read across all source files
	RowsOut     int64  // surviving LWW winners written to the new base
	NewBaseKey  string // S3 key of the merged base parquet
}

// FileProvider fetches manifest and lists actual files (optionally cross-check).
type FileProvider interface {
	LoadManifest(ctx context.Context, schemaID int16) (*manifest.Manifest, string, error)
	// SaveManifest persists the manifest as a commit point.
	// Implementations must mutate the provided manifest pointer in-place on
	// successful save:
	// - Version must increase monotonically.
	// - UpdatedAtMs must move forward.
	// Compactor relies on the same pointer carrying updated metadata and does
	// not mutate those fields directly.
	SaveManifest(ctx context.Context, schemaID int16, m *manifest.Manifest, etag string) (string, error)
}

// Compactor performs Base/Delta maintenance per schema. The dirty-ratio
// rewrite (#188) additionally needs Merger, S3, Bucket and DataPrefix; when
// any is missing a rewrite-eligible pass reports RewritePending instead.
type Compactor struct {
	Logger     *zap.Logger
	Config     cdc.CompactionConfig
	Provider   FileProvider
	Merger     Merger
	S3         cdc.S3ObjectClient
	Bucket     string
	DataPrefix string // root prefix for parquet files
	Resolver   manifest.PathResolver

	// ObjectReader hashes published objects for manifest checksum stamping
	// and pre-merge input verification (#347). Nil disables both.
	ObjectReader cdc.S3GetClient
}

// RunOnce executes compaction for a schema and returns a typed result.
func (c *Compactor) RunOnce(ctx context.Context) (CompactionResult, error) {
	if c.Provider == nil {
		return CompactionResult{}, fmt.Errorf("file provider is nil")
	}
	if c.Config.SchemaID == 0 {
		return CompactionResult{}, fmt.Errorf("schema_id required for compaction in MVP")
	}

	cfg := c.Config.WithDefaults()
	return c.compactSchemaWithRetry(ctx, c.Config.SchemaID, cfg)
}

// compactSchemaWithRetry wraps compactSchema with exponential backoff for transient errors.
func (c *Compactor) compactSchemaWithRetry(ctx context.Context, schemaID int16, cfg cdc.CompactionConfig) (CompactionResult, error) {
	var lastErr error
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		result, err := c.compactSchema(ctx, schemaID, cfg)
		if err == nil {
			return result, nil
		}
		lastErr = err

		if ctx.Err() != nil {
			return CompactionResult{}, ctx.Err()
		}
		if !isRetryable(err) {
			return CompactionResult{}, err
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
				return CompactionResult{}, ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	return CompactionResult{}, fmt.Errorf("compaction failed after %d retries: %w", cfg.MaxRetries, lastErr)
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

// isPreconditionFailed reports whether err is a CONFIRMED HTTP 412
// conditional-put rejection from the object store — the only save failure
// that proves the write did not commit. Transport errors, timeouts and
// other API failures are deliberately excluded: after those the put may
// have landed, and callers must treat the outcome as ambiguous.
func isPreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "PreconditionFailed" {
		return true
	}
	var respErr *awshttp.ResponseError
	return errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusPreconditionFailed
}

func (c *Compactor) compactSchema(ctx context.Context, schemaID int16, cfg cdc.CompactionConfig) (CompactionResult, error) {
	m, etag, err := c.Provider.LoadManifest(ctx, schemaID)
	if err != nil {
		return CompactionResult{}, fmt.Errorf("load manifest: %w", err)
	}
	if m == nil {
		c.Logger.Debug("no manifest for schema, skipping", zap.Int16("schema_id", schemaID))
		return CompactionResult{Outcome: Noop, SchemaID: schemaID}, nil
	}

	baseFiles := manifest.FilterByTier(m, "base")
	deltaFiles := manifest.FilterByTier(m, "delta")

	sortFilesByRowID(baseFiles)
	sortFilesByRowID(deltaFiles)

	c.Logger.Debug("compaction analysis",
		zap.Int16("schema_id", schemaID),
		zap.Int("base_files", len(baseFiles)),
		zap.Int("delta_files", len(deltaFiles)))

	var baseTotalBytes, deltaTotalBytes int64
	for _, f := range baseFiles {
		baseTotalBytes += f.SizeBytes
	}
	for _, f := range deltaFiles {
		deltaTotalBytes += f.SizeBytes
	}

	baseTotalMB := baseTotalBytes / (1024 * 1024)
	deltaTotalMB := deltaTotalBytes / (1024 * 1024)
	dirtyRatio := c.computeDirtyRatio(baseFiles, deltaFiles)
	telemetry.EmitCompactionDirtyRatio(ctx, schemaID, dirtyRatio)

	// Compare bytes, not truncated MB: sub-MB delta tiers must still be able
	// to promote once the byte-precise threshold is lowered (WithDefaults
	// derives TargetBaseSizeBytes from TargetBaseSizeMB when unset).
	needsPromotion := deltaTotalBytes >= cfg.TargetBaseSizeBytes
	needsRewrite := len(baseFiles) > 0 && dirtyRatio > float64(cfg.DirtyRatioPct)/100.0

	analysis := CompactionResult{Outcome: Noop, SchemaID: schemaID, DirtyRatio: dirtyRatio, BaseMB: baseTotalMB, DeltaMB: deltaTotalMB}

	if !needsPromotion && !needsRewrite {
		c.Logger.Debug("compaction not needed",
			zap.Int16("schema_id", schemaID),
			zap.Int64("base_mb", baseTotalMB),
			zap.Int64("delta_mb", deltaTotalMB))
		return analysis, nil
	}

	// Promotion takes precedence within one pass; a still-dirty schema is
	// rewritten on a later pass.
	if needsPromotion {
		return c.applyPromotion(ctx, schemaID, m, etag, analysis)
	}

	if !c.canRewrite() {
		telemetry.EmitCompactionRewritePending(ctx, schemaID)
		c.Logger.Warn("rewrite needed but the compactor has no merge wiring; skipping manifest update",
			zap.Int16("schema_id", schemaID),
			zap.Float64("dirty_ratio", dirtyRatio),
			zap.Int("dirty_ratio_pct", cfg.DirtyRatioPct),
			zap.Int64("base_mb", baseTotalMB),
			zap.Int64("delta_mb", deltaTotalMB))
		analysis.Outcome = RewritePending
		return analysis, nil
	}

	sources := make([]manifest.FileEntry, 0, len(baseFiles)+len(deltaFiles))
	sources = append(sources, baseFiles...)
	sources = append(sources, deltaFiles...)
	return c.runRewrite(ctx, schemaID, m, etag, sources, analysis)
}

// applyPromotion relabels every delta entry as base (manifest-only; the S3
// keys are untouched) and commits under the loaded ETag.
func (c *Compactor) applyPromotion(ctx context.Context, schemaID int16, m *manifest.Manifest, etag string, analysis CompactionResult) (CompactionResult, error) {
	c.Logger.Info("promoting deltas to base tier",
		zap.Int16("schema_id", schemaID),
		zap.Int64("delta_mb", analysis.DeltaMB))

	applied := false
	for i := range m.Files {
		if strings.EqualFold(m.Files[i].Tier, "delta") {
			m.Files[i].Tier = "base"
			applied = true
		}
	}
	if !applied {
		c.Logger.Warn("promotion found no delta entries; skipping manifest update",
			zap.Int16("schema_id", schemaID))
		return analysis, nil
	}

	newEtag, err := c.saveManifestChecked(ctx, schemaID, m, etag)
	if err != nil {
		return CompactionResult{}, fmt.Errorf("promotion manifest save for schema %d: %w", schemaID, err)
	}
	c.Logger.Info("compaction completed",
		zap.Int16("schema_id", schemaID),
		zap.Int64("version", m.Version),
		zap.String("etag", newEtag))

	analysis.Outcome = PromotionApplied
	analysis.Version = m.Version
	return analysis, nil
}

// saveManifestChecked commits the manifest under optimistic concurrency and
// enforces the FileProvider contract: the provider must advance Version and
// UpdatedAtMs in-place on success. Only a CONFIRMED conditional-put
// rejection (HTTP 412, or a provider pre-classified sentinel) maps to the
// retryable ErrConcurrentModification — any other failure is ambiguous (the
// put may have committed before the response was lost) and must surface
// as-is so callers never treat a possibly-committed write as a clean CAS
// loss.
func (c *Compactor) saveManifestChecked(ctx context.Context, schemaID int16, m *manifest.Manifest, etag string) (string, error) {
	prevVersion := m.Version
	prevUpdatedAtMs := m.UpdatedAtMs

	newEtag, err := c.Provider.SaveManifest(ctx, schemaID, m, etag)
	if err != nil {
		if errors.Is(err, ErrConcurrentModification) || isPreconditionFailed(err) {
			return "", fmt.Errorf("%w: conditional save of schema %d manifest rejected (stale etag %q): %w", ErrConcurrentModification, schemaID, etag, err)
		}
		return "", fmt.Errorf("save manifest for schema %d: %w", schemaID, err)
	}

	if m.Version <= prevVersion || m.UpdatedAtMs <= prevUpdatedAtMs {
		telemetry.EmitCompactionManifestContractViolation(ctx, schemaID)
		c.Logger.Error("save manifest contract violated: metadata not advanced",
			zap.Int16("schema_id", schemaID),
			zap.Int64("prev_version", prevVersion),
			zap.Int64("new_version", m.Version),
			zap.Int64("prev_updated_at_ms", prevUpdatedAtMs),
			zap.Int64("new_updated_at_ms", m.UpdatedAtMs),
			zap.String("etag", newEtag))
		return "", fmt.Errorf("%w: provider must mutate manifest in-place; prev(version=%d, updated_at_ms=%d), new(version=%d, updated_at_ms=%d)",
			ErrManifestMetadataContractViolation,
			prevVersion,
			prevUpdatedAtMs,
			m.Version,
			m.UpdatedAtMs,
		)
	}
	return newEtag, nil
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
