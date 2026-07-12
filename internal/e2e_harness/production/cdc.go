package production

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/manifest"
)

// FlushReport captures the observable effects of one CDC flush pass.
type FlushReport struct {
	DryRun          bool
	NewObjects      []string
	Manifests       map[int16]*manifest.Manifest
	UnflushedBefore int64
	UnflushedAfter  int64
}

// InitReport captures the effects of one base-file initialization export.
type InitReport struct {
	RowsExported int64
	FilesCreated int
	NewObjects   []string
	Manifest     *manifest.Manifest
}

// RunFlush executes the real CDC runner (cdc.Runner.RunOnce) against the
// Env's database and S3 prefix and reports what changed.
func (e *Env) RunFlush(ctx context.Context) (*FlushReport, error) {
	return e.runFlush(ctx, false)
}

// RunFlushDry runs the flusher in dry-run mode (for #180). Note the mainline
// dry-run still exports parquet objects; it skips marking rows flushed and
// updating the manifest — assert on UnflushedAfter and Manifests.
func (e *Env) RunFlushDry(ctx context.Context) (*FlushReport, error) {
	return e.runFlush(ctx, true)
}

// FlushOverrides customizes a single flush pass for failure-injection tests
// (#179). Zero-value fields fall back to the Env defaults RunFlush uses.
type FlushOverrides struct {
	S3     cdc.S3FullClient // nil: e.Cluster.S3
	Config *cdc.CDCConfig   // nil: e.CDC
	DryRun bool
}

func (e *Env) runFlush(ctx context.Context, dryRun bool) (*FlushReport, error) {
	report, err := e.RunFlushWith(ctx, FlushOverrides{DryRun: dryRun})
	if err != nil {
		return nil, fmt.Errorf("run flush (dry=%t): %w", dryRun, err)
	}
	return report, nil
}

// RunFlushWith executes one real CDC flush pass with per-run overrides.
// Unlike RunFlush it reports observable state even when the run fails: the
// report is always non-nil unless the pre-run state capture itself fails, so
// failure-boundary tests can assert partial side effects alongside the error.
func (e *Env) RunFlushWith(ctx context.Context, ov FlushOverrides) (*FlushReport, error) {
	before, err := e.countUnflushed(ctx)
	if err != nil {
		return nil, fmt.Errorf("capture pre-flush change_log state: %w", err)
	}
	keysBefore, err := e.listS3Keys(ctx)
	if err != nil {
		return nil, fmt.Errorf("capture pre-flush s3 listing: %w", err)
	}

	cfg := e.CDC
	if ov.Config != nil {
		cfg = *ov.Config
	}
	var s3Client cdc.S3FullClient = e.Cluster.S3
	if ov.S3 != nil {
		s3Client = ov.S3
	}

	runner := cdc.NewRunner(e.logger)
	defer func() { _ = runner.Close() }()
	runErr := runner.RunOnce(ctx, cfg, s3Client, ov.DryRun, e.Registry)
	if runErr != nil {
		runErr = fmt.Errorf("cdc run once (dry=%t): %w", ov.DryRun, runErr)
	}

	report := &FlushReport{DryRun: ov.DryRun, UnflushedBefore: before}
	if report.UnflushedAfter, err = e.countUnflushed(ctx); err != nil {
		return report, errors.Join(runErr, err)
	}
	keysAfter, err := e.listS3Keys(ctx)
	if err != nil {
		return report, errors.Join(runErr, err)
	}
	report.NewObjects = diffKeys(keysBefore, keysAfter)
	if report.Manifests, err = e.loadManifests(ctx); err != nil {
		return report, errors.Join(runErr, err)
	}
	return report, runErr
}

// RunInit exports base parquet files for one schema via the extracted
// cdc.RunInit driver and reports what changed.
func (e *Env) RunInit(ctx context.Context, schema SchemaRef) (*InitReport, error) {
	keysBefore, err := e.listS3Keys(ctx)
	if err != nil {
		return nil, err
	}

	summary, err := cdc.RunInit(ctx, cdc.InitOptions{
		Config:               e.CDC,
		S3Client:             e.Cluster.S3,
		SchemaRegistryTable:  e.Tables.SchemaRegistry,
		SchemaIDFilter:       int(schema.ID),
		AutoEstimateRowBytes: true,
		Logger:               e.logger,
		SchemaRegistry:       e.Registry,
	})
	if err != nil {
		return nil, fmt.Errorf("cdc init schema %d: %w", schema.ID, err)
	}

	keysAfter, err := e.listS3Keys(ctx)
	if err != nil {
		return nil, err
	}
	report := &InitReport{
		RowsExported: summary.TotalRowsExported,
		FilesCreated: summary.TotalFilesCreated,
		NewObjects:   diffKeys(keysBefore, keysAfter),
	}
	manifests, err := e.loadManifests(ctx)
	if err != nil {
		return nil, err
	}
	report.Manifest = manifests[schema.ID]
	return report, nil
}

// RunCompaction executes the real compactor (compaction.Compactor.RunOnce)
// against the Env's manifest for one schema and returns its typed result.
// Note the current compactor is manifest-level only: promotion retags delta
// entries as base once the delta tier reaches TargetBaseSizeMB (≥1 MB, out of
// reach at lifecycle-test scale), and the dirty-ratio rewrite is not
// implemented — it reports RewritePending and leaves the manifest untouched
// (internal/compaction/compactor.go). The federated read path never consults
// the manifest, so parquet contents and query results are unaffected either
// way today.
func (e *Env) RunCompaction(ctx context.Context, schema SchemaRef) (compaction.CompactionResult, error) {
	if e.CDC.ManifestTemplate == "" {
		return compaction.CompactionResult{}, fmt.Errorf("compaction requires a manifest (Env built WithoutManifest)")
	}
	provider := compaction.NewS3ManifestProvider(cdc.ManifestConfig{
		Bucket:       e.Cluster.Bucket,
		Prefix:       e.CDC.ManifestPrefix,
		PathTemplate: e.CDC.ManifestTemplate,
	}, e.Cluster.S3)
	compactor := &compaction.Compactor{
		Logger:   e.logger,
		Config:   cdc.CompactionConfig{SchemaID: schema.ID},
		Provider: provider,
	}
	result, err := compactor.RunOnce(ctx)
	if err != nil {
		return result, fmt.Errorf("compaction run once (schema %d): %w", schema.ID, err)
	}
	return result, nil
}

// countUnflushed counts change_log rows with flushed_at = 0 across all
// schemas in the per-test database.
func (e *Env) countUnflushed(ctx context.Context) (int64, error) {
	var count int64
	if err := e.Pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM change_log WHERE flushed_at = 0").Scan(&count); err != nil {
		return 0, fmt.Errorf("count unflushed change_log rows: %w", err)
	}
	return count, nil
}

// loadManifests fetches and parses the manifest of every fixture schema that
// has one. Missing manifests are simply absent from the map.
func (e *Env) loadManifests(ctx context.Context) (map[int16]*manifest.Manifest, error) {
	manifests := make(map[int16]*manifest.Manifest)
	if e.CDC.ManifestTemplate == "" {
		return manifests, nil
	}
	store := &manifest.S3Store{Client: e.Cluster.S3, Bucket: e.Cluster.Bucket}
	resolver := manifest.PathResolver{Prefix: e.CDC.ManifestPrefix, PathTemplate: e.CDC.ManifestTemplate}

	for _, ref := range DefaultSchemaFixtures() {
		path, err := resolver.Resolve(ref.ID)
		if err != nil {
			return nil, fmt.Errorf("resolve manifest path for schema %d: %w", ref.ID, err)
		}
		m, _, err := manifest.Load(ctx, store, path)
		if err != nil {
			continue // no manifest yet for this schema
		}
		manifests[ref.ID] = m
	}
	return manifests, nil
}

// diffKeys returns keys present in after but not in before, sorted.
func diffKeys(before, after []string) []string {
	seen := make(map[string]struct{}, len(before))
	for _, k := range before {
		seen[k] = struct{}{}
	}
	var added []string
	for _, k := range after {
		if _, ok := seen[k]; !ok {
			added = append(added, k)
		}
	}
	sort.Strings(added)
	return added
}
