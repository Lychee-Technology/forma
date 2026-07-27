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

// RunFlushDry runs the flusher in dry-run mode (#180): a dry run mutates
// nothing — no S3 objects (tmp or final), no flushed_at changes, and no
// manifest updates. TestDryRunImmutability pins all three surfaces.
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
	return e.RunInitWith(ctx, schema, InitOverrides{})
}

// InitOverrides customizes a single init pass (#180). Zero-value fields fall
// back to the defaults RunInit uses.
type InitOverrides struct {
	DryRun bool
}

// RunInitWith executes one real init pass with per-run overrides. In dry-run
// mode the mainline still counts the batches it skips, so the report's
// RowsExported/FilesCreated carry the planned work — callers use them as the
// positive control that the dry run had something to skip.
func (e *Env) RunInitWith(ctx context.Context, schema SchemaRef, ov InitOverrides) (*InitReport, error) {
	keysBefore, err := e.listS3Keys(ctx)
	if err != nil {
		return nil, fmt.Errorf("capture pre-init s3 listing: %w", err)
	}

	summary, err := cdc.RunInit(ctx, cdc.InitOptions{
		Config:               e.CDC,
		S3Client:             e.Cluster.S3,
		SchemaRegistryTable:  e.Tables.SchemaRegistry,
		SchemaIDFilter:       int(schema.ID),
		DryRun:               ov.DryRun,
		AutoEstimateRowBytes: true,
		Logger:               e.logger,
		SchemaRegistry:       e.Registry,
	})
	if err != nil {
		return nil, fmt.Errorf("cdc init schema %d (dry=%t): %w", schema.ID, ov.DryRun, err)
	}

	keysAfter, err := e.listS3Keys(ctx)
	if err != nil {
		return nil, fmt.Errorf("capture post-init s3 listing: %w", err)
	}
	report := &InitReport{
		RowsExported: summary.TotalRowsExported,
		FilesCreated: summary.TotalFilesCreated,
		NewObjects:   diffKeys(keysBefore, keysAfter),
	}
	manifests, err := e.loadManifests(ctx)
	if err != nil {
		return nil, fmt.Errorf("load manifests after init: %w", err)
	}
	report.Manifest = manifests[schema.ID]
	return report, nil
}

// RunCompaction executes the real compactor (compaction.Compactor.RunOnce)
// against the Env's manifest for one schema and returns its typed result.
// Reads are manifest-driven (engine.go wires manifest.QuerySource), so a
// compaction pass that mutates the manifest is directly query-observable.
func (e *Env) RunCompaction(ctx context.Context, schema SchemaRef) (compaction.CompactionResult, error) {
	return e.RunCompactionWith(ctx, schema, CompactionOverrides{})
}

// CompactionOverrides customizes a single compaction pass (#188). Zero-value
// fields fall back to the defaults RunCompaction uses.
type CompactionOverrides struct {
	// S3 replaces the cluster client for both manifest I/O and object
	// copy/delete work (decorate for ETag-conflict scenarios).
	S3 cdc.S3FullClient
	// TargetBaseSizeBytes lowers the byte-precise promotion threshold so
	// KB-scale delta tiers can promote (0: default 256 MB).
	TargetBaseSizeBytes int64
	// DirtyRatioPct tunes the rewrite trigger (0: default 5).
	DirtyRatioPct int
}

// RunCompactionWith executes one real compaction pass with per-run overrides,
// wiring the full compactor: manifest provider, merge engine and S3 object
// client over the chosen S3 client. The merge engine is built the way the
// production tool builds it — a cdc.NewDuckExporter DuckDB with the Env's S3
// httpfs config — so the rewrite's parquet traffic takes the production path
// (and, like the flush export, bypasses any injected S3 decorator).
func (e *Env) RunCompactionWith(ctx context.Context, schema SchemaRef, ov CompactionOverrides) (compaction.CompactionResult, error) {
	if e.CDC.ManifestTemplate == "" {
		return compaction.CompactionResult{}, fmt.Errorf("compaction requires a manifest (Env built WithoutManifest)")
	}
	var s3Client cdc.S3FullClient = e.Cluster.S3
	if ov.S3 != nil {
		s3Client = ov.S3
	}
	provider := compaction.NewS3ManifestProvider(cdc.ManifestConfig{
		Bucket:       e.Cluster.Bucket,
		Prefix:       e.CDC.ManifestPrefix,
		PathTemplate: e.CDC.ManifestTemplate,
	}, s3Client)

	s3Key, s3Secret, s3Token := cdc.ResolveStaticS3Credentials(e.CDC)
	exporter, err := cdc.NewDuckExporter(ctx, e.CDC, s3Key, s3Secret, s3Token, e.logger)
	if err != nil {
		return compaction.CompactionResult{}, fmt.Errorf("open merge duckdb: %w", err)
	}
	defer func() { _ = exporter.DB.Close() }()

	compactor := &compaction.Compactor{
		Logger: e.logger,
		Config: cdc.CompactionConfig{
			SchemaID:            schema.ID,
			TargetBaseSizeBytes: ov.TargetBaseSizeBytes,
			DirtyRatioPct:       ov.DirtyRatioPct,
		},
		Provider:   provider,
		Merger:     &compaction.DuckMerger{DB: exporter.DB},
		S3:         s3Client,
		Bucket:     e.Cluster.Bucket,
		DataPrefix: e.S3Prefix,
		Resolver: manifest.PathResolver{
			Prefix:       e.CDC.ManifestPrefix,
			PathTemplate: e.CDC.ManifestTemplate,
		},
	}
	result, err := compactor.RunOnce(ctx)
	if err != nil {
		return result, fmt.Errorf("compaction run once (schema %d): %w", schema.ID, err)
	}
	return result, nil
}

// RegisterParquetInManifest appends one parquet object to the schema's
// manifest so manifest-driven reads can see a file the production exporter
// would never write (e.g. a fabricated wrong-schema or 0-row parquet, #187).
// The entry carries only tier and path — the read path consumes nothing else.
func (e *Env) RegisterParquetInManifest(ctx context.Context, schema SchemaRef, key, tier string) error {
	if e.CDC.ManifestTemplate == "" {
		return fmt.Errorf("register parquet in manifest: Env built WithoutManifest")
	}
	store := &manifest.S3Store{Client: e.Cluster.S3, Bucket: e.Cluster.Bucket}
	resolver := manifest.PathResolver{Prefix: e.CDC.ManifestPrefix, PathTemplate: e.CDC.ManifestTemplate}
	path, err := resolver.Resolve(schema.ID)
	if err != nil {
		return fmt.Errorf("resolve manifest path for schema %d: %w", schema.ID, err)
	}
	if err := manifest.AppendFile(ctx, store, path, schema.ID, manifest.FileEntry{Tier: tier, Path: key}); err != nil {
		return fmt.Errorf("append parquet %s to schema %d manifest: %w", key, schema.ID, err)
	}
	return nil
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
