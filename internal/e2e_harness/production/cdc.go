package production

import (
	"context"
	"errors"
	"fmt"
	"sort"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/manifestv2"
)

// FlushReport captures the observable effects of one CDC flush pass.
type FlushReport struct {
	DryRun     bool
	NewObjects []string
	// Manifests is the published generation of every fixture schema whose
	// record is PUBLISHED after the run (loadManifests).
	Manifests       map[int16]*Manifest
	UnflushedBefore int64
	UnflushedAfter  int64
	// Flushed is the runner's own report of what each schema's flush
	// published: the generations it committed and the entries it marked.
	Flushed []cdc.SchemaFlush
	// Skipped is the runner's own report of schemas it did not export (#704).
	Skipped []cdc.SkippedSchema
}

// InitReport captures the effects of one base-file initialization export.
type InitReport struct {
	RowsExported int64
	FilesCreated int
	NewObjects   []string
	// Manifest is the schema's published generation after the run, nil
	// while its record is UNPUBLISHED.
	Manifest *Manifest
	// Schemas is the run's own report of what each schema's init published
	// and handed off.
	Schemas []cdc.SchemaInit
	// Skipped is the run's own report of schemas it did not export (#704).
	Skipped []cdc.SkippedSchema
}

// RunFlush executes the real CDC runner (cdc.Runner.RunOnce) against the
// Env's database and S3 prefix and reports what changed.
func (e *Env) RunFlush(ctx context.Context) (*FlushReport, error) {
	return e.runFlush(ctx, false)
}

// RunFlushDry runs the flusher in dry-run mode (#180): a dry run mutates
// nothing — no S3 objects, no flushed_at changes, and no publication. TestDryRunImmutability pins all three surfaces.
func (e *Env) RunFlushDry(ctx context.Context) (*FlushReport, error) {
	return e.runFlush(ctx, true)
}

// FlushOverrides customizes a single flush pass for failure-injection tests
// (#179). Zero-value fields fall back to the Env defaults RunFlush uses.
type FlushOverrides struct {
	S3     cdc.S3Client   // nil: e.Cluster.S3
	Config *cdc.CDCConfig // nil: e.CDC
	DryRun bool
	// Registry is the run's schema registry (nil: e.Registry), for a test
	// that acts between the run's start and its batches (#733).
	Registry forma.SchemaRegistry
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
	if cfg.RegistryEpochs == nil {
		cfg.RegistryEpochs = e.RegistryEpochs
	}
	var s3Client cdc.S3Client = e.Cluster.S3
	if ov.S3 != nil {
		s3Client = ov.S3
	}

	registry := e.Registry
	if ov.Registry != nil {
		registry = ov.Registry
	}
	runner := cdc.NewRunner(e.logger)
	defer func() { _ = runner.Close() }()
	runReport, runErr := runner.RunOnce(ctx, cfg, s3Client, ov.DryRun, registry)
	if runErr != nil {
		runErr = fmt.Errorf("cdc run once (dry=%t): %w", ov.DryRun, runErr)
	}

	report := &FlushReport{DryRun: ov.DryRun, UnflushedBefore: before, Flushed: runReport.Flushed, Skipped: runReport.Skipped}
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
	Config *cdc.CDCConfig // nil: e.CDC
	DryRun bool
	// AllSchemas runs init over every registered schema, as cdc-init does
	// without --schema-id, instead of the one schema named.
	AllSchemas bool
}

// RunInitWith executes one real init pass with per-run overrides: the V2
// cdc-init publishes an init generation and hands the exported rows' entries
// off at their export versions. In dry-run mode the mainline still counts the
// batches it skips, so the report's RowsExported/FilesCreated carry the
// planned work — callers use them as the positive control that the dry run
// had something to skip. A real run's per-schema reports are kept for the
// onboarding checks of handoff.go.
func (e *Env) RunInitWith(ctx context.Context, schema SchemaRef, ov InitOverrides) (*InitReport, error) {
	keysBefore, err := e.listS3Keys(ctx)
	if err != nil {
		return nil, fmt.Errorf("capture pre-init s3 listing: %w", err)
	}

	cfg := e.CDC
	if ov.Config != nil {
		cfg = *ov.Config
	}
	if cfg.RegistryEpochs == nil {
		cfg.RegistryEpochs = e.RegistryEpochs
	}
	schemaIDFilter := int(schema.ID)
	if ov.AllSchemas {
		schemaIDFilter = 0
	}
	summary, err := cdc.RunInit(ctx, cdc.InitOptions{
		Config:               cfg,
		S3Client:             e.Cluster.S3,
		SchemaRegistryTable:  e.Tables.SchemaRegistry,
		SchemaIDFilter:       schemaIDFilter,
		DryRun:               ov.DryRun,
		AutoEstimateRowBytes: true,
		Logger:               e.logger,
		SchemaRegistry:       e.Registry,
	})
	if err != nil {
		return nil, fmt.Errorf("cdc init schema %d (dry=%t): %w", schema.ID, ov.DryRun, err)
	}
	if !ov.DryRun {
		e.recordInits(summary.Schemas)
	}

	keysAfter, err := e.listS3Keys(ctx)
	if err != nil {
		return nil, fmt.Errorf("capture post-init s3 listing: %w", err)
	}
	report := &InitReport{
		RowsExported: summary.TotalRowsExported,
		FilesCreated: summary.TotalFilesCreated,
		NewObjects:   diffKeys(keysBefore, keysAfter),
		Schemas:      summary.Schemas,
		Skipped:      summary.Skipped,
	}
	manifests, err := e.loadManifests(ctx)
	if err != nil {
		return nil, fmt.Errorf("load manifests after init: %w", err)
	}
	report.Manifest = manifests[schema.ID]
	return report, nil
}

// RunCompaction runs one V2 compaction pass of schema (#890).
func (e *Env) RunCompaction(ctx context.Context, schema SchemaRef) (compaction.CompactionResult, error) {
	return e.RunCompactionWith(ctx, schema, CompactionOverrides{})
}

// CompactionOverrides customizes a single compaction pass (#188). Zero-value
// fields fall back to the defaults RunCompaction uses.
type CompactionOverrides struct {
	// S3 replaces the cluster client for every object and manifest read and
	// write of the pass (decorate to pause or fault it).
	S3 cdc.S3FullClient
	// TargetBaseSizeBytes lowers the byte-precise promotion threshold so
	// KB-scale delta tiers can promote (0: default 256 MB).
	TargetBaseSizeBytes int64
	// DirtyRatioPct tunes the rewrite trigger (0: default 5).
	DirtyRatioPct int
	// SkipInputChecksumVerify opts the pass out of the #347 pre-merge
	// verification of merge sources, as the compactor tool's
	// --skip-input-checksum-verify flag does.
	SkipInputChecksumVerify bool
}

// RunCompactionWith runs one V2 compaction pass with per-run overrides, wired
// as the compactor tool wires it: the publication database of the Env's CDC
// config, the cluster bucket under the Env's data prefix, a DuckDB merge
// engine on the CDC config's S3 credentials, and every merge source hashed
// against its listed digest. It is safe to call off the test goroutine.
func (e *Env) RunCompactionWith(ctx context.Context, schema SchemaRef, ov CompactionOverrides) (compaction.CompactionResult, error) {
	var s3Client cdc.S3FullClient = e.Cluster.S3
	if ov.S3 != nil {
		s3Client = ov.S3
	}
	db, err := cdc.OpenPublicationDB(ctx, e.CDC, e.logger)
	if err != nil {
		return compaction.CompactionResult{}, fmt.Errorf("compaction of schema %d: %w", schema.ID, err)
	}
	defer func() { _ = db.Close() }()
	s3Key, s3Secret, s3Token := cdc.ResolveStaticS3Credentials(e.CDC)
	exporter, err := cdc.NewDuckExporter(ctx, e.CDC, s3Key, s3Secret, s3Token, e.logger)
	if err != nil {
		return compaction.CompactionResult{}, fmt.Errorf("open merge duckdb: %w", err)
	}
	defer func() { _ = exporter.DB.Close() }()

	cfg := cdc.CompactionConfig{
		SchemaID:                schema.ID,
		TargetBaseSizeBytes:     ov.TargetBaseSizeBytes,
		DirtyRatioPct:           ov.DirtyRatioPct,
		SkipInputChecksumVerify: ov.SkipInputChecksumVerify,
	}
	store := &manifestv2.S3Store{Client: s3Client, Bucket: e.Cluster.Bucket}
	writer, err := compaction.NewWriter(compaction.WriterConfig{
		DB: db, Engine: e.CDC.Engine, EntityMain: e.CDC.EntityMainTable,
		Layout: manifestv2.Layout{Prefix: e.S3Prefix}, Store: store, Compaction: cfg,
	})
	if err != nil {
		return compaction.CompactionResult{}, err
	}
	compactor := &compaction.Compactor{
		Logger:       e.logger,
		Config:       cfg,
		Writer:       writer,
		Store:        store,
		Merger:       &compaction.DuckMerger{DB: exporter.DB, Logger: e.logger},
		Bucket:       e.Cluster.Bucket,
		ObjectReader: s3Client,
		Epoch:        e.RegistryEpochs[schema.ID],
	}
	result, err := compactor.RunOnce(ctx)
	if err != nil {
		return result, fmt.Errorf("compaction run once (schema %d): %w", schema.ID, err)
	}
	return result, nil
}

// recordInits keeps each schema's latest real cdc-init report.
func (e *Env) recordInits(inits []cdc.SchemaInit) {
	if e.inits == nil {
		e.inits = make(map[int16]cdc.SchemaInit)
	}
	for _, in := range inits {
		e.inits[in.SchemaID] = in
	}
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
