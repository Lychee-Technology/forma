package production

import (
	"context"
	"fmt"
	"sort"

	"github.com/lychee-technology/forma/internal/cdc"
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

func (e *Env) runFlush(ctx context.Context, dryRun bool) (*FlushReport, error) {
	before, err := e.countUnflushed(ctx)
	if err != nil {
		return nil, err
	}
	keysBefore, err := e.listS3Keys(ctx)
	if err != nil {
		return nil, err
	}

	runner := cdc.NewRunner(e.logger)
	defer func() { _ = runner.Close() }()
	if err := runner.RunOnce(ctx, e.CDC, e.Cluster.S3, dryRun, e.Registry); err != nil {
		return nil, fmt.Errorf("cdc run once (dry=%t): %w", dryRun, err)
	}

	report := &FlushReport{DryRun: dryRun, UnflushedBefore: before}
	if report.UnflushedAfter, err = e.countUnflushed(ctx); err != nil {
		return nil, err
	}
	keysAfter, err := e.listS3Keys(ctx)
	if err != nil {
		return nil, err
	}
	report.NewObjects = diffKeys(keysBefore, keysAfter)
	if report.Manifests, err = e.loadManifests(ctx); err != nil {
		return nil, err
	}
	return report, nil
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
