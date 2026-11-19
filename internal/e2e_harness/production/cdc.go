package production

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/compaction"
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

// compactionSkipReason is why every compaction scenario skips: a cut-over
// deployment keeps the V1 compactor stopped (docs/publication-protocol-v2.md,
// "Upgrade" step 1), and it cannot run against a V2 schema.
const compactionSkipReason = "the V1 compactor does no work against a Protocol V2 schema; " +
	"compaction scenarios run again on V2 compaction (#890)"

// RunCompaction runs one compaction pass of schema. Until V2 compaction lands
// (#890) it skips the calling test, naming #890.
func (e *Env) RunCompaction(ctx context.Context, schema SchemaRef) (compaction.CompactionResult, error) {
	e.T.Helper()
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
	// SkipInputChecksumVerify opts the pass out of the #347 pre-merge
	// verification of rewrite sources, as the compactor tool's
	// --skip-input-checksum-verify flag does.
	SkipInputChecksumVerify bool
}

// RunCompactionWith runs one compaction pass with per-run overrides. Until
// V2 compaction lands (#890) it skips the calling test, naming #890; it must
// be called on the test's own goroutine.
func (e *Env) RunCompactionWith(_ context.Context, schema SchemaRef, _ CompactionOverrides) (compaction.CompactionResult, error) {
	e.T.Helper()
	e.SkipUntilV2Compaction()
	return compaction.CompactionResult{}, fmt.Errorf("compaction of schema %d: no V2 compactor is wired (#890)", schema.ID)
}

// SkipUntilV2Compaction skips the calling test until V2 compaction lands
// (#890). A test that runs RunCompaction off its own goroutine calls it
// first, since a skip only stops the goroutine that calls it.
func (e *Env) SkipUntilV2Compaction() {
	e.T.Helper()
	skipUntilV2Compaction(e.T)
}

// skipUntilV2Compaction skips t until V2 compaction lands (#890). A subtest
// that compacts calls it with its own t first: the Env's skip stops the
// Env's test, which a subtest must not do.
func skipUntilV2Compaction(t testing.TB) {
	t.Helper()
	if v1CompactionStopped {
		t.Skip(compactionSkipReason)
	}
}

// v1CompactionStopped is true until V2 compaction (#890) replaces the skip
// with a real pass. It is a variable, not a constant, so that static analysis
// does not treat every compaction scenario's code after RunCompaction as
// unreachable while it skips.
var v1CompactionStopped = true

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
