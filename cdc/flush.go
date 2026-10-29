package cdc

import (
	"context"

	"github.com/lychee-technology/forma"
	internalcdc "github.com/lychee-technology/forma/internal/cdc"
	"go.uber.org/zap"
)

// Config controls change_log flushing and export behavior.
type Config = internalcdc.CDCConfig

// S3ObjectClient is the minimal S3 interface required by the flush path.
type S3ObjectClient = internalcdc.S3ObjectClient

// S3FullClient extends S3ObjectClient with manifest read/write operations.
type S3FullClient = internalcdc.S3FullClient

// FlushReport is what a flush pass reports besides its error: the schemas it
// left unexported because the parquet tiers cannot hold them. RunOnce drops
// it; call Flush to read it. The report does not depend on the logger.
type FlushReport = internalcdc.FlushReport

// SkippedSchema is one schema a flush pass did not export, and why. Its
// change_log rows stay unflushed, and federated reads of it are answered
// from PostgreSQL.
type SkippedSchema = internalcdc.SkippedSchema

// FlushRunner reuses AWS/S3/DuckDB initialization across multiple flush
// passes. Safe for long-lived use under credential rotation: superseded
// cached exporters are evicted and closed once idle (#331), so rotations do
// not accumulate open DuckDB instances. Call Close at shutdown to release
// whatever is still cached.
type FlushRunner struct {
	runner *internalcdc.Runner
}

// Runner is kept as a compatibility alias for FlushRunner.
type Runner = FlushRunner

// NewFlushRunner creates a reusable CDC flush runner.
func NewFlushRunner(logger *zap.Logger) *FlushRunner {
	return &FlushRunner{runner: internalcdc.NewRunner(logger)}
}

// NewRunner creates a reusable CDC flush runner.
func NewRunner(logger *zap.Logger) *FlushRunner {
	return NewFlushRunner(logger)
}

// Close releases any cached DuckDB exporters held by the runner.
func (r *FlushRunner) Close() error {
	if r == nil || r.runner == nil {
		return nil
	}
	return r.runner.Close()
}

// RunOnce performs one full CDC flush pass using the reusable runner.
func (r *FlushRunner) RunOnce(
	ctx context.Context,
	cfg Config,
	s3Client S3ObjectClient,
	dryRun bool,
	schemaRegistry forma.SchemaRegistry,
) error {
	_, err := r.Flush(ctx, cfg, s3Client, dryRun, schemaRegistry)
	return err
}

// Flush is RunOnce plus the pass's report. The report is returned alongside
// a schema's flush error too.
func (r *FlushRunner) Flush(
	ctx context.Context,
	cfg Config,
	s3Client S3ObjectClient,
	dryRun bool,
	schemaRegistry forma.SchemaRegistry,
) (FlushReport, error) {
	if r == nil || r.runner == nil {
		return internalcdc.Flush(ctx, cfg, s3Client, dryRun, zap.NewNop(), schemaRegistry)
	}
	return r.runner.Flush(ctx, cfg, s3Client, dryRun, schemaRegistry)
}

// RunOnce performs one full CDC flush pass across all schemas with unflushed rows.
func RunOnce(
	ctx context.Context,
	cfg Config,
	s3Client S3ObjectClient,
	dryRun bool,
	logger *zap.Logger,
	schemaRegistry forma.SchemaRegistry,
) error {
	return internalcdc.RunOnce(ctx, cfg, s3Client, dryRun, logger, schemaRegistry)
}

// Flush is the package-level RunOnce plus the pass's report.
func Flush(
	ctx context.Context,
	cfg Config,
	s3Client S3ObjectClient,
	dryRun bool,
	logger *zap.Logger,
	schemaRegistry forma.SchemaRegistry,
) (FlushReport, error) {
	return internalcdc.Flush(ctx, cfg, s3Client, dryRun, logger, schemaRegistry)
}
