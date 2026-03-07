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

// FlushRunner reuses AWS/S3/DuckDB initialization across multiple flush passes.
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
	if r == nil || r.runner == nil {
		return internalcdc.RunOnce(ctx, cfg, s3Client, dryRun, zap.NewNop(), schemaRegistry)
	}
	return r.runner.RunOnce(ctx, cfg, s3Client, dryRun, schemaRegistry)
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
