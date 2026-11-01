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
// left unexported because the parquet tiers cannot hold them (#704). It does
// not depend on the logger, so a runner built with NewFlushRunner(nil) still
// sees every skip.
type FlushReport = internalcdc.FlushReport

// SkippedSchema is one schema a flush pass did not export, and why. Its
// change_log rows stay unflushed, and federated reads of it are answered
// from PostgreSQL. Skipping it does not fail the pass.
type SkippedSchema = internalcdc.SkippedSchema

// ErrManifestStoreUnwired matches a flush refused because it names no
// ManifestTemplate, so it cannot list its delta in a manifest, while a
// manifest-driven reader may depend on one (#474): either the run does not
// declare GlobOnly, or it does and the schema's manifest at the default
// location lists files. It is a configuration error, not a transient one:
// retrying the same configuration is refused again, and the refused rows stay
// in the hot tier. It is the same value internal/cdc returns, so errors.Is
// matches the errors RunOnce returns.
var ErrManifestStoreUnwired = internalcdc.ErrManifestStoreUnwired

// ErrGlobOnlyWithTemplate matches a flush configuration that declares
// GlobOnly and also names a ManifestTemplate. Like ErrManifestStoreUnwired it
// is a configuration error that RunOnce returns before any side effect.
var ErrGlobOnlyWithTemplate = internalcdc.ErrGlobOnlyWithTemplate

// ErrSchemaLockLost matches a flush whose per-schema lock ended before its
// work did: under the aurora-dsql engine, a lease the run could not renew in
// time, or one another holder took over after it expired (#633). The schema's
// unfinished batch stays in the hot tier, so the condition is operational,
// not a configuration error: a later run can flush it once the lock can be
// held again. It is the same value internal/cdc returns, so errors.Is matches
// the errors RunOnce returns.
//
// A flush that finds a schema's lock held by another holder skips that schema
// without an error, so package cdc has no contention sentinel.
var ErrSchemaLockLost = internalcdc.ErrSchemaLockLost

// ErrSchemaAttrCacheUnavailable matches a flush that could not resolve a
// schema's attribute metadata cache. A run checks every schema's cache before
// any side effect, so one unresolvable schema fails the whole pass and
// flushes nothing (#193). It is the same value internal/cdc returns, so
// errors.Is matches the errors RunOnce returns.
//
// A match alone does not say whether retrying can help, because it covers
// two different failures:
//
//   - The registry returned no metadata: the cache is empty because the
//     schema is not registered, or RunOnce was given a nil registry. That is
//     a configuration error: retrying with the same registry fails again.
//   - The registry's lookup itself returned an error. The error RunOnce
//     returns then also wraps that lookup error, and whether it is transient
//     is the registry's to say: a caller-supplied SchemaRegistry may fail
//     once (a dropped connection, a timeout) and succeed on a later attempt.
//
// A caller deciding whether to retry inspects the wrapped cause with
// errors.Is or errors.As against the errors its registry returns, rather
// than treating every match as permanent.
var ErrSchemaAttrCacheUnavailable = internalcdc.ErrSchemaAttrCacheUnavailable

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

// RunOnce performs one full CDC flush pass using the reusable runner. The
// report is returned alongside a schema's flush error too.
func (r *FlushRunner) RunOnce(
	ctx context.Context,
	cfg Config,
	s3Client S3ObjectClient,
	dryRun bool,
	schemaRegistry forma.SchemaRegistry,
) (FlushReport, error) {
	if r == nil || r.runner == nil {
		return internalcdc.RunOnce(ctx, cfg, s3Client, dryRun, zap.NewNop(), schemaRegistry)
	}
	return r.runner.RunOnce(ctx, cfg, s3Client, dryRun, schemaRegistry)
}

// RunOnce performs one full CDC flush pass across all schemas with unflushed
// rows. The report is returned alongside a schema's flush error too.
func RunOnce(
	ctx context.Context,
	cfg Config,
	s3Client S3ObjectClient,
	dryRun bool,
	logger *zap.Logger,
	schemaRegistry forma.SchemaRegistry,
) (FlushReport, error) {
	return internalcdc.RunOnce(ctx, cfg, s3Client, dryRun, logger, schemaRegistry)
}
