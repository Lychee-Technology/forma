package cdc

import (
	"context"
	"database/sql"

	"github.com/lychee-technology/forma"
	internalcdc "github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/cdcv2"
	"github.com/lychee-technology/forma/internal/publication"
	"go.uber.org/zap"
)

// Config controls change_log flushing and export behavior.
type Config = internalcdc.CDCConfig

// S3Client is the object API a flush writes through: every parquet object
// and manifest generation is written once with a conditional put
// (If-None-Match: *), and a flush gets its parent manifest. *s3.Client
// satisfies it.
type S3Client = internalcdc.S3Client

// FlushReport is what a flush pass reports besides its error: what each
// schema published (Flushed) and the schemas it left unexported because the
// parquet tiers cannot hold them (#704). It does not depend on the logger, so
// a runner built with NewFlushRunner(nil) still sees all of it.
type FlushReport = internalcdc.FlushReport

// SchemaFlush is what one schema's flush published: the immutable manifest
// generations it committed through the schema's publication record, and the
// change_log entries it marked flushed in the same transactions.
type SchemaFlush = internalcdc.SchemaFlush

// SkippedSchema is one schema a flush pass did not export, and why. Its
// change_log rows stay unflushed, and federated reads of it are answered
// from PostgreSQL. Skipping it does not fail the pass.
type SkippedSchema = internalcdc.SkippedSchema

// ErrNoPublicationRecord matches a flush of a schema that has no publication
// record. init-db writes the record when it registers a schema, or recovers
// a lost one; until then nothing of the schema is flushed. It is a deployment
// fault, not a transient one.
var ErrNoPublicationRecord = publication.ErrNoRecord

// ErrAwaitingInit matches a flush of a schema whose lost record init-db
// recovered AWAITING_INIT: only a cdc-init publishes its next generation. Run
// cdc-init for the schema; its rows stay unflushed until then.
var ErrAwaitingInit = publication.ErrAwaitingInit

// ErrSchemaDropped matches a flush of a dropped schema whose id is not
// registered again yet.
var ErrSchemaDropped = publication.ErrDropped

// ErrPublicationUnresolved matches a flush whose publication COMMIT went
// without an answer and whose outcome neither the writer nor one settling
// attempt could tell. Its marks commit with it, so its entries are flushed or
// dirty as change_log says, and the next flush exports what is still dirty.
var ErrPublicationUnresolved = cdcv2.ErrUnresolved

// ErrBatchSizeTooLarge matches a flush configuration whose batch a
// PostgreSQL flush cannot mark in one statement (more than
// cdcv2.MaxMarkEntries entries). RunOnce returns it before any side effect.
var ErrBatchSizeTooLarge = internalcdc.ErrBatchSizeTooLarge

// ErrRegistryEpochsRequired refuses a flush whose Config carries no
// RegistryEpochs, before any I/O. Read them with ReadRegistryEpochs before
// loading the schema registry the flush is given.
var ErrRegistryEpochsRequired = internalcdc.ErrRegistryEpochsRequired

// ReadRegistryEpochs reads every schema's publication epoch on db, for
// Config.RegistryEpochs. Call it before loading the schema registry a flush
// is given, and again whenever that registry is loaded again: a flush refuses
// a schema whose record is at another epoch under its schema lock, since the
// registry may describe the schema it was before a drop. entityMain names the
// publication tables ("entity_main" when empty).
func ReadRegistryEpochs(ctx context.Context, db *sql.DB, entityMain string) (map[int16]int64, error) {
	return internalcdc.ReadRegistryEpochs(ctx, db, entityMain)
}

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
	s3Client S3Client,
	dryRun bool,
	schemaRegistry forma.SchemaRegistry,
) (FlushReport, error) {
	if r == nil || r.runner == nil {
		return internalcdc.RunOnce(ctx, cfg, s3Client, dryRun, zap.NewNop(), schemaRegistry)
	}
	return r.runner.RunOnce(ctx, cfg, s3Client, dryRun, schemaRegistry)
}

// RunOnce performs one full CDC flush pass across all schemas with unflushed
// rows: for each it stages delta objects and an immutable manifest
// generation, then commits the schema's publication record and the marks of
// the flushed entries in one transaction. s3Client nil uses a client built
// from the AWS configuration. The report is returned alongside a schema's
// flush error too.
func RunOnce(
	ctx context.Context,
	cfg Config,
	s3Client S3Client,
	dryRun bool,
	logger *zap.Logger,
	schemaRegistry forma.SchemaRegistry,
) (FlushReport, error) {
	return internalcdc.RunOnce(ctx, cfg, s3Client, dryRun, logger, schemaRegistry)
}
