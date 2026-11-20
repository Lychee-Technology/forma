package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/cdcv2"
	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/reconcile"
)

// discrepancyError signals that the reconcile run itself succeeded but left
// residual discrepancies; runToolMain maps it to exit code 2 so periodic
// checks can distinguish "inconsistent" (2) from "tool failed" (1).
type discrepancyError struct {
	count int
}

func (e *discrepancyError) Error() string {
	return fmt.Sprintf("%d schema(s) with residual discrepancies", e.count)
}

func (e *discrepancyError) ExitCode() int { return 2 }

// errRepairRemoved refuses --repair. Under Protocol V2 an object no
// generation lists is staging that never committed or garbage, never lost
// data, so there is nothing to adopt; and a lost manifest or object of a
// committed generation cannot be rebuilt from S3. The recovery is a re-init.
var errRepairRemoved = errors.New("--repair is removed under Protocol V2: an unlisted object is uncommitted staging or garbage, " +
	"never lost data, and a lost manifest or object cannot be rebuilt from S3; re-initialize the schema with cdc-init instead " +
	"(docs/manifest-reconcile.md)")

// Defaults of the protocol's time parameters (docs/manifest-reconcile.md,
// "Retention"). The grace must cover the reader's pass deadline plus the
// clock margin, and the orphan age the writers' staging budget plus it.
const (
	defaultRetentionGrace = 15 * time.Minute
	defaultClockMargin    = time.Minute
	defaultOrphanAge      = 2 * time.Hour
)

// reconcileOptions carries the parsed manifest-reconcile flag values.
type reconcileOptions struct {
	s3            s3Flags
	pg            postgresFlags
	db            cdc.CDCConfig // the publication database the records live on
	dataPrefix    string
	entityMain    string
	registryTable string
	schemaID      int16
	run           reconcile.Options
	// bindStorage binds the deployment to the configured storage target
	// and runs nothing else.
	bindStorage bool
	// endpoint is the endpoint the run's S3 client sends its requests to,
	// set once the client is built (buildReconcileObjects).
	endpoint string
}

// parseReconcileFlags parses and validates the subcommand flags. A nil
// options value with a nil error means help was requested.
func parseReconcileFlags(args []string) (*reconcileOptions, error) {
	fs := flag.NewFlagSet("manifest-reconcile", flag.ContinueOnError)
	fs.SetOutput(flag.CommandLine.Output())

	opts := &reconcileOptions{}
	opts.s3.register(fs, s3FlagOptions{
		bucketUsage:    "S3 bucket of the schemas' parquet objects and manifests (required)",
		bucketRequired: true,
	})
	opts.pg.register(fs, postgresFlagOptions{
		hostFlag: "pg-host", portFlag: "pg-port", userFlag: "pg-user", passwordFlag: "pg-password",
		databaseFlag: "pg-db", sslModeFlag: "pg-ssl-mode",
		hostDefault: "localhost", portDefault: 5432, userDefault: "postgres", databaseDefault: "forma", sslModeDefault: "require",
		hostUsage: "PostgreSQL host of the publication records", portUsage: "PostgreSQL port",
		userUsage: "PostgreSQL user", passwordUsage: "PostgreSQL password (or set PGPASSWORD env)",
		databaseUsage: "PostgreSQL database", sslModeUsage: "PostgreSQL sslmode",
		includeUseIAM: true, useIAMFlag: "pg-use-iam", useIAMUsage: "Use IAM authentication for PostgreSQL",
		engineFlag: "pg-engine",
	})
	dataPrefix := fs.String("data-prefix", defaultCDCDataPrefix, "S3 data prefix of the V2 object layout: cdc-flush and cdc-init's -s3-prefix")
	fs.StringVar(&opts.entityMain, "entity-main-table", "entity_main",
		"Entity main table name; the publication tables and the schema lock are named after it")
	registryTable := fs.String("schema-registry-table", "", "Schema registry table for schema enumeration (required)")
	schemaID := fs.Int("schema-id", 0, "Reconcile only this schema (0 = every registered schema and every schema with a record)")
	gc := fs.Bool("gc", false, "Collect garbage: raise the horizon, retire superseded generations, delete what no reader can still scan. "+
		"Without it the run is a read-only diagnosis that reports what a collection would delete")
	fs.BoolVar(&opts.bindStorage, "bind-storage", false,
		"Bind this database's deployment to the URL the run's client sends the bucket's requests to (--s3-endpoint, or the "+
			"AWS SDK's endpoint configuration such as AWS_ENDPOINT_URL_S3 without it, or the region's AWS endpoint, with the "+
			"addressing --s3-use-path chooses), --s3-bucket and "+
			"--data-prefix, the target its writers publish into, and exit. Garbage collection deletes nothing under any other target. Run it once, with the writers' own values and environment; "+
			"an existing binding to another target is kept and the run fails")
	repair := fs.Bool("repair", false, "Removed under Protocol V2; refused. Re-initialize with cdc-init instead")
	fs.BoolVar(&opts.run.VerifyChecksums, "verify-checksums", false,
		"Hash every object a retained generation lists and compare it with its manifest's sha256 and size (one full GET per object)")
	fs.DurationVar(&opts.run.RetentionGrace, "retention-grace", defaultRetentionGrace,
		"G: how long a superseded generation stays after a pass retired it; at least the reader's pass deadline plus --clock-margin")
	fs.DurationVar(&opts.run.ClockMargin, "clock-margin", defaultClockMargin,
		"C: drift allowed between the readers' clocks, the database clock and S3's LastModified")
	fs.DurationVar(&opts.run.OrphanAge, "orphan-age", defaultOrphanAge,
		"A: how far behind the database clock the collector raises its horizon; at least the writers' staging budget plus --clock-margin")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, nil
		}
		return nil, err
	}
	if *repair {
		return nil, errRepairRemoved
	}
	if opts.bindStorage && *gc {
		return nil, errors.New("--bind-storage binds and exits; run --gc separately")
	}
	if err := opts.s3.validate(true); err != nil {
		return nil, fmt.Errorf("validate s3 flags: %w", err)
	}
	if *registryTable == "" {
		return nil, fmt.Errorf("--schema-registry-table is required")
	}
	if *schemaID < 0 || *schemaID > 32767 {
		return nil, fmt.Errorf("--schema-id %d is not a schema id", *schemaID)
	}
	if err := validateParquetPrefixFlag("data-prefix", *dataPrefix); err != nil {
		return nil, err
	}
	engine, err := opts.pg.cdcEngine("pg-engine")
	if err != nil {
		return nil, err
	}
	opts.run.GC = *gc
	opts.run.ReaderDeadline = federated.DefaultPublishedPassDeadline
	if err := validateReconcileTimes(opts.run); err != nil {
		return nil, err
	}
	opts.db = cdc.CDCConfig{
		PGHost: opts.pg.host, PGPort: opts.pg.port, PGUser: opts.pg.user,
		PGPassword: opts.pg.resolvedPassword("PGPASSWORD"), PGDB: opts.pg.database, PGSSLMode: opts.pg.sslMode,
		PGUseIAM: opts.pg.useIAM, Engine: engine,
		S3Region: opts.s3.region, S3Endpoint: opts.s3.endpoint, S3UsePath: opts.s3.usePath,
	}
	opts.dataPrefix = *dataPrefix
	opts.registryTable = *registryTable
	opts.schemaID = int16(*schemaID)
	return opts, nil
}

// validateReconcileTimes refuses time parameters under which a reader could
// lose an object it scans (reconcile.Options.Validate), a clock margin of
// zero, which trusts S3's and the database's clocks to agree exactly, and an
// orphan age that would fence writers still inside their staging budget.
func validateReconcileTimes(o reconcile.Options) error {
	if o.ClockMargin <= 0 {
		return fmt.Errorf("--clock-margin %s must be positive: it covers the drift between S3's LastModified and the database clock", o.ClockMargin)
	}
	if err := o.Validate(); err != nil {
		return fmt.Errorf("validate --retention-grace, --clock-margin and --orphan-age: %w", err)
	}
	floor, ok := reconcile.AddDurations(cdcv2.DefaultStagingBudget, o.ClockMargin)
	if !ok {
		return fmt.Errorf("--clock-margin %s plus the writers' staging budget %s overflows a duration", o.ClockMargin, cdcv2.DefaultStagingBudget)
	}
	if o.OrphanAge < floor {
		return fmt.Errorf("--orphan-age %s is below the writers' staging budget %s plus --clock-margin: the horizon would refuse publications still inside their budget",
			o.OrphanAge, cdcv2.DefaultStagingBudget)
	}
	return nil
}

var runManifestReconcileFn = runManifestReconcile

func runManifestReconcile(ctx context.Context, args []string) error {
	opts, err := parseReconcileFlags(args)
	if err != nil {
		return fmt.Errorf("parse manifest-reconcile flags: %w", err)
	}
	if opts == nil { // help requested
		return nil
	}
	logger, err := buildToolLogger(false)
	if err != nil {
		return fmt.Errorf("create logger: %w", err)
	}
	defer func() { _ = logger.Sync() }()

	// The client is built before the database opens so that a client whose
	// destination cannot be told fails before any connection.
	objects, err := buildReconcileObjects(ctx, opts)
	if err != nil {
		return err
	}
	db, err := openReconcileDBFn(ctx, opts.db, logger)
	if err != nil {
		return fmt.Errorf("open the publication database: %w", err)
	}
	defer func() { _ = db.Close() }()
	if opts.bindStorage {
		return bindReconcileStorage(ctx, db, opts, os.Stdout)
	}
	r, err := newReconciler(opts, objects, db, logger)
	if err != nil {
		return fmt.Errorf("build reconciler: %w", err)
	}
	logger.Info("starting manifest reconcile",
		zap.String("endpoint", opts.endpoint), zap.String("bucket", opts.s3.bucket), zap.String("data_prefix", opts.dataPrefix),
		zap.Bool("gc", opts.run.GC), zap.Bool("verify_checksums", opts.run.VerifyChecksums),
		zap.Duration("retention_grace", opts.run.RetentionGrace), zap.Duration("orphan_age", opts.run.OrphanAge))
	report, err := r.Run(ctx)
	if err != nil {
		return fmt.Errorf("reconcile failed: %w", err)
	}
	report.Render(os.Stdout)
	return reconcileExitError(report)
}

// openReconcileDBFn opens the publication database as cdc-flush and the
// compactor open theirs; tests replace it.
var openReconcileDBFn = func(ctx context.Context, cfg cdc.CDCConfig, logger *zap.Logger) (*sql.DB, error) {
	return cdc.OpenPublicationDB(ctx, cfg, logger)
}

// newReconciler assembles the Reconciler from parsed flags and the opened
// clients, so the flag-to-behaviour wiring is testable without PostgreSQL or
// S3.
func newReconciler(opts *reconcileOptions, objects reconcile.ObjectStore, db *sql.DB, logger *zap.Logger) (*reconcile.Reconciler, error) {
	tables := publication.TablesFor(opts.entityMain)
	r := &reconcile.Reconciler{
		DB:      db,
		Tables:  tables,
		Layout:  manifestv2.Layout{Prefix: opts.dataPrefix},
		Objects: objects,
		Schemas: &reconcile.Schemas{DB: db, Registry: opts.registryTable, Tables: tables, Only: opts.schemaID},
		Logger:  logger,
		Opts:    opts.run,
		Storage: opts.storageTarget(),
	}
	if !opts.run.GC {
		// The diagnosis takes no lock: on Aurora DSQL the lock is a lease
		// row, and a read-only principal must be able to run the check.
		return r, nil
	}
	// The lock cdc-flush and cdc-init of the same engine take (#633).
	locker, err := cdc.NewSchemaLocker(db, opts.db.Engine, opts.entityMain)
	if err != nil {
		return nil, fmt.Errorf("manifest-reconcile schema lock: %w", err)
	}
	r.Locker = locker
	return r, nil
}

// buildReconcileObjects builds the S3 client the run lists, reads and
// deletes through, and records in opts the URL that client sends the
// bucket's requests to (reconcile.ClientEndpoint): the one the run is bound
// by and checked against. The SDK may take its endpoint from the
// environment (AWS_ENDPOINT_URL_S3, AWS_ENDPOINT_URL, a profile's
// endpoint_url) when --s3-endpoint is not given, and --s3-use-path changes
// the bucket's addressing at a given --s3-endpoint.
func buildReconcileObjects(ctx context.Context, opts *reconcileOptions) (*reconcile.S3Objects, error) {
	client, err := buildToolS3Client(ctx, opts.s3.region, opts.s3.endpoint, opts.s3.usePath)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	objects := &reconcile.S3Objects{Client: client, Bucket: opts.s3.bucket}
	endpoint, err := objects.Endpoint(ctx)
	if err != nil {
		return nil, fmt.Errorf("tell the S3 endpoint the run sends its requests to: %w", err)
	}
	opts.endpoint = endpoint
	return objects, nil
}

// storageTarget is the target the run lists and deletes in: the endpoint
// its S3 client sends requests to (buildReconcileObjects), not the
// --s3-endpoint flag, which an empty value leaves to the environment.
func (o *reconcileOptions) storageTarget() publication.StorageTarget {
	return publication.StorageTarget{Endpoint: o.endpoint, Bucket: o.s3.bucket, Prefix: o.dataPrefix}
}

// errStorageBoundElsewhere refuses --bind-storage on a deployment bound to
// another target: only an operator who deleted the binding row rebinds it.
var errStorageBoundElsewhere = errors.New("the deployment is bound to another storage target")

// bindReconcileStorage creates the storage binding table and binds the
// deployment to the configured target, unless it is bound already; a
// binding to another target is kept and fails the run. The DDL is its own
// statement, as Aurora DSQL needs.
func bindReconcileStorage(ctx context.Context, db *sql.DB, opts *reconcileOptions, out io.Writer) error {
	tables := publication.TablesFor(opts.entityMain)
	if _, err := db.ExecContext(ctx, publication.StorageStatement(opts.db.Engine, opts.entityMain)); err != nil {
		return fmt.Errorf("create the storage binding table %s: %w", tables.Storage, err)
	}
	want := opts.storageTarget()
	b, err := publication.BindStorage(ctx, publication.DB(db), tables, want)
	if err != nil {
		return fmt.Errorf("bind the deployment's storage: %w", err)
	}
	if !b.Target.Same(want) {
		return fmt.Errorf("bind the deployment's storage to %s: it is bound to %s since %d; delete the row of %s by hand "+
			"only once the writers publish into the new target (docs/manifest-reconcile.md, \"Storage binding\"): %w",
			want, b.Target, b.BoundAt, tables.Storage, errStorageBoundElsewhere)
	}
	_, err = fmt.Fprintf(out, "the deployment is bound to %s since %d\n", b.Target, b.BoundAt)
	if err != nil {
		return fmt.Errorf("write the binding: %w", err)
	}
	return nil
}

// reconcileExitError maps a rendered report to the process outcome:
// per-schema tool failures are a plain error (exit 1), since monitoring must
// not mistake an S3 outage for data inconsistency, while findings, skipped
// schemas and inconclusive diagnoses exit 2 via discrepancyError, and a clean
// run exits 0.
func reconcileExitError(report reconcile.Report) error {
	var failed []string
	residual := 0
	for _, s := range report.Schemas {
		if s.Err != nil {
			failed = append(failed, fmt.Sprintf("schema %d: %v", s.SchemaID, s.Err))
			continue
		}
		if s.Residual() {
			residual++
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("reconcile failed for %d schema(s): %s", len(failed), strings.Join(failed, "; "))
	}
	if residual > 0 {
		return &discrepancyError{count: residual}
	}
	return nil
}
