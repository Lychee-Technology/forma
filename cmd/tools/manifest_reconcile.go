package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"go.uber.org/zap"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifest"
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

// reconcileOptions carries the parsed manifest-reconcile flag values.
type reconcileOptions struct {
	s3              s3Flags
	pg              postgresFlags
	manifest        cdc.ManifestConfig
	dataPrefix      string
	entityMainTable string
	registryTable   string
	schemaID        int
	repair          bool
	gc              bool
	gcGrace         time.Duration
	etagRetries     int
	duck            duckExportFlags
}

// parseReconcileFlags parses and validates the subcommand flags. A nil
// options value with a nil error means help was requested.
func parseReconcileFlags(args []string) (*reconcileOptions, error) {
	fs := flag.NewFlagSet("manifest-reconcile", flag.ContinueOnError)
	fs.SetOutput(flag.CommandLine.Output())

	opts := &reconcileOptions{}
	opts.s3.register(fs, s3FlagOptions{
		bucketUsage:    "S3 bucket for parquet/manifest files (required)",
		bucketRequired: true,
	})
	opts.pg.register(fs, postgresFlagOptions{
		hostFlag:        "pg-host",
		portFlag:        "pg-port",
		userFlag:        "pg-user",
		passwordFlag:    "pg-password",
		databaseFlag:    "pg-db",
		sslModeFlag:     "pg-ssl-mode",
		hostDefault:     "localhost",
		portDefault:     5432,
		userDefault:     "postgres",
		databaseDefault: "forma",
		sslModeDefault:  "require",
		hostUsage:       "PostgreSQL host",
		portUsage:       "PostgreSQL port",
		userUsage:       "PostgreSQL user",
		passwordUsage:   "PostgreSQL password (or set PGPASSWORD env)",
		databaseUsage:   "PostgreSQL database",
		sslModeUsage:    "PostgreSQL sslmode",
	})

	// Must match the prefix the flusher/init exported under (cdc-flush
	// --s3-prefix / compactor --data-prefix): listing a different prefix
	// reports every real file as dangling and every listed key as orphaned.
	dataPrefix := fs.String("data-prefix", "data", "Data prefix for parquet files (must match the flusher's prefix)")
	entityMainTable := fs.String("entity-main-table", "entity_main", "Entity main table (repair guard's liveness check)")
	manifestPrefix := fs.String("manifest-prefix", "", "Manifest prefix in S3")
	manifestTemplate := fs.String("manifest-template", "manifest/{{.SchemaID}}.json", "Manifest path template")
	registryTable := fs.String("schema-registry-table", "", "Schema registry table for schema enumeration (required)")
	schemaID := fs.Int("schema-id", 0, "Reconcile only this schema (0 = all registered schemas)")
	repair := fs.Bool("repair", false, "Append manifest entries for delta-shaped orphans (metadata recomputed from parquet contents)")
	gc := fs.Bool("gc", false, "Delete base-shaped and _tmp orphans older than the grace period")
	gcGrace := fs.Duration("gc-grace", 15*time.Minute, "Minimum object age before --gc may delete it")
	etagRetries := fs.Int("etag-retries", 3, "Manifest save retries on optimistic-concurrency conflict")
	opts.duck.register(fs, duckExportFlagOptions{memLimitDefault: "4GB", queryTimeout: 5 * time.Minute})

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, nil
		}
		return nil, err
	}
	if err := opts.s3.validate(true); err != nil {
		return nil, fmt.Errorf("validate s3 flags: %w", err)
	}
	if *registryTable == "" {
		return nil, fmt.Errorf("--schema-registry-table is required")
	}

	if *gc && *gcGrace <= 0 {
		return nil, fmt.Errorf("--gc-grace must be positive; a zero grace would delete a live compaction's staging objects")
	}

	opts.manifest = cdc.ManifestConfig{
		Bucket:       opts.s3.bucket,
		Prefix:       *manifestPrefix,
		PathTemplate: *manifestTemplate,
	}
	opts.dataPrefix = *dataPrefix
	opts.entityMainTable = *entityMainTable
	opts.registryTable = *registryTable
	opts.schemaID = *schemaID
	opts.repair = *repair
	opts.gc = *gc
	opts.gcGrace = *gcGrace
	opts.etagRetries = *etagRetries
	return opts, nil
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

	db, err := buildToolSQLDB(opts.pg)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer func() { _ = db.Close() }()

	s3Client, err := buildToolS3Client(ctx, opts.s3.region, opts.s3.endpoint, opts.s3.usePath)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	objectStore := &reconcile.S3ObjectStore{Client: s3Client, Bucket: opts.s3.bucket}
	manifestStore := &manifest.S3Store{Client: s3Client, Bucket: opts.manifest.Bucket}
	resolver := manifest.PathResolver{Prefix: opts.manifest.Prefix, PathTemplate: opts.manifest.PathTemplate}
	r := &reconcile.Reconciler{
		Lister:     objectStore,
		Deleter:    objectStore,
		Manifests:  &reconcile.ResolverManifestStore{Store: manifestStore, Resolver: resolver},
		GCStates:   &reconcile.ManifestGCStateStore{Store: manifestStore, Resolver: resolver},
		Locker:     &reconcile.PGAdvisoryLocker{DB: db},
		Schemas:    &reconcile.RegistrySchemaEnumerator{DB: db, Table: opts.registryTable, SchemaIDFilter: opts.schemaID},
		Now:        time.Now,
		Bucket:     opts.s3.bucket,
		DataPrefix: opts.dataPrefix,
		Logger:     logger,
		Opts: reconcile.Options{
			Repair:         opts.repair,
			GC:             opts.gc,
			GCGrace:        opts.gcGrace,
			MaxETagRetries: opts.etagRetries,
		},
	}

	if opts.repair {
		exporter, err := openReconcileStatsEngine(ctx, opts, logger)
		if err != nil {
			return fmt.Errorf("build stats engine: %w", err)
		}
		defer func() { _ = exporter.DB.Close() }()
		r.Stats = &reconcile.DuckStatsReader{DB: exporter.DB, Bucket: opts.s3.bucket}
		r.LiveRows = &reconcile.PGLiveRows{DB: db, Table: opts.entityMainTable}
	}

	logger.Info("starting manifest reconcile",
		zap.String("bucket", opts.s3.bucket),
		zap.String("data_prefix", opts.dataPrefix),
		zap.Bool("repair", opts.repair),
		zap.Bool("gc", opts.gc))

	report, err := r.Run(ctx)
	if err != nil {
		return fmt.Errorf("reconcile failed: %w", err)
	}
	report.Render(os.Stdout)
	return reconcileExitError(report)
}

// reconcileExitError maps a rendered report to the process outcome:
// per-schema tool failures are a plain error (exit 1) — monitoring must not
// mistake an S3 outage for data inconsistency — while pure residual
// discrepancies exit 2 via discrepancyError, and a clean run exits 0.
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

// openReconcileStatsEngine builds the CDC DuckDB exporter the repair path
// reads parquet stats through — the same httpfs+credentials wiring the
// compactor's merge engine uses. The pool is pinned to one connection: the
// exporter's S3 session settings apply per connection, and a second pooled
// connection would read s3:// without credentials (#285).
func openReconcileStatsEngine(ctx context.Context, opts *reconcileOptions, logger *zap.Logger) (*cdc.DuckExporter, error) {
	key, secret, token := resolveMergeCredentials(ctx, opts.s3.region, logger)
	duckCfg := cdc.CDCConfig{
		DuckDBPath:              opts.duck.duckDBPath,
		DuckThreads:             opts.duck.duckThreads,
		DuckMemLimit:            opts.duck.duckMemLimit,
		QueryTimeout:            opts.duck.queryTimeout,
		ParquetCompression:      opts.duck.parquetCompression,
		ParquetCompressionLevel: opts.duck.parquetCompressionLevel,
		S3Bucket:                opts.s3.bucket,
		S3Endpoint:              opts.s3.endpoint,
		S3Region:                opts.s3.region,
		S3UseSSL:                opts.s3.useSSL,
		S3UsePath:               opts.s3.usePath,
		S3SessionToken:          token,
	}
	exporter, err := cdc.NewDuckExporter(ctx, duckCfg, key, secret, logger)
	if err != nil {
		return nil, fmt.Errorf("open stats duckdb: %w", err)
	}
	exporter.DB.SetMaxOpenConns(1)
	return exporter, nil
}

// buildToolSQLDB opens a database/sql handle (pgx driver) for tools that
// need session-scoped Postgres features — the reconcile advisory lock pins
// a single connection, which pgxpool does not expose. Values are quoted so
// passwords with spaces or quotes survive DSN parsing (the sibling copies
// in internal/cdc/flusher.go and init.go still interpolate raw — follow-up:
// extract one shared DSN builder).
func buildToolSQLDB(pg postgresFlags) (*sql.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		quotePGConnValue(pg.host), pg.port, quotePGConnValue(pg.user),
		quotePGConnValue(pg.resolvedPassword("PGPASSWORD")),
		quotePGConnValue(pg.database), quotePGConnValue(pg.sslMode))
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return nil, fmt.Errorf("open pg: %w", err)
	}
	return db, nil
}

// quotePGConnValue quotes one keyword/value DSN value per libpq rules:
// wrapped in single quotes with backslash and single-quote escaped.
func quotePGConnValue(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `'`, `\'`)
	return "'" + v + "'"
}
