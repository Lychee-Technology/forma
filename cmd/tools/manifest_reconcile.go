package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
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

// schemaIDList collects the repeatable --allow-empty-manifest-schema values;
// each occurrence is one schema ID (comma-separated also accepted).
type schemaIDList []int16

func (s *schemaIDList) String() string {
	parts := make([]string, 0, len(*s))
	for _, id := range *s {
		parts = append(parts, strconv.Itoa(int(id)))
	}
	return strings.Join(parts, ",")
}

func (s *schemaIDList) Set(v string) error {
	for _, part := range strings.Split(v, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := strconv.ParseInt(part, 10, 16)
		if err != nil {
			return fmt.Errorf("invalid schema id %q: %w", part, err)
		}
		*s = append(*s, int16(id))
	}
	return nil
}

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
	// allowEmptyManifestSchemas waives the #463 empty-manifest GC guard for
	// exactly these schema IDs (never globally).
	allowEmptyManifestSchemas []int16
	verifyStamps    bool
	verifyChecksums bool
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
	manifestPrefix := fs.String("manifest-prefix", "", "Manifest prefix in S3 (must match the writers' manifest prefix: a mis-pointed value resolves every manifest empty, classifying the whole base tier as orphaned — --gc refuses that signature, #463)")
	manifestTemplate := fs.String("manifest-template", "manifest/{{.SchemaID}}.json", "Manifest path template (must match the writers' template — same #463 hazard as --manifest-prefix)")
	registryTable := fs.String("schema-registry-table", "", "Schema registry table for schema enumeration (required)")
	schemaID := fs.Int("schema-id", 0, "Reconcile only this schema (0 = all registered schemas)")
	repair := fs.Bool("repair", false, "Append manifest entries for delta-shaped orphans and promote complete init-shaped base orphan sets (coverage + eviction-safety verified)")
	gc := fs.Bool("gc", false, "Delete base-shaped and _tmp orphans older than the grace period")
	var allowEmptyManifest schemaIDList
	fs.Var(&allowEmptyManifest, "allow-empty-manifest-schema",
		"Schema ID whose EMPTY manifest is operator-confirmed legitimate: --gc then accepts zero manifest entries for that schema instead of refusing (#463). Repeatable / comma-separable; deliberately schema-explicit — there is no global override")
	verifyStamps := fs.Bool("verify-stamps", false, "Compare every listed entry's column stamp against the parquet footer (byte-truth check for the #256 stamp trust)")
	verifyChecksums := fs.Bool("verify-checksums", false, "Re-hash every checksum-stamped object and compare with the manifest stamp (#347 byte-integrity scrub; one full GET per stamped entry)")
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
	if len(allowEmptyManifest) > 0 && !*gc {
		return nil, fmt.Errorf("--allow-empty-manifest-schema only modifies --gc's empty-manifest guard; pass it together with --gc")
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
	opts.allowEmptyManifestSchemas = allowEmptyManifest
	opts.verifyStamps = *verifyStamps
	opts.verifyChecksums = *verifyChecksums
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

	r := newReconciler(opts, s3Client, db, logger)

	// Both the repair guards and --verify-stamps read parquet through the
	// DuckDB stats engine; only repair needs the Postgres liveness surface,
	// so LiveRows stays under its own gate to keep the "may be nil unless
	// Opts.Repair" contract on the field honest. --verify-checksums needs
	// neither: it hashes raw bytes straight off the S3 client, so it costs no
	// DuckDB session at all.
	if opts.repair || opts.verifyStamps {
		exporter, err := openReconcileStatsEngine(ctx, opts, logger)
		if err != nil {
			return fmt.Errorf("build stats engine: %w", err)
		}
		defer func() { _ = exporter.DB.Close() }()
		r.Stats = &reconcile.DuckStatsReader{DB: exporter.DB, Bucket: opts.s3.bucket}
	}
	if opts.repair {
		r.LiveRows = &reconcile.PGLiveRows{DB: db, Table: opts.entityMainTable}
	}

	logger.Info("starting manifest reconcile",
		zap.String("bucket", opts.s3.bucket),
		zap.String("data_prefix", opts.dataPrefix),
		zap.Bool("repair", opts.repair),
		zap.Bool("gc", opts.gc),
		zap.Bool("verify_stamps", opts.verifyStamps),
		zap.Bool("verify_checksums", opts.verifyChecksums))

	report, err := r.Run(ctx)
	if err != nil {
		return fmt.Errorf("reconcile failed: %w", err)
	}
	report.Render(os.Stdout)
	return reconcileExitError(report)
}

// newReconciler assembles the Reconciler from parsed flags and the opened
// clients. It is a separate function so the flag → behavior wiring is
// unit-testable without Postgres or S3: everything below is pure
// construction, and a dropped assignment here is otherwise invisible until
// production (Objects in particular — verifyChecksums would then answer a
// configuration error instead of scrubbing, and --repair would silently
// publish its adopted and promoted entries unstamped).
func newReconciler(opts *reconcileOptions, s3Client *s3.Client, db *sql.DB, logger *zap.Logger) *reconcile.Reconciler {
	objectStore := &reconcile.S3ObjectStore{Client: s3Client, Bucket: opts.s3.bucket}
	manifestStore := &manifest.S3Store{Client: s3Client, Bucket: opts.manifest.Bucket}
	resolver := manifest.PathResolver{Prefix: opts.manifest.Prefix, PathTemplate: opts.manifest.PathTemplate}
	return &reconcile.Reconciler{
		Lister:    objectStore,
		Deleter:   objectStore,
		Objects:   s3Client, // raw byte reads: the #347 checksum scrub and repair/promotion stamping
		Manifests: &reconcile.ResolverManifestStore{Store: manifestStore, Resolver: resolver},
		GCStates:  &reconcile.ManifestGCStateStore{Store: manifestStore, Resolver: resolver},
		Locker:    &reconcile.PGAdvisoryLocker{DB: db},
		Schemas: &reconcile.RegistrySchemaEnumerator{
			DB: db, Table: opts.registryTable, SchemaIDFilter: opts.schemaID,
		},
		Now:        time.Now,
		Bucket:     opts.s3.bucket,
		DataPrefix: opts.dataPrefix,
		Logger:     logger,
		Opts: reconcile.Options{
			Repair:                    opts.repair,
			GC:                        opts.gc,
			GCGrace:                   opts.gcGrace,
			VerifyStamps:              opts.verifyStamps,
			VerifyChecksums:           opts.verifyChecksums,
			MaxETagRetries:            opts.etagRetries,
			AllowEmptyManifestSchemas: opts.allowEmptyManifestSchemas,
		},
	}
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
//
// Credentials come from the same shared rule the merge engine uses,
// cdc.ResolveStaticS3Credentials (#329). See the WARNING on openMergeEngine
// for the deliberate narrowing that rule implies: chain-only credential
// sources no longer reach the DuckDB httpfs session.
func openReconcileStatsEngine(ctx context.Context, opts *reconcileOptions, logger *zap.Logger) (*cdc.DuckExporter, error) {
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
	}
	key, secret, token := cdc.ResolveStaticS3Credentials(duckCfg)
	if key == "" {
		logger.Warn("no static S3 credentials resolved for the DuckDB engine; httpfs will read s3:// unsigned (#329)")
	}
	exporter, err := cdc.NewDuckExporter(ctx, duckCfg, key, secret, token, logger)
	if err != nil {
		return nil, fmt.Errorf("open stats duckdb: %w", err)
	}
	return exporter, nil
}

// buildToolSQLDB opens a database/sql handle (pgx driver) for tools that
// need session-scoped Postgres features — the reconcile advisory lock pins
// a single connection, which pgxpool does not expose. Values are quoted so
func buildToolSQLDB(pg postgresFlags) (*sql.DB, error) {
	connStr := cdc.BuildPGDSN(cdc.PGDSNParams{
		Host:     pg.host,
		Port:     pg.port,
		User:     pg.user,
		Password: pg.resolvedPassword("PGPASSWORD"),
		DB:       pg.database,
		SSLMode:  pg.sslMode,
	})
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return nil, fmt.Errorf("open pg: %w", err)
	}
	return db, nil
}
