package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/lychee-technology/forma/internal/bootstrap"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/s3endpoint"
	"github.com/lychee-technology/forma/internal/telemetry"
	"go.uber.org/zap"
)

// compactorOptions carries the parsed compactor flag values.
type compactorOptions struct {
	s3         s3Flags
	pg         postgresFlags
	compact    cdc.CompactionConfig
	db         cdc.CDCConfig // the publication database the record lives on
	entityMain string
	dataPrefix string
	duck       duckExportFlags
}

// parseCompactorFlags parses and validates the subcommand flags. A nil
// options value with a nil error means help was requested.
func parseCompactorFlags(args []string) (*compactorOptions, error) {
	fs := flag.NewFlagSet("compactor", flag.ContinueOnError)
	fs.SetOutput(flag.CommandLine.Output())

	opts := &compactorOptions{}
	schemaID := fs.Int("schema-id", 0, "Schema ID to compact (required)")
	opts.s3.register(fs, s3FlagOptions{
		bucketUsage:    "S3 bucket of the schema's parquet objects and manifests (required)",
		bucketRequired: true,
	})
	opts.pg.register(fs, postgresFlagOptions{
		hostFlag: "pg-host", portFlag: "pg-port", userFlag: "pg-user", passwordFlag: "pg-password",
		databaseFlag: "pg-db", sslModeFlag: "pg-ssl-mode",
		hostDefault: "localhost", portDefault: 5432, userDefault: "postgres", databaseDefault: "forma", sslModeDefault: "require",
		hostUsage: "PostgreSQL host of the schema's publication record", portUsage: "PostgreSQL port",
		userUsage: "PostgreSQL user", passwordUsage: "PostgreSQL password (or set PGPASSWORD env)",
		databaseUsage: "PostgreSQL database", sslModeUsage: "PostgreSQL sslmode",
		includeUseIAM: true, useIAMFlag: "pg-use-iam", useIAMUsage: "Use IAM authentication for PostgreSQL",
		engineFlag: "pg-engine",
	})
	fs.StringVar(&opts.entityMain, "entity-main-table", "entity_main",
		"Entity main table name; the publication record's tables are named after it")

	targetBaseSizeMB := fs.Int("target-base-size-mb", 256, "Delta tier size in MB at which its files are merged into one base object")
	maxDeltaSizeMB := fs.Int("max-delta-size-mb", 50, "Maximum delta size in MB")
	dirtyRatioPct := fs.Int("dirty-ratio-pct", 5, "Dirty ratio percentage (delta rows over base rows) past which every file is merged")

	skipInputChecksumVerify := fs.Bool("skip-input-checksum-verify", false,
		"Skip the #347 pre-merge integrity gate, which hashes every merge source against the sha256 its manifest lists. "+
			"Verification is on by default; use this flag only to unwedge compaction when a source is known-corrupt and the "+
			"corruption has been triaged")

	maxRetries := fs.Int("max-retries", 5, "How often a compaction that lost to a rival compaction or cdc-init reads the head and merges again")

	dataPrefix := fs.String("data-prefix", defaultCDCDataPrefix, "S3 data prefix of the V2 object layout: cdc-flush and cdc-init's -s3-prefix")

	// Merge engine: DuckDB reads the schema's parquet objects via httpfs,
	// so it needs the same S3 wiring the CDC exporters use.
	opts.duck.register(fs, duckExportFlagOptions{memLimitDefault: "4GB", queryTimeout: 5 * time.Minute})

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, nil
		}
		return nil, err
	}
	if *schemaID == 0 {
		return nil, fmt.Errorf("--schema-id is required")
	}
	if err := opts.s3.validate(true); err != nil {
		return nil, fmt.Errorf("validate s3 flags: %w", err)
	}
	if err := validateParquetPrefixFlag("data-prefix", *dataPrefix); err != nil {
		return nil, err
	}
	engine, err := opts.pg.cdcEngine("pg-engine")
	if err != nil {
		return nil, err
	}

	opts.compact = cdc.CompactionConfig{
		SchemaID:                int16(*schemaID),
		TargetBaseSizeMB:        *targetBaseSizeMB,
		MaxDeltaSizeMB:          *maxDeltaSizeMB,
		DirtyRatioPct:           *dirtyRatioPct,
		SkipInputChecksumVerify: *skipInputChecksumVerify,
		MaxRetries:              *maxRetries,
	}.WithDefaults()
	opts.db = cdc.CDCConfig{
		PGHost: opts.pg.host, PGPort: opts.pg.port, PGUser: opts.pg.user,
		PGPassword: opts.pg.resolvedPassword("PGPASSWORD"), PGDB: opts.pg.database, PGSSLMode: opts.pg.sslMode,
		PGUseIAM: opts.pg.useIAM, Engine: engine,
		S3Region: opts.s3.region, S3Endpoint: opts.s3.endpoint, S3UsePath: opts.s3.usePath,
	}
	opts.dataPrefix = *dataPrefix
	return opts, nil
}

// openMergeEngine builds the CDC DuckDB exporter the rewrite merges through,
// reading the bucket through endpoint, the one checkMergeRoute returned: the
// S3 client's endpoint, never DuckDB's own default (R4-F1).
// Credentials come from the single shared rule cdc.ResolveStaticS3Credentials
// (#329): explicit config wins, otherwise the AWS_ACCESS_KEY_ID /
// AWS_SECRET_ACCESS_KEY pair, with AWS_SESSION_TOKEN riding whichever source
// supplied that pair.
//
// WARNING — this is a deliberate narrowing. The merge engine previously
// resolved the FULL AWS default credential chain, so chain-only sources
// (IMDS/instance and container roles, SSO, assumed roles, web identity, shared
// profiles) reached the DuckDB httpfs SET statements. They no longer do: an
// operator whose credentials live only in an SSO session, ~/.aws/config, or an
// instance role now gets an engine with no S3 credentials at all, and DuckDB
// falls back to whatever its own environment chain provides — the warn log
// below is the only early signal before an opaque httpfs 403. The compactor
// exposes no credential flags, so in practice the contract is
// env-pair-or-nothing.
//
// Lambda and container deployments that already export the environment triple
// are unaffected — the environment is exactly what the shared rule reads. The
// correct way to restore chain support is to add explicit credential flags
// feeding cdc.CDCConfig.S3AccessKeyID/S3SecretAccessKey/S3SessionToken, not to
// reintroduce a second private resolver: two credential rules signing the same
// bucket differently is the failure mode #329 retires.
func openMergeEngine(ctx context.Context, opts *compactorOptions, endpoint string, logger *zap.Logger) (*cdc.DuckExporter, error) {
	duckCfg := cdc.CDCConfig{
		DuckDBPath:              opts.duck.duckDBPath,
		DuckThreads:             opts.duck.duckThreads,
		DuckMemLimit:            opts.duck.duckMemLimit,
		QueryTimeout:            opts.duck.queryTimeout,
		ParquetCompression:      opts.duck.parquetCompression,
		ParquetCompressionLevel: opts.duck.parquetCompressionLevel,
		S3Bucket:                opts.s3.bucket,
		S3Endpoint:              endpoint,
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
		return nil, fmt.Errorf("open merge duckdb: %w", err)
	}
	return exporter, nil
}

func logCompactionResult(logger *zap.Logger, result compaction.CompactionResult) {
	fields := []zap.Field{
		zap.Int16("schema_id", result.SchemaID),
		zap.String("outcome", string(result.Outcome)),
		zap.Int64("epoch", result.Epoch),
		zap.Int64("input_generation", result.InputGeneration),
		zap.Float64("dirty_ratio", result.DirtyRatio),
	}
	switch result.Outcome {
	case compaction.PromotionApplied, compaction.RewriteApplied:
		logger.Info("compaction completed", append(fields,
			zap.Int64("generation", result.Generation),
			zap.Int("files_merged", result.FilesMerged),
			zap.Int64("rows_in", result.RowsIn),
			zap.Int64("rows_out", result.RowsOut),
			zap.String("new_base_key", result.NewBaseKey))...)
	case compaction.RewritePending:
		logger.Warn("compaction deferred: merge pending", append(fields,
			zap.Int64("base_mb", result.BaseMB),
			zap.Int64("delta_mb", result.DeltaMB))...)
	default:
		logger.Info("compaction completed", fields...)
	}
}

// openCompactorDBFn opens the publication database; tests replace it.
var openCompactorDBFn = func(ctx context.Context, cfg cdc.CDCConfig, logger *zap.Logger) (*sql.DB, error) {
	return cdc.OpenPublicationDB(ctx, cfg, logger)
}

// runCompactor gives the pass stdout for its metric lines (#594). The
// subcommand's logs go to stderr and a successful run prints nothing else on
// stdout, so METRICS_STDOUT names the same stream here as in cmd/server and
// cmd/lambda and there is no machine-readable output for a line to corrupt.
func runCompactor(ctx context.Context, args []string) error {
	return runCompactorOut(ctx, args, os.Stdout)
}

// runCompactorOut runs one compaction pass. metricsOut is where
// METRICS_STDOUT=true writes the pass's metrics, one JSON line each.
func runCompactorOut(ctx context.Context, args []string, metricsOut io.Writer) error {
	opts, err := parseCompactorFlags(args)
	if err != nil {
		return err
	}
	if opts == nil { // help requested
		return nil
	}

	// After the flags, so --help still prints usage, and before any I/O: a set
	// METRICS_STDOUT that is not a boolean fails the run (#642) rather than
	// letting the pass run with no metrics.
	emitter, err := bootstrap.MetricEmitterFromEnv(metricsOut)
	if err != nil {
		return fmt.Errorf("invalid environment: %w", err)
	}

	logger, err := buildToolLogger(false)
	if err != nil {
		return fmt.Errorf("create logger: %w", err)
	}
	defer func() { _ = logger.Sync() }()

	sdkClient, err := buildToolS3Client(ctx, opts.s3.region, opts.s3.endpoint, opts.s3.usePath)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	// The writer binds and checks the deployment's storage target with the
	// bucket URL this client sends requests to, so it follows no redirect
	// to another route (s3endpoint.WithoutRedirects, cdcv2's storage.go).
	s3Client, err := s3endpoint.WithoutRedirects(sdkClient)
	if err != nil {
		return fmt.Errorf("build the compactor's S3 client: %w", err)
	}
	// The merge engine reads the sources again, configured from the flags
	// and, when they name no endpoint, the client's own endpoint for the
	// region: refuse a run whose client took its route from anywhere else.
	mergeEndpoint, err := checkMergeRoute(s3Client, opts.s3)
	if err != nil {
		return fmt.Errorf("check the compactor's merge route: %w", err)
	}

	db, err := openCompactorDBFn(ctx, opts.db, logger)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	exporter, err := openMergeEngine(ctx, opts, mergeEndpoint, logger)
	if err != nil {
		return fmt.Errorf("build merge engine: %w", err)
	}
	defer func() { _ = exporter.DB.Close() }()

	store := &manifestv2.S3Store{Client: s3Client, Bucket: opts.s3.bucket}
	writer, err := compaction.NewWriter(compaction.WriterConfig{
		DB: db, Engine: opts.db.Engine, EntityMain: opts.entityMain,
		Layout: manifestv2.Layout{Prefix: opts.dataPrefix}, Store: store, Compaction: opts.compact,
	})
	if err != nil {
		return err
	}
	c := &compaction.Compactor{
		Logger: logger,
		Config: opts.compact,
		Writer: writer,
		Store:  store,
		Merger: &compaction.DuckMerger{DB: exporter.DB, Logger: logger},
		Bucket: opts.s3.bucket,
		// The same concrete *s3.Client, read back through its GetObject: it
		// hashes every merge source against its listed sha256 (#347).
		ObjectReader: s3Client,
		// Nil unless METRICS_STDOUT is on, and a nil sink emits nothing (#594).
		Metrics: telemetry.NewSink(emitter),
	}

	logger.Info("starting compaction",
		zap.Int16("schema_id", opts.compact.SchemaID),
		zap.String("bucket", opts.s3.bucket))

	result, err := c.RunOnce(ctx)
	if err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}
	logCompactionResult(logger, result)
	return nil
}
