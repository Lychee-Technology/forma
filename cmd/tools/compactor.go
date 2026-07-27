package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/compaction"
	"go.uber.org/zap"
)

// compactorOptions carries the parsed compactor flag values.
type compactorOptions struct {
	s3         s3Flags
	compact    cdc.CompactionConfig
	manifest   cdc.ManifestConfig
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
		bucketUsage:    "S3 bucket for parquet/manifest files (required)",
		bucketRequired: true,
	})

	manifestPrefix := fs.String("manifest-prefix", "", "Manifest prefix in S3")
	manifestTemplate := fs.String("manifest-template", "manifest/{{.SchemaID}}.json", "Manifest path template")

	targetBaseSizeMB := fs.Int("target-base-size-mb", 256, "Target base file size in MB")
	maxDeltaSizeMB := fs.Int("max-delta-size-mb", 50, "Maximum delta size in MB")
	dirtyRatioPct := fs.Int("dirty-ratio-pct", 5, "Dirty ratio percentage threshold")

	maxRetries := fs.Int("max-retries", 5, "Maximum retry attempts")
	baseBackoffMs := fs.Int("base-backoff-ms", 100, "Base backoff in milliseconds")
	maxBackoffMs := fs.Int("max-backoff-ms", 10000, "Maximum backoff in milliseconds")

	dataPrefix := fs.String("data-prefix", "data", "Data prefix for parquet files")

	// Merge engine (dirty-ratio rewrite, #188): DuckDB reads the schema's
	// parquet set via httpfs, so it needs the same S3 wiring the CDC
	// exporters use.
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

	opts.compact = cdc.CompactionConfig{
		SchemaID:         int16(*schemaID),
		TargetBaseSizeMB: *targetBaseSizeMB,
		MaxDeltaSizeMB:   *maxDeltaSizeMB,
		DirtyRatioPct:    *dirtyRatioPct,
		MaxRetries:       *maxRetries,
		BaseBackoff:      time.Duration(*baseBackoffMs) * time.Millisecond,
		MaxBackoff:       time.Duration(*maxBackoffMs) * time.Millisecond,
	}.WithDefaults()
	opts.manifest = cdc.ManifestConfig{
		Bucket:       opts.s3.bucket,
		Prefix:       *manifestPrefix,
		PathTemplate: *manifestTemplate,
	}
	opts.dataPrefix = *dataPrefix
	return opts, nil
}

// resolveMergeCredentials resolves the FULL default AWS credential chain
// (env, shared profiles, assumed roles, web identity, IMDS) so DuckDB signs
// with the same identity as the SDK S3 client — including the session token
// temporary credentials require. Falls back to raw env vars when the chain
// yields nothing (e.g. anonymous local object stores).
func resolveMergeCredentials(ctx context.Context, region string, logger *zap.Logger) (key, secret, token string) {
	awsCfg, err := toolLoadAWSConfigFn(ctx, awsconfig.WithRegion(region))
	if err == nil {
		if creds, retrieveErr := awsCfg.Credentials.Retrieve(ctx); retrieveErr == nil {
			return creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken
		} else {
			err = retrieveErr
		}
	}
	logger.Warn("could not resolve AWS credential chain for the merge engine; falling back to env vars", zap.Error(err))
	return os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), os.Getenv("AWS_SESSION_TOKEN")
}

// openMergeEngine builds the CDC DuckDB exporter the rewrite merges through.
func openMergeEngine(ctx context.Context, opts *compactorOptions, logger *zap.Logger) (*cdc.DuckExporter, error) {
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
	exporter, err := cdc.NewDuckExporter(ctx, duckCfg, key, secret, token, logger)
	if err != nil {
		return nil, fmt.Errorf("open merge duckdb: %w", err)
	}
	return exporter, nil
}

func logCompactionResult(logger *zap.Logger, result compaction.CompactionResult) {
	switch result.Outcome {
	case compaction.PromotionApplied:
		logger.Info("compaction completed",
			zap.Int16("schema_id", result.SchemaID),
			zap.Int64("version", result.Version),
			zap.Float64("dirty_ratio", result.DirtyRatio))
	case compaction.RewriteApplied:
		logger.Info("compaction rewrite completed",
			zap.Int16("schema_id", result.SchemaID),
			zap.Int64("version", result.Version),
			zap.Float64("dirty_ratio", result.DirtyRatio),
			zap.Int("files_merged", result.FilesMerged),
			zap.Int64("rows_in", result.RowsIn),
			zap.Int64("rows_out", result.RowsOut),
			zap.String("new_base_key", result.NewBaseKey))
	case compaction.RewritePending:
		logger.Warn("compaction deferred: rewrite pending",
			zap.Int16("schema_id", result.SchemaID),
			zap.Float64("dirty_ratio", result.DirtyRatio),
			zap.Int64("base_mb", result.BaseMB),
			zap.Int64("delta_mb", result.DeltaMB))
	default:
		logger.Info("compaction completed",
			zap.Int16("schema_id", result.SchemaID),
			zap.String("outcome", string(result.Outcome)))
	}
}

func runCompactor(ctx context.Context, args []string) error {
	opts, err := parseCompactorFlags(args)
	if err != nil {
		return err
	}
	if opts == nil { // help requested
		return nil
	}

	logger, err := buildToolLogger(false)
	if err != nil {
		return fmt.Errorf("create logger: %w", err)
	}
	defer func() { _ = logger.Sync() }()

	s3Client, err := buildToolS3Client(ctx, opts.s3.region, opts.s3.endpoint, opts.s3.usePath)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	exporter, err := openMergeEngine(ctx, opts, logger)
	if err != nil {
		return fmt.Errorf("build merge engine: %w", err)
	}
	defer func() { _ = exporter.DB.Close() }()

	c := &compaction.Compactor{
		Logger:     logger,
		Config:     opts.compact,
		Provider:   compaction.NewS3ManifestProvider(opts.manifest, s3Client),
		Merger:     &compaction.DuckMerger{DB: exporter.DB},
		S3:         s3Client,
		Bucket:     opts.s3.bucket,
		DataPrefix: opts.dataPrefix,
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
