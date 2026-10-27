package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/bootstrap"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/manifest"
	"go.uber.org/zap"
)

// baselineParquetFlags names the object set baseline-schema audits: the one
// the federated read scans, so the flags mirror the server's DuckDB manifest
// settings. --no-parquet-tier is explicit, so a forgotten bucket is an error
// rather than a baseline that skipped the lakehouse tier.
type baselineParquetFlags struct {
	none             bool
	s3               s3Flags
	manifestPrefix   string
	manifestTemplate string
	dataPrefix       string
	duck             duckExportFlags
}

func (f *baselineParquetFlags) register(fs *flag.FlagSet) {
	fs.BoolVar(&f.none, "no-parquet-tier", false, "The deployment has no lakehouse tier: audit PostgreSQL only")
	f.s3.register(fs, s3FlagOptions{bucketUsage: "S3 bucket of the parquet objects and manifests (required unless --no-parquet-tier)"})
	fs.StringVar(&f.manifestPrefix, "manifest-prefix", bootstrap.Env("MANIFEST_PREFIX", ""), "Manifest prefix in S3 (the server's DUCKDB_MANIFEST_PREFIX)")
	fs.StringVar(&f.manifestTemplate, "manifest-template", bootstrap.Env("MANIFEST_TEMPLATE", ""),
		"Manifest path template (the server's DUCKDB_MANIFEST_TEMPLATE; required unless --no-parquet-tier)")
	fs.StringVar(&f.dataPrefix, "data-prefix", bootstrap.Env("S3_PREFIX", ""),
		"Parquet prefix of the fallback glob a schema with no manifest entries is read through (the server's DUCKDB_S3_PREFIX)")
	f.duck.register(fs, duckExportFlagOptions{memLimitDefault: "4GB", queryTimeout: 30 * time.Minute})
}

func (f baselineParquetFlags) validate() error {
	if f.none {
		return nil
	}
	if f.s3.bucket == "" || f.manifestTemplate == "" {
		return errors.New("--s3-bucket and --manifest-template are required to audit the parquet objects; " +
			"pass --no-parquet-tier when the deployment has no lakehouse tier")
	}
	return validateManifestTemplateFlag(f.manifestTemplate)
}

// open builds the parquet tier: the manifest source the read resolves its
// object set from, and a DuckDB session with the S3 credentials the CDC
// tools use. The returned close releases the DuckDB pool.
func (f baselineParquetFlags) open(ctx context.Context, logger *zap.Logger) (*parquetTier, func(), error) {
	if f.none {
		return nil, func() {}, nil
	}
	s3Client, err := buildToolS3Client(ctx, f.s3.region, f.s3.endpoint, f.s3.usePath)
	if err != nil {
		return nil, nil, fmt.Errorf("build the s3 client: %w", err)
	}
	source := manifest.NewS3QuerySource(s3Client, manifest.S3QuerySourceConfig{
		Bucket: f.s3.bucket, ManifestPrefix: f.manifestPrefix, ManifestTemplate: f.manifestTemplate, DataPrefix: f.dataPrefix,
	})
	duckCfg := cdc.CDCConfig{
		DuckDBPath: f.duck.duckDBPath, DuckThreads: f.duck.duckThreads, DuckMemLimit: f.duck.duckMemLimit,
		QueryTimeout: f.duck.queryTimeout, S3Bucket: f.s3.bucket, S3Endpoint: f.s3.endpoint,
		S3Region: f.s3.region, S3UseSSL: f.s3.useSSL, S3UsePath: f.s3.usePath,
	}
	key, secret, token := cdc.ResolveStaticS3Credentials(duckCfg)
	exporter, err := cdc.NewDuckExporter(ctx, duckCfg, key, secret, token, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("open duckdb: %w", err)
	}
	tier := &parquetTier{
		objects: func(ctx context.Context, schemaID int16) ([]string, error) {
			paths, _, err := source.Paths(ctx, schemaID)
			if err != nil {
				return nil, fmt.Errorf("read the manifest of schema id %d: %w", schemaID, err)
			}
			return paths, nil
		},
		audit: func(ctx context.Context, _ int16, paths []string, cache forma.SchemaAttributeCache) ([]federated.DomainFinding, error) {
			return federated.AuditParquetDomain(ctx, exporter.DB, paths, cache)
		},
	}
	return tier, func() { _ = exporter.DB.Close() }, nil
}

// runBaselineSchema seeds the first generation of registered schemas that
// have none: the upgrade step of a deployment that predates the ledger.
func runBaselineSchema(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("baseline-schema", flag.ContinueOnError)
	flags.SetOutput(os.Stdout)
	flags.Usage = func() {
		fmt.Println("Usage: forma-tools baseline-schema --schema-dir <dir> (--no-parquet-tier | --s3-bucket <b> --manifest-template <t>) [options]")
		fmt.Println("")
		fmt.Println("Records generation 1 of each registered schema with no active generation, after auditing its")
		fmt.Println("stored PostgreSQL and parquet data against the declaration in --schema-dir. Any stored value the")
		fmt.Println("declaration does not admit, or any object that cannot be read, refuses the baseline.")
		fmt.Println("")
		fmt.Println("It is offline: stop every writer and exporter of the old release and pause the compactor first,")
		fmt.Println("and start only the enforcing release afterwards. A write or an object set change during the")
		fmt.Println("audit refuses the baseline. --dry-run can run beside the old release.")
		fmt.Println("")
		fmt.Println("Options:")
		flags.PrintDefaults()
	}
	var opts baselineOptions
	pg, portErr := registerLedgerFlags(flags, &opts.ledgerToolOptions)
	flags.StringVar(&opts.tables.EAV, "eav-table", bootstrap.Env("EAV_TABLE", "eav_dev"), "EAV data table name")
	flags.StringVar(&opts.tables.EntityMain, "entity-main-table", bootstrap.Env("ENTITY_MAIN_TABLE", "entity_main_dev"), "Entity main table name")
	flags.StringVar(&opts.tables.ChangeLog, "change-log-table", bootstrap.EnvAllowEmpty("CHANGE_LOG_TABLE", "change_log_dev"),
		"Change log table name; empty for a deployment without CDC")
	var parquet baselineParquetFlags
	parquet.register(flags)
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("parse flags: %w", err)
	}
	if portErr != nil {
		return fmt.Errorf("resolve --db-port default: %w", portErr)
	}
	if opts.schemaDir == "" {
		return errors.New("--schema-dir is required")
	}
	if err := parquet.validate(); err != nil {
		return err
	}
	return runBaseline(ctx, pg, parquet, opts)
}

func runBaseline(ctx context.Context, pg *postgresFlags, parquet baselineParquetFlags, opts baselineOptions) error {
	logger, err := buildToolLogger(false)
	if err != nil {
		return fmt.Errorf("build logger: %w", err)
	}
	defer func() { _ = logger.Sync() }()
	tier, closeTier, err := parquet.open(ctx, logger)
	if err != nil {
		return err
	}
	defer closeTier()
	opts.parquet = tier
	pool, err := buildToolPostgresPool(ctx, pg.databaseConfig("DB_PASSWORD", toolPostgresPoolSettings{
		maxConnections: 2,
		timeout:        30 * time.Second,
	}))
	if err != nil {
		return fmt.Errorf("create connection pool: %w", err)
	}
	defer pool.Close()
	return baselineSchemas(ctx, pool, opts, os.Stdout)
}
