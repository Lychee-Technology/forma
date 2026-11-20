package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/bootstrap"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/federatedv2"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"go.uber.org/zap"
)

// baselineParquetFlags names the object set baseline-schema audits: the one
// the federated read scans, each schema's published generation
// (docs/publication-protocol-v2.md), so the flags name the server's bucket.
// --no-parquet-tier is explicit, so a forgotten bucket is an error rather than
// a baseline that skipped the lakehouse tier.
type baselineParquetFlags struct {
	none bool
	s3   s3Flags
	duck duckExportFlags
}

func (f *baselineParquetFlags) register(fs *flag.FlagSet) {
	fs.BoolVar(&f.none, "no-parquet-tier", false, "The deployment has no lakehouse tier: audit PostgreSQL only")
	f.s3.register(fs, s3FlagOptions{bucketUsage: "S3 bucket of the published parquet objects and manifests, the server's DUCKDB_S3_BUCKET (required unless --no-parquet-tier)"})
	f.duck.register(fs, duckExportFlagOptions{memLimitDefault: "4GB", queryTimeout: 30 * time.Minute})
}

func (f baselineParquetFlags) validate() error {
	if f.none {
		return nil
	}
	if f.s3.bucket == "" {
		return errors.New("--s3-bucket is required to audit the parquet objects; " +
			"pass --no-parquet-tier when the deployment has no lakehouse tier")
	}
	return nil
}

// open builds the parquet tier: each schema's published objects, read from
// its publication record on pool and its generation's manifest in the
// bucket, and a DuckDB session with the S3 credentials the CDC tools use. The
// returned close releases the DuckDB pool.
func (f baselineParquetFlags) open(ctx context.Context, logger *zap.Logger, pool *pgxpool.Pool, entityMain string) (*parquetTier, func(), error) {
	if f.none {
		return nil, func() {}, nil
	}
	s3Client, err := buildToolS3Client(ctx, f.s3.region, f.s3.endpoint, f.s3.usePath)
	if err != nil {
		return nil, nil, fmt.Errorf("build the s3 client: %w", err)
	}
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
		objects: publishedObjects(pool, publication.TablesFor(entityMain), &manifestv2.S3Store{Client: s3Client, Bucket: f.s3.bucket}, f.s3.bucket),
		audit: func(ctx context.Context, _ int16, paths []string, cache forma.SchemaAttributeCache) ([]federated.DomainFinding, error) {
			return federated.AuditParquetDomain(ctx, exporter.DB, paths, cache)
		},
	}
	return tier, func() { _ = exporter.DB.Close() }, nil
}

// publishedObjects answers, for a schema, the objects a federated read of it
// scans: its PUBLISHED record's generation's, as s3:// URIs the way the reader
// addresses them. A schema without a record, or whose record is UNPUBLISHED,
// AWAITING_INIT or DROPPED, has none: the reader scans no parquet for it, and
// the V2 cdc-init that publishes an AWAITING_INIT schema exports PostgreSQL's
// rows, which the baseline audits. A record or manifest that cannot be read,
// or that is invalid, refuses the baseline.
func publishedObjects(pool *pgxpool.Pool, tables publication.Tables, store manifestv2.Store, bucket string) func(context.Context, int16) ([]string, error) {
	uri := federatedv2.S3URI(bucket)
	return func(ctx context.Context, schemaID int16) ([]string, error) {
		rec, err := readPublicationRecord(ctx, pool, tables, schemaID)
		if errors.Is(err, publication.ErrNoRecord) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if rec.State != publication.Published {
			return nil, nil
		}
		ref, err := manifestv2.RecordRef(rec)
		if err != nil {
			return nil, fmt.Errorf("reference the manifest of schema id %d generation %d: %w", schemaID, rec.Generation, err)
		}
		m, err := manifestv2.Fetch(ctx, store, ref)
		if err != nil {
			return nil, fmt.Errorf("read the manifest of schema id %d generation %d: %w", schemaID, rec.Generation, err)
		}
		paths := make([]string, 0, len(m.Files))
		for _, file := range m.Files {
			paths = append(paths, uri(file.Key))
		}
		return paths, nil
	}
}

// readPublicationRecord reads schemaID's publication record in a read-only
// snapshot of its own.
func readPublicationRecord(ctx context.Context, pool *pgxpool.Pool, tables publication.Tables, schemaID int16) (publication.Record, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return publication.Record{}, fmt.Errorf("open a snapshot to read the publication record of schema id %d: %w", schemaID, err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	snap, err := publication.Read(ctx, publication.PgxTx(tx), tables, schemaID)
	if err != nil {
		return publication.Record{}, fmt.Errorf("read the publication record of schema id %d: %w", schemaID, err)
	}
	return snap.Record, nil
}

// runBaselineSchema seeds the first generation of registered schemas that
// have none: the upgrade step of a deployment that predates the ledger.
func runBaselineSchema(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("baseline-schema", flag.ContinueOnError)
	flags.SetOutput(os.Stdout)
	flags.Usage = func() {
		fmt.Println("Usage: forma-tools baseline-schema --schema-dir <dir> (--no-parquet-tier | --s3-bucket <b>) [options]")
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
	pool, err := buildToolPostgresPool(ctx, pg.databaseConfig("DB_PASSWORD", toolPostgresPoolSettings{
		maxConnections: 2,
		timeout:        30 * time.Second,
	}))
	if err != nil {
		return fmt.Errorf("create connection pool: %w", err)
	}
	defer pool.Close()
	tier, closeTier, err := parquet.open(ctx, logger, pool, opts.tables.EntityMain)
	if err != nil {
		return err
	}
	defer closeTier()
	opts.parquet = tier
	return baselineSchemas(ctx, pool, opts, os.Stdout)
}
