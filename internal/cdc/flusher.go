package cdc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awsCreds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dsql/auth"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/duckdb/duckdb-go/v2"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/cdcv2"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"go.uber.org/zap"
)

// loadAWSConfig is the seam for loading the ambient AWS configuration,
// mirroring internal/bootstrap.loadAWSConfig. Every cdc S3 client site —
// setupAWSClient and the Runner's getOrCreateS3Runtime — goes through this
// one seam so credential and region parity holds (#302, #326). Tests swap
// it; production always resolves the real chain.
var loadAWSConfig = config.LoadDefaultConfig

// generateIAMTokenFn is the function signature we use to generate IAM tokens.
// We wrap the upstream function to keep a stable signature for tests.
var generateIAMTokenFn = func(ctx context.Context, endpoint, region string, creds any) (string, error) {
	var cp aws.CredentialsProvider
	if c, ok := creds.(aws.CredentialsProvider); ok {
		cp = c
	}
	return auth.GenerateDbConnectAuthToken(ctx, endpoint, region, cp)
}

// S3FullClient is the object API the e2e harness wraps to inject faults
// and count calls: S3Client with DeleteObject and HeadObject, which the
// harness uses for cleanup and existence checks.
type S3FullClient interface {
	S3Client
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

// RunOnce performs one flush pass over every schema with unflushed entries,
// publishing each schema's batch through the V2 writer (internal/cdcv2): it
// stages delta objects and an immutable manifest generation, then moves the
// schema's publication record and marks the batch's entries in one
// transaction. s3Client is the client the objects and manifests are written
// through; nil uses one built from the AWS configuration (cfg.S3Region and
// the ambient chain). schemaRegistry is required: the export is
// schema-driven. The report lists what each schema published and the
// schemas the run skipped (#704), and is returned alongside a schema's flush
// error too.
func RunOnce(ctx context.Context, cfg CDCConfig, s3Client S3Client, dryRun bool, logger *zap.Logger, schemaRegistry forma.SchemaRegistry) (FlushReport, error) {
	if schemaRegistry == nil {
		return FlushReport{}, fmt.Errorf("schema registry is required for CDC export")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	cfg = cfg.WithDefaults()
	// A configuration the writer cannot run is refused before any AWS,
	// Postgres or DuckDB setup.
	if err := cfg.ValidateFlush(); err != nil {
		return FlushReport{}, fmt.Errorf("cdc flush configuration: %w", err)
	}
	if _, err := runEpochs(cfg); err != nil {
		return FlushReport{}, fmt.Errorf("cdc flush configuration: %w", err)
	}
	region, credProvider, defaultS3Client, err := setupAWSClient(ctx, cfg)
	if err != nil {
		return FlushReport{}, fmt.Errorf("setup aws client: %w", err)
	}
	store, err := newObjectStore(resolveS3Client(s3Client, defaultS3Client), cfg.S3Bucket)
	if err != nil {
		return FlushReport{}, fmt.Errorf("resolve the object store: %w", err)
	}

	db, _, err := setupPostgresConnection(ctx, cfg, region, credProvider, logger)
	if err != nil {
		return FlushReport{}, fmt.Errorf("setup postgres connection: %w", err)
	}
	defer db.Close()

	// DuckDB httpfs credentials follow the same both-halves rule as the SDK
	// client (#326), and the session token rides the source that supplied the
	// pair (#329). The exports write local scratch files, but the exporter
	// configures S3 for the tools that share it.
	s3Key, s3Secret, s3Token := ResolveStaticS3Credentials(cfg)
	duck, err := NewDuckExporter(ctx, cfg, s3Key, s3Secret, s3Token, logger)
	if err != nil {
		return FlushReport{}, fmt.Errorf("new duck exporter: %w", err)
	}
	defer duck.DB.Close()

	return runFlushPass(ctx, flushContextParams{
		cfg: cfg, db: db, duck: duck, store: store, dryRun: dryRun, logger: logger, schemaRegistry: schemaRegistry,
	})
}

// runFlushPass builds the run's flush context and flushes every schema with
// unflushed entries. Both RunOnce frames end here.
func runFlushPass(ctx context.Context, p flushContextParams) (FlushReport, error) {
	flushCtx, err := newSchemaFlushContext(ctx, p)
	if err != nil {
		return FlushReport{}, fmt.Errorf("prepare flush: %w", err)
	}
	schemaIDs, err := getUnflushedSchemaIDs(ctx, p.db, flushCtx.tableName)
	if err != nil {
		return FlushReport{}, fmt.Errorf("list schemas with unflushed rows: %w", err)
	}
	return flushCtx.processSchemas(ctx, schemaIDs)
}

// setupAWSClient initializes the AWS credentials and S3 client. Credential
// precedence and region handling deliberately mirror
// internal/bootstrap.NewS3Client — the two sites must stay in parity (#302).
func setupAWSClient(ctx context.Context, cfg CDCConfig) (string, aws.CredentialsProvider, *s3.Client, error) {
	var loadOpts []func(*config.LoadOptions) error
	if cfg.S3Region != "" {
		// WithRegion at load time so region-sensitive default-chain
		// resolution (STS, SSO) sees the configured region.
		loadOpts = append(loadOpts, config.WithRegion(cfg.S3Region))
	}
	awsCfg, err := loadAWSConfig(ctx, loadOpts...)
	if err != nil {
		return "", nil, nil, fmt.Errorf("load aws config: %w", err)
	}
	// Both-halves rule and config-wins precedence live in
	// ResolveStaticS3Credentials — the shared rule for every cdc credential
	// site (#326). Empty pair leaves the default chain in place. The session
	// token rides the source that supplied the pair (#329): dropping it here
	// signed temporary STS credentials as if they were long-lived keys, which
	// the storage endpoint can only report as an opaque signing failure.
	if staticKey, staticSecret, staticToken := ResolveStaticS3Credentials(cfg); staticKey != "" {
		awsCfg.Credentials = awsCreds.NewStaticCredentialsProvider(staticKey, staticSecret, staticToken)
	}

	// Build S3 client options
	var fullClient *s3.Client
	if cfg.S3Endpoint != "" {
		fullClient = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.BaseEndpoint = &cfg.S3Endpoint
			o.UsePathStyle = cfg.S3UsePath
		})
	} else {
		fullClient = s3.NewFromConfig(awsCfg)
	}

	return awsCfg.Region, awsCfg.Credentials, fullClient, nil
}

// setupPostgresConnection creates a Postgres connection, potentially using IAM
// auth, and answers the password it chose (the IAM token when one was
// generated). The flush's PostgreSQL reads, the export's included (#785), all
// go through this pool.
func setupPostgresConnection(ctx context.Context, cfg CDCConfig, region string, credProvider aws.CredentialsProvider, logger *zap.Logger) (*sql.DB, string, error) {
	pgPassword := cfg.PGPassword

	// Try IAM auth token generation when enabled
	if cfg.PGUseIAM {
		endpoint := fmt.Sprintf("%s:%d", cfg.PGHost, cfg.PGPort)
		if token, err := generateIAMTokenFn(ctx, endpoint, region, credProvider); err == nil && token != "" {
			pgPassword = token
			logger.Sugar().Infow("generated IAM auth token for Postgres connection (dsql)")
		} else {
			logger.Sugar().Warnw("failed to generate IAM auth token; falling back to PG_PASSWORD if set", "err", err)
		}
	}

	sslMode := cfg.PGSSLMode
	if sslMode == "" {
		sslMode = DefaultPGSSLMode
	}
	pgConnStr := BuildPGDSN(PGDSNParams{Host: cfg.PGHost, Port: cfg.PGPort, User: cfg.PGUser, Password: pgPassword, DB: cfg.PGDB, SSLMode: sslMode})

	db, err := sql.Open("pgx", pgConnStr)
	if err != nil {
		return nil, "", fmt.Errorf("open pg: %w", err)
	}

	return db, pgPassword, nil
}

// getUnflushedSchemaIDs queries for schema IDs with unflushed rows.
func getUnflushedSchemaIDs(ctx context.Context, db *sql.DB, tableName string) ([]int64, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT DISTINCT schema_id FROM %s WHERE flushed_at = 0", sanitizeIdentifier(tableName)))
	if err != nil {
		return nil, fmt.Errorf("query distinct schema ids: %w", err)
	}
	defer rows.Close()

	var schemaIDs []int64
	for rows.Next() {
		var sid int64
		if err := rows.Scan(&sid); err != nil {
			return nil, fmt.Errorf("scan schema id: %w", err)
		}
		schemaIDs = append(schemaIDs, sid)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate schema ids: %w", err)
	}
	return schemaIDs, nil
}

type schemaFlushContext struct {
	db             *sql.DB
	duck           *DuckExporter
	store          manifestv2.Store
	writer         *cdcv2.Writer
	cfg            CDCConfig
	tableName      string
	dryRun         bool
	logger         *zap.Logger
	schemaRegistry forma.SchemaRegistry
	attrCaches     map[int16]forma.SchemaAttributeCache
	// epochs are the record epochs the run's metadata belongs to, read when
	// the context is built, before processSchemas resolves the attributes
	// (runEpochs, refuseMovedEpoch).
	epochs map[int16]int64
	// locker is the per-schema lock of the run's database engine; nil takes
	// the PostgreSQL advisory lock.
	locker SchemaLocker
	// flushed collects what each schema published, for the run's report.
	flushed         []SchemaFlush
	processSchemaFn func(context.Context, int16) error
}

func (c *schemaFlushContext) processSchemas(ctx context.Context, schemaIDs []int64) (FlushReport, error) {
	if len(schemaIDs) == 0 {
		return FlushReport{}, nil
	}

	caches, err := resolveRunAttrCaches(ctx, c.schemaRegistry, schemaIDs)
	if err != nil {
		return FlushReport{}, fmt.Errorf("cdc flush pre-flight: %w", err)
	}
	c.attrCaches = caches
	schemaIDs, skipped := exportableSchemaIDs(c.logger, schemaIDs, caches)
	report := FlushReport{Skipped: skipped}

	processSchema := c.processSchemaFn
	if processSchema == nil {
		processSchema = c.processSchema
	}

	var schemaErrs []error
	for _, sid := range schemaIDs {
		schemaID := int16(sid)
		if err := processSchema(ctx, schemaID); err != nil {
			c.logger.Sugar().Errorw("process schema failed", "schema_id", schemaID, "err", err)
			schemaErrs = append(schemaErrs, fmt.Errorf("schema %d: %w", schemaID, err))
		}
	}
	report.Flushed = c.flushed
	if len(schemaErrs) > 0 {
		return report, errors.Join(schemaErrs...)
	}

	return report, nil
}

// processSchema handles the flush process for a single schema.
func (c *schemaFlushContext) processSchema(ctx context.Context, schemaID int16) error {
	c.logger.Sugar().Infow("processing schema", "schema_id", schemaID)

	locker := c.locker
	if locker == nil {
		locker = advisorySchemaLocker{db: c.db}
	}
	held, release, locked, err := locker.TryLock(ctx, schemaID)
	if err != nil {
		return fmt.Errorf("acquire schema lock: %w", err)
	}
	if !locked {
		c.logger.Sugar().Infow("lock not acquired, skipping", "schema_id", schemaID)
		return nil
	}
	defer release()
	return WithLockLoss(held, c.flushUnderLock(held, schemaID))
}

// shouldFlush determines if flush thresholds are met. The age trigger is the
// one place a wall-clock comparison against changed_at survives (#274): a
// clock-ahead version has negative age until the clock reaches it, so a LONE
// such row age-triggers only after drift + MaxAgeMs. That delays only the
// TRIGGER: any run started by the count threshold or by other rows exports
// and marks clock-ahead rows normally (deltaStager, the writer's exact marks).
func shouldFlush(cfg CDCConfig, cnt int64, oldest int64) bool {
	nowMs := time.Now().UnixMilli()
	if cfg.MinRecords > 0 && cnt >= int64(cfg.MinRecords) {
		return true
	}
	if oldest > 0 && nowMs-oldest >= cfg.MaxAgeMs {
		return true
	}
	return false
}

// OpenPublicationDB opens the PostgreSQL or Aurora DSQL pool a V2 writer
// outside cdc-flush and cdc-init (the compactor) publishes through, exactly
// as cdc-flush opens its own: cfg's PG* fields, and an IAM token from the
// AWS credentials cfg resolves when PGUseIAM is set.
func OpenPublicationDB(ctx context.Context, cfg CDCConfig, logger *zap.Logger) (*sql.DB, error) {
	region, credProvider := cfg.S3Region, aws.CredentialsProvider(nil)
	if cfg.PGUseIAM {
		r, creds, _, err := setupAWSClient(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("resolve the AWS credentials of the IAM database token: %w", err)
		}
		region, credProvider = r, creds
	}
	db, _, err := setupPostgresConnection(ctx, cfg, region, credProvider, logger)
	if err != nil {
		return nil, fmt.Errorf("open the publication database: %w", err)
	}
	return db, nil
}
