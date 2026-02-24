package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifest"
	"go.uber.org/zap"
)

const (
	defaultInitBatchSize = 50000
)

func runCDCInit(args []string) error {
	fs := flag.NewFlagSet("cdc-init", flag.ExitOnError)

	// Database connection
	pgHost := fs.String("pg-host", "localhost", "PostgreSQL host")
	pgPort := fs.Int("pg-port", 5432, "PostgreSQL port")
	pgUser := fs.String("pg-user", "postgres", "PostgreSQL user")
	pgPassword := fs.String("pg-password", "", "PostgreSQL password (or set PGPASSWORD env)")
	pgDB := fs.String("pg-db", "forma", "PostgreSQL database")
	pgSSLMode := fs.String("pg-ssl-mode", getenvDefault("PG_SSL_MODE", "require"), "PostgreSQL sslmode")

	// Table names
	entityMainTable := fs.String("entity-main-table", "entity_main", "Entity main table name")
	eavDataTable := fs.String("eav-table", "eav_data", "EAV data table name")

	// DuckDB settings
	duckDBPath := fs.String("duckdb-path", "", "DuckDB path (empty for :memory:)")
	duckThreads := fs.Int("duck-threads", 4, "DuckDB thread count")
	duckMemLimit := fs.String("duck-mem-limit", "8GB", "DuckDB memory limit")
	queryTimeout := fs.Duration("query-timeout", 10*time.Minute, "Query timeout")

	// S3 settings
	s3Bucket := fs.String("s3-bucket", "", "S3 bucket for parquet files (required)")
	s3Prefix := fs.String("s3-prefix", "base", "S3 prefix for base files")
	s3Endpoint := fs.String("s3-endpoint", "", "S3 endpoint (for MinIO)")
	s3Region := fs.String("s3-region", "us-east-1", "S3 region")
	s3UseSSL := fs.Bool("s3-use-ssl", true, "Use SSL for S3")
	s3UsePath := fs.Bool("s3-use-path", false, "Use path-style S3 addressing")

	// Manifest settings (optional - enables manifest tracking)
	manifestPrefix := fs.String("manifest-prefix", "", "Manifest prefix in S3 (enables manifest tracking)")
	manifestTemplate := fs.String("manifest-template", "manifest/{{.SchemaID}}.json", "Manifest path template")

	// Compression
	parquetCompression := fs.String("parquet-compression", "zstd", "Parquet compression codec")
	parquetCompressionLevel := fs.Int("parquet-compression-level", 3, "Parquet compression level")

	// Schema registry (required for cdc-init)
	schemaRegistryTable := fs.String("schema-registry-table", "", "Schema registry table name (required)")
	schemaDir := fs.String("schema-dir", "", "Directory with *_attributes.json files (required)")

	// Init-specific options
	batchSize := fs.Int("batch-size", defaultInitBatchSize, "Rows per batch (overridden by target-file-size-mb)")
	targetFileSizeMB := fs.Int("target-file-size-mb", 256, "Target parquet file size in MB (0 = use batch-size)")
	estimatedRowBytes := fs.Int("estimated-row-bytes", 0, "Override row size estimate in bytes (0 = auto from schema)")
	maxBatchSize := fs.Int("max-batch-size", 10000000, "Maximum batch size to cap memory usage")
	schemaIDFilter := fs.Int("schema-id", 0, "Specific schema ID to init (0 = all schemas)")
	dryRun := fs.Bool("dry-run", false, "Dry run mode (no actual export)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	// Validate required flags
	if *s3Bucket == "" {
		return fmt.Errorf("--s3-bucket is required")
	}
	if *schemaRegistryTable == "" {
		return fmt.Errorf("--schema-registry-table is required")
	}
	if *schemaDir == "" {
		return fmt.Errorf("--schema-dir is required")
	}

	// Get password from env if not provided
	password := *pgPassword
	if password == "" {
		password = os.Getenv("PGPASSWORD")
	}

	// Track if user explicitly provided estimated-row-bytes (0 means auto-calculate from schema)
	autoEstimateRowBytes := *estimatedRowBytes == 0

	cfg := cdc.CDCConfig{
		EntityMainTable:         *entityMainTable,
		EAVDataTable:            *eavDataTable,
		BatchSize:               *batchSize,
		TargetFileSizeMB:        *targetFileSizeMB,
		EstimatedRowBytes:       *estimatedRowBytes,
		MaxBatchSize:            *maxBatchSize,
		PGHost:                  *pgHost,
		PGPort:                  *pgPort,
		PGUser:                  *pgUser,
		PGPassword:              password,
		PGDB:                    *pgDB,
		PGSSLMode:               *pgSSLMode,
		DuckDBPath:              *duckDBPath,
		DuckThreads:             *duckThreads,
		DuckMemLimit:            *duckMemLimit,
		QueryTimeout:            *queryTimeout,
		ParquetCompression:      *parquetCompression,
		ParquetCompressionLevel: *parquetCompressionLevel,
		S3Bucket:                *s3Bucket,
		S3Prefix:                *s3Prefix,
		S3Endpoint:              *s3Endpoint,
		S3Region:                *s3Region,
		S3UseSSL:                *s3UseSSL,
		S3UsePath:               *s3UsePath,
		ManifestPrefix:          *manifestPrefix,
		ManifestTemplate:        *manifestTemplate,
	}.WithDefaults()

	// Create logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		return fmt.Errorf("create logger: %w", err)
	}
	defer func() { _ = logger.Sync() }()

	ctx := context.Background()

	// Create schema registry
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.PGHost, cfg.PGPort, cfg.PGUser, cfg.PGPassword, cfg.PGDB, cfg.PGSSLMode)
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return fmt.Errorf("create schema registry pool: %w", err)
	}
	defer pool.Close()

	schemaRegistry, err := internal.NewFileSchemaRegistry(pool, *schemaRegistryTable, *schemaDir)
	if err != nil {
		return fmt.Errorf("create schema registry: %w", err)
	}

	// Create S3 client
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.S3Region))
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	var s3Client *s3.Client
	if cfg.S3Endpoint != "" {
		s3Client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.BaseEndpoint = &cfg.S3Endpoint
			o.UsePathStyle = cfg.S3UsePath
		})
	} else {
		s3Client = s3.NewFromConfig(awsCfg)
	}

	// Run init
	logger.Info("starting CDC init",
		zap.String("bucket", cfg.S3Bucket),
		zap.String("prefix", cfg.S3Prefix),
		zap.String("manifest_template", cfg.ManifestTemplate),
		zap.Int("target_file_size_mb", cfg.TargetFileSizeMB),
		zap.Int("estimated_row_bytes", cfg.EstimatedRowBytes),
		zap.Int("max_batch_size", cfg.MaxBatchSize),
		zap.Int("fallback_batch_size", cfg.BatchSize),
		zap.Int("schema_id_filter", *schemaIDFilter),
		zap.Bool("dry_run", *dryRun),
		zap.Bool("auto_estimate_row_bytes", autoEstimateRowBytes))

	if err := runInit(ctx, initRunOptions{
		cfg:                  cfg,
		s3Client:             s3Client,
		schemaRegistryTable:  *schemaRegistryTable,
		schemaIDFilter:       *schemaIDFilter,
		dryRun:               *dryRun,
		autoEstimateRowBytes: autoEstimateRowBytes,
		logger:               logger,
		schemaRegistry:       schemaRegistry,
	}); err != nil {
		return fmt.Errorf("CDC init failed: %w", err)
	}

	logger.Info("CDC init completed")
	return nil
}

type initRunContext struct {
	cfg                  cdc.CDCConfig
	db                   *sql.DB
	duck                 *cdc.DuckExporter
	s3Client             cdc.S3ObjectClient
	schemaRegistry       forma.SchemaRegistry
	manifestStore        manifest.Store
	manifestResolver     manifest.PathResolver
	logger               *zap.Logger
	dryRun               bool
	autoEstimateRowBytes bool
	pgConnStr            string
}

type initRunOptions struct {
	cfg                  cdc.CDCConfig
	s3Client             *s3.Client
	schemaRegistryTable  string
	schemaIDFilter       int
	dryRun               bool
	autoEstimateRowBytes bool
	logger               *zap.Logger
	schemaRegistry       forma.SchemaRegistry
}

type initRunSummary struct {
	totalRowsExported int64
	totalFilesCreated int
}

type schemaInitState struct {
	schemaID     int16
	attrCache    forma.SchemaAttributeCache
	batchSize    int
	fileEntries  []manifest.FileEntry
	rowsExported int64
	filesCreated int
}

type schemaBatchExport struct {
	rowIDs    []uuid.UUID
	minRowID  string
	maxRowID  string
	tmpKey    string
	finalKey  string
	s3TmpPath string
}

func newInitRunContext(
	ctx context.Context,
	cfg cdc.CDCConfig,
	s3Client *s3.Client,
	dryRun bool,
	autoEstimateRowBytes bool,
	logger *zap.Logger,
	schemaRegistry forma.SchemaRegistry,
) (*initRunContext, error) {
	sslMode := cfg.PGSSLMode
	if sslMode == "" {
		sslMode = "require"
	}
	pgConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.PGHost, cfg.PGPort, cfg.PGUser, cfg.PGPassword, cfg.PGDB, sslMode)

	db, err := sql.Open("pgx", pgConnStr)
	if err != nil {
		return nil, fmt.Errorf("open pg: %w", err)
	}

	duck, err := cdc.NewDuckExporter(ctx, cfg, os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), logger)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("new duck exporter: %w", err)
	}

	runCtx := &initRunContext{
		cfg:                  cfg,
		db:                   db,
		duck:                 duck,
		s3Client:             s3Client,
		schemaRegistry:       schemaRegistry,
		logger:               logger,
		dryRun:               dryRun,
		autoEstimateRowBytes: autoEstimateRowBytes,
		pgConnStr:            pgConnStr,
	}

	if cfg.ManifestTemplate != "" {
		runCtx.manifestStore = &manifest.S3Store{
			Client: s3Client,
			Bucket: cfg.S3Bucket,
		}
		runCtx.manifestResolver = manifest.PathResolver{
			Prefix:       cfg.ManifestPrefix,
			PathTemplate: cfg.ManifestTemplate,
		}
	}

	return runCtx, nil
}

func (c *initRunContext) close() {
	if c == nil {
		return
	}
	if c.duck != nil && c.duck.DB != nil {
		_ = c.duck.DB.Close()
	}
	if c.db != nil {
		_ = c.db.Close()
	}
}

// runInit performs the initialization export for all or a specific schema.
func runInit(ctx context.Context, opts initRunOptions) error {
	runCtx, err := newInitRunContext(
		ctx,
		opts.cfg,
		opts.s3Client,
		opts.dryRun,
		opts.autoEstimateRowBytes,
		opts.logger,
		opts.schemaRegistry,
	)
	if err != nil {
		return err
	}
	defer runCtx.close()

	schemaIDs, err := getSchemaIDsToInit(ctx, runCtx.db, opts.schemaRegistryTable, opts.schemaIDFilter)
	if err != nil {
		return err
	}

	if len(schemaIDs) == 0 {
		opts.logger.Info("no schemas to initialize")
		return nil
	}

	opts.logger.Info("schemas to initialize", zap.Int("count", len(schemaIDs)), zap.Any("schema_ids", schemaIDs))

	summary, schemaErr := processInitSchemas(ctx, runCtx, schemaIDs)
	opts.logger.Info("CDC init summary",
		zap.Int64("total_rows_exported", summary.totalRowsExported),
		zap.Int("total_files_created", summary.totalFilesCreated))
	return schemaErr
}

func processInitSchemas(ctx context.Context, runCtx *initRunContext, schemaIDs []int64) (initRunSummary, error) {
	summary := initRunSummary{}
	var schemaErrors []error

	for _, sid := range schemaIDs {
		schemaID := int16(sid)
		rowsExported, filesCreated, err := initSchema(ctx, runCtx, schemaID)
		if err != nil {
			runCtx.logger.Error("failed to init schema", zap.Int16("schema_id", schemaID), zap.Error(err))
			schemaErrors = append(schemaErrors, fmt.Errorf("schema %d: %w", schemaID, err))
			continue
		}
		summary.totalRowsExported += rowsExported
		summary.totalFilesCreated += filesCreated
	}

	if len(schemaErrors) > 0 {
		return summary, errors.Join(schemaErrors...)
	}
	return summary, nil
}

// getSchemaIDsToInit returns the list of schema IDs to initialize.
func getSchemaIDsToInit(ctx context.Context, db *sql.DB, schemaRegistryTable string, schemaIDFilter int) ([]int64, error) {
	var query string
	var args []any

	if schemaIDFilter > 0 {
		query = fmt.Sprintf("SELECT schema_id FROM %s WHERE schema_id = $1", sanitizeIdentifier(schemaRegistryTable))
		args = []any{schemaIDFilter}
	} else {
		query = fmt.Sprintf("SELECT schema_id FROM %s ORDER BY schema_id", sanitizeIdentifier(schemaRegistryTable))
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query schema ids: %w", err)
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
	return schemaIDs, nil
}

// initSchema exports all existing data for a single schema to S3 base files.
func initSchema(ctx context.Context, runCtx *initRunContext, schemaID int16) (int64, int, error) {
	state, err := prepareSchemaInit(ctx, runCtx, schemaID)
	if err != nil {
		return 0, 0, err
	}
	if state == nil {
		return 0, 0, nil
	}

	if err := processSchemaBatches(ctx, runCtx, state); err != nil {
		return state.rowsExported, state.filesCreated, err
	}

	updateSchemaManifest(ctx, runCtx, state)

	runCtx.logger.Info("schema init completed",
		zap.Int16("schema_id", schemaID),
		zap.Int64("total_rows_exported", state.rowsExported),
		zap.Int("total_files_created", state.filesCreated))

	return state.rowsExported, state.filesCreated, nil
}

func prepareSchemaInit(ctx context.Context, runCtx *initRunContext, schemaID int16) (*schemaInitState, error) {
	runCtx.logger.Info("initializing schema", zap.Int16("schema_id", schemaID))

	totalRows, err := getEntityMainCount(ctx, runCtx.db, runCtx.cfg.EntityMainTable, schemaID)
	if err != nil {
		return nil, fmt.Errorf("get row count: %w", err)
	}
	if totalRows == 0 {
		runCtx.logger.Info("no rows to export", zap.Int16("schema_id", schemaID))
		return nil, nil
	}
	runCtx.logger.Info("rows to export", zap.Int16("schema_id", schemaID), zap.Int64("total_rows", totalRows))

	state := &schemaInitState{
		schemaID: schemaID,
	}
	state.attrCache = resolveSchemaAttrCache(runCtx, schemaID)
	state.batchSize = resolveInitBatchSize(runCtx, schemaID, state.attrCache)
	return state, nil
}

func resolveSchemaAttrCache(runCtx *initRunContext, schemaID int16) forma.SchemaAttributeCache {
	if runCtx.schemaRegistry == nil {
		return nil
	}
	_, cache, err := runCtx.schemaRegistry.GetSchemaAttributeCacheByID(schemaID)
	if err != nil {
		runCtx.logger.Warn("schema registry lookup failed, using generic projection", zap.Int16("schema_id", schemaID), zap.Error(err))
		return nil
	}
	return cache
}

func resolveInitBatchSize(runCtx *initRunContext, schemaID int16, attrCache forma.SchemaAttributeCache) int {
	batchSize := runCtx.cfg.BatchSize
	if runCtx.cfg.TargetFileSizeMB <= 0 {
		return batchSize
	}

	rowBytes := runCtx.cfg.EstimatedRowBytes
	if runCtx.autoEstimateRowBytes {
		rowBytes = estimateRowSizeBytes(attrCache)
	}
	batchSize = calculateBatchSize(runCtx.cfg.TargetFileSizeMB, rowBytes, runCtx.cfg.MaxBatchSize)
	runCtx.logger.Info("calculated batch size for target file size",
		zap.Int16("schema_id", schemaID),
		zap.Int("target_file_size_mb", runCtx.cfg.TargetFileSizeMB),
		zap.Int("estimated_row_bytes", rowBytes),
		zap.Int("calculated_batch_size", batchSize))
	return batchSize
}

func processSchemaBatches(ctx context.Context, runCtx *initRunContext, state *schemaInitState) error {
	return iterateSchemaBatches(ctx, runCtx, state, func(rowIDs []uuid.UUID) error {
		return exportSchemaBatch(ctx, runCtx, state, rowIDs)
	})
}

func iterateSchemaBatches(
	ctx context.Context,
	runCtx *initRunContext,
	state *schemaInitState,
	onBatch func(rowIDs []uuid.UUID) error,
) error {
	offset := 0
	for {
		rowIDs, err := selectEntityMainBatch(ctx, runCtx.db, runCtx.cfg.EntityMainTable, state.schemaID, offset, state.batchSize)
		if err != nil {
			return fmt.Errorf("select batch: %w", err)
		}
		if len(rowIDs) == 0 {
			return nil
		}

		if err := onBatch(rowIDs); err != nil {
			return err
		}
		offset += state.batchSize
	}
}

func buildSchemaBatchExport(runCtx *initRunContext, state *schemaInitState, rowIDs []uuid.UUID) schemaBatchExport {
	minRowID := rowIDs[0].String()
	maxRowID := rowIDs[len(rowIDs)-1].String()

	tmpUUID := uuid.Must(uuid.NewV7()).String()
	tmpKey := cdc.BuildBaseTempPath(runCtx.cfg.S3Prefix, state.schemaID, tmpUUID)
	finalKey := cdc.BuildBasePath(runCtx.cfg.S3Prefix, state.schemaID, minRowID, maxRowID)
	s3TmpPath := fmt.Sprintf("s3://%s/%s", runCtx.cfg.S3Bucket, tmpKey)

	return schemaBatchExport{
		rowIDs:    rowIDs,
		minRowID:  minRowID,
		maxRowID:  maxRowID,
		tmpKey:    tmpKey,
		finalKey:  finalKey,
		s3TmpPath: s3TmpPath,
	}
}

func recordSchemaBatchResult(state *schemaInitState, batch schemaBatchExport, createdAt int64) {
	state.fileEntries = append(state.fileEntries, manifest.FileEntry{
		Tier:       "base",
		Path:       batch.finalKey,
		RowIDMin:   batch.minRowID,
		RowIDMax:   batch.maxRowID,
		RowCount:   int64(len(batch.rowIDs)),
		CreatedMin: createdAt,
		CreatedMax: createdAt,
	})
	state.rowsExported += int64(len(batch.rowIDs))
	state.filesCreated++
}

func exportSchemaBatch(ctx context.Context, runCtx *initRunContext, state *schemaInitState, rowIDs []uuid.UUID) error {
	batch := buildSchemaBatchExport(runCtx, state, rowIDs)

	runCtx.logger.Info("exporting batch",
		zap.Int16("schema_id", state.schemaID),
		zap.Int("batch_size", len(batch.rowIDs)),
		zap.String("min_row_id", batch.minRowID),
		zap.String("max_row_id", batch.maxRowID),
		zap.String("tmp_path", batch.s3TmpPath),
		zap.String("final_key", batch.finalKey))

	if runCtx.dryRun {
		runCtx.logger.Info("dry-run: skipping export", zap.Int16("schema_id", state.schemaID), zap.Int("batch_size", len(batch.rowIDs)))
		state.rowsExported += int64(len(batch.rowIDs))
		state.filesCreated++
		return nil
	}

	if err := runCtx.duck.ExportBaseFileToTmp(ctx, runCtx.pgConnStr, batch.s3TmpPath, state.schemaID, batch.rowIDs, state.attrCache); err != nil {
		return fmt.Errorf("export batch: %w", err)
	}
	if err := cdc.CopyTmpToFinal(ctx, runCtx.s3Client, runCtx.cfg.S3Bucket, batch.tmpKey, batch.finalKey, runCtx.logger); err != nil {
		return fmt.Errorf("copy tmp->final: %w", err)
	}

	recordSchemaBatchResult(state, batch, time.Now().UnixMilli())

	runCtx.logger.Info("batch completed",
		zap.Int16("schema_id", state.schemaID),
		zap.Int64("rows_exported", state.rowsExported),
		zap.Int("files_created", state.filesCreated),
		zap.String("final_key", batch.finalKey))
	return nil
}

func updateSchemaManifest(ctx context.Context, runCtx *initRunContext, state *schemaInitState) {
	if runCtx.manifestStore == nil || len(state.fileEntries) == 0 || runCtx.dryRun {
		return
	}

	manifestPath, err := runCtx.manifestResolver.Resolve(state.schemaID)
	if err != nil {
		runCtx.logger.Error("failed to resolve manifest path", zap.Int16("schema_id", state.schemaID), zap.Error(err))
		return
	}
	if err := manifest.AppendFiles(ctx, runCtx.manifestStore, manifestPath, state.schemaID, state.fileEntries); err != nil {
		runCtx.logger.Error("failed to update manifest", zap.Int16("schema_id", state.schemaID), zap.Error(err))
		// Don't fail - the export succeeded, manifest is non-critical.
		return
	}
	runCtx.logger.Info("manifest updated",
		zap.Int16("schema_id", state.schemaID),
		zap.String("manifest_path", manifestPath),
		zap.Int("files_added", len(state.fileEntries)))
}

// getEntityMainCount returns the total number of non-deleted rows for a schema.
func getEntityMainCount(ctx context.Context, db *sql.DB, tableName string, schemaID int16) (int64, error) {
	if tableName == "" {
		tableName = "entity_main"
	}
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE ltbase_schema_id = $1 AND ltbase_deleted_at IS NULL",
		sanitizeIdentifier(tableName))
	row := db.QueryRowContext(ctx, query, schemaID)
	var cnt int64
	if err := row.Scan(&cnt); err != nil {
		return 0, fmt.Errorf("count rows: %w", err)
	}
	return cnt, nil
}

// selectEntityMainBatch returns a batch of row IDs ordered by ltbase_row_id.
func selectEntityMainBatch(ctx context.Context, db *sql.DB, tableName string, schemaID int16, offset, limit int) ([]uuid.UUID, error) {
	if tableName == "" {
		tableName = "entity_main"
	}
	query := fmt.Sprintf(`
		SELECT ltbase_row_id 
		FROM %s 
		WHERE ltbase_schema_id = $1 AND ltbase_deleted_at IS NULL 
		ORDER BY ltbase_row_id 
		LIMIT $2 OFFSET $3`,
		sanitizeIdentifier(tableName))

	rows, err := db.QueryContext(ctx, query, schemaID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("select batch: %w", err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan row id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// sanitizeIdentifier performs a minimal whitelist for table names.
func sanitizeIdentifier(name string) string {
	return internal.SanitizeIdentifier(name)
}

// estimateRowSizeBytes estimates the average row size in bytes based on schema attributes.
// This is used to calculate optimal batch size for target file sizes.
func estimateRowSizeBytes(attrCache forma.SchemaAttributeCache) int {
	// Base overhead: row_id (UUID=16), schema_id (int16=2), timestamps (3 * int64=24)
	const baseOverhead = 50

	if len(attrCache) == 0 {
		// Fallback when no schema: assume medium-sized rows
		return 500
	}

	totalBytes := baseOverhead
	for _, meta := range attrCache {
		switch meta.ValueType {
		case forma.ValueTypeText:
			// Text fields vary widely; assume average 100 bytes
			totalBytes += 100
		case forma.ValueTypeNumeric, forma.ValueTypeBigInt, forma.ValueTypeInteger:
			// Numeric types: 8 bytes (double/bigint)
			totalBytes += 8
		case forma.ValueTypeSmallInt:
			totalBytes += 2
		case forma.ValueTypeBool:
			totalBytes += 1
		case forma.ValueTypeDate, forma.ValueTypeDateTime:
			// Dates stored as unix_ms (int64)
			totalBytes += 8
		case forma.ValueTypeUUID:
			totalBytes += 16
		case forma.ValueTypeList:
			// Lists are variable; assume average 200 bytes
			totalBytes += 200
		default:
			// Unknown type, assume text-like
			totalBytes += 100
		}
	}
	return totalBytes
}

// calculateBatchSize computes optimal batch size to achieve target file size.
// Returns the calculated batch size, capped at maxBatchSize to avoid memory issues.
func calculateBatchSize(targetFileSizeMB int, estimatedRowBytes int, maxBatchSize int) int {
	if estimatedRowBytes <= 0 {
		estimatedRowBytes = 500 // fallback
	}
	if targetFileSizeMB <= 0 {
		targetFileSizeMB = 256 // default 256MB
	}

	targetBytes := int64(targetFileSizeMB) * 1024 * 1024
	batchSize := min(
		// Cap at maxBatchSize to avoid memory issues
		int(targetBytes/int64(estimatedRowBytes)), maxBatchSize)
	// Minimum batch size
	if batchSize < 1000 {
		batchSize = 1000
	}
	return batchSize
}
