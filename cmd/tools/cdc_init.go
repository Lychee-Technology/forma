package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/lib/pq"
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

	if err := runInit(ctx, cfg, s3Client, *schemaRegistryTable, *schemaIDFilter, *dryRun, autoEstimateRowBytes, logger, schemaRegistry); err != nil {
		return fmt.Errorf("CDC init failed: %w", err)
	}

	logger.Info("CDC init completed")
	return nil
}

// runInit performs the initialization export for all or a specific schema.
func runInit(ctx context.Context, cfg cdc.CDCConfig, s3Client *s3.Client, schemaRegistryTable string, schemaIDFilter int, dryRun bool, autoEstimateRowBytes bool, logger *zap.Logger, schemaRegistry forma.SchemaRegistry) error {
	// Setup Postgres connection
	sslMode := cfg.PGSSLMode
	if sslMode == "" {
		sslMode = "require"
	}
	pgConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.PGHost, cfg.PGPort, cfg.PGUser, cfg.PGPassword, cfg.PGDB, sslMode)

	db, err := sql.Open("postgres", pgConnStr)
	if err != nil {
		return fmt.Errorf("open pg: %w", err)
	}
	defer db.Close()

	// Setup DuckDB exporter
	duck, err := cdc.NewDuckExporter(ctx, cfg, os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), logger)
	if err != nil {
		return fmt.Errorf("new duck exporter: %w", err)
	}
	defer duck.DB.Close()

	// Setup manifest store if configured
	var manifestStore manifest.Store
	var manifestResolver manifest.PathResolver
	if cfg.ManifestTemplate != "" {
		manifestStore = &manifest.S3Store{
			Client: s3Client,
			Bucket: cfg.S3Bucket,
		}
		manifestResolver = manifest.PathResolver{
			Prefix:       cfg.ManifestPrefix,
			PathTemplate: cfg.ManifestTemplate,
		}
	}

	// Get schemas to process
	schemaIDs, err := getSchemaIDsToInit(ctx, db, schemaRegistryTable, schemaIDFilter)
	if err != nil {
		return err
	}

	if len(schemaIDs) == 0 {
		logger.Info("no schemas to initialize")
		return nil
	}

	logger.Info("schemas to initialize", zap.Int("count", len(schemaIDs)), zap.Any("schema_ids", schemaIDs))

	// Process each schema
	totalRowsExported := int64(0)
	totalFilesCreated := 0

	for _, sid := range schemaIDs {
		schemaID := int16(sid)
		rowsExported, filesCreated, err := initSchema(ctx, db, duck, s3Client, cfg, schemaID, pgConnStr, dryRun, autoEstimateRowBytes, logger, schemaRegistry, manifestStore, manifestResolver)
		if err != nil {
			logger.Error("failed to init schema", zap.Int16("schema_id", schemaID), zap.Error(err))
			continue
		}
		totalRowsExported += rowsExported
		totalFilesCreated += filesCreated
	}

	logger.Info("CDC init summary",
		zap.Int64("total_rows_exported", totalRowsExported),
		zap.Int("total_files_created", totalFilesCreated))

	return nil
}

// getSchemaIDsToInit returns the list of schema IDs to initialize.
func getSchemaIDsToInit(ctx context.Context, db *sql.DB, schemaRegistryTable string, schemaIDFilter int) ([]int64, error) {
	var query string
	var args []interface{}

	if schemaIDFilter > 0 {
		query = fmt.Sprintf("SELECT schema_id FROM %s WHERE schema_id = $1", sanitizeIdentifier(schemaRegistryTable))
		args = []interface{}{schemaIDFilter}
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
func initSchema(
	ctx context.Context,
	db *sql.DB,
	duck *cdc.DuckExporter,
	s3Client cdc.S3ObjectClient,
	cfg cdc.CDCConfig,
	schemaID int16,
	pgConnStr string,
	dryRun bool,
	autoEstimateRowBytes bool,
	logger *zap.Logger,
	schemaRegistry forma.SchemaRegistry,
	manifestStore manifest.Store,
	manifestResolver manifest.PathResolver,
) (int64, int, error) {
	logger.Info("initializing schema", zap.Int16("schema_id", schemaID))

	// Get total row count
	totalRows, err := getEntityMainCount(ctx, db, cfg.EntityMainTable, schemaID)
	if err != nil {
		return 0, 0, fmt.Errorf("get row count: %w", err)
	}

	if totalRows == 0 {
		logger.Info("no rows to export", zap.Int16("schema_id", schemaID))
		return 0, 0, nil
	}

	logger.Info("rows to export", zap.Int16("schema_id", schemaID), zap.Int64("total_rows", totalRows))

	// Get schema attribute cache
	var attrCache forma.SchemaAttributeCache
	if schemaRegistry != nil {
		if _, cache, err := schemaRegistry.GetSchemaAttributeCacheByID(schemaID); err != nil {
			logger.Warn("schema registry lookup failed, using generic projection", zap.Int16("schema_id", schemaID), zap.Error(err))
		} else {
			attrCache = cache
		}
	}

	// Calculate optimal batch size for target file size
	batchSize := cfg.BatchSize
	if cfg.TargetFileSizeMB > 0 {
		// Use provided estimate or calculate from schema if autoEstimateRowBytes is true
		rowBytes := cfg.EstimatedRowBytes
		if autoEstimateRowBytes {
			rowBytes = estimateRowSizeBytes(attrCache)
		}
		batchSize = calculateBatchSize(cfg.TargetFileSizeMB, rowBytes, cfg.MaxBatchSize)
		logger.Info("calculated batch size for target file size",
			zap.Int16("schema_id", schemaID),
			zap.Int("target_file_size_mb", cfg.TargetFileSizeMB),
			zap.Int("estimated_row_bytes", rowBytes),
			zap.Int("calculated_batch_size", batchSize))
	}

	// Collect file entries for manifest update
	var fileEntries []manifest.FileEntry

	// Process in batches
	var rowsExported int64
	var filesCreated int
	offset := 0

	for {
		// Select batch of row IDs
		rowIDs, err := selectEntityMainBatch(ctx, db, cfg.EntityMainTable, schemaID, offset, batchSize)
		if err != nil {
			return rowsExported, filesCreated, fmt.Errorf("select batch: %w", err)
		}

		if len(rowIDs) == 0 {
			break
		}

		// Determine min/max row_id for file naming
		minRowID := rowIDs[0].String()
		maxRowID := rowIDs[len(rowIDs)-1].String()

		// Build paths
		tmpUUID := uuid.Must(uuid.NewV7()).String()
		tmpKey := cdc.BuildBaseTempPath(cfg.S3Prefix, schemaID, tmpUUID)
		finalKey := cdc.BuildBasePath(cfg.S3Prefix, schemaID, minRowID, maxRowID)
		s3TmpPath := fmt.Sprintf("s3://%s/%s", cfg.S3Bucket, tmpKey)

		logger.Info("exporting batch",
			zap.Int16("schema_id", schemaID),
			zap.Int("batch_size", len(rowIDs)),
			zap.String("min_row_id", minRowID),
			zap.String("max_row_id", maxRowID),
			zap.String("tmp_path", s3TmpPath),
			zap.String("final_key", finalKey))

		if dryRun {
			logger.Info("dry-run: skipping export", zap.Int16("schema_id", schemaID), zap.Int("batch_size", len(rowIDs)))
			rowsExported += int64(len(rowIDs))
			filesCreated++
			offset += batchSize
			continue
		}

		// Export to tmp
		if err := duck.ExportBaseFileToTmp(ctx, pgConnStr, s3TmpPath, schemaID, rowIDs, attrCache); err != nil {
			return rowsExported, filesCreated, fmt.Errorf("export batch: %w", err)
		}

		// Copy tmp -> final
		if err := cdc.CopyTmpToFinal(ctx, s3Client, cfg.S3Bucket, tmpKey, finalKey, logger); err != nil {
			return rowsExported, filesCreated, fmt.Errorf("copy tmp->final: %w", err)
		}

		// Collect file entry for manifest
		createdAt := time.Now().UnixMilli()
		fileEntries = append(fileEntries, manifest.FileEntry{
			Tier:       "base",
			Path:       finalKey,
			RowIDMin:   minRowID,
			RowIDMax:   maxRowID,
			RowCount:   int64(len(rowIDs)),
			CreatedMin: createdAt,
			CreatedMax: createdAt,
		})

		rowsExported += int64(len(rowIDs))
		filesCreated++
		offset += batchSize

		logger.Info("batch completed",
			zap.Int16("schema_id", schemaID),
			zap.Int64("rows_exported", rowsExported),
			zap.Int("files_created", filesCreated),
			zap.String("final_key", finalKey))
	}

	// Update manifest with all file entries if configured and not dry-run
	if manifestStore != nil && len(fileEntries) > 0 && !dryRun {
		manifestPath, err := manifestResolver.Resolve(schemaID)
		if err != nil {
			logger.Error("failed to resolve manifest path", zap.Int16("schema_id", schemaID), zap.Error(err))
		} else {
			if err := manifest.AppendFiles(ctx, manifestStore, manifestPath, schemaID, fileEntries); err != nil {
				logger.Error("failed to update manifest", zap.Int16("schema_id", schemaID), zap.Error(err))
				// Don't fail - the export succeeded, manifest is non-critical
			} else {
				logger.Info("manifest updated",
					zap.Int16("schema_id", schemaID),
					zap.String("manifest_path", manifestPath),
					zap.Int("files_added", len(fileEntries)))
			}
		}
	}

	logger.Info("schema init completed",
		zap.Int16("schema_id", schemaID),
		zap.Int64("total_rows_exported", rowsExported),
		zap.Int("total_files_created", filesCreated))

	return rowsExported, filesCreated, nil
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
	if name == "" {
		return ""
	}
	return fmt.Sprintf(`"%s"`, name)
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
	batchSize := int(targetBytes / int64(estimatedRowBytes))

	// Cap at maxBatchSize to avoid memory issues
	if batchSize > maxBatchSize {
		batchSize = maxBatchSize
	}
	// Minimum batch size
	if batchSize < 1000 {
		batchSize = 1000
	}
	return batchSize
}
