package cdc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/manifest"
	"go.uber.org/zap"
)

// InitOptions configures a base-file initialization export run.
// It mirrors the cdc-init CLI options; the CLI in cmd/tools delegates here.
type InitOptions struct {
	Config               CDCConfig
	S3Client             *s3.Client
	SchemaRegistryTable  string
	SchemaIDFilter       int
	DryRun               bool
	AutoEstimateRowBytes bool
	Logger               *zap.Logger
	SchemaRegistry       forma.SchemaRegistry
}

// InitSummary reports totals across all schemas processed by RunInit.
type InitSummary struct {
	TotalRowsExported int64
	TotalFilesCreated int
}

type initRunContext struct {
	cfg                  CDCConfig
	db                   *sql.DB
	duck                 *DuckExporter
	s3Client             S3ObjectClient
	schemaRegistry       forma.SchemaRegistry
	manifestStore        manifest.Store
	manifestResolver     manifest.PathResolver
	logger               *zap.Logger
	dryRun               bool
	autoEstimateRowBytes bool
	pgConnStr            string
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

func newInitRunContext(ctx context.Context, opts InitOptions) (*initRunContext, error) {
	cfg := opts.Config
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

	s3Key := cfg.S3AccessKeyID
	s3Secret := cfg.S3SecretAccessKey
	if s3Key == "" {
		s3Key = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	if s3Secret == "" {
		s3Secret = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
	duck, err := NewDuckExporter(ctx, cfg, s3Key, s3Secret, opts.Logger)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("new duck exporter: %w", err)
	}

	runCtx := &initRunContext{
		cfg:                  cfg,
		db:                   db,
		duck:                 duck,
		s3Client:             opts.S3Client,
		schemaRegistry:       opts.SchemaRegistry,
		logger:               opts.Logger,
		dryRun:               opts.DryRun,
		autoEstimateRowBytes: opts.AutoEstimateRowBytes,
		pgConnStr:            pgConnStr,
	}

	if cfg.ManifestTemplate != "" {
		runCtx.manifestStore = &manifest.S3Store{
			Client: opts.S3Client,
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

// RunInit performs the initialization export for all or a specific schema:
// existing non-deleted entity_main rows are batched into base parquet files on
// S3 and (when a manifest template is configured) recorded in the schema
// manifest. Extracted from cmd/tools/cdc_init.go (mechanical move, #173).
func RunInit(ctx context.Context, opts InitOptions) (InitSummary, error) {
	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
	}

	runCtx, err := newInitRunContext(ctx, opts)
	if err != nil {
		return InitSummary{}, err
	}
	defer runCtx.close()

	schemaIDs, err := getSchemaIDsToInit(ctx, runCtx.db, opts.SchemaRegistryTable, opts.SchemaIDFilter)
	if err != nil {
		return InitSummary{}, err
	}

	if len(schemaIDs) == 0 {
		opts.Logger.Info("no schemas to initialize")
		return InitSummary{}, nil
	}

	opts.Logger.Info("schemas to initialize", zap.Int("count", len(schemaIDs)), zap.Any("schema_ids", schemaIDs))

	summary, schemaErr := processInitSchemas(ctx, runCtx, schemaIDs)
	opts.Logger.Info("CDC init summary",
		zap.Int64("total_rows_exported", summary.TotalRowsExported),
		zap.Int("total_files_created", summary.TotalFilesCreated))
	return summary, schemaErr
}

func processInitSchemas(ctx context.Context, runCtx *initRunContext, schemaIDs []int64) (InitSummary, error) {
	summary := InitSummary{}
	var schemaErrors []error

	for _, sid := range schemaIDs {
		schemaID := int16(sid)
		rowsExported, filesCreated, err := initSchema(ctx, runCtx, schemaID)
		if err != nil {
			runCtx.logger.Error("failed to init schema", zap.Int16("schema_id", schemaID), zap.Error(err))
			schemaErrors = append(schemaErrors, fmt.Errorf("schema %d: %w", schemaID, err))
			continue
		}
		summary.TotalRowsExported += rowsExported
		summary.TotalFilesCreated += filesCreated
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
	tmpKey := BuildBaseTempPath(runCtx.cfg.S3Prefix, state.schemaID, tmpUUID)
	finalKey := BuildBasePath(runCtx.cfg.S3Prefix, state.schemaID, minRowID, maxRowID)
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

	if err := runCtx.duck.ExportBaseFileToTmp(ctx, runCtx.cfg, runCtx.pgConnStr, batch.s3TmpPath, state.schemaID, batch.rowIDs, state.attrCache); err != nil {
		return fmt.Errorf("export batch: %w", err)
	}
	if err := CopyTmpToFinal(ctx, runCtx.s3Client, runCtx.cfg.S3Bucket, batch.tmpKey, batch.finalKey, runCtx.logger); err != nil {
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
