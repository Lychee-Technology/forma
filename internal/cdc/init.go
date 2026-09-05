package cdc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	// ReplaceDelta lets the run purge the schema's delta tier after its base
	// swap commits (#371). Without it a schema whose delta tier is non-empty
	// refuses with ErrDeltaTierPresent: a base-only swap would leave stale
	// delta generations (possibly with a superseded column type) under the
	// new base, and every read would fold them.
	ReplaceDelta bool
	// DeltaPrefix is the S3 prefix the flusher writes delta files under
	// (cdc-flush --s3-prefix). It is listed for delta-shaped objects the
	// manifest does not mention; empty skips the listing.
	DeltaPrefix string
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
	attrCaches           map[int16]forma.SchemaAttributeCache
	manifestStore        manifest.Store
	manifestResolver     manifest.PathResolver
	logger               *zap.Logger
	dryRun               bool
	autoEstimateRowBytes bool
	pgConnStr            string
	replaceDelta         bool
	deltaPrefix          string
	// tryLock and initSchemaFn are test seams (nil→real impl below), mirroring the flusher's processSchemaFn seam.
	tryLock      func(ctx context.Context, db *sql.DB, schemaID int16) (bool, func(), error)
	initSchemaFn func(ctx context.Context, runCtx *initRunContext, schemaID int16) (int64, int, error)
	// describeColumns is a test seam for the write-time footer probe that
	// stamps manifest entries (#256); nil uses the exporter's DuckDB session.
	describeColumns func(ctx context.Context, uri string) (map[string]string, error)
	// checksumObject is the run's content-hash seam (#347); nil skips
	// stamping. applyInitS3Wiring builds the real one over the run's S3 client.
	checksumObject func(ctx context.Context, key string) (string, error)
	// exportBaseFile is a test seam for the DuckDB base-file export (nil uses
	// the exporter), mirroring the flusher's exportSnapshot seam.
	exportBaseFile func(ctx context.Context, state *schemaInitState, batch schemaBatchExport) error
	// listObjectKeys and deleteObject are the delta-tier inventory and purge
	// seams (#371); nil means no usable S3 client. applyInitS3Wiring builds
	// the real ones over the run's S3 client.
	listObjectKeys func(ctx context.Context, prefix string) ([]string, error)
	deleteObject   func(ctx context.Context, key string) error
}

// normalizeInitOptions applies the same config defaults as Runner.RunOnce.
// Without them a zero-value CDCConfig yields BatchSize=0, and the batch
// query's LIMIT 0 would make RunInit report success after exporting nothing.
func normalizeInitOptions(opts InitOptions) InitOptions {
	opts.Config = opts.Config.WithDefaults()
	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
	}
	return opts
}

type schemaInitState struct {
	schemaID     int16
	attrCache    forma.SchemaAttributeCache
	batchSize    int
	fileEntries  []manifest.FileEntry
	rowsExported int64
	filesCreated int
	// deltaPurge is the set of bucket-relative delta keys the run deletes
	// once its manifest swap has committed (#371): the pre-flight inventory
	// plus any delta entry the publish finds in a reloaded manifest.
	deltaPurge []string
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
	pgConnStr := BuildPGDSN(PGDSNParams{Host: cfg.PGHost, Port: cfg.PGPort, User: cfg.PGUser, Password: cfg.PGPassword, DB: cfg.PGDB, SSLMode: sslMode})

	db, err := sql.Open("pgx", pgConnStr)
	if err != nil {
		return nil, fmt.Errorf("open pg: %w", err)
	}

	// DuckDB httpfs credentials follow the same both-halves rule as every
	// other cdc credential site (#326): with no fully-set static pair the
	// exporter receives empty strings and DuckDB inherits its own
	// environment chain. The session token rides the source that supplied the
	// pair (#329), so it is resolved together with the pair and passed on
	// rather than looked up again inside the exporter.
	s3Key, s3Secret, s3Token := ResolveStaticS3Credentials(cfg)
	duck, err := NewDuckExporter(ctx, cfg, s3Key, s3Secret, s3Token, opts.Logger)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("new duck exporter: %w", err)
	}

	runCtx := &initRunContext{
		cfg:                  cfg,
		db:                   db,
		duck:                 duck,
		schemaRegistry:       opts.SchemaRegistry,
		logger:               opts.Logger,
		dryRun:               opts.DryRun,
		autoEstimateRowBytes: opts.AutoEstimateRowBytes,
		pgConnStr:            pgConnStr,
		replaceDelta:         opts.ReplaceDelta,
		deltaPrefix:          opts.DeltaPrefix,
	}
	applyInitS3Wiring(runCtx, cfg, opts.S3Client)

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
	opts = normalizeInitOptions(opts)

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
	runCtx.attrCaches = make(map[int16]forma.SchemaAttributeCache, len(schemaIDs))
	for _, sid := range schemaIDs {
		schemaID := int16(sid)
		cache, err := resolveRequiredAttrCache(runCtx.schemaRegistry, schemaID)
		if err != nil {
			return InitSummary{}, fmt.Errorf("cdc init pre-flight: %w", err)
		}
		runCtx.attrCaches[schemaID] = cache
	}

	summary := InitSummary{}
	var schemaErrors []error

	for _, sid := range schemaIDs {
		schemaID := int16(sid)
		rowsExported, filesCreated, err := initSchemaUnderLock(ctx, runCtx, schemaID)
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
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate schema ids: %w", err)
	}
	return schemaIDs, nil
}

// initSchema exports all existing data for a single schema to S3 base files.
// The delta tier is inventoried first (#371): a non-empty delta tier refuses
// the schema unless the run may replace it, and with that permission the
// inventory is purged only after the manifest swap has committed.
func initSchema(ctx context.Context, runCtx *initRunContext, schemaID int16) (int64, int, error) {
	inventory, err := preflightDeltaTier(ctx, runCtx, schemaID)
	if err != nil {
		return 0, 0, err
	}

	state, err := prepareSchemaInit(ctx, runCtx, schemaID)
	if err != nil {
		return 0, 0, err
	}
	if state == nil {
		if !inventory.empty() {
			// No live rows means no base swap, and the purge is ordered
			// strictly after the swap, so the delta tier stays in place.
			runCtx.logger.Warn("schema has no live rows; delta tier left in place because there is no base swap to publish",
				zap.Int16("schema_id", schemaID),
				zap.Int("delta_objects", len(inventory.purgeKeys())))
		}
		return 0, 0, nil
	}
	state.deltaPurge = inventory.purgeKeys()

	if err := processSchemaBatches(ctx, runCtx, state); err != nil {
		return state.rowsExported, state.filesCreated, err
	}

	if err := publishAndPurge(ctx, runCtx, state); err != nil {
		return state.rowsExported, state.filesCreated, err
	}

	runCtx.logger.Info("schema init completed",
		zap.Int16("schema_id", schemaID),
		zap.Int64("total_rows_exported", state.rowsExported),
		zap.Int("total_files_created", state.filesCreated))

	return state.rowsExported, state.filesCreated, nil
}

// publishAndPurge orders the two post-export steps (#371): the manifest
// swap commits first, and only then are the superseded delta objects
// deleted. A failed publish therefore deletes nothing — the old manifest
// still lists the delta, and the export is unreachable garbage on
// write-once keys — while a failed delete leaves the swap published.
func publishAndPurge(ctx context.Context, runCtx *initRunContext, state *schemaInitState) error {
	if err := updateSchemaManifest(ctx, runCtx, state); err != nil {
		return err
	}
	return purgeDeltaTier(ctx, runCtx, state)
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
	// Cache was resolved and validated by the processInitSchemas pre-flight (#193).
	state.attrCache = runCtx.attrCaches[schemaID]
	state.batchSize = resolveInitBatchSize(runCtx, schemaID, state.attrCache)
	return state, nil
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
	var after *uuid.UUID
	for {
		rowIDs, err := selectEntityMainBatch(ctx, runCtx.db, runCtx.cfg.EntityMainTable, state.schemaID, after, state.batchSize)
		if err != nil {
			return fmt.Errorf("select batch: %w", err)
		}
		if len(rowIDs) == 0 {
			return nil
		}

		// Selection->export test seam, mirroring the flusher's (#182). Init
		// has no snapshot clock, so the hook receives 0.
		if hook := runCtx.cfg.BeforeExportHook; hook != nil {
			if err := hook(ctx, state.schemaID, rowIDs, 0); err != nil {
				return fmt.Errorf("before-export hook: %w", err)
			}
		}

		if err := onBatch(rowIDs); err != nil {
			return err
		}
		last := rowIDs[len(rowIDs)-1]
		after = &last
	}
}

func buildSchemaBatchExport(runCtx *initRunContext, state *schemaInitState, rowIDs []uuid.UUID) schemaBatchExport {
	minRowID := rowIDs[0].String()
	maxRowID := rowIDs[len(rowIDs)-1].String()

	tmpUUID := uuid.Must(uuid.NewV7()).String()
	tmpKey := BuildBaseTempPath(runCtx.cfg.S3Prefix, state.schemaID, tmpUUID)
	// tmp and final share the batch's UUID: the final key is write-once
	// (#416), and a shared id lets an operator pair a stranded _tmp object
	// with the final it was meant to become.
	finalKey := BuildBasePath(runCtx.cfg.S3Prefix, state.schemaID, minRowID, maxRowID, tmpUUID)
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

func recordSchemaBatchResult(state *schemaInitState, batch schemaBatchExport, createdAt int64, sizeBytes int64, columns map[string]string, checksum string) {
	state.fileEntries = append(state.fileEntries, manifest.FileEntry{
		Tier:       "base",
		Path:       batch.finalKey,
		RowIDMin:   batch.minRowID,
		RowIDMax:   batch.maxRowID,
		RowCount:   int64(len(batch.rowIDs)),
		CreatedMin: createdAt,
		CreatedMax: createdAt,
		SizeBytes:  sizeBytes,
		Columns:    columns,
		Checksum:   checksum,
	})
	state.rowsExported += int64(len(batch.rowIDs))
	state.filesCreated++
}

// exportBaseFileForBatch writes the batch's rows to the tmp object, through the
// run's export seam when one is installed (tests) and the DuckDB exporter
// otherwise.
func exportBaseFileForBatch(ctx context.Context, runCtx *initRunContext, state *schemaInitState, batch schemaBatchExport) error {
	if runCtx.exportBaseFile != nil {
		return runCtx.exportBaseFile(ctx, state, batch)
	}
	return runCtx.duck.ExportBaseFileToTmp(ctx, runCtx.cfg, runCtx.pgConnStr, batch.s3TmpPath, state.schemaID, batch.rowIDs, state.attrCache)
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

	if err := exportBaseFileForBatch(ctx, runCtx, state, batch); err != nil {
		return fmt.Errorf("export batch: %w", err)
	}
	if err := CopyTmpToFinal(ctx, runCtx.s3Client, runCtx.cfg.S3Bucket, batch.tmpKey, batch.finalKey, runCtx.logger); err != nil {
		return fmt.Errorf("copy tmp->final: %w", err)
	}

	sizeBytes, err := HeadObjectSize(ctx, runCtx.s3Client, runCtx.cfg.S3Bucket, batch.finalKey)
	if err != nil {
		// Best-effort: SizeBytes only feeds compaction's promotion heuristic.
		runCtx.logger.Warn("failed to stat final base object; manifest SizeBytes stays 0",
			zap.Int16("schema_id", state.schemaID),
			zap.String("final_key", batch.finalKey),
			zap.Error(err))
	}

	recordSchemaBatchResult(state, batch, time.Now().UnixMilli(), sizeBytes,
		initStampColumns(ctx, runCtx, state.schemaID, batch),
		initStampChecksum(ctx, runCtx, state.schemaID, batch))

	runCtx.logger.Info("batch completed",
		zap.Int16("schema_id", state.schemaID),
		zap.Int64("rows_exported", state.rowsExported),
		zap.Int("files_created", state.filesCreated),
		zap.String("final_key", batch.finalKey))
	return nil
}
