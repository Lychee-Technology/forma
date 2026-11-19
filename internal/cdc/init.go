package cdc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"go.uber.org/zap"
)

// InitOptions configures a cdc-init run. It mirrors the cdc-init CLI
// options; the CLI in cmd/tools delegates here.
type InitOptions struct {
	Config CDCConfig
	// S3Client is the client the base objects and the init manifests are
	// written through, once each. *s3.Client satisfies it.
	S3Client            S3Client
	SchemaRegistryTable string
	SchemaIDFilter      int
	DryRun              bool
	// AutoEstimateRowBytes sizes each base object from the schema's
	// attributes instead of Config.EstimatedRowBytes (TargetFileSizeMB).
	AutoEstimateRowBytes bool
	Logger               *zap.Logger
	SchemaRegistry       forma.SchemaRegistry
}

// InitSummary reports totals across all schemas processed by RunInit. A dry
// run counts the rows and objects it would export.
type InitSummary struct {
	TotalRowsExported int64
	TotalFilesCreated int
	// Schemas lists what each schema's init published and handed off, in run
	// order.
	Schemas []SchemaInit
	Skipped []SkippedSchema // schemas the parquet tiers cannot hold (#704)
}

// SchemaInit is what one schema's cdc-init published and handed off
// (cdcv2.InitReport).
type SchemaInit struct {
	SchemaID int16
	// Epoch is the record's epoch the run loaded and pinned, Generation the
	// init generation it committed (0 for a dry run).
	Epoch, Generation int64
	// Rows counts the live rows exported, Files the base objects, Entries the
	// exported rows that had an unflushed entry (the handoff's candidates),
	// and Marked those the handoff marked flushed at their export versions.
	// Unconfirmed counts entries a handoff chunk whose COMMIT went without an
	// answer may have marked beyond Marked.
	Rows            int64
	Files           int
	Entries, Marked int
	Unconfirmed     int
	// HandoffStopped says a later cdc-init or a drop replaced the init's
	// lineage during the handoff: the entries it had not marked stay dirty
	// for the next flush.
	HandoffStopped bool
}

type initRunContext struct {
	cfg                  CDCConfig
	db                   *sql.DB
	duck                 *DuckExporter
	store                manifestv2.Store
	schemaRegistry       forma.SchemaRegistry
	attrCaches           map[int16]forma.SchemaAttributeCache
	logger               *zap.Logger
	dryRun               bool
	autoEstimateRowBytes bool
	// locker is the run engine's schema lock (nil: the PostgreSQL advisory
	// lock); initSchemaFn is a test seam (nil→initSchema), mirroring the
	// flusher's processSchemaFn seam.
	locker       SchemaLocker
	initSchemaFn func(ctx context.Context, runCtx *initRunContext, schemaID int16) (SchemaInit, error)
}

// normalizeInitOptions applies the same config defaults as Runner.RunOnce.
// Without them a zero-value CDCConfig yields BatchSize=0, and every export
// snapshot would read no row.
func normalizeInitOptions(opts InitOptions) InitOptions {
	opts.Config = opts.Config.WithDefaults()
	if opts.Logger == nil {
		opts.Logger = zap.NewNop()
	}
	return opts
}

func newInitRunContext(ctx context.Context, opts InitOptions) (*initRunContext, error) {
	cfg := opts.Config
	store, err := newObjectStore(opts.S3Client, cfg.S3Bucket)
	if err != nil {
		return nil, fmt.Errorf("resolve the object store: %w", err)
	}
	pgConnStr := BuildPGDSN(PGDSNParams{Host: cfg.PGHost, Port: cfg.PGPort, User: cfg.PGUser, Password: cfg.PGPassword, DB: cfg.PGDB, SSLMode: cfg.PGSSLMode})
	db, err := sql.Open("pgx", pgConnStr)
	if err != nil {
		return nil, fmt.Errorf("open pg: %w", err)
	}
	locker, err := NewSchemaLocker(db, cfg.Engine, cfg.EntityMainTable)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("cdc init schema lock: %w", err)
	}

	// DuckDB httpfs credentials follow the same both-halves rule as every
	// other cdc credential site (#326), and the session token rides the
	// source that supplied the pair (#329). The exports write local scratch
	// files; the exporter still configures S3 as every tool's does.
	s3Key, s3Secret, s3Token := ResolveStaticS3Credentials(cfg)
	duck, err := NewDuckExporter(ctx, cfg, s3Key, s3Secret, s3Token, opts.Logger)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("new duck exporter: %w", err)
	}

	return &initRunContext{
		cfg:                  cfg,
		db:                   db,
		locker:               locker,
		duck:                 duck,
		store:                store,
		schemaRegistry:       opts.SchemaRegistry,
		logger:               opts.Logger,
		dryRun:               opts.DryRun,
		autoEstimateRowBytes: opts.AutoEstimateRowBytes,
	}, nil
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

// RunInit re-onboards all registered schemas, or the one SchemaIDFilter
// names, through the V2 writer (internal/cdcv2): for each it exports every
// live row to base objects, publishes them as an init generation under a new
// lineage, and hands the exported rows' unflushed entries off at exactly
// their export versions. It never clears change_log.
func RunInit(ctx context.Context, opts InitOptions) (InitSummary, error) {
	opts = normalizeInitOptions(opts)

	runCtx, err := newInitRunContext(ctx, opts)
	if err != nil {
		return InitSummary{}, fmt.Errorf("set up cdc init run: %w", err)
	}
	defer runCtx.close()

	schemaIDs, err := getSchemaIDsToInit(ctx, runCtx.db, opts.SchemaRegistryTable, opts.SchemaIDFilter)
	if err != nil {
		return InitSummary{}, fmt.Errorf("list schemas to initialize: %w", err)
	}

	if len(schemaIDs) == 0 {
		opts.Logger.Info("no schemas to initialize")
		return InitSummary{}, nil
	}

	opts.Logger.Info("schemas to initialize", zap.Int("count", len(schemaIDs)), zap.Any("schema_ids", schemaIDs))

	summary, schemaErr := processInitSchemas(ctx, runCtx, schemaIDs)
	opts.Logger.Info("CDC init summary",
		zap.Int64("total_rows_exported", summary.TotalRowsExported),
		zap.Int("total_files_created", summary.TotalFilesCreated),
		zap.Any("skipped_schemas", summary.Skipped))
	if err := refuseNamedSkippedSchema(opts.SchemaIDFilter, summary.Skipped); err != nil {
		return summary, fmt.Errorf("check the --schema-id filter: %w", err)
	}
	return summary, schemaErr
}

func processInitSchemas(ctx context.Context, runCtx *initRunContext, schemaIDs []int64) (InitSummary, error) {
	caches, err := resolveRunAttrCaches(ctx, runCtx.schemaRegistry, schemaIDs)
	if err != nil {
		return InitSummary{}, fmt.Errorf("cdc init pre-flight: %w", err)
	}
	runCtx.attrCaches = caches
	schemaIDs, skipped := exportableSchemaIDs(runCtx.logger, schemaIDs, runCtx.attrCaches)
	summary := InitSummary{Skipped: skipped}
	var schemaErrors []error

	for _, sid := range schemaIDs {
		schemaID := int16(sid)
		out, err := initSchemaUnderLock(ctx, runCtx, schemaID)
		if err != nil {
			// A failed schema is not counted as initialized; what its writer
			// got to is in the error and the log.
			runCtx.logger.Error("failed to init schema", zap.Int16("schema_id", schemaID), zap.Any("progress", out), zap.Error(err))
			schemaErrors = append(schemaErrors, fmt.Errorf("schema %d: %w", schemaID, err))
			continue
		}
		summary.Schemas = append(summary.Schemas, out)
		summary.TotalRowsExported += out.Rows
		summary.TotalFilesCreated += out.Files
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
