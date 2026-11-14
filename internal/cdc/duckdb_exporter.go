package cdc

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/duckdbinit"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/sqlutil"
	"go.uber.org/zap"
)

// DuckExporter handles DuckDB interactions for exporting snapshots to S3 temp path.
type DuckExporter struct {
	DB     *sql.DB
	Logger *zap.Logger
}

type exportModeSpec struct {
	defaultMemoryLimit string
	activeOnly         bool
	useChangeLog       bool
	schemaIDSelect     string
	rowIDSelect        string
	timeSlotSelect     string
	deletedAtSelect    string
}

// exportSQLPlan is one export: the statements that prepare its pinned
// DuckDB connection, the PostgreSQL reads staged on it, and the COPY that
// projects the staged rows to Parquet.
type exportSQLPlan struct {
	setupSQL string
	stages   []exportStage
	copySQL  string
}

// newDuckConnectorFn opens one DuckDB database instance for the exporter;
// tests replace it to stand in for a database DuckDB invalidates.
var newDuckConnectorFn = func(dsn string, connInit func(driver.ExecerContext) error) (driver.Connector, error) {
	return duckdb.NewConnector(dsn, connInit)
}

// NewDuckExporter opens a DuckDB pool whose every physical connection
// self-configures (pragmas, extensions, S3 session settings) via a connector
// init hook — session-scoped statements issued through the pool reach only
// one arbitrary connection (#285, same class as #245). A failed optional
// statement (a resource PRAGMA) is logged and skipped; a failed required one
// (httpfs, parquet, an S3 setting) fails the connection, so construction
// fails at the ping instead of returning an exporter whose first export
// cannot reach S3 (#752). The pool reopens its database after DuckDB
// invalidates it (#717).
//
// The engine serves the CDC exports (cdc-flush, cdc-init) and the S3-only
// tools (compactor, baseline statistics, manifest audit) alike. None of them
// reads PostgreSQL through DuckDB: the exports read it over pgx and stage the
// rows (#785), so postgres_scanner is not loaded at all.
//
// The caller passes the whole key/secret/token triple because a session token
// is only valid with the pair it was issued alongside; resolving it here would
// let a token from one source pair with a key from another (#329, see
// ResolveStaticS3Credentials).
func NewDuckExporter(ctx context.Context, cfg CDCConfig, s3AccessKey, s3Secret, s3SessionToken string, logger *zap.Logger) (*DuckExporter, error) {
	steps, err := buildExporterInitSteps(cfg, s3AccessKey, s3Secret, s3SessionToken)
	if err != nil {
		return nil, fmt.Errorf("build duckdb exporter init statements: %w", err)
	}
	// A Runner caches this exporter for the process lifetime (a warm Lambda),
	// so a DuckDB invalidation must not outlive the export that hit it: the
	// recovering connector reopens the database, with the same init hook, on
	// the next connection, and DB stays the same *sql.DB (#717).
	connInit := duckdbinit.MakeConnInit(steps, logger.Sugar(), duckdbinit.DefaultInitTimeout)
	connector, err := duckdbinit.NewRecoveringConnector(func() (driver.Connector, error) {
		return newDuckConnectorFn(cfg.DuckDBPath, connInit)
	}, logger.Sugar())
	if err != nil {
		return nil, fmt.Errorf("open duckdb connector: %w", err)
	}
	db := sql.OpenDB(connector)
	// Exports run sequentially and file-backed DuckDB is effectively
	// single-writer; per-connection init keeps a larger pool safe if ever
	// needed, but nothing needs one today (#285). An export pins its
	// connection for its staged temp tables (runExport).
	db.SetMaxOpenConns(1)

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping duckdb: %w", err)
	}
	return &DuckExporter{DB: db, Logger: logger}, nil
}

// ExportSnapshotToTmp exports the batch's unflushed change_log versions, with
// their main and EAV values, from pg (a pgx database/sql pool) to s3TmpPath,
// e.g. 's3://bucket/prefix/<schema_id>/_tmp/<tmp_uuid>.parquet'.
func (e *DuckExporter) ExportSnapshotToTmp(ctx context.Context, cfg CDCConfig, pg *sql.DB, s3TmpPath string, schemaID int16, snapshotTS int64, rowIDs []uuid.UUID, attrCache forma.SchemaAttributeCache) error {
	plan, err := buildDeltaExportPlan(stagedTransport, s3TmpPath, cfg, schemaID, snapshotTS, rowIDs, attrCache)
	if err != nil {
		return fmt.Errorf("export snapshot to tmp for schema %d: %w", schemaID, err)
	}

	return e.runExport(ctx, cfg, pg, plan, rowIDs, "duckdb export sql", "duckdb copy exec")
}

func buildDeltaExportPlan(transport exportTransport, s3TmpPath string, cfg CDCConfig, schemaID int16, snapshotTS int64, rowIDs []uuid.UUID, attrCache forma.SchemaAttributeCache) (exportSQLPlan, error) {
	plan, err := buildExportSQLPlan(exportModeSpec{
		defaultMemoryLimit: "4GB",
		activeOnly:         false,
		useChangeLog:       true,
		schemaIDSelect:     "cl.schema_id",
		rowIDSelect:        "cl.row_id",
		timeSlotSelect:     "cl.changed_at AS changed_at",
		// #274: live rows encode deleted_at = 0 on both tiers (base already
		// COALESCEs in init_exporter.go). The AS alias is load-bearing: without
		// it DuckDB names the parquet column "coalesce(cl.deleted_at, 0)". The
		// staged change_log column and the raw m.ltbase_deleted_at main
		// column below stay untouched.
		deletedAtSelect: "COALESCE(cl.deleted_at, 0) AS deleted_at",
	}, transport, s3TmpPath, cfg, schemaID, snapshotTS, rowIDs, attrCache)
	if err != nil {
		return exportSQLPlan{}, fmt.Errorf("build delta export SQL plan: %w", err)
	}
	return plan, nil
}

// runExport runs plan on one pinned DuckDB connection: it creates the
// staged temp tables, streams the PostgreSQL rows into them, COPYs the
// projection to Parquet, and drops the tables. No PostgreSQL connection
// string or credential enters DuckDB (#785).
func (e *DuckExporter) runExport(ctx context.Context, cfg CDCConfig, pg *sql.DB, plan exportSQLPlan, rowIDs []uuid.UUID, logMessage, execErrPrefix string) error {
	logArgs := []any{"sql_preview", plan.copySQL, "rows", len(rowIDs)}
	for _, stage := range plan.stages {
		logArgs = append(logArgs, stage.alias+"_query", stage.query)
	}
	e.Logger.Sugar().Infow(logMessage, logArgs...)
	timeout := cfg.QueryTimeout
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}
	ctx2, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, err := e.DB.Conn(ctx2)
	if err != nil {
		return fmt.Errorf("%s: pin duckdb connection: %w", execErrPrefix, err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx2, plan.setupSQL); err != nil {
		return fmt.Errorf("%s: create staging tables: %w", execErrPrefix, err)
	}
	defer e.dropStages(ctx, conn, plan.stages)

	started := time.Now()
	staged, err := stagePostgresRows(ctx2, pg, conn, plan.stages, rowIDs)
	if err != nil {
		return fmt.Errorf("%s: %w", execErrPrefix, err)
	}
	stagedAt := time.Now()
	if _, err := conn.ExecContext(ctx2, plan.copySQL); err != nil {
		return fmt.Errorf("%s: %w", execErrPrefix, err)
	}
	// The split between the PostgreSQL read and the COPY is the export's
	// throughput question (#785); this line answers it per batch.
	e.Logger.Sugar().Infow(logMessage+" done", "staged_rows", staged,
		"stage_ms", stagedAt.Sub(started).Milliseconds(), "copy_ms", time.Since(stagedAt).Milliseconds())
	return nil
}

// dropStages drops the export's temp tables, also after a failed or
// cancelled export, so a pooled connection does not hold a batch's rows
// until its next export replaces them.
func (e *DuckExporter) dropStages(ctx context.Context, conn *sql.Conn, stages []exportStage) {
	if len(stages) == 0 {
		return
	}
	dropCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if _, err := conn.ExecContext(dropCtx, buildStageDropSQL(stages)); err != nil {
		e.Logger.Sugar().Warnw("failed to drop cdc export staging tables", "err", err)
	}
}

func buildExportSQLPlan(spec exportModeSpec, transport exportTransport, s3TmpPath string, cfg CDCConfig, schemaID int16, snapshotTS int64, rowIDs []uuid.UUID, attrCache forma.SchemaAttributeCache) (exportSQLPlan, error) {
	if len(rowIDs) == 0 && spec.useChangeLog {
		return exportSQLPlan{}, errors.New("export snapshot: no row ids provided")
	}
	if len(attrCache) == 0 {
		// A resolvable attribute cache is mandatory: without it we cannot derive
		// the numeric-family typing (bool as 1/0, dates as epoch ms) the
		// federated reader expects, and the reader itself fails fast for no-cache
		// schemas (ErrSchemaMetadataCacheRequired). Pre-flight validation in the
		// flush/init loops should already have aborted; this is defense in depth
		// for any caller that bypasses it (#193).
		return exportSQLPlan{}, fmt.Errorf("build export SQL for schema %d: attribute metadata cache is required but empty: %w", schemaID, ErrSchemaAttrCacheUnavailable)
	}
	projection, err := buildSchemaDrivenProjection(attrCache)
	if err != nil {
		return exportSQLPlan{}, fmt.Errorf("build export projection for schema %d: %w", schemaID, err)
	}

	// A base export with no row ids is a legitimate request (#519): a schema
	// with zero live rows still needs one zero-row base object carrying the
	// full projected column set, so its manifest never lists nothing (an
	// empty manifest sends readers to the legacy glob fallback, see
	// manifest.QuerySource.Paths). Its reads select no rows; the staged
	// tables keep every column.
	entityMain, eavData := resolveMainAndEAVTableNames(cfg)
	var stages []exportStage
	if spec.useChangeLog {
		changeLog := sanitizeIdentifier(cfg.ChangeLogTable)
		if changeLog == "" {
			changeLog = "change_log"
		}
		stages = append(stages, exportStage{alias: "cl", table: stageTableChangeLog, columns: changeLogStageColumns,
			query: buildChangeLogQuery(changeLog, schemaID, snapshotTS, transport.rowFilter("row_id"))})
	}
	stages = append(stages,
		exportStage{alias: "m", table: stageTableMain, columns: projection.mainColumns,
			query: buildMainEntityQuery(entityMain, schemaID, stageColumnNames(projection.mainColumns), transport.rowFilter("ltbase_row_id"), spec.activeOnly)},
		exportStage{alias: "e_rows", table: stageTableEAV, columns: eavStageColumns,
			query: buildEAVQuery(eavData, schemaID, transport.rowFilter("row_id"), projection.eavAttrIDs)},
	)

	sources := make([]string, len(stages))
	for i, stage := range stages {
		sources[i] = transport.source(stage)
	}
	opts := resolveExportSQLOptions(cfg, spec.defaultMemoryLimit)
	mainSelectCols := append(spec.baseSelectColumns(), projection.mainProjections...)
	mainSelectCols = append(mainSelectCols, projection.eavSelect...)
	return exportSQLPlan{
		setupSQL: buildStageSetupSQL(opts.memoryLimit, stages),
		stages:   stages,
		copySQL:  buildProjectedExportSQL(spec, sources, buildParquetCopyOptions(opts), sqlutil.EscapeLiteral(s3TmpPath), mainSelectCols, projection.eavAgg),
	}, nil
}

func (spec exportModeSpec) baseSelectColumns() []string {
	return []string{
		spec.schemaIDSelect,
		spec.rowIDSelect,
		spec.timeSlotSelect,
		spec.deletedAtSelect,
		"m.ltbase_created_at",
		"m.ltbase_updated_at",
		"m.ltbase_deleted_at",
	}
}

// buildProjectedExportSQL renders the COPY of the export's projection. sources
// are the CTEs that expose the staged rows as cl (delta only), m and e_rows.
func buildProjectedExportSQL(spec exportModeSpec, sources []string, copyOptions, s3Esc string, mainSelectCols, eavAgg []string) string {
	eAggSQL := buildEAVAggregationSQL("e_rows", eavAgg)
	// LEFT JOIN to entity_main so change_log tombstones of hard-deleted rows
	// are exported instead of silently dropped (#173).
	from := fmt.Sprintf(`FROM cl
LEFT JOIN m
  ON cl.row_id = m.ltbase_row_id
LEFT JOIN (
  %s
) e ON cl.row_id = e.row_id`, eAggSQL)
	if !spec.useChangeLog {
		from = fmt.Sprintf(`FROM m
LEFT JOIN (
  %s
) e ON m.ltbase_row_id = e.row_id`, eAggSQL)
	}

	return fmt.Sprintf(`COPY (
WITH %s
SELECT
  %s
%s
) TO '%s' (%s);
`, strings.Join(sources, ",\n"), strings.Join(mainSelectCols, ",\n  "), from, s3Esc, copyOptions)
}

func sortedAttrKeys(cache forma.SchemaAttributeCache) []string {
	keys := make([]string, 0, len(cache))
	for k := range cache {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// safeColumnAlias derives the parquet column name for an attribute. It
// delegates to the shared writer/reader contract in sqlgen: the federated
// reader projects the same names back out of parquet (#260).
func safeColumnAlias(name string) string {
	return sqlgen.ParquetAttrColumn(name)
}

func duckTypeForValue(v forma.ValueType) string {
	switch v {
	case forma.ValueTypeText:
		return "VARCHAR"
	case forma.ValueTypeSmallInt:
		return "SMALLINT"
	case forma.ValueTypeInteger:
		return "INTEGER"
	case forma.ValueTypeBigInt:
		return "BIGINT"
	case forma.ValueTypeNumeric:
		return "DOUBLE"
	case forma.ValueTypeDate:
		return "DATE"
	case forma.ValueTypeDateTime:
		return "TIMESTAMP"
	case forma.ValueTypeUUID:
		return "UUID"
	case forma.ValueTypeBool:
		return "BOOLEAN"
	default:
		return "VARCHAR"
	}
}

// castMainValue projects a main-column-bound attribute into its parquet
// column. The output type must match what the federated reader consumes:
// its projection casts date/datetime attrs with CAST(attr AS BIGINT)
// (sqlgen.duckDBAttrCast) and scans them as epoch-ms int64. So every
// date/datetime encoding must export epoch-ms BIGINT here, matching
// castEAVValue's date handling (#194, #219).
func castMainValue(col string, meta forma.AttributeMetadata) string {
	switch meta.ValueType {
	case forma.ValueTypeBool:
		// Both spellings are the read contract (#404; the retired attach hot
		// leg read the column the same way, #858) and
		// both yield a nullable BOOLEAN: an unset optional bool is NULL in
		// the column, NULL on the hot leg, and must stay NULL in parquet. A
		// CASE ... ELSE FALSE wrapper here used to turn that NULL into a
		// persisted false, so `equals:false` and the returned entity changed
		// at the flush boundary (PR #564 review).
		switch meta.ColumnBinding.Encoding {
		case forma.MainColumnEncodingBoolInt:
			return sqlgen.BoolTruthiness(col)
		case forma.MainColumnEncodingBoolText:
			// bool_text stores "1"/"0"; read by that contract — not a wider
			// spelling set only this leg accepts.
			return fmt.Sprintf("%s = '1'", col)
		default:
			return fmt.Sprintf("TRY_CAST(%s AS BOOLEAN)", col)
		}
	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		return castDateMainValue(col, meta)
	case forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeNumeric:
		return fmt.Sprintf("TRY_CAST(%s AS %s)", col, duckTypeForValue(meta.ValueType))
	case forma.ValueTypeUUID:
		return fmt.Sprintf("TRY_CAST(%s AS UUID)", col)
	default:
		return fmt.Sprintf("CAST(%s AS VARCHAR)", col)
	}
}

// castDateMainValue normalizes a date/datetime main column to epoch-ms BIGINT
// regardless of its declared storage encoding, so the federated reader's
// CAST(attr AS BIGINT) projection and epoch-ms BIGINT predicate binds (#200)
// receive a consistent value on every tier (#219). The two storable shapes:
//   - rfc3339: an RFC3339 string in a text column -> the instant it names
//     as BIGINT milliseconds, through the hot leg's own read
//     (sqlgen.RFC3339EpochMs), so a row reads the same before and after
//     its flush.
//   - unix_ms / default: epoch millis already in a bigint column -> pass
//     through as BIGINT.
//
// Before #219 the non-unix_ms branch exported a native DATE/TIMESTAMP column,
// which the reader's CAST(... AS BIGINT) reinterpreted as days- or
// microseconds-since-epoch — silently off from the epoch-ms convention by a
// factor of 10^3-10^8 on the warm/cold tiers.
func castDateMainValue(col string, meta forma.AttributeMetadata) string {
	if meta.ColumnBinding != nil && meta.ColumnBinding.Encoding == forma.MainColumnEncodingRFC3339 {
		return sqlgen.RFC3339EpochMs(col)
	}
	return fmt.Sprintf("TRY_CAST(%s AS BIGINT)", col)
}

// castEAVValue projects an eav_data value into the parquet column for its
// value type. It must read the column the mainline write path populates
// (transform.populateTypedValue): numeric-family types (including bool as
// 1/0 and date/datetime as epoch millis) live in value_numeric; text and
// uuid live in value_text. The output types mirror the federated reader's
// EAV pivot (sqlgen.BuildSchemaProjection): BOOLEAN for bool, epoch-ms
// BIGINT for dates, native numeric types otherwise (#173).
// 热端 pivot(sqlgen.buildEAVPivotExpr)必须与本映射保持一致(#205)。
// list 属性以 items 类型的 elemMeta 调用本函数取元素 cast，聚合成 LIST 列(#204)。
func castEAVValue(meta forma.AttributeMetadata) string {
	switch meta.ValueType {
	case forma.ValueTypeBool:
		return sqlgen.BoolTruthiness("value_numeric")
	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		return "TRY_CAST(value_numeric AS BIGINT)"
	case forma.ValueTypeSmallInt, forma.ValueTypeInteger:
		// castEAVValue only ever serves EAV-only attributes (bound attrs
		// export through castMainValue), whose value_numeric holds float64
		// images (the write funnel narrows through numutil.Float64): export
		// by storage width DOUBLE so the parquet tiers keep values the
		// declared width cannot hold — over-range and non-integral history
		// alike — matching the hot pivot (#384).
		return "TRY_CAST(value_numeric AS DOUBLE)"
	case forma.ValueTypeBigInt, forma.ValueTypeNumeric:
		return fmt.Sprintf("TRY_CAST(value_numeric AS %s)", duckTypeForValue(meta.ValueType))
	case forma.ValueTypeUUID:
		return "CAST(value_text AS VARCHAR)"
	default:
		return "CAST(value_text AS VARCHAR)"
	}
}

func joinInt16(vals []int16) string {
	parts := make([]string, 0, len(vals))
	for _, v := range vals {
		parts = append(parts, fmt.Sprintf("%d", v))
	}
	return strings.Join(parts, ", ")
}
