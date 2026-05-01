package internal

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/telemetry"
)

type duckDBRowsIterator interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

// ExecuteDuckDBFederatedQuery runs the DuckDB optimized query template using the provided
// FederatedAttributeQuery. It fetches dirty IDs from the Postgres change_log (if available),
// injects exclusions into the DuckDB WHERE clause, executes the query against the global
// DuckDB client, and returns matched PersistentRecords along with the total record count.
//
// Note: This implementation performs a best-effort scan of columns produced by the
// optimized query template. It mirrors the column ordering used by the Postgres template:
//   - main table projection (entity_main columns, order defined by entityMainColumnDescriptors)
//   - attributes_json (TEXT)
//   - total_records (bigint)
//   - total_pages (bigint)
//   - current_page (int)
func (r *DBPersistentRecordRepository) ExecuteDuckDBFederatedQuery(
	ctx context.Context,
	tables StorageTables,
	q *FederatedAttributeQuery,
	limit, offset int,
	attributeOrders []AttributeOrder,
	opts *FederatedQueryOptions,
) ([]*PersistentRecord, int64, error) {
	// Backwards-compatible wrapper that uses the streaming iterator internally
	var recs []*PersistentRecord
	total, err := r.StreamDuckDBFederatedQuery(ctx, tables, q, limit, offset, attributeOrders, opts, func(ctx context.Context, rp *PersistentRecord) error {
		recs = append(recs, rp)
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return recs, total, nil
}

// StreamDuckDBFederatedQuery streams DuckDB federated query results using a rowHandler callback.
// It reuses the same rowHandler semantics as Postgres' StreamOptimizedQuery to avoid loading the
// entire result set into memory.
func (r *DBPersistentRecordRepository) StreamDuckDBFederatedQuery(
	ctx context.Context,
	tables StorageTables,
	q *FederatedAttributeQuery,
	limit, offset int,
	attributeOrders []AttributeOrder,
	opts *FederatedQueryOptions,
	rowHandler func(context.Context, *PersistentRecord) error,
) (int64, error) {
	if q == nil {
		return 0, fmt.Errorf("query cannot be nil")
	}

	// Initialize execution plan tracking
	planCtx := newDuckDBExecutionPlanContext(opts)

	// Acquire DuckDB client
	duck := r.duckDBClient
	if duck == nil || duck.DB == nil {
		planCtx.recordClientUnavailable()
		return 0, fmt.Errorf("duckdb client not available")
	}

	// Fetch dirty IDs and record in execution plan
	dirtyIDs, err := r.fetchAndRecordDirtyIDs(ctx, tables, q.SchemaID, planCtx)
	if err != nil {
		return 0, err
	}

	// Build and execute the query
	sqlStr, args, translateMs, err := r.buildDuckDBQueryWithPlan(ctx, tables, q, dirtyIDs, attributeOrders, limit, offset, planCtx)
	if err != nil {
		return 0, err
	}

	// Record translation in plan
	planCtx.recordTranslation(sqlStr, translateMs, q.UseMainAsAnchor)

	// Execute query
	planCtx.recordQueryStart()
	rows, err := duck.DB.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		planCtx.recordQueryFailure(err)
		return 0, fmt.Errorf("execute duckdb query: %w", err)
	}
	defer rows.Close()

	// Stream and process rows
	totalRecords, rowCount, err := r.streamDuckDBRows(ctx, rows, rowHandler)
	if err != nil {
		return 0, err
	}

	// Finalize execution plan
	r.finalizeDuckDBExecutionPlan(ctx, planCtx, dirtyIDs, totalRecords, rowCount)

	return totalRecords, nil
}

// fetchAndRecordDirtyIDs fetches dirty row IDs from Postgres and records in execution plan.
func (r *DBPersistentRecordRepository) fetchAndRecordDirtyIDs(
	ctx context.Context,
	tables StorageTables,
	schemaID int16,
	planCtx *duckDBExecutionPlanContext,
) ([]uuid.UUID, error) {
	if tables.ChangeLog == "" {
		return nil, nil
	}

	dirtyIDs, err := r.getDirtyIDFetcher()(ctx, tables.ChangeLog, schemaID)
	if err != nil {
		return nil, fmt.Errorf("fetch dirty ids: %w", err)
	}

	// Emit metric for dirty set size
	telemetry.EmitRowCount(ctx, "pg", int64(len(dirtyIDs)))

	// Record in execution plan
	planCtx.recordDirtyIDSource(tables.ChangeLog, len(dirtyIDs))

	return dirtyIDs, nil
}

// buildDuckDBQueryWithPlan builds the DuckDB query with execution plan recording.
func (r *DBPersistentRecordRepository) buildDuckDBQueryWithPlan(
	ctx context.Context,
	tables StorageTables,
	q *FederatedAttributeQuery,
	dirtyIDs []uuid.UUID,
	attributeOrders []AttributeOrder,
	limit, offset int,
	planCtx *duckDBExecutionPlanContext,
) (string, []any, int64, error) {
	// Build template params
	changeLogSchema, changeLogScanTable := duckDBPostgresScanLocation(tables.ChangeLog)
	mainSchema, mainScanTable := duckDBPostgresScanLocation(tables.EntityMain)
	eavSchema, eavScanTable := duckDBPostgresScanLocation(tables.EAVData)
	sqlParams := map[string]any{
		"EAVTable":             sanitizeIdentifier(tables.EAVData),
		"MainTable":            sanitizeIdentifier(tables.EntityMain),
		"ChangeLogTable":       sanitizeIdentifier(tables.ChangeLog),
		"ChangeLogSchema":      changeLogSchema,
		"ChangeLogScanTable":   changeLogScanTable,
		"MainSchema":           mainSchema,
		"MainScanTable":        mainScanTable,
		"EAVSchema":            eavSchema,
		"EAVScanTable":         eavScanTable,
		"MainProjection":       entityMainProjection,
		"SchemaID":             q.SchemaID,
		"UseMainTableAsAnchor": q.UseMainAsAnchor,
		"Anchor": map[string]any{
			"Condition": "1=1", // BuildDuckDBQuery will overwrite with actual where clause
		},
		"SortKeys": attributeOrders,
		"Limit":    limit,
		"Offset":   offset,
		"PageSize": limit,
	}

	if connStr := r.duckDBPostgresConnString(); connStr != "" {
		sqlParams["DuckDBPGConnString"] = connStr
	}
	if paths := duckDBParquetPathsForQuery(q); len(paths) > 0 {
		sqlParams["DuckDBS3Paths"] = paths
	}

	startTranslate := time.Now()

	// Build dual clauses (PG pushdown + DuckDB logical) if metadata cache available
	var cache forma.SchemaAttributeCache
	if r.metadataCache != nil {
		if c, ok := r.metadataCache.GetSchemaCacheByID(q.SchemaID); ok {
			cache = c
		}
	}
	paramIndex := 0
	dc, err := ToDualClauses(q.Condition, sanitizeIdentifier(tables.EAVData), q.SchemaID, cache, &paramIndex)
	if err != nil {
		return "", nil, 0, fmt.Errorf("to dual clauses: %w", err)
	}

	// Compute schema-driven projections for the template
	injectSchemaProjections(sqlParams, q.SchemaID, cache)

	// Record Postgres pushdown fragment
	planCtx.recordPushdownFragment(dc.PgMainClause)

	sqlStr, args, err := r.getDuckDBQueryBuilder()(r.getDuckDBTemplate(), sqlParams, q, dirtyIDs, &dc)
	translateMs := time.Since(startTranslate).Milliseconds()
	telemetry.EmitLatency(ctx, "translation", translateMs)
	if err != nil {
		return "", nil, 0, fmt.Errorf("build duckdb query: %w", err)
	}

	return sqlStr, args, translateMs, nil
}

func (r *DBPersistentRecordRepository) duckDBPostgresConnString() string {
	if r.pool == nil {
		return ""
	}
	if cfgPool, ok := r.pool.(*pgxpool.Pool); ok && cfgPool != nil {
		cfg := cfgPool.Config()
		if cfg != nil {
			connCfg := cfg.ConnConfig
			return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
				connCfg.Host,
				connCfg.Port,
				connCfg.User,
				connCfg.Password,
				connCfg.Database,
			)
		}
	}
	return ""
}

func duckDBParquetPathsForQuery(q *FederatedAttributeQuery) []string {
	if q == nil || q.DuckDBHints == nil || q.DuckDBHints.S3ParquetPathTemplate == "" {
		return nil
	}
	rendered, err := RenderS3ParquetPath(q.DuckDBHints.S3ParquetPathTemplate, q.SchemaID)
	if err != nil {
		return nil
	}
	parts := strings.Split(rendered, ",")
	paths := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			paths = append(paths, trimmed)
		}
	}
	return paths
}

// injectSchemaProjections computes schema-driven SQL fragments from the attribute
// cache and injects them into the template parameter map. For benchmark schemas
// (IDs 100-102) it uses the benchmark parquet shape; for production it uses the
// schema attribute cache for column bindings and EAV pivots.
func injectSchemaProjections(sqlParams map[string]any, schemaID int16, cache forma.SchemaAttributeCache) {
	if isBenchmarkSchemaID(schemaID) {
		// Benchmark schemas have flat parquet columns; use hardcoded benchmark projections
		sqlParams["S3SourceSelect"] = BuildBenchmarkS3Projection(schemaID)
		sqlParams["OuterSelect"] = BuildBenchmarkOuterSelect(schemaID)
		// Benchmark schemas need PG source projections based on their attribute layout
		sp, _ := BuildSchemaProjection(schemaID, cache)
		if sp != nil {
			sqlParams["PGSourceSelect"] = sp.PGSourceSelect
			sqlParams["PGGroupBy"] = sp.PGGroupBy
			sqlParams["EAVPivotSelect"] = sp.EAVPivotSelect
			sqlParams["EAVPivotAttrs"] = sp.EAVPivotAttrs
			sqlParams["HasEAVPivot"] = len(sp.EAVPivotAttrs) > 0
		}
		return
	}
	if len(cache) == 0 {
		// No cache: fall back to defaults that match the fixed layout
		sqlParams["S3SourceSelect"] = "row_id, ltbase_created_at AS created_at, ltbase_updated_at AS ver_ts, ltbase_deleted_at AS deleted_ts, name, age, tag"
		sqlParams["PGSourceSelect"] = "m.ltbase_row_id AS row_id, m.ltbase_created_at AS created_at, cl.changed_at AS ver_ts, cl.deleted_at AS deleted_ts, CAST(m.text_01 AS VARCHAR) AS name, CAST(m.integer_01 AS INTEGER) AS age, MAX(CASE WHEN e.attr_id = 205 THEN CAST(e.value_text AS VARCHAR) END) AS tag"
		sqlParams["PGGroupBy"] = "m.ltbase_row_id, m.ltbase_created_at, cl.changed_at, cl.deleted_at, m.text_01, m.integer_01"
		sqlParams["EAVPivotSelect"] = "MAX(CASE WHEN attr_id = 205 THEN CAST(e.value_text AS VARCHAR) END) AS tag"
		sqlParams["EAVPivotAttrs"] = "205"
		sqlParams["HasEAVPivot"] = true
		sqlParams["OuterSelect"] = fallbackOuterSelect(schemaID)
		return
	}

	// Production schema: compute projections from the attribute cache
	sp, err := BuildSchemaProjection(schemaID, cache)
	if err != nil || sp == nil {
		return
	}
	sqlParams["S3SourceSelect"] = sp.S3SourceSelect
	sqlParams["PGSourceSelect"] = sp.PGSourceSelect
	sqlParams["PGGroupBy"] = sp.PGGroupBy
	sqlParams["EAVPivotSelect"] = sp.EAVPivotSelect
	sqlParams["EAVPivotAttrs"] = sp.EAVPivotAttrs
	sqlParams["HasEAVPivot"] = len(sp.EAVPivotAttrs) > 0
	sqlParams["OuterSelect"] = sp.OuterSelect
}

// fallbackOuterSelect returns the fixed outer SELECT for the no-cache fallback path.
func fallbackOuterSelect(schemaID int16) string {
	return fmt.Sprintf(`%d::SMALLINT AS ltbase_schema_id,
			CAST(row_id AS UUID) AS ltbase_row_id,
			created_at AS ltbase_created_at,
			ver_ts AS ltbase_updated_at,
			deleted_ts AS ltbase_deleted_at,
			name AS text_01,
			age AS integer_01,
			'[]'::TEXT AS attributes_json,
			COUNT(DISTINCT row_id) OVER() AS total_records,
			CEIL(COUNT(DISTINCT row_id) OVER()::DOUBLE / NULLIF({{.PAGE_SIZE}}, 0))::BIGINT AS total_pages,
			(FLOOR({{.OFFSET}}::DOUBLE / NULLIF({{.PAGE_SIZE}}, 0)) + 1)::BIGINT AS current_page`, schemaID)
}

func duckDBPostgresScanLocation(name string) (string, string) {
	parts := strings.Split(name, ".")
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.Trim(part, " \"")
		if trimmed != "" {
			clean = append(clean, trimmed)
		}
	}
	if len(clean) >= 2 {
		return clean[0], clean[1]
	}
	if len(clean) == 1 {
		return "public", clean[0]
	}
	return "public", ""
}

// streamDuckDBRows iterates through DuckDB rows and invokes the handler.
func (r *DBPersistentRecordRepository) streamDuckDBRows(
	ctx context.Context,
	rows duckDBRowsIterator,
	rowHandler func(context.Context, *PersistentRecord) error,
) (int64, int64, error) {
	buffers := newDuckDBScanBuffers()

	var totalRecords int64
	totalSet := false
	rowCount := int64(0)

	for rows.Next() {
		scanArgs, attrsJSON, totalRec, _, _ := buffers.buildScanArgs()

		if err := rows.Scan(scanArgs...); err != nil {
			return 0, 0, fmt.Errorf("scan duckdb row: %w", err)
		}

		// Build record from buffers
		record := buffers.buildRecordFromBuffers()

		// Parse attributes JSON
		if attrsJSON.Valid {
			if err := parseDuckDBAttributesJSON(attrsJSON.String, record); err != nil {
				return 0, 0, err
			}
		}

		// Clean up empty maps
		cleanupEmptyMaps(record)

		if !totalSet && totalRec.Valid {
			totalRecords = totalRec.Int64
			totalSet = true
		}

		// Invoke handler
		if rowHandler != nil {
			if err := rowHandler(ctx, record); err != nil {
				return 0, 0, err
			}
		}

		rowCount++
	}

	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("iterate duckdb rows: %w", err)
	}

	return totalRecords, rowCount, nil
}

// finalizeDuckDBExecutionPlan completes the execution plan with timing and metrics.
func (r *DBPersistentRecordRepository) finalizeDuckDBExecutionPlan(
	ctx context.Context,
	planCtx *duckDBExecutionPlanContext,
	dirtyIDs []uuid.UUID,
	totalRecords int64,
	rowCount int64,
) {
	if planCtx.opts == nil || !planCtx.opts.IncludeExecutionPlan || planCtx.opts.ExecutionPlan == nil {
		return
	}

	qMs := time.Since(planCtx.startQuery).Milliseconds()

	// Update the last source with actual rows and duration
	if len(planCtx.opts.ExecutionPlan.Sources) > 0 {
		idx := len(planCtx.opts.ExecutionPlan.Sources) - 1
		dp := planCtx.opts.ExecutionPlan.Sources[idx]
		dp.ActualRows = rowCount
		dp.DurationMs = qMs
		planCtx.opts.ExecutionPlan.Sources[idx] = dp
	}

	planCtx.opts.ExecutionPlan.Timings["duckdb_fetch"] = qMs
	planCtx.opts.ExecutionPlan.Timings["total"] = time.Since(planCtx.startTotal).Milliseconds()

	// Emit telemetry
	telemetry.EmitLatency(ctx, "execution", qMs)
	streamMs := max(time.Since(planCtx.startQuery).Milliseconds()-qMs, 0)
	telemetry.EmitLatency(ctx, "streaming", streamMs)
	telemetry.EmitRowCount(ctx, "duckdb", rowCount)

	// Compute pushdown efficiency
	pgRows := computePgRowCount(planCtx.opts.ExecutionPlan, dirtyIDs)
	finalRows := totalRecords
	if finalRows <= 0 {
		finalRows = rowCount
	}
	if finalRows <= 0 {
		finalRows = 1
	}
	ratio := float64(pgRows) / float64(finalRows)
	telemetry.EmitPushdownEfficiency(ctx, 0, ratio) // schemaID not available here, use 0

	planCtx.opts.ExecutionPlan.Notes = append(planCtx.opts.ExecutionPlan.Notes,
		fmt.Sprintf("pushdown_efficiency=%.3f (pg_rows=%d final_rows=%d)", ratio, pgRows, finalRows))
}

// capturePgPushdownRowCount fetches the actual PG pushdown row count for tier-specific
// pushdown evidence. It reads the pushdown clause from the execution plan and runs a
// targeted COUNT query on the PG side to measure how many rows the pushdown clause
// selects from entity_main. This is diagnostic-only and non-blocking.
func (r *DBPersistentRecordRepository) capturePgPushdownRowCount(
	ctx context.Context,
	tables StorageTables,
	schemaID int16,
	planCtx *duckDBExecutionPlanContext,
) {
	if planCtx == nil || planCtx.opts == nil || !planCtx.opts.IncludeExecutionPlan ||
		planCtx.opts.ExecutionPlan == nil || r.pool == nil {
		return
	}

	// Find the PG pushdown fragment source
	var pgIdx int = -1
	for i, src := range planCtx.opts.ExecutionPlan.Sources {
		if src.Reason == "pushdown fragment" {
			pgIdx = i
			break
		}
	}
	if pgIdx < 0 {
		return
	}

	pgMainClause := planCtx.opts.ExecutionPlan.Sources[pgIdx].SQL
	if pgMainClause == "" {
		return
	}

	mainSchema, mainScanTable := duckDBPostgresScanLocation(tables.EntityMain)
	changeLogSchema, changeLogScanTable := duckDBPostgresScanLocation(tables.ChangeLog)

	// Build a PG-targeted COUNT query to measure pushdown selectivity
	countSQL := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM %s m
		JOIN %s cl ON cl.schema_id = m.ltbase_schema_id AND cl.row_id = m.ltbase_row_id
		WHERE cl.schema_id = %d AND cl.flushed_at = 0 AND m.ltbase_schema_id = %d AND (%s)`,
		sanitizeIdentifier(fmt.Sprintf("%s.%s", mainSchema, mainScanTable)),
		sanitizeIdentifier(fmt.Sprintf("%s.%s", changeLogSchema, changeLogScanTable)),
		schemaID, schemaID, pgMainClause)

	connStr := r.duckDBPostgresConnString()
	if connStr == "" {
		return
	}
	duck := r.duckDBClient
	if duck == nil || duck.DB == nil {
		return
	}

	var pgRowCount int64
	if err := duck.DB.QueryRowContext(ctx, countSQL).Scan(&pgRowCount); err != nil {
		// Non-fatal: pushdown evidence is best-effort
		return
	}

	planCtx.opts.ExecutionPlan.Sources[pgIdx].ActualRows = pgRowCount
	planCtx.opts.ExecutionPlan.Notes = append(planCtx.opts.ExecutionPlan.Notes,
		fmt.Sprintf("pg_pushdown_rows=%d", pgRowCount))
}
func computePgRowCount(plan *ExecutionPlan, dirtyIDs []uuid.UUID) int64 {
	var pgRows int64
	for _, src := range plan.Sources {
		if src.Engine == "postgres" {
			if src.ActualRows > 0 {
				pgRows += src.ActualRows
			} else if src.RowEstimate > 0 {
				pgRows += src.RowEstimate
			}
		}
	}
	return pgRows
}
