package federated

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/sqlutil"
	"github.com/lychee-technology/forma/internal/telemetry"
)

func isBenchmarkSchemaID(schemaID int16) bool { return sqlgen.IsBenchmarkSchemaID(schemaID) }

type duckDBRowsIterator interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// ExecuteDuckDBFederatedQuery runs the DuckDB optimized query template using the provided
// model.FederatedAttributeQuery. It fetches dirty IDs from the Postgres change_log (if available),
// injects exclusions into the DuckDB WHERE clause, executes the query against the global
// DuckDB client, and returns matched PersistentRecords along with the total record count.
//
// Note: This implementation performs a best-effort scan of columns produced by the
// optimized query template. It mirrors the column ordering used by the Postgres template:
//   - main table projection (entity_main columns, order defined by model.EntityMainColumnDescriptors)
//   - attributes_json (TEXT)
//   - total_records (bigint)
//   - total_pages (bigint)
//   - current_page (int)
func (e *DBFederatedQueryEngine) ExecuteDuckDBFederatedQuery(
	ctx context.Context,
	tables model.StorageTables,
	q *model.FederatedAttributeQuery,
	limit, offset int,
	attributeOrders []model.AttributeOrder,
	opts *model.FederatedQueryOptions,
) ([]*model.PersistentRecord, int64, error) {
	// Backwards-compatible wrapper that uses the streaming iterator internally
	var recs []*model.PersistentRecord
	total, err := e.StreamDuckDBFederatedQuery(ctx, tables, q, limit, offset, attributeOrders, opts, func(ctx context.Context, rp *model.PersistentRecord) error {
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
func (e *DBFederatedQueryEngine) StreamDuckDBFederatedQuery(
	ctx context.Context,
	tables model.StorageTables,
	q *model.FederatedAttributeQuery,
	limit, offset int,
	attributeOrders []model.AttributeOrder,
	opts *model.FederatedQueryOptions,
	rowHandler func(context.Context, *model.PersistentRecord) error,
) (int64, error) {
	if q == nil {
		return 0, fmt.Errorf("query cannot be nil")
	}

	// Initialize execution plan tracking
	planCtx := newDuckDBExecutionPlanContext(opts)

	if e == nil || e.duck == nil {
		planCtx.recordClientUnavailable()
		return 0, fmt.Errorf("duckdb client not available")
	}

	// Fetch dirty IDs and record in execution plan
	dirtyIDs, err := e.fetchAndRecordDirtyIDs(ctx, tables, q.SchemaID, planCtx)
	if err != nil {
		return 0, err
	}

	// Build and execute the query
	sqlStr, args, translateMs, err := e.buildDuckDBQueryWithPlan(ctx, tables, q, dirtyIDs, attributeOrders, limit, offset, planCtx)
	if err != nil {
		return 0, err
	}

	// Record translation in plan
	planCtx.recordTranslation(sqlStr, translateMs, q.UseMainAsAnchor)

	// Check circuit breaker before executing
	if e.breaker != nil && e.breaker.IsOpen() {
		planCtx.recordClientUnavailable()
		return 0, fmt.Errorf("duckdb circuit breaker open, query rejected")
	}

	// Execute query
	planCtx.recordQueryStart()
	rows, err := e.duck.Query(ctx, sqlStr, args...)
	if err != nil {
		planCtx.recordQueryFailure(err)
		// Record failure in circuit breaker
		if e.breaker != nil {
			e.breaker.RecordFailure()
		}
		return 0, fmt.Errorf("execute duckdb query: %w", err)
	}
	defer rows.Close()

	// Stream and process rows
	totalRecords, rowCount, err := e.streamDuckDBRows(ctx, rows, rowHandler)
	if err != nil {
		// Record failure in circuit breaker
		if e.breaker != nil {
			e.breaker.RecordFailure()
		}
		return 0, err
	}

	// Record success in circuit breaker
	if e.breaker != nil {
		e.breaker.RecordSuccess()
	}

	// Finalize execution plan
	e.finalizeDuckDBExecutionPlan(ctx, planCtx, dirtyIDs, totalRecords, rowCount)

	return totalRecords, nil
}

// fetchAndRecordDirtyIDs fetches dirty row IDs from Postgres and records in execution plan.
func (e *DBFederatedQueryEngine) fetchAndRecordDirtyIDs(
	ctx context.Context,
	tables model.StorageTables,
	schemaID int16,
	planCtx *duckDBExecutionPlanContext,
) ([]uuid.UUID, error) {
	if tables.ChangeLog == "" {
		return nil, nil
	}
	if e == nil || e.dirtyIDFetcher == nil {
		return nil, fmt.Errorf("dirty id fetcher not available")
	}

	dirtyIDs, err := e.dirtyIDFetcher.FetchDirtyRowIDs(ctx, tables.ChangeLog, schemaID)
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
func (e *DBFederatedQueryEngine) buildDuckDBQueryWithPlan(
	ctx context.Context,
	tables model.StorageTables,
	q *model.FederatedAttributeQuery,
	dirtyIDs []uuid.UUID,
	attributeOrders []model.AttributeOrder,
	limit, offset int,
	planCtx *duckDBExecutionPlanContext,
) (string, []any, int64, error) {
	// Build template params
	changeLogSchema, changeLogScanTable := duckDBPostgresScanLocation(tables.ChangeLog)
	mainSchema, mainScanTable := duckDBPostgresScanLocation(tables.EntityMain)
	eavSchema, eavScanTable := duckDBPostgresScanLocation(tables.EAVData)
	sqlParams := map[string]any{
		"EAVTable":             sqlutil.SanitizeIdentifier(tables.EAVData),
		"MainTable":            sqlutil.SanitizeIdentifier(tables.EntityMain),
		"ChangeLogTable":       sqlutil.SanitizeIdentifier(tables.ChangeLog),
		"ChangeLogSchema":      changeLogSchema,
		"ChangeLogScanTable":   changeLogScanTable,
		"MainSchema":           mainSchema,
		"MainScanTable":        mainScanTable,
		"EAVSchema":            eavSchema,
		"EAVScanTable":         eavScanTable,
		"MainProjection":       model.EntityMainProjection,
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

	if connStr := e.pgConnString; connStr != "" {
		sqlParams["DuckDBPGConnString"] = connStr
	}
	if paths := duckDBParquetPathsForQuery(q); len(paths) > 0 {
		sqlParams["DuckDBS3Paths"] = paths
	}

	startTranslate := time.Now()

	// Build dual clauses (PG pushdown + DuckDB logical) if metadata cache available
	var cache forma.SchemaAttributeCache
	if e.metadataCache != nil {
		if c, ok := e.metadataCache.GetSchemaCacheByID(q.SchemaID); ok {
			cache = c
		}
	}
	paramIndex := 0
	dc, err := sqlgen.ToDualClauses(q.Condition, sqlutil.SanitizeIdentifier(tables.EAVData), q.SchemaID, cache, &paramIndex)
	if err != nil {
		return "", nil, 0, fmt.Errorf("to dual clauses: %w", err)
	}

	// Compute schema-driven projections for the template
	injectSchemaProjections(sqlParams, q.SchemaID, cache)

	// Determine if the EAV pivot can be skipped entirely (all filter/sort
	// attributes are column-bound, no EAV-only attributes needed).
	// Skip this optimization for benchmark schemas because their output schema
	// includes ALL attributes (both column-bound and EAV-only) unlike production
	// which outputs a fixed entity_main layout.
	if !isBenchmarkSchemaID(q.SchemaID) && !needsEAVJoin(q, cache) {
		sqlParams["HasEAVPivot"] = false
		sp, _ := sqlgen.BuildSchemaProjection(q.SchemaID, cache)
		if sp != nil {
			sqlParams["PGSourceSelect"] = sp.BuildPGSelectNoEAV()
			sqlParams["PGGroupBy"] = sp.BuildPGGroupByNoEAV()
		}
	}

	// Record Postgres pushdown fragment
	planCtx.recordPushdownFragment(dc.PgMainClause)

	// For benchmark schemas, the DuckDB logical WHERE clause must reference
	// attribute names (the S3 parquet has flat attribute columns), not entity_main
	// column names which ToDualClauses resolves from the schema cache.
	if isBenchmarkSchemaID(q.SchemaID) {
		dc.DuckClause = translateDuckClauseToBenchmark(dc.DuckClause, cache)
	}

	sqlStr, args, err := e.getDuckDBQueryBuilder()(e.getDuckDBTemplate(), sqlParams, q, dirtyIDs, &dc)
	translateMs := time.Since(startTranslate).Milliseconds()
	telemetry.EmitLatency(ctx, "translation", translateMs)
	if err != nil {
		return "", nil, 0, fmt.Errorf("build duckdb query: %w", err)
	}

	return sqlStr, args, translateMs, nil
}

func duckDBParquetPathsForQuery(q *model.FederatedAttributeQuery) []string {
	if q == nil || q.DuckDBHints == nil || q.DuckDBHints.S3ParquetPathTemplate == "" {
		return nil
	}
	rendered, err := sqlgen.RenderS3ParquetPath(q.DuckDBHints.S3ParquetPathTemplate, q.SchemaID)
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
		// Benchmark schemas: use hardcoded benchmarks projections that match
		// the benchmark parquet shape exactly (flat columns for column-bound
		// attributes, JSON extraction for EAV-only attributes).
		proj := sqlgen.BuildBenchmarkProjections(schemaID)
		sqlParams["S3SourceSelect"] = proj.S3SourceSelect
		sqlParams["PGSourceSelect"] = proj.PGSourceSelect
		sqlParams["PGGroupBy"] = proj.PGGroupBy
		sqlParams["EAVPivotSelect"] = proj.EAVPivotSelect
		sqlParams["EAVPivotAttrs"] = proj.EAVPivotAttrs
		sqlParams["HasEAVPivot"] = len(proj.EAVPivotAttrs) > 0
		sqlParams["OuterSelect"] = sqlgen.BuildBenchmarkOuterSelect(schemaID)
		return
	}
	if len(cache) == 0 {
		// No cache: fall back to defaults that match the fixed layout without EAV
		sqlParams["S3SourceSelect"] = "row_id, ltbase_created_at AS created_at, ltbase_updated_at AS ver_ts, ltbase_deleted_at AS deleted_ts, name, age, tag"
		sqlParams["PGSourceSelect"] = "m.ltbase_row_id::VARCHAR AS row_id, m.ltbase_created_at AS created_at, cl.changed_at AS ver_ts, cl.deleted_at AS deleted_ts, CAST(m.text_01 AS VARCHAR) AS name, CAST(m.integer_01 AS INTEGER) AS age, ''::VARCHAR AS tag"
		sqlParams["PGGroupBy"] = "m.ltbase_row_id, m.ltbase_created_at, cl.changed_at, cl.deleted_at, m.text_01, m.integer_01"
		sqlParams["HasEAVPivot"] = false
		sqlParams["OuterSelect"] = fallbackOuterSelect(schemaID)
		return
	}

	// Production schema: compute projections from the attribute cache
	sp, err := sqlgen.BuildSchemaProjection(schemaID, cache)
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
			tag AS text_02,
			NULL::SMALLINT AS smallint_01,
			NULL::INTEGER AS integer_02,
			NULL::BIGINT AS bigint_01,
			NULL::BIGINT AS bigint_02,
			NULL::BIGINT AS bigint_03,
			NULL::BIGINT AS bigint_04,
			NULL::DOUBLE AS double_01,
			NULL::DOUBLE AS double_02,
			NULL::BOOLEAN AS boolean_01,
			NULL::UUID AS uuid_01,
			NULL::VARCHAR AS text_03,
			NULL::VARCHAR AS text_04,
			NULL::VARCHAR AS text_05,
			'[]'::TEXT AS attributes_json`, schemaID)
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
func (e *DBFederatedQueryEngine) streamDuckDBRows(
	ctx context.Context,
	rows duckDBRowsIterator,
	rowHandler func(context.Context, *model.PersistentRecord) error,
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
		model.CleanupEmptyMaps(record)

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
func (e *DBFederatedQueryEngine) finalizeDuckDBExecutionPlan(
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

// translateDuckClauseToBenchmark rewrites a DuckDB WHERE clause from entity_main
// column references to benchmark attribute names used in S3 parquet flat columns.
func translateDuckClauseToBenchmark(clause string, cache forma.SchemaAttributeCache) string {
	result := clause
	for attr, meta := range cache {
		if meta.ColumnBinding == nil {
			continue
		}
		colName := string(meta.ColumnBinding.ColumnName)
		result = strings.ReplaceAll(result, colName, attr)
	}
	return result
}

// needsEAVJoin checks whether the federated query requires an EAV data JOIN.
// It returns false when all filter conditions and sort keys reference only
// column-bound attributes, meaning the eav_data scan can be safely skipped.
func needsEAVJoin(q *model.FederatedAttributeQuery, cache forma.SchemaAttributeCache) bool {
	if q == nil || len(cache) == 0 {
		return true
	}
	if needsEAVForCondition(q.Condition, cache) {
		return true
	}
	for _, order := range q.AttributeOrders {
		if order.StorageLocation != forma.AttributeStorageLocationMain {
			return true
		}
	}
	return false
}

// needsEAVForCondition recursively checks if any condition references an
// EAV-only (non-column-bound) attribute.
func needsEAVForCondition(cond forma.Condition, cache forma.SchemaAttributeCache) bool {
	if cond == nil {
		return false
	}
	switch c := cond.(type) {
	case *forma.CompositeCondition:
		for _, child := range c.Conditions {
			if needsEAVForCondition(child, cache) {
				return true
			}
		}
	case *forma.KvCondition:
		if meta, ok := cache[c.Attr]; ok {
			if meta.ColumnBinding == nil {
				return true
			}
		}
	}
	return false
}

func computePgRowCount(plan *model.ExecutionPlan, dirtyIDs []uuid.UUID) int64 {
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
