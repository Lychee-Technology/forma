package internal

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

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

	dirtyIDs, err := r.FetchDirtyRowIDs(ctx, tables.ChangeLog, schemaID)
	if err != nil {
		return nil, fmt.Errorf("fetch dirty ids: %w", err)
	}

	// Emit metric for dirty set size
	EmitRowCount(ctx, "pg", int64(len(dirtyIDs)))

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
	sqlParams := map[string]any{
		"EAVTable":             sanitizeIdentifier(tables.EAVData),
		"MainTable":            sanitizeIdentifier(tables.EntityMain),
		"ChangeLogTable":       sanitizeIdentifier(tables.ChangeLog),
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

	// Record Postgres pushdown fragment
	planCtx.recordPushdownFragment(dc.PgMainClause)

	sqlStr, args, err := BuildDuckDBQuery(AdvancedQueryTemplateDuckDB, sqlParams, q, dirtyIDs, &dc)
	translateMs := time.Since(startTranslate).Milliseconds()
	EmitLatency(ctx, "translation", translateMs)
	if err != nil {
		return "", nil, 0, fmt.Errorf("build duckdb query: %w", err)
	}

	return sqlStr, args, translateMs, nil
}

// streamDuckDBRows iterates through DuckDB rows and invokes the handler.
func (r *DBPersistentRecordRepository) streamDuckDBRows(
	ctx context.Context,
	rows *sql.Rows,
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
	EmitLatency(ctx, "execution", qMs)
	streamMs := time.Since(planCtx.startQuery).Milliseconds() - qMs
	if streamMs < 0 {
		streamMs = 0
	}
	EmitLatency(ctx, "streaming", streamMs)
	EmitRowCount(ctx, "duckdb", rowCount)

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
	EmitPushdownEfficiency(ctx, 0, ratio) // schemaID not available here, use 0

	planCtx.opts.ExecutionPlan.Notes = append(planCtx.opts.ExecutionPlan.Notes,
		fmt.Sprintf("pushdown_efficiency=%.3f (pg_rows=%d final_rows=%d)", ratio, pgRows, finalRows))
}

// computePgRowCount calculates the Postgres row count from execution plan sources.
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
	// Fallback: use dirtyIDs size as a proxy
	if pgRows == 0 {
		pgRows = int64(len(dirtyIDs))
	}
	return pgRows
}
