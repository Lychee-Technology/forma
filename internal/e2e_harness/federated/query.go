package federated

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal"
)

// ExecuteFederatedQuery executes a federated query using DuckDB.
func (h *FederatedTestHarness) ExecuteFederatedQuery(ctx context.Context, opts *QueryOptions) (*QueryResult, error) {
	opts = normalizeQueryOptions(opts)
	start := time.Now()
	benchmarkProjection := usesBenchmarkProjection(opts)

	// Check which tiers have parquet files
	hasBaseFiles, hasDeltaFiles, err := h.checkTierFiles(ctx)
	if err != nil {
		return nil, err
	}

	// Get dirty IDs from change_log
	dirtyIDs, err := h.getDirtyIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("get dirty ids: %w", err)
	}

	// If no parquet files exist and no hot records, fall back to Postgres-only query
	if !hasBaseFiles && !hasDeltaFiles && len(dirtyIDs) == 0 {
		return h.ExecutePostgresQuery(ctx, opts)
	}

	// Build the S3 paths
	basePath := fmt.Sprintf("s3://%s/%s/%d/base/*.parquet", h.S3Bucket, h.S3Prefix, h.SchemaID)
	deltaPath := fmt.Sprintf("s3://%s/%s/%d/delta/*.parquet", h.S3Bucket, h.S3Prefix, h.SchemaID)

	// Build and execute the federated query
	query := h.buildFederatedQuerySQLDynamic(basePath, deltaPath, hasBaseFiles, hasDeltaFiles, dirtyIDs, opts)
	countQuery := h.buildFederatedQueryCountSQLDynamic(basePath, deltaPath, hasBaseFiles, hasDeltaFiles, dirtyIDs, opts)

	var totalRecords int64
	if err := h.Duck.DB.QueryRowContext(ctx, countQuery).Scan(&totalRecords); err != nil {
		if isFederatedTierFileError(err) {
			return h.ExecutePostgresQuery(ctx, opts)
		}
		return nil, fmt.Errorf("count query: %w", err)
	}
	if opts.CountOnly {
		return &QueryResult{TotalRecords: totalRecords, Duration: time.Since(start), Plan: buildExecutionPlan(len(dirtyIDs), hasBaseFiles, hasDeltaFiles, time.Since(start))}, nil
	}

	rows, err := h.Duck.DB.QueryContext(ctx, query)
	if err != nil {
		if isFederatedTierFileError(err) {
			return h.ExecutePostgresQuery(ctx, opts)
		}
		return nil, fmt.Errorf("execute query: %w", err)
	}
	defer rows.Close()

	records, err := h.scanQueryResults(rows, benchmarkProjection)
	if err != nil {
		return nil, err
	}

	duration := time.Since(start)
	plan := buildExecutionPlan(len(dirtyIDs), hasBaseFiles, hasDeltaFiles, duration)

	return &QueryResult{
		Records:      records,
		TotalRecords: totalRecords,
		Duration:     duration,
		Plan:         plan,
	}, nil
}

// normalizeQueryOptions sets default values for query options.
func normalizeQueryOptions(opts *QueryOptions) *QueryOptions {
	if opts == nil {
		return &QueryOptions{Limit: 100}
	}
	if opts.Limit == 0 {
		opts.Limit = 100
	}
	return opts
}

// checkTierFiles checks which tiers have parquet files.
func (h *FederatedTestHarness) checkTierFiles(ctx context.Context) (hasBase, hasDelta bool, err error) {
	baseFiles, err := h.ListParquetFiles(ctx, "base")
	if err != nil {
		return false, false, fmt.Errorf("list base files: %w", err)
	}
	deltaFiles, err := h.ListParquetFiles(ctx, "delta")
	if err != nil {
		return false, false, fmt.Errorf("list delta files: %w", err)
	}
	return len(baseFiles) > 0, len(deltaFiles) > 0, nil
}

// scanQueryResults scans DuckDB query rows into PersistentRecords.
func (h *FederatedTestHarness) scanQueryResults(rows *sql.Rows, benchmarkProjection bool) ([]*internal.PersistentRecord, error) {
	var records []*internal.PersistentRecord
	for rows.Next() {
		var rowID string
		var schemaID int16
		var changedAt, deletedAt int64
		var name sql.NullString
		var version sql.NullInt64
		var symbol, exchange, region sql.NullString
		var tradeType, tradeTime sql.NullInt64

		if benchmarkProjection {
			if err := rows.Scan(&rowID, &schemaID, &changedAt, &deletedAt, &name, &version, &symbol, &exchange, &region, &tradeType, &tradeTime); err != nil {
				return nil, fmt.Errorf("scan row: %w", err)
			}
		} else {
			if err := rows.Scan(&rowID, &schemaID, &changedAt, &deletedAt, &name, &version); err != nil {
				return nil, fmt.Errorf("scan row: %w", err)
			}
		}

		rec := &internal.PersistentRecord{
			RowID:        uuid.MustParse(rowID),
			SchemaID:     schemaID,
			CreatedAt:    changedAt,
			UpdatedAt:    changedAt,
			TextItems:    make(map[string]string),
			Float64Items: make(map[string]float64),
		}
		if deletedAt > 0 {
			rec.DeletedAt = &deletedAt
		}
		if name.Valid {
			rec.TextItems["name"] = name.String
		}
		if benchmarkProjection {
			if symbol.Valid {
				rec.TextItems["symbol"] = symbol.String
			}
			if exchange.Valid {
				rec.TextItems["exchange"] = exchange.String
			}
			if region.Valid {
				rec.TextItems["region"] = region.String
			}
		}
		if version.Valid {
			rec.Float64Items["version"] = float64(version.Int64)
		}
		if benchmarkProjection && (tradeType.Valid || tradeTime.Valid) {
			rec.Int64Items = make(map[string]int64)
			if tradeType.Valid {
				rec.Int64Items["tradeType"] = tradeType.Int64
			}
			if tradeTime.Valid {
				rec.Int64Items["tradeTime"] = tradeTime.Int64
			}
		}

		records = append(records, rec)
	}
	return records, nil
}

// buildExecutionPlan creates an execution plan with tier and timing info.
func buildExecutionPlan(dirtyIDCount int, hasBase, hasDelta bool, duration time.Duration) *internal.ExecutionPlan {
	planNotes := []string{fmt.Sprintf("dirty_ids_excluded:%d", dirtyIDCount)}
	if hasBase {
		planNotes = append(planNotes, "base_files_scanned")
	}
	if hasDelta {
		planNotes = append(planNotes, "delta_files_scanned")
	}
	planNotes = append(planNotes, "hot_buffer_scanned")

	return &internal.ExecutionPlan{
		Notes: planNotes,
		Timings: map[string]int64{
			"total": duration.Milliseconds(),
		},
	}
}

func isFederatedTierFileError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "No files found") || strings.Contains(err.Error(), "does not exist")
}

// buildFederatedQuerySQLDynamic builds the federated query SQL, only including tiers that have files.
func (h *FederatedTestHarness) buildFederatedQuerySQLDynamic(basePath, deltaPath string, hasBase, hasDelta bool, dirtyIDs []uuid.UUID, opts *QueryOptions) string {
	combinedQuery, benchmarkProjection := h.buildFederatedCombinedQuery(basePath, deltaPath, hasBase, hasDelta, dirtyIDs, opts)
	return buildFinalFederatedSelect(combinedQuery, opts, benchmarkProjection)
}

func (h *FederatedTestHarness) buildFederatedQueryCountSQLDynamic(basePath, deltaPath string, hasBase, hasDelta bool, dirtyIDs []uuid.UUID, opts *QueryOptions) string {
	combinedQuery, _ := h.buildFederatedCombinedQuery(basePath, deltaPath, hasBase, hasDelta, dirtyIDs, opts)
	return buildFinalFederatedCount(combinedQuery)
}

func (h *FederatedTestHarness) buildFederatedCombinedQuery(basePath, deltaPath string, hasBase, hasDelta bool, dirtyIDs []uuid.UUID, opts *QueryOptions) (string, bool) {
	dirtyExclusion := buildDirtyExclusion(dirtyIDs)
	rowIDFilter := buildRowIDFilter(opts)
	attributeFilter := buildAttributeFilterClause(opts)
	benchmarkProjection := usesBenchmarkProjection(opts)
	pgConnStr := h.buildPGConnString()

	// Build tier queries dynamically
	var tierQueries []string

	if hasBase {
		baseQuery := buildParquetTierQuery(basePath, "base", dirtyExclusion, rowIDFilter, attributeFilter, benchmarkProjection)
		tierQueries = append(tierQueries, baseQuery)
	}

	if hasDelta {
		deltaQuery := buildParquetTierQuery(deltaPath, "delta", dirtyExclusion, rowIDFilter, attributeFilter, benchmarkProjection)
		tierQueries = append(tierQueries, deltaQuery)
	}

	// Always include hot buffer (Postgres)
	hotQuery := buildHotTierQuery(pgConnStr, h.SchemaID, rowIDFilter, attributeFilter, benchmarkProjection)
	tierQueries = append(tierQueries, hotQuery)

	// Combine all tier queries with UNION ALL
	combinedQuery := strings.Join(tierQueries, "\n\t\t\tUNION ALL\n")
	return combinedQuery, benchmarkProjection
}

func buildParquetTierQuery(path, tier, dirtyExclusion, rowIDFilter, attributeFilter string, benchmarkProjection bool) string {
	if benchmarkProjection {
		return fmt.Sprintf(`
			SELECT row_id, schema_id, changed_at, deleted_at, name, version, symbol, exchange, region, tradeType, epoch_ms(tradeTime) as tradeTime, '%s' as tier
			FROM read_parquet('%s')
			WHERE deleted_at = 0 %s %s %s`, tier, path, dirtyExclusion, rowIDFilter, attributeFilter)
	}
	return fmt.Sprintf(`
			SELECT row_id, schema_id, changed_at, deleted_at, name, version, '%s' as tier
			FROM read_parquet('%s')
			WHERE deleted_at = 0 %s %s`, tier, path, dirtyExclusion, rowIDFilter)
}

func buildHotTierQuery(pgConnStr string, schemaID int16, rowIDFilter, attributeFilter string, benchmarkProjection bool) string {
	if benchmarkProjection {
		return fmt.Sprintf(`
		SELECT 
			cl.row_id::VARCHAR as row_id,
			cl.schema_id,
			cl.changed_at,
			cl.deleted_at,
			COALESCE(em.text_01, '') as name,
			0 as version,
			COALESCE(em.text_01, '') as symbol,
			'' as exchange,
			COALESCE(em.text_02, '') as region,
			COALESCE(em.smallint_01, 0) as tradeType,
			COALESCE(em.bigint_02, 0) as tradeTime,
			'hot' as tier
		FROM postgres_scan('%s', 'public', 'change_log') cl
		LEFT JOIN postgres_scan('%s', 'public', 'entity_main') em
			ON em.ltbase_schema_id = cl.schema_id AND em.ltbase_row_id::VARCHAR = cl.row_id::VARCHAR
		WHERE cl.flushed_at = 0 
			AND cl.schema_id = %d
			AND (cl.deleted_at = 0 OR cl.deleted_at IS NULL)
			%s
			%s`, pgConnStr, pgConnStr, schemaID, rowIDFilter, attributeFilter)
	}
	return fmt.Sprintf(`
		SELECT 
			cl.row_id::VARCHAR as row_id,
			cl.schema_id,
			cl.changed_at,
			cl.deleted_at,
			'' as name,
			0 as version,
			'hot' as tier
		FROM postgres_scan('%s', 'public', 'change_log') cl
		WHERE cl.flushed_at = 0 
			AND cl.schema_id = %d
			AND (cl.deleted_at = 0 OR cl.deleted_at IS NULL)
			%s`, pgConnStr, schemaID, rowIDFilter)
}

func buildFinalFederatedSelect(combinedQuery string, opts *QueryOptions, benchmarkProjection bool) string {
	cte := buildFederatedDeduplicatedCTE(combinedQuery)
	if benchmarkProjection {
		return fmt.Sprintf(`
		%s
		SELECT row_id, schema_id, changed_at, deleted_at, name, version, symbol, exchange, region, tradeType, tradeTime
		FROM deduplicated
		WHERE rn = 1
		ORDER BY %s
		LIMIT %d OFFSET %d
	`, cte, buildOrderByClause(opts), opts.Limit, opts.Offset)
	}
	return fmt.Sprintf(`
		%s
		SELECT row_id, schema_id, changed_at, deleted_at, name, version
		FROM deduplicated
		WHERE rn = 1
		ORDER BY row_id
		LIMIT %d OFFSET %d
	`, cte, opts.Limit, opts.Offset)
}

func buildFinalFederatedCount(combinedQuery string) string {
	return fmt.Sprintf(`
		%s
		SELECT COUNT(*)
		FROM deduplicated
		WHERE rn = 1
	`, buildFederatedDeduplicatedCTE(combinedQuery))
}

func buildFederatedDeduplicatedCTE(combinedQuery string) string {
	return fmt.Sprintf(`
		WITH combined AS (
			%s
		),
		deduplicated AS (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY row_id ORDER BY changed_at DESC) as rn
			FROM combined
		)
	`, combinedQuery)
}

func usesBenchmarkProjection(opts *QueryOptions) bool {
	if opts == nil {
		return false
	}
	if opts.SortBy != "" && opts.SortBy != "row_id" {
		return true
	}
	if opts.Filter == nil {
		return false
	}
	for key := range opts.Filter.Conditions {
		if benchmarkQueryColumn(key) != "" {
			return true
		}
	}
	return false
}

// buildDirtyExclusion builds the dirty ID exclusion clause.
func buildDirtyExclusion(dirtyIDs []uuid.UUID) string {
	if len(dirtyIDs) == 0 {
		return ""
	}
	ids := make([]string, len(dirtyIDs))
	for i, id := range dirtyIDs {
		ids[i] = fmt.Sprintf("'%s'", id.String())
	}
	return fmt.Sprintf("AND row_id NOT IN (%s)", strings.Join(ids, ","))
}

// buildRowIDFilter builds the row ID filter clause.
func buildRowIDFilter(opts *QueryOptions) string {
	if opts.Filter != nil && opts.Filter.RowID != uuid.Nil {
		return fmt.Sprintf("AND row_id = '%s'", opts.Filter.RowID.String())
	}
	return ""
}

func buildAttributeFilterClause(opts *QueryOptions) string {
	if opts == nil || opts.Filter == nil || len(opts.Filter.Conditions) == 0 {
		return ""
	}
	parts := make([]string, 0, len(opts.Filter.Conditions))
	for key, value := range opts.Filter.Conditions {
		column := benchmarkQueryColumn(key)
		if column == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("AND %s = %s", column, benchmarkSQLLiteral(value)))
	}
	return strings.Join(parts, " ")
}

func benchmarkQueryColumn(attribute string) string {
	switch attribute {
	case "symbol":
		return "symbol"
	case "exchange":
		return "exchange"
	case "region":
		return "region"
	case "tradeType":
		return "tradeType"
	case "tradeTime":
		return "tradeTime"
	default:
		return ""
	}
}

func benchmarkSQLLiteral(value any) string {
	switch v := value.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
	case int:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%v", v)
	default:
		return fmt.Sprintf("'%v'", v)
	}
}

func buildOrderByClause(opts *QueryOptions) string {
	if opts == nil {
		return "row_id ASC"
	}
	column := benchmarkQueryColumn(opts.SortBy)
	if column == "" {
		column = "row_id"
	}
	direction := "ASC"
	if opts.SortDesc {
		direction = "DESC"
	}
	return fmt.Sprintf("%s %s, row_id ASC", column, direction)
}

// buildPGConnString builds the Postgres connection string for DuckDB.
func (h *FederatedTestHarness) buildPGConnString() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s",
		h.PGHost, h.PGPort, "postgres", "password", "postgres")
}

// getDirtyIDs fetches unflushed row IDs from change_log.
func (h *FederatedTestHarness) getDirtyIDs(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := h.PGDB.QueryContext(ctx, `
		SELECT row_id FROM change_log 
		WHERE schema_id = $1 AND flushed_at = 0
	`, h.SchemaID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// ExecutePostgresQuery executes a direct Postgres query (no DuckDB).
func (h *FederatedTestHarness) ExecutePostgresQuery(ctx context.Context, opts *QueryOptions) (*QueryResult, error) {
	opts = normalizeQueryOptions(opts)
	start := time.Now()
	if opts.CountOnly {
		var total int64
		if err := h.PGDB.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM entity_main 
			WHERE ltbase_schema_id = $1 AND (ltbase_deleted_at IS NULL OR ltbase_deleted_at = 0)
		`, h.SchemaID).Scan(&total); err != nil {
			return nil, err
		}
		return &QueryResult{TotalRecords: total, Duration: time.Since(start)}, nil
	}

	query := `
		SELECT 
			em.ltbase_row_id,
			em.ltbase_schema_id,
			em.ltbase_created_at,
			em.ltbase_deleted_at
		FROM entity_main em
		WHERE em.ltbase_schema_id = $1
			AND (em.ltbase_deleted_at IS NULL OR em.ltbase_deleted_at = 0)
		ORDER BY em.ltbase_created_at DESC
		LIMIT $2 OFFSET $3
	`

	rows, err := h.PGDB.QueryContext(ctx, query, h.SchemaID, opts.Limit, opts.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []*internal.PersistentRecord
	for rows.Next() {
		var rowID uuid.UUID
		var schemaID int16
		var createdAt int64
		var deletedAt sql.NullInt64

		if err := rows.Scan(&rowID, &schemaID, &createdAt, &deletedAt); err != nil {
			return nil, err
		}

		rec := &internal.PersistentRecord{
			RowID:     rowID,
			SchemaID:  schemaID,
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
		}
		if deletedAt.Valid && deletedAt.Int64 > 0 {
			rec.DeletedAt = &deletedAt.Int64
		}

		records = append(records, rec)
	}

	// Get total count
	var total int64
	_ = h.PGDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM entity_main 
		WHERE ltbase_schema_id = $1 AND (ltbase_deleted_at IS NULL OR ltbase_deleted_at = 0)
	`, h.SchemaID).Scan(&total)

	return &QueryResult{
		Records:      records,
		TotalRecords: total,
		Duration:     time.Since(start),
	}, nil
}

// StreamFederatedQuery streams query results with a handler callback.
func (h *FederatedTestHarness) StreamFederatedQuery(ctx context.Context, opts *QueryOptions, handler func(*internal.PersistentRecord) error) error {
	result, err := h.ExecuteFederatedQuery(ctx, opts)
	if err != nil {
		return err
	}

	for _, rec := range result.Records {
		if err := handler(rec); err != nil {
			return err
		}
	}
	return nil
}
