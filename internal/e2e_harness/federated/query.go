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

const (
	benchmarkSchemaIDCustomer int16 = 100
	benchmarkSchemaIDSecurity int16 = 101
	benchmarkSchemaIDTrade    int16 = 102
)

// ExecuteFederatedQuery executes a federated query using DuckDB.
func (h *FederatedTestHarness) ExecuteFederatedQuery(ctx context.Context, opts *QueryOptions) (*QueryResult, error) {
	opts = normalizeQueryOptions(opts)
	start := time.Now()
	benchmarkProjection := usesBenchmarkProjection(opts)
	if benchmarkProjection {
		if err := prepareBenchmarkDuckDBMacros(ctx, h); err != nil {
			return nil, err
		}
	}

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
	hotAttributeFilter := buildHotAttributeFilterClause(opts)
	benchmarkProjection := usesBenchmarkProjection(opts)
	pgConnStr := h.buildPGConnString()

	// Build tier queries dynamically
	var tierQueries []string

	if hasBase {
		baseQuery := buildParquetTierQuery(basePath, h.SchemaID, "base", dirtyExclusion, rowIDFilter, attributeFilter, benchmarkProjection)
		tierQueries = append(tierQueries, baseQuery)
	}

	if hasDelta {
		deltaQuery := buildParquetTierQuery(deltaPath, h.SchemaID, "delta", dirtyExclusion, rowIDFilter, attributeFilter, benchmarkProjection)
		tierQueries = append(tierQueries, deltaQuery)
	}

	// Always include hot buffer (Postgres)
	hotQuery := buildHotTierQuery(pgConnStr, h.SchemaID, rowIDFilter, hotAttributeFilter, benchmarkProjection)
	tierQueries = append(tierQueries, hotQuery)

	// Combine all tier queries with UNION ALL
	combinedQuery := strings.Join(tierQueries, "\n\t\t\tUNION ALL\n")
	return combinedQuery, benchmarkProjection
}

func buildParquetTierQuery(path string, schemaID int16, tier, dirtyExclusion, rowIDFilter, attributeFilter string, benchmarkProjection bool) string {
	if benchmarkProjection {
		projection := benchmarkParquetProjection(schemaID, tier, path)
		return fmt.Sprintf(`
			%s
			WHERE 1 = 1 %s %s %s`, projection, dirtyExclusion, rowIDFilter, attributeFilter)
	}
	return fmt.Sprintf(`
			SELECT row_id, schema_id, changed_at, deleted_at, name, version, '%s' as tier
			FROM read_parquet('%s')
			WHERE 1 = 1 %s %s`, tier, path, dirtyExclusion, rowIDFilter)
}

func benchmarkParquetProjection(schemaID int16, tier, path string) string {
	switch schemaID {
	case benchmarkSchemaIDCustomer:
		return fmt.Sprintf(`SELECT row_id, schema_id, changed_at, deleted_at, name, version, '' as symbol, '' as exchange, region, 0 as tradeType, 0 as tradeTime, '%s' as tier FROM read_parquet('%s')`, tier, path)
	case benchmarkSchemaIDSecurity:
		return fmt.Sprintf(`SELECT row_id, schema_id, changed_at, deleted_at, name, version, symbol, '' as exchange, '' as region, 0 as tradeType, 0 as tradeTime, '%s' as tier FROM read_parquet('%s')`, tier, path)
	default:
		return fmt.Sprintf(`SELECT row_id, schema_id, changed_at, deleted_at, name, version, symbol, exchange, region, tradeType, epoch_ms(tradeTime) as tradeTime, '%s' as tier FROM read_parquet('%s')`, tier, path)
	}
}

func buildHotTierQuery(pgConnStr string, schemaID int16, rowIDFilter, attributeFilter string, benchmarkProjection bool) string {
	if benchmarkProjection {
		return fmt.Sprintf(`
		SELECT 
			cl.row_id::VARCHAR as row_id,
			cl.schema_id,
			cl.changed_at,
			cl.deleted_at,
			benchmark_name(hot_vals.attributes) as name,
			0 as version,
			benchmark_text(hot_vals.attributes, 'symbol', em.text_01) as symbol,
			benchmark_text(hot_vals.attributes, 'exchange', '') as exchange,
			benchmark_text(hot_vals.attributes, 'region', em.text_02) as region,
			benchmark_int(hot_vals.attributes, 'tradeType', em.smallint_01) as tradeType,
			benchmark_bigint(hot_vals.attributes, 'tradeTime', em.bigint_02) as tradeTime,
			'hot' as tier
		FROM postgres_scan('%s', 'public', 'change_log') cl
		LEFT JOIN postgres_scan('%s', 'public', 'entity_main') em
			ON em.ltbase_schema_id = cl.schema_id AND em.ltbase_row_id::VARCHAR = cl.row_id::VARCHAR
		LEFT JOIN (
			SELECT row_id::VARCHAR as row_id, schema_id, map(list(attr_name), list(attr_value)) as attributes
			FROM (
				SELECT e.row_id, e.schema_id, benchmark_attr_name(e.schema_id, e.attr_id) as attr_name,
					COALESCE(e.value_text, CAST(CAST(e.value_numeric AS BIGINT) AS VARCHAR), '') as attr_value
				FROM postgres_scan('%s', 'public', 'eav_data') e
				WHERE benchmark_attr_name(e.schema_id, e.attr_id) <> ''
			)
			GROUP BY schema_id, row_id
		) hot_vals ON hot_vals.schema_id = cl.schema_id AND hot_vals.row_id = cl.row_id::VARCHAR
		WHERE cl.flushed_at = 0 
			AND cl.schema_id = %d
			%s
			%s`, pgConnStr, pgConnStr, pgConnStr, schemaID, rowIDFilter, attributeFilter)
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
			%s`, pgConnStr, schemaID, rowIDFilter)
}

func buildFinalFederatedSelect(combinedQuery string, opts *QueryOptions, benchmarkProjection bool) string {
	cte := buildFederatedDeduplicatedCTE(combinedQuery)
	if benchmarkProjection {
		return fmt.Sprintf(`
		%s
		SELECT row_id, schema_id, changed_at, deleted_at, name, version, symbol, exchange, region, tradeType, tradeTime
		FROM deduplicated
		WHERE rn = 1 AND (deleted_at = 0 OR deleted_at IS NULL)
		ORDER BY %s
		LIMIT %d OFFSET %d
	`, cte, buildOrderByClause(opts), opts.Limit, opts.Offset)
	}
	return fmt.Sprintf(`
		%s
		SELECT row_id, schema_id, changed_at, deleted_at, name, version
		FROM deduplicated
		WHERE rn = 1 AND (deleted_at = 0 OR deleted_at IS NULL)
		ORDER BY row_id
		LIMIT %d OFFSET %d
	`, cte, opts.Limit, opts.Offset)
}

func buildFinalFederatedCount(combinedQuery string) string {
	return fmt.Sprintf(`
		%s
		SELECT COUNT(*)
		FROM deduplicated
		WHERE rn = 1 AND (deleted_at = 0 OR deleted_at IS NULL)
	`, buildFederatedDeduplicatedCTE(combinedQuery))
}

func buildFederatedDeduplicatedCTE(combinedQuery string) string {
	return fmt.Sprintf(`
		WITH combined AS (
			%s
		),
		deduplicated AS (
			SELECT *, ROW_NUMBER() OVER (
				PARTITION BY row_id
				ORDER BY changed_at DESC,
					CASE tier WHEN 'hot' THEN 3 WHEN 'delta' THEN 2 WHEN 'base' THEN 1 ELSE 0 END DESC,
					version DESC,
					deleted_at DESC,
					row_id ASC
			) as rn
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

func benchmarkAttrNameSQLCase() string {
	type attrKey struct {
		schemaID int16
		name     string
	}
	mapping := map[attrKey]int{
		{benchmarkSchemaIDTrade, "symbol"}:    benchmarkAttributeID(benchmarkSchemaIDTrade, "symbol"),
		{benchmarkSchemaIDTrade, "exchange"}:  benchmarkAttributeID(benchmarkSchemaIDTrade, "exchange"),
		{benchmarkSchemaIDTrade, "region"}:    benchmarkAttributeID(benchmarkSchemaIDTrade, "region"),
		{benchmarkSchemaIDTrade, "tradeType"}: benchmarkAttributeID(benchmarkSchemaIDTrade, "tradeType"),
		{benchmarkSchemaIDTrade, "tradeTime"}: benchmarkAttributeID(benchmarkSchemaIDTrade, "tradeTime"),
		{benchmarkSchemaIDTrade, "name"}:      benchmarkAttributeID(benchmarkSchemaIDTrade, "name"),
		{benchmarkSchemaIDCustomer, "region"}: benchmarkAttributeID(benchmarkSchemaIDCustomer, "region"),
		{benchmarkSchemaIDCustomer, "name"}:   benchmarkAttributeID(benchmarkSchemaIDCustomer, "name"),
		{benchmarkSchemaIDSecurity, "symbol"}: benchmarkAttributeID(benchmarkSchemaIDSecurity, "symbol"),
		{benchmarkSchemaIDSecurity, "name"}:   benchmarkAttributeID(benchmarkSchemaIDSecurity, "companyName"),
	}
	parts := make([]string, 0, len(mapping))
	for key, attrID := range mapping {
		parts = append(parts, fmt.Sprintf("WHEN schema_id = %d AND attr_id = %d THEN '%s'", key.schemaID, attrID, key.name))
	}
	return strings.Join(parts, " ")
}

func benchmarkAttributeID(schemaID int16, name string) int {
	hash := uint32(2166136261)
	input := fmt.Sprintf("%d:%s", schemaID, name)
	for i := 0; i < len(input); i++ {
		hash ^= uint32(input[i])
		hash *= 16777619
	}
	return int(hash%30000) + 1
}

func benchmarkFunctionsSQL() string {
	return fmt.Sprintf(`
		CREATE OR REPLACE MACRO benchmark_attr_name(schema_id, attr_id) AS (
			CASE %s ELSE '' END
		);
		CREATE OR REPLACE MACRO benchmark_text(attr_map, attr_name, fallback_value) AS (
			COALESCE(CAST(element_at(attr_map, attr_name) AS VARCHAR), fallback_value)
		);
		CREATE OR REPLACE MACRO benchmark_name(attr_map) AS (
			COALESCE(CAST(element_at(attr_map, 'name') AS VARCHAR), CAST(element_at(attr_map, 'symbol') AS VARCHAR), '')
		);
		CREATE OR REPLACE MACRO benchmark_int(attr_map, attr_name, fallback_value) AS (
			COALESCE(TRY_CAST(element_at(attr_map, attr_name) AS INTEGER), fallback_value)
		);
		CREATE OR REPLACE MACRO benchmark_bigint(attr_map, attr_name, fallback_value) AS (
			COALESCE(TRY_CAST(element_at(attr_map, attr_name) AS BIGINT), fallback_value)
		);
	`, benchmarkAttrNameSQLCase())
}

func prepareBenchmarkDuckDBMacros(ctx context.Context, h *FederatedTestHarness) error {
	_, err := h.Duck.DB.ExecContext(ctx, benchmarkFunctionsSQL())
	if err != nil {
		return fmt.Errorf("prepare benchmark duckdb macros: %w", err)
	}
	return nil
}

func buildHotAttributeFilterClause(opts *QueryOptions) string {
	if opts == nil || opts.Filter == nil || len(opts.Filter.Conditions) == 0 {
		return ""
	}
	parts := make([]string, 0, len(opts.Filter.Conditions))
	for key, value := range opts.Filter.Conditions {
		expression := benchmarkHotFilterExpression(key)
		if expression == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("AND %s = %s", expression, benchmarkSQLLiteral(value)))
	}
	return strings.Join(parts, " ")
}

func benchmarkHotFilterExpression(attribute string) string {
	switch attribute {
	case "symbol":
		return "benchmark_text(hot_vals.attributes, 'symbol', em.text_01)"
	case "exchange":
		return "benchmark_text(hot_vals.attributes, 'exchange', '')"
	case "region":
		return "benchmark_text(hot_vals.attributes, 'region', em.text_02)"
	case "tradeType":
		return "benchmark_int(hot_vals.attributes, 'tradeType', em.smallint_01)"
	case "tradeTime":
		return "benchmark_bigint(hot_vals.attributes, 'tradeTime', em.bigint_02)"
	default:
		return ""
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
