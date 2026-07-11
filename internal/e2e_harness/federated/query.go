package federated

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
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
	if opts.PreferHot {
		result, err := h.ExecutePostgresQuery(ctx, opts)
		if err != nil {
			return nil, err
		}
		if result.Plan == nil {
			result.Plan = &model.ExecutionPlan{Notes: []string{}, Timings: map[string]int64{}}
		}
		result.Plan.Notes = append(result.Plan.Notes, "prefer_hot_override", "postgres_only_execution")
		result.Plan.Timings["total"] = time.Since(start).Milliseconds()
		result.Duration = time.Since(start)
		return result, nil
	}
	benchmarkProjection := usesBenchmarkProjectionForSelect(opts)
	tradeTimeOnlyProjection := usesTradeTimeOnlyBenchmarkProjectionForSelect(opts)
	if needsBenchmarkDuckDBMacros(opts, benchmarkProjection, tradeTimeOnlyProjection) {
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
	if shouldSkipFederatedSelect(totalRecords, opts.Offset) {
		plan := buildExecutionPlan(len(dirtyIDs), hasBaseFiles, hasDeltaFiles, time.Since(start))
		plan.Notes = append(plan.Notes, "empty_page_short_circuit")
		return &QueryResult{
			Records:      nil,
			TotalRecords: totalRecords,
			Duration:     time.Since(start),
			Plan:         plan,
		}, nil
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
func (h *FederatedTestHarness) scanQueryResults(rows *sql.Rows, benchmarkProjection bool) ([]*model.PersistentRecord, error) {
	var records []*model.PersistentRecord
	for rows.Next() {
		var rowID string
		var schemaID int16
		var changedAt, deletedAt int64
		var name sql.NullString
		var version sql.NullInt64
		var symbol, exchange, region sql.NullString
		var tradeType sql.NullInt64
		var tradeTime any

		if benchmarkProjection {
			if err := rows.Scan(&rowID, &schemaID, &changedAt, &deletedAt, &name, &version, &symbol, &exchange, &region, &tradeType, &tradeTime); err != nil {
				return nil, fmt.Errorf("scan row: %w", err)
			}
		} else {
			if err := rows.Scan(&rowID, &schemaID, &changedAt, &deletedAt, &name, &version); err != nil {
				return nil, fmt.Errorf("scan row: %w", err)
			}
		}

		rec := &model.PersistentRecord{
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
		normalizedTradeTime, tradeTimeOK := normalizeBenchmarkTradeTimeValue(tradeTime)
		if benchmarkProjection && (tradeType.Valid || tradeTimeOK) {
			rec.Int64Items = make(map[string]int64)
			if tradeType.Valid {
				rec.Int64Items["tradeType"] = tradeType.Int64
			}
			if tradeTimeOK {
				rec.Int64Items["tradeTime"] = normalizedTradeTime
			}
		}

		records = append(records, rec)
	}
	return records, nil
}

func normalizeBenchmarkTradeTimeValue(value any) (int64, bool) {
	switch v := value.(type) {
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case int:
		return int64(v), true
	case float64:
		return int64(v), true
	case time.Time:
		return v.UnixMilli(), true
	case sql.NullInt64:
		if v.Valid {
			return v.Int64, true
		}
	case sql.NullTime:
		if v.Valid {
			return v.Time.UnixMilli(), true
		}
	case string:
		if unixMillis, err := strconv.ParseInt(v, 10, 64); err == nil {
			return unixMillis, true
		}
		if parsed, err := time.Parse(time.RFC3339, v); err == nil {
			return parsed.UnixMilli(), true
		}
	case []byte:
		return normalizeBenchmarkTradeTimeValue(string(v))
	}
	return 0, false
}

// buildExecutionPlan creates an execution plan with tier and timing info.
func buildExecutionPlan(dirtyIDCount int, hasBase, hasDelta bool, duration time.Duration) *model.ExecutionPlan {
	planNotes := []string{fmt.Sprintf("dirty_ids_excluded:%d", dirtyIDCount)}
	if hasBase {
		planNotes = append(planNotes, "base_files_scanned")
	}
	if hasDelta {
		planNotes = append(planNotes, "delta_files_scanned")
	}
	planNotes = append(planNotes, "hot_buffer_scanned")

	return &model.ExecutionPlan{
		Notes: planNotes,
		Timings: map[string]int64{
			"total": duration.Milliseconds(),
		},
	}
}

func shouldSkipFederatedSelect(totalRecords int64, offset int) bool {
	if totalRecords <= 0 {
		return true
	}
	if offset < 0 {
		return false
	}
	return int64(offset) >= totalRecords
}

func isFederatedTierFileError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "No files found") || strings.Contains(err.Error(), "does not exist")
}

// buildFederatedQuerySQLDynamic builds the federated query SQL, only including tiers that have files.
func (h *FederatedTestHarness) buildFederatedQuerySQLDynamic(basePath, deltaPath string, hasBase, hasDelta bool, dirtyIDs []uuid.UUID, opts *QueryOptions) string {
	benchmarkProjection := usesBenchmarkProjectionForSelect(opts)
	combinedQuery := h.buildFederatedCombinedQuery(basePath, deltaPath, hasBase, hasDelta, dirtyIDs, opts, benchmarkProjection, usesTradeTimeOnlyBenchmarkProjectionForSelect(opts))
	return buildFinalFederatedSelect(combinedQuery, opts, benchmarkProjection)
}

func (h *FederatedTestHarness) buildFederatedQueryCountSQLDynamic(basePath, deltaPath string, hasBase, hasDelta bool, dirtyIDs []uuid.UUID, opts *QueryOptions) string {
	combinedQuery := h.buildFederatedCombinedQuery(basePath, deltaPath, hasBase, hasDelta, dirtyIDs, opts, usesBenchmarkProjectionForCount(opts), false)
	return buildFinalFederatedCount(combinedQuery)
}

func (h *FederatedTestHarness) buildFederatedCombinedQuery(basePath, deltaPath string, hasBase, hasDelta bool, dirtyIDs []uuid.UUID, opts *QueryOptions, benchmarkProjection, tradeTimeOnlyProjection bool) string {
	dirtyExclusion := buildDirtyExclusion(dirtyIDs)
	rowIDFilter := buildRowIDFilter(opts)
	hotRowIDFilter := buildHotRowIDFilter(opts)
	attributeFilter := buildAttributeFilterClause(opts)
	timeWindowFilter := buildTradeTimeFilterClause(opts)
	hotAttributeFilter := buildHotAttributeFilterClauseTargeted(opts)
	hotTimeWindowFilter := buildHotTradeTimeFilterClauseTargeted(opts)
	pgConnStr := h.buildPGConnString()

	// Build tier queries dynamically
	var tierQueries []string

	if hasBase {
		baseQuery := buildParquetTierQuery(basePath, h.SchemaID, "base", dirtyExclusion, rowIDFilter, attributeFilter, timeWindowFilter, benchmarkProjection, tradeTimeOnlyProjection)
		tierQueries = append(tierQueries, baseQuery)
	}

	if hasDelta {
		deltaQuery := buildParquetTierQuery(deltaPath, h.SchemaID, "delta", dirtyExclusion, rowIDFilter, attributeFilter, timeWindowFilter, benchmarkProjection, tradeTimeOnlyProjection)
		tierQueries = append(tierQueries, deltaQuery)
	}

	// Always include hot buffer (Postgres)
	hotQuery := h.buildHotTierQuery(pgConnStr, h.SchemaID, hotRowIDFilter, hotAttributeFilter, hotTimeWindowFilter, benchmarkProjection, tradeTimeOnlyProjection)
	tierQueries = append(tierQueries, hotQuery)

	// Combine all tier queries with UNION ALL
	combinedQuery := strings.Join(tierQueries, "\n\t\t\tUNION ALL\n")
	return combinedQuery
}

func buildParquetTierQuery(path string, schemaID int16, tier, dirtyExclusion, rowIDFilter, attributeFilter, timeWindowFilter string, benchmarkProjection, tradeTimeOnlyProjection bool) string {
	if benchmarkProjection {
		projection := benchmarkParquetProjection(schemaID, tier, path, tradeTimeOnlyProjection)
		return fmt.Sprintf(`
			%s
			WHERE 1 = 1 %s %s %s %s`, projection, dirtyExclusion, rowIDFilter, attributeFilter, timeWindowFilter)
	}
	return fmt.Sprintf(`
			SELECT row_id, schema_id, changed_at, deleted_at, name, version, '%s' as tier
			FROM read_parquet('%s')
			WHERE 1 = 1 %s %s %s`, tier, path, dirtyExclusion, rowIDFilter, timeWindowFilter)
}

func benchmarkParquetProjection(schemaID int16, tier, path string, tradeTimeOnlyProjection bool) string {
	switch schemaID {
	case benchmarkSchemaIDCustomer:
		return fmt.Sprintf(`SELECT row_id, schema_id, changed_at, deleted_at, name, version, '' as symbol, '' as exchange, region, 0 as tradeType, 0 as tradeTime, '%s' as tier FROM read_parquet('%s')`, tier, path)
	case benchmarkSchemaIDSecurity:
		return fmt.Sprintf(`SELECT row_id, schema_id, changed_at, deleted_at, name, version, symbol, '' as exchange, '' as region, 0 as tradeType, 0 as tradeTime, '%s' as tier FROM read_parquet('%s')`, tier, path)
	default:
		if tradeTimeOnlyProjection {
			return fmt.Sprintf(`SELECT row_id, schema_id, changed_at, deleted_at, '' as name, version, '' as symbol, '' as exchange, '' as region, 0 as tradeType, tradeTime, '%s' as tier FROM read_parquet('%s')`, tier, path)
		}
		return fmt.Sprintf(`SELECT row_id, schema_id, changed_at, deleted_at, name, version, symbol, exchange, region, tradeType, tradeTime, '%s' as tier FROM read_parquet('%s')`, tier, path)
	}
}

func (h *FederatedTestHarness) buildHotTierQuery(pgConnStr string, schemaID int16, rowIDFilter, attributeFilter, timeWindowFilter string, benchmarkProjection, tradeTimeOnlyProjection bool) string {
	if benchmarkProjection {
		if tradeTimeOnlyProjection && schemaID == benchmarkSchemaIDTrade {
			return h.buildHotTradeTimeOnlyQuery(pgConnStr, schemaID, rowIDFilter)
		}
		return h.buildHotTierQueryTargeted(pgConnStr, schemaID, rowIDFilter, attributeFilter, timeWindowFilter)
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
			%s
			%s`, pgConnStr, schemaID, rowIDFilter, timeWindowFilter)
}

func buildHotTierQuery(pgConnStr string, schemaID int16, rowIDFilter, attributeFilter, timeWindowFilter string, benchmarkProjection, tradeTimeOnlyProjection bool) string {
	return (*FederatedTestHarness)(nil).buildHotTierQuery(pgConnStr, schemaID, rowIDFilter, attributeFilter, timeWindowFilter, benchmarkProjection, tradeTimeOnlyProjection)
}

func (h *FederatedTestHarness) buildHotTradeTimeOnlyQuery(pgConnStr string, schemaID int16, rowIDFilter string) string {
	tradeTimeAttrID := h.benchmarkAttributeID(schemaID, "tradeTime")
	return fmt.Sprintf(`
		SELECT 
			cl.row_id::VARCHAR as row_id,
			cl.schema_id,
			cl.changed_at,
			cl.deleted_at,
			'' as name,
			0 as version,
			'' as symbol,
			'' as exchange,
			'' as region,
			0 as tradeType,
			COALESCE(hot_vals.trade_time, em.bigint_02, 0) as tradeTime,
			'hot' as tier
		FROM postgres_scan('%s', 'public', 'change_log') cl
		LEFT JOIN postgres_scan('%s', 'public', 'entity_main') em
			ON em.ltbase_schema_id = cl.schema_id AND em.ltbase_row_id::VARCHAR = cl.row_id::VARCHAR
		LEFT JOIN (
			SELECT row_id::VARCHAR as row_id, schema_id,
				MAX(CASE WHEN attr_id = %d THEN value_numeric::BIGINT END) AS trade_time
			FROM postgres_scan('%s', 'public', 'eav_data')
			WHERE attr_id = %d
			GROUP BY schema_id, row_id
		) hot_vals ON hot_vals.schema_id = cl.schema_id AND hot_vals.row_id = cl.row_id::VARCHAR
		WHERE cl.flushed_at = 0 
			AND cl.schema_id = %d
			%s`, pgConnStr, pgConnStr, tradeTimeAttrID, pgConnStr, tradeTimeAttrID, schemaID, rowIDFilter)
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

func usesBenchmarkProjectionForSelect(opts *QueryOptions) bool {
	if opts == nil {
		return false
	}
	if requiresBenchmarkProjectedFilters(opts) {
		return true
	}
	if opts.SortBy != "" && opts.SortBy != "row_id" {
		return true
	}
	return false
}

func usesBenchmarkProjectionForCount(opts *QueryOptions) bool {
	if requiresBenchmarkProjectedFilters(opts) {
		return true
	}
	if opts == nil {
		return false
	}
	if opts.Offset <= 0 {
		return usesBenchmarkProjectionForSelect(opts)
	}
	return false
}

func usesTradeTimeOnlyBenchmarkProjectionForSelect(opts *QueryOptions) bool {
	if !usesBenchmarkProjectionForSelect(opts) || opts == nil {
		return false
	}
	if opts.SortBy != "tradeTime" || opts.Filter != nil {
		return false
	}
	return opts.TradeTimeStart == 0 && opts.TradeTimeEnd == 0
}

type hotTierEAVMapping struct {
	attrIDList   string
	pivotColumns string
	selectExprs  string
	nameExpr     string
}

func (h *FederatedTestHarness) benchmarkAttributeID(schemaID int16, name string) int {
	if h != nil && h.Registry != nil {
		if _, cache, err := h.Registry.GetSchemaAttributeCacheByID(schemaID); err == nil && cache != nil {
			if metadata, ok := cache[name]; ok {
				return int(metadata.AttributeID)
			}
		}
	}
	hash := uint32(2166136261)
	input := fmt.Sprintf("%d:%s", schemaID, name)
	for i := 0; i < len(input); i++ {
		hash ^= uint32(input[i])
		hash *= 16777619
	}
	return int(hash%30000) + 1
}

func (h *FederatedTestHarness) hotTierEAVMappingForSchema(schemaID int16) hotTierEAVMapping {
	switch schemaID {
	case benchmarkSchemaIDTrade:
		symbolID := h.benchmarkAttributeID(schemaID, "symbol")
		exchangeID := h.benchmarkAttributeID(schemaID, "exchange")
		regionID := h.benchmarkAttributeID(schemaID, "region")
		tradeTypeID := h.benchmarkAttributeID(schemaID, "tradeType")
		tradeTimeID := h.benchmarkAttributeID(schemaID, "tradeTime")
		return hotTierEAVMapping{
			attrIDList: fmt.Sprintf("%d, %d, %d, %d, %d", symbolID, exchangeID, regionID, tradeTypeID, tradeTimeID),
			pivotColumns: fmt.Sprintf(
				"MAX(CASE WHEN attr_id = %d THEN value_text END) AS symbol,\n\t\t\t"+
					"MAX(CASE WHEN attr_id = %d THEN value_text END) AS exchange,\n\t\t\t"+
					"MAX(CASE WHEN attr_id = %d THEN value_text END) AS region,\n\t\t\t"+
					"MAX(CASE WHEN attr_id = %d THEN value_numeric::BIGINT END) AS tradeType,\n\t\t\t"+
					"MAX(CASE WHEN attr_id = %d THEN value_numeric::BIGINT END) AS tradeTime",
				symbolID, exchangeID, regionID, tradeTypeID, tradeTimeID),
			selectExprs: "COALESCE(hot_vals.symbol, em.text_01) as symbol,\n\t\t\t" +
				"COALESCE(hot_vals.exchange, '') as exchange,\n\t\t\t" +
				"COALESCE(hot_vals.region, em.text_02) as region,\n\t\t\t" +
				"COALESCE(hot_vals.tradeType, em.smallint_01) as tradeType,\n\t\t\t" +
				"COALESCE(hot_vals.tradeTime, em.bigint_02) as tradeTime",
			nameExpr: "COALESCE(hot_vals.symbol, em.text_01, '')",
		}
	case benchmarkSchemaIDCustomer:
		regionID := h.benchmarkAttributeID(schemaID, "region")
		nameID := h.benchmarkAttributeID(schemaID, "name")
		return hotTierEAVMapping{
			attrIDList: fmt.Sprintf("%d, %d", regionID, nameID),
			pivotColumns: fmt.Sprintf(
				"MAX(CASE WHEN attr_id = %d THEN value_text END) AS region,\n\t\t\t"+
					"MAX(CASE WHEN attr_id = %d THEN value_text END) AS name",
				regionID, nameID),
			selectExprs: "'' as symbol,\n\t\t\t" +
				"'' as exchange,\n\t\t\t" +
				"COALESCE(hot_vals.region, em.text_02) as region,\n\t\t\t" +
				"0 as tradeType,\n\t\t\t" +
				"0 as tradeTime",
			nameExpr: "COALESCE(hot_vals.name, '')",
		}
	case benchmarkSchemaIDSecurity:
		symbolID := h.benchmarkAttributeID(schemaID, "symbol")
		nameID := h.benchmarkAttributeID(schemaID, "companyName")
		return hotTierEAVMapping{
			attrIDList: fmt.Sprintf("%d, %d", symbolID, nameID),
			pivotColumns: fmt.Sprintf(
				"MAX(CASE WHEN attr_id = %d THEN value_text END) AS symbol,\n\t\t\t"+
					"MAX(CASE WHEN attr_id = %d THEN value_text END) AS name",
				symbolID, nameID),
			selectExprs: "COALESCE(hot_vals.symbol, em.text_01) as symbol,\n\t\t\t" +
				"'' as exchange,\n\t\t\t" +
				"'' as region,\n\t\t\t" +
				"0 as tradeType,\n\t\t\t" +
				"0 as tradeTime",
			nameExpr: "COALESCE(hot_vals.name, hot_vals.symbol, '')",
		}
	default:
		return hotTierEAVMapping{}
	}
}

func hotTierEAVMappingForSchema(schemaID int16) hotTierEAVMapping {
	return (*FederatedTestHarness)(nil).hotTierEAVMappingForSchema(schemaID)
}

func (h *FederatedTestHarness) buildHotTierQueryTargeted(pgConnStr string, schemaID int16, rowIDFilter, attributeFilter, timeWindowFilter string) string {
	m := h.hotTierEAVMappingForSchema(schemaID)
	return fmt.Sprintf(`
		SELECT 
			cl.row_id::VARCHAR as row_id,
			cl.schema_id,
			cl.changed_at,
			cl.deleted_at,
			%s as name,
			0 as version,
			%s,
			'hot' as tier
		FROM postgres_scan('%s', 'public', 'change_log') cl
		LEFT JOIN postgres_scan('%s', 'public', 'entity_main') em
			ON em.ltbase_schema_id = cl.schema_id AND em.ltbase_row_id::VARCHAR = cl.row_id::VARCHAR
		LEFT JOIN (
			SELECT row_id::VARCHAR as row_id, schema_id,
				%s
			FROM postgres_scan('%s', 'public', 'eav_data')
			WHERE attr_id IN (%s)
			GROUP BY schema_id, row_id
		) hot_vals ON hot_vals.schema_id = cl.schema_id AND hot_vals.row_id = cl.row_id::VARCHAR
		WHERE cl.flushed_at = 0 
			AND cl.schema_id = %d
			%s
			%s
			%s`,
		m.nameExpr, m.selectExprs,
		pgConnStr, pgConnStr,
		m.pivotColumns, pgConnStr, m.attrIDList,
		schemaID, rowIDFilter, attributeFilter, timeWindowFilter)
}

func buildHotTierQueryTargeted(pgConnStr string, schemaID int16, rowIDFilter, attributeFilter, timeWindowFilter string) string {
	return (*FederatedTestHarness)(nil).buildHotTierQueryTargeted(pgConnStr, schemaID, rowIDFilter, attributeFilter, timeWindowFilter)
}

func needsBenchmarkDuckDBMacros(opts *QueryOptions, benchmarkProjection, tradeTimeOnlyProjection bool) bool {
	return false
}

func requiresBenchmarkProjectedFilters(opts *QueryOptions) bool {
	if opts == nil {
		return false
	}
	if opts.TradeTimeStart > 0 || opts.TradeTimeEnd > 0 {
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

func buildHotRowIDFilter(opts *QueryOptions) string {
	if opts.Filter != nil && opts.Filter.RowID != uuid.Nil {
		return fmt.Sprintf("AND cl.row_id = '%s'", opts.Filter.RowID.String())
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

func buildTradeTimeFilterClause(opts *QueryOptions) string {
	if opts == nil {
		return ""
	}
	parts := make([]string, 0, 2)
	expression := parquetTradeTimeFilterExpression()
	if opts.TradeTimeStart > 0 {
		parts = append(parts, fmt.Sprintf("AND %s >= %d", expression, opts.TradeTimeStart))
	}
	if opts.TradeTimeEnd > 0 {
		parts = append(parts, fmt.Sprintf("AND %s <= %d", expression, opts.TradeTimeEnd))
	}
	return strings.Join(parts, " ")
}

func parquetTradeTimeFilterExpression() string {
	return "tradeTime"
}

func buildHotTradeTimeFilterClauseTargeted(opts *QueryOptions) string {
	if opts == nil {
		return ""
	}
	parts := make([]string, 0, 2)
	expression := targetedHotFilterExpression("tradeTime")
	if opts.TradeTimeStart > 0 {
		parts = append(parts, fmt.Sprintf("AND %s >= %d", expression, opts.TradeTimeStart))
	}
	if opts.TradeTimeEnd > 0 {
		parts = append(parts, fmt.Sprintf("AND %s <= %d", expression, opts.TradeTimeEnd))
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

func (h *FederatedTestHarness) benchmarkAttrNameSQLCase() string {
	type attrKey struct {
		schemaID int16
		name     string
	}
	mapping := map[attrKey]int{
		{benchmarkSchemaIDTrade, "symbol"}:    h.benchmarkAttributeID(benchmarkSchemaIDTrade, "symbol"),
		{benchmarkSchemaIDTrade, "exchange"}:  h.benchmarkAttributeID(benchmarkSchemaIDTrade, "exchange"),
		{benchmarkSchemaIDTrade, "region"}:    h.benchmarkAttributeID(benchmarkSchemaIDTrade, "region"),
		{benchmarkSchemaIDTrade, "tradeType"}: h.benchmarkAttributeID(benchmarkSchemaIDTrade, "tradeType"),
		{benchmarkSchemaIDTrade, "tradeTime"}: h.benchmarkAttributeID(benchmarkSchemaIDTrade, "tradeTime"),
		{benchmarkSchemaIDCustomer, "region"}: h.benchmarkAttributeID(benchmarkSchemaIDCustomer, "region"),
		{benchmarkSchemaIDCustomer, "name"}:   h.benchmarkAttributeID(benchmarkSchemaIDCustomer, "name"),
		{benchmarkSchemaIDSecurity, "symbol"}: h.benchmarkAttributeID(benchmarkSchemaIDSecurity, "symbol"),
		{benchmarkSchemaIDSecurity, "name"}:   h.benchmarkAttributeID(benchmarkSchemaIDSecurity, "companyName"),
	}
	parts := make([]string, 0, len(mapping))
	for key, attrID := range mapping {
		parts = append(parts, fmt.Sprintf("WHEN schema_id = %d AND attr_id = %d THEN '%s'", key.schemaID, attrID, key.name))
	}
	return strings.Join(parts, " ")
}
func (h *FederatedTestHarness) benchmarkFunctionsSQL() string {
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
	`, h.benchmarkAttrNameSQLCase())
}

func prepareBenchmarkDuckDBMacros(ctx context.Context, h *FederatedTestHarness) error {
	_, err := h.Duck.DB.ExecContext(ctx, h.benchmarkFunctionsSQL())
	if err != nil {
		return fmt.Errorf("prepare benchmark duckdb macros: %w", err)
	}
	return nil
}

func buildHotAttributeFilterClauseTargeted(opts *QueryOptions) string {
	if opts == nil || opts.Filter == nil || len(opts.Filter.Conditions) == 0 {
		return ""
	}
	parts := make([]string, 0, len(opts.Filter.Conditions))
	for key, value := range opts.Filter.Conditions {
		expression := targetedHotFilterExpression(key)
		if expression == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("AND %s = %s", expression, benchmarkSQLLiteral(value)))
	}
	return strings.Join(parts, " ")
}

func targetedHotFilterExpression(attribute string) string {
	switch attribute {
	case "symbol":
		return "COALESCE(hot_vals.symbol, em.text_01)"
	case "exchange":
		return "COALESCE(hot_vals.exchange, '')"
	case "region":
		return "COALESCE(hot_vals.region, em.text_02)"
	case "tradeType":
		return "COALESCE(hot_vals.tradeType, em.smallint_01)"
	case "tradeTime":
		return "COALESCE(hot_vals.tradeTime, em.bigint_02)"
	default:
		return ""
	}
}

func buildOrderByClause(opts *QueryOptions) string {
	prefix := ""
	if opts == nil {
		return prefix + "row_id ASC"
	}
	column := benchmarkQueryColumn(opts.SortBy)
	if column == "" {
		column = "row_id"
	}
	direction := "ASC"
	if opts.SortDesc {
		direction = "DESC"
	}
	return fmt.Sprintf("%s%s %s, %srow_id ASC", prefix, column, direction, prefix)
}

// buildPGConnString builds the Postgres connection string for DuckDB.
func (h *FederatedTestHarness) buildPGConnString() string {
	host := h.PGHost
	if host == "" {
		host = "localhost"
	}
	port := h.PGPort
	if port == "" {
		port = "5432"
	}
	user := h.PGUser
	if user == "" {
		user = "postgres"
	}
	password := h.PGPassword
	if password == "" {
		password = "password"
	}
	database := h.PGDatabase
	if database == "" {
		database = "postgres"
	}
	sslMode := h.PGSSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		host, port, user, password, database, sslMode)
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
	countQuery, countArgs := h.buildPostgresOnlyCountQuery(opts)
	var total int64
	if err := h.PGDB.QueryRowContext(ctx, countQuery, countArgs...).Scan(&total); err != nil {
		return nil, err
	}
	if opts.CountOnly {
		return &QueryResult{TotalRecords: total, Duration: time.Since(start), Plan: buildPostgresOnlyExecutionPlan(time.Since(start), opts.PreferHot)}, nil
	}

	query, args := h.buildPostgresOnlySelectQuery(opts)
	rows, err := h.PGDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	benchmarkProjection := usesBenchmarkProjectionForSelect(opts)
	var records []*model.PersistentRecord
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
				return nil, err
			}
		} else {
			if err := rows.Scan(&rowID, &schemaID, &changedAt, &deletedAt, &name, &version); err != nil {
				return nil, err
			}
		}
		rec := &model.PersistentRecord{RowID: uuid.MustParse(rowID), SchemaID: schemaID, CreatedAt: changedAt, UpdatedAt: changedAt, TextItems: map[string]string{}, Float64Items: map[string]float64{}}
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

	return &QueryResult{
		Records:      records,
		TotalRecords: total,
		Duration:     time.Since(start),
		Plan:         buildPostgresOnlyExecutionPlan(time.Since(start), opts.PreferHot),
	}, nil
}

func (h *FederatedTestHarness) buildPostgresOnlySelectQuery(opts *QueryOptions) (string, []any) {
	args := []any{h.SchemaID}
	attrIDs := h.benchmarkPostgresAttributeIDs()
	query := strings.Builder{}
	query.WriteString(`
		SELECT
			cl.row_id::VARCHAR,
			cl.schema_id,
			cl.changed_at,
			COALESCE(cl.deleted_at, 0),
			COALESCE(hot_vals.name, hot_vals.symbol, '') as name,
			0 as version`)
	if usesBenchmarkProjectionForSelect(opts) {
		query.WriteString(`,
			COALESCE(hot_vals.symbol, em.text_01, '') as symbol,
			COALESCE(hot_vals.exchange, '') as exchange,
			COALESCE(hot_vals.region, em.text_02, '') as region,
			COALESCE(hot_vals.trade_type, em.smallint_01::BIGINT, 0) as tradeType,
			COALESCE(hot_vals.trade_time, em.bigint_02, 0) as tradeTime`)
	}
	query.WriteString(fmt.Sprintf(`
		FROM change_log cl
		LEFT JOIN entity_main em
			ON em.ltbase_schema_id = cl.schema_id AND em.ltbase_row_id = cl.row_id
		LEFT JOIN (
			SELECT schema_id, row_id,
				MAX(CASE WHEN attr_id = %d THEN value_text END) AS symbol,
				MAX(CASE WHEN attr_id = %d THEN value_text END) AS exchange,
				MAX(CASE WHEN attr_id = %d THEN value_text END) AS region,
				MAX(CASE WHEN attr_id = %d THEN value_numeric::BIGINT END) AS trade_type,
				MAX(CASE WHEN attr_id = %d THEN value_numeric::BIGINT END) AS trade_time,
				MAX(CASE WHEN attr_id = %d THEN value_text END) AS name
			FROM eav_data
			WHERE attr_id IN (%d, %d, %d, %d, %d, %d)
			GROUP BY schema_id, row_id
		) hot_vals ON hot_vals.schema_id = cl.schema_id AND hot_vals.row_id = cl.row_id
		WHERE cl.schema_id = $1 AND cl.flushed_at = 0 AND (cl.deleted_at IS NULL OR cl.deleted_at = 0)`,
		attrIDs.symbol, attrIDs.exchange, attrIDs.region, attrIDs.tradeType, attrIDs.tradeTime, attrIDs.name,
		attrIDs.symbol, attrIDs.exchange, attrIDs.region, attrIDs.tradeType, attrIDs.tradeTime, attrIDs.name))
	filterSQL, filterArgs := buildPostgresOnlyFilterClauses(opts, 2)
	query.WriteString(filterSQL)
	args = append(args, filterArgs...)
	query.WriteString(fmt.Sprintf(" ORDER BY %s LIMIT $%d OFFSET $%d", buildPostgresOnlyOrderBy(opts), len(args)+1, len(args)+2))
	args = append(args, opts.Limit, opts.Offset)
	return query.String(), args
}

func (h *FederatedTestHarness) buildPostgresOnlyCountQuery(opts *QueryOptions) (string, []any) {
	args := []any{h.SchemaID}
	attrIDs := h.benchmarkPostgresAttributeIDs()
	query := strings.Builder{}
	query.WriteString(fmt.Sprintf(`
		SELECT COUNT(*)
		FROM change_log cl
		LEFT JOIN entity_main em
			ON em.ltbase_schema_id = cl.schema_id AND em.ltbase_row_id = cl.row_id
		LEFT JOIN (
			SELECT schema_id, row_id,
				MAX(CASE WHEN attr_id = %d THEN value_text END) AS symbol,
				MAX(CASE WHEN attr_id = %d THEN value_text END) AS exchange,
				MAX(CASE WHEN attr_id = %d THEN value_text END) AS region,
				MAX(CASE WHEN attr_id = %d THEN value_numeric::BIGINT END) AS trade_type,
				MAX(CASE WHEN attr_id = %d THEN value_numeric::BIGINT END) AS trade_time,
				MAX(CASE WHEN attr_id = %d THEN value_text END) AS name
			FROM eav_data
			WHERE attr_id IN (%d, %d, %d, %d, %d, %d)
			GROUP BY schema_id, row_id
		) hot_vals ON hot_vals.schema_id = cl.schema_id AND hot_vals.row_id = cl.row_id
		WHERE cl.schema_id = $1 AND cl.flushed_at = 0 AND (cl.deleted_at IS NULL OR cl.deleted_at = 0)`,
		attrIDs.symbol, attrIDs.exchange, attrIDs.region, attrIDs.tradeType, attrIDs.tradeTime, attrIDs.name,
		attrIDs.symbol, attrIDs.exchange, attrIDs.region, attrIDs.tradeType, attrIDs.tradeTime, attrIDs.name))
	filterSQL, filterArgs := buildPostgresOnlyFilterClauses(opts, 2)
	query.WriteString(filterSQL)
	args = append(args, filterArgs...)
	return query.String(), args
}

func buildPostgresOnlyFilterClauses(opts *QueryOptions, placeholderStart int) (string, []any) {
	if opts == nil {
		return "", nil
	}
	args := make([]any, 0)
	parts := make([]string, 0)
	placeholder := placeholderStart
	if opts.Filter != nil && opts.Filter.RowID != uuid.Nil {
		parts = append(parts, fmt.Sprintf("AND cl.row_id = $%d", placeholder))
		args = append(args, opts.Filter.RowID)
		placeholder++
	}
	if opts.Filter != nil {
		for key, value := range opts.Filter.Conditions {
			expression := postgresOnlyFilterExpression(key)
			if expression == "" {
				continue
			}
			parts = append(parts, fmt.Sprintf("AND %s = $%d", expression, placeholder))
			args = append(args, value)
			placeholder++
		}
	}
	if opts.TradeTimeStart > 0 {
		parts = append(parts, fmt.Sprintf("AND %s >= $%d", postgresOnlyFilterExpression("tradeTime"), placeholder))
		args = append(args, opts.TradeTimeStart)
		placeholder++
	}
	if opts.TradeTimeEnd > 0 {
		parts = append(parts, fmt.Sprintf("AND %s <= $%d", postgresOnlyFilterExpression("tradeTime"), placeholder))
		args = append(args, opts.TradeTimeEnd)
	}
	if len(parts) == 0 {
		return "", nil
	}
	return " " + strings.Join(parts, " "), args
}

func buildPostgresOnlyOrderBy(opts *QueryOptions) string {
	if opts == nil || opts.SortBy == "" {
		return "cl.row_id ASC"
	}
	column := postgresOnlyFilterExpression(opts.SortBy)
	if column == "" {
		column = "cl.row_id"
	}
	direction := "ASC"
	if opts.SortDesc {
		direction = "DESC"
	}
	return fmt.Sprintf("%s %s, cl.row_id ASC", column, direction)
}

type postgresBenchmarkAttributeIDs struct {
	symbol    int
	exchange  int
	region    int
	tradeType int
	tradeTime int
	name      int
}

func (h *FederatedTestHarness) benchmarkPostgresAttributeIDs() postgresBenchmarkAttributeIDs {
	if h != nil && h.Registry != nil {
		if _, cache, err := h.Registry.GetSchemaAttributeCacheByID(benchmarkSchemaIDTrade); err == nil && cache != nil {
			ids := postgresBenchmarkAttributeIDs{}
			if meta, ok := cache["symbol"]; ok {
				ids.symbol = int(meta.AttributeID)
			}
			if meta, ok := cache["exchange"]; ok {
				ids.exchange = int(meta.AttributeID)
			}
			if meta, ok := cache["region"]; ok {
				ids.region = int(meta.AttributeID)
			}
			if meta, ok := cache["tradeType"]; ok {
				ids.tradeType = int(meta.AttributeID)
			}
			if meta, ok := cache["tradeTime"]; ok {
				ids.tradeTime = int(meta.AttributeID)
			}
			return ids
		}
	}
	return postgresBenchmarkAttributeIDs{
		symbol:    h.benchmarkAttributeID(benchmarkSchemaIDTrade, "symbol"),
		exchange:  h.benchmarkAttributeID(benchmarkSchemaIDTrade, "exchange"),
		region:    h.benchmarkAttributeID(benchmarkSchemaIDTrade, "region"),
		tradeType: h.benchmarkAttributeID(benchmarkSchemaIDTrade, "tradeType"),
		tradeTime: h.benchmarkAttributeID(benchmarkSchemaIDTrade, "tradeTime"),
		name:      h.benchmarkAttributeID(benchmarkSchemaIDTrade, "name"),
	}
}

func postgresOnlyFilterExpression(attribute string) string {
	switch attribute {
	case "symbol":
		return "COALESCE(hot_vals.symbol, em.text_01, '')"
	case "exchange":
		return "COALESCE(hot_vals.exchange, '')"
	case "region":
		return "COALESCE(hot_vals.region, em.text_02, '')"
	case "tradeType":
		return "COALESCE(hot_vals.trade_type, em.smallint_01::BIGINT, 0)"
	case "tradeTime":
		return "COALESCE(hot_vals.trade_time, em.bigint_02, 0)"
	default:
		return ""
	}
}

func buildPostgresOnlyExecutionPlan(duration time.Duration, preferHot bool) *model.ExecutionPlan {
	notes := []string{"hot_buffer_scanned", "postgres_only_execution"}
	if preferHot {
		notes = append(notes, "prefer_hot_override")
	}
	return &model.ExecutionPlan{Notes: notes, Timings: map[string]int64{"total": duration.Milliseconds()}}
}

// StreamFederatedQuery streams query results with a handler callback.
func (h *FederatedTestHarness) StreamFederatedQuery(ctx context.Context, opts *QueryOptions, handler func(*model.PersistentRecord) error) error {
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
