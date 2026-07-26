package federated

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// Federated multi-tier SQL assembly: the dynamic entry points, the UNION ALL
// combined query, parquet tier queries with row_id-semijoin predicate pushdown,
// post-LWW-dedup final select/count, and shared projection/filter helpers.
// query.go owns execution and scanning; query_hot.go owns hot-tier EAV SQL; and
// query_postgres_build.go owns PostgreSQL-only SQL assembly (#220).

// buildFederatedQuerySQLDynamic builds the federated query SQL, only including tiers that have files.
func (h *FederatedTestHarness) buildFederatedQuerySQLDynamic(basePath, deltaPath string, hasBase, hasDelta bool, dirtyIDs []uuid.UUID, opts *QueryOptions) string {
	benchmarkProjection := usesBenchmarkProjectionForSelect(opts)
	combinedQuery := h.buildFederatedCombinedQuery(basePath, deltaPath, hasBase, hasDelta, dirtyIDs, opts, benchmarkProjection, usesTradeTimeOnlyBenchmarkProjectionForSelect(opts))
	return buildFinalFederatedSelect(combinedQuery, opts, benchmarkProjection)
}

func (h *FederatedTestHarness) buildFederatedQueryCountSQLDynamic(basePath, deltaPath string, hasBase, hasDelta bool, dirtyIDs []uuid.UUID, opts *QueryOptions) string {
	combinedQuery := h.buildFederatedCombinedQuery(basePath, deltaPath, hasBase, hasDelta, dirtyIDs, opts, usesBenchmarkProjectionForCount(opts), false)
	return buildFinalFederatedCount(combinedQuery, opts)
}

func (h *FederatedTestHarness) buildFederatedCombinedQuery(basePath, deltaPath string, hasBase, hasDelta bool, dirtyIDs []uuid.UUID, opts *QueryOptions, benchmarkProjection, tradeTimeOnlyProjection bool) string {
	dirtyExclusion := buildDirtyExclusion(dirtyIDs)
	rowIDFilter := buildRowIDFilter(opts)
	hotRowIDFilter := buildHotRowIDFilter(opts)
	parquetPushdown := buildParquetPushdownSemijoin(basePath, deltaPath, hasBase, hasDelta, opts)
	// The hot tier keeps its pre-dedup predicates: every hot row_id is in the
	// dirty set (both derive from change_log.flushed_at = 0), so its S3
	// versions never enter the dedup and a filtered-out hot row cannot be
	// replaced by a stale parquet version — mirroring production pg_source
	// (#173). The parquet tiers get only the row_id semijoin (#213).
	hotAttributeFilter := buildHotAttributeFilterClauseTargeted(opts)
	hotTimeWindowFilter := buildHotTradeTimeFilterClauseTargeted(opts)
	pgConnStr := h.buildPGConnString()

	// Build tier queries dynamically
	var tierQueries []string

	if hasBase {
		baseQuery := buildParquetTierQuery(basePath, h.SchemaID, "base", dirtyExclusion, rowIDFilter, parquetPushdown, benchmarkProjection, tradeTimeOnlyProjection)
		tierQueries = append(tierQueries, baseQuery)
	}

	if hasDelta {
		deltaQuery := buildParquetTierQuery(deltaPath, h.SchemaID, "delta", dirtyExclusion, rowIDFilter, parquetPushdown, benchmarkProjection, tradeTimeOnlyProjection)
		tierQueries = append(tierQueries, deltaQuery)
	}

	// Always include hot buffer (Postgres)
	hotQuery := h.buildHotTierQuery(pgConnStr, h.SchemaID, hotRowIDFilter, hotAttributeFilter, hotTimeWindowFilter, benchmarkProjection, tradeTimeOnlyProjection)
	tierQueries = append(tierQueries, hotQuery)

	// Combine all tier queries with UNION ALL
	combinedQuery := strings.Join(tierQueries, "\n\t\t\tUNION ALL\n")
	return combinedQuery
}

func buildParquetTierQuery(path string, schemaID int16, tier, dirtyExclusion, rowIDFilter, pushdownSemijoin string, benchmarkProjection, tradeTimeOnlyProjection bool) string {
	if benchmarkProjection {
		projection := benchmarkParquetProjection(schemaID, tier, path, tradeTimeOnlyProjection)
		return fmt.Sprintf(`
			%s
			WHERE 1 = 1 %s %s %s`, projection, dirtyExclusion, rowIDFilter, pushdownSemijoin)
	}
	return fmt.Sprintf(`
			SELECT row_id, schema_id, changed_at, deleted_at, name, version, '%s' as tier
			FROM read_parquet('%s')
			WHERE 1 = 1 %s %s %s`, tier, path, dirtyExclusion, rowIDFilter, pushdownSemijoin)
}

// buildParquetPushdownSemijoin renders the attribute/time-window pushdown for
// the parquet tiers as a row_id semijoin over EVERY parquet tier that exists:
// a row qualifies when ANY of its S3 versions matches, all of its versions
// then enter the ranked dedup, and the post-dedup predicate in the final
// select judges the winner. The subquery must span base and delta together —
// a per-tier semijoin would drop a newer non-matching delta version while the
// stale matching base version survives, recreating the #213 resurrection
// (and multi-version files make the same bug possible within one tier).
// Mirrors the production s3_source semijoin from #173.
func buildParquetPushdownSemijoin(basePath, deltaPath string, hasBase, hasDelta bool, opts *QueryOptions) string {
	attributeFilter := buildAttributeFilterClause(opts)
	timeWindowFilter := buildTradeTimeFilterClause(opts)
	if attributeFilter == "" && timeWindowFilter == "" {
		return ""
	}
	var paths []string
	if hasBase {
		paths = append(paths, fmt.Sprintf("'%s'", basePath))
	}
	if hasDelta {
		paths = append(paths, fmt.Sprintf("'%s'", deltaPath))
	}
	if len(paths) == 0 {
		return ""
	}
	// Join the (possibly one-sided) clauses so an absent filter leaves no
	// trailing whitespace before the closing paren.
	predicates := strings.TrimSpace(attributeFilter + " " + timeWindowFilter)
	return fmt.Sprintf("AND row_id IN (SELECT row_id FROM read_parquet([%s]) WHERE 1 = 1 %s)",
		strings.Join(paths, ", "), predicates)
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

func buildFinalFederatedSelect(combinedQuery string, opts *QueryOptions, benchmarkProjection bool) string {
	cte := buildFederatedDeduplicatedCTE(combinedQuery)
	// The attribute/time-window predicates evaluate the rn = 1 winner only;
	// per-tier they exist solely as a row_id semijoin so every version of a
	// qualifying row enters the dedup. Inlining them per tier dropped newer
	// non-matching versions pre-dedup and resurrected stale rows whose old
	// values still matched — the harness twin of production #173 (#213).
	// Non-empty predicates imply benchmarkProjection (they force the wide
	// projection via requiresBenchmarkProjectedFilters), so the referenced
	// columns are always present; rendering them in both branches turns any
	// future break of that invariant into a loud binder error.
	attributeFilter := buildAttributeFilterClause(opts)
	timeWindowFilter := buildTradeTimeFilterClause(opts)
	if benchmarkProjection {
		return fmt.Sprintf(`
		%s
		SELECT row_id, schema_id, changed_at, deleted_at, name, version, symbol, exchange, region, tradeType, tradeTime
		FROM deduplicated
		WHERE rn = 1 AND (deleted_at = 0 OR deleted_at IS NULL) %s %s
		ORDER BY %s
		LIMIT %d OFFSET %d
	`, cte, attributeFilter, timeWindowFilter, buildOrderByClause(opts), opts.Limit, opts.Offset)
	}
	return fmt.Sprintf(`
		%s
		SELECT row_id, schema_id, changed_at, deleted_at, name, version
		FROM deduplicated
		WHERE rn = 1 AND (deleted_at = 0 OR deleted_at IS NULL) %s %s
		ORDER BY row_id
		LIMIT %d OFFSET %d
	`, cte, attributeFilter, timeWindowFilter, opts.Limit, opts.Offset)
}

func buildFinalFederatedCount(combinedQuery string, opts *QueryOptions) string {
	// Count shares the post-dedup predicates with the select path so
	// TotalRecords and Records can never disagree under filters (#213).
	return fmt.Sprintf(`
		%s
		SELECT COUNT(*)
		FROM deduplicated
		WHERE rn = 1 AND (deleted_at = 0 OR deleted_at IS NULL) %s %s
	`, buildFederatedDeduplicatedCTE(combinedQuery), buildAttributeFilterClause(opts), buildTradeTimeFilterClause(opts))
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
