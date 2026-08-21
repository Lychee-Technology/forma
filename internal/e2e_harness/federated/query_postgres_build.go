package federated

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
)

// PostgreSQL-only SQL assembly for the PreferHot and no-Parquet fallback paths.
// Split from query.go to keep direct Postgres query construction independent of
// execution and row scanning (#220).

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
	query.WriteString(buildPostgresOnlyScanSource(attrIDs))
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
	query.WriteString(`
		SELECT COUNT(*)`)
	query.WriteString(buildPostgresOnlyScanSource(attrIDs))
	filterSQL, filterArgs := buildPostgresOnlyFilterClauses(opts, 2)
	query.WriteString(filterSQL)
	args = append(args, filterArgs...)
	return query.String(), args
}

// buildPostgresOnlyScanSource renders the FROM/JOIN/WHERE block shared by the
// Postgres-only select and count builders: the change_log/entity_main joins, the
// hot_vals EAV pivot, and the unflushed-and-live guard. The count has to scan
// exactly the row set the select returns, so rendering the block once makes that
// structural rather than a convention two copies must keep agreeing on (#324).
// TestPostgresOnlyCountSharesSelectScanSource is the guard against re-divergence.
func buildPostgresOnlyScanSource(attrIDs postgresBenchmarkAttributeIDs) string {
	return fmt.Sprintf(`
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
		attrIDs.symbol, attrIDs.exchange, attrIDs.region, attrIDs.tradeType, attrIDs.tradeTime, attrIDs.name)
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
