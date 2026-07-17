package sqlgen

import (
	"fmt"
	"strings"
	"text/template"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// RenderDuckDBQuery renders a DuckDB SQL template (which uses "?" placeholders)
// and combines the provided whereArgs (typically from buildDuckClause)
// with the template-collected args. The order is: whereArgs first, then template args.
func RenderDuckDBQuery(tpl *template.Template, params any, whereArgs []any) (string, []any, error) {
	sql, tplArgs, err := RenderSQLTemplate(tpl, params)
	if err != nil {
		return "", nil, err
	}
	combined := make([]any, 0, len(whereArgs)+len(tplArgs))
	combined = append(combined, whereArgs...)
	combined = append(combined, tplArgs...)
	return sql, combined, nil
}

// BuildDuckDBQuery prepares a DuckDB SQL string and its arguments for a federated query.
// It accepts optional DualClauses produced by ToDualClauses; when provided it will use
// the DuckClause and DuckArgs as the base where clause and inject PgMainClause into template
// params so the template (or tests) can observe the pushdown fragment. Dirty-ID exclusions
// are appended to the DuckDB clause regardless of source.
func BuildDuckDBQuery(tpl *template.Template, params any, q *model.FederatedAttributeQuery, dirtyIDs []uuid.UUID, dual *DualClauses) (string, []any, error) {
	isAdvancedTemplate := tpl == AdvancedQueryTemplateDuckDB

	// Prepare where variables
	var whereClause string
	var whereArgs []any

	// Ensure params is a map so we can inject Anchor.Condition, PgMainClause, and dirty helpers
	m, ok := params.(map[string]any)
	if !ok {
		m = map[string]any{}
	}

	// Ensure Anchor map exists
	anchor, ok := m["Anchor"].(map[string]any)
	if !ok || anchor == nil {
		anchor = map[string]any{}
		m["Anchor"] = anchor
	}

	// If dual clauses provided, prefer them; otherwise fall back to legacy generator.
	if dual != nil && dual.DuckClause != "" {
		whereClause = dual.DuckClause
		whereArgs = make([]any, 0, len(dual.DuckArgs)+len(dual.PgMainArgs)+len(dual.DuckArgs))
		if len(dual.DuckArgs) > 0 {
			whereArgs = append(whereArgs, dual.DuckArgs...)
		}
		// Generic templates need the dirty-id exclusion physically appended into the clause.
		// The production DuckDB federated template manages dirty IDs via its own CTE/anti-join.
		if !isAdvancedTemplate && len(dirtyIDs) > 0 {
			var exclArgs []any
			whereClause, exclArgs = AppendDirtyExclusion(whereClause, dirtyIDs)
			whereArgs = append(whereArgs, exclArgs...)
		}
		anchor["Condition"] = whereClause

		// Inject PgMainClause for inspection / postgres_scan integration
		m["PgMainClause"] = dual.PgMainClause
		m["PgMainArgs"] = dual.PgMainArgs
		m["HasPgMainClause"] = dual.PgMainClause != ""
		if isAdvancedTemplate {
			m["LOGICAL_WHERE_CLAUSE"] = dual.DuckClause
			m["PG_WHERE_CLAUSE"] = defaultIfEmpty(dual.PgMainClause, "1=1")
			m["HasHot"] = FederatedQueryHasHot(q)
			// The advanced-template bind interleave is [DuckArgs, PgMainArgs,
			// DuckArgs] (s3 semijoin, pg_source WHERE, visible). When the tier
			// form prunes pg_source, its args must be dropped with it or every
			// later placeholder mis-binds.
			if FederatedQueryHasHot(q) && len(dual.PgMainArgs) > 0 {
				whereArgs = append(whereArgs, dual.PgMainArgs...)
			}
			if len(dual.DuckArgs) > 0 {
				whereArgs = append(whereArgs, dual.DuckArgs...)
			}
		}
		injectDuckDBTemplateParams(m, q, dual)
		if isAdvancedTemplate {
			if err := requireProjectionParams(m); err != nil {
				return "", nil, fmt.Errorf("build DuckDB query: %w", err)
			}
		}
		if !isAdvancedTemplate && len(dual.PgMainArgs) > 0 {
			whereArgs = append(whereArgs, dual.PgMainArgs...)
		}
		whereArgs = appendKeysetArgs(m, whereArgs)

		merged := MergeTemplateParamsWithDirtyIDs(m, dirtyIDs)
		return RenderDuckDBQuery(tpl, merged, whereArgs)
	}

	return buildDuckDBQueryLegacy(tpl, m, anchor, q, dirtyIDs, isAdvancedTemplate)
}

// buildDuckDBQueryLegacy renders the dual==nil fallback path (preserved for
// tests; production always takes the dual path). A nil query keeps the old
// GenerateDuckDBWhereClause(nil) contract: "1=1".
func buildDuckDBQueryLegacy(tpl *template.Template, m map[string]any, anchor map[string]any, q *model.FederatedAttributeQuery, dirtyIDs []uuid.UUID, isAdvancedTemplate bool) (string, []any, error) {
	var fallbackCond forma.Condition
	if q != nil {
		fallbackCond = q.Condition
	}
	whereClause, whereArgs, err := buildDuckClause(fallbackCond, nil)
	if err != nil {
		return "", nil, err
	}
	if !isAdvancedTemplate && len(dirtyIDs) > 0 {
		var exclArgs []any
		whereClause, exclArgs = AppendDirtyExclusion(whereClause, dirtyIDs)
		whereArgs = append(whereArgs, exclArgs...)
	}
	anchor["Condition"] = whereClause
	if isAdvancedTemplate {
		m["LOGICAL_WHERE_CLAUSE"] = whereClause
		m["HasHot"] = FederatedQueryHasHot(q)
		if len(whereArgs) > 0 {
			whereArgs = append(whereArgs, whereArgs...)
		}
	}
	injectDuckDBTemplateParams(m, q, nil)
	if isAdvancedTemplate {
		if err := requireProjectionParams(m); err != nil {
			return "", nil, fmt.Errorf("build DuckDB query: %w", err)
		}
	}
	whereArgs = appendKeysetArgs(m, whereArgs)

	merged := MergeTemplateParamsWithDirtyIDs(m, dirtyIDs)
	return RenderDuckDBQuery(tpl, merged, whereArgs)
}

// projectionParamKeys are the schema-derived projection params the advanced
// template renders (sorted; keep sorted so the missing-param error is
// deterministic). Production always sets all seven via the engine's
// injectSchemaProjections — from BuildSchemaProjection (schema cache) or
// BuildBenchmarkProjections (benchmark schemas). The renderer used to
// substitute a retired toy-schema projection (name/age/tag, phantom ltbase_*
// parquet aliases) for any missing key; that projection cannot be scanned by
// duckDBScanBuffers under the #147 positional-scan contract, so a missing key
// is now a hard error instead (#222).
var projectionParamKeys = []string{
	"EAVPivotAttrs",
	"EAVPivotSelect",
	"HasEAVPivot",
	"OuterSelect",
	"PGGroupBy",
	"PGSourceSelect",
	"S3SourceSelect",
}

// requireProjectionParams fails an advanced-template render whose params are
// missing any schema-projection key, naming every absent key.
func requireProjectionParams(params map[string]any) error {
	var missing []string
	for _, key := range projectionParamKeys {
		if _, ok := params[key]; !ok {
			missing = append(missing, key)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("advanced DuckDB template render is missing schema projection params [%s]; derive them via BuildSchemaProjection or BuildBenchmarkProjections — the toy-schema defaults were retired (#222)", strings.Join(missing, ", "))
}

func defaultIfEmpty(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

// FederatedQueryHasHot reports whether the hot tier participates in the
// advanced-template render (#184). Empty PreferredTiers means the default
// all-tier form (the HTTP layer and harness both normalize to all three, and
// legacy nil-query renders must keep the historical full shape); a non-empty
// list participates only when it names hot. Routing has already intercepted
// the hot-only cases (engine gate), so a false here always coexists with a
// parquet source. internal/queryplan mirrors this hasHot membership in the
// shape hash — the two must stay in lockstep or the plan cache serves the
// wrong skeleton.
func FederatedQueryHasHot(q *model.FederatedAttributeQuery) bool {
	if q == nil || len(q.PreferredTiers) == 0 {
		return true
	}
	for _, tier := range q.PreferredTiers {
		if tier == model.DataTierHot {
			return true
		}
	}
	return false
}

func injectDuckDBTemplateParams(params map[string]any, q *model.FederatedAttributeQuery, dual *DualClauses) {
	if q == nil {
		return
	}

	params["SCHEMA_ID"] = q.SchemaID
	params["PAGE_SIZE"] = q.Limit
	params["OFFSET"] = q.Offset

	if _, ok := params["PG_WHERE_CLAUSE"]; !ok {
		pgWhere := "1=1"
		if dual != nil && dual.PgMainClause != "" {
			pgWhere = dual.PgMainClause
		}
		params["PG_WHERE_CLAUSE"] = pgWhere
	}

	if _, ok := params["LOGICAL_WHERE_CLAUSE"]; !ok {
		if anchor, ok := params["Anchor"].(map[string]any); ok {
			if cond, ok := anchor["Condition"].(string); ok && cond != "" {
				params["LOGICAL_WHERE_CLAUSE"] = cond
			}
		}
	}

	if _, ok := params["PG_CONN"]; !ok {
		if raw, ok := params["DuckDBPGConnString"].(string); ok && raw != "" {
			params["PG_CONN"] = raw
		}
	}

	if _, ok := params["S3_PATHS"]; !ok {
		if paths, ok := params["DuckDBS3Paths"].([]string); ok && len(paths) > 0 {
			params["S3_PATHS"] = formatDuckDBPathList(paths)
		}
	}

	// Keyset pagination: inject cursor-derived WHERE clause and ORDER BY.
	// The clause uses positional "?" placeholders; its args are appended after
	// all condition args by appendKeysetArgs, matching the clause's position at
	// the end of the visible CTE.
	if q.KeysetCursor != nil && len(q.KeysetCursor.Columns) > 0 {
		keysetClause, keysetArgs := generateKeysetWhereClause(q.KeysetCursor, "")
		params["HAS_KEYSET"] = true
		params["KEYSET_WHERE_CLAUSE"] = keysetClause
		params["KEYSET_ARGS"] = keysetArgs
		params["ORDER_BY"] = buildKeysetOrderBy(q.KeysetCursor)
	} else {
		params["HAS_KEYSET"] = false
	}

	// Non-keyset ORDER BY: use AttributeOrders when present, fall back to created_at DESC.
	if _, ok := params["NON_KEYSET_ORDER_BY"]; !ok {
		params["NON_KEYSET_ORDER_BY"] = buildNonKeysetOrderBy(q)
	}
}

func formatDuckDBPathList(paths []string) string {
	quoted := make([]string, 0, len(paths))
	for _, path := range paths {
		quoted = append(quoted, fmt.Sprintf("'%s'", path))
	}
	if len(quoted) == 1 {
		return quoted[0]
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

// buildNonKeysetOrderBy constructs an ORDER BY fragment from a query's AttributeOrders.
// Main-table attributes are sorted by their bound ColumnName; EAV attributes are sorted
// by their logical AttrName, which the unified CTE exposes as a named column via the
// EAV pivot. A trailing "row_id ASC" tiebreak is always appended so equal-key rows have
// a total order — mirroring the PG optimized template's trailing m.ltbase_row_id —
// keeping LIMIT/OFFSET windows stable across requests (#183).
func buildNonKeysetOrderBy(q *model.FederatedAttributeQuery) string {
	const stableTiebreak = "row_id ASC"
	if q == nil || len(q.AttributeOrders) == 0 {
		return "created_at DESC, " + stableTiebreak
	}
	var parts []string
	for _, ao := range q.AttributeOrders {
		dir := "ASC"
		if ao.Desc() {
			dir = "DESC"
		}
		if ao.IsMainColumn() {
			parts = append(parts, fmt.Sprintf("%s %s", ao.ColumnName, dir))
		} else if ao.AttrName != "" {
			// EAV attributes are projected as named columns in the unified CTE via the
			// EAV pivot (MAX(CASE WHEN attr_id = N THEN value_col END) AS attr_name),
			// under their parquet column alias (#260).
			parts = append(parts, fmt.Sprintf("%s %s", ParquetAttrColumn(ao.AttrName), dir))
		}
		// If neither ColumnName nor AttrName is set the attribute is unresolvable; skip it.
	}
	if len(parts) == 0 {
		return "created_at DESC, " + stableTiebreak
	}
	return strings.Join(parts, ", ") + ", " + stableTiebreak
}

// appendKeysetArgs extracts KEYSET_ARGS from the params map and appends them
// to the provided args slice. The keyset args are removed from params so the
// template renderer does not see them.
func appendKeysetArgs(params map[string]any, args []any) []any {
	raw, ok := params["KEYSET_ARGS"]
	if !ok {
		return args
	}
	keysetArgs, ok := raw.([]interface{})
	if !ok || len(keysetArgs) == 0 {
		return args
	}
	delete(params, "KEYSET_ARGS")
	return append(args, keysetArgs...)
}
