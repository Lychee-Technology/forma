package internal

import (
	"fmt"
	"strings"
	"text/template"

	"github.com/google/uuid"
)

// RenderDuckDBQuery renders a DuckDB SQL template (which uses "?" placeholders)
// and combines the provided whereArgs (typically from GenerateDuckDBWhereClause)
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
func BuildDuckDBQuery(tpl *template.Template, params any, q *FederatedAttributeQuery, dirtyIDs []uuid.UUID, dual *DualClauses) (string, []any, error) {
	isAdvancedTemplate := tpl == AdvancedQueryTemplateDuckDB

	// Prepare where variables
	var whereClause string
	var whereArgs []any
	var err error

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
			if len(dual.PgMainArgs) > 0 {
				whereArgs = append(whereArgs, dual.PgMainArgs...)
			}
			if len(dual.DuckArgs) > 0 {
				whereArgs = append(whereArgs, dual.DuckArgs...)
			}
		}
		injectDuckDBTemplateParams(m, q, dual)
		if !isAdvancedTemplate && len(dual.PgMainArgs) > 0 {
			whereArgs = append(whereArgs, dual.PgMainArgs...)
		}

		merged := MergeTemplateParamsWithDirtyIDs(m, dirtyIDs)
		return RenderDuckDBQuery(tpl, merged, whereArgs)
	}

	// Legacy path
	if isAdvancedTemplate {
		whereClause, whereArgs, err = GenerateDuckDBWhereClause(q)
	} else {
		whereClause, whereArgs, err = GenerateDuckDBWhereClauseWithExclusions(q, dirtyIDs)
	}
	if err != nil {
		return "", nil, err
	}
	anchor["Condition"] = whereClause
	if isAdvancedTemplate {
		m["LOGICAL_WHERE_CLAUSE"] = whereClause
		if len(whereArgs) > 0 {
			whereArgs = append(whereArgs, whereArgs...)
		}
	}
	injectDuckDBTemplateParams(m, q, nil)

	merged := MergeTemplateParamsWithDirtyIDs(m, dirtyIDs)
	return RenderDuckDBQuery(tpl, merged, whereArgs)
}

func defaultIfEmpty(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

func injectDuckDBTemplateParams(params map[string]any, q *FederatedAttributeQuery, dual *DualClauses) {
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

	// Inject defaults for schema-driven projection parameters if not already set.
	// These defaults match the pre-dynamic-template fixed schema layout.
	if _, ok := params["S3SourceSelect"]; !ok {
		params["S3SourceSelect"] = "row_id, ltbase_created_at AS created_at, ltbase_updated_at AS ver_ts, ltbase_deleted_at AS deleted_ts, name, age, tag"
	}
	if _, ok := params["PGSourceSelect"]; !ok {
		params["PGSourceSelect"] = "m.ltbase_row_id AS row_id, m.ltbase_created_at AS created_at, cl.changed_at AS ver_ts, cl.deleted_at AS deleted_ts, CAST(m.text_01 AS VARCHAR) AS name, CAST(m.integer_01 AS INTEGER) AS age, MAX(CASE WHEN e.attr_id = 205 THEN CAST(e.value_text AS VARCHAR) END) AS tag"
	}
	if _, ok := params["PGGroupBy"]; !ok {
		params["PGGroupBy"] = "m.ltbase_row_id, m.ltbase_created_at, cl.changed_at, cl.deleted_at, m.text_01, m.integer_01"
	}
	if _, ok := params["EAVPivotSelect"]; !ok {
		params["EAVPivotSelect"] = "MAX(CASE WHEN attr_id = 205 THEN CAST(e.value_text AS VARCHAR) END) AS tag"
	}
	if _, ok := params["EAVPivotAttrs"]; !ok {
		params["EAVPivotAttrs"] = "205"
	}
	if _, ok := params["HasEAVPivot"]; !ok {
		params["HasEAVPivot"] = true
	}
	if _, ok := params["OuterSelect"]; !ok {
		schemaID := int16(0)
		if q != nil {
			schemaID = q.SchemaID
		}
		params["OuterSelect"] = fmt.Sprintf(`%d::SMALLINT AS ltbase_schema_id,
			CAST(row_id AS UUID) AS ltbase_row_id,
			created_at AS ltbase_created_at,
			ver_ts AS ltbase_updated_at,
			deleted_ts AS ltbase_deleted_at,
			name AS text_01,
			age AS integer_01,
			'[]'::TEXT AS attributes_json`, schemaID)
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
