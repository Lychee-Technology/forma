package internal

import (
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
		whereArgs = make([]any, 0, len(dual.DuckArgs))
		if len(dual.DuckArgs) > 0 {
			whereArgs = append(whereArgs, dual.DuckArgs...)
		}
		// Append dirty exclusions
		if len(dirtyIDs) > 0 {
			var exclArgs []any
			whereClause, exclArgs = AppendDirtyExclusion(whereClause, dirtyIDs)
			whereArgs = append(whereArgs, exclArgs...)
		}
		anchor["Condition"] = whereClause

		// Inject PgMainClause for inspection / postgres_scan integration
		m["PgMainClause"] = dual.PgMainClause
		m["PgMainArgs"] = dual.PgMainArgs
		m["HasPgMainClause"] = dual.PgMainClause != ""

		merged := MergeTemplateParamsWithDirtyIDs(m, dirtyIDs)
		return RenderDuckDBQuery(tpl, merged, whereArgs)
	}

	// Legacy path
	whereClause, whereArgs, err = GenerateDuckDBWhereClauseWithExclusions(q, dirtyIDs)
	if err != nil {
		return "", nil, err
	}
	anchor["Condition"] = whereClause

	merged := MergeTemplateParamsWithDirtyIDs(m, dirtyIDs)
	return RenderDuckDBQuery(tpl, merged, whereArgs)
}
