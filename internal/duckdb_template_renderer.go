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
// It generates a DuckDB WHERE clause (with optional dirty-ID exclusions), injects the
// clause and dirty-ids template helpers into params, then renders the template and
// returns the final SQL and combined args suitable for execution against DuckDB.
func BuildDuckDBQuery(tpl *template.Template, params any, q *FederatedAttributeQuery, dirtyIDs []uuid.UUID) (string, []any, error) {
	// Build where clause with exclusions (anti-join)
	whereClause, whereArgs, err := GenerateDuckDBWhereClauseWithExclusions(q, dirtyIDs)
	if err != nil {
		return "", nil, err
	}

	// Ensure params is a map so we can inject Anchor.Condition and dirty helpers
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
	anchor["Condition"] = whereClause

	// Inject HasDirtyIDs / DirtyIDsCSV for optional CTE rendering
	merged := MergeTemplateParamsWithDirtyIDs(m, dirtyIDs)

	// Render template and combine args (whereArgs first)
	return RenderDuckDBQuery(tpl, merged, whereArgs)
}
