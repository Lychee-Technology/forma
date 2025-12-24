package internal

import (
	"strings"

	"github.com/google/uuid"
)

// RenderDirtyIDsValuesCSV builds a VALUES-list fragment suitable for embedding
// into a SQL template like: VALUES {{.DirtyIDsCSV}} .
// Example output for two ids: "('id1'),('id2')"
func RenderDirtyIDsValuesCSV(dirtyIDs []uuid.UUID) string {
	if len(dirtyIDs) == 0 {
		return ""
	}
	parts := make([]string, 0, len(dirtyIDs))
	for _, id := range dirtyIDs {
		// cast to text in template via SELECT CAST(value AS VARCHAR) if desired
		parts = append(parts, "('"+id.String()+"')")
	}
	return strings.Join(parts, ",")
}

// MergeTemplateParamsWithDirtyIDs injects two helper fields into the provided
// params map (if map[string]any): HasDirtyIDs (bool) and DirtyIDsCSV (string).
// This is a convenience for callers that render the DuckDB SQL template and
// want to optionally include a dirty_ids CTE.
func MergeTemplateParamsWithDirtyIDs(params any, dirtyIDs []uuid.UUID) any {
	m, ok := params.(map[string]any)
	if !ok {
		return params
	}
	if len(dirtyIDs) == 0 {
		m["HasDirtyIDs"] = false
		m["DirtyIDsCSV"] = ""
		return m
	}
	m["HasDirtyIDs"] = true
	m["DirtyIDsCSV"] = RenderDirtyIDsValuesCSV(dirtyIDs)
	return m
}
