package sqlgen

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

func RenderDirtyIDsValuesCSV(dirtyIDs []uuid.UUID) string {
	if len(dirtyIDs) == 0 {
		return ""
	}
	parts := make([]string, len(dirtyIDs))
	for i, id := range dirtyIDs {
		parts[i] = fmt.Sprintf("('%s')", id.String())
	}
	return strings.Join(parts, ",")
}

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
