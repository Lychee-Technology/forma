package sqlgen

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
)

func generateKeysetWhereClause(cursor *KeysetCursor, tableAlias string, paramOffset int) (string, []interface{}) {
	if cursor == nil || len(cursor.Columns) == 0 {
		return "1=1", nil
	}

	colRef := func(col string) string {
		if tableAlias == "" {
			return col
		}
		return tableAlias + col
	}

	var clauses []string
	var args []interface{}

	for i := 0; i < len(cursor.Columns); i++ {
		col := cursor.Columns[i]
		op := keysetComparisonOp(col.Direction, cursor.Mode)

		var parts []string
		for j := 0; j < i; j++ {
			parts = append(parts, fmt.Sprintf("%s = $%d", colRef(cursor.Columns[j].Attribute), paramOffset+j+1))
		}
		parts = append(parts, fmt.Sprintf("%s %s $%d", colRef(col.Attribute), op, paramOffset+i+1))

		clauses = append(clauses, "("+strings.Join(parts, " AND ")+")")
	}

	for i := range cursor.Columns {
		if i < len(cursor.Values) {
			args = append(args, cursor.Values[i])
		} else {
			args = append(args, nil)
		}
	}

	if len(clauses) == 1 {
		return clauses[0], args
	}
	return strings.Join(clauses, " OR "), args
}

func keysetComparisonOp(direction forma.SortOrder, mode KeysetCursorMode) string {
	isAfter := mode == KeysetCursorModeAfter
	switch direction {
	case forma.SortOrderDesc:
		if isAfter {
			return "<"
		}
		return ">"
	default:
		if isAfter {
			return ">"
		}
		return "<"
	}
}

func buildKeysetOrderBy(cursor *KeysetCursor) string {
	if cursor == nil || len(cursor.Columns) == 0 {
		return "created_at DESC"
	}
	var parts []string
	for _, col := range cursor.Columns {
		dir := "ASC"
		if col.Direction == forma.SortOrderDesc {
			dir = "DESC"
		}
		parts = append(parts, fmt.Sprintf("%s %s", col.Attribute, dir))
	}
	return strings.Join(parts, ", ")
}
