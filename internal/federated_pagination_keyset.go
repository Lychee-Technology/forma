package internal

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
)

// generateKeysetWhereClause builds a composite keyset WHERE clause for
// cursor-based pagination. The clause filters rows based on the cursor
// position so only rows after (or before) the cursor are returned.
//
// The algorithm produces a disjunction of conjunctions, one for each column
// prefix. For a cursor on (col1 ASC, col2 DESC, col3 ASC) in "after" mode:
//
//	(col1 > $1)
//	 OR (col1 = $1 AND col2 < $2)
//	 OR (col1 = $1 AND col2 = $2 AND col3 > $3)
//
// The paramOffset controls the starting parameter index ($1, $2, ...).
// The tableAlias is used as a prefix for column references (e.g. "unified.").
//
// Returns the combined WHERE clause string and the argument slice in order.
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
	paramIdx := paramOffset

	for i := 0; i < len(cursor.Columns); i++ {
		col := cursor.Columns[i]
		op := keysetComparisonOp(col.Direction, cursor.Mode)

		var parts []string
		// Equality constraints for all prefix columns
		for j := 0; j < i; j++ {
			parts = append(parts, fmt.Sprintf("%s = $%d",
				colRef(cursor.Columns[j].Attribute), paramOffset+j+1))
		}
		// Inequality for the current column
		parts = append(parts, fmt.Sprintf("%s %s $%d",
			colRef(col.Attribute), op, paramOffset+i+1))

		clauses = append(clauses, "("+strings.Join(parts, " AND ")+")")

		// Arguments: each column contributes its value once
		// (collected after the loop to avoid duplicates)
		_ = paramIdx
	}

	// Collect all argument values in order
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

// keysetComparisonOp returns the SQL comparison operator for a keyset column
// given its sort direction and the cursor mode.
func keysetComparisonOp(direction forma.SortOrder, mode KeysetCursorMode) string {
	isAfter := mode == KeysetCursorModeAfter
	switch direction {
	case forma.SortOrderDesc:
		if isAfter {
			return "<"
		}
		return ">"
	default:
		// ASC or no explicit sort order
		if isAfter {
			return ">"
		}
		return "<"
	}
}

// extractCursorFromRecord builds a KeysetCursor from the last row of a page.
// The columns must match the query's sort columns plus a row_id tiebreaker.
func extractCursorFromRecord(record *PersistentRecord, columns []KeysetColumn) *KeysetCursor {
	if record == nil || len(columns) == 0 {
		return nil
	}
	values := make([]interface{}, len(columns))
	for i, col := range columns {
		values[i] = recordColumnValue(record, col)
	}
	return &KeysetCursor{
		Columns: columns,
		Values:  values,
		Mode:    KeysetCursorModeAfter,
	}
}

// recordColumnValue extracts a single column value from a PersistentRecord.
// It handles special columns like "row_id" and "created_at" transparently,
// and falls back to EAV attribute lookup for other columns.
func recordColumnValue(record *PersistentRecord, col KeysetColumn) interface{} {
	switch col.Attribute {
	case "row_id":
		return record.RowID.String()
	case "created_at":
		return record.CreatedAt
	case "ver_ts", "updated_at":
		return record.UpdatedAt
	case "deleted_ts", "deleted_at":
		if record.DeletedAt != nil {
			return *record.DeletedAt
		}
		return nil
	default:
		return eavColumnValue(record, col.Attribute)
	}
}

// eavColumnValue looks up an EAV attribute value from the record's
// OtherAttributes slice. Returns nil if not found.
func eavColumnValue(record *PersistentRecord, attrName string) interface{} {
	// EAV attributes are identified by name only here; the caller is responsible
	// for ensuring the attribute exists in the schema and is present in the record.
	// We search OtherAttributes by matching AttrID via a helper lookup.
	// For now, return nil — attribute-level extraction requires schema cache.
	return nil
}

// buildKeysetOrderBy generates a dynamic ORDER BY clause from keyset columns.
// This produces the ORDER BY fragment to use instead of the hardcoded
// "created_at DESC" when keyset pagination is active.
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
