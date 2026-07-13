package federated

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma/internal/model"

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
func generateKeysetWhereClause(cursor *model.KeysetCursor, tableAlias string, paramOffset int) (string, []interface{}) {
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
func keysetComparisonOp(direction forma.SortOrder, mode model.KeysetCursorMode) string {
	isAfter := mode == model.KeysetCursorModeAfter
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

// extractCursorFromRecord builds a model.KeysetCursor from the last row of a page.
// The columns must match the query's sort columns and MUST end with a row_id
// tiebreaker: a cursor whose final column is a non-unique key silently skips
// every row tied at the boundary (see validateKeysetTiebreak, #183). That
// requirement is no longer merely documented — both federated seams reject a
// cursor lacking the trailing row_id before it reaches the renderer.
func extractCursorFromRecord(record *model.PersistentRecord, columns []model.KeysetColumn) *model.KeysetCursor {
	if record == nil || len(columns) == 0 {
		return nil
	}
	values := make([]interface{}, len(columns))
	for i, col := range columns {
		values[i] = recordColumnValue(record, col)
	}
	return &model.KeysetCursor{
		Columns: columns,
		Values:  values,
		Mode:    model.KeysetCursorModeAfter,
	}
}

// recordColumnValue extracts a single column value from a model.PersistentRecord.
// It handles special columns like "row_id" and "created_at" transparently,
// and falls back to EAV attribute lookup for other columns.
func recordColumnValue(record *model.PersistentRecord, col model.KeysetColumn) interface{} {
	switch col.Attribute {
	case "row_id":
		return record.RowID.String()
	case "created_at":
		return record.CreatedAt
	case "ver_ts", "updated_at":
		return record.UpdatedAt
	case "schema_id":
		return int64(record.SchemaID)
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
// NOTE: This is currently a stub - full EAV cursor extraction requires schema cache.
func eavColumnValue(record *model.PersistentRecord, attrName string) interface{} {
	// EAV attributes are identified by name only here; the caller is responsible
	// for ensuring the attribute exists in the schema and is present in the record.
	// We search OtherAttributes by matching AttrID via a helper lookup.
	// For now, return nil — attribute-level extraction requires schema cache.
	return nil
}

// isSupportedKeysetColumn returns true if the attribute is supported for keyset cursor extraction.
// Currently only system columns and known main table columns are supported.
// EAV-only attributes require schema cache integration and are not yet supported.
func isSupportedKeysetColumn(attribute string) bool {
	switch attribute {
	case "row_id", "created_at", "updated_at", "deleted_at", "ver_ts", "deleted_ts", "schema_id":
		return true
	default:
		// EAV attributes and main column attributes are not yet supported
		return false
	}
}

// validateKeysetColumns checks if all keyset cursor columns are supported.
// Returns an error if any column is unsupported.
func validateKeysetColumns(columns []model.KeysetColumn) error {
	for _, col := range columns {
		if !isSupportedKeysetColumn(col.Attribute) {
			return fmt.Errorf("keyset pagination on attribute %q is not supported (EAV attributes require schema cache)", col.Attribute)
		}
	}
	return nil
}

// validateKeysetTiebreak enforces the keyset caller contract: the final cursor
// column MUST be row_id. The continuation predicate for the last cursor key is
// a strict inequality, so a cursor ending on a non-unique key (created_at, a
// business attribute, ...) excludes every row sharing that key's value at the
// page boundary — an entire tie group is silently skipped (#183). row_id is the
// only version-invariant unique column, so ending the cursor there gives the
// composite key a total order and makes each boundary tie resolvable. Empty and
// nil cursors are a no-op (the open first page carries no tiebreak obligation).
// Mirrors validateKeysetColumns: a plain read-path error, not an
// ErrInvalidInput-wrapped write-path validation.
func validateKeysetTiebreak(cursor *model.KeysetCursor) error {
	if cursor == nil || len(cursor.Columns) == 0 {
		return nil
	}
	last := cursor.Columns[len(cursor.Columns)-1].Attribute
	if last != "row_id" {
		return fmt.Errorf("keyset cursor final column is %q, expected \"row_id\": a cursor not ending on the unique row_id tiebreak silently skips every row tied on the composite key at the page boundary", last)
	}
	return nil
}

// buildKeysetOrderBy generates a dynamic ORDER BY clause from keyset columns.
// This produces the ORDER BY fragment to use instead of the hardcoded
// "created_at DESC" when keyset pagination is active.
func buildKeysetOrderBy(cursor *model.KeysetCursor) string {
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
