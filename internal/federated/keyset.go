package federated

import (
	"fmt"

	"github.com/lychee-technology/forma/internal/model"
)

// This file holds the keyset *validation* seams used by the federated
// pagination path. Keyset SQL generation lives in internal/sqlgen
// (keyset_where.go): it renders positional `?` placeholders because DuckDB
// treats `$n` as an absolute parameter index and shifts every `?` after it
// (#161/#212). An older `$n`-emitting copy of that codegen used to live here
// with no production callers; it was deleted in #217 so the repo carries
// exactly one keyset codegen.

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
