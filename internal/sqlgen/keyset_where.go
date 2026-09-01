package sqlgen

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/lychee-technology/forma"
)

// generateKeysetWhereClause builds the composite keyset cursor predicate for
// the DuckDB dialect. It emits positional "?" placeholders — never "$n" —
// because the federated statement binds strictly by appearance order and
// DuckDB treats "$n" as an absolute parameter index, shifting every "?"
// after it (#161); the clause renders last in the visible CTE (#212) and
// its args are appended last. Prefix columns repeat across disjuncts, so
// each value is appended once per occurrence: an n-column cursor yields
// n(n+1)/2 args.
//
// Values bind verbatim — no type coercion, no metadata lookup. The caller
// owns exactness: BIGINT cursor columns require int64 values (a float64
// above 2^53 makes `BIGINT col > DOUBLE param` skip or duplicate one page
// row — #281 / #205 M-2). No continuation-token codec exists today; a
// future decoder must produce int64 for integer values (UseNumber /
// numutil.Int64Exact), pinned by TestKeysetArgsPreserveInt64Above2p53.
// The cursor must satisfy model.KeysetCursor.ValidateShape: values align
// one-for-one with Columns, and the last column is the row_id tiebreak. A
// misaligned cursor is an error rather than a NULL bind (#381 item 7).
func generateKeysetWhereClause(cursor *model.KeysetCursor, tableAlias string) (string, []interface{}, error) {
	if !cursor.IsActive() {
		return "1=1", nil, nil
	}
	// The federated seams validate this too (federated.validateKeysetCursor),
	// but the check is repeated here so a direct sqlgen caller cannot bind SQL
	// NULL for an unfilled arm and receive a silently empty page (#381 item 7).
	if err := cursor.ValidateShape(); err != nil {
		return "", nil, fmt.Errorf("render keyset where clause: %w", err)
	}

	// Cursor columns fold like every other column reference in this dialect
	// (#260/#381): the visible CTE projects each attribute under its
	// ParquetAttrColumn alias, so an unfolded "contact.annualIncome" would
	// parse as table "contact", column "annualIncome". The fold is the
	// identity on every visible-CTE system column, so this is a no-op for
	// row_id / created_at / ver_ts / deleted_ts cursors.
	colRef := func(attr string) string {
		col := ParquetAttrColumn(attr)
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
			parts = append(parts, fmt.Sprintf("%s = ?", colRef(cursor.Columns[j].Attribute)))
			args = append(args, cursor.Values[j])
		}
		parts = append(parts, fmt.Sprintf("%s %s ?", colRef(col.Attribute), op))
		args = append(args, cursor.Values[i])

		clauses = append(clauses, "("+strings.Join(parts, " AND ")+")")
	}

	if len(clauses) == 1 {
		return clauses[0], args, nil
	}
	return strings.Join(clauses, " OR "), args, nil
}

func keysetComparisonOp(direction forma.SortOrder, mode model.KeysetCursorMode) string {
	isAfter := mode == model.KeysetCursorModeAfter
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

// buildKeysetOrderBy renders the keyset ORDER BY. Columns fold through
// ParquetAttrColumn for the same reason generateKeysetWhereClause's do: the
// ORDER BY runs against the visible CTE, whose attribute columns carry folded
// names (#260/#381). It must stay consistent with the WHERE clause — an
// ORDER BY disagreeing with the cursor predicate paginates incoherently.
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
		parts = append(parts, fmt.Sprintf("%s %s", ParquetAttrColumn(col.Attribute), dir))
	}
	return strings.Join(parts, ", ")
}
