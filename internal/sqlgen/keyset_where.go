package sqlgen

import (
	"fmt"
	"strings"

	"github.com/google/uuid"

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
// Every parameter renders as CAST(? AS <export type>), the type cursor
// normalization (sortkey.NormalizeCursor) recorded on the column beside a
// boundary of that type (design.md §6.1, One boundary type per cursor
// column). On a normalized value the cast is the identity, and it stops
// DuckDB from picking the comparison type from the bound value: with a
// float64 9.6, `10::BIGINT > ?` promotes the column to DOUBLE and holds,
// while `10::BIGINT > CAST(? AS BIGINT)` does not. The hot leg's Go
// predicate compares the same normalized value, so both legs answer alike.
// A column without an export type has not been normalized and is refused.
// A uuid boundary binds as its canonical text, which the cast reads back.
//
// deleted_ts renders as COALESCE(deleted_ts, 0), here and in the keyset
// ORDER BY: every row a page returns is live, and a live row holds 0 or
// NULL by its encoding (a hot row, a pre-#274 delta row), so the column
// reads 0 on every row and on both sides of a flush (design.md §6.1,
// deleted_ts reads 0). The dedup window and the outer select keep the raw
// column.
//
// The cursor must satisfy model.KeysetCursor.ValidateShape: values align
// one-for-one with Columns and none of them is nil, the last column is the
// row_id tiebreak, and the mode and directions are the enum values this file
// compares against. A cursor that would bind SQL NULL, or that the operator
// choice below would read as its fall-through default, is an error rather
// than a quietly wrong page (#381 item 7 and its two review follow-ups).
func generateKeysetWhereClause(cursor *model.KeysetCursor) (string, []interface{}, error) {
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
	//
	// The clause always renders against the unqualified columns of visible, so
	// there is no table alias to prefix. A tableAlias parameter used to exist:
	// both callers passed "", and its branch concatenated WITHOUT a separator
	// ("visible" + "row_id" = "visiblerow_id"), so the first caller to pass a
	// real alias would have emitted a broken identifier. Removed rather than
	// fixed — the qualification it promised has no use here (#381).
	params := make([]string, len(cursor.Columns))
	for i, col := range cursor.Columns {
		param, err := keysetParam(col)
		if err != nil {
			return "", nil, fmt.Errorf("render keyset where clause: %w", err)
		}
		params[i] = param
	}

	var clauses []string
	var args []interface{}

	for i := 0; i < len(cursor.Columns); i++ {
		col := cursor.Columns[i]
		op := keysetComparisonOp(col.Direction, cursor.Mode)

		var parts []string
		for j := 0; j < i; j++ {
			parts = append(parts, fmt.Sprintf("%s = %s", keysetColumnRef(cursor.Columns[j].Attribute), params[j]))
			args = append(args, keysetBindValue(cursor.Values[j]))
		}
		parts = append(parts, fmt.Sprintf("%s %s %s", keysetColumnRef(col.Attribute), op, params[i]))
		args = append(args, keysetBindValue(cursor.Values[i]))

		clauses = append(clauses, "("+strings.Join(parts, " AND ")+")")
	}

	if len(clauses) == 1 {
		return clauses[0], args, nil
	}
	return strings.Join(clauses, " OR "), args, nil
}

// keysetComparisonOp picks the continuation operator. The two switches below
// are exhaustive rather than defaulting: ValidateShape has already refused any
// mode other than after and any direction outside {asc, desc, ""}, so the
// fall-through arms are reached only by the values they name. Before that
// rule existed, an unset mode silently meant BEFORE and an unrecognised
// direction silently meant ASC, and both answered a successful page in the
// wrong direction (#381).
//
// The before arms are currently unreachable: ValidateShape refuses a before
// cursor because this flip is only half of a backward page — buildKeysetOrderBy
// still fetches in the forward order under the LIMIT, so `key < 7 ORDER BY key
// ASC LIMIT 2` answers [1, 2] rather than [5, 6]. They are kept because they
// are the correct half; #513 adds the reversed fetch order and re-admits the
// mode.
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
	return strings.Join(keysetOrderTerms(cursor), ", ")
}

// keysetOrderTerms returns buildKeysetOrderBy's terms, "<column> <dir>" each,
// so the parquet-only template can render the same order with an explicit
// NULLS LAST on every term.
func keysetOrderTerms(cursor *model.KeysetCursor) []string {
	if !cursor.IsActive() {
		return []string{"created_at DESC"}
	}
	parts := make([]string, 0, len(cursor.Columns))
	for _, col := range cursor.Columns {
		dir := "ASC"
		if col.Direction == forma.SortOrderDesc {
			dir = "DESC"
		}
		parts = append(parts, fmt.Sprintf("%s %s", keysetColumnRef(col.Attribute), dir))
	}
	return parts
}

// keysetDeletedTS is the system column a cursor may name whose NULL reads
// as 0 (design.md §6.1, deleted_ts reads 0).
const keysetDeletedTS = "deleted_ts"

// keysetColumnRef is how the keyset predicate and the keyset ORDER BY read a
// cursor column: its ParquetAttrColumn fold, and COALESCE(deleted_ts, 0) for
// deleted_ts, matched case-insensitively as DuckDB resolves the identifier.
func keysetColumnRef(attribute string) string {
	ref := ParquetAttrColumn(attribute)
	if DuckDBEqualFold(attribute, keysetDeletedTS) {
		return fmt.Sprintf("COALESCE(%s, 0)", ref)
	}
	return ref
}

// keysetExportTypes are the export types a normalized cursor column carries.
// The renderer interpolates the type into SQL, so this set is also the
// barrier that keeps anything else out of the statement.
var keysetExportTypes = map[string]struct{}{
	"BIGINT": {}, "INTEGER": {}, "SMALLINT": {}, "DOUBLE": {},
	"VARCHAR": {}, "BOOLEAN": {}, "UUID": {},
}

// keysetParam renders one boundary parameter, cast to its column's export
// type.
func keysetParam(col model.KeysetColumn) (string, error) {
	if _, ok := keysetExportTypes[col.ExportType]; !ok {
		return "", fmt.Errorf("keyset cursor column %q carries export type %q, which is not one of BIGINT, INTEGER, SMALLINT, DOUBLE, VARCHAR, BOOLEAN or UUID: the cursor was not normalized (sortkey.NormalizeCursor), so its boundary could compare at a type the hot leg does not", col.Attribute, col.ExportType)
	}
	return fmt.Sprintf("CAST(? AS %s)", col.ExportType), nil
}

// keysetBindValue binds a uuid boundary as its canonical text, which
// CAST(? AS UUID) reads back; every other normalized value binds as is.
func keysetBindValue(v interface{}) interface{} {
	if u, ok := v.(uuid.UUID); ok {
		return u.String()
	}
	return v
}
