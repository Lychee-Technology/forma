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
func generateKeysetWhereClause(cursor *model.KeysetCursor, tableAlias string) (string, []interface{}) {
	if cursor == nil || len(cursor.Columns) == 0 {
		return "1=1", nil
	}

	colRef := func(col string) string {
		if tableAlias == "" {
			return col
		}
		return tableAlias + col
	}
	valueAt := func(i int) interface{} {
		if i < len(cursor.Values) {
			return cursor.Values[i]
		}
		return nil
	}

	var clauses []string
	var args []interface{}

	for i := 0; i < len(cursor.Columns); i++ {
		col := cursor.Columns[i]
		op := keysetComparisonOp(col.Direction, cursor.Mode)

		var parts []string
		for j := 0; j < i; j++ {
			parts = append(parts, fmt.Sprintf("%s = ?", colRef(cursor.Columns[j].Attribute)))
			args = append(args, valueAt(j))
		}
		parts = append(parts, fmt.Sprintf("%s %s ?", colRef(col.Attribute), op))
		args = append(args, valueAt(i))

		clauses = append(clauses, "("+strings.Join(parts, " AND ")+")")
	}

	if len(clauses) == 1 {
		return clauses[0], args
	}
	return strings.Join(clauses, " OR "), args
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
