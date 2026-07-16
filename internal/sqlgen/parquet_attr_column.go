package sqlgen

import "strings"

// parquetAttrReplacer folds characters that DuckDB would misparse in an
// unquoted identifier. Dots are the load-bearing case: an unfolded
// "contact.annualIncome" parses as table "contact", column "annualIncome"
// (#260).
var parquetAttrReplacer = strings.NewReplacer("`", "", ".", "_", " ", "_", "[", "", "]", "")

// ParquetAttrColumn maps a logical attribute name to its physical column
// name — the single naming contract shared by the CDC parquet writer
// (internal/cdc, which aliases every exported attribute through it) and the
// federated DuckDB reader (this package, whose unified CTE columns must
// carry the same names). The two sides cannot diverge: LOGICAL_WHERE_CLAUSE
// renders both against raw read_parquet (physical columns) and against the
// visible CTE (unified columns), so unified name and parquet name must be
// the same string. The mapping must also stay byte-stable across releases:
// parquet files already flushed to S3 were written with it.
func ParquetAttrColumn(attr string) string {
	col := parquetAttrReplacer.Replace(attr)
	if col == "" {
		return "attr"
	}
	return col
}
