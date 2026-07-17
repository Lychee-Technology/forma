package sqlgen

import (
	"fmt"
	"sort"
	"strings"

	"github.com/lychee-technology/forma"
)

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

// reservedParquetColumns are column names the CDC export and federated read
// pipelines emit themselves. An attribute whose folded name lands on one
// (a nested property like "row.id" folds to "row_id") would either
// duplicate a SELECT-list column — binder ambiguity — or silently read the
// system value as the attribute. The set covers the parquet export columns
// (schema_id, row_id, changed_at, deleted_at, ltbase_*), the unified-CTE
// system columns (created_at, ver_ts, deleted_ts), the dedup machinery
// (source_tier_priority, rn), and the outer-select tail (attributes_json,
// total_records, total_pages, current_page).
var reservedParquetColumns = map[string]struct{}{
	"row_id":               {},
	"schema_id":            {},
	"changed_at":           {},
	"deleted_at":           {},
	"created_at":           {},
	"ver_ts":               {},
	"deleted_ts":           {},
	"source_tier_priority": {},
	"rn":                   {},
	"attributes_json":      {},
	"total_records":        {},
	"total_pages":          {},
	"current_page":         {},
	"ltbase_row_id":        {},
	"ltbase_schema_id":     {},
	"ltbase_created_at":    {},
	"ltbase_updated_at":    {},
	"ltbase_deleted_at":    {},
}

// ValidateParquetAttrColumns rejects attribute sets whose folded parquet
// column names land on a reserved system column or collide with each other
// (the fold is lossy: "contact.name" and "contact_name" both become
// contact_name). Schema registration calls it so an unusable schema is
// rejected before it accepts hot-tier writes; the CDC writer and the
// federated reader call it again as defense in depth. Plain operator
// error, never forma.ErrInvalidInput. Attributes are checked in sorted
// order so the error message is deterministic.
func ValidateParquetAttrColumns(cache forma.SchemaAttributeCache) error {
	names := make([]string, 0, len(cache))
	for name := range cache {
		names = append(names, name)
	}
	sort.Strings(names)

	colToAttr := make(map[string]string, len(names))
	for _, name := range names {
		col := ParquetAttrColumn(name)
		if _, ok := reservedParquetColumns[col]; ok {
			return fmt.Errorf(
				"attribute %q folds to parquet column %q, which is reserved for system columns; rename the attribute",
				name, col)
		}
		if prev, ok := colToAttr[col]; ok {
			return fmt.Errorf(
				"attributes %q and %q both map to parquet column %q; attribute names must remain distinct after identifier folding",
				prev, name, col)
		}
		colToAttr[col] = name
	}
	return nil
}
