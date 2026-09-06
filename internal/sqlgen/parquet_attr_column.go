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

// parquetAttrPlaceholder is the column ParquetAttrColumn substitutes when the
// fold empties a name ("", "[]", a lone backtick); a name like "[attr]"
// strips down onto it directly.
const parquetAttrPlaceholder = "attr"

// federatedDedupColumns are the visible-CTE columns that are dedup machinery,
// not data: a filter on either binds successfully and compares against the
// dedup rank, so it fails silently-wrong rather than loudly. Refused under
// any spelling, identity fold included (the keyset counterpart is
// federated.keysetRejectedColumns).
var federatedDedupColumns = map[string]struct{}{
	"rn":                   {},
	"source_tier_priority": {},
}

// ValidateUnregisteredParquetAttrColumn guards a filter attribute the schema
// cache does not know (#512). ValidateParquetAttrColumns cannot cover it: it
// checks registered attributes at registration, and an unregistered filter
// name is an arbitrary caller-supplied string that never went through it.
// The fold is lossy, so "created.at" lands on created_at and the DuckDB
// clause would silently filter on the creation timestamp instead of failing
// at the binder — the silent-wrong-answer family of #354.
//
// The rule mirrors federated.validateKeysetCursor (#509), judged on the
// FOLDED name because that is the identifier the generator emits, with the
// lookups lower-cased because DuckDB resolves unquoted identifiers
// case-insensitively. A non-identity fold onto the "attr" placeholder or onto
// any reserved parquet column is refused; the dedup machinery is refused
// under any spelling; an identity-up-to-case fold ("created_at",
// "Created_At") is admitted and left to fail at the binder or, on the
// federated route, at the PG EAV payload. Caller fault, so the error is a
// forma.InvalidInputf carrier that keeps the caller's spelling.
func ValidateUnregisteredParquetAttrColumn(attr, folded string) error {
	if strings.EqualFold(folded, parquetAttrPlaceholder) && !strings.EqualFold(attr, parquetAttrPlaceholder) {
		return forma.InvalidInputf(
			"filter attribute %q is not registered and folds onto the placeholder column %q, which would silently filter on a real attribute of that name: filter on a registered schema attribute",
			attr, folded)
	}
	key := strings.ToLower(folded)
	if _, ok := federatedDedupColumns[key]; ok {
		return forma.InvalidInputf(
			"filter attribute %q folds to %q, which is federated dedup machinery, not a queryable column: filter on a registered schema attribute",
			attr, folded)
	}
	if _, ok := reservedParquetColumns[key]; ok && !strings.EqualFold(attr, folded) {
		return forma.InvalidInputf(
			"filter attribute %q is not registered and folds to %q, a reserved system column, which would silently filter on that column instead of the attribute named: filter on a registered schema attribute, or name the system column %s directly",
			attr, folded, key)
	}
	return nil
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
