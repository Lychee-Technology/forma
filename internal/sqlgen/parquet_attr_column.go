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
		return ParquetAttrPlaceholder
	}
	return col
}

// reservedParquetColumns are column names the CDC export and federated read
// pipelines emit themselves. An attribute whose folded name lands on one
// (a nested property like "row.id" folds to "row_id") would either
// duplicate a SELECT-list column — binder ambiguity — or silently read the
// system value as the attribute. The set covers the parquet export columns
// (schema_id, row_id, changed_at, deleted_at, ltbase_*), the unified-CTE
// system columns (created_at, ver_ts, deleted_ts), the outer-select tail
// (attributes_json, total_records, total_pages, current_page), and the dedup
// machinery, composed in from federatedDedupColumns rather than restated as
// literals. ValidateParquetAttrColumns reads only this set, so a dedup column
// restated here could be left behind when the source gained one, and an
// attribute folding onto the new column would register and then bind
// against the dedup rank (#552). Package initialisation orders
// federatedDedupColumns first because this declaration depends on it;
// TestReservedParquetColumnsIsUnchanged pins the resulting contents.
var reservedParquetColumns = buildReservedParquetColumns(federatedDedupColumns)

// buildReservedParquetColumns unions the system columns with the dedup set it
// is handed. The dedup set is a parameter rather than the package variable so
// the composition can be exercised with a synthetic column in tests without
// editing the guard every other test runs against.
func buildReservedParquetColumns(dedup map[string]struct{}) map[string]struct{} {
	cols := map[string]struct{}{
		"row_id":            {},
		"schema_id":         {},
		"changed_at":        {},
		"deleted_at":        {},
		"created_at":        {},
		"ver_ts":            {},
		"deleted_ts":        {},
		"attributes_json":   {},
		"total_records":     {},
		"total_pages":       {},
		"current_page":      {},
		"ltbase_row_id":     {},
		"ltbase_schema_id":  {},
		"ltbase_created_at": {},
		"ltbase_updated_at": {},
		"ltbase_deleted_at": {},
	}
	for col := range dedup {
		cols[col] = struct{}{}
	}
	return cols
}

// ParquetAttrPlaceholder is the column ParquetAttrColumn substitutes when the
// fold empties a name ("", "[]", a lone backtick); a name like "[attr]"
// strips down onto it directly. ParquetAttrColumn itself returns it rather
// than a bare literal, and it is exported because internal/federated's cursor
// guard enforces the same placeholder rule: one definition of the string, not
// one per package (#531). A const, so a consumer cannot reassign it.
const ParquetAttrPlaceholder = "attr"

// federatedDedupColumns are the visible-CTE columns that are dedup machinery,
// not data: a filter on either binds successfully and compares against the
// dedup rank, so it fails silently-wrong rather than loudly. Refused under
// any spelling, identity fold included.
//
// This is the single definition of the set. internal/federated applies the
// same rule to keyset cursors and derives its own set from
// FederatedDedupColumns rather than redeclaring one (#531), and
// reservedParquetColumns composes it in rather than restating it (#552), so
// a column added here reaches the filter, cursor, and registration guards.
var federatedDedupColumns = map[string]struct{}{
	"rn":                   {},
	"source_tier_priority": {},
}

// FederatedDedupColumns returns the dedup-machinery column set. Its entries
// are stored lower-cased, matching the keys both guards look up: each folds
// the caller's name and passes the result through DuckDBFoldIdentifier
// before the map lookup, because DuckDB resolves unquoted identifiers
// case-insensitively and ASCII-only.
//
// The map is a fresh copy on every call: the set is package state that decides
// whether a query is refused, so handing out the live map would let any
// importer edit the guard rather than read it.
func FederatedDedupColumns() map[string]struct{} {
	cols := make(map[string]struct{}, len(federatedDedupColumns))
	for col := range federatedDedupColumns {
		cols[col] = struct{}{}
	}
	return cols
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
// lookups keyed on DuckDBFoldIdentifier and the comparisons made with
// DuckDBEqualFold because DuckDB resolves unquoted identifiers
// case-insensitively — and ASCII-only, so a non-ASCII name the engine keeps
// distinct from a system column ("row.İd", whose Unicode lower-case is
// "row_id") is not refused as one (#550). A non-identity fold onto the "attr"
// placeholder or onto any reserved parquet column is refused; the dedup
// machinery is refused under any spelling; an identity-up-to-case fold
// ("created_at", "Created_At") is admitted and left to fail at the binder or,
// on the federated route, at the PG EAV payload. Caller fault, so the error
// is a forma.InvalidInputf carrier that keeps the caller's spelling.
func ValidateUnregisteredParquetAttrColumn(attr, folded string) error {
	if DuckDBEqualFold(folded, ParquetAttrPlaceholder) && !DuckDBEqualFold(attr, ParquetAttrPlaceholder) {
		return forma.InvalidInputf(
			"filter attribute %q is not registered and folds onto the placeholder column %q, which would silently filter on a real attribute of that name: filter on a registered schema attribute",
			attr, folded)
	}
	key := DuckDBFoldIdentifier(folded)
	if _, ok := federatedDedupColumns[key]; ok {
		return forma.InvalidInputf(
			"filter attribute %q folds to %q, which is federated dedup machinery, not a queryable column: filter on a registered schema attribute",
			attr, folded)
	}
	if _, ok := reservedParquetColumns[key]; ok && !DuckDBEqualFold(attr, folded) {
		return forma.InvalidInputf(
			"filter attribute %q is not registered and folds to %q, a reserved system column, which would silently filter on that column instead of the attribute named: filter on a registered schema attribute",
			attr, folded)
	}
	return nil
}

// DuckDBFoldIdentifier lower-cases a folded column name the way DuckDB
// resolves an unquoted identifier: ASCII only. strings.ToLower is the wrong
// primitive here — it applies Unicode case mappings DuckDB does not, so it
// merges identifiers DuckDB keeps distinct and would reject valid schemas
// (#532). Verified against the pinned DuckDB (see
// TestDuckDBIdentifierFoldIsASCIIOnly, which asserts this function against
// the engine rather than against the documentation): "Á"/"á", "Ж"/"ж",
// "ẞ"/"ß" and "cafÉ"/"café" are distinct columns, and U+212A KELVIN SIGN
// does not resolve onto "k". The reserved half matters too: Go maps U+0130
// LATIN CAPITAL LETTER I WITH DOT ABOVE onto "i", so a Unicode fold would
// collapse the legitimate attribute "row_İd" onto the reserved "row_id" and
// fail registry construction for every schema in the directory.
//
// It is the one identifier-folding primitive behind every seam of the
// naming contract (docs/federated-query/design.md §4.4): the registration
// guard (ValidateParquetAttrColumns), the unregistered-filter guard
// (ValidateUnregisteredParquetAttrColumn) and, exported for that reason,
// internal/federated's keyset cursor guard (#550).
func DuckDBFoldIdentifier(col string) string {
	var folded []byte
	for i := 0; i < len(col); i++ {
		c := col[i]
		if c < 'A' || c > 'Z' {
			continue
		}
		if folded == nil {
			folded = []byte(col)
		}
		folded[i] = c + ('a' - 'A')
	}
	if folded == nil {
		return col
	}
	return string(folded)
}

// DuckDBEqualFold reports whether DuckDB resolves a and b onto the same
// unquoted identifier: equal after DuckDBFoldIdentifier, with no allocation.
// It is the counterpart of strings.EqualFold under the engine's rule, and
// the two disagree in both directions: strings.EqualFold merges U+212A
// KELVIN SIGN with "k" and U+017F LATIN SMALL LETTER LONG S with "s", which
// DuckDB keeps distinct, and it is what the guards compared with before
// #550. Pinned against the engine beside DuckDBFoldIdentifier.
func DuckDBEqualFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// ValidateParquetAttrColumns rejects attribute sets whose folded parquet
// column names land on a reserved system column or collide with each other
// (the fold is lossy: "contact.name" and "contact_name" both become
// contact_name). DuckDB resolves unquoted identifiers case-insensitively, so
// comparisons run on DuckDBFoldIdentifier keys — ASCII-only, matching the
// engine, so non-ASCII names DuckDB keeps distinct are not merged here —
// while errors preserve the caller's spelling and name the resolved column
// beside it, so a case-variant rejection stays legible against the reserved
// set an operator can grep.
// Schema registration calls it so an unusable schema is
// rejected before it accepts hot-tier writes; the CDC writer and the
// federated reader call it again as defense in depth. Plain operator
// error, never forma.ErrInvalidInput. Attributes are checked in sorted
// order so the error message is deterministic.
//
// Retired entries (#342) are in scope for the collision half only (#549).
// A fold collision is physical: read_parquet(..., union_by_name=true) merges
// case-variant column names from different files into one column and the
// compaction merge's SELECT * rewrites them as one, so a retired attribute's
// flushed column interferes with a same-folding column whether or not
// anything projects it. A reserved-column hit is a projection hazard
// instead: no flushed file holds two spellings of a system column — the
// exporter emits ltbase_created_at, never created_at, and DuckDB dedups a
// case-variant of a physical export column ("Row_Id" beside row_id) to
// Row_Id_1 at COPY time — so the ambiguity arises only when the attribute is
// projected beside the system column, and every projection is built from the
// active cache. A retired entry is never projected, sits unreferenced in the
// raw SELECT * scans, and is therefore exempt (pinned against the engine by
// TestParquetScan_RetiredReservedColumnIsInert); failing boot for it would
// impose a migration that fixes nothing.
func ValidateParquetAttrColumns(cache forma.SchemaAttributeCache) error {
	names := make([]string, 0, len(cache))
	for name := range cache {
		names = append(names, name)
	}
	sort.Strings(names)

	colToAttr := make(map[string]foldedAttr, len(names))
	for _, name := range names {
		cur := foldedAttr{name: name, col: ParquetAttrColumn(name), meta: cache[name]}
		key := DuckDBFoldIdentifier(cur.col)
		if _, ok := reservedParquetColumns[key]; ok && !cur.meta.Retired {
			return reservedParquetColumnError(cur.name, cur.col, key)
		}
		if prev, ok := colToAttr[key]; ok {
			return foldedColumnCollisionError(prev, cur, key)
		}
		colToAttr[key] = cur
	}
	return nil
}

// foldedAttr is one attribute as the guard sees it: the caller's spelling,
// the parquet column it folds to, and the metadata that says whether it is a
// retired ledger entry.
type foldedAttr struct {
	name string
	col  string
	meta forma.AttributeMetadata
}

// ledger describes a retired entry the way the remedy needs it: the id and
// valueType are what a migration must carry over (#342).
func (a foldedAttr) ledger() string {
	return fmt.Sprintf("%q (id %d, valueType %s)", a.name, a.meta.AttributeID, a.meta.ValueType)
}

// retiredLedgerMigration is the remedy every retired-entry rejection ends
// with. A retired entry is the attributeID ledger for values already flushed
// under its folded column, so "rename the attribute" is not available to it:
// a rename in place desynchronizes the ledger from the flushed data and from
// the #294-preserved EAV rows the id still owns. The only sound fix moves the
// flushed column and the ledger entry together (#549).
const retiredLedgerMigration = "migrate the flushed parquet column and the ledger entry to a new name together, keeping the attributeID"

// reservedParquetColumnError explains a reserved-column rejection. When the
// folded name is already lower case it is itself the reserved entry and the
// message says so plainly; when the caller's case differs, the message must
// also name the reserved column the identifier resolves onto, because that
// lower-cased name — not the caller's spelling — is what an operator finds in
// reservedParquetColumns (#532). Only active entries reach it (#549).
func reservedParquetColumnError(name, col, key string) error {
	if col == key {
		return fmt.Errorf(
			"attribute %q folds to parquet column %q, which is reserved for system columns; rename the attribute",
			name, col)
	}
	return fmt.Errorf(
		"attribute %q folds to parquet column %q, which DuckDB resolves case-insensitively onto the reserved system column %q; rename the attribute",
		name, col, key)
}

// foldedColumnCollisionError explains an intra-schema collision. Two
// attributes that fold to the same string share one parquet column and the
// message says exactly that; two that differ only in case keep distinct
// parquet columns — ParquetAttrColumn is byte-stable — and are merged only by
// DuckDB's case-insensitive identifier matching, so the message names both
// columns rather than claiming a single one that no parquet footer holds.
// When a retired entry is involved the remedy changes with it (#549): the
// retired side is the ledger and is never the one to rename, so a
// retired/active pair directs the rename at the active attribute and a
// retired/retired pair gets the migration remedy for both.
func foldedColumnCollisionError(prev, cur foldedAttr, key string) error {
	// Put the retired side first so the message reads "retired ... and
	// active ..." whichever way the sorted walk met them.
	if cur.meta.Retired && !prev.meta.Retired {
		prev, cur = cur, prev
	}
	shared := fmt.Sprintf("both map to parquet column %q", cur.col)
	if prev.col != cur.col {
		shared = fmt.Sprintf(
			"map to parquet columns %q and %q, which DuckDB resolves case-insensitively onto the same column %q",
			prev.col, cur.col, key)
	}
	switch {
	case prev.meta.Retired && cur.meta.Retired:
		return fmt.Errorf(
			"retired attributes %s and %s %s; both entries are the attributeID ledger for values already flushed under their columns, so neither can be renamed alone: %s",
			prev.ledger(), cur.ledger(), shared, retiredLedgerMigration)
	case prev.meta.Retired:
		return fmt.Errorf(
			"retired attribute %s and active attribute %q %s; the retired entry is the attributeID ledger for values already flushed under its column and cannot be renamed alone: rename the active attribute %q, or %s",
			prev.ledger(), cur.name, shared, cur.name, retiredLedgerMigration)
	}
	return fmt.Errorf(
		"attributes %q and %q %s; attribute names must remain distinct after identifier folding",
		prev.name, cur.name, shared)
}
