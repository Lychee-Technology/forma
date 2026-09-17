package sqlgen

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// TestParquetAttrColumn pins the physical parquet column naming contract.
// The mapping must stay byte-identical with what the CDC exporter has
// always written (formerly internal/cdc safeColumnAlias): existing delta
// and base parquet files on S3 were produced with exactly this folding.
func TestParquetAttrColumn(t *testing.T) {
	cases := map[string]string{
		"name":                 "name",                 // flat names pass through
		"contact.annualIncome": "contact_annualIncome", // dots fold to underscores
		"a.b.c":                "a_b_c",
		"with space":           "with_space",
		"tick`ed":              "ticked",
		"arr[0]":               "arr0",
		"":                     "attr", // empty falls back
	}
	for in, want := range cases {
		require.Equal(t, want, ParquetAttrColumn(in), "ParquetAttrColumn(%q)", in)
	}
}

// TestValidateParquetAttrColumns_ReservedSystemColumn pins the PR #273
// review P1: a valid nested property like "row.id" folds to "row_id", the
// parquet system column — projecting it would duplicate the export/read
// SELECT lists or silently read the system value as the attribute.
func TestValidateParquetAttrColumns_ReservedSystemColumn(t *testing.T) {
	for _, attr := range []string{
		"row.id",        // → row_id (parquet + unified CTE key)
		"changed.at",    // → changed_at (parquet export column)
		"created.at",    // → created_at (unified CTE column)
		"ver.ts",        // → ver_ts (unified CTE column)
		"ltbase.row_id", // → ltbase_row_id (base export column)
		"rn",            // ranked CTE dedup column
		"source_tier_priority",
		"attributes_json",
	} {
		err := ValidateParquetAttrColumns(forma.SchemaAttributeCache{
			attr: {AttributeID: 1, ValueType: forma.ValueTypeText},
		})
		require.Error(t, err, "attribute %q must be rejected", attr)
		require.Contains(t, err.Error(), attr)
		require.Contains(t, err.Error(), ParquetAttrColumn(attr))
		require.Contains(t, err.Error(), "reserved")
	}
}

// TestValidateParquetAttrColumns_CaseVariantReservedSystemColumn pins #532:
// DuckDB resolves unquoted identifiers case-insensitively, so case variants
// of system columns must be refused at registration before they can bind to a
// system value on the federated read path. The last two inputs make the fold
// and the case act together — "Row.ID" folds to "Row_ID" and only then
// resolves onto row_id — so the parquet-column assertion carries weight
// independently of the caller-spelling one.
func TestValidateParquetAttrColumns_CaseVariantReservedSystemColumn(t *testing.T) {
	for _, attr := range []string{
		"Created_At",
		"Row_ID",
		"RN",
		"Source_Tier_Priority",
		"Row.ID",     // → Row_ID → row_id
		"Created At", // → Created_At → created_at
	} {
		err := ValidateParquetAttrColumns(forma.SchemaAttributeCache{
			attr: {AttributeID: 1, ValueType: forma.ValueTypeText},
		})
		require.Error(t, err, "attribute %q must be rejected", attr)
		require.Contains(t, err.Error(), attr, "error must preserve the caller spelling")
		require.Contains(t, err.Error(), ParquetAttrColumn(attr), "error must name the parquet column")
		// The resolved system column is what the operator greps for in
		// reservedParquetColumns; without it a Created_At rejection is
		// unexplainable (review F1 on PR #548).
		require.Contains(t, err.Error(), strings.ToLower(ParquetAttrColumn(attr)),
			"error must name the reserved column the fold resolves onto")
		require.Contains(t, err.Error(), "reserved")
	}
}

// TestValidateParquetAttrColumns_AttrCollision keeps the attr-vs-attr half
// of the guard on the shared validator.
func TestValidateParquetAttrColumns_AttrCollision(t *testing.T) {
	err := ValidateParquetAttrColumns(forma.SchemaAttributeCache{
		"contact.name": {AttributeID: 1, ValueType: forma.ValueTypeText},
		"contact_name": {AttributeID: 2, ValueType: forma.ValueTypeText},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "contact.name")
	require.Contains(t, err.Error(), "contact_name")
}

// TestValidateParquetAttrColumns_CaseVariantAttrCollision pins #532:
// contact_name and Contact_Name resolve to the same DuckDB column even though
// ParquetAttrColumn preserves their spelling. The message must therefore name
// both physical columns and the single column they resolve onto — claiming
// one shared parquet column would contradict the naming contract this file
// documents (review F1 on PR #548).
func TestValidateParquetAttrColumns_CaseVariantAttrCollision(t *testing.T) {
	err := ValidateParquetAttrColumns(forma.SchemaAttributeCache{
		"contact_name": {AttributeID: 1, ValueType: forma.ValueTypeText},
		"Contact_Name": {AttributeID: 2, ValueType: forma.ValueTypeText},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "contact_name")
	require.Contains(t, err.Error(), "Contact_Name")
	require.Contains(t, err.Error(), "case-insensitively")
}

// TestValidateParquetAttrColumns_CaseVariantFoldedAttrCollision exercises the
// fold and the case together on the collision half: "Contact.Name" folds to
// "Contact_Name", a column distinct from "contact_name" in the parquet
// footer, and only DuckDB's identifier matching merges the two.
func TestValidateParquetAttrColumns_CaseVariantFoldedAttrCollision(t *testing.T) {
	err := ValidateParquetAttrColumns(forma.SchemaAttributeCache{
		"contact_name": {AttributeID: 1, ValueType: forma.ValueTypeText},
		"Contact.Name": {AttributeID: 2, ValueType: forma.ValueTypeText},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "Contact.Name", "error must preserve the caller spelling")
	require.Contains(t, err.Error(), "Contact_Name", "error must name the parquet column of the folded attribute")
	require.Contains(t, err.Error(), "contact_name")
}

func TestValidateParquetAttrColumns_ValidCachePasses(t *testing.T) {
	require.NoError(t, ValidateParquetAttrColumns(forma.SchemaAttributeCache{
		"contact.name":         {AttributeID: 1, ValueType: forma.ValueTypeText},
		"contact.annualIncome": {AttributeID: 2, ValueType: forma.ValueTypeBigInt},
		"flag":                 {AttributeID: 3, ValueType: forma.ValueTypeText},
		// camelCase does not fold onto snake_case system columns.
		"createdAt": {AttributeID: 4, ValueType: forma.ValueTypeDateTime},
		// Case normalization narrows nothing: a mixed-case name that neither
		// hits the reserved set nor has a case-variant sibling still passes
		// (review O5 on PR #548).
		"Contact_Phone": {AttributeID: 5, ValueType: forma.ValueTypeText},
		"Attr":          {AttributeID: 6, ValueType: forma.ValueTypeText},
	}))
}

// TestValidateParquetAttrColumns_NonASCIICaseVariantsPass is the counterpart
// to the ASCII rejection tests above: the case-insensitive comparison must
// stop where DuckDB's does. Every pair here is two distinct DuckDB
// identifiers, so merging them would report a collision that the engine
// would not, and one such legacy schema fails registry construction for the
// whole directory. "row_İd" is the reserved half of the same defect: Go maps
// U+0130 onto "i", so a Unicode fold reads it as the reserved "row_id"
// (review F1 on PR #548).
func TestValidateParquetAttrColumns_NonASCIICaseVariantsPass(t *testing.T) {
	require.NoError(t, ValidateParquetAttrColumns(forma.SchemaAttributeCache{
		"Á":      {AttributeID: 1, ValueType: forma.ValueTypeText},
		"á":      {AttributeID: 2, ValueType: forma.ValueTypeText},
		"Ж":      {AttributeID: 3, ValueType: forma.ValueTypeText},
		"ж":      {AttributeID: 4, ValueType: forma.ValueTypeText},
		"cafÉ":   {AttributeID: 5, ValueType: forma.ValueTypeText},
		"café":   {AttributeID: 6, ValueType: forma.ValueTypeText},
		"row_İd": {AttributeID: 7, ValueType: forma.ValueTypeText},
	}))

	// The ASCII half still bites in the same cache: adding a case variant of
	// an existing ASCII name is refused, so the pass above is the boundary
	// being respected, not the guard being switched off.
	require.Error(t, ValidateParquetAttrColumns(forma.SchemaAttributeCache{
		"Á":            {AttributeID: 1, ValueType: forma.ValueTypeText},
		"á":            {AttributeID: 2, ValueType: forma.ValueTypeText},
		"contact_name": {AttributeID: 3, ValueType: forma.ValueTypeText},
		"Contact_Name": {AttributeID: 4, ValueType: forma.ValueTypeText},
	}))
}

// TestDuckDBIdentifierFoldIsASCIIOnly pins duckdbFoldIdentifier against the
// engine instead of against DuckDB's documentation. Two attribute names
// share a parquet column exactly when DuckDB refuses to create a table
// holding both as quoted columns, so that refusal is the oracle: the guard
// must merge a pair if and only if DuckDB does. Every non-ASCII pair here is
// one Go's strings.ToLower merges, which is what makes it the wrong
// primitive (#532, review F1 on PR #548).
func TestDuckDBIdentifierFoldIsASCIIOnly(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	pairs := []struct {
		a, b string
		same bool // DuckDB resolves both onto one identifier
	}{
		{a: "col_A", b: "col_a", same: true},
		{a: "Contact_Name", b: "contact_name", same: true},
		{a: "Row_ID", b: "row_id", same: true},
		{a: "Á", b: "á", same: false},
		{a: "Ж", b: "ж", same: false},
		{a: "ẞ", b: "ß", same: false},
		{a: "cafÉ", b: "café", same: false},
		{a: "K", b: "k", same: false}, // U+212A KELVIN SIGN
		{a: "row_İd", b: "row_id", same: false},
	}

	for i, p := range pairs {
		_, err := db.Exec(fmt.Sprintf(`CREATE TABLE t%d ("%s" INT, "%s" INT)`, i, p.a, p.b))
		duckDBMerges := err != nil
		if duckDBMerges {
			require.Contains(t, err.Error(), "already exists",
				"unexpected DDL failure for %q / %q", p.a, p.b)
		}
		require.Equal(t, p.same, duckDBMerges,
			"DuckDB identifier folding changed for %q / %q; the guard's premise must be rechecked", p.a, p.b)
		require.Equal(t, duckDBMerges,
			duckdbFoldIdentifier(p.a) == duckdbFoldIdentifier(p.b),
			"duckdbFoldIdentifier disagrees with DuckDB on %q / %q", p.a, p.b)
	}
}

// TestParquetAttrColumnUsesPlaceholderConst pins that the empty-name fallback
// and the exported placeholder are the same string by construction (#531).
// ParquetAttrColumn used to return a bare "attr" literal beside a const of the
// same value, so a change to one silently left the other behind. The literal
// assertion stays: the value is a byte-stable contract with parquet files
// already flushed to S3, so it is pinned as a value, not only as an identity.
func TestParquetAttrColumnUsesPlaceholderConst(t *testing.T) {
	require.Equal(t, "attr", ParquetAttrPlaceholder)
	for _, empty := range []string{"", "[]", "`", "``", "[`]"} {
		require.Equal(t, ParquetAttrPlaceholder, ParquetAttrColumn(empty),
			"ParquetAttrColumn(%q) must fall back to the placeholder", empty)
	}
}

// TestFederatedDedupColumnsIsACopy pins the accessor internal/federated derives
// its keyset reject set from (#531). The set decides whether a query is
// refused, so the accessor must hand out a copy: a caller that edits what it
// gets back must not be able to edit the guard, in either direction.
func TestFederatedDedupColumnsIsACopy(t *testing.T) {
	want := map[string]struct{}{"rn": {}, "source_tier_priority": {}}
	require.Equal(t, want, FederatedDedupColumns())

	tampered := FederatedDedupColumns()
	delete(tampered, "rn")
	tampered["contact_name"] = struct{}{}

	require.Equal(t, want, FederatedDedupColumns(), "the accessor must not return the live map")
	require.Error(t, ValidateUnregisteredParquetAttrColumn("rn", ParquetAttrColumn("rn")),
		"deleting from a returned copy must not unblock a dedup column")
	require.NoError(t, ValidateUnregisteredParquetAttrColumn("contact_name", ParquetAttrColumn("contact_name")),
		"adding to a returned copy must not block an ordinary attribute")
}

// TestReservedParquetColumnsIsUnchanged pins the contents of the reserved set
// against a literal map. reservedParquetColumns gates schema registration, so
// #552's move from a literal to a composition over federatedDedupColumns has
// to be a pure identity on the current set: an entry gained here rejects a
// schema that registers today, an entry lost admits one that binds onto a
// system column. Asserting the outcome also pins the package-initialisation
// order the composition depends on, rather than trusting it.
func TestReservedParquetColumnsIsUnchanged(t *testing.T) {
	want := map[string]struct{}{
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
	require.Equal(t, want, reservedParquetColumns)
}

// TestBuildReservedParquetColumnsFollowsDedupSet proves the reserved set is
// composed from the dedup set rather than restating it (#552): a column that
// exists only in the dedup set handed to the builder must come out reserved.
// Driven through a synthetic set instead of the package variable so the
// package state other tests run against is never touched.
func TestBuildReservedParquetColumnsFollowsDedupSet(t *testing.T) {
	dedup := FederatedDedupColumns()
	dedup["tier_rank"] = struct{}{}

	got := buildReservedParquetColumns(dedup)

	require.Contains(t, got, "tier_rank", "a dedup column must reach the reserved set by construction")
	for col := range reservedParquetColumns {
		require.Contains(t, got, col, "composing in a dedup column must not drop reserved column %q", col)
	}
	require.Len(t, got, len(reservedParquetColumns)+1)
}

// TestValidateParquetAttrColumnsRejectsEveryDedupColumn is the registration
// half of the #531 drift guard, closing the gap #552 found: the filter and
// cursor guards derive from federatedDedupColumns, but registration read a
// separate literal, so a dedup column added to the source could be
// registered as an attribute and then bind against the dedup rank through
// the hasMeta branch of normalizeDuckPayload. This test drives
// ValidateParquetAttrColumns from the dedup set itself, under every spelling
// the fold and DuckDB's case-insensitive resolution reach a column by, so it
// bites on drift rather than restating the list.
func TestValidateParquetAttrColumnsRejectsEveryDedupColumn(t *testing.T) {
	require.NotEmpty(t, federatedDedupColumns, "sqlgen must define the dedup column set this guard enforces")

	for col := range federatedDedupColumns {
		spellings := []string{col, strings.ToUpper(col), "[" + col + "]", "[" + strings.ToUpper(col) + "]"}
		if i := strings.Index(col, "_"); i >= 0 {
			spellings = append(spellings, col[:i]+"."+col[i+1:], col[:i]+" "+col[i+1:])
		}
		for _, spelling := range spellings {
			err := ValidateParquetAttrColumns(forma.SchemaAttributeCache{
				spelling: {AttributeID: 1, ValueType: forma.ValueTypeText},
			})
			require.Error(t, err, "attribute %q folds onto dedup column %q and must be refused at registration", spelling, col)
			require.Contains(t, err.Error(), "reserved",
				"attribute %q must be refused as a reserved column, not by some other rule", spelling)
			require.Contains(t, err.Error(), spelling, "the error must keep the caller's spelling")
			require.Contains(t, err.Error(), col, "the error must name the reserved column the fold resolves onto")
		}
	}
}
