package sqlgen

import (
	"strings"
	"testing"

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
