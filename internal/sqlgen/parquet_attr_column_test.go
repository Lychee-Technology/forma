package sqlgen

import (
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

func TestValidateParquetAttrColumns_ValidCachePasses(t *testing.T) {
	require.NoError(t, ValidateParquetAttrColumns(forma.SchemaAttributeCache{
		"contact.name":         {AttributeID: 1, ValueType: forma.ValueTypeText},
		"contact.annualIncome": {AttributeID: 2, ValueType: forma.ValueTypeBigInt},
		"flag":                 {AttributeID: 3, ValueType: forma.ValueTypeText},
		// camelCase does not fold onto snake_case system columns.
		"createdAt": {AttributeID: 4, ValueType: forma.ValueTypeDateTime},
	}))
}
