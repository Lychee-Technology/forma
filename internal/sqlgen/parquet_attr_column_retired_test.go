package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// The tests in this file pin #549. A retired attribute is the attributeID
// ledger (#342) for values already flushed under its folded column, and the
// two halves of the guard treat it differently: the reserved-column half
// exempts it, because a retired column is never projected beside a system
// column and no flushed file holds two spellings of one (pinned against the
// engine in parquet_attr_column_retired_scan_test.go); the collision half
// keeps it in scope, because union_by_name physically merges same-folding
// columns, and a rejection that names it must say so and must not offer
// "rename the attribute" — a rename in place desynchronizes the ledger from
// the flushed data. Only schemameta passes retired entries through the
// guard; the CDC exporter and the projection pass active caches.

// A retired entry folding onto a reserved column — exact, case-variant, or a
// physical export column — registers. The same names active are refused,
// which is what shows the exemption keys on Retired, not on the name.
func TestValidateParquetAttrColumns_RetiredReservedSystemColumnExempt(t *testing.T) {
	for _, name := range []string{"created.at", "Created_At", "row.id", "Row_Id", "rn"} {
		t.Run(name, func(t *testing.T) {
			retired := forma.SchemaAttributeCache{
				name:    {AttributeID: 7, ValueType: forma.ValueTypeDate, Retired: true},
				"title": {AttributeID: 1, ValueType: forma.ValueTypeText},
			}
			require.NoError(t, ValidateParquetAttrColumns(retired),
				"a retired entry is never projected beside the system column it folds onto")

			active := forma.SchemaAttributeCache{
				name: {AttributeID: 7, ValueType: forma.ValueTypeDate},
			}
			err := ValidateParquetAttrColumns(active)
			require.Error(t, err, "the same name active is still a reserved-column hit")
			require.Contains(t, err.Error(), "rename the attribute")
		})
	}
}

// The exemption is from the reserved half only: a retired entry whose folded
// column is reserved still collides with a same-folding sibling.
func TestValidateParquetAttrColumns_RetiredReservedStillCollides(t *testing.T) {
	err := ValidateParquetAttrColumns(forma.SchemaAttributeCache{
		"Created_At": {AttributeID: 7, ValueType: forma.ValueTypeDate, Retired: true},
		"created.at": {AttributeID: 8, ValueType: forma.ValueTypeDate, Retired: true},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), `retired attributes "Created_At" (id 7, valueType date) and "created.at" (id 8, valueType date)`)
	require.Contains(t, err.Error(), "migrate the flushed parquet column and the ledger entry")
}

// A retired/active fold collision directs the rename at the active side and
// names the retired side as the ledger, mirroring the attributeID and
// main-column collision diagnostics in schemameta.
func TestValidateParquetAttrColumns_RetiredActiveCollision(t *testing.T) {
	for name, cache := range map[string]forma.SchemaAttributeCache{
		"retired sorts first": {
			"Contact_Name": {AttributeID: 7, ValueType: forma.ValueTypeDate, Retired: true},
			"contact_name": {AttributeID: 2, ValueType: forma.ValueTypeText},
		},
		"retired sorts last": {
			"contact.name": {AttributeID: 2, ValueType: forma.ValueTypeText},
			"contact_name": {AttributeID: 7, ValueType: forma.ValueTypeDate, Retired: true},
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := ValidateParquetAttrColumns(cache)
			require.Error(t, err)
			msg := err.Error()
			var retired, active string
			for attr, meta := range cache {
				if meta.Retired {
					retired = attr
				} else {
					active = attr
				}
			}
			require.Contains(t, msg, `retired attribute "`+retired+`"`)
			require.Contains(t, msg, "attributeID ledger")
			require.Contains(t, msg, "id 7")
			require.Contains(t, msg, `rename the active attribute "`+active+`"`)
			require.NotContains(t, msg, "attribute names must remain distinct",
				"the active-only wording must not be reused for a ledger collision")
		})
	}
}

// Two retired entries colliding are both ledger records: neither can be
// renamed in place, so the remedy is the migration one.
func TestValidateParquetAttrColumns_RetiredRetiredCollision(t *testing.T) {
	err := ValidateParquetAttrColumns(forma.SchemaAttributeCache{
		"contact.name": {AttributeID: 7, ValueType: forma.ValueTypeDate, Retired: true},
		"contact_name": {AttributeID: 8, ValueType: forma.ValueTypeText, Retired: true},
	})
	require.Error(t, err)
	msg := err.Error()
	require.Contains(t, msg, `retired attributes "contact.name" (id 7, valueType date) and "contact_name" (id 8, valueType text)`)
	require.Contains(t, msg, "attributeID ledger")
	require.Contains(t, msg, "migrate the flushed parquet column and the ledger entry")
	require.NotContains(t, msg, "rename the", "neither ledger entry may be told to rename")
}

// Active-only rejections keep their pre-#549 wording so the case-variant
// diagnostics pinned by #532 stay byte-stable.
func TestValidateParquetAttrColumns_ActiveMessagesUnchanged(t *testing.T) {
	err := ValidateParquetAttrColumns(forma.SchemaAttributeCache{
		"Created_At": {AttributeID: 1, ValueType: forma.ValueTypeDate},
	})
	require.EqualError(t, err,
		`attribute "Created_At" folds to parquet column "Created_At", which DuckDB resolves case-insensitively onto the reserved system column "created_at"; rename the attribute`)

	err = ValidateParquetAttrColumns(forma.SchemaAttributeCache{
		"contact.name": {AttributeID: 1, ValueType: forma.ValueTypeText},
		"contact_name": {AttributeID: 2, ValueType: forma.ValueTypeText},
	})
	require.EqualError(t, err,
		`attributes "contact.name" and "contact_name" both map to parquet column "contact_name"; attribute names must remain distinct after identifier folding`)
}
