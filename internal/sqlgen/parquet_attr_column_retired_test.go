package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// The tests in this file pin #549: a retired attribute is the attributeID
// ledger (#342) for values already flushed under its folded column, so a
// rejection that names one must say so and must not offer "rename the
// attribute" — a rename in place desynchronizes the ledger from the flushed
// data. Only schemameta passes retired entries through the guard; the CDC
// exporter and the projection pass active caches and keep the active messages.

// requireRetiredLedgerRemedy asserts the shape every retired-entry rejection
// shares: the retired name, its id and valueType, the word "retired", the
// migration remedy, and no rename remedy.
func requireRetiredLedgerRemedy(t *testing.T, err error, retiredName string) {
	t.Helper()
	require.Error(t, err)
	msg := err.Error()
	require.Contains(t, msg, `retired attribute "`+retiredName+`"`)
	require.Contains(t, msg, "attributeID ledger")
	require.Contains(t, msg, "id 7")
	require.Contains(t, msg, "valueType date")
	require.Contains(t, msg, "migrate the flushed parquet column and the ledger entry")
	require.NotContains(t, msg, "rename the attribute", "a retired entry must not be told to rename itself")
}

func TestValidateParquetAttrColumns_RetiredReservedSystemColumn(t *testing.T) {
	err := ValidateParquetAttrColumns(forma.SchemaAttributeCache{
		"created.at": {AttributeID: 7, ValueType: forma.ValueTypeDate, Retired: true},
	})
	requireRetiredLedgerRemedy(t, err, "created.at")
	require.Contains(t, err.Error(), `"created_at", which is reserved`)
}

// The #548 widening is the case that motivated #549: a retired Created_At
// passed the guard before #532 and hard-blocks boot after it.
func TestValidateParquetAttrColumns_RetiredCaseVariantReservedSystemColumn(t *testing.T) {
	err := ValidateParquetAttrColumns(forma.SchemaAttributeCache{
		"Created_At": {AttributeID: 7, ValueType: forma.ValueTypeDate, Retired: true},
	})
	requireRetiredLedgerRemedy(t, err, "Created_At")
	require.Contains(t, err.Error(), `case-insensitively onto the reserved system column "created_at"`)
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
