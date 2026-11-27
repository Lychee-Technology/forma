package sqlgen

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// TestEntityMainColumnNamesFollowsDescriptors pins the reserved entity_main
// set to model.EntityMainColumnDescriptors, the one statement of the column
// set (#470): every column is reserved, nothing else is, and a column added
// to the descriptors reaches the guard by construction.
func TestEntityMainColumnNamesFollowsDescriptors(t *testing.T) {
	require.Len(t, entityMainColumnNames, len(model.EntityMainColumnDescriptors))
	for _, desc := range model.EntityMainColumnDescriptors {
		assert.Contains(t, entityMainColumnNames, desc.Name)
	}

	descs := append([]model.ColumnDescriptor(nil), model.EntityMainColumnDescriptors...)
	descs = append(descs, model.ColumnDescriptor{Name: "Text_11", Kind: model.ColumnKindText})
	got := buildEntityMainColumnNames(descs)
	assert.Contains(t, got, "text_11", "a new column must be reserved under its folded key")
	assert.Len(t, got, len(entityMainColumnNames)+1)
}

// TestValidateParquetAttrColumns_RefusesEveryEntityMainColumnName drives the
// guard from the descriptors, so every column family is covered (text,
// smallint, integer, bigint, double, uuid, and the ltbase_* system and audit
// columns), with an EAV-backed attribute under the column's exact name.
func TestValidateParquetAttrColumns_RefusesEveryEntityMainColumnName(t *testing.T) {
	kinds := map[model.ColumnKind]bool{}
	for _, desc := range model.EntityMainColumnDescriptors {
		kinds[desc.Kind] = true
		err := ValidateParquetAttrColumns(forma.SchemaAttributeCache{
			"name":    {AttributeID: 1, ValueType: forma.ValueTypeText},
			desc.Name: {AttributeID: 2, ValueType: model.ColumnKindToValueType(desc.Kind)},
		})
		require.Error(t, err, "attribute %q is an entity_main column and must be refused", desc.Name)
		assert.Contains(t, err.Error(), `"`+desc.Name+`"`, "the error must name the attribute and the column")
		if model.IsSystemColumn(desc.Name) {
			// The five system stamps were already reserved parquet columns
			// (#260); that rule answers first and keeps its message.
			assert.Contains(t, err.Error(), "reserved for system columns")
			continue
		}
		assert.Contains(t, err.Error(), `entity_main column "`+desc.Name+`"`)
		assert.Contains(t, err.Error(), "rename the attribute")
	}
	assert.Len(t, kinds, 6, "every column family must be exercised")
}

// An attribute whose name only folds onto a column is refused too: the
// renderers emit the folded name, and DuckDB resolves it case-insensitively.
func TestValidateParquetAttrColumns_RefusesNamesFoldingToEntityMainColumn(t *testing.T) {
	cases := map[string]string{
		"TEXT_01":           "text_01",
		"Text_01":           "text_01",
		"text.01":           "text_01",
		"text 01":           "text_01",
		"[text_01]":         "text_01",
		"`integer_02`":      "integer_02",
		"Smallint.03":       "smallint_03",
		"BIGINT_01":         "bigint_01",
		"double 02":         "double_02",
		"UUID.01":           "uuid_01",
		"ltbase.created_by": "ltbase_created_by",
	}
	for name, col := range cases {
		err := ValidateParquetAttrColumns(forma.SchemaAttributeCache{
			name: {AttributeID: 1, ValueType: forma.ValueTypeText},
		})
		require.Error(t, err, "attribute %q folds onto %q and must be refused", name, col)
		assert.Contains(t, err.Error(), `attribute "`+name+`" folds to "`+ParquetAttrColumn(name)+`"`)
		assert.Contains(t, err.Error(), `entity_main column "`+col+`"`)
	}
}

// A column-bound attribute is judged by its name, not its binding: one named
// after another column is the #470 comment's integer_02 → m."integer_02"
// case, and one named after its own column is refused as well.
func TestValidateParquetAttrColumns_RefusesBoundAttributeNamedAfterColumn(t *testing.T) {
	for name, col := range map[string]forma.MainColumn{
		"integer_02": forma.MainColumnInteger03,
		"text_01":    forma.MainColumnText01,
	} {
		err := ValidateParquetAttrColumns(forma.SchemaAttributeCache{
			name: {AttributeID: 1, ValueType: forma.ValueTypeInteger,
				ColumnBinding: &forma.MainColumnBinding{ColumnName: col}},
		})
		require.Error(t, err, "attribute %q bound to %s", name, col)
		assert.Contains(t, err.Error(), `attribute "`+name+`" is named after entity_main column "`+name+`"`)
	}
}

// Names near a column but not on it keep registering, as does an ordinary
// attribute bound to a column.
func TestValidateParquetAttrColumns_AdmitsNamesBesideEntityMainColumns(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"status": {AttributeID: 1, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01}},
		"count": {AttributeID: 2, ValueType: forma.ValueTypeInteger,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnInteger03}},
		"text_11":       {AttributeID: 3, ValueType: forma.ValueTypeText},
		"text_1":        {AttributeID: 4, ValueType: forma.ValueTypeText},
		"text_01_label": {AttributeID: 5, ValueType: forma.ValueTypeText},
		"my.text_01":    {AttributeID: 6, ValueType: forma.ValueTypeText},
		"uuid_03":       {AttributeID: 7, ValueType: forma.ValueTypeUUID},
	}
	require.NoError(t, ValidateParquetAttrColumns(cache))
}

// A retired entry reaches no condition, sort or projection, so it is exempt
// like a retired reserved-column hit (#549); the same entry active is refused.
func TestValidateParquetAttrColumns_RetiredEntityMainColumnNameIsExempt(t *testing.T) {
	retired := forma.SchemaAttributeCache{
		"name":    {AttributeID: 1, ValueType: forma.ValueTypeText},
		"text_02": {AttributeID: 2, ValueType: forma.ValueTypeText, Retired: true},
	}
	require.NoError(t, ValidateParquetAttrColumns(retired))

	active := retired["text_02"]
	active.Retired = false
	retired["text_02"] = active
	require.Error(t, ValidateParquetAttrColumns(retired))
}

// The federated read path builds every query's projection through
// BuildSchemaProjection, whose outer select emits one alias per entity_main
// column beside the attribute's own visible column. It refuses the colliding
// cache on its own, so a registry that skipped admission still cannot render
// a federated query whose attribute and column namespaces overlap.
func TestBuildSchemaProjection_RefusesEntityMainColumnName(t *testing.T) {
	_, err := BuildSchemaProjection(7, forma.SchemaAttributeCache{
		"text_01": {AttributeID: 1, ValueType: forma.ValueTypeText},
	})
	require.Error(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "schema 7: "), "%v", err)
	assert.Contains(t, err.Error(), `entity_main column "text_01"`)
}

// ValidateReservedAttrNames is the reserved-name half of
// ValidateParquetAttrColumns alone, for caches no admission saw: it refuses
// every entity_main column and every system and dedup column, worded as
// admission words it, exempts retired entries, admits near-misses, and names
// the smallest colliding attribute so the message is deterministic.
func TestValidateReservedAttrNames(t *testing.T) {
	for _, desc := range model.EntityMainColumnDescriptors {
		cache := forma.SchemaAttributeCache{
			desc.Name: {AttributeID: 1, ValueType: model.ColumnKindToValueType(desc.Kind)},
		}
		err := ValidateReservedAttrNames(cache)
		require.Error(t, err, desc.Name)
		assert.Equal(t, ValidateParquetAttrColumns(cache).Error(), err.Error(), desc.Name)
	}
	for col := range reservedParquetColumns {
		cache := forma.SchemaAttributeCache{col: {AttributeID: 1, ValueType: forma.ValueTypeText}}
		err := ValidateReservedAttrNames(cache)
		require.Error(t, err, col)
		assert.Contains(t, err.Error(), "reserved for system columns", col)
		assert.Equal(t, ValidateParquetAttrColumns(cache).Error(), err.Error(), col)
	}

	err := ValidateReservedAttrNames(forma.SchemaAttributeCache{"Text.01": {AttributeID: 1}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `attribute "Text.01" folds to "Text_01"`)

	err = ValidateReservedAttrNames(forma.SchemaAttributeCache{"Row.ID": {AttributeID: 1}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `onto the reserved system column "row_id"`)

	err = ValidateReservedAttrNames(forma.SchemaAttributeCache{
		"uuid_01": {AttributeID: 1}, "integer_02": {AttributeID: 2}, "text_01": {AttributeID: 3},
		"rn": {AttributeID: 4},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `attribute "integer_02"`)

	require.NoError(t, ValidateReservedAttrNames(forma.SchemaAttributeCache{
		"text_11":    {AttributeID: 1},
		"my.text_01": {AttributeID: 2},
		"text_02":    {AttributeID: 3, Retired: true},
		"rn":         {AttributeID: 5, Retired: true},
		"row_ids":    {AttributeID: 6},
		"status": {AttributeID: 4, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01}},
	}))
	require.NoError(t, ValidateReservedAttrNames(nil))
}
