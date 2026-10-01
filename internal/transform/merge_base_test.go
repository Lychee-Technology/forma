package transform

import (
	"context"
	"math"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// newMergeBaseRegistry declares a required bigint at each float64 image
// destination (#590): eav_data scalar, eav_data list item, double_* column.
func newMergeBaseRegistry() forma.SchemaRegistry {
	return &stubSchemaRegistry{schemaID: 501, schemaName: "merge_base", cache: forma.SchemaAttributeCache{
		"name":   {AttributeName: "name", AttributeID: 1, ValueType: forma.ValueTypeText},
		"total":  {AttributeName: "total", AttributeID: 2, ValueType: forma.ValueTypeBigInt, Required: true},
		"totals": {AttributeName: "totals", AttributeID: 3, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeBigInt, Required: true},
		"ratio": {AttributeName: "ratio", AttributeID: 4, ValueType: forma.ValueTypeBigInt, Required: true,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnDouble01}},
	}}
}

// legacyBigintRecord stores a stored image the read refuses at every
// destination: a fraction, a whole number past int64, and both in a list.
func legacyBigintRecord(rowID uuid.UUID) *model.PersistentRecord {
	name, fraction, pastInt64, whole := "kept", 1000.5, math.Ldexp(1, 63), 7.0
	return &model.PersistentRecord{
		SchemaID:     501,
		RowID:        rowID,
		Float64Items: map[string]float64{string(forma.MainColumnDouble01): pastInt64},
		OtherAttributes: []model.EAVRecord{
			{SchemaID: 501, RowID: rowID, AttrID: 1, ValueText: &name},
			{SchemaID: 501, RowID: rowID, AttrID: 2, ValueNumeric: &fraction},
			{SchemaID: 501, RowID: rowID, AttrID: 3, ArrayIndices: "0", ValueNumeric: &whole},
			{SchemaID: 501, RowID: rowID, AttrID: 3, ArrayIndices: "1", ValueNumeric: &pastInt64},
		},
	}
}

func replacing(names ...string) func(string) bool {
	return func(attrName string) bool {
		for _, name := range names {
			if attrName == name {
				return true
			}
		}
		return false
	}
}

// The update that rewrites a stored image the read refuses must not be
// blocked by converting it (#590 review): MergeBase leaves the replaced
// attributes out unconverted, and they still satisfy the required policy
// because they are stored.
func TestMergeBaseSkipsReplacedAttributes(t *testing.T) {
	rowID := uuid.Must(uuid.NewV7())
	tr := NewPersistentRecordTransformer(newMergeBaseRegistry())

	got, err := tr.MergeBase(context.Background(), legacyBigintRecord(rowID), replacing("total", "totals", "ratio"))
	require.NoError(t, err)
	require.Equal(t, map[string]any{"name": "kept"}, got)
}

// Only the replaced attributes are spared: every other stored value is
// converted as the read converts it, so a refused image the update does not
// rewrite still fails the merge as a plain read-path error, not a 4xx.
func TestMergeBaseConvertsKeptAttributes(t *testing.T) {
	rowID := uuid.Must(uuid.NewV7())
	tr := NewPersistentRecordTransformer(newMergeBaseRegistry())

	for kept, want := range map[string]string{
		"total":  "stored bigint image 1000.5 is not a whole number",
		"totals": "stored bigint image 9223372036854775808 is outside the bigint range",
		"ratio":  "stored bigint image 9223372036854775808 is outside the bigint range",
	} {
		t.Run(kept, func(t *testing.T) {
			replaced := replacing(filterOut([]string{"total", "totals", "ratio"}, kept)...)
			_, err := tr.MergeBase(context.Background(), legacyBigintRecord(rowID), replaced)
			require.Error(t, err)
			require.Contains(t, err.Error(), want)
			require.Contains(t, err.Error(), "'"+kept+"'")
			require.Contains(t, err.Error(), rowID.String())
			require.NotErrorIs(t, err, forma.ErrInvalidInput)
		})
	}

	_, err := tr.MergeBase(context.Background(), legacyBigintRecord(rowID), nil)
	require.ErrorContains(t, err, "stored bigint image", "a nil replaced is FromPersistentRecord")
}

// Presence comes from storage, not from the update: an attribute the update
// replaces but the row never stored is still a required-policy drift.
func TestMergeBaseKeepsRequiredPolicyOnStoredPresence(t *testing.T) {
	rowID := uuid.Must(uuid.NewV7())
	record := legacyBigintRecord(rowID)
	record.OtherAttributes = record.OtherAttributes[:2] // no totals stored
	tr := NewPersistentRecordTransformer(newMergeBaseRegistry())

	_, err := tr.MergeBase(context.Background(), record, replacing("total", "totals", "ratio"))
	require.ErrorContains(t, err, "missing required attribute 'totals'")
	require.NotErrorIs(t, err, forma.ErrInvalidInput)
}

func filterOut(names []string, drop string) []string {
	out := make([]string, 0, len(names))
	for _, name := range names {
		if name != drop {
			out = append(out, name)
		}
	}
	return out
}
