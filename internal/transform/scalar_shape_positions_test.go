package transform

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

// The intake answers a nil Go slice at a list as nil, which a JSON Schema
// validator reads as null and strict validation refuses, and an empty one as
// empty, the empty-list marker. Before #696 copyArray kept the two apart; the
// list branch must too, or a strict update with []any(nil) clears the list.
func TestCheckUpdateValuesKeepsANilListApartFromAnEmptyOne(t *testing.T) {
	for name, tc := range map[string]struct {
		doc   map[string]any
		value func(map[string]any) any
	}{
		"top-level list": {map[string]any{"tags": []any(nil)}, func(v map[string]any) any { return v["tags"] }},
		"declared array": {map[string]any{"phones": []any(nil)}, func(v map[string]any) any { return v["phones"] }},
		"list in an array of objects": {map[string]any{"items": []any{map[string]any{"tags": []any(nil)}}},
			func(v map[string]any) any { return v["items"].([]any)[0].(map[string]any)["tags"] }},
	} {
		t.Run(name, func(t *testing.T) {
			checked, err := CheckUpdateValues(tc.doc, shapeCache())
			require.NoError(t, err)
			require.NoError(t, checked.Refusal)
			got, isArray := tc.value(checked.Values).([]any)
			require.True(t, isArray)
			require.Nil(t, got, "a nil slice must stay nil")
		})
	}
	checked, err := CheckUpdateValues(map[string]any{"tags": []any{}}, shapeCache())
	require.NoError(t, err)
	got, isArray := checked.Values["tags"].([]any)
	require.True(t, isArray)
	require.NotNil(t, got, "an empty slice must stay the empty-list marker")
	require.Empty(t, got)
}

// A primitive array the document declares under an element-typed ledger
// entry (DeclaredArray, pre-#204) is written by an update as by a create:
// one record per element, read back as the array. The same attribute
// without the declaration is a true scalar, and the array is refused.
func TestMergeUpdateWritesADeclaredArray(t *testing.T) {
	ctx := context.Background()
	cache := forma.SchemaAttributeCache{
		"stage":  {AttributeName: "stage", AttributeID: 1, ValueType: forma.ValueTypeText, ArrayAncestors: []string{}},
		"phones": {AttributeName: "phones", AttributeID: 2, ValueType: forma.ValueTypeText, ArrayAncestors: []string{}, DeclaredArray: true},
	}
	tr := NewPersistentRecordTransformer(shapeRegistry(cache))
	rowID := uuid.Must(uuid.NewV7())
	stored, err := tr.ToPersistentRecord(ctx, 696, rowID, map[string]any{"stage": "s", "phones": []any{"111"}})
	require.NoError(t, err)

	merged, err := tr.MergeUpdate(ctx, stored, setting(map[string]any{"phones": []any{"222", "333"}}))
	require.NoError(t, err)
	written, err := tr.ToPersistentRecord(ctx, 696, rowID, merged)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"1[]=s", "2[0]=222", "2[1]=333"}, storedKeys(written.OtherAttributes))
	read, err := tr.FromPersistentRecord(ctx, written)
	require.NoError(t, err)
	require.Equal(t, []any{"222", "333"}, read["phones"])

	checked, err := CheckUpdateValues(map[string]any{"phones": []any{"222"}}, cache)
	require.NoError(t, err)
	require.NoError(t, checked.Refusal)

	// Two indices are past what the declaration admits.
	_, err = tr.MergeUpdate(ctx, stored, setting(map[string]any{"phones": []any{[]any{"1"}}}))
	require.ErrorIs(t, err, forma.ErrInvalidInput)

	scalar := cache["phones"]
	scalar.DeclaredArray = false
	cache["phones"] = scalar
	tr = NewPersistentRecordTransformer(shapeRegistry(cache))
	_, err = tr.MergeUpdate(ctx, stored, setting(map[string]any{"phones": []any{"222"}}))
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.ErrorContains(t, err, scalarRefusal("phones", 2, forma.ValueTypeText, "an array"))
	checked, err = CheckUpdateValues(map[string]any{"phones": []any{"222"}}, cache)
	require.NoError(t, err)
	require.ErrorIs(t, checked.Refusal, forma.ErrInvalidInput)
}

// A stored record is written back where the row's rebuild put it, even past
// the indices the ancestry admits: the excess-indices rule judges what the
// caller sends, as the container rule does.
func TestMergeUpdateWritesBackAStoredRecordPastItsAncestry(t *testing.T) {
	ctx := context.Background()
	cache := forma.SchemaAttributeCache{
		"stage": {AttributeName: "stage", AttributeID: 1, ValueType: forma.ValueTypeText, ArrayAncestors: []string{}},
		"note":  {AttributeName: "note", AttributeID: 2, ValueType: forma.ValueTypeText, ArrayAncestors: []string{}},
	}
	tr := NewPersistentRecordTransformer(shapeRegistry(cache))
	rowID := uuid.Must(uuid.NewV7())
	stored, err := tr.ToPersistentRecord(ctx, 696, rowID, map[string]any{"stage": "s"})
	require.NoError(t, err)
	element := textRecord(rowID, 2, "0", "a")
	element.SchemaID = 696
	stored.OtherAttributes = append(stored.OtherAttributes, element)

	merged, err := tr.MergeUpdate(ctx, stored, setting(map[string]any{"stage": "t"}))
	require.NoError(t, err)
	written, err := tr.ToPersistentRecord(ctx, 696, rowID, merged)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"1[]=t", "2[0]=a"}, storedKeys(written.OtherAttributes))
}
