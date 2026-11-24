package transform

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// The shape rule (#696) over the issue's account schema, extended with a
// nested scalar, a list and an array of objects holding a scalar and a list:
// note is EAV text, tenant text bound to a main column.
func shapeCache() forma.SchemaAttributeCache {
	cache := forma.SchemaAttributeCache{
		"tenant": {AttributeName: "tenant", AttributeID: 1, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01}},
		"note":          {AttributeName: "note", AttributeID: 2, ValueType: forma.ValueTypeText},
		"count":         {AttributeName: "count", AttributeID: 3, ValueType: forma.ValueTypeInteger},
		"contact.total": {AttributeName: "contact.total", AttributeID: 4, ValueType: forma.ValueTypeBigInt},
		"contact.name":  {AttributeName: "contact.name", AttributeID: 5, ValueType: forma.ValueTypeText},
		"tags":          {AttributeName: "tags", AttributeID: 6, ValueType: forma.ValueTypeList},
		"items.name":    {AttributeName: "items.name", AttributeID: 7, ValueType: forma.ValueTypeText},
		"items.tags":    {AttributeName: "items.tags", AttributeID: 8, ValueType: forma.ValueTypeList},
	}
	declareArrayAncestors(cache, "items")
	return cache
}

func shapeRegistry(cache forma.SchemaAttributeCache) forma.SchemaRegistry {
	return &stubSchemaRegistry{schemaID: 696, schemaName: "shape", cache: cache}
}

// scalarRefusal is the published refusal of a container at a scalar.
func scalarRefusal(attr string, id int, vt forma.ValueType, kind string) string {
	return refusedValue(attr, id) + fmt.Sprintf("declared type %s takes a single value, got %s; an object or array, "+
		"empty or not, cannot be stored where a single value belongs (omit the key to keep the current value)", vt, kind)
}

// refusedShapes are updates the shape rule refuses, each with its published
// refusal: every container kind at a bound and an EAV scalar, a singleton
// array, the nested and literal spellings of a nested scalar, a scalar
// member of an array of objects in both spellings, and the containers a
// list cannot hold.
func refusedShapes() []intakeCase {
	cases := []intakeCase{}
	for _, attr := range []struct {
		name string
		id   int
	}{{"tenant", 1}, {"note", 2}} {
		for _, value := range []struct {
			kind  string
			value any
		}{
			{"an empty object", map[string]any{}},
			{"an empty array", []any{}},
			{"an array", []any{"a"}},
			{"an object", map[string]any{"x": "y"}},
		} {
			cases = append(cases, intakeCase{attr.name + " " + value.kind, map[string]any{attr.name: value.value},
				scalarRefusal(attr.name, attr.id, forma.ValueTypeText, value.kind)})
		}
	}
	return append(cases,
		intakeCase{"array of nulls", map[string]any{"note": []any{nil}}, scalarRefusal("note", 2, forma.ValueTypeText, "an array")},
		intakeCase{"nested array of arrays", map[string]any{"note": []any{[]any{"a"}}}, scalarRefusal("note", 2, forma.ValueTypeText, "an array")},
		intakeCase{"nested spelling", map[string]any{"contact": map[string]any{"total": []any{int64(1)}}},
			scalarRefusal("contact.total", 4, forma.ValueTypeBigInt, "an array")},
		intakeCase{"literal dotted spelling", map[string]any{"contact.total": map[string]any{}},
			scalarRefusal("contact.total", 4, forma.ValueTypeBigInt, "an empty object")},
		intakeCase{"scalar member of an array of objects", map[string]any{"items": []any{map[string]any{"name": []any{"x"}}}},
			scalarRefusal("items.name", 7, forma.ValueTypeText, "an array")},
		intakeCase{"literal key across an array of objects", map[string]any{"items.name": []any{"x", "y"}},
			scalarRefusal("items.name", 7, forma.ValueTypeText, "an array")},
		intakeCase{"object at a list", map[string]any{"tags": map[string]any{}},
			refusedValue("tags", 6) + "declared type list takes an array of single text values, got an empty object"},
		intakeCase{"object as a list's element", map[string]any{"tags": []any{"a", map[string]any{"x": "y"}}},
			refusedValue("tags", 6) + `list element at array indices "1" takes a single text value, got an object; a list's elements cannot be objects`},
		intakeCase{"object as an element of a list inside an array of objects",
			map[string]any{"items": []any{map[string]any{"tags": []any{map[string]any{}}}}},
			refusedValue("items.tags", 8) + `list element at array indices "0,0" takes a single text value, got an empty object; a list's elements cannot be objects`},
	)
}

// acceptedShapes are updates the rule leaves alone: single values, lists and
// their empty-list marker, arrays of objects, an object at a parent, and a Go
// value the conversion judges (a []string stringifies as text, as before).
func acceptedShapes() []map[string]any {
	return []map[string]any{
		{"tenant": "T", "note": "N", "count": 1},
		{"contact": map[string]any{"total": int64(1)}, "contact.name": "n"},
		{"contact": map[string]any{}},
		{"tags": []any{"a", "b"}},
		{"tags": []any{}},
		{"items": []any{map[string]any{"name": "x"}, map[string]any{"tags": []any{}}}},
		{"items.tags": []any{"a"}},
		{"items": []any{}},
		{"note": []string{"a"}},
	}
}

func TestCheckUpdateValuesRefusesContainersWhereASingleValueBelongs(t *testing.T) {
	for _, tc := range refusedShapes() {
		t.Run(tc.name, func(t *testing.T) {
			checked, err := CheckUpdateValues(tc.doc, shapeCache())
			require.NoError(t, err, "a shape is no unstorable text")
			require.ErrorIs(t, checked.Refusal, forma.ErrInvalidInput)
			msg, published := forma.ResolvePublicMessage(checked.Refusal)
			require.True(t, published, "must publish, not redact: %v", checked.Refusal)
			require.Equal(t, tc.want, msg)
		})
	}
	for i, doc := range acceptedShapes() {
		t.Run(fmt.Sprintf("accepted %d", i), func(t *testing.T) {
			checked, err := CheckUpdateValues(doc, shapeCache())
			require.NoError(t, err)
			require.NoError(t, checked.Refusal, "updates %v", doc)
		})
	}
}

// A shape refusal is of the conversion's class: the first refusal of that
// class in sorted key order is reported, and text PostgreSQL cannot hold
// inside a refused container is still the unconditional error (#679).
func TestCheckUpdateValuesShapeKeepsTheConversionOrderAndTextPrecedence(t *testing.T) {
	checked, err := CheckUpdateValues(map[string]any{"count": 1.5, "note": map[string]any{}}, shapeCache())
	require.NoError(t, err)
	require.ErrorContains(t, checked.Refusal, "attribute 'count'")

	checked, err = CheckUpdateValues(map[string]any{"contact": map[string]any{"total": []any{}}, "count": 1.5}, shapeCache())
	require.NoError(t, err)
	require.ErrorContains(t, checked.Refusal, "attribute 'contact.total'")

	_, err = CheckUpdateValues(map[string]any{"note": []any{"a\x00b"}}, shapeCache())
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.ErrorContains(t, err, "attribute 'note'")
	require.ErrorContains(t, err, nulRule)
}

// The merge refuses the caller's container in the document the update
// writes, whatever route reached it, and the published refusal is the
// intake's.
func TestMergeUpdateRefusesTheCallersContainerAtAScalar(t *testing.T) {
	ctx := context.Background()
	tr := NewPersistentRecordTransformer(shapeRegistry(shapeCache()))
	rowID := uuid.Must(uuid.NewV7())
	stored, err := tr.ToPersistentRecord(ctx, 696, rowID, map[string]any{
		"tenant": "T", "note": "N", "contact": map[string]any{"total": int64(1)},
		"items": []any{map[string]any{"name": "a"}},
	})
	require.NoError(t, err)
	for _, tc := range refusedShapes() {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tr.MergeUpdate(ctx, stored, setting(tc.doc))
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			msg, published := forma.ResolvePublicMessage(err)
			require.True(t, published, "must publish, not redact: %v", err)
			require.Equal(t, tc.want, msg)
		})
	}
	for i, doc := range acceptedShapes() {
		t.Run(fmt.Sprintf("accepted %d", i), func(t *testing.T) {
			merged, err := tr.MergeUpdate(ctx, stored, setting(doc))
			require.NoError(t, err)
			_, err = tr.ToPersistentRecord(ctx, 696, rowID, merged)
			require.NoError(t, err)
		})
	}
}

// Before #696, {} at a bound scalar wrote nothing and so kept the column, the
// same {} at an EAV scalar removed the record, and ["a"] stored an element
// record under a scalar. None of these documents reaches the write now.
func TestMergeUpdateNoLongerDecomposesAScalarsContainer(t *testing.T) {
	ctx := context.Background()
	tr := NewPersistentRecordTransformer(shapeRegistry(shapeCache()))
	stored, err := tr.ToPersistentRecord(ctx, 696, uuid.Must(uuid.NewV7()), map[string]any{"tenant": "T", "note": "N"})
	require.NoError(t, err)
	for _, updates := range []map[string]any{
		{"tenant": map[string]any{}}, {"note": map[string]any{}}, {"note": []any{}}, {"note": []any{"a"}},
	} {
		_, err := tr.MergeUpdate(ctx, stored, setting(updates))
		require.ErrorIs(t, err, forma.ErrInvalidInput, "updates %v", updates)
	}
}

// A cyclic container at a scalar is refused by the nesting cap, in the merge
// and at intake, rather than recursing without end (#406).
func TestShapeRuleStaysUnderTheNestingCap(t *testing.T) {
	cyclic := map[string]any{}
	cyclic["x"] = cyclic
	looped := []any{nil}
	looped[0] = looped

	ctx := context.Background()
	tr := NewPersistentRecordTransformer(shapeRegistry(shapeCache()))
	stored, err := tr.ToPersistentRecord(ctx, 696, uuid.Must(uuid.NewV7()), map[string]any{"note": "N"})
	require.NoError(t, err)
	for name, value := range map[string]any{"map": cyclic, "array": looped} {
		t.Run(name, func(t *testing.T) {
			_, err := tr.MergeUpdate(ctx, stored, setting(map[string]any{"note": value}))
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			require.ErrorContains(t, err, "payload nesting exceeds")

			// The intake leaves the cap to the merged write, as before.
			checked, err := CheckUpdateValues(map[string]any{"note": value}, shapeCache())
			require.NoError(t, err)
			require.NoError(t, checked.Refusal)
		})
	}
}

// A stored row is written back in the shape it was read. A primitive array
// recorded under its element type (pre-#204) beside a sibling, which is also
// how a row read without array ancestry rebuilds an array-of-objects member
// (rebuildInferred), holds stored values only at that scalar, so an update that does not
// address it writes every record back. A caller who sends such an array is
// refused: the attributes file says the attribute holds a single value.
func TestMergeUpdateWritesBackAStoredContainerAtAScalar(t *testing.T) {
	ctx := context.Background()
	cache := forma.SchemaAttributeCache{
		"stage":         {AttributeName: "stage", AttributeID: 1, ValueType: forma.ValueTypeText},
		"legacy.phones": {AttributeName: "legacy.phones", AttributeID: 2, ValueType: forma.ValueTypeText},
		"legacy.name":   {AttributeName: "legacy.name", AttributeID: 3, ValueType: forma.ValueTypeText},
	}
	tr := NewPersistentRecordTransformer(shapeRegistry(cache))
	rowID := uuid.Must(uuid.NewV7())
	stored := &model.PersistentRecord{SchemaID: 696, RowID: rowID, OtherAttributes: []model.EAVRecord{
		textRecord(rowID, 2, "0", "111"), textRecord(rowID, 2, "1", "222"), textRecord(rowID, 3, "", "L"),
	}}
	for i := range stored.OtherAttributes {
		stored.OtherAttributes[i].SchemaID = 696
	}
	read, err := tr.FromPersistentRecord(ctx, stored)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"legacy": map[string]any{"name": "L", "phones": []any{"111", "222"}}}, read)

	merged, err := tr.MergeUpdate(ctx, stored, setting(map[string]any{"stage": "x"}))
	require.NoError(t, err)
	written, err := tr.ToPersistentRecord(ctx, 696, rowID, merged)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"1[]=x", "2[0]=111", "2[1]=222", "3[]=L"}, storedKeys(written.OtherAttributes))

	merged, err = tr.MergeUpdate(ctx, stored, setting(map[string]any{"legacy.phones": "333"}))
	require.NoError(t, err, "a single value replaces the stored array")
	written, err = tr.ToPersistentRecord(ctx, 696, rowID, merged)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"2[]=333", "3[]=L"}, storedKeys(written.OtherAttributes))

	_, err = tr.MergeUpdate(ctx, stored, setting(map[string]any{"legacy": map[string]any{"phones": []any{"333"}}}))
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.ErrorContains(t, err, scalarRefusal("legacy.phones", 2, forma.ValueTypeText, "an array"))
}
