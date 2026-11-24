package transform

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// The shape rule of #696: an object or array where a single value belongs is
// refused whole, by the merged write and by the update intake alike, in
// every validation mode (neither consults JSON Schema validation).

const shapeSchemaID int16 = 696

// shapeSchemaDocument is the shape schema's document text. Its declarations reach
// the shape rule however the document spells them: a type union with null,
// a $ref into the document, a $ref into a sibling schema's file, an anyOf.
const shapeSchemaDocument = `{"type": "object", "$defs": {"code": {"type": "string"}}, "properties": {
	"tenant": {"type": "string"}, "note": {"type": "string"}, "count": {"type": "integer"},
	"addr": {"type": "object", "properties": {"city": {"type": "string"}}},
	"items": {"type": "array", "items": {"type": "object", "properties": {
		"name": {"type": "string"}, "tags": {"type": "array", "items": {"type": "string"}},
		"dims": {"type": "object", "properties": {"w": {"type": "string"}}}}}},
	"tags": {"type": "array", "items": {"type": "string"}},
	"phones": {"type": "array", "items": {"type": "string"}},
	"opaque": {},
	"maybe": {"type": ["string", "null"]},
	"code": {"$ref": "#/$defs/code"},
	"lead": {"$ref": "other.json#/$defs/lead_id", "description": "beside the $ref"},
	"either": {"anyOf": [{"type": "string"}, {"type": "integer"}]},
	"elsewhere": {"$ref": "missing.json#/$defs/x"}}}`

// newShapeRegistry serves the shape schema: a bound and an EAV text scalar,
// an integer, a nested scalar, an array of objects with a scalar and a list
// member, a list, a primitive array recorded under its element type before
// #204 (phones), scalars the document declares through a union, a $ref or
// an anyOf, an object member of the array of objects (items.dims.w), a leaf
// it leaves open (opaque) and one behind a $ref to a file
// the registry does not serve (elsewhere). ancestry says whether the
// registry derived array ancestry, as the file registry does when its
// parser resolves every attribute's ancestors.
func newShapeRegistry(ancestry bool) *stubSchemaRegistry {
	ancestors := func(names ...string) []string {
		if !ancestry {
			return nil
		}
		return append([]string{}, names...)
	}
	cache := forma.SchemaAttributeCache{
		"tenant":       {AttributeID: 1, ValueType: forma.ValueTypeText, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01}},
		"note":         {AttributeID: 2, ValueType: forma.ValueTypeText},
		"count":        {AttributeID: 3, ValueType: forma.ValueTypeInteger},
		"addr.city":    {AttributeID: 4, ValueType: forma.ValueTypeText},
		"items.name":   {AttributeID: 5, ValueType: forma.ValueTypeText},
		"items.tags":   {AttributeID: 6, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeText},
		"tags":         {AttributeID: 7, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeText},
		"phones":       {AttributeID: 8, ValueType: forma.ValueTypeText},
		"opaque":       {AttributeID: 9, ValueType: forma.ValueTypeText},
		"maybe":        {AttributeID: 10, ValueType: forma.ValueTypeText},
		"code":         {AttributeID: 11, ValueType: forma.ValueTypeText},
		"lead":         {AttributeID: 12, ValueType: forma.ValueTypeText},
		"either":       {AttributeID: 13, ValueType: forma.ValueTypeText},
		"elsewhere":    {AttributeID: 14, ValueType: forma.ValueTypeText},
		"items.dims.w": {AttributeID: 15, ValueType: forma.ValueTypeText},
	}
	for name, meta := range cache {
		meta.AttributeName = name
		meta.ArrayAncestors = ancestors()
		if strings.HasPrefix(name, "items.") {
			meta.ArrayAncestors = ancestors("items")
		}
		cache[name] = meta
	}
	return &stubSchemaRegistry{schemaID: shapeSchemaID, schemaName: "shape", cache: cache, document: shapeSchemaDocument,
		siblings: map[string]string{"other": `{"$defs": {"lead_id": {"type": "string", "format": "uuid"}}}`}}
}

func shapeRefusal(attr string, id int, got string) string {
	return fmt.Sprintf("invalid value for attribute '%s' (attrID=%d): declared type text takes a single value, got %s", attr, id, got)
}

type shapeCase struct {
	name string
	doc  map[string]any
	want string // a prefix of the published message
}

func refusedShapeCases() []shapeCase {
	return []shapeCase{
		{"bound, empty object", map[string]any{"tenant": map[string]any{}}, shapeRefusal("tenant", 1, "an empty object")},
		{"bound, empty array", map[string]any{"tenant": []any{}}, shapeRefusal("tenant", 1, "an empty array")},
		{"bound, singleton array", map[string]any{"tenant": []any{"a"}}, shapeRefusal("tenant", 1, "an array of 1 element")},
		{"bound, object", map[string]any{"tenant": map[string]any{"x": "y"}}, shapeRefusal("tenant", 1, "an object")},
		{"EAV, empty object", map[string]any{"note": map[string]any{}}, shapeRefusal("note", 2, "an empty object")},
		{"EAV, empty array", map[string]any{"note": []any{}}, shapeRefusal("note", 2, "an empty array")},
		{"EAV, array", map[string]any{"note": []any{"a", "b"}}, shapeRefusal("note", 2, "an array of 2 elements")},
		{"EAV, nested empty array", map[string]any{"note": []any{[]any{}}}, shapeRefusal("note", 2, "an array of 1 element")},
		{"EAV, array of an object", map[string]any{"note": []any{map[string]any{}}}, shapeRefusal("note", 2, "an array of 1 element")},
		{"integer, empty object", map[string]any{"count": map[string]any{}},
			"invalid value for attribute 'count' (attrID=3): declared type integer takes a single value, got an empty object"},
		{"nested spelling", map[string]any{"addr": map[string]any{"city": []any{}}}, shapeRefusal("addr.city", 4, "an empty array")},
		{"dotted spelling", map[string]any{"addr.city": map[string]any{}}, shapeRefusal("addr.city", 4, "an empty object")},
		{"member of an array of objects, array", map[string]any{"items": []any{map[string]any{"name": []any{"a"}}}},
			shapeRefusal("items.name", 5, "an array of 1 element")},
		{"member of an array of objects, object", map[string]any{"items": []any{map[string]any{"name": "ok"}, map[string]any{"name": map[string]any{}}}},
			shapeRefusal("items.name", 5, "an empty object")},
		{"list, object", map[string]any{"tags": map[string]any{}},
			"invalid value for attribute 'tags' (attrID=7): declared type list takes an array of text values, got an empty object"},
		{"list element, object", map[string]any{"tags": []any{"a", map[string]any{"x": "y"}}},
			"invalid value for attribute 'tags' (attrID=7): declared type list takes an array of text values, got an object"},
		{"pre-#204 array element, object", map[string]any{"phones": []any{map[string]any{}}}, shapeRefusal("phones", 8, "an empty object")},
		{"pre-#204 array element, array", map[string]any{"phones": []any{[]any{"1"}}},
			`invalid value for attribute 'phones' (attrID=8): declared type text takes a single value at array indices "0", got an array of 1 element`},
		{"pre-#204 array element, empty array", map[string]any{"phones": []any{"1", []any{}}},
			`invalid value for attribute 'phones' (attrID=8): declared type text takes a single value at array indices "1", got an empty array`},
		{"undeclared leaf, object", map[string]any{"opaque": map[string]any{}}, shapeRefusal("opaque", 9, "an empty object")},
		{"nullable type union, array", map[string]any{"maybe": []any{"a"}}, shapeRefusal("maybe", 10, "an array of 1 element")},
		{"local $ref, empty array", map[string]any{"code": []any{}}, shapeRefusal("code", 11, "an empty array")},
		{"sibling file $ref, array", map[string]any{"lead": []any{"x"}}, shapeRefusal("lead", 12, "an array of 1 element")},
		{"anyOf of single values, array", map[string]any{"either": []any{"x"}}, shapeRefusal("either", 13, "an array of 1 element")},
	}
}

// acceptedShapeCases are containers the rule leaves to the walk, with the
// records the write stores for them.
func acceptedShapeCases() []struct {
	name string
	doc  map[string]any
	want []string
} {
	return []struct {
		name string
		doc  map[string]any
		want []string
	}{
		{"list", map[string]any{"tags": []any{"a", "b"}}, []string{"7[0]=a", "7[1]=b"}},
		{"empty list marker", map[string]any{"tags": []any{}}, []string{"7[]=<none>"}},
		{"array of objects", map[string]any{"items": []any{map[string]any{"name": "a", "tags": []any{}}, map[string]any{"name": "b"}}},
			[]string{"5[0]=a", "6[0]=<none>", "5[1]=b"}},
		{"pre-#204 primitive array", map[string]any{"phones": []any{"1", "2"}}, []string{"8[0]=1", "8[1]=2"}},
		{"pre-#204 primitive array, empty", map[string]any{"phones": []any{}}, []string{}},
		{"undeclared leaf, array", map[string]any{"opaque": []any{"a"}}, []string{"9[0]=a"}},
		{"$ref to a file the registry does not serve, array", map[string]any{"elsewhere": []any{"a"}}, []string{"14[0]=a"}},
		{"empty parent object", map[string]any{"addr": map[string]any{}}, []string{}},
		{"bytes as text", map[string]any{"note": []byte("n")}, []string{"2[]=n"}},
		{"Go slice, left to the conversion", map[string]any{"note": []string{"a"}}, []string{"2[]=[a]"}},
	}
}

func requireShapeRefusal(t *testing.T, err error, want string, msgAndArgs ...any) {
	t.Helper()
	require.ErrorIs(t, err, forma.ErrInvalidInput, msgAndArgs...)
	message, published := forma.ResolvePublicMessage(err)
	require.True(t, published, err)
	assert.Contains(t, message, want, msgAndArgs...)
	assert.Contains(t, message, "cannot be stored where a single value belongs", msgAndArgs...)
}

// eavKeys names the records a written row stores in eav_data or a column.
func writtenKeys(record *model.PersistentRecord) []string {
	keys := storedKeys(record.OtherAttributes)
	if tenant, set := record.TextItems[string(forma.MainColumnText01)]; set {
		keys = append(keys, "1[]="+tenant)
	}
	return keys
}

// The merged write refuses the container with or without array ancestry,
// and the intake refuses it first, as the Refusal of the conversion class.
// A member of an array of objects is a scalar position either way: the
// registry's ancestry decides how a row is read, not what the attribute is.
func TestScalarShapeRefusedByTheWriteAndTheIntake(t *testing.T) {
	ctx := context.Background()
	for _, ancestry := range []bool{true, false} {
		registry := newShapeRegistry(ancestry)
		tr := NewPersistentRecordTransformer(registry)
		for _, tc := range refusedShapeCases() {
			label := fmt.Sprintf("ancestry=%v %s", ancestry, tc.name)
			_, err := tr.ToPersistentRecord(ctx, shapeSchemaID, uuid.New(), tc.doc)
			requireShapeRefusal(t, err, tc.want, "write %s", label)

			checked, err := CheckUpdateValues(tc.doc, registry.cache, SchemaShapesOf(registry, shapeSchemaID))
			require.NoError(t, err, label)
			requireShapeRefusal(t, checked.Refusal, tc.want, "intake %s", label)
			assert.Equal(t, tc.doc, checked.Values, "intake keeps the value as sent, %s", label)
		}
	}
}

func TestScalarShapeLeavesOtherContainersToTheWalk(t *testing.T) {
	ctx := context.Background()
	registry := newShapeRegistry(true)
	tr := NewPersistentRecordTransformer(registry)
	for _, tc := range acceptedShapeCases() {
		written, err := tr.ToPersistentRecord(ctx, shapeSchemaID, uuid.New(), tc.doc)
		require.NoError(t, err, tc.name)
		assert.ElementsMatch(t, tc.want, writtenKeys(written), tc.name)

		checked, err := CheckUpdateValues(tc.doc, registry.cache, SchemaShapesOf(registry, shapeSchemaID))
		require.NoError(t, err, tc.name)
		assert.NoError(t, checked.Refusal, tc.name)
	}
}

// Without a document, metadata cannot tell a scalar from a pre-#204
// primitive array, so only objects are refused.
func TestScalarShapeWithoutADocumentRefusesObjectsOnly(t *testing.T) {
	ctx := context.Background()
	registry := newShapeRegistry(true)
	registry.document = ""
	tr := NewPersistentRecordTransformer(registry)

	_, err := tr.ToPersistentRecord(ctx, shapeSchemaID, uuid.New(), map[string]any{"note": map[string]any{}})
	requireShapeRefusal(t, err, shapeRefusal("note", 2, "an empty object"))
	written, err := tr.ToPersistentRecord(ctx, shapeSchemaID, uuid.New(), map[string]any{"note": []any{"a"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"2[0]=a"}, writtenKeys(written))

	checked, err := CheckUpdateValues(map[string]any{"note": []any{"a"}}, registry.cache, SchemaShapes{})
	require.NoError(t, err)
	assert.NoError(t, checked.Refusal)
}

// The shape refusal is of the conversion class and keeps its order: the
// first refused position in the write's sorted key order wins, whichever
// rule refuses it.
func TestScalarShapeRefusalOrder(t *testing.T) {
	registry := newShapeRegistry(true)
	shapes := SchemaShapesOf(registry, shapeSchemaID)
	checked, err := CheckUpdateValues(map[string]any{"count": 1.5, "note": []any{}}, registry.cache, shapes)
	require.NoError(t, err)
	assert.ErrorContains(t, checked.Refusal, "invalid value for attribute 'count'")
	checked, err = CheckUpdateValues(map[string]any{"count": 2, "addr": map[string]any{"city": map[string]any{}}, "note": 1.5}, registry.cache, shapes)
	require.NoError(t, err)
	assert.ErrorContains(t, checked.Refusal, "invalid value for attribute 'addr.city'")
}

// storedShapeRow is a row of the shape schema whose note was stored as array
// elements, as report-only validation let an update write before #696.
func storedShapeRow(rowID uuid.UUID) *model.PersistentRecord {
	a, other := "a", "kept"
	return &model.PersistentRecord{SchemaID: shapeSchemaID, RowID: rowID, OtherAttributes: []model.EAVRecord{
		{SchemaID: shapeSchemaID, RowID: rowID, AttrID: 2, ArrayIndices: "0", ValueText: &a},
		{SchemaID: shapeSchemaID, RowID: rowID, AttrID: 4, ArrayIndices: "", ValueText: &other},
	}}
}

// An update that keeps such a stored value is refused as stored state,
// plainly, rather than deleting the record or blaming the caller; one that
// replaces the attribute writes the replacement.
func TestScalarShapeStoredArrayUnderAScalar(t *testing.T) {
	ctx := context.Background()
	tr := NewPersistentRecordTransformer(newShapeRegistry(true))
	rowID := uuid.New()

	for name, merge := range map[string]func(map[string]any) (map[string]any, error){
		"unrelated attribute": setting(map[string]any{"count": 1}),
		"no change":           keepAll,
	} {
		_, err := tr.MergeUpdate(ctx, storedShapeRow(rowID), merge)
		require.EqualError(t, err, fmt.Sprintf("record schema=696 row=%s attrID=2 arrayIndices=0: "+
			"stored text value of attribute 'note' rebuilds inside an object or array at attribute 'note', "+
			"where the write stores a single value, so this update cannot write it back; "+
			"an update must replace the attribute or a container holding it", rowID), name)
		require.NotErrorIs(t, err, forma.ErrInvalidInput, name)
	}

	merged, err := tr.MergeUpdate(ctx, storedShapeRow(rowID), setting(map[string]any{"note": "n"}))
	require.NoError(t, err)
	written, err := tr.ToPersistentRecord(ctx, shapeSchemaID, rowID, merged)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"2[]=n", "4[]=kept"}, writtenKeys(written))
}

// A caller's container survives the merge as sent, so the merged write
// refuses it as the caller's, whatever the row holds.
func TestScalarShapeCallerContainerReachesTheMergedWrite(t *testing.T) {
	ctx := context.Background()
	tr := NewPersistentRecordTransformer(newShapeRegistry(true))
	rowID := uuid.New()
	n := "n"
	stored := &model.PersistentRecord{SchemaID: shapeSchemaID, RowID: rowID, TextItems: map[string]string{"text_01": "T"},
		OtherAttributes: []model.EAVRecord{{SchemaID: shapeSchemaID, RowID: rowID, AttrID: 2, ValueText: &n}}}
	for _, attr := range []string{"tenant", "note"} {
		merged, err := tr.MergeUpdate(ctx, stored, setting(map[string]any{attr: map[string]any{}}))
		require.NoError(t, err, attr)
		_, err = tr.ToPersistentRecord(ctx, shapeSchemaID, rowID, merged)
		requireShapeRefusal(t, err, "invalid value for attribute '"+attr+"'", attr)
	}
}
