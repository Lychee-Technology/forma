package transform

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lychee-technology/forma"
)

// Which parents are arrays of objects, not only how many, and the literal
// dotted spelling, which takes no array at the parents it names (#696
// review round 4).

// arrangementRegistry serves document with a.b.v and items.phones, text
// attributes a document may declare primitive arrays recorded under their
// element type before #204.
func arrangementRegistry(document string) *stubSchemaRegistry {
	cache := forma.SchemaAttributeCache{
		"items.phones": {AttributeName: "items.phones", AttributeID: 3, ValueType: forma.ValueTypeText},
		"a.b.v":        {AttributeName: "a.b.v", AttributeID: 4, ValueType: forma.ValueTypeText},
	}
	return &stubSchemaRegistry{schemaID: shapeSchemaID, schemaName: "shape", cache: cache, document: document}
}

// One alternative declares a an array of objects holding an object b with a
// single value v; the other declares a an object holding an array of b
// objects with v an array of strings. Both put one array of objects around
// v, but a value under one is not under the other: an array sent for v
// under the first is refused, and the second keeps v's array, in either
// order. A literal dotted key crossing a parent either alternative may
// declare an array of objects is admitted by both, so v's array there is
// refused: the alternatives disagree on it.
func TestDeclaredShapeArrangementOfParents(t *testing.T) {
	const scalarUnderArray = `{"type": "array", "items": {"type": "object", "properties": {
		"b": {"type": "object", "properties": {"v": {"type": "string"}}}}}}`
	const arrayUnderObject = `{"type": "object", "properties": {
		"b": {"type": "array", "items": {"type": "object", "properties": {"v": {"type": "array", "items": {"type": "string"}}}}}}}`
	for _, keyword := range []string{"anyOf", "oneOf"} {
		for _, order := range [][2]string{{scalarUnderArray, arrayUnderObject}, {arrayUnderObject, scalarUnderArray}} {
			label := fmt.Sprintf("%s, %s first", keyword, order[0][:16])
			registry := arrangementRegistry(fmt.Sprintf(`{"type": "object", "properties": {"a": {%q: [%s, %s]}}}`, keyword, order[0], order[1]))
			declared := SchemaShapesOf(registry, shapeSchemaID).Declared("a.b.v")
			assert.Equal(t, ShapeScalar, declared.At(Ancestry{1, 0}, 0), label)
			assert.Equal(t, ShapeArray, declared.At(Ancestry{0, 1}, 0), label)
			assert.Equal(t, ShapeScalar, declared.At(Ancestry{0, 1}, 1), label)
			assert.Equal(t, ShapeUnresolved, declared.At(Ancestry{1, 1}, 0), label)
			assert.Equal(t, ShapeUnresolved, declared.At(Ancestry{0, 0}, 0), label)
			assert.Equal(t, ShapeScalar, declared.At(Ancestry{AncestorCrossed, AncestorCrossed}, 0), label)
			sure, most, exact := declared.Indices()
			assert.Equal(t, []any{1, 2, true}, []any{sure, most, exact}, label)

			requireShapeRefused(t, registry, map[string]any{"a": []any{map[string]any{"b": map[string]any{"v": []any{"x"}}}}},
				shapeRefusal("a.b.v", 4, "an array of 1 element"), label+", array of objects at a")
			requireShapeRefused(t, registry, map[string]any{"a": []any{map[string]any{"b": map[string]any{"v": []any{}}}}},
				shapeRefusal("a.b.v", 4, "an empty array"), label+", array of objects at a, empty array")
			requireShapeAccepted(t, registry, map[string]any{"a": map[string]any{"b": []any{map[string]any{"v": []any{"x", "y"}}}}},
				[]string{"4[0,0]=x", "4[0,1]=y"}, label+", array of objects at b")
			requireShapeAccepted(t, registry, map[string]any{"a": []any{map[string]any{"b": map[string]any{"v": "x"}}}},
				[]string{"4[0]=x"}, label+", single value under the array of objects at a")
			requireShapeRefused(t, registry, map[string]any{"a": map[string]any{"b": []any{map[string]any{"v": []any{[]any{"x"}}}}}},
				elementRefusal("a.b.v", 4, "0,0", "an array of 1 element"), label+", array of objects at b, inner array")

			requireShapeAccepted(t, registry, map[string]any{"a": map[string]any{"b.v": []any{"x"}}},
				[]string{"4[0]=x"}, label+", dotted b.v in an object a")
			requireShapeRefused(t, registry, map[string]any{"a": []any{map[string]any{"b.v": []any{"x"}}}},
				shapeRefusal("a.b.v", 4, "an array of 1 element"), label+", dotted b.v in an array of objects at a")
			requireShapeRefused(t, registry, map[string]any{"a.b.v": []any{"x"}},
				shapeRefusal("a.b.v", 4, "an array of 1 element"), label+", dotted a.b.v")
		}
	}
}

// A primitive array declared under an array of objects, written through a
// literal dotted key at the root: the key takes no index for items, so its
// array is phones' own, as the nested spelling's is inside an element, and
// each element is still judged where the document declares a single value.
func TestDeclaredShapeDottedKeyAcrossArrayOfObjects(t *testing.T) {
	registry := arrangementRegistry(`{"type": "object", "properties": {"items": {"type": "array", "items": {
		"type": "object", "properties": {"phones": {"type": "array", "items": {"type": "string"}}}}}}}`)
	declared := SchemaShapesOf(registry, shapeSchemaID).Declared("items.phones")
	assert.Equal(t, ShapeArray, declared.At(Ancestry{AncestorCrossed}, 0))
	assert.Equal(t, ShapeScalar, declared.At(Ancestry{AncestorCrossed}, 1))
	assert.Equal(t, ShapeArray, declared.At(Ancestry{1}, 0))

	requireShapeAccepted(t, registry, map[string]any{"items.phones": []any{"1"}}, []string{"3[0]=1"}, "dotted spelling")
	requireShapeAccepted(t, registry, map[string]any{"items.phones": []any{"1", "2"}}, []string{"3[0]=1", "3[1]=2"}, "dotted spelling, two elements")
	requireShapeAccepted(t, registry, map[string]any{"items": []any{map[string]any{"phones": []any{"1"}}, map[string]any{"phones": []any{"2", "3"}}}},
		[]string{"3[0,0]=1", "3[1,0]=2", "3[1,1]=3"}, "nested spelling")
	requireShapeAccepted(t, registry, map[string]any{"items": []any{map[string]any{"phones": "1"}}}, []string{"3[0]=1"}, "nested spelling, single value")

	for name, tc := range map[string]struct {
		doc  map[string]any
		want string
	}{
		"dotted spelling, inner array":       {map[string]any{"items.phones": []any{[]any{"1"}}}, elementRefusal("items.phones", 3, "0", "an array of 1 element")},
		"dotted spelling, inner empty array": {map[string]any{"items.phones": []any{"1", []any{}}}, elementRefusal("items.phones", 3, "1", "an empty array")},
		"nested spelling, inner array":       {map[string]any{"items": []any{map[string]any{"phones": []any{[]any{"1"}}}}}, elementRefusal("items.phones", 3, "0,0", "an array of 1 element")},
		"dotted spelling, object":            {map[string]any{"items.phones": map[string]any{"x": "1"}}, "declared type text takes a single value, got an object"},
	} {
		requireShapeRefused(t, registry, tc.doc, tc.want, name)
	}
}
