package transform

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

// Alternatives that do not settle a type, and alternatives that disagree on
// a parent's shape (#696 review round 3). Neither the write nor the intake
// consults JSON Schema validation, so what they answer here is what a
// manager answers in report-only mode and with no validator configured.

// alternativeRegistry serves document with the text scalars note and
// phones (a primitive array recorded under its element type before #204),
// and items.phones, a member of items.
func alternativeRegistry(document string) *stubSchemaRegistry {
	cache := forma.SchemaAttributeCache{
		"note":         {AttributeName: "note", AttributeID: 1, ValueType: forma.ValueTypeText},
		"phones":       {AttributeName: "phones", AttributeID: 2, ValueType: forma.ValueTypeText},
		"items.phones": {AttributeName: "items.phones", AttributeID: 3, ValueType: forma.ValueTypeText},
	}
	return &stubSchemaRegistry{schemaID: shapeSchemaID, schemaName: "shape", cache: cache, document: document}
}

// requireShapeAccepted writes doc and judges it at the intake: both accept
// it, and the write stores want.
func requireShapeAccepted(t *testing.T, registry *stubSchemaRegistry, doc map[string]any, want []string, label string) {
	t.Helper()
	written, err := NewPersistentRecordTransformer(registry).ToPersistentRecord(context.Background(), shapeSchemaID, uuid.New(), doc)
	require.NoError(t, err, label)
	assert.ElementsMatch(t, want, writtenKeys(written), label)
	checked, err := CheckUpdateValues(doc, registry.cache, SchemaShapesOf(registry, shapeSchemaID))
	require.NoError(t, err, label)
	assert.NoError(t, checked.Refusal, label)
}

// requireShapeRefused writes doc and judges it at the intake: both refuse
// it with want.
func requireShapeRefused(t *testing.T, registry *stubSchemaRegistry, doc map[string]any, want, label string) {
	t.Helper()
	_, err := NewPersistentRecordTransformer(registry).ToPersistentRecord(context.Background(), shapeSchemaID, uuid.New(), doc)
	requireShapeRefusal(t, err, want, "write %s", label)
	checked, err := CheckUpdateValues(doc, registry.cache, SchemaShapesOf(registry, shapeSchemaID))
	require.NoError(t, err, label)
	requireShapeRefusal(t, checked.Refusal, want, "intake %s", label)
}

func elementRefusal(attr string, id int, indices, got string) string {
	return fmt.Sprintf("invalid value for attribute '%s' (attrID=%d): declared type text takes a single value at array indices %q, got %s",
		attr, id, indices, got)
}

// An alternative that admits only null, or leaves the type open, does not
// hide the array another alternative declares, in either order: phones is
// still the pre-#204 array it was, and its elements are still judged. Below
// the open alternative an element's shape is not settled, so the census
// does not take it as exact; an element sent as an array is refused all the
// same. A single value whose other alternative is null is that value; one
// whose other is open is unresolved, and an array there is still refused.
func TestDeclaredShapeNullableAndOpenAlternatives(t *testing.T) {
	const asArray = `{"type": "array", "items": {"type": "string"}}`
	alternatives := map[string]struct {
		branch  string
		element DeclaredShape
		exact   bool
		note    DeclaredShape
	}{
		"nullable":  {`{"type": "null"}`, ShapeScalar, true, ShapeScalar},
		"open":      {`{}`, ShapeUnresolved, false, ShapeUnresolved},
		"described": {`{"description": "any"}`, ShapeUnresolved, false, ShapeUnresolved},
	}
	for _, keyword := range []string{"anyOf", "oneOf"} {
		for kind, alt := range alternatives {
			for _, order := range [][2]string{{asArray, alt.branch}, {alt.branch, asArray}} {
				label := fmt.Sprintf("%s %s, %s then %s", keyword, kind, order[0], order[1])
				registry := alternativeRegistry(fmt.Sprintf(`{"type": "object", "properties": {
					"phones": {%q: [%s, %s]}, "note": {%q: [{"type": "string"}, %s]}}}`,
					keyword, order[0], order[1], keyword, alt.branch))
				shapes := SchemaShapesOf(registry, shapeSchemaID)
				phones := shapes.Declared("phones")
				assert.Equal(t, ShapeArray, phones.At(0, 0), label)
				assert.Equal(t, alt.element, phones.At(0, 1), label)
				_, most, exact := phones.Indices()
				assert.Equal(t, 1, most, label)
				assert.Equal(t, alt.exact, exact, label)
				assert.Equal(t, alt.note, shapes.Declared("note").At(0, 0), label)

				requireShapeAccepted(t, registry, map[string]any{"phones": []any{"1", "2"}}, []string{"2[0]=1", "2[1]=2"}, label)
				requireShapeAccepted(t, registry, map[string]any{"phones": "1"}, []string{"2[]=1"}, label)
				requireShapeRefused(t, registry, map[string]any{"phones": []any{[]any{"1"}}}, elementRefusal("phones", 2, "0", "an array of 1 element"), label)
				requireShapeRefused(t, registry, map[string]any{"phones": []any{"1", []any{}}}, elementRefusal("phones", 2, "1", "an empty array"), label)
				noteRefusal := shapeRefusal("note", 1, "an array of 1 element")
				if alt.note == ShapeUnresolved {
					noteRefusal = noArrayRefusal("note", 1)
				}
				requireShapeRefused(t, registry, map[string]any{"note": []any{"n"}}, noteRefusal, label)
			}
		}
	}

	// A type union with null, and a nullable parent of an array of objects.
	registry := alternativeRegistry(`{"type": "object", "properties": {
		"phones": {"type": ["null", "array"], "items": {"type": "string"}},
		"items": {"anyOf": [{"type": "null"}, {"type": "array", "items": {"type": "object", "properties": {"phones": ` + asArray + `}}}]}}}`)
	requireShapeAccepted(t, registry, map[string]any{"phones": []any{"1"}}, []string{"2[0]=1"}, "type union")
	requireShapeAccepted(t, registry, map[string]any{"items": []any{map[string]any{"phones": []any{"1"}}}}, []string{"3[0,0]=1"}, "nullable parent")
	requireShapeRefused(t, registry, map[string]any{"items": []any{map[string]any{"phones": []any{[]any{"1"}}}}},
		elementRefusal("items.phones", 3, "0,0", "an array of 1 element"), "nullable parent")
}

// Where one alternative declares items an object and another an array of
// objects, and both declare phones a pre-#204 array of strings, the array
// ancestry of a value is the position it takes in the write, not the most
// any alternative admits: items: {phones: [["1"]]} meets phones under no
// array of objects, so ["1"] is an element of phones, and is refused, as is
// the empty inner array, which would otherwise write nothing. The valid
// object and array-of-objects forms are written as before, in either order.
// A stored record's indices do not say which form the row took, so the
// most indices every reading admits is the census's certainty (sure), not
// the most any admits.
func TestDeclaredShapeAncestryIsTheWalkedPosition(t *testing.T) {
	const asObject = `{"type": "object", "properties": {"phones": {"type": "array", "items": {"type": "string"}}}}`
	const asArray = `{"type": "array", "items": ` + asObject + `}`
	for _, keyword := range []string{"anyOf", "oneOf"} {
		for _, order := range [][2]string{{asObject, asArray}, {asArray, asObject}} {
			label := fmt.Sprintf("%s, %s first", keyword, order[0][:22])
			registry := alternativeRegistry(fmt.Sprintf(`{"type": "object", "properties": {"items": {%q: [%s, %s]}}}`, keyword, order[0], order[1]))
			declared := SchemaShapesOf(registry, shapeSchemaID).Declared("items.phones")
			for ancestors := 0; ancestors <= 1; ancestors++ {
				assert.Equal(t, ShapeArray, declared.At(ancestors, 0), "%s, %d arrays of objects", label, ancestors)
				assert.Equal(t, ShapeScalar, declared.At(ancestors, 1), "%s, %d arrays of objects", label, ancestors)
			}
			assert.Equal(t, ShapeUnresolved, declared.At(2, 0), label)
			sure, most, exact := declared.Indices()
			assert.Equal(t, []any{1, 2, true}, []any{sure, most, exact}, label)

			requireShapeAccepted(t, registry, map[string]any{"items": map[string]any{"phones": []any{"1", "2"}}},
				[]string{"3[0]=1", "3[1]=2"}, label+", object parent")
			requireShapeAccepted(t, registry, map[string]any{"items": []any{map[string]any{"phones": []any{"1"}}, map[string]any{"phones": []any{"2", "3"}}}},
				[]string{"3[0,0]=1", "3[1,0]=2", "3[1,1]=3"}, label+", array parent")
			requireShapeAccepted(t, registry, map[string]any{"items.phones": []any{"1"}}, []string{"3[0]=1"}, label+", dotted spelling")

			for name, doc := range map[string]map[string]any{
				"object parent, inner array":       {"items": map[string]any{"phones": []any{[]any{"1"}}}},
				"object parent, inner empty array": {"items": map[string]any{"phones": []any{[]any{}}}},
				"dotted spelling, inner array":     {"items.phones": []any{[]any{"1"}}},
			} {
				got := "an array of 1 element"
				if name == "object parent, inner empty array" {
					got = "an empty array"
				}
				requireShapeRefused(t, registry, doc, elementRefusal("items.phones", 3, "0", got), label+", "+name)
			}
			requireShapeRefused(t, registry, map[string]any{"items": []any{map[string]any{"phones": []any{"1", []any{"2"}}}}},
				elementRefusal("items.phones", 3, "0,1", "an array of 1 element"), label+", array parent, inner array")
			requireShapeRefused(t, registry, map[string]any{"items": []any{[]any{map[string]any{"phones": []any{"1"}}}}},
				"invalid value for attribute 'items.phones' (attrID=3): declared type text takes a single value (the schema document declares no array here)",
				label+", two arrays of objects the document does not declare")
		}
	}
}
