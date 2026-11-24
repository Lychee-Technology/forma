package transform

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
)

// What a schema document declares for an attribute, as the shape rule of
// #696 reads it (SchemaShapes.Declared).

func TestDeclaredShapeIndices(t *testing.T) {
	shapes := SchemaShapesOf(newShapeRegistry(false), shapeSchemaID)
	for _, tc := range []struct {
		name    string
		indices int
		exact   bool
	}{
		{"note", 0, true},
		{"addr.city", 0, true},
		{"items.name", 1, true},
		{"phones", 1, true},
		{"tags", 1, true},
		{"maybe", 0, true},
		{"lead", 0, true},
		{"opaque", 0, false},
		{"elsewhere", 0, false},
		{"undeclared", 0, false},
	} {
		sure, most, exact := shapes.Declared(tc.name).Indices()
		assert.Equal(t, tc.indices, sure, tc.name)
		assert.Equal(t, tc.indices, most, tc.name)
		assert.Equal(t, tc.exact, exact, tc.name)
	}
	phones := shapes.Declared("phones")
	assert.Equal(t, ShapeArray, phones.At(0, 0))
	assert.Equal(t, ShapeScalar, phones.At(0, 1))
	assert.Equal(t, ShapeScalar, phones.At(0, 2))
	assert.Equal(t, ShapeUnresolved, phones.At(1, 0), "phones inside an array of objects the document does not declare")
	name := shapes.Declared("items.name")
	assert.Equal(t, ShapeUnresolved, name.At(0, 0), "an object sent where the document declares an array of objects")
	assert.Equal(t, ShapeScalar, name.At(1, 0))
	assert.Equal(t, ShapeScalar, name.At(1, 1))
}

// A registry that serves the properties it parsed but no document text is
// read through those properties.
func TestDeclaredShapeFromParsedProperties(t *testing.T) {
	registry := newShapeRegistry(true)
	registry.document = ""
	registry.properties = map[string]*forma.PropertySchema{
		"note":   {Type: "string"},
		"phones": {Type: "array", Items: &forma.PropertySchema{Type: "string"}},
	}
	shapes := SchemaShapesOf(registry, shapeSchemaID)
	assert.Equal(t, ShapeScalar, shapes.Declared("note").At(0, 0))
	assert.Equal(t, ShapeArray, shapes.Declared("phones").At(0, 0))
	assert.Equal(t, ShapeUnresolved, shapes.Declared("opaque").At(0, 0))

	checked, err := CheckUpdateValues(map[string]any{"note": []any{"a"}}, registry.cache, shapes)
	require.NoError(t, err)
	requireShapeRefusal(t, checked.Refusal, shapeRefusal("note", 2, "an array of 1 element"))
}

// A $ref cycle, or a $ref the document does not hold, ends in no evidence.
func TestDeclaredShapeEndsOnARefCycle(t *testing.T) {
	registry := newShapeRegistry(true)
	registry.document = `{"type": "object", "$defs": {"a": {"$ref": "#/$defs/b"}, "b": {"$ref": "#/$defs/a"}},
		"properties": {"note": {"$ref": "#/$defs/a"}, "code": {"$ref": "#/$defs/none"}, "tenant": {"$ref": "../up.json"}}}`
	shapes := SchemaShapesOf(registry, shapeSchemaID)
	for _, name := range []string{"note", "code", "tenant"} {
		assert.Equal(t, ShapeUnresolved, shapes.Declared(name).At(0, 0), name)
	}
}

// The shipped schemas declare every non-list attribute so the shape rule
// can judge it, including visit's, whose registry derives no array ancestry
// because its parser leaves the $ref to lead.json open: leadId references a
// string there, and contactSnapshot an object of lead's (#696 review).
func TestDeclaredShapeOfShippedSchemas(t *testing.T) {
	registry, err := schemameta.NewFileSchemaRegistryFromDirectory("../../cmd/server/schemas")
	require.NoError(t, err)
	for _, schemaName := range registry.ListSchemas() {
		schemaID, cache, err := registry.GetSchemaAttributeCacheByName(schemaName)
		require.NoError(t, err)
		shapes := SchemaShapesOf(registry, schemaID)
		for name, meta := range cache {
			if meta.ValueType == forma.ValueTypeList {
				continue
			}
			declared := shapes.Declared(name)
			assert.NotEmpty(t, declared.placements, "%s.%s", schemaName, name)
			for _, p := range declared.placements {
				assert.NotEqual(t, ShapeUnresolved, p.shapes[0], "%s.%s", schemaName, name)
			}
		}
	}

	visitID, visit, err := registry.GetSchemaAttributeCacheByName("visit")
	require.NoError(t, err)
	require.Nil(t, visit["leadId"].ArrayAncestors, "the fixture must exercise a schema without derived ancestry")
	shapes := SchemaShapesOf(registry, visitID)
	assert.Equal(t, ShapeScalar, shapes.Declared("leadId").At(0, 0))
	assert.Equal(t, ShapeScalar, shapes.Declared("contactSnapshot.email").At(0, 0))

	// The update intake judges an update's values first, so leadId, a
	// required attribute, is refused as the shape there.
	for _, tc := range []struct {
		doc  map[string]any
		want string
	}{
		{map[string]any{"leadId": []any{"lead-1"}}, "invalid value for attribute 'leadId'"},
		{map[string]any{"contactSnapshot": map[string]any{"email": []any{}}}, "invalid value for attribute 'contactSnapshot.email'"},
	} {
		checked, err := CheckUpdateValues(tc.doc, visit, shapes)
		require.NoError(t, err)
		requireShapeRefusal(t, checked.Refusal, tc.want)
	}
	doc := map[string]any{
		"id": "v-1", "leadId": "lead-1", "userId": "u-1", "propertyId": "p-1",
		"scheduledStartAt": "2026-01-01T00:00:00Z", "status": "scheduled",
		"contactSnapshot": map[string]any{"isAnonymous": false, "email": []any{"a@x"}},
	}
	_, err = NewPersistentRecordTransformer(registry).ToPersistentRecord(context.Background(), visitID, uuid.New(), doc)
	requireShapeRefusal(t, err, "invalid value for attribute 'contactSnapshot.email'")
}

// An attribute declared in several alternatives of an anyOf or oneOf takes
// any shape one of them admits, whichever branch comes first: one branch
// declaring phones a string does not refuse the array another declares
// (#696 review). The branches are read wherever they are: around the
// properties, at the property, at an array's items and at a parent.
func TestDeclaredShapeAcrossAlternativeBranches(t *testing.T) {
	asString := `{"type": "string"}`
	asArray := `{"type": "array", "items": {"type": "string"}}`
	asNested := `{"type": "array", "items": {"type": "array", "items": {"type": "string"}}}`
	branches := func(keyword, first, second string) string {
		return `{"` + keyword + `": [` + first + `, ` + second + `]}`
	}
	declaring := func(property string) string {
		return `{"properties": {"phones": ` + property + `}}`
	}
	for _, keyword := range []string{"anyOf", "oneOf"} {
		for _, order := range [][2]string{{asString, asArray}, {asArray, asString}} {
			label := fmt.Sprintf("%s %s then %s", keyword, order[0], order[1])
			documents := map[string]string{
				"branches around the properties": `{"type": "object", "properties": {"note": {"type": "string"}}, "` + keyword +
					`": [` + declaring(order[0]) + `, ` + declaring(order[1]) + `]}`,
				"branches at the property": `{"type": "object", "properties": {"note": {"type": "string"}, "phones": ` +
					branches(keyword, order[0], order[1]) + `}}`,
				"own declaration and a branch": `{"type": "object", "properties": {"note": {"type": "string"}, "phones": ` +
					order[0] + `}, "` + keyword + `": [` + declaring(order[1]) + `, {"required": ["note"]}]}`,
			}
			for name, document := range documents {
				registry := newShapeRegistry(true)
				registry.document = document
				shapes := SchemaShapesOf(registry, shapeSchemaID)
				phones := shapes.Declared("phones")
				assert.Equal(t, ShapeArray, phones.At(0, 0), "%s, %s", name, label)
				assert.Equal(t, ShapeScalar, phones.At(0, 1), "%s, %s", name, label)
				assert.Equal(t, ShapeScalar, shapes.Declared("note").At(0, 0), "%s, %s", name, label)

				tr := NewPersistentRecordTransformer(registry)
				for _, value := range []any{[]any{"1", "2"}, "1"} {
					doc := map[string]any{"phones": value}
					_, err := tr.ToPersistentRecord(context.Background(), shapeSchemaID, uuid.New(), doc)
					require.NoError(t, err, "%s, %s, %v", name, label, value)
					checked, err := CheckUpdateValues(doc, registry.cache, shapes)
					require.NoError(t, err)
					assert.NoError(t, checked.Refusal, "%s, %s, %v", name, label, value)
				}
				_, err := tr.ToPersistentRecord(context.Background(), shapeSchemaID, uuid.New(), map[string]any{"phones": []any{[]any{"1"}}})
				requireShapeRefusal(t, err, `declared type text takes a single value at array indices "0"`, "%s, %s", name, label)
				_, err = tr.ToPersistentRecord(context.Background(), shapeSchemaID, uuid.New(), map[string]any{"note": []any{"n"}})
				requireShapeRefusal(t, err, shapeRefusal("note", 2, "an array of 1 element"), "%s, %s", name, label)
			}
		}
		for _, order := range [][2]string{{asArray, asNested}, {asNested, asArray}} {
			registry := newShapeRegistry(true)
			registry.document = `{"type": "object", "properties": {"phones": ` + branches(keyword, order[0], order[1]) + `}}`
			phones := SchemaShapesOf(registry, shapeSchemaID).Declared("phones")
			require.Len(t, phones.placements, 1)
			assert.Equal(t, []DeclaredShape{ShapeArray, ShapeArray, ShapeScalar}, phones.placements[0].shapes,
				"%s: an element one alternative declares an array is one", keyword)
		}
		for _, order := range [][2]string{
			{`{"type": "object", "properties": {"name": {"type": "string"}}}`, `{"type": "array", "items": {"type": "object", "properties": {"name": {"type": "string"}}}}`},
			{`{"type": "array", "items": {"type": "object", "properties": {"name": {"type": "string"}}}}`, `{"type": "object", "properties": {"name": {"type": "string"}}}`},
		} {
			registry := newShapeRegistry(true)
			registry.document = `{"type": "object", "properties": {"items": ` + branches(keyword, order[0], order[1]) + `}}`
			name := SchemaShapesOf(registry, shapeSchemaID).Declared("items.name")
			assert.Equal(t, ShapeScalar, name.At(0, 0), "%s: a parent one alternative declares an object", keyword)
			assert.Equal(t, ShapeScalar, name.At(1, 0), "%s: a parent one alternative declares an array of objects", keyword)
		}
	}
}

// Where one alternative leaves the property open and none declares an array,
// the declaration is unresolved: the write still refuses an array there.
func TestDeclaredShapeOpenAlternative(t *testing.T) {
	registry := newShapeRegistry(true)
	registry.document = `{"type": "object", "anyOf": [{"properties": {"note": {"type": "string"}}}, {"properties": {"note": {"description": "any"}}}]}`
	shapes := SchemaShapesOf(registry, shapeSchemaID)
	assert.Equal(t, ShapeUnresolved, shapes.Declared("note").At(0, 0))
	_, err := NewPersistentRecordTransformer(registry).ToPersistentRecord(context.Background(), shapeSchemaID, uuid.New(), map[string]any{"note": []any{"n"}})
	requireShapeRefusal(t, err, noArrayRefusal("note", 2))
}

// Every subschema is read once per answer, so a document whose $defs branch
// into each other many levels deep is answered promptly.
func TestDeclaredShapeBoundsBranchingDocuments(t *testing.T) {
	defs := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		next := fmt.Sprintf(`{"$ref": "#/$defs/d%d"}`, i+1)
		defs = append(defs, fmt.Sprintf(`"d%d": {"anyOf": [%s, %s], "allOf": [%s]}`, i, next, next, next))
	}
	defs = append(defs, `"d40": {"type": "array", "items": {"type": "string"}}`)
	registry := newShapeRegistry(true)
	registry.document = `{"type": "object", "$defs": {` + strings.Join(defs, ", ") + `}, "properties": {"phones": {"$ref": "#/$defs/d0"}}}`
	done := make(chan Declaration, 1)
	go func() { done <- SchemaShapesOf(registry, shapeSchemaID).Declared("phones") }()
	select {
	case phones := <-done:
		assert.Equal(t, ShapeArray, phones.At(0, 0))
	case <-time.After(10 * time.Second):
		t.Fatal("the lookup did not end")
	}
}
