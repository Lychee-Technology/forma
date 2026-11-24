package transform

import (
	"context"
	"testing"

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
		indices, exact := shapes.Declared(tc.name).Indices()
		assert.Equal(t, tc.indices, indices, tc.name)
		assert.Equal(t, tc.exact, exact, tc.name)
	}
	phones := shapes.Declared("phones")
	assert.Equal(t, ShapeArray, phones.At(0))
	assert.Equal(t, ShapeScalar, phones.At(1))
	assert.Equal(t, ShapeScalar, phones.At(2))
	name := shapes.Declared("items.name")
	assert.Equal(t, ShapeScalar, name.At(0), "an object sent where the document declares an array of objects")
	assert.Equal(t, ShapeScalar, name.At(1))
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
	assert.Equal(t, ShapeScalar, shapes.Declared("note").At(0))
	assert.Equal(t, ShapeArray, shapes.Declared("phones").At(0))
	assert.Equal(t, ShapeUnresolved, shapes.Declared("opaque").At(0))

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
		assert.Equal(t, ShapeUnresolved, shapes.Declared(name).At(0), name)
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
			assert.NotEqual(t, ShapeUnresolved, shapes.Declared(name).At(0), "%s.%s", schemaName, name)
		}
	}

	visitID, visit, err := registry.GetSchemaAttributeCacheByName("visit")
	require.NoError(t, err)
	require.Nil(t, visit["leadId"].ArrayAncestors, "the fixture must exercise a schema without derived ancestry")
	shapes := SchemaShapesOf(registry, visitID)
	assert.Equal(t, ShapeScalar, shapes.Declared("leadId").At(0))
	assert.Equal(t, ShapeScalar, shapes.Declared("contactSnapshot.email").At(0))

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
