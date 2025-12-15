package internal

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubSchemaRegistry struct {
	schemaID   int16
	schemaName string
	cache      forma.SchemaAttributeCache
}

func newStubSchemaRegistry() forma.SchemaRegistry {
	cache := forma.SchemaAttributeCache{
		"name":               {AttributeID: 1, ValueType: forma.ValueTypeText},
		"age":                {AttributeID: 2, ValueType: forma.ValueTypeNumeric},
		"person.name":        {AttributeID: 3, ValueType: forma.ValueTypeText},
		"person.age":         {AttributeID: 4, ValueType: forma.ValueTypeNumeric},
		"items":              {AttributeID: 5, ValueType: forma.ValueTypeText},
		"metadata.createdAt": {AttributeID: 6, ValueType: forma.ValueTypeDate},
		"metadata.active":    {AttributeID: 7, ValueType: forma.ValueTypeBool},
	}
	return &stubSchemaRegistry{
		schemaID:   100,
		schemaName: "test",
		cache:      cache,
	}
}

func copyAttributeCache(src forma.SchemaAttributeCache) forma.SchemaAttributeCache {
	dst := make(forma.SchemaAttributeCache, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func (s *stubSchemaRegistry) GetSchemaAttributeCacheByName(name string) (int16, forma.SchemaAttributeCache, error) {
	if name != s.schemaName {
		return 0, nil, fmt.Errorf("schema %s not found", name)
	}
	return s.schemaID, copyAttributeCache(s.cache), nil
}

func (s *stubSchemaRegistry) GetSchemaAttributeCacheByID(id int16) (string, forma.SchemaAttributeCache, error) {
	if id != s.schemaID {
		return "", nil, fmt.Errorf("schema id %d not found", id)
	}
	return s.schemaName, copyAttributeCache(s.cache), nil
}

func (s *stubSchemaRegistry) ListSchemas() []string {
	return []string{s.schemaName}
}

func (s *stubSchemaRegistry) GetSchemaByName(name string) (int16, forma.JSONSchema, error) {
	if name != s.schemaName {
		return 0, forma.JSONSchema{}, fmt.Errorf("schema %s not found", name)
	}
	return s.schemaID, forma.JSONSchema{ID: s.schemaID, Name: s.schemaName}, nil
}

func (s *stubSchemaRegistry) GetSchemaByID(id int16) (string, forma.JSONSchema, error) {
	if id != s.schemaID {
		return "", forma.JSONSchema{}, fmt.Errorf("schema id %d not found", id)
	}
	return s.schemaName, forma.JSONSchema{ID: s.schemaID, Name: s.schemaName}, nil
}

func TestTransformer_ToAttributes(t *testing.T) {
	ctx := context.Background()
	registry := newStubSchemaRegistry()
	transformer := NewTransformer(registry)

	schemaID, _, err := registry.GetSchemaAttributeCacheByName("test")
	require.NoError(t, err)

	rowID := uuid.Must(uuid.NewV7())
	createdAt := "2024-01-02T15:04:05Z"

	data := map[string]any{
		"name": "John Doe",
		"age":  30,
		"person": map[string]any{
			"name": "Alice",
			"age":  25,
		},
		"items": []any{"alpha", "beta"},
		"metadata": map[string]any{
			"createdAt": createdAt,
			"active":    true,
		},
	}

	attrs, err := transformer.ToAttributes(ctx, schemaID, rowID, data)
	require.NoError(t, err)
	require.Equal(t, 8, len(attrs))

	// Convert EntityAttributes to EAVRecords for lookup
	converter := NewAttributeConverter(registry)
	eavRecords, err := converter.ToEAVRecords(attrs, rowID)
	require.NoError(t, err)

	attrMap := buildAttributeLookup(t, registry, eavRecords)

	nameAttr := attrMap["name|"]
	require.NotNil(t, nameAttr)
	require.NotNil(t, nameAttr.ValueText)
	assert.Equal(t, "John Doe", *nameAttr.ValueText)

	ageAttr := attrMap["age|"]
	require.NotNil(t, ageAttr)
	require.NotNil(t, ageAttr.ValueNumeric)
	assert.Equal(t, float64(30), *ageAttr.ValueNumeric)

	personNameAttr := attrMap["person.name|"]
	require.NotNil(t, personNameAttr)
	assert.Equal(t, "Alice", *personNameAttr.ValueText)

	personAgeAttr := attrMap["person.age|"]
	require.NotNil(t, personAgeAttr)
	assert.Equal(t, float64(25), *personAgeAttr.ValueNumeric)

	item0Attr := attrMap["items|0"]
	require.NotNil(t, item0Attr)
	assert.Equal(t, "alpha", *item0Attr.ValueText)

	item1Attr := attrMap["items|1"]
	require.NotNil(t, item1Attr)
	assert.Equal(t, "beta", *item1Attr.ValueText)

	createdAtAttr := attrMap["metadata.createdAt|"]
	require.NotNil(t, createdAtAttr)
	require.NotNil(t, createdAtAttr.ValueNumeric)
	assert.Equal(t, createdAt, time.UnixMilli(int64(*createdAtAttr.ValueNumeric)).UTC().Format(time.RFC3339))

	activeAttr := attrMap["metadata.active|"]
	require.NotNil(t, activeAttr)
	require.NotNil(t, activeAttr.ValueNumeric)
	assert.True(t, *activeAttr.ValueNumeric > 0.5)
}

func TestTransformer_FromAttributes(t *testing.T) {
	ctx := context.Background()
	registry := newStubSchemaRegistry()
	transformer := NewTransformer(registry)

	schemaID, _, err := registry.GetSchemaAttributeCacheByName("test")
	require.NoError(t, err)

	rowID := uuid.Must(uuid.NewV7())
	createdAt := time.Date(2024, 3, 14, 9, 26, 0, 0, time.UTC)

	eavRecords := []EAVRecord{
		newTestAttribute(t, registry, schemaID, rowID, "name", "", "Jane Doe"),
		newTestAttribute(t, registry, schemaID, rowID, "age", "", 42),
		newTestAttribute(t, registry, schemaID, rowID, "person.name", "", "Bob"),
		newTestAttribute(t, registry, schemaID, rowID, "person.age", "", 28),
		newTestAttribute(t, registry, schemaID, rowID, "items", "0", "first"),
		newTestAttribute(t, registry, schemaID, rowID, "items", "1", "second"),
		newTestAttribute(t, registry, schemaID, rowID, "metadata.createdAt", "", createdAt),
		newTestAttribute(t, registry, schemaID, rowID, "metadata.active", "", false),
	}

	// Convert EAVRecords to EntityAttributes
	converter := NewAttributeConverter(registry)
	attrs, err := converter.FromEAVRecords(eavRecords)
	require.NoError(t, err)

	result, err := transformer.FromAttributes(ctx, attrs)
	require.NoError(t, err)

	assert.Equal(t, "Jane Doe", result["name"])
	assert.Equal(t, float64(42), result["age"])

	person, ok := result["person"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Bob", person["name"])
	assert.Equal(t, float64(28), person["age"])

	items, ok := result["items"].([]any)
	require.True(t, ok)
	require.Len(t, items, 2)
	assert.Equal(t, "first", items[0])
	assert.Equal(t, "second", items[1])

	metadata, ok := result["metadata"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, createdAt, metadata["createdAt"])
	assert.Equal(t, false, metadata["active"])
}

func TestTransformer_BatchRoundTrip(t *testing.T) {
	ctx := context.Background()
	registry := newStubSchemaRegistry()
	transformer := NewTransformer(registry)

	schemaID, _, err := registry.GetSchemaAttributeCacheByName("test")
	require.NoError(t, err)

	records := []map[string]any{
		{
			"name": "Alice",
			"age":  31,
			"items": []any{
				"one",
				"two",
			},
			"metadata": map[string]any{
				"createdAt": "2024-05-01T12:00:00Z",
				"active":    true,
			},
		},
		{
			"name": "Bob",
			"age":  29,
			"items": []any{
				"first",
			},
		},
	}

	jsonObjects := make([]any, len(records))
	for i := range records {
		jsonObjects[i] = records[i]
	}

	attrs, err := transformer.BatchToAttributes(ctx, schemaID, jsonObjects)
	require.NoError(t, err)
	require.NotEmpty(t, attrs)

	back, err := transformer.BatchFromAttributes(ctx, attrs)
	require.NoError(t, err)
	require.Len(t, back, 2)
}

func TestTransformer_ValidateAgainstSchema(t *testing.T) {
	transformer := NewTransformer(newStubSchemaRegistry())
	ctx := context.Background()

	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name": map[string]any{"type": "string"},
			"age":  map[string]any{"type": "integer"},
		},
		"required": []any{"name"},
	}

	validData := map[string]any{"name": "Alice", "age": 30}
	require.NoError(t, transformer.ValidateAgainstSchema(ctx, schema, validData))

	invalidData := map[string]any{"age": 30}
	err := transformer.ValidateAgainstSchema(ctx, schema, invalidData)
	require.Error(t, err)
}

func TestTransformer_ArrayInsideObjectDoesNotClobberObject(t *testing.T) {
	ctx := context.Background()
	registry := &stubSchemaRegistry{
		schemaID:   300,
		schemaName: "contact_schema",
		cache: forma.SchemaAttributeCache{
			"contact.name":   {AttributeID: 1, ValueType: forma.ValueTypeText},
			"contact.phones": {AttributeID: 2, ValueType: forma.ValueTypeText},
		},
	}

	transformer := NewTransformer(registry)
	schemaID, _, err := registry.GetSchemaAttributeCacheByName("contact_schema")
	require.NoError(t, err)

	rowID := uuid.Must(uuid.NewV7())
	data := map[string]any{
		"contact": map[string]any{
			"name":   "Alice",
			"phones": []any{"111", "222"},
		},
	}

	attrs, err := transformer.ToAttributes(ctx, schemaID, rowID, data)
	require.NoError(t, err)

	back, err := transformer.FromAttributes(ctx, attrs)
	require.NoError(t, err)

	contact, ok := back["contact"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Alice", contact["name"])
	assert.Equal(t, []any{"111", "222"}, contact["phones"])
}

func TestTransformer_ArrayOfObjectsStillMergesProperties(t *testing.T) {
	ctx := context.Background()
	registry := &stubSchemaRegistry{
		schemaID:   301,
		schemaName: "items_schema",
		cache: forma.SchemaAttributeCache{
			"items.title":  {AttributeID: 1, ValueType: forma.ValueTypeText},
			"items.active": {AttributeID: 2, ValueType: forma.ValueTypeBool},
		},
	}

	transformer := NewTransformer(registry)
	schemaID, _, err := registry.GetSchemaAttributeCacheByName("items_schema")
	require.NoError(t, err)

	rowID := uuid.Must(uuid.NewV7())
	data := map[string]any{
		"items": []any{
			map[string]any{"title": "First", "active": true},
			map[string]any{"title": "Second", "active": false},
		},
	}

	attrs, err := transformer.ToAttributes(ctx, schemaID, rowID, data)
	require.NoError(t, err)

	back, err := transformer.FromAttributes(ctx, attrs)
	require.NoError(t, err)

	items, ok := back["items"].([]any)
	require.True(t, ok)
	require.Len(t, items, 2)

	first, ok := items[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "First", first["title"])
	assert.Equal(t, true, first["active"])

	second, ok := items[1].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Second", second["title"])
	assert.Equal(t, false, second["active"])
}

func buildAttributeLookup(t *testing.T, registry forma.SchemaRegistry, attrs []EAVRecord) map[string]*EAVRecord {
	result := make(map[string]*EAVRecord)
	cacheBySchema := make(map[int16]forma.SchemaAttributeCache)

	for i := range attrs {
		attr := attrs[i]
		cache, ok := cacheBySchema[attr.SchemaID]
		if !ok {
			_, schemaCache, err := registry.GetSchemaAttributeCacheByID(attr.SchemaID)
			require.NoError(t, err)
			cache = schemaCache
			cacheBySchema[attr.SchemaID] = cache
		}

		name := ""
		for attrName, meta := range cache {
			if meta.AttributeID == attr.AttrID {
				name = attrName
				break
			}
		}
		require.NotEmpty(t, name, "attribute id %d not found", attr.AttrID)

		key := name + "|" + attr.ArrayIndices
		result[key] = &attr
	}

	return result
}

func newTestAttribute(t *testing.T, registry forma.SchemaRegistry, schemaID int16, rowID uuid.UUID, name string, indices string, value any) EAVRecord {
	_, cache, err := registry.GetSchemaAttributeCacheByID(schemaID)
	require.NoError(t, err)

	meta, ok := cache[name]
	require.True(t, ok, "attribute %s not found", name)

	attr := EAVRecord{
		SchemaID:     schemaID,
		RowID:        rowID,
		AttrID:       meta.AttributeID,
		ArrayIndices: indices,
	}

	require.NoError(t, populateTypedValue(&attr, value, meta.ValueType))
	return attr
}
