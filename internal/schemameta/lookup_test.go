package schemameta

import (
	"fmt"
	"maps"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubSchemaRegistry struct {
	schemaID   int16
	schemaName string
	cache      forma.SchemaAttributeCache
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

func copyAttributeCache(src forma.SchemaAttributeCache) forma.SchemaAttributeCache {
	dst := make(forma.SchemaAttributeCache, len(src))
	maps.Copy(dst, src)
	return dst
}

func TestGetSchemaMetadataUsesFileRegistryFastPath(t *testing.T) {
	registry := &fileSchemaRegistry{
		idToName: map[int16]string{501: "fast"},
		schemaAttributeCaches: map[int16]forma.SchemaAttributeCache{
			501: {
				"first":  {AttributeID: 7, ValueType: forma.ValueTypeText},
				"second": {AttributeID: 8, ValueType: forma.ValueTypeText},
			},
		},
		attrIDToName: map[int16]map[int16]string{
			501: {
				7: "first",
				8: "second",
			},
		},
	}

	cache, idToName, err := GetSchemaMetadata(registry, 501)
	require.NoError(t, err)
	assert.Equal(t, forma.ValueTypeText, cache["first"].ValueType)
	assert.Equal(t, "first", idToName[7])

	allocs := testing.AllocsPerRun(1000, func() {
		_, _, _ = GetSchemaMetadata(registry, 501)
	})
	assert.Zero(t, allocs)
}

func TestGetSchemaMetadataFallsBackForGenericRegistry(t *testing.T) {
	registry := &stubSchemaRegistry{
		schemaID:   501,
		schemaName: "fallback",
		cache: forma.SchemaAttributeCache{
			"first":  {AttributeID: 7, ValueType: forma.ValueTypeText},
			"second": {AttributeID: 8, ValueType: forma.ValueTypeText},
		},
	}

	cache, idToName, err := GetSchemaMetadata(registry, 501)
	require.NoError(t, err)
	assert.Equal(t, forma.ValueTypeText, cache["first"].ValueType)
	assert.Equal(t, "second", idToName[8])
}

func TestGetSchemaMetadataRejectsNilRegistry(t *testing.T) {
	_, _, err := GetSchemaMetadata(nil, 501)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema registry is not configured")
}

func TestGetSchemaMetadataWrapsRegistryErrors(t *testing.T) {
	registry := &stubSchemaRegistry{schemaID: 501, schemaName: "fallback"}

	_, _, err := GetSchemaMetadata(registry, 999)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load schema metadata for id 999")
}

func TestGetSchemaMetadataFallbackRejectsDuplicateAttributeIDs(t *testing.T) {
	registry := &stubSchemaRegistry{
		schemaID:   501,
		schemaName: "dup_attr_ids",
		cache: forma.SchemaAttributeCache{
			"first":  {AttributeID: 7, ValueType: forma.ValueTypeText},
			"second": {AttributeID: 7, ValueType: forma.ValueTypeText},
		},
	}

	_, _, err := GetSchemaMetadata(registry, 501)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate attribute id")
}

func TestFileSchemaRegistryRejectsDuplicateAttributeIDsAtLoad(t *testing.T) {
	dir := t.TempDir()
	writeJSONFile(t, fmt.Sprintf("%s/dup.json", dir), map[string]any{
		"type": "object",
	})
	writeJSONFile(t, fmt.Sprintf("%s/dup_attributes.json", dir), map[string]any{
		"first":  map[string]any{"attributeID": float64(7), "valueType": "text"},
		"second": map[string]any{"attributeID": float64(7), "valueType": "text"},
	})

	_, err := NewFileSchemaRegistryFromDirectory(dir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate attribute id")
}
