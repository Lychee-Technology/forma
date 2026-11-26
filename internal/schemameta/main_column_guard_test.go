package schemameta

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

// stubRegistry is a caller's own forma.SchemaRegistry: one schema, lead (id
// 7), whose attribute cache the test can swap to model a reload.
type stubRegistry struct {
	cache forma.SchemaAttributeCache
}

func (s *stubRegistry) GetSchemaAttributeCacheByName(name string) (int16, forma.SchemaAttributeCache, error) {
	if name != "lead" {
		return 0, nil, forma.NotFoundf("schema not found: %s", name)
	}
	return 7, s.cache, nil
}

func (s *stubRegistry) GetSchemaAttributeCacheByID(id int16) (string, forma.SchemaAttributeCache, error) {
	if id != 7 {
		return "", nil, forma.NotFoundf("schema not found")
	}
	return "lead", s.cache, nil
}

func (s *stubRegistry) GetSchemaByName(name string) (int16, forma.JSONSchema, error) {
	return 7, forma.JSONSchema{ID: 7, Name: name}, nil
}

func (s *stubRegistry) GetSchemaByID(id int16) (string, forma.JSONSchema, error) {
	return "lead", forma.JSONSchema{ID: id, Name: "lead"}, nil
}

func (s *stubRegistry) ListSchemas() []string { return []string{"lead"} }

func leadCache(name string) forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{name: {AttributeName: name, AttributeID: 1, ValueType: forma.ValueTypeText}}
}

// The registries this package builds were admitted when they loaded and never
// reload, so they are answered as they are: the very value, which keeps the
// schemaMetadataProvider fast path.
func TestGuardEntityMainColumnNames_AnswersAdmittedRegistryUnchanged(t *testing.T) {
	dir := t.TempDir()
	writeMainColumnNameSchema(t, dir, map[string]any{
		"label": map[string]any{"attributeID": float64(1), "valueType": "text"},
	})
	registry, err := NewFileSchemaRegistryFromDirectory(dir)
	require.NoError(t, err)

	guarded, err := GuardEntityMainColumnNames(registry)
	require.NoError(t, err)
	assert.Same(t, registry, guarded)

	tracked := &trackedRegistry{fileSchemaRegistry: registry.(*fileSchemaRegistry)}
	guarded, err = GuardEntityMainColumnNames(tracked)
	require.NoError(t, err)
	assert.Same(t, tracked, guarded)
}

// A caller's registry is checked over every schema it lists, so a colliding
// one fails startup, folded spellings included.
func TestGuardEntityMainColumnNames_RefusesCustomRegistryAtStartup(t *testing.T) {
	for name, col := range map[string]string{"text_01": "text_01", "Integer.02": "integer_02"} {
		_, err := GuardEntityMainColumnNames(&stubRegistry{cache: leadCache(name)})
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "failed to check the attributes of schema lead")
		assert.Contains(t, err.Error(), `attribute "`+name+`"`)
		assert.Contains(t, err.Error(), `entity_main column "`+col+`"`)
	}
}

// A cache the registry serves after startup is refused on both accessors, so
// a registry that reloads cannot slip a colliding name past the startup check.
func TestGuardEntityMainColumnNames_RefusesCacheServedAfterStartup(t *testing.T) {
	stub := &stubRegistry{cache: leadCache("label")}
	guarded, err := GuardEntityMainColumnNames(stub)
	require.NoError(t, err)

	id, cache, err := guarded.GetSchemaAttributeCacheByName("lead")
	require.NoError(t, err)
	assert.Equal(t, int16(7), id)
	assert.Contains(t, cache, "label")

	stub.cache = leadCache("text_02")
	_, _, err = guarded.GetSchemaAttributeCacheByName("lead")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema lead served by the configured SchemaRegistry")
	assert.Contains(t, err.Error(), `entity_main column "text_02"`)
	assert.NotErrorIs(t, err, forma.ErrInvalidInput)

	_, _, err = guarded.GetSchemaAttributeCacheByID(7)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema lead (id 7) served by the configured SchemaRegistry")
	assert.Contains(t, err.Error(), `entity_main column "text_02"`)
}

// The registry's own errors pass through as they were given, so a missing
// schema keeps its not-found status; retired entries and near-miss names are
// admitted, as at admission.
func TestGuardEntityMainColumnNames_KeepsRegistryErrorsAndAdmitsNonColliding(t *testing.T) {
	stub := &stubRegistry{cache: forma.SchemaAttributeCache{
		"text_11":       {AttributeID: 1, ValueType: forma.ValueTypeText},
		"text_01_label": {AttributeID: 2, ValueType: forma.ValueTypeText},
		"text_02":       {AttributeID: 3, ValueType: forma.ValueTypeText, Retired: true},
	}}
	guarded, err := GuardEntityMainColumnNames(stub)
	require.NoError(t, err)

	_, _, err = guarded.GetSchemaAttributeCacheByName("missing")
	assert.ErrorIs(t, err, forma.ErrNotFound)
	_, _, err = guarded.GetSchemaAttributeCacheByID(8)
	assert.ErrorIs(t, err, forma.ErrNotFound)

	_, err = GuardEntityMainColumnNames(nil)
	require.Error(t, err)
}
