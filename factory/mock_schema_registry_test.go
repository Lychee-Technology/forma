package factory

// The mock SchemaRegistry the factory tests wire in, kept out of factory_test.go
// so that file stays about the factory rather than about its fixtures.

import (
	"fmt"

	"github.com/lychee-technology/forma"
)

type mockSchemaRegistry struct {
	nameToID map[string]int16
	idToName map[int16]string
	schemas  map[string]forma.SchemaAttributeCache

	// schemaBody overrides the JSON Schema document served for every schema.
	// Empty means mockSchemaBody.
	schemaBody string
	// schemaDocMissing makes GetSchemaByName/GetSchemaByID report the schema
	// document as absent while ListSchemas still reports the name. This is not a
	// contrived state: the shipped fileSchemaRegistry behaves exactly this way for
	// a schema_registry row that has <name>_attributes.json on disk but no
	// <name>.json (schemameta/file_registry.go loadSchemaArtifacts).
	schemaDocMissing bool

	// getSchemaByNameCalls counts document reads. Two independent readers of the
	// same registry can be handed two different documents by an implementation
	// that serves them from a database, so how many times the registry is read
	// for the same purpose is a property worth pinning (#318 review).
	getSchemaByNameCalls int
}

// mockSchemaBody is the JSON Schema document the mock registry serves. It has to
// be a parseable, resolvable schema because schemavalidate.New fails closed on
// any schema it cannot resolve (#314), and the factory builds the validator over
// every registered schema.
const mockSchemaBody = `{"type":"object","properties":{"id":{"type":"string"}}}`

// body returns the schema document this mock serves.
func (m *mockSchemaRegistry) body() string {
	if m.schemaBody == "" {
		return mockSchemaBody
	}
	return m.schemaBody
}

func newMockSchemaRegistry() *mockSchemaRegistry {
	return &mockSchemaRegistry{
		nameToID: map[string]int16{"test": 1},
		idToName: map[int16]string{1: "test"},
		schemas: map[string]forma.SchemaAttributeCache{
			"test": {
				"id": forma.AttributeMetadata{
					AttributeName: "id",
					AttributeID:   1,
					ValueType:     forma.ValueTypeText,
				},
			},
		},
	}
}

func (m *mockSchemaRegistry) GetSchemaAttributeCacheByName(name string) (int16, forma.SchemaAttributeCache, error) {
	id, ok := m.nameToID[name]
	if !ok {
		return 0, nil, fmt.Errorf("schema not found: %s", name)
	}
	cache := m.schemas[name]
	return id, cache, nil
}

func (m *mockSchemaRegistry) GetSchemaAttributeCacheByID(id int16) (string, forma.SchemaAttributeCache, error) {
	name, ok := m.idToName[id]
	if !ok {
		return "", nil, fmt.Errorf("schema not found for ID: %d", id)
	}
	cache := m.schemas[name]
	return name, cache, nil
}

func (m *mockSchemaRegistry) GetSchemaByName(name string) (int16, forma.JSONSchema, error) {
	m.getSchemaByNameCalls++
	id, ok := m.nameToID[name]
	if !ok {
		return 0, forma.JSONSchema{}, fmt.Errorf("schema not found: %s", name)
	}
	if m.schemaDocMissing {
		return 0, forma.JSONSchema{}, fmt.Errorf("schema data not found: %s: %w", name, forma.ErrNotFound)
	}
	return id, forma.JSONSchema{ID: id, Name: name, Schema: m.body()}, nil
}

func (m *mockSchemaRegistry) GetSchemaByID(id int16) (string, forma.JSONSchema, error) {
	name, ok := m.idToName[id]
	if !ok {
		return "", forma.JSONSchema{}, fmt.Errorf("schema not found for ID: %d", id)
	}
	if m.schemaDocMissing {
		return "", forma.JSONSchema{}, fmt.Errorf("schema data not found for ID %d: %w", id, forma.ErrNotFound)
	}
	return name, forma.JSONSchema{ID: id, Name: name, Schema: m.body()}, nil
}

func (m *mockSchemaRegistry) ListSchemas() []string {
	schemas := make([]string, 0, len(m.nameToID))
	for name := range m.nameToID {
		schemas = append(schemas, name)
	}
	return schemas
}
