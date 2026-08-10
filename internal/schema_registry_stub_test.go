package internal

import (
	"fmt"
	"maps"

	"github.com/lychee-technology/forma"
)

type stubSchemaRegistry struct {
	schemaID   int16
	schemaName string
	cache      forma.SchemaAttributeCache
}

// stubSchemaDocument is the schema document this registry serves.
//
// It has to be a real document rather than the empty string it used to be. An
// empty string is not valid JSON, and no shipped registry produces that shape —
// the file registry either serves the document text or answers ErrNotFound — so
// serving "" only made NewEntityManager's relation-index load fail and log a
// warning in every test that used this stub. It declares no properties, so it
// carries no relation roots and the index stays empty, which is what these tests
// already got back when the index was gated on a schema directory they never
// set.
const stubSchemaDocument = `{"type":"object"}`

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
	return &stubSchemaRegistry{schemaID: 100, schemaName: "test", cache: cache}
}

func copyAttributeCache(src forma.SchemaAttributeCache) forma.SchemaAttributeCache {
	dst := make(forma.SchemaAttributeCache, len(src))
	maps.Copy(dst, src)
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
	return s.schemaID, forma.JSONSchema{ID: s.schemaID, Name: s.schemaName, Schema: stubSchemaDocument}, nil
}

func (s *stubSchemaRegistry) GetSchemaByID(id int16) (string, forma.JSONSchema, error) {
	if id != s.schemaID {
		return "", forma.JSONSchema{}, fmt.Errorf("schema id %d not found", id)
	}
	return s.schemaName, forma.JSONSchema{ID: s.schemaID, Name: s.schemaName, Schema: stubSchemaDocument}, nil
}
