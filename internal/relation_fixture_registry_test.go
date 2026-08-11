package internal

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// docSchemaRegistry is a forma.SchemaRegistry that serves a fixed set of schema
// documents. It exists because the relation fixtures under testdata are schema
// documents only — they carry no <name>_attributes.json — so
// schemameta.NewFileSchemaRegistryFromDirectory cannot load them, while the
// relation guard needs nothing but ListSchemas and GetSchemaByName.
//
// It is also the only way to write a registry whose documents differ from the
// files in SCHEMA_DIR, which is what the guard's byte source has to be pinned
// against.
type docSchemaRegistry struct {
	nameToID map[string]int16
	idToName map[int16]string
	docs     map[string]string
	// docErr, keyed by schema name, overrides GetSchemaByName for that name. A
	// registry that lists a name it cannot then serve is the failure this models.
	docErr map[string]error
}

// newDocSchemaRegistry assigns ids in sorted name order, so a fixture's ids are
// stable across runs.
func newDocSchemaRegistry(docs map[string]string) *docSchemaRegistry {
	names := make([]string, 0, len(docs))
	for name := range docs {
		names = append(names, name)
	}
	slices.Sort(names)

	r := &docSchemaRegistry{
		nameToID: make(map[string]int16, len(docs)),
		idToName: make(map[int16]string, len(docs)),
		docs:     docs,
		docErr:   make(map[string]error),
	}
	for i, name := range names {
		id := int16(100 + i)
		r.nameToID[name] = id
		r.idToName[id] = name
	}
	return r
}

func (r *docSchemaRegistry) GetSchemaAttributeCacheByName(name string) (int16, forma.SchemaAttributeCache, error) {
	id, ok := r.nameToID[name]
	if !ok {
		return 0, nil, fmt.Errorf("schema %s not found", name)
	}
	return id, forma.SchemaAttributeCache{}, nil
}

func (r *docSchemaRegistry) GetSchemaAttributeCacheByID(id int16) (string, forma.SchemaAttributeCache, error) {
	name, ok := r.idToName[id]
	if !ok {
		return "", nil, fmt.Errorf("schema id %d not found", id)
	}
	return name, forma.SchemaAttributeCache{}, nil
}

func (r *docSchemaRegistry) GetSchemaByName(name string) (int16, forma.JSONSchema, error) {
	if err, ok := r.docErr[name]; ok {
		return 0, forma.JSONSchema{}, fmt.Errorf("serve the registered document for schema %s: %w", name, err)
	}
	id, ok := r.nameToID[name]
	if !ok {
		return 0, forma.JSONSchema{}, fmt.Errorf("schema %s not found", name)
	}
	return id, forma.JSONSchema{ID: id, Name: name, Schema: r.docs[name]}, nil
}

func (r *docSchemaRegistry) GetSchemaByID(id int16) (string, forma.JSONSchema, error) {
	name, ok := r.idToName[id]
	if !ok {
		return "", forma.JSONSchema{}, fmt.Errorf("schema id %d not found", id)
	}
	return name, forma.JSONSchema{ID: id, Name: name, Schema: r.docs[name]}, nil
}

func (r *docSchemaRegistry) ListSchemas() []string {
	names := make([]string, 0, len(r.nameToID))
	for name := range r.nameToID {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// readSchemaDocs reads every .json file in dir as a schema document, keyed by
// file name without the extension.
func readSchemaDocs(t *testing.T, dir string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	docs := make(map[string]string)
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".json") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		docs[strings.TrimSuffix(name, ".json")] = string(body)
	}
	require.NotEmpty(t, docs, "fixture directory %s holds no schema documents", dir)
	return docs
}

// serveRelationFixture builds a registry over the documents of one committed
// fixture directory, so a test drives the guard over the same bytes the fixture
// holds.
func serveRelationFixture(t *testing.T, fixture string) *docSchemaRegistry {
	t.Helper()
	return newDocSchemaRegistry(readSchemaDocs(t, resolveRelationFixtureDir(fixture)))
}
