package internal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/stretchr/testify/require"
)

// serveInitDBSchemaSet builds a registry over the name set init-db registers
// from dir, each name serving its <name>.json bytes. That is what the DB-backed
// registry cmd/server and cmd/lambda build hands the relation guard, minus the
// database: the names come from the schema_registry table init-db seeded, and
// each document is read from SCHEMA_DIR by name.
func serveInitDBSchemaSet(t *testing.T, dir string) *docSchemaRegistry {
	t.Helper()
	names, err := schemameta.ListInitDBSchemaNames(dir)
	require.NoError(t, err)

	docs := make(map[string]string, len(names))
	for _, name := range names {
		body, err := os.ReadFile(filepath.Join(dir, name+".json"))
		require.NoError(t, err)
		docs[name] = string(body)
	}
	return newDocSchemaRegistry(docs)
}

// TestLoadRelationIndexAcceptsShippedInitDBSchemaSet drives the relation guard
// over the schemas production registers (#394). The shipped-schema tests load
// through NewFileSchemaRegistryFromDirectory, which leaves out *_full.json, so
// a *_full schema gaining an x-relation property that its "required" also names
// would pass them and fail at server startup. This one would not.
func TestLoadRelationIndexAcceptsShippedInitDBSchemaSet(t *testing.T) {
	registry := serveInitDBSchemaSet(t, shippedSchemaDir)

	dirRegistry, err := schemameta.NewFileSchemaRegistryFromDirectory(shippedSchemaDir)
	require.NoError(t, err)
	require.Subset(t, registry.ListSchemas(), dirRegistry.ListSchemas())
	require.Greater(t, len(registry.ListSchemas()), len(dirRegistry.ListSchemas()),
		"the init-db set must be wider than the directory registry's, or this test adds nothing to the shipped-schema tests")

	idx, err := LoadRelationIndex(registry)
	require.NoError(t, err)
	require.NotEmpty(t, idx.Relations("visit"), "visit must carry a relation for this test to mean anything")
}
