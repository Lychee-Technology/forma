package internal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// requiredRelationRootDir writes a two-schema directory whose child lists its
// x-relation property in root-level "required".
func requiredRelationRootDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	parent := `{
	  "type": "object",
	  "properties": {
	    "id": {"type": "string"},
	    "contact": {"type": "object", "properties": {"name": {"type": "string"}}}
	  }
	}`
	child := `{
	  "type": "object",
	  "required": ["id", "parentId", "contactSnapshot"],
	  "properties": {
	    "id": {"type": "string"},
	    "parentId": {"type": "string"},
	    "contactSnapshot": {
	      "$ref": "parent.json#/properties/contact",
	      "x-relation": {"key_property": "parentId"}
	    }
	  }
	}`

	require.NoError(t, os.WriteFile(filepath.Join(dir, "parent.json"), []byte(parent), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "child.json"), []byte(child), 0o600))
	return dir
}

// TestLoadRelationIndexRejectsRequiredRelationRoot is the #318 guard.
//
// A relation root is removed from every payload before the validator runs, so a
// schema that also demands it produces a missing-required rejection on every
// create and update, and the caller cannot fix it by sending the field. The
// failure has to happen at startup, where an operator sees it, rather than as a
// 4xx storm in production.
func TestLoadRelationIndexRejectsRequiredRelationRoot(t *testing.T) {
	_, err := LoadRelationIndex(requiredRelationRootDir(t))

	require.Error(t, err)
	require.ErrorContains(t, err, "child")
	require.ErrorContains(t, err, "contactSnapshot")
	require.ErrorContains(t, err, "required")
}

// TestValidateRelationSchemasAcceptsShippedSchemas is the other half: the guard
// must not reject anything that ships today. visit.json is the only schema in
// the repository carrying x-relation, and contactSnapshot is not required.
func TestValidateRelationSchemasAcceptsShippedSchemas(t *testing.T) {
	require.NoError(t, ValidateRelationSchemas(shippedSchemaDir))
	require.NoError(t, ValidateRelationSchemas("../cmd/sample/schemas"))
	require.NoError(t, ValidateRelationSchemas(""), "an unconfigured schema directory is not an error")
}
