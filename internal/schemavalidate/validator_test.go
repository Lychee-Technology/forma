package schemavalidate

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// shippedSchemaDir is the real server schema directory, as an absolute path.
// Tests that specifically exercise relative-path handling must not use this.
func shippedSchemaDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs("../../cmd/server/schemas")
	require.NoError(t, err)
	return dir
}

// shippedSchemaNames enumerates the schema directory the same way
// cmd/tools/init_db.go registers entities: every *.json that is not an
// *_attributes.json sidecar. Enumerating rather than hardcoding keeps the
// fail-closed contract in step with the directory as schemas are added.
func shippedSchemaNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	var names []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".json") || strings.HasSuffix(name, "_attributes.json") {
			continue
		}
		names = append(names, name)
	}
	// Guard against a globbing mistake silently emptying the table and making
	// every assertion below vacuous.
	require.NotEmpty(t, names, "no shipped schemas discovered in %s", dir)
	return names
}

// TestResolveShippedSchemas is the fail-closed contract: every schema the
// server ships must resolve, including the cross-file $refs in visit.json and
// log.json.
func TestResolveShippedSchemas(t *testing.T) {
	dir := shippedSchemaDir(t)
	for _, fileName := range shippedSchemaNames(t, dir) {
		t.Run(strings.TrimSuffix(fileName, ".json"), func(t *testing.T) {
			_, err := resolveSchemaFile(dir, fileName)
			require.NoError(t, err, "shipped schema %s must resolve", fileName)
		})
	}
}

// TestResolveThroughRelativeDir pins that a relative schema directory resolves.
// SCHEMA_DIR is operator-supplied, so a relative value with ".." must work:
// previously the ".." was parsed as the URI *host* and the loader read a path
// that appears in no schema. An absolute test directory cannot see this.
func TestResolveThroughRelativeDir(t *testing.T) {
	// Relative to this package directory, this is the shipped schema directory.
	const relDir = "../../cmd/server/schemas"
	for _, fileName := range shippedSchemaNames(t, relDir) {
		t.Run(strings.TrimSuffix(fileName, ".json"), func(t *testing.T) {
			_, err := resolveSchemaFile(relDir, fileName)
			require.NoError(t, err, "schema %s must resolve through a relative dir", fileName)
		})
	}
}

// TestResolveThroughAwkwardDirNames covers directory names that break naive
// string concatenation of a file:// URI: '#' was read as a fragment and '%' as
// a bad escape.
func TestResolveThroughAwkwardDirNames(t *testing.T) {
	for _, dirName := range []string{"has#hash", "has%percent", "has space", "has?question"} {
		t.Run(dirName, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), dirName)
			writeSchemaFile(t, dir, "target.json", `{"type":"string","maxLength":3}`)
			writeSchemaFile(t, dir, "root.json", `{"properties":{"x":{"$ref":"target.json"}}}`)

			resolved, err := resolveSchemaFile(dir, "root.json")
			require.NoError(t, err, "directory named %q must resolve", dirName)
			// Prove the ref really loaded rather than resolving to an empty schema.
			require.Error(t, resolved.Validate(map[string]any{"x": "far too long"}))
		})
	}
}

// TestLoaderRejectsNonSiblingRefs is the containment contract. Each ref below
// once bound silently to the same-named local file, so a document would be
// validated against constraints from an entirely unrelated schema. Rejection
// must be loud.
func TestLoaderRejectsNonSiblingRefs(t *testing.T) {
	for _, tc := range []struct{ name, ref string }{
		{"remote_https", "https://example.com/target.json"},
		{"remote_http", "http://example.com/target.json"},
		{"subdirectory", "sub/target.json"},
		{"parent_traversal", "../target.json"},
		{"absolute_path", "/etc/target.json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			// The trap: a local file of the same base name. Silent substitution
			// would validate against this instead of the intended target.
			writeSchemaFile(t, dir, "target.json", `{"type":"string","maxLength":1}`)
			writeSchemaFile(t, dir, "root.json",
				fmt.Sprintf(`{"properties":{"x":{"$ref":%q}}}`, tc.ref))

			_, err := resolveSchemaFile(dir, "root.json")
			require.Error(t, err, "ref %q must be rejected, not silently bound to a local file", tc.ref)
			require.Contains(t, err.Error(), "target.json", "error must name the offending reference")
		})
	}
}

// TestLoaderResolvesSiblingRef is the positive counterpart: a plain sibling
// reference must still work, so the containment check above is not simply
// rejecting everything.
func TestLoaderResolvesSiblingRef(t *testing.T) {
	dir := t.TempDir()
	writeSchemaFile(t, dir, "target.json", `{"type":"string","maxLength":3}`)
	writeSchemaFile(t, dir, "root.json", `{"properties":{"x":{"$ref":"target.json"}}}`)

	resolved, err := resolveSchemaFile(dir, "root.json")
	require.NoError(t, err)
	require.NoError(t, resolved.Validate(map[string]any{"x": "ok"}))
	require.Error(t, resolved.Validate(map[string]any{"x": "far too long"}))
}

// TestResolveSchemaFileMissingNamesSchema pins that a missing file reports the
// path, rather than returning the read error bare.
func TestResolveSchemaFileMissingNamesSchema(t *testing.T) {
	_, err := resolveSchemaFile(t.TempDir(), "absent.json")
	require.Error(t, err)
	require.Contains(t, err.Error(), "absent.json")
}

// TestValidateRejectsEnumViolation is the issue's own example: lead.json
// declares status as a four-value enum and "banana" is not one of them.
//
// The substring assertion couples to jsonschema-go's message prose
// ("enum: banana does not equal any of: [...]"). That is deliberate — it proves
// the offending value reaches the operator — but it will need updating if the
// library rewords its enum failure.
func TestValidateRejectsEnumViolation(t *testing.T) {
	dir := shippedSchemaDir(t)
	resolved, err := resolveSchemaFile(dir, "lead.json")
	require.NoError(t, err)

	err = resolved.Validate(map[string]any{"status": "banana"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "banana")
}

func writeSchemaFile(t *testing.T, dir, name, body string) {
	t.Helper()
	full := filepath.Join(dir, name)
	require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o750))
	require.NoError(t, os.WriteFile(full, []byte(body), 0o600))
}
