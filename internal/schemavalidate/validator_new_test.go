package schemavalidate

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// stubRegistry is the smallest forma.SchemaRegistry that New needs.
type stubRegistry struct {
	names   []string
	byName  map[string]forma.JSONSchema
	idsByNm map[string]int16
}

func (r *stubRegistry) ListSchemas() []string { return r.names }
func (r *stubRegistry) GetSchemaByName(name string) (int16, forma.JSONSchema, error) {
	js, ok := r.byName[name]
	if !ok {
		return 0, forma.JSONSchema{}, fmt.Errorf("no schema %q", name)
	}
	return r.idsByNm[name], js, nil
}
func (r *stubRegistry) GetSchemaByID(int16) (string, forma.JSONSchema, error) {
	return "", forma.JSONSchema{}, fmt.Errorf("not used")
}
func (r *stubRegistry) GetSchemaAttributeCacheByName(string) (int16, forma.SchemaAttributeCache, error) {
	return 0, nil, fmt.Errorf("not used")
}
func (r *stubRegistry) GetSchemaAttributeCacheByID(int16) (string, forma.SchemaAttributeCache, error) {
	return "", nil, fmt.Errorf("not used")
}

func registryWith(t *testing.T, name string, schemaJSON string, id int16) *stubRegistry {
	t.Helper()
	return &stubRegistry{
		names:   []string{name},
		byName:  map[string]forma.JSONSchema{name: {ID: id, Name: name, Schema: schemaJSON}},
		idsByNm: map[string]int16{name: id},
	}
}

// TestNewFailsClosedOnUnresolvableSchema pins the #314 decision: a schema that
// cannot resolve must stop the process at construction, naming the schema. A
// schema that silently stops validating is the failure this issue exists to fix.
func TestNewFailsClosedOnUnresolvableSchema(t *testing.T) {
	dir := t.TempDir()
	broken := `{"type":"object","properties":{"x":{"$ref":"missing.json#/$defs/nope"}}}`
	_, err := New(registryWith(t, "broken", broken, 7), dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "broken")
}

// TestNewRejectsUnsupportedSchemaVersion pins that an unsupported $schema is
// caught at construction. jsonschema-go's detectDraft silently falls through to
// draft 2020-12 for anything it does not recognise, so resolution succeeds and
// the failure only appears inside Validate — where it would be wrapped as
// ErrInvalidInput and answer 400 to every write, blaming a caller mistake that
// does not exist. It is a schema-configuration fault and belongs at startup.
func TestNewRejectsUnsupportedSchemaVersion(t *testing.T) {
	for _, tc := range []struct{ name, version string }{
		{"draft_2019_09", "https://json-schema.org/draft/2019-09/schema"},
		{"draft_06", "http://json-schema.org/draft-06/schema#"},
		{"typo_in_url", "https://json-schema.org/draft/2020-12/schemaX"},
		{"not_a_url", "2020-12"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := fmt.Sprintf(`{"$schema":%q,"type":"object"}`, tc.version)
			_, err := New(registryWith(t, "ev", schema, 3), t.TempDir())
			require.Error(t, err)
			require.NotErrorIs(t, err, forma.ErrInvalidInput,
				"a schema-configuration fault must not be a client error")
			require.Contains(t, err.Error(), "ev", "error must name the schema")
			require.Contains(t, err.Error(), tc.version, "error must name the version")
		})
	}
}

// TestNewAcceptsSupportedSchemaVersions is the positive counterpart: the guard
// above must not refuse to boot on a version the library can actually validate.
// An over-strict startup gate is worse than the bug it replaces.
func TestNewAcceptsSupportedSchemaVersions(t *testing.T) {
	for _, tc := range []struct{ name, version string }{
		{"draft_2020_12", "https://json-schema.org/draft/2020-12/schema"},
		{"draft_07_http", "http://json-schema.org/draft-07/schema#"},
		{"draft_07_https", "https://json-schema.org/draft-07/schema#"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := fmt.Sprintf(`{"$schema":%q,"type":"object"}`, tc.version)
			v, err := New(registryWith(t, "ev", schema, 3), t.TempDir())
			require.NoError(t, err)
			require.NoError(t, v.Validate(3, map[string]any{}))
		})
	}
}

// TestNewAcceptsAbsentSchemaVersion pins that omitting $schema stays legal: the
// library treats an empty version as supported and defaults to draft 2020-12.
func TestNewAcceptsAbsentSchemaVersion(t *testing.T) {
	v, err := New(registryWith(t, "ev", `{"type":"object"}`, 3), t.TempDir())
	require.NoError(t, err)
	require.NoError(t, v.Validate(3, map[string]any{}))
}

// TestNewRejectsUnknownTypeKeyword pins the same error-class fix for a "type"
// value outside the JSON Schema vocabulary. Resolution accepts it, and then
// every document fails the type check, so a typo'd type 400s every write with
// a message the caller cannot act on. The nested and $defs cases matter most:
// entity schemas declare types on properties, not on the root.
// The path assertion is the operator's only route to the typo: a shipped schema
// declares dozens of types, and the offending value alone does not say which
// property carries it.
func TestNewRejectsUnknownTypeKeyword(t *testing.T) {
	for _, tc := range []struct{ name, schema, bad, path string }{
		{"root", `{"type":"nosuchtype"}`, "nosuchtype", "the schema root"},
		{"property", `{"type":"object","properties":{"x":{"type":"strig"}}}`, "strig", "/properties/x"},
		{"defs", `{"type":"object","$defs":{"d":{"type":"bogus"}}}`, "bogus", "/$defs/d"},
		{"items", `{"type":"array","items":{"type":"nummber"}}`, "nummber", "/items"},
		{"nested", `{"properties":{"a":{"properties":{"b":{"type":"strin"}}}}}`, "strin", "/properties/a/properties/b"},
		{"allof", `{"allOf":[{"type":"object"},{"type":"objct"}]}`, "objct", "/allOf/1"},
		{
			"type_array",
			`{"type":"object","properties":{"x":{"type":["string","nil"]}}}`,
			"nil", "/properties/x",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(registryWith(t, "ev", tc.schema, 3), t.TempDir())
			require.Error(t, err)
			require.NotErrorIs(t, err, forma.ErrInvalidInput,
				"a schema-configuration fault must not be a client error")
			require.Contains(t, err.Error(), "ev", "error must name the schema")
			require.Contains(t, err.Error(), tc.bad, "error must name the offending type")
			require.Contains(t, err.Error(), tc.path, "error must name the subschema holding it")
		})
	}
}

// TestNewAcceptsEveryJSONSchemaType is the positive counterpart, covering the
// whole vocabulary in both the single-type and multi-type spellings so the
// guard cannot pass by rejecting broadly.
func TestNewAcceptsEveryJSONSchemaType(t *testing.T) {
	for _, typeName := range []string{"null", "boolean", "object", "array", "number", "string", "integer"} {
		t.Run(typeName, func(t *testing.T) {
			single := fmt.Sprintf(`{"type":"object","properties":{"x":{"type":%q}}}`, typeName)
			_, err := New(registryWith(t, "ev", single, 3), t.TempDir())
			require.NoError(t, err, "type %q must be accepted", typeName)

			multi := fmt.Sprintf(`{"type":"object","properties":{"x":{"type":[%q,"null"]}}}`, typeName)
			_, err = New(registryWith(t, "ev", multi, 3), t.TempDir())
			require.NoError(t, err, "type [%q, null] must be accepted", typeName)
		})
	}
}

// TestNewRejectsUnknownTypeInReferencedSchema extends the construction guards to
// the documents the loader pulls in.
//
// New checked only the registry roots, so a root referencing common.json with a
// typo'd "type" constructed cleanly and the fault surfaced inside Validate —
// where the blanket wrap turns it into ErrInvalidInput. That answers 400 to
// every create for an operator fault, and report-only updates absorb it and
// write anyway. The error must name the file, or the operator is left hunting
// through the root that merely references it.
func TestNewRejectsUnknownTypeInReferencedSchema(t *testing.T) {
	dir := t.TempDir()
	writeSchemaFile(t, dir, "common.json", `{"type":"strig"}`)
	root := `{"type":"object","properties":{"x":{"$ref":"common.json"}}}`

	_, err := New(registryWith(t, "ev", root, 3), dir)
	require.Error(t, err)
	require.NotErrorIs(t, err, forma.ErrInvalidInput,
		"a schema-configuration fault must not be a client error")
	require.Contains(t, err.Error(), "common.json", "error must name the offending file")
	require.Contains(t, err.Error(), "strig", "error must name the offending type")
}

// TestNewRejectsUnknownTypeInNestedReference pins that the check follows the
// reference chain rather than stopping one file out: root -> middle -> leaf.
func TestNewRejectsUnknownTypeInNestedReference(t *testing.T) {
	dir := t.TempDir()
	writeSchemaFile(t, dir, "leaf.json", `{"type":"nummber"}`)
	writeSchemaFile(t, dir, "middle.json", `{"properties":{"y":{"$ref":"leaf.json"}}}`)
	root := `{"type":"object","properties":{"x":{"$ref":"middle.json"}}}`

	_, err := New(registryWith(t, "ev", root, 3), dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "leaf.json")
	require.Contains(t, err.Error(), "nummber")
}

// TestNewAcceptsCyclicAndRepeatedReferences is the termination and cost guard on
// the check above. A cycle must not recurse forever, and a diamond must not make
// construction quadratic in the reference graph — both are why the checked set
// is cached by resolved path.
//
// The test would hang rather than fail on a runaway recursion, which is the
// honest signal: there is no partial answer to assert.
func TestNewAcceptsCyclicAndRepeatedReferences(t *testing.T) {
	dir := t.TempDir()
	// a <-> b is a genuine cycle; both arms of the diamond reach shared.json.
	writeSchemaFile(t, dir, "shared.json", `{"type":"string"}`)
	writeSchemaFile(t, dir, "a.json",
		`{"properties":{"toB":{"$ref":"b.json"},"s":{"$ref":"shared.json"}}}`)
	writeSchemaFile(t, dir, "b.json",
		`{"properties":{"toA":{"$ref":"a.json"},"s":{"$ref":"shared.json"}}}`)
	root := `{"type":"object","properties":{"x":{"$ref":"a.json"}}}`

	v, err := New(registryWith(t, "ev", root, 3), dir)
	require.NoError(t, err)
	// Prove the refs really loaded rather than resolving to empty schemas.
	require.ErrorIs(t, v.Validate(3, map[string]any{"x": map[string]any{"s": 12345}}),
		forma.ErrInvalidInput)
}

// TestShippedSchemasPassConstructionGuards is the anti-regression for the two
// guards above: every schema the server actually ships must still construct.
// A guard that rejects a production schema turns a latent 400 into a refusal to
// boot, which is a worse failure than the one being fixed.
func TestShippedSchemasPassConstructionGuards(t *testing.T) {
	dir := shippedSchemaDir(t)
	for _, fileName := range shippedSchemaNames(t, dir) {
		t.Run(strings.TrimSuffix(fileName, ".json"), func(t *testing.T) {
			body, err := os.ReadFile(filepath.Join(dir, fileName))
			require.NoError(t, err)

			name := strings.TrimSuffix(fileName, ".json")
			_, err = New(registryWith(t, name, string(body), 3), dir)
			require.NoError(t, err, "shipped schema %s must pass construction guards", fileName)
		})
	}
}
