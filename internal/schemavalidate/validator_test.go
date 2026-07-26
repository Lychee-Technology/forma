package schemavalidate

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
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

// TestValidateRoundTripsNativeValues pins that Validate marshals before
// validating. time.Time in a native map has Go type "object" to the validator
// and fails a "type":"string" property until it is round-tripped; two real
// call sites pass time.Now() for string-typed properties.
func TestValidateRoundTripsNativeValues(t *testing.T) {
	dir := t.TempDir()
	schema := `{"type":"object","properties":{"at":{"type":"string"}}}`
	v, err := New(registryWith(t, "ev", schema, 3), dir)
	require.NoError(t, err)

	require.NoError(t, v.Validate(3, map[string]any{"at": time.Now()}))
}

// TestValidateWrapsErrInvalidInput pins the error class: a schema violation is
// caller input, so it must surface as 4xx rather than a redacted 500 (#307).
func TestValidateWrapsErrInvalidInput(t *testing.T) {
	dir := t.TempDir()
	schema := `{"type":"object","properties":{"status":{"enum":["open","won"]}}}`
	v, err := New(registryWith(t, "ev", schema, 3), dir)
	require.NoError(t, err)

	err = v.Validate(3, map[string]any{"status": "banana"})
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.Contains(t, err.Error(), "banana")
}

// TestValidateUnknownSchemaIDIsNotClientError pins that a missing resolved
// schema is an operator problem, not the caller's: it must NOT wrap
// ErrInvalidInput, or a server misconfiguration would answer 400.
func TestValidateUnknownSchemaIDIsNotClientError(t *testing.T) {
	dir := t.TempDir()
	v, err := New(registryWith(t, "ev", `{"type":"object"}`, 3), dir)
	require.NoError(t, err)

	err = v.Validate(99, map[string]any{})
	require.Error(t, err)
	require.NotErrorIs(t, err, forma.ErrInvalidInput)
}

// TestValidateDoesNotReResolve pins that Validate reuses the schema resolved by
// New. Resolving is ~250x the cost of validating, so a regression here is a
// throughput cliff rather than a correctness bug.
//
// The mechanism matters: an earlier version of this test compared
// v.resolved[id] before and after, which can only fail if something *writes*
// the map — nothing ever does, so a Validate that re-resolved on every call
// still passed. This version removes the file the schema's $ref needs once New
// has returned. A correct Validate is unaffected because resolution is already
// complete; a Validate that re-resolves must invoke the loader, which can no
// longer read the file.
func TestValidateDoesNotReResolve(t *testing.T) {
	dir := t.TempDir()
	writeSchemaFile(t, dir, "target.json", `{"type":"string","maxLength":3}`)
	const root = `{"type":"object","properties":{"x":{"$ref":"target.json"}}}`
	writeSchemaFile(t, dir, "root.json", root)

	v, err := New(registryWith(t, "ev", root, 3), dir)
	require.NoError(t, err)

	require.NoError(t, os.Remove(filepath.Join(dir, "target.json")))

	// Prove the removal is load-bearing: resolving this schema now genuinely
	// fails, so the assertions below would catch a re-resolving Validate.
	_, err = resolveSchemaFile(dir, "root.json")
	require.Error(t, err, "removing the $ref target must make resolution fail")

	require.NoError(t, v.Validate(3, map[string]any{"x": "ok"}))
	// The ref target's own constraint is still enforced, so the cached schema is
	// the fully resolved one rather than a degraded copy that skipped the $ref.
	require.ErrorIs(t, v.Validate(3, map[string]any{"x": "far too long"}), forma.ErrInvalidInput)
}

// TestValidateOnNilValidatorIsOperatorError pins that a nil *Validator returns
// the plain operator error rather than panicking. Task 4 threads a *Validator
// that is nil when validation is unconfigured; Task 5 nil-checks before
// calling, so this is insurance against a future caller that does not.
func TestValidateOnNilValidatorIsOperatorError(t *testing.T) {
	var v *Validator
	err := v.Validate(3, map[string]any{})
	require.Error(t, err)
	require.NotErrorIs(t, err, forma.ErrInvalidInput)
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
func TestNewRejectsUnknownTypeKeyword(t *testing.T) {
	for _, tc := range []struct{ name, schema, bad string }{
		{"root", `{"type":"nosuchtype"}`, "nosuchtype"},
		{"property", `{"type":"object","properties":{"x":{"type":"strig"}}}`, "strig"},
		{"defs", `{"type":"object","$defs":{"d":{"type":"bogus"}}}`, "bogus"},
		{"items", `{"type":"array","items":{"type":"nummber"}}`, "nummber"},
		{"type_array", `{"type":"object","properties":{"x":{"type":["string","nil"]}}}`, "nil"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(registryWith(t, "ev", tc.schema, 3), t.TempDir())
			require.Error(t, err)
			require.NotErrorIs(t, err, forma.ErrInvalidInput,
				"a schema-configuration fault must not be a client error")
			require.Contains(t, err.Error(), "ev", "error must name the schema")
			require.Contains(t, err.Error(), tc.bad, "error must name the offending type")
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

func writeSchemaFile(t *testing.T, dir, name, body string) {
	t.Helper()
	full := filepath.Join(dir, name)
	require.NoError(t, os.MkdirAll(filepath.Dir(full), 0o750))
	require.NoError(t, os.WriteFile(full, []byte(body), 0o600))
}
