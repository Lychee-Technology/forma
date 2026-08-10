package internal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// relationFixtureDir names a committed schema directory under testdata. Each one
// holds a parent.json plus a child.json declaring a single x-relation root
// (contactSnapshot), and differs from the others only in how — or whether — that
// root is made required. They are committed rather than written per test so the
// factory test can point Entity.SchemaDirectory at the same bytes this package
// asserts on (#318).
func relationFixtureDir(name string) string {
	return filepath.Join("testdata", name)
}

// TestLoadRelationIndexRejectsRequiredRelationRoot is the #318 guard.
//
// A relation root is removed from every payload before the validator runs, so a
// schema that also demands it produces a missing-required rejection on every
// create — and on every update when strict update validation is on — which the
// caller cannot fix by sending the field. The failure has to happen at startup,
// where an operator sees it, rather than as a 4xx storm in production.
func TestLoadRelationIndexRejectsRequiredRelationRoot(t *testing.T) {
	_, err := LoadRelationIndex(relationFixtureDir("relation_required_root"))

	require.Error(t, err)
	require.ErrorContains(t, err, "child")
	require.ErrorContains(t, err, "contactSnapshot")
	require.ErrorContains(t, err, "required")
}

// TestLoadRelationIndexRejectsComposedRequiredRelationRoot covers the bypass the
// root-level-"required" read left open: the validator resolves composition, so a
// relation root made mandatory through any applicator that lands on the root
// object is enforced at runtime even though the root's own "required" array
// never names it.
//
// Each vector was confirmed against the library before the guard was widened:
// for the allOf case the parsed root's Required is empty while
// Resolved.Validate({"id":"x"}) answers `validating /allOf/0: required: missing
// properties: ["contactSnapshot"]`.
func TestLoadRelationIndexRejectsComposedRequiredRelationRoot(t *testing.T) {
	for _, fixture := range []string{
		"relation_required_allof",
		"relation_required_allof_nested",
		"relation_required_oneof",
		"relation_required_ifthen",
		"relation_required_if_only",
		"relation_required_dependent",
		"relation_required_dependent_schema",
	} {
		t.Run(fixture, func(t *testing.T) {
			_, err := LoadRelationIndex(relationFixtureDir(fixture))

			require.Error(t, err)
			require.ErrorContains(t, err, "child")
			require.ErrorContains(t, err, "contactSnapshot")
		})
	}
}

// TestLoadRelationIndexRefusesRootRefBesideRelation pins the "refuse what cannot
// be analysed" half. A $ref that applies to the root instance can compose a
// "required" this package cannot see: it does not resolve references, and
// jsonschema.Resolved offers nothing to borrow — its only schema accessor,
// Schema(), still answers the root with Ref set to the unresolved string after
// Resolve. Guessing would be worse than refusing.
func TestLoadRelationIndexRefusesRootRefBesideRelation(t *testing.T) {
	_, err := LoadRelationIndex(relationFixtureDir("relation_root_ref"))

	require.Error(t, err)
	require.ErrorContains(t, err, "child")
	require.ErrorContains(t, err, "$ref")
}

// TestLoadRelationIndexAcceptsRequiredOutsideRootScope is the false-rejection
// guard, and matters more than the vectors above: the walk must not treat every
// "required" it can reach as a root requirement.
//
//   - relation_ok_property_required puts "contactSnapshot" in a *property's* own
//     "required". That constrains the audit object's members, not the root's.
//   - relation_ok_not puts it under "not", where it asserts the opposite and can
//     never make the property mandatory.
//   - relation_ok_root_ref_no_relation carries a root $ref but declares no
//     x-relation, so nothing is stripped and there is nothing to refuse.
func TestLoadRelationIndexAcceptsRequiredOutsideRootScope(t *testing.T) {
	for _, fixture := range []string{
		"relation_ok_property_required",
		"relation_ok_not",
		"relation_ok_root_ref_no_relation",
	} {
		t.Run(fixture, func(t *testing.T) {
			require.NoError(t, ValidateRelationSchemas(relationFixtureDir(fixture)))
		})
	}
}

// TestValidateRelationSchemasAcceptsShippedSchemas is the other half: the guard
// must not reject anything that ships today. visit.json is the only schema in
// the repository carrying x-relation, and contactSnapshot is not required. No
// shipped schema uses allOf/anyOf/oneOf/if/dependentRequired or a root-level
// $ref either, so the composition walk adds no rejection here.
func TestValidateRelationSchemasAcceptsShippedSchemas(t *testing.T) {
	require.NoError(t, ValidateRelationSchemas(shippedSchemaDir))
	require.NoError(t, ValidateRelationSchemas("../cmd/sample/schemas"))
	require.NoError(t, ValidateRelationSchemas(""), "an unconfigured schema directory is not an error")
}

// TestLoadRelationIndexSkipsUnparseableFile pins the narrowed startup contract.
//
// By the time ValidateRelationSchemas runs, schemavalidate.New has already
// parsed every *registered* schema and failed closed on any that would not — so
// a document in SCHEMA_DIR that this loader cannot parse is not a registered
// entity schema at all, and refusing to boot over it would abort startup for a
// file nothing reads. It is skipped with a warning instead, and the schemas
// around it still index.
func TestLoadRelationIndexSkipsUnparseableFile(t *testing.T) {
	dir := t.TempDir()
	copyFixture(t, relationFixtureDir("relation_ok_not"), dir)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "stray.json"), []byte(`{`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "notobject.json"), []byte(`[1,2,3]`), 0o600))

	idx, err := LoadRelationIndex(dir)

	require.NoError(t, err, "a stray malformed .json must not abort startup")
	require.Len(t, idx.Relations("child"), 1,
		"the schemas beside the malformed file must still index")
}

// TestLoadRelationIndexFailsOnUnreadableDir pins the half that stays fatal. A
// SCHEMA_DIR that cannot be listed is a misconfiguration of the server itself,
// not a stray file in an otherwise valid directory, and silently indexing
// nothing would disable relation stripping for every schema.
func TestLoadRelationIndexFailsOnUnreadableDir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-dir")

	err := ValidateRelationSchemas(missing)

	require.Error(t, err)
	require.ErrorContains(t, err, missing)
}

// copyFixture copies a committed fixture directory's .json files into dst, so a
// test can add files beside them without mutating the fixture.
func copyFixture(t *testing.T, src, dst string) {
	t.Helper()
	entries, err := os.ReadDir(src)
	require.NoError(t, err)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		body, err := os.ReadFile(filepath.Join(src, e.Name()))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dst, e.Name()), body, 0o600))
	}
}
