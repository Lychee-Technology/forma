package internal

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/lychee-technology/forma/internal/schemameta"
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
	_, err := LoadRelationIndex(relationFixtureRegistry(t, "relation_required_root"))

	require.Error(t, err)
	require.ErrorContains(t, err, "child")
	require.ErrorContains(t, err, "contactSnapshot")
	require.ErrorContains(t, err, "required")
}

// TestLoadRelationIndexRejectsComposedRequiredRelationRoot covers the bypass the
// root-level-"required" read left open: the validator resolves composition, so a
// relation root can be made mandatory by an applicator that lands on the root
// object even though the root's own "required" array never names it.
//
// How much each vector is really enforced differs, and the list is not uniform.
// Measured by resolving each child.json and validating the payloads a caller can
// still send once the strip has removed contactSnapshot:
//
//   - allof, allof_nested, dependent, dependent_schema, dependencies_list and
//     dependencies_schema reject every such payload. The dependent* and
//     dependencies* keywords are conditional in general; these fixtures trigger
//     on "parentId", which they also list in root "required", so the branch is
//     universal here.
//   - oneof and ifthen reject only some. oneof accepts
//     {"id","parentId","fallback"}, whose other disjunct is satisfied, and
//     ifthen accepts the same payload, because without "kind" its "if" fails and
//     "else" asks only for "fallback". Both are refused whole: the guard cannot
//     decide which documents take which branch, and a schema unwritable for some
//     of its documents is still one an operator must fix.
//
// So this test asserts what the guard refuses, which for the branching fixtures
// is deliberately a superset of the documents the validator would reject. It is
// not a superset of the *schemas* it would reject: every fixture here has at
// least one post-strip payload the validator refuses. The fixture that had none
// — relation_required_if_only, whose "if" turns on the stripped property — moved
// to TestLoadRelationIndexAcceptsRequiredOutsideRootScope.
//
// For the allOf case, where guard and validator coincide exactly, the parsed
// root's Required is empty while Resolved.Validate({"id":"x"}) answers
// `validating /allOf/0: required: missing properties: ["contactSnapshot"]`.
//
// The two "dependencies" vectors are draft-07, which forma accepts
// (schemavalidate's supportedSchemaVersions). Under draft-07 the library
// enforces this rule from "dependencies" and ignores the 2020-12 spellings
// entirely, so these were a live bypass of the first widening: both returned nil
// from ValidateRelationSchemas while the resolved validator answered
// `dependentRequired["parentId"]: missing properties ["contactSnapshot"]` (array
// form) and `validating /dependencies/parentId: required: missing properties:
// ["contactSnapshot"]` (schema form) on the post-strip payload.
func TestLoadRelationIndexRejectsComposedRequiredRelationRoot(t *testing.T) {
	for _, fixture := range []string{
		"relation_required_allof",
		"relation_required_allof_nested",
		"relation_required_oneof",
		"relation_required_ifthen",
		"relation_required_dependent",
		"relation_required_dependent_schema",
		"relation_required_dependencies_list",
		"relation_required_dependencies_schema",
	} {
		t.Run(fixture, func(t *testing.T) {
			_, err := LoadRelationIndex(relationFixtureRegistry(t, fixture))

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
	_, err := LoadRelationIndex(relationFixtureRegistry(t, "relation_root_ref"))

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
//   - relation_ok_then_without_if carries a "then" with no "if". The validator
//     ignores it — pinned by TestConditionalFixturesAgainstTheValidator — which
//     is what walkConditional's gate encodes.
//   - relation_ok_if_without_branches and relation_required_if_only put the
//     relation root inside an "if". "if" asserts nothing about the instance, it
//     only selects a branch, so it can never make a property mandatory however
//     it comes out. if_only goes further and shows the consequence: its "if"
//     names the stripped property, so on every payload a caller can still send
//     the condition fails, "then" never applies, and the validator accepts —
//     also pinned by TestConditionalFixturesAgainstTheValidator.
func TestLoadRelationIndexAcceptsRequiredOutsideRootScope(t *testing.T) {
	for _, fixture := range []string{
		"relation_ok_property_required",
		"relation_ok_not",
		"relation_ok_root_ref_no_relation",
		"relation_ok_then_without_if",
		"relation_ok_if_without_branches",
		"relation_required_if_only",
	} {
		t.Run(fixture, func(t *testing.T) {
			require.NoError(t, ValidateRelationSchemas(relationFixtureRegistry(t, fixture)))
		})
	}
}

// TestValidateRelationSchemasAcceptsShippedSchemas is the other half: the guard
// must not reject anything that ships today. visit.json is the only schema in
// the repository carrying x-relation, and contactSnapshot is not required. No
// shipped schema uses allOf/anyOf/oneOf/if/dependentRequired or a root-level
// $ref either, so the composition walk adds no rejection here.
//
// Driven through the directory registry the server itself builds, so the guard
// sees exactly the set of names production registers — which excludes the
// *_full.json documents beside them. None of those carries x-relation, so that
// exclusion changes no verdict here; it is stated because the previous
// directory-scanning loader did read them.
func TestValidateRelationSchemasAcceptsShippedSchemas(t *testing.T) {
	for _, dir := range []string{shippedSchemaDir, "../cmd/sample/schemas"} {
		registry, err := schemameta.NewFileSchemaRegistryFromDirectory(dir)
		require.NoError(t, err, "registry over %s", dir)
		require.NoError(t, ValidateRelationSchemas(registry), "guard over %s", dir)
	}
	require.NoError(t, ValidateRelationSchemas(nil), "an absent registry is not an error")
}

// TestLoadRelationIndexFailsOnUnservableSchema pins one half of the second abort
// cause. A registry that lists a name it cannot then serve is broken about a
// schema the runtime resolves and validates writes against, so there is nothing
// to skip past — and no way to tell whether that schema declares a relation
// root.
func TestLoadRelationIndexFailsOnUnservableSchema(t *testing.T) {
	registry := relationFixtureRegistry(t, "relation_ok_not")
	registry.docErr["child"] = errors.New("backing store unavailable")

	err := ValidateRelationSchemas(registry)

	require.Error(t, err)
	require.ErrorContains(t, err, "child")
	require.ErrorContains(t, err, "backing store unavailable")
}

// TestLoadRelationIndexFailsOnUndecodableDocument is the other half: a listed
// schema whose document will not decode into a JSON object.
//
// The message names the schema rather than a path, because a registry need not
// have one — it may serve the document from a database or from memory.
func TestLoadRelationIndexFailsOnUndecodableDocument(t *testing.T) {
	registry := relationFixtureRegistry(t, "relation_ok_not")
	registry.docs["child"] = `{`

	err := ValidateRelationSchemas(registry)

	require.Error(t, err)
	require.ErrorContains(t, err, "child")
}
