package internal

import (
	"errors"
	"testing"

	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/stretchr/testify/require"
)

// TestLoadRelationIndexRejectsRequiredRelationRoot is the #318 guard, on the
// simplest member of the fragment it analyses: the root object's own "required".
//
// A relation root is removed from every payload before the validator runs, so a
// schema that also demands it produces a missing-required rejection on every
// create — and on every update when strict update validation is on — which the
// caller cannot fix by sending the field. The failure has to happen at startup,
// where an operator sees it, rather than as a 4xx storm in production.
func TestLoadRelationIndexRejectsRequiredRelationRoot(t *testing.T) {
	_, err := LoadRelationIndex(serveRelationFixture(t, "relation_required_root"))

	require.Error(t, err)
	require.ErrorContains(t, err, "child")
	require.ErrorContains(t, err, "contactSnapshot")
	require.ErrorContains(t, err, "required")
}

// TestLoadRelationIndexRejectsAllOfRequiredRelationRoot covers the rest of the
// fragment: a "required" reached through a chain of "allOf" branches.
//
// Every branch of an allOf applies to the same instance, unconditionally, so a
// "required" anywhere down such a chain holds on every document exactly as the
// root's own array does. That is the whole reason allOf is inside the fragment
// while every other applicator is outside it: no satisfiability question has to
// be answered to know the requirement bites.
//
// Measured for the flat case, where guard and validator coincide exactly: the
// parsed root's Required is empty while Resolved.Validate({"id":"x"}) answers
// `validating /allOf/0: required: missing properties: ["contactSnapshot"]`.
func TestLoadRelationIndexRejectsAllOfRequiredRelationRoot(t *testing.T) {
	for _, fixture := range []string{"relation_required_allof", "relation_required_allof_nested"} {
		t.Run(fixture, func(t *testing.T) {
			_, err := LoadRelationIndex(serveRelationFixture(t, fixture))

			require.Error(t, err)
			require.ErrorContains(t, err, "child")
			require.ErrorContains(t, err, "contactSnapshot")
		})
	}
}

// TestLoadRelationIndexAcceptsRequiredOutsideRootScope is the false-rejection
// guard: a "required" the walk can reach is not automatically a root
// requirement.
//
//   - relation_ok_property_required puts "contactSnapshot" in a *property's* own
//     "required". That constrains the audit object's members, not the root's,
//     and the walk never descends into "properties".
//   - relation_ok_not puts it under "not", which the walk does not enter.
//   - relation_ok_root_ref_no_relation declares no x-relation at all, so nothing
//     is stripped for it and the guard never looks at its requirements.
//   - relation_ok_then_without_if and relation_ok_if_without_branches carry
//     halves of a conditional the walk does not enter either.
//
// The first two matter most: they are the shapes where a wider walk would refuse
// a schema whose writes are perfectly fine. The rest are covered here as well as
// in TestFixturesTheGuardDeliberatelyDoesNotJudge, which records what the
// validator does with each.
func TestLoadRelationIndexAcceptsRequiredOutsideRootScope(t *testing.T) {
	for _, fixture := range []string{
		"relation_ok_property_required",
		"relation_ok_not",
		"relation_ok_root_ref_no_relation",
		"relation_ok_then_without_if",
		"relation_ok_if_without_branches",
	} {
		t.Run(fixture, func(t *testing.T) {
			_, err := LoadRelationIndex(serveRelationFixture(t, fixture))
			require.NoError(t, err)
		})
	}
}

// TestLoadRelationIndexAcceptsShippedSchemas is the other half: the guard
// must not reject anything that ships today. visit.json is the only schema in
// the repository carrying x-relation, and contactSnapshot is not required — not
// in the root array and not in any allOf, which is all the guard reads.
//
// Driven through the *directory* registry, which is not what the server builds:
// cmd/server and cmd/lambda both construct the database-backed registry
// (schemameta.NewFileSchemaRegistryContext), and cmd/tools' init-db seeds
// schema_registry with every .json except *_attributes.json — so production's
// ListSchemas does include visit_full, lead_full and the rest, which
// NewFileSchemaRegistryFromDirectory excludes (file_registry.go, the
// *_full.json test). This test therefore covers the directory-mode name set,
// which is cmd/sample's shape, not the set the production registry hands the
// guard.
//
// That gap changes no verdict today, because no *_full.json carries x-relation
// and the guard only looks at schemas that declare one. It is written down
// because the difference is invisible from here.
func TestLoadRelationIndexAcceptsShippedSchemas(t *testing.T) {
	for _, dir := range []string{shippedSchemaDir, "../cmd/sample/schemas"} {
		registry, err := schemameta.NewFileSchemaRegistryFromDirectory(dir)
		require.NoError(t, err, "registry over %s", dir)
		_, err = LoadRelationIndex(registry)
		require.NoError(t, err, "guard over %s", dir)
	}
	_, err := LoadRelationIndex(nil)
	require.NoError(t, err, "an absent registry is not an error")

	// Without this the test would pass over a registry that registered nothing
	// relation-bearing at all, which is the one way "the guard accepts the
	// shipped schemas" can be true and worthless.
	registry, err := schemameta.NewFileSchemaRegistryFromDirectory(shippedSchemaDir)
	require.NoError(t, err)
	idx, err := LoadRelationIndex(registry)
	require.NoError(t, err)
	require.NotEmpty(t, idx.Relations("visit"),
		"visit must carry a relation, or this test accepts an empty index")
}

// TestLoadRelationIndexFailsOnUnservableSchema pins one half of the only abort
// cause left besides a required relation root. A registry that lists a name it
// cannot then serve is broken about a schema the runtime resolves and validates
// writes against, so there is nothing to skip past — and no way to tell whether
// that schema declares a relation root.
func TestLoadRelationIndexFailsOnUnservableSchema(t *testing.T) {
	registry := serveRelationFixture(t, "relation_ok_not")
	registry.docErr["child"] = errors.New("backing store unavailable")

	_, err := LoadRelationIndex(registry)

	require.Error(t, err)
	require.ErrorContains(t, err, "child")
	require.ErrorContains(t, err, "backing store unavailable")
}

// TestLoadRelationIndexFailsOnUndecodableDocument is the other half: a listed
// schema whose document does not decode into any. "{" is the malformed-JSON form
// of that; a number outside float64's range is the other, and the same branch
// covers both — see LoadRelationIndex for why that is the boundary and why
// it stays a subset of what the validator refuses.
//
// The message names the schema rather than a path, because a registry need not
// have one — it may serve the document from a database or from memory.
func TestLoadRelationIndexFailsOnUndecodableDocument(t *testing.T) {
	registry := serveRelationFixture(t, "relation_ok_not")
	registry.docs["child"] = `{`

	_, err := LoadRelationIndex(registry)

	require.Error(t, err)
	require.ErrorContains(t, err, "child")
}

// TestLoadRelationIndexAcceptsNonObjectDocuments is the boundary between those
// two: a document that parses as JSON but is not an object declares no
// properties, so it has no relation roots, so there is nothing for this guard to
// strip or to refuse.
//
// "true" and "false" are the cases that matter, because they are legal JSON
// Schemas and the validator accepts them: jsonschema.Schema unmarshals, resolves
// and validates a boolean document, while json.Unmarshal into map[string]any
// answers "cannot unmarshal bool". Treating that as fatal would abort startup
// for a deployment the validator is perfectly happy with — and did, until this
// test.
//
// "[1,2,3]", `"x"` and "null" ride along. The first two are not legal schemas at
// all and schemavalidate.New refuses them; refusing them a second time here
// would only split one fault across two checks.
func TestLoadRelationIndexAcceptsNonObjectDocuments(t *testing.T) {
	docs := readSchemaDocs(t, resolveRelationFixtureDir("relation_ok_not"))
	for name, body := range map[string]string{
		"boolean_true":  `true`,
		"boolean_false": `false`,
		"array_doc":     `[1,2,3]`,
		"string_doc":    `"x"`,
		"null_doc":      `null`,
	} {
		docs[name] = body
	}

	idx, err := LoadRelationIndex(newDocSchemaRegistry(docs))

	require.NoError(t, err)
	require.Len(t, idx.Relations("child"), 1,
		"the schemas beside a non-object document must still index")
	for _, name := range []string{"boolean_true", "boolean_false", "array_doc", "string_doc", "null_doc"} {
		require.Empty(t, idx.Relations(name), "%s declares no properties, so no relation roots", name)
	}
}
