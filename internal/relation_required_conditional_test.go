package internal

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/stretchr/testify/require"
)

// validateFixtureChild resolves a fixture's child.json through the same
// constructor production uses and validates one payload against it.
//
// The guard predicts this function's answer, but only inside the fragment it
// analyses. Outside that fragment the guard says nothing at all, so this is what
// makes "the guard is silent here, and here is what the validator actually
// does" a measured claim rather than a remembered one.
func validateFixtureChild(t *testing.T, fixture string, payload map[string]any) error {
	t.Helper()
	dir := resolveRelationFixtureDir(fixture)
	registry := serveRelationFixture(t, fixture)

	validator, err := schemavalidate.New(registry, dir)
	require.NoError(t, err, "fixture %s must resolve", fixture)

	childID, _, err := registry.GetSchemaByName("child")
	require.NoError(t, err)

	return validator.Validate(childID, payload)
}

// buildStrippedPayload is the document a caller of one of these fixtures can still
// send once StripComputedFields has removed "contactSnapshot": both root-level
// required properties, and nothing under the relation root. Every claim below is
// about payloads of exactly this shape, because they are the only ones the
// guard's question is about.
func buildStrippedPayload() map[string]any {
	return map[string]any{"id": "x", "parentId": "p"}
}

// TestGuardNeverRefusesASchemaTheValidatorAccepts is the soundness property the
// #318 guard now trades everything else for: **zero false positives**.
//
// It is asserted mechanically over every committed fixture rather than over a
// hand-kept list, so a fixture added later is covered the day it lands. For each
// one it drives the real guard and the real resolved validator, and requires the
// implication
//
//	guard refuses  =>  the validator rejects the post-strip payload
//
// which is what "the analysed fragment is unconditional" means in practice: a
// requirement the guard collects holds on every document, so in particular on
// this one. The converse is deliberately *not* asserted — the guard is allowed
// to stay silent about a requirement the validator does enforce, and
// TestGuardIsSilentOnADoubleNegatedRequirement pins one such case.
func TestGuardNeverRefusesASchemaTheValidatorAccepts(t *testing.T) {
	fixtures := listRelationFixtures(t)
	require.NotEmpty(t, fixtures)

	for _, fixture := range fixtures {
		t.Run(fixture, func(t *testing.T) {
			if ValidateRelationSchemas(serveRelationFixture(t, fixture)) == nil {
				return
			}
			require.Error(t, validateFixtureChild(t, fixture, buildStrippedPayload()),
				"the guard refused %s, so the validator must reject the post-strip payload; "+
					"a guard that refuses a schema the validator accepts is unfixable at startup", fixture)
		})
	}
}

// listRelationFixtures names every committed relation fixture directory.
func listRelationFixtures(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir("testdata")
	require.NoError(t, err)

	var fixtures []string
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), "relation_") {
			fixtures = append(fixtures, e.Name())
		}
	}
	return fixtures
}

// TestGuardIsSilentOnADoubleNegatedRequirement is a **pinned false negative**,
// and is committed as one deliberately.
//
// {"not": {"not": {"required": ["contactSnapshot"]}}} is logically identical to
// requiring the property, and the validator enforces it as such. The guard does
// not look under "not" at all — deciding what an arbitrary negation demands is
// the satisfiability question the analysed fragment exists to avoid — so this
// schema boots and every write against it then fails.
//
// That is the accepted cost of zero false positives. The backstop is on the
// write path, not here: the failing write carries an operator-facing
// explanation naming the stripped root (see
// TestWriteDiagnosisExplainsAStrippedRelationRoot).
func TestGuardIsSilentOnADoubleNegatedRequirement(t *testing.T) {
	require.NoError(t, ValidateRelationSchemas(serveRelationFixture(t, "relation_required_double_not")),
		"the guard does not judge \"not\", so it must boot this schema")

	require.Error(t, validateFixtureChild(t, "relation_required_double_not", buildStrippedPayload()),
		"and the validator does demand the stripped root, which is what makes this a false negative")
}

// TestFixturesTheGuardDeliberatelyDoesNotJudge records what the validator
// actually does with each shape the guard steps over, so the cost of the
// fragment is written down in measurements rather than in prose.
//
// Every fixture here boots. What differs is what happens afterwards:
//
//   - anyof_true and if_false are the two shapes that were **false positives**
//     before the fragment shrank. anyOf's first branch is the boolean schema
//     true, which accepts everything, so the disjunction is satisfied whatever
//     the second branch asks for; "if": false never matches, so its "then" never
//     applies. The validator accepts both post-strip payloads, and refusing to
//     boot over them was simply wrong.
//   - oneof, ifthen, dependent, dependent_schema, dependencies_list and
//     dependencies_schema do constrain some documents. The validator rejects the
//     post-strip payload for all six.
//   - if_only and root_ref are accepted by the validator too: if_only's "if"
//     turns on the stripped property so its "then" never fires, and root_ref's
//     referenced parent.json requires nothing.
func TestFixturesTheGuardDeliberatelyDoesNotJudge(t *testing.T) {
	for fixture, validatorRejects := range map[string]bool{
		"relation_ok_anyof_true":                false,
		"relation_ok_if_false":                  false,
		"relation_required_if_only":             false,
		"relation_root_ref":                     false,
		"relation_required_oneof":               true,
		"relation_required_ifthen":              true,
		"relation_required_dependent":           true,
		"relation_required_dependent_schema":    true,
		"relation_required_dependencies_list":   true,
		"relation_required_dependencies_schema": true,
	} {
		t.Run(fixture, func(t *testing.T) {
			require.NoError(t, ValidateRelationSchemas(serveRelationFixture(t, fixture)),
				"the guard does not judge this shape, so startup must succeed")

			err := validateFixtureChild(t, fixture, buildStrippedPayload())
			if validatorRejects {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestConditionalFixturesAgainstTheValidator keeps the payload-level detail the
// if/then/else fixtures were built to record. "if" contributes no assertions of
// its own; only "then" and "else" do.
func TestConditionalFixturesAgainstTheValidator(t *testing.T) {
	t.Run("if_only demands nothing once the relation root is stripped", func(t *testing.T) {
		require.NoError(t, validateFixtureChild(t, "relation_required_if_only",
			map[string]any{"id": "x", "parentId": "p"}))
		require.NoError(t, validateFixtureChild(t, "relation_required_if_only",
			map[string]any{"id": "x", "parentId": "p", "fallback": "f"}))
	})

	t.Run("ifthen demands the relation root on the documents that take then", func(t *testing.T) {
		err := validateFixtureChild(t, "relation_required_ifthen",
			map[string]any{"id": "x", "parentId": "p", "kind": "snapshot"})

		require.Error(t, err)
		require.ErrorContains(t, err, "contactSnapshot")
	})

	t.Run("then without if is inert", func(t *testing.T) {
		require.NoError(t, validateFixtureChild(t, "relation_ok_then_without_if",
			map[string]any{"id": "x", "parentId": "p"}))
	})
}

// TestValidatorNamesTheMissingRootUnderOnlySomeApplicators is the measurement
// that decided how the runtime diagnosis is gated (see
// explainStrippedRelationRoots).
//
// The narrower gate would be "fire only when the validator's message names a
// relation root as a missing required property". The question is whether that
// gate would cover the shapes the startup guard no longer judges, and the answer
// is: some of them, not all. Measured here over every unjudged fixture, on the
// post-strip payload:
//
//   - the dependent* family names the property — `dependentRequired["parentId"]:
//     missing properties ["contactSnapshot"]` and the /dependencies/ and
//     /dependentSchemas/ spellings alike — so a message-shape gate would fire on
//     all four of those;
//   - "not", "oneOf" and the "if"/"else" form do not name it. The first two
//     render the offending branch anonymously (`not: validated against
//     <anonymous schema>`, `oneOf: did not validate against any of [...]`), and
//     the third reports whichever *other* branch the document actually took, so
//     its text is about "fallback" and never mentions the relation root at all.
//
// Three shapes with no message to key on is why the gate is deliberately widened
// to "the schema declares relation roots" rather than made a hybrid: a hybrid
// would have to recognise the anonymous forms by their prose, which is fragile,
// and would still miss any applicator not on the list.
//
// Both halves are pinned, not just the convenient one. If jsonschema-go starts
// naming the property under "not"/"oneOf"/"else", or stops naming it under
// dependent*, this test fails and the trade is worth re-deriving.
func TestValidatorNamesTheMissingRootUnderOnlySomeApplicators(t *testing.T) {
	for fixture, namesTheRoot := range map[string]bool{
		"relation_required_double_not":          false,
		"relation_required_oneof":               false,
		"relation_required_ifthen":              false,
		"relation_required_dependent":           true,
		"relation_required_dependent_schema":    true,
		"relation_required_dependencies_list":   true,
		"relation_required_dependencies_schema": true,
	} {
		t.Run(fixture, func(t *testing.T) {
			err := validateFixtureChild(t, fixture, buildStrippedPayload())
			require.Error(t, err)

			if namesTheRoot {
				require.ErrorContains(t, err, "contactSnapshot",
					"a message-shape gate would fire here")
				return
			}
			require.NotContains(t, err.Error(), "contactSnapshot",
				"a message-shape gate would stay silent here, which is why the diagnosis does not use one")
		})
	}

	// The counterpart inside the analysed fragment, where guard and message agree.
	err := validateFixtureChild(t, "relation_required_root", buildStrippedPayload())
	require.ErrorContains(t, err, "contactSnapshot")
}

// resolveRelationFixtureDir names a committed schema directory under testdata.
// Each one holds a parent.json plus a child.json declaring a single x-relation
// root (contactSnapshot), and differs from the others only in how — or whether —
// that root is made required. They are committed rather than written per test so
// the factory test can point Entity.SchemaDirectory at the same bytes this
// package asserts on (#318).
//
// The name prefix records what the *validator* does with the fixture, not what
// the guard does: relation_required_* holds a document whose post-strip payload
// the validator rejects, relation_ok_* one it accepts. The guard's own verdict
// is a separate question and is asserted per test, because since the fragment
// shrank the two no longer coincide.
func resolveRelationFixtureDir(name string) string {
	return filepath.Join("testdata", name)
}
