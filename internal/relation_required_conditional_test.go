package internal

import (
	"fmt"
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

	if err := validator.Validate(childID, payload); err != nil {
		// A plain wrap: it adds the fixture the verdict came from, and leaves the
		// published message and the ErrInvalidInput classification alone, which
		// TestWriteDiagnosisIsWithheldFromThePublishedMessage compares against.
		return fmt.Errorf("validate the payload against fixture %s: %w", fixture, err)
	}
	return nil
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
//
// It also holds every fact the fixture's sidecar records — the guard's verdict,
// the validator's, and for a rejection whether the message names the root — to
// what the guard and the validator measurably do, so a fixture that starts or
// stops being judged or rejected, or a sidecar that records the wrong answer, is
// a failure here rather than a silent move between the subsets the next two
// tests draw (#395).
func TestGuardNeverRefusesASchemaTheValidatorAccepts(t *testing.T) {
	fixtures := listRelationFixtures(t)
	require.NotEmpty(t, fixtures)

	for _, fixture := range fixtures {
		t.Run(fixture, func(t *testing.T) {
			verdict := loadRelationFixtureVerdict(t, fixture)
			_, err := LoadRelationIndex(serveRelationFixture(t, fixture))
			validatorErr := validateFixtureChild(t, fixture, buildStrippedPayload())
			if err != nil {
				// Only a relation-root refusal is evidence for the implication. The
				// guard's other abort causes — a registry that lists a name it cannot
				// serve, a document that does not decode — say nothing about what the
				// validator demands, so accepting one as a refusal would let a broken
				// fixture stand in for the property under test and pass whatever the
				// validator did.
				require.ErrorContains(t, err, "requires relation root",
					"%s failed the guard for a reason that is not a relation-root refusal, "+
						"so this test can draw no conclusion from it", fixture)
				require.Error(t, validatorErr,
					"the guard refused %s, so the validator must reject the post-strip payload; "+
						"a guard that refuses a schema the validator accepts is unfixable at startup", fixture)
			}
			// After the property, so a broken soundness property is reported as
			// itself rather than as a sidecar that cannot record it.
			require.NoError(t, relationFixtureVerdictMismatch(fixture, verdict, err, validatorErr))
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
	_, err := LoadRelationIndex(serveRelationFixture(t, "relation_required_double_not"))
	require.NoError(t, err, "the guard does not judge \"not\", so it must boot this schema")

	require.Error(t, validateFixtureChild(t, "relation_required_double_not", buildStrippedPayload()),
		"and the validator does demand the stripped root, which is what makes this a false negative")
}

// TestFixturesTheGuardDeliberatelyDoesNotJudge records what the validator
// actually does with each shape the guard steps over, so the cost of the
// fragment is written down in measurements rather than in prose.
//
// The set is every committed fixture whose verdict.txt says the guard boots it,
// enumerated from disk, and the expected validator verdict is the sidecar's
// (#395). Every fixture here boots. What differs is what happens afterwards;
// among them:
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
//   - double_not is rejected, and is the pinned false negative of
//     TestGuardIsSilentOnADoubleNegatedRequirement.
//   - the shapes outside the walk's root scope (relation_ok_not,
//     relation_ok_property_required and the rest listed in
//     TestLoadRelationIndexAcceptsRequiredOutsideRootScope) are all accepted.
func TestFixturesTheGuardDeliberatelyDoesNotJudge(t *testing.T) {
	seen := map[bool]int{}
	for _, fixture := range listRelationFixtures(t) {
		verdict := loadRelationFixtureVerdict(t, fixture)
		if verdict.GuardRefuses {
			continue
		}
		seen[verdict.ValidatorRejects]++
		t.Run(fixture, func(t *testing.T) {
			_, err := LoadRelationIndex(serveRelationFixture(t, fixture))
			require.NoError(t, err, "the guard does not judge this shape, so startup must succeed")

			err = validateFixtureChild(t, fixture, buildStrippedPayload())
			if verdict.ValidatorRejects {
				require.Error(t, err, "%s records validator: rejects", fixture)
			} else {
				require.NoError(t, err, "%s records validator: accepts", fixture)
			}
		})
	}
	// Both outcomes must be present, or the fragment's cost is not measured:
	// without a rejecting unjudged fixture there is no false negative on record,
	// and without an accepting one no former false positive.
	require.Positive(t, seen[true], "no unjudged fixture records validator: rejects")
	require.Positive(t, seen[false], "no unjudged fixture records validator: accepts")
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
// is: some of them, not all. Measured here over every fixture the validator
// rejects — the unjudged ones that question is about and the judged ones inside
// the fragment alike — on the post-strip payload, against the message
// expectation each fixture's verdict.txt records (#395):
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
// The judged fixtures (relation_required_root and the allOf pair) are the
// counterpart inside the analysed fragment, where guard and message agree.
//
// Both halves are pinned, not just the convenient one. If jsonschema-go starts
// naming the property under "not"/"oneOf"/"else", or stops naming it under
// dependent*, this test fails and the trade is worth re-deriving.
func TestValidatorNamesTheMissingRootUnderOnlySomeApplicators(t *testing.T) {
	// unjudgedSilent counts the case the wide gate exists for: a shape the guard
	// boots whose rejection gives a message-shape gate nothing to key on.
	namesTheRoot, unjudgedSilent := 0, 0
	for _, fixture := range listRelationFixtures(t) {
		verdict := loadRelationFixtureVerdict(t, fixture)
		if !verdict.ValidatorRejects {
			continue
		}
		if verdict.MessageNamesRoot {
			namesTheRoot++
		} else if !verdict.GuardRefuses {
			unjudgedSilent++
		}
		t.Run(fixture, func(t *testing.T) {
			err := validateFixtureChild(t, fixture, buildStrippedPayload())
			require.Error(t, err)

			if verdict.MessageNamesRoot {
				require.ErrorContains(t, err, "contactSnapshot",
					"%s records message: names-root, so a message-shape gate would fire here", fixture)
				return
			}
			require.NotContains(t, err.Error(), "contactSnapshot",
				"%s records message: omits-root, so a message-shape gate would stay silent here, "+
					"which is why the diagnosis does not use one", fixture)
		})
	}
	require.Positive(t, namesTheRoot, "no rejecting fixture records message: names-root")
	require.Positive(t, unjudgedSilent,
		"no unjudged rejecting fixture records message: omits-root, so nothing here justifies the wide gate")
}

// resolveRelationFixtureDir names a committed schema directory under testdata.
// Each one holds a parent.json plus a child.json declaring a single x-relation
// root (contactSnapshot), and differs from the others only in how — or whether —
// that root is made required. They are committed rather than written per test so
// the factory test can point Entity.SchemaDirectory at the same bytes this
// package asserts on (#318).
//
// What the guard and the validator do with a fixture is recorded in its
// verdict.txt (see relationFixtureVerdict), and that sidecar, not the name, is
// what the fixture-driven tests read. The name prefix is only a rough guide to
// the *validator's* answer — relation_required_* mostly marks a post-strip
// payload the validator rejects, relation_ok_* one it accepts — and it is not
// exact: relation_required_if_only and relation_root_ref are both accepted.
func resolveRelationFixtureDir(name string) string {
	return filepath.Join("testdata", name)
}
