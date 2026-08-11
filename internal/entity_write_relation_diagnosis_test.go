package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// buildFixtureManager builds a manager over one committed relation fixture: a
// live validator resolved from the fixture directory, and a relation index
// loaded from the same documents.
//
// The fixtures are the only schemas in the tree that require a relation root,
// which is the state the runtime diagnosis exists for — the shipped visit.json
// deliberately does not.
func buildFixtureManager(t *testing.T, fixture string) forma.EntityManager {
	t.Helper()

	registry := serveRelationFixture(t, fixture)
	validator, err := schemavalidate.New(registry, resolveRelationFixtureDir(fixture))
	require.NoError(t, err)

	config := createTestConfig()
	config.Entity.SchemaDirectory = resolveRelationFixtureDir(fixture)

	return NewEntityManager(
		transform.NewPersistentRecordTransformer(registry),
		newMockPersistentRecordRepository(), nil, registry, config, validator)
}

// createChild is the post-strip document for every relation fixture: both
// root-level required properties and nothing under the relation root.
func createChild() *forma.EntityOperation {
	return &forma.EntityOperation{
		Type:             forma.OperationCreate,
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "child"},
		Data:             map[string]any{"id": "x", "parentId": "p"},
	}
}

// TestWriteDiagnosisExplainsAStrippedRelationRoot is the completeness backstop
// for everything the startup fragment declines to judge.
//
// relation_required_double_not demands "contactSnapshot" through
// {"not":{"not":{"required":[…]}}}. The guard boots it — deciding what an
// arbitrary negation demands is the satisfiability question the fragment exists
// to avoid — so without this the operator watches every write fail with a
// message about a property the caller did send, or (as here) with library prose
// naming no property at all.
//
// The diagnosis rides forma.WithOperatorDetail: it reaches Error() and the log
// and never the published body.
func TestWriteDiagnosisExplainsAStrippedRelationRoot(t *testing.T) {
	manager := buildFixtureManager(t, "relation_required_double_not")

	_, err := manager.Create(context.Background(), createChild())

	require.Error(t, err)
	require.ErrorIs(t, err, forma.ErrInvalidInput, "the caller-facing class must not change")
	require.Contains(t, err.Error(), "contactSnapshot",
		"the operator must be told which property is removed before validation")
	require.Contains(t, err.Error(), "x-relation")
	require.True(t, forma.HasOperatorDetail(err))
}

// TestWriteDiagnosisIsWithheldFromThePublishedMessage is the constraint that
// makes the diagnosis safe to attach: the caller's body must be exactly what it
// was before.
//
// Asserted against the validator's own publication rather than against a
// remembered string, so it stays true if the library's prose changes.
func TestWriteDiagnosisIsWithheldFromThePublishedMessage(t *testing.T) {
	manager := buildFixtureManager(t, "relation_required_double_not")

	_, err := manager.Create(context.Background(), createChild())
	require.Error(t, err)

	published, ok := forma.ResolvePublicMessage(err)
	require.True(t, ok, "a schema violation still publishes a body")
	require.NotContains(t, published, "x-relation",
		"the explanation is operator-facing and must not enter the response body")

	wantPublished := validateFixtureChild(t, "relation_required_double_not",
		map[string]any{"id": "x", "parentId": "p"})
	require.Error(t, wantPublished)
	bare, ok := forma.ResolvePublicMessage(wantPublished)
	require.True(t, ok)
	require.Equal(t, bare, published,
		"the published message must be byte-identical to the undecorated validator's")
}

// TestWriteDiagnosisIsSilentWithoutRelationRoots pins the gate. A schema that
// declares no relation root strips nothing, so there is nothing to explain and
// nothing may be attached — otherwise every 4xx in the system would grow a
// paragraph about a mechanism it has no part in.
func TestWriteDiagnosisIsSilentWithoutRelationRoots(t *testing.T) {
	// relation_ok_root_ref_no_relation's child.json declares contactSnapshot with
	// a plain $ref and no x-relation marker, so nothing is a relation root.
	manager := buildFixtureManager(t, "relation_ok_root_ref_no_relation")

	_, err := manager.Create(context.Background(), &forma.EntityOperation{
		Type:             forma.OperationCreate,
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "child"},
		Data:             map[string]any{"id": 7, "parentId": "p"},
	})

	require.Error(t, err)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.False(t, forma.HasOperatorDetail(err),
		"a schema with no relation roots must produce an undecorated validation error")
}

// TestWriteDiagnosisIsSilentOnOperatorClassErrors covers the other half of the
// gate, and it is not a special case in the code: forma.WithOperatorDetail
// returns its input unchanged when that input publishes nothing
// (client_error.go), and schemavalidate.Validate returns a plain error rather
// than an ErrInvalidInput carrier for everything that is not a caller violation.
//
// So a missing resolved schema — an operator fault that must surface as a 500,
// not a 4xx — cannot pick up a diagnosis about relation roots on its way out.
func TestWriteDiagnosisIsSilentOnOperatorClassErrors(t *testing.T) {
	validator, err := schemavalidate.New(serveRelationFixture(t, "relation_required_double_not"),
		resolveRelationFixtureDir("relation_required_double_not"))
	require.NoError(t, err)

	// schemaID 9999 is registered nowhere, so Validate answers "no resolved JSON
	// schema", which is a plain error and not a violation.
	err = validateWritePayload(writeValidation{
		validator:     validator,
		schemaID:      9999,
		schemaName:    "child",
		data:          map[string]any{"id": "x"},
		relationRoots: []string{"contactSnapshot"},
		enforce:       true,
	})

	require.Error(t, err)
	require.NotErrorIs(t, err, forma.ErrInvalidInput,
		"a missing resolved schema is an operator fault, not a caller violation")
	require.False(t, forma.HasOperatorDetail(err))
	require.NotContains(t, err.Error(), "x-relation")
}

// TestWriteDiagnosisDoesNotChangeWhichWritesPass is the constraint that bounds
// the whole feature: it improves an error and nothing else.
//
// Asserted at validateWritePayload, which is the only place the decoration
// happens and the one seam all four write paths share. Both directions matter:
//
//   - relation_ok_not declares the same relation root and demands nothing of it,
//     so the post-strip document that fails under double_not still validates
//     clean here — the decoration cannot manufacture a failure, because it is
//     only reached once Validate has already answered non-nil;
//   - under report-only enforcement a violation is still absorbed. The
//     decoration joins the chain, so errors.Is must still reach
//     forma.ErrInvalidInput through it; if it did not, the enforce branch would
//     start treating a caller violation as an operator fault and fail writes
//     that used to be logged and accepted.
func TestWriteDiagnosisDoesNotChangeWhichWritesPass(t *testing.T) {
	t.Run("a passing payload stays passing", func(t *testing.T) {
		validator, schemaID := resolveFixtureValidator(t, "relation_ok_not")

		require.NoError(t, validateWritePayload(writeValidation{
			validator:     validator,
			schemaID:      schemaID,
			schemaName:    "child",
			data:          map[string]any{"id": "x", "parentId": "p"},
			relationRoots: []string{"contactSnapshot"},
			enforce:       true,
		}))
	})

	t.Run("report-only still absorbs the violation", func(t *testing.T) {
		validator, schemaID := resolveFixtureValidator(t, "relation_required_double_not")

		require.NoError(t, validateWritePayload(writeValidation{
			validator:     validator,
			schemaID:      schemaID,
			schemaName:    "child",
			data:          map[string]any{"id": "x", "parentId": "p"},
			relationRoots: []string{"contactSnapshot"},
			enforce:       false,
		}), "the decorated error must still classify as a caller violation")
	})
}

// resolveFixtureValidator builds the live validator for one fixture and returns
// it with the child schema's id.
func resolveFixtureValidator(t *testing.T, fixture string) (*schemavalidate.Validator, int16) {
	t.Helper()
	registry := serveRelationFixture(t, fixture)
	validator, err := schemavalidate.New(registry, resolveRelationFixtureDir(fixture))
	require.NoError(t, err)
	childID, _, err := registry.GetSchemaByName("child")
	require.NoError(t, err)
	return validator, childID
}

// TestExplainStrippedRelationRootsNamesEveryRootOnce pins the message's shape
// directly, away from the write path: every relation root the schema declares is
// named, in a stable order, because an operator reading a log line needs the
// list to be diffable across occurrences.
func TestExplainStrippedRelationRootsNamesEveryRootOnce(t *testing.T) {
	base := forma.InvalidInputf("schema validation failed: something")

	decorated := explainStrippedRelationRoots(base, "child", []string{"alpha", "beta"})

	require.NotSame(t, base, decorated)
	require.Contains(t, decorated.Error(), `["alpha" "beta"]`)
	require.Contains(t, decorated.Error(), "child")

	require.Same(t, base, explainStrippedRelationRoots(base, "child", nil),
		"no relation roots means no decoration at all")

	plain := errors.New("not a caller violation")
	require.Same(t, plain, explainStrippedRelationRoots(plain, "child", []string{"alpha"}))
}

// TestRelationRootNamesAreSortedAndNilSafe pins the input the diagnosis is built
// from. Sorted because RelationIndex stores relations in map-iteration order,
// which is randomised per run; nil-safe because a manager built without a
// registry has no index at all.
func TestRelationRootNamesAreSortedAndNilSafe(t *testing.T) {
	idx, err := LoadRelationIndex(serveRelationFixture(t, "relation_required_double_not"))
	require.NoError(t, err)

	require.Equal(t, []string{"contactSnapshot"}, idx.RelationRootNames("child"))
	require.Nil(t, idx.RelationRootNames("parent"))

	var nilIdx *RelationIndex
	require.Nil(t, nilIdx.RelationRootNames("child"))
}
