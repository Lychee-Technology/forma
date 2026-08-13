package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/lychee-technology/forma/internal/redact"
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

	return mustNewEntityManager(t,
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
	// The whole story in one place: this schema really does boot, so the failing
	// write below is what an operator meets rather than a hypothetical.
	_, bootErr := LoadRelationIndex(serveRelationFixture(t, "relation_required_double_not"))
	require.NoError(t, bootErr)

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

// TestBatchResultPublishesOnlyThePublishedMessage is the leak guard for the one
// write surface that has no HTTP boundary in front of it.
//
// forma.OperationError.Error is an exported, JSON-serialised field
// (types.go), and a best-effort batch is the only path that fills it: the
// atomic paths return the error itself, and internal/httpapi resolves the
// published message for every single-operation response. So the batch result is
// where the operator-only explanation this branch attaches would be published
// verbatim, x-relation and all.
//
// Driven through the real fixture rather than a hand-built error, so it fails
// for the reason the reviewer's reproduction failed: relation_required_double_not
// really does boot, really does strip the root, and really does carry the
// diagnosis by the time the batch records it.
func TestBatchResultPublishesOnlyThePublishedMessage(t *testing.T) {
	manager := buildFixtureManager(t, "relation_required_double_not")
	op := createChild()

	result, err := manager.BatchCreate(context.Background(),
		&forma.BatchOperation{Operations: []forma.EntityOperation{*op}})

	require.NoError(t, err, "a best-effort batch reports per-operation failures in its result")
	require.Len(t, result.Failed, 1)
	require.NotContains(t, result.Failed[0].Error, "x-relation",
		"the operator-only explanation must not reach an exported result field")
	require.NotContains(t, result.Failed[0].Error, "#318")

	// And it carries exactly what the single-operation path would publish, so the
	// two surfaces answer the same body for the same failure.
	//
	// Scrubbed on both sides: resolveBatchErrorMessage passes the publication
	// through redact.ConnStringPassword, exactly as internal/httpapi does before
	// writing one. This fixture's message holds no credential, so the two forms
	// are equal here — comparing against the scrubbed form is what keeps the
	// assertion true by construction rather than by what the fixture happens to
	// contain, and stops a future credential-carrying fixture turning this pin
	// into a contradiction of the scrub.
	_, singleErr := manager.Create(context.Background(), createChild())
	require.Error(t, singleErr)
	published, ok := forma.ResolvePublicMessage(singleErr)
	require.True(t, ok)
	require.Equal(t, redact.ConnStringPassword(published), result.Failed[0].Error)
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
// than an ErrInvalidInput carrier for everything that is not caller input.
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

	plain := errors.New("not a caller violation")
	require.Same(t, plain, explainStrippedRelationRoots(plain, "child", []string{"alpha"}),
		"a non-publishing error is returned unchanged, and that is forma.WithOperatorDetail's decision")
}

// TestWriteValidationAttachesNoDiagnosisWithoutRelationRoots pins the gate where
// it lives: at the call site, not inside the decorator.
//
// A schema that declares no relation root strips nothing, so there is nothing to
// explain and nothing may be attached — otherwise every validation error in the
// system would grow a paragraph about a mechanism it has no part in. The
// assertion is textual identity with the validator's own verdict under
// validateWritePayload's own wrap: any decoration at all would show up as extra
// text, which is the same property the old require.Same on the decorator
// asserted by reference.
//
// relation_required_double_not is the same fixture the decorated case uses, so
// the two differ only in whether the root list is supplied.
func TestWriteValidationAttachesNoDiagnosisWithoutRelationRoots(t *testing.T) {
	validator, schemaID := resolveFixtureValidator(t, "relation_required_double_not")
	payload := map[string]any{"id": "x", "parentId": "p"}

	// Validated over the same document validateWritePayload builds, not over the
	// raw payload: it normalizes dotted keys first (entity_write_validation.go),
	// and the assertion below is textual. This payload has no dotted key, so the
	// two documents are equal today — normalizing here is what stops that
	// coincidence being load-bearing.
	bare := validator.Validate(schemaID,
		transform.NormalizeDottedKeys(payload, nil, validator.ArrayPaths(schemaID)))
	require.Error(t, bare, "the fixture must fail validation for this to say anything")

	err := validateWritePayload(writeValidation{
		validator:  validator,
		schemaID:   schemaID,
		schemaName: "child",
		data:       payload,
		enforce:    true,
	})

	require.Error(t, err)
	require.False(t, forma.HasOperatorDetail(err))
	require.Equal(t,
		fmt.Sprintf("failed to validate payload against schema %d: %s", schemaID, bare.Error()),
		err.Error(),
		"with no relation roots the error carries the validator's verdict and the caller's wrap, and nothing else")
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

// TestRelationRootMemoAnswersLikeTheIndex pins the batch memo against the thing
// it caches. A memo that drifts from RelationIndex.RelationRootNames would feed
// the diagnosis a stale or wrong root list for every operation after the first.
func TestRelationRootMemoAnswersLikeTheIndex(t *testing.T) {
	idx, err := LoadRelationIndex(serveRelationFixture(t, "relation_required_double_not"))
	require.NoError(t, err)

	memo := newRelationRootMemo(idx)
	for range 3 {
		require.Equal(t, idx.RelationRootNames("child"), memo.resolve("child"))
		require.Equal(t, idx.RelationRootNames("parent"), memo.resolve("parent"))
	}

	// A manager built without a registry has no index at all, and the memo must
	// survive that rather than being guarded at every call site.
	var nilIdx *RelationIndex
	require.Nil(t, newRelationRootMemo(nilIdx).resolve("child"))
}

// TestRelationRootMemoLooksUpEachSchemaOnce is the half the answers cannot show:
// a cache hit and a recomputation return the same list, so only counting the
// lookups distinguishes them.
//
// The two schemas pin different things, and only one of them is about cost:
//
//   - "child" declares a relation root, so RelationRootNames allocates and sorts
//     on every call. Looking it up once per batch instead of once per operation
//     is the saving the memo exists for.
//   - "parent" declares none and answers nil. Re-deriving that costs one map
//     lookup, the same as the cache probe, so caching it saves nothing. It is
//     asserted because presence-keying is a deliberate uniformity choice — every
//     schema resolved once, whatever it answers — and a memo keyed on "is the
//     cached value non-nil" would quietly drop the nil half of that rule.
func TestRelationRootMemoLooksUpEachSchemaOnce(t *testing.T) {
	calls := map[string]int{}
	memo := &relationRootMemo{
		byName: make(map[string][]string),
		lookup: func(schema string) []string {
			calls[schema]++
			if schema == "child" {
				return []string{"contactSnapshot"}
			}
			return nil
		},
	}

	for range 5 {
		require.Equal(t, []string{"contactSnapshot"}, memo.resolve("child"))
		require.Nil(t, memo.resolve("parent"))
	}

	require.Equal(t, 1, calls["child"])
	require.Equal(t, 1, calls["parent"],
		"a nil answer must be cached as computed, or every operation re-runs the lookup")
}
