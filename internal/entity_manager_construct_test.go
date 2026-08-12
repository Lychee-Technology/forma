package internal

import (
	"errors"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// mustNewEntityManager builds a manager and fails the test if construction
// fails. It exists because NewEntityManager fails closed on a relation-index
// load (#388) while most tests in this package are about what a built manager
// does, not about building one — so they assert construction succeeded and move
// on. t.Helper keeps a failure attributed to the caller's line.
//
// Tests that are about construction itself do not use this: they call
// NewEntityManager directly and assert on both return values.
func mustNewEntityManager(
	t *testing.T,
	transformer model.PersistentRecordTransformer,
	repository model.PersistentRecordRepository,
	federatedQueryEngine model.FederatedQueryEngine,
	registry forma.SchemaRegistry,
	config *forma.Config,
	validator *schemavalidate.Validator,
	opts ...EntityManagerOption,
) forma.EntityManager {
	t.Helper()
	manager, err := NewEntityManager(
		transformer, repository, federatedQueryEngine, registry, config, validator, opts...)
	require.NoError(t, err, "build entity manager")
	return manager
}

// TestNewEntityManagerFailsWhenTheRelationGuardRefusesTheRegistry is the
// fail-closed pin for the direct-constructor path (#388).
//
// relation_required_root's child lists its x-relation root in root-level
// "required", so the relation guard refuses it: the root is stripped from every
// payload before the validator sees it, and the entity would be unwritable for
// the process lifetime. Before this, a direct constructor logged a warning and
// carried on with no index — which does not merely lose the guard's verdict, it
// disables stripping for every schema, so the relation subtree becomes
// caller-writable again.
//
// The manager must be nil as well as the error non-nil: a caller that ignores
// the error would otherwise hold a manager in exactly the state being refused.
func TestNewEntityManagerFailsWhenTheRelationGuardRefusesTheRegistry(t *testing.T) {
	registry := serveRelationFixture(t, "relation_required_root")

	manager, err := NewEntityManager(
		transform.NewPersistentRecordTransformer(registry), newMockPersistentRecordRepository(), nil,
		registry, createTestConfig(), nil)

	require.Error(t, err)
	require.Nil(t, manager)
	require.ErrorContains(t, err, "child", "the refusal must name the offending schema")
	require.ErrorContains(t, err, "contactSnapshot", "and the relation root it requires")
}

// TestNewEntityManagerFailsWhenTheRegistryCannotServeADocument is the other
// abort cause reaching the same place: a registry that lists a name it cannot
// then serve. The cause is preserved, so a caller can match on it.
func TestNewEntityManagerFailsWhenTheRegistryCannotServeADocument(t *testing.T) {
	registry := serveRelationFixture(t, "relation_ok_not")
	cause := errors.New("backing store unavailable")
	registry.docErr["child"] = cause

	manager, err := NewEntityManager(
		transform.NewPersistentRecordTransformer(registry), newMockPersistentRecordRepository(), nil,
		registry, createTestConfig(), nil)

	require.Error(t, err)
	require.Nil(t, manager)
	require.ErrorIs(t, err, cause, "the registry's own failure keeps its identity through the wrap")
}

// TestNewEntityManagerHoldsANonNilRelationIndex pins the invariant the error
// return buys: a manager that was built at all strips with a real index.
//
// Both halves matter. Over a registry that declares a relation the index carries
// it, so stripping is actually configured; over a nil registry — no schemas at
// all, so no relation can exist — the load answers an empty index rather than
// nil, which is why an absent registry is not an error and still leaves nothing
// nil to dereference.
func TestNewEntityManagerHoldsANonNilRelationIndex(t *testing.T) {
	registry := serveRelationFixture(t, "relation_ok_not")

	manager, err := NewEntityManager(
		transform.NewPersistentRecordTransformer(registry), newMockPersistentRecordRepository(), nil,
		registry, createTestConfig(), nil)
	require.NoError(t, err)
	require.NotNil(t, manager.(*entityManager).relations)
	require.Len(t, manager.(*entityManager).relations.Relations("child"), 1)

	absent, err := NewEntityManager(nil, nil, nil, nil, forma.DefaultConfig(nil), nil)
	require.NoError(t, err, "a registry that serves nothing declares no relations, so there is nothing to refuse")
	require.NotNil(t, absent.(*entityManager).relations)
}
