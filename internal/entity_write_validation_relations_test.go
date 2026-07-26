package internal

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// shippedSchemaDir is the directory the server actually ships. These tests run
// against it rather than a stub because the seam under test only exists where a
// schema declares x-relation, and visit.json is the only one that does.
const shippedSchemaDir = "../cmd/server/schemas"

// newShippedSchemaHarness builds a manager over the real shipped schemas, with a
// live validator and the relation index loaded from the same directory.
func newShippedSchemaHarness(t *testing.T) (forma.EntityManager, *writeSpy) {
	t.Helper()

	registry, err := schemameta.NewFileSchemaRegistryFromDirectory(shippedSchemaDir)
	require.NoError(t, err)

	validator, err := schemavalidate.New(registry, shippedSchemaDir)
	require.NoError(t, err)

	config := createTestConfig()
	config.Entity.SchemaDirectory = shippedSchemaDir

	spy := &writeSpy{inner: transform.NewPersistentRecordTransformer(registry)}
	manager := NewEntityManager(spy, newMockPersistentRecordRepository(), nil, registry, config, validator)
	return manager, spy
}

// validVisit is the minimum visit.json accepts: every root-level required
// property, each satisfying its own declared constraints.
func validVisit() map[string]any {
	return map[string]any{
		"id":               "11111111-1111-7111-8111-111111111111",
		"leadId":           "22222222-2222-7222-8222-222222222222",
		"userId":           "agent-1",
		"propertyId":       "property-1",
		"scheduledStartAt": "2026-07-25T10:00:00Z",
		"status":           "scheduled",
	}
}

// TestCreateAcceptsDottedKeyUnderRelationRoot pins the relation-strip seam.
//
// StripComputedFields removes a relation root by *exact* key, so the registered
// attribute contactSnapshot.name survives the strip. Expanding it afterwards
// rebuilds the very object that was just removed — as a partial one — and
// visit.json resolves contactSnapshot to lead.json#/properties/contact, which
// requires isAnonymous. The result was a 400 on a payload that was accepted and
// persisted before #314, and unfixably so: sending the whole nested object does
// not help, because the strip removes it and the dotted key rebuilds it.
//
// The value must still be persisted exactly as before. It is simply not
// schema-validated, the same shape as the documented array gap.
func TestCreateAcceptsDottedKeyUnderRelationRoot(t *testing.T) {
	manager, spy := newShippedSchemaHarness(t)

	data := validVisit()
	data["contactSnapshot.name"] = "Ada"

	_, err := manager.Create(context.Background(), createVisitOp(data))
	require.NoError(t, err, "a dotted key under a stripped relation root must not be rebuilt into a partial object")

	require.Len(t, spy.seen, 1)
	require.Contains(t, spy.seen[0].keys, "contactSnapshot.name",
		"the value must keep reaching storage under the caller's own spelling")
}

// TestCreateStillValidatesDottedKeyOutsideRelationRoot is the other half of the
// skip: it must not widen into "dotted keys are not validated". propertySnapshot
// is an ordinary object on visit.json, not a relation root, and its price
// declares minimum 0.
func TestCreateStillValidatesDottedKeyOutsideRelationRoot(t *testing.T) {
	manager, _ := newShippedSchemaHarness(t)

	data := validVisit()
	data["propertySnapshot.price"] = -1

	_, err := manager.Create(context.Background(), createVisitOp(data))

	require.ErrorIs(t, err, forma.ErrInvalidInput,
		"a dotted key that is not under a relation root must still be expanded and validated")
}

func createVisitOp(data map[string]any) *forma.EntityOperation {
	return &forma.EntityOperation{
		Type:             forma.OperationCreate,
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit"},
		Data:             data,
	}
}
