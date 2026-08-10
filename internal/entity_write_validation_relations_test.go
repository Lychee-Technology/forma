package internal

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// shippedSchemaDir is the directory the server actually ships. These tests run
// against it rather than a stub because the seam under test only exists where a
// schema declares x-relation, and visit.json is the only one that does.
const shippedSchemaDir = "../cmd/server/schemas"

// shippedHarness is one manager built over the real shipped schemas, plus the
// seams the relation tests need: the spy that records what reaches the writer,
// the repository to seed a pre-existing row, and the *inner* transformer to
// build that row with — seeding through the spy would pollute spy.seen.
type shippedHarness struct {
	manager       forma.EntityManager
	spy           *writeSpy
	repo          *mockPersistentRecordRepository
	transformer   model.PersistentRecordTransformer
	visitSchemaID int16
}

// newShippedSchemaHarness builds a manager over the real shipped schemas, with a
// live validator and the relation index loaded from the same directory.
func newShippedSchemaHarness(t *testing.T) shippedHarness {
	t.Helper()

	registry, err := schemameta.NewFileSchemaRegistryFromDirectory(shippedSchemaDir)
	require.NoError(t, err)

	validator, err := schemavalidate.New(registry, shippedSchemaDir)
	require.NoError(t, err)

	visitSchemaID, _, err := registry.GetSchemaByName("visit")
	require.NoError(t, err)

	config := createTestConfig()
	config.Entity.SchemaDirectory = shippedSchemaDir

	inner := transform.NewPersistentRecordTransformer(registry)
	spy := &writeSpy{inner: inner}
	repo := newMockPersistentRecordRepository()

	return shippedHarness{
		manager:       NewEntityManager(spy, repo, nil, registry, config, validator),
		spy:           spy,
		repo:          repo,
		transformer:   inner,
		visitSchemaID: visitSchemaID,
	}
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

// TestCreateDropsDottedKeyBeneathRelationRoot pins the #318 rule: nothing at or
// beneath an x-relation property is caller-writable, in either spelling.
//
// Before #318 StripComputedFields matched by exact key, so the nested spelling
// {"contactSnapshot": {...}} was discarded while the registered dotted
// descendant contactSnapshot.name survived and was persisted. That value was
// never readable — relation enrichment replaces the whole contactSnapshot object
// with the parent's fragment — and the next update deleted it, because the
// update path rebuilds the dotted name into a nested object that the strip then
// removes.
//
// Dropping stays silent. The nested spelling has always been dropped without a
// rejection; the dotted spelling merely joins it, so no payload accepted before
// #318 starts failing.
func TestCreateDropsDottedKeyBeneathRelationRoot(t *testing.T) {
	h := newShippedSchemaHarness(t)

	data := validVisit()
	data["contactSnapshot.name"] = "Ada"

	_, err := h.manager.Create(context.Background(), createVisitOp(data))
	require.NoError(t, err, "a dotted key beneath a relation root is dropped, not rejected")

	require.Len(t, h.spy.seen, 1)
	require.NotContains(t, h.spy.seen[0].keys, "contactSnapshot.name",
		"the relation subtree must not reach storage under any spelling")
}

// TestCreateStillValidatesDottedKeyOutsideRelationRoot is the other half of the
// drop: it must not widen into "dotted keys are not validated". propertySnapshot
// is an ordinary object on visit.json, not a relation root, and its price
// declares minimum 0.
func TestCreateStillValidatesDottedKeyOutsideRelationRoot(t *testing.T) {
	h := newShippedSchemaHarness(t)

	data := validVisit()
	data["propertySnapshot.price"] = -1

	_, err := h.manager.Create(context.Background(), createVisitOp(data))

	require.ErrorIs(t, err, forma.ErrInvalidInput,
		"a dotted key that is not under a relation root must still be expanded and validated")
}

// TestUpdateDropsExistingRelationSubtree covers the rows that already hold a
// caller-written contactSnapshot.* value from before #318.
//
// The update path rebuilds the dotted attribute name into a nested object
// (FromPersistentRecord → FromAttributes), so the strip removes it from the
// merged document and the scoped EAV replace does not write it back. Re-sending
// the dotted spelling in the update does not bring it back either.
func TestUpdateDropsExistingRelationSubtree(t *testing.T) {
	h := newShippedSchemaHarness(t)

	seed := validVisit()
	seed["contactSnapshot.name"] = "Ada"
	rowID := uuid.MustParse(seed["id"].(string))
	h.repo.storeRecord(buildPersistentRecord(t, h.transformer, h.visitSchemaID, rowID, seed))

	_, err := h.manager.Update(context.Background(), &forma.EntityOperation{
		Type:             forma.OperationUpdate,
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit", RowID: rowID},
		Updates:          map[string]any{"feedback": "ok", "contactSnapshot.name": "Grace"},
	})
	require.NoError(t, err)

	require.Len(t, h.spy.seen, 1)
	require.NotContains(t, h.spy.seen[0].keys, "contactSnapshot.name")
	require.NotContains(t, h.spy.seen[0].keys, "contactSnapshot")
	require.Contains(t, h.spy.seen[0].keys, "feedback", "the rest of the update still lands")
}

// TestBatchCreateAtomicDropsRelationSubtree and its update twin cover the batch
// service's own strip sites. #314's lesson: a write-path guard that is only
// tested through crud.Create leaves BatchCreate unguarded. The best-effort batch
// paths delegate to crud.Create/Update and are covered by those tests; only the
// atomic paths strip on their own.
func TestBatchCreateAtomicDropsRelationSubtree(t *testing.T) {
	h := newShippedSchemaHarness(t)

	data := validVisit()
	data["contactSnapshot.name"] = "Ada"

	_, err := h.manager.BatchCreate(context.Background(), &forma.BatchOperation{
		Atomic: true,
		Operations: []forma.EntityOperation{{
			Type:             forma.OperationCreate,
			EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit"},
			Data:             data,
		}},
	})
	require.NoError(t, err)

	require.Len(t, h.spy.seen, 1)
	require.NotContains(t, h.spy.seen[0].keys, "contactSnapshot.name")
}

// TestBatchUpdateAtomicDropsRelationSubtree is the update half named above: it
// pins that the atomic batch update strips the pre-existing dotted value the row
// already holds, not only the one the caller just sent.
func TestBatchUpdateAtomicDropsRelationSubtree(t *testing.T) {
	h := newShippedSchemaHarness(t)

	seed := validVisit()
	seed["contactSnapshot.name"] = "Ada"
	rowID := uuid.MustParse(seed["id"].(string))
	h.repo.storeRecord(buildPersistentRecord(t, h.transformer, h.visitSchemaID, rowID, seed))

	_, err := h.manager.BatchUpdate(context.Background(), &forma.BatchOperation{
		Atomic: true,
		Operations: []forma.EntityOperation{{
			Type:             forma.OperationUpdate,
			EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit", RowID: rowID},
			Updates:          map[string]any{"feedback": "ok", "contactSnapshot.name": "Grace"},
		}},
	})
	require.NoError(t, err)

	require.Len(t, h.spy.seen, 1)
	require.NotContains(t, h.spy.seen[0].keys, "contactSnapshot.name")
	require.NotContains(t, h.spy.seen[0].keys, "contactSnapshot")
}

func createVisitOp(data map[string]any) *forma.EntityOperation {
	return &forma.EntityOperation{
		Type:             forma.OperationCreate,
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit"},
		Data:             data,
	}
}
