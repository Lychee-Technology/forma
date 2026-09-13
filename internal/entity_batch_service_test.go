package internal

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/transform"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

func TestEntityManager_BatchCreate_CollectsErrors(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)

	req := &forma.BatchOperation{
		Operations: []forma.EntityOperation{
			{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit"},
				Type:             forma.OperationCreate,
				Data:             visitPayload("visit-batch-1"),
			},
			{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: "missing"},
				Type:             forma.OperationCreate,
				Data:             visitPayload("visit-batch-2"),
			},
		},
	}

	result, err := em.BatchCreate(ctx, req)
	if err != nil {
		t.Fatalf("BatchCreate failed: %v", err)
	}

	if len(result.Successful) != 1 {
		t.Fatalf("expected 1 successful, got %d", len(result.Successful))
	}
	if len(result.Failed) != 1 {
		t.Fatalf("expected 1 failed, got %d", len(result.Failed))
	}
	if result.Failed[0].Code != "CREATE_FAILED" {
		t.Fatalf("expected CREATE_FAILED code, got %s", result.Failed[0].Code)
	}
}

func TestEntityManager_BatchUpdate_CollectsErrors(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()

	schemaID, _, err := registry.GetSchemaAttributeCacheByName("visit")
	if err != nil {
		t.Fatalf("failed to get schema metadata: %v", err)
	}

	rowID := uuid.New()
	mockRepo.storeRecord(buildPersistentRecord(t, transformer, schemaID, rowID, visitPayload("visit-batch-update-1")))

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)

	req := &forma.BatchOperation{
		Operations: []forma.EntityOperation{
			{
				EntityIdentifier: forma.EntityIdentifier{
					SchemaName: "visit",
					RowID:      rowID,
				},
				Type: forma.OperationUpdate,
				Updates: map[string]any{
					"status": "visited",
				},
			},
			{
				EntityIdentifier: forma.EntityIdentifier{
					SchemaName: "visit",
				},
				Type:    forma.OperationUpdate,
				Updates: map[string]any{"status": "failed"},
			},
		},
	}

	result, err := em.BatchUpdate(ctx, req)
	if err != nil {
		t.Fatalf("BatchUpdate failed: %v", err)
	}

	if len(result.Successful) != 1 {
		t.Fatalf("expected 1 successful, got %d", len(result.Successful))
	}
	if len(result.Failed) != 1 {
		t.Fatalf("expected 1 failed, got %d", len(result.Failed))
	}
	if result.Failed[0].Code != "UPDATE_FAILED" {
		t.Fatalf("expected UPDATE_FAILED code, got %s", result.Failed[0].Code)
	}
}

func TestEntityManager_BatchDelete_CollectsErrors(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()

	schemaID, _, err := registry.GetSchemaAttributeCacheByName("visit")
	if err != nil {
		t.Fatalf("failed to get schema metadata: %v", err)
	}

	rowID := uuid.New()
	mockRepo.storeRecord(buildPersistentRecord(t, transformer, schemaID, rowID, visitPayload("visit-batch-delete-1")))

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)

	req := &forma.BatchOperation{
		Operations: []forma.EntityOperation{
			{
				EntityIdentifier: forma.EntityIdentifier{
					SchemaName: "visit",
					RowID:      rowID,
				},
				Type: forma.OperationDelete,
			},
			{
				EntityIdentifier: forma.EntityIdentifier{
					SchemaName: "visit",
				},
				Type: forma.OperationDelete,
			},
		},
	}

	result, err := em.BatchDelete(ctx, req)
	if err != nil {
		t.Fatalf("BatchDelete failed: %v", err)
	}

	if len(result.Successful) != 1 {
		t.Fatalf("expected 1 successful, got %d", len(result.Successful))
	}
	if len(result.Failed) != 1 {
		t.Fatalf("expected 1 failed, got %d", len(result.Failed))
	}
	if result.Failed[0].Code != "DELETE_FAILED" {
		t.Fatalf("expected DELETE_FAILED code, got %s", result.Failed[0].Code)
	}
	if _, exists := mockRepo.records[schemaID][rowID]; exists {
		t.Fatalf("expected record to be deleted")
	}
}

func TestEntityManager_BatchCreate_AtomicAllOrNothingOnRepositoryFailure(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()
	mockRepo.atomicInsertFailAt = 2
	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)

	req := &forma.BatchOperation{
		Atomic: true,
		Operations: []forma.EntityOperation{
			{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit"},
				Type:             forma.OperationCreate,
				Data:             visitPayload("visit-atomic-create-1"),
			},
			{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit"},
				Type:             forma.OperationCreate,
				Data:             visitPayload("visit-atomic-create-2"),
			},
		},
	}

	_, err = em.BatchCreate(ctx, req)
	if err == nil {
		t.Fatalf("expected batch create to fail")
	}

	schemaID, _, err := registry.GetSchemaAttributeCacheByName("visit")
	if err != nil {
		t.Fatalf("failed to get schema id: %v", err)
	}
	if len(mockRepo.records[schemaID]) != 0 {
		t.Fatalf("expected no records persisted on atomic failure")
	}
}

func TestEntityManager_BatchCreate_AtomicSuccess(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()
	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)

	req := &forma.BatchOperation{
		Atomic: true,
		Operations: []forma.EntityOperation{
			{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit"},
				Type:             forma.OperationCreate,
				Data:             visitPayload("visit-atomic-success-1"),
			},
			{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit"},
				Type:             forma.OperationCreate,
				Data:             visitPayload("visit-atomic-success-2"),
			},
		},
	}

	result, err := em.BatchCreate(ctx, req)
	if err != nil {
		t.Fatalf("batch create failed: %v", err)
	}
	if len(result.Successful) != 2 {
		t.Fatalf("expected 2 successful records, got %d", len(result.Successful))
	}
	if len(result.Failed) != 0 {
		t.Fatalf("expected 0 failed records, got %d", len(result.Failed))
	}
}

func TestEntityManager_BatchUpdate_AtomicAllOrNothingOnRepositoryFailure(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()
	mockRepo.atomicUpdateFailAt = 2

	schemaID, _, err := registry.GetSchemaAttributeCacheByName("visit")
	if err != nil {
		t.Fatalf("failed to get schema metadata: %v", err)
	}

	rowID1 := uuid.New()
	rowID2 := uuid.New()
	mockRepo.storeRecord(buildPersistentRecord(t, transformer, schemaID, rowID1, visitPayload("visit-atomic-update-1")))
	mockRepo.storeRecord(buildPersistentRecord(t, transformer, schemaID, rowID2, visitPayload("visit-atomic-update-2")))

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)

	req := &forma.BatchOperation{
		Atomic: true,
		Operations: []forma.EntityOperation{
			{
				EntityIdentifier: forma.EntityIdentifier{
					SchemaName: "visit",
					RowID:      rowID1,
				},
				Type:    forma.OperationUpdate,
				Updates: map[string]any{"status": "visited"},
			},
			{
				EntityIdentifier: forma.EntityIdentifier{
					SchemaName: "visit",
					RowID:      rowID2,
				},
				Type:    forma.OperationUpdate,
				Updates: map[string]any{"status": "cancelled"},
			},
		},
	}

	_, err = em.BatchUpdate(ctx, req)
	if err == nil {
		t.Fatalf("expected batch update to fail")
	}

	record1 := mockRepo.records[schemaID][rowID1]
	attrs1, err := transformer.FromPersistentRecord(ctx, record1)
	if err != nil {
		t.Fatalf("failed to convert record1: %v", err)
	}
	record2 := mockRepo.records[schemaID][rowID2]
	attrs2, err := transformer.FromPersistentRecord(ctx, record2)
	if err != nil {
		t.Fatalf("failed to convert record2: %v", err)
	}

	if attrs1["status"] != "scheduled" {
		t.Fatalf("expected row1 status unchanged, got %v", attrs1["status"])
	}
	if attrs2["status"] != "scheduled" {
		t.Fatalf("expected row2 status unchanged, got %v", attrs2["status"])
	}
}

func TestEntityManager_BatchDelete_AtomicAllOrNothingOnRepositoryFailure(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()
	mockRepo.atomicDeleteFailAt = 2

	schemaID, _, err := registry.GetSchemaAttributeCacheByName("visit")
	if err != nil {
		t.Fatalf("failed to get schema metadata: %v", err)
	}

	rowID1 := uuid.New()
	rowID2 := uuid.New()
	mockRepo.storeRecord(buildPersistentRecord(t, transformer, schemaID, rowID1, visitPayload("visit-atomic-delete-1")))
	mockRepo.storeRecord(buildPersistentRecord(t, transformer, schemaID, rowID2, visitPayload("visit-atomic-delete-2")))

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)

	req := &forma.BatchOperation{
		Atomic: true,
		Operations: []forma.EntityOperation{
			{
				EntityIdentifier: forma.EntityIdentifier{
					SchemaName: "visit",
					RowID:      rowID1,
				},
				Type: forma.OperationDelete,
			},
			{
				EntityIdentifier: forma.EntityIdentifier{
					SchemaName: "visit",
					RowID:      rowID2,
				},
				Type: forma.OperationDelete,
			},
		},
	}

	_, err = em.BatchDelete(ctx, req)
	if err == nil {
		t.Fatalf("expected batch delete to fail")
	}

	if _, exists := mockRepo.records[schemaID][rowID1]; !exists {
		t.Fatalf("expected row1 to remain after rollback")
	}
	if _, exists := mockRepo.records[schemaID][rowID2]; !exists {
		t.Fatalf("expected row2 to remain after rollback")
	}
}

// duplicateRowUpdateOps builds two update operations on the same visit row that
// touch disjoint attributes, so a correct merge keeps both and a lost update is
// observable.
func duplicateRowUpdateOps(rowID uuid.UUID) []forma.EntityOperation {
	return []forma.EntityOperation{
		{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit", RowID: rowID},
			Type:             forma.OperationUpdate,
			Updates:          map[string]any{"status": "visited"},
		},
		{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit", RowID: rowID},
			Type:             forma.OperationUpdate,
			Updates:          map[string]any{"feedback": "great visit"},
		},
	}
}

// seedVisitRow stores one visit row in a fresh mock repository and returns the
// wired entity manager together with the pieces a test needs to inspect it.
func seedVisitRow(t *testing.T, id string) (forma.EntityManager, *mockPersistentRecordRepository, model.PersistentRecordTransformer, int16, uuid.UUID) {
	t.Helper()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()

	schemaID, _, err := registry.GetSchemaAttributeCacheByName("visit")
	if err != nil {
		t.Fatalf("failed to get schema metadata: %v", err)
	}

	rowID := uuid.New()
	mockRepo.storeRecord(buildPersistentRecord(t, transformer, schemaID, rowID, visitPayload(id)))

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, createTestConfig(), nil)
	return em, mockRepo, transformer, schemaID, rowID
}

func TestEntityManager_BatchUpdate_AtomicRejectsDuplicateRowID(t *testing.T) {
	ctx := context.Background()
	em, mockRepo, transformer, schemaID, rowID := seedVisitRow(t, "visit-atomic-dup")

	result, err := em.BatchUpdate(ctx, &forma.BatchOperation{
		Atomic:     true,
		Operations: duplicateRowUpdateOps(rowID),
	})
	if err == nil {
		t.Fatalf("expected atomic batch with a duplicate row id to be rejected, got result %+v", result)
	}
	if !errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput, got %v", err)
	}
	msg, ok := forma.ResolvePublicMessage(err)
	if !ok {
		t.Fatalf("expected a published message, got %v", err)
	}
	for _, want := range []string{"operation[1]", "operation[0]", rowID.String(), "visit"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("expected published message to mention %q, got %q", want, msg)
		}
	}

	attrs, err := transformer.FromPersistentRecord(ctx, mockRepo.records[schemaID][rowID])
	if err != nil {
		t.Fatalf("failed to convert stored record: %v", err)
	}
	if attrs["status"] != "scheduled" {
		t.Fatalf("expected stored status unchanged, got %v", attrs["status"])
	}
	if _, exists := attrs["feedback"]; exists {
		t.Fatalf("expected no feedback stored, got %v", attrs["feedback"])
	}
}

func TestEntityManager_BatchUpdate_BestEffortAppliesDuplicateRowIDInOrder(t *testing.T) {
	ctx := context.Background()
	em, mockRepo, transformer, schemaID, rowID := seedVisitRow(t, "visit-best-effort-dup")

	result, err := em.BatchUpdate(ctx, &forma.BatchOperation{
		Atomic:     false,
		Operations: duplicateRowUpdateOps(rowID),
	})
	if err != nil {
		t.Fatalf("best-effort batch update failed: %v", err)
	}
	if len(result.Successful) != 2 || len(result.Failed) != 0 {
		t.Fatalf("expected both operations to succeed, got successful=%d failed=%d", len(result.Successful), len(result.Failed))
	}

	attrs, err := transformer.FromPersistentRecord(ctx, mockRepo.records[schemaID][rowID])
	if err != nil {
		t.Fatalf("failed to convert stored record: %v", err)
	}
	if attrs["status"] != "visited" {
		t.Fatalf("expected the first operation's status to survive, got %v", attrs["status"])
	}
	if attrs["feedback"] != "great visit" {
		t.Fatalf("expected the second operation's feedback to be applied, got %v", attrs["feedback"])
	}
}
