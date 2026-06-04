package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

func TestEntityManager_Update_MergesAndPreserves(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()

	schemaID, _, err := registry.GetSchemaAttributeCacheByName("visit")
	if err != nil {
		t.Fatalf("failed to get schema metadata: %v", err)
	}

	rowID := uuid.New()
	existing := map[string]any{
		"id":               "visit-update-1",
		"leadId":           "lead-1",
		"userId":           "user-1",
		"propertyId":       "property-1",
		"scheduledStartAt": "2024-01-01T00:00:00Z",
		"status":           "scheduled",
		"feedback":         "initial",
	}
	existingRecord := buildPersistentRecord(t, transformer, schemaID, rowID, existing)
	existingRecord.CreatedAt = 111
	deleted := int64(222)
	existingRecord.DeletedAt = &deleted
	mockRepo.storeRecord(existingRecord)

	em := NewEntityManager(transformer, mockRepo, registry, config)

	req := &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{
			SchemaName: "visit",
			RowID:      rowID,
		},
		Type: forma.OperationUpdate,
		Updates: map[string]any{
			"status":   "visited",
			"feedback": "updated",
		},
	}

	record, err := em.Update(ctx, req)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	if record.Attributes["status"] != "visited" {
		t.Fatalf("expected status to be updated, got %v", record.Attributes["status"])
	}
	if record.Attributes["feedback"] != "updated" {
		t.Fatalf("expected feedback to be updated, got %v", record.Attributes["feedback"])
	}
	if record.Attributes["propertyId"] != "property-1" {
		t.Fatalf("expected propertyId to remain, got %v", record.Attributes["propertyId"])
	}

	stored := mockRepo.records[schemaID][rowID]
	if stored == nil {
		t.Fatalf("expected updated record to be stored")
	}
	if stored.CreatedAt != 111 {
		t.Fatalf("expected CreatedAt preserved, got %d", stored.CreatedAt)
	}
	if stored.DeletedAt == nil || *stored.DeletedAt != 222 {
		t.Fatalf("expected DeletedAt preserved, got %v", stored.DeletedAt)
	}
}

func TestEntityManager_BatchCreate_CollectsErrors(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()

	em := NewEntityManager(transformer, mockRepo, registry, config)

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
	transformer := NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()

	schemaID, _, err := registry.GetSchemaAttributeCacheByName("visit")
	if err != nil {
		t.Fatalf("failed to get schema metadata: %v", err)
	}

	rowID := uuid.New()
	mockRepo.storeRecord(buildPersistentRecord(t, transformer, schemaID, rowID, visitPayload("visit-batch-update-1")))

	em := NewEntityManager(transformer, mockRepo, registry, config)

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
	transformer := NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()

	schemaID, _, err := registry.GetSchemaAttributeCacheByName("visit")
	if err != nil {
		t.Fatalf("failed to get schema metadata: %v", err)
	}

	rowID := uuid.New()
	mockRepo.storeRecord(buildPersistentRecord(t, transformer, schemaID, rowID, visitPayload("visit-batch-delete-1")))

	em := NewEntityManager(transformer, mockRepo, registry, config)

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

func TestEntityManager_Update_InvalidOptionalValueReturnsErrorAndPreservesStoredRecord(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry := newStubSchemaRegistry()
	transformer := NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()

	schemaID, _, err := registry.GetSchemaAttributeCacheByName("test")
	if err != nil {
		t.Fatalf("failed to get schema metadata: %v", err)
	}

	rowID := uuid.New()
	existing := buildPersistentRecord(t, transformer, schemaID, rowID, map[string]any{
		"name": "Alice",
		"age":  30,
	})
	mockRepo.storeRecord(existing)

	em := NewEntityManager(transformer, mockRepo, registry, config)
	req := &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "test", RowID: rowID},
		Type:             forma.OperationUpdate,
		Updates: map[string]any{
			"age": "not-a-number",
		},
	}

	_, err = em.Update(ctx, req)
	if err == nil {
		t.Fatal("expected update to fail")
	}
	if !errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput, got: %v", err)
	}

	stored := mockRepo.records[schemaID][rowID]
	if stored == nil {
		t.Fatal("expected stored record to remain")
	}
	reloaded, err := transformer.FromPersistentRecord(ctx, stored)
	if err != nil {
		t.Fatalf("failed to reload stored record: %v", err)
	}
	if reloaded["age"] != float64(30) {
		t.Fatalf("expected persisted age to remain 30, got %v", reloaded["age"])
	}
}

func TestEntityManager_BatchCreate_AtomicAllOrNothingOnRepositoryFailure(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()
	mockRepo.atomicInsertFailAt = 2
	em := NewEntityManager(transformer, mockRepo, registry, config)

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
	transformer := NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()
	em := NewEntityManager(transformer, mockRepo, registry, config)

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
	transformer := NewPersistentRecordTransformer(registry)
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

	em := NewEntityManager(transformer, mockRepo, registry, config)

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
	transformer := NewPersistentRecordTransformer(registry)
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

	em := NewEntityManager(transformer, mockRepo, registry, config)

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

func visitPayload(id string) map[string]any {
	return map[string]any{
		"id":               id,
		"leadId":           "lead-1",
		"userId":           "user-1",
		"propertyId":       "property-1",
		"scheduledStartAt": "2024-01-01T00:00:00Z",
		"status":           "scheduled",
	}
}
