package internal

import (
	"context"
	"testing"

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
