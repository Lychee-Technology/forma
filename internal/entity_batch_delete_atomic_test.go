package internal

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma/internal/transform"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

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
