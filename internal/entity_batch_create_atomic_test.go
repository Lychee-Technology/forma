package internal

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma/internal/transform"

	"github.com/lychee-technology/forma"
)

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
