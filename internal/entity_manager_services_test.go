package internal

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma/internal/transform"

	"github.com/lychee-technology/forma"
)

func TestNewEntityManagerInitializesLongLivedServices(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry := newStubSchemaRegistry()
	transformer := transform.NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()

	manager := NewEntityManager(transformer, mockRepo, nil, registry, config, nil)
	em, ok := manager.(*entityManager)
	if !ok {
		t.Fatalf("expected *entityManager, got %T", manager)
	}

	if em.crud == nil {
		t.Fatal("expected crud service to be initialized")
	}
	if em.query == nil {
		t.Fatal("expected query service to be initialized")
	}
	if em.batch == nil {
		t.Fatal("expected batch service to be initialized")
	}
	if em.relation == nil {
		t.Fatal("expected relation service to be initialized")
	}

	if em.batch.createOp == nil {
		t.Fatal("expected batch create operation to be initialized")
	}
	if em.batch.updateOp == nil {
		t.Fatal("expected batch update operation to be initialized")
	}
	if em.batch.deleteOp == nil {
		t.Fatal("expected batch delete operation to be initialized")
	}

	result, err := em.BatchCreate(ctx, &forma.BatchOperation{
		Operations: []forma.EntityOperation{
			{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: "test"},
				Type:             forma.OperationCreate,
				Data: map[string]any{
					"name": "Alice",
					"age":  30,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("BatchCreate failed: %v", err)
	}
	if len(result.Successful) != 1 {
		t.Fatalf("expected 1 successful result, got %d", len(result.Successful))
	}
	if len(result.Failed) != 0 {
		t.Fatalf("expected 0 failed results, got %d", len(result.Failed))
	}
	if result.Successful[0].SchemaName != "test" {
		t.Fatalf("expected successful schema name test, got %s", result.Successful[0].SchemaName)
	}
	if len(mockRepo.insertedRecords) != 1 {
		t.Fatalf("expected 1 inserted record, got %d", len(mockRepo.insertedRecords))
	}
}
