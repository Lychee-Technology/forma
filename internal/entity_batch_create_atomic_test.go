package internal

import (
	"context"
	"fmt"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
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

// TestEntityManager_BatchCreate_AtomicAnswersStoredTimestamps pins #701: the
// batch insert stamps created_at and updated_at from the repository's clock,
// so the response must be built from the record as stored, not from the
// transformer's earlier clock read.
func TestEntityManager_BatchCreate_AtomicAnswersStoredTimestamps(t *testing.T) {
	ctx := context.Background()
	registry := &stubSchemaRegistry{schemaID: 701, schemaName: "stamped", cache: forma.SchemaAttributeCache{
		"name":    {AttributeID: 1, ValueType: forma.ValueTypeText, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01}},
		"created": {AttributeID: 2, ValueType: forma.ValueTypeBigInt, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnCreatedAt}},
		"touched": {AttributeID: 3, ValueType: forma.ValueTypeBigInt, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnUpdatedAt}},
	}}
	repo := newMockPersistentRecordRepository()
	// The repository's clock, far from the transformer's time.Now(): the
	// Postgres batch insert restamps both columns (prepareCreate).
	const stored = int64(1767225600000) // 2026-01-01T00:00:00Z
	repo.insertFunc = func(_ context.Context, record *model.PersistentRecord) error {
		record.CreatedAt, record.UpdatedAt = stored, stored
		repo.insertedRecords = append(repo.insertedRecords, record)
		repo.storeRecord(record)
		return nil
	}
	em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry), repo, nil, registry, createTestConfig(), nil)

	result, err := em.BatchCreate(ctx, &forma.BatchOperation{
		Atomic: true,
		Operations: []forma.EntityOperation{
			{EntityIdentifier: forma.EntityIdentifier{SchemaName: "stamped"}, Type: forma.OperationCreate, Data: map[string]any{"name": "first"}},
			{EntityIdentifier: forma.EntityIdentifier{SchemaName: "stamped"}, Type: forma.OperationCreate, Data: map[string]any{"name": "second"}},
		},
	})
	if err != nil {
		t.Fatalf("atomic batch create: %v", err)
	}
	if len(result.Successful) != 2 || len(repo.insertedRecords) != 2 {
		t.Fatalf("expected 2 answered and 2 stored records, got %d and %d", len(result.Successful), len(repo.insertedRecords))
	}
	for i, answered := range result.Successful {
		if answered.RowID != repo.insertedRecords[i].RowID {
			t.Fatalf("operation[%d] answered row %s, stored row %s", i, answered.RowID, repo.insertedRecords[i].RowID)
		}
		if answered.Attributes["name"] != []string{"first", "second"}[i] {
			t.Fatalf("operation[%d] answered name %v", i, answered.Attributes["name"])
		}
		for _, alias := range []string{"created", "touched"} {
			if got := fmt.Sprint(answered.Attributes[alias]); got != fmt.Sprint(stored) {
				t.Fatalf("operation[%d] answered %s=%s, stored %d", i, alias, got, stored)
			}
		}
	}
}
