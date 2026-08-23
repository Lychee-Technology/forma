package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/lychee-technology/forma/internal/transform"

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
	transformer := transform.NewPersistentRecordTransformer(registry)
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

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)

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

func TestEntityManager_Update_InvalidOptionalValueReturnsErrorAndPreservesStoredRecord(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry := newStubSchemaRegistry()
	transformer := transform.NewPersistentRecordTransformer(registry)
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

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)
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
