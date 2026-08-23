package internal

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma/internal/transform"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// TestEntityManager_Create tests entity creation
func TestEntityManager_Create(t *testing.T) {
	// Setup
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)

	// Create mock repository
	mockRepo := newMockPersistentRecordRepository()

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)

	// Test data
	testData := map[string]any{
		"id":               "test-id-1",
		"leadId":           "lead-1",
		"userId":           "user-1",
		"propertyId":       "property-1",
		"scheduledStartAt": "2024-01-01T00:00:00Z",
		"status":           "scheduled",
	}

	// Execute
	req := &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{
			SchemaName: "visit",
		},
		Type: forma.OperationCreate,
		Data: testData,
	}

	record, err := em.Create(ctx, req)

	// Assert
	if err != nil {
		t.Errorf("Create failed: %v", err)
	}

	if record == nil {
		t.Fatal("Create returned nil record")
	}

	if record.SchemaName != "visit" {
		t.Errorf("Expected schema name 'visit', got '%s'", record.SchemaName)
	}

	if record.RowID == (uuid.UUID{}) {
		t.Error("Expected non-zero UUID, got zero UUID")
	}

	if len(mockRepo.insertedRecords) == 0 {
		t.Error("Expected persistent record to be inserted, but repository is empty")
	}
}

func TestEntityManager_Create_StripsRelationFields(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)

	req := &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "visit"},
		Type:             forma.OperationCreate,
		Data: map[string]any{
			"id":               "visit-rel-1",
			"leadId":           "lead-rel-1",
			"userId":           "user-1",
			"propertyId":       "prop-1",
			"scheduledStartAt": "2024-01-01T00:00:00Z",
			"status":           "scheduled",
			"contactSnapshot": map[string]any{
				"name":         "Alice",
				"primaryPhone": "123",
			},
		},
	}

	record, err := em.Create(ctx, req)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if _, exists := record.Attributes["contactSnapshot"]; exists {
		t.Fatalf("contactSnapshot should be stripped from create response")
	}

	if len(mockRepo.insertedRecords) != 1 {
		t.Fatalf("expected one record inserted, got %d", len(mockRepo.insertedRecords))
	}

	for _, attr := range mockRepo.insertedRecords[0].OtherAttributes {
		if attr.AttrID >= 21 && attr.AttrID <= 24 {
			t.Fatalf("contactSnapshot attribute %d should not be persisted", attr.AttrID)
		}
	}
}

// TestEntityManager_Get tests entity retrieval
func TestEntityManager_Get(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)

	schemaID, _, err := registry.GetSchemaAttributeCacheByName("visit")
	if err != nil {
		t.Fatalf("failed to get schema metadata: %v", err)
	}

	testRowID := uuid.New()
	testRecord, err := transformer.ToPersistentRecord(ctx, schemaID, testRowID, map[string]any{
		"id":               "test-id-1",
		"leadId":           "lead-1",
		"userId":           "user-1",
		"propertyId":       "property-1",
		"scheduledStartAt": "2024-01-01T00:00:00Z",
		"status":           "scheduled",
	})
	if err != nil {
		t.Fatalf("failed to build persistent record: %v", err)
	}
	mockRepo := newMockPersistentRecordRepository()
	mockRepo.storeRecord(testRecord)

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)

	// Execute
	req := &forma.QueryRequest{
		SchemaName: "visit",
		RowID:      &testRowID,
	}

	record, err := em.Get(ctx, req)

	// Assert
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}

	if record == nil {
		t.Fatal("Get returned nil record")
	}

	if record.RowID != testRowID {
		t.Errorf("Expected row ID %s, got %s", testRowID, record.RowID)
	}

	if record.Attributes["id"] != "test-id-1" {
		t.Errorf("Expected id 'test-id-1', got '%v'", record.Attributes["id"])
	}
}

func TestEntityManager_Get_EnrichesFromParent(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()

	leadSchemaID, _, err := registry.GetSchemaByName("lead")
	if err != nil {
		t.Fatalf("failed to get lead schema: %v", err)
	}
	visitSchemaID, _, err := registry.GetSchemaByName("visit")
	if err != nil {
		t.Fatalf("failed to get visit schema: %v", err)
	}

	leadRowID := uuid.New()
	leadID := uuid.New().String()
	mockRepo.storeRecord(buildPersistentRecord(t, transformer, leadSchemaID, leadRowID, map[string]any{
		"id":          leadID,
		"tenantId":    "tenant-1",
		"ownerUserId": "owner-1",
		"pipeline":    "buy",
		"stage":       "new",
		"status":      "open",
		"contact": map[string]any{
			"isAnonymous":  false,
			"name":         "Parent Lead",
			"primaryPhone": "123-456",
		},
		"createdAt": "2024-01-01T00:00:00Z",
		"updatedAt": "2024-01-02T00:00:00Z",
	}))

	visitRowID := uuid.New()
	mockRepo.storeRecord(buildPersistentRecord(t, transformer, visitSchemaID, visitRowID, map[string]any{
		"id":               "visit-enrich-1",
		"leadId":           leadID,
		"userId":           "user-1",
		"propertyId":       "prop-1",
		"scheduledStartAt": "2024-02-01T00:00:00Z",
		"status":           "scheduled",
	}))

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)

	req := &forma.QueryRequest{SchemaName: "visit", RowID: &visitRowID}
	record, err := em.Get(ctx, req)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	snapshot, ok := record.Attributes["contactSnapshot"].(map[string]any)
	if !ok {
		t.Fatalf("expected contactSnapshot to be populated from parent")
	}
	if snapshot["name"] != "Parent Lead" {
		t.Fatalf("expected contactSnapshot.name from parent, got %v", snapshot["name"])
	}
	if snapshot["primaryPhone"] != "123-456" {
		t.Fatalf("expected contactSnapshot.primaryPhone from parent, got %v", snapshot["primaryPhone"])
	}

	projectedReq := &forma.QueryRequest{SchemaName: "visit", RowID: &visitRowID, Attrs: []string{"userId"}}
	projectedRecord, err := em.Get(ctx, projectedReq)
	if err != nil {
		t.Fatalf("Get with projection failed: %v", err)
	}
	if _, exists := projectedRecord.Attributes["contactSnapshot"]; exists {
		t.Fatalf("contactSnapshot should be excluded when not requested")
	}
}

// TestEntityManager_Delete tests entity deletion
func TestEntityManager_Delete(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)

	mockRepo := newMockPersistentRecordRepository()

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)

	testRowID := uuid.New()

	// Execute
	req := &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{
			SchemaName: "visit",
			RowID:      testRowID,
		},
		Type: forma.OperationDelete,
	}

	err = em.Delete(ctx, req)

	// Assert
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}

	if mockRepo.deleteCalls == 0 {
		t.Error("Expected DeleteEntity to be called")
	}
}
