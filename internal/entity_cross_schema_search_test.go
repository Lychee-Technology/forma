package internal

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma/internal/transform"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// TestEntityManager_CrossSchemaSearch tests cross-schema search with single query
func TestEntityManager_CrossSchemaSearch(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)

	// Create mock repository with multi-schema support
	mockRepo := newMockPersistentRecordRepository()

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)

	// Setup test data for multiple schemas
	visitSchemaID, _, err := registry.GetSchemaAttributeCacheByName("visit")
	if err != nil {
		t.Fatalf("failed to get visit schema metadata: %v", err)
	}

	rowID1 := uuid.New()
	rowID2 := uuid.New()

	mockRepo.storeRecord(buildPersistentRecord(t, transformer, visitSchemaID, rowID1, map[string]any{
		"id":               "visit-1",
		"leadId":           "lead-1",
		"userId":           "user-1",
		"propertyId":       "property-sf-1",
		"scheduledStartAt": "2024-01-01T00:00:00Z",
		"status":           "scheduled",
		"feedback":         "Site visit in San Francisco",
	}))
	mockRepo.storeRecord(buildPersistentRecord(t, transformer, visitSchemaID, rowID2, map[string]any{
		"id":               "visit-2",
		"leadId":           "lead-2",
		"userId":           "user-2",
		"propertyId":       "property-sf-2",
		"scheduledStartAt": "2024-01-02T00:00:00Z",
		"status":           "visited",
		"feedback":         "Property viewing in San Francisco",
	}))

	// Execute
	req := &forma.CrossSchemaRequest{
		SchemaNames:  []string{"visit"},
		SearchTerm:   "San Francisco",
		Page:         1,
		ItemsPerPage: 10,
	}

	result, err := em.CrossSchemaSearch(ctx, req)

	// Assert
	if err != nil {
		t.Errorf("CrossSchemaSearch failed: %v", err)
	}

	if result == nil {
		t.Fatal("CrossSchemaSearch returned nil result")
	}

	if result.CurrentPage != 1 {
		t.Errorf("Expected current page 1, got %d", result.CurrentPage)
	}

	if result.ItemsPerPage != 10 {
		t.Errorf("Expected items per page 10, got %d", result.ItemsPerPage)
	}
}

// TestEntityManager_CrossSchemaSearch_ValidateSchemas tests schema validation
func TestEntityManager_CrossSchemaSearch_ValidateSchemas(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)

	mockRepo := newMockPersistentRecordRepository()

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)

	// Test with invalid schema name
	req := &forma.CrossSchemaRequest{
		SchemaNames:  []string{"visit", "nonexistent_schema"},
		SearchTerm:   "test",
		Page:         1,
		ItemsPerPage: 10,
	}

	_, err = em.CrossSchemaSearch(ctx, req)

	if err == nil {
		t.Error("Expected error for invalid schema, got nil")
	}
}

// TestEntityManager_CrossSchemaSearch_EmptySchemaNames tests error handling
func TestEntityManager_CrossSchemaSearch_EmptySchemaNames(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)

	mockRepo := newMockPersistentRecordRepository()

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)

	// Test with empty schema names
	req := &forma.CrossSchemaRequest{
		SchemaNames:  []string{},
		SearchTerm:   "test",
		Page:         1,
		ItemsPerPage: 10,
	}

	_, err = em.CrossSchemaSearch(ctx, req)

	if err == nil {
		t.Error("Expected error for empty schema names, got nil")
	}
}

// TestEntityManager_CrossSchemaSearch_EmptySearchTerm tests error handling
func TestEntityManager_CrossSchemaSearch_EmptySearchTerm(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)

	mockRepo := newMockPersistentRecordRepository()

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)

	// Test with empty search term
	req := &forma.CrossSchemaRequest{
		SchemaNames:  []string{"visit"},
		SearchTerm:   "",
		Page:         1,
		ItemsPerPage: 10,
	}

	_, err = em.CrossSchemaSearch(ctx, req)

	if err == nil {
		t.Error("Expected error for empty search term, got nil")
	}
}

// TestEntityManager_CrossSchemaSearch_Pagination tests pagination
func TestEntityManager_CrossSchemaSearch_Pagination(t *testing.T) {
	ctx := context.Background()
	config := &forma.Config{
		Query: forma.QueryConfig{
			DefaultPageSize: 10,
			MaxPageSize:     100,
		},
	}
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)

	mockRepo := newMockPersistentRecordRepository()

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)

	// Test with page 0 (should default to 1)
	req := &forma.CrossSchemaRequest{
		SchemaNames:  []string{"visit"},
		SearchTerm:   "test",
		Page:         0,
		ItemsPerPage: 0,
	}

	result, err := em.CrossSchemaSearch(ctx, req)

	if err != nil {
		t.Errorf("CrossSchemaSearch failed: %v", err)
	}

	if result.CurrentPage != 1 {
		t.Errorf("Expected page to default to 1, got %d", result.CurrentPage)
	}

	if result.ItemsPerPage != 10 {
		t.Errorf("Expected items per page to default to 10, got %d", result.ItemsPerPage)
	}
}
