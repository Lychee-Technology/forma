package internal

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"lychee.technology/ltbase/forma"
)

// TestEntityManager_Create tests entity creation
func TestEntityManager_Create(t *testing.T) {
	// Setup
	ctx := context.Background()
	config := createTestConfig()
	registry, err := NewFileSchemaRegistry("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := NewTransformer(registry)

	// Create mock repository
	mockRepo := &mockAttributeRepository{
		attributes: make(map[string][]Attribute),
	}

	em := NewEntityManager(transformer, mockRepo, registry, config)

	// Test data
	testData := map[string]any{
		"id":     "test-id-1",
		"status": "hot",
		"personalInfo": map[string]any{
			"name": map[string]any{
				"display": "John Doe",
			},
		},
		"contactInfo": map[string]any{
			"email": "john@example.com",
		},
	}

	// Execute
	req := &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{
			SchemaName: "lead",
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

	if record.SchemaName != "lead" {
		t.Errorf("Expected schema name 'lead', got '%s'", record.SchemaName)
	}

	if record.RowID == (uuid.UUID{}) {
		t.Error("Expected non-zero UUID, got zero UUID")
	}

	if len(mockRepo.attributes) == 0 {
		t.Error("Expected attributes to be inserted, but repository is empty")
	}
}

// TestEntityManager_Get tests entity retrieval
func TestEntityManager_Get(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := NewFileSchemaRegistry("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := NewTransformer(registry)

	schemaID, cache, err := registry.GetSchemaByName("lead")
	if err != nil {
		t.Fatalf("failed to get schema metadata: %v", err)
	}

	// Create test attributes
	testRowID := uuid.New()
	testAttributes := []Attribute{
		newSchemaAttribute(t, cache, schemaID, testRowID, "id", "", "test-id-1"),
		newSchemaAttribute(t, cache, schemaID, testRowID, "status", "", "hot"),
	}

	mockRepo := &mockAttributeRepository{
		attributes: map[string][]Attribute{
			testRowID.String(): testAttributes,
		},
	}

	em := NewEntityManager(transformer, mockRepo, registry, config)

	// Execute
	req := &forma.QueryRequest{
		SchemaName: "lead",
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

// TestEntityManager_Delete tests entity deletion
func TestEntityManager_Delete(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := NewFileSchemaRegistry("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := NewTransformer(registry)

	mockRepo := &mockAttributeRepository{
		attributes: make(map[string][]Attribute),
	}

	em := NewEntityManager(transformer, mockRepo, registry, config)

	testRowID := uuid.New()

	// Execute
	req := &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{
			SchemaName: "lead",
			RowID:      testRowID,
		},
		Type: forma.OperationDelete,
	}

	err = em.Delete(ctx, req)

	// Assert
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}

	if !mockRepo.deleteEntityCalled {
		t.Error("Expected DeleteEntity to be called")
	}
}

// TestSchemaRegistry_LoadSchemas tests schema loading
func TestSchemaRegistry_LoadSchemas(t *testing.T) {
	schemaDir := "../cmd/server/schemas"
	registry, err := NewFileSchemaRegistry(schemaDir)
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}

	// Test schema retrieval by name
	schemaID, cache, err := registry.GetSchemaByName("lead")
	if err != nil {
		t.Errorf("GetSchemaByName failed: %v", err)
	}

	if schemaID == 0 {
		t.Error("Expected non-zero schema ID")
	}

	if cache == nil {
		t.Error("Expected schema attribute cache, got nil")
	}

	// Check that we have some attributes in the cache
	if len(cache) == 0 {
		t.Error("Expected non-empty attribute cache")
	}

	// Test that we can access an attribute
	if _, ok := cache["id"]; !ok {
		t.Error("Expected 'id' attribute in cache")
	}

	// Test listing schemas
	schemas := registry.ListSchemas()
	if len(schemas) == 0 {
		t.Error("Expected at least one schema")
	}

	if len(schemas) < 2 {
		t.Logf("Warning: Expected at least 2 schemas, got %d", len(schemas))
	}
}

// TestSchemaRegistry_GetSchemaByID tests retrieval by ID
func TestSchemaRegistry_GetSchemaByID(t *testing.T) {
	registry, err := NewFileSchemaRegistry("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}

	// First get a schema by name to obtain its ID
	schemaID, _, err := registry.GetSchemaByName("lead")
	if err != nil {
		t.Fatalf("failed to get schema by name: %v", err)
	}

	// Now retrieve by ID
	name, schema, err := registry.GetSchemaByID(schemaID)
	if err != nil {
		t.Errorf("GetSchemaByID failed: %v", err)
	}

	if name != "lead" {
		t.Errorf("Expected name 'lead', got '%s'", name)
	}

	if schema == nil {
		t.Error("Expected schema data, got nil")
	}
}

// Mock repository for testing
type mockAttributeRepository struct {
	attributes                               map[string][]Attribute
	deleteEntityCalled                       bool
	existsEntityCalled                       bool
	insertAttributesCalled                   bool
	multiSchemaResult                        map[string][]Attribute
	queryAttributesCalledWithMultipleSchemas bool
	advancedRowIDs                           []uuid.UUID
	advancedTotal                            int64
	advancedErr                              error
	advancedQueryRowIDsFunc                  func(ctx context.Context, clause string, args []any, limit, offset int) ([]uuid.UUID, int64, error)
}

func (m *mockAttributeRepository) InsertAttributes(ctx context.Context, attributes []Attribute) error {
	m.insertAttributesCalled = true
	if len(attributes) > 0 {
		rowID := attributes[0].RowID
		m.attributes[rowID.String()] = append(m.attributes[rowID.String()], attributes...)
	}
	return nil
}

func (m *mockAttributeRepository) UpdateAttributes(ctx context.Context, attributes []Attribute) error {
	return nil
}

func (m *mockAttributeRepository) DeleteAttributes(ctx context.Context, schemaName string, rowIDs []uuid.UUID) error {
	return nil
}

func (m *mockAttributeRepository) GetAttributes(ctx context.Context, schemaName string, rowID uuid.UUID) ([]Attribute, error) {
	return m.attributes[rowID.String()], nil
}

func (m *mockAttributeRepository) QueryAttributes(ctx context.Context, query *AttributeQuery) ([]Attribute, error) {
	// Return mock results
	result := make([]Attribute, 0)
	for _, attrs := range m.multiSchemaResult {
		result = append(result, attrs...)
	}
	return result, nil
}

func (m *mockAttributeRepository) ExistsEntity(ctx context.Context, schemaName string, rowID uuid.UUID) (bool, error) {
	m.existsEntityCalled = true
	_, exists := m.attributes[rowID.String()]
	return exists, nil
}

func (m *mockAttributeRepository) DeleteEntity(ctx context.Context, schemaName string, rowID uuid.UUID) error {
	m.deleteEntityCalled = true
	delete(m.attributes, rowID.String())
	return nil
}

func (m *mockAttributeRepository) CountEntities(ctx context.Context, schemaName string, filters []forma.Filter) (int64, error) {
	return int64(len(m.attributes)), nil
}

func (m *mockAttributeRepository) BatchUpsertAttributes(ctx context.Context, attributes []Attribute) error {
	return m.InsertAttributes(ctx, attributes)
}

func (m *mockAttributeRepository) AdvancedQueryRowIDs(ctx context.Context, clause string, args []any, limit, offset int) ([]uuid.UUID, int64, error) {
	if m.advancedQueryRowIDsFunc != nil {
		return m.advancedQueryRowIDsFunc(ctx, clause, args, limit, offset)
	}
	return m.advancedRowIDs, m.advancedTotal, m.advancedErr
}

// TestEntityManager_CrossSchemaSearch tests cross-schema search with single query
func TestEntityManager_CrossSchemaSearch(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := NewFileSchemaRegistry("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := NewTransformer(registry)

	// Create mock repository with multi-schema support
	mockRepo := &mockAttributeRepository{
		attributes:        make(map[string][]Attribute),
		multiSchemaResult: make(map[string][]Attribute),
	}

	em := NewEntityManager(transformer, mockRepo, registry, config)

	// Setup test data for multiple schemas
	leadSchemaID, leadCache, err := registry.GetSchemaByName("lead")
	if err != nil {
		t.Fatalf("failed to get lead schema metadata: %v", err)
	}

	listingSchemaID, listingCache, err := registry.GetSchemaByName("listing")
	if err != nil {
		t.Fatalf("failed to get listing schema metadata: %v", err)
	}

	rowID1 := uuid.New()
	rowID2 := uuid.New()

	mockRepo.multiSchemaResult = map[string][]Attribute{
		"result1": {
			newSchemaAttribute(t, leadCache, leadSchemaID, rowID1, "id", "", "lead-1"),
			newSchemaAttribute(t, leadCache, leadSchemaID, rowID1, "status", "", "hot"),
		},
		"result2": {
			newSchemaAttribute(t, listingCache, listingSchemaID, rowID2, "listingId", "", "listing-1"),
			newSchemaAttribute(t, listingCache, listingSchemaID, rowID2, "building.address.city", "", "San Francisco"),
		},
	}

	// Execute
	req := &forma.CrossSchemaRequest{
		SchemaNames:  []string{"lead", "listing"},
		SearchTerm:   "San Francisco",
		Page:         1,
		ItemsPerPage: 10,
		Filters:      make(map[string]forma.Filter),
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
	registry, err := NewFileSchemaRegistry("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := NewTransformer(registry)

	mockRepo := &mockAttributeRepository{
		attributes: make(map[string][]Attribute),
	}

	em := NewEntityManager(transformer, mockRepo, registry, config)

	// Test with invalid schema name
	req := &forma.CrossSchemaRequest{
		SchemaNames:  []string{"lead", "nonexistent_schema"},
		SearchTerm:   "test",
		Page:         1,
		ItemsPerPage: 10,
		Filters:      make(map[string]forma.Filter),
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
	registry, err := NewFileSchemaRegistry("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := NewTransformer(registry)

	mockRepo := &mockAttributeRepository{
		attributes: make(map[string][]Attribute),
	}

	em := NewEntityManager(transformer, mockRepo, registry, config)

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
	registry, err := NewFileSchemaRegistry("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := NewTransformer(registry)

	mockRepo := &mockAttributeRepository{
		attributes: make(map[string][]Attribute),
	}

	em := NewEntityManager(transformer, mockRepo, registry, config)

	// Test with empty search term
	req := &forma.CrossSchemaRequest{
		SchemaNames:  []string{"lead"},
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
	registry, err := NewFileSchemaRegistry("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := NewTransformer(registry)

	mockRepo := &mockAttributeRepository{
		attributes:        make(map[string][]Attribute),
		multiSchemaResult: make(map[string][]Attribute),
	}

	em := NewEntityManager(transformer, mockRepo, registry, config)

	// Test with page 0 (should default to 1)
	req := &forma.CrossSchemaRequest{
		SchemaNames:  []string{"lead"},
		SearchTerm:   "test",
		Page:         0,
		ItemsPerPage: 0,
		Filters:      make(map[string]forma.Filter),
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

func TestEntityManager_AdvancedQuery(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := NewFileSchemaRegistry("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := NewTransformer(registry)

	schemaID, cache, err := registry.GetSchemaByName("lead")
	if err != nil {
		t.Fatalf("failed to get schema metadata: %v", err)
	}

	rowID := uuid.New()
	mockRepo := &mockAttributeRepository{
		attributes: map[string][]Attribute{
			rowID.String(): {
				newSchemaAttribute(t, cache, schemaID, rowID, "id", "", "lead-advanced"),
				newSchemaAttribute(t, cache, schemaID, rowID, "status", "", "hot"),
			},
		},
		advancedRowIDs: []uuid.UUID{rowID},
		advancedTotal:  1,
	}

	em := NewEntityManager(transformer, mockRepo, registry, config)

	req := &forma.AdvancedQueryRequest{
		SchemaName: "lead",
		Condition: &forma.CompositeCondition{
			Logic: forma.LogicAnd,
			Conditions: []forma.Condition{
				&forma.KvCondition{
					Attr:  "status",
					Value: "equals:hot",
				},
			},
		},
		Page:         1,
		ItemsPerPage: 10,
	}

	result, err := em.AdvancedQuery(ctx, req)
	if err != nil {
		t.Fatalf("AdvancedQuery failed: %v", err)
	}

	if result == nil {
		t.Fatal("AdvancedQuery returned nil result")
	}

	if len(result.Data) != 1 {
		t.Fatalf("expected 1 record, got %d", len(result.Data))
	}

	if result.TotalRecords != 1 {
		t.Fatalf("expected total records 1, got %d", result.TotalRecords)
	}

	if result.Data[0].RowID != rowID {
		t.Errorf("expected rowID %s, got %s", rowID, result.Data[0].RowID)
	}
}

func newSchemaAttribute(t *testing.T, cache forma.SchemaAttributeCache, schemaID int16, rowID uuid.UUID, name string, indices string, value any) Attribute {
	meta, ok := cache[name]
	if !ok {
		t.Fatalf("attribute %s not found in schema %d", name, schemaID)
	}

	attr := Attribute{
		SchemaID:     schemaID,
		RowID:        rowID,
		AttrID:       meta.AttributeID,
		ArrayIndices: indices,
	}

	if err := populateTypedValue(&attr, value, meta.ValueType); err != nil {
		t.Fatalf("failed to populate attribute %s: %v", name, err)
	}

	return attr
}

// Helper function to create test config
func createTestConfig() *forma.Config {
	return &forma.Config{
		Query: forma.QueryConfig{
			DefaultPageSize: 50,
			MaxPageSize:     100,
		},
	}
}

// TestFileSchemaRegistry_InvalidDirectory tests error handling for invalid directory
func TestFileSchemaRegistry_InvalidDirectory(t *testing.T) {
	_, err := NewFileSchemaRegistry("/nonexistent/directory")
	if err == nil {
		t.Error("Expected error for invalid directory, got nil")
	}
}

// TestFileSchemaRegistry_NoSchemaFiles tests error handling when no schema files exist
func TestFileSchemaRegistry_NoSchemaFiles(t *testing.T) {
	// Create a temporary empty directory
	tmpDir, err := os.MkdirTemp("", "test-schemas-")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	_, err = NewFileSchemaRegistry(tmpDir)
	if err == nil {
		t.Error("Expected error when no schema files found, got nil")
	}
}

// TestFileSchemaRegistry_InvalidJSON tests error handling for invalid JSON files
func TestFileSchemaRegistry_InvalidJSON(t *testing.T) {
	// Create a temporary directory with an invalid JSON file
	tmpDir, err := os.MkdirTemp("", "test-schemas-")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Write invalid JSON
	invalidFile := filepath.Join(tmpDir, "invalid.json")
	err = os.WriteFile(invalidFile, []byte("{invalid json"), 0644)
	if err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err = NewFileSchemaRegistry(tmpDir)
	if err == nil {
		t.Error("Expected error for invalid JSON, got nil")
	}
}
