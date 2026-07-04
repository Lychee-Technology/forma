package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// newFileSchemaRegistryFromDir creates a schema registry that loads schemas from
// a directory without requiring a database connection. Schema IDs are auto-assigned
// based on alphabetical order of schema names.
//
// Parameters:
//   - schemaDir: Directory containing *_attributes.json files
func newFileSchemaRegistryFromDir(schemaDir string) (forma.SchemaRegistry, error) {
	registry := &fileSchemaRegistry{
		schemaDir:             schemaDir,
		nameToID:              make(map[string]int16),
		idToName:              make(map[int16]string),
		schemaAttributeCaches: make(map[int16]forma.SchemaAttributeCache),
		schemas:               make(map[int16]forma.JSONSchema),
	}

	if err := registry.loadSchemasFromDir(); err != nil {
		return nil, err
	}

	return registry, nil
}

// loadSchemasFromDir scans the schema directory for *_attributes.json files
// and loads them, auto-assigning schema IDs alphabetically.
func (r *fileSchemaRegistry) loadSchemasFromDir() error {
	// Read directory contents
	entries, err := os.ReadDir(r.schemaDir)
	if err != nil {
		return fmt.Errorf("failed to read schema directory %s: %w", r.schemaDir, err)
	}

	// Find all *_attributes.json files
	var schemaNames []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if filepath.Ext(name) == ".json" && !hasSuffix(name, "_attributes.json") && !hasSuffix(name, "_full.json") {
			// This is a schema definition file (e.g., visit.json)
			schemaName := name[:len(name)-5] // remove .json extension
			schemaNames = append(schemaNames, schemaName)
		}
	}

	if len(schemaNames) == 0 {
		return fmt.Errorf("no schema files found in directory: %s", r.schemaDir)
	}

	// Sort schema names to ensure consistent ID assignment
	sort.Strings(schemaNames)

	// Load each schema
	for i, schemaName := range schemaNames {
		schemaID := int16(i + 1) // IDs start at 1

		// Load attribute metadata from corresponding _attributes.json file
		attributesFile := filepath.Join(r.schemaDir, schemaName+"_attributes.json")
		attributeData, err := os.ReadFile(attributesFile)
		if err != nil {
			return fmt.Errorf("failed to read attributes file %s: %w", attributesFile, err)
		}

		// Parse attribute metadata JSON
		var rawAttributes map[string]map[string]any
		if err := json.Unmarshal(attributeData, &rawAttributes); err != nil {
			return fmt.Errorf("failed to parse attributes file %s: %w", attributesFile, err)
		}

		// Convert to SchemaAttributeCache
		cache := make(forma.SchemaAttributeCache)
		for attrName, attrData := range rawAttributes {
			meta, err := parseFileAttributeMetadata(attrName, attrData, attributesFile)
			if err != nil {
				return err
			}
			cache[attrName] = meta
		}

		// Load main schema JSON file (e.g., lead.json)
		schemaFile := filepath.Join(r.schemaDir, schemaName+".json")
		schemaData, err := os.ReadFile(schemaFile)
		if err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("failed to read schema file %s: %w", schemaFile, err)
			}
		} else {
			jsonSchema, err := parseJSONSchemaFile(schemaData, schemaID, schemaName)
			if err != nil {
				return fmt.Errorf("failed to parse schema file %s: %w", schemaFile, err)
			}
			r.schemas[schemaID] = jsonSchema
		}

		r.nameToID[schemaName] = schemaID
		r.idToName[schemaID] = schemaName
		r.schemaAttributeCaches[schemaID] = cache
	}

	return nil
}

// TestEntityManager_Create tests entity creation
func TestEntityManager_Create(t *testing.T) {
	// Setup
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := NewPersistentRecordTransformer(registry)

	// Create mock repository
	mockRepo := newMockPersistentRecordRepository()

	em := NewEntityManager(transformer, mockRepo, nil, registry, config)

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
	transformer := NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()

	em := NewEntityManager(transformer, mockRepo, nil, registry, config)

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
	transformer := NewPersistentRecordTransformer(registry)

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

	em := NewEntityManager(transformer, mockRepo, nil, registry, config)

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
	transformer := NewPersistentRecordTransformer(registry)
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

	em := NewEntityManager(transformer, mockRepo, nil, registry, config)

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
	transformer := NewPersistentRecordTransformer(registry)

	mockRepo := newMockPersistentRecordRepository()

	em := NewEntityManager(transformer, mockRepo, nil, registry, config)

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

func TestEntityManager_QueryBuildsAttributeOrders(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := NewPersistentRecordTransformer(registry)

	mockRepo := newMockPersistentRecordRepository()

	em := NewEntityManager(transformer, mockRepo, nil, registry, config)

	_, cache, err := registry.GetSchemaAttributeCacheByName("visit")
	if err != nil {
		t.Fatalf("failed to get schema metadata: %v", err)
	}

	req := &forma.QueryRequest{
		SchemaName:   "visit",
		Page:         1,
		ItemsPerPage: 10,
		SortBy:       []string{"scheduledStartAt"},
		SortOrder:    forma.SortOrderDesc,
	}

	if _, err := em.Query(ctx, req); err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if mockRepo.lastQuery == nil {
		t.Fatal("expected repository to receive attribute query")
	}

	if len(mockRepo.lastQuery.AttributeOrders) != 1 {
		t.Fatalf("expected 1 attribute order, got %d", len(mockRepo.lastQuery.AttributeOrders))
	}

	meta, ok := cache["scheduledStartAt"]
	if !ok {
		t.Fatal("expected scheduledStartAt metadata in cache")
	}
	attrOrder := mockRepo.lastQuery.AttributeOrders[0]
	if attrOrder.AttrID != meta.AttributeID {
		t.Fatalf("expected attrID %d, got %d", meta.AttributeID, attrOrder.AttrID)
	}
	if attrOrder.ValueType != meta.ValueType {
		t.Fatalf("expected valueType %s, got %s", meta.ValueType, attrOrder.ValueType)
	}
	if attrOrder.SortOrder != forma.SortOrderDesc {
		t.Fatalf("expected sort order desc, got %s", attrOrder.SortOrder)
	}
}

func TestEntityManager_QueryInvalidSortAttribute(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := NewPersistentRecordTransformer(registry)

	mockRepo := newMockPersistentRecordRepository()

	em := NewEntityManager(transformer, mockRepo, nil, registry, config)

	req := &forma.QueryRequest{
		SchemaName:   "visit",
		Page:         1,
		ItemsPerPage: 10,
		SortBy:       []string{"nonexistent"},
	}

	_, err = em.Query(ctx, req)
	if err == nil {
		t.Fatal("expected error for invalid sort attribute, got nil")
	}

	if !strings.Contains(err.Error(), "unknown attribute") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEntityManager_QueryPropagatesCondition(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	reg, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := NewPersistentRecordTransformer(reg)
	mockRepo := newMockPersistentRecordRepository()
	em := NewEntityManager(transformer, mockRepo, nil, reg, config)

	condition := &forma.CompositeCondition{
		Logic: forma.LogicAnd,
		Conditions: []forma.Condition{
			&forma.KvCondition{Attr: "status", Value: "equals:scheduled"},
		},
	}

	req := &forma.QueryRequest{
		SchemaName:   "visit",
		Page:         1,
		ItemsPerPage: 5,
		Condition:    condition,
	}

	if _, err := em.Query(ctx, req); err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if mockRepo.lastQuery == nil {
		t.Fatal("expected attribute query to be captured")
	}

	if mockRepo.lastQuery.Condition != condition {
		t.Fatal("expected attribute query to receive composite condition")
	}
}

// TestSchemaRegistry_LoadSchemas tests schema loading
func TestSchemaRegistry_LoadSchemas(t *testing.T) {
	schemaDir := "../cmd/server/schemas"
	registry, err := newFileSchemaRegistryFromDir(schemaDir)
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}

	// Test schema retrieval by name
	schemaID, cache, err := registry.GetSchemaAttributeCacheByName("visit")
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
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}

	// First get a schema by name to obtain its ID
	schemaID, _, err := registry.GetSchemaAttributeCacheByName("visit")
	if err != nil {
		t.Fatalf("failed to get schema by name: %v", err)
	}

	// Now retrieve by ID
	name, schema, err := registry.GetSchemaAttributeCacheByID(schemaID)
	if err != nil {
		t.Errorf("GetSchemaByID failed: %v", err)
	}

	if name != "visit" {
		t.Errorf("Expected name 'visit', got '%s'", name)
	}

	if schema == nil {
		t.Error("Expected schema data, got nil")
	}
}

// Mock repository for testing
type mockPersistentRecordRepository struct {
	records            map[int16]map[uuid.UUID]*PersistentRecord
	insertedRecords    []*PersistentRecord
	deleteCalls        int
	lastQuery          *PersistentRecordQuery
	queries            []*PersistentRecordQuery
	queryFunc          func(ctx context.Context, query *PersistentRecordQuery) (*PersistentRecordPage, error)
	atomicInsertFailAt int
	atomicUpdateFailAt int
	atomicDeleteFailAt int
}

type mockFederatedQueryEngine struct {
	lastTables StorageTables
	lastQuery  *FederatedAttributeQuery
	lastOpts   *FederatedQueryOptions
	queryFunc  func(ctx context.Context, tables StorageTables, fq *FederatedAttributeQuery, opts *FederatedQueryOptions) (*PersistentRecordPage, error)
}

func newMockPersistentRecordRepository() *mockPersistentRecordRepository {
	return &mockPersistentRecordRepository{
		records: make(map[int16]map[uuid.UUID]*PersistentRecord),
	}
}

func (m *mockPersistentRecordRepository) storeRecord(record *PersistentRecord) {
	if record == nil {
		return
	}
	if m.records[record.SchemaID] == nil {
		m.records[record.SchemaID] = make(map[uuid.UUID]*PersistentRecord)
	}
	m.records[record.SchemaID][record.RowID] = record
}

func (m *mockPersistentRecordRepository) InsertPersistentRecord(ctx context.Context, tables StorageTables, record *PersistentRecord) error {
	m.insertedRecords = append(m.insertedRecords, record)
	m.storeRecord(record)
	return nil
}

func (m *mockPersistentRecordRepository) UpdatePersistentRecord(ctx context.Context, tables StorageTables, record *PersistentRecord) error {
	m.storeRecord(record)
	return nil
}

func (m *mockPersistentRecordRepository) DeletePersistentRecord(ctx context.Context, tables StorageTables, schemaID int16, rowID uuid.UUID) error {
	m.deleteCalls++
	if schemaRecords, ok := m.records[schemaID]; ok {
		delete(schemaRecords, rowID)
	}
	return nil
}

func (m *mockPersistentRecordRepository) GetPersistentRecord(ctx context.Context, tables StorageTables, schemaID int16, rowID uuid.UUID) (*PersistentRecord, error) {
	if schemaRecords, ok := m.records[schemaID]; ok {
		if record, ok := schemaRecords[rowID]; ok {
			return record, nil
		}
	}
	return nil, nil
}

func (m *mockPersistentRecordRepository) QueryPersistentRecords(ctx context.Context, query *PersistentRecordQuery) (*PersistentRecordPage, error) {
	if m.lastQuery == nil {
		m.lastQuery = query
	}
	m.queries = append(m.queries, query)
	if m.queryFunc != nil {
		return m.queryFunc(ctx, query)
	}

	schemaRecords := m.records[query.SchemaID]
	rowIDs := make([]uuid.UUID, 0, len(schemaRecords))
	for id := range schemaRecords {
		rowIDs = append(rowIDs, id)
	}

	sort.Slice(rowIDs, func(i, j int) bool {
		return rowIDs[i].String() < rowIDs[j].String()
	})

	total := len(rowIDs)
	start := min(query.Offset, total)
	end := total
	if query.Limit > 0 && start+query.Limit < end {
		end = start + query.Limit
	}

	selected := make([]*PersistentRecord, 0, end-start)
	for _, id := range rowIDs[start:end] {
		selected = append(selected, schemaRecords[id])
	}

	var totalPages int
	if query.Limit > 0 && total > 0 {
		totalPages = int((int64(total) + int64(query.Limit) - 1) / int64(query.Limit))
	}

	currentPage := 1
	if query.Limit > 0 && query.Offset > 0 {
		currentPage = (query.Offset / query.Limit) + 1
	}

	return &PersistentRecordPage{
		Records:      selected,
		TotalRecords: int64(total),
		TotalPages:   totalPages,
		CurrentPage:  currentPage,
	}, nil
}

func (m *mockFederatedQueryEngine) Query(ctx context.Context, tables StorageTables, fq *FederatedAttributeQuery, opts *FederatedQueryOptions) (*PersistentRecordPage, error) {
	if fq == nil {
		return nil, fmt.Errorf("federated query cannot be nil")
	}
	m.lastTables = tables
	m.lastQuery = fq
	m.lastOpts = opts
	if m.queryFunc != nil {
		return m.queryFunc(ctx, tables, fq, opts)
	}
	return &PersistentRecordPage{}, nil
}

func cloneRecordStore(src map[int16]map[uuid.UUID]*PersistentRecord) map[int16]map[uuid.UUID]*PersistentRecord {
	cloned := make(map[int16]map[uuid.UUID]*PersistentRecord, len(src))
	for schemaID, schemaRecords := range src {
		inner := make(map[uuid.UUID]*PersistentRecord, len(schemaRecords))
		maps.Copy(inner, schemaRecords)
		cloned[schemaID] = inner
	}
	return cloned
}

func (m *mockPersistentRecordRepository) BatchInsertPersistentRecords(ctx context.Context, tables StorageTables, records []*PersistentRecord) error {
	snapshot := cloneRecordStore(m.records)
	insertedCount := len(m.insertedRecords)

	for i, record := range records {
		if m.atomicInsertFailAt > 0 && i+1 == m.atomicInsertFailAt {
			m.records = snapshot
			m.insertedRecords = m.insertedRecords[:insertedCount]
			return fmt.Errorf("forced atomic insert failure at index %d", i)
		}
		if err := m.InsertPersistentRecord(ctx, tables, record); err != nil {
			m.records = snapshot
			m.insertedRecords = m.insertedRecords[:insertedCount]
			return err
		}
	}
	return nil
}

func (m *mockPersistentRecordRepository) BatchUpdatePersistentRecords(ctx context.Context, tables StorageTables, records []*PersistentRecord) error {
	snapshot := cloneRecordStore(m.records)

	for i, record := range records {
		if m.atomicUpdateFailAt > 0 && i+1 == m.atomicUpdateFailAt {
			m.records = snapshot
			return fmt.Errorf("forced atomic update failure at index %d", i)
		}
		if err := m.UpdatePersistentRecord(ctx, tables, record); err != nil {
			m.records = snapshot
			return err
		}
	}
	return nil
}

func (m *mockPersistentRecordRepository) BatchDeletePersistentRecords(ctx context.Context, tables StorageTables, keys []PersistentRecordKey) error {
	snapshot := cloneRecordStore(m.records)
	deleteCalls := m.deleteCalls

	for i, key := range keys {
		if m.atomicDeleteFailAt > 0 && i+1 == m.atomicDeleteFailAt {
			m.records = snapshot
			m.deleteCalls = deleteCalls
			return fmt.Errorf("forced atomic delete failure at index %d", i)
		}
		if err := m.DeletePersistentRecord(ctx, tables, key.SchemaID, key.RowID); err != nil {
			m.records = snapshot
			m.deleteCalls = deleteCalls
			return err
		}
	}
	return nil
}

func buildPersistentRecord(t *testing.T, transformer PersistentRecordTransformer, schemaID int16, rowID uuid.UUID, data map[string]any) *PersistentRecord {
	t.Helper()
	record, err := transformer.ToPersistentRecord(context.Background(), schemaID, rowID, data)
	if err != nil {
		t.Fatalf("failed to build persistent record: %v", err)
	}
	return record
}

// TestEntityManager_CrossSchemaSearch tests cross-schema search with single query
func TestEntityManager_CrossSchemaSearch(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := NewPersistentRecordTransformer(registry)

	// Create mock repository with multi-schema support
	mockRepo := newMockPersistentRecordRepository()

	em := NewEntityManager(transformer, mockRepo, nil, registry, config)

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
	transformer := NewPersistentRecordTransformer(registry)

	mockRepo := newMockPersistentRecordRepository()

	em := NewEntityManager(transformer, mockRepo, nil, registry, config)

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
	transformer := NewPersistentRecordTransformer(registry)

	mockRepo := newMockPersistentRecordRepository()

	em := NewEntityManager(transformer, mockRepo, nil, registry, config)

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
	transformer := NewPersistentRecordTransformer(registry)

	mockRepo := newMockPersistentRecordRepository()

	em := NewEntityManager(transformer, mockRepo, nil, registry, config)

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
	transformer := NewPersistentRecordTransformer(registry)

	mockRepo := newMockPersistentRecordRepository()

	em := NewEntityManager(transformer, mockRepo, nil, registry, config)

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

func TestEntityManager_QueryWithCondition(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := NewPersistentRecordTransformer(registry)

	schemaID, cache, err := registry.GetSchemaAttributeCacheByName("visit")
	if err != nil {
		t.Fatalf("failed to get schema metadata: %v", err)
	}

	rowID := uuid.New()
	mockRepo := newMockPersistentRecordRepository()
	mockRepo.storeRecord(buildPersistentRecord(t, transformer, schemaID, rowID, map[string]any{
		"id":               "visit-advanced",
		"leadId":           "lead-1",
		"userId":           "user-1",
		"propertyId":       "property-1",
		"scheduledStartAt": "2024-01-01T00:00:00Z",
		"status":           "scheduled",
	}))

	em := NewEntityManager(transformer, mockRepo, nil, registry, config)

	req := &forma.QueryRequest{
		SchemaName: "visit",
		Condition: &forma.CompositeCondition{
			Logic: forma.LogicAnd,
			Conditions: []forma.Condition{
				&forma.KvCondition{
					Attr:  "status",
					Value: "equals:scheduled",
				},
			},
		},
		Page:         1,
		ItemsPerPage: 10,
		SortBy:       []string{"scheduledStartAt"},
		SortOrder:    forma.SortOrderDesc,
	}

	result, err := em.Query(ctx, req)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result == nil {
		t.Fatal("Query returned nil result")
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

	if len(mockRepo.lastQuery.AttributeOrders) != 1 {
		t.Fatalf("expected 1 attribute order, got %d", len(mockRepo.lastQuery.AttributeOrders))
	}

	atMeta, ok := cache["scheduledStartAt"]
	if !ok {
		t.Fatalf("expected scheduledStartAt metadata")
	}

	attrOrder := mockRepo.lastQuery.AttributeOrders[0]
	if attrOrder.AttrID != atMeta.AttributeID {
		t.Fatalf("expected attrID %d, got %d", atMeta.AttributeID, attrOrder.AttrID)
	}
	if attrOrder.ValueType != atMeta.ValueType {
		t.Fatalf("expected value type %s, got %s", atMeta.ValueType, attrOrder.ValueType)
	}
	if attrOrder.SortOrder != forma.SortOrderDesc {
		t.Fatalf("expected sort order desc, got %s", attrOrder.SortOrder)
	}
}

func TestEntityManager_QueryWithConditionInvalidSortAttribute(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := NewPersistentRecordTransformer(registry)

	mockRepo := newMockPersistentRecordRepository()

	em := NewEntityManager(transformer, mockRepo, nil, registry, config)

	req := &forma.QueryRequest{
		SchemaName: "visit",
		Condition: &forma.CompositeCondition{
			Logic: forma.LogicAnd,
			Conditions: []forma.Condition{
				&forma.KvCondition{
					Attr:  "status",
					Value: "equals:scheduled",
				},
			},
		},
		SortBy: []string{"nonexistent"},
	}

	_, err = em.Query(ctx, req)
	if err == nil {
		t.Fatal("expected error for invalid sort attribute, got nil")
	}

	if !strings.Contains(err.Error(), "unknown attribute") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEntityManager_QueryUsesFederatedPathWhenEnabled(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := NewPersistentRecordTransformer(registry)

	schemaID, cache, err := registry.GetSchemaAttributeCacheByName("visit")
	if err != nil {
		t.Fatalf("failed to get schema metadata: %v", err)
	}

	rowID := uuid.New()
	record := buildPersistentRecord(t, transformer, schemaID, rowID, map[string]any{
		"id":               "visit-federated",
		"leadId":           "lead-1",
		"userId":           "user-1",
		"propertyId":       "property-1",
		"scheduledStartAt": "2024-01-01T00:00:00Z",
		"status":           "scheduled",
	})
	mockRepo := newMockPersistentRecordRepository()
	mockEngine := &mockFederatedQueryEngine{}
	mockEngine.queryFunc = func(ctx context.Context, tables StorageTables, fq *FederatedAttributeQuery, opts *FederatedQueryOptions) (*PersistentRecordPage, error) {
		return &PersistentRecordPage{Records: []*PersistentRecord{record}, TotalRecords: 1, TotalPages: 1, CurrentPage: 1}, nil
	}

	em := NewEntityManager(transformer, mockRepo, mockEngine, registry, config)
	req := &forma.QueryRequest{
		SchemaName:   "visit",
		Page:         1,
		ItemsPerPage: 10,
		SortBy:       []string{"scheduledStartAt"},
		SortOrder:    forma.SortOrderDesc,
		Federated: &forma.FederatedQueryRequest{
			Enabled:                  true,
			PreferredTiers:           []string{"hot", "warm", "cold"},
			UseMainAsAnchor:          true,
			S3ParquetPathTemplate:    "s3://bucket/prefix/{{.SchemaID}}/base/*.parquet, s3://bucket/prefix/{{.SchemaID}}/delta/*.parquet",
			AllowPartialDegradedMode: true,
		},
	}

	result, err := em.Query(ctx, req)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result == nil || len(result.Data) != 1 {
		t.Fatalf("expected one result, got %+v", result)
	}
	if mockEngine.lastQuery == nil {
		t.Fatal("expected federated query to be captured")
	}
	if mockEngine.lastQuery.SchemaID != schemaID {
		t.Fatalf("expected schema id %d, got %d", schemaID, mockEngine.lastQuery.SchemaID)
	}
	if got := mockEngine.lastQuery.PreferredTiers; len(got) != 3 || got[0] != DataTierHot || got[1] != DataTierWarm || got[2] != DataTierCold {
		t.Fatalf("unexpected preferred tiers: %+v", got)
	}
	if !mockEngine.lastQuery.UseMainAsAnchor {
		t.Fatal("expected use main as anchor to be forwarded")
	}
	if mockEngine.lastQuery.DuckDBHints == nil || mockEngine.lastQuery.DuckDBHints.S3ParquetPathTemplate == "" {
		t.Fatal("expected duckdb hints to be forwarded")
	}
	if mockEngine.lastOpts == nil || !mockEngine.lastOpts.AllowPartialDegradedMode {
		t.Fatal("expected federated options to be forwarded")
	}
	if len(mockEngine.lastQuery.AttributeOrders) != 1 {
		t.Fatalf("expected 1 attribute order, got %d", len(mockEngine.lastQuery.AttributeOrders))
	}
	atMeta, ok := cache["scheduledStartAt"]
	if !ok {
		t.Fatalf("expected scheduledStartAt metadata")
	}
	if mockEngine.lastQuery.AttributeOrders[0].AttrID != atMeta.AttributeID {
		t.Fatalf("expected attrID %d, got %d", atMeta.AttributeID, mockEngine.lastQuery.AttributeOrders[0].AttrID)
	}
}

func TestEntityManager_QueryWithNilConfigUsesDefaults(t *testing.T) {
	ctx := context.Background()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()

	em := NewEntityManager(transformer, mockRepo, nil, registry, nil)

	req := &forma.QueryRequest{
		SchemaName: "visit",
		Page:       0,
		Condition: &forma.KvCondition{
			Attr:  "status",
			Value: "equals:scheduled",
		},
	}

	result, err := em.Query(ctx, req)
	if err != nil {
		t.Fatalf("query failed with nil config: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if req.ItemsPerPage != 50 {
		t.Fatalf("expected default items_per_page=50, got %d", req.ItemsPerPage)
	}
}

// Helper function to create test config
func createTestConfig() *forma.Config {
	return &forma.Config{
		Query: forma.QueryConfig{
			DefaultPageSize: 50,
			MaxPageSize:     100,
		},
		Entity: forma.EntityConfig{
			SchemaDirectory: "../cmd/server/schemas",
		},
	}
}

// TestFileSchemaRegistry_InvalidDirectory tests error handling for invalid directory
func TestFileSchemaRegistry_InvalidDirectory(t *testing.T) {
	_, err := newFileSchemaRegistryFromDir("/nonexistent/directory")
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

	_, err = newFileSchemaRegistryFromDir(tmpDir)
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

	_, err = newFileSchemaRegistryFromDir(tmpDir)
	if err == nil {
		t.Error("Expected error for invalid JSON, got nil")
	}
}
