package internal

import (
	"context"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/transform"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

func TestEntityManager_QueryBuildsAttributeOrders(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)

	mockRepo := newMockPersistentRecordRepository()

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)

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
	transformer := transform.NewPersistentRecordTransformer(registry)

	mockRepo := newMockPersistentRecordRepository()

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)

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
	transformer := transform.NewPersistentRecordTransformer(reg)
	mockRepo := newMockPersistentRecordRepository()
	em := mustNewEntityManager(t, transformer, mockRepo, nil, reg, config, nil)

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

func TestEntityManager_QueryWithCondition(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)

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

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)

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
	transformer := transform.NewPersistentRecordTransformer(registry)

	mockRepo := newMockPersistentRecordRepository()

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, config, nil)

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
	transformer := transform.NewPersistentRecordTransformer(registry)

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
	mockEngine.queryFunc = func(ctx context.Context, tables model.StorageTables, fq *model.FederatedAttributeQuery, opts *model.FederatedQueryOptions) (*model.PersistentRecordPage, error) {
		return &model.PersistentRecordPage{Records: []*model.PersistentRecord{record}, TotalRecords: 1, TotalPages: 1, CurrentPage: 1}, nil
	}

	em := mustNewEntityManager(t, transformer, mockRepo, mockEngine, registry, config, nil)
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
	if got := mockEngine.lastQuery.PreferredTiers; len(got) != 3 || got[0] != model.DataTierHot || got[1] != model.DataTierWarm || got[2] != model.DataTierCold {
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
	transformer := transform.NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()

	em := mustNewEntityManager(t, transformer, mockRepo, nil, registry, nil, nil)

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
