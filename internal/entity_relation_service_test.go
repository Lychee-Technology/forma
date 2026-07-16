package internal

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/transform"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// mockByAttrValuesCall records one QueryPersistentRecordsByAttrValues
// invocation on mockPersistentRecordRepository (declared in
// entity_manager_test.go) for batching assertions.
type mockByAttrValuesCall struct {
	schemaID int16
	attr     string
	values   []string
	limit    int
}

func (m *mockPersistentRecordRepository) QueryPersistentRecordsByAttrValues(ctx context.Context, tables model.StorageTables, schemaID int16, attr string, values []string, limit int) (*model.PersistentRecordPage, error) {
	m.byAttrValuesCalls = append(m.byAttrValuesCalls, mockByAttrValuesCall{
		schemaID: schemaID,
		attr:     attr,
		values:   append([]string(nil), values...),
		limit:    limit,
	})

	valueSet := make(map[string]struct{}, len(values))
	for _, v := range values {
		valueSet[v] = struct{}{}
	}

	schemaRecords := m.records[schemaID]
	rowIDs := make([]uuid.UUID, 0, len(schemaRecords))
	for id := range schemaRecords {
		rowIDs = append(rowIDs, id)
	}
	sort.Slice(rowIDs, func(i, j int) bool {
		return rowIDs[i].String() < rowIDs[j].String()
	})

	selected := make([]*model.PersistentRecord, 0, len(values))
	for _, id := range rowIDs {
		if mockRecordMatchesAnyTextValue(schemaRecords[id], valueSet) {
			selected = append(selected, schemaRecords[id])
		}
	}

	return &model.PersistentRecordPage{
		Records:      selected,
		TotalRecords: int64(len(selected)),
		TotalPages:   1,
		CurrentPage:  1,
	}, nil
}

func mockRecordMatchesAnyTextValue(record *model.PersistentRecord, valueSet map[string]struct{}) bool {
	for _, text := range record.TextItems {
		if _, ok := valueSet[text]; ok {
			return true
		}
	}
	for _, eav := range record.OtherAttributes {
		if eav.ValueText == nil {
			continue
		}
		if _, ok := valueSet[*eav.ValueText]; ok {
			return true
		}
	}
	return false
}

// seedVisitEnrichmentFixture stores two parent leads (contact names
// "Parent 0"/"Parent 1") and three visits referencing them; it returns the
// lead schema id, the lead ids, and the visit->lead assignment.
func seedVisitEnrichmentFixture(t *testing.T, transformer model.PersistentRecordTransformer, mockRepo *mockPersistentRecordRepository, registry forma.SchemaRegistry) (int16, []string, map[string]string) {
	t.Helper()

	leadSchemaID, _, err := registry.GetSchemaByName("lead")
	if err != nil {
		t.Fatalf("failed to get lead schema: %v", err)
	}
	visitSchemaID, _, err := registry.GetSchemaByName("visit")
	if err != nil {
		t.Fatalf("failed to get visit schema: %v", err)
	}

	leadIDs := []string{uuid.New().String(), uuid.New().String()}
	for i, leadID := range leadIDs {
		mockRepo.storeRecord(buildPersistentRecord(t, transformer, leadSchemaID, uuid.New(), map[string]any{
			"id":          leadID,
			"tenantId":    "tenant-1",
			"ownerUserId": "owner-1",
			"pipeline":    "buy",
			"stage":       "new",
			"status":      "open",
			"contact": map[string]any{
				"isAnonymous":  false,
				"name":         fmt.Sprintf("Parent %d", i),
				"primaryPhone": fmt.Sprintf("555-000%d", i),
			},
			"createdAt": "2024-01-01T00:00:00Z",
			"updatedAt": "2024-01-02T00:00:00Z",
		}))
	}

	visitLeads := map[string]string{
		"visit-batch-1": leadIDs[0],
		"visit-batch-2": leadIDs[0],
		"visit-batch-3": leadIDs[1],
	}
	for visitID, leadID := range visitLeads {
		mockRepo.storeRecord(buildPersistentRecord(t, transformer, visitSchemaID, uuid.New(), map[string]any{
			"id":               visitID,
			"leadId":           leadID,
			"userId":           "user-1",
			"propertyId":       "prop-1",
			"scheduledStartAt": "2024-02-01T00:00:00Z",
			"status":           "scheduled",
		}))
	}

	return leadSchemaID, leadIDs, visitLeads
}

// TestEntityManager_QueryEnrichesRelationsWithSingleBatchedParentLookup pins
// the #268 fix: enriching a page of child records must issue exactly one
// set-based parent lookup carrying every distinct foreign key, never one
// OR-of-N condition query through QueryPersistentRecords.
func TestEntityManager_QueryEnrichesRelationsWithSingleBatchedParentLookup(t *testing.T) {
	ctx := context.Background()
	config := createTestConfig()
	registry, err := newFileSchemaRegistryFromDir("../cmd/server/schemas")
	if err != nil {
		t.Fatalf("failed to create schema registry: %v", err)
	}
	transformer := transform.NewPersistentRecordTransformer(registry)
	mockRepo := newMockPersistentRecordRepository()
	leadSchemaID, leadIDs, visitLeads := seedVisitEnrichmentFixture(t, transformer, mockRepo, registry)

	em := NewEntityManager(transformer, mockRepo, nil, registry, config)

	result, err := em.Query(ctx, &forma.QueryRequest{SchemaName: "visit", Page: 1, ItemsPerPage: 10})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(result.Data) != len(visitLeads) {
		t.Fatalf("expected %d visit records, got %d", len(visitLeads), len(result.Data))
	}

	parentNames := map[string]string{
		leadIDs[0]: "Parent 0",
		leadIDs[1]: "Parent 1",
	}
	for _, record := range result.Data {
		leadID, _ := record.Attributes["leadId"].(string)
		snapshot, ok := record.Attributes["contactSnapshot"].(map[string]any)
		if !ok {
			t.Fatalf("expected contactSnapshot populated for visit %v", record.Attributes["id"])
		}
		if snapshot["name"] != parentNames[leadID] {
			t.Fatalf("expected contactSnapshot.name %q for lead %s, got %v", parentNames[leadID], leadID, snapshot["name"])
		}
	}

	if len(mockRepo.byAttrValuesCalls) != 1 {
		t.Fatalf("expected exactly 1 batched parent lookup, got %d", len(mockRepo.byAttrValuesCalls))
	}
	call := mockRepo.byAttrValuesCalls[0]
	if call.schemaID != leadSchemaID {
		t.Fatalf("expected parent lookup against lead schema %d, got %d", leadSchemaID, call.schemaID)
	}
	if call.attr != "id" {
		t.Fatalf("expected parent lookup by attribute 'id', got %q", call.attr)
	}
	gotValues := append([]string(nil), call.values...)
	wantValues := append([]string(nil), leadIDs...)
	sort.Strings(gotValues)
	sort.Strings(wantValues)
	if !reflect.DeepEqual(gotValues, wantValues) {
		t.Fatalf("expected batched values %v, got %v", wantValues, gotValues)
	}

	for _, q := range mockRepo.queries {
		if q.SchemaID == leadSchemaID {
			t.Fatalf("expected no condition-based parent query against lead schema, got condition %#v", q.Condition)
		}
	}
}
