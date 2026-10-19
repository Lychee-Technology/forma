package internal

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/transform"
)

func TestFilterAttributes_EmptyAttrs(t *testing.T) {
	attributes := map[string]any{
		"name":  "John",
		"email": "john@example.com",
		"age":   30,
	}

	// When attrs is empty, should return original attributes
	result := FilterAttributes(attributes, nil)
	if len(result) != 3 {
		t.Errorf("expected 3 attributes, got %d", len(result))
	}

	result = FilterAttributes(attributes, []string{})
	if len(result) != 3 {
		t.Errorf("expected 3 attributes, got %d", len(result))
	}
}

func TestFilterAttributes_SimpleAttributes(t *testing.T) {
	attributes := map[string]any{
		"name":  "John",
		"email": "john@example.com",
		"age":   30,
		"phone": "123-456-7890",
	}

	// Filter to only name and email
	result := FilterAttributes(attributes, []string{"name", "email"})
	if len(result) != 2 {
		t.Errorf("expected 2 attributes, got %d", len(result))
	}
	if result["name"] != "John" {
		t.Errorf("expected name to be 'John', got %v", result["name"])
	}
	if result["email"] != "john@example.com" {
		t.Errorf("expected email to be 'john@example.com', got %v", result["email"])
	}
	if _, exists := result["age"]; exists {
		t.Errorf("expected age to be filtered out")
	}
}

func TestFilterAttributes_NestedAttributes(t *testing.T) {
	attributes := map[string]any{
		"name": "John",
		"contact": map[string]any{
			"phone":   "123-456-7890",
			"email":   "john@example.com",
			"address": "123 Main St",
		},
		"age": 30,
	}

	// Filter to nested attribute contact.phone
	result := FilterAttributes(attributes, []string{"name", "contact.phone"})
	if len(result) != 2 {
		t.Errorf("expected 2 top-level attributes, got %d", len(result))
	}
	if result["name"] != "John" {
		t.Errorf("expected name to be 'John', got %v", result["name"])
	}

	contact, ok := result["contact"].(map[string]any)
	if !ok {
		t.Fatalf("expected contact to be a map, got %T", result["contact"])
	}
	if contact["phone"] != "123-456-7890" {
		t.Errorf("expected contact.phone to be '123-456-7890', got %v", contact["phone"])
	}
	if _, exists := contact["email"]; exists {
		t.Errorf("expected contact.email to be filtered out")
	}
}

func TestFilterAttributes_MultipleNestedAttributes(t *testing.T) {
	attributes := map[string]any{
		"name": "John",
		"contact": map[string]any{
			"phone":   "123-456-7890",
			"email":   "john@example.com",
			"address": "123 Main St",
		},
		"status": "active",
	}

	// Filter to multiple nested attributes
	result := FilterAttributes(attributes, []string{"contact.phone", "contact.email"})
	if len(result) != 1 {
		t.Errorf("expected 1 top-level attribute, got %d", len(result))
	}

	contact, ok := result["contact"].(map[string]any)
	if !ok {
		t.Fatalf("expected contact to be a map, got %T", result["contact"])
	}
	if len(contact) != 2 {
		t.Errorf("expected 2 nested attributes, got %d", len(contact))
	}
	if contact["phone"] != "123-456-7890" {
		t.Errorf("expected contact.phone to be '123-456-7890', got %v", contact["phone"])
	}
	if contact["email"] != "john@example.com" {
		t.Errorf("expected contact.email to be 'john@example.com', got %v", contact["email"])
	}
}

func TestFilterAttributes_NonExistentAttribute(t *testing.T) {
	attributes := map[string]any{
		"name":  "John",
		"email": "john@example.com",
	}

	// Filter with non-existent attribute
	result := FilterAttributes(attributes, []string{"name", "phone"})
	if len(result) != 1 {
		t.Errorf("expected 1 attribute, got %d", len(result))
	}
	if result["name"] != "John" {
		t.Errorf("expected name to be 'John', got %v", result["name"])
	}
}

func TestFilterAttributes_WhitespaceHandling(t *testing.T) {
	attributes := map[string]any{
		"name":  "John",
		"email": "john@example.com",
	}

	// Filter with whitespace in attrs
	result := FilterAttributes(attributes, []string{" name ", "  email  "})
	if len(result) != 2 {
		t.Errorf("expected 2 attributes, got %d", len(result))
	}
}

func TestFilterAttributes_EntireNestedObject(t *testing.T) {
	attributes := map[string]any{
		"name": "John",
		"contact": map[string]any{
			"phone":   "123-456-7890",
			"email":   "john@example.com",
			"address": "123 Main St",
		},
	}

	// Filter to include entire nested object
	result := FilterAttributes(attributes, []string{"contact"})
	if len(result) != 1 {
		t.Errorf("expected 1 attribute, got %d", len(result))
	}

	contact, ok := result["contact"].(map[string]any)
	if !ok {
		t.Fatalf("expected contact to be a map, got %T", result["contact"])
	}
	if len(contact) != 3 {
		t.Errorf("expected 3 nested attributes, got %d", len(contact))
	}
}

func TestFilterDataRecord(t *testing.T) {
	rowID := uuid.New()
	record := &forma.DataRecord{
		SchemaName: "test_schema",
		RowID:      rowID,
		Attributes: map[string]any{
			"name":   "John",
			"email":  "john@example.com",
			"status": "active",
		},
	}

	// Filter data record
	result := FilterDataRecord(record, []string{"name", "email"})
	if result.SchemaName != "test_schema" {
		t.Errorf("expected schema_name to be 'test_schema', got %s", result.SchemaName)
	}
	if result.RowID != rowID {
		t.Errorf("expected row_id to be preserved")
	}
	if len(result.Attributes) != 2 {
		t.Errorf("expected 2 attributes, got %d", len(result.Attributes))
	}
}

func TestFilterDataRecord_NilRecord(t *testing.T) {
	result := FilterDataRecord(nil, []string{"name"})
	if result != nil {
		t.Errorf("expected nil result for nil record")
	}
}

func TestFilterDataRecord_EmptyAttrs(t *testing.T) {
	rowID := uuid.New()
	record := &forma.DataRecord{
		SchemaName: "test_schema",
		RowID:      rowID,
		Attributes: map[string]any{
			"name":   "John",
			"email":  "john@example.com",
			"status": "active",
		},
	}

	// Empty attrs should return original record
	result := FilterDataRecord(record, nil)
	if result != record {
		t.Errorf("expected same record reference when attrs is empty")
	}

	result = FilterDataRecord(record, []string{})
	if result != record {
		t.Errorf("expected same record reference when attrs is empty")
	}
}

// A path through an array of objects projects every element in place: each
// keeps its position and only the requested members, an element holding
// none of them reads as {}, and an array none of whose elements holds one is
// left out, as an object is. Paths sharing a container fill one copy of it,
// and the source is not modified.
func TestFilterAttributes_ArrayOfObjects(t *testing.T) {
	source := func() map[string]any {
		return map[string]any{
			"stage": "new",
			"propertyInterests": []any{
				map[string]any{"propertyId": "p1", "status": "viewed", "snapshot": map[string]any{"title": "Flat A", "price": 10}},
				map[string]any{"propertyId": "p2", "notes": "near the station"},
			},
			"order": map[string]any{"items": []any{
				map[string]any{"lots": []any{map[string]any{"code": "c00", "n": 1}, map[string]any{"code": "c01"}}},
				map[string]any{"sku": "b"},
			}},
			"contact": map[string]any{"phones": []any{"111", "222"}},
		}
	}
	for name, tc := range map[string]struct {
		attrs []string
		want  map[string]any
	}{
		"member of every element": {
			attrs: []string{"propertyInterests.propertyId"},
			want: map[string]any{"propertyInterests": []any{
				map[string]any{"propertyId": "p1"}, map[string]any{"propertyId": "p2"},
			}},
		},
		"members some elements lack": {
			attrs: []string{"propertyInterests.snapshot.title", "propertyInterests.notes"},
			want: map[string]any{"propertyInterests": []any{
				map[string]any{"snapshot": map[string]any{"title": "Flat A"}},
				map[string]any{"notes": "near the station"},
			}},
		},
		"member of one element": {
			attrs: []string{"propertyInterests.snapshot"},
			want: map[string]any{"propertyInterests": []any{
				map[string]any{"snapshot": map[string]any{"title": "Flat A", "price": 10}},
				map[string]any{},
			}},
		},
		"member of no element": {
			attrs: []string{"propertyInterests.missing", "stage"},
			want:  map[string]any{"stage": "new"},
		},
		"array of objects inside an array of objects": {
			attrs: []string{"order.items.lots.code"},
			want: map[string]any{"order": map[string]any{"items": []any{
				map[string]any{"lots": []any{map[string]any{"code": "c00"}, map[string]any{"code": "c01"}}},
				map[string]any{},
			}}},
		},
		"whole array beside one of its members": {
			attrs: []string{"propertyInterests.propertyId", "propertyInterests"},
			want:  map[string]any{"propertyInterests": source()["propertyInterests"]},
		},
		"primitive array": {
			attrs: []string{"contact.phones", "contact.phones.x"},
			want:  map[string]any{"contact": map[string]any{"phones": []any{"111", "222"}}},
		},
		"path beneath a primitive array": {
			attrs: []string{"contact.phones.x"},
			want:  map[string]any{},
		},
	} {
		t.Run(name, func(t *testing.T) {
			attributes := source()
			require.Equal(t, tc.want, FilterAttributes(attributes, tc.attrs))
			require.Equal(t, source(), attributes, "the source must not change")
		})
	}
}

// The rebuild places a record with fewer indices than its attribute's arrays
// of objects under a literal key beside the array when the row holds
// elements of it (#623). A path selects the literal key its segments spell
// and keeps it under that key, a path that ends partway through one keeps it
// whole, and a literal key no path reaches is left out. The source is not
// modified.
func TestFilterAttributes_LiteralKeys(t *testing.T) {
	source := func() map[string]any {
		return map[string]any{
			"requirement": map[string]any{
				"areas": []any{map[string]any{"city": "Tokyo"}}, "areas.note": "literal note",
			},
			"order": map[string]any{
				"items": []any{
					map[string]any{"sku": "a", "lots": []any{map[string]any{"code": "c"}}, "lots.qty": 2},
					map[string]any{"sku": "b"},
				},
				"items.tags": []any{},
			},
		}
	}
	for name, tc := range map[string]struct {
		attrs []string
		want  map[string]any
	}{
		"literal key beside an array": {
			attrs: []string{"requirement.areas.note"},
			want:  map[string]any{"requirement": map[string]any{"areas.note": "literal note"}},
		},
		"literal key inside an element": {
			attrs: []string{"order.items.lots.qty"},
			want: map[string]any{"order": map[string]any{"items": []any{
				map[string]any{"lots.qty": 2}, map[string]any{},
			}}},
		},
		"empty-list marker under a literal key": {
			attrs: []string{"order.items.tags"},
			want:  map[string]any{"order": map[string]any{"items.tags": []any{}}},
		},
		"path ending partway through a literal key": {
			attrs: []string{"requirement.areas"},
			want:  map[string]any{"requirement": source()["requirement"]},
		},
		"member beside a literal key": {
			attrs: []string{"requirement.areas.city"},
			want: map[string]any{"requirement": map[string]any{
				"areas": []any{map[string]any{"city": "Tokyo"}},
			}},
		},
		"members under both spellings": {
			attrs: []string{"order.items.lots.code", "order.items.lots.qty"},
			want: map[string]any{"order": map[string]any{"items": []any{
				map[string]any{"lots": []any{map[string]any{"code": "c"}}, "lots.qty": 2}, map[string]any{},
			}}},
		},
		"literal key no path reaches": {
			attrs: []string{"requirement.areas.ward", "order.items.lots.qty.x"},
			want:  map[string]any{},
		},
	} {
		t.Run(name, func(t *testing.T) {
			attributes := source()
			require.Equal(t, tc.want, FilterAttributes(attributes, tc.attrs))
			require.Equal(t, source(), attributes, "the source must not change")
		})
	}
}

// Get and Query project a lead's property interests, an array of objects
// whose elements nest a snapshot, in the shape the read rebuilds them in
// under the array ancestry the file registry derives (#623): each requested
// member stays in its element.
func TestProjectionOfObjectArrayMembers(t *testing.T) {
	ctx := context.Background()
	registry, err := newFileSchemaRegistryFromDir(shippedSchemaDir)
	require.NoError(t, err)
	schemaID, _, err := registry.GetSchemaByName("lead")
	require.NoError(t, err)
	tr := transform.NewPersistentRecordTransformer(registry)
	repo := newMockPersistentRecordRepository()
	rowID := uuid.New()
	repo.storeRecord(buildPersistentRecord(t, tr, schemaID, rowID, map[string]any{
		"id": uuid.New().String(), "tenantId": "tenant-1", "ownerUserId": "owner-1",
		"pipeline": "buy", "stage": "new", "status": "open",
		"contact":   map[string]any{"isAnonymous": false},
		"createdAt": "2024-01-01T00:00:00Z", "updatedAt": "2024-01-02T00:00:00Z",
		"propertyInterests": []any{
			map[string]any{"propertyId": "p1", "status": "viewed", "snapshot": map[string]any{"title": "Flat A"}},
			map[string]any{"propertyId": "p2", "status": "viewed", "notes": "near the station"},
		},
	}))
	em := mustNewEntityManager(t, tr, repo, nil, registry, createTestConfig(), nil)

	attrs := []string{"propertyInterests.propertyId", "propertyInterests.snapshot.title"}
	want := map[string]any{"propertyInterests": []any{
		map[string]any{"propertyId": "p1", "snapshot": map[string]any{"title": "Flat A"}},
		map[string]any{"propertyId": "p2"},
	}}
	got, err := em.Get(ctx, &forma.QueryRequest{SchemaName: "lead", RowID: &rowID, Attrs: attrs})
	require.NoError(t, err)
	require.Equal(t, want, got.Attributes, "Get")

	list, err := em.Query(ctx, &forma.QueryRequest{SchemaName: "lead", Page: 1, ItemsPerPage: 10, Attrs: attrs})
	require.NoError(t, err)
	require.Len(t, list.Data, 1)
	require.Equal(t, want, list.Data[0].Attributes, "Query")
}
