package internal

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"
)

const literalKeySchema = 706

// literalKeyRegistry declares, with array ancestry, an array of objects
// order.items[] holding a member, a list and an array of objects
// order.items[].lots[] with two members.
func literalKeyRegistry() forma.SchemaRegistry {
	items := []string{"order.items"}
	lots := []string{"order.items", "order.items.lots"}
	cache := forma.SchemaAttributeCache{
		"order.items.sku":       {AttributeID: 1, ValueType: forma.ValueTypeText, ArrayAncestors: items},
		"order.items.tags":      {AttributeID: 2, ValueType: forma.ValueTypeList, ArrayAncestors: items},
		"order.items.lots.code": {AttributeID: 3, ValueType: forma.ValueTypeText, ArrayAncestors: lots},
		"order.items.lots.qty":  {AttributeID: 4, ValueType: forma.ValueTypeText, ArrayAncestors: lots},
	}
	return &stubSchemaRegistry{schemaID: literalKeySchema, schemaName: "literal_key", cache: cache}
}

// A record with fewer indices than its attribute's arrays of objects reads
// under a literal key beside the array when the row holds elements of it
// (#623): a literal key inside an element of the outer array, and an
// empty-list marker beside the array. Get and Query select each by its
// attribute's path, under the key the full read places it under.
func TestProjectionSelectsLiteralPlacedRecords(t *testing.T) {
	ctx := context.Background()
	registry := literalKeyRegistry()
	tr := transform.NewPersistentRecordTransformer(registry)
	repo := newMockPersistentRecordRepository()
	rowID := uuid.New()
	written := func() map[string]any {
		return map[string]any{"order": map[string]any{
			"items": []any{
				map[string]any{"sku": "a", "lots": []any{map[string]any{"code": "c"}}, "lots.qty": "2"},
				map[string]any{"sku": "b"},
			},
			"items.tags": []any{},
		}}
	}
	repo.storeRecord(buildPersistentRecord(t, tr, literalKeySchema, rowID, written()))
	em := mustNewEntityManager(t, tr, repo, nil, registry, createTestConfig(), nil)

	full, err := em.Get(ctx, &forma.QueryRequest{SchemaName: "literal_key", RowID: &rowID})
	require.NoError(t, err)
	require.Equal(t, written(), full.Attributes, "the full read places both records under literal keys")

	for name, tc := range map[string]struct {
		attrs []string
		want  map[string]any
	}{
		"literal key inside an element": {
			attrs: []string{"order.items.lots.qty"},
			want: map[string]any{"order": map[string]any{"items": []any{
				map[string]any{"lots.qty": "2"}, map[string]any{},
			}}},
		},
		"empty-list marker beside the array": {
			attrs: []string{"order.items.tags"},
			want:  map[string]any{"order": map[string]any{"items.tags": []any{}}},
		},
		"the array and its literal keys": {
			attrs: []string{"order.items"},
			want:  written(),
		},
		"a member beside them": {
			attrs: []string{"order.items.sku"},
			want: map[string]any{"order": map[string]any{"items": []any{
				map[string]any{"sku": "a"}, map[string]any{"sku": "b"},
			}}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			assertProjectedReads(t, em, "literal_key", rowID, tc.attrs, tc.want)
		})
	}
}

// A lead update that names requirement.areas.note through a literal key,
// which report-only update validation lets through, stores the note with no
// index beside the stored areas, so the read places it under the literal key
// "areas.note". Get and Query still select it by its path.
func TestProjectionSelectsLiteralKeyUpdateOfLead(t *testing.T) {
	ctx := context.Background()
	registry, err := newFileSchemaRegistryFromDir(shippedSchemaDir)
	require.NoError(t, err)
	validator, err := schemavalidate.New(registry, shippedSchemaDir)
	require.NoError(t, err)
	config := createTestConfig()
	config.Entity.ValidateUpdatesStrict = false
	em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry),
		newMockPersistentRecordRepository(), nil, registry, config, validator)
	created, err := em.Create(ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "lead"},
		Type:             forma.OperationCreate,
		Data: map[string]any{
			"id": uuid.New().String(), "tenantId": "tenant-1", "ownerUserId": "owner-1",
			"pipeline": "buy", "stage": "new", "status": "open",
			"contact":     map[string]any{"isAnonymous": false},
			"requirement": map[string]any{"areas": []any{map[string]any{"city": "Tokyo"}}},
			"createdAt":   "2024-01-01T00:00:00Z", "updatedAt": "2024-01-02T00:00:00Z",
		},
	})
	require.NoError(t, err)
	_, err = em.Update(ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "lead", RowID: created.RowID},
		Type:             forma.OperationUpdate,
		Updates:          map[string]any{"requirement.areas.note": "literal note"},
	})
	require.NoError(t, err)

	requirement := map[string]any{"areas": []any{map[string]any{"city": "Tokyo"}}, "areas.note": "literal note"}
	full, err := em.Get(ctx, &forma.QueryRequest{SchemaName: "lead", RowID: &created.RowID})
	require.NoError(t, err)
	require.Equal(t, requirement, full.Attributes["requirement"], "the full read")

	assertProjectedReads(t, em, "lead", created.RowID, []string{"requirement.areas.note"},
		map[string]any{"requirement": map[string]any{"areas.note": "literal note"}})
	assertProjectedReads(t, em, "lead", created.RowID, []string{"requirement.areas"},
		map[string]any{"requirement": requirement})
}

// assertProjectedReads requires Get of rowID, and Query of its schema, which
// holds that row alone, to answer want for attrs.
func assertProjectedReads(t *testing.T, em forma.EntityManager, schema string, rowID uuid.UUID, attrs []string, want map[string]any) {
	t.Helper()
	ctx := context.Background()
	got, err := em.Get(ctx, &forma.QueryRequest{SchemaName: schema, RowID: &rowID, Attrs: attrs})
	require.NoError(t, err)
	require.Equal(t, want, got.Attributes, "Get %v", attrs)

	list, err := em.Query(ctx, &forma.QueryRequest{SchemaName: schema, Page: 1, ItemsPerPage: 10, Attrs: attrs})
	require.NoError(t, err)
	require.Len(t, list.Data, 1)
	require.Equal(t, want, list.Data[0].Attributes, "Query %v", attrs)
}
