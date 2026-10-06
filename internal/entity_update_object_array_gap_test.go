package internal

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/transform"
)

const objectArrayGapSchema = 626

// objectArrayGapRegistry declares an ordinary array of objects,
// items[].sku and items[].qty, beside stage.
func objectArrayGapRegistry() forma.SchemaRegistry {
	return &stubSchemaRegistry{schemaID: objectArrayGapSchema, schemaName: "object_array_gap", cache: forma.SchemaAttributeCache{
		"items.sku": {AttributeID: 1, ValueType: forma.ValueTypeText},
		"items.qty": {AttributeID: 2, ValueType: forma.ValueTypeText},
		"stage":     {AttributeID: 3, ValueType: forma.ValueTypeText},
	}}
}

// gapItems is an array of objects whose first element stores no record.
func gapItems() []any {
	return []any{map[string]any{}, map[string]any{"sku": "a"}}
}

// An element of an array of objects that stores no record used to read back
// as null, and every update that did not replace the array was then refused
// as a null element the caller never sent (#626). It reads as {}, so every
// update surface accepts the update, answers {} and writes back exactly the
// stored record beside its own.
func TestUnrelatedUpdateOverObjectArrayGapIsAccepted(t *testing.T) {
	ctx := context.Background()
	for _, surface := range updateSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			registry := objectArrayGapRegistry()
			tr := transform.NewPersistentRecordTransformer(registry)
			repo := newMockPersistentRecordRepository()
			rowID := uuid.New()
			repo.storeRecord(buildPersistentRecord(t, tr, objectArrayGapSchema, rowID,
				map[string]any{"items": gapItems(), "stage": "new"}))
			em := mustNewEntityManager(t, tr, repo, nil, registry, createTestConfig(), nil)

			got, err := surface.update(ctx, em, &forma.EntityOperation{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: "object_array_gap", RowID: rowID},
				Type:             forma.OperationUpdate,
				Updates:          map[string]any{"stage": "contacted"},
			})
			if err != nil {
				t.Fatalf("update of stage over an array of objects with an empty element: %v", err)
			}
			assertEqualValue(t, "answered items", gapItems(), got["items"])
			assertEqualValue(t, "answered stage", "contacted", got["stage"])
			assertEqualValue(t, "stored records", []string{"1[1]=a", "3[]=contacted"},
				storedTextRecords(repo.records[objectArrayGapSchema][rowID]))

			read, err := em.Get(ctx, &forma.QueryRequest{SchemaName: "object_array_gap", RowID: &rowID})
			if err != nil {
				t.Fatalf("get after the update: %v", err)
			}
			assertEqualValue(t, "read items", gapItems(), read.Attributes["items"])
		})
	}
}
