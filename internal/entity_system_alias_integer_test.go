package internal

import (
	"context"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/transform"
)

// A smallint or integer bound to a system timestamp column is admitted at
// registration (schemameta.ValidateColumnBinding) and reads the store's own
// epoch millis, which are past either range. The #729 refusal must not judge
// it: no write reaches the column and an update cannot name the alias, so a
// refusal would fail every Create response (after the insert), read and
// update of the schema with nothing to repair. What the alias answers is
// #745's to decide; these tests pin only that the row stays usable.

const sysAliasSchema = 745

func sysAliasRegistry() forma.SchemaRegistry {
	return &stubSchemaRegistry{schemaID: sysAliasSchema, schemaName: "sys_alias", cache: forma.SchemaAttributeCache{
		"name":    {AttributeID: 1, ValueType: forma.ValueTypeText, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01}},
		"created": {AttributeID: 2, ValueType: forma.ValueTypeInteger, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnCreatedAt}},
		"touched": {AttributeID: 3, ValueType: forma.ValueTypeSmallInt, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnUpdatedAt}},
	}}
}

func TestSystemTimestampAliasDeclaredIntegerStaysUsable(t *testing.T) {
	ctx := context.Background()
	registry := sysAliasRegistry()
	repo := newMockPersistentRecordRepository()
	// The Postgres insert stamps both columns with the current millis
	// (InsertPersistentRecord); the mock's default insert stamps nothing.
	now := time.Now().UnixMilli()
	repo.insertFunc = func(_ context.Context, record *model.PersistentRecord) error {
		record.CreatedAt, record.UpdatedAt = now, now
		repo.insertedRecords = append(repo.insertedRecords, record)
		repo.storeRecord(record)
		return nil
	}
	em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry), repo, nil, registry, createTestConfig(), nil)

	created, err := em.Create(ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "sys_alias"},
		Type:             forma.OperationCreate,
		Data:             map[string]any{"name": "first"},
	})
	if err != nil {
		t.Fatalf("create on a schema with integer aliases of the system timestamps: %v", err)
	}
	if len(repo.insertedRecords) != 1 {
		t.Fatalf("expected one insert, got %d", len(repo.insertedRecords))
	}
	for _, alias := range []string{"created", "touched"} {
		if _, ok := created.Attributes[alias]; !ok {
			t.Fatalf("create response lacks alias %s: %v", alias, created.Attributes)
		}
	}
	rowID := created.RowID

	if _, err := em.Get(ctx, &forma.QueryRequest{SchemaName: "sys_alias", RowID: &rowID}); err != nil {
		t.Fatalf("read of the created row: %v", err)
	}
	for _, surface := range updateSurfaces {
		t.Run(surface.name, func(t *testing.T) {
			_, err := surface.update(ctx, em, &forma.EntityOperation{
				EntityIdentifier: forma.EntityIdentifier{SchemaName: "sys_alias", RowID: rowID},
				Type:             forma.OperationUpdate,
				Updates:          map[string]any{"name": "renamed " + surface.name},
			})
			if err != nil {
				t.Fatalf("unrelated update: %v", err)
			}
		})
	}
}
