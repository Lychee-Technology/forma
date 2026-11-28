package internal

import (
	"context"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/transform"
)

// #745: a smallint or integer bound to a system timestamp column is refused
// at registration, at every load and by the guard over a caller's own
// registry (schemameta.ValidateColumnBinding, GuardReservedAttrNames), so
// the read decode judges a system alias like any other integer image. These
// tests pin what admission still lets through: a bigint alias of the
// timestamps and both integer widths on ltbase_schema_id stay usable through
// Create, read and every update surface, and read the store's exact values.

const sysAliasSchema = 745

func sysAliasRegistry() forma.SchemaRegistry {
	return &stubSchemaRegistry{schemaID: sysAliasSchema, schemaName: "sys_alias", cache: forma.SchemaAttributeCache{
		"name":    {AttributeID: 1, ValueType: forma.ValueTypeText, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01}},
		"created": {AttributeID: 2, ValueType: forma.ValueTypeBigInt, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnCreatedAt}},
		"touched": {AttributeID: 3, ValueType: forma.ValueTypeBigInt, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnUpdatedAt}},
		"kind":    {AttributeID: 4, ValueType: forma.ValueTypeSmallInt, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnSchemaID}},
	}}
}

// sysAliasRepo stamps both timestamps with now on insert, as the Postgres
// insert does (InsertPersistentRecord); the mock's default stamps nothing.
func sysAliasRepo(now int64) *mockPersistentRecordRepository {
	repo := newMockPersistentRecordRepository()
	repo.insertFunc = func(_ context.Context, record *model.PersistentRecord) error {
		record.CreatedAt, record.UpdatedAt = now, now
		repo.insertedRecords = append(repo.insertedRecords, record)
		repo.storeRecord(record)
		return nil
	}
	return repo
}

func TestSystemAliasesAdmissionKeepsReadExactly(t *testing.T) {
	ctx := context.Background()
	registry := sysAliasRegistry()
	now := time.Now().UnixMilli()
	repo := sysAliasRepo(now)
	em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry), repo, nil, registry, createTestConfig(), nil)

	created, err := em.Create(ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "sys_alias"},
		Type:             forma.OperationCreate,
		Data:             map[string]any{"name": "first"},
	})
	if err != nil {
		t.Fatalf("create on a schema with admitted system aliases: %v", err)
	}
	if len(repo.insertedRecords) != 1 {
		t.Fatalf("expected one insert, got %d", len(repo.insertedRecords))
	}
	want := map[string]any{"created": now, "touched": now, "kind": int16(sysAliasSchema)}
	for alias, value := range want {
		if got := created.Attributes[alias]; got != value {
			t.Fatalf("create response alias %s = %v (%T), want %v (%T)", alias, got, got, value, value)
		}
	}
	rowID := created.RowID

	read, err := em.Get(ctx, &forma.QueryRequest{SchemaName: "sys_alias", RowID: &rowID})
	if err != nil {
		t.Fatalf("read of the created row: %v", err)
	}
	for alias, value := range want {
		if got := read.Attributes[alias]; got != value {
			t.Fatalf("read alias %s = %v (%T), want %v (%T)", alias, got, got, value, value)
		}
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
