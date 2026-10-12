package factory

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/stretchr/testify/require"
)

func TestNewTransactorRefusesAManagerThisPackageDidNotBuild(t *testing.T) {
	// A decorator around a factory manager is still not one: the transactor
	// needs the manager's pool and services, not its interface.
	type decorated struct{ forma.EntityManager }
	for name, em := range map[string]forma.EntityManager{"nil": nil, "decorator": decorated{}} {
		txr, err := NewTransactor(em)
		require.ErrorIs(t, err, forma.ErrInvalidInput, name)
		require.Nil(t, txr, name)
	}
}

func TestNewTransactorAcceptsAFactoryManager_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool := testdb.Connect(t, ctx)
	schemaRegistryTable, entityMainTable, eavDataTable := createTempTables(t, ctx, pool)
	_, err := pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (schema_name, schema_id) VALUES ($1, $2)", schemaRegistryTable), "test", int16(1))
	require.NoError(t, err)
	dir := t.TempDir()
	writeAttributesFile(t, dir, "test", map[string]any{"id": map[string]any{"attributeID": float64(1), "valueType": "text"}})
	writeSchemaFile(t, dir, "test", map[string]any{"type": "object", "properties": map[string]any{"id": map[string]any{"type": "string"}}})

	config := forma.DefaultConfig(newMockSchemaRegistry())
	config.Database.TableNames = forma.TableNames{SchemaRegistry: schemaRegistryTable, EAVData: eavDataTable, EntityMain: entityMainTable}
	config.Entity.SchemaDirectory = dir
	em, err := NewEntityManagerWithConfig(config, pool)
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })

	txr, err := NewTransactor(em)
	require.NoError(t, err)
	var created *forma.DataRecord
	err = txr.InTx(ctx, func(ctx context.Context, tx forma.Tx) error {
		created, err = tx.Create(ctx, &forma.EntityOperation{
			EntityIdentifier: forma.EntityIdentifier{SchemaName: "test"},
			Data:             map[string]any{"id": "through-a-transaction"},
		})
		return err
	})
	require.NoError(t, err)

	got, err := em.Get(ctx, &forma.QueryRequest{SchemaName: "test", RowID: &created.RowID})
	require.NoError(t, err)
	require.Equal(t, "through-a-transaction", got.Attributes["id"])
}
