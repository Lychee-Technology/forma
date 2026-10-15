package factory

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/lychee-technology/forma/internal/uniquekey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A deployment that never used unique keys has neither key table and
// declares no key: the check sends nothing, so it needs no store.
func TestRequireUniqueKeysBuiltSkipsADeploymentWithoutKeys(t *testing.T) {
	catalog, err := internal.NewUniqueKeyCatalog(newMockSchemaRegistry())
	require.NoError(t, err)
	config := forma.DefaultConfig(newMockSchemaRegistry())
	config.Database.TableNames.EntityMain = "public.entity_main"

	require.NoError(t, requireUniqueKeysBuilt(context.Background(), nil, config, []string{"entity_main"}, catalog))
}

// The server refuses to start while a declared key is unbuilt or its tables
// are missing (#651), and starts once the key is built.
func TestNewEntityManagerWithConfig_Integration_RefusesAnUnbuiltUniqueKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := testdb.Connect(t, ctx)
	schemaRegistryTable, entityMainTable, eavDataTable := createTempTables(t, ctx, pool)
	_, err := pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (schema_name, schema_id) VALUES ('account', 1)", schemaRegistryTable))
	require.NoError(t, err)

	dir := t.TempDir()
	writeAttributesFile(t, dir, "account", map[string]any{
		"handle": map[string]any{"attributeID": float64(1), "valueType": "text"},
	})
	writeSchemaFile(t, dir, "account", map[string]any{
		"type":                "object",
		"properties":          map[string]any{"handle": map[string]any{"type": "string"}},
		"x-forma-unique-keys": []any{map[string]any{"id": 1, "name": "handle", "attributes": []any{"handle"}}},
	})
	registry, err := schemameta.NewFileSchemaRegistry(pool, schemaRegistryTable, dir)
	require.NoError(t, err)
	config := forma.DefaultConfig(registry)
	config.Database.TableNames = forma.TableNames{SchemaRegistry: schemaRegistryTable, EAVData: eavDataTable, EntityMain: entityMainTable}
	config.Entity.SchemaDirectory = dir

	_, err = NewEntityManagerWithConfig(config, pool)
	require.Error(t, err)
	assert.Contains(t, err.Error(), fmt.Sprintf("table public.%s is missing", uniquekey.KeyTable(entityMainTable)))
	assert.Contains(t, err.Error(), "run forma-tools init-db")

	keyTable, stateTable := uniquekey.KeyTable(entityMainTable), uniquekey.StateTable(entityMainTable)
	for _, stmt := range uniquekey.StatementsFor(entityMainTable) {
		_, err := pool.Exec(ctx, stmt)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s, %s", keyTable, stateTable))
	})

	_, err = NewEntityManagerWithConfig(config, pool)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unique key handle of schema account is not built: run forma-tools build-unique-keys --schema account --key handle")

	catalog, err := internal.NewUniqueKeyCatalog(registry)
	require.NoError(t, err)
	_, err = catalog.BuildUniqueKeys(ctx, pool, model.StorageTables{EntityMain: entityMainTable, EAVData: eavDataTable},
		internal.UniqueKeyBuildRequest{Schema: "account"})
	require.NoError(t, err)
	em, err := NewEntityManagerWithConfig(config, pool)
	require.NoError(t, err)
	require.NotNil(t, em)
}
