package factory

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
)

// #470: an attribute named after an entity_main column makes Query and
// CrossSchemaSearch filter on the column (hybridMainColumn,
// hasMainTableCondition). Both run on the EntityManager this factory builds,
// and the factory loads its metadata through the real loader before anything
// else; a stored schema with such a name therefore fails startup, so no
// manager, and no query or keyset page, ever sees it.
func TestNewEntityManager_RefusesStoredSchemaWithEntityMainColumnName(t *testing.T) {
	for attr, column := range map[string]string{"text_01": "text_01", "Integer.02": "integer_02"} {
		t.Run(attr, func(t *testing.T) {
			dir := t.TempDir()
			data, err := json.Marshal(map[string]any{
				attr: map[string]any{"attributeID": 1, "valueType": "text"},
			})
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(dir, "lead_attributes.json"), data, 0o600))

			pool, err := pgxmock.NewPool()
			require.NoError(t, err)
			defer pool.Close()
			pool.ExpectQuery(`SELECT schema_name, schema_id FROM`).
				WillReturnRows(pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("lead", int16(7)))

			deps := buildUnitEntityManagerDeps(schemameta.NewMetadataCache())
			deps.newMetadataLoader = func(_ *pgxpool.Pool, schemaTable, schemaDir string) metadataLoader {
				return schemameta.NewMetadataLoader(pool, schemaTable, schemaDir)
			}
			config := newUnitEntityManagerConfig(t)
			config.Entity.SchemaDirectory = dir

			em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)
			require.Error(t, err)
			assert.Nil(t, em)
			assert.Contains(t, err.Error(), "failed to load metadata")
			assert.Contains(t, err.Error(), `attribute "`+attr+`"`)
			assert.Contains(t, err.Error(), `entity_main column "`+column+`"`)
			require.NoError(t, pool.ExpectationsWereMet())
		})
	}
}

// leadRegistry is a caller-supplied registry serving one schema, lead (id 7),
// whose single attribute (id 1) is named attr.
func leadRegistry(attr string) *mockSchemaRegistry {
	registry := newMockSchemaRegistry()
	registry.nameToID = map[string]int16{"lead": 7}
	registry.idToName = map[int16]string{7: "lead"}
	registry.schemas = map[string]forma.SchemaAttributeCache{
		"lead": {attr: {AttributeName: attr, AttributeID: 1, ValueType: forma.ValueTypeText}},
	}
	return registry
}

// R1-F1 of #470's review: the manager, the transformer and the guards read
// config.SchemaRegistry, not the loader's cache, so valid files do not vouch
// for it. Here the files declare attribute 1 of lead as label, which admission
// accepts, while the caller's registry serves the same id as text_01: the API
// would answer that value as text_01 while Query and CrossSchemaSearch filter
// on the physical text_01 column. Construction must fail on the registry.
func TestNewEntityManager_RefusesCustomRegistryWithEntityMainColumnName(t *testing.T) {
	loaded := schemameta.NewMetadataCache()
	require.NoError(t, loaded.RegisterSchema("lead", 7, forma.SchemaAttributeCache{
		"label": {AttributeName: "label", AttributeID: 1, ValueType: forma.ValueTypeText},
	}), "the loader's metadata is valid and admitted")

	config := newUnitEntityManagerConfig(t)
	config.SchemaRegistry = leadRegistry("text_01")

	em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, buildUnitEntityManagerDeps(loaded))
	require.Error(t, err)
	assert.Nil(t, em)
	assert.Contains(t, err.Error(), "config.SchemaRegistry rejected")
	assert.Contains(t, err.Error(), "schema lead served by the configured SchemaRegistry")
	assert.Contains(t, err.Error(), `attribute "text_01" is named after entity_main column "text_01"`)
}

// A registry that passes at startup and later serves a colliding cache (a
// reload; forma.SchemaRegistry promises nothing about repeated reads) is
// refused on the read, so neither Query nor CrossSchemaSearch renders a
// condition over it.
func TestNewEntityManager_RefusesCustomRegistryCacheServedAfterStartup(t *testing.T) {
	registry := leadRegistry("label")
	config := newUnitEntityManagerConfig(t)
	config.SchemaRegistry = registry

	em, err := newEntityManagerWithConfigContext(context.Background(), config, nil,
		buildUnitEntityManagerDeps(schemameta.NewMetadataCache()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })

	registry.schemas["lead"] = forma.SchemaAttributeCache{
		"text_01": {AttributeName: "text_01", AttributeID: 1, ValueType: forma.ValueTypeText},
	}
	condition := &forma.KvCondition{Attr: "text_01", Value: "equals:x"}

	_, err = em.Query(context.Background(), &forma.QueryRequest{SchemaName: "lead", Condition: condition})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `entity_main column "text_01"`)
	assert.NotErrorIs(t, err, forma.ErrInvalidInput, "a misconfigured registry is an operator error")

	_, err = em.CrossSchemaSearch(context.Background(), &forma.CrossSchemaRequest{
		SchemaNames: []string{"lead"}, SearchTerm: "x", Condition: condition,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `entity_main column "text_01"`)
}

// R2-F1 of #470's review: the system and dedup names admission reserves
// (row_id, rn, ...) are refused in a caller-supplied registry too, at startup
// and when a reload serves one later.
func TestNewEntityManager_RefusesCustomRegistryWithReservedSystemName(t *testing.T) {
	for _, attr := range []string{"row_id", "rn"} {
		t.Run(attr, func(t *testing.T) {
			config := newUnitEntityManagerConfig(t)
			config.SchemaRegistry = leadRegistry(attr)

			em, err := newEntityManagerWithConfigContext(context.Background(), config, nil,
				buildUnitEntityManagerDeps(schemameta.NewMetadataCache()))
			require.Error(t, err)
			assert.Nil(t, em)
			assert.Contains(t, err.Error(), "config.SchemaRegistry rejected")
			assert.Contains(t, err.Error(), "schema lead served by the configured SchemaRegistry")
			assert.Contains(t, err.Error(), `attribute "`+attr+`" folds to parquet column "`+attr+`", which is reserved for system columns`)
		})
	}
}

func TestNewEntityManager_RefusesCustomRegistryReservedSystemNameServedAfterStartup(t *testing.T) {
	for _, attr := range []string{"row_id", "rn"} {
		t.Run(attr, func(t *testing.T) {
			registry := leadRegistry("label")
			config := newUnitEntityManagerConfig(t)
			config.SchemaRegistry = registry

			em, err := newEntityManagerWithConfigContext(context.Background(), config, nil,
				buildUnitEntityManagerDeps(schemameta.NewMetadataCache()))
			require.NoError(t, err)
			t.Cleanup(func() { _ = em.Close() })

			registry.schemas["lead"] = forma.SchemaAttributeCache{
				attr: {AttributeName: attr, AttributeID: 1, ValueType: forma.ValueTypeText},
			}
			condition := &forma.KvCondition{Attr: attr, Value: "equals:x"}

			_, err = em.Query(context.Background(), &forma.QueryRequest{SchemaName: "lead", Condition: condition})
			require.Error(t, err)
			assert.Contains(t, err.Error(), `parquet column "`+attr+`", which is reserved for system columns`)
			assert.NotErrorIs(t, err, forma.ErrInvalidInput, "a misconfigured registry is an operator error")

			_, err = em.CrossSchemaSearch(context.Background(), &forma.CrossSchemaRequest{
				SchemaNames: []string{"lead"}, SearchTerm: "x", Condition: condition,
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), `parquet column "`+attr+`", which is reserved for system columns`)
		})
	}
}
