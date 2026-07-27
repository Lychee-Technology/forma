package factory

import (
	"context"
	"fmt"
	"testing"

	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/schemameta"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
)

// ---------------------------------------------------------------------------
// Unit tests for NewEntityManagerWithConfig (uses test hooks + mock loader)
// ---------------------------------------------------------------------------

type mockMetadataLoader struct {
	cache *schemameta.MetadataCache
	err   error
}

func (m *mockMetadataLoader) LoadMetadata(ctx context.Context) (*schemameta.MetadataCache, error) {
	return m.cache, m.err
}

func buildUnitEntityManagerDeps(cache *schemameta.MetadataCache) entityManagerDependencies {
	deps := defaultEntityManagerDependencies()
	deps.collectTables = func(ctx context.Context, pool queryPool, schema string) ([]string, error) {
		return []string{"schema_registry", "eav_data", "entity_main"}, nil
	}
	deps.newMetadataLoader = func(pool *pgxpool.Pool, schemaTable, schemaDir string) metadataLoader {
		return &mockMetadataLoader{cache: cache, err: nil}
	}
	return deps
}

// newUnitEntityManagerConfig is the config half of the successful-construction
// fixture: the table names buildUnitEntityManagerDeps' collector reports, plus an
// empty schema directory.
func newUnitEntityManagerConfig(t *testing.T) *forma.Config {
	t.Helper()
	config := forma.DefaultConfig(newMockSchemaRegistry())
	config.Database.TableNames = forma.TableNames{
		SchemaRegistry: "schema_registry",
		EAVData:        "eav_data",
		EntityMain:     "entity_main",
	}
	config.Entity.SchemaDirectory = t.TempDir()
	return config
}

func TestNewEntityManagerWithConfig_Unit_TableCollectorError(t *testing.T) {
	t.Parallel()

	deps := defaultEntityManagerDependencies()
	deps.collectTables = func(ctx context.Context, pool queryPool, schema string) ([]string, error) {
		return nil, assert.AnError
	}

	config := forma.DefaultConfig(newMockSchemaRegistry())

	em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)

	assert.Nil(t, em)
	assert.Error(t, err)
}

func TestNewEntityManagerWithConfig_Unit_MissingRequiredTables(t *testing.T) {
	t.Parallel()

	deps := defaultEntityManagerDependencies()
	deps.collectTables = func(ctx context.Context, pool queryPool, schema string) ([]string, error) {
		return []string{"schema_registry"}, nil
	}

	config := forma.DefaultConfig(newMockSchemaRegistry())
	config.Database.TableNames = forma.TableNames{
		SchemaRegistry: "schema_registry",
		EAVData:        "eav_data",
		EntityMain:     "entity_main",
	}

	em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)

	assert.Nil(t, em)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "required tables are missing")
}

func TestNewEntityManagerWithConfig_Unit_MissingEntityMainTable(t *testing.T) {
	t.Parallel()

	deps := defaultEntityManagerDependencies()
	deps.collectTables = func(ctx context.Context, pool queryPool, schema string) ([]string, error) {
		// schema_registry and eav_data present, but entity_main is absent
		return []string{"schema_registry", "eav_data"}, nil
	}

	config := forma.DefaultConfig(newMockSchemaRegistry())
	config.Database.TableNames = forma.TableNames{
		SchemaRegistry: "schema_registry",
		EAVData:        "eav_data",
		EntityMain:     "entity_main",
	}

	em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)

	assert.Nil(t, em)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "required tables are missing")
}

func TestNewEntityManagerWithConfig_Unit_MetadataLoaderError(t *testing.T) {
	t.Parallel()

	cache := schemameta.NewMetadataCache()
	deps := buildUnitEntityManagerDeps(cache)
	deps.newMetadataLoader = func(pool *pgxpool.Pool, schemaTable, schemaDir string) metadataLoader {
		return &mockMetadataLoader{cache: cache, err: fmt.Errorf("simulated loader error")}
	}

	config := forma.DefaultConfig(newMockSchemaRegistry())
	config.Database.TableNames = forma.TableNames{
		SchemaRegistry: "schema_registry",
		EAVData:        "eav_data",
		EntityMain:     "entity_main",
	}
	config.Entity.SchemaDirectory = t.TempDir()

	em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)

	assert.Nil(t, em)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load metadata")
}

func TestNewEntityManagerWithConfig_Unit_NilSchemaRegistry(t *testing.T) {
	t.Parallel()

	cache := schemameta.NewMetadataCache()
	deps := buildUnitEntityManagerDeps(cache)

	config := forma.DefaultConfig(nil)
	config.Database.TableNames = forma.TableNames{
		SchemaRegistry: "schema_registry",
		EAVData:        "eav_data",
		EntityMain:     "entity_main",
	}
	config.Entity.SchemaDirectory = t.TempDir()

	em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)

	assert.Nil(t, em)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "config.SchemaRegistry is required")
}

func TestNewEntityManagerWithConfig_Unit_Success(t *testing.T) {
	t.Parallel()

	cache := schemameta.NewMetadataCache()
	deps := buildUnitEntityManagerDeps(cache)

	config := newUnitEntityManagerConfig(t)

	em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)

	assert.NotNil(t, em)
	assert.NoError(t, err)
}

func TestNewEntityManagerWithConfig_Unit_SchemaQualifiedTableNames(t *testing.T) {
	t.Parallel()

	cache := schemameta.NewMetadataCache()
	deps := buildUnitEntityManagerDeps(cache)
	deps.collectTables = func(ctx context.Context, pool queryPool, schema string) ([]string, error) {
		assert.Equal(t, "tenant", schema)
		return []string{"schema_registry", "eav_data", "entity_main"}, nil
	}
	deps.newMetadataLoader = func(pool *pgxpool.Pool, schemaTable, schemaDir string) metadataLoader {
		assert.Equal(t, "tenant.schema_registry", schemaTable)
		return &mockMetadataLoader{cache: cache, err: nil}
	}

	config := forma.DefaultConfig(newMockSchemaRegistry())
	config.Database.Schema = "tenant"
	config.Database.TableNames = forma.TableNames{
		SchemaRegistry: "tenant.schema_registry",
		EAVData:        "tenant.eav_data",
		EntityMain:     "tenant.entity_main",
	}
	config.Entity.SchemaDirectory = t.TempDir()

	em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)

	assert.NotNil(t, em)
	assert.NoError(t, err)
}

func TestNewEntityManagerWithConfig_Unit_SchemaParamQualifiesUnqualifiedTableNames(t *testing.T) {
	t.Parallel()

	cache := schemameta.NewMetadataCache()
	deps := buildUnitEntityManagerDeps(cache)
	deps.collectTables = func(ctx context.Context, pool queryPool, schema string) ([]string, error) {
		assert.Equal(t, "tenant", schema)
		return []string{"schema_registry", "eav_data", "entity_main"}, nil
	}
	deps.newMetadataLoader = func(pool *pgxpool.Pool, schemaTable, schemaDir string) metadataLoader {
		assert.Equal(t, "tenant.schema_registry", schemaTable)
		return &mockMetadataLoader{cache: cache, err: nil}
	}

	config := forma.DefaultConfig(newMockSchemaRegistry())
	config.Database.Schema = "tenant"
	config.Database.TableNames = forma.TableNames{
		SchemaRegistry: "schema_registry",
		EAVData:        "eav_data",
		EntityMain:     "entity_main",
		ChangeLog:      "change_log",
	}
	config.Entity.SchemaDirectory = t.TempDir()

	em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)

	assert.NotNil(t, em)
	assert.NoError(t, err)
}

func TestNewEntityManagerWithConfigContext_Unit_PropagatesContextToTableCollector(t *testing.T) {
	t.Parallel()

	cache := schemameta.NewMetadataCache()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	deps := buildUnitEntityManagerDeps(cache)
	deps.collectTables = func(ctx context.Context, pool queryPool, schema string) ([]string, error) {
		assert.ErrorIs(t, ctx.Err(), context.Canceled)
		return []string{"schema_registry", "eav_data", "entity_main"}, nil
	}

	config := forma.DefaultConfig(newMockSchemaRegistry())
	config.Database.TableNames = forma.TableNames{
		SchemaRegistry: "schema_registry",
		EAVData:        "eav_data",
		EntityMain:     "entity_main",
	}
	config.Entity.SchemaDirectory = t.TempDir()

	em, err := newEntityManagerWithConfigContext(ctx, config, nil, deps)

	assert.NotNil(t, em)
	assert.NoError(t, err)
}

func TestNewEntityManagerWithConfigContext_Unit_PropagatesContextToDuckDBFactory(t *testing.T) {
	t.Parallel()

	cache := schemameta.NewMetadataCache()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	deps := buildUnitEntityManagerDeps(cache)
	deps.newDuckDBClient = func(ctx context.Context, cfg forma.DuckDBConfig) (*federated.DuckDBClient, error) {
		assert.ErrorIs(t, ctx.Err(), context.Canceled)
		return nil, context.Canceled
	}

	config := forma.DefaultConfig(newMockSchemaRegistry())
	config.Database.TableNames = forma.TableNames{
		SchemaRegistry: "schema_registry",
		EAVData:        "eav_data",
		EntityMain:     "entity_main",
	}
	config.Entity.SchemaDirectory = t.TempDir()
	config.DuckDB.Enabled = true

	em, err := newEntityManagerWithConfigContext(ctx, config, nil, deps)

	assert.NotNil(t, em)
	assert.NoError(t, err)
}
