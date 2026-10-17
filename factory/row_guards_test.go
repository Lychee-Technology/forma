package factory

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The DSQL engine locks rows by writing their guard rows (#633), so a
// deployment without the guard table is refused at startup rather than at
// its first write. The PostgreSQL engine never reads the table.
func TestRequireRowGuards(t *testing.T) {
	config := forma.DefaultConfig(newMockSchemaRegistry())
	config.Database.TableNames.EntityMain = "public.entity_main"
	without := []string{"entity_main"}
	with := []string{"entity_main", "entity_main_row_guard"}

	require.NoError(t, requireRowGuards(config, without), "the postgresql engine needs no guard table")

	config.Database.Engine = forma.EngineAuroraDSQL
	err := requireRowGuards(config, without)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "public.entity_main_row_guard is missing")
	require.NoError(t, requireRowGuards(config, with))
}

// An unknown engine is refused before the table collector runs: falling back
// to the PostgreSQL engine's advisory locks on another database would lose
// every write exclusion.
func TestNewEntityManagerWithConfig_Unit_UnknownEngine(t *testing.T) {
	t.Parallel()

	deps := buildUnitEntityManagerDeps(schemameta.NewMetadataCache())
	deps.collectTables = func(ctx context.Context, pool queryPool, schema string) ([]string, error) {
		t.Fatal("an unknown engine must be refused before any I/O")
		return nil, nil
	}
	config := newUnitEntityManagerConfig(t)
	config.Database.Engine = "dsql"

	em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)

	assert.Nil(t, em)
	var configErr *forma.ConfigError
	require.ErrorAs(t, err, &configErr)
	assert.Equal(t, "database.engine", configErr.Field)
}

func TestNewEntityManagerWithConfig_Unit_DSQLNeedsRowGuards(t *testing.T) {
	t.Parallel()

	deps := buildUnitEntityManagerDeps(schemameta.NewMetadataCache())
	config := newUnitEntityManagerConfig(t)
	config.Database.Engine = forma.EngineAuroraDSQL

	em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)
	assert.Nil(t, em)
	require.ErrorContains(t, err, "entity_main_row_guard is missing")

	deps.collectTables = func(ctx context.Context, pool queryPool, schema string) ([]string, error) {
		return []string{"schema_registry", "eav_data", "entity_main", "entity_main_row_guard"}, nil
	}
	em, err = newEntityManagerWithConfigContext(context.Background(), config, nil, deps)
	require.NoError(t, err)
	assert.NotNil(t, em)
}
