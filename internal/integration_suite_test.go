package internal

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// integrationEnv wires together a real Postgres-backed EntityManager using the on-disk schemas.
type integrationEnv struct {
	ctx          context.Context
	manager      forma.EntityManager
	registry     forma.SchemaRegistry
	metadata     *MetadataCache
	tables       StorageTables
	schemaTable  string
	postgresPool *pgxpool.Pool
}

// setupIntegrationEnv provisions temporary tables, loads metadata, and builds an EntityManager.
func setupIntegrationEnv(t *testing.T) *integrationEnv {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	t.Cleanup(cancel)

	pool := connectTestPostgres(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)
	schemaRegistryTable := createSchemaRegistryTable(t, ctx, pool)

	registry, err := NewFileSchemaRegistry(pool, schemaRegistryTable, "../cmd/server/schemas")
	require.NoError(t, err)

	config := &forma.Config{
		Database: forma.DatabaseConfig{
			TableNames: forma.TableNames{
				SchemaRegistry: schemaRegistryTable,
				EntityMain:     tables.EntityMain,
				EAVData:        tables.EAVData,
				ChangeLog:      tables.ChangeLog,
			},
		},
		Query: forma.QueryConfig{
			DefaultPageSize: 20,
			MaxPageSize:     100,
		},
		Entity: forma.EntityConfig{
			SchemaDirectory: "../cmd/server/schemas",
		},
	}

	loader := NewMetadataLoader(pool, config.Database.TableNames.SchemaRegistry, config.Entity.SchemaDirectory)
	metadataCache, err := loader.LoadMetadata(ctx)
	require.NoError(t, err)

	transformer := NewPersistentRecordTransformer(registry)
	repo := NewDBPersistentRecordRepository(pool, metadataCache, nil)
	manager := NewEntityManager(transformer, repo, registry, config)

	return &integrationEnv{
		ctx:          ctx,
		manager:      manager,
		registry:     registry,
		metadata:     metadataCache,
		tables:       tables,
		schemaTable:  schemaRegistryTable,
		postgresPool: pool,
	}
}

// createSchemaRegistryTable mirrors the schema registry table using IDs from the file registry.
func createSchemaRegistryTable(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string {
	t.Helper()

	suffix := time.Now().UnixNano()
	tableName := fmt.Sprintf("schema_registry_it_%d", suffix)

	ddl := fmt.Sprintf("CREATE TABLE %s (schema_name TEXT PRIMARY KEY, schema_id SMALLINT NOT NULL)", sanitizeIdentifier(tableName))
	_, err := pool.Exec(ctx, ddl)
	require.NoError(t, err)

	schemas := map[string]int16{
		"activity":      100,
		"lead":          101,
		"visit":         102,
		"communication": 103,
		"log":           104,
	}

	for name, id := range schemas {
		require.NoError(t, err)

		_, err = pool.Exec(ctx,
			fmt.Sprintf("INSERT INTO %s (schema_name, schema_id) VALUES ($1, $2)", sanitizeIdentifier(tableName)),
			name, id,
		)
		require.NoError(t, err)
	}

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", sanitizeIdentifier(tableName)))
	})

	return tableName
}

func activityPayload(activityType string, extra map[string]any) map[string]any {
	payload := map[string]any{
		"id":             fmt.Sprintf("activity-%d", time.Now().UnixNano()),
		"type":           activityType,
		"direction":      extra["direction"],
		"at":             time.Now(),
		"userId":         "user-test",
		"summary":        fmt.Sprintf("Integration test %s activity", activityType),
		"nextFollowUpAt": time.Now().Add(24 * time.Hour),
	}

	for k, v := range extra {
		payload[k] = v
	}

	return payload
}

// TestIntegration_EntityLifecycle covers create -> read -> update -> list -> delete through Postgres.
func TestIntegration_EntityLifecycle(t *testing.T) {
	env := setupIntegrationEnv(t)

	created, err := env.manager.Create(env.ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "activity"},
		Type:             forma.OperationCreate,
		Data:             activityPayload("call", nil),
	})
	require.NoError(t, err)
	require.NotNil(t, created)
	assert.Equal(t, "activity", created.SchemaName)

	fetched, err := env.manager.Get(env.ctx, &forma.QueryRequest{
		SchemaName: "activity",
		RowID:      &created.RowID,
	})
	require.NoError(t, err)
	assert.Equal(t, created.RowID, fetched.RowID)
	assert.Equal(t, "call", fetched.Attributes["type"])

	updated, err := env.manager.Update(env.ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{
			SchemaName: "activity",
			RowID:      created.RowID,
		},
		Type: forma.OperationUpdate,
		Updates: map[string]any{
			"type":           "email",
			"summary":        "Updated to email activity",
			"direction":      "outbound",
			"at":             time.Now().Format(time.RFC3339),
			"nextFollowUpAt": time.Now().Add(24 * time.Hour).Format(time.RFC3339),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "email", updated.Attributes["type"])

	list, err := env.manager.Query(env.ctx, &forma.QueryRequest{
		SchemaName:   "activity",
		Page:         1,
		ItemsPerPage: 10,
	})
	require.NoError(t, err)
	require.Len(t, list.Data, 1)
	assert.Equal(t, created.RowID, list.Data[0].RowID)
	assert.Equal(t, 1, list.TotalRecords)

	err = env.manager.Delete(env.ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{
			SchemaName: "activity",
			RowID:      created.RowID,
		},
		Type: forma.OperationDelete,
	})
	require.NoError(t, err)

	_, err = env.manager.Get(env.ctx, &forma.QueryRequest{
		SchemaName: "activity",
		RowID:      &created.RowID,
	})
	require.Error(t, err)
}

// TestIntegration_AdvancedQuery exercises SQL generator + repository against live data.
func TestIntegration_AdvancedQuery(t *testing.T) {
	env := setupIntegrationEnv(t)

	callRecord, err := env.manager.Create(env.ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "activity"},
		Type:             forma.OperationCreate,
		Data:             activityPayload("call", nil),
	})
	require.NoError(t, err)
	require.NotNil(t, callRecord)

	_, err = env.manager.Create(env.ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "activity"},
		Type:             forma.OperationCreate,
		Data:             activityPayload("email", nil),
	})
	require.NoError(t, err)

	result, err := env.manager.Query(env.ctx, &forma.QueryRequest{
		SchemaName: "activity",
		Condition: &forma.CompositeCondition{
			Logic: forma.LogicAnd,
			Conditions: []forma.Condition{
				&forma.KvCondition{
					Attr:  "type",
					Value: "equals:call",
				},
			},
		},
		Page:         1,
		ItemsPerPage: 5,
	})
	require.NoError(t, err)

	require.Len(t, result.Data, 1)
	assert.Equal(t, callRecord.RowID, result.Data[0].RowID)
	assert.Equal(t, 1, result.TotalRecords)
	assert.Equal(t, 1, result.TotalPages)
	assert.False(t, result.HasNext)
}

// TestIntegration_AdvancedQuery_MixedSorting verifies sorting by both Main and EAV attributes.
func TestIntegration_AdvancedQuery_MixedSorting(t *testing.T) {
	env := setupIntegrationEnv(t)

	// Create records with different combinations of Main (type) and EAV (priority) attributes
	// Record 1: type=call, priority=25
	rec1, err := env.manager.Create(env.ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "activity"},
		Type:             forma.OperationCreate,
		Data: activityPayload("call", map[string]any{
			"direction": "inbound",
		}),
	})
	require.NoError(t, err)

	// Record 2: type=call, priority=35
	rec2, err := env.manager.Create(env.ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "activity"},
		Type:             forma.OperationCreate,
		Data: activityPayload("call", map[string]any{
			"direction": "outbound",
		}),
	})
	require.NoError(t, err)

	// Record 3: type=email, priority=45
	rec3, err := env.manager.Create(env.ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: "activity"},
		Type:             forma.OperationCreate,
		Data: activityPayload("email", map[string]any{
			"direction": "inbound",
		}),
	})
	require.NoError(t, err)

	// Query: Filter type=call, Sort by priority DESC
	// Expected order: rec2 (35), rec1 (25)
	result, err := env.manager.Query(env.ctx, &forma.QueryRequest{
		SchemaName: "activity",
		Condition: &forma.CompositeCondition{
			Logic: forma.LogicAnd,
			Conditions: []forma.Condition{
				&forma.KvCondition{
					Attr:  "type",
					Value: "equals:call",
				},
			},
		},
		SortBy:       []string{"direction"},
		SortOrder:    forma.SortOrderDesc,
		Page:         1,
		ItemsPerPage: 10,
	})
	require.NoError(t, err)

	require.Len(t, result.Data, 2)
	assert.Equal(t, rec2.RowID, result.Data[0].RowID)
	assert.Equal(t, rec1.RowID, result.Data[1].RowID)

	// Query: Sort by type ASC, then priority DESC
	// Expected order: rec3 (email, 45), rec2 (call, 35), rec1 (call, 25)
	// Note: QueryRequest currently only supports a list of SortBy strings and a single SortOrder.
	// If the API supported per-field direction, we'd test that.
	// For now, let's test sorting by multiple fields with same direction (DESC).
	// Sort by type DESC (email > call alphabetically), then priority DESC.
	// Expected: rec3 (email, 45), rec2 (call, 35), rec1 (call, 25)

	resultMixed, err := env.manager.Query(env.ctx, &forma.QueryRequest{
		SchemaName: "activity",
		Condition: &forma.CompositeCondition{
			Logic: forma.LogicAnd,
			Conditions: []forma.Condition{
				&forma.KvCondition{Attr: "id", Value: "starts_with:activity"},
			},
		}, // Match all
		SortBy:       []string{"type", "direction"},
		SortOrder:    forma.SortOrderDesc,
		Page:         1,
		ItemsPerPage: 10,
	})
	require.NoError(t, err)

	require.Len(t, resultMixed.Data, 3)
	assert.Equal(t, rec3.RowID, resultMixed.Data[0].RowID) // email, 45
	assert.Equal(t, rec2.RowID, resultMixed.Data[1].RowID) // call, 35
	assert.Equal(t, rec1.RowID, resultMixed.Data[2].RowID) // call, 25
}
