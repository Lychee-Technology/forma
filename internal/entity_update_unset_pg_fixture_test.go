package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/lychee-technology/forma/internal/transform"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL fixtures of the unset operator (#938): a manager and
// transactor over testdata/unset_ops (unsetdoc), or over testdata/unset_keys
// (unsetkeys, whose unique keys the repository enforces), validating updates
// in the default report-only mode.

const (
	unsetKeysSchemaDir  = "testdata/unset_keys"
	unsetKeysSchemaName = "unsetkeys"
)

// setupUnsetPG builds the environment over dir, whose one schema is
// registered as schemaID.
func setupUnsetPG(t *testing.T, dir, schema string, schemaID int16) *txEnv {
	t.Helper()
	return setupUnsetPGTuned(t, dir, schema, schemaID, nil)
}

// setupUnsetPGTuned is setupUnsetPG with tune, when not nil, adjusting the
// pool's config before the pool opens.
func setupUnsetPGTuned(t *testing.T, dir, schema string, schemaID int16, tune func(*pgxpool.Config)) *txEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	testdb.Connect(t, ctx) // skips, or fails in CI, when PostgreSQL is absent
	pool := openTunedPool(t, ctx, func(cfg *pgxpool.Config) {
		cfg.MaxConns = 8
		if tune != nil {
			tune(cfg)
		}
	})
	tables := createTempPersistentTables(t, ctx, pool)
	schemaTable := createSchemaRegistryTableWith(t, ctx, pool, map[string]int16{schema: schemaID})
	seedSchemaLedger(t, ctx, pool, schemaTable, dir)

	registry, err := schemameta.NewFileSchemaRegistry(pool, schemaTable, dir)
	require.NoError(t, err)
	metadata, err := schemameta.NewMetadataLoader(pool, schemaTable, dir).LoadMetadata(ctx)
	require.NoError(t, err)
	env := &integrationEnv{ctx: ctx, registry: registry, metadata: metadata, tables: tables, schemaTable: schemaTable, postgresPool: pool}

	catalog, err := NewUniqueKeyCatalog(registry)
	require.NoError(t, err)
	validator, err := schemavalidate.New(SnapshotSchemaDocuments(registry), dir)
	require.NoError(t, err)
	config := &forma.Config{
		Database: forma.DatabaseConfig{TableNames: forma.TableNames{
			SchemaRegistry: schemaTable, EntityMain: tables.EntityMain, EAVData: tables.EAVData, ChangeLog: tables.ChangeLog,
		}},
		Query:       forma.QueryConfig{DefaultPageSize: 20, MaxPageSize: 100},
		Entity:      forma.EntityConfig{SchemaDirectory: dir},
		Transaction: forma.TransactionConfig{DefaultTimeout: 30 * time.Second},
	}
	repo := NewDBPersistentRecordRepository(pool, metadata, WithUniqueKeyCatalog(catalog))
	env.manager = mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry), repo, nil, registry, config, validator,
		WithUniqueKeys(catalog))
	txr, err := NewTransactor(env.manager)
	require.NoError(t, err)
	return &txEnv{integrationEnv: env, txr: txr.(*transactor)}
}

func setupUnsetDocPG(t *testing.T) *txEnv {
	t.Helper()
	return setupUnsetPG(t, unsetSchemaDir, unsetSchemaName, 140)
}

func setupUnsetKeysPG(t *testing.T) *txEnv {
	t.Helper()
	return setupUnsetPG(t, unsetKeysSchemaDir, unsetKeysSchemaName, 141)
}

// unsetSurface is a single-row update route: EntityManager.Update or
// Tx.Update in a Tx of its own.
type unsetSurface struct {
	name   string
	update func(e *txEnv, op *forma.EntityOperation) (*forma.DataRecord, error)
}

var unsetSurfaces = []unsetSurface{
	{"Update", func(e *txEnv, op *forma.EntityOperation) (*forma.DataRecord, error) {
		return e.manager.Update(e.ctx, op)
	}},
	{"Tx.Update", func(e *txEnv, op *forma.EntityOperation) (*forma.DataRecord, error) {
		var record *forma.DataRecord
		err := e.inTx(func(ctx context.Context, tx forma.Tx) error {
			var err error
			record, err = tx.Update(ctx, op)
			return err
		})
		return record, err
	}},
}

// mainColumns answers the row's entity_main columns as PostgreSQL renders
// them, a NULL column as nil.
func (e *txEnv) mainColumns(t *testing.T, schema string, rowID uuid.UUID) map[string]any {
	t.Helper()
	var columns map[string]any
	require.NoError(t, json.Unmarshal([]byte(e.snapshot(t, schema, rowID).main), &columns))
	return columns
}

// eavRowCount counts the row's eav_data rows of attrID, its empty-list
// marker included.
func (e *txEnv) eavRowCount(t *testing.T, schema string, rowID uuid.UUID, attrID int16) int {
	t.Helper()
	var n int
	require.NoError(t, e.postgresPool.QueryRow(e.ctx, fmt.Sprintf(
		"SELECT count(*) FROM %s WHERE schema_id = $1 AND row_id = $2 AND attr_id = $3", sanitizeIdentifier(e.tables.EAVData)),
		e.schemaID(t, schema), rowID, attrID).Scan(&n))
	return n
}

// attrID answers attr's attribute ID in schema.
func (e *txEnv) attrID(t *testing.T, schema, attr string) int16 {
	t.Helper()
	_, cache, err := e.registry.GetSchemaAttributeCacheByName(schema)
	require.NoError(t, err)
	meta, ok := cache[attr]
	require.True(t, ok, "schema %s defines %s", schema, attr)
	return meta.AttributeID
}
