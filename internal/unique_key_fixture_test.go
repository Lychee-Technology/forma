package internal

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/lychee-technology/forma/internal/transform"
	"github.com/lychee-technology/forma/internal/uniquekey"
	"github.com/stretchr/testify/require"
)

// Fixtures for the real-PostgreSQL unique-key tests (#651): a manager and
// transactor over testdata/unique_keys, whose schemas declare keys over
// column-bound and EAV attributes, plus ukplain, which declares none.

const ukSchemaDir = "testdata/unique_keys"

const (
	ukAccount  = "ukaccount"
	ukFlowDef  = "ukflowdef"
	ukTaskLink = "uktasklink"
	ukInstance = "ukinstance"
	ukPlain    = "ukplain"
	ukTyped    = "uktyped"
	ukCreated  = "ukcreated"
)

var ukSchemaIDs = map[string]int16{ukAccount: 120, ukFlowDef: 121, ukTaskLink: 122, ukInstance: 123, ukPlain: 124, ukTyped: 125, ukCreated: 128}

// setupUniqueKeyEnv builds the keyed environment. Its pool holds enough
// connections for 16 concurrent writers and a pg_locks poller.
func setupUniqueKeyEnv(t *testing.T) *txEnv {
	t.Helper()
	return setupUniqueKeyEnvWith(t, nil)
}

// setupUniqueKeyEnvWith is setupUniqueKeyEnv with tune applied to the pool's
// configuration as well.
func setupUniqueKeyEnvWith(t *testing.T, tune func(*pgxpool.Config)) *txEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	testdb.Connect(t, ctx)
	pool := openTunedPool(t, ctx, func(cfg *pgxpool.Config) {
		cfg.MaxConns = 24
		if tune != nil {
			tune(cfg)
		}
	})
	tables := createTempPersistentTables(t, ctx, pool)
	schemaTable := createSchemaRegistryTableWith(t, ctx, pool, ukSchemaIDs)

	registry, err := schemameta.NewFileSchemaRegistry(pool, schemaTable, ukSchemaDir)
	require.NoError(t, err)
	metadata, err := schemameta.NewMetadataLoader(pool, schemaTable, ukSchemaDir).LoadMetadata(ctx)
	require.NoError(t, err)
	env := &integrationEnv{
		ctx: ctx, registry: registry, metadata: metadata,
		tables: tables, schemaTable: schemaTable, postgresPool: pool,
	}
	env.manager = newUniqueKeyManager(t, env)
	txr, err := NewTransactor(env.manager)
	require.NoError(t, err)
	return &txEnv{integrationEnv: env, txr: txr.(*transactor)}
}

// newUniqueKeyManager builds a manager over env's tables and registry. It
// validates writes as the factory's manager does: creates strictly, updates
// in the default report-only mode, which admits a merged document that
// violates the schema.
func newUniqueKeyManager(t *testing.T, env *integrationEnv, opts ...EntityManagerOption) forma.EntityManager {
	t.Helper()
	validator, err := schemavalidate.New(SnapshotSchemaDocuments(env.registry), ukSchemaDir)
	require.NoError(t, err)
	config := &forma.Config{
		Database: forma.DatabaseConfig{TableNames: forma.TableNames{
			SchemaRegistry: env.schemaTable,
			EntityMain:     env.tables.EntityMain,
			EAVData:        env.tables.EAVData,
			ChangeLog:      env.tables.ChangeLog,
		}},
		Query:       forma.QueryConfig{DefaultPageSize: 20, MaxPageSize: 100},
		Entity:      forma.EntityConfig{SchemaDirectory: ukSchemaDir},
		Transaction: forma.TransactionConfig{DefaultTimeout: 30 * time.Second},
	}
	repo := NewDBPersistentRecordRepository(env.postgresPool, env.metadata)
	return mustNewEntityManager(t, transform.NewPersistentRecordTransformer(env.registry), repo, nil, env.registry, config, validator, opts...)
}

// ukCatalog resolves the environment's unique keys, as the manager did.
func (e *txEnv) ukCatalog(t *testing.T) *UniqueKeyCatalog {
	t.Helper()
	catalog, err := NewUniqueKeyCatalog(e.registry)
	require.NoError(t, err)
	return catalog
}

// keyEntry is one key-table row.
type keyEntry struct {
	schemaID int16
	keyID    int16
	value    string
	rowID    uuid.UUID
}

// keyEntries reads the whole key table with direct SQL.
func (e *txEnv) keyEntries(t *testing.T) []keyEntry {
	t.Helper()
	rows, err := e.postgresPool.Query(e.ctx, fmt.Sprintf(
		"SELECT ltbase_schema_id, key_id, key_value, ltbase_row_id FROM %s ORDER BY 1, 2, 3",
		sanitizeIdentifier(uniquekey.KeyTable(e.tables.EntityMain))))
	require.NoError(t, err)
	defer rows.Close()
	var entries []keyEntry
	for rows.Next() {
		var k keyEntry
		require.NoError(t, rows.Scan(&k.schemaID, &k.keyID, &k.value, &k.rowID))
		entries = append(entries, k)
	}
	require.NoError(t, rows.Err())
	return entries
}

// keyEntriesOfRow answers the entries rowID holds.
func (e *txEnv) keyEntriesOfRow(t *testing.T, rowID uuid.UUID) []keyEntry {
	t.Helper()
	var held []keyEntry
	for _, k := range e.keyEntries(t) {
		if k.rowID == rowID {
			held = append(held, k)
		}
	}
	return held
}

// assertKeyTableMatchesRows is invariant 3 of #651: the key table holds
// exactly one entry per (live row, declared key with every component
// present), and no value is held twice. The expected entries come from an
// oracle independent of the write path's: the live row IDs by direct SQL, each
// row's attributes as the manager reads them back, and uniquekey.Encode.
func (e *txEnv) assertKeyTableMatchesRows(t *testing.T) {
	t.Helper()
	catalog := e.ukCatalog(t)
	var want []keyEntry
	held := map[entryKey]uuid.UUID{}
	for _, schemaID := range catalog.keyedSchemas() {
		schema := catalog.schemaName(schemaID)
		for _, rowID := range e.liveRowIDs(t, schemaID) {
			record, err := e.manager.Get(e.ctx, &forma.QueryRequest{SchemaName: schema, RowID: &rowID})
			require.NoError(t, err)
			for _, spec := range catalog.keysOf(schemaID) {
				value, present := oracleKeyValue(t, spec, record.Attributes)
				if !present {
					continue
				}
				k := entryKey{schemaID, spec.id, value}
				other, taken := held[k]
				require.Falsef(t, taken, "live rows %s and %s of schema %s share a value of key %s", other, rowID, schema, spec.name)
				held[k] = rowID
				want = append(want, keyEntry{schemaID: schemaID, keyID: spec.id, value: value, rowID: rowID})
			}
		}
	}
	require.ElementsMatch(t, want, e.keyEntries(t), "the key table must equal the entries recomputed from the live rows")
}

// liveRowIDs reads the IDs of schemaID's live rows with direct SQL.
func (e *txEnv) liveRowIDs(t *testing.T, schemaID int16) []uuid.UUID {
	t.Helper()
	rows, err := e.postgresPool.Query(e.ctx, fmt.Sprintf(
		"SELECT ltbase_row_id FROM %s WHERE ltbase_schema_id = $1 AND ltbase_deleted_at IS NULL",
		sanitizeIdentifier(e.tables.EntityMain)), schemaID)
	require.NoError(t, err)
	ids, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	require.NoError(t, err)
	return ids
}

// oracleKeyValue encodes spec's tuple from a record's attributes as read
// back; present is false when a component is absent.
func oracleKeyValue(t *testing.T, spec uniqueKeySpec, attributes map[string]any) (string, bool) {
	t.Helper()
	components := make([]uniquekey.Component, 0, len(spec.attrs))
	for _, attr := range spec.attrs {
		var v any = attributes
		for _, part := range strings.Split(attr.name, ".") {
			m, ok := v.(map[string]any)
			if !ok {
				return "", false
			}
			v = m[part]
		}
		if v == nil {
			return "", false
		}
		components = append(components, uniquekey.Component{Type: attr.meta.ValueType, Value: v})
	}
	value, err := uniquekey.Encode(components)
	require.NoError(t, err, "encode key %s", spec.name)
	return value, true
}

// ukCreateOp is a create of schema with data and, when rowID is not nil, the
// caller's row ID.
func ukCreateOp(schema string, rowID uuid.UUID, data map[string]any) *forma.EntityOperation {
	return &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: schema, RowID: rowID},
		Type:             forma.OperationCreate,
		Data:             data,
	}
}

func ukAccountData(tenant, email, handle string) map[string]any {
	data := map[string]any{"note": "n"}
	for attr, value := range map[string]string{"tenant": tenant, "email": email, "handle": handle} {
		if value != "" {
			data[attr] = value
		}
	}
	return data
}
