package internal

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Fixtures for the real-PostgreSQL Tx tests (#648): a manager over the
// shipped schemas with JSON Schema validation and strict updates on, its
// transactor, and post-state read with direct SQL, never through Forma.

// The three schemas of #593's rollback scenario.
const (
	txSchemaA = "activity"
	txSchemaB = "visit"
	txSchemaC = "communication"
)

type txEnv struct {
	*integrationEnv
	txr *transactor
	// fault is the armed statement fault, if any (see armFaultAfterMainInsert).
	fault atomic.Pointer[func(sql string) error]
}

func setupTxEnv(t *testing.T) *txEnv {
	t.Helper()
	env := setupIntegrationEnv(t)
	validator, err := schemavalidate.New(SnapshotSchemaDocuments(env.registry), "../cmd/server/schemas")
	require.NoError(t, err)
	config := &forma.Config{
		Database: forma.DatabaseConfig{TableNames: forma.TableNames{
			SchemaRegistry: env.schemaTable,
			EntityMain:     env.tables.EntityMain,
			EAVData:        env.tables.EAVData,
			ChangeLog:      env.tables.ChangeLog,
		}},
		Query:       forma.QueryConfig{DefaultPageSize: 20, MaxPageSize: 100},
		Entity:      forma.EntityConfig{SchemaDirectory: "../cmd/server/schemas", ValidateUpdatesStrict: true},
		Transaction: forma.TransactionConfig{DefaultTimeout: 30 * time.Second},
	}
	repo := NewDBPersistentRecordRepository(env.postgresPool, env.metadata)
	env.manager = mustNewEntityManager(t, transform.NewPersistentRecordTransformer(env.registry), repo, nil, env.registry, config, validator)

	txr, err := NewTransactor(env.manager)
	require.NoError(t, err)
	tx := &txEnv{integrationEnv: env, txr: txr.(*transactor)}
	tx.txr.statementFault = func(sql string) error {
		if fault := tx.fault.Load(); fault != nil {
			return (*fault)(sql)
		}
		return nil
	}
	return tx
}

// armFaultAfterMainInsert makes the next entity_main INSERT fail after the
// server has executed it, the way a storage error would, and disarms itself.
// It answers the injected error.
func (e *txEnv) armFaultAfterMainInsert() error {
	injected := fmt.Errorf("injected storage fault after the entity_main insert")
	prefix := "INSERT INTO " + sanitizeIdentifier(e.tables.EntityMain)
	fault := func(sql string) error {
		if !strings.HasPrefix(sql, prefix) {
			return nil
		}
		e.fault.Store(nil)
		return injected
	}
	e.fault.Store(&fault)
	return injected
}

func (e *txEnv) schemaID(t *testing.T, schema string) int16 {
	t.Helper()
	id, _, err := e.registry.GetSchemaAttributeCacheByName(schema)
	require.NoError(t, err)
	return id
}

func txActivity(summary string) map[string]any {
	return map[string]any{
		"id": "activity-" + uuid.NewString(), "type": "call", "direction": "outbound",
		"at": "2026-10-01T10:00:00Z", "userId": "user-tx", "summary": summary,
	}
}

func txVisit(status string) map[string]any {
	return map[string]any{
		"id": "visit-" + uuid.NewString(), "leadId": "lead-tx", "userId": "user-tx",
		"propertyId": "property-tx", "scheduledStartAt": "2026-10-02T10:00:00Z", "status": status,
	}
}

// txCommunication answers a valid communication, or with an invalid type one
// whose Create fails JSON Schema validation before any SQL is sent.
func txCommunication(kind string) map[string]any {
	return map[string]any{
		"id": "communication-" + uuid.NewString(), "leadId": "lead-tx", "userId": "user-tx",
		"type": kind, "at": "2026-10-03T10:00:00Z", "summary": "follow-up",
	}
}

func txCreateOp(schema string, data map[string]any) *forma.EntityOperation {
	return &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: schema}, Type: forma.OperationCreate, Data: data}
}

func txUpdateOp(schema string, rowID uuid.UUID, updates map[string]any) *forma.EntityOperation {
	return &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: schema, RowID: rowID}, Type: forma.OperationUpdate, Updates: updates}
}

func txDeleteOp(schema string, rowID uuid.UUID) *forma.EntityOperation {
	return &forma.EntityOperation{EntityIdentifier: forma.EntityIdentifier{SchemaName: schema, RowID: rowID}, Type: forma.OperationDelete}
}

// mustCreate creates a row outside any Tx and answers its row ID.
func (e *txEnv) mustCreate(t *testing.T, schema string, data map[string]any) uuid.UUID {
	t.Helper()
	record, err := e.manager.Create(e.ctx, txCreateOp(schema, data))
	require.NoError(t, err)
	return record.RowID
}

// rowSnapshot is one row's complete stored state in all three tables, as
// PostgreSQL renders it: equal snapshots mean nothing about the row changed.
type rowSnapshot struct {
	main, eav, changeLog string
}

func (e *txEnv) snapshot(t *testing.T, schema string, rowID uuid.UUID) rowSnapshot {
	t.Helper()
	schemaID := e.schemaID(t, schema)
	var s rowSnapshot
	queries := []struct {
		dest *string
		sql  string
	}{
		{&s.main, "SELECT COALESCE((SELECT row_to_json(m)::text FROM %s m WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2), '')"},
		{&s.eav, "SELECT COALESCE(json_agg(e ORDER BY attr_id, array_indices)::text, '[]') FROM %s e WHERE schema_id = $1 AND row_id = $2"},
		{&s.changeLog, "SELECT COALESCE(json_agg(c ORDER BY flushed_at, changed_at)::text, '[]') FROM %s c WHERE schema_id = $1 AND row_id = $2"},
	}
	for i, table := range []string{e.tables.EntityMain, e.tables.EAVData, e.tables.ChangeLog} {
		sql := fmt.Sprintf(queries[i].sql, sanitizeIdentifier(table))
		require.NoError(t, e.postgresPool.QueryRow(e.ctx, sql, schemaID, rowID).Scan(queries[i].dest))
	}
	return s
}

// schemaRowCount counts every row of schema in entity_main, EAV and
// change_log together: zero means no write to that schema survived.
func (e *txEnv) schemaRowCount(t *testing.T, schema string) int {
	t.Helper()
	sql := fmt.Sprintf(`SELECT (SELECT count(*) FROM %s WHERE ltbase_schema_id = $1)
		+ (SELECT count(*) FROM %s WHERE schema_id = $1)
		+ (SELECT count(*) FROM %s WHERE schema_id = $1)`,
		sanitizeIdentifier(e.tables.EntityMain), sanitizeIdentifier(e.tables.EAVData), sanitizeIdentifier(e.tables.ChangeLog))
	var n int
	require.NoError(t, e.postgresPool.QueryRow(e.ctx, sql, e.schemaID(t, schema)).Scan(&n))
	return n
}

// hotVersion answers a row's stored ltbase_updated_at and its hot change_log
// entries' changed_at values, read on q: the pool, or a session's own
// transaction to see its uncommitted writes.
func (e *txEnv) hotVersion(t *testing.T, q singleRowQuerier, schema string, rowID uuid.UUID) (int64, []int64) {
	t.Helper()
	schemaID := e.schemaID(t, schema)
	var updatedAt int64
	require.NoError(t, q.QueryRow(e.ctx, fmt.Sprintf("SELECT ltbase_updated_at FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2",
		sanitizeIdentifier(e.tables.EntityMain)), schemaID, rowID).Scan(&updatedAt))
	var hot []int64
	require.NoError(t, q.QueryRow(e.ctx, fmt.Sprintf("SELECT COALESCE(array_agg(changed_at), '{}') FROM %s WHERE schema_id = $1 AND row_id = $2 AND flushed_at = 0",
		sanitizeIdentifier(e.tables.ChangeLog)), schemaID, rowID).Scan(&hot))
	return updatedAt, hot
}

// attrs reads a row through the manager, outside any Tx.
func (e *txEnv) attrs(t *testing.T, schema string, rowID uuid.UUID) map[string]any {
	t.Helper()
	record, err := e.manager.Get(e.ctx, &forma.QueryRequest{SchemaName: schema, RowID: &rowID})
	require.NoError(t, err)
	return record.Attributes
}

// inTx runs fn in a Tx on the env's context.
func (e *txEnv) inTx(fn func(ctx context.Context, tx forma.Tx) error, opts ...forma.TxOption) error {
	return e.txr.InTx(e.ctx, fn, opts...)
}
