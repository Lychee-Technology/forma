package internal

import (
	"context"
	"regexp"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The engine selects the row locking (#633); an unknown engine is refused
// rather than run with PostgreSQL's locks.
func TestRowLockingForEngine(t *testing.T) {
	for engine, want := range map[forma.DatabaseEngine]rowLocking{
		"":                     advisoryLocking{},
		forma.EnginePostgreSQL: advisoryLocking{},
		forma.EngineAuroraDSQL: guardRowLocking{},
	} {
		got, err := rowLockingFor(engine)
		require.NoError(t, err, "engine %q", engine)
		assert.Equal(t, want, got, "engine %q", engine)
	}
	_, err := WithDatabaseEngine("dsql")
	var configErr *forma.ConfigError
	require.ErrorAs(t, err, &configErr)
	assert.Equal(t, "database.engine", configErr.Field)
}

// The aurora-dsql engine begins every write at DSQL's one isolation level
// and re-checks rows without a locking clause; the PostgreSQL engine keeps
// READ COMMITTED where a write reads under its lock.
func TestRowLockingTransactionShape(t *testing.T) {
	assert.Equal(t, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, advisoryLocking{}.writeTxOptions(true))
	assert.Equal(t, pgx.TxOptions{}, advisoryLocking{}.writeTxOptions(false))
	assert.Equal(t, "FOR UPDATE OF m SKIP LOCKED", advisoryLocking{}.recheckClause(true))
	assert.Equal(t, pgx.TxOptions{}, guardRowLocking{}.writeTxOptions(true))
	assert.Equal(t, pgx.TxOptions{}, guardRowLocking{}.writeTxOptions(false))
	assert.Empty(t, guardRowLocking{}.recheckClause(true))
	assert.Empty(t, guardRowLocking{}.recheckClause(false))
}

func TestGuardUpsertSQL(t *testing.T) {
	assert.Equal(t, `INSERT INTO "m_row_guard" AS g (ltbase_schema_id, ltbase_row_id, generation) VALUES ($1, $2, 1), ($3, $4, 1)`+
		` ON CONFLICT (ltbase_schema_id, ltbase_row_id) DO UPDATE SET generation = g.generation + 1`, guardUpsertSQL(`"m_row_guard"`, 2))
}

// lockRows writes each distinct key's guard once, in sortedRowKeys order
// (#554), at most guardRowsPerStatement a statement; one upsert may not name
// a row twice.
func TestGuardRowLockingWritesDistinctKeysInOrderAndChunks(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	keys := make([]model.PersistentRecordKey, 0, guardRowsPerStatement+2)
	for range guardRowsPerStatement + 1 {
		keys = append(keys, model.PersistentRecordKey{SchemaID: 2, RowID: uuid.New()})
	}
	keys = append(keys, keys[3]) // a duplicate
	sorted := sortedRowKeys(keys[:guardRowsPerStatement+1])
	args := func(ks []model.PersistentRecordKey) []any {
		out := make([]any, 0, 2*len(ks))
		for _, k := range ks {
			out = append(out, k.SchemaID, k.RowID)
		}
		return out
	}

	table := `"entity_main_row_guard"`
	mock.ExpectBegin()
	mock.ExpectExec("^" + regexp.QuoteMeta(guardUpsertSQL(table, guardRowsPerStatement)) + "$").
		WithArgs(args(sorted[:guardRowsPerStatement])...).WillReturnResult(pgxmock.NewResult("INSERT", guardRowsPerStatement))
	mock.ExpectExec("^" + regexp.QuoteMeta(guardUpsertSQL(table, 1)) + "$").
		WithArgs(args(sorted[guardRowsPerStatement:])...).WillReturnResult(pgxmock.NewResult("INSERT", 1))

	tx, err := mock.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, guardRowLocking{}.lockRows(ctx, tx, model.StorageTables{EntityMain: "entity_main"}, keys))
	require.NoError(t, mock.ExpectationsWereMet())
}

// A write on the aurora-dsql engine sends a plain BEGIN and a guard upsert
// where the PostgreSQL engine sends READ COMMITTED and an advisory lock.
func TestDSQLEngineDeleteWritesTheGuardRow(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	engine, err := WithDatabaseEngine(forma.EngineAuroraDSQL)
	require.NoError(t, err)
	repo := NewDBPersistentRecordRepository(mock, schemameta.NewMetadataCache(), engine)
	rowID := uuid.New()
	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_data", ChangeLog: "change_log"}

	mock.ExpectBeginTx(pgx.TxOptions{})
	mock.ExpectExec("^"+regexp.QuoteMeta(guardUpsertSQL(`"entity_main_row_guard"`, 1))+"$").
		WithArgs(int16(1), rowID).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectQuery(`DELETE FROM "entity_main"`).WithArgs(int16(1), rowID).WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at"}))
	mock.ExpectRollback()

	err = repo.DeletePersistentRecord(ctx, tables, 1, rowID, model.RowGuard{})
	require.ErrorIs(t, err, forma.ErrNotFound, "no row to delete")
	require.NoError(t, mock.ExpectationsWereMet())
}
