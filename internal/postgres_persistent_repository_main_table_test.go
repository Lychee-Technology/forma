package internal

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lychee-technology/forma"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsertAndUpdateMainRow(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	record := &model.PersistentRecord{
		SchemaID:  1,
		RowID:     rowID,
		CreatedAt: 10,
		UpdatedAt: 20,
		TextItems: map[string]string{"text_01": "hello"},
	}

	insertQuery, insertArgs, err := buildInsertMainStatement("entity_main", record)
	require.NoError(t, err)
	mock.ExpectExec("^" + regexp.QuoteMeta(insertQuery) + "$").
		WithArgs(insertArgs...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	updateQuery, updateArgs, err := buildUpdateMainStatement("entity_main", record)
	require.NoError(t, err)
	// PG computes the effective version (GREATEST over the previous row
	// version, #274) and updateMainRow adopts it into record.UpdatedAt.
	mock.ExpectQuery("^" + regexp.QuoteMeta(updateQuery) + "$").
		WithArgs(updateArgs...).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at"}).AddRow(int64(21)))

	repo := &DBPersistentRecordRepository{}
	inserted, err := repo.insertMainRow(ctx, mock, "entity_main", record)
	require.NoError(t, err)
	assert.True(t, inserted)
	require.NoError(t, repo.updateMainRow(ctx, mock, "entity_main", record))
	assert.Equal(t, int64(21), record.UpdatedAt, "updateMainRow must adopt the RETURNING'd effective version")

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestMergeClassifiesUniqueViolationAsConflict is the pre-#645 regression,
// moved from updateMainRow to the write path: a 23505 is now classified once at
// the transaction boundary (inWriteTx) rather than at each statement, and
// must still satisfy ErrConflict with the unchanged published message.
func TestMergeClassifiesUniqueViolationAsConflict(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	record := &model.PersistentRecord{SchemaID: 1, RowID: rowID, TextItems: map[string]string{"text_01": "hello"}}
	repo := NewDBPersistentRecordRepository(mock, nil)
	repo.withClock(func() time.Time { return time.UnixMilli(20) })
	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table", ChangeLog: "change_log"}

	expected := *record
	expected.UpdatedAt = 20
	updateQuery, updateArgs, err := buildUpdateMainStatement(tables.EntityMain, &expected)
	require.NoError(t, err)
	pgDetail := "Key (ltbase_schema_id, ltbase_row_id)=(1, 11111111-1111-1111-1111-111111111111) already exists."

	mock.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	mock.ExpectExec(`^SELECT pg_advisory_xact_lock`).WithArgs(pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectQuery(`SELECT .* FROM "entity_main" m`).WithArgs(int16(1), rowID).
		WillReturnRows(pgxmock.NewRows(singleRowColumns()).AddRow(singleRowValues(map[string]any{
			"ltbase_schema_id": int64(1),
			"ltbase_row_id":    rowID.String(),
		}, `[]`)...))
	mock.ExpectQuery("^" + regexp.QuoteMeta(updateQuery) + "$").WithArgs(updateArgs...).
		WillReturnError(&pgconn.PgError{Code: "23505", Detail: pgDetail, ConstraintName: "entity_main_pkey"})
	mock.ExpectRollback()

	_, err = repo.MergePersistentRecord(ctx, tables, 1, rowID, model.RowGuard{},
		func(context.Context, *model.PersistentRecord, model.TxClock) (*model.PersistentRecord, error) {
			return record, nil
		})
	require.Error(t, err)
	assert.ErrorIs(t, err, forma.ErrConflict)
	assert.ErrorIs(t, err, forma.ErrUniqueConflict)

	msg, ok := forma.ResolvePublicMessage(err)
	require.True(t, ok, "a unique conflict must publish: %v", err)
	assert.Equal(t, "the write conflicts with a row that already exists", msg)
	assert.Contains(t, err.Error(), pgDetail, "the operator copy must keep the driver detail")
	assert.Contains(t, err.Error(), "update main row for "+rowID.String(), "the operator copy must keep the write path's context")
	assert.True(t, forma.HasOperatorDetail(err))

	require.NoError(t, mock.ExpectationsWereMet())
}
