package internal

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #554 pins for the batch merge: one READ COMMITTED transaction, every lock
// taken in sorted order before any read, each merge base read inside the
// transaction, writes in input order, and the answer is what storage stored.

func batchMergeTestRepo(t *testing.T, mock pgxmock.PgxPoolIface) *DBPersistentRecordRepository {
	t.Helper()
	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("mock_schema", 1, forma.SchemaAttributeCache{
		"a": {AttributeName: "a", AttributeID: 11, ValueType: forma.ValueTypeText},
	}))
	return NewDBPersistentRecordRepository(mock, mc)
}

func TestBatchMergeLocksSortedReadsInsideTransactionAndAnswersStored(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	repo := batchMergeTestRepo(t, mock)
	fixed := time.Date(2024, 4, 5, 6, 7, 8, 0, time.UTC)
	repo.withClock(func() time.Time { return fixed })
	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table", ChangeLog: "change_log"}

	// Input order descending: the lock pass must still be ascending, while
	// reads, writes and the answer follow input order.
	keys := []model.PersistentRecordKey{
		{SchemaID: 1, RowID: lockOrderHighRow},
		{SchemaID: 1, RowID: lockOrderLowRow},
	}
	merged := make([]*model.PersistentRecord, len(keys))
	effective := []int64{fixed.UnixMilli() + 5, fixed.UnixMilli()}

	mock.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	expectSortedLockPass(mock, 1, lockOrderLowRow, lockOrderHighRow)
	for i, key := range keys {
		text := "merged-" + key.RowID.String()[:8]
		merged[i] = &model.PersistentRecord{
			SchemaID:  1,
			RowID:     key.RowID,
			CreatedAt: 111,
			TextItems: map[string]string{"text_01": text},
			OtherAttributes: []model.EAVRecord{
				{SchemaID: 1, RowID: key.RowID, AttrID: 11, ValueText: &text},
			},
		}
		expected := *merged[i]
		expected.UpdatedAt = fixed.UnixMilli()
		updateQuery, updateArgs, err := buildUpdateMainStatement(tables.EntityMain, &expected)
		require.NoError(t, err)
		_, eavArgs, err := buildAttributeValuesClause(merged[i].OtherAttributes)
		require.NoError(t, err)

		mock.ExpectQuery(`SELECT .* FROM "entity_main" m\s+LEFT JOIN LATERAL`).
			WithArgs(int16(1), key.RowID).
			WillReturnRows(pgxmock.NewRows(singleRowColumns()).AddRow(singleRowValues(map[string]any{
				"ltbase_schema_id":  int64(1),
				"ltbase_row_id":     key.RowID.String(),
				"ltbase_created_at": int64(111),
				"ltbase_updated_at": int64(222),
				"text_01":           "base-" + key.RowID.String()[:8],
			}, `[]`)...))
		mock.ExpectQuery("^" + regexp.QuoteMeta(updateQuery) + "$").
			WithArgs(updateArgs...).
			WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at"}).AddRow(effective[i]))
		mock.ExpectExec(`^DELETE FROM "eav_table" WHERE schema_id = \$1 AND row_id = \$2 AND attr_id = ANY\(\$3\)$`).
			WithArgs(int16(1), key.RowID, []int16{11}).
			WillReturnResult(pgxmock.NewResult("DELETE", 1))
		mock.ExpectExec(`^INSERT INTO "eav_table"`).
			WithArgs(eavArgs...).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))
		mock.ExpectExec(`^INSERT INTO "change_log"`).
			WithArgs(int16(1), key.RowID, int64(0), effective[i], nil).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))
	}
	mock.ExpectCommit()
	mock.ExpectRollback()

	seen := make([]*model.PersistentRecord, len(keys))
	stored, err := repo.BatchMergePersistentRecords(ctx, tables, keys,
		func(_ context.Context, i int, existing *model.PersistentRecord) (*model.PersistentRecord, error) {
			seen[i] = existing
			return merged[i], nil
		})
	require.NoError(t, err)
	require.Len(t, stored, len(keys))

	for i, key := range keys {
		require.NotNil(t, seen[i], "merge[%d] must receive the row read inside the transaction", i)
		assert.Equal(t, "base-"+key.RowID.String()[:8], seen[i].TextItems["text_01"])
		assert.Equal(t, int64(222), seen[i].UpdatedAt)
		require.NotNil(t, stored[i])
		assert.Equal(t, key.RowID, stored[i].RowID, "answers are positional")
		assert.Equal(t, effective[i], stored[i].UpdatedAt,
			"the answered record must carry the effective version PG computed (#274)")
	}

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBatchMergeWhenRowMissingHandsNilAndWritesNothing(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	repo := batchMergeTestRepo(t, mock)
	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table", ChangeLog: "change_log"}
	rowID := uuid.MustParse("33333333-3333-3333-3333-333333333333")

	mock.ExpectBeginTx(pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	expectSortedLockPass(mock, 1, rowID)
	mock.ExpectQuery(`SELECT .* FROM "entity_main" m`).
		WithArgs(int16(1), rowID).
		WillReturnRows(pgxmock.NewRows(singleRowColumns()))
	// No UPDATE, no EAV write, no changelog upsert — the transaction rolls back.
	mock.ExpectRollback()

	called := false
	stored, err := repo.BatchMergePersistentRecords(ctx, tables, []model.PersistentRecordKey{{SchemaID: 1, RowID: rowID}},
		func(_ context.Context, i int, existing *model.PersistentRecord) (*model.PersistentRecord, error) {
			called = true
			assert.Equal(t, 0, i)
			assert.Nil(t, existing, "an absent row must reach merge as nil")
			return nil, forma.NotFoundf("operation[%d]: entity not found: %s/%s", i, "mock_schema", rowID)
		})
	require.Error(t, err)
	require.True(t, called)
	assert.Nil(t, stored)
	require.ErrorIs(t, err, forma.ErrNotFound)
	assert.ErrorContains(t, err, "merge record for "+rowID.String())
	msg, ok := forma.ResolvePublicMessage(err)
	require.True(t, ok, "merge callback's published message must survive the repository wrap")
	assert.Equal(t, "operation[0]: entity not found: mock_schema/"+rowID.String(), msg)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBatchMergeRejectsNilMergeAndEmptyKeys(t *testing.T) {
	repo := &DBPersistentRecordRepository{}
	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table"}

	stored, err := repo.BatchMergePersistentRecords(context.Background(), tables, []model.PersistentRecordKey{{SchemaID: 1, RowID: uuid.New()}}, nil)
	require.Error(t, err)
	assert.Nil(t, stored)

	// An empty batch is a no-op that opens no transaction (the pool is nil).
	stored, err = repo.BatchMergePersistentRecords(context.Background(), tables, nil,
		func(context.Context, int, *model.PersistentRecord) (*model.PersistentRecord, error) { return nil, nil })
	require.NoError(t, err)
	assert.Empty(t, stored)
}
