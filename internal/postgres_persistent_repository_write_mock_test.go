package internal

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Write-path (update/delete) pgxmock coverage, split from
// postgres_persistent_repository_repo_test.go to keep both under the
// 500-line source limit. The #274 monotonic-version plumbing pins live here.

func TestUpdatePersistentRecordWithMockPool(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	// The update path scopes its EAV delete to the attributeIDs the current
	// schema can address (#294), so the repository needs a registered cache.
	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("mock_schema", 1, forma.SchemaAttributeCache{
		"a": {AttributeName: "a", AttributeID: 11, ValueType: forma.ValueTypeText},
	}))
	repo := NewDBPersistentRecordRepository(mock, mc)
	fixed := time.Date(2024, 4, 5, 6, 7, 8, 0, time.UTC)
	repo.withClock(func() time.Time { return fixed })

	rowID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	text := "bar"
	record := &model.PersistentRecord{
		SchemaID: 1,
		RowID:    rowID,
		TextItems: map[string]string{
			"text_01": "hello",
		},
		OtherAttributes: []model.EAVRecord{
			{SchemaID: 1, RowID: rowID, AttrID: 11, ArrayIndices: "", ValueText: &text},
		},
	}

	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table", ChangeLog: "change_log"}
	fixedMillis := fixed.UnixMilli()

	expected := *record
	expected.UpdatedAt = fixedMillis

	updateQuery, updateArgs, err := buildUpdateMainStatement(tables.EntityMain, &expected)
	require.NoError(t, err)
	_, eavArgs, err := buildAttributeValuesClause(record.OtherAttributes)
	require.NoError(t, err)

	// PG computes GREATEST($now, prev + 1) and RETURNING hands it back; a
	// same-millisecond prior write makes the effective version run AHEAD of
	// the frozen clock (#274). change_log must receive that effective value,
	// never the raw clock read.
	effectiveMillis := fixedMillis + 5

	mock.ExpectBegin()
	mock.ExpectQuery("^" + regexp.QuoteMeta(updateQuery) + "$").
		WithArgs(updateArgs...).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at"}).AddRow(effectiveMillis))
	mock.ExpectExec(`^DELETE FROM "eav_table" WHERE schema_id = \$1 AND row_id = \$2 AND attr_id = ANY\(\$3\)$`).
		WithArgs(int16(1), rowID, []int16{11}).
		WillReturnResult(pgxmock.NewResult("DELETE", 1))
	mock.ExpectExec(`^INSERT INTO "eav_table"`).
		WithArgs(eavArgs...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec(`^INSERT INTO "change_log"`).
		WithArgs(int16(1), rowID, int64(0), effectiveMillis, nil).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()
	mock.ExpectRollback()

	err = repo.UpdatePersistentRecord(ctx, tables, record)
	require.NoError(t, err)
	assert.Equal(t, effectiveMillis, record.UpdatedAt,
		"UpdatePersistentRecord must adopt the effective version PG computed (#274)")

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestUpdatePersistentRecord_NoSchemaCache_FailsBeforeDelete pins the #294
// read-path consistency class: without metadata the repository cannot know
// which attrIDs the current schema addresses, so it must fail rather than fall
// back to an unscoped delete that would purge dropped-attribute EAV rows.
func TestUpdatePersistentRecord_NoSchemaCache_FailsBeforeDelete(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	repo := NewDBPersistentRecordRepository(mock, schemameta.NewMetadataCache())
	fixed := time.Date(2024, 4, 5, 6, 7, 8, 0, time.UTC)
	repo.withClock(func() time.Time { return fixed })

	rowID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	record := &model.PersistentRecord{SchemaID: 7, RowID: rowID}
	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table", ChangeLog: "change_log"}

	expected := *record
	expected.UpdatedAt = fixed.UnixMilli()
	updateQuery, updateArgs, err := buildUpdateMainStatement(tables.EntityMain, &expected)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectQuery("^" + regexp.QuoteMeta(updateQuery) + "$").
		WithArgs(updateArgs...).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at"}).AddRow(fixed.UnixMilli()))
	// No EAV delete, no EAV insert, no changelog upsert — transaction rolls back.
	mock.ExpectRollback()

	err = repo.UpdatePersistentRecord(ctx, tables, record)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no cache for schema id 7")
	require.NotErrorIs(t, err, forma.ErrInvalidInput)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDeletePersistentRecordWithMockPool(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	repo := NewDBPersistentRecordRepository(mock, nil)
	fixed := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)
	repo.withClock(func() time.Time { return fixed })

	rowID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table", ChangeLog: "change_log"}
	fixedMillis := fixed.UnixMilli()

	// The deleted row's version runs AHEAD of the frozen clock (per-row
	// monotonic updates, #274): the tombstone must be stamped strictly after
	// it, or the delete would lose the LWW tie to the version it removes.
	prevUpdatedAt := fixedMillis + 10
	wantStamp := prevUpdatedAt + 1

	mock.ExpectBegin()
	mock.ExpectExec(`^SELECT pg_advisory_xact_lock`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectQuery(`^DELETE FROM "entity_main"`).
		WithArgs(int16(1), rowID).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at"}).AddRow(prevUpdatedAt))
	mock.ExpectExec(`^DELETE FROM "eav_table"`).
		WithArgs(int16(1), rowID).
		WillReturnResult(pgxmock.NewResult("DELETE", 1))
	mock.ExpectExec(`^INSERT INTO "change_log"`).
		WithArgs(int16(1), rowID, int64(0), wantStamp, wantStamp).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()
	mock.ExpectRollback()

	err = repo.DeletePersistentRecord(ctx, tables, 1, rowID)
	require.NoError(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDeletePersistentRecord_WhenRowMissing_ReturnsNotFound(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	repo := NewDBPersistentRecordRepository(mock, nil)
	rowID := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table", ChangeLog: "change_log"}

	mock.ExpectBegin()
	mock.ExpectExec(`^SELECT pg_advisory_xact_lock`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectQuery(`^DELETE FROM "entity_main"`).
		WithArgs(int16(1), rowID).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at"})) // no row deleted
	mock.ExpectRollback()

	err = repo.DeletePersistentRecord(ctx, tables, 1, rowID)
	require.Error(t, err)
	require.ErrorIs(t, err, forma.ErrNotFound)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDeletePersistentRecord_WhenRowMissing_DoesNotWriteChangelog(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	repo := NewDBPersistentRecordRepository(mock, nil)
	rowID := uuid.MustParse("cccccccc-cccc-cccc-cccc-cccccccccccc")
	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table", ChangeLog: "change_log"}

	mock.ExpectBegin()
	mock.ExpectExec(`^SELECT pg_advisory_xact_lock`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectQuery(`^DELETE FROM "entity_main"`).
		WithArgs(int16(1), rowID).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at"})) // no row deleted
	// No EAV delete, no changelog upsert expected — transaction rolls back
	mock.ExpectRollback()

	err = repo.DeletePersistentRecord(ctx, tables, 1, rowID)
	require.Error(t, err)
	require.ErrorIs(t, err, forma.ErrNotFound)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdatePersistentRecord_WhenRowMissing_ReturnsNotFound(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	repo := NewDBPersistentRecordRepository(mock, nil)
	fixed := time.Date(2024, 4, 5, 6, 7, 8, 0, time.UTC)
	repo.withClock(func() time.Time { return fixed })
	fixedMillis := fixed.UnixMilli()

	rowID := uuid.MustParse("99999999-9999-9999-9999-999999999999")
	record := &model.PersistentRecord{
		SchemaID:  1,
		RowID:     rowID,
		UpdatedAt: fixedMillis,
		TextItems: map[string]string{"text_01": "hello"},
	}
	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table", ChangeLog: "change_log"}

	updateQuery, updateArgs, err := buildUpdateMainStatement(tables.EntityMain, record)
	require.NoError(t, err)

	mock.ExpectBegin()
	// UPDATE matches no row — RETURNING yields no rows
	mock.ExpectQuery("^" + regexp.QuoteMeta(updateQuery) + "$").
		WithArgs(updateArgs...).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at"}))
	mock.ExpectRollback()

	err = repo.UpdatePersistentRecord(ctx, tables, record)
	require.Error(t, err)
	require.ErrorIs(t, err, forma.ErrNotFound)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdatePersistentRecord_WhenRowMissing_DoesNotWriteEAVOrChangelog(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	repo := NewDBPersistentRecordRepository(mock, nil)
	fixed := time.Date(2024, 4, 5, 6, 7, 8, 0, time.UTC)
	repo.withClock(func() time.Time { return fixed })
	fixedMillis := fixed.UnixMilli()

	rowID := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	text := "v"
	record := &model.PersistentRecord{
		SchemaID:        1,
		RowID:           rowID,
		UpdatedAt:       fixedMillis,
		TextItems:       map[string]string{"text_01": "hello"},
		OtherAttributes: []model.EAVRecord{{SchemaID: 1, RowID: rowID, AttrID: 5, ValueText: &text}},
	}
	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table", ChangeLog: "change_log"}

	updateQuery, updateArgs, err := buildUpdateMainStatement(tables.EntityMain, record)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectQuery("^" + regexp.QuoteMeta(updateQuery) + "$").
		WithArgs(updateArgs...).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at"})) // no row matched
	// No EAV delete, no EAV insert, no changelog upsert expected — transaction rolls back
	mock.ExpectRollback()

	err = repo.UpdatePersistentRecord(ctx, tables, record)
	require.Error(t, err)
	require.ErrorIs(t, err, forma.ErrNotFound)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBatchDeletePersistentRecordsRollsBackOnError(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	repo := NewDBPersistentRecordRepository(mock, nil)

	rowID1 := uuid.MustParse("66666666-6666-6666-6666-666666666666")
	rowID2 := uuid.MustParse("77777777-7777-7777-7777-777777777777")
	keys := []model.PersistentRecordKey{
		{SchemaID: 1, RowID: rowID1},
		{SchemaID: 1, RowID: rowID2},
	}
	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table", ChangeLog: ""}

	mock.ExpectBegin()
	mock.ExpectExec(`^SELECT pg_advisory_xact_lock`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectQuery(`^DELETE FROM "entity_main"`).
		WithArgs(int16(1), rowID1).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at"}).AddRow(int64(100)))
	mock.ExpectExec(`^DELETE FROM "eav_table"`).
		WithArgs(int16(1), rowID1).
		WillReturnResult(pgxmock.NewResult("DELETE", 1))
	mock.ExpectExec(`^SELECT pg_advisory_xact_lock`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectQuery(`^DELETE FROM "entity_main"`).
		WithArgs(int16(1), rowID2).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at"}).AddRow(int64(100)))
	mock.ExpectExec(`^DELETE FROM "eav_table"`).
		WithArgs(int16(1), rowID2).
		WillReturnError(assert.AnError)
	mock.ExpectRollback()

	err = repo.BatchDeletePersistentRecords(ctx, tables, keys)
	require.Error(t, err)
	require.Contains(t, err.Error(), "key[1]")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBatchDeletePersistentRecords_WhenRowMissing_ReturnsNotFound(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	repo := NewDBPersistentRecordRepository(mock, nil)

	rowID1 := uuid.MustParse("dddddddd-dddd-dddd-dddd-dddddddddddd")
	rowID2 := uuid.MustParse("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")
	keys := []model.PersistentRecordKey{
		{SchemaID: 1, RowID: rowID1},
		{SchemaID: 1, RowID: rowID2},
	}
	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table", ChangeLog: "change_log"}

	mock.ExpectBegin()
	// First key deletes successfully.
	mock.ExpectExec(`^SELECT pg_advisory_xact_lock`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectQuery(`^DELETE FROM "entity_main"`).
		WithArgs(int16(1), rowID1).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at"}).AddRow(int64(100)))
	mock.ExpectExec(`^DELETE FROM "eav_table"`).
		WithArgs(int16(1), rowID1).
		WillReturnResult(pgxmock.NewResult("DELETE", 1))
	mock.ExpectExec(`^INSERT INTO "change_log"`).
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	// Second key row does not exist — RETURNING yields no rows.
	mock.ExpectExec(`^SELECT pg_advisory_xact_lock`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectQuery(`^DELETE FROM "entity_main"`).
		WithArgs(int16(1), rowID2).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at"}))
	mock.ExpectRollback()

	err = repo.BatchDeletePersistentRecords(ctx, tables, keys)
	require.Error(t, err)
	require.ErrorIs(t, err, forma.ErrNotFound)
	require.Contains(t, err.Error(), "key[1]")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBatchDeletePersistentRecords_WhenRowMissing_DoesNotWriteEAVOrChangelog(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	repo := NewDBPersistentRecordRepository(mock, nil)

	rowID := uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
	keys := []model.PersistentRecordKey{{SchemaID: 1, RowID: rowID}}
	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table", ChangeLog: "change_log"}

	mock.ExpectBegin()
	mock.ExpectExec(`^SELECT pg_advisory_xact_lock`).
		WithArgs(pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	mock.ExpectQuery(`^DELETE FROM "entity_main"`).
		WithArgs(int16(1), rowID).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at"})) // no row deleted
	// No EAV delete, no changelog upsert expected — transaction rolls back.
	mock.ExpectRollback()

	err = repo.BatchDeletePersistentRecords(ctx, tables, keys)
	require.Error(t, err)
	require.ErrorIs(t, err, forma.ErrNotFound)

	require.NoError(t, mock.ExpectationsWereMet())
}
