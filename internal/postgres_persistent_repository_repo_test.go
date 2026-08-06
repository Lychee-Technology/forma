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

func optimizedQueryFixtureColumnsAndValues(rowID uuid.UUID, totalRecords int64) ([]string, []any) {
	columns := make([]string, 0, len(model.EntityMainColumnDescriptors)+4)
	values := make([]any, 0, len(model.EntityMainColumnDescriptors)+4)
	for _, desc := range model.EntityMainColumnDescriptors {
		columns = append(columns, desc.Name)
		switch desc.Name {
		case "ltbase_schema_id":
			values = append(values, int64(1))
		case "ltbase_row_id":
			values = append(values, rowID.String())
		case "ltbase_created_at":
			values = append(values, int64(100))
		case "ltbase_updated_at":
			values = append(values, int64(200))
		default:
			values = append(values, nil)
		}
	}
	columns = append(columns, "attributes_json", "total_records", "total_pages", "current_page")
	values = append(values, []byte("[]"), totalRecords, int64(1), int32(1))
	return columns, values
}

func TestInsertPersistentRecordWithMockPool(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	repo := NewDBPersistentRecordRepository(mock, nil)
	fixed := time.Date(2024, 3, 4, 5, 6, 7, 0, time.UTC)
	repo.withClock(func() time.Time { return fixed })

	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	text := "foo"
	record := &model.PersistentRecord{
		SchemaID: 1,
		RowID:    rowID,
		TextItems: map[string]string{
			"text_01": "hello",
		},
		OtherAttributes: []model.EAVRecord{
			{SchemaID: 1, RowID: rowID, AttrID: 10, ArrayIndices: "", ValueText: &text},
		},
	}

	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table", ChangeLog: "change_log"}
	fixedMillis := fixed.UnixMilli()

	expected := *record
	expected.CreatedAt = fixedMillis
	expected.UpdatedAt = fixedMillis

	insertQuery, insertArgs, err := buildInsertMainStatement(tables.EntityMain, &expected)
	require.NoError(t, err)
	_, eavArgs, err := buildAttributeValuesClause(record.OtherAttributes)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec("^" + regexp.QuoteMeta(insertQuery) + "$").
		WithArgs(insertArgs...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec(`^INSERT INTO "eav_table"`).
		WithArgs(eavArgs...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec(`^INSERT INTO "change_log"`).
		WithArgs(int16(1), rowID, int64(0), fixedMillis, nil).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()
	mock.ExpectRollback()

	err = repo.InsertPersistentRecord(ctx, tables, record)
	require.NoError(t, err)
	assert.Equal(t, fixedMillis, record.CreatedAt)
	assert.Equal(t, fixedMillis, record.UpdatedAt)

	require.NoError(t, mock.ExpectationsWereMet())
}

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

func TestBatchInsertPersistentRecordsWithMockPool(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	repo := NewDBPersistentRecordRepository(mock, nil)
	fixed := time.Date(2024, 6, 7, 8, 9, 10, 0, time.UTC)
	repo.withClock(func() time.Time { return fixed })
	fixedMillis := fixed.UnixMilli()

	rowID1 := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	rowID2 := uuid.MustParse("55555555-5555-5555-5555-555555555555")
	records := []*model.PersistentRecord{
		{SchemaID: 1, RowID: rowID1, TextItems: map[string]string{"text_01": "one"}},
		{SchemaID: 1, RowID: rowID2, TextItems: map[string]string{"text_01": "two"}},
	}
	tables := model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table", ChangeLog: "change_log"}

	expected1 := *records[0]
	expected1.CreatedAt = fixedMillis
	expected1.UpdatedAt = fixedMillis
	insertQuery1, insertArgs1, err := buildInsertMainStatement(tables.EntityMain, &expected1)
	require.NoError(t, err)

	expected2 := *records[1]
	expected2.CreatedAt = fixedMillis
	expected2.UpdatedAt = fixedMillis
	insertQuery2, insertArgs2, err := buildInsertMainStatement(tables.EntityMain, &expected2)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec("^" + regexp.QuoteMeta(insertQuery1) + "$").
		WithArgs(insertArgs1...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec(`^INSERT INTO "change_log"`).
		WithArgs(int16(1), rowID1, int64(0), fixedMillis, nil).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec("^" + regexp.QuoteMeta(insertQuery2) + "$").
		WithArgs(insertArgs2...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec(`^INSERT INTO "change_log"`).
		WithArgs(int16(1), rowID2, int64(0), fixedMillis, nil).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()
	mock.ExpectRollback()

	err = repo.BatchInsertPersistentRecords(ctx, tables, records)
	require.NoError(t, err)
	assert.Equal(t, fixedMillis, records[0].CreatedAt)
	assert.Equal(t, fixedMillis, records[0].UpdatedAt)
	assert.Equal(t, fixedMillis, records[1].CreatedAt)
	assert.Equal(t, fixedMillis, records[1].UpdatedAt)

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
	mock.ExpectQuery(`^DELETE FROM "entity_main"`).
		WithArgs(int16(1), rowID1).
		WillReturnRows(pgxmock.NewRows([]string{"ltbase_updated_at"}).AddRow(int64(100)))
	mock.ExpectExec(`^DELETE FROM "eav_table"`).
		WithArgs(int16(1), rowID1).
		WillReturnResult(pgxmock.NewResult("DELETE", 1))
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

func TestInsertUpdatePersistentRecordNilRecord(t *testing.T) {
	repo := &DBPersistentRecordRepository{}

	err := repo.InsertPersistentRecord(context.Background(), model.StorageTables{}, nil)
	require.Error(t, err)

	err = repo.UpdatePersistentRecord(context.Background(), model.StorageTables{}, nil)
	require.Error(t, err)
}

func TestGetPersistentRecordNotFound(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	columns := make([]string, 0, len(model.EntityMainColumnDescriptors))
	for _, desc := range model.EntityMainColumnDescriptors {
		columns = append(columns, desc.Name)
	}
	rows := pgxmock.NewRows(columns)

	mock.ExpectQuery(`SELECT .* FROM "entity_main"`).
		WithArgs(int16(1), rowID).
		WillReturnRows(rows)

	repo := NewDBPersistentRecordRepository(mock, nil)
	record, err := repo.GetPersistentRecord(ctx, model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table"}, 1, rowID)
	require.NoError(t, err)
	assert.Nil(t, record)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetPersistentRecordWithAttributes(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	columns := make([]string, 0, len(model.EntityMainColumnDescriptors))
	values := make([]any, 0, len(model.EntityMainColumnDescriptors))
	for _, desc := range model.EntityMainColumnDescriptors {
		columns = append(columns, desc.Name)
		switch desc.Name {
		case "ltbase_schema_id":
			values = append(values, int64(1))
		case "ltbase_row_id":
			values = append(values, rowID.String())
		case "ltbase_created_at":
			values = append(values, int64(100))
		case "ltbase_updated_at":
			values = append(values, int64(200))
		case "text_01":
			values = append(values, "hello")
		default:
			values = append(values, nil)
		}
	}
	mainRows := pgxmock.NewRows(columns).AddRow(values...)

	text := "foo"
	num := 42.5
	attrRows := pgxmock.NewRows([]string{"schema_id", "row_id", "attr_id", "array_indices", "value_text", "value_numeric"}).
		AddRow(int16(1), rowID, int16(10), "", &text, (*float64)(nil)).
		AddRow(int16(1), rowID, int16(11), "0", (*string)(nil), &num)

	mock.ExpectQuery(`SELECT .* FROM "entity_main"`).
		WithArgs(int16(1), rowID).
		WillReturnRows(mainRows)
	mock.ExpectQuery(`SELECT schema_id, row_id, attr_id, array_indices, value_text, value_numeric FROM "eav_table"`).
		WithArgs(int16(1), rowID).
		WillReturnRows(attrRows)

	repo := NewDBPersistentRecordRepository(mock, nil)
	record, err := repo.GetPersistentRecord(ctx, model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table"}, 1, rowID)
	require.NoError(t, err)
	require.NotNil(t, record)

	assert.Equal(t, int16(1), record.SchemaID)
	assert.Equal(t, rowID, record.RowID)
	assert.Equal(t, map[string]string{"text_01": "hello"}, record.TextItems)
	require.Len(t, record.OtherAttributes, 2)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestQueryPersistentRecordsWithMockPool(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	columns, values := optimizedQueryFixtureColumnsAndValues(rowID, 1)
	rows := pgxmock.NewRows(columns).AddRow(values...)
	mock.ExpectQuery("WITH anchor").WithArgs(int16(1), 50, 0).WillReturnRows(rows)

	repo := NewDBPersistentRecordRepository(mock, nil)
	page, err := repo.QueryPersistentRecords(ctx, &model.PersistentRecordQuery{
		Tables:   model.StorageTables{EntityMain: "main_table", EAVData: "eav_table"},
		SchemaID: 1,
	})
	require.NoError(t, err)
	require.NotNil(t, page)
	require.Len(t, page.Records, 1)

	assert.Equal(t, int64(1), page.TotalRecords)
	assert.Equal(t, 1, page.TotalPages)
	assert.Equal(t, 1, page.CurrentPage)
	assert.Equal(t, rowID, page.Records[0].RowID)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDBFederatedQueryEngine_FallsBackToPostgresWhenDuckDBCircuitBreakerOpen(t *testing.T) {
	t.Skip("moved to internal/federated package; test accesses unexported buildDuckSQL field")
}

func TestStreamOptimizedQuery_PropagatesRowHandlerError(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rowID := uuid.MustParse("88888888-8888-8888-8888-888888888888")
	columns, values := optimizedQueryFixtureColumnsAndValues(rowID, 1)
	rows := pgxmock.NewRows(columns).AddRow(values...)
	mock.ExpectQuery("WITH anchor").WithArgs(int16(1), 10, 0).WillReturnRows(rows)

	repo := NewDBPersistentRecordRepository(mock, nil)

	handlerCalls := 0
	total, err := repo.StreamOptimizedQuery(ctx, model.StorageTables{EntityMain: "main_table", EAVData: "eav_table"}, 1, "1=1", nil, 10, 0, nil, true, func(rp *model.PersistentRecord) error {
		handlerCalls++
		return assert.AnError
	})
	require.Error(t, err)
	require.ErrorIs(t, err, assert.AnError)
	require.Equal(t, int64(1), total)
	require.Equal(t, 1, handlerCalls)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestQueryPersistentRecordsMissingCache(t *testing.T) {
	cache := schemameta.NewMetadataCache()
	repo := NewDBPersistentRecordRepository(nil, cache)

	_, err := repo.QueryPersistentRecords(context.Background(), &model.PersistentRecordQuery{
		Tables:   model.StorageTables{EntityMain: "main_table", EAVData: "eav_table"},
		SchemaID: 1,
	})
	require.Error(t, err)
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

func TestParseEAVAttribute_NilSchemaID_ReturnsError(t *testing.T) {
	_, err := model.ParseEAVAttribute(map[string]any{
		"schema_id": nil,
		"attr_id":   float64(10),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "schema_id")
}

func TestParseEAVAttribute_NilAttrID_ReturnsError(t *testing.T) {
	_, err := model.ParseEAVAttribute(map[string]any{
		"schema_id": float64(1),
		"attr_id":   nil,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "attr_id")
}

func TestParseEAVAttribute_WrongTypeSchemaID_ReturnsError(t *testing.T) {
	_, err := model.ParseEAVAttribute(map[string]any{
		"schema_id": "not-a-number",
		"attr_id":   float64(10),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "schema_id")
}

func TestParseEAVAttribute_ValidFields_Succeeds(t *testing.T) {
	rowID := uuid.New()
	valueText := "hello"
	valueNumeric := float64(42)

	attr, err := model.ParseEAVAttribute(map[string]any{
		"schema_id":     float64(5),
		"attr_id":       float64(11),
		"row_id":        rowID.String(),
		"array_indices": "0,1",
		"value_text":    valueText,
		"value_numeric": valueNumeric,
	})
	require.NoError(t, err)
	assert.Equal(t, int16(5), attr.SchemaID)
	assert.Equal(t, int16(11), attr.AttrID)
	assert.Equal(t, rowID, attr.RowID)
	assert.Equal(t, "0,1", attr.ArrayIndices)
	require.NotNil(t, attr.ValueText)
	assert.Equal(t, "hello", *attr.ValueText)
	require.NotNil(t, attr.ValueNumeric)
	assert.Equal(t, float64(42), *attr.ValueNumeric)
}

func TestParseAttributesJSON_MalformedAttribute_ReturnsError(t *testing.T) {
	record := &model.PersistentRecord{}
	// schema_id is null in the JSON blob
	malformed := []byte(`[{"schema_id":null,"attr_id":10,"row_id":"00000000-0000-0000-0000-000000000001","array_indices":"","value_text":"x","value_numeric":null}]`)
	err := model.ParseAttributesJSON(malformed, record)
	require.Error(t, err)
	require.Contains(t, err.Error(), "schema_id")
}
