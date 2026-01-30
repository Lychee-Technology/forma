package internal

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsertPersistentRecordWithMockPool(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	repo := NewDBPersistentRecordRepository(mock, nil, nil)
	fixed := time.Date(2024, 3, 4, 5, 6, 7, 0, time.UTC)
	repo.withClock(func() time.Time { return fixed })

	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	text := "foo"
	record := &PersistentRecord{
		SchemaID: 1,
		RowID:    rowID,
		TextItems: map[string]string{
			"text_01": "hello",
		},
		OtherAttributes: []EAVRecord{
			{SchemaID: 1, RowID: rowID, AttrID: 10, ArrayIndices: "", ValueText: &text},
		},
	}

	tables := StorageTables{EntityMain: "entity_main", EAVData: "eav_table", ChangeLog: "change_log"}
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

	repo := NewDBPersistentRecordRepository(mock, nil, nil)
	fixed := time.Date(2024, 4, 5, 6, 7, 8, 0, time.UTC)
	repo.withClock(func() time.Time { return fixed })

	rowID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	text := "bar"
	record := &PersistentRecord{
		SchemaID: 1,
		RowID:    rowID,
		TextItems: map[string]string{
			"text_01": "hello",
		},
		OtherAttributes: []EAVRecord{
			{SchemaID: 1, RowID: rowID, AttrID: 11, ArrayIndices: "", ValueText: &text},
		},
	}

	tables := StorageTables{EntityMain: "entity_main", EAVData: "eav_table", ChangeLog: "change_log"}
	fixedMillis := fixed.UnixMilli()

	expected := *record
	expected.UpdatedAt = fixedMillis

	updateQuery, updateArgs, err := buildUpdateMainStatement(tables.EntityMain, &expected)
	require.NoError(t, err)
	_, eavArgs, err := buildAttributeValuesClause(record.OtherAttributes)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectExec("^" + regexp.QuoteMeta(updateQuery) + "$").
		WithArgs(updateArgs...).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectExec(`^DELETE FROM "eav_table"`).
		WithArgs(int16(1), rowID).
		WillReturnResult(pgxmock.NewResult("DELETE", 1))
	mock.ExpectExec(`^INSERT INTO "eav_table"`).
		WithArgs(eavArgs...).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec(`^INSERT INTO "change_log"`).
		WithArgs(int16(1), rowID, int64(0), fixedMillis, nil).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()
	mock.ExpectRollback()

	err = repo.UpdatePersistentRecord(ctx, tables, record)
	require.NoError(t, err)
	assert.Equal(t, fixedMillis, record.UpdatedAt)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDeletePersistentRecordWithMockPool(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	repo := NewDBPersistentRecordRepository(mock, nil, nil)
	fixed := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)
	repo.withClock(func() time.Time { return fixed })

	rowID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	tables := StorageTables{EntityMain: "entity_main", EAVData: "eav_table", ChangeLog: "change_log"}
	fixedMillis := fixed.UnixMilli()

	mock.ExpectBegin()
	mock.ExpectExec(`^DELETE FROM "entity_main"`).
		WithArgs(int16(1), rowID).
		WillReturnResult(pgxmock.NewResult("DELETE", 1))
	mock.ExpectExec(`^DELETE FROM "eav_table"`).
		WithArgs(int16(1), rowID).
		WillReturnResult(pgxmock.NewResult("DELETE", 1))
	mock.ExpectExec(`^INSERT INTO "change_log"`).
		WithArgs(int16(1), rowID, int64(0), fixedMillis, fixedMillis).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()
	mock.ExpectRollback()

	err = repo.DeletePersistentRecord(ctx, tables, 1, rowID)
	require.NoError(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertUpdatePersistentRecordNilRecord(t *testing.T) {
	repo := &DBPersistentRecordRepository{}

	err := repo.InsertPersistentRecord(context.Background(), StorageTables{}, nil)
	require.Error(t, err)

	err = repo.UpdatePersistentRecord(context.Background(), StorageTables{}, nil)
	require.Error(t, err)
}

func TestGetPersistentRecordNotFound(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	columns := make([]string, 0, len(entityMainColumnDescriptors))
	for _, desc := range entityMainColumnDescriptors {
		columns = append(columns, desc.name)
	}
	rows := pgxmock.NewRows(columns)

	mock.ExpectQuery(`SELECT .* FROM "entity_main"`).
		WithArgs(int16(1), rowID).
		WillReturnRows(rows)

	repo := NewDBPersistentRecordRepository(mock, nil, nil)
	record, err := repo.GetPersistentRecord(ctx, StorageTables{EntityMain: "entity_main", EAVData: "eav_table"}, 1, rowID)
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
	columns := make([]string, 0, len(entityMainColumnDescriptors))
	values := make([]any, 0, len(entityMainColumnDescriptors))
	for _, desc := range entityMainColumnDescriptors {
		columns = append(columns, desc.name)
		switch desc.name {
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

	repo := NewDBPersistentRecordRepository(mock, nil, nil)
	record, err := repo.GetPersistentRecord(ctx, StorageTables{EntityMain: "entity_main", EAVData: "eav_table"}, 1, rowID)
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
	columns := make([]string, 0, len(entityMainColumnDescriptors)+4)
	values := make([]any, 0, len(entityMainColumnDescriptors)+4)
	for _, desc := range entityMainColumnDescriptors {
		columns = append(columns, desc.name)
		switch desc.name {
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
	columns = append(columns, "attributes_json", "total_records", "total_pages", "current_page")
	values = append(values, []byte("[]"), int64(1), int64(1), int32(1))

	rows := pgxmock.NewRows(columns).AddRow(values...)
	mock.ExpectQuery("WITH anchor").WithArgs(int16(1), 50, 0).WillReturnRows(rows)

	repo := NewDBPersistentRecordRepository(mock, nil, nil)
	page, err := repo.QueryPersistentRecords(ctx, &PersistentRecordQuery{
		Tables:   StorageTables{EntityMain: "main_table", EAVData: "eav_table"},
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

func TestQueryPersistentRecordsMissingCache(t *testing.T) {
	cache := NewMetadataCache()
	repo := NewDBPersistentRecordRepository(nil, cache, nil)

	_, err := repo.QueryPersistentRecords(context.Background(), &PersistentRecordQuery{
		Tables:   StorageTables{EntityMain: "main_table", EAVData: "eav_table"},
		SchemaID: 1,
	})
	require.Error(t, err)
}
