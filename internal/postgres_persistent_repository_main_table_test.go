package internal

import (
	"context"
	"regexp"
	"testing"

	"github.com/google/uuid"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadMainRecordParsesColumns(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	uuid2 := uuid.MustParse("22222222-2222-2222-2222-222222222222")

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
		case "ltbase_deleted_at":
			values = append(values, int64(300))
		case "text_01":
			values = append(values, "hello")
		case "smallint_01":
			values = append(values, int64(7))
		case "integer_01":
			values = append(values, int64(11))
		case "bigint_01":
			values = append(values, int64(123))
		case "double_01":
			values = append(values, float64(9.5))
		case "uuid_01":
			values = append(values, uuid2.String())
		default:
			values = append(values, nil)
		}
	}

	rows := pgxmock.NewRows(columns).AddRow(values...)
	mock.ExpectQuery(`SELECT .* FROM "entity_main"`).
		WithArgs(int16(1), rowID).
		WillReturnRows(rows)

	repo := NewDBPersistentRecordRepository(mock, nil, nil)
	record, err := repo.loadMainRecord(ctx, "entity_main", 1, rowID)
	require.NoError(t, err)
	require.NotNil(t, record)

	assert.Equal(t, int16(1), record.SchemaID)
	assert.Equal(t, rowID, record.RowID)
	assert.Equal(t, int64(100), record.CreatedAt)
	assert.Equal(t, int64(200), record.UpdatedAt)
	require.NotNil(t, record.DeletedAt)
	assert.Equal(t, int64(300), *record.DeletedAt)

	assert.Equal(t, map[string]string{"text_01": "hello"}, record.TextItems)
	assert.Equal(t, map[string]int16{"smallint_01": 7}, record.Int16Items)
	assert.Equal(t, map[string]int32{"integer_01": 11}, record.Int32Items)
	assert.Equal(t, map[string]int64{"bigint_01": 123}, record.Int64Items)
	assert.Equal(t, map[string]float64{"double_01": 9.5}, record.Float64Items)
	assert.Equal(t, map[string]uuid.UUID{"uuid_01": uuid2}, record.UUIDItems)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLoadMainRecordNotFound(t *testing.T) {
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
	record, err := repo.loadMainRecord(ctx, "entity_main", 1, rowID)
	require.NoError(t, err)
	assert.Nil(t, record)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertAndUpdateMainRow(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	record := &PersistentRecord{
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
	mock.ExpectExec("^" + regexp.QuoteMeta(updateQuery) + "$").
		WithArgs(updateArgs...).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	repo := &DBPersistentRecordRepository{}
	require.NoError(t, repo.insertMainRow(ctx, mock, "entity_main", record))
	require.NoError(t, repo.updateMainRow(ctx, mock, "entity_main", record))

	require.NoError(t, mock.ExpectationsWereMet())
}
