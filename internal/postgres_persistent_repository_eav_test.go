package internal

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsertEAVAttributesNoop(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	repo := &PostgresPersistentRecordRepository{}
	require.NoError(t, repo.insertEAVAttributes(ctx, mock, "eav_table", nil))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertEAVAttributesExecutesBatch(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	text := "foo"
	num := 42.5
	attrs := []EAVRecord{
		{SchemaID: 1, RowID: rowID, AttrID: 10, ArrayIndices: "", ValueText: &text},
		{SchemaID: 1, RowID: rowID, AttrID: 11, ArrayIndices: "0", ValueNumeric: &num},
	}

	mock.ExpectExec(`INSERT INTO "eav_table"`).
		WithArgs(
			int16(1), rowID, int16(10), "", &text, (*float64)(nil),
			int16(1), rowID, int16(11), "0", (*string)(nil), &num,
		).
		WillReturnResult(pgxmock.NewResult("INSERT", int64(len(attrs))))

	repo := &PostgresPersistentRecordRepository{}
	require.NoError(t, repo.insertEAVAttributes(ctx, mock, "eav_table", attrs))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReplaceEAVAttributes(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	text := "foo"
	attrs := []EAVRecord{
		{SchemaID: 1, RowID: rowID, AttrID: 10, ArrayIndices: "", ValueText: &text},
	}

	mock.ExpectExec(`DELETE FROM "eav_table"`).
		WithArgs(int16(1), rowID).
		WillReturnResult(pgxmock.NewResult("DELETE", 1))
	mock.ExpectExec(`INSERT INTO "eav_table"`).
		WithArgs(int16(1), rowID, int16(10), "", &text, (*float64)(nil)).
		WillReturnResult(pgxmock.NewResult("INSERT", int64(len(attrs))))

	repo := &PostgresPersistentRecordRepository{}
	require.NoError(t, repo.replaceEAVAttributes(ctx, mock, "eav_table", 1, rowID, attrs))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestFetchAttributes(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	text := "foo"
	num := 42.5
	rows := pgxmock.NewRows([]string{"schema_id", "row_id", "attr_id", "array_indices", "value_text", "value_numeric"}).
		AddRow(int16(1), rowID, int16(10), "", &text, (*float64)(nil)).
		AddRow(int16(1), rowID, int16(11), "0", (*string)(nil), &num)

	mock.ExpectQuery(`SELECT schema_id, row_id, attr_id, array_indices, value_text, value_numeric FROM "eav_table"`).
		WithArgs(int16(1), rowID).
		WillReturnRows(rows)

	repo := NewPostgresPersistentRecordRepository(mock, nil)
	attrs, err := repo.fetchAttributes(ctx, "eav_table", 1, rowID)
	require.NoError(t, err)
	require.Len(t, attrs, 2)

	assert.Equal(t, int16(1), attrs[0].SchemaID)
	assert.Equal(t, rowID, attrs[0].RowID)
	assert.Equal(t, int16(10), attrs[0].AttrID)
	assert.Equal(t, "", attrs[0].ArrayIndices)
	assert.NotNil(t, attrs[0].ValueText)
	assert.Equal(t, "foo", *attrs[0].ValueText)
	assert.Nil(t, attrs[0].ValueNumeric)

	assert.Equal(t, int16(1), attrs[1].SchemaID)
	assert.Equal(t, rowID, attrs[1].RowID)
	assert.Equal(t, int16(11), attrs[1].AttrID)
	assert.Equal(t, "0", attrs[1].ArrayIndices)
	assert.Nil(t, attrs[1].ValueText)
	assert.NotNil(t, attrs[1].ValueNumeric)
	assert.Equal(t, 42.5, *attrs[1].ValueNumeric)

	require.NoError(t, mock.ExpectationsWereMet())
}
