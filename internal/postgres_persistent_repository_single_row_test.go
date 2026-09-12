package internal

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// singleRowColumns mirrors the projection buildSingleRowSelect selects: every
// entity_main column, then the aggregated attributes JSON.
func singleRowColumns() []string {
	columns := make([]string, 0, len(model.EntityMainColumnDescriptors)+1)
	for _, desc := range model.EntityMainColumnDescriptors {
		columns = append(columns, desc.Name)
	}
	return append(columns, "attributes_json")
}

// singleRowValues builds one scannable row for the single-row projection.
// override supplies the columns the test cares about; every other column
// scans as NULL.
func singleRowValues(override map[string]any, attributesJSON string) []any {
	values := make([]any, 0, len(model.EntityMainColumnDescriptors)+1)
	for _, desc := range model.EntityMainColumnDescriptors {
		values = append(values, override[desc.Name])
	}
	return append(values, []byte(attributesJSON))
}

// TestGetPersistentRecordReadsMainAndEAVInOneStatement is the #457 torn-read
// pin. The read used to issue two untransacted statements (entity_main, then
// eav_data), so a commit landing between them handed back a record mixing
// version N main columns with version N+1 attributes. One statement is one
// snapshot: pgxmock is told to expect exactly one query, and a second would
// fail ExpectationsWereMet.
func TestGetPersistentRecordReadsMainAndEAVInOneStatement(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	values := singleRowValues(map[string]any{
		"ltbase_schema_id":  int64(1),
		"ltbase_row_id":     rowID.String(),
		"ltbase_created_at": int64(100),
		"ltbase_updated_at": int64(200),
		"text_01":           "hello",
	}, `[{"schema_id":1,"row_id":"`+rowID.String()+
		`","attr_id":10,"array_indices":"","value_text":"foo","value_numeric":null},`+
		`{"schema_id":1,"row_id":"`+rowID.String()+
		`","attr_id":11,"array_indices":"0","value_text":null,"value_numeric":42.5}]`)

	mock.ExpectQuery(`SELECT .* FROM "entity_main" m`).
		WithArgs(int16(1), rowID).
		WillReturnRows(pgxmock.NewRows(singleRowColumns()).AddRow(values...))

	repo := NewDBPersistentRecordRepository(mock, nil)
	record, err := repo.GetPersistentRecord(ctx, model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table"}, 1, rowID)
	require.NoError(t, err)
	require.NotNil(t, record)

	assert.Equal(t, int16(1), record.SchemaID)
	assert.Equal(t, rowID, record.RowID)
	assert.Equal(t, int64(100), record.CreatedAt)
	assert.Equal(t, int64(200), record.UpdatedAt)
	assert.Equal(t, map[string]string{"text_01": "hello"}, record.TextItems)
	require.Len(t, record.OtherAttributes, 2)
	require.NotNil(t, record.OtherAttributes[0].ValueText)
	assert.Equal(t, "foo", *record.OtherAttributes[0].ValueText)
	require.NotNil(t, record.OtherAttributes[1].ValueNumeric)
	assert.InDelta(t, 42.5, *record.OtherAttributes[1].ValueNumeric, 1e-9)

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestGetPersistentRecordScansEveryColumnKind keeps the coverage the deleted
// loadMainRecord tests carried: every typed buffer in the main projection
// still reaches the right map on the record.
func TestGetPersistentRecordScansEveryColumnKind(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	uuid2 := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	values := singleRowValues(map[string]any{
		"ltbase_schema_id":  int64(1),
		"ltbase_row_id":     rowID.String(),
		"ltbase_created_at": int64(100),
		"ltbase_updated_at": int64(200),
		"ltbase_deleted_at": int64(300),
		"ltbase_created_by": "creator-1",
		"ltbase_updated_by": "updater-1",
		"ltbase_deleted_by": "deleter-1",
		"text_01":           "hello",
		"smallint_01":       int64(7),
		"integer_01":        int64(11),
		"bigint_01":         int64(123),
		"double_01":         float64(9.5),
		"uuid_01":           uuid2.String(),
	}, `[]`)

	mock.ExpectQuery(`SELECT .* FROM "entity_main" m`).
		WithArgs(int16(1), rowID).
		WillReturnRows(pgxmock.NewRows(singleRowColumns()).AddRow(values...))

	repo := NewDBPersistentRecordRepository(mock, nil)
	record, err := repo.GetPersistentRecord(ctx, model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table"}, 1, rowID)
	require.NoError(t, err)
	require.NotNil(t, record)

	assert.Equal(t, int16(1), record.SchemaID)
	assert.Equal(t, rowID, record.RowID)
	assert.Equal(t, int64(100), record.CreatedAt)
	assert.Equal(t, int64(200), record.UpdatedAt)
	require.NotNil(t, record.DeletedAt)
	assert.Equal(t, int64(300), *record.DeletedAt)
	assert.Equal(t, map[string]string{
		"ltbase_created_by": "creator-1",
		"ltbase_deleted_by": "deleter-1",
		"ltbase_updated_by": "updater-1",
		"text_01":           "hello",
	}, record.TextItems)
	assert.Equal(t, map[string]int16{"smallint_01": 7}, record.Int16Items)
	assert.Equal(t, map[string]int32{"integer_01": 11}, record.Int32Items)
	assert.Equal(t, map[string]int64{"bigint_01": 123}, record.Int64Items)
	assert.Equal(t, map[string]float64{"double_01": 9.5}, record.Float64Items)
	assert.Equal(t, map[string]uuid.UUID{"uuid_01": uuid2}, record.UUIDItems)
	assert.Empty(t, record.OtherAttributes)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetPersistentRecordSingleStatementNotFound(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rowID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	mock.ExpectQuery(`SELECT .* FROM "entity_main" m`).
		WithArgs(int16(1), rowID).
		WillReturnRows(pgxmock.NewRows(singleRowColumns()))

	repo := NewDBPersistentRecordRepository(mock, nil)
	record, err := repo.GetPersistentRecord(ctx, model.StorageTables{EntityMain: "entity_main", EAVData: "eav_table"}, 1, rowID)
	require.NoError(t, err)
	assert.Nil(t, record)

	require.NoError(t, mock.ExpectationsWereMet())
}

// The pool type must satisfy singleRowQuerier so the same loader can run on a
// transaction in the #457 merge path.
var _ singleRowQuerier = (*pgxpool.Pool)(nil)
