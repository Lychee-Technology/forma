package internal

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemameta"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsertEAVAttributesNoop(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	repo := &DBPersistentRecordRepository{}
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
	attrs := []model.EAVRecord{
		{SchemaID: 1, RowID: rowID, AttrID: 10, ArrayIndices: "", ValueText: &text},
		{SchemaID: 1, RowID: rowID, AttrID: 11, ArrayIndices: "0", ValueNumeric: &num},
	}

	mock.ExpectExec(`INSERT INTO "eav_table"`).
		WithArgs(
			int16(1), rowID, int16(10), "", &text, (*float64)(nil),
			int16(1), rowID, int16(11), "0", (*string)(nil), &num,
		).
		WillReturnResult(pgxmock.NewResult("INSERT", int64(len(attrs))))

	repo := &DBPersistentRecordRepository{}
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
	attrs := []model.EAVRecord{
		{SchemaID: 1, RowID: rowID, AttrID: 10, ArrayIndices: "", ValueText: &text},
	}

	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("mock_schema", 1, forma.SchemaAttributeCache{
		"a": {AttributeName: "a", AttributeID: 10, ValueType: forma.ValueTypeText},
	}))

	mock.ExpectExec(`^DELETE FROM "eav_table" WHERE schema_id = \$1 AND row_id = \$2 AND attr_id = ANY\(\$3\)$`).
		WithArgs(int16(1), rowID, []int16{10}).
		WillReturnResult(pgxmock.NewResult("DELETE", 1))
	mock.ExpectExec(`INSERT INTO "eav_table"`).
		WithArgs(int16(1), rowID, int16(10), "", &text, (*float64)(nil)).
		WillReturnResult(pgxmock.NewResult("INSERT", int64(len(attrs))))

	repo := &DBPersistentRecordRepository{metadataCache: mc}
	require.NoError(t, repo.replaceEAVAttributes(ctx, mock, "eav_table", 1, rowID, attrs))
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestReplaceEAVAttributes_DeleteScopedToCurrentSchemaAttrIDs is the #294
// "preserve" half: attrID 99 was dropped by schema evolution, so it is absent
// from the delete scope and its EAV rows survive the update untouched. The
// scope is sorted, because map iteration order is random and the bind value
// must be deterministic — the ids below are declared out of order on purpose.
//
// Every attribute carries a distinct id. This test previously registered two
// names on attributeID 12 to exercise the dedup in knownAttrIDs, but since #342
// duplicate attribute ids are rejected at every registration path — the file
// and DB loaders and MetadataCache.RegisterSchema alike — so that state is
// unconstructible and the slices.Compact there is defence in depth only.
func TestReplaceEAVAttributes_DeleteScopedToCurrentSchemaAttrIDs(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")

	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("evolved_schema", 4, forma.SchemaAttributeCache{
		"zeta":  {AttributeName: "zeta", AttributeID: 21, ValueType: forma.ValueTypeText},
		"alpha": {AttributeName: "alpha", AttributeID: 3, ValueType: forma.ValueTypeNumeric},
		"mid":   {AttributeName: "mid", AttributeID: 12, ValueType: forma.ValueTypeText},
		"beta":  {AttributeName: "beta", AttributeID: 7, ValueType: forma.ValueTypeText},
	}))

	// attrID 99 (dropped attribute) is deliberately NOT in the scope.
	mock.ExpectExec(`^DELETE FROM "eav_table" WHERE schema_id = \$1 AND row_id = \$2 AND attr_id = ANY\(\$3\)$`).
		WithArgs(int16(4), rowID, []int16{3, 7, 12, 21}).
		WillReturnResult(pgxmock.NewResult("DELETE", 1))

	repo := &DBPersistentRecordRepository{metadataCache: mc}
	require.NoError(t, repo.replaceEAVAttributes(ctx, mock, "eav_table", 4, rowID, nil))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReplaceEAVAttributes_NilMetadataCache_SkipsDelete(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")

	// No DELETE and no INSERT expected — the scope cannot be resolved.
	repo := &DBPersistentRecordRepository{}
	err = repo.replaceEAVAttributes(ctx, mock, "eav_table", 3, rowID, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no metadata cache configured")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReplaceEAVAttributes_MissingSchemaCache_SkipsDelete(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")

	// No DELETE and no INSERT expected — the scope cannot be resolved.
	repo := &DBPersistentRecordRepository{metadataCache: schemameta.NewMetadataCache()}
	err = repo.replaceEAVAttributes(ctx, mock, "eav_table", 9, rowID, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no cache for schema id 9")
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

	repo := NewDBPersistentRecordRepository(mock, nil)
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
