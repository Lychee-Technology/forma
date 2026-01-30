package internal

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithClockAndNowMillis(t *testing.T) {
	repo := NewDBPersistentRecordRepository(nil, nil, nil)
	fixed := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	repo.withClock(func() time.Time { return fixed })

	got := repo.nowMillis()
	assert.Equal(t, fixed.UnixMilli(), got)

	repo.withClock(nil)
	got = repo.nowMillis()
	assert.Equal(t, fixed.UnixMilli(), got)
}

func TestValidateTables(t *testing.T) {
	err := validateTables(StorageTables{})
	require.Error(t, err)

	err = validateTables(StorageTables{EntityMain: "entity", EAVData: "eav"})
	require.NoError(t, err)

	// ChangeLog is optional; writes are allowed even if ChangeLog is not provided
	err = validateWriteTables(StorageTables{EntityMain: "entity", EAVData: "eav"})
	require.NoError(t, err)

	err = validateWriteTables(StorageTables{EntityMain: "entity", EAVData: "eav", ChangeLog: "change_log"})
	require.NoError(t, err)
}

func TestSortedColumnKeys(t *testing.T) {
	keys, err := sortedColumnKeys(map[string]string{"text_02": "b", "text_01": "a"}, allowedTextColumns)
	require.NoError(t, err)
	assert.Equal(t, []string{"text_01", "text_02"}, keys)

	_, err = sortedColumnKeys(map[string]string{"nope": "x"}, allowedTextColumns)
	require.Error(t, err)
}

func TestMainColumnHelpers(t *testing.T) {
	assert.True(t, isMainTableColumn("text_01"))
	assert.True(t, isMainTableColumn("ltbase_schema_id"))
	assert.False(t, isMainTableColumn("nope"))

	desc := getMainColumnDescriptor("ltbase_schema_id")
	require.NotNil(t, desc)
	assert.Equal(t, columnKindSmallint, desc.kind)
	assert.Nil(t, getMainColumnDescriptor("nope"))
}

func TestBuildInsertMainStatement(t *testing.T) {
	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	uuid2 := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	deleted := int64(300)

	record := &PersistentRecord{
		SchemaID:  7,
		RowID:     rowID,
		CreatedAt: 100,
		UpdatedAt: 200,
		DeletedAt: &deleted,
		TextItems: map[string]string{"text_02": "b", "text_01": "a"},
		Int16Items: map[string]int16{
			"smallint_02": 2,
		},
		Int32Items: map[string]int32{
			"integer_01": 10,
		},
		Int64Items: map[string]int64{
			"bigint_01": 1000,
		},
		Float64Items: map[string]float64{
			"double_03": 3.3,
			"double_01": 1.1,
		},
		UUIDItems: map[string]uuid.UUID{
			"uuid_02": uuid2,
		},
	}

	query, args, err := buildInsertMainStatement("entity_main", record)
	require.NoError(t, err)

	expectedColumns := []string{
		"ltbase_schema_id",
		"ltbase_row_id",
		"ltbase_created_at",
		"ltbase_updated_at",
		"ltbase_deleted_at",
		"text_01",
		"text_02",
		"smallint_02",
		"integer_01",
		"bigint_01",
		"double_01",
		"double_03",
		"uuid_02",
	}
	placeholders := make([]string, len(expectedColumns))
	for i := range expectedColumns {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	expectedQuery := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		sanitizeIdentifier("entity_main"),
		strings.Join(expectedColumns, ", "),
		strings.Join(placeholders, ", "),
	)

	assert.Equal(t, expectedQuery, query)

	expectedArgs := []any{
		int16(7),
		rowID,
		int64(100),
		int64(200),
		int64(300),
		"a",
		"b",
		int16(2),
		int32(10),
		int64(1000),
		float64(1.1),
		float64(3.3),
		uuid2,
	}
	assert.Equal(t, expectedArgs, args)
}

func TestBuildInsertMainStatementRejectsUnknownColumn(t *testing.T) {
	record := &PersistentRecord{
		SchemaID: 1,
		RowID:    uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		TextItems: map[string]string{
			"unknown": "nope",
		},
	}

	_, _, err := buildInsertMainStatement("entity_main", record)
	require.Error(t, err)
}

func TestBuildUpdateMainStatement(t *testing.T) {
	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	record := &PersistentRecord{
		SchemaID:  7,
		RowID:     rowID,
		UpdatedAt: 200,
		TextItems: map[string]string{"text_02": "b", "text_01": "a"},
		Int16Items: map[string]int16{
			"smallint_01": 1,
		},
		Float64Items: map[string]float64{
			"double_02": 2.5,
		},
	}

	query, args, err := buildUpdateMainStatement("entity_main", record)
	require.NoError(t, err)

	expectedQuery := "UPDATE " + sanitizeIdentifier("entity_main") + " SET " +
		"ltbase_updated_at = $1, ltbase_deleted_at = $2, text_01 = $3, text_02 = $4, smallint_01 = $5, double_02 = $6 " +
		"WHERE ltbase_schema_id = $7 AND ltbase_row_id = $8"

	assert.Equal(t, expectedQuery, query)

	expectedArgs := []any{
		int64(200),
		nil,
		"a",
		"b",
		int16(1),
		float64(2.5),
		int16(7),
		rowID,
	}
	assert.Equal(t, expectedArgs, args)
}

func TestBuildAttributeValuesClause(t *testing.T) {
	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	text := "foo"
	num := 12.5
	attrs := []EAVRecord{
		{SchemaID: 1, RowID: rowID, AttrID: 10, ArrayIndices: "", ValueText: &text},
		{SchemaID: 1, RowID: rowID, AttrID: 11, ArrayIndices: "0", ValueNumeric: &num},
	}

	values, args, err := buildAttributeValuesClause(attrs)
	require.NoError(t, err)

	assert.Equal(t, "($1, $2, $3, $4, $5, $6), ($7, $8, $9, $10, $11, $12)", values)
	expectedArgs := []any{
		int16(1),
		rowID,
		int16(10),
		"",
		&text,
		(*float64)(nil),
		int16(1),
		rowID,
		int16(11),
		"0",
		(*string)(nil),
		&num,
	}
	assert.Equal(t, expectedArgs, args)

	values, args, err = buildAttributeValuesClause(nil)
	require.NoError(t, err)
	assert.Equal(t, "", values)
	assert.Nil(t, args)
}

func TestComputeTotalPages(t *testing.T) {
	assert.Equal(t, 0, computeTotalPages(0, 10))
	assert.Equal(t, 0, computeTotalPages(10, 0))
	assert.Equal(t, 2, computeTotalPages(10, 5))
	assert.Equal(t, 3, computeTotalPages(11, 5))
}

func TestParseKvConditionForColumnWithMeta(t *testing.T) {
	textCond := &forma.KvCondition{Attr: "text_01", Value: "starts_with:hello"}
	op, val, err := parseKvConditionForColumnWithMeta(textCond, "text_01", nil)
	require.NoError(t, err)
	assert.Equal(t, "LIKE", op)
	assert.Equal(t, "hello%", val)

	numericCond := &forma.KvCondition{Attr: "bigint_01", Value: "gt:42"}
	op, val, err = parseKvConditionForColumnWithMeta(numericCond, "bigint_01", nil)
	require.NoError(t, err)
	assert.Equal(t, ">", op)
	assert.Equal(t, int64(42), val)

	date := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	meta := &forma.AttributeMetadata{
		ValueType: forma.ValueTypeDateTime,
		ColumnBinding: &forma.MainColumnBinding{
			ColumnName: "bigint_01",
			Encoding:   forma.MainColumnEncodingUnixMs,
		},
	}
	dateCond := &forma.KvCondition{Attr: "bigint_01", Value: "equals:" + date.Format(time.RFC3339)}
	op, val, err = parseKvConditionForColumnWithMeta(dateCond, "bigint_01", meta)
	require.NoError(t, err)
	assert.Equal(t, "=", op)
	assert.Equal(t, date.UnixMilli(), val)

	badCond := &forma.KvCondition{Attr: "text_01", Value: "nope:1"}
	_, _, err = parseKvConditionForColumnWithMeta(badCond, "text_01", nil)
	require.Error(t, err)

	_, _, err = parseKvConditionForColumnWithMeta(textCond, "missing", nil)
	require.Error(t, err)
}

func TestConvertDateValueForQuery(t *testing.T) {
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	meta := &forma.AttributeMetadata{
		ColumnBinding: &forma.MainColumnBinding{
			ColumnName: "bigint_01",
			Encoding:   forma.MainColumnEncodingUnixMs,
		},
	}

	val, err := convertDateValueForQuery(when.Format(time.RFC3339), meta)
	require.NoError(t, err)
	assert.Equal(t, when.UnixMilli(), val)

	meta.ColumnBinding.Encoding = forma.MainColumnEncodingISO8601
	val, err = convertDateValueForQuery("1700000000000", meta)
	require.NoError(t, err)
	assert.Equal(t, time.UnixMilli(1700000000000).Format(time.RFC3339), val)

	_, err = convertDateValueForQuery("not-a-date", meta)
	require.Error(t, err)
}

func TestHasMainTableCondition(t *testing.T) {
	assert.True(t, hasMainTableCondition(nil, nil))

	cond := &forma.CompositeCondition{
		Logic:      forma.LogicOr,
		Conditions: []forma.Condition{&forma.KvCondition{Attr: "text_01", Value: "hello"}},
	}
	assert.True(t, hasMainTableCondition(cond, nil))

	cache := forma.SchemaAttributeCache{
		"attr_foo": {
			AttributeName: "attr_foo",
			ColumnBinding: &forma.MainColumnBinding{ColumnName: "text_01"},
		},
	}
	assert.True(t, hasMainTableCondition(&forma.KvCondition{Attr: "attr_foo", Value: "hello"}, cache))
	assert.False(t, hasMainTableCondition(&forma.KvCondition{Attr: "attr_bar", Value: "hello"}, nil))
}

func TestBuildHybridConditionsMainColumn(t *testing.T) {
	repo := &DBPersistentRecordRepository{}
	query := AttributeQuery{
		SchemaID:  1,
		Condition: &forma.KvCondition{Attr: "text_01", Value: "hello"},
	}

	clause, args, err := repo.buildHybridConditions("eav_table", "main_table", query, 1, true)
	require.NoError(t, err)
	assert.Equal(t, "m.\"text_01\" = $2", clause)
	assert.Equal(t, []any{"hello"}, args)

	clause, args, err = repo.buildHybridConditions("eav_table", "main_table", query, 1, false)
	require.NoError(t, err)
	expectedClause := fmt.Sprintf(
		"EXISTS (SELECT 1 FROM %s m WHERE m.ltbase_row_id = t.row_id AND m.\"text_01\" = $2)",
		sanitizeIdentifier("main_table"),
	)
	assert.Equal(t, expectedClause, clause)
	assert.Equal(t, []any{"hello"}, args)

	query.Condition = nil
	clause, args, err = repo.buildHybridConditions("eav_table", "main_table", query, 1, true)
	require.NoError(t, err)
	assert.Equal(t, "1=1", clause)
	assert.Nil(t, args)
}

func TestRunOptimizedQueryValidation(t *testing.T) {
	repo := &DBPersistentRecordRepository{}

	_, _, err := repo.runOptimizedQuery(
		context.Background(),
		StorageTables{EntityMain: "main", EAVData: "eav"},
		1,
		"",
		nil,
		10,
		0,
		nil,
		true,
	)
	require.Error(t, err)

	_, _, err = repo.runOptimizedQuery(
		context.Background(),
		StorageTables{EntityMain: "main", EAVData: "eav"},
		0,
		"1=1",
		nil,
		10,
		0,
		nil,
		true,
	)
	require.Error(t, err)
}

func TestRunOptimizedQueryWithMockPool(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	repo := NewDBPersistentRecordRepository(mock, nil, nil)

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
		case "ltbase_deleted_at":
			values = append(values, nil)
		case "text_01":
			values = append(values, "hello")
		default:
			values = append(values, nil)
		}
	}

	columns = append(columns, "attributes_json", "total_records", "total_pages", "current_page")
	values = append(values, []byte("[]"), int64(1), int64(1), int32(1))

	rows := pgxmock.NewRows(columns).AddRow(values...)
	mock.ExpectQuery("WITH anchor").WithArgs(int16(1), 10, 0).WillReturnRows(rows)

	records, total, err := repo.runOptimizedQuery(
		ctx,
		StorageTables{EntityMain: "main_table", EAVData: "eav_table"},
		1,
		"1=1",
		nil,
		10,
		0,
		nil,
		true,
	)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, int64(1), total)
	assert.Equal(t, int16(1), records[0].SchemaID)
	assert.Equal(t, rowID, records[0].RowID)
	assert.Equal(t, map[string]string{"text_01": "hello"}, records[0].TextItems)
	assert.Nil(t, records[0].OtherAttributes)

	require.NoError(t, mock.ExpectationsWereMet())
}
