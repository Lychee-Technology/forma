package internal

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/lychee-technology/forma/internal/schemameta"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithClockAndNowMillis(t *testing.T) {
	repo := NewDBPersistentRecordRepository(nil, nil)
	fixed := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	repo.withClock(func() time.Time { return fixed })

	got := repo.nowMillis()
	assert.Equal(t, fixed.UnixMilli(), got)

	repo.withClock(nil)
	got = repo.nowMillis()
	assert.Equal(t, fixed.UnixMilli(), got)
}

func TestValidateTables(t *testing.T) {
	err := validateTables(model.StorageTables{})
	require.Error(t, err)

	err = validateTables(model.StorageTables{EntityMain: "entity", EAVData: "eav"})
	require.NoError(t, err)

	// ChangeLog is optional; writes are allowed even if ChangeLog is not provided
	err = validateWriteTables(model.StorageTables{EntityMain: "entity", EAVData: "eav"})
	require.NoError(t, err)

	err = validateWriteTables(model.StorageTables{EntityMain: "entity", EAVData: "eav", ChangeLog: "change_log"})
	require.NoError(t, err)
}

func TestSortedColumnKeys(t *testing.T) {
	keys, err := sortedColumnKeys(map[string]string{"text_02": "b", "text_01": "a"}, model.AllowedTextColumns)
	require.NoError(t, err)
	assert.Equal(t, []string{"text_01", "text_02"}, keys)

	_, err = sortedColumnKeys(map[string]string{"nope": "x"}, model.AllowedTextColumns)
	require.Error(t, err)
}

func TestMainColumnHelpers(t *testing.T) {
	assert.True(t, model.IsMainTableColumn("text_01"))
	assert.True(t, model.IsMainTableColumn("ltbase_schema_id"))
	assert.True(t, model.IsMainTableColumn("ltbase_created_by"))
	assert.True(t, model.IsMainTableColumn("ltbase_updated_by"))
	assert.True(t, model.IsMainTableColumn("ltbase_deleted_by"))
	assert.False(t, model.IsMainTableColumn("nope"))

	desc := model.GetMainColumnDescriptor("ltbase_schema_id")
	require.NotNil(t, desc)
	assert.Equal(t, model.ColumnKindSmallint, desc.Kind)
	desc = model.GetMainColumnDescriptor("ltbase_created_by")
	require.NotNil(t, desc)
	assert.Equal(t, model.ColumnKindText, desc.Kind)
	assert.Nil(t, model.GetMainColumnDescriptor("nope"))
}

func TestBuildInsertMainStatement(t *testing.T) {
	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	uuid2 := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	deleted := int64(300)

	record := &model.PersistentRecord{
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
	record := &model.PersistentRecord{
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
	record := &model.PersistentRecord{
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

	// GREATEST + RETURNING (#274): the stored version must advance past the
	// row's previous one even on a same-millisecond (or backwards) clock, and
	// the caller adopts the effective value for change_log.
	expectedQuery := "UPDATE " + sanitizeIdentifier("entity_main") + " SET " +
		"ltbase_updated_at = GREATEST($1, ltbase_updated_at + 1), ltbase_deleted_at = $2, text_01 = $3, text_02 = $4, smallint_01 = $5, double_02 = $6 " +
		"WHERE ltbase_schema_id = $7 AND ltbase_row_id = $8 RETURNING ltbase_updated_at"

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
	attrs := []model.EAVRecord{
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
	assert.Equal(t, 0, model.ComputeTotalPages(0, 10))
	assert.Equal(t, 0, model.ComputeTotalPages(10, 0))
	assert.Equal(t, 2, model.ComputeTotalPages(10, 5))
	assert.Equal(t, 3, model.ComputeTotalPages(11, 5))
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
	query := model.AttributeQuery{
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

func TestBuildHybridConditionsBoundAuditColumn(t *testing.T) {
	cache := schemameta.NewMetadataCache()
	require.NoError(t, cache.RegisterSchema("log", 1, forma.SchemaAttributeCache{
		"createdBy": {
			AttributeName: "createdBy",
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnCreatedBy},
		},
	}))
	repo := &DBPersistentRecordRepository{
		metadataCache: cache,
	}
	query := model.AttributeQuery{
		SchemaID:  1,
		Condition: &forma.KvCondition{Attr: "createdBy", Value: "equals:user-123"},
	}

	clause, args, err := repo.buildHybridConditions("eav_table", "main_table", query, 1, true)
	require.NoError(t, err)
	assert.Equal(t, "m.\"ltbase_created_by\" = $2", clause)
	assert.Equal(t, []any{"user-123"}, args)
}

func TestRunOptimizedQueryValidation(t *testing.T) {
	repo := &DBPersistentRecordRepository{}

	_, _, err := repo.runOptimizedQuery(
		context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav"},
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
		model.StorageTables{EntityMain: "main", EAVData: "eav"},
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

	repo := NewDBPersistentRecordRepository(mock, nil)

	rowID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
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
		model.StorageTables{EntityMain: "main_table", EAVData: "eav_table"},
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

// TestStreamOptimizedQueryRenderCache pins #142: two calls with the same
// query shape render the SQL template once; a different shape renders again.
func TestStreamOptimizedQueryRenderCache(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	repo := NewDBPersistentRecordRepository(mock, nil)
	tables := model.StorageTables{EntityMain: "main_t", EAVData: "eav_t", ChangeLog: "cl_t"}
	for i := 0; i < 2; i++ {
		mock.ExpectQuery("WITH").
			WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnRows(pgxmock.NewRows([]string{"row_id"}))
		_, err = repo.StreamOptimizedQuery(ctx, tables, 1, "m.\"integer_01\" > $2", []any{int64(5)}, 10, 0, nil, true, nil)
		require.NoError(t, err)
	}
	hits, misses := repo.planCache.Stats()
	require.Equal(t, int64(1), hits, "second identical shape must hit the render cache")
	require.Equal(t, int64(1), misses)

	// Different shape (different clause) renders again.
	mock.ExpectQuery("WITH").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{"row_id"}))
	_, err = repo.StreamOptimizedQuery(ctx, tables, 1, "m.\"text_01\" = $2", []any{"x"}, 10, 0, nil, true, nil)
	require.NoError(t, err)
	_, misses = repo.planCache.Stats()
	require.Equal(t, int64(2), misses)
}

// TestOptimizedQueryShapeKey pins that every render-affecting input changes
// the key and that values do not participate.
func TestOptimizedQueryShapeKey(t *testing.T) {
	tables := model.StorageTables{EntityMain: "m", EAVData: "e", ChangeLog: "c"}
	orders := []model.AttributeOrder{{AttrID: 7, ColumnName: "integer_01", SortOrder: forma.SortOrderDesc}}
	base := optimizedQueryShapeKey(tables, true, "clause", 2, orders)

	require.Equal(t, base, optimizedQueryShapeKey(tables, true, "clause", 2, orders))
	require.NotEqual(t, base, optimizedQueryShapeKey(tables, false, "clause", 2, orders))
	require.NotEqual(t, base, optimizedQueryShapeKey(tables, true, "other", 2, orders))
	require.NotEqual(t, base, optimizedQueryShapeKey(tables, true, "clause", 3, orders))
	require.NotEqual(t, base, optimizedQueryShapeKey(tables, true, "clause", 2, nil))
	require.NotEqual(t, base, optimizedQueryShapeKey(model.StorageTables{EntityMain: "m2", EAVData: "e", ChangeLog: "c"}, true, "clause", 2, orders))
}

// BenchmarkOptimizedQueryRender quantifies the per-request template render
// cost the #142 cache eliminates on the page-1 hot path.
func BenchmarkOptimizedQueryRender(b *testing.B) {
	tables := model.StorageTables{EntityMain: "main_t", EAVData: "eav_t", ChangeLog: "cl_t"}
	sqlParams := map[string]any{
		"EAVTable":             sanitizeIdentifier(tables.EAVData),
		"MainTable":            sanitizeIdentifier(tables.EntityMain),
		"ChangeLogTable":       sanitizeIdentifier(tables.ChangeLog),
		"MainProjection":       model.EntityMainProjection,
		"SchemaID":             "$1",
		"UseMainTableAsAnchor": true,
		"Anchor":               map[string]any{"Condition": `m."integer_01" > $2`},
		"SortKeys":             []model.AttributeOrder{},
		"Limit":                "$3",
		"Offset":               "$4",
		"PageSize":             "$3",
	}

	b.Run("uncached", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := renderTemplate(optimizedQuerySQLTemplate, sqlParams); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("cached", func(b *testing.B) {
		cache := queryplan.NewCache(16)
		key := queryplan.Key{
			Kind:      "postgres_optimized_template",
			SchemaID:  1,
			ShapeHash: strconv.FormatUint(optimizedQueryShapeKey(tables, true, `m."integer_01" > $2`, 1, nil), 16),
		}
		for i := 0; i < b.N; i++ {
			if _, _, err := cache.GetOrBuild(key, func() (any, error) {
				return renderTemplate(optimizedQuerySQLTemplate, sqlParams)
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// TestTombstoneStamp pins the delete-side monotonicity rule (#274): the
// tombstone version is the clock read unless the deleted row's version has
// run ahead of it, in which case it lands strictly past that version.
func TestTombstoneStamp(t *testing.T) {
	assert.Equal(t, int64(100), tombstoneStamp(100, 50), "clock ahead of the row: stamp the clock read")
	assert.Equal(t, int64(100), tombstoneStamp(100, 99), "prev+1 == now: the clock read already outranks the row")
	assert.Equal(t, int64(101), tombstoneStamp(100, 100), "same millisecond: advance past the live version")
	assert.Equal(t, int64(201), tombstoneStamp(100, 200), "clock-ahead row: advance past it, not the clock")
}
