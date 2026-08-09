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

func newByAttrValuesTestCache(t *testing.T) *schemameta.MetadataCache {
	t.Helper()
	cache := schemameta.NewMetadataCache()
	require.NoError(t, cache.RegisterSchema("parent", 1, forma.SchemaAttributeCache{
		"id": {
			AttributeID: 26,
			ValueType:   forma.ValueTypeText,
		},
		"code": {
			AttributeID:   30,
			ValueType:     forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_01")},
		},
		"rank": {
			AttributeID: 31,
			ValueType:   forma.ValueTypeInteger,
		},
		"total": {
			AttributeID: 32,
			ValueType:   forma.ValueTypeBigInt,
		},
		"ratio": {
			AttributeID: 33,
			ValueType:   forma.ValueTypeNumeric,
		},
	}))
	return cache
}

// TestQueryPersistentRecordsByAttrValuesUsesAnyArray pins the #268 fix: the
// parent lookup must be one set-based anchor over the EAV text index
// (attr_id = $2 AND value_text = ANY($3)), never OR-of-N correlated EXISTS.
func TestQueryPersistentRecordsByAttrValuesUsesAnyArray(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	repo := NewDBPersistentRecordRepository(mock, newByAttrValuesTestCache(t))
	tables := model.StorageTables{EntityMain: "main_table", EAVData: "eav_table"}

	rowID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	columns, values := optimizedQueryFixtureColumnsAndValues(rowID, int64(2))

	ids := []string{"parent-a", "parent-b"}
	mock.ExpectQuery(`t\.attr_id = \$2 AND t\.value_text = ANY\(\$3\)`).
		WithArgs(int16(1), int16(26), ids, 2, 0).
		WillReturnRows(pgxmock.NewRows(columns).AddRow(values...))

	page, err := repo.QueryPersistentRecordsByAttrValues(ctx, tables, 1, "id", ids, len(ids))
	require.NoError(t, err)
	require.Len(t, page.Records, 1)
	assert.Equal(t, rowID, page.Records[0].RowID)
	assert.Equal(t, int64(2), page.TotalRecords)

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestQueryPersistentRecordsByAttrValuesEmptyValuesSkipsQuery asserts the
// degenerate ANY('{}') query is never issued.
func TestQueryPersistentRecordsByAttrValuesEmptyValuesSkipsQuery(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	repo := NewDBPersistentRecordRepository(mock, newByAttrValuesTestCache(t))
	tables := model.StorageTables{EntityMain: "main_table", EAVData: "eav_table"}

	page, err := repo.QueryPersistentRecordsByAttrValues(ctx, tables, 1, "id", nil, 0)
	require.NoError(t, err)
	assert.Empty(t, page.Records)
	assert.Equal(t, int64(0), page.TotalRecords)

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestQueryPersistentRecordsByAttrValuesHotColumnUsesMainAnchor asserts a
// main-table-bound attribute anchors on the hot column instead of the EAV
// table.
func TestQueryPersistentRecordsByAttrValuesHotColumnUsesMainAnchor(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	repo := NewDBPersistentRecordRepository(mock, newByAttrValuesTestCache(t))
	tables := model.StorageTables{EntityMain: "main_table", EAVData: "eav_table"}

	rowID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	columns, values := optimizedQueryFixtureColumnsAndValues(rowID, int64(1))

	codes := []string{"c-1", "c-2"}
	mock.ExpectQuery(`m\."text_01" = ANY\(\$2\)`).
		WithArgs(int16(1), codes, 2, 0).
		WillReturnRows(pgxmock.NewRows(columns).AddRow(values...))

	page, err := repo.QueryPersistentRecordsByAttrValues(ctx, tables, 1, "code", codes, len(codes))
	require.NoError(t, err)
	require.Len(t, page.Records, 1)
	assert.Equal(t, int64(1), page.TotalRecords)

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestQueryPersistentRecordsByAttrValuesNumericValues asserts numeric EAV
// attributes bind a float64 array against value_numeric.
func TestQueryPersistentRecordsByAttrValuesNumericValues(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	repo := NewDBPersistentRecordRepository(mock, newByAttrValuesTestCache(t))
	tables := model.StorageTables{EntityMain: "main_table", EAVData: "eav_table"}

	rowID := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	columns, values := optimizedQueryFixtureColumnsAndValues(rowID, int64(1))

	mock.ExpectQuery(`t\.attr_id = \$2 AND t\.value_numeric = ANY\(\$3\)`).
		WithArgs(int16(1), int16(31), []any{int64(7), int64(9)}, 2, 0).
		WillReturnRows(pgxmock.NewRows(columns).AddRow(values...))

	page, err := repo.QueryPersistentRecordsByAttrValues(ctx, tables, 1, "rank", []string{"7", "9"}, 2)
	require.NoError(t, err)
	require.Len(t, page.Records, 1)

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestQueryPersistentRecordsByAttrValuesUnknownAttr asserts the error names
// the offending attribute instead of scanning a wrong column.
func TestQueryPersistentRecordsByAttrValuesUnknownAttr(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	repo := NewDBPersistentRecordRepository(mock, newByAttrValuesTestCache(t))
	tables := model.StorageTables{EntityMain: "main_table", EAVData: "eav_table"}

	_, err = repo.QueryPersistentRecordsByAttrValues(ctx, tables, 1, "missing", []string{"x"}, 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing")

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestQueryPersistentRecordsByAttrValuesBindsIntegralOperandsExactly pins the
// #355 fix: the batch anchor must reach the same verdict as the predicate
// binders (#281, #357) on the same literal. 2^53+1 is not representable as
// float64 — strconv.ParseFloat rounds it down to 2^53, so the pre-fix bind
// silently addressed a *different* value than the caller named. Both accepted
// spellings of an integral literal must arrive as exact int64.
func TestQueryPersistentRecordsByAttrValuesBindsIntegralOperandsExactly(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	repo := NewDBPersistentRecordRepository(mock, newByAttrValuesTestCache(t))
	tables := model.StorageTables{EntityMain: "main_table", EAVData: "eav_table"}

	rowID := uuid.MustParse("55555555-5555-5555-5555-555555555555")
	columns, values := optimizedQueryFixtureColumnsAndValues(rowID, int64(1))

	mock.ExpectQuery(`t\.attr_id = \$2 AND t\.value_numeric = ANY\(\$3\)`).
		WithArgs(int16(1), int16(32),
			[]any{int64(9007199254740993), int64(9007199254740992)}, 2, 0).
		WillReturnRows(pgxmock.NewRows(columns).AddRow(values...))

	page, err := repo.QueryPersistentRecordsByAttrValues(ctx, tables, 1, "total",
		[]string{"9007199254740993", "9.007199254740992e15"}, 2)
	require.NoError(t, err)
	require.Len(t, page.Records, 1)

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestQueryPersistentRecordsByAttrValuesKeepsFractionalOperandsFloat64 pins the
// other half of the contract. A genuinely fractional operand must stay float64:
// EAV storage holds what the write path's float64 hop produced, so binding an
// exact decimal instead would stop matching the value that was actually stored.
// A mixed list must be typed per element, not collapsed to one array type.
func TestQueryPersistentRecordsByAttrValuesKeepsFractionalOperandsFloat64(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	repo := NewDBPersistentRecordRepository(mock, newByAttrValuesTestCache(t))
	tables := model.StorageTables{EntityMain: "main_table", EAVData: "eav_table"}

	rowID := uuid.MustParse("66666666-6666-6666-6666-666666666666")
	columns, values := optimizedQueryFixtureColumnsAndValues(rowID, int64(1))

	mock.ExpectQuery(`t\.attr_id = \$2 AND t\.value_numeric = ANY\(\$3\)`).
		WithArgs(int16(1), int16(33), []any{9.5, int64(8)}, 2, 0).
		WillReturnRows(pgxmock.NewRows(columns).AddRow(values...))

	page, err := repo.QueryPersistentRecordsByAttrValues(ctx, tables, 1, "ratio",
		[]string{"9.5", "8"}, 2)
	require.NoError(t, err)
	require.Len(t, page.Records, 1)

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestQueryPersistentRecordsByAttrValuesRejectsNonNumericOperand asserts the
// error names the numeric family and the offending value, and no longer claims
// "float64" — the parse is no longer a strconv.ParseFloat call.
func TestQueryPersistentRecordsByAttrValuesRejectsNonNumericOperand(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	repo := NewDBPersistentRecordRepository(mock, newByAttrValuesTestCache(t))
	tables := model.StorageTables{EntityMain: "main_table", EAVData: "eav_table"}

	_, err = repo.QueryPersistentRecordsByAttrValues(ctx, tables, 1, "total",
		[]string{"not-a-number"}, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), `invalid value "not-a-number"`)
	require.Contains(t, err.Error(), "bigint")
	require.NotContains(t, err.Error(), "float64")

	require.NoError(t, mock.ExpectationsWereMet())
}
