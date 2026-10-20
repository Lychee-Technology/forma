package sqlgen

import (
	"database/sql"
	"math"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// #627: a smallint/integer/bigint bound to a column that holds values its
// declared type cannot — a wider integer column, or a double_* column, which
// keeps the float64 image — reached the outer select raw, where the strict
// CAST(attr AS <type>) failed every federated query that reached an
// unflushed row holding such an image. The CDC export reads the same column
// through TRY_CAST, so the row's parquet copy held NULL or a rounded value.
// The hot leg now reads it the export's way, and the pg-main pushdown reads
// it the hot leg's way.

func boundNumericMeta(vt forma.ValueType, col forma.MainColumn) forma.AttributeMetadata {
	return forma.AttributeMetadata{AttributeID: 1, ValueType: vt,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: col}}
}

// TestHotLegProjections_WideIntegerColumnsReadAtDeclaredType walks every
// numeric-family pair schemameta.ValidateColumnBinding admits. Only a column
// wider than the declared integer type is read through TRY_CAST, on both hot
// projections; a column of the type's own width or narrower, and every
// numeric binding, keep the raw column.
func TestHotLegProjections_WideIntegerColumnsReadAtDeclaredType(t *testing.T) {
	cols := map[string]forma.MainColumn{
		"smallint": forma.MainColumnSmallint01, "integer": forma.MainColumnInteger01,
		"bigint": forma.MainColumnBigint01, "double": forma.MainColumnDouble01,
	}
	for _, tc := range []struct {
		vt     forma.ValueType
		column string
		read   string // the hot leg's read of the column; "" means the raw column
	}{
		{forma.ValueTypeSmallInt, "smallint", ""},
		{forma.ValueTypeSmallInt, "integer", "TRY_CAST(m.integer_01 AS SMALLINT)"},
		{forma.ValueTypeSmallInt, "bigint", "TRY_CAST(m.bigint_01 AS SMALLINT)"},
		{forma.ValueTypeSmallInt, "double", "TRY_CAST(m.double_01 AS SMALLINT)"},
		{forma.ValueTypeInteger, "smallint", ""},
		{forma.ValueTypeInteger, "integer", ""},
		{forma.ValueTypeInteger, "bigint", "TRY_CAST(m.bigint_01 AS INTEGER)"},
		{forma.ValueTypeInteger, "double", "TRY_CAST(m.double_01 AS INTEGER)"},
		{forma.ValueTypeBigInt, "smallint", ""},
		{forma.ValueTypeBigInt, "integer", ""},
		{forma.ValueTypeBigInt, "bigint", ""},
		{forma.ValueTypeBigInt, "double", "TRY_CAST(m.double_01 AS BIGINT)"},
		{forma.ValueTypeNumeric, "smallint", ""},
		{forma.ValueTypeNumeric, "integer", ""},
		{forma.ValueTypeNumeric, "bigint", ""},
		{forma.ValueTypeNumeric, "double", ""},
	} {
		t.Run(string(tc.vt)+"_in_"+tc.column, func(t *testing.T) {
			col := cols[tc.column]
			sp, err := BuildSchemaProjection(7, forma.SchemaAttributeCache{"x": boundNumericMeta(tc.vt, col)})
			require.NoError(t, err)
			read := tc.read
			if read == "" {
				read = "m." + string(col)
			}
			require.Contains(t, sp.PGSourceSelect, "COALESCE(ANY_VALUE(hot_vals.x), "+read+") AS x")
			require.Contains(t, sp.BuildPGSelectNoEAV(), read+" AS x")
		})
	}
}

// TestMainColIntegerExpr_ReadsTheExportImage runs the hot-leg read and the
// outer select's cast on DuckDB over the images a wider column can hold. An
// image the declared type cannot hold reads NULL instead of failing the
// statement, and a fraction rounds half to even: the values TRY_CAST gives
// the parquet copy (cdc.castMainValue), and what the census reports for the
// double_* case (#618). The strict-cast chain the hot leg used to run fails
// on the same images.
func TestMainColIntegerExpr_ReadsTheExportImage(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	valid := func(v int64) sql.NullInt64 { return sql.NullInt64{Int64: v, Valid: true} }
	for _, tc := range []struct {
		name   string
		meta   forma.AttributeMetadata
		kind   model.ColumnKind
		image  string // a DuckDB literal of the stored column value
		want   sql.NullInt64
		strict bool // the raw image also survives the strict cast
	}{
		{"bigint 2^53+2", boundNumericMeta(forma.ValueTypeBigInt, forma.MainColumnDouble01), model.ColumnKindDouble, "9007199254740994::DOUBLE", valid(9007199254740994), true},
		{"bigint -2^63", boundNumericMeta(forma.ValueTypeBigInt, forma.MainColumnDouble01), model.ColumnKindDouble, "-9223372036854775808::DOUBLE", valid(math.MinInt64), true},
		{"bigint 1000.5", boundNumericMeta(forma.ValueTypeBigInt, forma.MainColumnDouble01), model.ColumnKindDouble, "1000.5::DOUBLE", valid(1000), true},
		{"bigint 1001.5", boundNumericMeta(forma.ValueTypeBigInt, forma.MainColumnDouble01), model.ColumnKindDouble, "1001.5::DOUBLE", valid(1002), true},
		{"bigint 2^63", boundNumericMeta(forma.ValueTypeBigInt, forma.MainColumnDouble01), model.ColumnKindDouble, "9223372036854775808::DOUBLE", sql.NullInt64{}, false},
		{"bigint NaN", boundNumericMeta(forma.ValueTypeBigInt, forma.MainColumnDouble01), model.ColumnKindDouble, "'NaN'::DOUBLE", sql.NullInt64{}, false},
		{"bigint Infinity", boundNumericMeta(forma.ValueTypeBigInt, forma.MainColumnDouble01), model.ColumnKindDouble, "'Infinity'::DOUBLE", sql.NullInt64{}, false},
		{"bigint -Infinity", boundNumericMeta(forma.ValueTypeBigInt, forma.MainColumnDouble01), model.ColumnKindDouble, "'-Infinity'::DOUBLE", sql.NullInt64{}, false},
		{"bigint NULL", boundNumericMeta(forma.ValueTypeBigInt, forma.MainColumnDouble01), model.ColumnKindDouble, "NULL::DOUBLE", sql.NullInt64{}, true},
		{"integer MaxInt32", boundNumericMeta(forma.ValueTypeInteger, forma.MainColumnBigint01), model.ColumnKindBigint, "2147483647::BIGINT", valid(math.MaxInt32), true},
		{"integer 2^32", boundNumericMeta(forma.ValueTypeInteger, forma.MainColumnBigint01), model.ColumnKindBigint, "4294967296::BIGINT", sql.NullInt64{}, false},
		{"smallint 2^15", boundNumericMeta(forma.ValueTypeSmallInt, forma.MainColumnInteger01), model.ColumnKindInteger, "32768::INTEGER", sql.NullInt64{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			read, ok := mainColIntegerExpr(tc.meta)
			require.True(t, ok)
			col := string(tc.meta.ColumnBinding.ColumnName)
			outer := duckDBMainColCast("x", tc.meta.ValueType, tc.kind)
			stored := "(SELECT " + tc.image + " AS " + col + ") m"

			var got sql.NullInt64
			require.NoError(t, db.QueryRow("SELECT "+outer+" FROM (SELECT "+read+" AS x FROM "+stored+")").Scan(&got),
				"the hot leg must read the image without failing the statement")
			require.Equal(t, tc.want, got)

			err := db.QueryRow("SELECT " + outer + " FROM (SELECT m." + col + " AS x FROM " + stored + ")").Scan(&got)
			if tc.strict {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, "Conversion Error", "the raw image must still break the strict cast")
		})
	}
}

// TestBuildPgMainClause_WideIntegerColumnFiltersTheHotRead: the pg-main
// pushdown pre-filters pg_source, and `visible` filters the projected value
// again, so a leaf on the raw column was stricter than the read wherever the
// read changes the value. It now compares the same declared-type read, for
// a null test and an operand alike; a column of the type's own width keeps
// the raw compare.
func TestBuildPgMainClause_WideIntegerColumnFiltersTheHotRead(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"ratio": boundNumericMeta(forma.ValueTypeBigInt, forma.MainColumnDouble01),
		"qty":   boundNumericMeta(forma.ValueTypeInteger, forma.MainColumnBigint01),
		"total": boundNumericMeta(forma.ValueTypeBigInt, forma.MainColumnBigint02),
	}
	for _, tc := range []struct {
		cond string
		attr string
		want string
		args []any
	}{
		{"is_null:true", "ratio", "TRY_CAST(m.double_01 AS BIGINT) IS NULL", nil},
		{"is_not_null:true", "ratio", "TRY_CAST(m.double_01 AS BIGINT) IS NOT NULL", nil},
		{"equals:1000", "ratio", "TRY_CAST(m.double_01 AS BIGINT) = ?", []any{int64(1000)}},
		{"lte:1000", "qty", "TRY_CAST(m.bigint_01 AS INTEGER) <= ?", []any{int64(1000)}},
		{"is_null:true", "total", "m.bigint_02 IS NULL", nil},
		{"equals:1000", "total", "m.bigint_02 = ?", []any{int64(1000)}},
	} {
		t.Run(tc.attr+"_"+tc.cond, func(t *testing.T) {
			paramIndex := 0
			clause, args, err := buildPgMainClause(&forma.KvCondition{Attr: tc.attr, Value: tc.cond}, cache, &paramIndex)
			require.NoError(t, err)
			require.Equal(t, tc.want, clause)
			require.Equal(t, tc.args, args)
		})
	}
}
