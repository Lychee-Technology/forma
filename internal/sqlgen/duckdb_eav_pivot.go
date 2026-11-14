package sqlgen

import (
	"fmt"

	"github.com/lychee-technology/forma"
)

// eavValueColumn returns the appropriate eav_data column for a given value type.
func eavValueColumn(vt forma.ValueType) string {
	switch vt {
	case forma.ValueTypeNumeric, forma.ValueTypeInteger, forma.ValueTypeBigInt,
		forma.ValueTypeSmallInt, forma.ValueTypeDate, forma.ValueTypeDateTime,
		forma.ValueTypeBool:
		return "value_numeric"
	default:
		return "value_text"
	}
}

// mainColRFC3339Expr returns a DuckDB expression that normalises a
// column-bound date/datetime main-table column stored with the rfc3339
// encoding (an RFC3339 string in a text column) to epoch-ms BIGINT, the
// convention every other tier projects (#200). It is RFC3339EpochMs, the
// CDC export's read (cdc.castDateMainValue, #219), so the hot leg and the
// parquet legs agree on the value.
func mainColRFC3339Expr(colName string) string {
	return RFC3339EpochMs("m." + colName)
}

// mainColRFC3339Read returns the pg-main read of a column-bound
// date/datetime stored with the rfc3339 encoding (mainColRFC3339Expr), or
// false for any other binding.
func mainColRFC3339Read(meta forma.AttributeMetadata) (string, bool) {
	if meta.ValueType != forma.ValueTypeDate && meta.ValueType != forma.ValueTypeDateTime {
		return "", false
	}
	if meta.ColumnBinding == nil || meta.ColumnBinding.Encoding != forma.MainColumnEncodingRFC3339 {
		return "", false
	}
	return mainColRFC3339Expr(string(meta.ColumnBinding.ColumnName)), true
}

// integerWidths and columnWidths rank the integer valueTypes and the numeric
// main column types on one scale: each holds every value of the ranks below
// it, and a double column also holds fractions, NaN and the infinities. A
// column type missing here reads raw.
// TestPgMainClause_WideIntegerColumnsReadAtDeclaredType walks every
// numeric binding registration admits, so an unranked one fails there.
var (
	integerWidths = map[forma.ValueType]struct {
		rank     int
		duckType string
	}{
		forma.ValueTypeSmallInt: {1, "SMALLINT"},
		forma.ValueTypeInteger:  {2, "INTEGER"},
		forma.ValueTypeBigInt:   {3, "BIGINT"},
	}
	columnWidths = map[forma.MainColumnType]int{
		forma.MainColumnTypeSmallint: 1,
		forma.MainColumnTypeInteger:  2,
		forma.MainColumnTypeBigint:   3,
		forma.MainColumnTypeDouble:   4,
	}
)

// mainColIntegerExpr returns the pg-main read of a column-bound
// smallint/integer/bigint whose column holds values its declared type cannot:
// an integer column wider than the type, or a double column, which keeps the
// float64 image (#590). It reads the column at the declared type through the
// CDC export's spelling (cdc.castMainValue), so an image past the type's
// range, NaN or ±Infinity reads NULL and a fraction rounds half to even, on
// the unflushed row exactly as on its parquet copy. The raw image used to
// reach the outer select's strict cast instead, which failed every query
// that reached the row (#627). The write funnel refuses such images
// (checkStorageFit judges the declared type first), so they are history
// written before it or by direct SQL.
func mainColIntegerExpr(meta forma.AttributeMetadata) (string, bool) {
	declared, ok := integerWidths[meta.ValueType]
	if !ok || columnWidths[meta.ColumnBinding.ColumnType()] <= declared.rank {
		return "", false
	}
	return fmt.Sprintf("TRY_CAST(m.%s AS %s)", meta.ColumnBinding.ColumnName, declared.duckType), true
}
