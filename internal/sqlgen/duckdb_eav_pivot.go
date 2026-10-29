package sqlgen

import (
	"fmt"

	"github.com/lychee-technology/forma"
)

// buildEAVPivotExpr renders the hot-tier EAV pivot expression for one attribute.
// The output type must mirror the CDC export side (cdc.castEAVValue) so the
// pg_source leg type-unifies with the parquet legs through COALESCE and
// UNION ALL instead of widening to DOUBLE — which silently rounded bound
// bigint values above 2^53 and crashed CAST-back at a stored MaxInt64 (#205).
// TRY_CAST (not CAST) matches export semantics: an out-of-range NUMERIC in
// eav_data becomes NULL on every tier alike instead of a read-path crash.
// EAV-only integer/smallint cast by storage width (DOUBLE — the write funnel
// narrows everything through float64), not declared width, so a stored value
// the declared type cannot hold answers the same on hot Postgres and every
// DuckDB tier (#384); the write funnel now rejects new ones
// (transform.checkDeclaredIntegerFit).
func buildEAVPivotExpr(a attrProjectionInfo) string {
	if a.meta.ValueType == forma.ValueTypeList {
		// One eav_data row per element: aggregate into a LIST in element-index
		// order instead of MAX-collapsing, mirroring the CDC export side
		// (cdc.castEAVValue with the items-typed meta) so the hot leg
		// type-unifies with the parquet LIST column. The empty-list marker row
		// (both value columns NULL) is the only row excluded from the element
		// aggregate; the presence count still sees it, so explicit [] stays
		// distinguishable from an absent attribute (NULL) on every tier
		// (#204). The marker is told by its NULL values, not its
		// array_indices: inside an array of objects it carries its element's
		// (#623). A legacy scalar row (array_indices '' carrying a value,
		// written before the schema declared the attribute a list) therefore
		// pivots as a one-element list instead of being dropped as [] (#372);
		// validate-schema-consistency reports such rows.
		return fmt.Sprintf(
			"CASE WHEN count(*) FILTER (WHERE attr_id = %d) > 0 THEN coalesce(list(%s ORDER BY TRY_CAST(array_indices AS BIGINT)) FILTER (WHERE attr_id = %d AND (value_text IS NOT NULL OR value_numeric IS NOT NULL)), []) END",
			a.attrID, eavElementCastExpr(a.meta.EffectiveItemsType()), a.attrID)
	}
	if a.meta.ValueType == forma.ValueTypeBool {
		// Wrap in the truthiness comparison so the pivot column is BOOLEAN,
		// not DOUBLE (#182), spelled by the shared #404 rule.
		return BoolTruthiness(fmt.Sprintf("MAX(CASE WHEN attr_id = %d THEN value_numeric END)", a.attrID))
	}
	base := fmt.Sprintf("MAX(CASE WHEN attr_id = %d THEN %s END)",
		a.attrID, eavValueColumn(a.meta.ValueType))
	switch a.meta.ValueType {
	case forma.ValueTypeBigInt, forma.ValueTypeDate, forma.ValueTypeDateTime:
		return fmt.Sprintf("TRY_CAST(%s AS BIGINT)", base)
	case forma.ValueTypeInteger, forma.ValueTypeSmallInt:
		if a.isColumn {
			// A bound attribute keeps its declared width on both COALESCE
			// arms: the partner reads its main column at the declared type
			// too, through the same TRY_CAST when the column is wider
			// (mainColIntegerExpr, #627), and the export casts to it.
			if a.meta.ValueType == forma.ValueTypeInteger {
				return fmt.Sprintf("TRY_CAST(%s AS INTEGER)", base)
			}
			return fmt.Sprintf("TRY_CAST(%s AS SMALLINT)", base)
		}
		// EAV-only storage width is DOUBLE, like the numeric class: the write
		// funnel narrows everything through float64 (numutil.Float64), so
		// whatever value_numeric holds — including out-of-declared-range and
		// non-integral history — is a float64 image, and DOUBLE reproduces it
		// exactly on every tier. A narrower integer cast either NULLed the
		// value (BIGINT-overflow) or rounded it (1.5 → 2) only on the DuckDB
		// legs while hot Postgres compared the raw NUMERIC (#384).
		return fmt.Sprintf("TRY_CAST(%s AS DOUBLE)", base)
	default:
		// numeric 保持 DOUBLE;text/uuid 走 value_text,无 cast
		return base
	}
}

// eavElementCastExpr renders the raw-column cast for one list element of the
// given items type. Must mirror cdc.castEAVValue applied to the same type so
// hot LIST elements type-unify with the parquet LIST column (#204, cf. #205).
func eavElementCastExpr(vt forma.ValueType) string {
	switch vt {
	case forma.ValueTypeBool:
		return BoolTruthiness("value_numeric")
	case forma.ValueTypeBigInt, forma.ValueTypeDate, forma.ValueTypeDateTime:
		return "TRY_CAST(value_numeric AS BIGINT)"
	case forma.ValueTypeInteger, forma.ValueTypeSmallInt:
		// List storage is one float64-funneled NUMERIC eav row per element
		// and lists are always EAV-only: storage width DOUBLE, as in the
		// scalar EAV-only arm of buildEAVPivotExpr (#384).
		return "TRY_CAST(value_numeric AS DOUBLE)"
	case forma.ValueTypeNumeric:
		return "TRY_CAST(value_numeric AS DOUBLE)"
	default: // text / uuid
		return "value_text"
	}
}

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

// mainColBoolExpr returns a DuckDB boolean expression that normalises a
// column-bound bool main-table column to a BOOLEAN value.
// bool_text encoding stores "1"/"0" as text and is read by that contract;
// bool_smallint stores 0/1 as SMALLINT and is read by the shared #404
// truthiness. Both must produce a BOOLEAN so that
// COALESCE(hot_vals.<attr>, <expr>) is type-safe.
func mainColBoolExpr(colName string, enc forma.MainColumnEncoding) string {
	if enc == forma.MainColumnEncodingBoolText {
		return fmt.Sprintf("m.%s = '1'", colName)
	}
	// Default covers MainColumnEncodingBoolInt and any other numeric-like encoding.
	return BoolTruthiness("m." + colName)
}

// mainColISO8601Expr returns a DuckDB expression that normalises a
// column-bound date/datetime main-table column stored with the iso8601
// encoding (an RFC3339 string in a text column) to epoch-ms BIGINT, the
// convention every other tier projects (#200). It is the CDC export's
// spelling (cdc.castDateMainValue, #219) so the hot leg and the parquet legs
// agree on the value, and TRY_CAST so a non-date string reads NULL on every
// tier alike instead of failing the read (#555).
func mainColISO8601Expr(colName string) string {
	return fmt.Sprintf("epoch_ms(TRY_CAST(m.%s AS TIMESTAMP))", colName)
}

// mainColISO8601Read returns the hot-leg read of a column-bound
// date/datetime stored with the iso8601 encoding (mainColISO8601Expr), or
// false for any other binding.
func mainColISO8601Read(meta forma.AttributeMetadata) (string, bool) {
	if meta.ValueType != forma.ValueTypeDate && meta.ValueType != forma.ValueTypeDateTime {
		return "", false
	}
	if meta.ColumnBinding.Encoding != forma.MainColumnEncodingISO8601 {
		return "", false
	}
	return mainColISO8601Expr(string(meta.ColumnBinding.ColumnName)), true
}

// integerWidths and columnWidths rank the integer valueTypes and the numeric
// main column types on one scale: each holds every value of the ranks below
// it, and a double column also holds fractions, NaN and the infinities. A
// column type missing here reads raw.
// TestHotLegProjections_WideIntegerColumnsReadAtDeclaredType walks every
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

// mainColIntegerExpr returns the hot-leg read of a column-bound
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

// boundMainExpr returns the hot-leg read of a column-bound attribute whose
// stored image is not the unified column type the parquet legs carry, or
// false when the raw column already is that image.
func boundMainExpr(meta forma.AttributeMetadata) (string, bool) {
	colName := string(meta.ColumnBinding.ColumnName)
	switch meta.ValueType {
	case forma.ValueTypeBool:
		return mainColBoolExpr(colName, meta.ColumnBinding.Encoding), true
	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		return mainColISO8601Read(meta)
	case forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt:
		return mainColIntegerExpr(meta)
	}
	return "", false
}
