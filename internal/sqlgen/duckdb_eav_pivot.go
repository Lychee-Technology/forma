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
		// (array_indices '', both value columns NULL) is the only row excluded
		// from the element aggregate; the presence count still sees it, so
		// explicit [] stays distinguishable from an absent attribute (NULL) on
		// every tier (#204). A legacy scalar row (array_indices '' carrying a
		// value, written before the schema declared the attribute a list)
		// therefore pivots as a one-element list instead of being dropped as
		// [] (#372); validate-schema-consistency reports such rows.
		return fmt.Sprintf(
			"CASE WHEN count(*) FILTER (WHERE attr_id = %d) > 0 THEN coalesce(list(%s ORDER BY TRY_CAST(array_indices AS BIGINT)) FILTER (WHERE attr_id = %d AND (array_indices <> '' OR value_text IS NOT NULL OR value_numeric IS NOT NULL)), []) END",
			a.attrID, eavElementCastExpr(a.meta.EffectiveItemsType()), a.attrID)
	}
	if a.meta.ValueType == forma.ValueTypeBool {
		// Wrap in <> 0 so the pivot column is BOOLEAN, not DOUBLE (#182).
		return fmt.Sprintf("(MAX(CASE WHEN attr_id = %d THEN value_numeric END) <> 0)", a.attrID)
	}
	base := fmt.Sprintf("MAX(CASE WHEN attr_id = %d THEN %s END)",
		a.attrID, eavValueColumn(a.meta.ValueType))
	switch a.meta.ValueType {
	case forma.ValueTypeBigInt, forma.ValueTypeDate, forma.ValueTypeDateTime:
		return fmt.Sprintf("TRY_CAST(%s AS BIGINT)", base)
	case forma.ValueTypeInteger, forma.ValueTypeSmallInt:
		if a.isColumn {
			// Bound storage is physically int4/int2: declared width IS the
			// storage width, and the COALESCE partner m.<col> is just as
			// narrow, so nothing wider can ever round-trip.
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
		return "(value_numeric <> 0)"
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
// bool_text encoding stores "1"/"0" as text; bool_smallint stores 0/1 as SMALLINT.
// Both must produce a BOOLEAN so that COALESCE(hot_vals.<attr>, <expr>) is type-safe.
func mainColBoolExpr(colName string, enc forma.MainColumnEncoding) string {
	if enc == forma.MainColumnEncodingBoolText {
		return fmt.Sprintf("m.%s = '1'", colName)
	}
	// Default covers MainColumnEncodingBoolInt and any other numeric-like encoding.
	return fmt.Sprintf("m.%s <> 0", colName)
}
