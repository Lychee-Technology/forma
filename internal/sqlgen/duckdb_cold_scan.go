package sqlgen

import (
	"fmt"
	"strings"

	"github.com/lychee-technology/forma"
)

// NullScanColumn names one current-schema attribute column that is
// physically absent from EVERY parquet file in a query's resolved scan set
// (#255): an attribute added to the schema before its first flush. The scan
// source projects it as a typed NULL so both the explicit projection
// (S3SourceSelect) and the semijoin's logical clause bind, with exact SQL
// NULL semantics for filters, sorts, and keysets.
type NullScanColumn struct {
	// Name is the folded parquet column name (ParquetAttrColumn output).
	Name string
	// DuckDBType renders as NULL::<type>. It must type-unify with the
	// pg_source leg of the UNION ALL, so it mirrors the hot-tier EAV pivot
	// (buildEAVPivotExpr) and the CDC export (cdc.castEAVValue) — the #205
	// no-widening parity.
	DuckDBType string
}

// BuildParquetScanSource renders the parquet scan for the advanced
// template's two scan sites. With no missing columns the output is
// byte-identical to the pre-#255 template text — the idle-state invariant
// every rendered-SQL contract (including the #214 design-doc guard) relies
// on. With missing columns it wraps the scan so each cold-absent column
// exists as a typed NULL.
func BuildParquetScanSource(pathsSQL string, missing []NullScanColumn) string {
	base := fmt.Sprintf("read_parquet(%s, union_by_name=true)", pathsSQL)
	if len(missing) == 0 {
		return base
	}
	parts := make([]string, 0, len(missing)+1)
	parts = append(parts, "*")
	for _, mc := range missing {
		parts = append(parts, fmt.Sprintf("NULL::%s AS %s", mc.DuckDBType, mc.Name))
	}
	return fmt.Sprintf("(SELECT %s FROM %s) AS cold_scan", strings.Join(parts, ", "), base)
}

// DuckDBNullScanType maps an attribute's value type to the DuckDB type its
// parquet column would carry, for NULL::<type> augmentation. Kept in
// lockstep with buildEAVPivotExpr / eavElementCastExpr (hot leg) and
// cdc.castEAVValue (export leg): a mismatch would widen the UNION ALL and
// re-open #205.
func DuckDBNullScanType(vt forma.ValueType, itemsType forma.ValueType) string {
	if vt == forma.ValueTypeList {
		return duckDBNullScalarType(itemsType) + "[]"
	}
	return duckDBNullScalarType(vt)
}

func duckDBNullScalarType(vt forma.ValueType) string {
	switch vt {
	case forma.ValueTypeBool:
		return "BOOLEAN"
	case forma.ValueTypeBigInt, forma.ValueTypeDate, forma.ValueTypeDateTime:
		return "BIGINT"
	case forma.ValueTypeInteger:
		return "INTEGER"
	case forma.ValueTypeSmallInt:
		return "SMALLINT"
	case forma.ValueTypeNumeric:
		return "DOUBLE"
	default: // text / uuid — VARCHAR on every tier
		return "VARCHAR"
	}
}
