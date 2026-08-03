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

// ParquetNullRowIDMessage is the text the scan-level row_id guard raises. It
// names the invariant rather than the object: the scan source is schema-blind
// and path-blind, and a path in a DuckDB error would be a storage-location
// leak (#306). The manifest entry and the pre-read validator identify the
// offending object.
const ParquetNullRowIDMessage = "parquet scan produced NULL row_id: a scanned object violates the export schema invariant (#189/#256)"

// parquetRowIDGuardItem rewrites row_id in place so any scanned parquet row
// whose row_id is NULL fails the query. It closes the one silent-loss channel
// the #256 manifest stamps opened: a stamp with valid system columns spares
// its path the footer probe, so a rogue overwrite (or a tampered manifest)
// can put an object whose real bytes lack row_id into a scan the validator
// waved through. union_by_name NULL-fills the absent column, those rows drop
// out of the dirty anti-join, and the query succeeds while ignoring the file.
//
// Three engine behaviors make this shape the one that works (proved against
// the pinned DuckDB in duckdb_cold_scan_guard_test.go):
//   - error() is NOT folded at bind time inside the COALESCE, so a healthy
//     scan is unaffected;
//   - error() carries no type of its own, so COALESCE adopts row_id's — UUID
//     for production exports, VARCHAR for the benchmark shape (schemas
//     100-102 render through this same source). An explicit CAST would bind
//     one of the two and coerce the other (#147);
//   - REPLACE puts the guard IN the row_id expression, so projection pushdown
//     cannot prune it while row_id is still read — and both template scan
//     sites read it (the anti-join and the semijoin's SELECT row_id).
//
// A scan set where NO object carries row_id fails to bind the REPLACE list
// instead. Different message, same contract: loud, never silent.
var parquetRowIDGuardItem = fmt.Sprintf(
	"* REPLACE (COALESCE(row_id, error('%s')) AS row_id)", ParquetNullRowIDMessage)

// BuildParquetScanSource renders the parquet scan for the advanced
// template's two scan sites: the row_id guard above, plus (#255) a typed NULL
// for every current-schema column absent from every file in the set.
//
// The guard renders unconditionally, so this no longer has a "bare
// read_parquet" idle state — the pre-#256 byte-identity of the zero-missing
// output is deliberately retired. Every rendered-SQL contract that pinned it
// (including the #214 design-doc guard and docs/federated-query/design.md §5)
// moves in lockstep.
func BuildParquetScanSource(pathsSQL string, missing []NullScanColumn) string {
	base := fmt.Sprintf("read_parquet(%s, union_by_name=true)", pathsSQL)
	parts := make([]string, 0, len(missing)+1)
	parts = append(parts, parquetRowIDGuardItem)
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
