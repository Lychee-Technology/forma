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

// ParquetNullRowIDMessage and ParquetNullChangedAtMessage are the texts the
// scan-level system-column guard raises. Each names the offending column and
// the invariant rather than the object: the scan source is schema-blind and
// path-blind, and a path in a DuckDB error would be a storage-location leak
// (#306). The manifest entry and the pre-read validator identify the offending
// object.
//
// Correlating a fired guard back to a specific object is the read path's job:
// on a read failure that neither the missing-object classification (#187) nor
// the corruption confirmation (#251) claims, federated.identifyGuardViolations
// re-reads each manifest-listed object through this same guarded source one
// file at a time and names the violator(s) in the returned
// ParquetGuardViolationError and the engine log (#351). The trigger is
// deliberately not a guard-specific classification: recognizing a fired guard
// would mean matching error text, which misses the BIGINT CAST channel
// entirely — its wording is DuckDB's own — so identification decides by
// differential drain instead. Manual bisection (design.md §5) remains the
// fallback for hint-authored path sets, which identification does not cover.
const (
	ParquetNullRowIDMessage     = "parquet scan produced NULL row_id: a scanned object violates the export schema invariant (#189/#256)"
	ParquetNullChangedAtMessage = "parquet scan produced NULL changed_at: a scanned object violates the export schema invariant (#189/#256)"
)

// parquetSystemColumnGuardItem rewrites the parquetcheck system columns in
// place so any scanned parquet row that violates the export schema invariant
// fails the query. It closes the one silent-loss channel the #256 manifest
// stamps opened: a stamp with valid system columns spares its path the footer
// probe, so a rogue overwrite (or a tampered manifest) can put an object whose
// real bytes lack a system column into a scan the validator waved through.
// union_by_name NULL-fills the absent column and the rows flow on — a missing
// row_id drops them out of the dirty anti-join, a missing changed_at feeds
// NULL into LWW version ordering — and the query succeeds either way.
//
// Two guard shapes, because the two channels differ:
//
//   - PRESENCE (NULL → error): applied to row_id and changed_at, both of which
//     are never legitimately NULL. Flush exports cl.changed_at from a NOT NULL
//     change_log column; init/base exports m.ltbase_updated_at, likewise NOT
//     NULL (#210); the benchmark shape carries changed_at directly
//     (duckdb_benchmark_projection.go). deleted_at gets NO presence guard:
//     although both exporters now COALESCE live rows to 0 (#274), delta
//     objects written BEFORE #274 still encode live rows as NULL and remain
//     readable until compaction retires them, so a NULL-based presence guard
//     would still error on healthy legacy data. See the residual note below.
//
//   - TYPE (CAST): applied to changed_at, deleted_at and ltbase_created_at,
//     all BIGINT in the production AND benchmark shapes. Without it a rogue
//     file carrying one as VARCHAR widens the union_by_name result to VARCHAR
//     and ordering silently goes lexicographic ('9' > '100') — for changed_at
//     that misfolds LWW, for ltbase_created_at it misorders the default page
//     (#460). The CAST re-pins BIGINT: numeric strings coerce
//     value-preservingly, garbage fails loudly.
//     row_id gets NO cast — it is UUID in production exports and VARCHAR in
//     the benchmark shape, so any cast would bind one and coerce the other
//     (#147); its untyped COALESCE adopts whichever the file carries.
//
// ltbase_created_at is type-pinned but NOT presence-guarded, for the same
// reason deleted_at is not: hard-delete tombstones legitimately carry a NULL
// creation stamp (the delta export LEFT JOINs entity_main, #173), so a
// value-presence guard would fail every healthy scan touching one. Its
// COLUMN presence is enforced a layer up, by the parquetcheck invariant and
// the pre-read validator — which is what keeps a mixed-generation scan set
// from NULL-padding it into the result (#460).
//
// Three engine behaviors make this shape the one that works (proved against
// the pinned DuckDB in duckdb_cold_scan_guard_test.go):
//   - error() is NOT folded at bind time inside a COALESCE — with either or
//     both guarded columns present — so a healthy scan is unaffected;
//   - error() carries no type of its own, so COALESCE adopts the column's;
//   - REPLACE puts each guard IN the column's own expression, so projection
//     pushdown cannot prune it while that column is still read — and both
//     template scan sites read row_id (the anti-join and the semijoin's
//     SELECT row_id) while the merge reads changed_at.
//
// RESIDUAL (#365): deleted_at's PRESENCE cannot be value-guarded here while
// pre-#274 delta objects (live rows encoded as NULL) are still readable —
// such a guard would fail every healthy scan touching one. Its TYPE is pinned
// by the CAST above; a file missing the column entirely still reaches the
// merge as NULL (characterized in duckdb_cold_scan_guard_test.go), covered
// only by the pre-read footer probe and the manifest stamp. #274 normalized
// the delta encoding to 0 for NEW objects; extending the presence guard is
// gated on the legacy objects being retired by compaction — tracked in #365.
//
// A scan set where NO object carries a guarded column fails to bind the
// REPLACE list instead. Different message, same contract: loud, never silent.
var parquetSystemColumnGuardItem = fmt.Sprintf(
	"* REPLACE (COALESCE(row_id, error('%s')) AS row_id, "+
		"CAST(COALESCE(changed_at, error('%s')) AS BIGINT) AS changed_at, "+
		"CAST(deleted_at AS BIGINT) AS deleted_at, "+
		"CAST(ltbase_created_at AS BIGINT) AS ltbase_created_at)",
	ParquetNullRowIDMessage, ParquetNullChangedAtMessage)

// BuildParquetScanSource renders the parquet scan for the advanced
// template's two scan sites: the system-column guard above, plus (#255) a
// typed NULL for every current-schema column absent from every file in the
// set.
//
// The guard renders unconditionally, so this no longer has a "bare
// read_parquet" idle state — the pre-#256 byte-identity of the zero-missing
// output is deliberately retired. Every rendered-SQL contract that pinned it
// (including the #214 design-doc guard and docs/federated-query/design.md §5)
// moves in lockstep.
func BuildParquetScanSource(pathsSQL string, missing []NullScanColumn) string {
	base := fmt.Sprintf("read_parquet(%s, union_by_name=true)", pathsSQL)
	parts := make([]string, 0, len(missing)+1)
	parts = append(parts, parquetSystemColumnGuardItem)
	for _, mc := range missing {
		parts = append(parts, fmt.Sprintf("NULL::%s AS %s", mc.DuckDBType, mc.Name))
	}
	return fmt.Sprintf("(SELECT %s FROM %s) AS cold_scan", strings.Join(parts, ", "), base)
}

// DuckDBNullScanType maps an attribute's metadata to the DuckDB type its
// parquet column would carry, for NULL::<type> augmentation. Kept in
// lockstep with buildEAVPivotExpr / eavElementCastExpr (hot leg) and
// cdc.castEAVValue / castMainValue (export leg): a mismatch would widen the
// UNION ALL and re-open #205. The binding matters since #384: EAV-only
// integer/smallint carry storage width (DOUBLE — value_numeric holds float64
// images) on every DuckDB surface, while column-bound ones keep the physical
// int4/int2 width.
func DuckDBNullScanType(meta forma.AttributeMetadata) string {
	if meta.ValueType == forma.ValueTypeList {
		// Lists are always EAV-only; elements carry EAV storage width.
		return duckDBNullScalarType(meta.EffectiveItemsType(), false) + "[]"
	}
	return duckDBNullScalarType(meta.ValueType, meta.ColumnBinding != nil)
}

func duckDBNullScalarType(vt forma.ValueType, isColumn bool) string {
	switch vt {
	case forma.ValueTypeBool:
		return "BOOLEAN"
	case forma.ValueTypeBigInt, forma.ValueTypeDate, forma.ValueTypeDateTime:
		return "BIGINT"
	case forma.ValueTypeInteger:
		if isColumn {
			return "INTEGER"
		}
		return "DOUBLE"
	case forma.ValueTypeSmallInt:
		if isColumn {
			return "SMALLINT"
		}
		return "DOUBLE"
	case forma.ValueTypeNumeric:
		return "DOUBLE"
	default: // text / uuid — VARCHAR on every tier
		return "VARCHAR"
	}
}
