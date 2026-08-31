// Package parquetcheck defines the parquet export schema invariant shared by
// every Forma parquet consumer: the system columns each generation carries
// regardless of attribute evolution, with the exact DuckDB types both
// exporters emit (delta flush and init/compaction base). The federated read
// path and the compaction merge both enforce it before trusting a file
// (#189): their scans run union_by_name, which would otherwise NULL-pad a
// malformed file's system columns and silently misfold its rows instead of
// failing loudly (#187).
package parquetcheck

import "fmt"

// SystemColumn is one invariant column with its required DuckDB type.
type SystemColumn struct {
	Name     string
	DuckType string
}

// SystemColumns lists the invariant, ordered so violation errors are
// deterministic — row_id first, since everything folds on it, then the fold
// columns, then the projected value columns.
//
// ltbase_created_at joined the invariant with #460, when the federated
// reader started projecting it as created_at instead of aliasing changed_at.
// A required input column MUST be validated at this boundary: without it a
// mixed-generation scan set (some objects carrying the column, some not) is
// NULL-padded by union_by_name, and those rows reach the caller with a NULL
// created_at. They are then ordered by that NULL under the default
// ORDER BY created_at DESC instead of by their real creation time, so their
// page position is wrong and MOVES once compaction folds the object into a
// file that carries the column — and a created_at keyset cursor over them has
// no usable boundary. Validating it here turns that into the loud pre-read
// failure every other system column already gets.
//
// Its TYPE is invariant; its VALUE is not required to be non-NULL. The delta
// export LEFT JOINs entity_main so a hard-deleted row's change_log tombstone
// still exports (#173), and those rows legitimately carry a NULL creation
// stamp. They are dropped by the deleted_ts filter before any ORDER BY, so
// the NULL never reaches a caller — see sqlgen.buildS3Projection.
func SystemColumns() []SystemColumn {
	return []SystemColumn{
		{Name: "row_id", DuckType: "UUID"},
		{Name: "changed_at", DuckType: "BIGINT"},
		{Name: "deleted_at", DuckType: "BIGINT"},
		{Name: "ltbase_created_at", DuckType: "BIGINT"},
	}
}

// Check verifies one probed parquet schema (column name → DuckDB type)
// against the invariant. The returned error names the object, the offending
// column, and the expected state; callers wrap it with their own sentinel or
// operational context.
func Check(path string, cols map[string]string) error {
	for _, sys := range SystemColumns() {
		got, ok := cols[sys.Name]
		if !ok {
			return fmt.Errorf(
				"parquet object %s violates the export schema invariant: system column %q (%s) is missing — the object was not written by a Forma exporter",
				path, sys.Name, sys.DuckType)
		}
		if got != sys.DuckType {
			return fmt.Errorf(
				"parquet object %s violates the export schema invariant: system column %q has type %s, want %s",
				path, sys.Name, got, sys.DuckType)
		}
	}
	return nil
}
