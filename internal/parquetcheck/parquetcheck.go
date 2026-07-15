// Package parquetcheck defines the parquet export schema invariant shared by
// every Forma parquet consumer: the three system columns each generation
// carries regardless of attribute evolution, with the exact DuckDB types both
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
// deterministic — row_id first, since everything folds on it.
func SystemColumns() []SystemColumn {
	return []SystemColumn{
		{Name: "row_id", DuckType: "UUID"},
		{Name: "changed_at", DuckType: "BIGINT"},
		{Name: "deleted_at", DuckType: "BIGINT"},
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
