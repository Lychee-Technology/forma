package federated

import (
	"sort"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// coldMissingColumns computes the #255 augmentation set: current-schema
// attributes whose folded parquet column is absent from the ENTIRE resolved
// scan set (an attribute added before its first flush). unionCols must be a
// COMPLETE footer union (validator complete=true); passing nil — union
// unknown — yields nil, which renders the unaugmented scan and preserves
// today's loud classified failure. Sorted by column name so the rendered
// SQL and the plan-cache scope key are deterministic.
//
// unionCols also carries the parquet system columns (row_id, changed_at,
// deleted_at), which is harmless: only schema-attribute folded names are
// ever probed against it, and those never collide with the system set (the
// #260 reserved-column guard rejects such schemas at registration).
func coldMissingColumns(cache forma.SchemaAttributeCache, unionCols map[string]string) []sqlgen.NullScanColumn {
	if len(cache) == 0 || unionCols == nil {
		return nil
	}
	var missing []sqlgen.NullScanColumn
	for name, meta := range cache {
		col := sqlgen.ParquetAttrColumn(name)
		if _, ok := unionCols[col]; ok {
			continue
		}
		missing = append(missing, sqlgen.NullScanColumn{
			Name:       col,
			DuckDBType: sqlgen.DuckDBNullScanType(meta),
		})
	}
	sort.Slice(missing, func(i, j int) bool { return missing[i].Name < missing[j].Name })
	return missing
}
