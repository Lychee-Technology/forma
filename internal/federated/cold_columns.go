package federated

import (
	"sort"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// coldScanSet is the per-query attribute-column treatment the parquet scan
// source needs, computed from a COMPLETE footer union:
//
//   - missing (#255): attributes whose folded column is absent from the
//     ENTIRE resolved scan set (added before their first flush), rendered
//     as NULL::<type>.
//   - pinned (#371): attributes present in the set whose type disagrees
//     with the schema's expected scan type or differs between files,
//     rendered as CAST(col AS <type>) in the scan's REPLACE list.
//
// Both slices are sorted by column name so the rendered SQL and the
// plan-cache scope key are deterministic.
type coldScanSet struct {
	missing []sqlgen.ScanColumn
	pinned  []sqlgen.ScanColumn
}

func (s coldScanSet) empty() bool {
	return len(s.missing) == 0 && len(s.pinned) == 0
}

// coldScanColumns computes the coldScanSet for cache against union. union
// must be COMPLETE (validator complete=true); an unknown union (zero value)
// yields an empty set, which renders the plain guarded scan and preserves
// today's loud classified failure for both the binder (#255) and the
// conversion (#315) case.
//
// The pin is selective on purpose: a healthy scan set — every file carries
// the column at the type the exporters write, which DuckDBNullScanType
// mirrors — pins nothing, so the rendered SQL is byte-identical to the
// unpinned form and attribute filter pushdown into parquet stays CAST-free.
// Only a stale-typed generation (the #371 INTEGER→VARCHAR drift) or a
// cross-generation disagreement earns the CAST. VARCHAR and UUID are one
// type for this purpose (sqlgen.ScanTypesCompatible).
//
// union also carries the parquet system columns (row_id, changed_at,
// deleted_at), which is harmless: only schema-attribute folded names are
// ever probed against it, and those never collide with the system set (the
// #260 reserved-column guard rejects such schemas at registration).
func coldScanColumns(cache forma.SchemaAttributeCache, union columnUnion) coldScanSet {
	if len(cache) == 0 || union.types == nil {
		return coldScanSet{}
	}
	var set coldScanSet
	for name, meta := range cache {
		col := sqlgen.ParquetAttrColumn(name)
		want := sqlgen.DuckDBNullScanType(meta)
		observed, present := union.types[col]
		switch {
		case !present:
			set.missing = append(set.missing, sqlgen.ScanColumn{Name: col, DuckDBType: want})
		case union.isMixed(col) || !sqlgen.ScanTypesCompatible(observed, want):
			set.pinned = append(set.pinned, sqlgen.ScanColumn{Name: col, DuckDBType: want})
		}
	}
	sortScanColumns(set.missing)
	sortScanColumns(set.pinned)
	return set
}

func sortScanColumns(cols []sqlgen.ScanColumn) {
	sort.Slice(cols, func(i, j int) bool { return cols[i].Name < cols[j].Name })
}
