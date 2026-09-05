package federated

// columnUnion is the pre-read validator's report of the resolved scan set's
// parquet columns. types holds one DuckDB type per column name, first-seen
// across the set; mixed names every column that was seen with more than one
// type. Both are consumed by coldScanColumns: an attribute absent from types
// is augmented as a typed NULL (#255), and one that is mixed — or whose
// first-seen type disagrees with the schema — is re-pinned in the scan's
// REPLACE list (#371) so union_by_name cannot widen it quietly.
//
// A zero columnUnion (types == nil) means "unknown": nothing was validated,
// and neither augmentation nor pinning may run on it.
type columnUnion struct {
	types map[string]string
	mixed map[string]struct{}
}

func newColumnUnion() columnUnion {
	return columnUnion{types: map[string]string{}, mixed: map[string]struct{}{}}
}

// merge folds one footer's columns into the running union. The first-seen
// type stays the reported one; a later footer that disagrees marks the
// column mixed instead of overwriting, so the report is order-independent
// in what it flags even though the surviving type name is not.
func (u columnUnion) merge(cols map[string]string) {
	for name, typ := range cols {
		seen, ok := u.types[name]
		if !ok {
			u.types[name] = typ
			continue
		}
		if seen != typ {
			u.mixed[name] = struct{}{}
		}
	}
}

// isMixed reports whether name was seen with more than one type.
func (u columnUnion) isMixed(name string) bool {
	_, ok := u.mixed[name]
	return ok
}
