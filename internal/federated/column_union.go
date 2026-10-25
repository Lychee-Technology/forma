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
//
// files keeps each contributing object's own columns, in validation order, for
// the #732 type guard: a column the union reports as mixed or stale is judged
// object by object, and the refusal names the object. The maps are the
// validator's cached ones, never copied, so keeping them costs one slice entry
// per object. unverified lists the concrete objects whose columns could not be
// read at all — the guard cannot judge them, and the read fails closed on them
// when the manifest authored the path set.
type columnUnion struct {
	types      map[string]string
	mixed      map[string]struct{}
	files      []unionFile
	unverified []string
}

// unionFile is one validated object's columns (name → DuckDB type).
type unionFile struct {
	path string
	cols map[string]string
}

func newColumnUnion() columnUnion {
	return columnUnion{types: map[string]string{}, mixed: map[string]struct{}{}}
}

// merge folds one object's footer columns into the running union. The first-seen
// type stays the reported one; a later footer that disagrees marks the
// column mixed instead of overwriting, so the report is order-independent
// in what it flags even though the surviving type name is not.
func (u *columnUnion) merge(path string, cols map[string]string) {
	u.files = append(u.files, unionFile{path: path, cols: cols})
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
