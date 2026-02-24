package internal

import "strings"

// Column slices define the physical EAV column layout in the entity_main table.
// These are declared once and treated as read-only after package init.
var (
	textColumns     = []string{"text_01", "text_02", "text_03", "text_04", "text_05", "text_06", "text_07", "text_08", "text_09", "text_10"}
	smallintColumns = []string{"smallint_01", "smallint_02", "smallint_03"}
	integerColumns  = []string{"integer_01", "integer_02", "integer_03"}
	bigintColumns   = []string{"bigint_01", "bigint_02", "bigint_03"}
	doubleColumns   = []string{"double_01", "double_02", "double_03"}
	uuidColumns     = []string{"uuid_01", "uuid_02"}
)

// allowedXxxColumns are fast O(1) lookup sets built from the column slices above.
var (
	allowedTextColumns     = makeColumnSet(textColumns)
	allowedSmallintColumns = makeColumnSet(smallintColumns)
	allowedIntegerColumns  = makeColumnSet(integerColumns)
	allowedBigintColumns   = makeColumnSet(bigintColumns)
	allowedDoubleColumns   = makeColumnSet(doubleColumns)
	allowedUUIDColumns     = makeColumnSet(uuidColumns)
)

func makeColumnSet(columns []string) map[string]struct{} {
	set := make(map[string]struct{}, len(columns))
	for _, col := range columns {
		set[col] = struct{}{}
	}
	return set
}

type columnKind int

const (
	columnKindText columnKind = iota
	columnKindSmallint
	columnKindInteger
	columnKindBigint
	columnKindDouble
	columnKindUUID
)

type columnDescriptor struct {
	name string
	kind columnKind
}

// systemColumnDescriptors contains the five fixed metadata columns that appear
// before the EAV columns in every entity_main SELECT projection.
// They are not present in the allowedXxx sets, so getMainColumnDescriptor
// checks this list separately.
var systemColumnDescriptors = []columnDescriptor{
	{name: "ltbase_schema_id", kind: columnKindSmallint},
	{name: "ltbase_row_id", kind: columnKindUUID},
	{name: "ltbase_created_at", kind: columnKindBigint},
	{name: "ltbase_updated_at", kind: columnKindBigint},
	{name: "ltbase_deleted_at", kind: columnKindBigint},
}

// systemColumnSet is the O(1) lookup equivalent of systemColumnDescriptors.
var systemColumnSet = func() map[string]columnDescriptor {
	m := make(map[string]columnDescriptor, len(systemColumnDescriptors))
	for _, d := range systemColumnDescriptors {
		m[d.name] = d
	}
	return m
}()

// entityMainColumnDescriptors is the ordered list of all columns (system + EAV)
// used to scan rows from entity_main. It is built once at package load time via
// a package-level var initializer, avoiding an init() side-effect.
var entityMainColumnDescriptors = buildEntityMainColumnDescriptors()

func buildEntityMainColumnDescriptors() []columnDescriptor {
	capacity := len(systemColumnDescriptors) +
		len(textColumns) + len(smallintColumns) + len(integerColumns) +
		len(bigintColumns) + len(doubleColumns) + len(uuidColumns)
	descs := make([]columnDescriptor, 0, capacity)

	descs = append(descs, systemColumnDescriptors...)

	for _, col := range textColumns {
		descs = append(descs, columnDescriptor{name: col, kind: columnKindText})
	}
	for _, col := range smallintColumns {
		descs = append(descs, columnDescriptor{name: col, kind: columnKindSmallint})
	}
	for _, col := range integerColumns {
		descs = append(descs, columnDescriptor{name: col, kind: columnKindInteger})
	}
	for _, col := range bigintColumns {
		descs = append(descs, columnDescriptor{name: col, kind: columnKindBigint})
	}
	for _, col := range doubleColumns {
		descs = append(descs, columnDescriptor{name: col, kind: columnKindDouble})
	}
	for _, col := range uuidColumns {
		descs = append(descs, columnDescriptor{name: col, kind: columnKindUUID})
	}
	return descs
}

// entityMainProjection is the comma-separated column list used in SELECT statements
// against entity_main. Built once at package load time.
var entityMainProjection = func() string {
	names := make([]string, 0, len(entityMainColumnDescriptors))
	for _, d := range entityMainColumnDescriptors {
		names = append(names, d.name)
	}
	return strings.Join(names, ", ")
}()

// isMainTableColumn reports whether name is a recognised entity_main column.
// Uses O(1) map lookups — no linear scan.
func isMainTableColumn(name string) bool {
	if _, ok := systemColumnSet[name]; ok {
		return true
	}
	if _, ok := allowedTextColumns[name]; ok {
		return true
	}
	if _, ok := allowedSmallintColumns[name]; ok {
		return true
	}
	if _, ok := allowedIntegerColumns[name]; ok {
		return true
	}
	if _, ok := allowedBigintColumns[name]; ok {
		return true
	}
	if _, ok := allowedDoubleColumns[name]; ok {
		return true
	}
	if _, ok := allowedUUIDColumns[name]; ok {
		return true
	}
	return false
}

// getMainColumnDescriptor returns the descriptor for a named entity_main column,
// or nil if the name is not recognised. Checks the system-column map first, then
// falls back to the ordered EAV descriptor list (linear but called infrequently).
func getMainColumnDescriptor(name string) *columnDescriptor {
	if d, ok := systemColumnSet[name]; ok {
		return &d
	}
	for _, desc := range entityMainColumnDescriptors[len(systemColumnDescriptors):] {
		if desc.name == name {
			return &desc
		}
	}
	return nil
}
