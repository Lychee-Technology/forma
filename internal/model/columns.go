package model

import "strings"

// Column slices define the physical hot-storage columns available in entity_main.
// These are declared once and treated as read-only after package init.
var (
	TextColumns     = []string{"ltbase_created_by", "ltbase_deleted_by", "ltbase_updated_by", "text_01", "text_02", "text_03", "text_04", "text_05", "text_06", "text_07", "text_08", "text_09", "text_10"}
	SmallintColumns = []string{"smallint_01", "smallint_02", "smallint_03"}
	IntegerColumns  = []string{"integer_01", "integer_02", "integer_03"}
	BigintColumns   = []string{"bigint_01", "bigint_02", "bigint_03"}
	DoubleColumns   = []string{"double_01", "double_02", "double_03"}
	UUIDColumns     = []string{"uuid_01", "uuid_02"}
)

// allowedXxxColumns are fast O(1) lookup sets built from the column slices above.
var (
	AllowedTextColumns     = makeColumnSet(TextColumns)
	AllowedSmallintColumns = makeColumnSet(SmallintColumns)
	AllowedIntegerColumns  = makeColumnSet(IntegerColumns)
	AllowedBigintColumns   = makeColumnSet(BigintColumns)
	AllowedDoubleColumns   = makeColumnSet(DoubleColumns)
	AllowedUUIDColumns     = makeColumnSet(UUIDColumns)
)

func makeColumnSet(columns []string) map[string]struct{} {
	set := make(map[string]struct{}, len(columns))
	for _, col := range columns {
		set[col] = struct{}{}
	}
	return set
}

type ColumnKind int

const (
	ColumnKindText ColumnKind = iota
	ColumnKindSmallint
	ColumnKindInteger
	ColumnKindBigint
	ColumnKindDouble
	ColumnKindUUID
)

type ColumnDescriptor struct {
	Name string
	Kind ColumnKind
}

// SystemColumnDescriptors contains the five fixed metadata columns that appear
// before the EAV columns in every entity_main SELECT projection.
// They are not present in the allowedXxx sets, so GetMainColumnDescriptor
// checks this list separately.
var SystemColumnDescriptors = []ColumnDescriptor{
	{Name: "ltbase_schema_id", Kind: ColumnKindSmallint},
	{Name: "ltbase_row_id", Kind: ColumnKindUUID},
	{Name: "ltbase_created_at", Kind: ColumnKindBigint},
	{Name: "ltbase_updated_at", Kind: ColumnKindBigint},
	{Name: "ltbase_deleted_at", Kind: ColumnKindBigint},
}

// systemColumnSet is the O(1) lookup equivalent of SystemColumnDescriptors.
var systemColumnSet = func() map[string]ColumnDescriptor {
	m := make(map[string]ColumnDescriptor, len(SystemColumnDescriptors))
	for _, d := range SystemColumnDescriptors {
		m[d.Name] = d
	}
	return m
}()

// EntityMainColumnDescriptors is the ordered list of all columns (system + EAV)
// used to scan rows from entity_main. It is built once at package load time via
// a package-level var initializer, avoiding an init() side-effect.
var EntityMainColumnDescriptors = buildEntityMainColumnDescriptors()

func buildEntityMainColumnDescriptors() []ColumnDescriptor {
	capacity := len(SystemColumnDescriptors) +
		len(TextColumns) + len(SmallintColumns) + len(IntegerColumns) +
		len(BigintColumns) + len(DoubleColumns) + len(UUIDColumns)
	descs := make([]ColumnDescriptor, 0, capacity)

	descs = append(descs, SystemColumnDescriptors...)

	for _, col := range TextColumns {
		descs = append(descs, ColumnDescriptor{Name: col, Kind: ColumnKindText})
	}
	for _, col := range SmallintColumns {
		descs = append(descs, ColumnDescriptor{Name: col, Kind: ColumnKindSmallint})
	}
	for _, col := range IntegerColumns {
		descs = append(descs, ColumnDescriptor{Name: col, Kind: ColumnKindInteger})
	}
	for _, col := range BigintColumns {
		descs = append(descs, ColumnDescriptor{Name: col, Kind: ColumnKindBigint})
	}
	for _, col := range DoubleColumns {
		descs = append(descs, ColumnDescriptor{Name: col, Kind: ColumnKindDouble})
	}
	for _, col := range UUIDColumns {
		descs = append(descs, ColumnDescriptor{Name: col, Kind: ColumnKindUUID})
	}
	return descs
}

// EntityMainProjection is the comma-separated column list used in SELECT statements
// against entity_main. Built once at package load time.
var EntityMainProjection = func() string {
	names := make([]string, 0, len(EntityMainColumnDescriptors))
	for _, d := range EntityMainColumnDescriptors {
		names = append(names, d.Name)
	}
	return strings.Join(names, ", ")
}()

// IsMainTableColumn reports whether name is a recognised entity_main column.
// Uses O(1) map lookups — no linear scan.
func IsMainTableColumn(name string) bool {
	if _, ok := systemColumnSet[name]; ok {
		return true
	}
	if _, ok := AllowedTextColumns[name]; ok {
		return true
	}
	if _, ok := AllowedSmallintColumns[name]; ok {
		return true
	}
	if _, ok := AllowedIntegerColumns[name]; ok {
		return true
	}
	if _, ok := AllowedBigintColumns[name]; ok {
		return true
	}
	if _, ok := AllowedDoubleColumns[name]; ok {
		return true
	}
	if _, ok := AllowedUUIDColumns[name]; ok {
		return true
	}
	return false
}

// GetMainColumnDescriptor returns the descriptor for a named entity_main column,
// or nil if the name is not recognised. Checks the system-column map first, then
// falls back to the ordered EAV descriptor list (linear but called infrequently).
func GetMainColumnDescriptor(name string) *ColumnDescriptor {
	if d, ok := systemColumnSet[name]; ok {
		return &d
	}
	for _, desc := range EntityMainColumnDescriptors[len(SystemColumnDescriptors):] {
		if desc.Name == name {
			return &desc
		}
	}
	return nil
}
