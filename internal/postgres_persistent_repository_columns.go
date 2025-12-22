package internal

import "strings"

var (
	textColumns = []string{
		"text_01", "text_02", "text_03", "text_04", "text_05",
		"text_06", "text_07", "text_08", "text_09", "text_10",
	}
	smallintColumns = []string{"smallint_01", "smallint_02", "smallint_03"}
	integerColumns  = []string{"integer_01", "integer_02", "integer_03"}
	bigintColumns   = []string{"bigint_01", "bigint_02", "bigint_03"}
	doubleColumns   = []string{"double_01", "double_02", "double_03"}
	uuidColumns     = []string{"uuid_01", "uuid_02"}
)

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

var entityMainColumnDescriptors = []columnDescriptor{}
var entityMainProjection string

func init() {
	projection := make([]string, 0, 5+len(textColumns)+len(smallintColumns)+len(integerColumns)+len(bigintColumns)+len(doubleColumns)+len(uuidColumns))

	// Add system fields first
	entityMainColumnDescriptors = append(entityMainColumnDescriptors, columnDescriptor{name: "ltbase_schema_id", kind: columnKindSmallint})
	projection = append(projection, "ltbase_schema_id")

	entityMainColumnDescriptors = append(entityMainColumnDescriptors, columnDescriptor{name: "ltbase_row_id", kind: columnKindUUID})
	projection = append(projection, "ltbase_row_id")

	entityMainColumnDescriptors = append(entityMainColumnDescriptors, columnDescriptor{name: "ltbase_created_at", kind: columnKindBigint})
	projection = append(projection, "ltbase_created_at")

	entityMainColumnDescriptors = append(entityMainColumnDescriptors, columnDescriptor{name: "ltbase_updated_at", kind: columnKindBigint})
	projection = append(projection, "ltbase_updated_at")

	entityMainColumnDescriptors = append(entityMainColumnDescriptors, columnDescriptor{name: "ltbase_deleted_at", kind: columnKindBigint})
	projection = append(projection, "ltbase_deleted_at")

	// Add remaining text columns (skip text_01 as it's already added)
	for _, col := range textColumns {
		entityMainColumnDescriptors = append(entityMainColumnDescriptors, columnDescriptor{name: col, kind: columnKindText})
		projection = append(projection, col)
	}
	for _, col := range smallintColumns {
		entityMainColumnDescriptors = append(entityMainColumnDescriptors, columnDescriptor{name: col, kind: columnKindSmallint})
		projection = append(projection, col)
	}
	for _, col := range integerColumns {
		entityMainColumnDescriptors = append(entityMainColumnDescriptors, columnDescriptor{name: col, kind: columnKindInteger})
		projection = append(projection, col)
	}
	for _, col := range bigintColumns {
		entityMainColumnDescriptors = append(entityMainColumnDescriptors, columnDescriptor{name: col, kind: columnKindBigint})
		projection = append(projection, col)
	}
	for _, col := range doubleColumns {
		entityMainColumnDescriptors = append(entityMainColumnDescriptors, columnDescriptor{name: col, kind: columnKindDouble})
		projection = append(projection, col)
	}
	for _, col := range uuidColumns {
		entityMainColumnDescriptors = append(entityMainColumnDescriptors, columnDescriptor{name: col, kind: columnKindUUID})
		projection = append(projection, col)
	}
	entityMainProjection = strings.Join(projection, ", ")
}

func isMainTableColumn(name string) bool {
	for _, desc := range entityMainColumnDescriptors {
		if desc.name == name {
			return true
		}
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

func getMainColumnDescriptor(name string) *columnDescriptor {
	for _, desc := range entityMainColumnDescriptors {
		if desc.name == name {
			return &desc
		}
	}
	return nil
}
