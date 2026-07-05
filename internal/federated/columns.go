package federated

import "strings"

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

var systemColumnDescriptors = []columnDescriptor{
	{name: "ltbase_schema_id", kind: columnKindSmallint},
	{name: "ltbase_row_id", kind: columnKindUUID},
	{name: "ltbase_created_at", kind: columnKindBigint},
	{name: "ltbase_updated_at", kind: columnKindBigint},
	{name: "ltbase_deleted_at", kind: columnKindBigint},
}

var entityMainColumnDescriptors = buildEntityMainColumnDescriptors()

var entityMainProjection = func() string {
	names := make([]string, 0, len(entityMainColumnDescriptors))
	for _, d := range entityMainColumnDescriptors {
		names = append(names, d.name)
	}
	return strings.Join(names, ", ")
}()

func buildEntityMainColumnDescriptors() []columnDescriptor {
	textColumns := []string{"ltbase_created_by", "ltbase_deleted_by", "ltbase_updated_by", "text_01", "text_02", "text_03", "text_04", "text_05", "text_06", "text_07", "text_08", "text_09", "text_10"}
	smallintColumns := []string{"smallint_01", "smallint_02", "smallint_03"}
	integerColumns := []string{"integer_01", "integer_02", "integer_03"}
	bigintColumns := []string{"bigint_01", "bigint_02", "bigint_03"}
	doubleColumns := []string{"double_01", "double_02", "double_03"}
	uuidColumns := []string{"uuid_01", "uuid_02"}

	capacity := len(systemColumnDescriptors) + len(textColumns) + len(smallintColumns) + len(integerColumns) + len(bigintColumns) + len(doubleColumns) + len(uuidColumns)
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
