package sqlgen

import "github.com/lychee-technology/forma/internal/model"

// pgByteOrderCollation is the PostgreSQL collation whose order is the bytes of
// the UTF-8 text. It is the order of Forma's text range operators on every
// route (#728): DuckDB compares VARCHAR that way on all three tiers, and it
// has no equivalent of an arbitrary PostgreSQL collation, so naming this one
// on the Postgres route is what keeps an answer independent of the database's
// default collation.
//
// A btree index built under another collation cannot seek such a compare, and
// that includes the default collation of a database whose default is C,
// because the planner matches collations by OID (docs/schema-indexes.md).
const pgByteOrderCollation = `"C"`

// inByteOrder spells a text column for a range compare. The clause sits on the
// column, not the placeholder, so the bind still takes its type from the column.
func inByteOrder(column string) string {
	return column + " COLLATE " + pgByteOrderCollation
}

func isRangeComparison(sqlOp string) bool {
	switch sqlOp {
	case "<", "<=", ">", ">=":
		return true
	}
	return false
}

func isTextColumn(colName string) bool {
	desc := model.GetMainColumnDescriptor(colName)
	return desc != nil && desc.Kind == model.ColumnKindText
}
