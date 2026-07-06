package sqlgen

import (
	"fmt"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/conditionexpr"
)

// parseDateValue parses a date value string and converts it based on storage encoding.
// Supports both ISO 8601 format strings and Unix millisecond timestamps.
// Returns the parsed value ready for SQL query based on the column encoding.
func parseDateValue(valStr string, meta forma.AttributeMetadata) (any, error) {
	parsedTime, err := conditionexpr.ParseRFC3339OrUnixMs(valStr)
	if err != nil {
		return nil, err
	}

	// Convert based on storage encoding
	if meta.ColumnBinding != nil {
		encoding := meta.ColumnBinding.Encoding
		switch encoding {
		case forma.MainColumnEncodingUnixMs:
			// Return Unix milliseconds as int64 for bigint column
			return parsedTime.UnixMilli(), nil
		case forma.MainColumnEncodingISO8601:
			// Return ISO 8601 string for text column
			return parsedTime.Format(time.RFC3339), nil
		}
	}

	// Default: return as time.Time for EAV storage (stored as unix ms in value_numeric)
	return parsedTime.UnixMilli(), nil
}

func ParseDateValue(valStr string, meta forma.AttributeMetadata) (any, error) {
	return parseDateValue(valStr, meta)
}

// SQLGenerator converts parsed conditions into SQL fragments and argument lists.
type SQLGenerator struct{}

// NewSQLGenerator constructs a SQLGenerator.
func NewSQLGenerator() *SQLGenerator {
	return &SQLGenerator{}
}

// pgEavTypedEmitter renders typed predicate leaves as EAV EXISTS subqueries
// with $N placeholders. All parsing and value conversion happened in the
// normalizer; the emitter only assigns placeholders and formats the shell.
type pgEavTypedEmitter struct {
	eavTable   string
	paramIndex *int
}

func (e *pgEavTypedEmitter) EmitTypedLeaf(leaf *PredicateLeaf) (string, []any, error) {
	p := leaf.PgEav
	if p.Err != nil {
		return "", nil, p.Err
	}

	var args []any

	*e.paramIndex++
	attrIdPlaceholder := fmt.Sprintf("$%d", *e.paramIndex)
	args = append(args, p.AttrID)

	*e.paramIndex++
	valuePlaceholder := fmt.Sprintf("$%d", *e.paramIndex)
	args = append(args, p.Value)

	sql := fmt.Sprintf(
		"EXISTS (SELECT 1 FROM %s x WHERE x.schema_id = e.schema_id AND x.row_id = e.row_id AND x.attr_id = %s AND x.%s %s %s)",
		e.eavTable,
		attrIdPlaceholder,
		p.ValueColumn,
		p.SQLOp,
		valuePlaceholder,
	)

	return sql, args, nil
}

// pgEavClausesFromTree walks an already-normalized predicate tree for the
// EAV target, letting ToDualClauses share one normalization pass.
func pgEavClausesFromTree(tree PredicateNode, eavTable string, paramIndex *int) (string, []any, error) {
	emitter := &pgEavTypedEmitter{eavTable: eavTable, paramIndex: paramIndex}
	return walkPredicate(tree, pgEavStyle, nil, emitter)
}

// ToSQLClauses builds the SQL clause and arguments for a condition tree.
func (g *SQLGenerator) ToSQLClauses(
	condition forma.Condition,
	eavTable string,
	schemaID int16,
	cache forma.SchemaAttributeCache,
	paramIndex *int,
) (string, []any, error) {
	if condition == nil {
		return "", nil, nil
	}
	return pgEavClausesFromTree(normalizePredicates(condition, cache, targetPgEav), eavTable, paramIndex)
}

// ToSqlClauses is kept for backward compatibility.
func (g *SQLGenerator) ToSqlClauses(
	condition forma.Condition,
	eavTable string,
	schemaID int16,
	cache forma.SchemaAttributeCache,
	paramIndex *int,
) (string, []any, error) {
	return g.ToSQLClauses(condition, eavTable, schemaID, cache, paramIndex)
}
