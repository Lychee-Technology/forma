package sqlgen

import (
	"fmt"
	"strconv"
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

// pgEavLeafEmitter renders KvCondition leaves as EAV EXISTS subqueries with $N placeholders.
type pgEavLeafEmitter struct {
	eavTable   string
	schemaID   int16
	cache      forma.SchemaAttributeCache
	paramIndex *int
}

func (e *pgEavLeafEmitter) EmitLeaf(kv *forma.KvCondition) (string, []any, error) {
	_ = e.schemaID

	meta, ok := e.cache[kv.Attr]
	if !ok {
		return "", nil, fmt.Errorf("attribute not found in cache: %s", kv.Attr)
	}

	operator, err := parseOperatorValueStrict(kv.Value)
	if err != nil {
		return "", nil, err
	}
	opStr := operator.op
	valStr := operator.value

	var valueColumn string
	var parsedValue any

	switch meta.ValueType {
	case forma.ValueTypeText, forma.ValueTypeUUID:
		valueColumn = "value_text"
		parsedValue = valStr
	case forma.ValueTypeNumeric, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeSmallInt:
		valueColumn = "value_numeric"
		parsed := tryParseNumber(valStr)
		switch v := parsed.(type) {
		case int64:
			parsedValue = float64(v)
		case float64:
			parsedValue = v
		default:
			return "", nil, fmt.Errorf("invalid numeric value for '%s': %s", kv.Attr, valStr)
		}
	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		valueColumn = "value_numeric"
		var err error
		parsedValue, err = parseDateValue(valStr, meta)
		if err != nil {
			return "", nil, fmt.Errorf("invalid date value for '%s': %w", kv.Attr, err)
		}
	case forma.ValueTypeBool:
		valueColumn = "value_numeric"
		parsedInt, err := strconv.Atoi(valStr)
		if err != nil {
			return "", nil, fmt.Errorf("invalid boolean value for '%s': %s", kv.Attr, valStr)
		}
		if parsedInt > 0 {
			parsedValue = float64(1)
		} else {
			parsedValue = float64(0)
		}
	default:
		return "", nil, fmt.Errorf("unsupported value_type '%s' for attribute '%s'", meta.ValueType, kv.Attr)
	}

	sqlOperator, err := toSQLOperator(opStr, valStr)
	if err != nil {
		return "", nil, err
	}
	sqlOp := sqlOperator.sqlOp
	if sqlOp == "LIKE" {
		parsedValue = sqlOperator.value
	}

	if meta.ValueType != forma.ValueTypeText && sqlOp == "LIKE" {
		return "", nil, fmt.Errorf("operator '%s' only supported for text attributes, not '%s'", opStr, meta.ValueType)
	}
	if meta.ValueType == forma.ValueTypeBool && sqlOp != "=" && sqlOp != "!=" {
		return "", nil, fmt.Errorf("operator '%s' not supported for boolean attributes", opStr)
	}

	var args []any

	*e.paramIndex++
	attrIdPlaceholder := fmt.Sprintf("$%d", *e.paramIndex)
	args = append(args, meta.AttributeID)

	*e.paramIndex++
	valuePlaceholder := fmt.Sprintf("$%d", *e.paramIndex)
	args = append(args, parsedValue)

	sql := fmt.Sprintf(
		"EXISTS (SELECT 1 FROM %s x WHERE x.schema_id = e.schema_id AND x.row_id = e.row_id AND x.attr_id = %s AND x.%s %s %s)",
		e.eavTable,
		attrIdPlaceholder,
		valueColumn,
		sqlOp,
		valuePlaceholder,
	)

	return sql, args, nil
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
	emitter := &pgEavLeafEmitter{
		eavTable:   eavTable,
		schemaID:   schemaID,
		cache:      cache,
		paramIndex: paramIndex,
	}
	return walkCondition(condition, pgEavStyle, nil, emitter)
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
