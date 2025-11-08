package internal

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"lychee.technology/ltbase/forma"
)

// parseDateValue parses a date value string and converts it based on storage encoding.
// Supports both ISO 8601 format strings and Unix millisecond timestamps.
// Returns the parsed value ready for SQL query based on the column encoding.
func parseDateValue(valStr string, meta AttributeMetadata) (any, error) {
	// First, try to parse as ISO 8601 format
	parsedTime, err := time.Parse(time.RFC3339, valStr)
	if err != nil {
		// Try parsing as Unix milliseconds
		parsedInt64, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid date value: expected ISO 8601 format or unix milliseconds, got '%s'", valStr)
		}
		parsedTime = time.UnixMilli(parsedInt64)
	}

	// Convert based on storage encoding
	if meta.Storage != nil && meta.Storage.ColumnBinding != nil {
		encoding := meta.Storage.ColumnBinding.Encoding
		switch encoding {
		case MainColumnEncodingUnixMs:
			// Return Unix milliseconds as int64 for bigint column
			return parsedTime.UnixMilli(), nil
		case MainColumnEncodingISO8601:
			// Return ISO 8601 string for text column
			return parsedTime.Format(time.RFC3339), nil
		}
	}

	// Default: return as time.Time for EAV storage (stored as unix ms in value_numeric)
	return parsedTime.UnixMilli(), nil
}

// SQLGenerator converts parsed conditions into SQL fragments and argument lists.
type SQLGenerator struct{}

// NewSQLGenerator constructs a SQLGenerator.
func NewSQLGenerator() *SQLGenerator {
	return &SQLGenerator{}
}

// ToSqlClauses builds the SQL clause and arguments for a condition tree.
func (g *SQLGenerator) ToSqlClauses(
	condition forma.Condition,
	eavTable string,
	schemaID int16,
	cache SchemaAttributeCache,
	paramIndex *int,
) (string, []any, error) {
	if condition == nil {
		return "", nil, nil
	}
	return g.buildCondition(condition, eavTable, schemaID, cache, paramIndex)
}

func (g *SQLGenerator) buildCondition(
	condition forma.Condition,
	eavTable string,
	schemaID int16,
	cache SchemaAttributeCache,
	paramIndex *int,
) (string, []any, error) {
	switch cond := condition.(type) {
	case *forma.CompositeCondition:
		return g.buildComposite(cond, eavTable, schemaID, cache, paramIndex)
	case *forma.KvCondition:
		return g.buildKv(cond, eavTable, schemaID, cache, paramIndex)
	default:
		return "", nil, fmt.Errorf("unsupported condition type %T", condition)
	}
}

func (g *SQLGenerator) buildComposite(
	c *forma.CompositeCondition,
	eavTable string,
	schemaID int16,
	cache SchemaAttributeCache,
	paramIndex *int,
) (string, []any, error) {
	if len(c.Conditions) == 0 {
		return "", nil, nil
	}

	var sqlJoiner string
	switch c.Logic {
	case forma.LogicAnd:
		sqlJoiner = " AND "
	case forma.LogicOr:
		sqlJoiner = " OR "
	default:
		return "", nil, fmt.Errorf("unknown logic: %s", c.Logic)
	}

	var childClauses []string
	var allArgs []any

	for _, cond := range c.Conditions {
		sql, args, err := g.buildCondition(cond, eavTable, schemaID, cache, paramIndex)
		if err != nil {
			return "", nil, err
		}

		if sql == "" {
			continue
		}

		childClauses = append(childClauses, fmt.Sprintf("(%s)", sql))
		allArgs = append(allArgs, args...)
	}

	if len(childClauses) == 0 {
		return "", nil, nil
	}

	if len(childClauses) == 1 {
		return childClauses[0], allArgs, nil
	}

	finalSql := "(" + strings.Join(childClauses, sqlJoiner) + ")"
	return finalSql, allArgs, nil
}

func (g *SQLGenerator) buildKv(
	kv *forma.KvCondition,
	eavTable string,
	schemaID int16,
	cache SchemaAttributeCache,
	paramIndex *int,
) (string, []any, error) {
	_ = schemaID

	meta, ok := cache[kv.Attr]
	if !ok {
		return "", nil, fmt.Errorf("attribute not found in cache: %s", kv.Attr)
	}

	parts := strings.SplitN(kv.Value, ":", 2)
	var opStr, valStr string
	if len(parts) == 1 {
		opStr, valStr = "equals", kv.Value
	} else {
		opStr, valStr = parts[0], parts[1]
		if opStr == "" || valStr == "" {
			return "", nil, fmt.Errorf("invalid KvCondition value format: %s", kv.Value)
		}
	}

	var valueColumn string
	var parsedValue any

	switch meta.ValueType {
	case ValueTypeText:
		valueColumn = "value_text"
		parsedValue = valStr
	case ValueTypeNumeric:
		valueColumn = "value_numeric"
		parsedValue = tryParseNumber(valStr)
		if _, ok := parsedValue.(string); ok {
			return "", nil, fmt.Errorf("invalid numeric value for '%s': %s", kv.Attr, valStr)
		}
	case ValueTypeDate, ValueTypeDateTime:
		valueColumn = "value_numeric"
		var err error
		parsedValue, err = parseDateValue(valStr, meta)
		if err != nil {
			return "", nil, fmt.Errorf("invalid date value for '%s': %w", kv.Attr, err)
		}
	case ValueTypeBool:
		valueColumn = "value_bool"
		parsedFloat, err := strconv.ParseFloat(valStr, 32)
		if err != nil {
			return "", nil, fmt.Errorf("invalid boolean value for '%s': %s", kv.Attr, valStr)
		}
		parsedValue = parsedFloat > 0.5
	default:
		return "", nil, fmt.Errorf("unsupported value_type '%s' for attribute '%s'", meta.ValueType, kv.Attr)
	}

	var sqlOp string
	switch opStr {
	case "equals":
		sqlOp = "="
	case "gt":
		sqlOp = ">"
	case "gte":
		sqlOp = ">="
	case "lt":
		sqlOp = "<"
	case "lte":
		sqlOp = "<="
	case "not_equals":
		sqlOp = "!="
	case "starts_with":
		sqlOp = "LIKE"
		parsedValue = valStr + "%"
	case "contains":
		sqlOp = "LIKE"
		parsedValue = "%" + valStr + "%"
	default:
		return "", nil, fmt.Errorf("unsupported operator: %s", opStr)
	}

	if meta.ValueType != ValueTypeText && sqlOp == "LIKE" {
		return "", nil, fmt.Errorf("operator '%s' only supported for text attributes, not '%s'", opStr, meta.ValueType)
	}
	if meta.ValueType == ValueTypeBool && sqlOp != "=" && sqlOp != "!=" {
		return "", nil, fmt.Errorf("operator '%s' not supported for boolean attributes", opStr)
	}

	var args []any

	*paramIndex++
	attrIdPlaceholder := fmt.Sprintf("$%d", *paramIndex)
	args = append(args, meta.AttributeID)

	*paramIndex++
	valuePlaceholder := fmt.Sprintf("$%d", *paramIndex)
	args = append(args, parsedValue)

	sql := fmt.Sprintf(
		"EXISTS (SELECT 1 FROM %s x WHERE x.schema_id = e.schema_id AND x.row_id = e.row_id AND x.attr_id = %s AND x.%s %s %s)",
		eavTable,
		attrIdPlaceholder,
		valueColumn,
		sqlOp,
		valuePlaceholder,
	)

	return sql, args, nil
}
