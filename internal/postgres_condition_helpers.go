package internal

import (
	"github.com/lychee-technology/forma/internal/numutil"
	"fmt"
	"github.com/lychee-technology/forma/internal/model"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/conditionexpr"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// hasMainTableCondition checks if a condition contains predicates on main table columns
func hasMainTableCondition(cond forma.Condition, cache forma.SchemaAttributeCache) bool {
	if cond == nil {
		// An empty condition implies no filtering; treat as having main table condition
		return true
	}
	switch c := cond.(type) {
	case *forma.CompositeCondition:
		if c == nil {
			return false
		}
		for _, child := range c.Conditions {
			if hasMainTableCondition(child, cache) {
				return true
			}
		}
		return false
	case *forma.KvCondition:
		if c == nil {
			return false
		}
		// Check if it's a raw main table column name
		if model.IsMainTableColumn(c.Attr) {
			return true
		}
		// Check if it's an attribute with column_binding to main table
		if cache != nil {
			if meta, ok := cache[c.Attr]; ok {
				if meta.Location() == forma.AttributeStorageLocationMain {
					return true
				}
			}
		}
		return false
	default:
		return false
	}
}

// parseKvConditionForColumnWithMeta parses a KvCondition for a specific column name with metadata
// The meta parameter is used to determine the encoding for date/time values
func parseKvConditionForColumnWithMeta(kv *forma.KvCondition, colName string, meta *forma.AttributeMetadata) (string, any, error) {
	opStr, valStr := parseOperatorAndValue(kv.Value)

	sqlOp, err := operatorToSQL(opStr)
	if err != nil {
		return "", nil, err
	}

	// Adjust value for pattern operators
	switch opStr {
	case "starts_with":
		valStr = valStr + "%"
	case "contains":
		valStr = "%" + valStr + "%"
	}

	desc := model.GetMainColumnDescriptor(colName)
	if desc == nil {
		return "", nil, fmt.Errorf("unknown main table column: %s", colName)
	}

	parsedValue, err := parseColumnValue(desc, valStr, meta)
	if err != nil {
		return "", nil, err
	}

	return sqlOp, parsedValue, nil
}

// parseOperatorAndValue extracts operator and value from a condition value string
func parseOperatorAndValue(value string) (string, string) {
	parts := strings.SplitN(value, ":", 2)
	if len(parts) == 1 {
		return "equals", value
	}
	return parts[0], parts[1]
}

// operatorToSQL converts a string operator to its SQL equivalent
func operatorToSQL(opStr string) (string, error) {
	sqlOp, err := conditionexpr.ToSQLOperator(opStr, "")
	if err != nil {
		return "", err
	}
	return sqlOp.SQLOperator, nil
}

// parseColumnValue parses and converts a value string based on column type
func parseColumnValue(desc *model.ColumnDescriptor, valStr string, meta *forma.AttributeMetadata) (any, error) {
	switch desc.Kind {
	case model.ColumnKindText:
		return valStr, nil
	case model.ColumnKindSmallint, model.ColumnKindInteger, model.ColumnKindBigint, model.ColumnKindDouble:
		// Check if this is a date/time field that needs conversion
		if meta != nil && (meta.ValueType == forma.ValueTypeDate || meta.ValueType == forma.ValueTypeDateTime) {
			return convertDateValueForQuery(valStr, meta)
		}
		return numutil.TryParseNumber(valStr), nil
	case model.ColumnKindUUID:
		return valStr, nil
	default:
		return valStr, nil
	}
}

// convertDateValueForQuery converts a date value string to the appropriate format for querying.
// It supports both ISO 8601 format strings and Unix millisecond timestamps as input.
// The output format is determined by the storage encoding in metadata.
func convertDateValueForQuery(valStr string, meta *forma.AttributeMetadata) (any, error) {
	if meta == nil {
		emptyMeta := forma.AttributeMetadata{}
		return sqlgen.ParseDateValue(valStr, emptyMeta)
	}
	return sqlgen.ParseDateValue(valStr, *meta)
}
