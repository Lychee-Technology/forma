package sqlgen

import (
	"github.com/lychee-technology/forma/internal/numutil"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/conditionexpr"
)

// operatorValuePair holds a parsed operator and value from a kv condition value string.
type operatorValuePair struct {
	op    string
	value string
}

// parseOperatorValue extracts operator and value from "op:value" format.
// If no colon is found, defaults to "equals" operator with full string as value.
func parseOperatorValue(kvValue string) operatorValuePair {
	parsed := conditionexpr.ParseOperatorValueLenient(kvValue)
	return operatorValuePair{op: parsed.Operator, value: parsed.Value}
}

// parseOperatorValueStrict validates "op:value" format when a separator is present.
// It returns an error when either side is empty to avoid ambiguous predicates.
func parseOperatorValueStrict(kvValue string) (operatorValuePair, error) {
	parsed, err := conditionexpr.ParseOperatorValueStrict(kvValue)
	if err != nil {
		return operatorValuePair{}, err
	}
	return operatorValuePair{op: parsed.Operator, value: parsed.Value}, nil
}

// sqlOperatorResult holds the SQL operator and possibly modified value for LIKE patterns.
type sqlOperatorResult struct {
	sqlOp string
	value string
}

// toSQLOperator converts a string operator to SQL operator syntax.
// For LIKE operators, it also modifies the value to include wildcards.
func toSQLOperator(op, value string) (sqlOperatorResult, error) {
	normalized, err := conditionexpr.ToSQLOperator(op, value)
	if err != nil {
		return sqlOperatorResult{}, err
	}
	return sqlOperatorResult{
		sqlOp: normalized.SQLOperator,
		value: normalized.Value,
	}, nil
}

// convertPgMainValue converts a string value to the appropriate Go type based on attribute metadata.
// This is used for Postgres main table pushdown predicates.
func convertPgMainValue(valStr string, attr string, meta forma.AttributeMetadata) (any, error) {
	switch meta.ValueType {
	case forma.ValueTypeText, forma.ValueTypeUUID:
		return valStr, nil

	case forma.ValueTypeNumeric, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeSmallInt:
		parsed := numutil.TryParseNumber(valStr)
		switch v := parsed.(type) {
		case int64:
			return float64(v), nil
		case float64:
			return v, nil
		default:
			return nil, fmt.Errorf("invalid numeric value for '%s': %s", attr, valStr)
		}

	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		parsedValue, err := parseDateValue(valStr, meta)
		if err != nil {
			return nil, fmt.Errorf("invalid date value for '%s': %w", attr, err)
		}
		return parsedValue, nil

	case forma.ValueTypeBool:
		return convertPgBoolValue(valStr, attr, meta)

	default:
		return nil, fmt.Errorf("unsupported value_type '%s' for attribute '%s'", meta.ValueType, attr)
	}
}

// convertPgBoolValue converts a boolean string value respecting column encoding.
func convertPgBoolValue(valStr string, attr string, meta forma.AttributeMetadata) (any, error) {
	parsedInt, err := strconv.Atoi(valStr)
	if err != nil {
		return nil, fmt.Errorf("invalid boolean value for '%s': %s", attr, valStr)
	}

	if meta.ColumnBinding == nil {
		// default to text "1"/"0"
		if parsedInt > 0 {
			return "1", nil
		}
		return "0", nil
	}

	switch meta.ColumnBinding.Encoding {
	case forma.MainColumnEncodingBoolInt:
		if parsedInt > 0 {
			return int64(1), nil
		}
		return int64(0), nil
	case forma.MainColumnEncodingBoolText:
		if parsedInt > 0 {
			return "1", nil
		}
		return "0", nil
	default:
		// default to text "1"/"0"
		if parsedInt > 0 {
			return "1", nil
		}
		return "0", nil
	}
}

// detectValueType infers the forma.ValueType from a string literal when no metadata is available.
func detectValueType(valStr string) forma.ValueType {
	if _, err := uuid.Parse(valStr); err == nil {
		return forma.ValueTypeUUID
	}
	if ls := strings.ToLower(valStr); ls == "true" || ls == "false" || ls == "1" || ls == "0" {
		return forma.ValueTypeBool
	}
	if _, err := strconv.ParseFloat(valStr, 64); err == nil {
		return forma.ValueTypeNumeric
	}
	if _, err := time.Parse(time.RFC3339Nano, valStr); err == nil {
		return forma.ValueTypeDateTime
	}
	if _, err := strconv.ParseInt(valStr, 10, 64); err == nil {
		return forma.ValueTypeNumeric
	}
	return forma.ValueTypeText
}

// parseDuckDBRawParam parses a string value into a typed Go value for DuckDB parameters.
func parseDuckDBRawParam(valStr string, attr string, valueType forma.ValueType) (any, error) {
	switch valueType {
	case forma.ValueTypeUUID:
		return valStr, nil

	case forma.ValueTypeBool:
		if b, e := strconv.ParseBool(strings.ToLower(valStr)); e == nil {
			return b, nil
		} else if valStr == "1" {
			return true, nil
		} else if valStr == "0" {
			return false, nil
		}
		return valStr, nil

	case forma.ValueTypeNumeric, forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt:
		if f, e := strconv.ParseFloat(valStr, 64); e == nil {
			return f, nil
		}
		return nil, fmt.Errorf("invalid numeric literal for %s: %s", attr, valStr)

	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		if t, e := time.Parse(time.RFC3339Nano, valStr); e == nil {
			return t.UTC(), nil
		} else if i, e := strconv.ParseInt(valStr, 10, 64); e == nil {
			return time.UnixMilli(i).UTC(), nil
		}
		return nil, fmt.Errorf("invalid date literal for %s: %s", attr, valStr)

	default:
		return valStr, nil
	}
}

// resolveMainTableColumn returns the column name prefixed with "m." for main table queries.
func resolveMainTableColumn(attr string, meta forma.AttributeMetadata) string {
	if meta.ColumnBinding != nil {
		return "m." + string(meta.ColumnBinding.ColumnName)
	}
	return "m." + attr
}

// resolveDuckDBColumn returns the column name for DuckDB queries.
func resolveDuckDBColumn(attr string, cache forma.SchemaAttributeCache) string {
	if m, ok := cache[attr]; ok && m.ColumnBinding != nil {
		return string(m.ColumnBinding.ColumnName)
	}
	return attr
}
