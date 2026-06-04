package internal

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// MapValueTypeToDuckDBType maps forma.ValueType to a DuckDB SQL type string.
// For LIST types, returns the element type only (caller must wrap in LIST(...) if needed).
func MapValueTypeToDuckDBType(v forma.ValueType) string {
	switch v {
	case forma.ValueTypeText:
		return "VARCHAR"
	case forma.ValueTypeUUID:
		return "VARCHAR"
	case forma.ValueTypeSmallInt:
		return "SMALLINT"
	case forma.ValueTypeInteger:
		return "INTEGER"
	case forma.ValueTypeBigInt:
		return "BIGINT"
	case forma.ValueTypeNumeric:
		// Use DECIMAL to preserve numeric precision instead of DOUBLE
		// DuckDB DECIMAL defaults to DECIMAL(18,3) if not specified
		return "DECIMAL"
	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		// Use TIMESTAMP for temporal types (configurable in future)
		return "TIMESTAMP"
	case forma.ValueTypeBool:
		return "BOOLEAN"
	case forma.ValueTypeList:
		// LIST is a container type; typically used as LIST(element_type).
		// Return VARCHAR as default element type; caller can override.
		return "VARCHAR"
	default:
		// Fallback to VARCHAR for unknown types
		return "VARCHAR"
	}
}

// MapValueTypeToListDuckDBType returns the DuckDB LIST type for an array of the given element type.
// e.g., ValueTypeText -> "LIST(VARCHAR)", ValueTypeInteger -> "LIST(INTEGER)"
func MapValueTypeToListDuckDBType(elementType forma.ValueType) string {
	elemDuckType := MapValueTypeToDuckDBType(elementType)
	return "LIST(" + elemDuckType + ")"
}

// IsListType returns true if the ValueType represents a list/array.
func IsListType(v forma.ValueType) bool {
	return v == forma.ValueTypeList
}

// CastExpression returns a DuckDB-safe CAST expression for a column or expression.
// The caller is responsible for ensuring the identifier/expression is safe (e.g. using ident helper).
func CastExpression(columnOrExpr string, v forma.ValueType) string {
	return fmt.Sprintf("CAST(%s AS %s)", columnOrExpr, MapValueTypeToDuckDBType(v))
}

// ToDuckDBParam converts a Go value to the form expected by DuckDB drivers for the given value type.
// Examples:
//   - uuid.UUID -> string
//   - time.Time -> time.Time (TIMESTAMP)
//   - numeric types -> float64
func ToDuckDBParam(value any, v forma.ValueType) (any, error) {
	if value == nil {
		return nil, nil
	}
	switch v {
	case forma.ValueTypeUUID:
		switch t := value.(type) {
		case uuid.UUID:
			return t.String(), nil
		case *uuid.UUID:
			uuidValue, isNil := optionalPointerValue(t)
			if isNil {
				return nil, nil
			}
			return uuidValue.String(), nil
		case string:
			return t, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to UUID param", value)
		}
	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		// Expect a time.Time for TIMESTAMP mapping; accept epoch strings/numbers handled elsewhere if needed.
		switch t := value.(type) {
		case time.Time:
			return t.UTC(), nil
		case *time.Time:
			timeValue, isNil := optionalPointerValue(t)
			if isNil {
				return nil, nil
			}
			return timeValue.UTC(), nil
		default:
			return nil, fmt.Errorf("cannot convert %T to TIMESTAMP param", value)
		}
	case forma.ValueTypeBool:
		switch b := value.(type) {
		case bool:
			return b, nil
		case *bool:
			boolValue, isNil := optionalPointerValue(b)
			if isNil {
				return nil, nil
			}
			return boolValue, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to BOOLEAN param", value)
		}
	case forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeNumeric:
		if n, ok := value.(string); ok {
			// leave parsing to caller; return string so it can be param-parsed by template renderer if desired
			return n, nil
		}

		numeric, isNil, err := toOptionalFloat64Param(value)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %T to numeric param", value)
		}
		if isNil {
			return nil, nil
		}
		return numeric, nil
	case forma.ValueTypeText:
		switch s := value.(type) {
		case string:
			return s, nil
		case *string:
			textValue, isNil := optionalPointerValue(s)
			if isNil {
				return nil, nil
			}
			return textValue, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to text param", value)
		}
	default:
		// Fallback: return as-is
		return value, nil
	}
}

func toOptionalFloat64Param(value any) (float64, bool, error) {
	switch v := value.(type) {
	case *float64:
		return optionalFloat64FromPointer(v)
	case *float32:
		return optionalFloat64FromPointer(v)
	case *int:
		return optionalFloat64FromPointer(v)
	case *int16:
		return optionalFloat64FromPointer(v)
	case *int32:
		return optionalFloat64FromPointer(v)
	case *int64:
		return optionalFloat64FromPointer(v)
	default:
		num, err := toFloat64(value)
		if err != nil {
			return 0, false, err
		}
		return num, false, nil
	}
}
