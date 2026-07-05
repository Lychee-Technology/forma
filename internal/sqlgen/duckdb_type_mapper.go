package sqlgen

import (
	"github.com/lychee-technology/forma/internal/numutil"
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
		// Use explicit-precision DECIMAL to preserve numeric precision instead of DOUBLE.
		// DECIMAL(38,10) supports 38 digits total, 10 fractional — enough for financial
		// and scientific use-cases without default truncation.
		return "DECIMAL(38,10)"
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
			uuidValue, isNil := numutil.OptionalPointerValue(t)
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
			timeValue, isNil := numutil.OptionalPointerValue(t)
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
			boolValue, isNil := numutil.OptionalPointerValue(b)
			if isNil {
				return nil, nil
			}
			return boolValue, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to BOOLEAN param", value)
		}
	case forma.ValueTypeSmallInt, forma.ValueTypeInteger:
		numeric, isNil, err := toOptionalFloat64Param(value)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %T to numeric param", value)
		}
		if isNil {
			return nil, nil
		}
		return numeric, nil
	case forma.ValueTypeBigInt, forma.ValueTypeNumeric:
		// Preserve exact precision by converting to string — DuckDB accepts
		// string representations for DECIMAL and HUGEINT column bindings.
		if s, ok := value.(string); ok {
			return s, nil
		}
		switch v := value.(type) {
		case *float64:
			if v == nil {
				return nil, nil
			}
			return decimalString(*v), nil
		case *float32:
			if v == nil {
				return nil, nil
			}
			return decimalString(float64(*v)), nil
		case *int:
			if v == nil {
				return nil, nil
			}
			return fmt.Sprintf("%d", *v), nil
		case *int16:
			if v == nil {
				return nil, nil
			}
			return fmt.Sprintf("%d", *v), nil
		case *int32:
			if v == nil {
				return nil, nil
			}
			return fmt.Sprintf("%d", *v), nil
		case *int64:
			if v == nil {
				return nil, nil
			}
			return fmt.Sprintf("%d", *v), nil
		case float64:
			return decimalString(v), nil
		case float32:
			return decimalString(float64(v)), nil
		case int64:
			return fmt.Sprintf("%d", v), nil
		case int:
			return fmt.Sprintf("%d", v), nil
		case int32:
			return fmt.Sprintf("%d", v), nil
		case int16:
			return fmt.Sprintf("%d", v), nil
		default:
			return nil, fmt.Errorf("cannot convert %T to bigint/numeric param", value)
		}
	case forma.ValueTypeText:
		switch s := value.(type) {
		case string:
			return s, nil
		case *string:
			textValue, isNil := numutil.OptionalPointerValue(s)
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

func decimalString(v float64) string {
	return fmt.Sprintf("%.15g", v)
}

func toOptionalFloat64Param(value any) (float64, bool, error) {
	switch v := value.(type) {
	case *float64:
		return numutil.OptionalFloat64FromPointer(v)
	case *float32:
		return numutil.OptionalFloat64FromPointer(v)
	case *int:
		return numutil.OptionalFloat64FromPointer(v)
	case *int16:
		return numutil.OptionalFloat64FromPointer(v)
	case *int32:
		return numutil.OptionalFloat64FromPointer(v)
	case *int64:
		return numutil.OptionalFloat64FromPointer(v)
	default:
		num, err := numutil.Float64(value)
		if err != nil {
			return 0, false, err
		}
		return num, false, nil
	}
}
