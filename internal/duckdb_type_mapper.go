package internal

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// MapValueTypeToDuckDBType maps forma.ValueType to a DuckDB SQL type string.
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
		return "DOUBLE"
	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		// Use TIMESTAMP for temporal types (configurable in future)
		return "TIMESTAMP"
	case forma.ValueTypeBool:
		return "BOOLEAN"
	default:
		// Fallback to VARCHAR for unknown types
		return "VARCHAR"
	}
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
			if t == nil {
				return nil, nil
			}
			return t.String(), nil
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
			if t == nil {
				return nil, nil
			}
			return t.UTC(), nil
		default:
			return nil, fmt.Errorf("cannot convert %T to TIMESTAMP param", value)
		}
	case forma.ValueTypeBool:
		switch b := value.(type) {
		case bool:
			return b, nil
		case *bool:
			if b == nil {
				return nil, nil
			}
			return *b, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to BOOLEAN param", value)
		}
	case forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeNumeric:
		// Normalize numeric inputs to float64 for driver compatibility where appropriate.
		switch n := value.(type) {
		case float64:
			return n, nil
		case *float64:
			if n == nil {
				return nil, nil
			}
			return *n, nil
		case float32:
			return float64(n), nil
		case *float32:
			if n == nil {
				return nil, nil
			}
			return float64(*n), nil
		case int:
			return float64(n), nil
		case *int:
			if n == nil {
				return nil, nil
			}
			return float64(*n), nil
		case int16:
			return float64(n), nil
		case *int16:
			if n == nil {
				return nil, nil
			}
			return float64(*n), nil
		case int32:
			return float64(n), nil
		case *int32:
			if n == nil {
				return nil, nil
			}
			return float64(*n), nil
		case int64:
			return float64(n), nil
		case *int64:
			if n == nil {
				return nil, nil
			}
			return float64(*n), nil
		case string:
			// leave parsing to caller; return string so it can be param-parsed by template renderer if desired
			return n, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to numeric param", value)
		}
	case forma.ValueTypeText:
		switch s := value.(type) {
		case string:
			return s, nil
		case *string:
			if s == nil {
				return nil, nil
			}
			return *s, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to text param", value)
		}
	default:
		// Fallback: return as-is
		return value, nil
	}
}
