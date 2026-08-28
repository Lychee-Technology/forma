package sqlgen

import (
	"fmt"
	"strconv"
	"time"

	"github.com/lychee-technology/forma/internal/numutil"

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
	case forma.ValueTypeSmallInt, forma.ValueTypeInteger:
		// #384: predicate operands cast by column storage width. The EAV
		// write funnel narrows every numeric-family value through float64
		// (numutil.Float64), so an EAV integer/smallint column's true storage
		// width is DOUBLE — which is also what every DuckDB tier projects for
		// it (buildEAVPivotExpr) — and a bound int4/int2 column compares
		// against a DOUBLE operand by lossless promotion (INT32 ≪ 2^53). The
		// old declared-width cast was strict, so an out-of-range operand
		// raised a Conversion Error on the DuckDB route while Postgres
		// answered normally; at DOUBLE any magnitude simply compares.
		return "DOUBLE"
	case forma.ValueTypeBigInt:
		return "BIGINT"
	case forma.ValueTypeNumeric:
		// #384: every numeric column on every tier is DOUBLE (EAV pivot,
		// parquet export, main double columns). The former DECIMAL(38,10)
		// operand cast silently truncated operands beyond 10 fractional
		// digits (false mismatch against the DOUBLE column) and overflowed
		// past ~1e28 (one-sided query failure).
		return "DOUBLE"
	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		// Date/datetime attribute columns in the federated CTEs are epoch-ms
		// BIGINT on all three sides — EAV pivot TRY_CAST(value_numeric AS
		// BIGINT) (#173), main-column unix_ms export (#194), and the reader
		// projection CAST(attr AS BIGINT) — so predicates must compare
		// BIGINT, never TIMESTAMP (#200).
		return "BIGINT"
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
// e.g., ValueTypeText -> "LIST(VARCHAR)", ValueTypeInteger -> "LIST(DOUBLE)".
// Element types ride MapValueTypeToDuckDBType and therefore EAV storage width
// (#384), which is always right today because list attributes cannot bind a
// main column (their storage is one eav_data row per element, mirrored by
// eavElementCastExpr). If list column bindings ever appear, the
// storage-vs-declared width choice needs an explicit decision here.
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

// ToDuckDBParam converts a Go value to the form expected by DuckDB drivers for
// the given value type. The predicate normalizer binds this result, not
// parseDuckDBRawParam's, so each numeric arm's output type is load-bearing:
//   - uuid.UUID -> string
//   - time.Time -> int64 epoch-ms (BIGINT)
//   - smallint/integer -> an int64 passes through unchanged (#355); every other
//     accepted input widens to float64
//   - bigint/numeric -> a decimal string, never a number (see
//     toDuckDBDecimalParam): exact for int/int64/string inputs, while a float64
//     input is rendered by decimalString's shortest round-trip form, so the
//     DuckDB side recovers the identical float64 the PG side binds (#384 P1b:
//     the former %.15g dropped the 16th-17th significant digits and made
//     equality miss rows only on the DuckDB route)
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
		// Bind epoch-ms int64 to match the BIGINT storage convention (#200).
		switch t := value.(type) {
		case time.Time:
			return t.UTC().UnixMilli(), nil
		case *time.Time:
			timeValue, isNil := numutil.OptionalPointerValue(t)
			if isNil {
				return nil, nil
			}
			return timeValue.UTC().UnixMilli(), nil
		case int64:
			return t, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to epoch-ms BIGINT param", value)
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
		// An exact int64 from parseDuckDBRawParam binds through unchanged
		// (#355). Without this the float64 funnel below would undo the
		// int64-first parse one call later, at predicate_normalizer.go's
		// ToDuckDBParam hop. Everything else keeps the funnel: INTEGER and
		// SMALLINT ranges end at 2^31/2^15 and float64 is exact well past both.
		if exact, ok := value.(int64); ok {
			return exact, nil
		}
		numeric, isNil, err := toOptionalFloat64Param(value)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %T to numeric param", value)
		}
		if isNil {
			return nil, nil
		}
		return numeric, nil
	case forma.ValueTypeBigInt, forma.ValueTypeNumeric:
		return toDuckDBDecimalParam(value)
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

// toDuckDBDecimalParam converts a bigint/numeric value to its exact-precision
// string form — DuckDB accepts string representations for DECIMAL and HUGEINT
// column bindings.
func toDuckDBDecimalParam(value any) (any, error) {
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
}

// decimalString renders a float64 operand in its shortest round-trip decimal
// form: CAST(<it> AS DOUBLE) recovers the identical float64, matching the
// value pgx binds to the Postgres NUMERIC comparison (#384 P1b). %.15g is not
// enough — float64 needs up to 17 significant digits to round-trip.
func decimalString(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
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
