package sortkey

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// NormalizeCursor returns a copy of cursor whose every boundary holds a
// value of its column's export type, and whose every column carries that
// type (design.md §6.1, One boundary type per cursor column). The parquet
// leg binds each boundary as CAST(? AS <export type>), on which the
// normalized value is the identity, and the Go predicate compares the same
// value with the comparator. So both legs compare one boundary against keys
// of one type, and DuckDB never promotes the column to the bound value's
// type.
//
// Go does not reproduce DuckDB's coercions; a boundary of another type is
// refused rather than converted:
//
//   - BIGINT, INTEGER, SMALLINT: a Go integer within the type's range, or an
//     integral json.Number literal. Every float is refused, a whole one too.
//   - DOUBLE: a float, or an integer the nearest double holds exactly,
//     judged by its value whatever its representation (a uint64 past int64
//     or an integral json.Number past either included).
//   - VARCHAR: a string. A uuid attribute's boundary must parse as a uuid,
//     and takes its canonical lower-case text.
//   - BOOLEAN: a bool.
//   - UUID (row_id): a uuid.UUID, 16 bytes, or text that parses as a uuid.
//
// A column's export type comes from the system column it names, else from
// the request's sort key of that name (orders), else from the schema's
// metadata (cache), where the name resolves as DuckDB binds it: the
// attribute whose parquet column it reaches, ASCII case aside. A boundary taken from a returned row always passes:
// both legs return keys of the export type. The cursor must have passed
// model.KeysetCursor.ValidateShape. An inactive cursor is returned as is.
// Normalizing a normalized cursor returns an equal one.
func NormalizeCursor(cursor *model.KeysetCursor, orders []model.AttributeOrder, cache forma.SchemaAttributeCache) (*model.KeysetCursor, error) {
	if !cursor.IsActive() {
		return cursor, nil
	}
	if len(cursor.Values) != len(cursor.Columns) {
		return nil, fmt.Errorf("normalize keyset cursor: it carries %d column(s) but %d value(s)", len(cursor.Columns), len(cursor.Values))
	}
	out := &model.KeysetCursor{
		Columns: make([]model.KeysetColumn, len(cursor.Columns)),
		Values:  make([]any, len(cursor.Values)),
		Mode:    cursor.Mode,
	}
	for i, col := range cursor.Columns {
		spec, err := cursorColumnSpec(col.Attribute, orders, cache)
		if err != nil {
			return nil, fmt.Errorf("normalize keyset cursor: %w", err)
		}
		if col.ExportType != "" && col.ExportType != string(spec.typ) {
			return nil, fmt.Errorf("normalize keyset cursor: column %q carries export type %s, but the schema exports it as %s", col.Attribute, col.ExportType, spec.typ)
		}
		v, err := normalizeBoundary(spec, cursor.Values[i])
		if err != nil {
			return nil, fmt.Errorf("keyset cursor value %d (for column %q, exported as %s) is refused: %w", i+1, col.Attribute, spec.typ, err)
		}
		out.Columns[i] = col
		out.Columns[i].ExportType = string(spec.typ)
		out.Values[i] = v
	}
	return out, nil
}

// NormalizeBoundary normalizes one boundary value to export type t, by
// NormalizeCursor's rules for a column that is not a uuid attribute.
func NormalizeBoundary(t ExportType, v any) (any, error) {
	return normalizeBoundary(columnSpec{typ: t}, v)
}

func normalizeBoundary(spec columnSpec, raw any) (any, error) {
	v := deref(raw)
	switch spec.typ {
	case BigInt:
		return integerBoundary(v, math.MinInt64, math.MaxInt64)
	case Integer:
		return integerBoundary(v, math.MinInt32, math.MaxInt32)
	case SmallInt:
		return integerBoundary(v, math.MinInt16, math.MaxInt16)
	case Double:
		return doubleBoundary(v)
	case Varchar:
		return varcharBoundary(v, spec.uuidText)
	case Boolean:
		if b, ok := v.(bool); ok {
			return b, nil
		}
		return nil, fmt.Errorf("a %T boundary is not a bool", v)
	case UUID:
		return uuidBoundary(v)
	default:
		return nil, fmt.Errorf("export type %q has no boundary rule", spec.typ)
	}
}

// deref reads a boundary behind a non-nil pointer, as a cursor decoded into
// typed fields carries one.
func deref(v any) any {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr && !rv.IsNil() {
		return rv.Elem().Interface()
	}
	return v
}

// goInteger returns v as an int64 when v is a Go integer type or an
// integral json.Number literal. known is false for every other value;
// inRange is false for an unsigned value or an integral literal past int64.
func goInteger(v any) (n int64, known, inRange bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true, true
	case int8:
		return int64(x), true, true
	case int16:
		return int64(x), true, true
	case int32:
		return int64(x), true, true
	case int64:
		return x, true, true
	case uint8:
		return int64(x), true, true
	case uint16:
		return int64(x), true, true
	case uint32:
		return int64(x), true, true
	case uint:
		return uintInteger(uint64(x))
	case uint64:
		return uintInteger(x)
	case json.Number:
		if _, ok := integerLiteral(x); !ok {
			return 0, false, false
		}
		i, err := strconv.ParseInt(string(x), 10, 64)
		if err != nil {
			return 0, true, false // an integral literal past int64
		}
		return i, true, true
	default:
		return 0, false, false
	}
}

func uintInteger(x uint64) (int64, bool, bool) {
	if x > math.MaxInt64 {
		return 0, true, false
	}
	return int64(x), true, true
}

func integerBoundary(v any, lo, hi int64) (any, error) {
	n, known, inRange := goInteger(v)
	if !known {
		switch v.(type) {
		case float32, float64:
			return nil, fmt.Errorf("a float boundary (%v) cannot stand for an integer column: 9.6 would be rounded on one leg and promote the column on the other, and a float past 2^53 cannot name its neighbouring integers (#281); pass an integer", v)
		default:
			return nil, fmt.Errorf("a %T boundary is not an integer: pass a Go integer or an integral json.Number", v)
		}
	}
	if !inRange || n < lo || n > hi {
		return nil, fmt.Errorf("the boundary %v lies outside the column type's range [%d, %d]", v, lo, hi)
	}
	return n, nil
}

// doubleBoundary admits a float as it is, and an integer only when the
// nearest double is that integer. An integer is judged by its value, never
// by its representation: an int64, a uint64 past int64 and an integral
// json.Number literal past either are held to the same exactness rule, so
// 2^63 passes however it is spelled and 2^63 + 1 is refused however it is
// spelled. A json.Number with a fraction or an exponent is a float literal.
func doubleBoundary(v any) (any, error) {
	var n *big.Int
	switch x := v.(type) {
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case json.Number:
		i, ok := integerLiteral(x)
		if !ok {
			f, err := strconv.ParseFloat(string(x), 64)
			if err != nil {
				return nil, fmt.Errorf("the json.Number %q is not a double: %w", x, err)
			}
			return f, nil
		}
		n = i
	case uint:
		n = new(big.Int).SetUint64(uint64(x))
	case uint64:
		n = new(big.Int).SetUint64(x)
	default:
		i, known, _ := goInteger(v)
		if !known {
			return nil, fmt.Errorf("a %T boundary is not a number", v)
		}
		n = big.NewInt(i)
	}
	f, exact := exactDouble(n)
	if !exact {
		return nil, fmt.Errorf("the integer boundary %v has no exact double image: the nearest double would stand for a neighbouring integer (#281); pass the float the column holds", v)
	}
	return f, nil
}

// integerLiteral reads a json.Number written as a decimal integer, of any
// size. ok is false for a literal with a fraction or an exponent.
func integerLiteral(x json.Number) (*big.Int, bool) {
	return new(big.Int).SetString(string(x), 10)
}

// exactDouble returns n's nearest double, and whether that double is n
// itself.
func exactDouble(n *big.Int) (float64, bool) {
	f, acc := new(big.Float).SetInt(n).Float64()
	return f, acc == big.Exact
}

func varcharBoundary(v any, uuidText bool) (any, error) {
	s, ok := v.(string)
	if !ok {
		if !uuidText {
			return nil, fmt.Errorf("a %T boundary is not a string", v)
		}
		u, err := uuidBoundary(v)
		if err != nil {
			return nil, err
		}
		return u.(uuid.UUID).String(), nil
	}
	if !uuidText {
		return s, nil
	}
	u, err := uuid.Parse(s)
	if err != nil {
		return nil, fmt.Errorf("the boundary %q of a uuid attribute does not parse as a uuid: %w", s, err)
	}
	return u.String(), nil
}

func uuidBoundary(v any) (any, error) {
	switch x := v.(type) {
	case uuid.UUID:
		return x, nil
	case [16]byte:
		return uuid.UUID(x), nil
	case string:
		u, err := uuid.Parse(x)
		if err != nil {
			return nil, fmt.Errorf("the boundary %q does not parse as a uuid: %w", x, err)
		}
		return u, nil
	default:
		return nil, fmt.Errorf("a %T boundary is not a uuid: pass a uuid.UUID or its text", v)
	}
}
