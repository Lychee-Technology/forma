package sqlgen

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/conditionexpr"
	"github.com/lychee-technology/forma/internal/iso8601"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/numutil"
)

// SetOperand is the PostgreSQL right-hand side of an in / not_in leaf
// (#646): the whole list binds as ONE array placeholder, so the clause text
// never depends on the list's length or contents. Both plan caches rely on
// that: the OLTP render key covers the clause text and the arg count, and
// the federated key covers only (attribute, operator).
type SetOperand struct {
	// SQLOp is conditionexpr.SQLOpAny (in) or conditionexpr.SQLOpAll (not_in).
	SQLOp string
	// Values is the exactly-typed slice the array binds: []string, []int64,
	// []float64 or []bool.
	Values any
	// ArrayCast is the PostgreSQL array type the bind is cast to; empty lets
	// the compared column type the bind (the eav_data value columns).
	ArrayCast string
	// BoolEncoding is set for a bool attribute bound to an entity_main
	// column: the comparison then runs on the column's read-contract truth
	// value (BoolMainTruth), not its raw image.
	BoolEncoding forma.MainColumnEncoding
}

// RenderPg spells the comparison of lhs against the array placeholder ph.
func (s SetOperand) RenderPg(lhs, ph string) string {
	rhs := ph
	if s.ArrayCast != "" {
		rhs = ph + "::" + s.ArrayCast
	}
	return renderSetComparison(s.SQLOp, lhs, rhs)
}

// mainLHS returns the left-hand side over an m-qualified entity_main column.
func (s SetOperand) mainLHS(column string) string {
	if s.BoolEncoding != "" {
		return BoolMainTruth(column, s.BoolEncoding)
	}
	return column
}

// DuckSetOperand is the DuckDB form of an in / not_in leaf. The list binds
// as one positional parameter cast to ListType[]; duckdb-go needs the Go
// slice's element type to match that cast exactly, so the payload's Param
// holds the slice built by duckSetValues.
type DuckSetOperand struct {
	SQLOp    string
	ListType string
	// LHS is the attribute column, cast to VARCHAR for a uuid attribute: a
	// parquet tier may carry the column as UUID, and DuckDB does not coerce
	// a UUID against a VARCHAR[] the way it does against a VARCHAR scalar.
	LHS string
}

// Render spells the DuckDB comparison with its single "?" placeholder.
func (s DuckSetOperand) Render() string {
	return renderSetComparison(s.SQLOp, s.LHS, "CAST(? AS "+s.ListType+"[])")
}

// renderSetComparison is the one spelling of both set operators on both
// dialects. not_in carries an explicit IS NOT NULL: "<> ALL" over an empty
// array is true even for NULL, and an absent value never matches not_in.
func renderSetComparison(sqlOp, lhs, rhs string) string {
	if sqlOp == conditionexpr.SQLOpAll {
		return "(" + lhs + " IS NOT NULL AND " + lhs + " <> ALL(" + rhs + "))"
	}
	return lhs + " = ANY(" + rhs + ")"
}

// coerceListElements converts the literal spellings of an in / not_in list
// into canonical Go values under the equals rules of the attribute's value
// type: string (text, canonical lowercase uuid), int64 or float64 (numeric
// family), int64 epoch milliseconds (date, datetime) or bool. A failing
// element is invalid input naming the attribute, the value type and the
// index.
func coerceListElements(attr, op string, meta forma.AttributeMetadata, raw []string) ([]any, error) {
	if meta.ValueType == forma.ValueTypeList {
		return nil, forma.InvalidInputf(
			"operator '%s' is not supported on list attribute '%s': it compares a scalar value against the list operand",
			op, attr)
	}
	out := make([]any, 0, len(raw))
	for i, literal := range raw {
		v, err := coerceListElement(attr, meta, literal)
		if err != nil {
			return nil, forma.WrapPublicf(err, "element at index %d of the '%s' list for %s attribute '%s'",
				i, op, meta.ValueType, attr)
		}
		out = append(out, v)
	}
	return out, nil
}

func coerceListElement(attr string, meta forma.AttributeMetadata, literal string) (any, error) {
	switch meta.ValueType {
	case forma.ValueTypeText:
		return literal, nil
	case forma.ValueTypeUUID:
		// Canonical form, as the write path stores it in value_text, so the
		// text and uuid storage forms answer one list identically.
		u, err := uuid.Parse(literal)
		if err != nil {
			return nil, forma.InvalidInputf("%q is not a UUID", literal)
		}
		return u.String(), nil
	case forma.ValueTypeNumeric, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeSmallInt:
		v, ok, err := numericOperand(attr, literal, meta.ValueType)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, forma.InvalidInputf("%q is not a number", literal)
		}
		return v, nil
	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		return coerceDateListElement(attr, meta, literal)
	case forma.ValueTypeBool:
		b, ok := parseBoolOperand(literal)
		if !ok {
			return nil, forma.InvalidInputf("%q is not a boolean", literal)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("unsupported value_type '%s' for attribute '%s'", meta.ValueType, attr)
	}
}

func coerceDateListElement(attr string, meta forma.AttributeMetadata, literal string) (any, error) {
	if conditionexpr.IsServerTime(literal) {
		return nil, forma.InvalidInputf("the $now operand is not accepted inside a list; compare against it with equals:$now")
	}
	t, err := conditionexpr.ParseRFC3339OrUnixMs(literal)
	if err != nil {
		return nil, err
	}
	ms := t.UnixMilli()
	if meta.ColumnBinding != nil && meta.ColumnBinding.Encoding == forma.MainColumnEncodingISO8601 {
		if _, err := iso8601FilterImage(attr, literal, ms, meta); err != nil {
			return nil, err
		}
	}
	return ms, nil
}

// pgEavSetValues builds the array bound against the eav_data value column.
// bigint keeps exact int64 (its storage is BIGINT-backed on every tier);
// the DOUBLE-width numeric classes take the float64 image their storage
// holds (NarrowEAVNumericOperand). That includes EAV-only integer and
// smallint (#384), so a fractional element is kept there and matches
// fractional history exactly as an equals operand does.
func pgEavSetValues(attr string, vt forma.ValueType, elems []any) (any, error) {
	switch vt {
	case forma.ValueTypeText, forma.ValueTypeUUID:
		return exactElems[string](attr, elems)
	case forma.ValueTypeBigInt, forma.ValueTypeDate, forma.ValueTypeDateTime:
		return int64Elems(attr, elems)
	case forma.ValueTypeNumeric, forma.ValueTypeInteger, forma.ValueTypeSmallInt:
		return float64Elems(attr, elems)
	case forma.ValueTypeBool:
		return exactElems[bool](attr, elems)
	default:
		return nil, fmt.Errorf("unsupported value_type '%s' for attribute '%s'", vt, attr)
	}
}

// pgMainSetOperand builds the entity_main form. The array type follows the
// physical column (a validated descriptor), so the comparison stays on the
// column's own btree operator family.
func pgMainSetOperand(sqlOp, colName string, meta forma.AttributeMetadata, elems []any) (SetOperand, error) {
	desc := model.GetMainColumnDescriptor(colName)
	if desc == nil {
		return SetOperand{}, fmt.Errorf("unknown main table column: %s", colName)
	}
	attr := colName
	set := SetOperand{SQLOp: sqlOp}
	var err error
	switch {
	case meta.ValueType == forma.ValueTypeBool:
		set.BoolEncoding = forma.MainColumnEncodingBoolInt
		if meta.ColumnBinding != nil && meta.ColumnBinding.Encoding == forma.MainColumnEncodingBoolText {
			set.BoolEncoding = forma.MainColumnEncodingBoolText
		}
		set.ArrayCast = "boolean[]"
		set.Values, err = exactElems[bool](attr, elems)
	case desc.Kind == model.ColumnKindText && (meta.ValueType == forma.ValueTypeDate || meta.ValueType == forma.ValueTypeDateTime):
		set.ArrayCast = "text[]"
		set.Values, err = iso8601Elems(attr, elems)
	case desc.Kind == model.ColumnKindText:
		set.ArrayCast = "text[]"
		set.Values, err = exactElems[string](attr, elems)
	case desc.Kind == model.ColumnKindUUID:
		set.ArrayCast = "uuid[]"
		set.Values, err = exactElems[string](attr, elems)
	case desc.Kind == model.ColumnKindDouble:
		set.ArrayCast = "float8[]"
		set.Values, err = float64Elems(attr, elems)
	default:
		set.ArrayCast = "bigint[]"
		set.Values, err = int64Elems(attr, elems)
	}
	return set, err
}

// duckSetValues builds the operand of the DuckDB form, typed by the
// attribute's DuckDB operand type (MapValueTypeToDuckDBType).
func duckSetValues(sqlOp, column string, vt forma.ValueType, elems []any) (DuckSetOperand, any, error) {
	listType := MapValueTypeToDuckDBType(vt)
	set := DuckSetOperand{SQLOp: sqlOp, ListType: listType, LHS: column}
	var values any
	var err error
	switch listType {
	case "DOUBLE":
		values, err = float64Elems(column, elems)
	case "BIGINT":
		values, err = int64Elems(column, elems)
	case "BOOLEAN":
		values, err = exactElems[bool](column, elems)
	default:
		if vt == forma.ValueTypeUUID {
			set.LHS = "CAST(" + column + " AS VARCHAR)"
		}
		values, err = exactElems[string](column, elems)
	}
	return set, values, err
}

func exactElems[T any](attr string, elems []any) ([]T, error) {
	out := make([]T, 0, len(elems))
	for i, e := range elems {
		v, ok := e.(T)
		if !ok {
			var zero T
			return nil, fmt.Errorf("list element %d for attribute '%s' is %T, expected %T", i, attr, e, zero)
		}
		out = append(out, v)
	}
	return out, nil
}

// int64Elems builds an integer-typed array, for whole-number storage only:
// bigint, date and datetime values, and integer-family entity_main columns.
// An element that is not a whole number in int64 range is dropped, not
// refused: it is a valid literal under the equals rules, and it can equal no
// value that storage holds, so dropping it changes neither operator's answer
// (#646 D4). A float64 element whose image is whole is kept as that integer,
// the value an equals operand compares (wholeNumberComparison).
func int64Elems(attr string, elems []any) ([]int64, error) {
	out := make([]int64, 0, len(elems))
	for i, e := range elems {
		switch v := e.(type) {
		case int64:
			out = append(out, v)
		case float64:
			if n, verdict := numutil.ClassifyInt64(v); verdict == numutil.IntegralityExact {
				out = append(out, n)
			}
		default:
			return nil, fmt.Errorf("list element %d for attribute '%s' is %T, expected int64", i, attr, e)
		}
	}
	return out, nil
}

func float64Elems(attr string, elems []any) ([]float64, error) {
	out := make([]float64, 0, len(elems))
	for i, e := range elems {
		switch v := e.(type) {
		case int64:
			out = append(out, float64(v))
		case float64:
			out = append(out, v)
		default:
			return nil, fmt.Errorf("list element %d for attribute '%s' is %T, expected a number", i, attr, e)
		}
	}
	return out, nil
}

// iso8601Elems renders epoch-millisecond elements as the canonical text
// image an iso8601-bound column stores. coerceDateListElement already
// refused every element the image cannot hold.
func iso8601Elems(attr string, elems []any) ([]string, error) {
	ms, err := exactElems[int64](attr, elems)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(ms))
	for i, v := range ms {
		image, rule := iso8601.Image(v)
		if rule != "" {
			return nil, fmt.Errorf("list element %d for attribute '%s' has no iso8601 image: %s", i, attr, rule)
		}
		out = append(out, image)
	}
	return out, nil
}
