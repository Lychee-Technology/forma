package sqlgen

import (
	"fmt"
	"math"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/numutil"
)

// numericOperand parses the operand of a numeric-family leaf. It is the one
// parse every binder applies (ConvertPgMainValue, parsePgEavValue,
// parseDuckDBRawParam, coerceListElement), so the binders cannot disagree on
// what a literal denotes. A literal denoting an integer in int64 range is an
// exact int64 in any spelling (#357); any other number is its float64 image.
//
// ok is false when the literal is not a finite number; each binder words
// that refusal itself, since its message is published. NaN and ±Inf are not
// values the numeric types hold, and they compared differently per route:
// PostgreSQL orders NaN above every number, so lt:NaN matched every row on
// the EAV route, while the entity_main route failed to encode it after the
// statement had started. The bigint range rule (#502) is judged first, so a
// bigint operand keeps its out-of-range message.
func numericOperand(attr, literal string, vt forma.ValueType) (v any, ok bool, err error) {
	switch parsed := numutil.TryParseNumber(literal).(type) {
	case int64:
		return parsed, true, nil
	case float64:
		if err := checkBigIntOperandRange(attr, literal, vt, parsed); err != nil {
			return nil, false, err
		}
		if math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			return nil, false, nil
		}
		return parsed, true, nil
	default:
		return nil, false, nil
	}
}

// twoTo63 is the first float64 past MaxInt64 (float64(MaxInt64) rounds to it).
const twoTo63 = 9223372036854775808.0

// wholeNumberComparison rewrites `col <sqlOp> operand`, where col holds only
// whole numbers (an int2, int4 or int8 column), into the equivalent
// `col <op> n` with n an int64. An operand that is an int64, or a float64
// image that is a whole number in int64 range, keeps sqlOp. Any other
// operand lies strictly between two integers or outside int64, so it equals
// no stored value and orders like its nearest whole bound:
//
//	= x            no value
//	!= x           every present value
//	< x, <= x      <= floor(x)
//	> x, >= x      >= ceil(x)
//
// A bound past int64 admits every value on its side, or none. "No value" is
// `< MinInt64` and "every present value" `<= MaxInt64`, so the result is
// always one comparison against one int64 bind, and NULL stays NULL.
//
// Binding the operand unconverted is what this replaces: PostgreSQL types the
// placeholder from the column, so pgx truncated 7.9 to 7 against an int4
// column (equals:7.9 matched 7) and refused 2147483648 after the statement
// had started. The operator a fractional or out-of-range operand renders
// differs from the one an int64 renders, so the federated shape hash keys on
// that class (queryplan numericOperandClass).
func wholeNumberComparison(sqlOp string, operand any) (string, int64, error) {
	if !isNumericComparison(sqlOp) {
		return "", 0, fmt.Errorf("operator %s is not a numeric comparison", sqlOp)
	}
	var f float64
	switch v := operand.(type) {
	case int64:
		return sqlOp, v, nil
	case float64:
		f = v
	default:
		return "", 0, fmt.Errorf("whole-number comparison operand %v is %T, expected int64 or float64", operand, operand)
	}
	n, verdict := numutil.ClassifyInt64(f)
	switch verdict {
	case numutil.IntegralityExact:
		return sqlOp, n, nil
	case numutil.IntegralityUnknown:
		return "", 0, fmt.Errorf("whole-number comparison operand %v is not a finite number", f)
	}
	switch sqlOp {
	case "=":
		return "<", math.MinInt64, nil
	case "!=":
		return "<=", math.MaxInt64, nil
	case "<", "<=":
		return atMostWhole(math.Floor(f))
	default: // ">", ">="
		return atLeastWhole(math.Ceil(f))
	}
}

func atMostWhole(bound float64) (string, int64, error) {
	switch {
	case bound >= twoTo63:
		return "<=", math.MaxInt64, nil
	case bound < -twoTo63:
		return "<", math.MinInt64, nil
	}
	return "<=", int64(bound), nil
}

func atLeastWhole(bound float64) (string, int64, error) {
	switch {
	case bound < -twoTo63:
		return "<=", math.MaxInt64, nil
	case bound >= twoTo63:
		return "<", math.MinInt64, nil
	}
	return ">=", int64(bound), nil
}

// isNumericComparison reports whether sqlOp is one wholeNumberComparison
// rewrites.
func isNumericComparison(sqlOp string) bool {
	switch sqlOp {
	case "=", "!=", "<", "<=", ">", ">=":
		return true
	}
	return false
}

func isNumericValueType(vt forma.ValueType) bool {
	switch vt {
	case forma.ValueTypeNumeric, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeSmallInt:
		return true
	}
	return false
}

// hybridMainComparison is the main-branch payload of a plain comparison
// leaf. A numeric operand against a whole-number entity_main column
// compares as the int64 bound wholeNumberComparison derives, bound as
// bigint: no column width can then refuse it, and int2/int4/int8 against
// bigint stays on the column's btree operator family.
func hybridMainComparison(colName string, vt forma.ValueType, sqlOp string, value any) HybridLeafPayload {
	p := HybridLeafPayload{IsMain: true, MainColumn: colName, MainSQLOp: sqlOp, MainValue: value}
	if !isNumericValueType(vt) || !isNumericComparison(sqlOp) || !isWholeNumberColumn(colName) {
		return p
	}
	op, n, err := wholeNumberComparison(sqlOp, value)
	if err != nil {
		return HybridLeafPayload{IsMain: true, Err: fmt.Errorf("compare attribute column %s: %w", colName, err)}
	}
	p.MainSQLOp, p.MainValue, p.MainCast = op, n, "bigint"
	return p
}

func isWholeNumberColumn(colName string) bool {
	desc := model.GetMainColumnDescriptor(colName)
	if desc == nil {
		return false
	}
	switch desc.Kind {
	case model.ColumnKindSmallint, model.ColumnKindInteger, model.ColumnKindBigint:
		return true
	}
	return false
}

// duckWholeNumberComparison applies wholeNumberComparison to a DuckDB leaf
// on a bigint attribute, the one numeric class DuckDB compares at BIGINT:
// CAST(? AS BIGINT) rounds a fractional operand, so equals:7.5 matched 8.
// The integer, smallint and numeric classes compare at DOUBLE there
// (MapValueTypeToDuckDBType), where every finite operand compares exactly.
func duckWholeNumberComparison(attr string, vt forma.ValueType, sqlOp string, param any) (string, any, error) {
	if vt != forma.ValueTypeBigInt || !isNumericComparison(sqlOp) {
		return sqlOp, param, nil
	}
	op, n, err := wholeNumberComparison(sqlOp, param)
	if err != nil {
		return "", nil, fmt.Errorf("compare bigint attribute '%s': %w", attr, err)
	}
	return op, n, nil
}
