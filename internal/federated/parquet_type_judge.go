package federated

import (
	"fmt"
	"math"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// The #732 type judgement: whether one live object's column type reads
// losslessly under the attribute's CURRENT declaration. The read path reads a
// column as union_by_name unifies it across the scan set, then casts it to the
// declared type (the #371 pin and sqlgen.duckDBAttrCast), so a type the
// declaration did not write is converted on every query. The judgement sorts
// each (object type, declaration) pair into one of four classes:
//
//   - lossless: every value the object type can hold reads unchanged, so no
//     scan is needed — the type the current declaration exports, or a widening
//     such as SMALLINT under integer;
//   - needs proof: some values read unchanged and some do not (DOUBLE under
//     bigint), so the object's values decide; violation is the DuckDB
//     predicate a scan looks for;
//   - unprovable: the same, for a pair this guard has no value proof for (a
//     list element); refused, since D2 refuses what it cannot verify;
//   - incompatible: a different type family (BOOLEAN under bigint, a scalar
//     under a list), refused without a scan.
type judgementKind int

const (
	judgeLossless judgementKind = iota
	judgeNeedsProof
	judgeUnprovable
	judgeIncompatible
)

type typeJudgement struct {
	kind judgementKind
	// violation is true for a non-NULL value the declaration cannot read
	// unchanged; demand says what such a value fails to be. Both are set only
	// for judgeNeedsProof.
	violation string
	demand    string
}

// intRange is an inclusive whole-number domain.
type intRange struct{ lo, hi int64 }

var (
	smallintRange = intRange{math.MinInt16, math.MaxInt16}
	integerRange  = intRange{math.MinInt32, math.MaxInt32}
	bigintRange   = intRange{math.MinInt64, math.MaxInt64}
	// exactDoubleRange is where a float64 names every integer exactly: the
	// domain of a bigint bound to a double_* column (#590), and of a BIGINT
	// value that union_by_name widens to DOUBLE.
	exactDoubleRange = intRange{-1 << 53, 1 << 53}
)

func (r intRange) within(o intRange) bool { return r.lo >= o.lo && r.hi <= o.hi }

func (r intRange) intersect(o intRange) intRange {
	return intRange{max(r.lo, o.lo), min(r.hi, o.hi)}
}

// physicalRange is the domain of a parquet integer type.
func physicalRange(fileType string) (intRange, bool) {
	switch fileType {
	case "SMALLINT":
		return smallintRange, true
	case "INTEGER":
		return integerRange, true
	case "BIGINT":
		return bigintRange, true
	default:
		return intRange{}, false
	}
}

// declaredRange is the whole-number domain a smallint/integer/bigint
// declaration admits in its storage. A bigint bound to a double_* column keeps
// only its float64 image, so it is exact only within ±2^53 (#590).
func declaredRange(meta forma.AttributeMetadata) intRange {
	switch meta.ValueType {
	case forma.ValueTypeSmallInt:
		return smallintRange
	case forma.ValueTypeInteger:
		return integerRange
	}
	if meta.ColumnBinding != nil && meta.ColumnBinding.ColumnType() == forma.MainColumnTypeDouble {
		return exactDoubleRange
	}
	return bigintRange
}

// judgeColumn judges an object whose column (col, already quoted) has
// fileType, against meta. viaDouble reports that another object in the scan
// set holds the column as DOUBLE (DOUBLE[] for a list): union_by_name then
// reads a BIGINT object's values through DOUBLE, which is exact only within
// ±2^53, so a BIGINT object needs that proof even under a bigint declaration.
func judgeColumn(col, fileType string, meta forma.AttributeMetadata, viaDouble bool) typeJudgement {
	fileType = strings.ToUpper(fileType)
	want := sqlgen.DuckDBNullScanType(meta)
	if meta.ValueType != forma.ValueTypeList {
		if strings.HasSuffix(fileType, "[]") {
			return typeJudgement{kind: judgeIncompatible}
		}
		return judgeScalar(col, fileType, want, meta, viaDouble)
	}
	if !strings.HasSuffix(fileType, "[]") {
		return typeJudgement{kind: judgeIncompatible}
	}
	// Lists are EAV-only, so an element carries EAV storage width.
	elem := forma.AttributeMetadata{ValueType: meta.EffectiveItemsType()}
	j := judgeScalar(col, strings.TrimSuffix(fileType, "[]"), strings.TrimSuffix(want, "[]"), elem, viaDouble)
	if j.kind == judgeNeedsProof {
		return typeJudgement{kind: judgeUnprovable}
	}
	return j
}

func judgeScalar(col, fileType, want string, meta forma.AttributeMetadata, viaDouble bool) typeJudgement {
	hopped := viaDouble && fileType == "BIGINT"
	if sqlgen.ScanTypesCompatible(fileType, want) && !hopped {
		return typeJudgement{kind: judgeLossless}
	}
	switch meta.ValueType {
	case forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt:
		r := declaredRange(meta)
		if hopped {
			r = r.intersect(exactDoubleRange)
		}
		return judgeWhole(col, fileType, r)
	case forma.ValueTypeNumeric:
		return judgeDouble(col, fileType)
	case forma.ValueTypeBool:
		if fileType == "VARCHAR" {
			return roundTrip(col, "BOOLEAN", "a boolean")
		}
	}
	// text, uuid, date and datetime read only their own export types: a
	// numeric under text, or anything but epoch-ms BIGINT under a date, is a
	// different family.
	return typeJudgement{kind: judgeIncompatible}
}

// judgeWhole judges fileType against a whole-number domain r.
func judgeWhole(col, fileType string, r intRange) typeJudgement {
	demand := fmt.Sprintf("a whole number in [%d, %d]", r.lo, r.hi)
	if pr, ok := physicalRange(fileType); ok {
		if pr.within(r) {
			return typeJudgement{kind: judgeLossless}
		}
		return typeJudgement{kind: judgeNeedsProof, violation: outsideRange(col, r, false), demand: demand}
	}
	switch fileType {
	case "DOUBLE":
		return typeJudgement{
			kind:      judgeNeedsProof,
			violation: fmt.Sprintf("NOT isfinite(%[1]s) OR %[1]s <> trunc(%[1]s) OR %[2]s", col, outsideRange(col, r, true)),
			demand:    demand,
		}
	case "VARCHAR":
		j := roundTrip(col, "BIGINT", demand)
		j.violation += " OR " + outsideRange(fmt.Sprintf("TRY_CAST(%s AS BIGINT)", col), r, false)
		return j
	}
	return typeJudgement{kind: judgeIncompatible}
}

// judgeDouble judges fileType against numeric (DOUBLE). A SMALLINT or
// INTEGER widens exactly. A BIGINT reads as lossless too, rounding beyond
// ±2^53: D2 allows bigint to numeric with that rounding (#733), and the
// OLTP route reads a bigint_* column under numeric through the same
// float64 conversion. Text must also parse to a finite number: 'nan' and
// 'inf' read back unchanged, but the numeric write path refuses non-finite
// values (transform.finiteForEAV).
func judgeDouble(col, fileType string) typeJudgement {
	switch fileType {
	case "SMALLINT", "INTEGER", "BIGINT", "DOUBLE":
		return typeJudgement{kind: judgeLossless}
	case "VARCHAR":
		j := roundTrip(col, "DOUBLE", "a finite number")
		j.violation += fmt.Sprintf(" OR NOT isfinite(TRY_CAST(%s AS DOUBLE))", col)
		return j
	}
	return typeJudgement{kind: judgeIncompatible}
}

// roundTrip demands that a text value parse as castType and print back as the
// same text. TRY_CAST alone is not enough: it rounds '9.5' to an INTEGER 10.
func roundTrip(col, castType, demand string) typeJudgement {
	return typeJudgement{
		kind:      judgeNeedsProof,
		violation: fmt.Sprintf("TRY_CAST(%[1]s AS %[2]s) IS NULL OR CAST(TRY_CAST(%[1]s AS %[2]s) AS VARCHAR) <> %[1]s", col, castType),
		demand:    demand + " whose text reads back unchanged",
	}
}

// outsideRange renders "expr is outside r". Over a DOUBLE the int64 bounds
// are not exact float64 values, so the full int64 domain is the half-open
// [-2^63, 2^63); every narrower bound here is an exact float64.
func outsideRange(expr string, r intRange, overDouble bool) string {
	if overDouble && r == bigintRange {
		return fmt.Sprintf("%[1]s < -9223372036854775808.0 OR %[1]s >= 9223372036854775808.0", expr)
	}
	return fmt.Sprintf("%[1]s < %[2]d OR %[1]s > %[3]d", expr, r.lo, r.hi)
}
