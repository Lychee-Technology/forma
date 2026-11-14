package sortkey

import (
	"fmt"
	"math"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/numutil"
)

// DeriveAttribute derives an attribute's hot sort key from the value its
// storage holds, by the rules the CDC export applies to it
// (cdc.castMainValue for a column-bound attribute, cdc.castEAVValue for an
// EAV-only one), so the key compares as the row's next export will
// (design.md §6.1, the derivation table under Ordering parity).
//
// stored is the value as pgx scans it: for a column-bound attribute the
// entity_main column's value (int16, int32, int64, float64, string, or a
// uuid for a uuid column), for an EAV-only one value_numeric (a
// pgtype.Numeric, decoded as #785 stages it, or its float64) or value_text
// (a string). nil is SQL NULL and derives NULL, as does a value the
// export's TRY_CAST refuses. An input of a kind the attribute's storage
// cannot hold is an error.
func DeriveAttribute(meta forma.AttributeMetadata, stored any) (any, error) {
	spec, err := attributeSpec(meta)
	if err != nil {
		return nil, fmt.Errorf("derive sort key: %w", err)
	}
	stored, err = decodeNumeric(stored)
	if err != nil {
		return nil, fmt.Errorf("derive sort key of attribute %q: %w", meta.AttributeName, err)
	}
	if stored == nil {
		return nil, nil
	}
	key, err := deriveBySpec(meta, spec, stored)
	if err != nil {
		return nil, fmt.Errorf("derive %s sort key of attribute %q from a stored %T: %w", spec.typ, meta.AttributeName, stored, err)
	}
	return key, nil
}

func deriveBySpec(meta forma.AttributeMetadata, spec columnSpec, stored any) (any, error) {
	switch spec.typ {
	case Boolean:
		return deriveBool(meta, stored)
	case BigInt:
		if s, ok := stored.(string); ok && isRFC3339Bound(meta) {
			return RFC3339EpochMs(s), nil
		}
		return tryCastInteger(stored, math.MinInt64, math.MaxInt64)
	case Integer:
		return tryCastInteger(stored, math.MinInt32, math.MaxInt32)
	case SmallInt:
		return tryCastInteger(stored, math.MinInt16, math.MaxInt16)
	case Double:
		return tryCastDouble(stored)
	case Varchar:
		return deriveVarchar(meta, spec, stored)
	default:
		return nil, fmt.Errorf("no derivation for export type %s", spec.typ)
	}
}

// DeriveSystem derives the hot sort key of a system column of the visible
// CTE from its stored value: row_id from entity_main's uuid, created_at
// from ltbase_created_at, ver_ts from the newest change_log entry's
// changed_at. deleted_ts derives the constant 0 whatever is stored: the
// hot leg reads live rows only, and the export writes a live row's
// deleted_at as COALESCE(cl.deleted_at, 0) (design.md §6.1, deleted_ts
// reads 0).
func DeriveSystem(column string, stored any) (any, error) {
	_, key, ok := systemColumn(column)
	if !ok {
		return nil, fmt.Errorf("derive sort key: %q is not a system column (row_id, created_at, ver_ts, deleted_ts)", column)
	}
	if key == ColumnDeletedTS {
		return int64(0), nil
	}
	if u, ok := stored.(pgtype.UUID); stored == nil || (ok && !u.Valid) {
		return nil, nil
	}
	if key == ColumnRowID {
		u, ok := asUUID(stored)
		if !ok {
			return nil, fmt.Errorf("derive sort key of row_id: a stored %T is not a uuid", stored)
		}
		return u, nil
	}
	n, known, inRange := goInteger(stored)
	if !known || !inRange {
		return nil, fmt.Errorf("derive sort key of %s: a stored %T is not a bigint", key, stored)
	}
	return n, nil
}

// decodeNumeric reads a value_numeric NUMERIC as #785's stage decode does.
func decodeNumeric(stored any) (any, error) {
	n, ok := stored.(pgtype.Numeric)
	if !ok {
		return stored, nil
	}
	if !n.Valid {
		return nil, nil
	}
	f, err := numutil.NumericToDouble(n)
	if err != nil {
		return nil, fmt.Errorf("decode value_numeric: %w", err)
	}
	return f, nil
}

func isRFC3339Bound(meta forma.AttributeMetadata) bool {
	return meta.ColumnBinding != nil && meta.ColumnBinding.Encoding == forma.MainColumnEncodingRFC3339
}

// deriveBool applies the export's truthiness: sqlgen.BoolTruthiness
// (`> 0.5`, in DuckDB's DOUBLE order, so NaN is true) on value_numeric and
// on a bool_smallint column, `= '1'` on a bool_text column.
func deriveBool(meta forma.AttributeMetadata, stored any) (any, error) {
	if meta.ColumnBinding != nil && meta.ColumnBinding.Encoding == forma.MainColumnEncodingBoolText {
		s, ok := stored.(string)
		if !ok {
			return nil, fmt.Errorf("a bool_text column holds text")
		}
		return s == "1", nil
	}
	if b, ok := stored.(bool); ok {
		return b, nil
	}
	if f, ok := stored.(float64); ok {
		return CompareDouble(f, 0.5) > 0, nil
	}
	n, known, _ := goInteger(stored)
	if !known {
		return nil, fmt.Errorf("a boolean's storage holds a number")
	}
	return n > 0, nil
}

// tryCastInteger is DuckDB's TRY_CAST to an integer type of range [lo, hi]:
// an integer past the range is NULL, and a double is rounded half to even
// first, with NaN and ±Infinity NULL (TestDeriveLockstepWithExport pins it).
func tryCastInteger(stored any, lo, hi int64) (any, error) {
	if f, ok := stored.(float64); ok {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, nil
		}
		r := math.RoundToEven(f)
		// float64(hi)+1 is exact for the 16- and 32-bit ranges and rounds to
		// 2^63 for BIGINT's, the first double past it.
		if r < float64(lo) || r >= float64(hi)+1 {
			return nil, nil
		}
		return int64(r), nil
	}
	n, known, inRange := goInteger(stored)
	if !known {
		return nil, fmt.Errorf("an integer's storage holds a number")
	}
	if !inRange || n < lo || n > hi {
		return nil, nil
	}
	return n, nil
}

// tryCastDouble reads a number as DOUBLE: a double as is, NaN and ±Infinity
// kept, an integer as its nearest double.
func tryCastDouble(stored any) (any, error) {
	if f, ok := stored.(float64); ok {
		return f, nil
	}
	n, known, _ := goInteger(stored)
	if !known {
		return nil, fmt.Errorf("a numeric's storage holds a number")
	}
	return float64(n), nil
}

// deriveVarchar keeps text as stored. A column-bound uuid exports through
// TRY_CAST(col AS UUID), so it takes its canonical lower-case text; an
// EAV-only uuid exports value_text as stored.
func deriveVarchar(meta forma.AttributeMetadata, spec columnSpec, stored any) (any, error) {
	if s, ok := stored.(string); ok {
		if spec.uuidText && meta.ColumnBinding != nil {
			u, err := uuid.Parse(s)
			if err != nil {
				return nil, nil
			}
			return u.String(), nil
		}
		return s, nil
	}
	if !spec.uuidText {
		return nil, fmt.Errorf("a text attribute's storage holds text")
	}
	u, ok := asUUID(stored)
	if !ok {
		return nil, fmt.Errorf("a uuid attribute's storage holds a uuid or its text")
	}
	return u.String(), nil
}

func asUUID(stored any) (uuid.UUID, bool) {
	switch v := stored.(type) {
	case uuid.UUID:
		return v, true
	case [16]byte:
		return uuid.UUID(v), true
	case pgtype.UUID:
		return uuid.UUID(v.Bytes), v.Valid
	case string:
		u, err := uuid.Parse(v)
		return u, err == nil
	default:
		return uuid.UUID{}, false
	}
}
