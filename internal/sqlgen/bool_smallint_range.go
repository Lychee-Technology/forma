package sqlgen

import (
	"math"

	"github.com/lychee-technology/forma"
)

// BoolSmallintRange is the entity_main pushdown form of a bool_smallint
// equality leaf (#565): `<col> BETWEEN lo AND hi`, with the bounds — not
// the operator — carrying the verdict. Both pushdown routes (the pg-main
// clause the DuckDB hot leg embeds in its postgres_scan, and the hybrid
// builder's main branch on the Postgres route) render it, so a stored
// image outside the 0/1 write contract answers a filter the way every
// reader already reads it.
//
// Why a range and not `= 1`/`= 0`: the raw compare read a stored 2 as
// neither true nor false while BoolTruthiness (`> 0.5`) read it as true on
// the PG-EAV, DuckDB and CDC legs and the Go read funnel, so the same row
// was returned as true by an unfiltered read and skipped by `equals:true`.
//
// Why BETWEEN with fixed integer bounds and not `> 0.5` / `<= 0.5`: the
// federated plan cache keys on the query shape without operands
// (queryplan.HashFederatedQueryShape) and reuses the cached PgMainClause
// text on a hit, so the clause text may not depend on whether the operand
// is true or false — only the binds may. Integer bounds also keep the
// predicate on the integer_ops btree family; a `0.5` literal against an
// int2 column casts the column and leaves the index unusable. On a SMALLINT
// domain `BETWEEN 1 AND 32767` is exactly `> 0.5` and `BETWEEN -32768 AND
// 0` is exactly `<= 0.5`, so the verdict is the BoolTruthiness verdict
// (TestBoolSmallintRange_MatchesBoolTruthiness executes both). NULL stays
// NULL on both spellings.
//
// not_equals:X is equals:!X on a two-valued domain, so the four
// operator/operand combinations collapse onto the two ranges below and the
// clause text never changes.
type BoolSmallintRange struct {
	Lo, Hi int64
}

// boolSmallintRangeFor returns the range that selects rows whose stored
// image reads as truthy (true) or falsy (false).
func boolSmallintRangeFor(truthy bool) BoolSmallintRange {
	if truthy {
		return BoolSmallintRange{Lo: 1, Hi: math.MaxInt16}
	}
	return BoolSmallintRange{Lo: math.MinInt16, Hi: 0}
}

// Render spells the predicate over an already-qualified column and the two
// placeholders the emitter allocated, in bind order (lo then hi). It is the
// one spelling both pushdown emitters use.
func (r BoolSmallintRange) Render(column, lo, hi string) string {
	return column + " BETWEEN " + lo + " AND " + hi
}

// Args returns the binds in placeholder order.
func (r BoolSmallintRange) Args() []any {
	return []any{r.Lo, r.Hi}
}

// pgMainBoolRange classifies an entity_main leaf: ok is true only for a
// bool_smallint-bound attribute under `=` or `!=`, in which case the
// returned range replaces the value bind. Any other leaf (bool_text, other
// value types, other operators) keeps the ConvertPgMainValue bind. The
// operand parse is the engine-shared parseBoolOperand rule, so an invalid
// spelling is the same user-facing rejection ConvertPgMainValue raises.
func pgMainBoolRange(meta forma.AttributeMetadata, sqlOp, valStr, attr string) (BoolSmallintRange, bool, error) {
	if meta.ValueType != forma.ValueTypeBool || meta.ColumnBinding == nil ||
		meta.ColumnBinding.Encoding != forma.MainColumnEncodingBoolInt {
		return BoolSmallintRange{}, false, nil
	}
	if sqlOp != "=" && sqlOp != "!=" {
		return BoolSmallintRange{}, false, nil
	}
	parsed, ok := parseBoolOperand(valStr)
	if !ok {
		return BoolSmallintRange{}, false, forma.InvalidInputf("invalid boolean value for '%s': %s", attr, valStr)
	}
	return boolSmallintRangeFor(parsed == (sqlOp == "=")), true, nil
}
