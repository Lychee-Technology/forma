package sqlgen

import (
	"math"

	"github.com/lychee-technology/forma"
)

// BoolMainPredicate is the entity_main pushdown form of a column-bound bool
// equality leaf (#565). Both pushdown routes (the pg-main clause the DuckDB
// hot leg embeds in its postgres_scan, and the hybrid builder's main branch
// on the Postgres route) render it, so a stored image outside the write
// contract answers a filter the way every reader already reads it:
//
//   - bool_smallint: `<col> BETWEEN lo AND hi` with the bounds — not the
//     operator — carrying the verdict. The raw `= 1`/`= 0` compare read a
//     stored 2 as neither true nor false while BoolTruthiness (`> 0.5`)
//     read it as true on the PG-EAV, DuckDB and CDC legs and the Go read
//     funnel, so the same row was returned as true by an unfiltered read
//     and skipped by `equals:true`. On a SMALLINT domain `BETWEEN 1 AND
//     32767` is exactly `> 0.5` and `BETWEEN -32768 AND 0` is exactly
//     `<= 0.5` (TestBoolMainPredicate_MatchesReadContract executes both).
//   - bool_text: `(<col> = '1') = ?` with a bool bind. The raw `= '0'` /
//     `!= '0'` compare placed a stored 'true' on the truthy side while the
//     read contract (`= '1'`, mainColBoolExpr) reads it as false. The left
//     side is that contract verbatim; there is no range equivalent on a
//     text domain.
//
// Why the clause text never depends on the operand: the federated plan
// cache keys on the query shape without operands
// (queryplan.HashFederatedQueryShape) and reuses the cached PgMainClause
// text on a hit, so `equals:true` and `equals:false` may differ only in
// their binds. This rules out the literal `> 0.5` / `<= 0.5` rewrite.
// Integer bounds also keep the smallint predicate on the integer_ops btree
// family; a `0.5` literal against an int2 column casts the column. NULL
// stays NULL under every spelling.
//
// not_equals:X is equals:!X on a two-valued domain, so the four
// operator/operand combinations collapse onto Truthy.
type BoolMainPredicate struct {
	Encoding forma.MainColumnEncoding
	// Truthy selects rows whose stored image reads as true; false selects
	// the rows that read as false.
	Truthy bool
}

// Arity is the number of placeholders the predicate consumes, in the order
// Args returns them.
func (p BoolMainPredicate) Arity() int {
	if p.Encoding == forma.MainColumnEncodingBoolText {
		return 1
	}
	return 2
}

// Render spells the predicate over an already-qualified column and the
// Arity() placeholders the emitter allocated, in bind order. It is the one
// spelling both pushdown emitters use.
func (p BoolMainPredicate) Render(column string, placeholders ...string) string {
	if p.Encoding == forma.MainColumnEncodingBoolText {
		return "(" + column + " = '1') = " + placeholders[0]
	}
	return column + " BETWEEN " + placeholders[0] + " AND " + placeholders[1]
}

// Args returns the binds in placeholder order.
func (p BoolMainPredicate) Args() []any {
	if p.Encoding == forma.MainColumnEncodingBoolText {
		return []any{p.Truthy}
	}
	if p.Truthy {
		return []any{int64(1), int64(math.MaxInt16)}
	}
	return []any{int64(math.MinInt16), int64(0)}
}

// pgMainBoolPredicate classifies an entity_main leaf: ok is true only for a
// bool attribute bound with a bool encoding under `=` or `!=`, in which
// case the returned predicate replaces the value bind. Any other leaf
// (other value types, other operators) keeps the ConvertPgMainValue bind.
// The operand parse is the engine-shared parseBoolOperand rule, so an
// invalid spelling is the same user-facing rejection ConvertPgMainValue
// raises.
func pgMainBoolPredicate(meta forma.AttributeMetadata, sqlOp, valStr, attr string) (BoolMainPredicate, bool, error) {
	if meta.ValueType != forma.ValueTypeBool || meta.ColumnBinding == nil {
		return BoolMainPredicate{}, false, nil
	}
	enc := meta.ColumnBinding.Encoding
	if enc != forma.MainColumnEncodingBoolInt && enc != forma.MainColumnEncodingBoolText {
		return BoolMainPredicate{}, false, nil
	}
	if sqlOp != "=" && sqlOp != "!=" {
		return BoolMainPredicate{}, false, nil
	}
	parsed, ok := parseBoolOperand(valStr)
	if !ok {
		return BoolMainPredicate{}, false, forma.InvalidInputf("invalid boolean value for '%s': %s", attr, valStr)
	}
	return BoolMainPredicate{Encoding: enc, Truthy: parsed == (sqlOp == "=")}, true, nil
}
