package sqlgen

import "math"

// buildCharBigIntCases covers the bigint storage class above 2^53, where a
// float64 bind stops being exact (#281). It lives in its own file so the main
// characterization matrix stays inside the 500-line source limit; the cases are
// appended to the same matrix by TestToDualClauses_Characterization.
func buildCharBigIntCases() []charCase {
	return []charCase{
		{
			// #281: bound bigint above 2^53 — all three emitters bind exactly.
			// Duck param is the exact decimal string (toDuckDBDecimalParam renders
			// int64 via %d); pre-#281 it was "9.00719925474099e+15" (%.15g of the
			// rounded float64) and PgArgs carried float64(9007199254740992).
			name: "bigint bound above 2^53: main/eav int64, duck exact string",
			cond: charKv("amount", "equals:9007199254740993"),
			want: DualClauses{
				PgMainClause: "m.bigint_02 = ?", PgMainArgs: []any{int64(9007199254740993)},
				PgClause: charEavClause("$2", "value_numeric", "=", "$3"), PgArgs: []any{int16(12), int64(9007199254740993)},
				DuckClause: "amount = CAST(? AS BIGINT)", DuckArgs: []any{"9007199254740993"},
			},
			span: 3,
		},
		{
			name: "bigint bound MaxInt64: 19 digits survive %d, would die in %.15g",
			cond: charKv("amount", "gte:9223372036854775807"),
			want: DualClauses{
				PgMainClause: "m.bigint_02 >= ?", PgMainArgs: []any{int64(math.MaxInt64)},
				PgClause: charEavClause("$2", "value_numeric", ">=", "$3"), PgArgs: []any{int16(12), int64(math.MaxInt64)},
				DuckClause: "amount >= CAST(? AS BIGINT)", DuckArgs: []any{"9223372036854775807"},
			},
			span: 3,
		},
		{
			// EAV-only bigint: the bind is exact, but eav_data storage rounds at
			// write (2^53 ceiling, #205) — so above 2^53 an exact equality bind
			// misses on every tier alike. That parity is the contract; the
			// pre-#281 float64 bind "matched" only by colliding with the same
			// rounding error.
			name: "bigint EAV-only above 2^53: exact bind, storage-capped semantics",
			cond: charKv("total", "equals:9007199254740993"),
			want: DualClauses{
				PgMainClause: "", PgMainArgs: nil,
				PgClause: charEavClause("$1", "value_numeric", "=", "$2"), PgArgs: []any{int16(13), int64(9007199254740993)},
				DuckClause: "total = CAST(? AS BIGINT)", DuckArgs: []any{"9007199254740993"},
			},
			span: 2,
		},
		{
			// #357: integral decimal spelling — same exactness as the bare
			// integer above. The condition DSL has always accepted "…993.0";
			// pre-#357 it rode ParseFloat's rounded float64, so the same value
			// bound exactly in one spelling and inexactly in the other.
			name: "bigint decimal spelling above 2^53: all three exact",
			cond: charKv("amount", "equals:9007199254740993.0"),
			want: DualClauses{
				PgMainClause: "m.bigint_02 = ?", PgMainArgs: []any{int64(9007199254740993)},
				PgClause: charEavClause("$2", "value_numeric", "=", "$3"), PgArgs: []any{int16(12), int64(9007199254740993)},
				DuckClause: "amount = CAST(? AS BIGINT)", DuckArgs: []any{"9007199254740993"},
			},
			span: 3,
		},
		{
			// #357: exponent spelling — 9.007199254740993e15 is exactly 2^53+1,
			// a value float64 cannot hold, so only reading the spelling exactly
			// — never the parsed float64 — can recover it.
			name: "bigint exponent spelling above 2^53: all three exact",
			cond: charKv("amount", "gte:9.007199254740993e15"),
			want: DualClauses{
				PgMainClause: "m.bigint_02 >= ?", PgMainArgs: []any{int64(9007199254740993)},
				PgClause: charEavClause("$2", "value_numeric", ">=", "$3"), PgArgs: []any{int16(12), int64(9007199254740993)},
				DuckClause: "amount >= CAST(? AS BIGINT)", DuckArgs: []any{"9007199254740993"},
			},
			span: 3,
		},
		{
			// #357 negative control: a genuinely fractional literal keeps
			// float64 on every emitter — the refinement must not turn the
			// numeric family into an integer-only family.
			name: "bigint fractional spelling: stays float64 on all three",
			cond: charKv("amount", "lt:9007199254740993.5"),
			want: DualClauses{
				PgMainClause: "m.bigint_02 < ?", PgMainArgs: []any{float64(9007199254740993.5)},
				PgClause: charEavClause("$2", "value_numeric", "<", "$3"), PgArgs: []any{int16(12), float64(9007199254740993.5)},
				// float64 in shortest round-trip form (#384 P1b): fractional
				// literals have no exact integral form to refine to, and the
				// rendered string must recover the identical float64
				// (9007199254740993.5 rounds to ...994 at parse time).
				DuckClause: "amount < CAST(? AS BIGINT)", DuckArgs: []any{"9.007199254740994e+15"},
			},
			span: 3,
		},
	}
}
