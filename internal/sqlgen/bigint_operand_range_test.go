package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// TestBigIntOperandRangeAllBinders pins the #502 contract on the three
// predicate binders — ConvertPgMainValue (PG main + hybrid), parsePgEavValue
// (PG EAV EXISTS) and parseDuckDBRawParam (DuckDB): an integral bigint operand
// outside int64 range is invalid input on every binder, so both routes answer
// the same 400 instead of Postgres comparing NUMERIC while DuckDB raises a
// Conversion Error on CAST('1e+30' AS BIGINT). The bound is the same one the
// #384 write funnel enforces, so no legal data can sit on either side of the
// rejected comparison, and every rejected literal has an exact in-range
// equivalent (gt:1e30 ≡ gt:9223372036854775807).
func TestBigIntOperandRangeAllBinders(t *testing.T) {
	cases := []struct {
		name    string
		literal string
		want    any // nil means rejected with ErrInvalidInput
	}{
		// Boundary literals bind exactly (#357): the check must not clip them.
		{"max_int64_bare", "9223372036854775807", int64(9223372036854775807)},
		{"min_int64_bare", "-9223372036854775808", int64(-9223372036854775808)},
		{"max_int64_exponent_spelling", "9.223372036854775807e18", int64(9223372036854775807)},
		{"in_range_exponent", "1e18", int64(1000000000000000000)},
		// A fractional literal in range stays float64 — pre-existing contract,
		// pinned by the characterization matrix; not this issue's corner.
		{"fractional_in_range", "9007199254740993.5", 9007199254740994.0},
		// Integral literals past int64 in every spelling ParseFloat accepts.
		{"1e30", "1e30", nil},
		{"2p63_bare", "9223372036854775808", nil},
		{"negative_1e19", "-1e19", nil},
		{"hex_float_2p70", "0x1p70", nil},
		// The bound is on magnitude, not integrality: a fractional literal
		// past int64 is rejected too, exactly as the write funnel rejects a
		// non-integral out-of-range value. Contrast fractional_in_range.
		{"fractional_out_of_range", "1.5e30", nil},
		{"negative_fractional_out_of_range", "-9.5e18", nil},
		{"inf", "Inf", nil},
		{"negative_inf", "-Inf", nil},
		{"nan", "NaN", nil},
	}

	meta := forma.AttributeMetadata{ValueType: forma.ValueTypeBigInt}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pgMain, pgMainErr := ConvertPgMainValue(tc.literal, "probe", meta)
			_, pgEav, pgEavErr := parsePgEavValue("probe", meta, tc.literal)
			duck, duckErr := parseDuckDBRawParam(tc.literal, "probe", forma.ValueTypeBigInt)

			if tc.want == nil {
				for name, err := range map[string]error{"ConvertPgMainValue": pgMainErr, "parsePgEavValue": pgEavErr, "parseDuckDBRawParam": duckErr} {
					require.ErrorIs(t, err, forma.ErrInvalidInput, name)
					require.Contains(t, err.Error(), "out of range for bigint attribute 'probe'", name)
					require.Contains(t, err.Error(), tc.literal, "%s must name the literal as given", name)
				}
				return
			}
			require.NoError(t, pgMainErr)
			require.NoError(t, pgEavErr)
			require.NoError(t, duckErr)
			require.Equal(t, tc.want, pgMain, "ConvertPgMainValue")
			require.Equal(t, tc.want, pgEav, "parsePgEavValue")
			require.Equal(t, tc.want, duck, "parseDuckDBRawParam")
		})
	}
}

// TestBigIntOperandRangeIsBigIntOnly guards the fence: the DOUBLE-width
// classes (integer/smallint/numeric, #384) compare an operand of any magnitude
// at DOUBLE on both engines, so 1e30 must keep parsing as float64 there.
func TestBigIntOperandRangeIsBigIntOnly(t *testing.T) {
	for _, vt := range []forma.ValueType{forma.ValueTypeInteger, forma.ValueTypeSmallInt, forma.ValueTypeNumeric} {
		t.Run(string(vt), func(t *testing.T) {
			meta := forma.AttributeMetadata{ValueType: vt}
			pgMain, err := ConvertPgMainValue("1e30", "probe", meta)
			require.NoError(t, err)
			require.Equal(t, 1e30, pgMain)
			_, pgEav, err := parsePgEavValue("probe", meta, "1e30")
			require.NoError(t, err)
			require.Equal(t, 1e30, pgEav)
			duck, err := parseDuckDBRawParam("1e30", "probe", vt)
			require.NoError(t, err)
			require.Equal(t, 1e30, duck)
		})
	}
}

// TestBigIntOperandRange_Entrypoints proves the rejection reaches the caller
// from every generator entrypoint, on both the bound (amount) and EAV-only
// (total) bigint attributes: ToDualClauses (the federated route), the PG-only
// EAV generator (hot route) and the DuckDB-only clause builder. The DuckDB
// entrypoint is asserted on its own so the DuckDB route can never fall back
// to a runtime Conversion Error if the PG binders change.
func TestBigIntOperandRange_Entrypoints(t *testing.T) {
	cache := characterizationCache()
	cases := []struct {
		name        string
		cond        forma.Condition
		wantDualErr string
		wantErrTail string
	}{
		{
			name:        "bound bigint gt 1e30 rejected in pg-main first",
			cond:        charKv("amount", "gt:1e30"),
			wantDualErr: "pg main generation: ",
			wantErrTail: "value 1e30 out of range for bigint attribute 'amount' (operand must fit [-9223372036854775808, 9223372036854775807])",
		},
		{
			name:        "eav-only bigint equals 2^63 rejected in EAV",
			cond:        charKv("total", "equals:9223372036854775808"),
			wantDualErr: "pg sql generation: ",
			wantErrTail: "value 9223372036854775808 out of range for bigint attribute 'total' (operand must fit [-9223372036854775808, 9223372036854775807])",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			paramIndex := 0
			_, err := ToDualClauses(tc.cond, "eav_table", 7, cache, &paramIndex)
			require.Error(t, err)
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			require.Contains(t, err.Error(), tc.wantDualErr+tc.wantErrTail)

			paramIndex = 0
			_, _, err = NewSQLGenerator().ToSQLClauses(tc.cond, "eav_table", 7, cache, &paramIndex)
			require.Error(t, err, "PG-only EAV entrypoint")
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			require.Contains(t, err.Error(), tc.wantErrTail)

			_, _, err = BuildDuckClause(tc.cond, cache)
			require.Error(t, err, "DuckDB-only entrypoint")
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			require.Contains(t, err.Error(), tc.wantErrTail)
		})
	}
}
