package cdc

import (
	"database/sql/driver"
	"math"
	"math/big"
	"testing"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

// NUMERIC decodes to the DOUBLE the DuckDB Postgres scanner produced for the
// export before #785: the nearest double, NaN and ±Infinity as themselves,
// ±Infinity past the double range and zero below it, where pgx's own
// conversion refuses the overflow.
func TestNumericToDouble_MatchesScanner(t *testing.T) {
	for text, want := range map[string]float64{
		"0":                   0,
		"0.1":                 0.1,
		"-12345.6789":         -12345.6789,
		"9007199254740993":    9007199254740992,
		"Infinity":            math.Inf(1),
		"-Infinity":           math.Inf(-1),
		"123456789.123456789": 123456789.12345679,
	} {
		var n pgtype.Numeric
		require.NoError(t, n.Scan(text), text)
		got, err := numericToDouble(n)
		require.NoError(t, err, text)
		require.Equal(t, want, got, text)
	}

	// The binary protocol yields the digits and the exponent, which is how
	// the values past the double range arrive.
	for name, tc := range map[string]struct {
		n    pgtype.Numeric
		want float64
	}{
		"1.8e308": {pgtype.Numeric{Int: big.NewInt(18), Exp: 307, Valid: true}, math.Inf(1)},
		"-1e400":  {pgtype.Numeric{Int: big.NewInt(-1), Exp: 400, Valid: true}, math.Inf(-1)},
		"1e-400":  {pgtype.Numeric{Int: big.NewInt(1), Exp: -400, Valid: true}, 0},
		"no int":  {pgtype.Numeric{Exp: 3, Valid: true}, 0},
	} {
		got, err := numericToDouble(tc.n)
		require.NoError(t, err, name)
		require.Equal(t, tc.want, got, name)
	}

	var nan pgtype.Numeric
	require.NoError(t, nan.Scan("NaN"))
	got, err := numericToDouble(nan)
	require.NoError(t, err)
	require.True(t, math.IsNaN(got))
}

// A staged double accepts double precision and NUMERIC alike, and NULL stays
// NULL for every staged type.
func TestStageRowDecoder_ValuesAndNulls(t *testing.T) {
	d := newStageRowDecoder([]stageColumn{
		{"a", stageSmallint}, {"b", stageInteger}, {"c", stageBigint},
		{"d", stageDouble}, {"e", stageVarchar}, {"f", stageUUID},
	})
	id := uuid.New()
	require.NoError(t, d.targets[0].(*pgtype.Int2).Scan(int64(-3)))
	require.NoError(t, d.targets[1].(*pgtype.Int4).Scan(int64(7)))
	require.NoError(t, d.targets[2].(*pgtype.Int8).Scan(int64(1)<<40))
	require.NoError(t, d.targets[3].(*stagedDouble).ScanFloat64(pgtype.Float8{Float64: 2.5, Valid: true}))
	require.NoError(t, d.targets[4].(*pgtype.Text).Scan("x"))
	require.NoError(t, d.targets[5].(*pgtype.UUID).Scan(id.String()))
	require.Equal(t, []driver.Value{int16(-3), int32(7), int64(1) << 40, 2.5, "x", duckdb.UUID(id)}, d.values())

	require.NoError(t, d.targets[3].(*stagedDouble).ScanNumeric(pgtype.Numeric{}))
	for _, target := range d.targets[:3] {
		require.NoError(t, target.(interface{ Scan(any) error }).Scan(nil))
	}
	require.NoError(t, d.targets[4].(*pgtype.Text).Scan(nil))
	require.NoError(t, d.targets[5].(*pgtype.UUID).Scan(nil))
	require.Equal(t, []driver.Value{nil, nil, nil, nil, nil, nil}, d.values())
}
