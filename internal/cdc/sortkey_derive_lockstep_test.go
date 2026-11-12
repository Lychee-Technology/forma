package cdc

import (
	"database/sql"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sortkey"
)

// deriveCase is one stored value at a derivation's edge: the attribute,
// what the export stages for it (the staged DuckDB type and the value
// staged, as #785's decoder hands it to the Appender), and what pgx scans
// from the same storage, which the hot leg derives its key from.
type deriveCase struct {
	meta   forma.AttributeMetadata
	staged string
	value  any // bound to DuckDB, cast to staged
	stored any // handed to sortkey.DeriveAttribute
}

func boundTo(col string, enc forma.MainColumnEncoding) *forma.MainColumnBinding {
	return &forma.MainColumnBinding{ColumnName: forma.MainColumn(col), Encoding: enc}
}

// numericCases stages value_numeric images: pgx scans a NUMERIC, and the
// export stages its double (numericToDouble, #785).
func numericCases(t *testing.T, meta forma.AttributeMetadata, images ...string) []deriveCase {
	t.Helper()
	cases := make([]deriveCase, 0, len(images))
	for _, image := range images {
		n := numericImage(t, image)
		f, err := numericToDouble(n)
		require.NoError(t, err, image)
		cases = append(cases, deriveCase{meta: meta, staged: "DOUBLE", value: f, stored: n})
	}
	return cases
}

// numericImage scans a NUMERIC's text, or builds a "<digits>e<exp>" image
// from its digits and exponent, as the binary protocol delivers a value past
// the double range.
func numericImage(t *testing.T, image string) pgtype.Numeric {
	t.Helper()
	var n pgtype.Numeric
	if mant, exp, ok := strings.Cut(image, "e"); ok {
		digits, okInt := new(big.Int).SetString(mant, 10)
		e, err := strconv.ParseInt(exp, 10, 32)
		require.True(t, okInt, image)
		require.NoError(t, err, image)
		return pgtype.Numeric{Int: digits, Exp: int32(e), Valid: true}
	}
	require.NoError(t, n.Scan(image), image)
	return n
}

func columnCases(meta forma.AttributeMetadata, staged string, values ...any) []deriveCase {
	cases := make([]deriveCase, 0, len(values))
	for _, v := range values {
		bind := v
		if u, ok := v.(uuid.UUID); ok {
			bind = u.String()
		}
		cases = append(cases, deriveCase{meta: meta, staged: staged, value: bind, stored: v})
	}
	return cases
}

// deriveCases reaches every row of design.md §6.1.1's derivation table:
// each export type, through each storage the write funnel can give it.
func deriveCases(t *testing.T) [][]deriveCase {
	eav := func(vt forma.ValueType) forma.AttributeMetadata {
		return forma.AttributeMetadata{AttributeName: "a", ValueType: vt}
	}
	col := func(vt forma.ValueType, column string, enc forma.MainColumnEncoding) forma.AttributeMetadata {
		return forma.AttributeMetadata{AttributeName: "a", ValueType: vt, ColumnBinding: boundTo(column, enc)}
	}
	edges := []string{"0", "2.5", "3.5", "-2.5", "0.49999999999999994", "9007199254740993", "9223372036854775807",
		"-9223372036854775808", "1e19", "1e400", "NaN", "Infinity", "-Infinity"}
	return [][]deriveCase{
		numericCases(t, eav(forma.ValueTypeBigInt), edges...),
		numericCases(t, eav(forma.ValueTypeDate), "1700000000000", "-62167219200000", "2.5", "NaN"),
		columnCases(col(forma.ValueTypeDate, "bigint_01", forma.MainColumnEncodingUnixMs), "BIGINT",
			int64(math.MinInt64), int64(-1), int64(0), int64(math.MaxInt64)),
		columnCases(col(forma.ValueTypeDateTime, "text_01", forma.MainColumnEncodingRFC3339), "VARCHAR",
			"2024-01-02T03:04:05.678Z", "1969-12-31T23:59:59.999Z", "0000-01-01T00:00:00.000Z",
			"9999-12-31T23:59:59.999Z", "9999-12-30T23:00:00.000-24:60", "0000-01-01T01:00:00.000+24:60",
			"2024-01-02T11:04:05+08:00", "2024-01-02T03:04:05Z", "2024-02-30T00:00:00.000Z", "not a date"),
		columnCases(col(forma.ValueTypeBigInt, "bigint_01", ""), "BIGINT", int64(math.MaxInt64), int64(math.MinInt64)),
		columnCases(col(forma.ValueTypeBigInt, "double_01", ""), "DOUBLE", 2.5, 1e19, -9.223372036854775808e18, math.NaN()),
		columnCases(col(forma.ValueTypeInteger, "integer_01", ""), "INTEGER", int32(math.MaxInt32), int32(math.MinInt32)),
		columnCases(col(forma.ValueTypeInteger, "bigint_01", ""), "BIGINT", int64(math.MaxInt32)+1, int64(math.MinInt32)),
		columnCases(col(forma.ValueTypeInteger, "double_01", ""), "DOUBLE", 2147483647.5, -2147483648.5, 2.5, math.Inf(1), math.NaN()),
		columnCases(col(forma.ValueTypeInteger, "smallint_01", ""), "SMALLINT", int16(-7)),
		columnCases(col(forma.ValueTypeSmallInt, "smallint_01", ""), "SMALLINT", int16(math.MaxInt16), int16(math.MinInt16)),
		columnCases(col(forma.ValueTypeSmallInt, "integer_01", ""), "INTEGER", int32(40000), int32(-32768)),
		columnCases(col(forma.ValueTypeSmallInt, "double_01", ""), "DOUBLE", 32767.5, -32768.5, 0.5, math.NaN()),
		numericCases(t, eav(forma.ValueTypeInteger), "2.5", "1e400", "NaN", "-0", "4294967296"),
		numericCases(t, eav(forma.ValueTypeSmallInt), "70000", "-1.5"),
		numericCases(t, eav(forma.ValueTypeNumeric), "9007199254740993", "-0", "0.1", "1e-400", "-Infinity"),
		columnCases(col(forma.ValueTypeNumeric, "double_01", ""), "DOUBLE", math.NaN(), math.Copysign(0, -1), math.Inf(1), 0.1),
		columnCases(col(forma.ValueTypeNumeric, "bigint_01", ""), "BIGINT", int64(1<<53+1), int64(math.MaxInt64)),
		columnCases(col(forma.ValueTypeNumeric, "integer_01", ""), "INTEGER", int32(-3)),
		numericCases(t, eav(forma.ValueTypeBool), "0", "1", "0.5", "0.5000001", "NaN", "-1", "2", "-Infinity"),
		columnCases(col(forma.ValueTypeBool, "smallint_01", forma.MainColumnEncodingBoolInt), "SMALLINT", int16(0), int16(1), int16(2), int16(-1)),
		columnCases(col(forma.ValueTypeBool, "text_02", forma.MainColumnEncodingBoolText), "VARCHAR", "1", "0", "true", ""),
		columnCases(col(forma.ValueTypeText, "text_01", ""), "VARCHAR", "", "abc", "B", "é"),
		columnCases(eav(forma.ValueTypeText), "VARCHAR", "", "Z", "a"),
		columnCases(col(forma.ValueTypeUUID, "uuid_01", ""), "UUID",
			uuid.MustParse("6BA7B810-9DAD-11D1-80B4-00C04FD430C8"), uuid.MustParse("ffffffff-0000-0000-0000-000000000000")),
		columnCases(eav(forma.ValueTypeUUID), "VARCHAR", "6ba7b810-9dad-11d1-80b4-00c04fd430c8", "00000000-0000-0000-0000-000000000001"),
	}
}

// TestDeriveLockstepWithExport pins every row of the derivation table: for
// each stored value, the hot key Go derives (sortkey.DeriveAttribute) equals
// the value the CDC export writes for it (castMainValue / castEAVValue over
// the staged value, read back at the column's export type), and the Go
// comparator orders each attribute's values as DuckDB's ORDER BY orders the
// exported ones.
func TestDeriveLockstepWithExport(t *testing.T) {
	db, err := sql.Open("duckdb", "?lambda_syntax=DISABLE_SINGLE_ARROW")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	for _, group := range deriveCases(t) {
		meta := group[0].meta
		typ, err := sortkey.AttributeType(meta)
		require.NoError(t, err)
		label := fmt.Sprintf("%s (binding %+v)", meta.ValueType, meta.ColumnBinding)
		_, err = db.Exec(fmt.Sprintf("CREATE OR REPLACE TABLE exported (id BIGINT, v %s)", typ))
		require.NoError(t, err)
		keys := make([][]any, len(group))
		for i, c := range group {
			want := exportedValue(t, db, c, typ)
			got, err := sortkey.DeriveAttribute(c.meta, c.stored)
			require.NoError(t, err, "%s: %v", label, c.stored)
			requireSameKey(t, want, got, "%s: stored %T(%v)", label, c.stored, c.stored)
			_, err = db.Exec("INSERT INTO exported VALUES (?, ?)", int64(i), want)
			require.NoError(t, err)
			keys[i] = []any{got, int64(i)}
		}
		for _, desc := range []bool{false, true} {
			order := sortkey.Order{Columns: []sortkey.Column{{Name: "v", Type: typ, Desc: desc}, {Name: "id", Type: sortkey.BigInt}}}
			sort.SliceStable(keys, func(a, b int) bool { return order.Compare(keys[a], keys[b]) < 0 })
			dir := map[bool]string{false: "ASC", true: "DESC"}[desc]
			rows, err := db.Query("SELECT id FROM exported ORDER BY v " + dir + ", id")
			require.NoError(t, err)
			var got []int64
			for rows.Next() {
				var id int64
				require.NoError(t, rows.Scan(&id))
				got = append(got, id)
			}
			require.NoError(t, rows.Close())
			want := make([]int64, len(keys))
			for i, k := range keys {
				want[i] = k[1].(int64)
			}
			require.Equal(t, want, got, "%s: ORDER BY v %s", label, dir)
		}
	}
}

// exportedValue runs the export's cast over the staged value and reads the
// result at the column's export type, as the parquet leg reads the column.
func exportedValue(t *testing.T, db *sql.DB, c deriveCase, typ sortkey.ExportType) any {
	t.Helper()
	var query string
	if c.meta.ColumnBinding != nil {
		query = fmt.Sprintf("SELECT CAST((%s) AS %s) FROM (SELECT CAST(? AS %s) AS c) m", castMainValue("m.c", c.meta), typ, c.staged)
	} else {
		column := "value_numeric"
		if c.staged == "VARCHAR" {
			column = "value_text"
		}
		query = fmt.Sprintf("SELECT CAST((%s) AS %s) FROM (SELECT CAST(? AS %s) AS %s) e", castEAVValue(c.meta), typ, c.staged, column)
	}
	var v any
	require.NoError(t, db.QueryRow(query, c.value).Scan(&v), query)
	switch x := v.(type) {
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	default:
		return v
	}
}

func requireSameKey(t *testing.T, want, got any, msgAndArgs ...any) {
	t.Helper()
	wf, wok := want.(float64)
	gf, gok := got.(float64)
	if wok && gok {
		same := math.IsNaN(wf) && math.IsNaN(gf) || wf == gf && math.Signbit(wf) == math.Signbit(gf)
		require.True(t, same,
			append([]any{fmt.Sprintf("want %v, got %v: ", wf, gf)}, msgAndArgs...)...)
		return
	}
	require.Equal(t, want, got, msgAndArgs...)
}
