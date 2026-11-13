package sortkey

import (
	"database/sql"
	"math"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
)

func TestSystemColumnName(t *testing.T) {
	for name, want := range map[string]string{"row_id": ColumnRowID, "Created_At": ColumnCreatedAt, "VER_TS": ColumnVerTS, "deleted_ts": ColumnDeletedTS} {
		got, ok := SystemColumnName(name)
		require.True(t, ok, name)
		require.Equal(t, want, got)
	}
	_, ok := SystemColumnName("count")
	require.False(t, ok)
}

func TestColumnAttribute(t *testing.T) {
	cache := forma.SchemaAttributeCache{"count": {AttributeID: 1, ValueType: forma.ValueTypeInteger}}
	meta, err := ColumnAttribute("COUNT", cache)
	require.NoError(t, err)
	require.Equal(t, "count", meta.AttributeName, "the attribute DuckDB binds the name to")
	_, err = ColumnAttribute("created_at", cache)
	require.ErrorContains(t, err, "is a system column")
	_, err = ColumnAttribute("missing", cache)
	require.ErrorContains(t, err, "neither a system column")
}

// An EAV-only attribute's key is the greatest derived value of its rows, as
// the export's MAX(CASE WHEN attr_id = … THEN <cast> END) collapses them.
func TestDeriveEAVAttribute(t *testing.T) {
	text := forma.AttributeMetadata{AttributeName: "note", ValueType: forma.ValueTypeText}
	got, err := DeriveEAVAttribute(text, []any{"m", nil, "q", "B"})
	require.NoError(t, err)
	require.Equal(t, "q", got, "byte order: B sorts before m")

	num := forma.AttributeMetadata{AttributeName: "w", ValueType: forma.ValueTypeNumeric}
	got, err = DeriveEAVAttribute(num, []any{1.5, math.Inf(1), math.NaN(), nil})
	require.NoError(t, err)
	require.True(t, math.IsNaN(got.(float64)), "NaN is DuckDB's greatest DOUBLE")
	got, err = DeriveEAVAttribute(num, []any{pgtype.Numeric{Valid: false}, nil})
	require.NoError(t, err)
	require.Nil(t, got)
	got, err = DeriveEAVAttribute(num, nil)
	require.NoError(t, err)
	require.Nil(t, got, "no row derives NULL")

	small := forma.AttributeMetadata{AttributeName: "s", ValueType: forma.ValueTypeSmallInt}
	got, err = DeriveEAVAttribute(small, []any{1.5, 40000.0})
	require.NoError(t, err)
	require.Equal(t, 40000.0, got, "an EAV-only smallint exports by its DOUBLE storage width")

	flag := forma.AttributeMetadata{AttributeName: "f", ValueType: forma.ValueTypeBool}
	got, err = DeriveEAVAttribute(flag, []any{0.0, 1.0})
	require.NoError(t, err)
	require.Equal(t, true, got)

	_, err = DeriveEAVAttribute(forma.AttributeMetadata{AttributeName: "b", ValueType: forma.ValueTypeText,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_01")}}, []any{"x"})
	require.ErrorContains(t, err, "is bound to column")
	_, err = DeriveEAVAttribute(forma.AttributeMetadata{AttributeName: "l", ValueType: forma.ValueTypeList}, []any{"x"})
	require.ErrorContains(t, err, "is a list")
	_, err = DeriveEAVAttribute(text, []any{1.5})
	require.Error(t, err, "a text attribute's storage holds text")
}

// DuckDB's MAX picks what DeriveEAVAttribute picks over the export types an
// EAV-only attribute can have: DOUBLE with NaN and ±Infinity, VARCHAR by
// bytes, BOOLEAN, and NULLs skipped.
func TestDuckDBMaxMatchesDeriveEAVAttribute(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	cases := []struct {
		meta   forma.AttributeMetadata
		duck   string
		values []any
	}{
		{forma.AttributeMetadata{AttributeName: "d", ValueType: forma.ValueTypeNumeric}, "DOUBLE", []any{1.5, math.NaN(), math.Inf(1), nil}},
		{forma.AttributeMetadata{AttributeName: "d", ValueType: forma.ValueTypeNumeric}, "DOUBLE", []any{math.Inf(-1), math.Copysign(0, -1), nil}},
		{forma.AttributeMetadata{AttributeName: "t", ValueType: forma.ValueTypeText}, "VARCHAR", []any{"B", "a", "Z", "é", nil}},
		{forma.AttributeMetadata{AttributeName: "b", ValueType: forma.ValueTypeBool}, "BOOLEAN", []any{false, nil, true}},
	}
	for _, c := range cases {
		_, err := db.Exec("CREATE OR REPLACE TABLE v (x " + c.duck + ")")
		require.NoError(t, err)
		for _, v := range c.values {
			_, err = db.Exec("INSERT INTO v VALUES (?)", v)
			require.NoError(t, err)
		}
		var duckMax any
		require.NoError(t, db.QueryRow("SELECT MAX(x) FROM v").Scan(&duckMax))

		stored := c.values
		if c.meta.ValueType == forma.ValueTypeBool {
			stored = []any{0.0, nil, 1.0} // value_numeric's images of false and true
		}
		got, err := DeriveEAVAttribute(c.meta, stored)
		require.NoError(t, err)
		if f, ok := duckMax.(float64); ok && math.IsNaN(f) {
			require.True(t, math.IsNaN(got.(float64)), "%s MAX", c.duck)
			continue
		}
		require.Equal(t, duckMax, got, "%s MAX", c.duck)
	}
}
