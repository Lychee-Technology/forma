package cdc

import (
	"database/sql"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

// TestCastMainValue_BoolEncodings pins the #404 read-side rule on the export
// leg: a bool_smallint column is read as the nearest of 0/1 (the `> 0.5`
// truthiness every other reader spells), and a bool_text column by its
// "1"/"0" contract — the same `= '1'` the DuckDB hot-leg pivot uses, not a
// wider LOWER(...) IN ('true','1') set only this exporter accepted. Neither
// is wrapped in CASE ... ELSE FALSE: the expression must stay nullable.
func TestCastMainValue_BoolEncodings(t *testing.T) {
	boolInt := &forma.MainColumnBinding{ColumnName: forma.MainColumnSmallint01, Encoding: forma.MainColumnEncodingBoolInt}
	boolText := &forma.MainColumnBinding{ColumnName: forma.MainColumnText02, Encoding: forma.MainColumnEncodingBoolText}

	require.Equal(t, "(m.smallint_01 > 0.5)",
		castMainValue("m.smallint_01", forma.AttributeMetadata{ValueType: forma.ValueTypeBool, ColumnBinding: boolInt}))
	require.Equal(t, "m.text_02 = '1'",
		castMainValue("m.text_02", forma.AttributeMetadata{ValueType: forma.ValueTypeBool, ColumnBinding: boolText}))
}

// TestCastMainValue_BoolExportPreservesNull executes the export expression
// against a real DuckDB engine over every storage image the column can hold,
// including NULL. An unset optional bool is NULL in entity_main and NULL on
// the hot leg (sqlgen.mainColBoolExpr); the exporter used to wrap the
// truthiness in CASE ... ELSE FALSE, so three-valued logic persisted false
// and `equals:false` matched the row only once it had been flushed (PR #564
// review, finding 1). The off-contract rows pin the other half: the parquet
// image is the same verdict the hot leg reaches, not a wider one.
func TestCastMainValue_BoolExportPreservesNull(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	boolInt := forma.AttributeMetadata{ValueType: forma.ValueTypeBool,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnSmallint01, Encoding: forma.MainColumnEncodingBoolInt}}
	boolText := forma.AttributeMetadata{ValueType: forma.ValueTypeBool,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText02, Encoding: forma.MainColumnEncodingBoolText}}

	cases := []struct {
		name   string
		meta   forma.AttributeMetadata
		source string // the stored column value, as a DuckDB literal
		want   sql.NullBool
	}{
		{"smallint NULL", boolInt, "NULL::SMALLINT", sql.NullBool{}},
		{"smallint 1", boolInt, "1::SMALLINT", sql.NullBool{Bool: true, Valid: true}},
		{"smallint 0", boolInt, "0::SMALLINT", sql.NullBool{Bool: false, Valid: true}},
		{"smallint 2 (off-contract)", boolInt, "2::SMALLINT", sql.NullBool{Bool: true, Valid: true}},
		{"smallint -1 (off-contract)", boolInt, "-1::SMALLINT", sql.NullBool{Bool: false, Valid: true}},
		{"text NULL", boolText, "NULL::VARCHAR", sql.NullBool{}},
		{"text 1", boolText, "'1'", sql.NullBool{Bool: true, Valid: true}},
		{"text 0", boolText, "'0'", sql.NullBool{Bool: false, Valid: true}},
		{"text true (off-contract)", boolText, "'true'", sql.NullBool{Bool: false, Valid: true}},
		{"text empty (off-contract)", boolText, "''", sql.NullBool{Bool: false, Valid: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expr := castMainValue("m", tc.meta)
			var got sql.NullBool
			require.NoError(t,
				db.QueryRow("SELECT "+expr+" FROM (SELECT "+tc.source+" AS m) t").Scan(&got),
				"export expr %q must evaluate", expr)
			require.Equal(t, tc.want, got, "export of %s through %q", tc.source, expr)
		})
	}
}

// TestCastEAVValue_BoolMatchesHotLegSpelling: the export leg and the hot-leg
// pivot must render the bool truthiness identically, not merely to the same
// type (#404) — a stored 0.3 must read the same from parquet and from
// Postgres.
func TestCastEAVValue_BoolMatchesHotLegSpelling(t *testing.T) {
	require.Equal(t, sqlgen.BoolTruthiness("value_numeric"),
		castEAVValue(forma.AttributeMetadata{ValueType: forma.ValueTypeBool}))
}
