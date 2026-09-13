package cdc

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

// TestCastMainValue_BoolEncodings pins the #404 read-side rule on the export
// leg: a bool_smallint column is read as the nearest of 0/1 (the `> 0.5`
// truthiness every other reader spells), and a bool_text column by its
// "1"/"0" contract — the same `= '1'` the DuckDB hot-leg pivot uses, not a
// wider LOWER(...) IN ('true','1') set only this exporter accepted.
func TestCastMainValue_BoolEncodings(t *testing.T) {
	boolInt := &forma.MainColumnBinding{ColumnName: forma.MainColumnSmallint01, Encoding: forma.MainColumnEncodingBoolInt}
	boolText := &forma.MainColumnBinding{ColumnName: forma.MainColumnText02, Encoding: forma.MainColumnEncodingBoolText}

	require.Equal(t, "CASE WHEN (m.smallint_01 > 0.5) THEN TRUE ELSE FALSE END",
		castMainValue("m.smallint_01", forma.AttributeMetadata{ValueType: forma.ValueTypeBool, ColumnBinding: boolInt}))
	require.Equal(t, "CASE WHEN m.text_02 = '1' THEN TRUE ELSE FALSE END",
		castMainValue("m.text_02", forma.AttributeMetadata{ValueType: forma.ValueTypeBool, ColumnBinding: boolText}))
}

// TestCastEAVValue_BoolMatchesHotLegSpelling: the export leg and the hot-leg
// pivot must render the bool truthiness identically, not merely to the same
// type (#404) — a stored 0.3 must read the same from parquet and from
// Postgres.
func TestCastEAVValue_BoolMatchesHotLegSpelling(t *testing.T) {
	require.Equal(t, sqlgen.BoolTruthiness("value_numeric"),
		castEAVValue(forma.AttributeMetadata{ValueType: forma.ValueTypeBool}))
}
