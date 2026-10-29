package sqlgen

import (
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// #709: a date/datetime bound to a text column with the iso8601 encoding is
// read on the hot leg through the export's TRY_CAST (mainColISO8601Expr), so
// a string that is not a timestamp reads NULL on every tier (#555). The
// pg-main null test compared the raw column instead, which is not NULL, so
// is_null dropped such an unflushed row from pg_source before `visible`
// could match it, and matched it once its parquet copy served it.

func iso8601DateMeta(vt forma.ValueType, col forma.MainColumn) forma.AttributeMetadata {
	return forma.AttributeMetadata{AttributeID: 1, ValueType: vt,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: col, Encoding: forma.MainColumnEncodingISO8601}}
}

// TestBuildPgMainClause_ISO8601NullTestFiltersTheHotRead: a null test on an
// iso8601-bound date tests the hot read; a comparison keeps the raw column
// against the canonical image, and a unix_ms date keeps the raw null test.
func TestBuildPgMainClause_ISO8601NullTestFiltersTheHotRead(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"seenAt": iso8601DateMeta(forma.ValueTypeDateTime, forma.MainColumnText02),
		"bornOn": iso8601DateMeta(forma.ValueTypeDate, forma.MainColumnText03),
		"stamp": {AttributeID: 3, ValueType: forma.ValueTypeDateTime,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnBigint01, Encoding: forma.MainColumnEncodingUnixMs}},
	}
	for _, tc := range []struct {
		attr string
		cond string
		want string
		args []any
	}{
		{"seenAt", "is_null:true", "epoch_ms(TRY_CAST(m.text_02 AS TIMESTAMP)) IS NULL", nil},
		{"seenAt", "is_not_null:true", "epoch_ms(TRY_CAST(m.text_02 AS TIMESTAMP)) IS NOT NULL", nil},
		{"bornOn", "is_null:true", "epoch_ms(TRY_CAST(m.text_03 AS TIMESTAMP)) IS NULL", nil},
		{"seenAt", "gt:2024-01-02T03:04:05Z", "m.text_02 > ?", []any{"2024-01-02T03:04:05Z"}},
		{"stamp", "is_null:true", "m.bigint_01 IS NULL", nil},
	} {
		t.Run(tc.attr+"_"+tc.cond, func(t *testing.T) {
			paramIndex := 0
			clause, args, err := buildPgMainClause(&forma.KvCondition{Attr: tc.attr, Value: tc.cond}, cache, &paramIndex)
			require.NoError(t, err)
			require.Equal(t, tc.want, clause)
			require.Equal(t, tc.args, args)
		})
	}
}

// TestISO8601NullPushdownAgreesWithTheHotRead runs the pushdown leaf and the
// hot read on DuckDB over the strings an iso8601 column can hold: for every
// one, the pushdown keeps the row exactly when the read's null test does,
// so the pre-filter never drops a row `visible` would match.
func TestISO8601NullPushdownAgreesWithTheHotRead(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	cache := forma.SchemaAttributeCache{"seenAt": iso8601DateMeta(forma.ValueTypeDateTime, forma.MainColumnText02)}
	read := mainColISO8601Expr(string(forma.MainColumnText02))
	for _, op := range []string{"is_null:true", "is_not_null:true"} {
		paramIndex := 0
		clause, _, err := buildPgMainClause(&forma.KvCondition{Attr: "seenAt", Value: op}, cache, &paramIndex)
		require.NoError(t, err)
		for _, image := range []sql.NullString{
			{String: "2024-01-02T03:04:05Z", Valid: true},
			{String: "not a date", Valid: true},
			{String: "", Valid: true},
			{String: "2024-13-45T00:00:00Z", Valid: true},
			{},
		} {
			t.Run(op+"/"+image.String, func(t *testing.T) {
				var pushed, readNull bool
				require.NoError(t, db.QueryRow(
					"SELECT "+clause+", ("+read+") IS NULL FROM (SELECT CAST(? AS VARCHAR) AS text_02) m",
					image).Scan(&pushed, &readNull))
				require.Equal(t, readNull == (op == "is_null:true"), pushed)
			})
		}
	}
}
