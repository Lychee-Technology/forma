package sqlgen

import (
	"database/sql"
	"fmt"
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

// TestBuildPgMainClause_ISO8601NullTestFiltersTheHotRead: is_null on an
// iso8601-bound date tests the hot read; is_not_null and a comparison keep
// the raw column, and a unix_ms date keeps the raw null test.
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
		{"seenAt", "is_null:true", ISO8601EpochMs("m.text_02") + " IS NULL", nil},
		{"seenAt", "is_not_null:true", "m.text_02 IS NOT NULL", nil},
		{"bornOn", "is_null:true", ISO8601EpochMs("m.text_03") + " IS NULL", nil},
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

// TestISO8601NullPushdownIsNoStricterThanTheHotRead runs the pushdown leaf
// and the hot projection on DuckDB over the strings an iso8601 column can
// hold, with and without a retained eav_data value (an EAV-to-bound
// promotion keeps the rows written before it). The EAV-joined projection
// reads COALESCE(<eav pivot>, <main read>), so for every combination the
// pushdown must keep the row whenever that projection's null test does:
// otherwise the pre-filter drops a row `visible` would match. A NULL column
// with a retained value is left out: the raw column test drops it whatever
// the leaf renders, for every bound attribute, which is #742's open question
// of what a retained value means.
func TestISO8601NullPushdownIsNoStricterThanTheHotRead(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	cache := forma.SchemaAttributeCache{"seenAt": iso8601DateMeta(forma.ValueTypeDateTime, forma.MainColumnText02)}
	projection := "COALESCE(eav, " + mainColISO8601Expr(string(forma.MainColumnText02)) + ")"
	images := []sql.NullString{
		{String: "2024-01-02T03:04:05Z", Valid: true},
		{String: "not a date", Valid: true},
		{String: "", Valid: true},
		{String: "2024-13-45T00:00:00Z", Valid: true},
		{},
	}
	retained := []sql.NullInt64{{}, {Int64: 1704164645000, Valid: true}}
	for _, op := range []string{"is_null:true", "is_not_null:true"} {
		paramIndex := 0
		clause, _, err := buildPgMainClause(&forma.KvCondition{Attr: "seenAt", Value: op}, cache, &paramIndex)
		require.NoError(t, err)
		for _, image := range images {
			for _, eav := range retained {
				if !image.Valid && eav.Valid {
					continue
				}
				t.Run(fmt.Sprintf("%s/%q/eav=%v", op, image.String, eav.Valid), func(t *testing.T) {
					var pushed, projectedNull bool
					require.NoError(t, db.QueryRow(
						"SELECT "+clause+", ("+projection+") IS NULL"+
							" FROM (SELECT CAST(? AS VARCHAR) AS text_02, CAST(? AS BIGINT) AS eav) m",
						image, eav).Scan(&pushed, &projectedNull))
					if projectedNull == (op == "is_null:true") {
						require.True(t, pushed, "the pushdown drops a row the projection matches")
					}
				})
			}
		}
	}
}
