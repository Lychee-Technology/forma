package sqlgen

import (
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// #555: a date/datetime attribute bound to a text column with the iso8601
// encoding is admitted by schemameta.ValidateColumnBinding (#459), and the
// write path, the Postgres read path and the CDC export all round-trip it.
// The DuckDB reader did not: the hot leg projected the raw VARCHAR against
// the pivot's epoch-ms BIGINT, and the outer select cast the unified
// epoch-ms back to BIGINT under the text column's alias, which the reader
// scans as a string and transform.readWithEncoding then fails to parse as
// RFC3339. These tests pin both ends.

func iso8601Cache() forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{
		"name": {AttributeID: 1, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01}},
		"seenAt": {AttributeID: 2, ValueType: forma.ValueTypeDateTime,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText02, Encoding: forma.MainColumnEncodingISO8601}},
		"bornOn": {AttributeID: 3, ValueType: forma.ValueTypeDate,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText03, Encoding: forma.MainColumnEncodingISO8601}},
	}
}

// TestHotLegProjections_ISO8601ColumnsNormaliseToEpochMs: both hot-leg
// projections read an iso8601-bound date/datetime column through the same
// epoch-ms spelling the CDC export uses (cdc.castDateMainValue, #219), so
// the COALESCE arms agree in type and the UNION ALL with the parquet legs
// carries one BIGINT convention (#200).
func TestHotLegProjections_ISO8601ColumnsNormaliseToEpochMs(t *testing.T) {
	sp, err := BuildSchemaProjection(7, iso8601Cache())
	require.NoError(t, err)
	require.False(t, sp.HasEAVAttrs, "fixture must qualify for the no-EAV shortcut")

	require.Contains(t, sp.PGSourceSelect,
		"COALESCE(ANY_VALUE(hot_vals.seenAt), epoch_ms(TRY_CAST(m.text_02 AS TIMESTAMP))) AS seenAt")
	require.Contains(t, sp.PGSourceSelect,
		"COALESCE(ANY_VALUE(hot_vals.bornOn), epoch_ms(TRY_CAST(m.text_03 AS TIMESTAMP))) AS bornOn")
	require.Contains(t, sp.PGSourceSelect, "COALESCE(ANY_VALUE(hot_vals.name), m.text_01) AS name",
		"a plain text column still projects raw")

	noEAV := sp.BuildPGSelectNoEAV()
	require.Contains(t, noEAV, "epoch_ms(TRY_CAST(m.text_02 AS TIMESTAMP)) AS seenAt")
	require.Contains(t, noEAV, "epoch_ms(TRY_CAST(m.text_03 AS TIMESTAMP)) AS bornOn")
	require.Contains(t, noEAV, "m.text_01 AS name")
	require.NotContains(t, noEAV, "m.text_02 AS seenAt", "the raw ISO string must not reach the UNION ALL")
	require.NotContains(t, noEAV, "m.text_03 AS bornOn")
}

// TestMainColISO8601Expr_ExecutesToEpochMs runs the hot-leg spelling on
// DuckDB over the images a text column can carry: the write path's UTC
// RFC3339 string (transform.storeWithEncoding), one carrying fractional
// seconds, a non-date string (NULL through TRY_CAST rather than a read-path
// crash, matching the export) and NULL. The TIMESTAMP cast is deliberately
// the export's, not TIMESTAMPTZ: with the bundled ICU an offset-less string
// would parse in the session TimeZone, host-dependent, whereas the funnel
// only ever writes `Z`.
func TestMainColISO8601Expr_ExecutesToEpochMs(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	for _, tc := range []struct {
		image string
		want  sql.NullInt64
	}{
		{"'2024-01-02T03:04:05Z'", sql.NullInt64{Int64: 1704164645000, Valid: true}},
		{"'2024-01-02T03:04:05.250Z'", sql.NullInt64{Int64: 1704164645250, Valid: true}},
		{"'1970-01-01T00:00:00Z'", sql.NullInt64{Int64: 0, Valid: true}},
		{"'not a date'", sql.NullInt64{}},
		{"NULL", sql.NullInt64{}},
	} {
		t.Run(tc.image, func(t *testing.T) {
			var got sql.NullInt64
			query := "SELECT " + mainColISO8601Expr("text_02") +
				" FROM (SELECT CAST(" + tc.image + " AS VARCHAR) AS text_02) m"
			require.NoError(t, db.QueryRow(query).Scan(&got))
			require.Equal(t, tc.want, got)
		})
	}
}

// TestBuildOuterSelect_ISO8601CastsBackToRFC3339: the outer select aliases
// the attribute to its text column, and the reader scans that column as a
// string which transform.readWithEncoding parses as RFC3339. CAST(attr AS
// BIGINT) put "1704164645000" into that slot. The epoch-ms verdict is cast
// back to the image the write path stores — RFC3339, UTC, second
// precision — so a DuckDB read decodes exactly like a Postgres read.
func TestBuildOuterSelect_ISO8601CastsBackToRFC3339(t *testing.T) {
	sp, err := BuildSchemaProjection(7, iso8601Cache())
	require.NoError(t, err)
	require.Contains(t, sp.OuterSelect,
		"strftime(epoch_ms(CAST(seenAt AS BIGINT)), '%Y-%m-%dT%H:%M:%SZ') AS text_02")
	require.Contains(t, sp.OuterSelect,
		"strftime(epoch_ms(CAST(bornOn AS BIGINT)), '%Y-%m-%dT%H:%M:%SZ') AS text_03")
	require.Contains(t, sp.OuterSelect, "CAST(name AS VARCHAR) AS text_01")

	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	for _, tc := range []struct {
		verdict string // the epoch-ms BIGINT every leg hands the outer select
		want    sql.NullString
	}{
		{"1704164645000", sql.NullString{String: "2024-01-02T03:04:05Z", Valid: true}},
		{"0", sql.NullString{String: "1970-01-01T00:00:00Z", Valid: true}},
		{"-86400000", sql.NullString{String: "1969-12-31T00:00:00Z", Valid: true}},
		{"NULL::BIGINT", sql.NullString{}},
	} {
		t.Run(tc.verdict, func(t *testing.T) {
			var got sql.NullString
			query := "SELECT " + duckDBMainColCast("seenAt", forma.ValueTypeDateTime, model.ColumnKindText) +
				" FROM (SELECT " + tc.verdict + " AS seenAt) t"
			require.NoError(t, db.QueryRow(query).Scan(&got),
				"outer select must scan with the reader's string type")
			require.Equal(t, tc.want, got)
		})
	}

	// A date/datetime that lands in a bigint column (default / unix_ms) is
	// untouched: the epoch-ms BIGINT is the stored image.
	require.Equal(t, "CAST(joined AS BIGINT)",
		duckDBMainColCast("joined", forma.ValueTypeDate, model.ColumnKindBigint))
}
