package sqlgen

import (
	"database/sql"
	"strconv"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/rfc3339"
	"github.com/stretchr/testify/require"
)

// #555: a date/datetime attribute bound to a text column with the rfc3339
// encoding is admitted by schemameta.ValidateColumnBinding (#459), and the
// write path, the Postgres read path and the CDC export all round-trip it.
// The DuckDB reader did not: the hot leg projected the raw VARCHAR against
// the pivot's epoch-ms BIGINT, and the outer select cast the unified
// epoch-ms back to BIGINT under the text column's alias, which the reader
// scans as a string and transform.readWithEncoding then fails to parse as
// RFC3339. These tests pin both ends.

func rfc3339Cache() forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{
		"name": {AttributeID: 1, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01}},
		"seenAt": {AttributeID: 2, ValueType: forma.ValueTypeDateTime,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText02, Encoding: forma.MainColumnEncodingRFC3339}},
		"bornOn": {AttributeID: 3, ValueType: forma.ValueTypeDate,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText03, Encoding: forma.MainColumnEncodingRFC3339}},
	}
}

// TestHotLegProjections_RFC3339ColumnsNormaliseToEpochMs: both hot-leg
// projections read an rfc3339-bound date/datetime column through the same
// epoch-ms spelling the CDC export uses (cdc.castDateMainValue, #219), so
// the COALESCE arms agree in type and the UNION ALL with the parquet legs
// carries one BIGINT convention (#200).
func TestHotLegProjections_RFC3339ColumnsNormaliseToEpochMs(t *testing.T) {
	sp, err := BuildSchemaProjection(7, rfc3339Cache())
	require.NoError(t, err)
	require.False(t, sp.HasEAVAttrs, "fixture must qualify for the no-EAV shortcut")

	require.Contains(t, sp.PGSourceSelect,
		"COALESCE(ANY_VALUE(hot_vals.seenAt), "+RFC3339EpochMs("m.text_02")+") AS seenAt")
	require.Contains(t, sp.PGSourceSelect,
		"COALESCE(ANY_VALUE(hot_vals.bornOn), "+RFC3339EpochMs("m.text_03")+") AS bornOn")
	require.Contains(t, sp.PGSourceSelect, "COALESCE(ANY_VALUE(hot_vals.name), m.text_01) AS name",
		"a plain text column still projects raw")

	noEAV := sp.BuildPGSelectNoEAV()
	require.Contains(t, noEAV, RFC3339EpochMs("m.text_02")+" AS seenAt")
	require.Contains(t, noEAV, RFC3339EpochMs("m.text_03")+" AS bornOn")
	require.Contains(t, noEAV, "m.text_01 AS name")
	require.NotContains(t, noEAV, "m.text_02 AS seenAt", "the raw ISO string must not reach the UNION ALL")
	require.NotContains(t, noEAV, "m.text_03 AS bornOn")
}

// TestMainColRFC3339Expr_ExecutesToEpochMs runs the hot-leg spelling on
// DuckDB over the images a text column can carry: the write path's UTC
// RFC3339 string (transform.storeWithEncoding), one carrying fractional
// seconds, a non-date string (NULL through TRY_CAST rather than a read-path
// crash, matching the export) and NULL. Offsets and session TimeZones are
// rfc3339_read_duckdb_test.go's (#756).
func TestMainColRFC3339Expr_ExecutesToEpochMs(t *testing.T) {
	db, err := sql.Open("duckdb", strictLambdaDSN)
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
			query := "SELECT " + mainColRFC3339Expr("text_02") +
				" FROM (SELECT CAST(" + tc.image + " AS VARCHAR) AS text_02) m"
			require.NoError(t, db.QueryRow(query).Scan(&got))
			require.Equal(t, tc.want, got)
		})
	}
}

// TestBuildOuterSelect_RFC3339CastsBackToRFC3339: the outer select aliases
// the attribute to its text column, and the reader scans that column as a
// string which transform.readWithEncoding parses as RFC3339. CAST(attr AS
// BIGINT) put "1704164645000" into that slot. The epoch-ms verdict is cast
// back to the image the write path stores — RFC 3339, UTC, three fractional
// digits (#842) — so a DuckDB read decodes exactly like a Postgres read, to
// the millisecond. An instant an offset image carried past the four-digit
// UTC years keeps a four-digit wall clock at a 25-hour offset, with its
// fraction.
func TestBuildOuterSelect_RFC3339CastsBackToRFC3339(t *testing.T) {
	sp, err := BuildSchemaProjection(7, rfc3339Cache())
	require.NoError(t, err)
	require.Contains(t, sp.OuterSelect, rfc3339ImageFromEpochMs("seenAt")+" AS text_02")
	require.Contains(t, sp.OuterSelect, rfc3339ImageFromEpochMs("bornOn")+" AS text_03")
	require.Contains(t, sp.OuterSelect, "CAST(name AS VARCHAR) AS text_01")

	db, err := sql.Open("duckdb", strictLambdaDSN)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	for _, tc := range []struct {
		verdict string // the epoch-ms BIGINT every leg hands the outer select
		want    sql.NullString
	}{
		{"1704164645000", sql.NullString{String: "2024-01-02T03:04:05.000Z", Valid: true}},
		{"1704164645250", sql.NullString{String: "2024-01-02T03:04:05.250Z", Valid: true}},
		{"1704164645005", sql.NullString{String: "2024-01-02T03:04:05.005Z", Valid: true}},
		{"0", sql.NullString{String: "1970-01-01T00:00:00.000Z", Valid: true}},
		{"-1", sql.NullString{String: "1969-12-31T23:59:59.999Z", Valid: true}},
		{"-86400000", sql.NullString{String: "1969-12-31T00:00:00.000Z", Valid: true}},
		{"253402300799999", sql.NullString{String: "9999-12-31T23:59:59.999Z", Valid: true}},
		{"-62167219200000", sql.NullString{String: "0000-01-01T00:00:00.000Z", Valid: true}},
		// Past the four-digit UTC years: 10000-01-01T00:30:00.250Z and
		// -0001-12-31T23:30:00.750Z, spelled at a 25-hour offset.
		{"253402302600250", sql.NullString{String: "9999-12-30T23:30:00.250-24:60", Valid: true}},
		{"-62167220999250", sql.NullString{String: "0000-01-02T00:30:00.750+24:60", Valid: true}},
		{"NULL::BIGINT", sql.NullString{}},
	} {
		t.Run(tc.verdict, func(t *testing.T) {
			var got sql.NullString
			query := "SELECT " + duckDBMainColCast("seenAt", forma.ValueTypeDateTime, model.ColumnKindText) +
				" FROM (SELECT " + tc.verdict + " AS seenAt) t"
			require.NoError(t, db.QueryRow(query).Scan(&got),
				"outer select must scan with the reader's string type")
			require.Equal(t, tc.want, got)
			if ms, err := strconv.ParseInt(tc.verdict, 10, 64); err == nil {
				if image, rule := rfc3339.Image(ms); rule == "" {
					require.Equal(t, image, got.String, "inside the four-digit years the outer select spells rfc3339.Image")
				}
			}
		})
	}

	// A date/datetime that lands in a bigint column (default / unix_ms) is
	// untouched: the epoch-ms BIGINT is the stored image.
	require.Equal(t, "CAST(joined AS BIGINT)",
		duckDBMainColCast("joined", forma.ValueTypeDate, model.ColumnKindBigint))
}
