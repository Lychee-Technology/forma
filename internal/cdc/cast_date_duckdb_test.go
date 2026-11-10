package cdc

import (
	"database/sql"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// TestCastDateMainValue_ExecutesToEpochMs proves the #219 normalization end to
// end against a real DuckDB engine: the expression castMainValue emits for each
// date/datetime encoding evaluates to the SAME epoch-ms BIGINT, and that value
// reads back unchanged through the federated reader's CAST(attr AS BIGINT)
// projection. Pre-#219 the rfc3339/default branches produced native
// DATE/TIMESTAMP columns that CAST(... AS BIGINT) reinterpreted as
// days/microseconds — a silent 10^3-10^8x scale error on the warm/cold tiers.
func TestCastDateMainValue_ExecutesToEpochMs(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	// 2024-01-02T03:04:05Z == 1704164645000 ms since the epoch.
	const wantEpochMs int64 = 1704164645000

	cases := []struct {
		name string
		meta forma.AttributeMetadata
		// source SQL column value expression matching how the write path stores
		// this encoding: rfc3339 -> RFC3339 text; unix_ms/default -> epoch-ms bigint.
		sourceExpr string
	}{
		{
			name:       "rfc3339_text",
			meta:       forma.AttributeMetadata{ValueType: forma.ValueTypeDateTime, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText02, Encoding: forma.MainColumnEncodingRFC3339}},
			sourceExpr: "'2024-01-02T03:04:05.000Z'",
		},
		{
			// The whole-second image the iso8601 encoding stored before
			// #842 exports the same instant, so a row the migration has not
			// reached yet flushes unchanged.
			name:       "rfc3339_text_legacy_whole_second",
			meta:       forma.AttributeMetadata{ValueType: forma.ValueTypeDateTime, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText02, Encoding: forma.MainColumnEncodingRFC3339}},
			sourceExpr: "'2024-01-02T03:04:05Z'",
		},
		{
			// The same instant written with a UTC offset: the export applies
			// it, as the OLTP read does (#756).
			name:       "rfc3339_text_with_offset",
			meta:       forma.AttributeMetadata{ValueType: forma.ValueTypeDateTime, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText02, Encoding: forma.MainColumnEncodingRFC3339}},
			sourceExpr: "'2024-01-02T11:04:05+08:00'",
		},
		{
			name:       "unix_ms_bigint",
			meta:       forma.AttributeMetadata{ValueType: forma.ValueTypeDateTime, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnBigint02, Encoding: forma.MainColumnEncodingUnixMs}},
			sourceExpr: "1704164645000::BIGINT",
		},
		{
			name:       "default_bigint",
			meta:       forma.AttributeMetadata{ValueType: forma.ValueTypeDate, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnBigint02}},
			sourceExpr: "1704164645000::BIGINT",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Export side: evaluate the exporter's cast over the stored value.
			exportExpr := castMainValue("m", tc.meta)
			var exported int64
			require.NoError(t,
				db.QueryRow("SELECT "+exportExpr+" FROM (SELECT "+tc.sourceExpr+" AS m) t").Scan(&exported),
				"export expr %q must evaluate", exportExpr)
			require.Equal(t, wantEpochMs, exported, "export must yield epoch-ms")

			// Read side: the federated reader casts the parquet column with
			// CAST(attr AS BIGINT); on an epoch-ms BIGINT column that is identity.
			var readBack int64
			require.NoError(t,
				db.QueryRow("SELECT CAST(m AS BIGINT) FROM (SELECT ?::BIGINT AS m) t", exported).Scan(&readBack))
			require.Equal(t, wantEpochMs, readBack, "reader CAST(attr AS BIGINT) must preserve epoch-ms")
		})
	}
}

// TestCastDateMainValue_KeepsTheMillisecond (#842): the export of an rfc3339
// image keeps its millisecond, so two images that differ only there export
// two instants, and the parquet copy names the instant the OLTP read does.
func TestCastDateMainValue_KeepsTheMillisecond(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	meta := forma.AttributeMetadata{ValueType: forma.ValueTypeDateTime,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText02, Encoding: forma.MainColumnEncodingRFC3339}}
	for image, want := range map[string]int64{
		"2024-01-02T03:04:05.000Z": 1704164645000,
		"2024-01-02T03:04:05.001Z": 1704164645001,
		"2024-01-02T03:04:05.250Z": 1704164645250,
		"2024-01-02T03:04:05.999Z": 1704164645999,
		"1969-12-31T23:59:59.999Z": -1,
	} {
		var exported int64
		require.NoError(t, db.QueryRow("SELECT "+castMainValue("m", meta)+" FROM (SELECT CAST(? AS VARCHAR) AS m) t", image).Scan(&exported))
		require.Equal(t, want, exported, image)
	}
}
