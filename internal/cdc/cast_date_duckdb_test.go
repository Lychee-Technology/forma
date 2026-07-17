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
// projection. Pre-#219 the iso8601/default branches produced native
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
		// this encoding: iso8601 -> RFC3339 text; unix_ms/default -> epoch-ms bigint.
		sourceExpr string
	}{
		{
			name:       "iso8601_text",
			meta:       forma.AttributeMetadata{ValueType: forma.ValueTypeDateTime, ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText02, Encoding: forma.MainColumnEncodingISO8601}},
			sourceExpr: "'2024-01-02T03:04:05Z'",
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
