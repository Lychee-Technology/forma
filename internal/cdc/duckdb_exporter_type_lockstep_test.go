package cdc

import (
	"database/sql"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

// eavExportFixture mimics the eav_data columns castEAVValue reads, at the
// types the export query actually carries: value_numeric is DOUBLE (the
// numeric-family column every scalar cast starts from) and value_text is
// VARCHAR.
const eavExportFixture = "(SELECT CAST(1 AS DOUBLE) AS value_numeric, CAST('x' AS VARCHAR) AS value_text)"

// TestCastEAVValueMatchesNullScanTypeof is the #255 type-lockstep proof for
// the CDC export leg, against a real DuckDB engine: the parquet column type
// the exporter mints for an attribute must equal the type
// sqlgen.DuckDBNullScanType names for the same attribute. The federated read
// UNIONs a NULL-augmented parquet leg (for a column no generation carries
// yet) with real parquet columns written by this exporter — divergence widens
// the UNION ALL and re-opens #205.
//
// The comparison is on typeof() rather than on the rendered SQL text: bool
// renders as `(value_numeric <> 0)`, which carries no type literal to
// extract — only the engine knows the resulting type.
func TestCastEAVValueMatchesNullScanTypeof(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	scalars := []forma.ValueType{
		forma.ValueTypeBool, forma.ValueTypeBigInt, forma.ValueTypeDate,
		forma.ValueTypeDateTime, forma.ValueTypeInteger, forma.ValueTypeSmallInt,
		forma.ValueTypeNumeric, forma.ValueTypeText, forma.ValueTypeUUID,
	}
	for _, vt := range scalars {
		t.Run(string(vt), func(t *testing.T) {
			exportExpr := castEAVValue(forma.AttributeMetadata{ValueType: vt})

			var exported string
			require.NoError(t,
				db.QueryRow("SELECT typeof(("+exportExpr+")) FROM "+eavExportFixture).Scan(&exported),
				"export cast %q must evaluate", exportExpr)

			var scan string
			require.NoError(t,
				db.QueryRow("SELECT typeof(NULL::"+sqlgen.DuckDBNullScanType(forma.AttributeMetadata{ValueType: vt})+")").Scan(&scan))

			require.Equal(t, exported, scan,
				"cold NULL scan type must equal the exported parquet column type for %s (#205 no-widening)", vt)
		})
	}
}
