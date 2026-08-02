package sqlgen

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// Idle-state invariant (#255): no missing columns renders the exact
// pre-#255 scan text, keeping every rendered-SQL contract byte-identical.
func TestBuildParquetScanSourceEmptyIsBareReadParquet(t *testing.T) {
	got := BuildParquetScanSource("'s3://b/base/a.parquet'", nil)
	require.Equal(t, "read_parquet('s3://b/base/a.parquet', union_by_name=true)", got)
}

func TestBuildParquetScanSourceWrapsMissingColumnsAsTypedNulls(t *testing.T) {
	got := BuildParquetScanSource("['s3://b/a.parquet', 's3://b/b.parquet']", []NullScanColumn{
		{Name: "score", DuckDBType: "INTEGER"},
		{Name: "tags", DuckDBType: "BIGINT[]"},
	})
	require.Equal(t,
		"(SELECT *, NULL::INTEGER AS score, NULL::BIGINT[] AS tags "+
			"FROM read_parquet(['s3://b/a.parquet', 's3://b/b.parquet'], union_by_name=true)) AS cold_scan",
		got)
}

// TestDuckDBNullScanTypeMirrorsTierTypes documents the intended map as a
// literal table. It cannot detect divergence from the other tiers on its own
// (it validates the table against itself) — the cross-leg proof is
// TestDuckDBNullScanTypeMatchesHotLegTypeof below (hot pivot) and
// cdc.TestCastEAVValueMatchesNullScanTypeof (export leg).
func TestDuckDBNullScanTypeMirrorsTierTypes(t *testing.T) {
	cases := []struct {
		vt, items forma.ValueType
		want      string
	}{
		{forma.ValueTypeBool, "", "BOOLEAN"},
		{forma.ValueTypeBigInt, "", "BIGINT"},
		{forma.ValueTypeDate, "", "BIGINT"},
		{forma.ValueTypeDateTime, "", "BIGINT"},
		{forma.ValueTypeInteger, "", "INTEGER"},
		{forma.ValueTypeSmallInt, "", "SMALLINT"},
		{forma.ValueTypeNumeric, "", "DOUBLE"},
		{forma.ValueTypeText, "", "VARCHAR"},
		{forma.ValueTypeUUID, "", "VARCHAR"},
		{forma.ValueTypeList, forma.ValueTypeBigInt, "BIGINT[]"},
		{forma.ValueTypeList, forma.ValueTypeText, "VARCHAR[]"},
	}
	for _, c := range cases {
		require.Equal(t, c.want, DuckDBNullScanType(c.vt, c.items), "vt=%s items=%s", c.vt, c.items)
	}
}

// minimalAdvancedParams builds a fully renderable advanced-template param map
// whose parquet scan source carries the given cold-missing columns. It mirrors
// the production param set (buildDuckDBQueryWithPlan) closely enough to render
// the template directly, without going through BuildDuckDBQuery — so the two
// scan sites can be counted in isolation.
func minimalAdvancedParams(t *testing.T, missing []NullScanColumn) map[string]any {
	t.Helper()
	return injectTestRenderParams(t, map[string]any{
		"PG_CONN":              "dbname=forma host=localhost",
		"ChangeLogSchema":      "public",
		"ChangeLogScanTable":   "change_log",
		"MainSchema":           "public",
		"MainScanTable":        "entity_main_dev",
		"EAVSchema":            "public",
		"EAVScanTable":         "eav_data_dev",
		"SCHEMA_ID":            int16(1),
		"HasHot":               true,
		"HasDirtyIDs":          false,
		"DirtyIDsCSV":          "",
		"FlushGraceCutoffMs":   FlushGraceCutoffDisabled,
		"LOGICAL_WHERE_CLAUSE": "1=1",
		"PG_WHERE_CLAUSE":      "1=1",
		"HAS_KEYSET":           false,
		"PAGE_SIZE":            10,
		"OFFSET":               0,
		"NON_KEYSET_ORDER_BY":  "created_at DESC, row_id ASC",
		"S3_SCAN_SOURCE":       BuildParquetScanSource("'s3://b/a.parquet'", missing),
	}, 1)
}

// The advanced template must consume the scan source at BOTH sites:
// s3_source's FROM and the pushdown semijoin's inner FROM.
func TestAdvancedTemplateRendersScanSourceAtBothSites(t *testing.T) {
	params := map[string]any{"S3_PATHS": "'s3://b/base/a.parquet'"}
	injectDuckDBTemplateParams(params, nil, nil)
	// nil query keeps this a pure param-derivation probe.
	src, _ := params["S3_SCAN_SOURCE"].(string)
	require.Equal(t, "read_parquet('s3://b/base/a.parquet', union_by_name=true)", src)

	params = map[string]any{
		"S3_PATHS":           "'s3://b/base/a.parquet'",
		"ColdMissingColumns": []NullScanColumn{{Name: "score", DuckDBType: "INTEGER"}},
	}
	injectDuckDBTemplateParams(params, nil, nil)
	src, _ = params["S3_SCAN_SOURCE"].(string)
	require.Contains(t, src, "NULL::INTEGER AS score")

	// End-to-end render: exactly the template's two scan sites carry the
	// wrapped source (rendered via the real advanced template).
	var b strings.Builder
	require.NoError(t, AdvancedQueryTemplateDuckDB.Execute(&b, minimalAdvancedParams(t, []NullScanColumn{{Name: "score", DuckDBType: "INTEGER"}})))
	require.Equal(t, 2, strings.Count(b.String(), "NULL::INTEGER AS score"))
	require.Equal(t, 2, strings.Count(b.String(), "read_parquet("))
}

// eavFixtureSubquery mimics the eav_data columns the hot-leg pivot reads, at
// the types they actually carry in the pivot: value_numeric is DOUBLE (the
// numeric-family column every scalar cast starts from) and value_text is
// VARCHAR.
const eavFixtureSubquery = "(SELECT CAST(1 AS DOUBLE) AS value_numeric, CAST('x' AS VARCHAR) AS value_text)"

// TestDuckDBNullScanTypeMatchesHotLegTypeof is the #255 type-lockstep proof
// against a real DuckDB engine: for every value type, the type DuckDB gives
// the hot-leg EAV cast (eavElementCastExpr) must equal the type DuckDB gives
// NULL::DuckDBNullScanType(...). Divergence widens the UNION ALL between the
// pg_source leg and the NULL-augmented parquet leg and re-opens #205.
//
// The comparison is on typeof() rather than on the rendered SQL text: bool
// renders as `(value_numeric <> 0)` and text as a bare `value_text`, so
// neither carries a type literal to extract — only the engine knows.
func TestDuckDBNullScanTypeMatchesHotLegTypeof(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	typeOf := func(t *testing.T, expr string) string {
		t.Helper()
		var typ string
		require.NoError(t, db.QueryRow("SELECT typeof("+expr+")").Scan(&typ), "expr %q must evaluate", expr)
		return typ
	}

	scalars := []forma.ValueType{
		forma.ValueTypeBool, forma.ValueTypeBigInt, forma.ValueTypeDate,
		forma.ValueTypeDateTime, forma.ValueTypeInteger, forma.ValueTypeSmallInt,
		forma.ValueTypeNumeric, forma.ValueTypeText, forma.ValueTypeUUID,
	}
	for _, vt := range scalars {
		t.Run(string(vt), func(t *testing.T) {
			var hot string
			hotExpr := "typeof((" + eavElementCastExpr(vt) + ")) FROM " + eavFixtureSubquery
			require.NoError(t, db.QueryRow("SELECT "+hotExpr).Scan(&hot),
				"hot-leg cast %q must evaluate", eavElementCastExpr(vt))

			cold := typeOf(t, "NULL::"+DuckDBNullScanType(vt, ""))
			require.Equal(t, hot, cold,
				"cold NULL scan type must equal the hot-leg EAV cast type for %s (#205 no-widening)", vt)
		})
	}

	// LIST: the elem+"[]" construction has to name the same type DuckDB gives
	// an aggregated list of the element cast.
	t.Run("list_bigint", func(t *testing.T) {
		var hot string
		require.NoError(t, db.QueryRow(
			"SELECT typeof(list(x)) FROM (SELECT "+eavElementCastExpr(forma.ValueTypeBigInt)+
				" AS x FROM "+eavFixtureSubquery+")").Scan(&hot))
		cold := typeOf(t, "NULL::"+DuckDBNullScanType(forma.ValueTypeList, forma.ValueTypeBigInt))
		require.Equal(t, hot, cold, "LIST element type must round-trip through the []-suffix construction")
	})
}
