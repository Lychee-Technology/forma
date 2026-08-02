package sqlgen

import (
	"strings"
	"testing"

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
